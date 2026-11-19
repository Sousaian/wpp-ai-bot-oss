from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from loguru import logger
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from src.agents.agent_runner import processar
from src.config import settings
from src.database.models import Agente, InboundEvent, Sessao
from src.database.repositories import (
    agente_repo,
    consumo_repo,
    delivery_state_repo,
    inbound_event_repo,
    mensagem_repo,
    outbox_repo,
    scheduler_repo,
    sessao_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services import handoff_manager
from src.services.evolution_client import evolution_client
from src.services.delivery_adapters import decode_evolution_text


MENSAGEM_LIMITE_ATINGIDO = (
    "No momento, nosso atendimento automático está temporariamente indisponível. "
    "Por favor, tente novamente mais tarde."
)
MENSAGEM_ERRO_TECNICO = (
    "Desculpe, estou com problemas técnicos no momento. "
    "Um atendente vai te ajudar em breve."
)


class InvalidEvolutionEvent(ValueError):
    def __init__(self, reason: str, *, retryable: bool = False) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retryable = retryable


class DeliveryOwnershipError(RuntimeError):
    pass


class ReceiptConfigurationError(RuntimeError):
    pass


class ScheduledIntakeUnavailable(DeliveryOwnershipError):
    pass


@dataclass(frozen=True)
class EvolutionMessageEvent:
    instance_name: str
    provider_message_id: str
    telefone: str
    texto: str
    event_type: str = "messages.upsert"

    @property
    def actionable_fields(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "event_type": self.event_type,
            "instance_name": self.instance_name,
            "provider_message_id": self.provider_message_id,
            "remote_phone": self.telefone,
            "text": self.texto,
        }


@dataclass(frozen=True)
class OrchestrationResult:
    body: dict[str, object]
    http_status: int = 200


def _required_text(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _extract_instance_name(payload: dict[str, Any]) -> str | None:
    instance = payload.get("instance")
    if isinstance(instance, dict):
        return _required_text(instance.get("instanceName") or instance.get("name"))
    direct = _required_text(instance)
    if direct:
        return direct
    data = payload.get("data")
    return _required_text(data.get("instance")) if isinstance(data, dict) else None


def _normalize_phone(remote_jid: object) -> str | None:
    value = _required_text(remote_jid)
    if value is None or value.endswith("@g.us"):
        return None
    normalized = value.removesuffix("@s.whatsapp.net")
    if not normalized or not normalized.replace("+", "", 1).isdigit():
        return None
    return normalized


def _extract_text(message: object) -> str | None:
    if not isinstance(message, dict):
        return None
    conversation = _required_text(message.get("conversation"))
    if conversation:
        return conversation
    extended = message.get("extendedTextMessage")
    return _required_text(extended.get("text")) if isinstance(extended, dict) else None


def parse_event(payload: object) -> EvolutionMessageEvent | OrchestrationResult:
    if not isinstance(payload, dict):
        raise InvalidEvolutionEvent("invalid_payload")
    event_type = payload.get("event")
    if event_type != "messages.upsert":
        return OrchestrationResult(
            {"status": "ignored", "reason": "unsupported_event", "event": event_type}
        )
    instance_name = _extract_instance_name(payload)
    if instance_name is None:
        raise InvalidEvolutionEvent("missing_instance")
    if len(instance_name) > 255:
        raise InvalidEvolutionEvent("invalid_instance")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise InvalidEvolutionEvent("invalid_payload")
    key = data.get("key")
    if not isinstance(key, dict):
        raise InvalidEvolutionEvent("invalid_payload")
    from_me = key.get("fromMe")
    if from_me is True:
        return OrchestrationResult({"status": "ignored", "reason": "own_message"})
    if from_me is not False:
        raise InvalidEvolutionEvent("invalid_from_me")
    telefone = _normalize_phone(key.get("remoteJid"))
    if telefone is None:
        return OrchestrationResult({"status": "ignored", "reason": "invalid_phone"})
    if len(telefone) > 50:
        raise InvalidEvolutionEvent("invalid_phone")
    if not isinstance(data.get("message"), dict):
        return OrchestrationResult(
            {"status": "ignored", "reason": "missing_message_content"}
        )
    texto = _extract_text(data["message"])
    if texto is None:
        return OrchestrationResult({"status": "ignored", "reason": "no_text"})
    provider_message_id = _required_text(key.get("id"))
    if provider_message_id is None:
        raise InvalidEvolutionEvent("missing_provider_message_id")
    if len(provider_message_id) > 255:
        raise InvalidEvolutionEvent("invalid_provider_message_id")
    return EvolutionMessageEvent(
        instance_name=instance_name,
        provider_message_id=provider_message_id,
        telefone=telefone,
        texto=texto,
    )


def _receipt_keyring() -> inbound_event_repo.ReceiptHMACKeyRing:
    configured: dict[int, bytes] = {}
    for version, secret in settings.inbound_receipt_hmac_keys.items():
        value = secret.get_secret_value() if isinstance(secret, SecretStr) else str(secret)
        if not value:
            raise ReceiptConfigurationError("receipt HMAC key is blank")
        configured[int(version)] = value.encode()
    if not configured:
        raise ReceiptConfigurationError("receipt HMAC key ring is unavailable")
    return inbound_event_repo.ReceiptHMACKeyRing(
        current_version=settings.inbound_receipt_hmac_current_version,
        keys=configured,
    )


def _validate_runtime_ownership(state: object) -> str:
    mode = getattr(state, "mode", None)
    epoch = getattr(state, "cutover_epoch", None)
    if mode == "frozen":
        raise DeliveryOwnershipError("delivery runtime is frozen")
    if mode != settings.delivery_mode:
        raise DeliveryOwnershipError("configured delivery mode does not match database")
    if mode == "outbox":
        if not settings.delivery_cutover_epoch or epoch != settings.delivery_cutover_epoch:
            raise DeliveryOwnershipError("configured delivery epoch does not match database")
    if mode not in {"direct", "outbox"}:
        raise DeliveryOwnershipError("unsupported delivery runtime mode")
    return mode


async def _send_direct(
    *,
    agente: Agente,
    telefone: str,
    mensagem: str,
) -> bool:
    mensagem = mensagem.strip()
    if not mensagem:
        raise ValueError("direct Evolution message must not be blank")
    try:
        await evolution_client.send_text_message(
            telefone,
            mensagem,
            agente.instance_name,
        )
        return True
    except Exception as exc:
        logger.error(
            "Evolution direct delivery failed | agente={} erro_tipo={}",
            agente.id,
            type(exc).__name__,
        )
        return False


async def _stage_evolution_text(
    db: AsyncSession,
    *,
    agente: Agente,
    sessao: Sessao,
    inbound_event: InboundEvent,
    cutover_epoch: str,
    telefone: str,
    mensagem: str,
    intent: str,
    now: datetime,
) -> None:
    payload = {
        "phone": telefone,
        "message": mensagem.strip(),
        "instance_name": agente.instance_name,
    }
    decode_evolution_text("evolution.send_text.v1", 1, payload)
    sequence = (await sessao_repo.allocate_delivery_sequences(db, sessao))[0]
    await outbox_repo.enqueue(
        db,
        cutover_epoch=cutover_epoch,
        dedupe_key=f"evolution:inbound:{inbound_event.id}:{intent}",
        provider="evolution",
        operation="evolution.send_text.v1",
        payload_version=1,
        payload=payload,
        ordering_key=sessao.delivery_stream_key,
        stream_sequence=sequence,
        next_attempt_at=now,
        expires_at=now
        + timedelta(seconds=settings.delivery_automated_ttl_seconds),
        priority=0,
        agente_id=agente.id,
        sessao_id=sessao.id,
        inbound_event_id=inbound_event.id,
        _delivery_class="legacy_automated",  # S5: explicit new-row intent
    )


async def _record_receipt(
    db: AsyncSession,
    *,
    event: EvolutionMessageEvent,
    agente: Agente,
    receipt_disposition: inbound_event_repo.ReceiptDisposition,
) -> inbound_event_repo.ReceiptResult:
    return await inbound_event_repo.record(
        db,
        provider="evolution",
        source_scope=event.instance_name,
        provider_event_id=event.provider_message_id,
        event_type=event.event_type,
        actionable_fields=event.actionable_fields,
        keyring=_receipt_keyring(),
        receipt_disposition=receipt_disposition,
        agente_id=agente.id,
    )


async def _orchestrate_scheduled_intake(
    *,
    event: EvolutionMessageEvent,
    uow: UnitOfWork,
    authenticated: bool,
    agente_id: int,
) -> OrchestrationResult:
    if not authenticated or settings.provider_webhook_auth_mode != "enforced":
        raise ScheduledIntakeUnavailable("scheduled intake requires enforced auth")
    if not settings.inbox_scheduler_enabled or not settings.inbox_scheduler_epoch:
        raise ScheduledIntakeUnavailable("scheduled intake feature gate is disabled")
    if not settings.delivery_cutover_epoch:
        raise ScheduledIntakeUnavailable("scheduled intake delivery epoch is unavailable")

    db = uow.session
    try:
        runtime, scheduler, agente, lane = await scheduler_repo.lock_authority_prefix(
            db,
            agente_id=agente_id,
            scheduler_epoch=settings.inbox_scheduler_epoch,
            delivery_runtime_epoch=settings.delivery_cutover_epoch,
            require_enabled=False,
            create_lane=True,
        )
        if (
            runtime.mode != settings.delivery_mode
            or agente.instance_name != event.instance_name
            or not agente.ativo
            or not agente.inbox_scheduler_enabled
        ):
            raise ScheduledIntakeUnavailable("scheduled intake gate changed")

        sessao = await sessao_repo.get_or_create_for_update(
            db,
            agente.id,
            event.telefone,
        )
        graph = await scheduler_repo.lock_intake_turn_graph(
            db,
            sessao_id=sessao.id,
        )
        receipt_result = await _record_receipt(
            db,
            event=event,
            agente=agente,
            receipt_disposition=(
                "scheduled" if sessao.handler == "bot" else "ignored_nonbot"
            ),
        )
        receipt = receipt_result.event

        if receipt_result.replay:
            await scheduler_repo.require_consistent_replay_disposition(
                db,
                receipt=receipt,
            )
            if receipt.receipt_disposition == "scheduled":
                await scheduler_repo.require_complete_intake_replay(
                    db,
                    receipt=receipt,
                    agente_id=agente.id,
                    sessao_id=sessao.id,
                )
            elif receipt.receipt_disposition in {
                "ignored_nonbot",
                "legacy_evolution",
            }:
                if (
                    receipt.processed_at is None
                    or receipt.agente_id != agente.id
                    or receipt.sessao_id != sessao.id
                ):
                    raise scheduler_repo.IncompleteIntakeMembershipError(
                        "non-scheduled historical receipt is incomplete"
                    )
            else:
                raise scheduler_repo.IncompleteIntakeMembershipError(
                    "historical receipt disposition requires audited repair"
                )
            await uow.commit()
            return OrchestrationResult(
                {"status": "already_accepted", "delivery_status": "already_accepted"},
                http_status=200,
            )

        # T08-1: replay precedes the state gate, never capacity admission.
        # New identity/session writes in this UoW roll back on refusal.
        scheduler_repo.require_new_intake_enabled(scheduler)

        if sessao.handler != "bot":
            await inbound_event_repo.mark_processed(
                db,
                receipt.id,
                agente_id=agente.id,
                sessao_id=sessao.id,
            )
            await uow.commit()
            reason = "human_session" if sessao.handler == "humano" else "closed_session"
            return OrchestrationResult({"status": "ignored", "reason": reason})

        now = await delivery_state_repo.get_database_now(db)
        incoming_bytes = len(event.texto.encode("utf-8"))
        target = await scheduler_repo.select_or_create_intake_target(
            db,
            scheduler=scheduler,
            lane=lane,
            sessao=sessao,
            scheduler_epoch=settings.inbox_scheduler_epoch,
            incoming_bytes=incoming_bytes,
            now=now,
            locked_graph=graph,
        )
        message = await mensagem_repo.salvar(db, sessao.id, "user", event.texto)
        _, created = await scheduler_repo.append_input_membership(
            db,
            scheduler=scheduler,
            turn_id=target.turn.id,
            sessao_id=sessao.id,
            mensagem_id=message.id,
            inbound_event_id=receipt.id,
            content_bytes=incoming_bytes,
        )
        if not created:
            raise scheduler_repo.SchedulerConflictError(
                "new receipt unexpectedly reused a turn membership"
            )
        await scheduler_repo.update_intake_readiness(db, turn=target.turn)
        await uow.commit()
        return OrchestrationResult(
            {"status": "accepted", "delivery_status": "scheduled"},
            http_status=202,
        )
    except (
        scheduler_repo.SchedulerCapExceeded,
        scheduler_repo.SchedulerConflictError,
        scheduler_repo.SchedulerNotReadyError,
    ) as exc:
        logger.warning(
            "Scheduled intake failed closed | erro_tipo={} motivo={}",
            type(exc).__name__,
            str(exc),
        )
        raise ScheduledIntakeUnavailable(str(exc)) from exc


async def orchestrate(
    *,
    event: EvolutionMessageEvent,
    uow: UnitOfWork,
    authenticated: bool,
) -> OrchestrationResult:
    db = uow.session
    preliminary_agent = await agente_repo.get_by_instance_name(
        db,
        event.instance_name,
    )
    preliminary_agent_id = preliminary_agent.id if preliminary_agent is not None else None
    scheduler_opt_in = (
        preliminary_agent is not None and preliminary_agent.inbox_scheduler_enabled
    )
    # The gate read is a plain REPEATABLE READ select. End its transaction so
    # the read view used for limit, history, cap and sequence decisions opens
    # only after the runtime/agent/session (or scheduler mutex) locks below.
    # Both branches re-lock and re-validate the agent with fresh reads; the
    # expired preliminary ORM identity is never consulted again.
    del preliminary_agent
    await db.rollback()
    if preliminary_agent_id is None:
        await uow.commit()
        return OrchestrationResult({"status": "ignored", "reason": "agent_not_found"})
    if scheduler_opt_in:
        return await _orchestrate_scheduled_intake(
            event=event,
            uow=uow,
            authenticated=authenticated,
            agente_id=preliminary_agent_id,
        )

    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)
    mode = _validate_runtime_ownership(runtime)
    if mode == "outbox" and not authenticated:
        raise DeliveryOwnershipError("outbox ingress requires enforced authentication")

    agente = await agente_repo.get_by_instance_name(
        db,
        event.instance_name,
        for_share=True,
    )
    if agente is None:
        await uow.commit()
        return OrchestrationResult({"status": "ignored", "reason": "agent_not_found"})
    if agente.inbox_scheduler_enabled:
        raise ScheduledIntakeUnavailable("agent scheduler opt-in changed")

    receipt_result: inbound_event_repo.ReceiptResult | None = None
    if authenticated:
        receipt_result = await _record_receipt(
            db,
            event=event,
            agente=agente,
            receipt_disposition="legacy_evolution",
        )
        if receipt_result.replay:
            if receipt_result.event.processed_at is None:
                raise RuntimeError("durable receipt exists without processed work")
            await uow.commit()
            return OrchestrationResult(
                {
                    "status": "already_accepted",
                    "delivery_status": "already_accepted",
                }
            )

    sessao = await sessao_repo.get_or_create_for_update(
        db,
        agente.id,
        event.telefone,
    )
    receipt = receipt_result.event if receipt_result is not None else None

    if sessao.handler == "humano":
        if receipt is not None:
            await inbound_event_repo.mark_processed(
                db,
                receipt.id,
                agente_id=agente.id,
                sessao_id=sessao.id,
            )
        await uow.commit()
        return OrchestrationResult({"status": "ignored", "reason": "human_session"})
    if sessao.handler == "encerrada":
        if receipt is not None:
            await inbound_event_repo.mark_processed(
                db,
                receipt.id,
                agente_id=agente.id,
                sessao_id=sessao.id,
            )
        await uow.commit()
        return OrchestrationResult({"status": "ignored", "reason": "closed_session"})

    now = await delivery_state_repo.get_database_now(db)
    limit_reached = await consumo_repo.atingiu_limite_mensal(agente.id, db)
    if limit_reached:
        if mode == "outbox":
            assert receipt is not None
            await _stage_evolution_text(
                db,
                agente=agente,
                sessao=sessao,
                inbound_event=receipt,
                cutover_epoch=runtime.cutover_epoch,
                telefone=event.telefone,
                mensagem=MENSAGEM_LIMITE_ATINGIDO,
                intent="soft-limit",
                now=now,
            )
            delivery_status = "queued"
        else:
            delivered = await _send_direct(
                agente=agente,
                telefone=event.telefone,
                mensagem=MENSAGEM_LIMITE_ATINGIDO,
            )
            delivery_status = "sent" if delivered else "failed"
        if receipt is not None:
            await inbound_event_repo.mark_processed(
                db,
                receipt.id,
                agente_id=agente.id,
                sessao_id=sessao.id,
            )
        await uow.commit()
        return OrchestrationResult(
            {
                "status": "accepted",
                "reason": "temporarily_unavailable",
                "delivery_status": delivery_status,
            }
        )

    try:
        resposta, needs_transfer = await processar(
            agente,
            sessao.id,
            event.texto,
            db,
        )
        resumo: str | None = None
        if needs_transfer:
            resumo = await handoff_manager.preparar_resumo_handoff(
                agente=agente,
                sessao=sessao,
                telefone_lead=event.telefone,
                db=db,
            )
    except consumo_repo.UsagePersistenceInvariantError:
        raise
    except Exception as exc:
        logger.error(
            "Evolution message processing failed | agente={} sessao={} erro_tipo={}",
            agente.id,
            sessao.id,
            type(exc).__name__,
        )
        await sessao_repo.encerrar_locked(db, sessao, motivo="falha_tecnica")
        if mode == "outbox":
            assert receipt is not None
            await _stage_evolution_text(
                db,
                agente=agente,
                sessao=sessao,
                inbound_event=receipt,
                cutover_epoch=runtime.cutover_epoch,
                telefone=event.telefone,
                mensagem=MENSAGEM_ERRO_TECNICO,
                intent="technical-error",
                now=now,
            )
            delivery_status = "queued"
        else:
            delivered = await _send_direct(
                agente=agente,
                telefone=event.telefone,
                mensagem=MENSAGEM_ERRO_TECNICO,
            )
            delivery_status = "sent" if delivered else "failed"
        if receipt is not None:
            await inbound_event_repo.mark_processed(
                db,
                receipt.id,
                agente_id=agente.id,
                sessao_id=sessao.id,
            )
        await uow.commit()
        return OrchestrationResult(
            {
                "status": "accepted",
                "reason": "processing_error",
                "delivery_status": delivery_status,
            }
        )

    if mode == "outbox":
        assert receipt is not None
        if resposta:
            await _stage_evolution_text(
                db,
                agente=agente,
                sessao=sessao,
                inbound_event=receipt,
                cutover_epoch=runtime.cutover_epoch,
                telefone=event.telefone,
                mensagem=resposta,
                intent="bot-response",
                now=now,
            )
        if needs_transfer:
            assert resumo is not None
            await handoff_manager.planejar_handoff(
                agente=agente,
                sessao=sessao,
                telefone_lead=event.telefone,
                resumo=resumo,
                inbound_event=receipt,
                cutover_epoch=runtime.cutover_epoch,
                db=db,
                now=now,
                setup_ttl_seconds=settings.delivery_handoff_setup_ttl_seconds,
            )
        delivery_status = "queued"
        status = "queued"
    else:
        delivered = True
        delivery_status = "skipped_empty_transfer_response"
        if resposta:
            delivered = await _send_direct(
                agente=agente,
                telefone=event.telefone,
                mensagem=resposta,
            )
            delivery_status = "sent" if delivered else "failed"
        if needs_transfer:
            await handoff_manager.executar_handoff_direto(
                agente=agente,
                sessao=sessao,
                telefone_lead=event.telefone,
                db=db,
                resumo_preparado=resumo,
            )
        status = "success" if delivered else "partial_success"

    if receipt is not None:
        await inbound_event_repo.mark_processed(
            db,
            receipt.id,
            agente_id=agente.id,
            sessao_id=sessao.id,
        )
    await uow.commit()
    return OrchestrationResult(
        {
            "status": status,
            "needs_transfer": needs_transfer,
            "delivery_status": delivery_status,
        }
    )
