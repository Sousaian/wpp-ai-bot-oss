from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from loguru import logger
from pydantic import SecretStr
from sqlalchemy import select

from src.config import settings
from src.database.models import HandoffAttempt, OutboxDelivery, Sessao
from src.database.repositories import (
    agente_repo,
    delivery_state_repo,
    delivery_worker_repo,
    handoff_attempt_repo,
    inbound_event_repo,
    outbox_repo,
    sessao_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services.delivery_adapters import decode_evolution_text
from src.services.evolution_client import evolution_client


class ChatwootOwnershipError(RuntimeError):
    pass


class ChatwootReceiptConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ChatwootReplyEvent:
    account_id: int
    remote_account_id: int | None
    message_id: int
    conversation_id: int
    content: str
    inbox_id: int | None
    contact_id: int | None
    handoff_key: str | None

    @property
    def actionable_fields(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "event_type": "message_created",
            "account_id": self.account_id,
            "remote_account_id": self.remote_account_id,
            "message_id": self.message_id,
            "conversation_id": self.conversation_id,
            "content": self.content,
            "inbox_id": self.inbox_id,
            "contact_id": self.contact_id,
            "handoff_key": self.handoff_key,
        }


@dataclass(frozen=True)
class ChatwootOrchestrationResult:
    body: dict[str, object]
    http_status: int = 200


def _receipt_keyring() -> inbound_event_repo.ReceiptHMACKeyRing:
    keys: dict[int, bytes] = {}
    for version, secret in settings.inbound_receipt_hmac_keys.items():
        value = secret.get_secret_value() if isinstance(secret, SecretStr) else str(secret)
        if not value:
            raise ChatwootReceiptConfigurationError("receipt HMAC key is blank")
        keys[int(version)] = value.encode()
    if not keys:
        raise ChatwootReceiptConfigurationError("receipt HMAC key ring is unavailable")
    return inbound_event_repo.ReceiptHMACKeyRing(
        current_version=settings.inbound_receipt_hmac_current_version,
        keys=keys,
    )


def _validate_runtime(state: object) -> str:
    mode = getattr(state, "mode", None)
    epoch = getattr(state, "cutover_epoch", None)
    if mode == "frozen":
        raise ChatwootOwnershipError("delivery runtime is frozen")
    if mode != settings.delivery_mode:
        raise ChatwootOwnershipError("configured delivery mode does not match database")
    if mode == "outbox":
        if not settings.delivery_cutover_epoch or epoch != settings.delivery_cutover_epoch:
            raise ChatwootOwnershipError("configured delivery epoch does not match database")
    if mode not in {"direct", "outbox"}:
        raise ChatwootOwnershipError("unsupported delivery runtime mode")
    return mode


async def _direct_compatibility(
    *,
    event: ChatwootReplyEvent,
    uow: UnitOfWork,
) -> ChatwootOrchestrationResult:
    """Pre-observation direct owner; never reachable in outbox mode."""

    sessao = await sessao_repo.get_by_chatwoot_conversation_id(
        uow.session, event.conversation_id
    )
    if sessao is None:
        await uow.commit()
        return ChatwootOrchestrationResult(
            {"status": "ignored", "reason": "session_not_found"}
        )
    agente = sessao.agente
    if agente is None:
        await uow.commit()
        return ChatwootOrchestrationResult(
            {"status": "ignored", "reason": "agent_not_found"}
        )
    phone = sessao.telefone
    instance_name = agente.instance_name
    session_id = sessao.id
    await uow.commit()
    try:
        await evolution_client.send_text_message(phone, event.content, instance_name)
    except Exception as exc:
        logger.error(
            "Chatwoot direct compatibility delivery failed | sessao={} "
            "conversation={} erro_tipo={}",
            session_id,
            event.conversation_id,
            type(exc).__name__,
        )
        return ChatwootOrchestrationResult(
            {"status": "error", "reason": "evolution_send_failed"}
        )
    return ChatwootOrchestrationResult(
        {"status": "sent", "conversation": event.conversation_id}
    )


async def _resolve_attempt(
    db,
    event: ChatwootReplyEvent,
) -> tuple[HandoffAttempt | None, OutboxDelivery | None]:
    attempt = await handoff_attempt_repo.get_by_conversation_id(
        db, event.conversation_id
    )
    if attempt is not None:
        return attempt, None
    if event.handoff_key is None:
        return None, None
    delivery = await outbox_repo.get_by_delivery_key(db, event.handoff_key)
    if delivery is None:
        raise ChatwootOwnershipError("Chatwoot marker is not locally resolvable")
    if delivery.handoff_attempt_id is None:
        return None, delivery
    attempt = await db.get(HandoffAttempt, delivery.handoff_attempt_id)
    return attempt, delivery


async def _lock_and_validate_current(
    db,
    *,
    attempt_id: int,
    conversation_id: int,
) -> tuple[Sessao | None, HandoffAttempt | None]:
    initial = await db.get(HandoffAttempt, attempt_id)
    if initial is None:
        return None, None
    sessao = await sessao_repo.lock_by_id(
        db,
        initial.sessao_id,
        refresh_existing=True,
    )
    if sessao is None:
        return None, None
    attempt = (
        await db.execute(
            select(HandoffAttempt)
            .where(HandoffAttempt.id == attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if attempt is None or attempt.sessao_id != sessao.id:
        return sessao, None
    if (
        sessao.handoff_generation != attempt.generation
        or attempt.status != "ready"
        or attempt.chatwoot_conversation_id != conversation_id
        or sessao.chatwoot_conversation_id != conversation_id
        or sessao.handler != "encerrada"
        or sessao.motivo_encerramento != "handoff"
    ):
        return sessao, None
    return sessao, attempt


async def orchestrate(
    *,
    event: ChatwootReplyEvent,
    uow: UnitOfWork,
    authenticated: bool,
) -> ChatwootOrchestrationResult:
    db = uow.session
    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)
    mode = _validate_runtime(runtime)
    if mode == "outbox" and not authenticated:
        raise ChatwootOwnershipError("outbox ingress requires enforced authentication")
    if mode == "direct":
        return await _direct_compatibility(event=event, uow=uow)

    receipt = await inbound_event_repo.record(
        db,
        provider="chatwoot",
        source_scope=str(event.account_id),
        provider_event_id=str(event.message_id),
        event_type="message_created",
        actionable_fields=event.actionable_fields,
        keyring=_receipt_keyring(),
        receipt_disposition="chatwoot",
    )
    if receipt.replay:
        if receipt.event.processed_at is None:
            raise RuntimeError("durable receipt exists without processed work")
        await uow.commit()
        return ChatwootOrchestrationResult(
            {"status": "already_accepted", "delivery_status": "already_accepted"}
        )

    attempt, marker_delivery = await _resolve_attempt(db, event)
    if attempt is None:
        await inbound_event_repo.mark_processed(
            db, receipt.event.id, agente_id=None, sessao_id=None
        )
        await uow.commit()
        return ChatwootOrchestrationResult(
            {"status": "ignored", "reason": "session_not_found"}
        )

    if marker_delivery is not None:
        if (
            event.remote_account_id != event.account_id
            or event.handoff_key is None
            or event.inbox_id is None
            or event.contact_id is None
        ):
            await inbound_event_repo.mark_processed(
                db,
                receipt.event.id,
                agente_id=attempt.agente_id,
                sessao_id=attempt.sessao_id,
            )
            await uow.commit()
            return ChatwootOrchestrationResult(
                {"status": "ignored", "reason": "unverified_handoff_marker"}
            )
        completion = await delivery_worker_repo.complete_chatwoot_handoff_from_callback(
            db,
            sessao_id=attempt.sessao_id,
            attempt_id=attempt.id,
            cutover_epoch=runtime.cutover_epoch,
            contact_id=event.contact_id,
            conversation_id=event.conversation_id,
            delivery_id=marker_delivery.id,
            delivery_key=event.handoff_key,
            inbox_id=event.inbox_id,
            transition_ttl_seconds=settings.delivery_transition_ttl_seconds,
        )
        if completion == "busy":
            raise ChatwootOwnershipError("Chatwoot handoff is owned by a live worker")
        if completion not in {"completed", "already_ready"}:
            await inbound_event_repo.mark_processed(
                db,
                receipt.event.id,
                agente_id=attempt.agente_id,
                sessao_id=attempt.sessao_id,
            )
            await uow.commit()
            return ChatwootOrchestrationResult(
                {
                    "status": "ignored",
                    "reason": (
                        "cancelled_handoff" if completion == "cancelled" else "stale_handoff"
                    ),
                }
            )

    sessao, current = await _lock_and_validate_current(
        db,
        attempt_id=attempt.id,
        conversation_id=event.conversation_id,
    )
    if sessao is None or current is None:
        await inbound_event_repo.mark_processed(
            db,
            receipt.event.id,
            agente_id=attempt.agente_id,
            sessao_id=attempt.sessao_id,
        )
        await uow.commit()
        return ChatwootOrchestrationResult(
            {"status": "ignored", "reason": "stale_handoff"}
        )

    agente = await agente_repo.get_by_id(db, sessao.agente_id)
    if (
        agente is None
        or not agente.ativo
        or agente.chatwoot_inbox_id != current.chatwoot_inbox_id
        or (event.inbox_id is not None and event.inbox_id != current.chatwoot_inbox_id)
        or (
            event.contact_id is not None
            and current.chatwoot_contact_id is not None
            and event.contact_id != current.chatwoot_contact_id
        )
    ):
        await inbound_event_repo.mark_processed(
            db,
            receipt.event.id,
            agente_id=sessao.agente_id,
            sessao_id=sessao.id,
        )
        await uow.commit()
        return ChatwootOrchestrationResult(
            {"status": "ignored", "reason": "handoff_ownership_mismatch"}
        )

    payload = {
        "phone": sessao.telefone,
        "message": event.content,
        "instance_name": agente.instance_name,
    }
    decode_evolution_text("evolution.send_text.v1", 1, payload)
    sequence = (await sessao_repo.allocate_delivery_sequences(db, sessao))[0]
    now = await delivery_state_repo.get_database_now(db)
    await outbox_repo.enqueue(
        db,
        cutover_epoch=runtime.cutover_epoch,
        dedupe_key=f"evolution:chatwoot:{event.account_id}:{event.message_id}",
        provider="evolution",
        operation="evolution.send_text.v1",
        payload_version=1,
        payload=payload,
        ordering_key=sessao.delivery_stream_key,
        stream_sequence=sequence,
        next_attempt_at=now,
        expires_at=now + timedelta(seconds=settings.delivery_human_reply_ttl_seconds),
        priority=30,
        agente_id=agente.id,
        sessao_id=sessao.id,
        handoff_attempt_id=current.id,
        inbound_event_id=receipt.event.id,
        _delivery_class="human",  # S5: explicit new-row intent
    )
    await inbound_event_repo.mark_processed(
        db,
        receipt.event.id,
        agente_id=agente.id,
        sessao_id=sessao.id,
    )
    await uow.commit()
    return ChatwootOrchestrationResult(
        {
            "status": "queued",
            "delivery_status": "queued",
            "conversation": event.conversation_id,
        },
        http_status=202,
    )
