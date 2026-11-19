from datetime import datetime, timedelta
from typing import Optional

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.agents.agent_runner import gerar_resumo
from src.database.models import Agente, Sessao
from src.database.models import InboundEvent, OutboxDelivery
from src.database.repositories import (
    consumo_repo,
    handoff_attempt_repo,
    outbox_repo,
    sessao_repo,
)
from src.services.evolution_client import evolution_client
from src.services import chatwoot_client
from src.services.delivery_adapters import (
    decode_chatwoot_handoff,
    decode_evolution_text,
)


MENSAGEM_TRANSICAO = (
    "Obrigado pela sua mensagem! Um de nossos especialistas "
    "entrará em contato com você em breve para continuar o atendimento. 😊"
)


def _resumo_fallback(sessao_id: int, telefone_lead: str) -> str:
    return f"Handoff solicitado. Sessão: {sessao_id} | Lead: {telefone_lead}"


async def planejar_handoff(
    *,
    agente: Agente,
    sessao: Sessao,
    telefone_lead: str,
    resumo: str,
    inbound_event: InboundEvent,
    cutover_epoch: str,
    db: AsyncSession,
    now: datetime,
    setup_ttl_seconds: int,
) -> tuple[OutboxDelivery, OutboxDelivery]:
    """Stage the complete handoff lifecycle without provider I/O.

    The caller holds the session lock. Repository calls preserve the global
    runtime -> session -> attempt -> delivery lock order. The transition gets
    its Evolution sequence immediately and remains unscheduled until the
    Chatwoot job is confirmed by the worker.
    """

    inbox_id = agente.chatwoot_inbox_id
    if not isinstance(inbox_id, int) or isinstance(inbox_id, bool) or inbox_id < 1:
        raise ValueError("agent has no valid Chatwoot inbox")

    resumo_normalizado = resumo.strip()
    if not resumo_normalizado:
        raise ValueError("handoff summary must not be blank")

    attempt = await handoff_attempt_repo.create_for_locked_session(
        db,
        sessao=sessao,
        chatwoot_inbox_id=inbox_id,
    )
    open_payload = {
        "phone": telefone_lead,
        "summary": resumo_normalizado,
        "inbox_id": inbox_id,
        "handoff_key": attempt.handoff_key,
    }
    decode_chatwoot_handoff("chatwoot.open_handoff.v1", 1, open_payload)
    open_job, _ = await outbox_repo.enqueue(
        db,
        cutover_epoch=cutover_epoch,
        dedupe_key=f"chatwoot:handoff:{attempt.handoff_key}:open",
        provider="chatwoot",
        operation="chatwoot.open_handoff.v1",
        payload_version=1,
        payload=open_payload,
        ordering_key=attempt.handoff_key,
        stream_sequence=0,
        next_attempt_at=now,
        expires_at=now + timedelta(seconds=setup_ttl_seconds),
        priority=10,
        agente_id=agente.id,
        sessao_id=sessao.id,
        handoff_attempt_id=attempt.id,
        inbound_event_id=inbound_event.id,
        _delivery_class="provider_setup_context",  # S5: explicit new-row intent
    )
    transition_sequence = (
        await sessao_repo.allocate_delivery_sequences(db, sessao, count=1)
    )[0]
    transition_payload = {
        "phone": telefone_lead,
        "message": MENSAGEM_TRANSICAO,
        "instance_name": agente.instance_name,
    }
    decode_evolution_text("evolution.send_text.v1", 1, transition_payload)
    transition_job, _ = await outbox_repo.reserve_transition(
        db,
        dependency=open_job,
        cutover_epoch=cutover_epoch,
        dedupe_key=f"evolution:handoff:{attempt.handoff_key}:transition",
        payload_version=1,
        payload=transition_payload,
        ordering_key=sessao.delivery_stream_key,
        stream_sequence=transition_sequence,
        priority=20,
        agente_id=agente.id,
        sessao_id=sessao.id,
        handoff_attempt_id=attempt.id,
        inbound_event_id=inbound_event.id,
    )
    await sessao_repo.encerrar_locked(db, sessao, motivo="handoff")
    return open_job, transition_job


async def executar_handoff_direto(
    agente: Agente,
    sessao: Sessao,
    telefone_lead: str,
    db: AsyncSession,
    resumo_preparado: str | None = None,
) -> None:
    """
    Compatibility-only direct owner for pre-cutover installations.

    The authoritative runtime branch in ``evolution_orchestrator`` is the only
    caller. Outbox mode calls ``planejar_handoff`` and never enters here.

    Fluxo:
    1. Gera resumo via IA
    2. Busca/Cria contato no Chatwoot
    3. Cria conversa no inbox configurado para o agente
    4. Envia mensagem de transição ao lead via Evolution
    5. Encerra a sessao local com chatwoot_conversation_id (se disponível)

    Falhas na API do Chatwoot são logadas, mas não impedem o encerramento da sessão.
    """
    conversation_id: Optional[int] = None

    resumo = resumo_preparado
    if resumo is None:
        resumo = await preparar_resumo_handoff(
            agente=agente,
            sessao=sessao,
            telefone_lead=telefone_lead,
            db=db,
        )
    resumo = resumo.strip() or _resumo_fallback(sessao.id, telefone_lead)

    # Integração com Chatwoot
    try:
        inbox_id = agente.chatwoot_inbox_id
        if not inbox_id:
            raise ValueError(f"Agente {agente.id} não possui chatwoot_inbox_id configurado")

        contact_id = await chatwoot_client.get_or_create_contact(telefone_lead)
        conversation_id = await chatwoot_client.create_conversation(
            contact_id=contact_id,
            inbox_id=inbox_id,
            resumo=resumo,
        )
        logger.info(
            "Handoff: conversa criada no Chatwoot | sessao={} | conv={}",
            sessao.id,
            conversation_id,
        )
    except Exception as exc:
        logger.error(
            "Handoff: falha na integração com Chatwoot | sessao={} | agente={} | "
            "erro_tipo={}",
            sessao.id,
            agente.id,
            type(exc).__name__,
        )
        # Não re-raise: prosseguir para encerrar sessao

    # Mensagem de transição ao lead
    try:
        await evolution_client.send_text_message(telefone_lead, MENSAGEM_TRANSICAO, agente.instance_name)
    except Exception as exc:
        logger.error(
            "Handoff: erro ao enviar mensagem de transição | sessao={} | erro_tipo={}",
            sessao.id,
            type(exc).__name__,
        )

    # Encerramento da sessão — sempre executado
    await sessao_repo.encerrar_sessao(
        db=db,
        sessao_id=sessao.id,
        motivo="handoff",
        chatwoot_conversation_id=conversation_id,
    )

    logger.info(
        "Handoff concluído | sessao={} | chatwoot_conv={}",
        sessao.id,
        conversation_id,
    )


async def preparar_resumo_handoff(
    *,
    agente: Agente,
    sessao: Sessao,
    telefone_lead: str,
    db: AsyncSession,
) -> str:
    """Prepare summary before any handoff delivery side effect.

    Provider failures retain the legacy fallback. A usage durability failure
    is deliberately not caught, allowing the route to roll back and return a
    retryable non-2xx response before contacting either delivery provider.
    """

    try:
        await db.refresh(sessao)
        limite_atingido = await consumo_repo.atingiu_limite_mensal(agente.id, db)
        if limite_atingido:
            logger.warning(
                "Handoff: soft limit mensal atingido; resumo LLM bloqueado | "
                "agente={} sessao={}",
                agente.id,
                sessao.id,
            )
            return _resumo_fallback(sessao.id, telefone_lead)
        else:
            resumo = (await gerar_resumo(sessao.id, agente, db)).strip()
            if resumo:
                return resumo
            logger.warning(
                "Handoff: resumo LLM vazio; usando fallback | agente={} sessao={}",
                agente.id,
                sessao.id,
            )
            return _resumo_fallback(sessao.id, telefone_lead)
    except consumo_repo.UsagePersistenceInvariantError:
        raise
    except Exception as exc:
        logger.error(
            "Handoff: erro ao gerar resumo | sessao={} | erro_tipo={}",
            sessao.id,
            type(exc).__name__,
        )
        return _resumo_fallback(sessao.id, telefone_lead)
