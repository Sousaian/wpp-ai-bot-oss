from datetime import datetime
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.database.models import Sessao
from loguru import logger


async def get_by_agente_telefone(db: AsyncSession, agente_id: int, telefone: str) -> Sessao | None:
    """Busca sessão pelo par agente+telefone — usado no webhook handler"""
    result = await db.execute(
        select(Sessao).where(
            Sessao.agente_id == agente_id,
            Sessao.telefone == telefone
        )
    )
    return result.scalar_one_or_none()


async def create(db: AsyncSession, agente_id: int, telefone: str) -> Sessao:
    """Cria nova sessão"""
    sessao = Sessao(agente_id=agente_id, telefone=telefone)
    db.add(sessao)
    await db.flush()
    await db.refresh(sessao)
    logger.info("Sessão criada: agente={} sessao={}", agente_id, sessao.id)
    return sessao


async def get_or_create_for_update(
    db: AsyncSession,
    agente_id: int,
    telefone: str,
) -> Sessao:
    """Atomically get/create a session and hold its row lock until commit.

    The random stream key is generated before the upsert but is only retained
    by the transaction that wins first creation. Duplicate-key handling is
    limited to the existing ``(agente_id, telefone)`` row and does not hide
    unrelated constraint failures.
    """

    stmt = mysql_insert(Sessao).values(
        agente_id=agente_id,
        telefone=telefone,
        delivery_stream_key=str(uuid4()),
    )
    stmt = stmt.on_duplicate_key_update(id=Sessao.id)
    await db.execute(stmt)

    result = await db.execute(
        select(Sessao)
        .where(
            Sessao.agente_id == agente_id,
            Sessao.telefone == telefone,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result.scalar_one()


async def lock_by_id(
    db: AsyncSession,
    sessao_id: int,
    *,
    refresh_existing: bool = False,
) -> Sessao | None:
    """Lock a session, optionally replacing any stale identity-map snapshot."""

    query = select(Sessao).where(Sessao.id == sessao_id).with_for_update()
    if refresh_existing:
        query = query.execution_options(populate_existing=True)
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def lock_by_agente_telefone(
    db: AsyncSession,
    agente_id: int,
    telefone: str,
) -> Sessao | None:
    result = await db.execute(
        select(Sessao)
        .where(
            Sessao.agente_id == agente_id,
            Sessao.telefone == telefone,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def lock_all_by_agent(
    db: AsyncSession,
    agente_id: int,
) -> list[Sessao]:
    """Lock every session before an agent-wide delivery/delete decision."""

    result = await db.execute(
        select(Sessao)
        .where(Sessao.agente_id == agente_id)
        .order_by(Sessao.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(result.scalars().all())


async def allocate_delivery_sequences(
    db: AsyncSession,
    sessao: Sessao,
    *,
    count: int = 1,
) -> list[int]:
    """Allocate consecutive stream positions from an already locked session."""

    if count < 1:
        raise ValueError("count must be positive")
    start = int(sessao.next_delivery_sequence)
    sessao.next_delivery_sequence = start + count
    await db.flush()
    return list(range(start, start + count))


async def incrementar_mensagens_locked(db: AsyncSession, sessao: Sessao) -> None:
    """Increment the counter on the session row already locked by the caller."""

    sessao.total_mensagens = int(sessao.total_mensagens) + 1
    await db.flush()


async def encerrar_locked(
    db: AsyncSession,
    sessao: Sessao,
    *,
    motivo: str,
    chatwoot_conversation_id: int | None = None,
) -> None:
    """Close the session without reacquiring it in another lock order."""

    sessao.handler = "encerrada"
    sessao.encerrada_em = datetime.utcnow()
    sessao.motivo_encerramento = motivo
    if chatwoot_conversation_id is not None:
        sessao.chatwoot_conversation_id = chatwoot_conversation_id
    await db.flush()


async def encerrar_sessao(
    db: AsyncSession,
    sessao_id: int,
    motivo: str,
    chatwoot_conversation_id: int | None = None,
) -> None:
    """Marca a sessão como encerrada com metadados de encerramento."""
    result = await db.execute(select(Sessao).where(Sessao.id == sessao_id))
    sessao = result.scalar_one_or_none()
    if not sessao:
        return

    sessao.handler = "encerrada"
    sessao.encerrada_em = datetime.utcnow()
    sessao.motivo_encerramento = motivo
    if chatwoot_conversation_id is not None:
        sessao.chatwoot_conversation_id = chatwoot_conversation_id

    await db.flush()
    logger.info(f"Sessão encerrada: sessao={sessao_id} motivo={motivo}")


async def incrementar_mensagens(db: AsyncSession, sessao_id: int) -> None:
    """Incrementa contador de mensagens da sessão"""
    await db.execute(
        update(Sessao)
        .where(Sessao.id == sessao_id)
        .values(total_mensagens=Sessao.total_mensagens + 1)
    )
    await db.flush()


async def get_all(db: AsyncSession, agente_id: int = None, handler: str = None) -> list[Sessao]:
    """Lista sessões com filtros opcionais"""
    query = select(Sessao)
    if agente_id:
        query = query.where(Sessao.agente_id == agente_id)
    if handler:
        query = query.where(Sessao.handler == handler)
    result = await db.execute(query.order_by(Sessao.ultima_interacao.desc()))
    return result.scalars().all()


async def get_by_chatwoot_conversation_id(db: AsyncSession, conversation_id: int) -> Sessao | None:
    """Busca sessão pelo ID do Chatwoot com o agente carregado para uso async."""
    result = await db.execute(
        select(Sessao)
        .options(selectinload(Sessao.agente))
        .where(Sessao.chatwoot_conversation_id == conversation_id)
    )
    return result.scalar_one_or_none()
