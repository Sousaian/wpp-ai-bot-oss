from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import HandoffAttempt, Sessao


class HandoffStateError(ValueError):
    pass


async def create_for_locked_session(
    db: AsyncSession,
    *,
    sessao: Sessao,
    chatwoot_inbox_id: int,
) -> HandoffAttempt:
    """Start a new lifecycle using the generation on a locked session row."""

    if chatwoot_inbox_id < 1:
        raise ValueError("chatwoot_inbox_id must be positive")
    sessao.handoff_generation = int(sessao.handoff_generation) + 1
    attempt = HandoffAttempt(
        handoff_key=str(uuid4()),
        sessao_id=sessao.id,
        agente_id=sessao.agente_id,
        generation=sessao.handoff_generation,
        status="pending",
        chatwoot_inbox_id=chatwoot_inbox_id,
    )
    db.add(attempt)
    await db.flush()
    return attempt


async def get_current_for_locked_session(
    db: AsyncSession,
    sessao: Sessao,
) -> HandoffAttempt | None:
    if sessao.handoff_generation < 1:
        return None
    result = await db.execute(
        select(HandoffAttempt).where(
            HandoffAttempt.sessao_id == sessao.id,
            HandoffAttempt.generation == sessao.handoff_generation,
        )
    )
    return result.scalar_one_or_none()


async def get_by_conversation_id(
    db: AsyncSession,
    conversation_id: int,
    *,
    for_update: bool = False,
) -> HandoffAttempt | None:
    query = select(HandoffAttempt).where(
        HandoffAttempt.chatwoot_conversation_id == conversation_id
    )
    if for_update:
        query = query.with_for_update()
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def get_by_handoff_key(
    db: AsyncSession,
    handoff_key: str,
    *,
    for_update: bool = False,
) -> HandoffAttempt | None:
    query = select(HandoffAttempt).where(HandoffAttempt.handoff_key == handoff_key)
    if for_update:
        query = query.with_for_update()
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def mark_ready(
    db: AsyncSession,
    *,
    sessao: Sessao,
    attempt_id: int,
    contact_id: int,
    conversation_id: int,
    ready_at: datetime | None = None,
) -> HandoffAttempt:
    """Mark ready after the caller has locked ``sessao``.

    Lifecycle mutations always acquire session, then attempt, then delivery
    rows. Requiring the locked session object prevents the former inverse
    attempt-to-session lock order.
    """

    result = await db.execute(
        select(HandoffAttempt)
        .where(HandoffAttempt.id == attempt_id)
        .with_for_update()
    )
    attempt = result.scalar_one()
    if attempt.sessao_id != sessao.id:
        raise HandoffStateError("handoff does not belong to locked session")
    if sessao.handoff_generation != attempt.generation:
        raise HandoffStateError("handoff generation is no longer current")
    if attempt.status != "pending":
        raise HandoffStateError(f"cannot ready handoff from {attempt.status}")

    attempt.status = "ready"
    attempt.chatwoot_contact_id = contact_id
    attempt.chatwoot_conversation_id = conversation_id
    attempt.ready_at = ready_at or datetime.utcnow()

    sessao.chatwoot_conversation_id = conversation_id
    await db.flush()
    return attempt


async def mark_terminal(
    db: AsyncSession,
    *,
    sessao: Sessao,
    attempt_id: int,
    status: str,
    terminal_at: datetime | None = None,
) -> HandoffAttempt:
    if status not in {"failed", "superseded", "cancelled"}:
        raise ValueError("invalid terminal handoff status")
    result = await db.execute(
        select(HandoffAttempt)
        .where(HandoffAttempt.id == attempt_id)
        .with_for_update()
    )
    attempt = result.scalar_one()
    if attempt.sessao_id != sessao.id:
        raise HandoffStateError("handoff does not belong to locked session")
    if attempt.status in {"failed", "superseded", "cancelled"}:
        if attempt.status != status:
            raise HandoffStateError("handoff already has another terminal status")
        return attempt
    attempt.status = status
    attempt.terminal_at = terminal_at or datetime.utcnow()
    await db.flush()
    return attempt


async def supersede_current_and_resume(
    db: AsyncSession,
    *,
    sessao: Sessao,
    resumed_at: datetime | None = None,
) -> HandoffAttempt | None:
    """Resume without deleting history or reusing the old remote mapping."""

    attempt = await get_current_for_locked_session(db, sessao)
    if attempt is not None and attempt.status in {"pending", "ready"}:
        attempt.status = "superseded"
        attempt.terminal_at = resumed_at or datetime.utcnow()

    sessao.handler = "bot"
    sessao.chatwoot_conversation_id = None
    sessao.encerrada_em = None
    sessao.motivo_encerramento = None
    await db.flush()
    return attempt
