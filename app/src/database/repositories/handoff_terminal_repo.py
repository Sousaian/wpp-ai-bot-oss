"""Shared state transition for a failed, decided handoff under existing locks."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    AgentServiceLane,
    ConversationTurn,
    Sessao,
    TurnRevision,
)
from src.database.repositories import sessao_repo


async def terminalize_failed_handoff_locked(
    db: AsyncSession,
    *,
    session: Sessao,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    revision: TurnRevision,
    successor: ConversationTurn | None,
    successor_revision: TurnRevision | None,
    now: datetime,
    terminal_reason: str,
) -> None:
    """Close a decided handoff without reacquiring any lifecycle lock.

    Callers own the canonical runtime → scheduler → agent → lane → session →
    turns → revisions → successor memberships → attempts prefix. Memberships
    are intentionally untouched: accepted late input stays durable, visible,
    and uncovered for operator action.
    """

    revision.status = "dead"
    turn.status = "dead"
    turn.session_slot = "terminal"
    turn.terminal_reason = terminal_reason
    turn.terminal_at = now
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    if successor is not None:
        successor.status = "cancelled"
        successor.session_slot = "terminal"
        successor.terminal_reason = terminal_reason
        successor.terminal_at = now
        if successor_revision is not None:
            successor_revision.status = "cancelled"
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    await sessao_repo.encerrar_locked(db, session, motivo="falha_tecnica")
    await db.flush()
