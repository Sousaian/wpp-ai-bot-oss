"""Ticket07 fenced lifecycle and operator transitions for scheduled work.

Every mutation here follows the scheduler-aware canonical order with locking
reads only (``populate_existing``):

``delivery_runtime_state (S) -> scheduler_runtime_state (X) -> agentes ->
agent_service_lanes -> sessoes (id asc) -> conversation_turns (id asc) ->
turn_revisions (id asc) -> generation_attempts (id asc) -> scheduled_handoffs
-> handoff_attempts (id asc) -> outbox_deliveries (id asc)``.

The runtime gate is taken in any mode (``direct``, ``outbox``, ``frozen``):
lifecycle and audited operator decisions keep working while frozen and never
acquire the gate after a lifecycle row. Identities that cannot be derived from
an already locked row come from a scalar discovery read whose read view is
closed (rollback) before the first lock wait; the locked graph re-validates
them. Whole transactions are retried on 1205/1213/1305, never a suffix.

Callers must not hold unflushed ORM writes: a pending write would be
autoflushed before the first lock (outside the canonical order) or silently
overwritten by a ``populate_existing`` read, so it fails closed instead.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
import random
from typing import Literal, TypeVar

from loguru import logger
from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import db_errors
from src.database.models import (
    AgentServiceLane,
    Agente,
    ConsumoLLM,
    ConversationTurn,
    DeliveryOperatorAction,
    DeliveryRuntimeState,
    GenerationAttempt,
    HandoffAttempt,
    LLMUsageReconciliation,
    OutboxDelivery,
    ScheduledHandoff,
    SchedulerRuntimeState,
    Sessao,
    TurnRevision,
)
from src.database.repositories import (
    delivery_state_repo,
    outbox_repo,
    scheduler_repo,
    sessao_repo,
)


T = TypeVar("T")
SIGNAL_THRESHOLD = 1
NONTERMINAL_REVISION_STATUSES = frozenset(
    {"collecting", "generating", "draft", "typing", "final_candidate"}
)
OPEN_EFFECT_STATUSES = frozenset({"blocked", "pending", "processing", "retry"})
QUEUED_EFFECT_STATUSES = frozenset({"blocked", "pending", "retry"})


class LifecycleRefused(ValueError):
    """Guarded refusal: the state forbids the action (HTTP 409 / CLI error)."""


class PendingWritesError(RuntimeError):
    """The caller holds unflushed ORM writes; nothing was locked or changed."""


class StaleTransactionError(RuntimeError):
    """S3: the operation must open its own transaction; nothing was locked."""


class _Rediscover(scheduler_repo.SchedulerConflictError):
    """Scalar discovery went stale before the canonical locks; retry it all."""


_counts: dict[str, int] = {
    "turns_cancelled": 0,
    "effects_cancelled": 0,
    "cancel_requested_live": 0,
    "orphan_effects_resolved": 0,
    "lane_kept_for_accounting": 0,
    "rediscovered": 0,
    "opted_in_inbox_clear_refused": 0,
    "agent_delete_evidence_refused": 0,
    "session_delete_handoff_refused": 0,
    "deadlock_retried": 0,
}


def lifecycle_counts() -> dict[str, int]:
    return dict(_counts)


def _signal(name: str, message: str, *args: object, error: bool = False) -> None:
    _counts[name] += 1
    log = logger.error if error else logger.warning
    log(message + " | count={} threshold={}", *args, _counts[name], SIGNAL_THRESHOLD)


def assert_no_pending_writes(db: AsyncSession) -> None:
    if db.new or db.dirty or db.deleted:
        raise PendingWritesError("lifecycle operation requires a session without pending writes")


async def run_with_deadlock_retry(
    db: AsyncSession,
    operation: Callable[[AsyncSession], Awaitable[T]],
    *,
    max_attempts: int = 4,
    max_jitter_seconds: float = 0.05,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Re-run a complete request transaction on the same session.

    Used by FastAPI routes whose session is owned by ``get_db``. The whole
    transaction (discovery included) restarts after a rollback; a lost
    transaction is never continued nor absorbed (A4).
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    assert_no_pending_writes(db)
    last: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await operation(db)
        except BaseException as exc:
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            lost = db_errors.transaction_lost_dbapi_error(exc)
            if lost is None and not isinstance(exc, _Rediscover):
                raise
            last = exc
            await db.rollback()
            if isinstance(exc, _Rediscover):
                _signal("rediscovered", "Lifecycle discovery went stale; retrying")
            else:
                _signal("deadlock_retried", "Lifecycle transaction lost; retrying")
            if attempt == max_attempts:
                break
            await sleep(random.random() * max_jitter_seconds)
    raise scheduler_repo.SchedulerDeadlockRetryExhausted(
        f"lifecycle transaction exhausted {max_attempts} attempts"
    ) from last


async def run_in_new_session(
    session_factory: Callable[[], AsyncSession],
    operation: Callable[[AsyncSession], Awaitable[T]],
    *,
    max_attempts: int = 4,
) -> T:
    """Operator entry point: one fresh session, whole-transaction retries.

    Lifecycle operations close their discovery read view with ``rollback``
    before locking, which is not allowed inside ``session.begin()``; the
    session therefore autobegins and is committed here explicitly.
    """

    async with session_factory() as db:
        result = await run_with_deadlock_retry(db, operation, max_attempts=max_attempts)
        await db.commit()
        return result


async def _db_now(db: AsyncSession) -> datetime:
    # No table access: never opens the InnoDB read view.
    return (await db.execute(select(func.utc_timestamp(6)))).scalar_one()


async def _locked(db: AsyncSession, model, *criteria, order=None, index: str | None = None):
    query = select(model)
    if index is not None:  # FU-2: scope the locking access path, not optimizer cost.
        query = query.with_hint(model, f"FORCE INDEX ({index})", "mysql")
    query = (
        query.where(*criteria)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if order is not None:
        query = query.order_by(*order)
    return list((await db.execute(query)).scalars())


# ── canonical prefix ───────────────────────────────────────────────────────
@dataclass
class LifecyclePrefix:
    runtime: DeliveryRuntimeState
    scheduler: SchedulerRuntimeState
    agent: Agente | None
    lane: AgentServiceLane | None


async def lock_lifecycle_prefix(db: AsyncSession, *, agente_id: int) -> LifecyclePrefix:
    """runtime (S, any mode) -> scheduler (X) -> agent (X) -> lane (X).

    Unlike ``scheduler_repo.lock_authority_prefix`` it never requires outbox
    or an enabled scheduler and never mutates the lane epoch: lifecycle and
    frozen operator decisions remain possible in every state.
    """

    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)
    scheduler = await scheduler_repo.get_scheduler_state(db, for_update=True)
    agents = await _locked(db, Agente, Agente.id == agente_id)
    agent = agents[0] if agents else None
    lane = None
    if agent is not None:
        lanes = await _locked(db, AgentServiceLane, AgentServiceLane.agente_id == agente_id)
        lane = lanes[0] if lanes else None
    return LifecyclePrefix(runtime=runtime, scheduler=scheduler, agent=agent, lane=lane)


@dataclass
class ScheduledGraph:
    turns: list[ConversationTurn]
    revisions: dict[int, list[TurnRevision]]
    attempts: list[GenerationAttempt]
    scheduled: dict[int, ScheduledHandoff]
    handoff_attempts: dict[int, HandoffAttempt]
    effects: dict[int, list[OutboxDelivery]]


async def lock_scheduled_graph(
    db: AsyncSession,
    *,
    session_ids: Sequence[int],
    current_handoff: Sequence[tuple[int, int]] = (),
) -> ScheduledGraph:
    """Lock the nonterminal scheduled work of locked sessions, phase by phase.

    ``current_handoff`` adds each session's current ``(sessao_id,
    handoff_generation)`` attempt to the ordered handoff-attempt phase.
    """

    session_ids = sorted(set(session_ids))
    turns = (
        await _locked(
            db,
            ConversationTurn,
            ConversationTurn.sessao_id.in_(session_ids),
            ConversationTurn.session_slot.in_(("active", "successor")),
            ConversationTurn.status.in_(scheduler_repo.NONTERMINAL_TURN_STATUSES),
            order=(ConversationTurn.id,),
            index="idx_turn_session_status",  # FU-2
        )
        if session_ids
        else []
    )
    turn_ids = [row.id for row in turns]
    revisions: dict[int, list[TurnRevision]] = {row.id: [] for row in turns}
    attempts: list[GenerationAttempt] = []
    scheduled: dict[int, ScheduledHandoff] = {}
    if turn_ids:
        for row in await _locked(
            db, TurnRevision, TurnRevision.turn_id.in_(turn_ids), order=(TurnRevision.id,)
        ):
            revisions[row.turn_id].append(row)
        attempts = await _locked(
            db,
            GenerationAttempt,
            GenerationAttempt.turn_id.in_(turn_ids),
            order=(GenerationAttempt.id,),
        )
        for row in await _locked(
            db,
            ScheduledHandoff,
            ScheduledHandoff.conversation_turn_id.in_(turn_ids),
            order=(ScheduledHandoff.handoff_attempt_id,),
        ):
            scheduled[row.conversation_turn_id] = row
    attempt_filter = [HandoffAttempt.id.in_([row.handoff_attempt_id for row in scheduled.values()])]
    for sessao_id, generation in current_handoff:
        if generation > 0:
            attempt_filter.append(
                (HandoffAttempt.sessao_id == sessao_id)
                & (HandoffAttempt.generation == generation)
            )
    handoff_attempts = {
        row.id: row
        for row in await _locked(
            db, HandoffAttempt, or_(*attempt_filter), order=(HandoffAttempt.id,)
        )
    }
    effects: dict[int, list[OutboxDelivery]] = {row.id: [] for row in turns}
    if turn_ids:
        for row in await _locked(
            db,
            OutboxDelivery,
            OutboxDelivery.conversation_turn_id.in_(turn_ids),
            OutboxDelivery.delivery_class == "automated",
            order=(OutboxDelivery.id,),
        ):
            effects[row.conversation_turn_id].append(row)
    return ScheduledGraph(
        turns=turns,
        revisions=revisions,
        attempts=attempts,
        scheduled=scheduled,
        handoff_attempts=handoff_attempts,
        effects=effects,
    )


# ── shared settlement ──────────────────────────────────────────────────────
def _owns_lane(lane: AgentServiceLane | None, turn: ConversationTurn) -> bool:
    return bool(
        lane is not None
        and lane.owner_token is not None
        and lane.active_turn_id == turn.id
        and turn.claim_token == lane.owner_token
    )


def _clear_claim(turn: ConversationTurn) -> None:
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None


def _release_lane(lane: AgentServiceLane) -> None:
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None


def cancel_open_effects_locked(
    rows: Sequence[OutboxDelivery], *, now: datetime, turn_id: int
) -> bool:
    """Cancel every open automated effect; ``True`` while one is still in I/O.

    Queued rows become ``cancelled``. A ``processing`` row under a live lease
    only gets ``cancel_requested_at``: its owner's finalization records the
    remote truth (``delivered_after_cancel`` or the failure) and settles the
    turn. An expired/absent lease is resolved as ambiguous (Y1), never erased.
    """

    live = False
    for row in rows:
        if row.status in QUEUED_EFFECT_STATUSES:
            row.status = "cancelled"
            if row.cancel_requested_at is None:
                row.cancel_requested_at = now
            row.terminal_at = now
            row.terminal_reason = "cancel_requested"
            _counts["effects_cancelled"] += 1
        elif row.status == "processing":
            if row.cancel_requested_at is None:
                row.cancel_requested_at = now
            if row.lock_until is not None and row.lock_until > now:
                live = True
                _signal(
                    "cancel_requested_live",
                    "Lifecycle cancel waits for an automated effect in flight | turn={} delivery={}",
                    turn_id,
                    row.id,
                )
                continue
            row.lease_token = None
            row.lock_owner = None
            row.lock_until = None
            row.ambiguity_count += 1
            row.last_error_code = "lease_expired_ambiguous"
            row.status = "cancelled"
            row.terminal_at = now
            row.terminal_reason = "cancel_requested"
            _signal(
                "orphan_effects_resolved",
                "Lifecycle cancel resolved an expired automated lease as ambiguous "
                "| turn={} delivery={}",
                turn_id,
                row.id,
                error=True,
            )
    return live


@dataclass
class SettleOutcome:
    cancelled_turns: list[int] = field(default_factory=list)
    waiting_turns: list[int] = field(default_factory=list)
    lane_kept_turns: list[int] = field(default_factory=list)


def _terminalize(turn: ConversationTurn, *, reason: str, now: datetime) -> None:
    turn.status = "cancelled"
    turn.session_slot = "terminal"
    turn.terminal_reason = reason
    turn.terminal_at = now
    turn.retry_at = None
    _counts["turns_cancelled"] += 1


async def cancel_scheduled_turns_locked(
    db: AsyncSession,
    *,
    graph: ScheduledGraph,
    lane: AgentServiceLane | None,
    reason: str,
    handoff_reason: str,
    now: datetime,
) -> SettleOutcome:
    """Formally cancel every nonterminal turn of the locked graph.

    Not admitted: revisions and turn ``cancelled``. If the lane owner still has
    a ``started`` generation attempt for the turn, the lane and the turn claim
    stay with that owner so its exact accounting (or the bounded drain into
    ``outcome_unknown``) is never lost; lane recovery releases it afterwards.
    Admitted: open effects cancelled (in-flight ones fenced by
    ``cancel_requested_at``); the turn becomes ``cancelled`` once every
    effect is terminal, otherwise it waits ``awaiting_delivery`` and the
    effect's finalization or settle discovery ends it. No successor is
    promoted: every nonterminal turn of the affected sessions is cancelled.
    """

    outcome = SettleOutcome()
    for turn in graph.turns:
        if turn.session_slot == "terminal":
            continue
        owns = _owns_lane(lane, turn)
        revisions = graph.revisions.get(turn.id, [])
        final = next((row for row in revisions if row.id == turn.final_revision_id), None)
        if final is not None and final.status == "admitted":
            scheduled = graph.scheduled.get(turn.id)
            effects = [
                row for row in graph.effects.get(turn.id, ()) if row.turn_revision_id == final.id
            ]
            if cancel_open_effects_locked(effects, now=now, turn_id=turn.id):
                turn.status = "awaiting_delivery"
                turn.retry_at = None
                if owns:
                    _release_lane(lane)
                _clear_claim(turn)
                outcome.waiting_turns.append(turn.id)
                continue
            _terminalize(
                turn,
                reason=handoff_reason if scheduled is not None else reason,
                now=now,
            )
            if owns:
                _release_lane(lane)
            _clear_claim(turn)
            outcome.cancelled_turns.append(turn.id)
            continue
        for revision in revisions:
            if revision.status in NONTERMINAL_REVISION_STATUSES:
                revision.status = "cancelled"
        in_flight = owns and any(
            row.turn_id == turn.id
            and row.outcome == "started"
            and row.lane_owner_token == lane.owner_token
            for row in graph.attempts
        )
        _terminalize(turn, reason=reason, now=now)
        if in_flight:
            # The owner keeps renewing while its provider call runs; its late
            # completion records accounting only (the turn is terminal), then
            # lane recovery drains/releases. Never an orphan started attempt.
            outcome.lane_kept_turns.append(turn.id)
            _signal(
                "lane_kept_for_accounting",
                "Lifecycle cancel kept the lane for in-flight generation accounting "
                "| agente={} turn={}",
                turn.agente_id,
                turn.id,
            )
        else:
            if owns:
                _release_lane(lane)
            _clear_claim(turn)
        outcome.cancelled_turns.append(turn.id)
    await db.flush()
    if outcome.cancelled_turns:
        logger.warning(
            "Scheduled turns cancelled by lifecycle | reason={} turns={} waiting={}",
            reason,
            len(outcome.cancelled_turns),
            len(outcome.waiting_turns),
        )
    return outcome


async def _lock_session_by_phone(
    db: AsyncSession, *, agente_id: int, telefone: str
) -> Sessao | None:
    rows = await _locked(
        db, Sessao, Sessao.agente_id == agente_id, Sessao.telefone == telefone
    )
    return rows[0] if rows else None


# ── /transferir and /retomar ───────────────────────────────────────────────
@dataclass(frozen=True)
class SessionLifecycleResult:
    sessao_id: int
    cancelled_turns: tuple[int, ...]
    waiting_turns: tuple[int, ...]


async def transfer_session(
    db: AsyncSession, *, agente_id: int, telefone: str, motivo: str
) -> SessionLifecycleResult | None:
    """Manual transfer: cancel scheduled bot work and close the session."""

    assert_no_pending_writes(db)
    prefix = await lock_lifecycle_prefix(db, agente_id=agente_id)
    if prefix.agent is None:
        return None
    session = await _lock_session_by_phone(db, agente_id=agente_id, telefone=telefone)
    if session is None:
        return None
    graph = await lock_scheduled_graph(db, session_ids=[session.id])
    now = await _db_now(db)
    outcome = await cancel_scheduled_turns_locked(
        db,
        graph=graph,
        lane=prefix.lane,
        reason="session_transferred",
        handoff_reason="session_transferred",
        now=now,
    )
    await sessao_repo.encerrar_locked(db, session, motivo=motivo)
    return SessionLifecycleResult(
        session.id, tuple(outcome.cancelled_turns), tuple(outcome.waiting_turns)
    )


async def resume_session(
    db: AsyncSession, *, agente_id: int, telefone: str
) -> SessionLifecycleResult | None:
    """``/retomar``: supersede the handoff, cancel stale work, back to bot.

    The legacy route already cancels every queued delivery of the session
    whatever its handler; scheduled turns follow the same rule.
    """

    assert_no_pending_writes(db)
    prefix = await lock_lifecycle_prefix(db, agente_id=agente_id)
    if prefix.agent is None:
        return None
    session = await _lock_session_by_phone(db, agente_id=agente_id, telefone=telefone)
    if session is None:
        return None
    graph = await lock_scheduled_graph(
        db,
        session_ids=[session.id],
        current_handoff=[(session.id, int(session.handoff_generation))],
    )
    now = await _db_now(db)
    outcome = await cancel_scheduled_turns_locked(
        db,
        graph=graph,
        lane=prefix.lane,
        reason="session_resumed",
        handoff_reason="handoff_superseded",
        now=now,
    )
    current = next(
        (
            row
            for row in graph.handoff_attempts.values()
            if row.sessao_id == session.id and row.generation == session.handoff_generation
        ),
        None,
    )
    if current is not None and current.status in {"pending", "ready"}:
        current.status = "superseded"
        current.terminal_at = now
    session.handler = "bot"
    session.chatwoot_conversation_id = None
    session.encerrada_em = None
    session.motivo_encerramento = None
    await db.flush()
    await outbox_repo.request_cancellation_for_session(
        db, sessao_id=session.id, requested_at=now
    )
    return SessionLifecycleResult(
        session.id, tuple(outcome.cancelled_turns), tuple(outcome.waiting_turns)
    )


# ── agent update / disable ─────────────────────────────────────────────────
@dataclass(frozen=True)
class AgentUpdateResult:
    agent: Agente
    cancelled_turns: tuple[int, ...]
    waiting_turns: tuple[int, ...]


async def _discover_agent_sessions(db: AsyncSession, *, agente_id: int) -> list[int]:
    """Scalar discovery hint in its own read view, closed before any lock."""

    ids = list(
        (
            await db.execute(
                select(ConversationTurn.sessao_id)
                .where(
                    ConversationTurn.agente_id == agente_id,
                    ConversationTurn.session_slot.in_(("active", "successor")),
                )
                .distinct()
                .order_by(ConversationTurn.sessao_id)
            )
        ).scalars()
    )
    await db.rollback()
    return ids


async def update_agent(
    db: AsyncSession,
    *,
    agente_id: int,
    payload: dict[str, object],
    prepare: Callable[[Agente, dict[str, object]], dict[str, object]] | None = None,
) -> AgentUpdateResult | None:
    """Fenced agent update; disabling cancels the agent's scheduled work.

    ``ativo`` true->false cancels every nonterminal scheduled turn of the
    agent (``agent_disabled``); re-enabling never replays them. Turning the
    scheduler opt-in off while scheduled work is pending is refused: legacy
    and scheduled generation would otherwise run for the same sessions.
    """

    assert_no_pending_writes(db)
    lifecycle_hint = (
        payload.get("ativo") is False or payload.get("inbox_scheduler_enabled") is False
    )
    session_ids = (
        await _discover_agent_sessions(db, agente_id=agente_id) if lifecycle_hint else []
    )
    prefix = await lock_lifecycle_prefix(db, agente_id=agente_id)
    agent = prefix.agent
    if agent is None:
        return None
    sessions = (
        await _locked(db, Sessao, Sessao.id.in_(session_ids), order=(Sessao.id,))
        if session_ids
        else []
    )
    # T08-6: an opted-in agent keeps a handoff inbox; evaluated on the fresh
    # locked row, so it serializes with the guarded opt-in (scheduler X).
    if (
        "chatwoot_inbox_id" in payload
        and payload["chatwoot_inbox_id"] is None
        and payload.get("inbox_scheduler_enabled", agent.inbox_scheduler_enabled)
    ):
        _signal(
            "opted_in_inbox_clear_refused",
            "Opted-in agent inbox clear refused | agente={}",
            agente_id,
            error=True,
        )
        raise LifecycleRefused("scheduler opt-in requires chatwoot_inbox_id; opt out first")
    disabling = bool(payload.get("ativo") is False and agent.ativo)
    opting_out = bool(
        payload.get("inbox_scheduler_enabled") is False and agent.inbox_scheduler_enabled
    )
    graph = None
    if disabling or opting_out:
        graph = await lock_scheduled_graph(db, session_ids=[row.id for row in sessions])
        # First consistent read only after every lock wait: no new turn can
        # appear while scheduler/agent/lane are held (intake takes them first).
        stray = (
            await db.execute(
                select(ConversationTurn.id)
                .where(
                    ConversationTurn.agente_id == agente_id,
                    ConversationTurn.session_slot.in_(("active", "successor")),
                    ConversationTurn.sessao_id.not_in([row.id for row in sessions] or [0]),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if stray is not None:
            raise _Rediscover("agent gained scheduled work after discovery")
        if opting_out and not disabling and graph.turns:
            raise LifecycleRefused("agent scheduler opt-out requires settled scheduled work")
    outcome = SettleOutcome()
    if disabling and graph is not None:
        now = await _db_now(db)
        outcome = await cancel_scheduled_turns_locked(
            db,
            graph=graph,
            lane=prefix.lane,
            reason="agent_disabled",
            handoff_reason="agent_disabled",
            now=now,
        )
        if outcome.cancelled_turns or outcome.waiting_turns:
            logger.warning(
                "Agent disabled; scheduled work cancelled and never replayed | agente={} "
                "turns={} waiting={}",
                agente_id,
                len(outcome.cancelled_turns),
                len(outcome.waiting_turns),
            )
    # Derived fields are computed from the fresh locked row, never from a
    # preloaded identity (no pending-write overwrite, no stale fallback).
    values = prepare(agent, dict(payload)) if prepare is not None else dict(payload)
    for name, value in values.items():
        setattr(agent, name, value)
    await db.flush()
    await db.refresh(agent)
    return AgentUpdateResult(
        agent, tuple(outcome.cancelled_turns), tuple(outcome.waiting_turns)
    )


# ── deletes ────────────────────────────────────────────────────────────────
SCHEDULER_HISTORY_MESSAGE = "historico de scheduler retido; aguarde retencao segura ou resolva pendencias"


async def _has_scheduler_history(db: AsyncSession, *criteria, index: str) -> bool:
    rows = (
        await db.execute(
            select(ConversationTurn.id)
            .with_hint(ConversationTurn, f"FORCE INDEX ({index})", "mysql")  # FU-2
            .where(*criteria)
            .order_by(ConversationTurn.id)
            .limit(1)
            .with_for_update()
        )
    ).scalars()
    return next(iter(rows), None) is not None


SESSION_HANDOFF_MESSAGE = "historico de handoff retido; a sessao nao pode ser excluida"


async def delete_session(
    db: AsyncSession,
    *,
    agente_id: int,
    telefone: str,
    runtime_guard: Callable[[DeliveryRuntimeState], None] | None = None,
) -> Literal["deleted", "not_found"]:
    """Delete only a session without scheduler history, open deliveries or
    ``handoff_attempts`` (S3: never cascaded away through the API). The
    session ledger survives: ``consumo_llm``/``llm_usage_reconciliation``
    keep their rows with ``sessao_id`` set to NULL.

    Must open its own transaction (same READ COMMITTED pattern as
    ``delete_agent``). ``runtime_guard`` sees the locked runtime row first.
    """

    await _begin_read_committed(db)
    prefix = await lock_lifecycle_prefix(db, agente_id=agente_id)
    if runtime_guard is not None:
        runtime_guard(prefix.runtime)
    if prefix.agent is None:
        return "not_found"
    session = await _lock_session_by_phone(db, agente_id=agente_id, telefone=telefone)
    if session is None:
        return "not_found"
    if await _has_scheduler_history(db, ConversationTurn.sessao_id == session.id, index="idx_turn_session_status"):
        raise LifecycleRefused(SCHEDULER_HISTORY_MESSAGE)
    await outbox_repo.assert_session_deletable(db, session.id)
    # Non-locking read after the session X lock: every handoff_attempts
    # insert takes S on the session (FK), so none is in flight; RC makes the
    # read current and no secondary-index gap lock reaches other sessions.
    handoff = select(HandoffAttempt.id).where(HandoffAttempt.sessao_id == session.id).limit(1)
    if (await db.execute(handoff)).scalar_one_or_none() is not None:
        _signal(
            "session_delete_handoff_refused",
            "Session delete refused: retained handoff | agente={} sessao={}",
            agente_id,
            session.id,
            error=True,
        )
        raise LifecycleRefused(SESSION_HANDOFF_MESSAGE)
    # Only API delete path for sessoes (S3 static guard): never a repo helper.
    sessao_id = session.id
    await db.delete(session)
    await db.flush()
    logger.info("Sessão deletada: id={}", sessao_id)
    return "deleted"


# S3 (P2-2): usage/handoff evidence an agent delete would CASCADE away.
AGENT_DELETE_EVIDENCE_TABLES = (ConsumoLLM, HandoffAttempt, LLMUsageReconciliation)
AGENT_EVIDENCE_MESSAGE = (
    "evidencia de uso ou handoff retida; desative o agente (ativo=false) em vez de excluir"
)


async def _begin_read_committed(db: AsyncSession) -> None:
    """First statement of the delete transaction (S3, A1/A3).

    READ COMMITTED makes the later non-locking evidence read current (no
    snapshot older than the agent X lock) and keeps every locking read of the
    delete on record locks only. MySQL refuses (1568) once any statement
    opened the transaction, so a stale read view fails closed.
    """

    assert_no_pending_writes(db)
    try:
        await db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    except DBAPIError as exc:
        if 1568 in db_errors.mysql_error_codes(exc):
            raise StaleTransactionError("agent delete requires its own fresh transaction") from exc
        raise


async def _has_agent_evidence(db: AsyncSession, agente_id: int) -> str | None:
    """Non-locking existence read, called only after the agent X lock.

    Every insert into these tables takes S on the agent row (FK check), so
    none can be in flight or start while the X lock is held; READ COMMITTED
    makes the read see every committed row. No gap/next-key lock is taken on
    the secondary agent indexes, so other tenants' inserts never wait.
    """

    for model in AGENT_DELETE_EVIDENCE_TABLES:
        query = select(model.id).where(model.agente_id == agente_id).limit(1)
        if (await db.execute(query)).scalar_one_or_none() is not None:
            return model.__tablename__
    return None


async def delete_agent(
    db: AsyncSession,
    *,
    agente_id: int,
    runtime_guard: Callable[[DeliveryRuntimeState], None] | None = None,
) -> Literal["deleted", "not_found"]:
    """Delete only an agent without scheduler history, open deliveries or
    usage/handoff evidence (``consumo_llm``, ``handoff_attempts``,
    ``llm_usage_reconciliation`` are never deleted through the API).

    Must open its own transaction. ``runtime_guard`` sees the locked runtime
    row (S) before anything else is checked.
    """

    await _begin_read_committed(db)
    prefix = await lock_lifecycle_prefix(db, agente_id=agente_id)
    if runtime_guard is not None:
        runtime_guard(prefix.runtime)
    if prefix.agent is None:
        return "not_found"
    await _locked(db, Sessao, Sessao.agente_id == agente_id, order=(Sessao.id,))
    if await _has_scheduler_history(db, ConversationTurn.agente_id == agente_id, index="idx_turn_ready_claim"):
        raise LifecycleRefused(SCHEDULER_HISTORY_MESSAGE)
    await outbox_repo.assert_agent_deletable(db, agente_id)
    evidence = await _has_agent_evidence(db, agente_id)
    if evidence is not None:
        _signal(
            "agent_delete_evidence_refused",
            "Agent delete refused: retained evidence | agente={} table={}",
            agente_id,
            evidence,
            error=True,
        )
        raise LifecycleRefused(AGENT_EVIDENCE_MESSAGE)
    agent = prefix.agent
    await db.delete(agent)
    await db.flush()
    logger.info("Agente deletado: id={}", agente_id)
    return "deleted"


# ── audited operator decisions ─────────────────────────────────────────────
def _snapshot_action(
    *,
    delivery: OutboxDelivery | None,
    action: str,
    actor: str,
    reason: str,
    runtime: DeliveryRuntimeState,
) -> DeliveryOperatorAction:
    """Audit row bound to the current runtime incarnation and row evidence."""

    return DeliveryOperatorAction(
        delivery_id=delivery.id if delivery is not None else None,
        action=action[:50],
        actor=actor,
        reason=reason[:500],
        snapshot_cutover_epoch=(
            delivery.cutover_epoch if delivery is not None else runtime.cutover_epoch
        ),
        snapshot_status=delivery.status if delivery is not None else runtime.mode,
        snapshot_attempt_count=delivery.attempt_count if delivery is not None else None,
        snapshot_ambiguity_count=delivery.ambiguity_count if delivery is not None else None,
        snapshot_terminal_at=delivery.terminal_at if delivery is not None else None,
    )


@dataclass(frozen=True)
class _DeliveryLinks:
    agente_id: int | None
    sessao_id: int | None
    conversation_turn_id: int | None
    delivery_class: str


async def _discover_delivery(db: AsyncSession, delivery_id: int) -> _DeliveryLinks | None:
    row = (
        await db.execute(
            select(
                OutboxDelivery.agente_id,
                OutboxDelivery.sessao_id,
                OutboxDelivery.conversation_turn_id,
                OutboxDelivery.delivery_class,
            ).where(OutboxDelivery.id == delivery_id)
        )
    ).one_or_none()
    await db.rollback()
    if row is None:
        return None
    return _DeliveryLinks(
        row.agente_id, row.sessao_id, row.conversation_turn_id, row.delivery_class
    )


def _handoff_current(
    session: Sessao, attempt: HandoffAttempt | None
) -> bool:
    return bool(
        attempt is not None
        and attempt.sessao_id == session.id
        and attempt.generation == session.handoff_generation
        and attempt.status in {"pending", "ready"}
        and session.handler == "encerrada"
        and session.motivo_encerramento == "handoff"
    )


async def operator_cancel_automated(
    db: AsyncSession, *, delivery_id: int, actor: str, reason: str
) -> OutboxDelivery:
    """Cancel one automated effect under the canonical lane prefix.

    Queued -> ``cancelled`` (``operator_cancelled``); in flight -> only
    ``cancel_requested_at`` (its finalization records the truth). The turn is
    settled in the same UoW by the current lifecycle: ordinary -> terminal and
    successor promoted; current handoff -> parked; superseded -> cancelled.
    Works in every runtime mode, including ``frozen``.
    """

    from src.database.repositories import turn_delivery_repo

    actor, reason = delivery_state_repo.validate_audit_metadata(actor=actor, reason=reason)
    links = await _discover_delivery(db, delivery_id)
    if links is None:
        raise LookupError("delivery not found")
    if (
        links.delivery_class != "automated"
        or links.agente_id is None
        or links.sessao_id is None
        or links.conversation_turn_id is None
    ):
        raise LifecycleRefused("delivery is not an automated scheduler effect")
    prefix = await lock_lifecycle_prefix(db, agente_id=links.agente_id)
    session = next(
        iter(await _locked(db, Sessao, Sessao.id == links.sessao_id)), None
    )
    if prefix.agent is None or session is None:
        raise LifecycleRefused("automated delivery lost its lifecycle rows")
    graph = await lock_scheduled_graph(
        db,
        session_ids=[session.id],
        current_handoff=[(session.id, int(session.handoff_generation))],
    )
    delivery = next(
        (
            row
            for rows in graph.effects.values()
            for row in rows
            if row.id == delivery_id
        ),
        None,
    )
    if delivery is None:
        # The turn is already terminal: only the row itself is locked.
        delivery = next(iter(await _locked(db, OutboxDelivery, OutboxDelivery.id == delivery_id)), None)
        if delivery is None:
            raise LookupError("delivery not found")
    if delivery.conversation_turn_id != links.conversation_turn_id:
        raise _Rediscover("automated delivery identity changed")
    now = await _db_now(db)
    if delivery.status in OPEN_EFFECT_STATUSES:
        cancel_open_effects_locked([delivery], now=now, turn_id=delivery.conversation_turn_id)
        if delivery.status == "cancelled":
            delivery.terminal_reason = "operator_cancelled"
    elif delivery.status not in {"delivered", "dead", "cancelled"}:
        raise LifecycleRefused("delivery cannot be cancelled")
    db.add(
        _snapshot_action(
            delivery=delivery,
            action="cancel",
            actor=actor,
            reason=reason,
            runtime=prefix.runtime,
        )
    )
    await db.flush()
    turn = next((row for row in graph.turns if row.id == delivery.conversation_turn_id), None)
    if turn is not None and delivery.status == "cancelled":
        revision = next(
            (row for row in graph.revisions.get(turn.id, ()) if row.id == delivery.turn_revision_id),
            None,
        )
        scheduled = graph.scheduled.get(turn.id)
        attempt = (
            graph.handoff_attempts.get(scheduled.handoff_attempt_id)
            if scheduled is not None
            else None
        )
        await turn_delivery_repo.settle_turn_after_effect(
            db,
            graph=scheduler_repo.AutomatedDeliveryGraph(
                runtime=prefix.runtime,
                scheduler=prefix.scheduler,
                agent=prefix.agent,
                lane=prefix.lane,
                session=session,
                turn=turn,
                revision=revision,
                handoff_attempt=attempt,
                delivery=delivery,
                dependency=None,
                now=now,
            ),
            effect=delivery,
            reason="operator_cancelled",
        )
    await db.flush()
    return delivery


@dataclass(frozen=True)
class TurnResolution:
    turn_id: int
    resolution: str
    status: str
    rearmed: tuple[int, ...]


async def resolve_parked_turn(
    db: AsyncSession,
    *,
    turn_id: int,
    resolution: Literal["retry", "abandon"],
    actor: str,
    reason: str,
    ttl_seconds: int,
    accept_duplicate_risk: bool = False,
) -> TurnResolution:
    """Audited exit for an admitted handoff turn parked with dead effects.

    ``retry`` re-arms the undelivered dead/cancelled automated effects of the
    *current* handoff generation on the current delivery/scheduler
    incarnation, resets the post-admission cap and leaves the lane-owned
    admission (discovery -> requeue -> claim) to send them. ``abandon`` ends
    the turn ``cancelled``/``operator_abandoned`` with every open effect
    resolved; the handoff itself (session, Chatwoot) is untouched.
    """

    actor, reason = delivery_state_repo.validate_audit_metadata(actor=actor, reason=reason)
    if resolution not in {"retry", "abandon"}:
        raise ValueError("unknown turn resolution")
    if not 1 <= ttl_seconds <= 86400:
        raise ValueError("ttl_seconds must be between 1 and 86400")
    identity = (
        await db.execute(
            select(ConversationTurn.agente_id, ConversationTurn.sessao_id).where(
                ConversationTurn.id == turn_id
            )
        )
    ).one_or_none()
    await db.rollback()
    if identity is None:
        raise LookupError("turn not found")
    prefix = await lock_lifecycle_prefix(db, agente_id=identity.agente_id)
    session = next(iter(await _locked(db, Sessao, Sessao.id == identity.sessao_id)), None)
    if prefix.agent is None or session is None:
        raise LifecycleRefused("turn lost its lifecycle rows")
    graph = await lock_scheduled_graph(
        db,
        session_ids=[session.id],
        current_handoff=[(session.id, int(session.handoff_generation))],
    )
    turn = next((row for row in graph.turns if row.id == turn_id), None)
    if turn is None:
        raise LifecycleRefused("turn is already terminal")
    final = next(
        (row for row in graph.revisions.get(turn.id, ()) if row.id == turn.final_revision_id),
        None,
    )
    scheduled = graph.scheduled.get(turn.id)
    if (
        final is None
        or final.status != "admitted"
        or turn.status != "awaiting_delivery"
        or scheduled is None
    ):
        raise LifecycleRefused("only an admitted handoff turn awaiting delivery can be resolved")
    if resolution == "retry" and (_owns_lane(prefix.lane, turn) or turn.claim_token is not None):
        raise LifecycleRefused("turn is owned by a lane worker")
    effects = [row for row in graph.effects.get(turn.id, ()) if row.turn_revision_id == final.id]
    processing = any(row.status == "processing" for row in effects)
    if resolution == "retry" and processing:
        raise LifecycleRefused("an automated effect is still in flight")
    attempt = graph.handoff_attempts.get(scheduled.handoff_attempt_id)
    now = await _db_now(db)
    parked = turn.delivery_attempt_count >= prefix.scheduler.max_delivery_attempts_per_turn or any(
        row.status in {"dead", "cancelled"} for row in effects
    )
    if not parked and not (resolution == "abandon" and processing):
        raise LifecycleRefused("turn is not parked")
    rearmed: list[OutboxDelivery] = []
    if resolution == "retry":
        if not (
            prefix.agent.ativo
            and prefix.agent.inbox_scheduler_enabled
            and _handoff_current(session, attempt)
        ):
            raise LifecycleRefused("retry requires the current handoff generation and an enabled agent")
        if prefix.runtime.mode != "outbox" or prefix.scheduler.scheduler_epoch != turn.scheduler_epoch:
            raise LifecycleRefused("retry requires the current delivery and scheduler incarnation")
        for row in effects:
            if row.status not in {"dead", "cancelled"}:
                continue
            if row.cutover_epoch != prefix.runtime.cutover_epoch:
                raise LifecycleRefused("effect belongs to another delivery incarnation")
            if row.last_error_code == "duplicate_remote_effect":
                raise LifecycleRefused("duplicate remote effects require provider resolution")
            if not row.payload:
                raise LifecycleRefused("effect payload was scrubbed")
            if row.ambiguity_count > 0 and not accept_duplicate_risk:
                raise LifecycleRefused("ambiguous effect requires --accept-duplicate-risk")
            rearmed.append(row)
        if not rearmed and turn.delivery_attempt_count == 0:
            raise LifecycleRefused("nothing to re-arm")
        for row in rearmed:
            row.cancel_requested_at = None
            row.terminal_at = None
            row.terminal_reason = None
            row.last_error_code = None
            if row.automated_effect == "handoff_transition":
                row.status = "blocked"
                row.next_attempt_at = None
                row.expires_at = None
            else:
                row.status = "retry"
                row.next_attempt_at = now
                row.expires_at = now + timedelta(seconds=ttl_seconds)
        turn.delivery_attempt_count = 0
    else:
        live = cancel_open_effects_locked(effects, now=now, turn_id=turn.id)
        if not live:
            _terminalize(turn, reason="operator_abandoned", now=now)
            if _owns_lane(prefix.lane, turn):
                _release_lane(prefix.lane)
            _clear_claim(turn)
    for row in effects if resolution == "abandon" else rearmed:
        db.add(
            _snapshot_action(
                delivery=row,
                action=f"turn_resolve:{resolution}",
                actor=actor,
                reason=f"turn={turn.id};{reason}",
                runtime=prefix.runtime,
            )
        )
    if resolution == "retry" and not rearmed:
        db.add(
            _snapshot_action(
                delivery=None,
                action="turn_resolve:retry",
                actor=actor,
                reason=f"turn={turn.id};cap_reset;{reason}",
                runtime=prefix.runtime,
            )
        )
    await db.flush()
    if resolution == "abandon" and turn.session_slot == "terminal":
        await scheduler_repo._promote_successor_after_terminal_active(db, sessao_id=session.id)
    await db.flush()
    logger.warning(
        "Parked turn resolved by operator | turn={} resolution={} rearmed={}",
        turn.id,
        resolution,
        len(rearmed),
    )
    return TurnResolution(turn.id, resolution, turn.status, tuple(row.id for row in rearmed))


# ── generation quarantine (unknown outcome) ────────────────────────────────
QUARANTINE_EVIDENCE = frozenset(
    {"provider_generation_lookup", "provider_dashboard", "provider_support", "no_provider_record"}
)


@dataclass(frozen=True)
class QuarantineRow:
    attempt_id: int
    attempt_key: str
    agente_id: int
    sessao_id: int
    turn_id: int
    revision_id: int
    attempt_type: str
    outcome: str
    accounting_state: str
    safe_error_code: str | None
    provider_generation_id: str | None
    consumo_llm_id: int | None
    started_at: datetime
    outcome_recorded_at: datetime | None
    accounting_drain_until: datetime | None


def _quarantine_row(row: GenerationAttempt) -> QuarantineRow:
    return QuarantineRow(
        attempt_id=row.id,
        attempt_key=row.attempt_key,
        agente_id=row.agente_id,
        sessao_id=row.sessao_id,
        turn_id=row.turn_id,
        revision_id=row.revision_id,
        attempt_type=row.attempt_type,
        outcome=row.outcome,
        accounting_state=row.accounting_state,
        safe_error_code=row.safe_error_code,
        provider_generation_id=row.provider_generation_id,
        consumo_llm_id=row.consumo_llm_id,
        started_at=row.started_at,
        outcome_recorded_at=row.outcome_recorded_at,
        accounting_drain_until=row.accounting_drain_until,
    )


async def list_generation_quarantine(
    db: AsyncSession, *, agente_id: int | None = None, limit: int = 100
) -> list[QuarantineRow]:
    """Unresolved audit quarantines (read-only; no content, phones or keys)."""

    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    query = (
        select(GenerationAttempt)
        .where(
            GenerationAttempt.accounting_state == "reconciliation_required",
            GenerationAttempt.reconciled_at.is_(None),
        )
        .order_by(GenerationAttempt.id)
        .limit(limit)
    )
    if agente_id is not None:
        query = query.where(GenerationAttempt.agente_id == agente_id)
    return [_quarantine_row(row) for row in (await db.execute(query)).scalars()]


async def inspect_generation_attempt(db: AsyncSession, *, attempt_key: str) -> dict[str, object]:
    """Exact attempt plus the evidence an operator needs to decide."""

    row = (
        await db.execute(select(GenerationAttempt).where(GenerationAttempt.attempt_key == attempt_key))
    ).scalar_one_or_none()
    if row is None:
        raise LookupError("generation attempt not found")
    lane = (
        await db.execute(select(AgentServiceLane).where(AgentServiceLane.agente_id == row.agente_id))
    ).scalar_one_or_none()
    turn = await db.get(ConversationTurn, row.turn_id)
    revision = await db.get(TurnRevision, row.revision_id)
    call_type = "resposta" if row.attempt_type == "response" else "resumo"
    consumption = list(
        (
            await db.execute(
                select(ConsumoLLM)
                .where(
                    ConsumoLLM.agente_id == row.agente_id,
                    ConsumoLLM.sessao_id == row.sessao_id,
                    ConsumoLLM.modelo_llm == row.modelo_llm,
                    ConsumoLLM.tipo_chamada == call_type,
                )
                .order_by(ConsumoLLM.id.desc())
                .limit(50)
            )
        ).scalars()
    )
    linked_ids = set(
        (
            await db.execute(
                select(GenerationAttempt.consumo_llm_id).where(
                    GenerationAttempt.consumo_llm_id.in_([c.id for c in consumption] or [0])
                )
            )
        ).scalars()
    )
    reconciliation = list(
        (
            await db.execute(
                select(LLMUsageReconciliation)
                .where(
                    LLMUsageReconciliation.agente_id == row.agente_id,
                    LLMUsageReconciliation.sessao_id == row.sessao_id,
                    LLMUsageReconciliation.tipo_chamada == call_type,
                )
                .order_by(LLMUsageReconciliation.id)
                .limit(50)
            )
        ).scalars()
    )
    return {
        "attempt": _quarantine_row(row).__dict__,
        "scheduler_epoch": row.scheduler_epoch,
        "modelo_llm": row.modelo_llm,
        "reconciled_at": row.reconciled_at,
        "reconciliation_actor": row.reconciliation_actor,
        "lane_owned_by_attempt": bool(lane is not None and lane.owner_token == row.lane_owner_token),
        "lane_lease_until": lane.lease_until if lane is not None else None,
        "turn_status": turn.status if turn is not None else None,
        "turn_slot": turn.session_slot if turn is not None else None,
        "revision_status": revision.status if revision is not None else None,
        "unlinked_consumption": [
            {
                "consumo_llm_id": c.id,
                "provider_generation_id": c.provider_generation_id,
                "tokens_input": c.tokens_input,
                "tokens_output": c.tokens_output,
                "custo_creditos": str(c.custo_creditos),
                "criado_em": c.criado_em,
            }
            for c in consumption
            if c.id not in linked_ids
        ],
        "usage_reconciliation": [
            {
                "id": r.id,
                "status": r.status,
                "provider_generation_id": r.provider_generation_id,
                "tokens_input": r.tokens_input,
                "tokens_output": r.tokens_output,
                "custo_creditos": str(r.custo_creditos),
            }
            for r in reconciliation
        ],
    }


@dataclass(frozen=True)
class QuarantineResolution:
    attempt_id: int
    outcome: str
    consumo_llm_id: int | None
    reconciled: bool


async def resolve_generation_quarantine(
    db: AsyncSession,
    *,
    attempt_key: str,
    outcome: Literal["succeeded", "failed"],
    evidence: str,
    actor: str,
    note: str,
    consumo_llm_id: int | None = None,
    accept_duplicate_spend_risk: bool = False,
) -> QuarantineResolution:
    """Audited resolution of one exact quarantined generation attempt.

    ``succeeded`` links the exact commercial ledger row (tokens/cost copied
    from it and re-verified by the attempt guard). ``failed`` keeps no usage
    (never an invented zero-cost success) and requires the operator to accept
    that a retried turn may spend again. The attempt stays
    ``reconciliation_required``; ``reconciled_at`` records the decision and
    only then lets lane recovery and admission continue. No lane, turn or
    session authority is acquired: the lifecycle fences stay in force.
    Lock order mirrors exact accounting: agent (S) -> session (S) ->
    attempt (X) -> consumo (X).
    """

    actor, note = delivery_state_repo.validate_audit_metadata(actor=actor, reason=note)
    if evidence not in QUARANTINE_EVIDENCE:
        raise ValueError("unknown evidence kind")
    if outcome == "succeeded" and consumo_llm_id is None:
        raise LifecycleRefused("succeeded resolution requires an exact --consumo-id")
    if outcome == "failed":
        if consumo_llm_id is not None:
            raise LifecycleRefused("failed resolution cannot link consumption")
        if not accept_duplicate_spend_risk:
            raise LifecycleRefused("failed resolution requires --accept-duplicate-spend-risk")
    risk = "duplicate_spend_accepted" if outcome == "failed" else "exact_ledger_link"
    reason = f"evidence={evidence};risk={risk};note={note}"[:500]
    identity = (
        await db.execute(
            select(
                GenerationAttempt.agente_id,
                GenerationAttempt.sessao_id,
                GenerationAttempt.scheduler_epoch,
                GenerationAttempt.lane_owner_token,
            ).where(GenerationAttempt.attempt_key == attempt_key)
        )
    ).one_or_none()
    await db.rollback()
    if identity is None:
        raise LookupError("generation attempt not found")
    from src.database.repositories import turn_generation_repo

    if not await turn_generation_repo._share_lock_accounting_parents(
        db, agente_id=identity.agente_id, sessao_id=identity.sessao_id
    ):
        raise LifecycleRefused("attempt lost its accounting parents")
    attempt = next(
        iter(
            await _locked(db, GenerationAttempt, GenerationAttempt.attempt_key == attempt_key)
        ),
        None,
    )
    if attempt is None or attempt.outcome == "started":
        raise LifecycleRefused("attempt is not a recorded quarantine")
    values: dict[str, object] = {}
    if consumo_llm_id is not None:
        consumo = next(iter(await _locked(db, ConsumoLLM, ConsumoLLM.id == consumo_llm_id)), None)
        if consumo is None:
            raise LifecycleRefused("consumption row not found")
        values = dict(
            provider_generation_id=consumo.provider_generation_id,
            tokens_input=consumo.tokens_input,
            tokens_output=consumo.tokens_output,
            tokens_reasoning=consumo.tokens_reasoning,
            tokens_cache_read=consumo.tokens_cache_read,
            tokens_cache_write=consumo.tokens_cache_write,
            custo_creditos=Decimal(consumo.custo_creditos),
            consumo_llm_id=consumo.id,
        )
    already = attempt.reconciled_at is not None
    accepted = await scheduler_repo.resolve_generation_reconciliation(
        db,
        attempt_key=attempt_key,
        scheduler_epoch=identity.scheduler_epoch,
        lane_owner_token=identity.lane_owner_token,
        actor=actor,
        reason=reason,
        resolved_outcome=outcome,
        safe_error_code=attempt.safe_error_code,
        **values,
    )
    if not accepted:
        raise LifecycleRefused("resolution evidence does not match the exact attempt")
    if not already:
        runtime = await delivery_state_repo.get_runtime_state(db)
        db.add(
            _snapshot_action(
                delivery=None,
                action=f"generation_resolve:{outcome}",
                actor=actor,
                reason=f"attempt={attempt.id};{reason}",
                runtime=runtime,
            )
        )
        logger.warning(
            "Generation quarantine resolved | agente={} attempt={} outcome={} evidence={}",
            attempt.agente_id,
            attempt.id,
            outcome,
            evidence,
        )
    await db.flush()
    return QuarantineResolution(attempt.id, outcome, consumo_llm_id, not already)
