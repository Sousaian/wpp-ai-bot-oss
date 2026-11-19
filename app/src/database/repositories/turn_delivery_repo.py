"""Ticket06 durable automated-delivery admission and completion.

Every mutation follows the scheduler prefix and locks ``outbox_deliveries``
last. The legacy outbox claim commits first; this module then re-authorizes the
exact delivery before any Evolution I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from loguru import logger
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    ConversationTurn,
    DeliveryOperatorAction,
    HandoffAttempt,
    OutboxDelivery,
    ScheduledHandoff,
    TurnRevision,
    SchedulerRuntimeState,
)
from src.database.repositories import outbox_repo, scheduler_repo, sessao_repo
from src.database.repositories import turn_generation_repo as generation
from src.services.delivery_adapters import decode_evolution_text


Effect = Literal["reply", "handoff_transition"]
_terminal_at_admission_count = 0
_epoch_mismatch_count = 0
_fifo_defer_count = 0


def epoch_mismatch_count() -> int:
    return _epoch_mismatch_count


def fifo_defer_count() -> int:
    return _fifo_defer_count


def _count_fifo_defer(graph: generation._OwnedGraph, effect: OutboxDelivery) -> None:
    """Session-scoped signal for an admission held behind an open stream row."""

    global _fifo_defer_count
    _fifo_defer_count += 1
    logger.warning(
        "Automated admission deferred behind an open stream row | agente={} "
        "sessao={} turn={} delivery={} count={}",
        graph.turn.agente_id, graph.session.id, graph.turn.id, effect.id,
        _fifo_defer_count,
    )


MIN_RETRY_SECONDS = 1.0


def terminal_at_admission_count() -> int:
    return _terminal_at_admission_count


@dataclass(frozen=True)
class AdmissionResult:
    delivery_id: int
    effect: Effect
    created: bool
    outcome: Literal["admitted", "parked", "dead", "deferred"] = "admitted"


@dataclass(frozen=True)
class PresenceAuthority:
    phone: str
    instance_name: str
    remaining_delay_ms: int


def _candidate_matches(
    candidate: generation.FinalCandidate,
    revision: TurnRevision,
) -> bool:
    return bool(
        revision.id == candidate.revision_id
        and revision.revision_number == candidate.revision_number
        and revision.sealed_first_input_sequence
        == candidate.sealed_first_input_sequence
        and revision.sealed_last_input_sequence
        == candidate.sealed_last_input_sequence
        and revision.draft_kind == candidate.draft_kind
        and bool(revision.needs_handoff) == candidate.needs_handoff
        and revision.typing_deadline_at == candidate.typing_deadline_at
    )


async def authorize_presence(
    db: AsyncSession,
    *,
    lease: generation.TurnLease,
    revision_id: int,
) -> PresenceAuthority | None:
    """Fresh pre-I/O check for one composing/paused dispatch."""

    try:
        graph = await generation._lock_owned_graph(db, lease)
    except generation.StaleTurnOwnership:
        return None
    revision = graph.current
    if (
        revision.id != revision_id
        or revision.status != "typing"
        or revision.typing_deadline_at is None
    ):
        return None
    remaining = max(
        0,
        min(12000, int((revision.typing_deadline_at - graph.now).total_seconds() * 1000)),
    )
    return PresenceAuthority(
        phone=graph.session.telefone,
        instance_name=graph.agent.instance_name,
        remaining_delay_ms=remaining,
    )


async def _lock_scheduled_handoff(
    db: AsyncSession,
    *,
    turn_id: int,
    revision_id: int,
) -> tuple[ScheduledHandoff | None, HandoffAttempt | None]:
    scheduled = (
        await db.execute(
            select(ScheduledHandoff)
            .where(
                ScheduledHandoff.conversation_turn_id == turn_id,
                ScheduledHandoff.turn_revision_id == revision_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if scheduled is None:
        return None, None
    attempt = (
        await db.execute(
            select(HandoffAttempt)
            .where(HandoffAttempt.id == scheduled.handoff_attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return scheduled, attempt


async def _lock_effect_rows(
    db: AsyncSession,
    *,
    turn_id: int,
    revision_id: int,
    handoff_attempt_id: int | None = None,
) -> tuple[OutboxDelivery, ...]:
    predicate = (
        OutboxDelivery.handoff_attempt_id == handoff_attempt_id
        if handoff_attempt_id is not None
        else (
            (OutboxDelivery.conversation_turn_id == turn_id)
            & (OutboxDelivery.turn_revision_id == revision_id)
        )
    )
    rows = tuple(
        (
            await db.execute(
                select(OutboxDelivery)
                .where(predicate)
                .order_by(OutboxDelivery.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    return tuple(
        row
        for row in rows
        if row.delivery_class == "automated"
        and row.conversation_turn_id == turn_id
        and row.turn_revision_id == revision_id
    )


async def admit_candidate(
    db: AsyncSession,
    *,
    candidate: generation.FinalCandidate,
    ttl_seconds: int,
) -> AdmissionResult:
    """Admit or resume one exact revision while retaining its lane owner."""

    if ttl_seconds < 1:
        raise ValueError("ttl_seconds must be positive")
    graph = await generation._lock_owned_graph(db, candidate.lease, lock_successor=True)
    revision = graph.current
    if not _candidate_matches(candidate, revision):
        raise generation.StaleTurnOwnership("delivery candidate changed")
    now = graph.now

    if revision.status == "final_candidate":
        if candidate.needs_handoff:
            raise scheduler_repo.SchedulerConflictError(
                "handoff candidate must be finalized before delivery admission"
            )
        revision.status = "admitted"
        revision.admitted_at = now
        graph.turn.final_revision_id = revision.id
        graph.turn.status = "awaiting_delivery"
        # P2-1: after admission the cap counts only failed sends; recoveries
        # spent during generation (shared column) must not shrink it.
        graph.turn.delivery_attempt_count = 0
        sequence = (
            await sessao_repo.allocate_delivery_sequences(db, graph.session, count=1)
        )[0]
        payload = {
            "phone": graph.session.telefone,
            "message": revision.draft_text or "",
            "instance_name": graph.agent.instance_name,
        }
        decode_evolution_text("evolution.send_text.v1", 1, payload)
        row, created = await outbox_repo.enqueue(
            db,
            cutover_epoch=candidate.lease.delivery_runtime_epoch,
            dedupe_key=f"evolution:turn:{graph.turn.turn_key}:reply",
            provider="evolution",
            operation="evolution.send_text.v1",
            payload_version=1,
            payload=payload,
            ordering_key=graph.session.delivery_stream_key,
            stream_sequence=sequence,
            next_attempt_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
            priority=20,
            agente_id=graph.agent.id,
            sessao_id=graph.session.id,
            _delivery_class="automated",
            _automated_effect="reply",
            _scheduler_epoch=graph.turn.scheduler_epoch,
            _conversation_turn_id=graph.turn.id,
            _turn_revision_id=revision.id,
        )
        await db.flush()
        if await _fifo_blocked(db, row):
            # Never hold the lane behind an open earlier stream row (human
            # reply, legacy row): discovery requeues once it is terminal.
            _count_fifo_defer(graph, row)
            deferred = await _defer_admission(db, graph=graph, effect=row)
            return AdmissionResult(row.id, "reply", created, deferred.outcome)
        return AdmissionResult(row.id, "reply", created)

    if (
        revision.status != "admitted"
        or graph.turn.final_revision_id != revision.id
        or graph.turn.status != "claimed"
    ):
        raise generation.StaleTurnOwnership("admitted delivery candidate is stale")

    scheduled, attempt = await _lock_scheduled_handoff(
        db, turn_id=graph.turn.id, revision_id=revision.id
    )
    effects = await _lock_effect_rows(
        db,
        turn_id=graph.turn.id,
        revision_id=revision.id,
        handoff_attempt_id=(scheduled.handoff_attempt_id if scheduled else None),
    )
    if scheduled is not None and (
        attempt is None
        or attempt.sessao_id != graph.session.id
        or attempt.generation != graph.session.handoff_generation
        or attempt.status not in {"pending", "ready"}
    ):
        raise generation.StaleTurnOwnership("handoff attempt is no longer current")
    by_effect = {row.automated_effect: row for row in effects}
    if len(by_effect) != len(effects) or not effects:
        raise scheduler_repo.SchedulerConflictError(
            "admitted revision has inconsistent automated effects"
        )

    selected: OutboxDelivery | None = None
    reply = by_effect.get("reply")
    transition = by_effect.get("handoff_transition")
    if scheduled is None:
        if reply is None or transition is not None or reply.handoff_attempt_id is not None:
            raise scheduler_repo.SchedulerConflictError(
                "ordinary admitted revision has invalid effects"
            )
        selected = reply
    else:
        if attempt is None or transition is None:
            raise scheduler_repo.SchedulerConflictError(
                "scheduled handoff lost its transition authority"
            )
        if reply is not None and reply.status not in {"delivered", "dead", "cancelled"}:
            selected = reply
        elif reply is not None and reply.status != "delivered":
            raise scheduler_repo.SchedulerConflictError(
                "handoff reply terminal failure requires operator resolution"
            )
        else:
            # All handoff deliveries were locked in ascending ID above. Repeat
            # the dependency as an authority locking read, never a plain view.
            dependency = (
                await db.execute(
                    select(OutboxDelivery)
                    .where(OutboxDelivery.id == transition.depends_on_delivery_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if attempt.status != "ready" or dependency is None or dependency.status != "delivered":
                return await _defer_admission(db, graph=graph, effect=transition)
            if transition.status == "blocked":
                transition.status = "pending"
                transition.next_attempt_at = now
                transition.expires_at = now + timedelta(seconds=ttl_seconds)
            selected = transition

    if selected.status == "processing":
        if selected.lock_until is not None and selected.lock_until > now:
            # Never hold the agent lane waiting for another worker's lease.
            return await _defer_admission(db, graph=graph, effect=selected)
        # Generic maintenance never recovers automated rows; the lane-owned
        # admission does, under the full canonical prefix. Remote acceptance of
        # the expired claim is unknown: bounded at-least-once, counted ambiguous.
        _clear_delivery_lease(selected)
        selected.ambiguity_count += 1
        selected.last_error_code = "lease_expired_ambiguous"
        if selected.cancel_requested_at is not None:
            selected.status = "cancelled"
            selected.terminal_at = now
            selected.terminal_reason = "cancel_requested"
        else:
            selected.status = "retry"
            selected.next_attempt_at = now
    if selected.status in {"pending", "retry"} and (
        selected.expires_at is None or selected.expires_at <= now
    ):
        selected.status = "dead"
        selected.terminal_at = now
        selected.terminal_reason = "expired"
        selected.last_error_code = "automated_effect_expired"
    if selected.status in {"dead", "cancelled"}:
        return await _terminal_effect_at_admission(
            db, graph=graph, effect=selected, handoff=scheduled is not None
        )
    if selected.status not in {"pending", "retry"}:
        raise scheduler_repo.SchedulerConflictError(
            "automated effect is not eligible for admission"
        )
    if selected.next_attempt_at is None or selected.next_attempt_at > now:
        return await _defer_admission(db, graph=graph, effect=selected)
    if selected.cutover_epoch != candidate.lease.delivery_runtime_epoch:
        # Never claimable under this runtime: release the lane (rollover is
        # ticket 07/08) instead of holding it until the lease expires.
        global _epoch_mismatch_count
        _epoch_mismatch_count += 1
        logger.error(
            "Automated effect on a stale delivery epoch; admission deferred | "
            "agente={} turn={} delivery={} count={} threshold=1",
            graph.turn.agente_id, graph.turn.id, selected.id, _epoch_mismatch_count,
        )
        return await _defer_admission(db, graph=graph, effect=selected)
    if await _fifo_blocked(db, selected):
        # The claim would never pick it while an earlier stream row is open.
        _count_fifo_defer(graph, selected)
        return await _defer_admission(db, graph=graph, effect=selected)
    graph.turn.status = "awaiting_delivery"
    await db.flush()
    return AdmissionResult(selected.id, selected.automated_effect, False)


async def _defer_admission(
    db: AsyncSession,
    *,
    graph: generation._OwnedGraph,
    effect: OutboxDelivery,
) -> AdmissionResult:
    """Release the lane without counting; discovery requeues when eligible."""

    turn = graph.turn
    lane = graph.lane
    turn.status = "awaiting_delivery"
    turn.retry_at = None
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    await db.flush()
    return AdmissionResult(effect.id, effect.automated_effect, False, "deferred")


async def _terminal_effect_at_admission(
    db: AsyncSession,
    *,
    graph: generation._OwnedGraph,
    effect: OutboxDelivery,
    handoff: bool,
) -> AdmissionResult:
    """A lane-owned admission found its effect dead/cancelled/expired.

    Ordinary turn: no authorized delivery remains, so the turn is terminal and
    its successor is promoted. Handoff turn: the dead row is the operator work
    item (ticket 07); the turn parks without orphaning scheduled effects.
    """

    global _terminal_at_admission_count
    turn = graph.turn
    lane = graph.lane
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    turn.retry_at = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    _terminal_at_admission_count += 1
    if handoff:
        turn.status = "awaiting_delivery"
        outcome: Literal["parked", "dead", "deferred"] = "parked"
    elif not await _resolve_remaining_effects(
        db,
        turn_id=turn.id,
        revision_id=graph.current.id,
        now=graph.now,
        reason=effect.terminal_reason or "automated_effect_terminal",
        processing_status="dead",
    ):
        # Y1: never end a turn while one of its effects is still in flight.
        turn.status = "awaiting_delivery"
        outcome = "deferred"
    else:
        turn.status = "dead"
        turn.session_slot = "terminal"
        turn.terminal_reason = effect.terminal_reason or "automated_effect_terminal"
        turn.terminal_at = graph.now
        outcome = "dead"
    logger.error(
        "Automated effect terminal at admission | agente={} turn={} delivery={} "
        "effect={} reason={} outcome={} count={} threshold=1",
        turn.agente_id,
        turn.id,
        effect.id,
        effect.automated_effect,
        effect.terminal_reason,
        outcome,
        _terminal_at_admission_count,
    )
    await db.flush()
    if outcome == "dead":
        await scheduler_repo._promote_successor_after_terminal_active(
            db, sessao_id=graph.session.id
        )
        await db.flush()
    return AdmissionResult(effect.id, effect.automated_effect, False, outcome)


_ACTIVE_STREAM_STATUSES = ("blocked", "pending", "processing", "retry")


def _due(effect, now):
    """An effect the lane-owned admission can act on right now."""

    return or_(
        (effect.c.status.in_(("pending", "retry"))) & (effect.c.next_attempt_at <= now),
        (effect.c.status == "processing")
        & (effect.c.lock_until.is_(None) | (effect.c.lock_until <= now)),
    )


def _fifo_clear(effect):
    """No earlier non-terminal row in the same stream (mirrors CLAIM_SQL)."""

    predecessor = OutboxDelivery.__table__.alias("stream_predecessor")
    return ~(
        select(predecessor.c.id)
        .where(
            predecessor.c.ordering_key == effect.c.ordering_key,
            predecessor.c.stream_sequence < effect.c.stream_sequence,
            predecessor.c.status.in_(_ACTIVE_STREAM_STATUSES),
        )
        .exists()
    )


async def list_admitted_turns_ready_for_admission(
    db: AsyncSession,
    *,
    scheduler_epoch: str,
    limit: int,
) -> list[tuple[int, int, int]]:
    """Bounded discovery of every admitted turn that can progress.

    Two kinds of rows on a free lane (no owner: neither busy nor quarantined):

    - dispatch: active agent with opt-in, lifecycle still authorizing the
      staged effects, and an effect the admission can act on — due, on the
      current delivery epoch and not behind a non-terminal FIFO predecessor;
    - settle: the effects can no longer be dispatched (ordinary reply
      terminal without delivery, or a handoff superseded/no longer current).
      Requeue settles these turns under locks instead of waking the lane.

    A consistent read used only as a hint; requeue re-checks under locks.
    Rows that can do neither are never listed, so stuck rows never fill the
    page.
    """

    if not 1 <= limit <= 1000:
        raise ValueError("limit must be between 1 and 1000")
    from src.database.models import AgentServiceLane, Agente, DeliveryRuntimeState, Sessao

    now = func.utc_timestamp(6)
    reply = OutboxDelivery.__table__.alias("reply_effect")
    transition = OutboxDelivery.__table__.alias("transition_effect")
    opened = OutboxDelivery.__table__.alias("open_effect")
    runtime_epoch = (
        select(DeliveryRuntimeState.cutover_epoch)
        .where(DeliveryRuntimeState.id == 1)
        .scalar_subquery()
    )

    def dispatchable(effect):
        return _due(effect, now) & (effect.c.cutover_epoch == runtime_epoch) & _fifo_clear(effect)

    handoff_current = (
        (Sessao.handler == "encerrada")
        & (Sessao.motivo_encerramento == "handoff")
        & (HandoffAttempt.generation == Sessao.handoff_generation)
        & HandoffAttempt.status.in_(("pending", "ready"))
    )
    ordinary_dispatch = (
        ScheduledHandoff.handoff_attempt_id.is_(None)
        & (Sessao.handler == "bot")
        & reply.c.id.is_not(None)
        & dispatchable(reply)
    )
    handoff_dispatch = (
        ScheduledHandoff.handoff_attempt_id.is_not(None)
        & handoff_current
        & or_(
            reply.c.id.is_not(None) & dispatchable(reply),
            (reply.c.id.is_(None) | (reply.c.status == "delivered"))
            & (opened.c.status == "delivered")
            & (HandoffAttempt.status == "ready")
            & (
                ((transition.c.status == "blocked") & _fifo_clear(transition))
                | dispatchable(transition)
            ),
        )
    )
    ordinary_cap = ConversationTurn.delivery_attempt_count >= (
        select(SchedulerRuntimeState.max_delivery_attempts_per_turn)
        .where(SchedulerRuntimeState.id == 1).scalar_subquery()
    )
    ordinary_refused = or_(
        reply.c.cancel_requested_at.is_not(None),
        Agente.ativo.is_(False),
        Agente.inbox_scheduler_enabled.is_(False),
        Sessao.handler != "bot",
    )
    live_effect = OutboxDelivery.__table__.alias("live_effect")
    no_live_effect = ~(
        select(live_effect.c.id)
        .where(
            live_effect.c.conversation_turn_id == ConversationTurn.id,
            live_effect.c.turn_revision_id == TurnRevision.id,
            live_effect.c.delivery_class == "automated",
            live_effect.c.status == "processing",
            live_effect.c.lock_until > now,
        )
        .exists()
    )
    # Y1: a turn whose effect is still under a live lease is never listed for
    # settlement (no hot loop); it reappears once that lease lapses.
    settle = no_live_effect & or_(
        ScheduledHandoff.handoff_attempt_id.is_(None)
        & or_(
            reply.c.status.in_(("dead", "cancelled")),
            ordinary_cap & reply.c.status.in_(_ACTIVE_STREAM_STATUSES),
            (reply.c.status == "processing")
            & ordinary_refused
            & (reply.c.lock_until.is_(None) | (reply.c.lock_until <= now)),
        ),
        ScheduledHandoff.handoff_attempt_id.is_not(None) & ~handoff_current,
    )
    rows = await db.execute(
        select(
            ConversationTurn.id,
            ConversationTurn.agente_id,
            ConversationTurn.sessao_id,
        )
        .join(
            TurnRevision,
            (TurnRevision.id == ConversationTurn.final_revision_id)
            & (TurnRevision.status == "admitted"),
        )
        .join(
            AgentServiceLane,
            (AgentServiceLane.agente_id == ConversationTurn.agente_id)
            & (AgentServiceLane.scheduler_epoch == ConversationTurn.scheduler_epoch),
        )
        .join(Agente, Agente.id == ConversationTurn.agente_id)
        .join(Sessao, Sessao.id == ConversationTurn.sessao_id)
        .outerjoin(
            ScheduledHandoff,
            (ScheduledHandoff.conversation_turn_id == ConversationTurn.id)
            & (ScheduledHandoff.turn_revision_id == TurnRevision.id),
        )
        .outerjoin(
            HandoffAttempt, HandoffAttempt.id == ScheduledHandoff.handoff_attempt_id
        )
        .outerjoin(
            reply,
            (reply.c.conversation_turn_id == ConversationTurn.id)
            & (reply.c.turn_revision_id == TurnRevision.id)
            & (reply.c.automated_effect == "reply"),
        )
        .outerjoin(
            transition,
            (transition.c.conversation_turn_id == ConversationTurn.id)
            & (transition.c.turn_revision_id == TurnRevision.id)
            & (transition.c.automated_effect == "handoff_transition"),
        )
        .outerjoin(opened, opened.c.id == transition.c.depends_on_delivery_id)
        .where(
            ConversationTurn.scheduler_epoch == scheduler_epoch,
            ConversationTurn.status == "awaiting_delivery",
            ConversationTurn.session_slot == "active",
            AgentServiceLane.owner_token.is_(None),
            or_(
                Agente.ativo.is_(True)
                & Agente.inbox_scheduler_enabled.is_(True)
                & or_(ordinary_dispatch, handoff_dispatch),
                settle,
            ),
        )
        .order_by(ConversationTurn.scheduling_sequence, ConversationTurn.id)
        .limit(limit)
    )
    return [
        (int(row.id), int(row.agente_id), int(row.sessao_id)) for row in rows
    ]


def _effect_due(effect: OutboxDelivery | None, now: datetime) -> bool:
    return bool(
        effect is not None
        and (
            (
                effect.status in {"pending", "retry"}
                and effect.next_attempt_at is not None
                and effect.next_attempt_at <= now
            )
            or (
                effect.status == "processing"
                and (effect.lock_until is None or effect.lock_until <= now)
            )
        )
    )


async def _fifo_blocked(db: AsyncSession, effect: OutboxDelivery) -> bool:
    """Consistent read after all requeue locks: earlier non-terminal stream row."""

    return bool(
        (
            await db.execute(
                select(OutboxDelivery.id)
                .where(
                    OutboxDelivery.ordering_key == effect.ordering_key,
                    OutboxDelivery.stream_sequence < effect.stream_sequence,
                    OutboxDelivery.status.in_(_ACTIVE_STREAM_STATUSES),
                )
                .limit(1)
            )
        ).first()
    )


async def requeue_admitted_turn(
    db: AsyncSession,
    *,
    turn_id: int,
    agente_id: int,
    sessao_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
) -> bool:
    """Move one waiting admitted turn to the fair tail, or settle it.

    Under the canonical locks: a turn whose effects can no longer be
    dispatched is settled here (ordinary → dead + successor promoted;
    superseded handoff → cancelled + successor promoted). A dispatchable turn
    becomes ``retry`` at the lane tail. A turn found ineligible still rotates
    to the tail so a bounded discovery page never stays full of rows that
    cannot progress.
    """

    runtime, scheduler, agent, lane = await scheduler_repo.lock_authority_prefix(
        db,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        require_enabled=False,
    )
    if scheduler.state not in {"enabled", "draining"} or lane.owner_token is not None:
        return False
    from src.database.models import Sessao

    session = (
        await db.execute(
            select(Sessao)
            .where(Sessao.id == sessao_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    turn = (
        await db.execute(
            select(ConversationTurn)
            .where(ConversationTurn.id == turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        session is None
        or turn is None
        or turn.agente_id != agente_id
        or turn.sessao_id != sessao_id
        or turn.status != "awaiting_delivery"
        or turn.session_slot != "active"
        or turn.final_revision_id is None
    ):
        return False
    await scheduler_repo.lock_pending_successor(db, sessao_id=session.id)
    revision = (
        await db.execute(
            select(TurnRevision)
            .where(TurnRevision.id == turn.final_revision_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    scheduled, attempt = await _lock_scheduled_handoff(
        db, turn_id=turn.id, revision_id=turn.final_revision_id
    )
    effects = await _lock_effect_rows(
        db,
        turn_id=turn.id,
        revision_id=turn.final_revision_id,
        handoff_attempt_id=scheduled.handoff_attempt_id if scheduled else None,
    )
    by_effect = {row.automated_effect: row for row in effects}
    reply = by_effect.get("reply")
    transition = by_effect.get("handoff_transition")
    transition_dependency = None
    if transition is not None:
        transition_dependency = (
            await db.execute(
                select(OutboxDelivery)
                .where(OutboxDelivery.id == transition.depends_on_delivery_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    if revision is None or revision.status != "admitted":
        return False
    graph = scheduler_repo.AutomatedDeliveryGraph(
        runtime=runtime,
        scheduler=scheduler,
        agent=agent,
        lane=lane,
        session=session,
        turn=turn,
        revision=revision,
        handoff_attempt=attempt if scheduled is not None else None,
        delivery=None,
        dependency=None,
        now=now,
    )
    settle_effect: OutboxDelivery | None = None
    if scheduled is None:
        if reply is not None and reply.status in {"dead", "cancelled"}:
            settle_effect = reply
        elif reply is not None and (
            turn.delivery_attempt_count >= scheduler.max_delivery_attempts_per_turn
            or (reply.status == "processing" and (
                reply.cancel_requested_at is not None or not _lifecycle_allows(graph)
            ))
        ):
            # Settlement does not need a dispatchable agent or current effect
            # epoch. Live leases keep waiting; expired/absent leases are unknown.
            at_cap = turn.delivery_attempt_count >= scheduler.max_delivery_attempts_per_turn
            reason = "delivery_attempt_cap" if at_cap else "lifecycle_refused"
            if not scheduler_repo.resolve_open_automated_effects_locked(
                effects, now=now, reason=reason,
                processing_status="dead", turn_id=turn.id,
            ):
                return False
            settle_effect = reply
    elif not _handoff_current(graph):
        settle_effect = transition or reply
    if settle_effect is not None:
        await _settle_turn_after_effect(
            db,
            graph=graph,
            effect=settle_effect,
            reason=settle_effect.terminal_reason or "automated_effect_terminal",
        )
        await db.flush()
        return False

    def dispatchable(effect: OutboxDelivery | None) -> bool:
        return bool(
            effect is not None
            and _effect_due(effect, now)
            and effect.cutover_epoch == runtime.cutover_epoch
        )

    eligible = False
    candidate: OutboxDelivery | None = None
    if agent.ativo and agent.inbox_scheduler_enabled:
        if scheduled is None:
            if session.handler == "bot" and dispatchable(reply):
                candidate = reply
        elif attempt is not None:
            if dispatchable(reply):
                candidate = reply
            elif bool(
                transition is not None
                and (reply is None or reply.status == "delivered")
                and attempt.status == "ready"
                and transition_dependency is not None
                and transition_dependency.status == "delivered"
                and (transition.status == "blocked" or dispatchable(transition))
            ):
                candidate = transition
    if candidate is not None:
        eligible = not await _fifo_blocked(db, candidate)
    turn.scheduling_sequence = lane.next_scheduling_sequence
    lane.next_scheduling_sequence += 1
    if eligible:
        turn.status = "retry"
        turn.retry_at = now
        turn.ready_at = now
    await db.flush()
    return eligible


def _clear_delivery_lease(delivery: OutboxDelivery) -> None:
    delivery.lease_token = None
    delivery.lock_owner = None
    delivery.lock_until = None


_handoff_effect_terminal_count = 0
_preio_release_count = 0
_lifecycle_settle_count = 0
PREIO_LANE_BACKOFF_SECONDS = 1.0


def handoff_effect_terminal_count() -> int:
    return _handoff_effect_terminal_count


def preio_release_count() -> int:
    return _preio_release_count


def lifecycle_settle_count() -> int:
    return _lifecycle_settle_count


def _owns_lane(graph: scheduler_repo.AutomatedDeliveryGraph) -> bool:
    turn = graph.turn
    lane = graph.lane
    return bool(
        turn is not None
        and lane.owner_token is not None
        and lane.active_turn_id == turn.id
        and turn.claim_token == lane.owner_token
    )


def _release_owned_lane(graph: scheduler_repo.AutomatedDeliveryGraph) -> None:
    """Release the lane only if this turn owns it; always clear the turn claim."""

    turn = graph.turn
    lane = graph.lane
    if _owns_lane(graph):
        lane.owner_token = None
        lane.owner_worker_id = None
        lane.lease_until = None
        lane.active_turn_id = None
        lane.active_revision_id = None
    if turn is not None:
        turn.claim_token = None
        turn.claim_owner = None
        turn.claim_until = None


def _handoff_current(graph: scheduler_repo.AutomatedDeliveryGraph) -> bool:
    attempt = graph.handoff_attempt
    session = graph.session
    return bool(
        attempt is not None
        and session is not None
        and attempt.sessao_id == session.id
        and attempt.generation == session.handoff_generation
        and attempt.status in {"pending", "ready"}
        and session.handler == "encerrada"
        and session.motivo_encerramento == "handoff"
    )


def _lifecycle_allows(graph: scheduler_repo.AutomatedDeliveryGraph) -> bool:
    """The lifecycle part of the dispatch fence, independent of the lease."""

    agent = graph.agent
    session = graph.session
    turn = graph.turn
    revision = graph.revision
    if (
        session is None
        or turn is None
        or revision is None
        or not agent.ativo
        or not agent.inbox_scheduler_enabled
        or turn.session_slot == "terminal"
        or revision.status != "admitted"
        or turn.final_revision_id != revision.id
    ):
        return False
    if graph.handoff_attempt is None:
        return session.handler == "bot"
    return _handoff_current(graph)


async def _resolve_remaining_effects(
    db: AsyncSession,
    *,
    turn_id: int,
    revision_id: int,
    now: datetime,
    reason: str,
    processing_status: Literal["cancelled", "dead"],
) -> bool:
    """Resolve every open automated effect of one turn (deliveries locked last, id asc).

    ``False`` means a ``processing`` row still holds a live lease: nothing was
    touched and the turn must keep waiting (Y1).
    """

    rows = (
        await db.execute(
            select(OutboxDelivery)
            .where(
                OutboxDelivery.conversation_turn_id == turn_id,
                OutboxDelivery.turn_revision_id == revision_id,
                OutboxDelivery.delivery_class == "automated",
            )
            .order_by(OutboxDelivery.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalars().all()
    return scheduler_repo.resolve_open_automated_effects_locked(
        rows,
        now=now,
        reason=reason,
        processing_status=processing_status,
        turn_id=turn_id,
    )


async def _settle_turn_after_effect(
    db: AsyncSession,
    *,
    graph: scheduler_repo.AutomatedDeliveryGraph,
    effect: OutboxDelivery,
    reason: str,
) -> str:
    """Settle the admitted turn once one effect reached a terminal truth.

    The effect's own status is already the truth (delivered, dead or
    cancelled). The *current* lifecycle decides the turn: an ordinary turn
    becomes terminal and promotes its successor; a current handoff continues
    (reply delivered) or parks (effect dead); a superseded handoff is
    cancelled with its remaining effects and promotes the successor. No
    admitted turn is ever left without a path forward.

    Y1: a turn only becomes terminal once every one of its effects is
    terminal. A sibling still ``processing`` under a live lease keeps the turn
    ``awaiting_delivery`` (``"waiting"``); an expired one is resolved as
    ambiguous in the same UoW.
    """

    global _handoff_effect_terminal_count, _lifecycle_settle_count
    turn = graph.turn
    revision = graph.revision
    session = graph.session
    now = graph.now
    if (
        turn is None
        or revision is None
        or session is None
        or turn.session_slot == "terminal"
        or turn.final_revision_id != revision.id
    ):
        return "untouched"
    handoff = graph.handoff_attempt is not None
    current = _handoff_current(graph)

    def release_waiting() -> None:
        turn.status = "awaiting_delivery"
        turn.retry_at = None
        _release_owned_lane(graph)

    async def terminal(
        status: str,
        terminal_reason: str,
        effect_reason: str,
        processing_status: Literal["cancelled", "dead"],
    ) -> bool:
        if not await _resolve_remaining_effects(
            db,
            turn_id=turn.id,
            revision_id=revision.id,
            now=now,
            reason=effect_reason,
            processing_status=processing_status,
        ):
            release_waiting()
            await db.flush()
            return False
        turn.status = status
        turn.session_slot = "terminal"
        turn.terminal_reason = terminal_reason
        turn.terminal_at = now
        turn.retry_at = None
        _release_owned_lane(graph)
        await db.flush()
        await scheduler_repo._promote_successor_after_terminal_active(
            db, sessao_id=session.id
        )
        return True

    # An accepted abandon during live I/O remains authoritative after the
    # owner finishes. The exact effect's append-only operator audit is the
    # durable intent; cancelling an individual delivery alone still parks a
    # current handoff under the existing retry/abandon contract.
    # FU-2: all automated audit writers hold the scheduler/turn/effect locks
    # already held here, acquired before the read view. A plain read sees
    # their committed intent without locking a possibly absent index gap.
    abandoned = (await db.execute(
        select(DeliveryOperatorAction.id)
        .where(
            DeliveryOperatorAction.delivery_id == effect.id,
            DeliveryOperatorAction.action == "turn_resolve:abandon",
        )
        .limit(1)
    )).scalar_one_or_none()
    if abandoned is not None:
        if not await terminal("cancelled", "operator_abandoned", "cancel_requested", "cancelled"):
            return "waiting"
        return "cancelled"

    if handoff and not current:
        if not await terminal(
            "cancelled", "handoff_superseded", "handoff_superseded", "cancelled"
        ):
            return "waiting"
        _lifecycle_settle_count += 1
        logger.warning(
            "Superseded handoff turn settled | agente={} turn={} delivery={} "
            "effect={} status={} count={} threshold=1",
            turn.agente_id, turn.id, effect.id, effect.automated_effect,
            effect.status, _lifecycle_settle_count,
        )
        await db.flush()
        return "cancelled"
    if effect.status == "delivered":
        if handoff and effect.automated_effect == "handoff_transition":
            if not await terminal(
                "handoff", "handoff_delivered", "handoff_delivered", "dead"
            ):
                return "waiting"
            return "handoff"
        if handoff:
            release_waiting()
            await db.flush()
            return "awaiting_delivery"
        # Evaluated before terminal() marks the slot terminal.
        lifecycle_allowed = _lifecycle_allows(graph)
        if not await terminal("delivered", "delivered", "delivered", "dead"):
            return "waiting"
        if not lifecycle_allowed:
            _lifecycle_settle_count += 1
            logger.warning(
                "Ordinary reply delivered after lifecycle change | agente={} turn={} "
                "count={} threshold=1",
                turn.agente_id, turn.id, _lifecycle_settle_count,
            )
        return "delivered"
    if handoff:
        release_waiting()
        _handoff_effect_terminal_count += 1
        logger.error(
            "Handoff automated effect terminal; turn parked | agente={} turn={} "
            "delivery={} effect={} reason={} count={} threshold=1",
            turn.agente_id, turn.id, effect.id, effect.automated_effect,
            effect.terminal_reason, _handoff_effect_terminal_count,
        )
        await db.flush()
        return "parked"
    if not await terminal("dead", reason, reason, "dead"):
        return "waiting"
    return "dead"


settle_turn_after_effect = _settle_turn_after_effect
_runtime_gate_release_count = 0
PREIO_RUNTIME_BACKOFF_SECONDS = 1.0


def runtime_gate_release_count() -> int:
    return _runtime_gate_release_count


async def _release_claim_at_runtime_gate(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
) -> bool:
    """Return a claim refused by the runtime/scheduler gate to ``retry``.

    Called after the gate rows (runtime S, scheduler X when reached) are held;
    only the exact delivery row is locked next. No I/O happened, so the lease
    is cleared without ambiguity and with positive backoff. Nothing else
    changes: the turn and lane are recovered by their own owners once the
    gate allows it, and a frozen runtime keeps the row visible to rollback
    checks and audited operator cancel.
    """

    global _runtime_gate_release_count
    delivery = (
        await db.execute(
            select(OutboxDelivery)
            .where(OutboxDelivery.id == delivery_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        delivery is None
        or delivery.status != "processing"
        or delivery.lease_token != lease_token
        or delivery.cutover_epoch != cutover_epoch
        or delivery.delivery_class != "automated"
    ):
        return False
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    _clear_delivery_lease(delivery)
    delivery.status = "retry"
    delivery.next_attempt_at = now + timedelta(seconds=PREIO_RUNTIME_BACKOFF_SECONDS)
    delivery.last_error_code = "preio_refused_runtime"
    _runtime_gate_release_count += 1
    logger.error(
        "Automated claim released before I/O by the runtime gate | delivery={} "
        "count={} threshold=1",
        delivery.id,
        _runtime_gate_release_count,
    )
    await db.flush()
    return False


async def authorize_before_io(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    hint: scheduler_repo.AutomatedDeliveryHint,
    io_budget_seconds: float,
) -> bool:
    """Canonical pre-I/O re-authorization of a committed automated claim.

    On success the delivery lease, the lane and the turn are all extended to
    at least ``now + io_budget`` so no authorized send can outlive them. When
    the claim is refused before any I/O, the row is released without
    ambiguity and the turn is settled in the same UoW: cancel/lifecycle →
    effect ``cancelled`` + turn settled; lane lost (or the delivery lease
    already lapsed unrecovered) → ``retry`` with positive backoff. A runtime
    or scheduler gate refusal (frozen, divergent epoch, scheduler not
    running) also returns the exact claim to ``retry`` (ticket07).
    """

    global _preio_release_count
    if io_budget_seconds <= 0:
        raise ValueError("io_budget_seconds must be positive")
    try:
        graph = await scheduler_repo.lock_automated_delivery_graph(
            db, delivery_id=delivery_id, cutover_epoch=cutover_epoch, hint=hint
        )
    except (scheduler_repo.SchedulerNotReadyError, scheduler_repo.SchedulerConflictError):
        # Runtime/scheduler gate refused (frozen, divergent epoch). The gate
        # rows are already held; locking only the delivery next keeps the
        # canonical order. No I/O happened: release without ambiguity.
        return await _release_claim_at_runtime_gate(
            db, delivery_id=delivery_id, lease_token=lease_token, cutover_epoch=cutover_epoch
        )
    if graph is None:
        return False
    refusal = scheduler_repo.automated_authority_refusal(
        graph,
        delivery_lease_token=lease_token,
        cutover_epoch=cutover_epoch,
        hint=hint,
    )
    delivery = graph.delivery
    now = graph.now
    io_until = now + timedelta(seconds=io_budget_seconds)
    if refusal is None:
        # Exact authority held: extend every lease the send depends on.
        if delivery.lock_until is None or delivery.lock_until < io_until:
            delivery.lock_until = io_until
        if graph.lane.lease_until is None or graph.lane.lease_until < io_until:
            graph.lane.lease_until = io_until
        graph.turn.claim_until = graph.lane.lease_until
        await db.flush()
        return True
    owned_unrecovered = bool(
        delivery is not None
        and delivery.status == "processing"
        and delivery.lease_token == lease_token
        and delivery.cutover_epoch == cutover_epoch
    )
    if refusal == "runtime":
        return await _release_claim_at_runtime_gate(
            db, delivery_id=delivery_id, lease_token=lease_token, cutover_epoch=cutover_epoch
        )
    if not owned_unrecovered:
        return False
    # No I/O happened: release the claim without counting ambiguity.
    _clear_delivery_lease(delivery)
    delivery.last_error_code = f"preio_refused_{refusal}"
    _preio_release_count += 1
    logger.error(
        "Automated claim released before I/O | delivery={} refusal={} count={} "
        "threshold=1",
        delivery.id,
        refusal,
        _preio_release_count,
    )
    if refusal in {"cancelled", "lifecycle"}:
        delivery.status = "cancelled"
        delivery.terminal_at = now
        delivery.terminal_reason = (
            "cancel_requested" if refusal == "cancelled" else "lifecycle_refused"
        )
        await db.flush()
        await _settle_turn_after_effect(
            db, graph=graph, effect=delivery, reason=delivery.terminal_reason
        )
    else:
        delivery.status = "retry"
        delivery.next_attempt_at = now + timedelta(seconds=PREIO_LANE_BACKOFF_SECONDS)
    await db.flush()
    return False


async def complete_automated(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    hint: scheduler_repo.AutomatedDeliveryHint,
    external_id: str | None,
    http_status: int,
) -> bool:
    """Record an accepted send; lifecycle changes during I/O never refuse it."""

    graph = await scheduler_repo.lock_automated_delivery_graph(
        db, delivery_id=delivery_id, cutover_epoch=cutover_epoch, hint=hint
    )
    if graph is None or scheduler_repo.automated_authority_refusal(
        graph,
        delivery_lease_token=lease_token,
        cutover_epoch=cutover_epoch,
        hint=hint,
        for_completion=True,
    ) is not None:
        return False
    delivery = graph.delivery
    now = graph.now
    delivery.status = "delivered"
    delivery.external_id = external_id
    delivery.last_http_status = http_status
    delivery.last_error_code = None
    delivery.delivered_at = now
    delivery.terminal_at = now
    delivery.terminal_reason = (
        "delivered_after_cancel"
        if delivery.cancel_requested_at is not None
        else "delivered"
    )
    _clear_delivery_lease(delivery)
    await db.flush()
    await _settle_turn_after_effect(
        db, graph=graph, effect=delivery, reason="delivered"
    )
    await db.flush()
    return True


async def fail_automated(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    hint: scheduler_repo.AutomatedDeliveryHint,
    error_code: str,
    http_status: int | None,
    retry_delay_seconds: float | None,
    ambiguous: bool,
) -> Literal["stale", "retry", "dead", "parked", "cancelled"]:
    """Record a failed send attempt; only this path spends the turn cap.

    The failure is always recorded for the exact lease owner. A retry is kept
    only while the current lifecycle still authorizes the effect; otherwise
    the effect becomes terminal and the turn is settled in the same UoW.
    """

    graph = await scheduler_repo.lock_automated_delivery_graph(
        db, delivery_id=delivery_id, cutover_epoch=cutover_epoch, hint=hint
    )
    if graph is None or scheduler_repo.automated_authority_refusal(
        graph,
        delivery_lease_token=lease_token,
        cutover_epoch=cutover_epoch,
        hint=hint,
        for_completion=True,
    ) is not None:
        return "stale"
    delivery = graph.delivery
    turn = graph.turn
    now = graph.now
    if retry_delay_seconds is not None:
        # A failed send always yields with strictly positive backoff.
        retry_delay_seconds = max(MIN_RETRY_SECONDS, retry_delay_seconds)
    delivery.last_error_code = error_code
    delivery.last_http_status = http_status
    if ambiguous:
        delivery.ambiguity_count += 1
    _clear_delivery_lease(delivery)
    turn.delivery_attempt_count += 1
    retry_at = (
        now + timedelta(seconds=retry_delay_seconds)
        if retry_delay_seconds is not None
        else None
    )
    at_cap = turn.delivery_attempt_count >= graph.scheduler.max_delivery_attempts_per_turn
    can_retry = bool(
        delivery.cancel_requested_at is None
        and retry_at is not None
        and delivery.expires_at is not None
        and retry_at < delivery.expires_at
        and not at_cap
        and _lifecycle_allows(graph)
    )
    if can_retry:
        delivery.status = "retry"
        delivery.next_attempt_at = retry_at
        if _owns_lane(graph):
            turn.scheduling_sequence = graph.lane.next_scheduling_sequence
            graph.lane.next_scheduling_sequence += 1
            turn.status = "retry"
            turn.retry_at = retry_at
            turn.ready_at = retry_at
        _release_owned_lane(graph)
        await db.flush()
        return "retry"
    if delivery.cancel_requested_at is not None:
        delivery.status = "cancelled"
        delivery.terminal_reason = "cancel_requested"
    else:
        delivery.status = "dead"
        delivery.terminal_reason = (
            "automated_delivery_attempt_cap" if at_cap else error_code
        )
    delivery.terminal_at = now
    await db.flush()
    settled = await _settle_turn_after_effect(
        db,
        graph=graph,
        effect=delivery,
        reason="delivery_attempt_cap" if at_cap else delivery.terminal_reason,
    )
    await db.flush()
    if settled == "parked":
        return "parked"
    if settled == "cancelled" or delivery.status == "cancelled":
        return "cancelled"
    return "dead"


async def renew_automated_authority(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    hint: scheduler_repo.AutomatedDeliveryHint,
    delivery_lease_seconds: int,
    lane_lease_seconds: int,
) -> bool:
    """Renew under the full fence; a cancel or lifecycle change aborts I/O."""

    authority = await scheduler_repo.lock_automated_delivery_authority(
        db,
        delivery_id=delivery_id,
        delivery_lease_token=lease_token,
        cutover_epoch=cutover_epoch,
        hint=hint,
    )
    if authority is None:
        return False
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    delivery_until = now + timedelta(seconds=delivery_lease_seconds)
    lane_until = now + timedelta(seconds=lane_lease_seconds)
    if authority.delivery.lock_until is None or authority.delivery.lock_until < delivery_until:
        authority.delivery.lock_until = delivery_until
    # Never shorten an extension granted for the in-flight I/O deadline.
    if authority.lane.lease_until is None or authority.lane.lease_until < lane_until:
        authority.lane.lease_until = lane_until
    authority.turn.claim_until = authority.lane.lease_until
    await db.flush()
    return True
