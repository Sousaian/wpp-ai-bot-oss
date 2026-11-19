"""Terminal handoff summary and exact late-context capture (ticket 05).

Every public function is the body of ONE short transaction; callers wrap it in
``scheduler_repo.run_transaction_with_deadlock_retry``. No provider I/O occurs
while these locks are held.

Owner transactions take locking reads only, in the canonical prefix:
``delivery_runtime_state (S) -> scheduler_runtime_state -> agentes ->
agent_service_lanes -> sessoes -> conversation_turns (active, successor; id
ascending) -> turn_revisions (id ascending) -> turn_input_memberships of the
successor (id ascending) -> summary generation_attempts``. The first
consistent read (soft limit, history, late message content) happens after the
last lock wait. Inserts that follow only touch parents already locked above:
``consumo_llm`` (agent/session), ``handoff_attempts`` (agent/session),
``scheduled_handoffs`` (attempt/turn/revision/summary attempt),
``handoff_context_assignments`` (membership/attempt) and
``outbox_deliveries`` (agent/session/attempt/turn/revision/prior delivery), so
no implicit FK-parent or unique-index lock reverses the order.

The stale summary task uses ``turn_generation_repo.record_generation_accounting_only``
(``agentes`` S -> ``sessoes`` S -> attempt X -> ``consumo_llm``) and never
requests lane/session/turn authority.

Handoff admission leaves the turn ``active``/``awaiting_delivery`` with its
admitted revision and releases the lane: Chatwoot setup and context notes wait
without holding the agent. The ``automated`` transition stays ``blocked``;
ticket 06 re-queues and claims the turn to authorize it under lane authority.
The admitted revision's ``draft_text`` (the handoff reply) is kept for ticket 06.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal
from uuid import uuid4

from loguru import logger
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.agents.agent_runner import PROMPT_RESUMO_HANDOFF
from src.database.models import (
    ConversationTurn,
    GenerationAttempt,
    HandoffContextAssignment,
    Mensagem,
    ScheduledHandoff,
    TurnInputMembership,
    TurnRevision,
)
from src.database.repositories import (
    consumo_repo,
    handoff_terminal_repo,
    handoff_attempt_repo,
    outbox_repo,
    scheduler_repo,
    sessao_repo,
    turn_generation_repo as generation,
)
from src.services.delivery_adapters import (
    decode_chatwoot_context_note,
    decode_chatwoot_handoff,
    decode_evolution_text,
)
from src.services.handoff_context import LateInputRecord, plan_context_chunks
from src.services.handoff_manager import MENSAGEM_TRANSICAO, _resumo_fallback


SummaryKind = Literal["generated", "soft_limit_fallback", "technical_fallback"]

SUMMARY_HISTORY_LIMIT = 30


class HandoffAdmissionValidationError(ValueError):
    """A deterministic validation failure in repository-owned admission data."""


class HandoffLocalInvariantError(RuntimeError):
    """A repository-owned invariant of handoff admission does not hold.

    Raised explicitly (never via ``assert``, which ``-O`` strips) only at our
    own checks, so the service may classify it as deterministic. A bare
    ``AssertionError`` stays unclassified: MySQL drivers raise it on protocol
    desync and SQLAlchemy does not wrap it.
    """


@dataclass(frozen=True)
class SummaryCall:
    """A committed started summary attempt; the provider call happens next."""

    attempt_key: str
    modelo_llm: str
    messages: list[dict[str, str]]
    accounting_drain_seconds: int


@dataclass(frozen=True)
class FinalizedHandoff:
    handoff_attempt_id: int
    summary_kind: SummaryKind
    open_delivery_id: int
    transition_delivery_id: int
    context_delivery_ids: tuple[int, ...]
    covered_membership_ids: tuple[int, ...]


@dataclass(frozen=True)
class HandoffParked:
    """An already admitted handoff turn was reclaimed; no LLM, lane released."""

    handoff_attempt_id: int


@dataclass(frozen=True)
class HandoffReleased:
    """Guarded refusal: lane yielded with backoff, nothing finalized."""

    reason: str


HandoffStep = SummaryCall | FinalizedHandoff | HandoffParked | HandoffReleased
# HandoffFailed (below) is returned only by fail_handoff_terminally.


@dataclass
class _HandoffGraph:
    owned: generation._OwnedGraph
    successor: ConversationTurn | None
    successor_revision: TurnRevision | None
    memberships: list[TurnInputMembership]
    attempts: list[GenerationAttempt]


async def _lock_handoff_graph(
    db: AsyncSession,
    lease: generation.TurnLease,
) -> _HandoffGraph:
    """Exact owner authority plus the session's whole live turn graph.

    Turns and revisions are locked exactly like intake (turns ascending, then
    revisions ascending) after the session row, so a concurrent intake waits
    on the session and then observes the committed terminal state.
    """

    try:
        _, scheduler, agent, lane = await scheduler_repo.lock_authority_prefix(
            db,
            agente_id=lease.agente_id,
            scheduler_epoch=lease.scheduler_epoch,
            delivery_runtime_epoch=lease.delivery_runtime_epoch,
            require_enabled=False,
        )
    except (
        scheduler_repo.SchedulerConflictError,
        scheduler_repo.SchedulerNotReadyError,
        LookupError,
    ) as exc:
        raise generation.StaleTurnOwnership("scheduler authority changed") from exc
    if scheduler.state not in {"enabled", "draining"}:
        raise generation.StaleTurnOwnership("scheduler cannot continue work in current state")
    session = await sessao_repo.lock_by_id(db, lease.sessao_id, refresh_existing=True)
    if session is None:
        raise generation.StaleTurnOwnership("session disappeared")
    turn_graph = await scheduler_repo.lock_intake_turn_graph(db, sessao_id=session.id)
    turn = next((row for row in turn_graph.turns if row.id == lease.turn_id), None)
    successor = next(
        (row for row in turn_graph.turns if row.session_slot == "successor"), None
    )
    revisions = {
        row.id: row for row in turn_graph.revisions if turn and row.turn_id == turn.id
    }
    successor_revision = None
    if successor is not None:
        successor_revision = next(
            (
                row
                for row in turn_graph.revisions
                if row.id == successor.current_revision_id
            ),
            None,
        )
    memberships: list[TurnInputMembership] = []
    if successor is not None:
        memberships = list(
            (
                await db.execute(
                    select(TurnInputMembership)
                    .where(TurnInputMembership.turn_id == successor.id)
                    .order_by(TurnInputMembership.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
    # Last lock wait of every owner transaction: the turn's generation
    # attempts. Consistent reads (soft limit, history, content) follow it.
    attempts = list(
        (
            await db.execute(
                select(GenerationAttempt)
                .where(GenerationAttempt.turn_id == lease.turn_id)
                .order_by(GenerationAttempt.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    now = await generation._db_now(db)
    if (
        turn is None
        or turn.sessao_id != lease.sessao_id
        or turn.agente_id != lease.agente_id
        or turn.scheduler_epoch != lease.scheduler_epoch
        or turn.session_slot != "active"
        or turn.claim_token != lease.owner_token
        or turn.claim_until is None
        or turn.claim_until <= now
        or lane.owner_token != lease.owner_token
        or lane.active_turn_id != lease.turn_id
        or lane.lease_until is None
        or lane.lease_until <= now
        or turn.current_revision_id not in revisions
        or lane.active_revision_id != turn.current_revision_id
    ):
        raise generation.StaleTurnOwnership("stale turn ownership")
    return _HandoffGraph(
        owned=generation._OwnedGraph(
            scheduler=scheduler,
            agent=agent,
            lane=lane,
            session=session,
            turn=turn,
            revisions=revisions,
            now=now,
        ),
        successor=successor,
        successor_revision=successor_revision,
        memberships=memberships,
        attempts=attempts,
    )


def _assert_candidate(
    graph: generation._OwnedGraph,
    candidate: generation.FinalCandidate,
) -> TurnRevision:
    """Only the exact sealed, unsuperseded, unadmitted handoff candidate."""

    revision = graph.current
    if (
        candidate.lease.turn_id != graph.turn.id
        or candidate.revision_id != revision.id
        or candidate.revision_key != revision.revision_key
        or revision.status != "final_candidate"
        or revision.draft_kind != "generated"
        or revision.needs_handoff is not True
        or revision.typing_deadline_at is None
        or revision.typing_deadline_at > graph.now
        or revision.sealed_first_input_sequence
        != candidate.sealed_first_input_sequence
        or revision.sealed_last_input_sequence
        != candidate.sealed_last_input_sequence
        or graph.turn.final_revision_id is not None
    ):
        raise generation.StaleTurnOwnership("handoff candidate is stale or superseded")
    return revision


def _lifecycle_refused(graph: generation._OwnedGraph) -> bool:
    return (
        not graph.agent.ativo
        or not graph.agent.inbox_scheduler_enabled
        or graph.session.handler != "bot"
    )


def _setup_unconfigured(graph: generation._OwnedGraph) -> bool:
    inbox_id = graph.agent.chatwoot_inbox_id
    return not isinstance(inbox_id, int) or isinstance(inbox_id, bool) or inbox_id < 1


def _validate_successor(graph: _HandoffGraph) -> None:
    """Accepted late input lives only in an ungenerated collecting successor."""

    if graph.successor is None:
        if graph.memberships:
            raise scheduler_repo.SchedulerConflictError("late input without successor")
        return
    revision = graph.successor_revision
    if (
        revision is None
        or revision.turn_id != graph.successor.id
        or revision.revision_number != 1
        or revision.status != "collecting"
        or revision.sealed_at is not None
        or graph.successor.status != "pending"
        or any(row.membership_state != "active" for row in graph.memberships)
        or any(row.sessao_id != graph.owned.session.id for row in graph.memberships)
    ):
        raise scheduler_repo.SchedulerConflictError(
            "handoff successor is not an ungenerated collecting turn"
        )


async def _assert_no_unsealed_current_input(
    db: AsyncSession, graph: generation._OwnedGraph
) -> None:
    # ticket04 supersedes before sealing when active input exists, so a final
    # candidate never owns unsealed input. Fail closed if that ever changes.
    if await generation._active_input_count(db, graph.turn.id):
        raise scheduler_repo.SchedulerConflictError(
            "final candidate turn still owns unsealed input"
        )


async def _summary_messages(
    db: AsyncSession,
    *,
    graph: generation._OwnedGraph,
) -> list[dict[str, str]]:
    """Accepted history through the final candidate; successor input excluded."""

    rows = list(
        (
            await db.execute(
                select(Mensagem)
                .outerjoin(
                    TurnInputMembership,
                    TurnInputMembership.mensagem_id == Mensagem.id,
                )
                .outerjoin(
                    ConversationTurn,
                    ConversationTurn.id == TurnInputMembership.turn_id,
                )
                .where(
                    Mensagem.sessao_id == graph.session.id,
                    or_(
                        Mensagem.role == "assistant",
                        TurnInputMembership.id.is_(None),
                        ConversationTurn.session_slot == "terminal",
                        ConversationTurn.id == graph.turn.id,
                    ),
                )
                .order_by(Mensagem.criado_em.desc(), Mensagem.id.desc())
                .limit(SUMMARY_HISTORY_LIMIT)
            )
        ).scalars()
    )
    rows.reverse()
    return [{"role": "system", "content": PROMPT_RESUMO_HANDOFF}] + [
        {"role": row.role, "content": row.conteudo} for row in rows
    ]


async def _scheduled_for_revision(
    db: AsyncSession, revision_id: int
) -> ScheduledHandoff | None:
    return (
        await db.execute(
            select(ScheduledHandoff).where(
                ScheduledHandoff.turn_revision_id == revision_id
            )
        )
    ).scalar_one_or_none()


async def _park_admitted(
    db: AsyncSession,
    graph: _HandoffGraph,
    candidate: generation.FinalCandidate,
) -> HandoffParked | None:
    """A reclaimed admitted handoff turn returns to its blocked waiting state.

    Generation, refresh and summary never run again for it: only ticket 06
    transition admission may act on an admitted handoff revision.
    """

    owned = graph.owned
    revision = owned.current
    if (
        revision.id != candidate.revision_id
        or revision.status != "admitted"
        or owned.turn.final_revision_id != revision.id
    ):
        return None
    scheduled = await _scheduled_for_revision(db, revision.id)
    if scheduled is None or scheduled.conversation_turn_id != owned.turn.id:
        raise scheduler_repo.SchedulerConflictError(
            "admitted revision without scheduled handoff authority"
        )
    _release_to_awaiting_delivery(owned)
    await db.flush()
    logger.warning(
        "Admitted handoff turn reclaimed; parked without generation | agente={} turn={}",
        owned.agent.id,
        owned.turn.id,
    )
    return HandoffParked(handoff_attempt_id=scheduled.handoff_attempt_id)


def _release_to_awaiting_delivery(graph: generation._OwnedGraph) -> None:
    turn = graph.turn
    turn.status = "awaiting_delivery"
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    graph.lane.owner_token = None
    graph.lane.owner_worker_id = None
    graph.lane.lease_until = None
    graph.lane.active_turn_id = None
    graph.lane.active_revision_id = None


async def begin_handoff(
    db: AsyncSession,
    *,
    candidate: generation.FinalCandidate,
    resolve_model,
    setup_ttl_seconds: int,
    automated_ttl_seconds: int,
    release_backoff_seconds: int,
) -> HandoffStep:
    """Decide the summary source under fresh locks; commit a started attempt.

    Fallback summaries (soft limit, quarantine, attempt cap, empty history)
    finalize the handoff in this same transaction without any LLM call.
    """

    graph = await _lock_handoff_graph(db, candidate.lease)
    owned = graph.owned
    parked = await _park_admitted(db, graph, candidate)
    if parked is not None:
        return parked
    _assert_candidate(owned, candidate)
    if _lifecycle_refused(owned) or _setup_unconfigured(owned):
        reason = (
            "lifecycle_refused" if _lifecycle_refused(owned) else "handoff_setup_unconfigured"
        )
        logger.error(
            "Handoff admission refused; lane yielded | agente={} turn={} motivo={}",
            owned.agent.id,
            owned.turn.id,
            reason,
        )
        released = await generation._release_locked(
            db, owned, delay_seconds=release_backoff_seconds, reason=reason
        )
        return HandoffReleased(released.reason)
    _validate_successor(graph)
    if any(row.outcome == "started" for row in graph.attempts):
        raise scheduler_repo.SchedulerConflictError(
            "turn already has an in-flight generation attempt"
        )
    await _assert_no_unsealed_current_input(db, owned)

    fallback_kind: SummaryKind | None = None
    if await consumo_repo.atingiu_limite_mensal(owned.agent.id, db):
        logger.warning(
            "Handoff summary skipped by soft limit | agente={} turn={}",
            owned.agent.id,
            owned.turn.id,
        )
        fallback_kind = "soft_limit_fallback"
    elif await generation._agent_quarantined(db, owned.agent.id):
        logger.warning(
            "Handoff summary skipped by generation quarantine | agente={} turn={}",
            owned.agent.id,
            owned.turn.id,
        )
        fallback_kind = "technical_fallback"
    elif (
        await scheduler_repo.count_generation_attempts(
            db, turn_id=owned.turn.id, attempt_type="summary"
        )
        >= owned.scheduler.max_generation_attempts_per_turn
    ):
        logger.warning(
            "Handoff summary attempt cap reached | agente={} turn={}",
            owned.agent.id,
            owned.turn.id,
        )
        fallback_kind = "technical_fallback"
    if fallback_kind is None:
        messages = await _summary_messages(db, graph=owned)
        if len(messages) == 1:
            fallback_kind = "technical_fallback"
    if fallback_kind is not None:
        return await _finalize_locked(
            db,
            graph=graph,
            candidate=candidate,
            summary=_resumo_fallback(owned.session.id, owned.session.telefone),
            summary_kind=fallback_kind,
            summary_attempt=None,
            setup_ttl_seconds=setup_ttl_seconds,
            automated_ttl_seconds=automated_ttl_seconds,
        )

    modelo = resolve_model(owned.agent)
    attempt = await scheduler_repo.insert_generation_attempt(
        db,
        scheduler=owned.scheduler,
        turn=owned.turn,
        revision=owned.current,
        owner_token=candidate.lease.owner_token,
        attempt_type="summary",
        modelo_llm=modelo,
    )
    await db.flush()
    return SummaryCall(
        attempt_key=attempt.attempt_key,
        modelo_llm=modelo,
        messages=messages,
        accounting_drain_seconds=int(owned.scheduler.accounting_drain_seconds),
    )


async def complete_summary_and_finalize(
    db: AsyncSession,
    *,
    candidate: generation.FinalCandidate,
    attempt_key: str,
    outcome: generation.GenerationOutcome,
    summary_text: str | None,
    setup_ttl_seconds: int,
    automated_ttl_seconds: int,
) -> FinalizedHandoff:
    """Owner path: exact summary accounting and handoff admission, one UoW."""

    graph = await _lock_handoff_graph(db, candidate.lease)
    owned = graph.owned
    revision = _assert_candidate(owned, candidate)
    attempt = next((row for row in graph.attempts if row.attempt_key == attempt_key), None)
    if (
        attempt is None
        or attempt.turn_id != owned.turn.id
        or attempt.revision_id != revision.id
        or attempt.attempt_type != "summary"
        or attempt.lane_owner_token != candidate.lease.owner_token
        or attempt.outcome != "started"
    ):
        raise generation.StaleTurnOwnership("summary attempt is not the owned in-flight attempt")
    if _lifecycle_refused(owned) or _setup_unconfigured(owned):
        # Lifecycle changed during the call: only accounting may happen, and
        # the caller falls back to the accounting-only transaction.
        raise generation.StaleTurnOwnership("handoff lifecycle changed during summary")
    _validate_successor(graph)
    await _assert_no_unsealed_current_input(db, owned)
    if not await generation._account_attempt(db, attempt=attempt, outcome=outcome):
        raise scheduler_repo.SchedulerConflictError("summary accounting was rejected")
    text_value = (summary_text or "").strip()
    if outcome.outcome == "succeeded" and text_value:
        summary, kind = text_value, "generated"
    else:
        summary = _resumo_fallback(owned.session.id, owned.session.telefone)
        kind = "technical_fallback"
    return await _finalize_locked(
        db,
        graph=graph,
        candidate=candidate,
        summary=summary,
        summary_kind=kind,
        summary_attempt=attempt,
        setup_ttl_seconds=setup_ttl_seconds,
        automated_ttl_seconds=automated_ttl_seconds,
    )


async def _late_records(
    db: AsyncSession,
    memberships: list[TurnInputMembership],
    sessao_id: int,
) -> tuple[LateInputRecord, ...]:
    if not memberships:
        return ()
    messages = {
        row.id: row
        for row in (
            await db.execute(
                select(Mensagem).where(
                    Mensagem.id.in_([row.mensagem_id for row in memberships])
                )
            )
        ).scalars()
    }
    ordered = sorted(memberships, key=lambda row: (row.input_sequence, row.id))
    records = []
    for row in ordered:
        message = messages.get(row.mensagem_id)
        if message is None or message.sessao_id != sessao_id or message.role != "user":
            raise scheduler_repo.SchedulerConflictError("late-input message identity is inconsistent")
        records.append(LateInputRecord(row.id, row.input_sequence, message.conteudo))
    return tuple(records)


async def _finalize_locked(
    db: AsyncSession,
    *,
    graph: _HandoffGraph,
    candidate: generation.FinalCandidate,
    summary: str,
    summary_kind: SummaryKind,
    summary_attempt: GenerationAttempt | None,
    setup_ttl_seconds: int,
    automated_ttl_seconds: int,
) -> FinalizedHandoff:
    """Atomic handoff admission. All authority rows are already locked."""

    owned = graph.owned
    revision = _assert_candidate(owned, candidate)
    summary = summary.strip()
    if not summary:
        raise HandoffAdmissionValidationError("handoff summary must not be blank")
    if setup_ttl_seconds < 1:
        raise HandoffAdmissionValidationError("setup_ttl_seconds must be positive")
    if automated_ttl_seconds < 1:
        raise HandoffAdmissionValidationError("automated_ttl_seconds must be positive")
    if (
        summary_kind == "generated"
        and (summary_attempt is None or summary_attempt.outcome != "succeeded")
    ) or (summary_kind == "soft_limit_fallback" and summary_attempt is not None):
        raise scheduler_repo.SchedulerConflictError("summary source is inconsistent")
    inbox_id = int(owned.agent.chatwoot_inbox_id)
    now = owned.now

    records = await _late_records(db, graph.memberships, owned.session.id)
    attempt = await handoff_attempt_repo.create_for_locked_session(
        db,
        sessao=owned.session,
        chatwoot_inbox_id=inbox_id,
    )
    try:
        chunks = plan_context_chunks(attempt.id, records)
    except (ValueError, AssertionError) as exc:
        # Only errors raised by our pure planner (validation and its internal
        # invariant assertions) are promoted. A bare
        # ValueError from a driver or an unrelated dependency must remain an
        # unclassified/retryable admission failure in the service layer.
        raise HandoffAdmissionValidationError(str(exc)) from exc
    # A split record spans consecutive notes; its assignment pins the note
    # holding its first part. Coverage stays exactly once per membership.
    source_by_membership: dict[int, str] = {}
    ordered: list[int] = []
    for chunk in chunks:
        for membership_id in chunk.membership_ids:
            source_by_membership.setdefault(membership_id, chunk.source_id)
            if not ordered or ordered[-1] != membership_id:
                ordered.append(membership_id)
    if ordered != [record.membership_id for record in records]:
        raise HandoffLocalInvariantError("late-input chunk coverage is not exact")

    db.add(
        ScheduledHandoff(
            handoff_attempt_id=attempt.id,
            agente_id=owned.agent.id,
            sessao_id=owned.session.id,
            conversation_turn_id=owned.turn.id,
            turn_revision_id=revision.id,
            summary_generation_attempt_id=(
                summary_attempt.id if summary_attempt is not None else None
            ),
            summary_kind=summary_kind,
            scheduler_epoch=owned.turn.scheduler_epoch,
        )
    )
    for ordinal, record in enumerate(records, start=1):
        db.add(
            HandoffContextAssignment(
                turn_input_membership_id=record.membership_id,
                handoff_attempt_id=attempt.id,
                agente_id=owned.agent.id,
                sessao_id=owned.session.id,
                coverage_ordinal=ordinal,
                source_id=source_by_membership[record.membership_id],
            )
        )
    await db.flush()
    for membership in graph.memberships:
        membership.membership_state = "post_handoff_pending"
        membership.assigned_revision_id = None
    await db.flush()

    open_payload = {
        "phone": owned.session.telefone,
        "summary": summary,
        "inbox_id": inbox_id,
        "handoff_key": attempt.handoff_key,
    }
    decode_chatwoot_handoff("chatwoot.open_handoff.v1", 1, open_payload)
    open_delivery, _ = await outbox_repo.enqueue(
        db,
        cutover_epoch=candidate.lease.delivery_runtime_epoch,
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
        agente_id=owned.agent.id,
        sessao_id=owned.session.id,
        handoff_attempt_id=attempt.id,
        _delivery_class="provider_setup_context",
    )
    context_ids: list[int] = []
    dependency = open_delivery
    for chunk in chunks:
        payload = {
            "attempt_id": attempt.id,
            "handoff_key": attempt.handoff_key,
            "inbox_id": inbox_id,
            "source_id": chunk.source_id,
            "content": chunk.content,
            "body_sha256": chunk.body_sha256,
            "chunk": chunk.ordinal,
            "chunks": chunk.total,
            "coverage": list(chunk.coverage_ids),
        }
        decode_chatwoot_context_note("chatwoot.add_private_note.v1", 1, payload)
        note, _ = await outbox_repo.enqueue(
            db,
            cutover_epoch=candidate.lease.delivery_runtime_epoch,
            dedupe_key=f"chatwoot:context:{chunk.source_id}",
            provider="chatwoot",
            operation="chatwoot.add_private_note.v1",
            payload_version=1,
            payload=payload,
            ordering_key=str(uuid4()),
            stream_sequence=0,
            next_attempt_at=None,
            expires_at=None,
            priority=9,
            agente_id=owned.agent.id,
            sessao_id=owned.session.id,
            handoff_attempt_id=attempt.id,
            depends_on_delivery_id=dependency.id,
            _delivery_class="provider_setup_context",
        )
        context_ids.append(note.id)
        dependency = note

    # Handoff admission reserves both user-visible effects atomically. Ticket 06
    # still owns dispatch admission: these rows cannot be claimed without a
    # live lane/turn/revision fence. Reserving here is required because stream
    # sequences are immutable and the reply must precede the transition.
    revision.status = "admitted"
    revision.admitted_at = now
    owned.turn.final_revision_id = revision.id
    # P2-1: after admission the cap counts only failed sends; recoveries spent
    # during generation (shared column) must not shrink it. The turn is already
    # locked in the canonical prefix of this UoW.
    owned.turn.delivery_attempt_count = 0
    reply_text = (revision.draft_text or "").strip()
    sequence_count = 2 if reply_text else 1
    effect_sequences = await sessao_repo.allocate_delivery_sequences(
        db, owned.session, count=sequence_count
    )
    if reply_text:
        reply_payload = {
            "phone": owned.session.telefone,
            "message": reply_text,
            "instance_name": owned.agent.instance_name,
        }
        decode_evolution_text("evolution.send_text.v1", 1, reply_payload)
        await outbox_repo.enqueue(
            db,
            cutover_epoch=candidate.lease.delivery_runtime_epoch,
            dedupe_key=f"evolution:handoff:{attempt.handoff_key}:reply",
            provider="evolution",
            operation="evolution.send_text.v1",
            payload_version=1,
            payload=reply_payload,
            ordering_key=owned.session.delivery_stream_key,
            stream_sequence=effect_sequences[0],
            next_attempt_at=now,
            expires_at=now + timedelta(seconds=automated_ttl_seconds),
            priority=20,
            agente_id=owned.agent.id,
            sessao_id=owned.session.id,
            handoff_attempt_id=attempt.id,
            _delivery_class="automated",
            _automated_effect="reply",
            _scheduler_epoch=owned.turn.scheduler_epoch,
            _conversation_turn_id=owned.turn.id,
            _turn_revision_id=revision.id,
        )
    transition_payload = {
        "phone": owned.session.telefone,
        "message": MENSAGEM_TRANSICAO,
        "instance_name": owned.agent.instance_name,
    }
    decode_evolution_text("evolution.send_text.v1", 1, transition_payload)
    transition_sequence = effect_sequences[-1]
    # Blocked on the Chatwoot open and never unblocked by open completion or
    # callback: only ticket 06 lane-authorized admission may schedule it.
    transition, _ = await outbox_repo.enqueue(
        db,
        cutover_epoch=candidate.lease.delivery_runtime_epoch,
        dedupe_key=f"evolution:handoff:{attempt.handoff_key}:transition",
        provider="evolution",
        operation="evolution.send_text.v1",
        payload_version=1,
        payload=transition_payload,
        ordering_key=owned.session.delivery_stream_key,
        stream_sequence=transition_sequence,
        next_attempt_at=None,
        expires_at=None,
        priority=20,
        agente_id=owned.agent.id,
        sessao_id=owned.session.id,
        handoff_attempt_id=attempt.id,
        depends_on_delivery_id=open_delivery.id,
        _delivery_class="automated",
        _automated_effect="handoff_transition",
        _scheduler_epoch=owned.turn.scheduler_epoch,
        _conversation_turn_id=owned.turn.id,
        _turn_revision_id=revision.id,
    )

    await sessao_repo.encerrar_locked(db, owned.session, motivo="handoff")
    if graph.successor is not None:
        graph.successor.status = "cancelled"
        graph.successor.session_slot = "terminal"
        graph.successor.terminal_reason = "post_handoff_context"
        graph.successor.terminal_at = now
        if graph.successor_revision is None:
            raise HandoffLocalInvariantError("successor revision disappeared")
        graph.successor_revision.status = "cancelled"
    _release_to_awaiting_delivery(owned)
    await db.flush()
    logger.info(
        "Scheduled handoff admitted | agente={} turn={} attempt={} contexto={}",
        owned.agent.id,
        owned.turn.id,
        attempt.id,
        len(records),
    )
    return FinalizedHandoff(
        handoff_attempt_id=attempt.id,
        summary_kind=summary_kind,
        open_delivery_id=open_delivery.id,
        transition_delivery_id=transition.id,
        context_delivery_ids=tuple(context_ids),
        covered_membership_ids=tuple(record.membership_id for record in records),
    )


@dataclass(frozen=True)
class HandoffFailed:
    """Terminal, operator-visible admission failure; nothing repays the LLM."""

    reason: str


async def fail_handoff_terminally(
    db: AsyncSession,
    *,
    candidate: generation.FinalCandidate,
    attempt_key: str | None,
    outcome: generation.GenerationOutcome | None,
    reason: str,
) -> HandoffFailed:
    """Close a handoff that failed deterministically, under owner locks.

    Used only when a local, deterministic computation of admission failed (a
    retry would fail the same way and repay the summary). The exact summary
    attempt is accounted, the candidate revision and turn become ``dead``, the
    successor is cancelled (never orphaned behind a dead active turn) and the
    session closes as ``falha_tecnica``, so later inbound keeps closed-session
    behavior. Successor memberships stay uncovered and visible on the
    cancelled turn for the operator. Same canonical prefix as admission.
    """

    graph = await _lock_handoff_graph(db, candidate.lease)
    owned = graph.owned
    revision = _assert_candidate(owned, candidate)
    if attempt_key is not None:
        attempt = next(
            (row for row in graph.attempts if row.attempt_key == attempt_key), None
        )
        if attempt is None or attempt.revision_id != revision.id:
            raise generation.StaleTurnOwnership("summary attempt is not owned")
        if attempt.outcome == "started":
            if outcome is None or not await generation._account_attempt(
                db, attempt=attempt, outcome=outcome
            ):
                raise scheduler_repo.SchedulerConflictError(
                    "summary accounting was rejected"
                )
    now = owned.now
    turn = owned.turn
    await handoff_terminal_repo.terminalize_failed_handoff_locked(
        db,
        session=owned.session,
        lane=owned.lane,
        turn=turn,
        revision=revision,
        successor=graph.successor,
        successor_revision=graph.successor_revision,
        now=now,
        terminal_reason="handoff_admission_failed",
    )
    logger.error(
        "Handoff admission failed terminally; operator action required | "
        "agente={} turn={} motivo={} contexto_pendente={}",
        owned.agent.id,
        turn.id,
        reason,
        len(graph.memberships),
    )
    return HandoffFailed(reason=reason)
