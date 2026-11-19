"""Fenced generation, one-refresh and local typing transactions (ticket 04).

Every function here is the body of ONE short transaction; callers wrap it in
``scheduler_repo.run_transaction_with_deadlock_retry``. No function performs
external I/O or holds a lock across it.

Owner paths lock the canonical prefix with locking reads only:
``delivery_runtime_state -> scheduler_runtime_state -> agentes ->
agent_service_lanes -> sessoes -> conversation_turns -> turn_revisions ->
generation_attempts -> consumo_llm / mensagens``. The session id comes from the
immutable claimed turn identity, so no plain read opens the REPEATABLE READ
view before a lock wait; the first consistent read (caps, soft limit, history)
happens after every lifecycle lock is held.

The accounting-only path for a stale task takes ``agentes`` FOR SHARE ->
``sessoes`` FOR SHARE -> its exact ``generation_attempts`` FOR UPDATE ->
``consumo_llm`` (insert/lock). The share locks are the parent rows the
consumption FKs would otherwise lock after the attempt; taking them first keeps
the canonical order against recovery (agent X ... attempt). It never requests
runtime, scheduler, lane, turn or revision locks and never upgrades S to X.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

from loguru import logger
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    AgentServiceLane,
    Agente,
    ConversationTurn,
    GenerationAttempt,
    HandoffAttempt,
    Mensagem,
    ScheduledHandoff,
    SchedulerRuntimeState,
    Sessao,
    TurnInputMembership,
    TurnRevision,
)
from src.database.repositories import (
    consumo_repo,
    mensagem_repo,
    scheduler_repo,
    sessao_repo,
)


DraftKind = Literal["generated", "soft_limit_fallback", "technical_fallback"]
GenerationOutcomeKind = Literal["succeeded", "failed", "outcome_unknown"]

CANDIDATE_REVISION_STATUSES = ("final_candidate", "admitted")


class StaleTurnOwnership(scheduler_repo.SchedulerConflictError):
    """The caller no longer holds exact lane/turn authority."""


@dataclass(frozen=True)
class TurnLease:
    turn_id: int
    agente_id: int
    sessao_id: int
    owner_token: str
    scheduler_epoch: str
    delivery_runtime_epoch: str

    @classmethod
    def from_claim(cls, claim: scheduler_repo.ClaimedTurn) -> "TurnLease":
        return cls(
            turn_id=claim.turn_id,
            agente_id=claim.agente_id,
            sessao_id=claim.sessao_id,
            owner_token=claim.claim_token,
            scheduler_epoch=claim.scheduler_epoch,
            delivery_runtime_epoch=claim.delivery_runtime_epoch,
        )


@dataclass(frozen=True)
class DraftInput:
    kind: DraftKind
    text: str
    needs_handoff: bool


@dataclass(frozen=True)
class GenerationUsage:
    modelo_llm: str
    tokens_input: int
    tokens_output: int
    tokens_reasoning: int | None
    tokens_cache_read: int | None
    tokens_cache_write: int | None
    custo_creditos: Decimal
    provider_generation_id: str | None
    modelo_llm_reportado: str | None = None


@dataclass(frozen=True)
class GenerationOutcome:
    outcome: GenerationOutcomeKind
    usage: GenerationUsage | None = None
    safe_error_code: str | None = None

    def __post_init__(self) -> None:
        if (self.outcome == "succeeded") != (self.usage is not None):
            raise ValueError("only a succeeded generation carries usage")
        if self.outcome != "succeeded" and not self.safe_error_code:
            raise ValueError("unsuccessful generation requires a safe error code")


@dataclass(frozen=True)
class TurnPolicy:
    """Business inputs supplied by the service layer."""

    lease_seconds: int
    release_backoff_seconds: int
    soft_limit_text: str
    technical_fallback_text: str
    resolve_model: Callable[[Agente], str]
    # Returns (typing_delay_ms, typing_jitter_ms) for a draft text. Called once
    # per draft; the result is persisted and never recomputed.
    typing_schedule: Callable[[str], tuple[int, int]]


@dataclass(frozen=True)
class WaitStep:
    phase: Literal["collecting", "typing"]
    revision_id: int
    until: datetime
    db_now: datetime


@dataclass(frozen=True)
class GenerateStep:
    revision_id: int
    revision_number: int
    attempt_key: str
    modelo_llm: str
    system_prompt: str
    history: tuple[tuple[str, str], ...]
    batch: tuple[tuple[int, str], ...]
    accounting_drain_seconds: int


@dataclass(frozen=True)
class FinalCandidate:
    lease: TurnLease
    revision_id: int
    revision_key: str
    revision_number: int
    revision_status: str
    sealed_first_input_sequence: int
    sealed_last_input_sequence: int
    draft_kind: str
    needs_handoff: bool
    typing_deadline_at: datetime


@dataclass(frozen=True)
class ReleasedStep:
    reason: str


TurnStep = WaitStep | GenerateStep | FinalCandidate | ReleasedStep


@dataclass(frozen=True)
class TurnPeek:
    claim_token: str | None
    current_revision_id: int | None
    ready_at: datetime
    active_inputs: int
    db_now: datetime


@dataclass
class _OwnedGraph:
    scheduler: SchedulerRuntimeState
    agent: Agente
    lane: AgentServiceLane
    session: Sessao
    turn: ConversationTurn
    revisions: dict[int, TurnRevision]
    now: datetime

    @property
    def current(self) -> TurnRevision:
        return self.revisions[self.turn.current_revision_id]


async def _db_now(db: AsyncSession) -> datetime:
    # No table access: does not open the InnoDB read view.
    return (await db.execute(select(func.utc_timestamp(6)))).scalar_one()


async def _lock_owned_graph(
    db: AsyncSession, lease: TurnLease, *, lock_successor: bool = False
) -> _OwnedGraph:
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
        raise StaleTurnOwnership("scheduler authority changed") from exc
    if scheduler.state not in {"enabled", "draining"}:
        raise StaleTurnOwnership("scheduler cannot continue work in current state")
    session = (
        await db.execute(
            select(Sessao)
            .where(Sessao.id == lease.sessao_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    turn = (
        await db.execute(
            select(ConversationTurn)
            .where(ConversationTurn.id == lease.turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if lock_successor and session is not None and turn is not None:
        await scheduler_repo.lock_pending_successor(db, sessao_id=session.id)
    revisions = {
        row.id: row
        for row in (
            await db.execute(
                select(TurnRevision)
                .where(TurnRevision.turn_id == lease.turn_id)
                .order_by(TurnRevision.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    }
    now = await _db_now(db)
    if (
        session is None
        or turn is None
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
        raise StaleTurnOwnership("stale turn ownership")
    return _OwnedGraph(
        scheduler=scheduler,
        agent=agent,
        lane=lane,
        session=session,
        turn=turn,
        revisions=revisions,
        now=now,
    )


def _renew(graph: _OwnedGraph, lease_seconds: int) -> None:
    lease_until = graph.now + timedelta(seconds=lease_seconds)
    graph.lane.lease_until = lease_until
    graph.turn.claim_until = lease_until


async def _active_input_count(db: AsyncSession, turn_id: int) -> int:
    return int(
        (
            await db.execute(
                select(func.count(TurnInputMembership.id)).where(
                    TurnInputMembership.turn_id == turn_id,
                    TurnInputMembership.membership_state == "active",
                )
            )
        ).scalar_one()
        or 0
    )


async def _agent_quarantined(db: AsyncSession, agente_id: int) -> bool:
    return bool(
        (
            await db.execute(
                select(func.count(GenerationAttempt.id)).where(
                    GenerationAttempt.agente_id == agente_id,
                    GenerationAttempt.accounting_state == "reconciliation_required",
                    GenerationAttempt.reconciled_at.is_(None),
                )
            )
        ).scalar_one()
    )


async def _release_locked(
    db: AsyncSession,
    graph: _OwnedGraph,
    *,
    delay_seconds: int,
    reason: str,
) -> ReleasedStep:
    """Yield the lane without touching revision state, FIFO key or counters."""

    if delay_seconds < 1:
        raise ValueError("release delay must be positive")
    resume_at = graph.now + timedelta(seconds=delay_seconds)
    turn = graph.turn
    lane = graph.lane
    turn.status = "retry"
    turn.retry_at = resume_at
    turn.ready_at = resume_at
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    await db.flush()
    return ReleasedStep(reason=reason)


_lifecycle_cancel_count = 0


def lifecycle_cancel_count() -> int:
    return _lifecycle_cancel_count


async def _cancel_for_lifecycle_locked(db: AsyncSession, graph: _OwnedGraph) -> ReleasedStep:
    """Formally cancel the owned turn and its successor (ticket07).

    The owner already holds runtime -> scheduler -> agent -> lane -> session ->
    turn (+ successor) -> revisions; the shared lifecycle graph re-locks those
    and continues with attempts -> scheduled handoffs -> handoff attempts ->
    automated effects in canonical order. An admitted turn with an effect still
    in flight waits ``awaiting_delivery`` (its finalization settles it).
    """

    global _lifecycle_cancel_count
    from src.database.repositories import lifecycle_repo

    if not graph.agent.ativo:
        reason = "agent_disabled"
    elif not graph.agent.inbox_scheduler_enabled:
        reason = "agent_opted_out"
    else:
        reason = "session_closed"
    locked = await lifecycle_repo.lock_scheduled_graph(db, session_ids=[graph.session.id])
    outcome = await lifecycle_repo.cancel_scheduled_turns_locked(
        db,
        graph=locked,
        lane=graph.lane,
        reason=reason,
        handoff_reason="handoff_superseded",
        now=graph.now,
    )
    _lifecycle_cancel_count += 1
    logger.warning(
        "Owned turn cancelled by lifecycle fence | agente={} turn={} reason={} "
        "waiting={} count={} threshold=1",
        graph.agent.id,
        graph.turn.id,
        reason,
        len(outcome.waiting_turns),
        _lifecycle_cancel_count,
    )
    return ReleasedStep(reason="lifecycle_cancelled")


async def _supersede(
    db: AsyncSession,
    graph: _OwnedGraph,
    revision: TurnRevision,
    *,
    reason: str,
) -> TurnRevision:
    """Spend the sole refresh: replace revision 1 by a collecting revision 2."""

    replacement = await scheduler_repo.supersede_revision_for_refresh_locked(
        db,
        lane=graph.lane,
        turn=graph.turn,
        revision=revision,
        reason=reason,
    )
    graph.revisions[replacement.id] = replacement
    return replacement


def _store_draft(graph: _OwnedGraph, revision: TurnRevision, draft: DraftInput) -> None:
    revision.generation_completed_at = graph.now
    revision.draft_kind = draft.kind
    revision.draft_text = draft.text
    revision.draft_text_bytes = len(draft.text.encode("utf-8"))
    revision.needs_handoff = draft.needs_handoff


def _schedule_typing(
    graph: _OwnedGraph,
    revision: TurnRevision,
    policy: TurnPolicy,
) -> WaitStep:
    delay_ms, jitter_ms = policy.typing_schedule(revision.draft_text or "")
    if not 2000 <= delay_ms <= 12000 or not -1000 <= jitter_ms <= 1000:
        raise ValueError("typing schedule is outside the persisted bounds")
    revision.typing_delay_ms = delay_ms
    revision.typing_jitter_ms = jitter_ms
    revision.typing_deadline_at = revision.generation_completed_at + timedelta(
        milliseconds=delay_ms
    )
    revision.status = "typing"
    graph.turn.status = "typing"
    return WaitStep(
        phase="typing",
        revision_id=revision.id,
        until=revision.typing_deadline_at,
        db_now=graph.now,
    )


async def _load_context(
    db: AsyncSession,
    graph: _OwnedGraph,
    revision: TurnRevision,
) -> tuple[tuple[tuple[str, str], ...], tuple[tuple[int, str], ...]]:
    batch = tuple(
        (
            await db.execute(
                select(Mensagem)
                .join(
                    TurnInputMembership,
                    TurnInputMembership.mensagem_id == Mensagem.id,
                )
                .where(
                    TurnInputMembership.turn_id == graph.turn.id,
                    TurnInputMembership.input_sequence
                    >= revision.sealed_first_input_sequence,
                    TurnInputMembership.input_sequence
                    <= revision.sealed_last_input_sequence,
                )
                .order_by(TurnInputMembership.input_sequence)
            )
        ).scalars()
    )
    if len(batch) != revision.sealed_input_count or any(
        row.role != "user" or row.sessao_id != graph.session.id for row in batch
    ):
        raise scheduler_repo.SchedulerConflictError("sealed batch identity is inconsistent")
    # Accepted history only: assistant replies, legacy inputs without
    # membership, and inputs of terminal turns. Pending/active inputs of this
    # or a successor turn never leak in; the sealed batch is appended once.
    history = list(
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
                    ),
                )
                .order_by(Mensagem.criado_em.desc(), Mensagem.id.desc())
                .limit(max(graph.agent.max_historico or 0, 0))
            )
        ).scalars()
    )
    history.reverse()
    return (
        tuple((row.role, row.conteudo) for row in history),
        tuple((row.id, row.conteudo) for row in batch),
    )


async def _seal_final_candidate(
    db: AsyncSession,
    graph: _OwnedGraph,
    revision: TurnRevision,
) -> None:
    """Freeze the post-typing candidate and record the accepted reply once.

    This is not send admission. The accepted generated reply enters assistant
    history here (as the legacy flow does before staging delivery), and the
    accepted-turn counter increments exactly once per turn. Fallback texts
    never become history or count as an accepted reply.
    """

    revision.status = "final_candidate"
    if revision.draft_kind == "generated":
        if revision.draft_text:
            await mensagem_repo.salvar(
                db,
                graph.session.id,
                "assistant",
                revision.draft_text,
            )
        await sessao_repo.incrementar_mensagens(db, graph.session.id)
    await db.flush()


def _final_candidate(lease: TurnLease, revision: TurnRevision) -> FinalCandidate:
    return FinalCandidate(
        lease=lease,
        revision_id=revision.id,
        revision_key=revision.revision_key,
        revision_number=revision.revision_number,
        revision_status=revision.status,
        sealed_first_input_sequence=revision.sealed_first_input_sequence,
        sealed_last_input_sequence=revision.sealed_last_input_sequence,
        draft_kind=revision.draft_kind,
        needs_handoff=bool(revision.needs_handoff),
        typing_deadline_at=revision.typing_deadline_at,
    )


async def _admitted_handoff_delivery(db: AsyncSession, graph: _OwnedGraph) -> bool:
    """True only for the current-generation admitted handoff of this session.

    Locks follow the canonical prefix: the owned graph already holds
    agent → lane → session → turn → revisions; the scheduled handoff and its
    attempt are locked after them (``handoff_attempts`` precedes deliveries).
    """

    revision = graph.current
    if (
        revision.status != "admitted"
        or graph.turn.final_revision_id != revision.id
        or graph.turn.status != "claimed"
        or not graph.agent.ativo
        or not graph.agent.inbox_scheduler_enabled
        or graph.session.handler != "encerrada"
        or graph.session.motivo_encerramento != "handoff"
    ):
        return False
    scheduled = (
        await db.execute(
            select(ScheduledHandoff)
            .where(
                ScheduledHandoff.conversation_turn_id == graph.turn.id,
                ScheduledHandoff.turn_revision_id == revision.id,
                ScheduledHandoff.scheduler_epoch == graph.turn.scheduler_epoch,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if scheduled is None:
        return False
    attempt = (
        await db.execute(
            select(HandoffAttempt)
            .where(HandoffAttempt.id == scheduled.handoff_attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return bool(
        attempt is not None
        and attempt.sessao_id == graph.session.id
        and attempt.generation == graph.session.handoff_generation
        and attempt.status in {"pending", "ready"}
    )


async def advance_turn(
    db: AsyncSession,
    *,
    lease: TurnLease,
    policy: TurnPolicy,
) -> TurnStep:
    """Advance one owned turn from fresh locked state; restart-safe."""

    # The successor is pre-locked (turns before revisions) so a lifecycle
    # cancellation can include it without an inverse lock edge (ticket07).
    graph = await _lock_owned_graph(db, lease, lock_successor=True)
    _renew(graph, policy.lease_seconds)
    if await _admitted_handoff_delivery(db, graph):
        # The session was closed by this exact admitted handoff; delivering
        # its staged reply/transition is the admitted effect, not new work.
        await db.flush()
        return _final_candidate(lease, graph.current)
    if (
        not graph.agent.ativo
        or not graph.agent.inbox_scheduler_enabled
        or graph.session.handler != "bot"
    ):
        # Ticket07 formal cancellation: no provider call, history write or
        # seal, and no release/backoff loop on the lifecycle fence.
        return await _cancel_for_lifecycle_locked(db, graph)
    for _ in range(4):
        revision = graph.current
        if revision.status == "collecting":
            if graph.turn.ready_at > graph.now:
                await db.flush()
                return WaitStep(
                    phase="collecting",
                    revision_id=revision.id,
                    until=graph.turn.ready_at,
                    db_now=graph.now,
                )
            await scheduler_repo.seal_locked_revision(
                db,
                lane=graph.lane,
                turn=graph.turn,
                revision=revision,
            )
            continue
        if revision.status == "generating":
            if revision.revision_number == 1 and await _active_input_count(
                db, graph.turn.id
            ):
                await _supersede(
                    db, graph, revision, reason="refresh_input_before_generation"
                )
                continue
            started = int(
                (
                    await db.execute(
                        select(func.count(GenerationAttempt.id)).where(
                            GenerationAttempt.turn_id == graph.turn.id,
                            GenerationAttempt.outcome == "started",
                        )
                    )
                ).scalar_one()
                or 0
            )
            if started:
                raise scheduler_repo.SchedulerConflictError(
                    "turn already has an in-flight generation attempt"
                )
            if await _agent_quarantined(db, graph.agent.id):
                return await _release_locked(
                    db,
                    graph,
                    delay_seconds=policy.release_backoff_seconds,
                    reason="generation_quarantined",
                )
            if await consumo_repo.atingiu_limite_mensal(graph.agent.id, db):
                logger.warning(
                    "Turn generation skipped by soft limit | agente={} turn={}",
                    graph.agent.id,
                    graph.turn.id,
                )
                _store_draft(
                    graph,
                    revision,
                    DraftInput("soft_limit_fallback", policy.soft_limit_text, False),
                )
                step = _schedule_typing(graph, revision, policy)
                await db.flush()
                return step
            attempts = await scheduler_repo.count_generation_attempts(
                db, turn_id=graph.turn.id, attempt_type="response"
            )
            if attempts >= graph.scheduler.max_generation_attempts_per_turn:
                _store_draft(
                    graph,
                    revision,
                    DraftInput("technical_fallback", policy.technical_fallback_text, False),
                )
                step = _schedule_typing(graph, revision, policy)
                await db.flush()
                return step
            history, batch = await _load_context(db, graph, revision)
            modelo_llm = policy.resolve_model(graph.agent)
            # A reclaimed turn resumes as "claimed"; restore the live phase.
            graph.turn.status = "generating"
            attempt = await scheduler_repo.insert_generation_attempt(
                db,
                scheduler=graph.scheduler,
                turn=graph.turn,
                revision=revision,
                owner_token=lease.owner_token,
                attempt_type="response",
                modelo_llm=modelo_llm,
            )
            await db.flush()
            return GenerateStep(
                revision_id=revision.id,
                revision_number=revision.revision_number,
                attempt_key=attempt.attempt_key,
                modelo_llm=modelo_llm,
                system_prompt=graph.agent.system_prompt,
                history=history,
                batch=batch,
                accounting_drain_seconds=int(graph.scheduler.accounting_drain_seconds),
            )
        if revision.status == "typing":
            if revision.revision_number == 1 and await _active_input_count(
                db, graph.turn.id
            ):
                await _supersede(
                    db, graph, revision, reason="refresh_input_during_typing"
                )
                continue
            graph.turn.status = "typing"
            if graph.now >= revision.typing_deadline_at:
                await _seal_final_candidate(db, graph, revision)
                return _final_candidate(lease, revision)
            await db.flush()
            return WaitStep(
                phase="typing",
                revision_id=revision.id,
                until=revision.typing_deadline_at,
                db_now=graph.now,
            )
        if revision.status in CANDIDATE_REVISION_STATUSES:
            if revision.status == "final_candidate":
                graph.turn.status = "typing"
            await db.flush()
            return _final_candidate(lease, revision)
        raise scheduler_repo.SchedulerConflictError(
            f"unsupported revision state {revision.status}"
        )
    raise scheduler_repo.SchedulerConflictError("turn did not converge")


async def _lock_attempt(db: AsyncSession, attempt_key: str) -> GenerationAttempt | None:
    return (
        await db.execute(
            select(GenerationAttempt)
            .where(GenerationAttempt.attempt_key == attempt_key)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _account_attempt(
    db: AsyncSession,
    *,
    attempt: GenerationAttempt,
    outcome: GenerationOutcome,
) -> bool:
    """Write exact attempt accounting; usage of a success is always kept."""

    consumo_id: int | None = None
    usage = outcome.usage
    if usage is not None:
        persisted = await consumo_repo.registrar_ou_reconciliar(
            db,
            agente_id=attempt.agente_id,
            sessao_id=attempt.sessao_id,
            # Requested allowlisted id from the immutable attempt: billing and
            # the exact link key. The provider echo is stored separately.
            modelo_llm=attempt.modelo_llm,
            modelo_llm_reportado=usage.modelo_llm_reportado,
            tipo_chamada="resposta" if attempt.attempt_type == "response" else "resumo",
            tokens_input=usage.tokens_input,
            tokens_output=usage.tokens_output,
            tokens_reasoning=usage.tokens_reasoning,
            tokens_cache_read=usage.tokens_cache_read,
            tokens_cache_write=usage.tokens_cache_write,
            custo_creditos=usage.custo_creditos,
            provider_generation_id=usage.provider_generation_id,
        )
        if (
            persisted.consumo is not None
            and persisted.consumo.modelo_llm == attempt.modelo_llm
        ):
            consumo_id = persisted.consumo.id
        else:
            # Durable usage exists (consumption or reconciliation row) but no
            # exact commercial link: the attempt stays quarantined.
            logger.error(
                "Generation usage requires reconciliation | agente={} attempt={}",
                attempt.agente_id,
                attempt.id,
            )
    recorded = await scheduler_repo.record_generation_outcome(
        db,
        attempt_key=attempt.attempt_key,
        lane_owner_token=attempt.lane_owner_token,
        scheduler_epoch=attempt.scheduler_epoch,
        outcome=outcome.outcome,
        provider_generation_id=usage.provider_generation_id if usage else None,
        tokens_input=usage.tokens_input if usage else None,
        tokens_output=usage.tokens_output if usage else None,
        tokens_reasoning=usage.tokens_reasoning if usage else None,
        tokens_cache_read=usage.tokens_cache_read if usage else None,
        tokens_cache_write=usage.tokens_cache_write if usage else None,
        custo_creditos=usage.custo_creditos if usage else None,
        consumo_llm_id=consumo_id,
        safe_error_code=outcome.safe_error_code,
    )
    return recorded


async def record_generation_result(
    db: AsyncSession,
    *,
    lease: TurnLease,
    attempt_key: str,
    outcome: GenerationOutcome,
    draft: DraftInput | None,
    policy: TurnPolicy,
) -> TurnStep:
    """Owner path: exact accounting plus draft acceptance in one transaction."""

    if (outcome.outcome == "outcome_unknown") != (draft is None):
        raise ValueError("only an unknown outcome has no draft")
    graph = await _lock_owned_graph(db, lease)
    attempt = await _lock_attempt(db, attempt_key)
    revision = graph.current
    if (
        attempt is None
        or attempt.turn_id != graph.turn.id
        or attempt.revision_id != revision.id
        or attempt.lane_owner_token != lease.owner_token
        or attempt.outcome != "started"
        or revision.status != "generating"
    ):
        raise StaleTurnOwnership("generation attempt is not the owned in-flight attempt")
    if not await _account_attempt(db, attempt=attempt, outcome=outcome):
        raise scheduler_repo.SchedulerConflictError("generation accounting was rejected")
    _renew(graph, policy.lease_seconds)
    if draft is None:
        logger.error(
            "Generation outcome unknown; agent quarantined | agente={} attempt={}",
            graph.agent.id,
            attempt.id,
        )
        return await _release_locked(
            db,
            graph,
            delay_seconds=policy.release_backoff_seconds,
            reason="generation_outcome_unknown",
        )
    _store_draft(graph, revision, draft)
    if revision.revision_number == 1 and await _active_input_count(db, graph.turn.id):
        # Outcome and usage are already captured; the draft never becomes
        # history and the turn spends its only refresh.
        replacement = await _supersede(
            db, graph, revision, reason="refresh_input_during_generation"
        )
        await db.flush()
        return WaitStep(
            phase="collecting",
            revision_id=replacement.id,
            until=graph.turn.ready_at,
            db_now=graph.now,
        )
    step = _schedule_typing(graph, revision, policy)
    await db.flush()
    return step


async def _share_lock_accounting_parents(
    db: AsyncSession,
    *,
    agente_id: int,
    sessao_id: int,
) -> bool:
    """Canonical-order S locks on the parents consumption FKs will check."""

    agent = (
        await db.execute(
            select(Agente.id).where(Agente.id == agente_id).with_for_update(read=True)
        )
    ).scalar_one_or_none()
    session = (
        await db.execute(
            select(Sessao.id).where(Sessao.id == sessao_id).with_for_update(read=True)
        )
    ).scalar_one_or_none()
    return agent is not None and session is not None


async def record_generation_accounting_only(
    db: AsyncSession,
    *,
    agente_id: int,
    sessao_id: int,
    attempt_key: str,
    lane_owner_token: str,
    outcome: GenerationOutcome,
) -> bool:
    """Stale-task path: finalize only its exact attempt; no domain effect.

    ``agente_id``/``sessao_id`` come from the immutable claimed turn identity
    and are re-checked against the locked attempt.
    """

    if not await _share_lock_accounting_parents(
        db, agente_id=agente_id, sessao_id=sessao_id
    ):
        return False
    attempt = await _lock_attempt(db, attempt_key)
    if (
        attempt is None
        or attempt.lane_owner_token != lane_owner_token
        or attempt.agente_id != agente_id
        or attempt.sessao_id != sessao_id
    ):
        return False
    recorded = await _account_attempt(db, attempt=attempt, outcome=outcome)
    if not recorded:
        logger.error(
            "Late generation accounting rejected; usage kept for reconciliation "
            "| agente={} attempt={}",
            attempt.agente_id,
            attempt.id,
        )
    return recorded


async def release_turn(
    db: AsyncSession,
    *,
    lease: TurnLease,
    delay_seconds: int,
    reason: str,
) -> ReleasedStep | None:
    """Voluntarily yield an owned turn (shutdown); state resumes on reclaim."""

    try:
        graph = await _lock_owned_graph(db, lease)
    except StaleTurnOwnership:
        return None
    return await _release_locked(db, graph, delay_seconds=delay_seconds, reason=reason)


async def peek_turn(db: AsyncSession, *, turn_id: int) -> TurnPeek | None:
    """Unlocked hint in its own transaction; never an authority decision."""

    row = (
        await db.execute(
            select(
                ConversationTurn.claim_token,
                ConversationTurn.current_revision_id,
                ConversationTurn.ready_at,
            ).where(ConversationTurn.id == turn_id)
        )
    ).one_or_none()
    if row is None:
        return None
    return TurnPeek(
        claim_token=row.claim_token,
        current_revision_id=row.current_revision_id,
        ready_at=row.ready_at,
        active_inputs=await _active_input_count(db, turn_id),
        db_now=await _db_now(db),
    )


async def list_ready_agent_ids(
    db: AsyncSession,
    *,
    scheduler_epoch: str,
    limit: int,
) -> list[int]:
    """Discovery hint only; claim_ready_turn re-decides under locks."""

    now = func.utc_timestamp(6)
    rows = (
        await db.execute(
            select(ConversationTurn.agente_id)
            .join(
                AgentServiceLane,
                AgentServiceLane.agente_id == ConversationTurn.agente_id,
            )
            .where(
                AgentServiceLane.owner_token.is_(None),
                ~select(GenerationAttempt.id)
                .where(
                    GenerationAttempt.agente_id == ConversationTurn.agente_id,
                    GenerationAttempt.accounting_state == "reconciliation_required",
                    GenerationAttempt.reconciled_at.is_(None),
                )
                .exists(),
                ConversationTurn.scheduler_epoch == scheduler_epoch,
                ConversationTurn.session_slot == "active",
                ConversationTurn.status.in_(("pending", "retry")),
                ConversationTurn.ready_at <= now,
                or_(
                    ConversationTurn.retry_at.is_(None),
                    ConversationTurn.retry_at <= now,
                ),
            )
            .group_by(ConversationTurn.agente_id)
            .order_by(func.min(ConversationTurn.ready_at), ConversationTurn.agente_id)
            .limit(limit)
        )
    ).scalars()
    return list(rows)


async def list_expired_lane_agent_ids(db: AsyncSession, *, limit: int) -> list[int]:
    """Expired lanes that recovery can act on now (discovery hint only).

    A lane whose expired owner left an unresolved reconciliation quarantine,
    or a started attempt still inside its accounting drain, stays owned on
    purpose (``recover_expired_lane`` returns ``quarantined``). Such lanes are
    skipped here so they neither occupy the bounded recovery window nor take
    the scheduler mutex every pass; the quarantine itself stays durable on the
    lane and attempt rows. A drain becomes eligible again once it elapses.
    """

    now = func.utc_timestamp(6)
    held_by_quarantine = (
        select(GenerationAttempt.id)
        .where(
            GenerationAttempt.turn_id == AgentServiceLane.active_turn_id,
            GenerationAttempt.lane_owner_token == AgentServiceLane.owner_token,
            or_(
                (GenerationAttempt.accounting_state == "reconciliation_required")
                & GenerationAttempt.reconciled_at.is_(None),
                (GenerationAttempt.outcome == "started")
                & (GenerationAttempt.accounting_drain_until > now),
            ),
        )
        .exists()
    )
    rows = (
        await db.execute(
            select(AgentServiceLane.agente_id)
            .where(
                AgentServiceLane.owner_token.is_not(None),
                AgentServiceLane.lease_until <= now,
                ~held_by_quarantine,
            )
            .order_by(AgentServiceLane.lease_until, AgentServiceLane.agente_id)
            .limit(limit)
        )
    ).scalars()
    return list(rows)


async def list_expired_lane_hints(
    db: AsyncSession, *, limit: int
) -> list[tuple[int, int, int]]:
    """``(agente_id, turn_id, sessao_id)`` of recoverable expired lanes.

    Same predicate as ``list_expired_lane_agent_ids``; the identities feed
    ``scheduler_repo.recover_expired_lane`` so its lock transaction needs no
    plain read (§16.1 A1). A consistent read in its own transaction, used
    only as a hint re-validated under the lane lock.
    """

    now = func.utc_timestamp(6)
    held_by_quarantine = (
        select(GenerationAttempt.id)
        .where(
            GenerationAttempt.turn_id == AgentServiceLane.active_turn_id,
            GenerationAttempt.lane_owner_token == AgentServiceLane.owner_token,
            or_(
                (GenerationAttempt.accounting_state == "reconciliation_required")
                & GenerationAttempt.reconciled_at.is_(None),
                (GenerationAttempt.outcome == "started")
                & (GenerationAttempt.accounting_drain_until > now),
            ),
        )
        .exists()
    )
    rows = await db.execute(
        select(
            AgentServiceLane.agente_id,
            AgentServiceLane.active_turn_id,
            ConversationTurn.sessao_id,
        )
        .join(ConversationTurn, ConversationTurn.id == AgentServiceLane.active_turn_id)
        .where(
            AgentServiceLane.owner_token.is_not(None),
            AgentServiceLane.lease_until <= now,
            ~held_by_quarantine,
        )
        .order_by(AgentServiceLane.lease_until, AgentServiceLane.agente_id)
        .limit(limit)
    )
    return [(int(r.agente_id), int(r.active_turn_id), int(r.sessao_id)) for r in rows]


async def ready_turn_hint(
    db: AsyncSession, *, agente_id: int, scheduler_epoch: str
) -> tuple[int, int] | None:
    """``(turn_id, sessao_id)`` of the agent's head ready turn (hint only)."""

    now = func.utc_timestamp(6)
    row = (
        await db.execute(
            select(ConversationTurn.id, ConversationTurn.sessao_id)
            .where(
                ConversationTurn.agente_id == agente_id,
                ConversationTurn.scheduler_epoch == scheduler_epoch,
                ConversationTurn.session_slot == "active",
                ConversationTurn.status.in_(("pending", "retry")),
                ConversationTurn.ready_at <= now,
                or_(ConversationTurn.retry_at.is_(None), ConversationTurn.retry_at <= now),
            )
            .order_by(ConversationTurn.scheduling_sequence, ConversationTurn.id)
            .limit(1)
        )
    ).one_or_none()
    return (int(row.id), int(row.sessao_id)) if row is not None else None
