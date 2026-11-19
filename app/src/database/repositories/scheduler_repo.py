from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import random
from typing import Awaitable, Callable, Literal, Sequence, TypeVar
from uuid import UUID, uuid4

from loguru import logger
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.models import (
    AgentServiceLane,
    Agente,
    ConversationTurn,
    ConsumoLLM,
    DeliveryOperatorAction,
    DeliveryRuntimeState,
    GenerationAttempt,
    HandoffAttempt,
    InboundEvent,
    Mensagem,
    OutboxDelivery,
    ScheduledHandoff,
    SchedulerRuntimeState,
    Sessao,
    TurnInputMembership,
    TurnRevision,
)
from src.database import db_errors
from src.database.repositories import delivery_state_repo, handoff_terminal_repo, replica_repo


# Canonical mutation order for every scheduler-aware path:
# delivery_runtime_state -> scheduler_runtime_state -> agentes ->
# agent_service_lanes -> sessoes (ascending) -> conversation_turns /
# turn_revisions (ascending) -> inbound_events -> mensagens ->
# turn_input_memberships -> handoff_attempts -> outbox_deliveries.
#
# Existing outbox batch claim is deliberately a separate transaction. It must
# commit before an automated delivery is re-authorized through
# lock_automated_delivery_authority(); no code may acquire a lane while holding
# a delivery row lock.

NONTERMINAL_TURN_STATUSES = (
    "pending",
    "claimed",
    "generating",
    "typing",
    "awaiting_delivery",
    "retry",
)
NONTERMINAL_DELIVERY_STATUSES = ("blocked", "pending", "processing", "retry")
HANDOFF_CAP_TERMINAL_REASON = "handoff_recovery_cap"
HANDOFF_CAP_SIGNAL_THRESHOLD = 1
_handoff_cap_signal_count = 0
_admitted_handoff_park_signal_count = 0


def handoff_cap_signal_count() -> int:
    return _handoff_cap_signal_count


def admitted_handoff_park_signal_count() -> int:
    return _admitted_handoff_park_signal_count


class SchedulerConflictError(ValueError):
    pass


class SchedulerNotReadyError(ValueError):
    pass


class SchedulerCapExceeded(ValueError):
    pass


class SchedulerDeadlockRetryExhausted(RuntimeError):
    pass


class IncompleteIntakeMembershipError(SchedulerConflictError):
    """A committed receipt cannot be acknowledged without its exact input graph."""


@dataclass(frozen=True)
class IntakeTarget:
    turn: ConversationTurn
    revision: TurnRevision
    created: bool


@dataclass(frozen=True)
class IntakeTurnGraph:
    turns: tuple[ConversationTurn, ...]
    revisions: tuple[TurnRevision, ...]


@dataclass(frozen=True)
class ClaimedTurn:
    turn_id: int
    turn_key: str
    agente_id: int
    sessao_id: int
    claim_token: str
    scheduler_epoch: str
    delivery_runtime_epoch: str
    arrival_sequence: int
    scheduling_sequence: int


@dataclass(frozen=True)
class AutomatedDeliveryAuthority:
    delivery: OutboxDelivery
    runtime: DeliveryRuntimeState
    scheduler: SchedulerRuntimeState
    lane: AgentServiceLane
    session: Sessao
    turn: ConversationTurn
    revision: TurnRevision
    handoff_attempt: HandoffAttempt | None


@dataclass(frozen=True)
class AutomatedDeliveryHint:
    """Immutable links returned by the committed claim transaction."""

    delivery_class: str
    automated_effect: str | None
    agente_id: int | None
    sessao_id: int | None
    conversation_turn_id: int | None
    turn_revision_id: int | None
    handoff_attempt_id: int | None
    scheduler_epoch: str | None


T = TypeVar("T")
_COST_QUANTUM = Decimal("0.00000001")


def _uuid(value: str, field: str) -> None:
    try:
        UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"{field} must be a UUID") from exc


def _normalized_cost(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(value).quantize(_COST_QUANTUM)


def _generation_evidence_tuple(
    *,
    outcome: str,
    provider_generation_id: str | None,
    tokens_input: int | None,
    tokens_output: int | None,
    tokens_reasoning: int | None,
    tokens_cache_read: int | None,
    tokens_cache_write: int | None,
    custo_creditos: Decimal | None,
    consumo_llm_id: int | None,
    safe_error_code: str | None,
) -> tuple[object, ...]:
    return (
        outcome,
        provider_generation_id,
        tokens_input,
        tokens_output,
        tokens_reasoning,
        tokens_cache_read,
        tokens_cache_write,
        _normalized_cost(custo_creditos),
        consumo_llm_id,
        safe_error_code,
    )


def _stored_generation_evidence(row: GenerationAttempt) -> tuple[object, ...]:
    return _generation_evidence_tuple(
        outcome=row.outcome,
        provider_generation_id=row.provider_generation_id,
        tokens_input=row.tokens_input,
        tokens_output=row.tokens_output,
        tokens_reasoning=row.tokens_reasoning,
        tokens_cache_read=row.tokens_cache_read,
        tokens_cache_write=row.tokens_cache_write,
        custo_creditos=row.custo_creditos,
        consumo_llm_id=row.consumo_llm_id,
        safe_error_code=row.safe_error_code,
    )


def _requested_generation_evidence(
    *,
    outcome: str,
    provider_generation_id: str | None,
    tokens_input: int | None,
    tokens_output: int | None,
    tokens_reasoning: int | None,
    tokens_cache_read: int | None,
    tokens_cache_write: int | None,
    custo_creditos: Decimal | None,
    consumo_llm_id: int | None,
    safe_error_code: str | None,
) -> tuple[object, ...]:
    return _generation_evidence_tuple(
        outcome=outcome,
        provider_generation_id=provider_generation_id,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        tokens_reasoning=tokens_reasoning,
        tokens_cache_read=tokens_cache_read,
        tokens_cache_write=tokens_cache_write,
        custo_creditos=custo_creditos,
        consumo_llm_id=consumo_llm_id,
        safe_error_code=safe_error_code,
    )


async def _lock_matching_consumption(
    db: AsyncSession,
    *,
    attempt: GenerationAttempt,
    consumo_llm_id: int | None,
    provider_generation_id: str | None,
    tokens_input: int | None,
    tokens_output: int | None,
    tokens_reasoning: int | None,
    tokens_cache_read: int | None,
    tokens_cache_write: int | None,
    custo_creditos: Decimal | None,
) -> bool:
    """Prove an exact commercial ledger link after locking the attempt."""

    if consumo_llm_id is None:
        return False
    if tokens_input is None or tokens_output is None or custo_creditos is None:
        return False
    consumo = (
        await db.execute(
            select(ConsumoLLM)
            .where(ConsumoLLM.id == consumo_llm_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if consumo is None:
        return False
    # FU-2: never lock the possibly absent uq_generation_consumo_link key.
    # Distinct ledgers share its tail gap; S gap locks deadlock on UPDATE.
    # _write_generation_accounting lets the unique index arbitrate the link.
    expected_call_type = "resposta" if attempt.attempt_type == "response" else "resumo"
    return bool(
        consumo is not None
        and consumo.agente_id == attempt.agente_id
        and consumo.sessao_id == attempt.sessao_id
        and consumo.modelo_llm == attempt.modelo_llm
        and consumo.tipo_chamada == expected_call_type
        and consumo.provider_generation_id == provider_generation_id
        and consumo.tokens_input == tokens_input
        and consumo.tokens_output == tokens_output
        and consumo.tokens_reasoning == tokens_reasoning
        and consumo.tokens_cache_read == tokens_cache_read
        and consumo.tokens_cache_write == tokens_cache_write
        and _normalized_cost(consumo.custo_creditos)
        == _normalized_cost(custo_creditos)
    )


def _generation_link_duplicate(exc: IntegrityError, values: dict[str, object]) -> bool:
    """Match the reported index, never the untrusted duplicate entry value."""

    args = getattr(exc.orig, "args", ())
    if len(args) < 2 or not isinstance(args[1], str):
        return False
    indexes = ["uq_generation_consumo_link"]
    if values.get("consumo_llm_id") is not None:
        # With an exact ledger and non-NULL provider ID both unique keys
        # conflict; MySQL can report the provider key first.
        indexes.append("uq_generation_provider_id")
    return any(args[1].endswith(f"for key '{prefix}{index}'")
               for index in indexes for prefix in ("generation_attempts.", ""))


async def _write_generation_accounting(
    db: AsyncSession,
    *,
    row: GenerationAttempt,
    values: dict[str, object],
) -> bool:
    """FU-2: exact ledger uniqueness, without a speculative gap lock.

    The attempt and existing ledger are already X-locked. Keep a duplicate
    link local to a savepoint; transaction loss always escapes to the caller.
    """

    try:
        async with db.begin_nested():
            for name, value in values.items():
                setattr(row, name, value)
            await db.flush()
    except BaseException as exc:
        if db_errors.transaction_lost(exc):
            raise
        if (
            isinstance(exc, IntegrityError)
            and _mysql_error_code(exc) == 1062
            and _generation_link_duplicate(exc, values)
        ):
            # Savepoint rollback expires the modified ORM state. Callers may
            # log the attempt identity, so restore it before returning False.
            await db.refresh(row)
            return False
        raise
    return True


def _mysql_error_code(exc: BaseException) -> int | None:
    current: BaseException | None = exc
    while current is not None:
        args = getattr(current, "args", ())
        if args and isinstance(args[0], int):
            return args[0]
        current = getattr(current, "orig", None)
    return None


def _retryable_transaction_error(exc: BaseException) -> bool:
    # Includes 1305: a savepoint rollback that failed because InnoDB already
    # rolled back the whole transaction after a deadlock.
    return isinstance(exc, DBAPIError) and db_errors.transaction_lost(exc)


async def run_transaction_with_deadlock_retry(
    session_factory: async_sessionmaker[AsyncSession],
    operation: Callable[[AsyncSession], Awaitable[T]],
    *,
    max_attempts: int = 4,
    max_jitter_seconds: float = 0.05,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Retry a complete short transaction, never a suffix of its lock graph."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    if max_jitter_seconds < 0:
        raise ValueError("max_jitter_seconds cannot be negative")
    last_error: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            async with session_factory() as db:
                async with db.begin():
                    return await operation(db)
        except BaseException as exc:
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            if not _retryable_transaction_error(exc):
                raise
            last_error = exc
            if attempt == max_attempts:
                break
            await sleep(random.random() * max_jitter_seconds)
    raise SchedulerDeadlockRetryExhausted(
        f"scheduler transaction exhausted {max_attempts} attempts"
    ) from last_error


async def get_scheduler_state(
    db: AsyncSession,
    *,
    for_update: bool = False,
    for_share: bool = False,
) -> SchedulerRuntimeState:
    if for_update and for_share:
        raise ValueError("scheduler state cannot be locked twice")
    query = select(SchedulerRuntimeState).where(SchedulerRuntimeState.id == 1)
    if for_update:
        query = query.with_for_update()
    elif for_share:
        query = query.with_for_update(read=True)
    if for_update or for_share:
        query = query.execution_options(populate_existing=True)
    row = (await db.execute(query)).scalar_one_or_none()
    if row is None:
        raise RuntimeError("scheduler runtime singleton is missing")
    return row


async def count_unknown_nonterminal_deliveries(db: AsyncSession) -> int:
    return int(
        (
            await db.execute(
                select(func.count(OutboxDelivery.id)).where(
                    OutboxDelivery.delivery_class == "legacy_unknown",
                    OutboxDelivery.status.in_(NONTERMINAL_DELIVERY_STATUSES),
                )
            )
        ).scalar_one()
        or 0
    )


async def count_unknown_generation_outcomes(db: AsyncSession) -> int:
    """Count unresolved audit quarantines, not the historical outcome label."""

    return int(
        (
            await db.execute(
                select(func.count(GenerationAttempt.id)).where(
                    GenerationAttempt.accounting_state == "reconciliation_required",
                    GenerationAttempt.reconciled_at.is_(None),
                )
            )
        ).scalar_one()
        or 0
    )


# T08-1: allowlist shared by CLI, repository and state-matrix tests.
SCHEDULER_TRANSITIONS = {
    "disabled": frozenset({"enabling"}),
    "enabling": frozenset({"enabled", "disabled"}),
    "enabled": frozenset({"draining"}),
    "draining": frozenset({"disabled"}),
}
_drain_refusal_count = 0
_intake_state_refusal_count = 0


# T08-3: enabling -> enabled refusal by replica compatibility / attestation.
_replica_gate_refusal_count = 0


class ReplicaGateRefused(SchedulerNotReadyError):
    def __init__(self, reasons: tuple[str, ...]) -> None:
        self.reasons = reasons
        super().__init__("scheduler enable gate refused: " + ", ".join(reasons))


def replica_gate_refusal_count() -> int:
    return _replica_gate_refusal_count


class SchedulerDrainBlocked(SchedulerNotReadyError):
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
        super().__init__("scheduler clearance blocked: " + ", ".join(
            f"{category}={count}" for category, count in counts.items() if count
        ))


def drain_refusal_count() -> int:
    return _drain_refusal_count


def intake_state_refusal_count() -> int:
    return _intake_state_refusal_count


def require_new_intake_enabled(scheduler: SchedulerRuntimeState) -> None:
    global _intake_state_refusal_count
    if scheduler.state != "enabled":
        _intake_state_refusal_count += 1
        logger.error("Scheduler new intake refused | count={} threshold=1 state={}",
                     _intake_state_refusal_count, scheduler.state)
        refusal = SchedulerNotReadyError("scheduler refuses new intake")
        refusal.scheduler_state = scheduler.state  # T08-5: technical 503 code
        raise refusal


# Literal SQL lists derived from the shared allowlists (one source of truth).
_NONTERMINAL_TURN_SQL = "(" + ",".join(f"'{s}'" for s in NONTERMINAL_TURN_STATUSES) + ")"
_NONTERMINAL_DELIVERY_SQL = "(" + ",".join(f"'{s}'" for s in NONTERMINAL_DELIVERY_STATUSES) + ")"


async def begin_scheduler_control(db: AsyncSession) -> None:
    """Open the scheduler control UoW as READ COMMITTED (A1/A3).

    Must be the first statement of its own transaction: an earlier read view
    or pending write fails closed. READ COMMITTED keeps the clearance locking
    reads current and limits them to record locks on matching clustered rows,
    so no gap/next-key lock or secondary-index lock can block (or deadlock
    with) writers that do not take the scheduler mutex.
    """

    if db.new or db.dirty or db.deleted:
        raise SchedulerConflictError("scheduler control requires no pending writes")
    try:
        # MySQL itself refuses (1568) once any statement opened the transaction.
        await db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    except DBAPIError as exc:
        if _mysql_error_code(exc) == 1568:
            raise SchedulerConflictError(
                "scheduler control requires its own fresh transaction"
            ) from exc
        raise


async def scheduler_clearance_counts(db: AsyncSession) -> dict[str, int]:
    """Current locking counts, called only after runtime -> scheduler locks.

    Runs inside ``begin_scheduler_control``. Every scan is forced onto the
    clustered index and keeps S record locks only on rows that block, through
    commit. Rows that can become blocking again (insert or revival) are only
    written under the scheduler mutex held here; late accounting/quarantine
    resolution only changes an attempt that is already counted and locked.
    """
    counts: dict[str, int] = {}
    # Canonical lane -> turn -> attempt -> scheduled handoff -> delivery.
    counts["owned_lanes"] = int((await db.execute(text(
        "SELECT COUNT(*) FROM agent_service_lanes FORCE INDEX (PRIMARY) "
        "WHERE owner_token IS NOT NULL FOR SHARE"
    ))).scalar_one())
    counts["nonterminal_turns"] = int((await db.execute(text(
        "SELECT COUNT(*) FROM conversation_turns FORCE INDEX (PRIMARY) "
        f"WHERE status IN {_NONTERMINAL_TURN_SQL} FOR SHARE"
    ))).scalar_one())
    attempts = (await db.execute(text(
        "SELECT COALESCE(SUM(outcome IN ('started','outcome_unknown')),0), "
        "COALESCE(SUM(accounting_state='reconciliation_required' AND reconciled_at IS NULL),0) "
        "FROM generation_attempts FORCE INDEX (PRIMARY) "
        "WHERE outcome IN ('started','outcome_unknown') "
        "OR (accounting_state='reconciliation_required' AND reconciled_at IS NULL) FOR SHARE"
    ))).one()
    counts["unresolved_attempts"] = int(attempts[0])
    counts["unreconciled_attempts"] = int(attempts[1])
    counts["nonterminal_scheduled_handoffs"] = int((await db.execute(text(
        "SELECT COUNT(*) FROM scheduled_handoffs sh FORCE INDEX (PRIMARY) "
        "STRAIGHT_JOIN conversation_turns t FORCE INDEX (PRIMARY) ON t.id=sh.conversation_turn_id "
        f"WHERE t.status IN {_NONTERMINAL_TURN_SQL} FOR SHARE"
    ))).scalar_one())
    deliveries = (await db.execute(text(
        "SELECT COALESCE(SUM(delivery_class='automated'),0), "
        "COALESCE(SUM(delivery_class='legacy_unknown'),0) "
        "FROM outbox_deliveries FORCE INDEX (PRIMARY) "
        "WHERE delivery_class IN ('automated','legacy_unknown') "
        f"AND status IN {_NONTERMINAL_DELIVERY_SQL} FOR SHARE"
    ))).one()
    counts["automated_deliveries"] = int(deliveries[0])
    counts["legacy_unknown_deliveries"] = int(deliveries[1])
    return counts


async def _require_scheduler_clearance(db: AsyncSession) -> None:
    global _drain_refusal_count
    counts = await scheduler_clearance_counts(db)
    if any(counts.values()):
        _drain_refusal_count += 1
        logger.error("Scheduler clearance refused | count={} threshold=1 categories={}",
                     _drain_refusal_count, counts)
        raise SchedulerDrainBlocked(counts)


async def transition_scheduler_state(
    db: AsyncSession,
    *,
    target: Literal["disabled", "enabling", "enabled", "draining"],
    scheduler_epoch: str,
    expected_version: int,
    actor: str,
    reason: str,
    replica_attestation: replica_repo.EnableAttestation | None = None,
) -> SchedulerRuntimeState:
    """Apply an audited lifecycle edge under runtime -> scheduler locks.

    Must open its own transaction (``begin_scheduler_control``). The audit
    row (no FK parent) is written before the clearance reads, so nothing
    after them can wait on a writer that does not hold the scheduler mutex.
    ``enabling -> enabled`` also requires the T08-3 replica gate.
    """
    global _replica_gate_refusal_count
    _uuid(scheduler_epoch, "scheduler_epoch")
    actor, reason = delivery_state_repo.validate_audit_metadata(actor=actor, reason=reason)
    if len(actor) > 255 or expected_version < 1:
        raise ValueError("invalid scheduler transition metadata")
    await begin_scheduler_control(db)
    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)
    state = await get_scheduler_state(db, for_update=True)
    if state.version != expected_version:
        raise SchedulerConflictError("scheduler version changed")
    if target not in SCHEDULER_TRANSITIONS.get(state.state, ()):
        raise SchedulerConflictError(f"illegal scheduler transition {state.state}->{target}")
    if state.state != "disabled" and state.scheduler_epoch != scheduler_epoch:
        raise SchedulerConflictError("scheduler epoch cannot rotate while active")
    audit = {
        "v": 1, "from_version": state.version, "to_version": state.version + 1,
        "from_epoch": state.scheduler_epoch, "to_epoch": scheduler_epoch,
        "delivery_epoch": runtime.cutover_epoch, "reason": reason,
    }
    # T08-3: every fresh replica compatible + explicit operator attestation.
    # Plain READ COMMITTED reads after runtime/scheduler locks and before the
    # audit row and clearance locks: never waits on a heartbeat writer.
    if target == "enabled":
        fleet = await replica_repo.load_fleet(db)
        refused = replica_repo.enable_gate_reasons(
            fleet, attestation=replica_attestation, target_epoch=scheduler_epoch,
        )
        if refused:
            _replica_gate_refusal_count += 1
            logger.error("Scheduler enable gate refused | count={} threshold=1 reasons={}",
                         _replica_gate_refusal_count, ",".join(refused))
            raise ReplicaGateRefused(refused)
        operator = replica_attestation.operator
        audit.update({
            "v": 2, "attested_old_replicas_removed": True,
            "code_version": operator.code_version,
            "schema_version": fleet.db_schema_version,
            "replicas": replica_repo.kind_counts(fleet.replicas),
        })
    elif replica_attestation is not None:
        raise ValueError("replica attestation applies only to enabling->enabled")
    audit_reason = json.dumps(audit, separators=(",", ":"), ensure_ascii=False)
    if len(audit_reason) > 500:
        raise ValueError("scheduler audit reason exceeds 500 characters")
    db.add(DeliveryOperatorAction(
        delivery_id=None, action=f"scheduler:{state.state}:{target}",
        actor=actor, reason=audit_reason,
    ))
    await db.flush()
    # Also guard abort-enabling and rotation from disabled, never infer that
    # disabled means no accepted work (audited repair may have been required).
    if target == "disabled" or state.state == "disabled":
        await _require_scheduler_clearance(db)
    if target in {"enabling", "enabled"}:
        if runtime.mode != "outbox":
            raise SchedulerNotReadyError("scheduler enablement requires outbox runtime")
        counts = await scheduler_clearance_counts(db)
        if counts["legacy_unknown_deliveries"]:
            raise SchedulerNotReadyError("unknown nonterminal delivery classification")
        if counts["unresolved_attempts"] or counts["unreconciled_attempts"]:
            raise SchedulerNotReadyError("unknown generation outcome requires reconciliation")
    state.state = target
    state.scheduler_epoch = scheduler_epoch
    state.version += 1
    state.changed_by = actor
    state.change_reason = reason
    await db.flush()
    return state


async def _lock_agent(db: AsyncSession, agente_id: int) -> Agente:
    row = (
        await db.execute(
            select(Agente)
            .where(Agente.id == agente_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if row is None:
        raise LookupError("agent not found")
    return row


async def _lock_lane(
    db: AsyncSession,
    *,
    agente_id: int,
    scheduler_epoch: str,
    create: bool,
) -> AgentServiceLane:
    query = (
        select(AgentServiceLane)
        .where(AgentServiceLane.agente_id == agente_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    row = (await db.execute(query)).scalar_one_or_none()
    if row is None and create:
        try:
            async with db.begin_nested():
                db.add(
                    AgentServiceLane(
                        agente_id=agente_id,
                        scheduler_epoch=scheduler_epoch,
                    )
                )
                await db.flush()
        except IntegrityError:
            pass
        row = (await db.execute(query)).scalar_one_or_none()
    if row is None:
        raise LookupError("agent lane not found")
    if row.scheduler_epoch != scheduler_epoch:
        if row.owner_token is not None:
            raise SchedulerConflictError("owned lane belongs to another scheduler epoch")
        row.scheduler_epoch = scheduler_epoch
        await db.flush()
    return row


async def lock_authority_prefix(
    db: AsyncSession,
    *,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str | None = None,
    require_enabled: bool = True,
    create_lane: bool = False,
) -> tuple[DeliveryRuntimeState, SchedulerRuntimeState, Agente, AgentServiceLane]:
    """Lock fresh runtime -> scheduler -> agent -> lane authority."""

    _uuid(scheduler_epoch, "scheduler_epoch")
    if delivery_runtime_epoch is not None:
        _uuid(delivery_runtime_epoch, "delivery_runtime_epoch")
    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)
    # Exclusive scheduler singleton lock is also the global capacity mutex.
    # It keeps cross-agent cap checks atomic without holding it across I/O.
    scheduler = await get_scheduler_state(db, for_update=True)
    if runtime.mode != "outbox":
        raise SchedulerNotReadyError("delivery runtime is not outbox")
    if (
        delivery_runtime_epoch is not None
        and runtime.cutover_epoch != delivery_runtime_epoch
    ):
        raise SchedulerConflictError("delivery runtime epoch changed")
    if scheduler.scheduler_epoch != scheduler_epoch:
        raise SchedulerConflictError("scheduler epoch changed")
    if require_enabled and scheduler.state != "enabled":
        raise SchedulerNotReadyError("scheduler is not enabled")
    agent = await _lock_agent(db, agente_id)
    lane = await _lock_lane(
        db,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        create=create_lane,
    )
    return runtime, scheduler, agent, lane


async def allocate_lane_sequences(
    db: AsyncSession,
    lane: AgentServiceLane,
) -> tuple[int, int]:
    arrival = lane.next_arrival_sequence
    scheduling = lane.next_scheduling_sequence
    lane.next_arrival_sequence += 1
    lane.next_scheduling_sequence += 1
    await db.flush()
    return arrival, scheduling


async def assert_intake_caps(
    db: AsyncSession,
    *,
    scheduler: SchedulerRuntimeState,
    agente_id: int,
    sessao_id: int,
    incoming_bytes: int,
    additional_turns: int = 0,
) -> None:
    if incoming_bytes < 0:
        raise ValueError("incoming_bytes cannot be negative")
    if additional_turns not in {0, 1}:
        raise ValueError("additional_turns must be zero or one")
    session_count, session_bytes = (
        await db.execute(
            select(
                func.count(TurnInputMembership.id),
                func.coalesce(func.sum(TurnInputMembership.content_bytes), 0),
            )
            .join(
                ConversationTurn,
                ConversationTurn.id == TurnInputMembership.turn_id,
            )
            .where(
                TurnInputMembership.sessao_id == sessao_id,
                ConversationTurn.status.in_(NONTERMINAL_TURN_STATUSES),
            )
        )
    ).one()
    agent_turns, agent_bytes = (
        await db.execute(
            select(
                func.count(func.distinct(ConversationTurn.id)),
                func.coalesce(func.sum(TurnInputMembership.content_bytes), 0),
            )
            .select_from(ConversationTurn)
            .outerjoin(
                TurnInputMembership,
                TurnInputMembership.turn_id == ConversationTurn.id,
            )
            .where(
                ConversationTurn.agente_id == agente_id,
                ConversationTurn.status.in_(NONTERMINAL_TURN_STATUSES),
            )
        )
    ).one()
    global_turns, global_bytes = (
        await db.execute(
            select(
                func.count(func.distinct(ConversationTurn.id)),
                func.coalesce(func.sum(TurnInputMembership.content_bytes), 0),
            )
            .select_from(ConversationTurn)
            .outerjoin(
                TurnInputMembership,
                TurnInputMembership.turn_id == ConversationTurn.id,
            )
            .where(ConversationTurn.status.in_(NONTERMINAL_TURN_STATUSES))
        )
    ).one()
    if session_count + 1 > scheduler.max_pending_inputs_per_session:
        raise SchedulerCapExceeded("session pending input count cap exceeded")
    if session_bytes + incoming_bytes > scheduler.max_pending_input_bytes_per_session:
        raise SchedulerCapExceeded("session pending input byte cap exceeded")
    if agent_turns + additional_turns > scheduler.max_queued_turns_per_agent:
        raise SchedulerCapExceeded("agent queued turn cap exceeded")
    if agent_bytes + incoming_bytes > scheduler.max_queued_bytes_per_agent:
        raise SchedulerCapExceeded("agent queued byte cap exceeded")
    if global_turns + additional_turns > scheduler.max_global_queued_turns:
        raise SchedulerCapExceeded("global queued turn cap exceeded")
    if global_bytes + incoming_bytes > scheduler.max_global_queued_bytes:
        raise SchedulerCapExceeded("global queued byte cap exceeded")


async def create_turn(
    db: AsyncSession,
    *,
    lane: AgentServiceLane,
    sessao: Sessao,
    scheduler_epoch: str,
    ready_at: datetime,
    successor: bool,
) -> ConversationTurn:
    if sessao.agente_id != lane.agente_id:
        raise SchedulerConflictError("session and lane agent differ")
    arrival, scheduling = await allocate_lane_sequences(db, lane)
    row = ConversationTurn(
        agente_id=lane.agente_id,
        sessao_id=sessao.id,
        scheduler_epoch=scheduler_epoch,
        arrival_sequence=arrival,
        scheduling_sequence=scheduling,
        status="pending",
        session_slot="successor" if successor else "active",
        ready_at=ready_at,
    )
    db.add(row)
    await db.flush()
    return row


async def create_revision(
    db: AsyncSession,
    *,
    turn: ConversationTurn,
    scheduler_epoch: str,
    replacement_of_revision_id: int | None = None,
) -> TurnRevision:
    revision_number = 2 if replacement_of_revision_id is not None else 1
    if revision_number == 2 and turn.refresh_count >= 1:
        raise SchedulerCapExceeded("turn refresh budget exhausted")
    row = TurnRevision(
        turn_id=turn.id,
        scheduler_epoch=scheduler_epoch,
        revision_number=revision_number,
        refresh_ordinal=revision_number - 1,
        replacement_of_revision_id=replacement_of_revision_id,
    )
    db.add(row)
    await db.flush()
    turn.current_revision_id = row.id
    await db.flush()
    return row


async def supersede_revision_for_refresh_locked(
    db: AsyncSession,
    *,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    revision: TurnRevision,
    reason: str,
) -> TurnRevision:
    """Spend the single refresh under an already locked owner graph.

    This is shared by ordinary ticket04 advancement and cap recovery so both
    create the same revision-2 authority.  ``ready_at`` is intentionally left
    untouched: intake already persisted the 2s/5s window from the first new
    active membership.
    """

    if (
        revision.turn_id != turn.id
        or turn.current_revision_id != revision.id
        or lane.active_turn_id != turn.id
        or lane.active_revision_id != revision.id
        or revision.revision_number != 1
        or revision.status in {"superseded", "cancelled", "dead", "admitted"}
    ):
        raise SchedulerConflictError("only the live first revision is replaceable")
    reason = reason.strip()
    if not reason:
        raise ValueError("refresh supersession reason must not be blank")
    revision.status = "superseded"
    revision.superseded_reason = reason
    await db.flush()
    replacement = await create_revision(
        db,
        turn=turn,
        scheduler_epoch=turn.scheduler_epoch,
        replacement_of_revision_id=revision.id,
    )
    turn.status = "claimed"
    lane.active_revision_id = replacement.id
    await db.flush()
    return replacement


async def lock_intake_turn_graph(
    db: AsyncSession,
    *,
    sessao_id: int,
) -> IntakeTurnGraph:
    """Lock turns then revisions in ascending canonical order."""

    turns = tuple(
        (
            await db.execute(
                select(ConversationTurn)
                .with_hint(ConversationTurn, "FORCE INDEX (idx_turn_session_status)", "mysql")  # FU-2
                .where(
                    ConversationTurn.sessao_id == sessao_id,
                    ConversationTurn.session_slot.in_(("active", "successor")),
                    ConversationTurn.status.in_(NONTERMINAL_TURN_STATUSES),
                )
                .order_by(ConversationTurn.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    turn_ids = [row.id for row in turns]
    revisions = tuple(
        (
            await db.execute(
                select(TurnRevision)
                .where(TurnRevision.turn_id.in_(turn_ids))
                .order_by(TurnRevision.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    ) if turn_ids else ()
    return IntakeTurnGraph(turns=turns, revisions=revisions)


async def select_or_create_intake_target(
    db: AsyncSession,
    *,
    scheduler: SchedulerRuntimeState,
    lane: AgentServiceLane,
    sessao: Sessao,
    scheduler_epoch: str,
    incoming_bytes: int,
    now: datetime,
    locked_graph: IntakeTurnGraph | None = None,
    silence_seconds: int = 2,
) -> IntakeTarget:
    """Lock a session's complete turn graph and choose active or sole successor.

    Callers already hold runtime -> scheduler -> agent -> lane -> session. The
    ordered turn/revision locks keep intake on the canonical lifecycle prefix.
    """

    if silence_seconds < 1:
        raise ValueError("silence_seconds must be positive")
    graph = locked_graph or await lock_intake_turn_graph(db, sessao_id=sessao.id)
    turns = list(graph.turns)
    revisions = list(graph.revisions)
    revisions_by_id = {row.id: row for row in revisions}
    active = next((row for row in turns if row.session_slot == "active"), None)
    successor = next((row for row in turns if row.session_slot == "successor"), None)
    if active is None and successor is not None:
        raise SchedulerConflictError("successor exists without an active turn")

    target = active
    create_successor = False
    if active is not None:
        current = revisions_by_id.get(active.current_revision_id)
        if current is None:
            raise IncompleteIntakeMembershipError(
                "active turn is missing its current revision"
            )
        sealed_refresh = current.revision_number == 2 and current.sealed_at is not None
        final_range = bool(
            active.final_revision_id is not None
            or current.status in {"final_candidate", "admitted"}
            or active.status == "awaiting_delivery"
        )
        if sealed_refresh or final_range:
            target = successor
            create_successor = successor is None
        elif successor is not None:
            raise SchedulerConflictError("successor exists before the active range is sealed")

    if target is None:
        await assert_intake_caps(
            db,
            scheduler=scheduler,
            agente_id=lane.agente_id,
            sessao_id=sessao.id,
            incoming_bytes=incoming_bytes,
            additional_turns=1,
        )
        target = await create_turn(
            db,
            lane=lane,
            sessao=sessao,
            scheduler_epoch=scheduler_epoch,
            ready_at=now + timedelta(seconds=silence_seconds),
            successor=create_successor,
        )
        revision = await create_revision(
            db,
            turn=target,
            scheduler_epoch=scheduler_epoch,
        )
        return IntakeTarget(turn=target, revision=revision, created=True)

    revision = revisions_by_id.get(target.current_revision_id)
    if revision is None:
        raise IncompleteIntakeMembershipError(
            "intake turn is missing its current revision"
        )
    if target.scheduler_epoch != scheduler_epoch or revision.scheduler_epoch != scheduler_epoch:
        raise SchedulerConflictError("intake target belongs to another scheduler epoch")
    return IntakeTarget(turn=target, revision=revision, created=False)


async def require_complete_intake_replay(
    db: AsyncSession,
    *,
    receipt: InboundEvent,
    agente_id: int,
    sessao_id: int,
) -> TurnInputMembership:
    """Validate a replay without changing capacity, readiness or membership."""

    if receipt.mensagem_id is None:
        raise IncompleteIntakeMembershipError(
            "historical receipt is missing its durable message association"
        )
    if receipt.processed_at is None:
        raise IncompleteIntakeMembershipError(
            "historical receipt membership is not finalized"
        )
    message = (
        await db.execute(
            select(Mensagem)
            .where(Mensagem.id == receipt.mensagem_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    membership = (
        await db.execute(
            select(TurnInputMembership)
            .where(TurnInputMembership.inbound_event_id == receipt.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        message is None
        or membership is None
        or receipt.agente_id != agente_id
        or receipt.sessao_id != sessao_id
        or message.id != membership.mensagem_id
        or message.sessao_id != sessao_id
        or message.role != "user"
        or membership.sessao_id != sessao_id
    ):
        raise IncompleteIntakeMembershipError(
            "historical receipt has inconsistent durable turn membership"
        )
    return membership


async def require_consistent_replay_disposition(
    db: AsyncSession,
    *,
    receipt: InboundEvent,
) -> None:
    """Reject receipts whose immutable outcome contradicts durable evidence."""

    if receipt.receipt_disposition != "scheduled" and receipt.mensagem_id is not None:
        raise IncompleteIntakeMembershipError(
            "historical receipt disposition contradicts scheduled evidence"
        )
    if receipt.receipt_disposition != "scheduled":
        membership_id = (
            await db.execute(
                select(TurnInputMembership.id)
                .where(TurnInputMembership.inbound_event_id == receipt.id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if membership_id is not None:
            # This corruption path fails immediately and never acquires the
            # linked message after the membership, preserving normal order.
            raise IncompleteIntakeMembershipError(
                "historical receipt disposition contradicts scheduled evidence"
            )
    if (
        receipt.provider == "chatwoot"
        and receipt.receipt_disposition != "chatwoot"
    ) or (
        receipt.provider != "chatwoot"
        and receipt.receipt_disposition == "chatwoot"
    ):
        raise IncompleteIntakeMembershipError(
            "historical receipt disposition contradicts provider identity"
        )


async def update_intake_readiness(
    db: AsyncSession,
    *,
    turn: ConversationTurn,
    silence_seconds: int = 2,
    collection_cap_seconds: int = 5,
) -> datetime:
    """Set readiness from the exact still-active batch using database timestamps."""

    if silence_seconds < 1 or collection_cap_seconds < silence_seconds:
        raise ValueError("invalid intake collection timing")
    first_at, last_at = (
        await db.execute(
            select(
                func.min(TurnInputMembership.created_at),
                func.max(TurnInputMembership.created_at),
            ).where(
                TurnInputMembership.turn_id == turn.id,
                TurnInputMembership.membership_state == "active",
            )
        )
    ).one()
    if first_at is None or last_at is None:
        raise IncompleteIntakeMembershipError("intake target has no active input")
    ready_at = min(
        last_at + timedelta(seconds=silence_seconds),
        first_at + timedelta(seconds=collection_cap_seconds),
    )
    turn.ready_at = ready_at
    await db.flush()
    return ready_at


async def append_input_membership(
    db: AsyncSession,
    *,
    scheduler: SchedulerRuntimeState,
    turn_id: int,
    sessao_id: int,
    mensagem_id: int,
    inbound_event_id: int,
    content_bytes: int,
) -> tuple[TurnInputMembership, bool]:
    """Attach one trusted receipt/message identity after lane then session locks."""

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
    if session is None or turn is None or turn.sessao_id != sessao_id:
        raise SchedulerConflictError("turn/session membership authority changed")
    # Identity locks follow the published receipt -> message -> membership
    # order after lifecycle authority. A savepoint around receipt insertion
    # does not shorten either lock's lifetime below the outer transaction.
    receipt = (
        await db.execute(
            select(InboundEvent)
            .where(InboundEvent.id == inbound_event_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    message = (
        await db.execute(
            select(Mensagem)
            .where(Mensagem.id == mensagem_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if receipt is None or message is None:
        raise LookupError("receipt or message not found")
    existing = (
        await db.execute(
            select(TurnInputMembership)
            .where(TurnInputMembership.inbound_event_id == inbound_event_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.mensagem_id != mensagem_id or existing.turn_id != turn_id:
            raise SchedulerConflictError("receipt already belongs to another input")
        return existing, False
    if receipt.processed_at is not None:
        raise SchedulerConflictError("historical receipt is missing turn membership")
    if message.sessao_id != sessao_id or message.role != "user":
        raise SchedulerConflictError("message is not a user input for the locked session")
    await assert_intake_caps(
        db,
        scheduler=scheduler,
        agente_id=turn.agente_id,
        sessao_id=sessao_id,
        incoming_bytes=content_bytes,
    )
    existing_turn_inputs = int(
        (
            await db.execute(
                select(func.count(TurnInputMembership.id)).where(
                    TurnInputMembership.turn_id == turn_id
                )
            )
        ).scalar_one()
        or 0
    )
    next_sequence = int(
        (
            await db.execute(
                select(func.coalesce(func.max(TurnInputMembership.input_sequence), 0)).where(
                    TurnInputMembership.sessao_id == sessao_id
                )
            )
        ).scalar_one()
    ) + 1
    row = TurnInputMembership(
        turn_id=turn_id,
        sessao_id=sessao_id,
        mensagem_id=mensagem_id,
        inbound_event_id=inbound_event_id,
        input_sequence=next_sequence,
        content_bytes=content_bytes,
    )
    db.add(row)
    await db.flush()
    if existing_turn_inputs > 0:
        turn.input_version += 1
    receipt.agente_id = turn.agente_id
    receipt.sessao_id = sessao_id
    receipt.mensagem_id = mensagem_id
    receipt.processed_at = func.utc_timestamp(6)
    await db.flush()
    return row, True


async def claim_ready_turn(
    db: AsyncSession,
    *,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    worker_id: str,
    lease_seconds: int,
    candidate: tuple[int, int] | None = None,
) -> ClaimedTurn | None:
    """Claim the head ready turn of one agent lane.

    ``candidate`` is the ``(turn_id, sessao_id)`` hint of a committed scalar
    discovery (``turn_generation_repo.ready_turn_hint``): with it no plain read
    precedes a lock wait (§16.1 A1); quarantine and head-of-line are checked
    by the first consistent read, after every lock. Without it (direct
    callers only) the identity is discovered under the scheduler mutex.
    """

    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    worker_id = worker_id.strip()
    if not worker_id:
        raise ValueError("worker_id must not be blank")
    _, scheduler, _, lane = await lock_authority_prefix(
        db,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        require_enabled=False,
    )
    if scheduler.state not in {"enabled", "draining"}:
        raise SchedulerNotReadyError("scheduler does not permit accepted-work claims")
    if candidate is not None:
        return await _claim_hinted_turn(
            db,
            scheduler=scheduler,
            lane=lane,
            agente_id=agente_id,
            delivery_runtime_epoch=delivery_runtime_epoch,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
            candidate=candidate,
        )
    if await count_unknown_generation_outcomes(db):
        agent_unknown = int(
            (
                await db.execute(
                    select(func.count(GenerationAttempt.id)).where(
                        GenerationAttempt.agente_id == agente_id,
                        GenerationAttempt.accounting_state
                        == "reconciliation_required",
                        GenerationAttempt.reconciled_at.is_(None),
                    )
                )
            ).scalar_one()
            or 0
        )
        if agent_unknown:
            return None
    if lane.owner_token is not None:
        # Expiry never transfers authority. Recovery must fence and release the
        # exact prior owner in its own canonical transaction first.
        return None
    candidate_identity = (
        await db.execute(
            select(ConversationTurn.id, ConversationTurn.sessao_id)
            .where(
                ConversationTurn.agente_id == agente_id,
                ConversationTurn.scheduler_epoch == scheduler.scheduler_epoch,
                ConversationTurn.session_slot == "active",
                ConversationTurn.status.in_(("pending", "retry")),
                ConversationTurn.ready_at <= func.utc_timestamp(6),
                (
                    (ConversationTurn.retry_at.is_(None))
                    | (ConversationTurn.retry_at <= func.utc_timestamp(6))
                ),
            )
            .order_by(ConversationTurn.scheduling_sequence, ConversationTurn.id)
            .limit(1)
        )
    ).one_or_none()
    if candidate_identity is None:
        return None
    session = (
        await db.execute(
            select(Sessao)
            .where(Sessao.id == candidate_identity.sessao_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    candidate = (
        await db.execute(
            select(ConversationTurn)
            .where(
                ConversationTurn.id == candidate_identity.id,
                ConversationTurn.scheduler_epoch == scheduler.scheduler_epoch,
                ConversationTurn.session_slot == "active",
                ConversationTurn.status.in_(("pending", "retry")),
                ConversationTurn.ready_at <= func.utc_timestamp(6),
                (
                    (ConversationTurn.retry_at.is_(None))
                    | (ConversationTurn.retry_at <= func.utc_timestamp(6))
                ),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if session is None or candidate is None:
        return None
    token = str(uuid4())
    lease_until = (
        await db.execute(
            select(
                func.timestampadd(text("SECOND"), lease_seconds, func.utc_timestamp(6))
            )
        )
    ).scalar_one()
    lane.owner_token = token
    lane.owner_worker_id = worker_id
    lane.lease_until = lease_until
    lane.active_turn_id = candidate.id
    lane.active_revision_id = candidate.current_revision_id
    candidate.claim_token = token
    candidate.claim_owner = worker_id
    candidate.claim_until = lease_until
    candidate.status = "claimed"
    await db.flush()
    return ClaimedTurn(
        turn_id=candidate.id,
        turn_key=candidate.turn_key,
        agente_id=candidate.agente_id,
        sessao_id=candidate.sessao_id,
        claim_token=token,
        scheduler_epoch=scheduler.scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        arrival_sequence=candidate.arrival_sequence,
        scheduling_sequence=candidate.scheduling_sequence,
    )


def _eligible_turn_criteria(scheduler_epoch: str) -> tuple:
    return (
        ConversationTurn.scheduler_epoch == scheduler_epoch,
        ConversationTurn.session_slot == "active",
        ConversationTurn.status.in_(("pending", "retry")),
        ConversationTurn.ready_at <= func.utc_timestamp(6),
        (
            (ConversationTurn.retry_at.is_(None))
            | (ConversationTurn.retry_at <= func.utc_timestamp(6))
        ),
    )


def _grant_claim(
    *,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    worker_id: str,
    lease_until: datetime,
) -> str:
    token = str(uuid4())
    lane.owner_token = token
    lane.owner_worker_id = worker_id
    lane.lease_until = lease_until
    lane.active_turn_id = turn.id
    lane.active_revision_id = turn.current_revision_id
    turn.claim_token = token
    turn.claim_owner = worker_id
    turn.claim_until = lease_until
    turn.status = "claimed"
    return token


async def _claim_hinted_turn(
    db: AsyncSession,
    *,
    scheduler: SchedulerRuntimeState,
    lane: AgentServiceLane,
    agente_id: int,
    delivery_runtime_epoch: str,
    worker_id: str,
    lease_seconds: int,
    candidate: tuple[int, int],
) -> ClaimedTurn | None:
    if lane.owner_token is not None:
        return None
    turn_id, sessao_id = candidate
    session = (
        await db.execute(
            select(Sessao)
            .where(Sessao.id == sessao_id, Sessao.agente_id == agente_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    turn = (
        await db.execute(
            select(ConversationTurn)
            .where(
                ConversationTurn.id == turn_id,
                ConversationTurn.agente_id == agente_id,
                ConversationTurn.sessao_id == sessao_id,
                *_eligible_turn_criteria(scheduler.scheduler_epoch),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if session is None or turn is None:
        return None
    # First consistent read: after every lock wait of this transaction.
    quarantined = (
        await db.execute(
            select(GenerationAttempt.id)
            .where(
                GenerationAttempt.agente_id == agente_id,
                GenerationAttempt.accounting_state == "reconciliation_required",
                GenerationAttempt.reconciled_at.is_(None),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    head = (
        await db.execute(
            select(ConversationTurn.id)
            .where(
                ConversationTurn.agente_id == agente_id,
                *_eligible_turn_criteria(scheduler.scheduler_epoch),
            )
            .order_by(ConversationTurn.scheduling_sequence, ConversationTurn.id)
            .limit(1)
        )
    ).scalar_one_or_none()
    if quarantined is not None or head != turn.id:
        return None
    lease_until = (
        await db.execute(
            select(
                func.timestampadd(text("SECOND"), lease_seconds, func.utc_timestamp(6))
            )
        )
    ).scalar_one()
    token = _grant_claim(lane=lane, turn=turn, worker_id=worker_id, lease_until=lease_until)
    await db.flush()
    return ClaimedTurn(
        turn_id=turn.id,
        turn_key=turn.turn_key,
        agente_id=turn.agente_id,
        sessao_id=turn.sessao_id,
        claim_token=token,
        scheduler_epoch=scheduler.scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        arrival_sequence=turn.arrival_sequence,
        scheduling_sequence=turn.scheduling_sequence,
    )


async def _lock_owned_turn(
    db: AsyncSession,
    *,
    turn_id: int,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    owner_token: str,
    sessao_id: int | None = None,
) -> tuple[
    DeliveryRuntimeState,
    SchedulerRuntimeState,
    AgentServiceLane,
    Sessao,
    ConversationTurn,
]:
    runtime, scheduler, _, lane = await lock_authority_prefix(
        db,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        require_enabled=False,
    )
    if scheduler.state not in {"enabled", "draining"}:
        raise SchedulerNotReadyError("scheduler cannot continue work in current state")
    if sessao_id is None:
        # Direct callers without the claim identity only; owner paths pass
        # the immutable claimed ``sessao_id`` so no plain read precedes a
        # lock wait (§16.1 A1).
        sessao_id = (
            await db.execute(
                select(ConversationTurn.sessao_id).where(ConversationTurn.id == turn_id)
            )
        ).scalar_one_or_none()
    if sessao_id is None:
        raise SchedulerConflictError("turn no longer exists")
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
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    if (
        session is None
        or turn is None
        or turn.sessao_id != sessao_id
        or lane.owner_token != owner_token
        or lane.active_turn_id != turn_id
        or turn.claim_token != owner_token
        or turn.scheduler_epoch != scheduler_epoch
        or lane.lease_until is None
        or lane.lease_until <= now
        or turn.claim_until is None
        or turn.claim_until <= now
    ):
        raise SchedulerConflictError("stale turn ownership")
    return runtime, scheduler, lane, session, turn


async def renew_lane_lease(
    db: AsyncSession,
    *,
    turn_id: int,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    owner_token: str,
    lease_seconds: int,
    sessao_id: int | None = None,
) -> bool:
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")
    try:
        _, _, lane, _, turn = await _lock_owned_turn(
            db,
            turn_id=turn_id,
            agente_id=agente_id,
            scheduler_epoch=scheduler_epoch,
            delivery_runtime_epoch=delivery_runtime_epoch,
            owner_token=owner_token,
            sessao_id=sessao_id,
        )
    except SchedulerConflictError:
        return False
    lease_until = (
        await db.execute(
            select(
                func.timestampadd(text("SECOND"), lease_seconds, func.utc_timestamp(6))
            )
        )
    ).scalar_one()
    lane.lease_until = lease_until
    turn.claim_until = lease_until
    await db.flush()
    return True


async def lock_pending_successor(db: AsyncSession, *, sessao_id: int) -> ConversationTurn | None:
    """Pre-lock the successor after the active turn, before revisions/effects.

    The session is already locked: intake cannot introduce a new successor
    before settlement promotes the row. Re-reading it later takes no new lock.
    """

    return (
        await db.execute(
            select(ConversationTurn)
            .with_hint(ConversationTurn, "FORCE INDEX (idx_turn_session_status)", "mysql")  # FU-2
            .where(
                ConversationTurn.sessao_id == sessao_id,
                ConversationTurn.session_slot == "successor",
                ConversationTurn.status == "pending",
            )
            .order_by(ConversationTurn.id)
            .limit(1)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _promote_successor_after_terminal_active(
    db: AsyncSession,
    *,
    sessao_id: int,
) -> ConversationTurn | None:
    """Re-home the sole pending successor once the active turn went terminal.

    Callers hold the session lock and have just made the active turn terminal
    (flushed), so the successor is locked after it in ascending turn order.
    Without this a dead active turn would orphan its successor and every later
    intake for the session would fail closed.
    """

    successor = (
        await db.execute(
            select(ConversationTurn)
            .with_hint(ConversationTurn, "FORCE INDEX (idx_turn_session_status)", "mysql")  # FU-2
            .where(
                ConversationTurn.sessao_id == sessao_id,
                ConversationTurn.session_slot == "successor",
                ConversationTurn.status == "pending",
            )
            .order_by(ConversationTurn.id)
            .limit(1)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if successor is not None:
        successor.session_slot = "active"
        await db.flush()
    return successor


@dataclass(frozen=True)
class _CapTurnGraph:
    revision: TurnRevision | None
    predecessor_revision: TurnRevision | None
    successor: ConversationTurn | None
    successor_revision: TurnRevision | None
    active_memberships: tuple[TurnInputMembership, ...]
    successor_memberships: tuple[TurnInputMembership, ...]
    attempts: tuple[GenerationAttempt, ...]


async def _lock_cap_turn_graph(
    db: AsyncSession,
    *,
    session: Sessao,
    turn: ConversationTurn,
) -> _CapTurnGraph:
    """Lock cap-decision state in the ticket05 owner prefix."""

    graph = await lock_intake_turn_graph(db, sessao_id=session.id)
    active = next((row for row in graph.turns if row.id == turn.id), None)
    if active is None or active.session_slot != "active":
        raise SchedulerConflictError("cap target is no longer the active turn")
    revision = next(
        (row for row in graph.revisions if row.id == active.current_revision_id), None
    )
    predecessor_revision = next(
        (
            row
            for row in graph.revisions
            if revision is not None
            and row.id == revision.replacement_of_revision_id
        ),
        None,
    )
    successor = next(
        (row for row in graph.turns if row.session_slot == "successor"), None
    )
    successor_revision = None
    successor_memberships: tuple[TurnInputMembership, ...] = ()
    if successor is not None:
        successor_revision = next(
            (
                row
                for row in graph.revisions
                if row.id == successor.current_revision_id
            ),
            None,
        )
    turn_ids = [row.id for row in graph.turns]
    memberships = tuple(
        (
            await db.execute(
                select(TurnInputMembership)
                .where(TurnInputMembership.turn_id.in_(turn_ids))
                .order_by(TurnInputMembership.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    ) if turn_ids else ()
    active_memberships = tuple(
        row for row in memberships if row.turn_id == active.id
    )
    if successor is not None:
        successor_memberships = tuple(
            row for row in memberships if row.turn_id == successor.id
        )
    attempts = tuple(
        (
            await db.execute(
                select(GenerationAttempt)
                .where(GenerationAttempt.turn_id == turn.id)
                .order_by(GenerationAttempt.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    return _CapTurnGraph(
        revision=revision,
        predecessor_revision=predecessor_revision,
        successor=successor,
        successor_revision=successor_revision,
        active_memberships=active_memberships,
        successor_memberships=successor_memberships,
        attempts=attempts,
    )


def _handoff_cap_disposition(
    revision: TurnRevision | None,
    predecessor_revision: TurnRevision | None,
    active_memberships: tuple[TurnInputMembership, ...],
) -> Literal["ordinary", "refresh", "decided"]:
    """Classify a cap from locked immutable draft and membership state."""

    carried_handoff_intent = bool(
        revision is not None
        and revision.revision_number == 2
        # Only a newer model decision overrides the carried handoff. Local
        # fallbacks exist because generation was unavailable and therefore do
        # not reverse the predecessor's durable generated intent.
        and revision.draft_kind in {
            None,
            "soft_limit_fallback",
            "technical_fallback",
        }
        and predecessor_revision is not None
        and revision.replacement_of_revision_id == predecessor_revision.id
        and predecessor_revision.status == "superseded"
        and predecessor_revision.superseded_reason
        == "refresh_input_at_scheduler_cap"
        and predecessor_revision.draft_kind == "generated"
        and predecessor_revision.needs_handoff is True
    )
    if carried_handoff_intent:
        # No newer draft exists to supersede the durable cap-time handoff
        # intent.  Infrastructure failure before rev2 drafts must fail closed.
        return "decided"
    if (
        revision is None
        or revision.draft_kind != "generated"
        or revision.needs_handoff is not True
        or revision.status not in {"draft", "typing", "final_candidate"}
    ):
        return "ordinary"
    if revision.status == "final_candidate":
        return "decided"
    pending_refresh = tuple(
        row for row in active_memberships if row.membership_state == "active"
    )
    if revision.revision_number == 1 and pending_refresh:
        return "refresh"
    # Revision 2 has spent the only refresh.  A generated handoff draft can no
    # longer be invalidated even if corrupt data exposes another active input.
    return "decided"


async def _refresh_handoff_at_cap_locked(
    db: AsyncSession,
    *,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    graph: _CapTurnGraph,
    retry_at: datetime,
    now: datetime,
) -> None:
    """Persist the mandatory one-time refresh and release the capped owner."""

    if retry_at <= now:
        raise ValueError("retry_at must be in the future")
    if graph.revision is None:
        raise SchedulerConflictError("refresh-cap revision disappeared")
    await supersede_revision_for_refresh_locked(
        db,
        lane=lane,
        turn=turn,
        revision=graph.revision,
        reason="refresh_input_at_scheduler_cap",
    )
    # Carry the recovery count at its cap.  This is a single bounded reprieve:
    # revision 2 is never refresh-eligible, so its next cap terminates through
    # the decided-handoff or ordinary branch.
    turn.scheduling_sequence = lane.next_scheduling_sequence
    lane.next_scheduling_sequence += 1
    turn.status = "retry"
    turn.retry_at = retry_at
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    await db.flush()


async def _terminalize_handoff_cap_locked(
    db: AsyncSession,
    *,
    session: Sessao,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    graph: _CapTurnGraph,
    now: datetime,
    cap_path: str,
) -> None:
    global _handoff_cap_signal_count
    if graph.revision is None:
        raise SchedulerConflictError("handoff cap revision disappeared")
    await handoff_terminal_repo.terminalize_failed_handoff_locked(
        db,
        session=session,
        lane=lane,
        turn=turn,
        revision=graph.revision,
        successor=graph.successor,
        successor_revision=graph.successor_revision,
        now=now,
        terminal_reason=HANDOFF_CAP_TERMINAL_REASON,
    )
    _handoff_cap_signal_count += 1
    logger.error(
        "Decided handoff terminalized at scheduler cap | agente={} turn={} "
        "path={} contexto_pendente={} count={} threshold={}",
        turn.agente_id,
        turn.id,
        cap_path,
        len(graph.successor_memberships),
        _handoff_cap_signal_count,
        HANDOFF_CAP_SIGNAL_THRESHOLD,
    )


def _release_owner(lane: AgentServiceLane, turn: ConversationTurn) -> None:
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None


_orphan_effect_resolved_count = 0
_settle_waiting_live_effect_count = 0
OPEN_AUTOMATED_STATUSES = frozenset({"blocked", "pending", "processing", "retry"})


def orphan_effect_resolved_count() -> int:
    return _orphan_effect_resolved_count


def settle_waiting_live_effect_count() -> int:
    return _settle_waiting_live_effect_count


def resolve_open_automated_effects_locked(
    rows: Sequence[OutboxDelivery],
    *,
    now: datetime,
    reason: str,
    processing_status: Literal["cancelled", "dead"],
    turn_id: int,
) -> bool:
    """Resolve every non-terminal automated effect of a turn that is ending (Y1).

    Callers hold the canonical prefix and these rows locked last (id asc).
    A ``processing`` row whose lease is still live belongs to an I/O in flight:
    nothing is touched and ``False`` is returned, so the turn keeps waiting and
    the discovery settle branch re-finds it once the lease lapses (or the
    owner's own completion settles it). Otherwise queued rows are cancelled
    and an expired/absent ``processing`` lease is resolved like the lane-owned
    admission recovery: remote acceptance is unknown, so ``ambiguity_count``
    is incremented (bounded at-least-once) and the truth is never erased.
    """

    global _orphan_effect_resolved_count, _settle_waiting_live_effect_count
    if any(
        row.status == "processing"
        and row.lock_until is not None
        and row.lock_until > now
        for row in rows
    ):
        _settle_waiting_live_effect_count += 1
        logger.warning(
            "Turn settlement waits for a live automated lease | turn={} count={}",
            turn_id,
            _settle_waiting_live_effect_count,
        )
        return False
    for row in rows:
        if row.status in {"blocked", "pending", "retry"}:
            row.status = "cancelled"
            row.terminal_at = now
            row.terminal_reason = reason
        elif row.status == "processing":
            row.lease_token = None
            row.lock_owner = None
            row.lock_until = None
            row.ambiguity_count += 1
            row.last_error_code = "lease_expired_ambiguous"
            row.status = (
                "cancelled" if row.cancel_requested_at is not None else processing_status
            )
            row.terminal_at = now
            row.terminal_reason = (
                "cancel_requested" if row.cancel_requested_at is not None else reason
            )
            _orphan_effect_resolved_count += 1
            logger.error(
                "Orphan automated effect resolved as ambiguous | turn={} delivery={} "
                "effect={} status={} reason={} count={} threshold=1",
                turn_id,
                row.id,
                row.automated_effect,
                row.status,
                row.terminal_reason,
                _orphan_effect_resolved_count,
            )
    return True


async def _settle_admitted_turn_locked(
    db: AsyncSession,
    *,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    graph: _CapTurnGraph,
    scheduler: SchedulerRuntimeState,
    now: datetime,
    cap_path: str,
) -> Literal["released", "parked", "dead"] | None:
    """Lane expiry/yield of an admitted turn (ticket06 P2-1).

    Waiting for a delivery-worker claim never spends ``delivery_attempt_count``
    (only a failed send does, in ``turn_delivery_repo.fail_automated``). Below
    the cap the owner is released and the turn waits ``awaiting_delivery`` for
    discovery/requeue. At the cap a handoff parks with its staged effects
    (R5-a); an ordinary turn becomes ``dead`` and promotes its successor
    (§10.12) — it is never parked. Returns ``None`` when the turn is not an
    admitted delivery turn.
    """

    global _admitted_handoff_park_signal_count
    revision = graph.revision
    if (
        revision is None
        or revision.status != "admitted"
        or turn.status not in {"claimed", "awaiting_delivery"}
        or turn.final_revision_id != revision.id
    ):
        return None
    scheduled = (
        await db.execute(
            select(ScheduledHandoff)
            .where(
                ScheduledHandoff.conversation_turn_id == turn.id,
                ScheduledHandoff.turn_revision_id == revision.id,
                ScheduledHandoff.scheduler_epoch == turn.scheduler_epoch,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    deliveries = tuple(
        (
            await db.execute(
                select(OutboxDelivery)
                .where(
                    OutboxDelivery.conversation_turn_id == turn.id,
                    OutboxDelivery.turn_revision_id == revision.id,
                    OutboxDelivery.delivery_class == "automated",
                )
                .order_by(OutboxDelivery.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    if scheduled is None and (
        len(deliveries) != 1
        or deliveries[0].automated_effect != "reply"
        or deliveries[0].handoff_attempt_id is not None
    ):
        return None
    if scheduled is not None and not any(
        row.automated_effect == "handoff_transition"
        and row.handoff_attempt_id == scheduled.handoff_attempt_id
        for row in deliveries
    ):
        return None
    turn.retry_at = None
    _release_owner(lane, turn)
    if turn.delivery_attempt_count < scheduler.max_delivery_attempts_per_turn:
        turn.status = "awaiting_delivery"
        await db.flush()
        return "released"
    if scheduled is not None:
        turn.status = "awaiting_delivery"
        _admitted_handoff_park_signal_count += 1
        logger.error(
            "Admitted handoff parked at scheduler cap | agente={} turn={} path={} "
            "count={} threshold=1",
            turn.agente_id,
            turn.id,
            cap_path,
            _admitted_handoff_park_signal_count,
        )
        await db.flush()
        return "parked"
    # Ordinary admitted turn whose real send failures reached the cap. A live
    # processing lease is never killed (Y1): the turn waits released and is
    # settled by the owner's finalization or after the lease lapses.
    if not resolve_open_automated_effects_locked(
        deliveries,
        now=now,
        reason="delivery_attempt_cap",
        processing_status="dead",
        turn_id=turn.id,
    ):
        turn.status = "awaiting_delivery"
        await db.flush()
        return "released"
    turn.status = "dead"
    turn.session_slot = "terminal"
    turn.terminal_reason = "delivery_attempt_cap"
    turn.terminal_at = now
    await db.flush()
    await _promote_successor_after_terminal_active(db, sessao_id=turn.sessao_id)
    await db.flush()
    return "dead"


def _owner_has_unresolved_attempt(
    attempts: Sequence[GenerationAttempt], owner_token: str | None
) -> bool:
    return any(
        row.lane_owner_token == owner_token
        and row.accounting_state == "reconciliation_required"
        and row.reconciled_at is None
        for row in attempts
    )


def _drain_started_attempts(
    attempts: Sequence[GenerationAttempt],
    *,
    owner_token: str | None,
    scheduler: SchedulerRuntimeState,
    now: datetime,
) -> bool:
    """Bounded accounting drain of the expired owner's started attempts.

    Returns ``True`` while a drain is still running; a lapsed drain becomes
    ``outcome_unknown``/``reconciliation_required`` (never zero cost).
    """

    drain_pending = False
    for attempt in attempts:
        if attempt.lane_owner_token != owner_token or attempt.outcome != "started":
            continue
        if attempt.accounting_drain_until is None:
            attempt.accounting_drain_until = now + timedelta(
                seconds=scheduler.accounting_drain_seconds
            )
        if attempt.accounting_drain_until > now:
            drain_pending = True
            continue
        attempt.outcome = "outcome_unknown"
        attempt.accounting_state = "reconciliation_required"
        attempt.safe_error_code = "lane_lease_expired_unknown_outcome"
        attempt.outcome_recorded_at = now
    return drain_pending


async def _recover_lane_of_cancelled_turn(
    db: AsyncSession,
    *,
    scheduler: SchedulerRuntimeState,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    now: datetime,
) -> Literal["idle", "quarantined"]:
    """Expired lane kept by a lifecycle-cancelled turn for its accounting.

    Ticket07 cancellation keeps the lane with the owner whose generation was
    in flight. After expiry its started attempt is drained exactly like any
    expired owner; the lane is released only once nothing is unaccounted.
    """

    attempts = tuple(
        (
            await db.execute(
                select(GenerationAttempt)
                .where(GenerationAttempt.turn_id == turn.id)
                .order_by(GenerationAttempt.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    drain_pending = _drain_started_attempts(
        attempts, owner_token=lane.owner_token, scheduler=scheduler, now=now
    )
    await db.flush()
    if drain_pending or _owner_has_unresolved_attempt(attempts, lane.owner_token):
        return "quarantined"
    _release_owner(lane, turn)
    await db.flush()
    return "idle"


async def recover_expired_lane(
    db: AsyncSession,
    *,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    retry_at: datetime,
    expected_turn_id: int | None = None,
    expected_sessao_id: int | None = None,
) -> Literal["idle", "retry", "quarantined", "dead"]:
    """Recover one expired owner without allowing a second active generation.

    ``expected_turn_id``/``expected_sessao_id`` come from the committed scalar
    discovery (``turn_generation_repo.list_expired_lane_hints``); with them no
    plain read precedes a lock wait (§16.1 A1) and the locked lane must still
    point at that exact turn.
    """

    _, scheduler, _, lane = await lock_authority_prefix(
        db,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        require_enabled=False,
    )
    if scheduler.state not in {"enabled", "draining"}:
        raise SchedulerNotReadyError("scheduler cannot recover work in current state")
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    if lane.owner_token is None or lane.active_turn_id is None:
        return "idle"
    if lane.lease_until is None or lane.lease_until > now:
        raise SchedulerConflictError("lane lease has not expired")
    if expected_sessao_id is not None:
        if expected_turn_id != lane.active_turn_id:
            raise SchedulerConflictError("expired lane hint is stale")
        sessao_id = expected_sessao_id
    else:
        sessao_id = (
            await db.execute(
                select(ConversationTurn.sessao_id).where(
                    ConversationTurn.id == lane.active_turn_id
                )
            )
        ).scalar_one_or_none()
    if sessao_id is None:
        raise SchedulerConflictError("expired lane points to missing turn")
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
            .where(ConversationTurn.id == lane.active_turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        session is None
        or turn is None
        or turn.sessao_id != sessao_id
        or lane.owner_token != turn.claim_token
    ):
        raise SchedulerConflictError("expired lane points to stale turn identity")
    if turn.claim_until is None or turn.claim_until > now:
        raise SchedulerConflictError("lane and turn lease deadlines disagree")
    if turn.session_slot == "terminal":
        return await _recover_lane_of_cancelled_turn(
            db, scheduler=scheduler, lane=lane, turn=turn, now=now
        )
    cap_graph = await _lock_cap_turn_graph(db, session=session, turn=turn)
    started_attempts = tuple(
        attempt
        for attempt in cap_graph.attempts
        if attempt.lane_owner_token == lane.owner_token and attempt.outcome == "started"
    )
    if started_attempts:
        drain_pending = False
        for attempt in started_attempts:
            if attempt.accounting_drain_until is None:
                attempt.accounting_drain_until = now + timedelta(
                    seconds=scheduler.accounting_drain_seconds
                )
            if attempt.accounting_drain_until > now:
                drain_pending = True
                continue
            attempt.outcome = "outcome_unknown"
            attempt.accounting_state = "reconciliation_required"
            attempt.safe_error_code = "lane_lease_expired_unknown_outcome"
            attempt.outcome_recorded_at = now
        await db.flush()
        # The expired identity stays quarantined during the bounded accounting
        # drain and after a genuine unknown until audited resolution.
        if drain_pending:
            return "quarantined"
    # Decided from the attempts locked above, never from a consistent read.
    if _owner_has_unresolved_attempt(cap_graph.attempts, lane.owner_token):
        return "quarantined"
    admitted = await _settle_admitted_turn_locked(
        db,
        lane=lane,
        turn=turn,
        graph=cap_graph,
        scheduler=scheduler,
        now=now,
        cap_path="expired_lane",
    )
    if admitted is not None:
        return "dead" if admitted == "dead" else "idle"
    if turn.delivery_attempt_count >= scheduler.max_delivery_attempts_per_turn:
        cap_disposition = _handoff_cap_disposition(
            cap_graph.revision,
            cap_graph.predecessor_revision,
            cap_graph.active_memberships,
        )
        if cap_disposition == "refresh":
            await _refresh_handoff_at_cap_locked(
                db,
                lane=lane,
                turn=turn,
                graph=cap_graph,
                retry_at=retry_at,
                now=now,
            )
            return "retry"
        if cap_disposition == "decided":
            await _terminalize_handoff_cap_locked(
                db,
                session=session,
                lane=lane,
                turn=turn,
                graph=cap_graph,
                now=now,
                cap_path="expired_lane",
            )
            return "dead"
        turn.status = "dead"
        turn.session_slot = "terminal"
        turn.terminal_reason = "lease_recovery_attempt_cap"
        turn.terminal_at = now
        await db.flush()
        await _promote_successor_after_terminal_active(db, sessao_id=session.id)
        outcome: Literal["retry", "dead"] = "dead"
    else:
        if retry_at <= now:
            raise ValueError("retry_at must be in the future")
        turn.delivery_attempt_count += 1
        turn.scheduling_sequence = lane.next_scheduling_sequence
        lane.next_scheduling_sequence += 1
        turn.status = "retry"
        turn.retry_at = retry_at
        turn.ready_at = retry_at
        outcome = "retry"
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    await db.flush()
    return outcome


async def seal_revision_range(
    db: AsyncSession,
    *,
    turn_id: int,
    sessao_id: int,
    revision_id: int,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    owner_token: str,
    expected_input_version: int,
) -> TurnRevision:
    _, _, lane, _, turn = await _lock_owned_turn(
        db,
        turn_id=turn_id,
        sessao_id=sessao_id,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        owner_token=owner_token,
    )
    if turn.input_version != expected_input_version:
        raise SchedulerConflictError("turn input version changed")
    revision = (
        await db.execute(
            select(TurnRevision)
            .where(TurnRevision.id == revision_id, TurnRevision.turn_id == turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if revision is None or revision.scheduler_epoch != scheduler_epoch:
        raise SchedulerConflictError("revision authority changed")
    return await seal_locked_revision(db, lane=lane, turn=turn, revision=revision)


async def seal_locked_revision(
    db: AsyncSession,
    *,
    lane: AgentServiceLane,
    turn: ConversationTurn,
    revision: TurnRevision,
) -> TurnRevision:
    """Seal an already exact-owner-locked revision over its fresh input range."""

    turn_id = turn.id
    if revision.turn_id != turn_id:
        raise SchedulerConflictError("revision authority changed")
    if revision.sealed_at is not None:
        raise SchedulerConflictError("sealed revision range is immutable")
    replacement = None
    if revision.replacement_of_revision_id is not None:
        replacement = (
            await db.execute(
                select(TurnRevision)
                .where(
                    TurnRevision.id == revision.replacement_of_revision_id,
                    TurnRevision.turn_id == turn_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if replacement is None or replacement.sealed_at is None:
            raise SchedulerConflictError("replacement revision has no sealed predecessor")
    memberships = tuple(
        (
            await db.execute(
                select(TurnInputMembership)
                .where(
                    TurnInputMembership.turn_id == turn_id,
                    TurnInputMembership.membership_state.in_(("active", "sealed")),
                )
                .order_by(TurnInputMembership.input_sequence, TurnInputMembership.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    if not memberships:
        raise SchedulerConflictError("revision cannot seal an empty input range")
    sequences = [row.input_sequence for row in memberships]
    if sequences != list(range(sequences[0], sequences[-1] + 1)):
        raise SchedulerConflictError("revision input range is not contiguous")
    if replacement is not None:
        if (
            sequences[0] != replacement.sealed_first_input_sequence
            or sequences[-1] <= replacement.sealed_last_input_sequence
        ):
            raise SchedulerConflictError(
                "replacement revision must include original and new input"
            )
    revision.sealed_first_input_sequence = sequences[0]
    revision.sealed_last_input_sequence = sequences[-1]
    revision.sealed_input_count = len(memberships)
    revision.sealed_input_bytes = sum(row.content_bytes for row in memberships)
    revision.sealed_at = func.utc_timestamp(6)
    revision.status = "generating"
    for membership in memberships:
        if membership.membership_state == "active":
            membership.membership_state = "sealed"
            membership.assigned_revision_id = revision.id
    turn.current_revision_id = revision.id
    turn.status = "generating"
    lane.active_revision_id = revision.id
    await db.flush()
    await db.refresh(revision)
    return revision


async def start_generation_attempt(
    db: AsyncSession,
    *,
    turn_id: int,
    sessao_id: int,
    revision_id: int,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    owner_token: str,
    attempt_type: Literal["response", "summary"],
    modelo_llm: str,
) -> GenerationAttempt:
    _, scheduler, _, _, turn = await _lock_owned_turn(
        db,
        turn_id=turn_id,
        sessao_id=sessao_id,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        owner_token=owner_token,
    )
    revision = (
        await db.execute(
            select(TurnRevision)
            .where(TurnRevision.id == revision_id, TurnRevision.turn_id == turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if revision is None or revision.status != "generating":
        raise SchedulerConflictError("revision is not generation-ready")
    return await insert_generation_attempt(
        db,
        scheduler=scheduler,
        turn=turn,
        revision=revision,
        owner_token=owner_token,
        attempt_type=attempt_type,
        modelo_llm=modelo_llm,
    )


async def count_generation_attempts(
    db: AsyncSession,
    *,
    turn_id: int,
    attempt_type: Literal["response", "summary"],
) -> int:
    return int(
        (
            await db.execute(
                select(func.count(GenerationAttempt.id)).where(
                    GenerationAttempt.turn_id == turn_id,
                    GenerationAttempt.attempt_type == attempt_type,
                )
            )
        ).scalar_one()
        or 0
    )


async def insert_generation_attempt(
    db: AsyncSession,
    *,
    scheduler: SchedulerRuntimeState,
    turn: ConversationTurn,
    revision: TurnRevision,
    owner_token: str,
    attempt_type: Literal["response", "summary"],
    modelo_llm: str,
) -> GenerationAttempt:
    """Insert a started attempt after exact-owner turn/revision locks."""

    turn_id = turn.id
    revision_id = revision.id
    agente_id = turn.agente_id
    scheduler_epoch = turn.scheduler_epoch
    valid_status = "generating" if attempt_type == "response" else "final_candidate"
    if revision.turn_id != turn_id or revision.status != valid_status:
        raise SchedulerConflictError("revision is not generation-ready")
    attempt_count = int(
        (
            await db.execute(
                select(func.count(GenerationAttempt.id)).where(
                    GenerationAttempt.turn_id == turn_id,
                    GenerationAttempt.attempt_type == attempt_type,
                )
            )
        ).scalar_one()
        or 0
    )
    if attempt_count >= scheduler.max_generation_attempts_per_turn:
        raise SchedulerCapExceeded("generation attempt cap exceeded")
    row = GenerationAttempt(
        agente_id=agente_id,
        sessao_id=turn.sessao_id,
        turn_id=turn_id,
        revision_id=revision_id,
        scheduler_epoch=scheduler_epoch,
        lane_owner_token=owner_token,
        attempt_type=attempt_type,
        attempt_ordinal=attempt_count + 1,
        modelo_llm=modelo_llm,
    )
    db.add(row)
    await db.flush()
    return row


async def record_generation_outcome(
    db: AsyncSession,
    *,
    attempt_key: str,
    lane_owner_token: str,
    scheduler_epoch: str,
    outcome: Literal["succeeded", "failed", "outcome_unknown"],
    provider_generation_id: str | None = None,
    tokens_input: int | None = None,
    tokens_output: int | None = None,
    tokens_reasoning: int | None = None,
    tokens_cache_read: int | None = None,
    tokens_cache_write: int | None = None,
    custo_creditos: Decimal | None = None,
    consumo_llm_id: int | None = None,
    safe_error_code: str | None = None,
) -> bool:
    """Record only immutable exact-attempt accounting, even after lease loss."""

    _uuid(attempt_key, "attempt_key")
    _uuid(lane_owner_token, "lane_owner_token")
    _uuid(scheduler_epoch, "scheduler_epoch")
    row = (
        await db.execute(
            select(GenerationAttempt)
            .where(GenerationAttempt.attempt_key == attempt_key)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if row is None:
        return False
    if (
        row.lane_owner_token != lane_owner_token
        or row.scheduler_epoch != scheduler_epoch
    ):
        return False
    requested_evidence = _requested_generation_evidence(
        outcome=outcome,
        provider_generation_id=provider_generation_id,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        tokens_reasoning=tokens_reasoning,
        tokens_cache_read=tokens_cache_read,
        tokens_cache_write=tokens_cache_write,
        custo_creditos=custo_creditos,
        consumo_llm_id=consumo_llm_id,
        safe_error_code=safe_error_code,
    )
    if row.outcome != "started":
        if _stored_generation_evidence(row) != requested_evidence:
            return False
        if outcome == "succeeded" and consumo_llm_id is not None:
            return await _lock_matching_consumption(
                db,
                attempt=row,
                consumo_llm_id=consumo_llm_id,
                provider_generation_id=provider_generation_id,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                tokens_reasoning=tokens_reasoning,
                tokens_cache_read=tokens_cache_read,
                tokens_cache_write=tokens_cache_write,
                custo_creditos=custo_creditos,
            )
        return True
    if row.accounting_drain_until is not None:
        now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        if row.accounting_drain_until <= now:
            return False
    for value in (
        tokens_input,
        tokens_output,
        tokens_reasoning,
        tokens_cache_read,
        tokens_cache_write,
    ):
        if value is not None and value < 0:
            raise ValueError("generation token counts cannot be negative")
    if custo_creditos is not None and custo_creditos < 0:
        raise ValueError("generation cost cannot be negative")
    if consumo_llm_id is not None and not await _lock_matching_consumption(
        db,
        attempt=row,
        consumo_llm_id=consumo_llm_id,
        provider_generation_id=provider_generation_id,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        tokens_reasoning=tokens_reasoning,
        tokens_cache_read=tokens_cache_read,
        tokens_cache_write=tokens_cache_write,
        custo_creditos=custo_creditos,
    ):
        return False
    return await _write_generation_accounting(db, row=row, values={
        "outcome": outcome,
        "provider_generation_id": provider_generation_id,
        "tokens_input": tokens_input,
        "tokens_output": tokens_output,
        "tokens_reasoning": tokens_reasoning,
        "tokens_cache_read": tokens_cache_read,
        "tokens_cache_write": tokens_cache_write,
        "custo_creditos": custo_creditos,
        "consumo_llm_id": consumo_llm_id,
        "safe_error_code": safe_error_code,
        "accounting_state": (
            "reconciliation_required"
            if row.accounting_drain_until is not None
            else "recorded"
            if consumo_llm_id is not None
            else "reconciliation_required"
            if outcome in {"succeeded", "outcome_unknown"}
            else "pending"
        ),
        "outcome_recorded_at": func.utc_timestamp(6),
    })


async def resolve_generation_reconciliation(
    db: AsyncSession,
    *,
    attempt_key: str,
    scheduler_epoch: str,
    lane_owner_token: str,
    actor: str,
    reason: str,
    resolved_outcome: Literal["succeeded", "failed"],
    provider_generation_id: str | None = None,
    tokens_input: int | None = None,
    tokens_output: int | None = None,
    tokens_reasoning: int | None = None,
    tokens_cache_read: int | None = None,
    tokens_cache_write: int | None = None,
    custo_creditos: Decimal | None = None,
    consumo_llm_id: int | None = None,
    safe_error_code: str | None = None,
) -> bool:
    """Resolve exact accounting uncertainty without reacquiring domain authority."""

    _uuid(attempt_key, "attempt_key")
    _uuid(scheduler_epoch, "scheduler_epoch")
    _uuid(lane_owner_token, "lane_owner_token")
    actor = actor.strip()
    reason = reason.strip()
    if not actor or not reason:
        raise ValueError("reconciliation actor and reason must not be blank")
    row = (
        await db.execute(
            select(GenerationAttempt)
            .where(GenerationAttempt.attempt_key == attempt_key)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if row is None or (
        row.scheduler_epoch != scheduler_epoch
        or row.lane_owner_token != lane_owner_token
    ):
        return False
    requested_evidence = _requested_generation_evidence(
        outcome=resolved_outcome,
        provider_generation_id=provider_generation_id,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        tokens_reasoning=tokens_reasoning,
        tokens_cache_read=tokens_cache_read,
        tokens_cache_write=tokens_cache_write,
        custo_creditos=custo_creditos,
        consumo_llm_id=consumo_llm_id,
        safe_error_code=safe_error_code,
    )
    if row.reconciled_at is not None:
        if (
            row.reconciliation_actor != actor
            or row.reconciliation_reason != reason
            or _stored_generation_evidence(row) != requested_evidence
        ):
            return False
        if resolved_outcome == "succeeded":
            return await _lock_matching_consumption(
                db,
                attempt=row,
                consumo_llm_id=consumo_llm_id,
                provider_generation_id=provider_generation_id,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                tokens_reasoning=tokens_reasoning,
                tokens_cache_read=tokens_cache_read,
                tokens_cache_write=tokens_cache_write,
                custo_creditos=custo_creditos,
            )
        return True
    if row.accounting_state != "reconciliation_required":
        return False
    for value in (
        tokens_input,
        tokens_output,
        tokens_reasoning,
        tokens_cache_read,
        tokens_cache_write,
    ):
        if value is not None and value < 0:
            raise ValueError("generation token counts cannot be negative")
    if custo_creditos is not None and custo_creditos < 0:
        raise ValueError("generation cost cannot be negative")
    if resolved_outcome == "succeeded" and not await _lock_matching_consumption(
        db,
        attempt=row,
        consumo_llm_id=consumo_llm_id,
        provider_generation_id=provider_generation_id,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        tokens_reasoning=tokens_reasoning,
        tokens_cache_read=tokens_cache_read,
        tokens_cache_write=tokens_cache_write,
        custo_creditos=custo_creditos,
    ):
        return False
    if (
        resolved_outcome != "succeeded"
        and consumo_llm_id is not None
        and not await _lock_matching_consumption(
            db,
            attempt=row,
            consumo_llm_id=consumo_llm_id,
            provider_generation_id=provider_generation_id,
            tokens_input=tokens_input,
            tokens_output=tokens_output,
            tokens_reasoning=tokens_reasoning,
            tokens_cache_read=tokens_cache_read,
            tokens_cache_write=tokens_cache_write,
            custo_creditos=custo_creditos,
        )
    ):
        return False
    values: dict[str, object] = {}
    if row.outcome == "outcome_unknown":
        if safe_error_code != row.safe_error_code:
            return False
        values["outcome"] = resolved_outcome
        values["provider_generation_id"] = provider_generation_id
        values["tokens_input"] = tokens_input
        values["tokens_output"] = tokens_output
        values["tokens_reasoning"] = tokens_reasoning
        values["tokens_cache_read"] = tokens_cache_read
        values["tokens_cache_write"] = tokens_cache_write
        values["custo_creditos"] = custo_creditos
        values["consumo_llm_id"] = consumo_llm_id
    elif row.outcome != resolved_outcome:
        return False
    elif _stored_generation_evidence(row) != _requested_generation_evidence(
        outcome=resolved_outcome,
        provider_generation_id=provider_generation_id,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        tokens_reasoning=tokens_reasoning,
        tokens_cache_read=tokens_cache_read,
        tokens_cache_write=tokens_cache_write,
        custo_creditos=custo_creditos,
        consumo_llm_id=row.consumo_llm_id,
        safe_error_code=safe_error_code,
    ):
        # Known late provider evidence is immutable. Only an exact matching
        # commercial link may be attached while writing reconciliation audit.
        return False
    elif row.consumo_llm_id is None:
        values["consumo_llm_id"] = consumo_llm_id
    elif row.consumo_llm_id != consumo_llm_id:
        return False
    values["reconciled_at"] = func.utc_timestamp(6)
    values["reconciliation_actor"] = actor
    values["reconciliation_reason"] = reason
    return await _write_generation_accounting(db, row=row, values=values)


async def admit_revision(
    db: AsyncSession,
    *,
    turn_id: int,
    sessao_id: int,
    revision_id: int,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    owner_token: str,
    expected_first_input_sequence: int,
    expected_last_input_sequence: int,
) -> bool:
    _uuid(delivery_runtime_epoch, "delivery_runtime_epoch")
    runtime, _, lane, _, turn = await _lock_owned_turn(
        db,
        turn_id=turn_id,
        sessao_id=sessao_id,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        owner_token=owner_token,
    )
    revision = (
        await db.execute(
            select(TurnRevision)
            .where(TurnRevision.id == revision_id, TurnRevision.turn_id == turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        runtime.cutover_epoch != delivery_runtime_epoch
        or revision is None
        or revision.status != "final_candidate"
        or revision.scheduler_epoch != scheduler_epoch
        or revision.sealed_first_input_sequence != expected_first_input_sequence
        or revision.sealed_last_input_sequence != expected_last_input_sequence
        or lane.active_revision_id != revision_id
        or (revision.replacement_of_revision_id is not None and turn.refresh_count >= 1)
    ):
        return False
    revision.status = "admitted"
    revision.admitted_at = func.utc_timestamp(6)
    turn.final_revision_id = revision_id
    turn.status = "awaiting_delivery"
    if revision.replacement_of_revision_id is not None:
        turn.refresh_count = 1
    await db.flush()
    return True


async def yield_turn_for_retry(
    db: AsyncSession,
    *,
    turn_id: int,
    sessao_id: int,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    owner_token: str,
    retry_at: datetime,
) -> bool:
    _, scheduler, lane, session, turn = await _lock_owned_turn(
        db,
        turn_id=turn_id,
        sessao_id=sessao_id,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        owner_token=owner_token,
    )
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    if retry_at <= now:
        raise ValueError("retry_at must be in the future")
    cap_graph = await _lock_cap_turn_graph(db, session=session, turn=turn)
    if (
        await _settle_admitted_turn_locked(
            db,
            lane=lane,
            turn=turn,
            graph=cap_graph,
            scheduler=scheduler,
            now=now,
            cap_path="yield_retry",
        )
        is not None
    ):
        return False
    if turn.delivery_attempt_count >= scheduler.max_delivery_attempts_per_turn:
        cap_disposition = _handoff_cap_disposition(
            cap_graph.revision,
            cap_graph.predecessor_revision,
            cap_graph.active_memberships,
        )
        if cap_disposition == "refresh":
            await _refresh_handoff_at_cap_locked(
                db,
                lane=lane,
                turn=turn,
                graph=cap_graph,
                retry_at=retry_at,
                now=now,
            )
            return True
        if cap_disposition == "decided":
            await _terminalize_handoff_cap_locked(
                db,
                session=session,
                lane=lane,
                turn=turn,
                graph=cap_graph,
                now=now,
                cap_path="yield_retry",
            )
            return False
        turn.status = "dead"
        turn.session_slot = "terminal"
        turn.terminal_reason = "delivery_attempt_cap"
        turn.terminal_at = now
        await db.flush()
        await _promote_successor_after_terminal_active(db, sessao_id=turn.sessao_id)
    else:
        turn.delivery_attempt_count += 1
        turn.scheduling_sequence = lane.next_scheduling_sequence
        lane.next_scheduling_sequence += 1
        turn.status = "retry"
        turn.retry_at = retry_at
        turn.ready_at = retry_at
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    await db.flush()
    return turn.status == "retry"


async def complete_turn_and_release_lane(
    db: AsyncSession,
    *,
    turn_id: int,
    sessao_id: int,
    revision_id: int,
    agente_id: int,
    scheduler_epoch: str,
    delivery_runtime_epoch: str,
    owner_token: str,
    terminal_status: Literal["delivered", "handoff", "cancelled", "dead"],
    terminal_reason: str,
    promote_successor: bool,
) -> ConversationTurn:
    """Finalize only the exact owner/revision and optionally promote one successor."""

    _, _, lane, session, turn = await _lock_owned_turn(
        db,
        turn_id=turn_id,
        sessao_id=sessao_id,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=delivery_runtime_epoch,
        owner_token=owner_token,
    )
    await lock_pending_successor(db, sessao_id=session.id)
    revision = (
        await db.execute(
            select(TurnRevision)
            .where(TurnRevision.id == revision_id, TurnRevision.turn_id == turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        revision is None
        or turn.final_revision_id != revision_id
        or lane.active_revision_id != revision_id
        or revision.status != "admitted"
    ):
        raise SchedulerConflictError("terminal completion revision is stale")
    open_effect = (
        await db.execute(
            select(OutboxDelivery.id)
            .where(
                OutboxDelivery.conversation_turn_id == turn.id,
                OutboxDelivery.turn_revision_id == revision_id,
                OutboxDelivery.delivery_class == "automated",
                OutboxDelivery.status.in_(tuple(OPEN_AUTOMATED_STATUSES)),
            )
            .order_by(OutboxDelivery.id)
            .limit(1)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if open_effect is not None:
        # Fail closed (Y1): ending the turn here would orphan a staged effect;
        # automated effects settle their turn in turn_delivery_repo.
        raise SchedulerConflictError("terminal completion would orphan an automated effect")
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    turn.status = terminal_status
    turn.session_slot = "terminal"
    turn.terminal_reason = terminal_reason
    turn.terminal_at = now
    turn.claim_token = None
    turn.claim_owner = None
    turn.claim_until = None
    lane.owner_token = None
    lane.owner_worker_id = None
    lane.lease_until = None
    lane.active_turn_id = None
    lane.active_revision_id = None
    await db.flush()
    if promote_successor:
        await _promote_successor_after_terminal_active(db, sessao_id=session.id)
    else:
        orphan = (
            await db.execute(
                select(ConversationTurn.id)
                .with_hint(ConversationTurn, "FORCE INDEX (idx_turn_session_status)", "mysql")  # FU-2
                .where(
                    ConversationTurn.sessao_id == session.id,
                    ConversationTurn.session_slot == "successor",
                    ConversationTurn.status == "pending",
                )
                .limit(1)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if orphan is not None:
            # Fail closed: completing without promotion would orphan it.
            raise SchedulerConflictError("terminal completion would orphan a successor")
    return turn


AuthorityRefusal = Literal["runtime", "delivery", "lane", "cancelled", "lifecycle"]


@dataclass
class AutomatedDeliveryGraph:
    """Every authority row of one automated delivery, locked canonically."""

    runtime: DeliveryRuntimeState
    scheduler: SchedulerRuntimeState
    agent: Agente
    lane: AgentServiceLane
    session: Sessao | None
    turn: ConversationTurn | None
    revision: TurnRevision | None
    handoff_attempt: HandoffAttempt | None
    delivery: OutboxDelivery | None
    dependency: OutboxDelivery | None
    now: datetime


async def lock_automated_delivery_graph(
    db: AsyncSession,
    *,
    delivery_id: int,
    cutover_epoch: str,
    hint: AutomatedDeliveryHint,
) -> AutomatedDeliveryGraph | None:
    """Lock runtime(S) → scheduler → agent → lane → session → turn → revision
    → handoff_attempt → deliveries (id asc). Links come only from the claim
    hint; no plain read precedes the lock waits."""

    links = hint
    if (
        links.delivery_class != "automated"
        or links.automated_effect not in {"reply", "handoff_transition"}
        or links.agente_id is None
        or links.sessao_id is None
        or links.conversation_turn_id is None
        or links.turn_revision_id is None
        or links.scheduler_epoch is None
    ):
        return None
    runtime, scheduler, agent, lane = await lock_authority_prefix(
        db,
        agente_id=links.agente_id,
        scheduler_epoch=links.scheduler_epoch,
        delivery_runtime_epoch=cutover_epoch,
        require_enabled=False,
    )

    async def locked(model, ident):
        return (
            await db.execute(
                select(model)
                .where(model.id == ident)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()

    session = await locked(Sessao, links.sessao_id)
    turn = await locked(ConversationTurn, links.conversation_turn_id)
    if session is not None and turn is not None and turn.session_slot == "active":
        await lock_pending_successor(db, sessao_id=session.id)
    revision = await locked(TurnRevision, links.turn_revision_id)
    handoff_attempt = (
        await locked(HandoffAttempt, links.handoff_attempt_id)
        if links.handoff_attempt_id is not None
        else None
    )
    if links.handoff_attempt_id is not None:
        delivery_rows = tuple(
            (
                await db.execute(
                    select(OutboxDelivery)
                    .where(OutboxDelivery.handoff_attempt_id == links.handoff_attempt_id)
                    .order_by(OutboxDelivery.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            )
            .scalars()
            .all()
        )
        delivery = next((row for row in delivery_rows if row.id == delivery_id), None)
    else:
        delivery = await locked(OutboxDelivery, delivery_id)
        delivery_rows = ()
    dependency = next(
        (
            row
            for row in delivery_rows
            if delivery is not None and row.id == delivery.depends_on_delivery_id
        ),
        None,
    )
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    return AutomatedDeliveryGraph(
        runtime=runtime,
        scheduler=scheduler,
        agent=agent,
        lane=lane,
        session=session,
        turn=turn,
        revision=revision,
        handoff_attempt=handoff_attempt,
        delivery=delivery,
        dependency=dependency,
        now=now,
    )


def automated_authority_refusal(
    graph: AutomatedDeliveryGraph,
    *,
    delivery_lease_token: str,
    cutover_epoch: str,
    hint: AutomatedDeliveryHint,
    for_completion: bool = False,
) -> AuthorityRefusal | None:
    """Allowlist of the exact conditions for automated delivery authority.

    Dispatch and renewal (``for_completion=False``) require every fence:
    runtime/epochs, exact delivery ownership, cancellation, lifecycle and the
    live lane. Finalization (``for_completion=True``) records what happened to
    an I/O this owner already started, so it requires only *authority over the
    effect* — runtime gate/epochs and the exact processing lease token and
    links (legacy ``_owns`` parity). Cancellation, lifecycle and lane changes
    during the I/O cannot refuse the truth; they only decide how the turn is
    settled afterwards (``turn_delivery_repo``).
    """

    links = hint
    now = graph.now
    scheduler = graph.scheduler
    if (
        scheduler.state not in {"enabled", "draining"}
        or graph.runtime.cutover_epoch != cutover_epoch
    ):
        return "runtime"
    delivery = graph.delivery
    if (
        delivery is None
        or delivery.status != "processing"
        or delivery.lease_token != delivery_lease_token
        or (
            not for_completion
            and (delivery.lock_until is None or delivery.lock_until <= now)
        )
        or delivery.cutover_epoch != cutover_epoch
        or delivery.delivery_class != "automated"
        or delivery.automated_effect != links.automated_effect
        or delivery.agente_id != links.agente_id
        or delivery.sessao_id != links.sessao_id
        or delivery.conversation_turn_id != links.conversation_turn_id
        or delivery.turn_revision_id != links.turn_revision_id
        or delivery.handoff_attempt_id != links.handoff_attempt_id
        or delivery.scheduler_epoch != scheduler.scheduler_epoch
    ):
        return "delivery"
    if for_completion:
        return None
    if delivery.cancel_requested_at is not None:
        return "cancelled"
    session, turn, revision = graph.session, graph.turn, graph.revision
    attempt = graph.handoff_attempt
    agent = graph.agent
    if (
        session is None
        or turn is None
        or revision is None
        or not agent.ativo
        or not agent.inbox_scheduler_enabled
        or turn.status in {"delivered", "dead", "cancelled", "handoff"}
        or turn.session_slot == "terminal"
        or revision.status != "admitted"
        or turn.final_revision_id != revision.id
        or (links.handoff_attempt_id is not None and attempt is None)
    ):
        return "lifecycle"
    if attempt is None:
        if session.handler != "bot":
            return "lifecycle"
    elif (
        attempt.agente_id != links.agente_id
        or attempt.sessao_id != links.sessao_id
        or attempt.generation != session.handoff_generation
        or attempt.status not in {"pending", "ready"}
        or session.handler != "encerrada"
        or session.motivo_encerramento != "handoff"
    ):
        return "lifecycle"
    lane = graph.lane
    if (
        lane.scheduler_epoch != scheduler.scheduler_epoch
        or lane.owner_token is None
        or lane.lease_until is None
        or lane.lease_until <= now
        or lane.active_turn_id != turn.id
        or lane.active_revision_id != revision.id
        or session.id != turn.sessao_id
        or turn.agente_id != links.agente_id
        or turn.sessao_id != links.sessao_id
        or turn.scheduler_epoch != scheduler.scheduler_epoch
        or turn.claim_token != lane.owner_token
        or turn.claim_until is None
        or turn.claim_until <= now
        or turn.status != "awaiting_delivery"
        or revision.turn_id != turn.id
        or revision.scheduler_epoch != scheduler.scheduler_epoch
        or revision.sealed_first_input_sequence is None
        or revision.sealed_last_input_sequence is None
    ):
        return "lane"
    if links.automated_effect == "handoff_transition" and (
        graph.dependency is None
        or graph.dependency.status != "delivered"
        or attempt is None
        or attempt.status != "ready"
    ):
        return "lane"
    return None


async def lock_automated_delivery_authority(
    db: AsyncSession,
    *,
    delivery_id: int,
    delivery_lease_token: str,
    cutover_epoch: str,
    hint: AutomatedDeliveryHint,
    for_completion: bool = False,
) -> AutomatedDeliveryAuthority | None:
    """Re-authorize after the outbox claim transaction has committed."""

    graph = await lock_automated_delivery_graph(
        db, delivery_id=delivery_id, cutover_epoch=cutover_epoch, hint=hint
    )
    if graph is None or automated_authority_refusal(
        graph,
        delivery_lease_token=delivery_lease_token,
        cutover_epoch=cutover_epoch,
        hint=hint,
        for_completion=for_completion,
    ) is not None:
        return None
    return AutomatedDeliveryAuthority(
        delivery=graph.delivery,
        runtime=graph.runtime,
        scheduler=graph.scheduler,
        lane=graph.lane,
        session=graph.session,
        turn=graph.turn,
        revision=graph.revision,
        handoff_attempt=graph.handoff_attempt,
    )
