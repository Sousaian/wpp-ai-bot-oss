from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import exists, func, select, text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    DeliveryOperatorAction,
    DeliveryRuntimeState,
    DeliveryWorkerHeartbeat,
    LLMUsageReconciliation,
    OutboxDelivery,
)


DeliveryMode = Literal["direct", "outbox", "frozen"]


class RuntimeStateConflictError(ValueError):
    pass


@dataclass(frozen=True)
class WorkerReadiness:
    db_now: datetime
    heartbeat: datetime | None
    oldest_due: datetime | None
    runtime_mode: DeliveryMode
    runtime_epoch: str


@dataclass(frozen=True)
class WorkerFleetReadiness:
    db_now: datetime
    workers: tuple[DeliveryWorkerHeartbeat, ...]
    oldest_due: datetime | None


@dataclass(frozen=True)
class RollbackReadiness:
    nonterminal: int
    processing: int
    ambiguous_unresolved: int
    terminal_unresolved: int
    usage_unresolved: int

    @property
    def ready(self) -> bool:
        return not any(
            (
                self.nonterminal,
                self.processing,
                self.ambiguous_unresolved,
                self.terminal_unresolved,
                self.usage_unresolved,
            )
        )


async def get_database_now(db: AsyncSession) -> datetime:
    """Return the database UTC clock for transactional scheduling."""

    return (await db.execute(select(func.utc_timestamp(6)))).scalar_one()


async def get_worker_readiness(
    db: AsyncSession,
    *,
    worker_id: str,
    cutover_epoch: str | None,
) -> WorkerReadiness:
    UUID(worker_id)
    db_now = await get_database_now(db)
    heartbeat = (
        await db.execute(
            select(DeliveryWorkerHeartbeat.last_seen_at).where(
                DeliveryWorkerHeartbeat.worker_id == worker_id
            )
        )
    ).scalar_one_or_none()
    oldest = (
        await db.execute(
            select(func.min(OutboxDelivery.next_attempt_at)).where(
                OutboxDelivery.status.in_(("pending", "retry")),
                OutboxDelivery.next_attempt_at <= func.utc_timestamp(6),
                OutboxDelivery.expires_at > func.utc_timestamp(6),
                *(
                    (OutboxDelivery.cutover_epoch == cutover_epoch,)
                    if cutover_epoch
                    else ()
                ),
            )
        )
    ).scalar_one()
    state = await get_runtime_state(db)
    return WorkerReadiness(
        db_now=db_now,
        heartbeat=heartbeat,
        oldest_due=oldest,
        runtime_mode=state.mode,
        runtime_epoch=state.cutover_epoch,
    )


def validate_audit_metadata(*, actor: str, reason: str) -> tuple[str, str]:
    normalized_actor = actor.strip() if isinstance(actor, str) else ""
    normalized_reason = reason.strip() if isinstance(reason, str) else ""
    if not normalized_actor:
        raise ValueError("actor must not be blank")
    if not normalized_reason:
        raise ValueError("reason must not be blank")
    return normalized_actor, normalized_reason


async def record_heartbeat(
    db: AsyncSession,
    *,
    worker_id: str,
    code_version: str,
    current_lease_count: int,
    dispatch_enabled: bool = False,
    configured_mode: Literal["direct", "outbox"] = "direct",
    configured_epoch: str | None = None,
    provider_ready: bool = False,
    runtime_compatible: bool = False,
) -> DeliveryWorkerHeartbeat:
    if current_lease_count < 0:
        raise ValueError("current_lease_count cannot be negative")
    UUID(worker_id)
    if configured_epoch is not None:
        UUID(configured_epoch)
    now = func.utc_timestamp(6)
    stmt = mysql_insert(DeliveryWorkerHeartbeat).values(
        worker_id=worker_id,
        code_version=code_version,
        started_at=now,
        last_seen_at=now,
        current_lease_count=current_lease_count,
        dispatch_enabled=dispatch_enabled,
        configured_mode=configured_mode,
        configured_epoch=configured_epoch,
        provider_ready=provider_ready,
        runtime_compatible=runtime_compatible,
    )
    stmt = stmt.on_duplicate_key_update(
        code_version=stmt.inserted.code_version,
        last_seen_at=now,
        current_lease_count=stmt.inserted.current_lease_count,
        dispatch_enabled=stmt.inserted.dispatch_enabled,
        configured_mode=stmt.inserted.configured_mode,
        configured_epoch=stmt.inserted.configured_epoch,
        provider_ready=stmt.inserted.provider_ready,
        runtime_compatible=stmt.inserted.runtime_compatible,
    )
    await db.execute(stmt)
    await db.flush()
    row = await db.get(DeliveryWorkerHeartbeat, worker_id)
    if row is None:
        raise RuntimeError("heartbeat upsert did not produce a row")
    await db.refresh(row)
    return row


async def get_worker_fleet_readiness(
    db: AsyncSession,
    *,
    fresh_seconds: int = 60,
) -> WorkerFleetReadiness:
    if fresh_seconds < 1:
        raise ValueError("fresh_seconds must be positive")
    db_now = await get_database_now(db)
    workers = tuple(
        (
            await db.execute(
                select(DeliveryWorkerHeartbeat)
                .where(
                    DeliveryWorkerHeartbeat.last_seen_at
                    >= func.timestampadd(
                        text("SECOND"),
                        -fresh_seconds,
                        func.utc_timestamp(6),
                    )
                )
                .order_by(DeliveryWorkerHeartbeat.worker_id)
            )
        )
        .scalars()
        .all()
    )
    oldest_due = (
        await db.execute(
            select(func.min(OutboxDelivery.next_attempt_at)).where(
                OutboxDelivery.status.in_(("pending", "retry")),
                OutboxDelivery.next_attempt_at <= func.utc_timestamp(6),
                OutboxDelivery.expires_at > func.utc_timestamp(6),
            )
        )
    ).scalar_one()
    return WorkerFleetReadiness(
        db_now=db_now,
        workers=workers,
        oldest_due=oldest_due,
    )


async def get_rollback_readiness(
    db: AsyncSession,
    *,
    cutover_epoch: str | None = None,
) -> RollbackReadiness:
    if cutover_epoch is not None:
        UUID(cutover_epoch)
    resolved = exists(
        select(DeliveryOperatorAction.id).where(
            DeliveryOperatorAction.delivery_id == OutboxDelivery.id,
            DeliveryOperatorAction.action.like("rollback_resolve:%"),
            DeliveryOperatorAction.snapshot_cutover_epoch
            == OutboxDelivery.cutover_epoch,
            DeliveryOperatorAction.snapshot_status == OutboxDelivery.status,
            DeliveryOperatorAction.snapshot_attempt_count
            == OutboxDelivery.attempt_count,
            DeliveryOperatorAction.snapshot_ambiguity_count
            == OutboxDelivery.ambiguity_count,
            DeliveryOperatorAction.snapshot_terminal_at
            == OutboxDelivery.terminal_at,
        )
    )
    epoch_filter = (
        (OutboxDelivery.cutover_epoch == cutover_epoch,)
        if cutover_epoch is not None
        else ()
    )
    nonterminal = int(
        (
            await db.execute(
                select(func.count(OutboxDelivery.id)).where(
                    *epoch_filter,
                    OutboxDelivery.status.in_(("blocked", "pending", "processing", "retry")),
                )
            )
        ).scalar_one()
        or 0
    )
    processing = int(
        (
            await db.execute(
                select(func.count(OutboxDelivery.id)).where(
                    *epoch_filter, OutboxDelivery.status == "processing"
                )
            )
        ).scalar_one()
        or 0
    )
    ambiguous_unresolved = int(
        (
            await db.execute(
                select(func.count(OutboxDelivery.id)).where(
                    *epoch_filter,
                    OutboxDelivery.ambiguity_count > 0,
                    ~resolved,
                )
            )
        ).scalar_one()
        or 0
    )
    terminal_unresolved = int(
        (
            await db.execute(
                select(func.count(OutboxDelivery.id)).where(
                    *epoch_filter,
                    OutboxDelivery.status.in_(("dead", "cancelled")),
                    ~resolved,
                )
            )
        ).scalar_one()
        or 0
    )
    usage_unresolved = int(
        (
            await db.execute(
                select(func.count(LLMUsageReconciliation.id)).where(
                    LLMUsageReconciliation.status != "resolved"
                )
            )
        ).scalar_one()
        or 0
    )
    return RollbackReadiness(
        nonterminal=nonterminal,
        processing=processing,
        ambiguous_unresolved=ambiguous_unresolved,
        terminal_unresolved=terminal_unresolved,
        usage_unresolved=usage_unresolved,
    )


async def record_rollback_resolution(
    db: AsyncSession,
    *,
    delivery_id: int,
    resolution: Literal["delivered", "cancelled", "duplicate-risk-accepted"],
    actor: str,
    reason: str,
) -> DeliveryOperatorAction:
    state = await get_runtime_state(db, for_update=True)
    if state.mode != "frozen":
        raise ValueError("rollback resolution requires frozen runtime authority")
    row = (
        await db.execute(
            select(OutboxDelivery)
            .where(OutboxDelivery.id == delivery_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        raise LookupError("delivery not found")
    if row.status not in {"delivered", "dead", "cancelled"}:
        raise ValueError("only terminal deliveries can receive rollback resolution")
    if row.terminal_at is None:
        raise ValueError("terminal delivery is missing terminal evidence")
    actor, reason = validate_audit_metadata(actor=actor, reason=reason)
    action = DeliveryOperatorAction(
        delivery_id=delivery_id,
        action=f"rollback_resolve:{resolution}",
        actor=actor,
        reason=reason,
        snapshot_cutover_epoch=row.cutover_epoch,
        snapshot_status=row.status,
        snapshot_attempt_count=row.attempt_count,
        snapshot_ambiguity_count=row.ambiguity_count,
        snapshot_terminal_at=row.terminal_at,
    )
    db.add(action)
    await db.flush()
    return action


async def get_runtime_state(
    db: AsyncSession,
    *,
    for_update: bool = False,
    for_share: bool = False,
) -> DeliveryRuntimeState:
    if for_update and for_share:
        raise ValueError("runtime state cannot be locked twice")
    query = select(DeliveryRuntimeState).where(DeliveryRuntimeState.id == 1)
    if for_update:
        query = query.with_for_update()
    elif for_share:
        query = query.with_for_update(read=True)
    if for_update or for_share:
        query = query.execution_options(populate_existing=True)
    result = await db.execute(query)
    state = result.scalar_one_or_none()
    if state is None:
        raise RuntimeError("delivery runtime singleton is missing")
    return state


async def change_runtime_state(
    db: AsyncSession,
    *,
    mode: DeliveryMode,
    cutover_epoch: str,
    actor: str,
    reason: str,
    expected_version: int,
) -> DeliveryRuntimeState:
    UUID(cutover_epoch)
    actor, reason = validate_audit_metadata(actor=actor, reason=reason)
    state = await get_runtime_state(db, for_update=True)
    if state.version != expected_version:
        raise RuntimeStateConflictError("delivery runtime version changed")
    state.mode = mode
    state.cutover_epoch = cutover_epoch
    state.version += 1
    state.changed_by = actor
    state.change_reason = reason
    await db.flush()
    return state


async def record_operator_action(
    db: AsyncSession,
    *,
    delivery_id: int | None,
    action: str,
    actor: str,
    reason: str,
) -> DeliveryOperatorAction:
    actor, reason = validate_audit_metadata(actor=actor, reason=reason)
    row = DeliveryOperatorAction(
        delivery_id=delivery_id,
        action=action,
        actor=actor,
        reason=reason,
    )
    db.add(row)
    await db.flush()
    return row
