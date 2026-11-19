"""T08-3: replica heartbeats and fleet compatibility (DB access + pure policy).

Every app, delivery-worker and turn-worker process upserts one row in
``replica_heartbeats`` with the exact configuration it runs with. Readiness and
the scheduler ``enabling -> enabled`` gate compare the fresh, non-stopped rows
with each other, with the database and with the reading process.

A fresh heartbeat proves presence; a missing or stale heartbeat never proves
absence (old binaries do not write this table). The enable gate therefore also
requires the operator's explicit attestation that old replicas were removed
from ingress and dispatch.

Reads are plain consistent reads. Inside the scheduler control UoW (READ
COMMITTED) each statement sees the latest committed rows and never waits on a
heartbeat writer, which holds no other lock.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Literal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


ReplicaKind = Literal["app", "delivery_worker", "turn_worker"]
REPLICA_KINDS: tuple[ReplicaKind, ...] = ("app", "delivery_worker", "turn_worker")
# Same freshness horizon as delivery_state_repo.get_worker_fleet_readiness.
FRESH_SECONDS = 60
# Same 90-day horizon as delivery heartbeats/receipts (AGENTS.md §10.10).
RETENTION_DAYS = 90

# Allowlisted technical reason codes (no PII, no configuration values).
READINESS_REASONS = frozenset({
    "replica_state_unavailable",
    "invalid_heartbeat",
    "schema_version_unsupported",
    "schema_version_mismatch",
    "code_version_mismatch",
    "delivery_mode_mismatch",
    "delivery_epoch_mismatch",
    "scheduler_flag_mismatch",
    "scheduler_epoch_mismatch",
    "webhook_auth_mismatch",
    "dispatch_flag_mismatch",
    "turn_processor_flag_mismatch",
    "legacy_replica_detected",
    "delivery_epoch_not_current",
    "scheduler_epoch_not_current",
    "scheduler_flag_not_current",
})
GATE_REASONS = READINESS_REASONS | frozenset({
    "operator_attestation_missing",
    "replica_kind_missing:app",
    "replica_kind_missing:delivery_worker",
    "replica_kind_missing:turn_worker",
    "runtime_not_outbox",
    "delivery_mode_not_outbox",
    "delivery_epoch_not_runtime",
    "scheduler_flag_disabled",
    "scheduler_epoch_not_target",
    "webhook_auth_not_enforced",
    "dispatch_disabled",
})

# Fields every member of the fleet must share exactly (one shared .env).
_SHARED_FIELDS = (
    ("code_version", "code_version_mismatch"),
    ("schema_version", "schema_version_mismatch"),
    ("delivery_mode", "delivery_mode_mismatch"),
    ("delivery_epoch", "delivery_epoch_mismatch"),
    ("scheduler_enabled", "scheduler_flag_mismatch"),
    ("scheduler_epoch", "scheduler_epoch_mismatch"),
    ("webhook_auth_mode", "webhook_auth_mismatch"),
    ("dispatch_enabled", "dispatch_flag_mismatch"),
)


@dataclass(frozen=True)
class Fingerprint:
    code_version: str
    schema_version: int
    delivery_mode: str
    delivery_epoch: str | None
    scheduler_enabled: bool
    scheduler_epoch: str | None
    turn_processor_enabled: bool
    dispatch_enabled: bool
    webhook_auth_mode: str


@dataclass(frozen=True)
class ReplicaRow:
    replica_id: str
    replica_kind: str
    fingerprint: Fingerprint
    last_seen_at: datetime


@dataclass(frozen=True)
class FleetSnapshot:
    db_now: datetime
    replicas: tuple[ReplicaRow, ...]
    invalid_rows: int
    legacy_unmatched: int
    db_schema_version: int
    runtime_mode: str
    runtime_epoch: str
    scheduler_state: str
    scheduler_epoch: str


@dataclass(frozen=True)
class EnableAttestation:
    """Operator evidence for ``enabling -> enabled`` (flag + CLI fingerprint)."""

    old_replicas_removed: bool
    operator: Fingerprint


class ReplicaHeartbeatError(ValueError):
    pass


def _check_uuid(value: str | None, name: str) -> None:
    if value is None:
        return
    try:
        UUID(value)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ReplicaHeartbeatError(f"{name} must be a UUID") from exc


def validate_fingerprint(fingerprint: Fingerprint) -> None:
    if not isinstance(fingerprint.code_version, str) or not fingerprint.code_version:
        raise ReplicaHeartbeatError("code_version must not be blank")
    if type(fingerprint.schema_version) is not int or fingerprint.schema_version < 1:
        raise ReplicaHeartbeatError("schema_version must be a positive int")
    if fingerprint.delivery_mode not in {"direct", "outbox"}:
        raise ReplicaHeartbeatError("delivery_mode is invalid")
    if fingerprint.webhook_auth_mode not in {"provisioning", "enforced"}:
        raise ReplicaHeartbeatError("webhook_auth_mode is invalid")
    for name in ("scheduler_enabled", "turn_processor_enabled", "dispatch_enabled"):
        if type(getattr(fingerprint, name)) is not bool:
            raise ReplicaHeartbeatError(f"{name} must be a bool")
    _check_uuid(fingerprint.delivery_epoch, "delivery_epoch")
    _check_uuid(fingerprint.scheduler_epoch, "scheduler_epoch")


async def record(
    db: AsyncSession,
    *,
    replica_id: str,
    replica_kind: ReplicaKind,
    fingerprint: Fingerprint,
    stopped: bool = False,
) -> None:
    """Upsert this process' heartbeat; ``stopped`` marks a clean exit.

    A later live heartbeat clears ``stopped_at`` again (fail closed: presence).
    """

    _check_uuid(replica_id, "replica_id")
    if replica_kind not in REPLICA_KINDS:
        raise ReplicaHeartbeatError("replica_kind is invalid")
    validate_fingerprint(fingerprint)
    stopped_sql = "UTC_TIMESTAMP(6)" if stopped else "NULL"
    await db.execute(
        text(
            "INSERT INTO replica_heartbeats (replica_id, replica_kind, code_version, "
            "schema_version, delivery_mode, delivery_epoch, scheduler_enabled, "
            "scheduler_epoch, turn_processor_enabled, dispatch_enabled, "
            "webhook_auth_mode, started_at, last_seen_at, stopped_at) VALUES "
            "(:id, :kind, :code, :schema, :mode, :d_epoch, :s_enabled, :s_epoch, "
            ":processor, :dispatch, :auth, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), "
            f"{stopped_sql}) AS new ON DUPLICATE KEY UPDATE "
            "replica_kind=new.replica_kind, code_version=new.code_version, "
            "schema_version=new.schema_version, delivery_mode=new.delivery_mode, "
            "delivery_epoch=new.delivery_epoch, scheduler_enabled=new.scheduler_enabled, "
            "scheduler_epoch=new.scheduler_epoch, "
            "turn_processor_enabled=new.turn_processor_enabled, "
            "dispatch_enabled=new.dispatch_enabled, webhook_auth_mode=new.webhook_auth_mode, "
            "last_seen_at=new.last_seen_at, stopped_at=new.stopped_at"
        ),
        {
            "id": replica_id,
            "kind": replica_kind,
            "code": fingerprint.code_version,
            "schema": fingerprint.schema_version,
            "mode": fingerprint.delivery_mode,
            "d_epoch": fingerprint.delivery_epoch,
            "s_enabled": fingerprint.scheduler_enabled,
            "s_epoch": fingerprint.scheduler_epoch,
            "processor": fingerprint.turn_processor_enabled,
            "dispatch": fingerprint.dispatch_enabled,
            "auth": fingerprint.webhook_auth_mode,
        },
    )


def _row_fingerprint(row) -> Fingerprint | None:
    """Strict decode; anything unexpected is an invalid (incompatible) row."""

    try:
        fingerprint = Fingerprint(
            code_version=row.code_version,
            schema_version=row.schema_version,
            delivery_mode=row.delivery_mode,
            delivery_epoch=row.delivery_epoch,
            scheduler_enabled=_strict_bool(row.scheduler_enabled),
            scheduler_epoch=row.scheduler_epoch,
            turn_processor_enabled=_strict_bool(row.turn_processor_enabled),
            dispatch_enabled=_strict_bool(row.dispatch_enabled),
            webhook_auth_mode=row.webhook_auth_mode,
        )
        validate_fingerprint(fingerprint)
    except (ReplicaHeartbeatError, TypeError, ValueError):
        return None
    if row.replica_kind not in REPLICA_KINDS:
        return None
    return fingerprint


def _strict_bool(value: object) -> bool:
    if type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise ValueError("boolean column holds a non-boolean value")


async def load_fleet(db: AsyncSession) -> FleetSnapshot:
    """Fresh live replicas, unmatched legacy delivery workers and DB authority."""

    db_now = (await db.execute(text("SELECT UTC_TIMESTAMP(6)"))).scalar_one()
    rows = (
        await db.execute(
            text(
                "SELECT replica_id, replica_kind, code_version, schema_version, "
                "delivery_mode, delivery_epoch, scheduler_enabled, scheduler_epoch, "
                "turn_processor_enabled, dispatch_enabled, webhook_auth_mode, "
                "last_seen_at FROM replica_heartbeats "
                "WHERE stopped_at IS NULL AND last_seen_at >= "
                "TIMESTAMPADD(SECOND, -:fresh, :now) ORDER BY replica_id"
            ),
            {"fresh": FRESH_SECONDS, "now": db_now},
        )
    ).all()
    replicas: list[ReplicaRow] = []
    invalid = 0
    for row in rows:
        fingerprint = _row_fingerprint(row)
        if fingerprint is None:
            invalid += 1
            continue
        replicas.append(
            ReplicaRow(
                replica_id=row.replica_id,
                replica_kind=row.replica_kind,
                fingerprint=fingerprint,
                last_seen_at=row.last_seen_at,
            )
        )
    # A fresh legacy delivery heartbeat is accounted for only by a replica row
    # of the same process that is itself fresh or cleanly stopped. Anything
    # else is an unknown (possibly old) dispatcher.
    legacy_unmatched = int(
        (
            await db.execute(
                text(
                    "SELECT COUNT(*) FROM delivery_worker_heartbeats AS w "
                    "WHERE w.last_seen_at >= TIMESTAMPADD(SECOND, -:fresh, :now) "
                    "AND NOT EXISTS (SELECT 1 FROM replica_heartbeats AS r "
                    "WHERE r.replica_id = w.worker_id "
                    "AND r.replica_kind = 'delivery_worker' "
                    "AND (r.stopped_at IS NOT NULL OR r.last_seen_at >= "
                    "TIMESTAMPADD(SECOND, -:fresh, :now)))"
                ),
                {"fresh": FRESH_SECONDS, "now": db_now},
            )
        ).scalar_one()
    )
    authority = (
        await db.execute(
            text(
                "SELECT s.schema_version, s.state, s.scheduler_epoch, "
                "d.mode, d.cutover_epoch FROM scheduler_runtime_state AS s "
                "JOIN delivery_runtime_state AS d ON d.id = 1 WHERE s.id = 1"
            )
        )
    ).one()
    return FleetSnapshot(
        db_now=db_now,
        replicas=tuple(replicas),
        invalid_rows=invalid,
        legacy_unmatched=legacy_unmatched,
        db_schema_version=int(authority.schema_version),
        runtime_mode=authority.mode,
        runtime_epoch=authority.cutover_epoch,
        scheduler_state=authority.state,
        scheduler_epoch=authority.scheduler_epoch,
    )


async def health_scheduler_state(db: AsyncSession) -> str:
    """Health-only plain read, before fleet reads in the same snapshot.

    Never used by the enable gate, which reads authority after its locks.
    """

    return (await db.execute(text(
        "SELECT state FROM scheduler_runtime_state WHERE id = 1"
    ))).scalar_one()


async def db_schema_version(db: AsyncSession) -> int:
    return int(
        (
            await db.execute(
                text("SELECT schema_version FROM scheduler_runtime_state WHERE id = 1")
            )
        ).scalar_one()
    )


async def own_heartbeat_fresh(db: AsyncSession, *, replica_id: str) -> bool:
    _check_uuid(replica_id, "replica_id")
    return bool(
        (
            await db.execute(
                text(
                    "SELECT COUNT(*) FROM replica_heartbeats WHERE replica_id = :id "
                    "AND stopped_at IS NULL AND last_seen_at >= "
                    "TIMESTAMPADD(SECOND, -:fresh, UTC_TIMESTAMP(6))"
                ),
                {"id": replica_id, "fresh": FRESH_SECONDS},
            )
        ).scalar_one()
    )


async def purge_expired(db: AsyncSession, *, limit: int = 500) -> int:
    """Delete heartbeats unseen for RETENTION_DAYS, by primary key.

    Discovery is a bounded plain read (read view closed before the delete);
    the delete re-checks age per id, so a row revived meanwhile is kept.
    """

    ids = await expired_ids(db, limit=limit)
    await db.rollback()
    return await delete_if_expired(db, ids)


async def expired_ids(db: AsyncSession, *, limit: int) -> list[str]:
    if limit < 1:
        raise ValueError("limit must be positive")
    return [
        row[0]
        for row in (
            await db.execute(
                text(
                    "SELECT replica_id FROM replica_heartbeats FORCE INDEX "
                    "(idx_replica_heartbeats_last_seen) WHERE last_seen_at < "
                    "TIMESTAMPADD(DAY, -:days, UTC_TIMESTAMP(6)) "
                    "ORDER BY last_seen_at, replica_id LIMIT :limit"
                ),
                {"days": RETENTION_DAYS, "limit": limit},
            )
        ).all()
    ]


async def delete_if_expired(db: AsyncSession, ids: Iterable[str]) -> int:
    removed = 0
    for replica_id in ids:
        result = await db.execute(
            text(
                "DELETE FROM replica_heartbeats WHERE replica_id = :id AND "
                "last_seen_at < TIMESTAMPADD(DAY, -:days, UTC_TIMESTAMP(6))"
            ),
            {"id": replica_id, "days": RETENTION_DAYS},
        )
        removed += int(result.rowcount or 0)
    return removed


# ── pure policy ─────────────────────────────────────────────────────────
def _members(snapshot: FleetSnapshot, reader: Fingerprint | None) -> list[Fingerprint]:
    members = [row.fingerprint for row in snapshot.replicas]
    if reader is not None:
        members.append(reader)
    return members


def readiness_reasons(
    snapshot: FleetSnapshot, *, reader: Fingerprint | None
) -> tuple[str, ...]:
    """Reasons the fresh fleet (plus the reading process) is incompatible."""

    reasons: set[str] = set()
    if snapshot.invalid_rows:
        reasons.add("invalid_heartbeat")
    if snapshot.legacy_unmatched:
        reasons.add("legacy_replica_detected")
    if reader is not None and reader.schema_version != snapshot.db_schema_version:
        reasons.add("schema_version_unsupported")
    if any(
        row.fingerprint.schema_version != snapshot.db_schema_version
        for row in snapshot.replicas
    ):
        reasons.add("schema_version_mismatch")
    members = _members(snapshot, reader)
    for field, reason in _SHARED_FIELDS:
        if len({getattr(member, field) for member in members}) > 1:
            reasons.add(reason)
    if any(
        row.replica_kind == "turn_worker" and not row.fingerprint.turn_processor_enabled
        for row in snapshot.replicas
    ):
        reasons.add("turn_processor_flag_mismatch")
    if snapshot.runtime_mode == "outbox" and any(
        member.delivery_mode == "outbox"
        and member.delivery_epoch != snapshot.runtime_epoch
        for member in members
    ):
        reasons.add("delivery_epoch_not_current")
    if snapshot.scheduler_state != "disabled" and any(
        member.scheduler_enabled and member.scheduler_epoch != snapshot.scheduler_epoch
        for member in members
    ):
        reasons.add("scheduler_epoch_not_current")
    if snapshot.scheduler_state in {"enabled", "draining"} and any(
        not member.scheduler_enabled for member in members
    ):
        reasons.add("scheduler_flag_not_current")
    return tuple(sorted(reasons))


def enable_gate_reasons(
    snapshot: FleetSnapshot,
    *,
    attestation: EnableAttestation | None,
    target_epoch: str,
) -> tuple[str, ...]:
    """Every reason ``enabling -> enabled`` must refuse; empty means allowed."""

    operator = attestation.operator if attestation is not None else None
    reasons = set(readiness_reasons(snapshot, reader=operator))
    if attestation is None or attestation.old_replicas_removed is not True:
        reasons.add("operator_attestation_missing")
    present = {row.replica_kind for row in snapshot.replicas}
    for kind in REPLICA_KINDS:
        if kind not in present:
            reasons.add(f"replica_kind_missing:{kind}")
    if snapshot.runtime_mode != "outbox":
        reasons.add("runtime_not_outbox")
    for member in _members(snapshot, operator):
        if member.delivery_mode != "outbox":
            reasons.add("delivery_mode_not_outbox")
        if member.delivery_epoch != snapshot.runtime_epoch:
            reasons.add("delivery_epoch_not_runtime")
        if not member.scheduler_enabled:
            reasons.add("scheduler_flag_disabled")
        if member.scheduler_epoch != target_epoch:
            reasons.add("scheduler_epoch_not_target")
        if member.webhook_auth_mode != "enforced":
            reasons.add("webhook_auth_not_enforced")
        if not member.dispatch_enabled:
            reasons.add("dispatch_disabled")
    return tuple(sorted(reasons))


def kind_counts(replicas: Iterable[ReplicaRow]) -> dict[str, int]:
    counts = {kind: 0 for kind in REPLICA_KINDS}
    for row in replicas:
        counts[row.replica_kind] = counts.get(row.replica_kind, 0) + 1
    return counts
