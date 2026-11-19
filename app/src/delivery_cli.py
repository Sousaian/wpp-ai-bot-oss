from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import timedelta
import json
from pathlib import Path
import sys
from uuid import UUID

from src import agent_scheduler_cli  # T08-6
from src.utils.image_version import require_runtime_version
from src.config import settings
from src.database.connection import AsyncSessionLocal, engine
from src.database.models import OutboxDelivery
from src.database.repositories import (
    delivery_state_repo,
    delivery_worker_repo,
    lifecycle_repo,
    scheduler_repo,
    scheduler_diagnostics_repo,  # S2
)
from sqlalchemy import select
from src.services.delivery_adapters import (
    DuplicateRemoteEffect,
    InvalidDeliveryPayload,
    create_or_reconcile_chatwoot_context_note,
    decode_chatwoot_context_note,
    decode_chatwoot_handoff,
    open_or_reconcile_chatwoot_handoff,
)
from src.services import scheduler_observability
from src.services.provider_auth_bootstrap import (
    provider_auth_readiness,
    reset_provider_auth_snapshot_cache,
)
from src.database.repositories import replica_repo
from src.services import replica_compat
from src.utils.logger import setup_logger


def _emit(value: object) -> None:
    print(json.dumps(value, sort_keys=True, default=str))


def _common_actor_reason(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--actor", required=True, type=_nonblank_metadata)
    parser.add_argument("--reason", required=True, type=_nonblank_metadata)


def _nonblank_metadata(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("value must not be blank")
    return normalized


def _uuid_text(value: str) -> str:
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise argparse.ArgumentTypeError("value must be a UUID") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Container-only durable delivery operations"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    listing = subparsers.add_parser("list")
    listing.add_argument("--status", action="append", default=[])
    listing.add_argument("--limit", type=int, default=100)

    retry = subparsers.add_parser("retry")
    retry.add_argument("delivery_id", type=int)
    retry.add_argument("--ttl-seconds", type=int, default=1800)
    _common_actor_reason(retry)

    cancel = subparsers.add_parser("cancel")
    cancel.add_argument("delivery_id", type=int)
    _common_actor_reason(cancel)

    reconcile = subparsers.add_parser("reconcile")
    reconcile.add_argument("delivery_id", type=int)
    _common_actor_reason(reconcile)

    resolve_note = subparsers.add_parser("resolve-context-note")
    resolve_note.add_argument("delivery_id", type=int)
    resolve_note.add_argument(
        "--resolution",
        required=True,
        choices=("delivered", "retry-safe", "cancelled"),
    )
    resolve_note.add_argument("--ttl-seconds", type=int, default=1800)
    _common_actor_reason(resolve_note)

    turn_resolve = subparsers.add_parser("turn-resolve")
    turn_resolve.add_argument("turn_id", type=int)
    turn_resolve.add_argument("--resolution", required=True, choices=("retry", "abandon"))
    turn_resolve.add_argument("--ttl-seconds", type=int, default=1800)
    turn_resolve.add_argument("--accept-duplicate-risk", action="store_true")
    _common_actor_reason(turn_resolve)

    q_list = subparsers.add_parser("quarantine-list")
    q_list.add_argument("--agente-id", type=int)
    q_list.add_argument("--limit", type=int, default=100)

    q_inspect = subparsers.add_parser("quarantine-inspect")
    q_inspect.add_argument("attempt_key", type=_uuid_text)

    q_resolve = subparsers.add_parser("quarantine-resolve")
    q_resolve.add_argument("attempt_key", type=_uuid_text)
    q_resolve.add_argument("--outcome", required=True, choices=("succeeded", "failed"))
    q_resolve.add_argument(
        "--evidence", required=True, choices=sorted(lifecycle_repo.QUARANTINE_EVIDENCE)
    )
    q_resolve.add_argument("--consumo-id", type=int)
    q_resolve.add_argument("--accept-duplicate-spend-risk", action="store_true")
    _common_actor_reason(q_resolve)

    # T08-1: internal audited state machine, no HTTP endpoint.
    scheduler = subparsers.add_parser("scheduler-state")
    states = scheduler.add_subparsers(dest="scheduler_command", required=True)
    states.add_parser("show")
    for target in scheduler_repo.SCHEDULER_TRANSITIONS:
        command = states.add_parser(target)
        command.add_argument("--epoch", required=True, type=_uuid_text)
        command.add_argument("--expected-version", required=True, type=int)
        _common_actor_reason(command)
        if target == "enabled":
            # T08-3: explicit, audited operator attestation (gate refuses without it).
            command.add_argument("--attest-old-replicas-removed", action="store_true")

    # T08-6: guarded per-agent opt-in/opt-out, audited, no HTTP endpoint.
    agent_scheduler_cli.register(
        subparsers, uuid_text=_uuid_text, actor_reason=_common_actor_reason
    )
    # end T08-6

    subparsers.add_parser("health")

    cutover = subparsers.add_parser("cutover")
    cutover.add_argument("--epoch", required=True, type=_uuid_text)
    cutover.add_argument("--expected-version", required=True, type=int)
    _common_actor_reason(cutover)

    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--expected-version", required=True, type=int)
    _common_actor_reason(freeze)

    check = subparsers.add_parser("rollback-check")
    check.add_argument("--epoch", type=_uuid_text)

    resolve = subparsers.add_parser("resolve-rollback")
    resolve.add_argument("delivery_id", type=int)
    resolve.add_argument(
        "--resolution",
        required=True,
        choices=("delivered", "cancelled", "duplicate-risk-accepted"),
    )
    _common_actor_reason(resolve)

    restore = subparsers.add_parser("restore-direct")
    restore.add_argument("--epoch", required=True, type=_uuid_text)
    restore.add_argument("--expected-version", required=True, type=int)
    _common_actor_reason(restore)
    return parser


async def _list(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as db:
        rows = await delivery_worker_repo.list_metadata(
            db,
            statuses=tuple(args.status) or None,
            limit=args.limit,
        )
    _emit([asdict(row) for row in rows])
    return 0


async def _retry(args: argparse.Namespace) -> int:
    if args.ttl_seconds < 1 or args.ttl_seconds > 86400:
        raise ValueError("ttl-seconds must be between 1 and 86400")
    async with AsyncSessionLocal() as db:
        row = await delivery_worker_repo.operator_retry(
            db,
            delivery_id=args.delivery_id,
            actor=args.actor,
            reason=args.reason,
            ttl_seconds=args.ttl_seconds,
        )
        await db.commit()
    _emit(
        {
            "delivery_id": row.id,
            "status": row.status,
            "warning": (
                "Evolution replay may duplicate a previously accepted message"
                if row.provider == "evolution"
                else None
            ),
        }
    )
    return 0


async def _cancel(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as db:
        delivery_class = (
            await db.execute(
                select(OutboxDelivery.delivery_class).where(
                    OutboxDelivery.id == args.delivery_id
                )
            )
        ).scalar_one_or_none()
    if delivery_class == "automated":
        # Canonical lane prefix + same-UoW turn settlement (ticket07).
        row = await lifecycle_repo.run_in_new_session(
            AsyncSessionLocal,
            lambda db: lifecycle_repo.operator_cancel_automated(
                db, delivery_id=args.delivery_id, actor=args.actor, reason=args.reason
            ),
        )
        _emit({"delivery_id": row.id, "status": row.status})
        return 0
    async with AsyncSessionLocal() as db:
        row = await delivery_worker_repo.operator_cancel(
            db,
            delivery_id=args.delivery_id,
            actor=args.actor,
            reason=args.reason,
        )
        await db.commit()
    _emit({"delivery_id": row.id, "status": row.status})
    return 0


async def _turn_resolve(args: argparse.Namespace) -> int:
    result = await lifecycle_repo.run_in_new_session(
        AsyncSessionLocal,
        lambda db: lifecycle_repo.resolve_parked_turn(
            db,
            turn_id=args.turn_id,
            resolution=args.resolution,
            actor=args.actor,
            reason=args.reason,
            ttl_seconds=args.ttl_seconds,
            accept_duplicate_risk=args.accept_duplicate_risk,
        ),
    )
    _emit(
        {
            "turn_id": result.turn_id,
            "resolution": result.resolution,
            "status": result.status,
            "rearmed": list(result.rearmed),
            "warning": (
                "Evolution replay may duplicate a previously accepted message"
                if result.rearmed
                else None
            ),
        }
    )
    return 0


async def _quarantine_list(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as db:
        rows = await lifecycle_repo.list_generation_quarantine(
            db, agente_id=args.agente_id, limit=args.limit
        )
    _emit([asdict(row) for row in rows])
    return 0


async def _quarantine_inspect(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as db:
        _emit(await lifecycle_repo.inspect_generation_attempt(db, attempt_key=args.attempt_key))
    return 0


async def _quarantine_resolve(args: argparse.Namespace) -> int:
    result = await lifecycle_repo.run_in_new_session(
        AsyncSessionLocal,
        lambda db: lifecycle_repo.resolve_generation_quarantine(
            db,
            attempt_key=args.attempt_key,
            outcome=args.outcome,
            evidence=args.evidence,
            actor=args.actor,
            note=args.reason,
            consumo_llm_id=args.consumo_id,
            accept_duplicate_spend_risk=args.accept_duplicate_spend_risk,
        ),
    )
    _emit(asdict(result))
    return 0


async def _reconcile(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as db:
        claimed = await delivery_worker_repo.claim_for_operator_reconciliation(
            db,
            delivery_id=args.delivery_id,
            actor=args.actor,
            reason=args.reason,
            lease_seconds=settings.delivery_chatwoot_lease_seconds,
        )
        if claimed.operation == "chatwoot.add_private_note.v1":
            payload = decode_chatwoot_context_note(
                claimed.operation, claimed.payload_version, claimed.payload
            )
            if claimed.sessao_id is None or claimed.handoff_attempt_id is None:
                raise InvalidDeliveryPayload("context note lacks lifecycle links")
            ownership = await delivery_worker_repo.get_context_note_dispatch_ownership(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                sessao_id=claimed.sessao_id,
                attempt_id=claimed.handoff_attempt_id,
                source_id=payload.source_id,
                coverage=payload.coverage,
                authority="operator",
            )
        else:
            payload = decode_chatwoot_handoff(
                claimed.operation, claimed.payload_version, claimed.payload
            )
            ownership = await delivery_worker_repo.get_handoff_dispatch_ownership(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                authority="operator",
            )
        await db.commit()
    try:
        if (
            ownership is None
            or ownership.handoff_key != payload.handoff_key
            or ownership.inbox_id != payload.inbox_id
        ):
            raise InvalidDeliveryPayload("Chatwoot handoff ownership mismatch")
        if claimed.operation == "chatwoot.add_private_note.v1":
            result = await create_or_reconcile_chatwoot_context_note(
                payload,
                contact_id=ownership.contact_id,
                conversation_id=ownership.conversation_id,
                timeout_seconds=float(settings.delivery_chatwoot_deadline_seconds),
                reconcile_only=True,
            )
        else:
            result = await open_or_reconcile_chatwoot_handoff(
                payload,
                delivery_key=claimed.delivery_key,
                timeout_seconds=float(settings.delivery_chatwoot_deadline_seconds),
                reconcile_only=True,
            )
    except Exception as exc:
        duplicate = isinstance(exc, DuplicateRemoteEffect)
        invalid = isinstance(exc, InvalidDeliveryPayload)
        async with AsyncSessionLocal() as db:
            outcome = await delivery_worker_repo.fail_processing(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                error_code=(
                    "duplicate_remote_effect"
                    if duplicate
                    else "operator_reconciliation_failed"
                ),
                http_status=None,
                retry_delay_seconds=None if duplicate or invalid or claimed.operation == "chatwoot.add_private_note.v1" else 5,
                ambiguous=not invalid,
                authority="operator",
            )
            await db.commit()
        _emit(
            {
                "delivery_id": claimed.id,
                "status": outcome,
                "matches": "multiple" if duplicate else "unknown",
            }
        )
        return 3 if duplicate or invalid else 2
    if result is None:
        async with AsyncSessionLocal() as db:
            outcome = await delivery_worker_repo.fail_processing(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                error_code="marker_not_found",
                http_status=None,
                retry_delay_seconds=(
                    None
                    if claimed.operation == "chatwoot.add_private_note.v1"
                    else 5
                ),
                ambiguous=claimed.operation == "chatwoot.add_private_note.v1",
                authority="operator",
            )
            await db.commit()
        _emit({"delivery_id": claimed.id, "status": outcome, "matches": 0})
        return 2
    async with AsyncSessionLocal() as db:
        if claimed.operation == "chatwoot.add_private_note.v1":
            accepted = await delivery_worker_repo.complete_chatwoot_context_note(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                sessao_id=int(claimed.sessao_id),
                attempt_id=int(claimed.handoff_attempt_id),
                source_id=payload.source_id,
                coverage=payload.coverage,
                external_id=str(result.external_id),
                http_status=result.http_status,
                next_chunk_ttl_seconds=settings.delivery_handoff_setup_ttl_seconds,
                authority="operator",
            )
        else:
            accepted = await delivery_worker_repo.complete_chatwoot_handoff(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                contact_id=int(result.contact_id),
                conversation_id=int(result.conversation_id),
                http_status=result.http_status,
                transition_ttl_seconds=settings.delivery_transition_ttl_seconds,
                authority="operator",
            )
        await db.commit()
    _emit(
        {
            "delivery_id": claimed.id,
            "status": "delivered" if accepted else "stale",
            "matches": 1,
        }
    )
    return 0 if accepted else 4


async def _resolve_context_note(args: argparse.Namespace) -> int:
    if args.ttl_seconds < 1 or args.ttl_seconds > 86400:
        raise ValueError("ttl-seconds must be between 1 and 86400")
    async with AsyncSessionLocal() as db:
        row = await delivery_worker_repo.resolve_context_note_ambiguity(
            db,
            delivery_id=args.delivery_id,
            resolution=args.resolution,
            actor=args.actor,
            reason=args.reason,
            ttl_seconds=args.ttl_seconds,
        )
        await db.commit()
    _emit(
        {
            "delivery_id": row.id,
            "status": row.status,
            "resolution": args.resolution,
        }
    )
    return 0


async def _health() -> int:
    try:
        snapshot = json.loads(
            Path(settings.delivery_worker_health_path).read_text()
        )
        worker_id = snapshot["worker_id"]
        if not isinstance(worker_id, str):
            raise ValueError("worker_id is invalid")
        maintenance_skips = snapshot.get("maintenance_consecutive_skips", 0)
        maintenance_threshold = snapshot.get(
            "maintenance_skip_error_threshold",
            settings.delivery_maintenance_skip_error_threshold,
        )
        maintenance_snapshot_healthy = snapshot.get(
            "maintenance_healthy",
            maintenance_skips < maintenance_threshold,
        )
        if (
            type(maintenance_skips) is not int
            or maintenance_skips < 0
            or type(maintenance_threshold) is not int
            or maintenance_threshold < 1
            or type(maintenance_snapshot_healthy) is not bool
        ):
            raise ValueError("maintenance skip snapshot is invalid")
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        _emit({"healthy": False, "reason": "local_snapshot_unavailable"})
        return 1

    async with AsyncSessionLocal() as db:
        readiness = await delivery_state_repo.get_worker_readiness(
            db,
            worker_id=worker_id,
            cutover_epoch=settings.delivery_cutover_epoch,
        )
    heartbeat_ok = bool(
        readiness.heartbeat
        and readiness.db_now - readiness.heartbeat <= timedelta(seconds=60)
    )
    backlog_ok = bool(
        not settings.delivery_dispatch_enabled
        or readiness.oldest_due is None
        or readiness.db_now - readiness.oldest_due <= timedelta(minutes=5)
    )
    if settings.delivery_mode == "direct":
        runtime_ok = readiness.runtime_mode in {"direct", "frozen"}
    else:
        runtime_ok = bool(
            settings.delivery_cutover_epoch
            and readiness.runtime_mode == "outbox"
            and readiness.runtime_epoch == settings.delivery_cutover_epoch
        )
    provider_ok = bool(snapshot.get("provider_ready"))
    maintenance_ok = bool(
        maintenance_snapshot_healthy
        and maintenance_skips < maintenance_threshold
    )
    local_ready = bool(
        snapshot.get("ready")
        and snapshot.get("runtime_compatible")
        and provider_ok
        and maintenance_ok
    )
    # T08-5: container-only aggregate scheduler block (epoch value included);
    # unreadable metrics fail closed only while the DB scheduler is active.
    scheduler = await scheduler_observability.scheduler_health(
        AsyncSessionLocal, configured_epoch=settings.inbox_scheduler_epoch, include_epoch=True,
    )
    # T08-3: fresh replicas must agree in code/schema version, epochs and flags.
    replicas = await replica_compat.fleet_report(
        AsyncSessionLocal, reader=replica_compat.local_fingerprint()
    )
    healthy = bool(
        heartbeat_ok and backlog_ok and runtime_ok and local_ready and scheduler["healthy"]
    )
    healthy = healthy and replicas.healthy
    _emit(
        {
            "healthy": healthy,
            "scheduler": scheduler,
            "replica_compatibility": replicas.as_dict(),
            "heartbeat_fresh": heartbeat_ok,
            "backlog_within_slo": backlog_ok,
            "provider_ready": provider_ok,
            "runtime_compatible": runtime_ok,
            "maintenance_healthy": maintenance_ok,
            "maintenance_consecutive_skips": maintenance_skips,
            "maintenance_skip_error_threshold": maintenance_threshold,
            "worker_id": worker_id,
            # P3-1: diagnostics only; candidate signals never change readiness.
            "retention_task": snapshot.get("retention_task"),
            "scheduler_retention_health": snapshot.get("scheduler_retention_health"),
        }
    )
    return 0 if healthy else 1


async def _cutover(args: argparse.Namespace) -> int:
    if settings.delivery_mode != "direct" or settings.delivery_dispatch_enabled:
        raise ValueError("cutover command requires direct mode with dispatch disabled")
    reset_provider_auth_snapshot_cache()
    provider = await provider_auth_readiness()
    if not provider.ready:
        raise ValueError("provider readiness is not valid")
    async with AsyncSessionLocal() as db:
        state = await delivery_state_repo.get_runtime_state(db, for_update=True)
        if state.mode != "direct":
            raise ValueError("cutover requires DB runtime mode direct")
        if state.cutover_epoch == args.epoch:
            raise ValueError("cutover requires a new epoch")
        fleet = await delivery_state_repo.get_worker_fleet_readiness(db)
        if not fleet.workers:
            raise ValueError("cutover requires a fresh idle worker heartbeat")
        if any(
            worker.dispatch_enabled
            or worker.current_lease_count
            or worker.configured_mode != "direct"
            or not worker.runtime_compatible
            for worker in fleet.workers
        ):
            raise ValueError("cutover worker fleet is not idle and direct-compatible")
        if fleet.oldest_due is not None:
            raise ValueError("cutover requires no due outbox backlog")
        unresolved = await delivery_state_repo.get_rollback_readiness(
            db
        )
        if not unresolved.ready:
            raise ValueError("cutover requires zero unresolved delivery state")
        updated = await delivery_state_repo.change_runtime_state(
            db,
            mode="outbox",
            cutover_epoch=args.epoch,
            actor=args.actor,
            reason=args.reason,
            expected_version=args.expected_version,
        )
        await delivery_state_repo.record_operator_action(
            db,
            delivery_id=None,
            action="cutover",
            actor=args.actor,
            reason=args.reason,
        )
        await db.commit()
    _emit(
        {
            "status": "db_cutover_recorded",
            "mode": updated.mode,
            "epoch": updated.cutover_epoch,
            "version": updated.version,
            "dispatch_enabled": False,
            "next": "restart app/worker with matching outbox epoch before enabling dispatch",
        }
    )
    return 0


async def _freeze(args: argparse.Namespace) -> int:
    if settings.delivery_dispatch_enabled:
        raise ValueError("freeze requires dispatch disabled in operator environment")
    async with AsyncSessionLocal() as db:
        state = await delivery_state_repo.get_runtime_state(db, for_update=True)
        if state.mode != "outbox":
            raise ValueError("freeze requires DB runtime mode outbox")
        fleet = await delivery_state_repo.get_worker_fleet_readiness(db)
        if not fleet.workers or any(
            worker.dispatch_enabled or worker.current_lease_count
            for worker in fleet.workers
        ):
            raise ValueError("freeze requires fresh dispatch-disabled workers with no leases")
        rollback = await delivery_state_repo.get_rollback_readiness(
            db
        )
        if rollback.processing:
            raise ValueError("freeze requires every processing row to settle")
        updated = await delivery_state_repo.change_runtime_state(
            db,
            mode="frozen",
            cutover_epoch=state.cutover_epoch,
            actor=args.actor,
            reason=args.reason,
            expected_version=args.expected_version,
        )
        await delivery_state_repo.record_operator_action(
            db,
            delivery_id=None,
            action="freeze",
            actor=args.actor,
            reason=args.reason,
        )
        await db.commit()
    _emit({"status": "frozen", "epoch": updated.cutover_epoch, "version": updated.version})
    return 0


async def _rollback_check(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as db:
        state = await delivery_state_repo.get_runtime_state(db)
        check = await delivery_state_repo.get_rollback_readiness(
            db, cutover_epoch=args.epoch
        )
    payload = asdict(check)
    payload.update(
        {
            "ready": check.ready,
            "epoch": args.epoch or "all",
            "runtime_mode": state.mode,
        }
    )
    _emit(payload)
    return 0 if check.ready and state.mode == "frozen" else 2


async def _resolve_rollback(args: argparse.Namespace) -> int:
    async with AsyncSessionLocal() as db:
        action = await delivery_state_repo.record_rollback_resolution(
            db,
            delivery_id=args.delivery_id,
            resolution=args.resolution,
            actor=args.actor,
            reason=args.reason,
        )
        await db.commit()
    _emit(
        {
            "status": "resolved_for_rollback",
            "delivery_id": args.delivery_id,
            "action_id": action.id,
            "resolution": args.resolution,
        }
    )
    return 0


async def _restore_direct(args: argparse.Namespace) -> int:
    if settings.delivery_dispatch_enabled:
        raise ValueError("direct restoration requires dispatch disabled")
    async with AsyncSessionLocal() as db:
        state = await delivery_state_repo.get_runtime_state(db, for_update=True)
        if state.mode != "frozen":
            raise ValueError("direct restoration requires frozen DB runtime")
        if args.epoch == state.cutover_epoch:
            raise ValueError("direct restoration requires a new epoch")
        fleet = await delivery_state_repo.get_worker_fleet_readiness(db)
        if not fleet.workers or any(
            worker.dispatch_enabled or worker.current_lease_count
            for worker in fleet.workers
        ):
            raise ValueError("direct restoration requires settled dispatch-disabled workers")
        check = await delivery_state_repo.get_rollback_readiness(
            db
        )
        if not check.ready:
            raise ValueError("direct restoration has unresolved delivery state")
        updated = await delivery_state_repo.change_runtime_state(
            db,
            mode="direct",
            cutover_epoch=args.epoch,
            actor=args.actor,
            reason=args.reason,
            expected_version=args.expected_version,
        )
        await delivery_state_repo.record_operator_action(
            db,
            delivery_id=None,
            action="restore_direct",
            actor=args.actor,
            reason=args.reason,
        )
        await db.commit()
    _emit(
        {
            "status": "direct_restored",
            "epoch": updated.cutover_epoch,
            "version": updated.version,
            "warning": "restart all app/worker replicas with matching direct configuration before ingress resumes",
        }
    )
    return 0


async def _scheduler_state(args: argparse.Namespace) -> int:
    async def operation(db):
        if args.scheduler_command == "show":
            # S2: separate MVCC diagnostic; never acquire the control locks.
            return await scheduler_diagnostics_repo.scheduler_snapshot(db)
        else:
            # T08-3: attestation + this operator's fingerprint for the enable gate.
            attestation = (
                replica_repo.EnableAttestation(
                    old_replicas_removed=bool(args.attest_old_replicas_removed),
                    operator=replica_compat.local_fingerprint(),
                )
                if args.scheduler_command == "enabled"
                else None
            )
            row = await scheduler_repo.transition_scheduler_state(
                db, target=args.scheduler_command, scheduler_epoch=args.epoch,
                expected_version=args.expected_version, actor=args.actor, reason=args.reason,
                replica_attestation=attestation,
            )
        return {
            "state": row.state, "epoch": row.scheduler_epoch, "version": row.version,
            "clearance": await scheduler_repo.scheduler_clearance_counts(db),
        }

    try:
        result = await scheduler_repo.run_transaction_with_deadlock_retry(
            AsyncSessionLocal, operation,
        )
    except scheduler_repo.SchedulerDrainBlocked as exc:
        _emit({"status": "refused", "error_type": type(exc).__name__, "clearance": exc.counts})
        return 1
    except scheduler_repo.ReplicaGateRefused as exc:  # T08-3
        _emit({"status": "refused", "error_type": type(exc).__name__, "replica_reasons": list(exc.reasons)})
        return 1
    _emit(result)
    return 0


async def _run(args: argparse.Namespace) -> int:
    if args.command == "scheduler-state":
        return await _scheduler_state(args)
    # T08-6
    if args.command == "agent-scheduler":
        return await agent_scheduler_cli.run(args, AsyncSessionLocal, _emit)
    if args.command == "list":
        return await _list(args)
    if args.command == "retry":
        return await _retry(args)
    if args.command == "cancel":
        return await _cancel(args)
    if args.command == "reconcile":
        return await _reconcile(args)
    if args.command == "resolve-context-note":
        return await _resolve_context_note(args)
    if args.command == "turn-resolve":
        return await _turn_resolve(args)
    if args.command == "quarantine-list":
        return await _quarantine_list(args)
    if args.command == "quarantine-inspect":
        return await _quarantine_inspect(args)
    if args.command == "quarantine-resolve":
        return await _quarantine_resolve(args)
    if args.command == "health":
        return await _health()
    if args.command == "cutover":
        return await _cutover(args)
    if args.command == "freeze":
        return await _freeze(args)
    if args.command == "rollback-check":
        return await _rollback_check(args)
    if args.command == "resolve-rollback":
        return await _resolve_rollback(args)
    if args.command == "restore-direct":
        return await _restore_direct(args)
    raise RuntimeError("unknown command")


def main() -> int:
    require_runtime_version(settings.delivery_worker_code_version)
    setup_logger()
    args = _parser().parse_args()

    async def entrypoint() -> int:
        try:
            return await _run(args)
        finally:
            await engine.dispose()

    try:
        return asyncio.run(entrypoint())
    except Exception as exc:
        _emit({"status": "error", "error_type": type(exc).__name__})
        return 1


if __name__ == "__main__":
    sys.exit(main())
