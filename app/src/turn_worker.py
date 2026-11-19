"""Process entrypoint for fenced inbox generation and delivery admission.

All gates default off. When explicitly enabled, the worker integrates ticket05
handoff finalization and ticket06 automated-delivery admission; it still refuses
to run without both configured epochs.

T08-3: refuses to start when the DB schema version differs from this binary,
writes a replica heartbeat while running and exposes ``health`` for the
Compose healthcheck (DB + local snapshot only, no provider fan-out).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import signal
import sys
from uuid import UUID, uuid4

from loguru import logger

from src.utils.image_version import require_runtime_version
from src.config import settings
from src.database.connection import AsyncSessionLocal, engine
from src.database.repositories import replica_repo
from src.services import replica_compat
from src.services.turn_delivery import FinalCandidateRouter
from src.services.turn_processor import TurnProcessor
from src.utils.logger import setup_logger
from src.utils.shutdown import wait_until, WORKER_STOP_GRACE_SECONDS
from src.utils.process_shutdown import (
    ProcessDeadline, current, set_current, hard_deadline, writers_quiescent, marker_finished,
    MARKER_RESERVE_SECONDS, DISPOSE_RESERVE_SECONDS,
)


EXIT_DISABLED = 0
EXIT_REFUSED = 2
EXIT_UNHEALTHY = 1
WORKER_PREFIX = "turn-"


async def _run() -> int | None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    guard = current()
    controllers = set(asyncio.all_tasks())
    shutdown_started: float | None = None

    def request_stop() -> None:
        nonlocal shutdown_started
        if guard is not None:
            guard.arm()
        if shutdown_started is None:
            shutdown_started = guard.deadline - guard.seconds if guard is not None else loop.time()
        stop.set()

    # Before schema_supported or ANY bootstrap I/O. main() also installs the
    # thread guard synchronously before asyncio.run creates its event loop.
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            if guard is None:
                loop.add_signal_handler(signum, request_stop)
            else:
                def raw_signal(*_):
                    guard.arm()
                    loop.call_soon_threadsafe(request_stop)
                signal.signal(signum, raw_signal)
        except NotImplementedError:
            pass
    if guard is not None and guard.deadline is not None:
        request_stop()
    stop_task = asyncio.create_task(stop.wait())
    controllers.add(stop_task)
    heartbeat = None
    processor = None
    run_task = None
    schema_task = None
    try:
        if settings.inbox_scheduler_epoch is None or settings.delivery_cutover_epoch is None:
            raise RuntimeError("turn worker requires both configured epochs")
        if stop.is_set():
            return None
        schema_task = asyncio.create_task(replica_compat.schema_supported(AsyncSessionLocal))
        await asyncio.wait({schema_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if stop.is_set():
            return None
        if not schema_task.result():
            logger.error("Inbox turn processor refused: DB schema version is not supported")
            return EXIT_REFUSED
        replica_id = str(uuid4())
        heartbeat = replica_compat.HeartbeatLoop(AsyncSessionLocal, kind="turn_worker", replica_id=replica_id)
        processor = TurnProcessor(
            AsyncSessionLocal, worker_id=f"{WORKER_PREFIX}{replica_id}",
            scheduler_epoch=settings.inbox_scheduler_epoch,
            delivery_runtime_epoch=settings.delivery_cutover_epoch,
            consumer=FinalCandidateRouter(AsyncSessionLocal),
        )
        heartbeat.start()
        logger.info("Inbox turn processor started | worker={}", processor.worker_id)
        run_task = asyncio.create_task(processor.run(stop))
        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if run_task.done():
            run_task.result()
    finally:
        stop_task.cancel()
        started = shutdown_started if shutdown_started is not None else loop.time()
        # Production gets the REAL Compose budget, not drain+abort (35s).
        # Unit callers without a process guard retain their explicit test budget.
        fallback = settings.delivery_shutdown_grace_seconds + settings.delivery_shutdown_abort_seconds
        deadline = hard_deadline(guard, fallback)
        reserve = min(DISPOSE_RESERVE_SECONDS, fallback / 4) if guard is None else DISPOSE_RESERVE_SECONDS
        marker_reserve = min(MARKER_RESERVE_SECONDS, fallback / 4) if guard is None else MARKER_RESERVE_SECONDS
        writers_end = deadline - marker_reserve - reserve
        marker_end = deadline - reserve
        drain_end = min(started + settings.delivery_shutdown_grace_seconds, writers_end)
        try:
            # Cancel discovery/bootstrap only. Active jobs keep the full drain.
            for task in (run_task, schema_task):
                if task is not None and not task.done():
                    task.cancel()
            if processor is not None:
                await _cleanup_stage(
                    processor.shutdown(max(0.0, drain_end - loop.time())), writers_end,
                )

            async def before_marker() -> bool:
                ignored = controllers | {asyncio.current_task()}
                if heartbeat is not None:
                    ignored.add(heartbeat._shutdown_task)
                safe = await writers_quiescent(writers_end, ignored)
                if not safe:
                    replica_compat.signal_cleanup_failure("turn_worker")
                    if guard is not None:
                        guard.mark_unsafe()
                return safe

            if heartbeat is not None:
                await heartbeat.stop(max(0.0, marker_end - loop.time()), before_marker=before_marker)
                if guard is not None:
                    await marker_finished(
                        guard, recorded=heartbeat.stopped_recorded,
                        deadline=marker_end, controllers=controllers,
                    )
            elif not await before_marker() and guard is not None:
                guard.mark_unsafe()
        finally:
            await _cleanup_stage(engine.dispose(), deadline)
        logger.info("Inbox turn processor stopped")
    return None


async def _cleanup_stage(operation, deadline: float) -> None:
    task = asyncio.create_task(operation)
    finished = await wait_until(task, deadline)
    if not finished or task.cancelled() or task.exception() is not None:
        replica_compat.signal_cleanup_failure("turn_worker")


def _snapshot_replica_id(now: datetime) -> tuple[str | None, bool]:
    """Replica id from the local snapshot and whether the snapshot is fresh."""

    try:
        snapshot = json.loads(Path(settings.inbox_turn_worker_health_path).read_text())
        worker_id = snapshot["worker_id"]
        updated_at = datetime.fromisoformat(snapshot["updated_at"])
        if not isinstance(worker_id, str) or not worker_id.startswith(WORKER_PREFIX):
            raise ValueError("worker_id is invalid")
        replica_id = str(UUID(worker_id.removeprefix(WORKER_PREFIX)))
        if updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None, False
    age = now - updated_at
    # Small negative tolerance for clock granularity; future snapshots fail.
    fresh = -timedelta(seconds=5) <= age <= timedelta(seconds=replica_repo.FRESH_SECONDS)
    return replica_id, fresh


async def _health() -> int:
    if not settings.inbox_turn_processor_enabled:
        _emit({"healthy": False, "reason": "turn_processor_disabled"})
        return EXIT_UNHEALTHY
    replica_id, snapshot_fresh = _snapshot_replica_id(datetime.now(timezone.utc))
    if replica_id is None:
        _emit({"healthy": False, "reason": "local_snapshot_unavailable"})
        return EXIT_UNHEALTHY
    heartbeat_fresh = False
    try:
        async with AsyncSessionLocal() as db:
            heartbeat_fresh = await replica_repo.own_heartbeat_fresh(db, replica_id=replica_id)
            replicas = await replica_compat.fleet_report_db(
                db, reader=replica_compat.local_fingerprint()
            )
    except Exception as exc:
        logger.warning("Turn worker health unavailable | erro_tipo={}", type(exc).__name__)
        _emit({"healthy": False, "reason": "readiness_unavailable"})
        return EXIT_UNHEALTHY
    finally:
        await engine.dispose()
    healthy = bool(snapshot_fresh and heartbeat_fresh and replicas.healthy)
    _emit(
        {
            "healthy": healthy,
            "snapshot_fresh": snapshot_fresh,
            "heartbeat_fresh": heartbeat_fresh,
            "replica_compatibility": replicas.as_dict(),
            "worker_id": f"{WORKER_PREFIX}{replica_id}",
        }
    )
    return 0 if healthy else EXIT_UNHEALTHY


def _emit(value: object) -> None:
    print(json.dumps(value, sort_keys=True, default=str))


def main(argv: list[str] | None = None) -> int:
    require_runtime_version(settings.delivery_worker_code_version)
    if argv == ["health"]:
        return asyncio.run(_health())
    if argv:
        logger.error("Unknown turn worker arguments")
        return EXIT_REFUSED
    if not settings.inbox_turn_processor_enabled:
        logger.info("Inbox turn processor disabled; exiting without claims")
        return EXIT_DISABLED
    if (
        not settings.inbox_scheduler_enabled
        or not settings.inbox_scheduler_epoch
        or not settings.delivery_cutover_epoch
    ):
        logger.error(
            "Inbox turn processor refused: scheduler gate or epochs are missing"
        )
        return EXIT_REFUSED
    guard = ProcessDeadline(WORKER_STOP_GRACE_SECONDS)
    set_current(guard)
    previous = {signum: signal.signal(signum, lambda *_: guard.arm())
                for signum in (signal.SIGTERM, signal.SIGINT)}
    try:
        result = asyncio.run(_run())
        guard.finish()  # Only AFTER asyncio.run's task/executor teardown.
        return EXIT_REFUSED if result == EXIT_REFUSED else 0
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        set_current(None)


if __name__ == "__main__":
    setup_logger()
    sys.exit(main(sys.argv[1:]))
