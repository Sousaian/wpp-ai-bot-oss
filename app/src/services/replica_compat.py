"""T08-3: process-side replica heartbeat and compatibility readiness.

``EXPECTED_SCHEMA_VERSION`` is compiled into this binary and must equal
``scheduler_runtime_state.schema_version`` (bumped by the additive T08-3
migration). ``code_version`` is the operator-declared build identity
``DELIVERY_WORKER_CODE_VERSION``, shared by every replica kind.

No function here calls a provider: readiness is DB + local process state only.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from collections.abc import Awaitable, Callable
from uuid import uuid4

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.utils.image_version import image_code_version
from src.config import settings
from src.database.repositories import replica_repo
from src.utils.shutdown import observe, wait_until


EXPECTED_SCHEMA_VERSION = 3  # S1: durable retention checkpoint
# Fixed cadence, independent of tunables: four beats per freshness window.
HEARTBEAT_SECONDS = replica_repo.FRESH_SECONDS // 4
STOP_BEAT_TIMEOUT_SECONDS = 5.0
SIGNAL_THRESHOLD = 1
BLOCKING_STATES = frozenset({"enabling", "enabled", "draining"})

_counts = {
    "heartbeat_failures": 0,
    "incompatible_reports": 0,
    "report_unavailable": 0,
    "retention_failures": 0,
    "cleanup_failures": 0,
}
_report_available = True
_report_compatible = True


def counts() -> dict[str, int]:
    return dict(_counts)


def reset_health_transition_state() -> None:
    """Reset transition memory without erasing monotonic counters."""

    global _report_available, _report_compatible
    _report_available = True
    _report_compatible = True


def _signal(name: str, message: str, *args: object) -> None:
    _counts[name] += 1
    logger.error(message + " | count={} threshold={}", *args, _counts[name], SIGNAL_THRESHOLD)


def signal_cleanup_failure(kind: replica_repo.ReplicaKind) -> None:
    _signal("cleanup_failures", "Replica lifecycle cleanup incomplete | kind={}", kind)


def _report_unavailable(*, error_type: str, scheduler_state: str | None) -> None:
    global _report_available
    if _report_available:
        _signal(
            "report_unavailable",
            "Replica compatibility unavailable | erro_tipo={} state={} blocks_health={}",
            error_type,
            scheduler_state,
            scheduler_state in BLOCKING_STATES,
        )
    _report_available = False


def _report_result(*, reasons: tuple[str, ...], scheduler_state: str | None) -> None:
    global _report_available, _report_compatible
    _report_available = True
    compatible = not reasons
    if not compatible and _report_compatible:
        _signal(
            "incompatible_reports",
            "Replica fleet incompatible | reasons={} state={} blocks_health={}",
            ",".join(reasons),
            scheduler_state,
            scheduler_state in BLOCKING_STATES,
        )
    _report_compatible = compatible


def local_fingerprint() -> replica_repo.Fingerprint:
    return replica_repo.Fingerprint(
        code_version=image_code_version(settings.delivery_worker_code_version),
        schema_version=EXPECTED_SCHEMA_VERSION,
        delivery_mode=settings.delivery_mode,
        delivery_epoch=settings.delivery_cutover_epoch,
        scheduler_enabled=bool(settings.inbox_scheduler_enabled),
        scheduler_epoch=settings.inbox_scheduler_epoch,
        turn_processor_enabled=bool(settings.inbox_turn_processor_enabled),
        dispatch_enabled=bool(settings.delivery_dispatch_enabled),
        webhook_auth_mode=settings.provider_webhook_auth_mode,
    )


async def beat(
    sessions: async_sessionmaker[AsyncSession],
    *,
    kind: replica_repo.ReplicaKind,
    replica_id: str,
    stopped: bool = False,
) -> bool:
    """Best-effort heartbeat in its own transaction; never raises (but cancel)."""

    try:
        async with sessions() as db:
            try:
                await replica_repo.record(
                    db,
                    replica_id=replica_id,
                    replica_kind=kind,
                    fingerprint=local_fingerprint(),
                    stopped=stopped,
                )
                await db.commit()
            except asyncio.CancelledError:
                # Discard the connection, not just the transaction. Cancellation
                # can interrupt aiomysql protocol I/O or leave an open write.
                try:
                    await db.invalidate()
                except Exception:
                    signal_cleanup_failure(kind)
                raise
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _signal(
            "heartbeat_failures",
            "Replica heartbeat failed | kind={} replica={} erro_tipo={}",
            kind,
            replica_id,
            type(exc).__name__,
        )
        return False


async def purge_expired(sessions: async_sessionmaker[AsyncSession]) -> int:
    """90-day replica heartbeat retention; never raises (but cancel)."""

    try:
        async with sessions() as db:
            purged = await replica_repo.purge_expired(db)
            await db.commit()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _signal(
            "retention_failures",
            "Replica heartbeat retention failed | erro_tipo={}",
            type(exc).__name__,
        )
        return 0
    if purged:
        logger.info("Replica heartbeat retention | purged={}", purged)
    return purged


class HeartbeatLoop:
    """Background heartbeat for app and turn-worker processes."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        kind: replica_repo.ReplicaKind,
        replica_id: str | None = None,
        interval_seconds: float = HEARTBEAT_SECONDS,
    ) -> None:
        self.sessions = sessions
        self.kind = kind
        self.replica_id = replica_id or str(uuid4())
        self.interval = interval_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._shutdown_task: asyncio.Task[None] | None = None
        self.stopped_recorded = False

    def start(self) -> None:
        if self._task is None and not self._stop.is_set():
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while not self._stop.is_set():
            await beat(self.sessions, kind=self.kind, replica_id=self.replica_id)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass

    async def stop(
        self, deadline_seconds: float = STOP_BEAT_TIMEOUT_SECONDS, *,
        before_marker: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        """One total budget, including cancellation cleanup and final marker.

        Concurrent/repeated callers share the first deadline and marker. A
        cancelled caller propagates cancellation without cancelling shared work.
        """

        if self._shutdown_task is None:
            deadline = asyncio.get_running_loop().time() + max(0.0, deadline_seconds)
            self._stop.set()
            self._shutdown_task = asyncio.create_task(self._shutdown(deadline, before_marker))
            self._shutdown_task.add_done_callback(observe)
        try:
            await asyncio.shield(self._shutdown_task)
        except asyncio.CancelledError:
            # The first caller does not own shared process shutdown.
            raise

    async def _shutdown(self, deadline: float, before_marker=None) -> None:
        task = self._task
        try:
            if task is not None:
                task.cancel()
                if not await wait_until(task, deadline):
                    self._stop_timeout()
                    return  # Never race a marker against unfinished DB cleanup.
            if asyncio.get_running_loop().time() >= deadline:
                self._stop_timeout()
                return
            if before_marker is not None and not await before_marker():
                _signal("heartbeat_failures", "Replica stopped marker withheld | kind={}", self.kind)
                return
            task = asyncio.create_task(beat(
                self.sessions, kind=self.kind, replica_id=self.replica_id, stopped=True
            ))
            if not await wait_until(task, deadline):
                self._stop_timeout()
            elif not task.cancelled() and task.exception() is None:
                self.stopped_recorded = bool(task.result())
        finally:
            if task is not None and not task.done():
                task.cancel()

    def _stop_timeout(self) -> None:
        _signal(
            "heartbeat_failures",
            "Replica shutdown budget exhausted | kind={}",
            self.kind,
        )


@dataclass(frozen=True)
class FleetReport:
    compatible: bool
    reasons: tuple[str, ...]
    replicas: dict[str, int] = field(default_factory=dict)
    legacy_unmatched: int = 0
    db_schema_version: int | None = None
    scheduler_state: str | None = None

    @property
    def blocks_health(self) -> bool:
        return not self.compatible and self.scheduler_state in BLOCKING_STATES

    @property
    def healthy(self) -> bool:
        return not self.blocks_health

    def as_dict(self) -> dict[str, object]:
        return {
            "compatible": self.compatible,
            "healthy": self.healthy,
            "state": self.scheduler_state,
            "blocks_health": self.blocks_health,
            "reasons": list(self.reasons),
            "replicas": dict(self.replicas),
            "legacy_unmatched": self.legacy_unmatched,
            "db_schema_version": self.db_schema_version,
            "expected_schema_version": EXPECTED_SCHEMA_VERSION,
            "counts": counts(),
            "signal_threshold": SIGNAL_THRESHOLD,
        }


async def fleet_report_db(
    db: AsyncSession, *, reader: replica_repo.Fingerprint | None
) -> FleetReport:
    """Strict compatibility; health blocks only for an active DB scheduler.

    Capture state before the fleet so an unavailable fleet keeps the known
    state. No second session, lock or local flag substitutes for DB state.
    """

    scheduler_state = None
    try:
        scheduler_state = await replica_repo.health_scheduler_state(db)
        snapshot = await replica_repo.load_fleet(db)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _report_unavailable(error_type=type(exc).__name__, scheduler_state=scheduler_state)
        return FleetReport(False, ("replica_state_unavailable",), scheduler_state=scheduler_state)
    reasons = replica_repo.readiness_reasons(snapshot, reader=reader)
    _report_result(reasons=reasons, scheduler_state=snapshot.scheduler_state)
    return FleetReport(
        compatible=not reasons,
        reasons=reasons,
        replicas=replica_repo.kind_counts(snapshot.replicas),
        legacy_unmatched=snapshot.legacy_unmatched,
        db_schema_version=snapshot.db_schema_version,
        scheduler_state=snapshot.scheduler_state,
    )


async def fleet_report(
    sessions: async_sessionmaker[AsyncSession],
    *,
    reader: replica_repo.Fingerprint | None,
) -> FleetReport:
    try:
        async with sessions() as db:
            return await fleet_report_db(db, reader=reader)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _report_unavailable(error_type=type(exc).__name__, scheduler_state=None)
        return FleetReport(False, ("replica_state_unavailable",))


async def schema_supported(sessions: async_sessionmaker[AsyncSession]) -> bool:
    """True only when the DB schema version equals this binary's (fail closed)."""

    try:
        async with sessions() as db:
            version = await replica_repo.db_schema_version(db)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _signal(
            "report_unavailable",
            "Replica schema version unavailable | erro_tipo={}",
            type(exc).__name__,
        )
        return False
    return version == EXPECTED_SCHEMA_VERSION
