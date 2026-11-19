from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import signal
import time
from uuid import uuid4
from typing import TypeVar

import httpx
from loguru import logger
from sqlalchemy.exc import DBAPIError

from src.utils.image_version import require_runtime_version, image_code_version
from src.config import AUTOMATED_IO_MARGIN_SECONDS, settings
from src.database import db_errors
from src.database.connection import AsyncSessionLocal, engine
from src.database.repositories import (
    agente_repo,
    delivery_state_repo,
    delivery_worker_repo,
    scheduler_repo,
    scheduler_retention_repo,
    turn_delivery_repo,
    usage_reconciliation_repo,
)
from src.services import chatwoot_client, replica_compat
from src.services.delivery_adapters import (
    DuplicateRemoteEffect,
    InvalidDeliveryPayload,
    ProviderResultError,
    classify_http_failure,
    create_or_reconcile_chatwoot_context_note,
    decode_chatwoot_context_note,
    decode_chatwoot_handoff,
    decode_evolution_text,
    open_or_reconcile_chatwoot_handoff,
    retry_after_seconds,
    send_evolution_text,
)
from src.utils.logger import setup_logger


BACKOFF_SECONDS = (5, 15, 45, 120, 300, 900, 1800, 3600)


def _automated_hint(
    delivery: delivery_worker_repo.ClaimedDelivery,
) -> scheduler_repo.AutomatedDeliveryHint:
    return scheduler_repo.AutomatedDeliveryHint(
        delivery_class=delivery.delivery_class,
        automated_effect=delivery.automated_effect,
        agente_id=delivery.agente_id,
        sessao_id=delivery.sessao_id,
        conversation_turn_id=delivery.conversation_turn_id,
        turn_revision_id=delivery.turn_revision_id,
        handoff_attempt_id=delivery.handoff_attempt_id,
        scheduler_epoch=delivery.scheduler_epoch,
    )


@dataclass(frozen=True)
class FailureDecision:
    error_code: str
    retry: bool
    ambiguous: bool
    http_status: int | None = None
    retry_after_seconds: int | None = None


def _backoff_seconds(
    attempt_count: int,
    *,
    retry_after: int | None,
    random_value: Callable[[], float] = random.random,
) -> float:
    stage = BACKOFF_SECONDS[min(max(attempt_count - 1, 0), len(BACKOFF_SECONDS) - 1)]
    jittered = max(1.0, stage * min(max(random_value(), 0.0), 1.0))
    if retry_after is not None:
        return float(max(jittered, min(max(retry_after, 0), 3600)))
    return min(jittered, 3600.0)


def _classify_exception(provider: str, exc: BaseException) -> FailureDecision:
    if isinstance(exc, InvalidDeliveryPayload):
        return FailureDecision(type(exc).__name__.lower(), False, False)
    if isinstance(exc, ProviderResultError):
        return FailureDecision("provider_result_ambiguous", True, True)
    if isinstance(exc, DuplicateRemoteEffect):
        return FailureDecision("duplicate_remote_effect", False, False)
    if isinstance(exc, chatwoot_client.ChatwootContractError):
        return FailureDecision("provider_contract_invalid", False, False)
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        retry, ambiguous, code = classify_http_failure(
            "chatwoot" if provider == "chatwoot" else "evolution",
            status,
        )
        return FailureDecision(
            code,
            retry,
            ambiguous,
            http_status=status,
            retry_after_seconds=retry_after_seconds(exc.response),
        )
    if isinstance(exc, (httpx.ReadTimeout, httpx.ReadError, asyncio.TimeoutError)):
        return FailureDecision("response_ambiguous", True, True)
    if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
        return FailureDecision("provider_connect_failed", True, False)
    if isinstance(exc, httpx.HTTPError):
        return FailureDecision("provider_transport_failed", True, provider == "chatwoot")
    return FailureDecision("unexpected_dispatch_failure", True, False)


_RetentionValue = TypeVar("_RetentionValue")


class DeliveryWorker:
    def __init__(self) -> None:
        self.worker_id = str(uuid4())
        self._stop = asyncio.Event()
        self._abort = asyncio.Event()
        self._drain_deadline: float | None = None
        self._shutdown_deadline: float | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._loop_tasks: set[asyncio.Task] = set()
        self._readiness_ok = False
        self._provider_ready = False
        self._runtime_compatible = False
        self._last_readiness_check = 0.0
        self._last_maintenance = 0.0
        self._last_retention = 0.0
        self._retention_task: asyncio.Task | None = None
        self._retention_errors: dict[str, int] = {}
        # FU-1: watchdog uses monotonic durations, UTC only for display.
        self._retention_observing_since = time.monotonic()
        self._retention_state = "idle"
        self._retention_started_at: str | None = None
        self._retention_started_monotonic: float | None = None
        self._retention_last_completed_at: str | None = None
        self._retention_last_completed_monotonic: float | None = None
        self._retention_watchdog_counts: dict[str, int] = {}
        self._retention_watchdog_episodes: set[str] = set()
        self._last_heartbeat = 0.0
        self._maintenance_consecutive_skips = 0
        self._automated_authority_skip_count = 0
        # T08-3: replica compatibility heartbeat (fixed cadence) + stop marker.
        self._last_replica_beat = 0.0
        self._replica_drained = False

    @property
    def active_count(self) -> int:
        return len(self._tasks)

    @property
    def maintenance_healthy(self) -> bool:
        return (
            self._maintenance_consecutive_skips
            < settings.delivery_maintenance_skip_error_threshold
        )

    def request_stop(self) -> None:
        if self._drain_deadline is None:
            stopped_at = time.monotonic()
            self._drain_deadline = (
                stopped_at + settings.delivery_shutdown_grace_seconds
            )
            self._shutdown_deadline = (
                self._drain_deadline + settings.delivery_shutdown_abort_seconds
            )
        self._stop.set()

    async def _await_loop_operation(self, operation: Awaitable):
        task = asyncio.create_task(operation)
        self._loop_tasks.add(task)
        stop_wait = asyncio.create_task(self._stop.wait())
        keep_for_shutdown = False
        try:
            await asyncio.wait(
                {task, stop_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._stop.is_set():
                keep_for_shutdown = True
                if not task.done():
                    task.cancel()
                return False, None
            return True, task.result()
        except asyncio.CancelledError:
            keep_for_shutdown = True
            task.cancel()
            raise
        finally:
            stop_wait.cancel()
            await asyncio.gather(stop_wait, return_exceptions=True)
            if not keep_for_shutdown:
                self._loop_tasks.discard(task)

    def _retention_task_snapshot(self) -> dict:
        now = time.monotonic()
        running = self._retention_task is not None and not self._retention_task.done()
        if not running and self._retention_state == "running":
            # FU-1: cancellation before the coroutine's first instruction has
            # no wrapper finally; a completed Task cannot still be running.
            self._retention_state = "failed"
        running_seconds = (max(0.0, now - self._retention_started_monotonic)
                           if running and self._retention_started_monotonic is not None else None)
        last_good = (self._retention_last_completed_monotonic
                     if self._retention_last_completed_monotonic is not None
                     else self._retention_observing_since)
        conditions = {
            "running_over_budget": running_seconds is not None and running_seconds >
                settings.scheduler_retention_max_seconds + settings.delivery_retention_overrun_margin_seconds,
            "completion_overdue": now - last_good > settings.delivery_retention_interval_seconds *
                settings.delivery_retention_completion_max_intervals,
        }
        for reason, active in conditions.items():
            if active and reason not in self._retention_watchdog_episodes:
                self._retention_watchdog_counts[reason] = self._retention_watchdog_counts.get(reason, 0) + 1
                logger.error("Delivery retention watchdog | reason={} count={} threshold=1", reason,
                             self._retention_watchdog_counts[reason])
                self._retention_watchdog_episodes.add(reason)
            elif not active:
                self._retention_watchdog_episodes.discard(reason)
        return {"state": "running" if running else self._retention_state,
                "started_at": self._retention_started_at,
                "last_completed_at": self._retention_last_completed_at,
                "running_seconds": running_seconds,
                "seconds_without_completion": max(0.0, now - last_good),
                "stale": any(conditions.values()) or self._retention_state == "failed",
                "counts": dict(self._retention_watchdog_counts), "error_threshold": 1}

    def _publish_health_snapshot(self) -> None:
        retention_task = self._retention_task_snapshot()
        scheduler_retention_repo.observe_watchdog()  # FU-1: independent of stuck task
        snapshot = {
            "retention_task": retention_task,
            "worker_id": self.worker_id,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "active": self.active_count,
            "ready": self._readiness_ok and self.maintenance_healthy,
            "provider_ready": self._provider_ready,
            "runtime_compatible": self._runtime_compatible,
            "maintenance_consecutive_skips": self._maintenance_consecutive_skips,
            "maintenance_skip_error_threshold": (
                settings.delivery_maintenance_skip_error_threshold
            ),
            "maintenance_healthy": self.maintenance_healthy,
            # Automated pre-I/O refusals (no I/O happened); threshold 1.
            "automated_authority_skips": self._automated_authority_skip_count,
            "automated_preio_released": turn_delivery_repo.preio_release_count(),
            "automated_runtime_gate_released": (
                turn_delivery_repo.runtime_gate_release_count()
            ),
            "automated_retention_nonterminal_skips": delivery_worker_repo.retention_skip_count(),
            "scheduler_retention": scheduler_retention_repo.retention_counts(),
            "retention_stage_errors": dict(self._retention_errors),  # R2-S1
            "scheduler_retention_health": scheduler_retention_repo.retention_health(),  # S1
            "scheduler_retention_error_threshold": scheduler_retention_repo.SIGNAL_THRESHOLD,
            "signal_threshold": 1,
            "replica_compat": replica_compat.counts(),  # T08-3
        }
        if retention_task["stale"]:
            snapshot["scheduler_retention_health"] = {
                **snapshot["scheduler_retention_health"], "stale": True, "available": False,
                "draft_available": False}
        target = Path(settings.delivery_worker_health_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(
            f".{target.name}.{self.worker_id}.{os.getpid()}.{uuid4().hex}.tmp"
        )
        try:
            temporary.write_text(json.dumps(snapshot, sort_keys=True))
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    async def _heartbeat(self) -> None:
        async with AsyncSessionLocal() as db:
            await delivery_state_repo.record_heartbeat(
                db,
                worker_id=self.worker_id,
                code_version=image_code_version(settings.delivery_worker_code_version),
                current_lease_count=self.active_count,
                dispatch_enabled=settings.delivery_dispatch_enabled,
                configured_mode=settings.delivery_mode,
                configured_epoch=settings.delivery_cutover_epoch,
                provider_ready=self._provider_ready,
                runtime_compatible=self._runtime_compatible,
            )
            await db.commit()
        if self._replica_drained:  # T08-3: final beat records the clean exit.
            await self._replica_beat()
        self._publish_health_snapshot()

    async def _replica_beat(self) -> None:
        # T08-3: best-effort, own transaction; failure is counted and logged.
        await replica_compat.beat(
            AsyncSessionLocal,
            kind="delivery_worker",
            replica_id=self.worker_id,
            stopped=self._replica_drained,
        )

    @staticmethod
    def _state_matches_configuration(state) -> bool:
        if settings.delivery_mode == "direct":
            return state.mode in {"direct", "frozen"}
        return bool(
            settings.delivery_cutover_epoch
            and state.mode == "outbox"
            and state.cutover_epoch == settings.delivery_cutover_epoch
        )

    async def _check_readiness(self) -> bool:
        try:
            async with AsyncSessionLocal() as db:
                state = await delivery_state_repo.get_runtime_state(db)
                self._runtime_compatible = self._state_matches_configuration(state)
                if not self._runtime_compatible:
                    self._provider_ready = False
                    self._readiness_ok = False
                    return False
                if not settings.delivery_dispatch_enabled:
                    self._provider_ready = True
                    self._readiness_ok = True
                    return True
                if settings.delivery_mode != "outbox":
                    self._provider_ready = False
                    self._readiness_ok = False
                    return False
                inbox_ids = await agente_repo.get_active_chatwoot_inbox_ids(db)
        except Exception as exc:
            logger.warning(
                "Delivery runtime readiness failed | worker={} error_type={}",
                self.worker_id,
                type(exc).__name__,
            )
            self._provider_ready = False
            self._runtime_compatible = False
            self._readiness_ok = False
            return False
        if not settings.delivery_cutover_epoch:
            self._provider_ready = False
            self._readiness_ok = False
            return False
        try:
            await chatwoot_client.check_delivery_contract(
                inbox_ids,
                bootstrap_attribute=False,
            )
        except Exception as exc:
            logger.warning(
                "Delivery provider readiness failed | worker={} error_type={}",
                self.worker_id,
                type(exc).__name__,
            )
            self._provider_ready = False
            self._readiness_ok = False
            return False
        self._provider_ready = True
        self._readiness_ok = True
        return True

    async def _maintenance(self) -> None:
        try:
            async with AsyncSessionLocal() as db:
                result = await delivery_worker_repo.run_preclaim_maintenance(db)
                usage = await usage_reconciliation_repo.reconcile_due(
                    db,
                    limit=settings.delivery_usage_reconciliation_batch,
                )
                await db.commit()
        except DBAPIError as exc:
            # Deadlock/lock timeout/lost savepoint: InnoDB already discarded
            # the whole pass. Nothing is retained; the next interval reruns
            # the complete maintenance transaction.
            if not db_errors.transaction_lost(exc):
                raise
            self._maintenance_consecutive_skips += 1
            log = logger.error if not self.maintenance_healthy else logger.warning
            log(
                "Delivery maintenance transaction lost; retrying next interval | "
                "worker={} consecutive_skips={} error_threshold={}",
                self.worker_id,
                self._maintenance_consecutive_skips,
                settings.delivery_maintenance_skip_error_threshold,
            )
            return
        previous_skips = self._maintenance_consecutive_skips
        self._maintenance_consecutive_skips = 0
        context_note_ambiguous = getattr(result, "context_note_ambiguous", 0)
        automated_skipped = getattr(result, "automated_skipped", 0)
        if previous_skips:
            logger.info(
                "Delivery maintenance recovered | worker={} previous_consecutive_skips={}",
                self.worker_id,
                previous_skips,
            )
        if any(
            (
                result.recovered,
                result.cancelled,
                result.expired,
                result.dependency_terminal,
                context_note_ambiguous,
                automated_skipped,
                usage.examined,
            )
        ):
            logger.info(
                "Delivery maintenance | worker={} recovered={} ambiguity={} "
                "cancelled={} expired={} dependency_terminal={} "
                "context_note_ambiguous={} automated_skipped={} usage_examined={}",
                self.worker_id,
                result.recovered,
                result.ambiguity_counted,
                result.cancelled,
                result.expired,
                result.dependency_terminal,
                context_note_ambiguous,
                automated_skipped,
                usage.examined,
            )
        if context_note_ambiguous >= 1:
            logger.error(
                "Chatwoot context-note ambiguity requires operator reconciliation "
                "| count={} error_threshold=1",
                context_note_ambiguous,
            )
        if automated_skipped >= 1:
            logger.error(
                "Automated delivery requires scheduler-authorized lifecycle handling "
                "| count={} error_threshold=1",
                automated_skipped,
            )

    async def _retention_stage(self, stage: str, operation: Awaitable[_RetentionValue]) -> _RetentionValue | None:
        # R2-S1: each stage owns its transaction/retry boundary. Cancellation
        # propagates; transient DB errors never terminate dispatch or later passes.
        try:
            return await operation
        except Exception as exc:
            self._retention_errors[stage] = self._retention_errors.get(stage, 0) + 1
            logger.error("Delivery retention stage failed | stage={} count={} threshold=1 erro_tipo={}",
                         stage, self._retention_errors[stage], type(exc).__name__)
            return None

    async def _legacy_retention(self) -> delivery_worker_repo.CleanupResult:
        async with AsyncSessionLocal() as db:
            result = await delivery_worker_repo.cleanup_retention(db)
            await db.commit()
        return result

    async def _retention(self) -> None:
        # R2-S1: legacy commits before scheduler; graph authority never follows
        # delivery locks. A failed stage ends only that stage's pass.
        result = await self._retention_stage("legacy", self._legacy_retention())
        scheduler = await self._retention_stage("scheduler",
            scheduler_retention_repo.cleanup_retention(AsyncSessionLocal))
        await self._retention_stage("replicas", replica_compat.purge_expired(AsyncSessionLocal))
        if scheduler is not None and scheduler.examined:
            logger.info("Scheduler retention | worker={} result={}", self.worker_id, scheduler)
        if result is not None and any(
            (
                result.payloads_scrubbed,
                result.receipts_purged,
                result.heartbeats_purged,
                result.audits_purged,
                result.deliveries_purged,
            )
        ):
            logger.info(
                "Delivery retention | worker={} payloads={} receipts={} heartbeats={} "
                "audits={} deliveries={}",
                self.worker_id,
                result.payloads_scrubbed,
                result.receipts_purged,
                result.heartbeats_purged,
                result.audits_purged,
                result.deliveries_purged,
            )

    async def _run_retention(self) -> None:
        # P3-1: candidate retention signals do not invalidate stage/measurement completion.
        before = (sum(self._retention_errors.values()), scheduler_retention_repo.measurement_failure_count(),
                  replica_compat.counts().get("retention_failures", 0))
        try:
            await self._retention()
            after = (sum(self._retention_errors.values()), scheduler_retention_repo.measurement_failure_count(),
                     replica_compat.counts().get("retention_failures", 0))
            if before == after:
                self._retention_state = "idle"
                self._retention_last_completed_at = datetime.now(timezone.utc).isoformat()
                self._retention_last_completed_monotonic = time.monotonic()
            else:
                self._retention_state = "failed"
        except BaseException:
            self._retention_state = "failed"
            raise

    def _schedule_retention(self, now: float) -> None:
        # Independent single-flight task; no await on the dispatch/heartbeat path.
        if self._retention_task is not None and not self._retention_task.done():
            return
        if (self._retention_started_at is not None
            and now - self._last_retention < settings.delivery_retention_interval_seconds):
            return
        self._last_retention = now
        self._retention_state = "running"
        self._retention_started_at = datetime.now(timezone.utc).isoformat()
        self._retention_started_monotonic = now
        task = asyncio.create_task(self._run_retention(), name="delivery-retention")
        self._retention_task = task
        self._loop_tasks.add(task)
        task.add_done_callback(self._loop_tasks.discard)
        task.add_done_callback(lambda done: self._report_task_results({done}))

    async def _claim(self, capacity: int) -> list[delivery_worker_repo.ClaimedDelivery]:
        if (
            capacity <= 0
            or self._stop.is_set()
            or not settings.delivery_dispatch_enabled
            or settings.delivery_mode != "outbox"
            or not self._readiness_ok
            or not settings.delivery_cutover_epoch
        ):
            return []
        try:
            return await delivery_worker_repo.claim_due_with_retry(
                AsyncSessionLocal,
                available_slots=capacity,
                worker_id=self.worker_id,
                cutover_epoch=settings.delivery_cutover_epoch,
                evolution_lease_seconds=settings.delivery_evolution_lease_seconds,
                chatwoot_lease_seconds=settings.delivery_chatwoot_lease_seconds,
                max_attempts=settings.delivery_claim_max_attempts,
                max_jitter_seconds=settings.delivery_claim_retry_jitter_seconds,
            )
        except delivery_worker_repo.ClaimRetryExhausted:
            logger.warning(
                "Delivery claim contention exhausted | worker={} attempts={}",
                self.worker_id,
                settings.delivery_claim_max_attempts,
            )
            return []

    async def _renew_loop(
        self,
        delivery: delivery_worker_repo.ClaimedDelivery,
        lease_seconds: int,
        ownership_lost: asyncio.Event,
    ) -> None:
        try:
            renew_interval = settings.delivery_lease_renew_interval_seconds
            if delivery.delivery_class == "automated":
                renew_interval = min(
                    renew_interval,
                    max(1.0, settings.inbox_turn_lease_seconds / 3),
                )
            while True:
                await asyncio.sleep(renew_interval)
                if delivery.delivery_class == "automated":
                    renewed = await scheduler_repo.run_transaction_with_deadlock_retry(
                        AsyncSessionLocal,
                        lambda db: turn_delivery_repo.renew_automated_authority(
                            db,
                            delivery_id=delivery.id,
                            lease_token=delivery.lease_token,
                            cutover_epoch=delivery.cutover_epoch,
                            hint=_automated_hint(delivery),
                            delivery_lease_seconds=lease_seconds,
                            lane_lease_seconds=settings.inbox_turn_lease_seconds,
                        ),
                    )
                else:
                    async with AsyncSessionLocal() as db:
                        renewed = await delivery_worker_repo.renew_lease(
                            db,
                            delivery_id=delivery.id,
                            lease_token=delivery.lease_token,
                            cutover_epoch=delivery.cutover_epoch,
                            lease_seconds=lease_seconds,
                        )
                        await db.commit()
                if not renewed:
                    ownership_lost.set()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Delivery lease renewal failed | delivery={} provider={} error_type={}",
                delivery.id,
                delivery.provider,
                type(exc).__name__,
            )
            ownership_lost.set()

    async def _dispatch_chatwoot(
        self,
        delivery: delivery_worker_repo.ClaimedDelivery,
        *,
        deadline_at: float,
        progress: dict[str, bool] | None = None,
    ):
        if delivery.operation == "chatwoot.add_private_note.v1":
            payload = decode_chatwoot_context_note(
                delivery.operation, delivery.payload_version, delivery.payload
            )
            if delivery.sessao_id is None or delivery.handoff_attempt_id is None:
                raise InvalidDeliveryPayload("context note lacks local lifecycle links")
            async with AsyncSessionLocal() as db:
                ownership = (
                    await delivery_worker_repo.get_context_note_dispatch_ownership(
                        db,
                        delivery_id=delivery.id,
                        lease_token=delivery.lease_token,
                        cutover_epoch=delivery.cutover_epoch,
                        sessao_id=delivery.sessao_id,
                        attempt_id=delivery.handoff_attempt_id,
                        source_id=payload.source_id,
                        coverage=payload.coverage,
                    )
                )
            if (
                ownership is None
                or ownership.status != "ready"
                or ownership.handoff_key != payload.handoff_key
                or ownership.inbox_id != payload.inbox_id
            ):
                raise InvalidDeliveryPayload("Chatwoot context-note ownership mismatch")
            return await create_or_reconcile_chatwoot_context_note(
                payload,
                contact_id=ownership.contact_id,
                conversation_id=ownership.conversation_id,
                deadline_at=deadline_at,
                progress=progress,
            )

        payload = decode_chatwoot_handoff(
            delivery.operation, delivery.payload_version, delivery.payload
        )
        async with AsyncSessionLocal() as db:
            ownership = await delivery_worker_repo.get_handoff_dispatch_ownership(
                db,
                delivery_id=delivery.id,
                lease_token=delivery.lease_token,
                cutover_epoch=delivery.cutover_epoch,
            )
        if (
            ownership is None
            or ownership.status != "pending"
            or ownership.handoff_key != payload.handoff_key
            or ownership.inbox_id != payload.inbox_id
        ):
            raise InvalidDeliveryPayload("Chatwoot handoff ownership mismatch")
        return await open_or_reconcile_chatwoot_handoff(
            payload,
            delivery_key=delivery.delivery_key,
            deadline_at=deadline_at,
        )

    async def _run_provider_operation(
        self,
        delivery: delivery_worker_repo.ClaimedDelivery,
        *,
        deadline_at: float,
    ):
        result = None
        failure: FailureDecision | None = None
        progress: dict[str, bool] = {"post_sent": False}
        try:
            async with asyncio.timeout_at(deadline_at):
                try:
                    if delivery.provider == "evolution":
                        payload = decode_evolution_text(
                            delivery.operation,
                            delivery.payload_version,
                            delivery.payload,
                        )
                        result = await send_evolution_text(
                            payload,
                            timeout_seconds=max(deadline_at - time.monotonic(), 0.001),
                        )
                    elif delivery.provider == "chatwoot":
                        result = await self._dispatch_chatwoot(
                            delivery,
                            deadline_at=deadline_at,
                            progress=progress,
                        )
                        if result is None:
                            raise ProviderResultError(
                                "Chatwoot reconciliation returned no result"
                            )
                    else:
                        raise InvalidDeliveryPayload("unsupported provider")
                except BaseException as exc:
                    if isinstance(
                        exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)
                    ):
                        raise
                    failure = _classify_exception(delivery.provider, exc)
                    if (
                        delivery.provider == "chatwoot"
                        and failure.retry
                        and failure.ambiguous
                        and (
                            delivery.operation != "chatwoot.add_private_note.v1"
                            or progress["post_sent"]
                        )
                    ):
                        try:
                            if delivery.operation == "chatwoot.add_private_note.v1":
                                payload = decode_chatwoot_context_note(
                                    delivery.operation,
                                    delivery.payload_version,
                                    delivery.payload,
                                )
                                if (
                                    delivery.sessao_id is None
                                    or delivery.handoff_attempt_id is None
                                ):
                                    raise InvalidDeliveryPayload(
                                        "context note lacks local lifecycle links"
                                    )
                                async with AsyncSessionLocal() as db:
                                    ownership = await delivery_worker_repo.get_context_note_dispatch_ownership(
                                        db,
                                        delivery_id=delivery.id,
                                        lease_token=delivery.lease_token,
                                        cutover_epoch=delivery.cutover_epoch,
                                        sessao_id=delivery.sessao_id,
                                        attempt_id=delivery.handoff_attempt_id,
                                        source_id=payload.source_id,
                                        coverage=payload.coverage,
                                    )
                                if ownership is None:
                                    raise InvalidDeliveryPayload(
                                        "Chatwoot context-note ownership mismatch"
                                    )
                                result = await create_or_reconcile_chatwoot_context_note(
                                    payload,
                                    contact_id=ownership.contact_id,
                                    conversation_id=ownership.conversation_id,
                                    deadline_at=deadline_at,
                                    reconcile_only=True,
                                )
                            else:
                                payload = decode_chatwoot_handoff(
                                    delivery.operation,
                                    delivery.payload_version,
                                    delivery.payload,
                                )
                                result = await open_or_reconcile_chatwoot_handoff(
                                    payload,
                                    delivery_key=delivery.delivery_key,
                                    deadline_at=deadline_at,
                                    reconcile_only=True,
                                )
                        except BaseException as reconcile_exc:
                            if isinstance(
                                reconcile_exc,
                                (
                                    asyncio.CancelledError,
                                    KeyboardInterrupt,
                                    SystemExit,
                                ),
                            ):
                                raise
                            reconciled_failure = _classify_exception(
                                delivery.provider,
                                reconcile_exc,
                            )
                            if not reconciled_failure.retry:
                                failure = reconciled_failure
                        else:
                            if result is not None:
                                failure = None
        except asyncio.TimeoutError as exc:
            result = None
            failure = _classify_exception(delivery.provider, exc)
        if (
            delivery.operation == "chatwoot.add_private_note.v1"
            and failure is not None
            and (failure.ambiguous or progress["post_sent"])
            and failure.error_code != "duplicate_remote_effect"
        ):
            # After the create POST every unverified outcome is ambiguous,
            # whatever its classification (including unknown exceptions).
            if not progress["post_sent"]:
                # Failed during the read-only mapping check or exhaustive scan:
                # no create was sent by this attempt, so a retry is safe and
                # will scan again before any POST.
                failure = FailureDecision(
                    "context_note_pre_post_failure",
                    True,
                    False,
                    http_status=failure.http_status,
                    retry_after_seconds=failure.retry_after_seconds,
                )
            else:
                # Zero or unverifiable exact matches after an ambiguous create
                # are not proof of rejection: never retry, require an operator.
                failure = FailureDecision(
                    "context_note_ambiguous",
                    False,
                    True,
                    http_status=failure.http_status,
                )
        return result, failure

    async def _dispatch_one(
        self,
        delivery: delivery_worker_repo.ClaimedDelivery,
    ) -> None:
        started = time.monotonic()
        automated_hint = _automated_hint(delivery)
        if delivery.delivery_class == "automated":
            # Whole-transaction retry on 1205/1213/1305; the lane is extended
            # past the provider deadline before any I/O is authorized.
            authorized = await scheduler_repo.run_transaction_with_deadlock_retry(
                AsyncSessionLocal,
                lambda db: turn_delivery_repo.authorize_before_io(
                    db,
                    delivery_id=delivery.id,
                    lease_token=delivery.lease_token,
                    cutover_epoch=delivery.cutover_epoch,
                    hint=automated_hint,
                    io_budget_seconds=(
                        settings.delivery_evolution_deadline_seconds
                        + AUTOMATED_IO_MARGIN_SECONDS
                    ),
                ),
            )
            if not authorized:
                self._automated_authority_skip_count += 1
                logger.error(
                    "Automated delivery claim failed pre-I/O authorization | "
                    "delivery={} count={} threshold=1",
                    delivery.id,
                    self._automated_authority_skip_count,
                )
                return
        lease_seconds = (
            settings.delivery_chatwoot_lease_seconds
            if delivery.provider == "chatwoot"
            else settings.delivery_evolution_lease_seconds
        )
        deadline_seconds = (
            settings.delivery_chatwoot_deadline_seconds
            if delivery.provider == "chatwoot"
            else settings.delivery_evolution_deadline_seconds
        )
        deadline_at = started + deadline_seconds
        ownership_lost = asyncio.Event()
        renewal = asyncio.create_task(
            self._renew_loop(delivery, lease_seconds, ownership_lost),
            name=f"delivery-renew-{delivery.id}",
        )
        provider_task = asyncio.create_task(
            self._run_provider_operation(delivery, deadline_at=deadline_at),
            name=f"delivery-provider-{delivery.id}",
        )
        ownership_wait = asyncio.create_task(
            ownership_lost.wait(), name=f"delivery-ownership-{delivery.id}"
        )
        abort_wait = asyncio.create_task(
            self._abort.wait(), name=f"delivery-abort-{delivery.id}"
        )
        result = None
        failure: FailureDecision | None = None
        authority_lost = False
        shutdown_aborted = False
        try:
            await asyncio.wait(
                {provider_task, ownership_wait, abort_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            authority_lost = ownership_lost.is_set()
            shutdown_aborted = self._abort.is_set()
            if authority_lost or shutdown_aborted:
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)
            else:
                result, failure = await provider_task
        except asyncio.CancelledError:
            if self._abort.is_set():
                logger.warning(
                    "Delivery task cancelled after shutdown drain deadline | "
                    "delivery={} provider={} attempt={} remote_outcome=unknown",
                    delivery.id,
                    delivery.provider,
                    delivery.attempt_count,
                )
            raise
        finally:
            for task in (ownership_wait, abort_wait, renewal):
                task.cancel()
            await asyncio.gather(
                ownership_wait, abort_wait, renewal, return_exceptions=True
            )
            if not provider_task.done():
                provider_task.cancel()
                await asyncio.gather(provider_task, return_exceptions=True)

        if authority_lost or ownership_lost.is_set():
            logger.warning(
                "Delivery provider cancelled after authority loss | delivery={} "
                "provider={} attempt={} remote_outcome=unknown",
                delivery.id,
                delivery.provider,
                delivery.attempt_count,
            )
            return
        if shutdown_aborted or self._abort.is_set():
            logger.warning(
                "Delivery provider cancelled after shutdown drain deadline | "
                "delivery={} provider={} attempt={} remote_outcome=unknown",
                delivery.id,
                delivery.provider,
                delivery.attempt_count,
            )
            return

        if failure is None and result is not None:
            async with AsyncSessionLocal() as db:
                if delivery.provider == "chatwoot":
                    if delivery.operation == "chatwoot.add_private_note.v1":
                        note_payload = decode_chatwoot_context_note(
                            delivery.operation,
                            delivery.payload_version,
                            delivery.payload,
                        )
                        if delivery.sessao_id is None or delivery.handoff_attempt_id is None:
                            accepted = False
                        else:
                            accepted = await delivery_worker_repo.complete_chatwoot_context_note(
                                db,
                                delivery_id=delivery.id,
                                lease_token=delivery.lease_token,
                                cutover_epoch=delivery.cutover_epoch,
                                sessao_id=delivery.sessao_id,
                                attempt_id=delivery.handoff_attempt_id,
                                source_id=note_payload.source_id,
                                coverage=note_payload.coverage,
                                external_id=str(result.external_id),
                                http_status=result.http_status,
                                next_chunk_ttl_seconds=settings.delivery_handoff_setup_ttl_seconds,
                            )
                    else:
                        accepted = await delivery_worker_repo.complete_chatwoot_handoff(
                            db,
                            delivery_id=delivery.id,
                            lease_token=delivery.lease_token,
                            cutover_epoch=delivery.cutover_epoch,
                            contact_id=int(result.contact_id),
                            conversation_id=int(result.conversation_id),
                            http_status=result.http_status,
                            transition_ttl_seconds=settings.delivery_transition_ttl_seconds,
                        )
                else:
                    if delivery.delivery_class == "automated":
                        # Own transaction with whole-txn deadlock retry: a lost
                        # completion would otherwise be recovered as a resend.
                        accepted = await scheduler_repo.run_transaction_with_deadlock_retry(
                            AsyncSessionLocal,
                            lambda tx: turn_delivery_repo.complete_automated(
                                tx,
                                delivery_id=delivery.id,
                                lease_token=delivery.lease_token,
                                cutover_epoch=delivery.cutover_epoch,
                                hint=automated_hint,
                                external_id=result.external_id,
                                http_status=result.http_status,
                            ),
                        )
                    else:
                        accepted = await delivery_worker_repo.complete_evolution(
                            db,
                            delivery_id=delivery.id,
                            lease_token=delivery.lease_token,
                            cutover_epoch=delivery.cutover_epoch,
                            external_id=result.external_id,
                            http_status=result.http_status,
                        )
                await db.commit()
            if not accepted:
                self._readiness_ok = False
            logger.info(
                "Delivery completed | delivery={} provider={} operation={} attempt={} "
                "accepted={} latency_ms={}",
                delivery.id,
                delivery.provider,
                delivery.operation,
                delivery.attempt_count,
                accepted,
                int((time.monotonic() - started) * 1000),
            )
            return

        assert failure is not None
        retry_delay = None
        if failure.retry:
            retry_delay = _backoff_seconds(
                delivery.attempt_count,
                retry_after=failure.retry_after_seconds,
            )
        async with AsyncSessionLocal() as db:
            if delivery.delivery_class == "automated":
                outcome = await scheduler_repo.run_transaction_with_deadlock_retry(
                    AsyncSessionLocal,
                    lambda tx: turn_delivery_repo.fail_automated(
                        tx,
                        delivery_id=delivery.id,
                        lease_token=delivery.lease_token,
                        cutover_epoch=delivery.cutover_epoch,
                        hint=automated_hint,
                        error_code=failure.error_code,
                        http_status=failure.http_status,
                        retry_delay_seconds=retry_delay,
                        ambiguous=failure.ambiguous,
                    ),
                )
            else:
                outcome = await delivery_worker_repo.fail_processing(
                    db,
                    delivery_id=delivery.id,
                    lease_token=delivery.lease_token,
                    cutover_epoch=delivery.cutover_epoch,
                    error_code=failure.error_code,
                    http_status=failure.http_status,
                    retry_delay_seconds=retry_delay,
                    ambiguous=failure.ambiguous,
                )
            await db.commit()
        logger.warning(
            "Delivery failed | delivery={} provider={} operation={} attempt={} "
            "code={} outcome={} latency_ms={}",
            delivery.id,
            delivery.provider,
            delivery.operation,
            delivery.attempt_count,
            failure.error_code,
            outcome,
            int((time.monotonic() - started) * 1000),
        )

    def _track(self, delivery: delivery_worker_repo.ClaimedDelivery) -> None:
        task = asyncio.create_task(
            self._dispatch_one(delivery),
            name=f"delivery-{delivery.id}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @staticmethod
    def _report_task_results(tasks: set[asyncio.Task]) -> None:
        for task in tasks:
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.error(
                    "Delivery task ended with error | error_type={}",
                    type(exc).__name__,
                )

    async def _drain_active(self) -> None:
        pending = set(self._tasks) | set(self._loop_tasks)
        if not pending:
            return
        if self._drain_deadline is None:
            self.request_stop()
        assert self._drain_deadline is not None
        assert self._shutdown_deadline is not None
        remaining = max(0.0, self._drain_deadline - time.monotonic())
        done, pending = await asyncio.wait(pending, timeout=remaining)
        self._report_task_results(done)
        self._loop_tasks.difference_update(done)
        if not pending:
            return

        logger.warning(
            "Delivery shutdown drain deadline reached | worker={} pending={}",
            self.worker_id,
            len(pending),
        )
        self._abort.set()
        for task in pending:
            task.cancel()
        done, pending = await asyncio.wait(
            pending,
            timeout=max(0.0, self._shutdown_deadline - time.monotonic()),
        )
        self._report_task_results(done)
        self._loop_tasks.difference_update(done)
        if pending:
            logger.error(
                "Delivery shutdown cleanup bound exhausted | worker={} pending={}",
                self.worker_id,
                len(pending),
            )

    async def _bounded_shutdown_operation(
        self,
        operation: Awaitable,
        *,
        deadline: float,
        label: str,
    ) -> None:
        task = asyncio.create_task(operation)
        remaining = max(0.0, deadline - time.monotonic())
        operation_seconds = remaining * 0.75
        done, pending = await asyncio.wait({task}, timeout=operation_seconds)
        if pending:
            task.cancel()
            done, pending = await asyncio.wait(
                pending,
                timeout=max(0.0, deadline - time.monotonic()),
            )
        self._report_task_results(done)
        if pending:
            logger.error(
                "Delivery shutdown operation exceeded absolute budget | "
                "worker={} operation={}",
                self.worker_id,
                label,
            )

    async def _finalize_shutdown(self) -> None:
        assert self._shutdown_deadline is not None
        remaining = max(0.0, self._shutdown_deadline - time.monotonic())
        if remaining <= 0:
            return
        heartbeat_deadline = time.monotonic() + (remaining / 2)
        await self._bounded_shutdown_operation(
            self._heartbeat(),
            deadline=heartbeat_deadline,
            label="heartbeat",
        )
        if self._shutdown_deadline - time.monotonic() <= 0:
            return
        await self._bounded_shutdown_operation(
            engine.dispose(),
            deadline=self._shutdown_deadline,
            label="engine_dispose",
        )

    async def run(self) -> None:
        logger.info(
            "Delivery worker started | worker={} dispatch_enabled={} configured_mode={}",
            self.worker_id,
            settings.delivery_dispatch_enabled,
            settings.delivery_mode,
        )
        try:
            while not self._stop.is_set():
                now = time.monotonic()
                if now - self._last_readiness_check >= settings.delivery_readiness_interval_seconds:
                    completed, _ = await self._await_loop_operation(
                        self._check_readiness()
                    )
                    if not completed:
                        break
                    self._last_readiness_check = now
                if now - self._last_maintenance >= settings.delivery_maintenance_interval_seconds:
                    completed, _ = await self._await_loop_operation(
                        self._maintenance()
                    )
                    if not completed:
                        break
                    self._last_maintenance = now
                self._schedule_retention(now)  # R2-S1: never await retention here
                if now - self._last_heartbeat >= settings.delivery_heartbeat_interval_seconds:
                    completed, _ = await self._await_loop_operation(self._heartbeat())
                    if not completed:
                        break
                    self._last_heartbeat = now
                # T08-3: replica compatibility heartbeat.
                if now - self._last_replica_beat >= replica_compat.HEARTBEAT_SECONDS:
                    completed, _ = await self._await_loop_operation(self._replica_beat())
                    if not completed:
                        break
                    self._last_replica_beat = now
                capacity = settings.delivery_worker_concurrency - self.active_count
                completed, claimed = await self._await_loop_operation(
                    self._claim(capacity)
                )
                if not completed or self._stop.is_set():
                    break
                for delivery in claimed:
                    self._track(delivery)
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=settings.delivery_poll_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            if not self._stop.is_set():
                self.request_stop()
            if self._retention_task is not None:
                self._retention_task.cancel()
            await self._drain_active()
            self._replica_drained = True  # T08-3
            self._readiness_ok = False
            self._provider_ready = False
            self._runtime_compatible = False
            await self._finalize_shutdown()
            logger.info("Delivery worker stopped | worker={}", self.worker_id)


async def _main() -> None:
    require_runtime_version(settings.delivery_worker_code_version)
    worker = DeliveryWorker()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, worker.request_stop)
        except NotImplementedError:
            pass
    await worker.run()


if __name__ == "__main__":
    setup_logger()
    asyncio.run(_main())
