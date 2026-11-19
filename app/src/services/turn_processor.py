"""Separate, fenced per-number turn processor (ticket 04).

One locally authorized generation turn per agent lane; distinct agents run
concurrently. Each turn is driven from durable state by short transactions in
``turn_generation_repo``; no database lock is held while waiting or while the
provider call runs. The processor stops at the post-typing final-candidate
seal and hands it to a consumer. Ticket06 adds best-effort Evolution presence;
durable text remains in the outbox and is never sent by this processor.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import random
from typing import Protocol, TypeVar
from uuid import uuid4

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.agents import agent_runner
from src.config import settings
from src.database.repositories import (
    delivery_state_repo,
    lifecycle_repo,
    scheduler_repo,
    turn_generation_repo as repo,
    turn_delivery_repo,
)
from src.services import llm_client
from src.services.delivery_adapters import send_evolution_presence
from src.services.evolution_orchestrator import (
    MENSAGEM_ERRO_TECNICO,
    MENSAGEM_LIMITE_ATINGIDO,
)


T = TypeVar("T")
LLMCall = Callable[..., Awaitable[llm_client.LLMResult]]
PresenceCall = Callable[..., Awaitable[int]]

TYPING_MIN_MS = 2000
TYPING_MAX_MS = 12000


def typing_schedule(
    text: str,
    *,
    jitter_max_ms: int,
    rng: random.Random,
) -> tuple[int, int]:
    """Balanced length-based delay: clamp(2 + chars/25, 2, 12)s, bounded jitter.

    Returns (persisted delay, drawn jitter). The delay is clamped again after
    jitter so it always stays inside 2-12 seconds.
    """

    if not 0 <= jitter_max_ms <= 1000:
        raise ValueError("typing jitter must be within 0-1000 ms")
    base_ms = round(min(max(2.0 + len(text) / 25.0, 2.0), 12.0) * 1000)
    jitter_ms = rng.randint(-jitter_max_ms, jitter_max_ms) if jitter_max_ms else 0
    delay_ms = min(max(base_ms + jitter_ms, TYPING_MIN_MS), TYPING_MAX_MS)
    return delay_ms, jitter_ms


@dataclass
class LeaseHandle:
    """Ownership signal shared with a final-candidate consumer."""

    lease: repo.TurnLease
    lost: asyncio.Event = field(default_factory=asyncio.Event)


class FinalCandidateConsumer(Protocol):
    async def __call__(
        self,
        candidate: repo.FinalCandidate,
        handle: LeaseHandle,
    ) -> None: ...


def outcome_from_result(result: llm_client.LLMResult) -> repo.GenerationOutcome:
    return repo.GenerationOutcome(
        outcome="succeeded",
        usage=repo.GenerationUsage(
            modelo_llm=result.modelo_llm,
            tokens_input=result.tokens_input,
            tokens_output=result.tokens_output,
            tokens_reasoning=result.tokens_reasoning,
            tokens_cache_read=result.tokens_cache_read,
            tokens_cache_write=result.tokens_cache_write,
            custo_creditos=result.custo_creditos,
            provider_generation_id=result.provider_generation_id,
            modelo_llm_reportado=result.modelo_llm_reportado,
        ),
    )


def draft_for(
    outcome: repo.GenerationOutcome,
    result: llm_client.LLMResult | None,
) -> repo.DraftInput | None:
    """Controlled draft for a known outcome; ``None`` for an unknown one."""

    if outcome.outcome == "outcome_unknown":
        return None
    if outcome.outcome == "succeeded" and result is not None:
        texto, needs_transfer = agent_runner.normalizar_resposta(result.texto)
        if texto or needs_transfer:
            return repo.DraftInput("generated", texto, needs_transfer)
    return repo.DraftInput("technical_fallback", MENSAGEM_ERRO_TECNICO, False)


class TurnProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        worker_id: str,
        scheduler_epoch: str,
        delivery_runtime_epoch: str,
        consumer: FinalCandidateConsumer,
        llm_call: LLMCall | None = None,
        presence_call: PresenceCall | None = None,
        lease_seconds: int | None = None,
        poll_interval_seconds: float | None = None,
        generation_timeout_seconds: float | None = None,
        typing_jitter_ms: int | None = None,
        release_backoff_seconds: int | None = None,
        concurrency: int | None = None,
        rng: random.Random | None = None,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        self._sessions = session_factory
        self.worker_id = worker_id.strip()
        self.scheduler_epoch = scheduler_epoch
        self.delivery_runtime_epoch = delivery_runtime_epoch
        self._consumer = consumer
        self._llm_call = llm_call or llm_client.completar_tentativa_unica
        self._presence_call = presence_call or send_evolution_presence
        self.lease_seconds = lease_seconds or settings.inbox_turn_lease_seconds
        self.poll_interval = (
            poll_interval_seconds
            if poll_interval_seconds is not None
            else settings.inbox_turn_poll_interval_seconds
        )
        self.generation_timeout = (
            generation_timeout_seconds
            if generation_timeout_seconds is not None
            else settings.inbox_generation_timeout_seconds
        )
        self.release_backoff = (
            release_backoff_seconds or settings.inbox_turn_release_backoff_seconds
        )
        self.concurrency = concurrency or settings.inbox_turn_processor_concurrency
        jitter = (
            typing_jitter_ms if typing_jitter_ms is not None else settings.inbox_typing_jitter_ms
        )
        rng = rng or random.Random()
        self.policy = repo.TurnPolicy(
            lease_seconds=self.lease_seconds,
            release_backoff_seconds=self.release_backoff,
            soft_limit_text=MENSAGEM_LIMITE_ATINGIDO,
            technical_fallback_text=MENSAGEM_ERRO_TECNICO,
            resolve_model=agent_runner.resolver_modelo_llm,
            typing_schedule=lambda text: typing_schedule(
                text, jitter_max_ms=jitter, rng=rng
            ),
        )
        self._stopping = asyncio.Event()
        self._active: dict[int, asyncio.Task[str]] = {}
        self._presence_sent: set[tuple[int, int]] = set()
        self._presence_tasks: dict[tuple[int, int], asyncio.Task[None]] = {}
        self._presence_failure_count = 0
        self._health_failure_count = 0

    @property
    def presence_failure_count(self) -> int:
        return self._presence_failure_count

    def _publish_health_snapshot(self) -> None:
        snapshot = {
            "worker_id": self.worker_id,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "active_agents": len(self._active),
            "presence_failures": self._presence_failure_count,
            "admitted_handoff_cap_parks": (
                scheduler_repo.admitted_handoff_park_signal_count()
            ),
            "handoff_cap_terminalizations": scheduler_repo.handoff_cap_signal_count(),
            "automated_terminal_at_admission": (
                turn_delivery_repo.terminal_at_admission_count()
            ),
            "handoff_effect_terminal": turn_delivery_repo.handoff_effect_terminal_count(),
            "lifecycle_settled": turn_delivery_repo.lifecycle_settle_count(),
            "epoch_mismatch_deferred": turn_delivery_repo.epoch_mismatch_count(),
            "fifo_deferred": turn_delivery_repo.fifo_defer_count(),
            "orphan_effects_resolved": scheduler_repo.orphan_effect_resolved_count(),
            "settle_waiting_live_lease": (
                scheduler_repo.settle_waiting_live_effect_count()
            ),
            "owner_lifecycle_cancelled": repo.lifecycle_cancel_count(),
            "lifecycle": lifecycle_repo.lifecycle_counts(),
            "runtime_gate_released": turn_delivery_repo.runtime_gate_release_count(),
            "signal_threshold": 1,
        }
        target = Path(settings.inbox_turn_worker_health_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(
            f".{target.name}.{self.worker_id}.{os.getpid()}.{uuid4().hex}.tmp"
        )
        try:
            temporary.write_text(json.dumps(snapshot, sort_keys=True))
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    # ── transactions ────────────────────────────────────────────────────
    async def _txn(self, operation: Callable[[AsyncSession], Awaitable[T]]) -> T:
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self._sessions, operation
        )

    # ── lifecycle ───────────────────────────────────────────────────────
    @property
    def active_agents(self) -> frozenset[int]:
        return frozenset(self._active)

    async def run_pass(self) -> list[asyncio.Task[str]]:
        """Recover expired lanes, then dispatch at most one task per agent."""

        if self._stopping.is_set():
            return []
        handoff_ready = await self._txn(
            lambda db: turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db,
                scheduler_epoch=self.scheduler_epoch,
                limit=self.concurrency * 2,
            )
        )
        for turn_id, agente_id, sessao_id in handoff_ready:
            await self._txn(
                lambda db, turn_id=turn_id, agente_id=agente_id, sessao_id=sessao_id: (
                    turn_delivery_repo.requeue_admitted_turn(
                        db,
                        turn_id=turn_id,
                        agente_id=agente_id,
                        sessao_id=sessao_id,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.delivery_runtime_epoch,
                    )
                )
            )
        expired = await self._txn(
            lambda db: repo.list_expired_lane_hints(db, limit=self.concurrency)
        )
        for agente_id, turn_id, sessao_id in expired:
            if agente_id in self._active:
                continue
            await self._recover(agente_id, hint=(turn_id, sessao_id))
        capacity = self.concurrency - len(self._active)
        if capacity <= 0:
            return []
        ready = await self._txn(
            lambda db: repo.list_ready_agent_ids(
                db,
                scheduler_epoch=self.scheduler_epoch,
                limit=self.concurrency * 2,
            )
        )
        started: list[asyncio.Task[str]] = []
        for agente_id in ready:
            if capacity <= 0:
                break
            if agente_id in self._active:
                continue
            task = asyncio.create_task(self.process_agent(agente_id))
            self._active[agente_id] = task
            task.add_done_callback(lambda _t, a=agente_id: self._active.pop(a, None))
            started.append(task)
            capacity -= 1
        return started

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set() and not self._stopping.is_set():
            try:
                await self.run_pass()
            except Exception as exc:
                logger.error(
                    "Turn processor pass failed | worker={} erro_tipo={}",
                    self.worker_id,
                    type(exc).__name__,
                )
            finally:
                try:
                    self._publish_health_snapshot()
                except Exception as exc:
                    self._health_failure_count += 1
                    logger.error(
                        "Turn worker health snapshot failed | worker={} erro_tipo={} "
                        "count={} threshold=1",
                        self.worker_id,
                        type(exc).__name__,
                        self._health_failure_count,
                    )
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

    async def shutdown(self, grace_seconds: float) -> None:
        """Stop claiming; let in-flight turns drain, then cancel what remains."""

        self._stopping.set()
        presence = list(self._presence_tasks.values())
        for task in presence:
            task.cancel()
        if presence:
            await asyncio.gather(*presence, return_exceptions=True)
        self._presence_tasks.clear()
        tasks = list(self._active.values())
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=grace_seconds)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _recover(
        self, agente_id: int, *, hint: tuple[int, int] | None = None
    ) -> None:
        async def operation(db: AsyncSession) -> str:
            now = await delivery_state_repo.get_database_now(db)
            return await scheduler_repo.recover_expired_lane(
                db,
                agente_id=agente_id,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_runtime_epoch,
                retry_at=now + timedelta(seconds=1),
                expected_turn_id=hint[0] if hint else None,
                expected_sessao_id=hint[1] if hint else None,
            )

        try:
            outcome = await self._txn(operation)
            logger.warning(
                "Expired turn lane recovered | agente={} resultado={}", agente_id, outcome
            )
        except (
            scheduler_repo.SchedulerConflictError,
            scheduler_repo.SchedulerNotReadyError,
            ValueError,
        ) as exc:
            logger.warning(
                "Expired lane recovery skipped | agente={} erro_tipo={}",
                agente_id,
                type(exc).__name__,
            )

    # ── one turn ────────────────────────────────────────────────────────
    async def process_agent(self, agente_id: int) -> str:
        if self._stopping.is_set():
            return "stopping"
        try:
            # Scalar discovery commits first; the claim transaction then takes
            # only locking reads before its last lock wait (§16.1 A1).
            hint = await self._txn(
                lambda db: repo.ready_turn_hint(
                    db, agente_id=agente_id, scheduler_epoch=self.scheduler_epoch
                )
            )
            if hint is None:
                return "idle"
            claim = await self._txn(
                lambda db: scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agente_id,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.delivery_runtime_epoch,
                    worker_id=self.worker_id,
                    lease_seconds=self.lease_seconds,
                    candidate=hint,
                )
            )
        except (
            scheduler_repo.SchedulerConflictError,
            scheduler_repo.SchedulerNotReadyError,
        ):
            return "not_claimed"
        if claim is None:
            return "idle"
        lease = repo.TurnLease.from_claim(claim)
        try:
            return await self._drive(lease)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Turn processing stopped | agente={} turn={} erro_tipo={}",
                lease.agente_id,
                lease.turn_id,
                type(exc).__name__,
            )
            return "error"
        finally:
            # Presence dedupe is per claimed turn; a later reclaim may re-type
            # only from the persisted deadline, never a rerolled delay.
            self._presence_sent = {
                key for key in self._presence_sent if key[0] != lease.turn_id
            }

    async def _drive(self, lease: repo.TurnLease) -> str:
        while True:
            if self._stopping.is_set():
                released = await self._txn(
                    lambda db: repo.release_turn(
                        db, lease=lease, delay_seconds=1, reason="shutdown"
                    )
                )
                return "released_shutdown" if released else "lost"
            try:
                step = await self._txn(
                    lambda db: repo.advance_turn(db, lease=lease, policy=self.policy)
                )
            except repo.StaleTurnOwnership:
                return "lost"
            if isinstance(step, repo.WaitStep):
                await self._wait(lease, step)
                continue
            if isinstance(step, repo.GenerateStep):
                result = await self._generate(lease, step)
                if result is not None:
                    return result
                continue
            if isinstance(step, repo.FinalCandidate):
                await self._hand_off(lease, step)
                return "final_candidate"
            return step.reason

    async def _wait(self, lease: repo.TurnLease, step: repo.WaitStep) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + max(0.0, (step.until - step.db_now).total_seconds())
        renew_due = started + self.lease_seconds / 3
        until = step.until
        if step.phase == "typing":
            await self._start_presence(lease, step)
        while not self._stopping.is_set():
            now = loop.time()
            if now >= deadline or now >= renew_due:
                return
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=min(deadline, renew_due, now + self.poll_interval) - now,
                )
                return
            except asyncio.TimeoutError:
                pass
            peek = await self._txn(lambda db: repo.peek_turn(db, turn_id=lease.turn_id))
            if (
                peek is None
                or peek.claim_token != lease.owner_token
                or peek.current_revision_id != step.revision_id
            ):
                return
            if step.phase == "typing" and peek.active_inputs:
                await self._pause_presence(lease, step.revision_id)
                return
            if step.phase == "collecting" and peek.ready_at != until:
                until = peek.ready_at
                deadline = loop.time() + max(
                    0.0, (peek.ready_at - peek.db_now).total_seconds()
                )

    async def _start_presence(self, lease: repo.TurnLease, step: repo.WaitStep) -> None:
        key = (lease.turn_id, step.revision_id)
        if key in self._presence_sent:
            return
        authority = await self._txn(
            lambda db: turn_delivery_repo.authorize_presence(
                db, lease=lease, revision_id=step.revision_id
            )
        )
        if authority is None or authority.remaining_delay_ms <= 0:
            return
        self._presence_sent.add(key)

        async def dispatch() -> None:
            try:
                await self._presence_call(
                    phone=authority.phone,
                    instance_name=authority.instance_name,
                    presence="composing",
                    delay_ms=authority.remaining_delay_ms,
                    timeout_seconds=authority.remaining_delay_ms / 1000 + 3,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._presence_failure_count += 1
                logger.error(
                    "Evolution composing failed; durable text preserved | "
                    "agente={} turn={} revision={} error_type={} count={} threshold=1",
                    lease.agente_id,
                    lease.turn_id,
                    step.revision_id,
                    type(exc).__name__,
                    self._presence_failure_count,
                )
            finally:
                self._presence_tasks.pop(key, None)

        self._presence_tasks[key] = asyncio.create_task(
            dispatch(), name=f"turn-presence-{lease.turn_id}-{step.revision_id}"
        )

    async def _pause_presence(self, lease: repo.TurnLease, revision_id: int) -> None:
        key = (lease.turn_id, revision_id)
        task = self._presence_tasks.pop(key, None)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        authority = await self._txn(
            lambda db: turn_delivery_repo.authorize_presence(
                db, lease=lease, revision_id=revision_id
            )
        )
        if authority is None:
            return
        try:
            await self._presence_call(
                phone=authority.phone,
                instance_name=authority.instance_name,
                presence="paused",
                delay_ms=0,
                timeout_seconds=3,
            )
        except Exception as exc:
            self._presence_failure_count += 1
            logger.error(
                "Evolution paused cleanup failed; durable text preserved | "
                "agente={} turn={} revision={} error_type={} count={} threshold=1",
                lease.agente_id,
                lease.turn_id,
                revision_id,
                type(exc).__name__,
                self._presence_failure_count,
            )

    async def _renew_loop(self, lease: repo.TurnLease, handle: LeaseHandle) -> None:
        interval = self.lease_seconds / 3
        while True:
            await asyncio.sleep(interval)
            try:
                renewed = await self._txn(
                    lambda db: scheduler_repo.renew_lane_lease(
                        db,
                        turn_id=lease.turn_id,
                        agente_id=lease.agente_id,
                        scheduler_epoch=lease.scheduler_epoch,
                        delivery_runtime_epoch=lease.delivery_runtime_epoch,
                        owner_token=lease.owner_token,
                        lease_seconds=self.lease_seconds,
                        sessao_id=lease.sessao_id,
                    )
                )
            except scheduler_repo.SchedulerNotReadyError:
                renewed = False
            except Exception as exc:
                logger.warning(
                    "Turn lease renewal failed | agente={} erro_tipo={}",
                    lease.agente_id,
                    type(exc).__name__,
                )
                continue
            if not renewed:
                handle.lost.set()
                return

    async def _await_provider(
        self,
        provider: asyncio.Task[llm_client.LLMResult],
        handle: LeaseHandle,
        drain_seconds: int,
    ) -> tuple[repo.GenerationOutcome, llm_client.LLMResult | None]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.generation_timeout
        lost_wait = asyncio.create_task(handle.lost.wait())
        try:
            done, _ = await asyncio.wait(
                {provider, lost_wait},
                timeout=self.generation_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if provider not in done and lost_wait in done:
                # Lease lost: no new I/O, but the started call may finish
                # inside the bounded accounting-drain budget.
                drain_until = min(deadline, loop.time() + drain_seconds)
                done, _ = await asyncio.wait(
                    {provider}, timeout=max(0.0, drain_until - loop.time())
                )
                code = "lease_lost_drain_timeout"
            else:
                code = "generation_timeout"
        finally:
            lost_wait.cancel()
        if provider not in done:
            provider.cancel()
            await asyncio.gather(provider, return_exceptions=True)
            return repo.GenerationOutcome("outcome_unknown", safe_error_code=code), None
        return self._classify(provider)

    @staticmethod
    def _classify(
        provider: asyncio.Task[llm_client.LLMResult],
    ) -> tuple[repo.GenerationOutcome, llm_client.LLMResult | None]:
        if provider.cancelled():
            return (
                repo.GenerationOutcome(
                    "outcome_unknown", safe_error_code="provider_call_cancelled"
                ),
                None,
            )
        exc = provider.exception()
        if exc is None:
            result = provider.result()
            return outcome_from_result(result), result
        if isinstance(exc, llm_client.LLMProviderError):
            kind = "failed" if exc.outcome_known else "outcome_unknown"
            return repo.GenerationOutcome(kind, safe_error_code=exc.safe_error_code), None
        return (
            repo.GenerationOutcome("outcome_unknown", safe_error_code="provider_exception"),
            None,
        )

    async def _generate(
        self,
        lease: repo.TurnLease,
        step: repo.GenerateStep,
    ) -> str | None:
        """Run one committed attempt; returns a terminal label or None to continue."""

        handle = LeaseHandle(lease)
        messages = agent_runner.montar_contexto_turno(
            step.system_prompt, step.history, step.batch
        )
        renewer = asyncio.create_task(self._renew_loop(lease, handle))
        provider = asyncio.create_task(
            self._llm_call(
                modelo_llm=step.modelo_llm,
                messages=messages,
                tipo_chamada="resposta",
            )
        )
        try:
            outcome, result = await self._await_provider(
                provider, handle, step.accounting_drain_seconds
            )
        except asyncio.CancelledError:
            # Forced shutdown: the remote call may still be billed.
            provider.cancel()
            renewer.cancel()
            await asyncio.gather(provider, renewer, return_exceptions=True)
            unknown = repo.GenerationOutcome(
                "outcome_unknown", safe_error_code="processor_cancelled"
            )
            await asyncio.shield(self._finish(lease, step, unknown, None, handle))
            raise
        renewer.cancel()
        await asyncio.gather(renewer, return_exceptions=True)
        return await self._finish(lease, step, outcome, result, handle)

    async def _finish(
        self,
        lease: repo.TurnLease,
        step: repo.GenerateStep,
        outcome: repo.GenerationOutcome,
        result: llm_client.LLMResult | None,
        handle: LeaseHandle,
    ) -> str | None:
        draft = draft_for(outcome, result)
        if not handle.lost.is_set():
            try:
                next_step = await self._txn(
                    lambda db: repo.record_generation_result(
                        db,
                        lease=lease,
                        attempt_key=step.attempt_key,
                        outcome=outcome,
                        draft=draft,
                        policy=self.policy,
                    )
                )
            except asyncio.CancelledError:
                # Cancelled while recording a known outcome: account it
                # exactly before propagating (never left ``started``).
                await asyncio.shield(self._account_only(lease, step, outcome))
                raise
            except Exception as exc:
                # Includes stale ownership and rejected accounting. Usage is
                # still finalized below for this exact attempt only.
                logger.warning(
                    "Owner generation finalization refused | agente={} turn={} erro_tipo={}",
                    lease.agente_id,
                    lease.turn_id,
                    type(exc).__name__,
                )
            else:
                if isinstance(next_step, repo.ReleasedStep):
                    return next_step.reason
                return None
        await asyncio.shield(self._account_only(lease, step, outcome))
        logger.warning(
            "Stale generation finalized accounting only | agente={} turn={}",
            lease.agente_id,
            lease.turn_id,
        )
        return "lost"

    async def _account_only(
        self,
        lease: repo.TurnLease,
        step: repo.GenerateStep,
        outcome: repo.GenerationOutcome,
    ) -> bool:
        return await self._txn(
            lambda db: repo.record_generation_accounting_only(
                db,
                agente_id=lease.agente_id,
                sessao_id=lease.sessao_id,
                attempt_key=step.attempt_key,
                lane_owner_token=lease.owner_token,
                outcome=outcome,
            )
        )

    async def _hand_off(
        self,
        lease: repo.TurnLease,
        candidate: repo.FinalCandidate,
    ) -> None:
        handle = LeaseHandle(lease)
        renewer = asyncio.create_task(self._renew_loop(lease, handle))
        try:
            await self._consumer(candidate, handle)
        finally:
            renewer.cancel()
            await asyncio.gather(renewer, return_exceptions=True)
