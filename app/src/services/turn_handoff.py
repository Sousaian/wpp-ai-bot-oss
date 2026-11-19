"""Final-candidate handoff consumer (ticket 05).

Runs under the turn processor's lease renewer. Short transactions only:
``begin_handoff`` (locks, soft limit, started summary attempt or fallback
finalization) -> provider call without locks -> ``complete_summary_and_finalize``
(exact accounting + atomic handoff admission in one UoW). A lost lease or any
refusal leaves only the exact-attempt accounting-only write. No delivery or
presence provider I/O happens here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.agents import agent_runner
from src.config import settings
from src.database.repositories import outbox_repo, scheduler_repo
from src.database.repositories import turn_generation_repo as generation
from src.database.repositories import turn_handoff_repo as repo
from src.services import llm_client
from src.services.delivery_adapters import InvalidDeliveryPayload
from src.services.turn_processor import LeaseHandle, TurnProcessor


T = TypeVar("T")
LLMCall = Callable[..., Awaitable[llm_client.LLMResult]]


TERMINAL_FAILURE_FALLBACK_SIGNAL_THRESHOLD = 1


def _deterministic_admission_failure(exc: BaseException) -> bool:
    """Allowlist failures proven local and repeatable; retry every unknown."""

    if isinstance(exc, generation.StaleTurnOwnership):
        return False
    # A bare AssertionError is deliberately absent: pymysql/aiomysql raise it
    # on wire-protocol desync, unwrapped by SQLAlchemy, so it is transient.
    # HandoffLocalInvariantError is raised only by our explicit checks.
    # HandoffAdmissionValidationError is raised only around our local inputs
    # and pure chunk planner (its ValueError/AssertionError are promoted at
    # that call boundary), never around a driver call.
    # OutboxConflictError and SchedulerConflictError are explicit durable-state
    # conflicts produced by our repositories after authority locks.
    # InvalidDeliveryPayload is our deterministic staged-payload validator.
    return isinstance(
        exc,
        (
            repo.HandoffLocalInvariantError,
            repo.HandoffAdmissionValidationError,
            outbox_repo.OutboxConflictError,
            scheduler_repo.SchedulerConflictError,
            InvalidDeliveryPayload,
        ),
    )


class HandoffFinalizer:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        llm_call: LLMCall | None = None,
        generation_timeout_seconds: float | None = None,
        setup_ttl_seconds: int | None = None,
        automated_ttl_seconds: int | None = None,
        release_backoff_seconds: int | None = None,
    ) -> None:
        self._sessions = sessions
        self._llm_call = llm_call or llm_client.completar_tentativa_unica
        self._timeout = (
            generation_timeout_seconds
            if generation_timeout_seconds is not None
            else settings.inbox_generation_timeout_seconds
        )
        self._setup_ttl = (
            setup_ttl_seconds
            if setup_ttl_seconds is not None
            else settings.delivery_handoff_setup_ttl_seconds
        )
        self._automated_ttl = (
            automated_ttl_seconds
            if automated_ttl_seconds is not None
            else settings.delivery_automated_ttl_seconds
        )
        self._release_backoff = (
            release_backoff_seconds or settings.inbox_turn_release_backoff_seconds
        )
        self._terminal_failure_fallback_count = 0

    @property
    def terminal_failure_fallback_count(self) -> int:
        return self._terminal_failure_fallback_count

    def _signal_terminal_failure_fallback(
        self,
        candidate: generation.FinalCandidate,
        original: BaseException,
        terminal: BaseException,
    ) -> None:
        self._terminal_failure_fallback_count += 1
        logger.error(
            "Handoff terminalization degraded to accounting only | "
            "agente={} turn={} original_tipo={} terminal_tipo={} count={} threshold={}",
            candidate.lease.agente_id,
            candidate.lease.turn_id,
            type(original).__name__,
            type(terminal).__name__,
            self._terminal_failure_fallback_count,
            TERMINAL_FAILURE_FALLBACK_SIGNAL_THRESHOLD,
        )

    async def _txn(self, operation: Callable[[AsyncSession], Awaitable[T]]) -> T:
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self._sessions, operation
        )

    async def __call__(
        self,
        candidate: generation.FinalCandidate,
        handle: LeaseHandle,
    ) -> repo.HandoffStep | repo.HandoffFailed | str:
        if not candidate.needs_handoff:
            raise ValueError("ticket05 consumer accepts only handoff candidates")
        try:
            step = await self._txn(
                lambda db: repo.begin_handoff(
                    db,
                    candidate=candidate,
                    resolve_model=agent_runner.resolver_modelo_llm,
                    setup_ttl_seconds=self._setup_ttl,
                    automated_ttl_seconds=self._automated_ttl,
                    release_backoff_seconds=self._release_backoff,
                )
            )
        except generation.StaleTurnOwnership:
            return "lost"
        except Exception as exc:
            if not _deterministic_admission_failure(exc):
                raise
            # Deterministic local failure (e.g. a fallback finalization):
            # terminal and visible, never a lease-expiry loop.
            try:
                return await self._fail_terminally(candidate, None, None, exc)
            except Exception as terminal_exc:
                self._signal_terminal_failure_fallback(candidate, exc, terminal_exc)
                raise
        if not isinstance(step, repo.SummaryCall):
            return step

        provider = asyncio.create_task(
            self._llm_call(
                modelo_llm=step.modelo_llm,
                messages=step.messages,
                tipo_chamada="resumo",
            )
        )
        try:
            outcome, result = await self._await_provider(provider, handle, step)
        except asyncio.CancelledError:
            # Forced shutdown: the remote call may still be billed.
            provider.cancel()
            await asyncio.gather(provider, return_exceptions=True)
            unknown = generation.GenerationOutcome(
                "outcome_unknown", safe_error_code="summary_cancelled"
            )
            await asyncio.shield(self._accounting_only(candidate, step, unknown))
            raise

        if not handle.lost.is_set():
            try:
                return await self._txn(
                    lambda db: repo.complete_summary_and_finalize(
                        db,
                        candidate=candidate,
                        attempt_key=step.attempt_key,
                        outcome=outcome,
                        summary_text=result.texto if result is not None else None,
                        setup_ttl_seconds=self._setup_ttl,
                        automated_ttl_seconds=self._automated_ttl,
                    )
                )
            except asyncio.CancelledError:
                # Cancelled mid-admission (shutdown): the summary outcome is
                # known and paid. Record that exact attempt before propagating
                # so lane recovery never turns it into outcome_unknown.
                await asyncio.shield(self._accounting_only(candidate, step, outcome))
                raise
            except Exception as exc:
                # The whole admission rolled back. Stale ownership, lifecycle
                # change or transient loss: account the exact attempt only (a
                # reclaim may repay the summary, bounded by the attempt cap).
                # A deterministic local failure would fail identically on
                # every reclaim, so it is closed terminally instead.
                logger.warning(
                    "Handoff admission refused after summary | agente={} turn={} erro_tipo={}",
                    candidate.lease.agente_id,
                    candidate.lease.turn_id,
                    type(exc).__name__,
                )
                if _deterministic_admission_failure(exc):
                    try:
                        return await self._fail_terminally(
                            candidate, step.attempt_key, outcome, exc
                        )
                    except asyncio.CancelledError:
                        await asyncio.shield(
                            self._accounting_only(candidate, step, outcome)
                        )
                        raise
                    except Exception as terminal_exc:
                        # Never strand a known paid attempt as ``started`` if
                        # the deterministic terminalization transaction itself
                        # fails. Account that exact attempt, emit the threshold
                        # signal above, and let bounded reclaim decide next.
                        self._signal_terminal_failure_fallback(
                            candidate, exc, terminal_exc
                        )
        # Shielded: a cancellation here must not leave the paid attempt started.
        await asyncio.shield(self._accounting_only(candidate, step, outcome))
        return "summary_accounting_only"

    async def _fail_terminally(
        self,
        candidate: generation.FinalCandidate,
        attempt_key: str | None,
        outcome: generation.GenerationOutcome | None,
        exc: BaseException,
    ) -> repo.HandoffFailed:
        return await self._txn(
            lambda db: repo.fail_handoff_terminally(
                db,
                candidate=candidate,
                attempt_key=attempt_key,
                outcome=outcome,
                reason=type(exc).__name__,
            )
        )

    async def _accounting_only(
        self,
        candidate: generation.FinalCandidate,
        step: repo.SummaryCall,
        outcome: generation.GenerationOutcome,
    ) -> bool:
        recorded = await self._txn(
            lambda db: generation.record_generation_accounting_only(
                db,
                agente_id=candidate.lease.agente_id,
                sessao_id=candidate.lease.sessao_id,
                attempt_key=step.attempt_key,
                lane_owner_token=candidate.lease.owner_token,
                outcome=outcome,
            )
        )
        logger.warning(
            "Stale handoff summary finalized accounting only | agente={} turn={} gravado={}",
            candidate.lease.agente_id,
            candidate.lease.turn_id,
            recorded,
        )
        return recorded

    async def _await_provider(
        self,
        provider: asyncio.Task[llm_client.LLMResult],
        handle: LeaseHandle,
        step: repo.SummaryCall,
    ) -> tuple[generation.GenerationOutcome, llm_client.LLMResult | None]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        lost_wait = asyncio.create_task(handle.lost.wait())
        try:
            done, _ = await asyncio.wait(
                {provider, lost_wait},
                timeout=self._timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if provider not in done and lost_wait in done:
                # Lease lost: no new I/O; the started call may finish inside
                # the bounded accounting-drain budget.
                drain_until = min(deadline, loop.time() + step.accounting_drain_seconds)
                done, _ = await asyncio.wait(
                    {provider}, timeout=max(0.0, drain_until - loop.time())
                )
                code = "summary_lease_lost_drain_timeout"
            else:
                code = "summary_timeout"
        finally:
            lost_wait.cancel()
            await asyncio.gather(lost_wait, return_exceptions=True)
        if provider not in done:
            provider.cancel()
            await asyncio.gather(provider, return_exceptions=True)
            return generation.GenerationOutcome("outcome_unknown", safe_error_code=code), None
        return TurnProcessor._classify(provider)
