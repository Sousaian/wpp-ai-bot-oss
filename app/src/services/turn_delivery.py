"""Final-candidate router for ticket06 automated delivery admission."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import settings
from src.database.repositories import scheduler_repo, turn_delivery_repo
from src.database.repositories import turn_generation_repo as generation
from src.services.turn_handoff import HandoffFinalizer
from src.services.turn_processor import LeaseHandle


T = TypeVar("T")


class FinalCandidateRouter:
    """Route final handoffs to ticket05 and all admitted sends to ticket06."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        handoff: HandoffFinalizer | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        self._sessions = sessions
        self._handoff = handoff or HandoffFinalizer(sessions)
        self._ttl = ttl_seconds or settings.delivery_automated_ttl_seconds

    async def _txn(self, operation: Callable[[AsyncSession], Awaitable[T]]) -> T:
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self._sessions, operation
        )

    async def __call__(
        self,
        candidate: generation.FinalCandidate,
        handle: LeaseHandle,
    ) -> object:
        if candidate.revision_status == "final_candidate" and candidate.needs_handoff:
            return await self._handoff(candidate, handle)
        if handle.lost.is_set():
            return "lost"
        return await self._txn(
            lambda db: turn_delivery_repo.admit_candidate(
                db,
                candidate=candidate,
                ttl_seconds=self._ttl,
            )
        )
