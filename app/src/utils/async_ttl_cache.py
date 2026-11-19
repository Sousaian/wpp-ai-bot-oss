"""Small process-local async TTL cache with single-flight refreshes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from time import monotonic
from typing import Generic, TypeVar, cast


T = TypeVar("T")
_MISSING = object()


class AsyncSingleFlightTTL(Generic[T]):
    """Cache one value and coalesce concurrent refreshes.

    A cached value is returned only while ``clock() < expires_at``.  Once the
    TTL expires, every caller awaits the same refresh task; an old healthy
    value is never used as a fallback when that refresh fails.
    """

    def __init__(self, *, clock: Callable[[], float] = monotonic) -> None:
        self._clock = clock
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = asyncio.Lock()
        self._value: T | object = _MISSING
        self._expires_at = 0.0
        self._error: Exception | None = None
        self._closed = False
        self._inflight: asyncio.Task[T] | None = None

    def _bind_loop(self) -> None:
        # Module-level instance: a new event loop (tests, restart) never sees
        # a lock or task bound to a previous loop, nor its cached value.
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self._loop = loop
            self._lock = asyncio.Lock()
            self._inflight = None
            self._closed = False
            self.reset()

    async def get(
        self,
        loader: Callable[[], Awaitable[T]],
        *,
        ttl_seconds: float,
    ) -> T:
        self._bind_loop()
        if self._closed:
            raise asyncio.CancelledError("health cache is closed")
        now = self._clock()
        if self._value is not _MISSING and now < self._expires_at:
            return cast(T, self._value)

        if self._error is not None and now < self._expires_at:
            raise self._error

        async with self._lock:
            now = self._clock()
            if self._value is not _MISSING and now < self._expires_at:
                return cast(T, self._value)
            if self._error is not None and now < self._expires_at:
                raise self._error
            task = self._inflight
            if task is None:
                task = asyncio.create_task(self._load(loader, ttl_seconds))
                self._inflight = task
                # Observe errors even if every HTTP waiter disconnects.
                task.add_done_callback(self._observe_result)

        # R2-S4: a disconnected waiter never owns cancellation or cleanup.
        return await asyncio.shield(task)

    async def _load(self, loader: Callable[[], Awaitable[T]], ttl_seconds: float) -> T:
        """Publish and retire exactly once, even when no waiters remain.

        There is no await between publication and retirement. Only this task
        clears its flight; waiter cancellation cannot open a second flight.
        """

        try:
            value = await loader()
        except Exception as exc:
            # Normal health loaders normalize failures to unavailable reports.
            # Unexpected exceptions are also cached, never an old healthy value.
            self._value = _MISSING
            self._error = exc
            self._expires_at = self._clock() + ttl_seconds
            raise
        else:
            self._value = value
            self._error = None
            self._expires_at = self._clock() + ttl_seconds
            return value
        finally:
            if self._inflight is asyncio.current_task():
                self._inflight = None

    @staticmethod
    def _observe_result(task: asyncio.Task[T]) -> None:
        if not task.cancelled():
            task.exception()

    async def aclose(self) -> None:
        """Process shutdown owns cancellation; drain the load's cleanup."""

        self._bind_loop()
        self._closed = True
        task = self._inflight
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.reset()

    def reset(self) -> None:
        """Drop only completed cache state; intended for process/test lifecycle."""

        self._value = _MISSING
        self._error = None
        self._expires_at = 0.0

