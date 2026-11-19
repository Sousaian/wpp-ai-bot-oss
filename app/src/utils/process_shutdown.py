"""Process deadline survives event-loop stalls and asyncio.run teardown.

The daemon timer is armed by the FIRST signal, not by lifespan cleanup. It is
cancelled only after asyncio.run/Server.run has returned. Forced exit is a crash:
no new accounting/recovery path, and no promise of flushing arbitrary blocked
log sinks. The final static ERROR bypasses Loguru locks and Python buffering.
"""
from __future__ import annotations

import asyncio
import os
import threading
import time
from collections.abc import Callable

from loguru import logger

from src.utils.shutdown import APP_STOP_GRACE_SECONDS, WORKER_STOP_GRACE_SECONDS


PROCESS_MARGIN_SECONDS = .5
MARKER_RESERVE_SECONDS = .25
DISPOSE_RESERVE_SECONDS = .25
APP_HTTP_DRAIN_SECONDS = 8
EXIT_FORCED = 75
_counts = {"forced_exits": 0, "unsafe_stops": 0, "direct_shutdown_cancellations": 0, "marker_failed": 0}
_current: ProcessDeadline | None = None


def counts() -> dict[str, int]:
    return dict(_counts)


def signal(name: str) -> None:
    _counts[name] += 1
    logger.error("Process shutdown degraded | reason={} count={} threshold=1", name, _counts[name])


class ProcessDeadline:
    def __init__(self, grace_seconds: float, *, exit_process: Callable[[int], None] = os._exit):
        self.seconds = grace_seconds - PROCESS_MARGIN_SECONDS
        self.deadline: float | None = None
        self.unsafe = False
        self._timer: threading.Timer | None = None
        self._exit = exit_process

    def arm(self) -> None:
        if self.deadline is not None:
            return
        self.deadline = time.monotonic() + self.seconds
        self._timer = threading.Timer(self.seconds, self.force_exit)
        self._timer.daemon = True
        try:
            self._timer.start()
        except Exception:
            # No timer means no process bound. Crash now, never fall back to
            # an event-loop-only deadline (also safe in a raw signal handler).
            self.unsafe = True
            self.force_exit()

    def mark_unsafe(self) -> None:
        if not self.unsafe:
            self.unsafe = True
            signal("unsafe_stops")

    def force_exit(self) -> None:
        _counts["forced_exits"] += 1
        # Non-blocking, already-flushed diagnostic, no arbitrary values or
        # handler locks. A full/broken stderr must never defeat the deadline.
        try:
            os.set_blocking(2, False)
            os.write(2, b"ERROR | process_shutdown_forced | count=1 threshold=1 exit_code=75\n")
        except (OSError, ValueError):
            pass
        self._exit(EXIT_FORCED)

    def finish(self) -> None:
        # Called OUTSIDE asyncio.run; its unbounded _cancel_all_tasks may never
        # return. Unsafe cleanup must exit as crash even if teardown did finish.
        if self.unsafe:
            self.force_exit()
            return
        if self._timer is not None:
            self._timer.cancel()


def current() -> ProcessDeadline | None:
    return _current


def set_current(value: ProcessDeadline | None) -> None:
    global _current
    _current = value


def hard_deadline(guard: ProcessDeadline | None, fallback_seconds: float) -> float:
    # loop.time() and time.monotonic() use the same clock on supported CPython.
    if guard is not None:
        guard.arm()
        assert guard.deadline is not None
        return guard.deadline
    return asyncio.get_running_loop().time() + fallback_seconds


async def writers_quiescent(deadline: float, controllers: set[asyncio.Task]) -> bool:
    """Conservative: every task outside known process controllers may write.

    Controllers are captured before any application work is started. HTTP
    accept/discovery has stopped before this gate; no new producer can appear.
    Unknown tasks (including shield-created accounting/SQLAlchemy cleanup) are
    awaited, never blanket-cancelled. Recheck after callbacks have run.
    """
    while True:
        await asyncio.sleep(0)
        pending = asyncio.all_tasks() - controllers - {asyncio.current_task()}
        pending = {task for task in pending if not task.done()}
        if not pending:
            return True
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        await asyncio.wait(pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)


async def marker_finished(
    guard: ProcessDeadline | None, *, recorded: bool, deadline: float,
    controllers: set[asyncio.Task],
) -> None:
    """An absent best-effort marker is not itself a crash.

    Recheck ALL possible writers after the failed marker's DB cleanup. Only a
    quiescent process still inside its marker budget may exit normally; timeout
    or pending work keeps the original crash semantics and watchdog armed.
    """
    if guard is None or recorded or guard.unsafe:
        return
    if (asyncio.get_running_loop().time() >= deadline
            or not await writers_quiescent(deadline, controllers)
            or asyncio.get_running_loop().time() >= deadline):
        guard.mark_unsafe()
        return
    signal("marker_failed")
