"""Monotonic shutdown deadlines, including cancellation-resistant cleanup.

Unlike wait_for(), wait() does not extend the deadline while a cancelled task
cleans up. Late tasks are cancelled and observed, never silently discarded.
No helper can preempt synchronous code blocking the event loop.
"""

from __future__ import annotations

import asyncio


# Compose parity is tested; reserve time before Docker's SIGKILL.
APP_STOP_GRACE_SECONDS = 10.0
WORKER_STOP_GRACE_SECONDS = 40.0
APP_CLEANUP_SECONDS = 1.5  # HTTP drain 8s + cleanup 1.5s + process margin .5s


def observe(task: asyncio.Task) -> None:
    if not task.cancelled():
        task.exception()


async def wait_until(task: asyncio.Task, deadline: float) -> bool:
    """Wait only until deadline; caller cancellation cancels owned task too."""

    task.add_done_callback(observe)
    try:
        done, _ = await asyncio.wait(
            {task}, timeout=max(0.0, deadline - asyncio.get_running_loop().time())
        )
        return bool(done)
    finally:
        if not task.done():
            task.cancel()
