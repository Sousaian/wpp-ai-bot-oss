"""Uvicorn entrypoint with signal-anchored PROCESS shutdown, not just lifespan."""
from __future__ import annotations

import asyncio
import signal as process_signal
import threading

import uvicorn
from loguru import logger

from src.utils.image_version import require_runtime_version
from src.config import settings
from src.utils.process_shutdown import (
    ProcessDeadline, set_current, signal, APP_STOP_GRACE_SECONDS, APP_HTTP_DRAIN_SECONDS,
)


class ShutdownRequestSignal:
    """Signal direct cancellation risk; never invent/rewrite usage accounting."""
    def __init__(self, app, guard):
        self.app, self.guard = app, guard

    async def __call__(self, scope, receive, send):
        try:
            return await self.app(scope, receive, send)
        except asyncio.CancelledError:
            if (self.guard.deadline is not None and settings.delivery_mode == "direct"
                    and scope.get("type") == "http" and scope.get("method") == "POST"
                    and scope.get("path") == "/webhook"):
                signal("direct_shutdown_cancellations")
            raise


class DeadlineServer(uvicorn.Server):
    def __init__(self, config, guard):
        super().__init__(config)
        self.guard = guard

    def install_signal_handlers(self):
        # Uvicorn 0.27 normally queues handlers via loop.add_signal_handler.
        # That cannot arm a timer while the loop is blocked. Python's raw
        # main-thread handler arms it immediately, then sets Uvicorn's flags.
        if threading.current_thread() is threading.main_thread():
            for signum in (process_signal.SIGTERM, process_signal.SIGINT):
                self.previous_handlers[signum] = process_signal.signal(signum, self.handle_exit)

    def run(self, sockets=None):
        self.previous_handlers = {}
        try:
            return super().run(sockets=sockets)
        finally:
            for signum, handler in self.previous_handlers.items():
                process_signal.signal(signum, handler)

    def handle_exit(self, sig, frame):
        self.guard.arm()  # Thread timer before HTTP drain/lifespan/asyncio.run.
        super().handle_exit(sig, frame)


def main(app=None, *, host: str | None = None, port: int | None = None) -> None:
    from src.utils.logger import setup_logger
    setup_logger("logs/app.log")
    require_runtime_version(settings.delivery_worker_code_version)
    if settings.debug:
        # Reload spawns a supervisor/child pair outside this single-process
        # signal/deadline contract. Refuse explicitly, never silently ignore it.
        logger.error("app_server_debug_reload_not_supported")
        raise SystemExit(2)
    guard = ProcessDeadline(APP_STOP_GRACE_SECONDS)
    set_current(guard)
    if app is None:
        from src.main import app
    config = uvicorn.Config(
        ShutdownRequestSignal(app, guard),
        host=settings.app_host if host is None else host,
        port=settings.app_port if port is None else port,
        timeout_graceful_shutdown=APP_HTTP_DRAIN_SECONDS,
        # request.client is the TCP peer; services/auth/origem.py alone decides
        # whether it is the trusted proxy (FORWARDED_ALLOW_IPS has no effect).
        proxy_headers=False,
    )
    # Config reconfigures stdlib logging; install privacy filter AFTER it.
    from src.utils.access_log import configure_uvicorn_access_log_redaction
    configure_uvicorn_access_log_redaction()
    try:
        DeadlineServer(config, guard).run()
        guard.finish()  # Not lifespan's end: includes asyncio.run teardown.
    finally:
        set_current(None)


if __name__ == "__main__":
    main()
