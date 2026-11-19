import asyncio
import os
import unittest
from unittest.mock import patch, Mock, AsyncMock
from src.utils import process_shutdown as ps
from src.services import replica_compat as rc


class ProcessShutdownUnitTests(unittest.IsolatedAsyncioTestCase):
    def test_timer_first_signal_and_process_exit_code(self):
        exit_process = Mock()
        with patch.object(ps.threading, "Timer") as timer, patch.object(ps.time, "monotonic", return_value=10):
            guard = ps.ProcessDeadline(40, exit_process=exit_process)
            guard.arm()
            guard.arm()
            self.assertEqual(guard.deadline, 49.5)
            timer.assert_called_once_with(39.5, guard.force_exit)
            timer.return_value.start.assert_called_once()
            with patch.object(ps.os, "set_blocking"), patch.object(ps.os, "write") as write:
                guard.force_exit()
            exit_process.assert_called_once_with(75)
            self.assertIn(b"threshold=1", write.call_args.args[1])
        with patch.object(ps.threading, "Timer") as broken_timer, patch.object(ps.os, "set_blocking"), patch.object(ps.os, "write"):
            broken_timer.return_value.start.side_effect = RuntimeError("synthetic thread exhaustion")
            fail_exit = Mock()
            failed = ps.ProcessDeadline(40, exit_process=fail_exit)
            failed.arm()
            self.assertTrue(failed.unsafe)
            fail_exit.assert_called_once_with(75)

    def test_finish_does_not_cancel_timer_for_unsafe_shutdown(self):
        guard = ps.ProcessDeadline(10, exit_process=Mock())
        with patch.object(ps.threading, "Timer"), patch.object(guard, "force_exit") as force:
            guard.arm()
            guard.mark_unsafe()
            guard.finish()
            force.assert_called_once()

    async def test_quiescence_includes_shield_created_writer(self):
        release = asyncio.Event()
        task = asyncio.create_task(release.wait())
        try:
            safe = await ps.writers_quiescent(asyncio.get_running_loop().time() + .02, set())
            self.assertFalse(safe)
            self.assertFalse(task.cancelled())
            release.set()
            await task
            self.assertTrue(await ps.writers_quiescent(asyncio.get_running_loop().time() + .02, set()))
        finally:
            task.cancel()

    async def test_marker_withheld_when_gate_finds_pending_writer(self):
        with patch.object(rc, "beat", AsyncMock(return_value=True)) as beat:
            heartbeat = rc.HeartbeatLoop(None, kind="app")
            await heartbeat.stop(.1, before_marker=AsyncMock(return_value=False))
            self.assertFalse(heartbeat.stopped_recorded)
            beat.assert_not_awaited()

    def test_budget_uses_real_compose_grace_and_http_drain(self):
        self.assertEqual(ps.ProcessDeadline(40).seconds, 39.5)
        self.assertEqual(ps.ProcessDeadline(10).seconds, 9.5)
        self.assertEqual(ps.APP_HTTP_DRAIN_SECONDS, 8)
        self.assertEqual(39.5 - ps.MARKER_RESERVE_SECONDS - ps.DISPOSE_RESERVE_SECONDS, 39)

    def test_entrypoint_docs_http_and_concurrent_cache_contract_parity(self):
        from pathlib import Path
        from src import app_server
        from src.utils import shutdown
        from src.utils.access_log import PhoneRedactingAccessFilter
        import logging
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        self.assertIn('CMD ["python", "-m", "src.app_server"]', (root / "app/Dockerfile").read_text())
        docs = (root / "AGENTS.md").read_text()
        self.assertNotIn("ambas as cargas antes de parar", docs)
        self.assertIn("cancela concorrentemente ambas as cargas e o beat", docs)
        self.assertIn(f"timeout_graceful_shutdown={ps.APP_HTTP_DRAIN_SECONDS}", docs)
        self.assertIn("marker_failed", docs)
        self.assertIn("app_server_debug_reload_not_supported", docs)
        self.assertIn("APP_HOST", docs)
        runbook = (root / "docs/inbox-scheduler-runbook.md").read_text()
        self.assertIn("marker_failed", runbook)
        self.assertIn("app_server_debug_reload_not_supported", runbook)
        self.assertEqual(ps.APP_HTTP_DRAIN_SECONDS + shutdown.APP_CLEANUP_SECONDS + ps.PROCESS_MARGIN_SECONDS, 10)
        with patch.object(app_server, "DeadlineServer") as server:
            app_server.main(app=AsyncMock())
            config = server.call_args.args[0]
            self.assertEqual(config.timeout_graceful_shutdown, ps.APP_HTTP_DRAIN_SECONDS)
        self.assertTrue(any(isinstance(f, PhoneRedactingAccessFilter) for f in logging.getLogger("uvicorn.access").filters))

    def test_entrypoints_preserve_configured_host_port_and_docker_defaults(self):
        import os
        import runpy
        from src import app_server
        from src.config import Settings
        with patch.dict(os.environ, {"APP_HOST": "127.0.0.1", "APP_PORT": "5098", "DEBUG": "false"}):
            configured = Settings()
        with patch.object(app_server, "settings", configured), patch.object(app_server, "DeadlineServer") as server:
            app_server.main(app=AsyncMock())
            config = server.call_args.args[0]
            self.assertEqual((config.host, config.port), ("127.0.0.1", 5098))
        with patch.object(app_server, "main") as entrypoint:
            runpy.run_module("src.main", run_name="__main__")
            from src.config import settings
            self.assertEqual(entrypoint.call_args.kwargs, {"host": settings.app_host, "port": settings.app_port})
        self.assertEqual(Settings.model_fields["app_host"].default, "0.0.0.0")
        self.assertEqual(Settings.model_fields["app_port"].default, 5000)

    def test_debug_reload_is_refused_with_named_error_before_server_start(self):
        from src import app_server
        with patch.object(app_server.settings, "debug", True), patch.object(app_server, "DeadlineServer") as server, patch.object(app_server.logger, "error") as error:
            with self.assertRaises(SystemExit) as refused:
                app_server.main(app=AsyncMock())
            self.assertEqual(refused.exception.code, 2)
            server.assert_not_called()
            error.assert_called_once_with("app_server_debug_reload_not_supported")

    async def test_failed_marker_is_clean_only_without_pending_writers_or_timeout(self):
        exit_process = Mock()
        guard = ps.ProcessDeadline(10, exit_process=exit_process)
        before = ps.counts()
        await ps.marker_finished(guard, recorded=False, deadline=asyncio.get_running_loop().time() + .05, controllers=set())
        self.assertFalse(guard.unsafe)
        self.assertEqual(ps.counts()["marker_failed"], before["marker_failed"] + 1)
        self.assertEqual(ps.counts()["unsafe_stops"], before["unsafe_stops"])
        guard.finish()
        exit_process.assert_not_called()
        pending = asyncio.create_task(asyncio.Event().wait())
        try:
            await ps.marker_finished(guard, recorded=False, deadline=asyncio.get_running_loop().time() + .01, controllers=set())
            self.assertTrue(guard.unsafe)
            self.assertEqual(ps.counts()["marker_failed"], before["marker_failed"] + 1)
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        timed_out = ps.ProcessDeadline(10, exit_process=Mock())
        await ps.marker_finished(timed_out, recorded=False, deadline=0, controllers=set())
        self.assertTrue(timed_out.unsafe)

        # Same classification for an early dispose failure: an exception alone
        # is not pending SQL or exhausted time, and must not manufacture crash75.
        from src import main
        from types import SimpleNamespace
        clean = ps.ProcessDeadline(10, exit_process=Mock())
        try:
            with patch.object(main, "current", return_value=clean), patch.object(main, "_scheduler_health_cache", SimpleNamespace(aclose=AsyncMock())), patch.object(main, "_replica_health_cache", SimpleNamespace(aclose=AsyncMock())), patch.object(main, "engine", SimpleNamespace(dispose=AsyncMock(side_effect=RuntimeError("synthetic")))), patch.object(rc, "beat", AsyncMock(return_value=True)):
                async with main.lifespan(main.app):
                    pass
            self.assertFalse(clean.unsafe)
            clean.finish()
            clean._exit.assert_not_called()
        finally:
            if clean._timer is not None:
                clean._timer.cancel()
