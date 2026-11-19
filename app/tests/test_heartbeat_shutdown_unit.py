"""F0-T5: deadlines run against production shutdown code, no providers."""
import asyncio
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
import os
import unittest
from unittest.mock import AsyncMock, patch

from loguru import logger

from src import main, turn_worker
from src.config import Settings, settings
from src.services import replica_compat as rc
from src.utils import shutdown


class HeartbeatShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tasks = []
        self.release = asyncio.Event()

    async def asyncTearDown(self):
        self.release.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def stop_bounded(self, loop, budget=.08):
        task = asyncio.create_task(loop.stop(budget))
        self.tasks.append(task)
        done, _ = await asyncio.wait({task}, timeout=budget + .12)
        self.assertIn(task, done, "stop exceeded its total budget")
        task.result()

    async def test_cancel_cooperative_beat_and_write_marker_once(self):
        entered, cleaned = asyncio.Event(), asyncio.Event()
        markers = []

        async def beat(*args, stopped=False, **kwargs):
            if stopped:
                self.assertTrue(cleaned.is_set())
                markers.append(True)
                return True
            entered.set()
            try:
                await self.release.wait()
            finally:
                cleaned.set()

        with patch.object(rc, "beat", beat):
            loop = rc.HeartbeatLoop(None, kind="app")
            loop.start()
            await entered.wait()
            await self.stop_bounded(loop)
            await loop.stop(.001)
            loop.start()
        self.assertEqual(markers, [True])
        self.assertTrue(loop._task.cancelled())

    async def test_cancel_resistant_beat_cannot_extend_deadline(self):
        entered = asyncio.Event()
        markers = []

        async def beat(*args, stopped=False, **kwargs):
            if stopped:
                markers.append(True)
                return True
            entered.set()
            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    pass

        before = rc.counts()["heartbeat_failures"]
        logs = []
        sink = logger.add(lambda m: logs.append(m.record))
        try:
            with patch.object(rc, "beat", beat):
                loop = rc.HeartbeatLoop(None, kind="app")
                loop.start()
                self.tasks.append(loop._task)
                await entered.wait()
                await self.stop_bounded(loop)
        finally:
            logger.remove(sink)
        self.assertEqual(markers, [])
        self.assertEqual(rc.counts()["heartbeat_failures"], before + 1)
        self.assertTrue(any(r["level"].name == "ERROR" and r["exception"] is None
                            and "threshold=1" in r["message"] for r in logs))

    async def test_marker_uses_remaining_not_five_seconds(self):
        entered = asyncio.Event()
        marker = asyncio.Event()

        async def beat(*args, stopped=False, **kwargs):
            if stopped:
                marker.set()
                await self.release.wait()
                return True
            entered.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                await asyncio.sleep(.05)
                raise

        with patch.object(rc, "beat", beat):
            loop = rc.HeartbeatLoop(None, kind="app")
            loop.start()
            await entered.wait()
            await self.stop_bounded(loop, .08)
        self.assertTrue(marker.is_set())

    async def test_cancel_resistant_marker_cannot_extend_deadline(self):
        async def beat(*args, **kwargs):
            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    pass
        with patch.object(rc, "beat", beat):
            loop = rc.HeartbeatLoop(None, kind="app")
            # Capture marker task too, so a deliberately hostile double is
            # released even on a mutated implementation failure.
            original = asyncio.create_task
            def track(coro, **kw):
                task = original(coro, **kw)
                self.tasks.append(task)
                return task
            with patch.object(rc.asyncio, "create_task", track):
                await self.stop_bounded(loop)

    async def test_no_active_task_still_writes_marker(self):
        beat = AsyncMock(return_value=True)
        with patch.object(rc, "beat", beat):
            loop = rc.HeartbeatLoop(None, kind="app")
            await loop.stop(.1)
            await loop.stop(.1)
        beat.assert_awaited_once_with(None, kind="app", replica_id=loop.replica_id, stopped=True)

    async def test_zero_budget_signals_and_never_starts_marker(self):
        beat = AsyncMock(return_value=True)
        before = rc.counts()["heartbeat_failures"]
        with patch.object(rc, "beat", beat):
            await rc.HeartbeatLoop(None, kind="app").stop(0)
        beat.assert_not_awaited()
        self.assertEqual(rc.counts()["heartbeat_failures"], before + 1)

    async def test_caller_cancel_does_not_cancel_shared_shutdown(self):
        entered, cleaned = asyncio.Event(), asyncio.Event()
        async def beat(*args, **kwargs):
            entered.set()
            try:
                await self.release.wait()
            finally:
                cleaned.set()
            return True
        with patch.object(rc, "beat", beat):
            loop = rc.HeartbeatLoop(None, kind="app")
            caller_a = asyncio.create_task(loop.stop(.2))
            self.tasks.append(caller_a)
            await entered.wait()
            caller_b = asyncio.create_task(loop.stop(5))
            self.tasks.append(caller_b)
            caller_a.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await caller_a
            self.assertFalse(loop._shutdown_task.cancelled())
            self.release.set()
            results = await asyncio.wait_for(asyncio.gather(caller_b, return_exceptions=True), .3)
            self.assertEqual(results, [None])
            await loop.stop(.01)
            self.assertTrue(cleaned.is_set())
            self.assertTrue(loop.stopped_recorded)

    async def test_concurrent_stops_share_first_deadline_and_marker(self):
        beat = AsyncMock(return_value=True)
        with patch.object(rc, "beat", beat):
            loop = rc.HeartbeatLoop(None, kind="app")
            await asyncio.gather(loop.stop(.1), loop.stop(10))
        self.assertEqual(beat.await_count, 1)

    async def test_failed_marker_already_signals_heartbeat_failure(self):
        # Real beat catches ordinary DB failures; stop must not retry a marker.
        sessions = SimpleNamespace()
        before = rc.counts()["heartbeat_failures"]
        await rc.HeartbeatLoop(sessions, kind="app").stop(.1)
        self.assertEqual(rc.counts()["heartbeat_failures"], before + 1)

    async def test_app_caches_and_heartbeat_share_total_budget(self):
        entered = asyncio.Event()
        async def stalled(*args, **kwargs):
            entered.set()
            await self.release.wait()
        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "APP_CLEANUP_SECONDS", .08))
            stack.enter_context(patch.object(main._scheduler_health_cache, "aclose", stalled))
            stack.enter_context(patch.object(main._replica_health_cache, "aclose", stalled))
            stack.enter_context(patch.object(rc, "beat", stalled))
            async def lifecycle():
                async with main.lifespan(main.app):
                    await entered.wait()
            task = asyncio.create_task(lifecycle())
            self.tasks.append(task)
            done, _ = await asyncio.wait({task}, timeout=.3)
            self.assertIn(task, done)
            task.result()

    async def worker(self, *, heartbeat_error=False, slow_processor=False, slow_dispose=False):
        entered, drain_done = asyncio.Event(), asyncio.Event()
        callbacks = {}
        processor = SimpleNamespace(worker_id="synthetic")
        async def run(stop):
            entered.set()
            await self.release.wait()
        async def drain(grace):
            if slow_processor:
                await self.release.wait()
            drain_done.set()
        processor.run = run
        processor.shutdown = drain
        dispose = AsyncMock()
        if slow_dispose:
            async def stuck():
                await self.release.wait()
            dispose.side_effect = stuck
        hb = SimpleNamespace(start=lambda: None, stop=AsyncMock())
        if heartbeat_error:
            hb.stop.side_effect = RuntimeError("synthetic")
        with ExitStack() as stack:
            for name, value in (("inbox_scheduler_epoch", "epoch"), ("delivery_cutover_epoch", "epoch"),
                                ("delivery_shutdown_grace_seconds", .12),
                                ("delivery_shutdown_abort_seconds", .08)):
                stack.enter_context(patch.object(settings, name, value))
            stack.enter_context(patch.object(turn_worker, "TurnProcessor", return_value=processor))
            stack.enter_context(patch.object(turn_worker, "FinalCandidateRouter"))
            stack.enter_context(patch.object(rc, "HeartbeatLoop", return_value=hb))
            stack.enter_context(patch.object(rc, "schema_supported", AsyncMock(return_value=True)))
            stack.enter_context(patch.object(turn_worker, "engine", SimpleNamespace(dispose=dispose)))
            stack.enter_context(patch.object(asyncio.get_running_loop(), "add_signal_handler",
                lambda signum, callback: callbacks.setdefault(signum, callback)))
            task = asyncio.create_task(turn_worker._run())
            self.tasks.append(task)
            await entered.wait()
            started = asyncio.get_running_loop().time()
            callbacks[turn_worker.signal.SIGTERM]()
            # Repeated signal does not extend the window.
            callbacks[turn_worker.signal.SIGINT]()
            done, _ = await asyncio.wait({task}, timeout=.35)
            self.assertIn(task, done, "worker exceeded total drain + abort budget")
            self.assertLess(asyncio.get_running_loop().time() - started, .35)
            if heartbeat_error:
                with self.assertRaises(RuntimeError):
                    task.result()
            else:
                task.result()
            dispose.assert_awaited_once()
            if not slow_processor:
                self.assertTrue(drain_done.is_set())
            self.assertLessEqual(hb.stop.call_args.args[0], .2)
        return hb

    async def test_turn_worker_disposes_after_heartbeat_exception(self):
        await self.worker(heartbeat_error=True)

    async def test_turn_worker_bounds_processor_and_dispose(self):
        before = rc.counts()["cleanup_failures"]
        await self.worker(slow_processor=True, slow_dispose=True)
        self.assertGreaterEqual(rc.counts()["cleanup_failures"], before + 2)

    async def test_turn_worker_heartbeat_gets_remaining_budget(self):
        await self.worker()

    def test_compose_and_documented_budget_parity(self):
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        compose = (root / "docker-compose.yml").read_text()
        for service, grace in (("app", shutdown.APP_STOP_GRACE_SECONDS),
                               ("turn-worker", shutdown.WORKER_STOP_GRACE_SECONDS),
                               ("delivery-worker", shutdown.WORKER_STOP_GRACE_SECONDS)):
            block = compose.split(f"\n  {service}:\n")[1]
            import re
            block = re.split(r"\n  [\w-]+:", block)[0]
            self.assertIn(f"stop_grace_period: {int(grace)}s", block)
        self.assertEqual(Settings.model_fields["delivery_shutdown_grace_seconds"].default, 30)
        self.assertEqual(Settings.model_fields["delivery_shutdown_abort_seconds"].default, 5)
        self.assertLess(30 + 5, shutdown.WORKER_STOP_GRACE_SECONDS)
        self.assertLess(shutdown.APP_CLEANUP_SECONDS, shutdown.APP_STOP_GRACE_SECONDS)
        self.assertLess(shutdown.APP_CLEANUP_SECONDS, rc.STOP_BEAT_TIMEOUT_SECONDS)
        docs = (root / "AGENTS.md").read_text()
        self.assertIn(f"APP_CLEANUP_SECONDS={shutdown.APP_CLEANUP_SECONDS:g}", docs)
        self.assertIn(f"STOP_BEAT_TIMEOUT_SECONDS={rc.STOP_BEAT_TIMEOUT_SECONDS:g}", docs)
        env = (root / ".env.example").read_text()
        self.assertIn(
            f"cleanup {shutdown.APP_CLEANUP_SECONDS:g}s < explicit Compose grace {shutdown.APP_STOP_GRACE_SECONDS:g}s",
            env,
        )
        self.assertIn(f"stop_grace_period: {int(shutdown.WORKER_STOP_GRACE_SECONDS)}s", docs)
        self.assertIn("DELIVERY_SHUTDOWN_GRACE_SECONDS=30", env)
        self.assertIn("DELIVERY_SHUTDOWN_ABORT_SECONDS=5", (root / ".env.example").read_text())
