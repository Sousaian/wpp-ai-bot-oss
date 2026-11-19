"""F0-T5 real MySQL cancellation: MDL, row write, lifecycle drain.

Requires an explicitly marked disposable schema and PROCESS / performance_schema
read access. No provider call. All waits use observed DB barriers, not sleeps to
assume that a statement has reached a lock.
"""
import asyncio
from contextlib import ExitStack
import os
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import turn_worker
from src.database.repositories import replica_repo
from src.services import replica_compat as rc
from tests.disposable_db import assert_disposable_database


class HeartbeatShutdownMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        url = os.environ["OUTBOX_TEST_DATABASE_URL"]
        self.engine = create_async_engine(url, pool_size=3, max_overflow=0)
        self.observer = create_async_engine(url, poolclass=NullPool)
        await assert_disposable_database(self, self.engine)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self.replica_id = str(uuid4())
        self.tasks = []
        self.invalidated = asyncio.Event()
        self.invalidations = []
        @event.listens_for(self.engine.sync_engine, "invalidate")
        def invalidated(connection, record, exception):
            self.invalidations.append(True)
            self.invalidated.set()
        self.holder = None
        self.release = asyncio.Event()

    async def asyncTearDown(self):
        self.release.set()
        if self.holder is not None:
            await self.holder.execute(text("UNLOCK TABLES"))
            await self.holder.close()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        async with self.observer.begin() as conn:
            await conn.execute(text("DELETE FROM replica_heartbeats WHERE replica_id=:r"), {"r": self.replica_id})
        await self.engine.dispose()
        await self.observer.dispose()

    async def poll(self, operation, *, timeout=3):
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            value = await operation()
            if value:
                return value
            self.assertLess(asyncio.get_running_loop().time(), deadline, "DB barrier not reached")
            await asyncio.sleep(.05)

    async def scalar(self, sql, **params):
        async with self.observer.connect() as conn:
            return (await conn.execute(text(sql), params)).scalar_one()

    async def assert_no_orphans(self, connection_id):
        async def absent():
            return not await self.scalar(
                "SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_ID=:id", id=connection_id)
        await self.poll(absent)
        async def no_transaction():
            count = await self.scalar(
                "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id=:id", id=connection_id)
            if count:
                await asyncio.sleep(.25)
            return not count
        # INNODB_TRX's diagnostic snapshot can lag the connection teardown.
        await self.poll(no_transaction, timeout=5)
        self.assertEqual(await self.scalar(
            "SELECT COUNT(*) FROM performance_schema.metadata_locks l JOIN performance_schema.threads t "
            "ON t.THREAD_ID=l.OWNER_THREAD_ID WHERE t.PROCESSLIST_ID=:id", id=connection_id), 0)
        self.assertEqual(await self.scalar(
            "SELECT COUNT(*) FROM performance_schema.data_locks l JOIN performance_schema.threads t "
            "ON t.THREAD_ID=l.THREAD_ID WHERE t.PROCESSLIST_ID=:id", id=connection_id), 0)
        print("F0T5_ORPHANS → connection_gone=1 innodb_trx=0 metadata_locks=0 data_locks=0", flush=True)

    async def stop_bounded(self, heartbeat, budget=.6):
        started = asyncio.get_running_loop().time()
        task = asyncio.create_task(heartbeat.stop(budget))
        self.tasks.append(task)
        done, _ = await asyncio.wait({task}, timeout=budget + .2)
        self.assertIn(task, done, "heartbeat shutdown exceeded total deadline")
        task.result()
        self.assertLess(asyncio.get_running_loop().time() - started, budget + .2)

    async def mdl_case(self, *, release_for_marker):
        self.assertTrue(await rc.beat(self.sessions, kind="app", replica_id=self.replica_id))
        self.holder = await self.observer.connect()
        await self.holder.execute(text("LOCK TABLES replica_heartbeats WRITE"))
        entered = asyncio.Event()
        original = replica_repo.record
        connection_id = None
        async def record(db, **kwargs):
            nonlocal connection_id
            if not kwargs["stopped"]:
                connection_id = (await db.execute(text("SELECT CONNECTION_ID()"))).scalar_one()
                entered.set()
            return await original(db, **kwargs)
        with patch.object(replica_repo, "record", record):
            heartbeat = rc.HeartbeatLoop(self.sessions, kind="app", replica_id=self.replica_id)
            heartbeat.start()
            self.tasks.append(heartbeat._task)
            await entered.wait()
            async def waiting_mdl():
                return await self.scalar(
                    "SELECT COUNT(*) FROM performance_schema.metadata_locks l "
                    "JOIN performance_schema.threads t ON t.THREAD_ID=l.OWNER_THREAD_ID "
                    "WHERE t.PROCESSLIST_ID=:id AND l.OBJECT_NAME='replica_heartbeats' "
                    "AND l.LOCK_STATUS='PENDING'", id=connection_id)
            self.assertGreater(await self.poll(waiting_mdl), 0)
            print("F0T5_MDL → separate_holder=1 heartbeat_lock_status=PENDING", flush=True)
            async def unlock_on_invalidation():
                await self.invalidated.wait()
                await self.holder.execute(text("UNLOCK TABLES"))
            if release_for_marker:
                task = asyncio.create_task(unlock_on_invalidation())
                self.tasks.append(task)
            before = rc.counts()["heartbeat_failures"]
            await self.stop_bounded(heartbeat)
            if not release_for_marker:
                self.assertGreater(rc.counts()["heartbeat_failures"], before)
                await self.holder.execute(text("UNLOCK TABLES"))
            else:
                self.assertTrue(self.invalidated.is_set())
                self.assertEqual(await self.scalar(
                    "SELECT COUNT(*) FROM replica_heartbeats WHERE replica_id=:r AND stopped_at IS NOT NULL",
                    r=self.replica_id), 1)
                print("F0T5_MDL → stopped_marker=1 before_total_deadline=1", flush=True)
        await self.assert_no_orphans(connection_id)

    async def test_real_mdl_cancel_then_marker_with_remaining_budget(self):
        await self.mdl_case(release_for_marker=True)

    async def test_real_mdl_held_through_deadline_is_bounded(self):
        await self.mdl_case(release_for_marker=False)

    async def test_cancel_after_row_write_invalidates_before_marker(self):
        entered = asyncio.Event()
        original = replica_repo.record
        connection_id = None
        async def record(db, **kwargs):
            nonlocal connection_id
            await original(db, **kwargs)
            if not kwargs["stopped"]:
                connection_id = (await db.execute(text("SELECT CONNECTION_ID()"))).scalar_one()
                entered.set()
                await self.release.wait()  # open transaction really owns the X lock
            else:
                self.assertTrue(self.invalidated.is_set(), "cancelled write connection must be invalidated")
        with patch.object(replica_repo, "record", record):
            heartbeat = rc.HeartbeatLoop(self.sessions, kind="app", replica_id=self.replica_id)
            heartbeat.start()
            self.tasks.append(heartbeat._task)
            await entered.wait()
            self.assertGreater(await self.scalar(
                "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id=:id", id=connection_id), 0)
            self.assertGreater(await self.scalar(
                "SELECT COUNT(*) FROM performance_schema.data_locks l JOIN performance_schema.threads t "
                "ON t.THREAD_ID=l.THREAD_ID WHERE t.PROCESSLIST_ID=:id AND l.LOCK_MODE LIKE '%X%'", id=connection_id), 0)
            await self.stop_bounded(heartbeat)
            self.assertTrue(self.invalidations)
        await self.assert_no_orphans(connection_id)
        self.assertEqual(await self.scalar(
            "SELECT COUNT(*) FROM replica_heartbeats WHERE replica_id=:r AND stopped_at IS NOT NULL", r=self.replica_id), 1)

    async def test_turn_worker_with_real_mdl_beat_disposes(self):
        # Same production _run(), real HeartbeatLoop + MySQL, synthetic processor.
        self.holder = await self.observer.connect()
        await self.holder.execute(text("LOCK TABLES replica_heartbeats WRITE"))
        heartbeat = rc.HeartbeatLoop(self.sessions, kind="turn_worker", replica_id=self.replica_id)
        callbacks = {}
        started = asyncio.Event()
        processor = AsyncMock()
        processor.worker_id = "synthetic"
        async def run(stop):
            started.set()
            await self.release.wait()
        processor.run.side_effect = run
        dispose = AsyncMock(wraps=self.engine.dispose)
        from src.config import settings
        with ExitStack() as patches:
            for name, value in (("inbox_scheduler_epoch", str(uuid4())), ("delivery_cutover_epoch", str(uuid4())),
                                ("delivery_shutdown_grace_seconds", .2), ("delivery_shutdown_abort_seconds", .2)):
                patches.enter_context(patch.object(settings, name, value))
            patches.enter_context(patch.object(rc, "schema_supported", AsyncMock(return_value=True)))
            patches.enter_context(patch.object(rc, "HeartbeatLoop", return_value=heartbeat))
            patches.enter_context(patch.object(turn_worker, "TurnProcessor", return_value=processor))
            patches.enter_context(patch.object(turn_worker, "engine", type("Engine", (), {"dispose": dispose})()))
            patches.enter_context(patch.object(asyncio.get_running_loop(), "add_signal_handler",
                lambda s, cb: callbacks.setdefault(s, cb)))
            task = asyncio.create_task(turn_worker._run())
            self.tasks.append(task)
            await started.wait()
            async def pending():
                return await self.scalar(
                    "SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_NAME='replica_heartbeats' "
                    "AND LOCK_STATUS='PENDING'")
            await self.poll(pending)
            before = asyncio.get_running_loop().time()
            callbacks[turn_worker.signal.SIGTERM]()
            done, _ = await asyncio.wait({task}, timeout=.65)
            self.assertIn(task, done)
            task.result()
            self.assertLess(asyncio.get_running_loop().time() - before, .65)
            dispose.assert_awaited_once()
            print("F0T5_WORKER → real_mdl=1 dispose=1 signal_budget=0.4s tolerance=0.25s", flush=True)
