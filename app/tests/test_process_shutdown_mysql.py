"""P38: REAL process exits, production main()/Uvicorn, SIGTERM and MySQL MDL.

No coroutine-return assertion substitutes for process.wait(). Every subprocess
uses synthetic providers, inherits the disposable marker, and owns real sockets.
"""
import asyncio
from datetime import timedelta
import os
from pathlib import Path
import signal
import sys
from uuid import uuid4

from sqlalchemy import text, select, update, func
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.engine import make_url

from src.database.models import AgentServiceLane, ConversationTurn, GenerationAttempt, ConsumoLLM, LLMUsageReconciliation
from src.database.repositories import replica_repo, scheduler_repo, delivery_state_repo
from src.services import replica_compat
from tests.disposable_db import assert_disposable_database
from tests.test_turn_processor_mysql import TurnProcessorMySQLTests


class ProcessShutdownMySQLTests(TurnProcessorMySQLTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await assert_disposable_database(self, self.engine)
        self.children = []
        self.admin_engine = None
        self.db_user = None
        self.child_database_url = None
        self.child_environment: dict[str, str] = {}
        self.holder = None
        self.mdl_reader = None
        self.queued_lock = None
        self.release_file = Path("/runtime/p38-" + str(uuid4()))
        self.ids = set()
        self.replica_id = str(uuid4())
        self.trace = []
        self.lines = asyncio.Queue()
        self.readers = []
        self.request_task = None
        self.proxy = None
        self.proxy_writers = []
        self.proxy_tasks = []
        self.resume_proxy = asyncio.Event()
        self.freeze_proxy = asyncio.Event()
        self.frozen_traffic = asyncio.Event()
        self.proxy_url = None
        async with self.engine.begin() as conn:
            await conn.execute(text("UPDATE scheduler_runtime_state SET schema_version=3 WHERE id=1"))
        await replica_compat.beat(self.sessions, kind="turn_worker", replica_id=self.replica_id)

    async def asyncTearDown(self):
        for child in self.children:
            if child.returncode is None:
                child.kill()
                await child.wait()
        if self.proxy is not None:
            await self.close_proxy()
        if self.holder is not None:
            await self.unlock()
        if self.request_task is not None:
            self.request_task.cancel()
            await asyncio.gather(self.request_task, return_exceptions=True)
        await asyncio.gather(*self.readers, return_exceptions=True)
        if self.admin_engine is not None:
            if self.db_user is not None:
                async with self.admin_engine.connect() as admin:
                    await admin.execute(text(f"DROP USER IF EXISTS '{self.db_user}'@'%'"))
            await self.admin_engine.dispose()
        async with self.engine.begin() as conn:
            await conn.execute(text("DELETE FROM replica_heartbeats WHERE replica_id=:id"), {"id": self.replica_id})
        await super().asyncTearDown()

    async def lock(self, table):
        self.holder = await self.engine.connect()
        await self.holder.execute(text(f"LOCK TABLES {table} WRITE"))

    async def queue_lock(self, table):
        self.mdl_reader = await self.engine.connect()
        await self.mdl_reader.execute(text(f"SELECT id FROM {table} LIMIT 1"))
        self.holder = await self.engine.connect()
        cid = (await self.holder.execute(text("SELECT CONNECTION_ID()"))).scalar_one()
        self.queued_lock = asyncio.create_task(self.holder.execute(text(f"LOCK TABLES {table} WRITE")))
        async def queued():
            async with self.engine.connect() as conn:
                return (await conn.execute(text("SELECT COUNT(*) FROM performance_schema.metadata_locks l "
                    "JOIN performance_schema.threads t ON t.THREAD_ID=l.OWNER_THREAD_ID "
                    "WHERE t.PROCESSLIST_ID=:id AND l.OBJECT_NAME=:table AND l.LOCK_STATUS='PENDING'"),
                    {"id": cid, "table": table})).scalar_one() > 0
        await self._until(queued, timeout=5, message="MDL writer queued behind separate read transaction")

    async def unlock(self):
        if self.mdl_reader is not None:
            await self.mdl_reader.rollback()
            await self.mdl_reader.close()
            self.mdl_reader = None
        if self.queued_lock is not None:
            await self.queued_lock
            self.queued_lock = None
        await self.holder.execute(text("UNLOCK TABLES"))
        await self.holder.close()
        self.holder = None

    async def start(self, scenario):
        env = dict(os.environ,
            P38_SCENARIO=scenario, P38_REPLICA_ID=self.replica_id, P38_PORT="5099",
            P38_RELEASE_FILE=str(self.release_file),
            INBOX_TURN_PROCESSOR_ENABLED="true", INBOX_SCHEDULER_ENABLED="true",
            INBOX_SCHEDULER_EPOCH=self.scheduler_epoch, DELIVERY_CUTOVER_EPOCH=self.delivery_epoch,
            DELIVERY_MODE="outbox", PROVIDER_WEBHOOK_AUTH_MODE="enforced",
            INBOUND_RECEIPT_HMAC_CURRENT_VERSION="1", INBOUND_RECEIPT_HMAC_KEYS='{"1":"synthetic-process-receipt"}',
            INBOX_TURN_WORKER_HEALTH_PATH="/runtime/child-health.json",

        )
        if self.proxy_url is not None:
            env["DATABASE_URL"] = self.proxy_url
        if self.child_database_url is not None:
            env["DATABASE_URL"] = self.child_database_url
        if scenario == "app_direct":
            env.update(DELIVERY_MODE="direct", INBOX_SCHEDULER_ENABLED="false", INBOX_TURN_PROCESSOR_ENABLED="false")
        self.child_environment = env.copy()
        child = await asyncio.create_subprocess_exec(sys.executable,
            str(Path(__file__).with_name("process_shutdown_child.py")), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        self.children.append(child)
        async def pump(stream, label):
            async for raw in stream:
                line = raw.decode(errors="replace").rstrip()
                self.trace.append(label + " " + line)
                if line.startswith("PROOF_SQL"):
                    self.ids.add(int(line.split()[-1]))
                if label == "stdout":
                    self.lines.put_nowait(line)
        self.readers.extend([asyncio.create_task(pump(child.stdout, "stdout")),
                             asyncio.create_task(pump(child.stderr, "stderr"))])
        return child

    async def wait_line(self, prefix, timeout=8):
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            try:
                line = await asyncio.wait_for(self.lines.get(), max(.001, deadline - asyncio.get_running_loop().time()))
            except asyncio.TimeoutError:
                self.fail("subprocess barrier missing: " + prefix + "\n" + "\n".join(self.trace))
            if line.startswith(prefix):
                return line

    async def pending_mdl(self, table):
        async def pending():
            async with self.engine.connect() as conn:
                rows = (await conn.execute(text(
                    "SELECT t.PROCESSLIST_ID FROM performance_schema.metadata_locks l "
                    "JOIN performance_schema.threads t ON t.THREAD_ID=l.OWNER_THREAD_ID "
                    "WHERE l.OBJECT_NAME=:table AND l.LOCK_STATUS='PENDING'"), {"table": table})).scalars().all()
            return any(cid in self.ids for cid in rows)
        try:
            await self._until(pending, timeout=10, message="real child MDL wait")
        except AssertionError:
            self.fail("child MDL barrier missing: " + table + "\n" + "\n".join(self.trace))
        print("P38_MDL", table, "PENDING separate_connection=1", flush=True)

    async def terminate(self, child, *, forced, app=False, fast=False):
        started = asyncio.get_running_loop().time()
        os.kill(child.pid, signal.SIGTERM)
        limit = 10 if app else 40
        done, _ = await asyncio.wait({asyncio.create_task(child.wait())}, timeout=limit + 3)
        self.assertTrue(done, "process survived absolute deadline (asyncio.run teardown still running)")
        elapsed = asyncio.get_running_loop().time() - started
        self.assertEqual(child.returncode, 75 if forced else 0, "\n".join(self.trace[-25:]))
        self.assertLess(elapsed, limit - .15)
        if fast:
            self.assertLess(elapsed, 2)
        await asyncio.gather(*self.readers)
        print(f"P38_PROCESS elapsed={elapsed:.3f}s compose={limit} exit={child.returncode}", flush=True)
        print("\n".join(self.trace), flush=True)
        self.assertFalse(any("PROOF_LATE_SQL" in line for line in self.trace))
        return elapsed

    async def stopped(self, *, locked=False):
        sql = text("SELECT COUNT(*) FROM replica_heartbeats WHERE replica_id=:id AND stopped_at IS NOT NULL")
        if locked:
            return (await self.holder.execute(sql, {"id": self.replica_id})).scalar_one()
        async with self.engine.connect() as conn:
            return (await conn.execute(sql, {"id": self.replica_id})).scalar_one()

    async def no_orphans(self):
        for cid in self.ids:
            async def absent():
                async with self.engine.connect() as conn:
                    threads = (await conn.execute(text("SELECT COUNT(*) FROM performance_schema.threads WHERE PROCESSLIST_ID=:id"), {"id": cid})).scalar_one()
                    trx = (await conn.execute(text("SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_mysql_thread_id=:id"), {"id": cid})).scalar_one()
                if trx:
                    await asyncio.sleep(.25)  # diagnostic snapshot cache
                return threads == 0 and trx == 0
            await self._until(absent, timeout=5, message="child connection/transaction gone")
            async with self.engine.connect() as conn:
                for table, join in (("metadata_locks", "l.OWNER_THREAD_ID"), ("data_locks", "l.THREAD_ID")):
                    count = (await conn.execute(text(f"SELECT COUNT(*) FROM performance_schema.{table} l JOIN performance_schema.threads t ON t.THREAD_ID={join} WHERE t.PROCESSLIST_ID=:id"), {"id": cid})).scalar_one()
                    self.assertEqual(count, 0)
        print("P38_ORPHANS innodb_trx=0 metadata_locks=0 data_locks=0", flush=True)

    async def request(self, path, *, payload=None):
        import httpx
        async def send():
            async with httpx.AsyncClient(timeout=65, trust_env=False) as client:
                for _ in range(100):
                    try:
                        if payload is not None:
                            bearer = self.child_environment.get("EVOLUTION_WEBHOOK_BEARER_TOKEN")
                            self.assertTrue(bearer, "child bearer fixture is required")
                            return await client.post("http://127.0.0.1:5099" + path, json=payload,
                                headers={"Authorization": f"Bearer {bearer}"})
                        return await client.get("http://127.0.0.1:5099" + path)
                    except httpx.ConnectError:
                        await asyncio.sleep(.02)
        self.request_task = asyncio.create_task(send())
        return self.request_task

    async def close_proxy(self):
        self.resume_proxy.set()
        self.proxy.close()
        await self.proxy.wait_closed()
        for writer in self.proxy_writers:
            writer.close()
        await asyncio.gather(*self.proxy_tasks, return_exceptions=True)
        self.proxy = None

    async def test_worker_half_open_tcp_proxy_exits_and_leaves_no_locks(self):
        from sqlalchemy.engine import make_url
        async def handle(reader, writer):
            self.proxy_tasks.append(asyncio.current_task())
            upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", 3306)
            self.proxy_writers.extend([writer, upstream_writer])
            async def pipe(source, target):
                try:
                    while data := await source.read(65536):
                        if self.freeze_proxy.is_set():
                            self.frozen_traffic.set()
                            await self.resume_proxy.wait()
                        target.write(data)
                        await target.drain()
                except (OSError, ConnectionError):
                    pass
                finally:
                    target.close()
            await asyncio.gather(pipe(reader, upstream_writer), pipe(upstream_reader, writer))
        self.proxy = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = self.proxy.sockets[0].getsockname()[1]
        self.proxy_url = make_url(os.environ["DATABASE_URL"]).set(port=port).render_as_string(hide_password=False)
        child = await self.start("worker_socket")
        await self.wait_line("PROOF_READY")
        self.freeze_proxy.set()
        await asyncio.wait_for(self.frozen_traffic.wait(), 5)
        print("P38_SOCKET authenticated_mysql=1 tcp_bytes_frozen=1", flush=True)
        elapsed = await self.terminate(child, forced=True)
        self.assertGreater(elapsed, 38.5)
        self.assertEqual(await self.stopped(), 0)
        await self.close_proxy()
        await self.no_orphans()

    async def marker_failure_db_down(self, *, app):
        # Root URL exists ONLY in the disposable test harness. Lock/KILL only
        # this child's unique synthetic account, never the diagnostic parent.
        admin_url = os.environ.get("P40_ADMIN_DATABASE_URL")
        self.assertTrue(admin_url, "P40_ADMIN_DATABASE_URL is required for ACCOUNT LOCK proof")
        self.admin_engine = create_async_engine(admin_url)
        await assert_disposable_database(self, self.admin_engine)
        self.db_user = "p40_marker_" + uuid4().hex[:12]
        database = make_url(admin_url).database
        self.assertRegex(database, r"^[a-zA-Z0-9_]+$")
        async with self.admin_engine.connect() as admin:
            await admin.execute(text(f"CREATE USER '{self.db_user}'@'%' IDENTIFIED BY 'synthetic'"))
            await admin.execute(text(f"GRANT SELECT,INSERT,UPDATE,DELETE ON `{database}`.* TO '{self.db_user}'@'%'"))
        self.child_database_url = make_url(os.environ["DATABASE_URL"]).set(username=self.db_user, password="synthetic").render_as_string(hide_password=False)
        child = await self.start("app_dbdown" if app else "worker_dbdown")
        await self.wait_line("PROOF_READY")
        async with self.admin_engine.connect() as admin:
            await admin.execute(text(f"ALTER USER '{self.db_user}'@'%' ACCOUNT LOCK"))
            ids = (await admin.execute(text("SELECT ID FROM information_schema.PROCESSLIST WHERE USER=:user"), {"user": self.db_user})).scalars().all()
            self.assertTrue(ids)
            for cid in ids:
                await admin.execute(text(f"KILL CONNECTION {int(cid)}"))
        await self.terminate(child, forced=False, app=app, fast=True)
        self.assertEqual(await self.stopped(), 0)
        self.assertTrue(any("reason=marker_failed" in line and "threshold=1" in line for line in self.trace))
        self.assertFalse(any("reason=unsafe_stops" in line or "process_shutdown_forced" in line for line in self.trace))
        await self.no_orphans()
        print("P40_MARKER_FAILURE account_locked=1 killed_connections=1 writers=0 marker_failed=1 unsafe_stops=0 forced_exits=0 exit=0", flush=True)

    async def test_worker_db_down_without_writer_is_marker_failed_not_unsafe(self):
        await self.marker_failure_db_down(app=False)

    async def test_app_db_down_without_writer_is_marker_failed_not_unsafe(self):
        await self.marker_failure_db_down(app=True)

    async def test_clean_worker_process_marks_stopped(self):
        child = await self.start("worker_clean")
        await self.wait_line("PROOF_READY")
        await self.terminate(child, forced=False, fast=True)
        self.assertEqual(await self.stopped(), 1)
        await self.no_orphans()

    async def test_startup_schema_real_mdl_signal_is_watched(self):
        await self.lock("scheduler_runtime_state")
        child = await self.start("startup")
        await self.wait_line("PROOF_SQL scheduler_runtime_state")
        await self.pending_mdl("scheduler_runtime_state")
        await self.terminate(child, forced=False, fast=True)
        self.assertEqual(await self.stopped(), 0)
        await self.unlock()
        await self.no_orphans()

    async def test_worker_resistant_real_beat_exits_process(self):
        await self.lock("replica_heartbeats")
        child = await self.start("worker_beat")
        await self.wait_line("PROOF_SQL replica_heartbeats")
        await self.pending_mdl("replica_heartbeats")
        elapsed = await self.terminate(child, forced=True)
        self.assertGreater(elapsed, 38.5, "worker discarded real grace/accounting budget")
        self.assertEqual(await self.stopped(locked=True), 0)
        self.assertTrue(any("reason=unsafe_stops" in line for line in self.trace))
        self.assertFalse(any("reason=marker_failed" in line for line in self.trace))
        await self.unlock()
        await self.no_orphans()

    async def test_accounting_real_mdl_abandoned_then_existing_recovery(self):
        agent, instance = await self._agent("process-accounting")
        await self._intake(instance, "5511990000088", "p38-paid", "hello")
        await self._force_ready()
        child = await self.start("worker_accounting")
        await self.wait_line("PROOF_PAID")
        await self.queue_lock("consumo_llm")
        self.release_file.touch()
        await self.pending_mdl("consumo_llm")
        elapsed = await self.terminate(child, forced=True)
        self.assertGreater(elapsed, 38.5)
        self.assertEqual(await self.stopped(), 0)
        await self.unlock()
        await self.no_orphans()
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
            self.assertEqual(attempt.outcome, "started")
            self.assertIsNone(attempt.consumo_llm_id)
        async with self.engine.begin() as conn:
            await conn.execute(update(AgentServiceLane).where(AgentServiceLane.agente_id == agent).values(lease_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
            await conn.execute(update(ConversationTurn).where(ConversationTurn.agente_id == agent).values(claim_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
        async def recover(db):
            now = await delivery_state_repo.get_database_now(db)
            return await scheduler_repo.recover_expired_lane(db, agente_id=agent,
                scheduler_epoch=self.scheduler_epoch, delivery_runtime_epoch=self.delivery_epoch,
                retry_at=now + timedelta(seconds=1))
        recovered = await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, recover)
        self.assertEqual(recovered, "quarantined")
        print("P38_RECOVERY attempt=started ledger=NULL existing_recovery=quarantined", flush=True)

    async def test_app_resistant_real_mdl_exits_process_without_marker(self):
        await self.lock("replica_heartbeats")
        child = await self.start("app_beat")
        await self.wait_line("PROOF_SQL replica_heartbeats")
        await self.pending_mdl("replica_heartbeats")
        await self.terminate(child, forced=True, app=True)
        self.assertEqual(await self.stopped(locked=True), 0)
        self.assertTrue(any("reason=unsafe_stops" in line for line in self.trace))
        self.assertFalse(any("reason=marker_failed" in line for line in self.trace))
        await self.unlock()
        await self.no_orphans()

    async def test_app_long_http_drain_is_bounded_and_clean_marker_present(self):
        child = await self.start("app_long")
        await self.wait_line("PROOF_READY")
        await self.request("/proof/long")
        await self.wait_line("PROOF_HTTP")
        elapsed = await self.terminate(child, forced=False, app=True)
        self.assertLess(elapsed, 9)
        self.assertEqual(await self.stopped(), 1)
        await self.no_orphans()

    async def test_app_occupied_event_loop_still_exits_from_thread(self):
        child = await self.start("app_busy")
        await self.wait_line("PROOF_READY")
        await self.request("/proof/long")
        await self.wait_line("PROOF_HTTP")
        await self.terminate(child, forced=True, app=True)
        self.assertEqual(await self.stopped(), 0)
        await self.no_orphans()

    async def test_direct_paid_result_cancelled_by_http_drain_rolls_back_with_signal(self):
        agent, instance = await self._agent("process-direct")
        async with self.engine.begin() as conn:
            await conn.execute(text("UPDATE delivery_runtime_state SET mode='direct' WHERE id=1"))
            await conn.execute(text("UPDATE scheduler_runtime_state SET state='disabled' WHERE id=1"))
            await conn.execute(text("UPDATE agentes SET inbox_scheduler_enabled=FALSE WHERE id=:id"), {"id": agent})
        child = await self.start("app_direct")
        await self.wait_line("PROOF_READY")
        await self.request("/webhook", payload={"event": "messages.upsert", "instance": instance,
            "data": {"key": {"id": "p38-direct-paid", "fromMe": False, "remoteJid": "5511990000089@s.whatsapp.net"},
                     "message": {"conversation": "hello"}}})
        await self.wait_line("PROOF_PAID")
        self.release_file.touch()
        await self.wait_line("PROOF_DIRECT_COMMIT_BARRIER")
        await self.terminate(child, forced=False, app=True)
        self.assertEqual(await self.stopped(), 1)
        self.assertTrue(any("direct_shutdown_cancellations" in line and "threshold=1" in line for line in self.trace))
        await self.no_orphans()
        self.assertEqual(await self._scalar(select(func.count(ConsumoLLM.id))), 0)
        self.assertEqual(await self._scalar(select(func.count(LLMUsageReconciliation.id))), 0)
        print("P38_DIRECT paid_result=1 rollback=1 ledger=0 reconciliation=0 cancellation_signal=1", flush=True)


def load_tests(loader, tests, pattern):
    return __import__('unittest').TestSuite(ProcessShutdownMySQLTests(name) for name in (
        "test_clean_worker_process_marks_stopped",
        "test_worker_db_down_without_writer_is_marker_failed_not_unsafe",
        "test_app_db_down_without_writer_is_marker_failed_not_unsafe",
        "test_startup_schema_real_mdl_signal_is_watched",
        "test_worker_resistant_real_beat_exits_process",
        "test_accounting_real_mdl_abandoned_then_existing_recovery",
        "test_app_resistant_real_mdl_exits_process_without_marker",
        "test_app_long_http_drain_is_bounded_and_clean_marker_present",
        "test_app_occupied_event_loop_still_exits_from_thread",
        "test_worker_half_open_tcp_proxy_exits_and_leaves_no_locks",
        "test_direct_paid_result_cancelled_by_http_drain_rolls_back_with_signal",
    ))
