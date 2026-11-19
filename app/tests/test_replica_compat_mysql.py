"""T08-3: replica heartbeat, fleet compatibility and enable gate on real MySQL.

Fake providers only; no LLM, Evolution or Chatwoot call is possible.
"""

import asyncio
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi import Response
from loguru import logger
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import delivery_cli, delivery_worker, turn_worker
from src import main as main_module
from src.config import settings
from src.database.models import DeliveryOperatorAction, ReplicaHeartbeat, SchedulerRuntimeState
from src.database.repositories import delivery_state_repo, replica_repo, scheduler_repo
from src.services import replica_compat
from tests import replica_fixtures as fx
from tests.disposable_db import assert_disposable_database
from tests.test_handoff_terminal_mysql import TABLES


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")
ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
SHARED_CASES = (
    ("code_version", "outbox-v2", "code_version_mismatch"),
    ("schema_version", 2, "schema_version_mismatch"),
    ("delivery_mode", "direct", "delivery_mode_mismatch"),
    ("delivery_epoch", None, "delivery_epoch_mismatch"),
    ("scheduler_enabled", False, "scheduler_flag_mismatch"),
    ("scheduler_epoch", None, "scheduler_epoch_mismatch"),
    ("webhook_auth_mode", "provisioning", "webhook_auth_mismatch"),
    ("dispatch_enabled", False, "dispatch_flag_mismatch"),
)


class ReplicaCompatMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        # Owns the whole disposable DB; require the runner's exact shared marker.
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        await assert_disposable_database(self, self.engine)
        self.sessions = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.delivery_epoch = str(uuid4())
        self.scheduler_epoch = str(uuid4())
        await self._wipe()
        async with self.engine.begin() as connection:
            await connection.execute(text(
                "UPDATE delivery_runtime_state SET mode='outbox', cutover_epoch=:e, "
                "version=version+1, changed_by='t083', change_reason='disposable' WHERE id=1"),
                {"e": self.delivery_epoch})
            await connection.execute(text(
                "UPDATE scheduler_runtime_state SET state='enabling', scheduler_epoch=:e, "
                "version=10, schema_version=3, changed_by='t083', change_reason='disposable' "
                "WHERE id=1"), {"e": self.scheduler_epoch})
        self.patches = ExitStack()
        for name, value in (
            ("provider_webhook_auth_mode", "enforced"),
            ("delivery_mode", "outbox"),
            ("delivery_cutover_epoch", self.delivery_epoch),
            ("delivery_dispatch_enabled", True),
            ("inbox_scheduler_enabled", True),
            ("inbox_scheduler_epoch", self.scheduler_epoch),
            ("inbox_turn_processor_enabled", True),
            ("delivery_worker_code_version", fx.CODE_VERSION),
        ):
            self.patches.enter_context(patch.object(settings, name, value))
        # Keep /health on this test's NullPool engine and event loop. The
        # process-global pooled engine cannot safely carry aiomysql connections
        # across IsolatedAsyncioTestCase loops in the integrated discovery.
        self.patches.enter_context(
            patch.object(main_module, "check_db_connection", self._check_db_connection)
        )
        self.ready = fx.ready_fingerprint(
            delivery_epoch=self.delivery_epoch, scheduler_epoch=self.scheduler_epoch)
        # S4: fresh /health cache and transition memory per test.
        main_module.reset_health_observability_caches()
        main_module.reset_health_transition_state()
        replica_compat.reset_health_transition_state()

    async def asyncTearDown(self) -> None:
        self.patches.close()
        await self._wipe()
        await self.engine.dispose()

    async def _wipe(self) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table_name in TABLES:
                await connection.execute(text(f"DELETE FROM {table_name}"))
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))

    # ── helpers ─────────────────────────────────────────────────────────
    async def _fleet(self, **overrides) -> list[str]:
        ids = []
        for kind in replica_repo.REPLICA_KINDS:
            ids.append(await fx.put_replica(self.sessions, replace(self.ready, **overrides), kind=kind))
        return ids

    async def _reasons(self, reader=None) -> tuple[str, ...]:
        async with self.sessions() as db:
            snapshot = await replica_repo.load_fleet(db)
        return replica_repo.readiness_reasons(snapshot, reader=reader)

    async def _check_db_connection(self) -> bool:
        async with self.sessions() as db:
            await db.execute(text("SELECT 1"))
        return True

    async def _legacy(self, worker_id: str, *, age_seconds: int = 0) -> None:
        async with self.sessions() as db:
            await delivery_state_repo.record_heartbeat(
                db, worker_id=worker_id, code_version=fx.CODE_VERSION, current_lease_count=0,
                dispatch_enabled=True, configured_mode="outbox",
                configured_epoch=self.delivery_epoch, provider_ready=True, runtime_compatible=True)
            if age_seconds:
                await db.execute(text(
                    "UPDATE delivery_worker_heartbeats SET last_seen_at = "
                    "TIMESTAMPADD(SECOND, -:a, UTC_TIMESTAMP(6)) WHERE worker_id=:w"),
                    {"a": age_seconds, "w": worker_id})
            await db.commit()

    async def _enable(self, attestation, *, epoch=None, version=10):
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions, lambda db: scheduler_repo.transition_scheduler_state(
                db, target="enabled", scheduler_epoch=epoch or self.scheduler_epoch,
                expected_version=version, actor="t083", reason="synthetic enable",
                replica_attestation=attestation))

    async def _refused(self, attestation, *expected: str) -> tuple[str, ...]:
        before = scheduler_repo.replica_gate_refusal_count()
        with self.assertRaises(scheduler_repo.ReplicaGateRefused) as caught:
            await self._enable(attestation)
        for reason in expected:
            self.assertIn(reason, caught.exception.reasons)
        self.assertTrue(set(caught.exception.reasons) <= replica_repo.GATE_REASONS)
        self.assertEqual(scheduler_repo.replica_gate_refusal_count(), before + 1)
        async with self.sessions() as db:
            state = await db.get(SchedulerRuntimeState, 1)
            self.assertEqual((state.state, state.version), ("enabling", 10))
            actions = (await db.execute(text("SELECT COUNT(*) FROM delivery_operator_actions"))).scalar_one()
        self.assertEqual(actions, 0)
        return caught.exception.reasons

    def _attest(self, fingerprint=None, attested=True):
        return replica_repo.EnableAttestation(
            old_replicas_removed=attested, operator=fingerprint or self.ready)

    # ── heartbeat row ───────────────────────────────────────────────────
    async def test_heartbeat_roundtrip_stop_marker_and_revival(self) -> None:
        replica_id = str(uuid4())
        self.assertTrue(await replica_compat.beat(self.sessions, kind="turn_worker", replica_id=replica_id))
        async with self.sessions() as db:
            row = await db.get(ReplicaHeartbeat, replica_id)
            self.assertEqual(
                (row.replica_kind, row.code_version, row.schema_version, row.delivery_mode,
                 row.delivery_epoch, row.scheduler_enabled, row.scheduler_epoch,
                 row.turn_processor_enabled, row.dispatch_enabled, row.webhook_auth_mode, row.stopped_at),
                ("turn_worker", fx.CODE_VERSION, 3, "outbox", self.delivery_epoch, True,
                 self.scheduler_epoch, True, True, "enforced", None))
            self.assertTrue(await replica_repo.own_heartbeat_fresh(db, replica_id=replica_id))
        await replica_compat.beat(self.sessions, kind="turn_worker", replica_id=replica_id, stopped=True)
        async with self.sessions() as db:
            self.assertFalse(await replica_repo.own_heartbeat_fresh(db, replica_id=replica_id))
            self.assertEqual(replica_repo.kind_counts((await replica_repo.load_fleet(db)).replicas)["turn_worker"], 0)
        # A later live beat clears the marker again (presence, fail closed).
        await replica_compat.beat(self.sessions, kind="turn_worker", replica_id=replica_id)
        async with self.sessions() as db:
            self.assertTrue(await replica_repo.own_heartbeat_fresh(db, replica_id=replica_id))

    async def test_missing_configuration_value_never_defaults_compatible(self) -> None:
        # No column default: a writer that omits a flag fails instead of
        # inserting a value that would read as compatible.
        for column in ("replica_kind", "schema_version", "scheduler_enabled", "turn_processor_enabled",
                       "dispatch_enabled", "webhook_auth_mode", "delivery_mode", "code_version"):
            values = {
                "replica_id": str(uuid4()), "replica_kind": "'app'", "code_version": "'" + fx.CODE_VERSION + "'",
                "schema_version": "3", "delivery_mode": "'outbox'", "scheduler_enabled": "1",
                "turn_processor_enabled": "1", "dispatch_enabled": "1",
                "webhook_auth_mode": "'enforced'",
            }
            values.pop(column)
            columns = ", ".join(list(values) + ["started_at", "last_seen_at"])
            literals = ", ".join(
                [f"'{values['replica_id']}'"] + [v for k, v in values.items() if k != "replica_id"]
                + ["UTC_TIMESTAMP(6)", "UTC_TIMESTAMP(6)"])
            with self.subTest(column=column):
                async with self.sessions() as db:
                    with self.assertRaisesRegex(DBAPIError, r"\(1364,"):
                        await db.execute(text(f"INSERT INTO replica_heartbeats ({columns}) VALUES ({literals})"))
                    await db.rollback()
        with self.assertRaises(replica_repo.ReplicaHeartbeatError):
            async with self.sessions() as db:
                await replica_repo.record(db, replica_id=str(uuid4()), replica_kind="app",
                                          fingerprint=replace(self.ready, delivery_epoch="not-a-uuid"))
        # Closed value sets are enforced by the DB, case-sensitively (ascii_bin).
        replica_id = await fx.put_replica(self.sessions, self.ready)
        for column, value in (("replica_kind", "worker"), ("replica_kind", "App"),
                              ("delivery_mode", "frozen"), ("webhook_auth_mode", "Enforced")):
            with self.subTest(column=column, value=value):
                async with self.sessions() as db:
                    with self.assertRaisesRegex(DBAPIError, r"\(3819,"):
                        await db.execute(text(f"UPDATE replica_heartbeats SET {column}=:v WHERE replica_id=:i"),
                                         {"v": value, "i": replica_id})
                    await db.rollback()

    async def test_invalid_stored_row_is_incompatible(self) -> None:
        await self._fleet()
        for column, value in (("delivery_epoch", "'not-a-uuid'"), ("code_version", "''"),
                              ("scheduler_epoch", "'x'")):
            replica_id = await fx.put_replica(self.sessions, self.ready)
            async with self.sessions() as db:
                await db.execute(text(f"UPDATE replica_heartbeats SET {column}={value} WHERE replica_id=:i"),
                                 {"i": replica_id})
                await db.commit()
            with self.subTest(column=column):
                self.assertIn("invalid_heartbeat", await self._reasons())
            async with self.sessions() as db:
                await db.execute(text("DELETE FROM replica_heartbeats WHERE replica_id=:i"), {"i": replica_id})
                await db.commit()
        self.assertEqual(await self._reasons(self.ready), ())

    # ── readiness matrix ────────────────────────────────────────────────
    async def test_mixed_version_epoch_flag_matrix(self) -> None:
        cases = 0
        for field, value, reason in SHARED_CASES:
            for divergent in ("replica", "reader"):
                await self._wipe()
                await fx.set_schema_version(self.sessions, 3)
                await self._fleet()
                reader = self.ready
                if divergent == "replica":
                    await fx.put_replica(self.sessions, replace(self.ready, **{field: value}),
                                         kind="delivery_worker")
                else:
                    reader = replace(self.ready, **{field: value})
                with self.subTest(field=field, divergent=divergent):
                    reasons = await self._reasons(reader)
                    self.assertIn(reason, reasons)
                    self.assertTrue(set(reasons) <= replica_repo.READINESS_REASONS)
                cases += 1
        self.assertEqual(cases, 2 * len(SHARED_CASES))
        await self._wipe()
        await fx.set_schema_version(self.sessions, 3)
        await self._fleet()
        self.assertEqual(await self._reasons(self.ready), ())

    async def test_schema_version_supported_mismatch_and_never_lowered(self) -> None:
        await self._fleet()
        await fx.set_schema_version(self.sessions, 1)
        reasons = await self._reasons(self.ready)
        self.assertIn("schema_version_unsupported", reasons)
        self.assertIn("schema_version_mismatch", reasons)
        self.assertFalse(await replica_compat.schema_supported(self.sessions))
        await fx.set_schema_version(self.sessions, 3)
        self.assertTrue(await replica_compat.schema_supported(self.sessions))
        self.assertEqual(await self._reasons(self.ready), ())

    async def test_turn_processor_flag_only_binds_turn_workers(self) -> None:
        await self._fleet()
        await fx.put_replica(self.sessions, replace(self.ready, turn_processor_enabled=False), kind="app")
        self.assertEqual(await self._reasons(self.ready), ())
        await fx.put_replica(self.sessions, replace(self.ready, turn_processor_enabled=False),
                             kind="turn_worker")
        self.assertEqual(await self._reasons(self.ready), ("turn_processor_flag_mismatch",))

    async def test_stale_stopped_rows_and_unmatched_legacy_heartbeats(self) -> None:
        await self._fleet()
        divergent = replace(self.ready, code_version="outbox-v0")
        await fx.put_replica(self.sessions, divergent, age_seconds=replica_repo.FRESH_SECONDS + 1)
        await fx.put_replica(self.sessions, divergent, stopped=True)
        self.assertEqual(await self._reasons(self.ready), ())
        # A fresh old-binary dispatcher writes only the legacy table.
        old = str(uuid4())
        await self._legacy(old)
        self.assertEqual(await self._reasons(self.ready), ("legacy_replica_detected",))
        # Same process accounted for: fresh replica row -> ok; clean stop -> ok.
        await fx.put_replica(self.sessions, self.ready, kind="delivery_worker", replica_id=old)
        self.assertEqual(await self._reasons(self.ready), ())
        await fx.put_replica(self.sessions, self.ready, kind="delivery_worker", replica_id=old, stopped=True)
        self.assertEqual(await self._reasons(self.ready), ())
        # A stale replica row does not account for a fresh legacy heartbeat.
        await fx.put_replica(self.sessions, self.ready, kind="delivery_worker", replica_id=old,
                             age_seconds=replica_repo.FRESH_SECONDS + 1)
        self.assertEqual(await self._reasons(self.ready), ("legacy_replica_detected",))
        # A row of another kind with the same id does not either.
        async with self.sessions() as db:
            await db.execute(text("DELETE FROM replica_heartbeats WHERE replica_id=:i"), {"i": old})
            await db.commit()
        await fx.put_replica(self.sessions, self.ready, kind="app", replica_id=old)
        self.assertIn("legacy_replica_detected", await self._reasons(self.ready))
        # Stale legacy heartbeat is ignored (absence is not proof: see gate).
        await self._wipe()
        await self._fleet()
        await self._legacy(str(uuid4()), age_seconds=replica_repo.FRESH_SECONDS + 1)
        self.assertEqual(await self._reasons(self.ready), ())

    async def test_db_authority_currency_rules(self) -> None:
        await self._fleet()
        cases = 0
        for state, expected in (("disabled", ()), ("enabling", ("scheduler_epoch_not_current",)),
                                ("enabled", ("scheduler_epoch_not_current",)),
                                ("draining", ("scheduler_epoch_not_current",))):
            async with self.sessions() as db:
                await db.execute(text("UPDATE scheduler_runtime_state SET state=:s, scheduler_epoch=:e"),
                                 {"s": state, "e": str(uuid4())})
                await db.commit()
            with self.subTest(state=state):
                self.assertEqual(await self._reasons(self.ready), expected)
            cases += 1
        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_runtime_state SET state='enabled', scheduler_epoch=:e"),
                             {"e": self.scheduler_epoch})
            await db.commit()
        disabled = replace(self.ready, scheduler_enabled=False, scheduler_epoch=None)
        await self._wipe()
        await fx.set_schema_version(self.sessions, 3)
        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_runtime_state SET state='enabled', scheduler_epoch=:e"),
                             {"e": self.scheduler_epoch})
            await db.execute(text("UPDATE delivery_runtime_state SET mode='outbox', cutover_epoch=:e"),
                             {"e": self.delivery_epoch})
            await db.commit()
        for kind in replica_repo.REPLICA_KINDS:
            await fx.put_replica(self.sessions, disabled, kind=kind)
        self.assertEqual(await self._reasons(disabled), ("scheduler_flag_not_current",))
        cases += 1
        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_runtime_state SET state='disabled'"))
            await db.execute(text("UPDATE delivery_runtime_state SET cutover_epoch=:e"), {"e": str(uuid4())})
            await db.commit()
        self.assertEqual(await self._reasons(disabled), ("delivery_epoch_not_current",))
        cases += 1
        self.assertEqual(cases, 6)

    async def test_report_unavailable_fails_closed(self) -> None:
        def broken():
            raise RuntimeError("synthetic")
        before = replica_compat.counts()["report_unavailable"]
        report = await replica_compat.fleet_report(broken, reader=self.ready)
        self.assertEqual((report.compatible, report.reasons), (False, ("replica_state_unavailable",)))
        self.assertEqual(replica_compat.counts()["report_unavailable"], before + 1)
        self.assertIsNone(report.scheduler_state)
        self.assertTrue(report.healthy)
        # Same fail-closed result when the read itself fails inside a session.
        failing_db = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("synthetic")))
        report = await replica_compat.fleet_report_db(failing_db, reader=self.ready)
        self.assertEqual((report.compatible, report.reasons), (False, ("replica_state_unavailable",)))
        # S4: still unavailable → no second signal; a new episode signals again.
        self.assertEqual(replica_compat.counts()["report_unavailable"], before + 1)
        replica_compat.reset_health_transition_state()
        report = await replica_compat.fleet_report_db(failing_db, reader=self.ready)
        self.assertEqual((report.compatible, report.reasons), (False, ("replica_state_unavailable",)))
        self.assertEqual(replica_compat.counts()["report_unavailable"], before + 2)
        self.assertFalse(await replica_compat.schema_supported(broken))
        before = replica_compat.counts()["heartbeat_failures"]
        self.assertFalse(await replica_compat.beat(broken, kind="app", replica_id=str(uuid4())))
        self.assertEqual(replica_compat.counts()["heartbeat_failures"], before + 1)

    # ── enable gate ─────────────────────────────────────────────────────
    async def test_enable_gate_attestation_kinds_and_audit(self) -> None:
        await self._refused(None, "operator_attestation_missing", "replica_kind_missing:app",
                            "replica_kind_missing:delivery_worker", "replica_kind_missing:turn_worker")
        await self._fleet()
        # Heartbeats alone never prove old replicas are gone.
        self.assertEqual(await self._refused(self._attest(attested=False)), ("operator_attestation_missing",))
        self.assertEqual(await self._refused(None), ("operator_attestation_missing",))
        cases = 0
        for missing in replica_repo.REPLICA_KINDS:
            async with self.sessions() as db:
                await db.execute(text("UPDATE replica_heartbeats SET stopped_at=UTC_TIMESTAMP(6) "
                                      "WHERE replica_kind=:k"), {"k": missing})
                await db.commit()
            with self.subTest(missing=missing):
                self.assertEqual(await self._refused(self._attest()), (f"replica_kind_missing:{missing}",))
            await fx.put_replica(self.sessions, self.ready, kind=missing)
            cases += 1
        self.assertEqual(cases, 3)
        row = await self._enable(self._attest())
        self.assertEqual((row.state, row.version), ("enabled", 11))
        async with self.sessions() as db:
            audit = (await db.execute(text("SELECT action, reason FROM delivery_operator_actions"))).one()
        self.assertEqual(audit.action, "scheduler:enabling:enabled")
        payload = json.loads(audit.reason)
        self.assertEqual(payload["v"], 2)
        self.assertIs(payload["attested_old_replicas_removed"], True)
        self.assertEqual((payload["code_version"], payload["schema_version"]), (fx.CODE_VERSION, 3))
        # Stopped rows are not fresh members: one live replica per kind.
        self.assertEqual(payload["replicas"], {"app": 1, "delivery_worker": 1, "turn_worker": 1})
        # Attestation is only meaningful for enabling -> enabled.
        with self.assertRaises(ValueError):
            await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions, lambda db: scheduler_repo.transition_scheduler_state(
                    db, target="draining", scheduler_epoch=self.scheduler_epoch, expected_version=11,
                    actor="t083", reason="x", replica_attestation=self._attest()))

    async def test_enable_gate_reason_matrix(self) -> None:
        cases = 0
        uniform = (
            ({"delivery_mode": "direct"}, "delivery_mode_not_outbox"),
            ({"delivery_epoch": str(uuid4())}, "delivery_epoch_not_runtime"),
            ({"scheduler_enabled": False}, "scheduler_flag_disabled"),
            ({"scheduler_epoch": str(uuid4())}, "scheduler_epoch_not_target"),
            ({"webhook_auth_mode": "provisioning"}, "webhook_auth_not_enforced"),
            ({"dispatch_enabled": False}, "dispatch_disabled"),
        )
        for overrides, reason in uniform:
            await self._wipe()
            await fx.set_schema_version(self.sessions, 3)
            fingerprint = replace(self.ready, **overrides)
            await self._fleet(**overrides)
            with self.subTest(reason=reason):
                self.assertIn(reason, await self._refused(self._attest(fingerprint)))
            cases += 1
        # Every readiness divergence also refuses the gate.
        for field, value, reason in SHARED_CASES:
            await self._wipe()
            await fx.set_schema_version(self.sessions, 3)
            await self._fleet()
            await fx.put_replica(self.sessions, replace(self.ready, **{field: value}), kind="turn_worker")
            with self.subTest(field=field):
                self.assertIn(reason, await self._refused(self._attest()))
            cases += 1
        await self._wipe()
        await self._fleet()
        await self._legacy(str(uuid4()))
        self.assertEqual(await self._refused(self._attest()), ("legacy_replica_detected",))
        cases += 1
        await self._wipe()
        await self._fleet()
        await fx.set_schema_version(self.sessions, 1)
        self.assertIn("schema_version_unsupported", await self._refused(self._attest()))
        cases += 1
        await fx.set_schema_version(self.sessions, 3)
        async with self.sessions() as db:
            await db.execute(text("UPDATE delivery_runtime_state SET mode='direct'"))
            await db.commit()
        self.assertIn("runtime_not_outbox", await self._refused(self._attest()))
        cases += 1
        self.assertEqual(cases, len(uniform) + len(SHARED_CASES) + 3)

    async def test_gate_reads_committed_heartbeats_after_waiting_for_scheduler_lock(self) -> None:
        # A1: the heartbeat read follows the scheduler lock wait; a divergent
        # replica committed during the wait is seen (statement-level RC view).
        await self._fleet()
        gate = None
        try:
            async with self.sessions() as holder:
                await holder.execute(text("SELECT id FROM scheduler_runtime_state WHERE id=1 FOR UPDATE"))
                gate = asyncio.create_task(self._enable(self._attest()))
                await self._until_lock_wait()
                await fx.put_replica(
                    self.sessions,
                    replace(self.ready, code_version="outbox-v0"),
                    kind="app",
                )
                await holder.rollback()
            with self.assertRaises(scheduler_repo.ReplicaGateRefused) as caught:
                await gate
        finally:
            if gate is not None and not gate.done():
                gate.cancel()
            if gate is not None:
                await asyncio.gather(gate, return_exceptions=True)
        self.assertIn("code_version_mismatch", caught.exception.reasons)

    async def test_gate_and_heartbeat_writer_never_wait_on_each_other(self) -> None:
        for order in ("writer_first", "gate_first") * 3:
            await self._wipe()
            await fx.set_schema_version(self.sessions, 3)
            async with self.sessions() as db:
                await db.execute(text("UPDATE scheduler_runtime_state SET state='enabling', version=10"))
                await db.commit()
            ids = await self._fleet()
            with self.subTest(order=order):
                if order == "writer_first":
                    async with self.sessions() as writer:
                        await replica_repo.record(
                            writer,
                            replica_id=ids[0],
                            replica_kind="app",
                            fingerprint=replace(self.ready, code_version="outbox-v9"),
                        )
                        # Uncommitted X on a heartbeat row: the gate does not wait
                        # and sees only the committed (compatible) value.
                        row = await asyncio.wait_for(self._enable(self._attest()), timeout=10)
                        self.assertEqual(row.state, "enabled")
                        await writer.rollback()
                else:
                    async with self.sessions() as gate_session:
                        row = await scheduler_repo.transition_scheduler_state(
                            gate_session,
                            target="enabled",
                            scheduler_epoch=self.scheduler_epoch,
                            expected_version=10,
                            actor="t083",
                            reason="barrier",
                            replica_attestation=self._attest(),
                        )
                        # Gate holds runtime S / scheduler X / clearance S: a heartbeat
                        # writer (own transaction) commits without waiting.
                        self.assertTrue(await asyncio.wait_for(
                            replica_compat.beat(self.sessions, kind="app", replica_id=ids[0]), timeout=10))
                        await gate_session.commit()
                        self.assertEqual(row.state, "enabled")

    async def test_legacy_retention_lock_does_not_block_gate(self) -> None:
        await self._fleet()
        old = str(uuid4())
        await self._legacy(old, age_seconds=200 * 86400)
        async with self.sessions() as cleaner:
            result = await cleaner.execute(text(
                "DELETE FROM delivery_worker_heartbeats WHERE last_seen_at < "
                "TIMESTAMPADD(DAY, -90, UTC_TIMESTAMP(6))"))
            self.assertEqual(result.rowcount, 1)
            row = await asyncio.wait_for(self._enable(self._attest()), timeout=10)
            self.assertEqual(row.state, "enabled")
            await cleaner.rollback()

    async def _until_lock_wait(self) -> None:
        deadline = asyncio.get_running_loop().time() + 20
        while True:
            async with self.sessions() as db:
                waiting = (await db.execute(text(
                    "SELECT COUNT(*) FROM performance_schema.data_lock_waits"))).scalar_one()
            if waiting:
                return
            if asyncio.get_running_loop().time() > deadline:
                self.fail("no real InnoDB lock wait observed")
            await asyncio.sleep(0.03)

    # ── CLI and health surfaces ─────────────────────────────────────────
    async def test_cli_enabled_requires_flag_reports_reasons_and_audits(self) -> None:
        await self._fleet()
        base = ["scheduler-state", "enabled", "--epoch", self.scheduler_epoch, "--expected-version", "10",
                "--actor", "t083", "--reason", "CLI enable"]
        for name in ("draining", "disabled", "enabling"):
            with self.assertRaises(SystemExit):
                delivery_cli._parser().parse_args(["scheduler-state", name, "--epoch", self.scheduler_epoch,
                    "--expected-version", "10", "--actor", "a", "--reason", "r",
                    "--attest-old-replicas-removed"])
        with patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                patch.object(delivery_cli, "_emit") as emit:
            self.assertEqual(await delivery_cli._run(delivery_cli._parser().parse_args(base)), 1)
            self.assertEqual(emit.call_args.args[0], {"status": "refused", "error_type": "ReplicaGateRefused",
                                                      "replica_reasons": ["operator_attestation_missing"]})
            with patch.object(settings, "delivery_worker_code_version", "outbox-v2"), \
                    patch.object(replica_compat, "image_code_version", return_value="outbox-v2"):
                self.assertEqual(await delivery_cli._run(delivery_cli._parser().parse_args(
                    base + ["--attest-old-replicas-removed"])), 1)
            self.assertIn("code_version_mismatch", emit.call_args.args[0]["replica_reasons"])
            self.assertEqual(await delivery_cli._run(delivery_cli._parser().parse_args(
                base + ["--attest-old-replicas-removed"])), 0)
            self.assertEqual(emit.call_args.args[0]["state"], "enabled")
        async with self.sessions() as db:
            reason = (await db.execute(text("SELECT reason FROM delivery_operator_actions"))).scalar_one()
        self.assertIs(json.loads(reason)["attested_old_replicas_removed"], True)

    async def _healthy_delivery_fleet(self) -> str:
        worker_id = str(uuid4())
        await self._legacy(worker_id)
        await fx.put_replica(self.sessions, self.ready, kind="delivery_worker", replica_id=worker_id)
        await fx.put_replica(self.sessions, self.ready, kind="app")
        await fx.put_replica(self.sessions, self.ready, kind="turn_worker")
        return worker_id

    async def _probe_health_surfaces(self, worker_id: str) -> list[tuple[bool, dict]]:
        """Real DB for all consumers; only provider snapshot and local files fake."""
        async with self.sessions() as db:
            turn_id = (await db.execute(text(
                "SELECT replica_id FROM replica_heartbeats WHERE replica_kind='turn_worker' LIMIT 1"
            ))).scalar_one()
        result = []
        with tempfile.TemporaryDirectory() as directory:
            delivery_path = Path(directory) / "delivery.json"
            delivery_path.write_text(json.dumps({"worker_id": worker_id, "ready": True,
                "provider_ready": True, "runtime_compatible": True}))
            turn_path = Path(directory) / "turn.json"
            turn_path.write_text(json.dumps({"worker_id": f"turn-{turn_id}",
                "updated_at": datetime.now(timezone.utc).isoformat()}))
            emitted = []
            with patch.object(main_module, "provider_auth_readiness",
                              AsyncMock(return_value=SimpleNamespace(ready=True))), \
                    patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                    patch.object(turn_worker, "AsyncSessionLocal", self.sessions), \
                    patch.object(turn_worker, "engine", SimpleNamespace(dispose=AsyncMock())), \
                    patch.object(settings, "delivery_worker_health_path", str(delivery_path)), \
                    patch.object(settings, "inbox_turn_worker_health_path", str(turn_path)), \
                    patch.object(delivery_cli, "_emit", emitted.append), \
                    patch.object(turn_worker, "_emit", emitted.append):
                # S4: each surface is its own process in production; fresh
                # cache and transition memory per surface keep one signal each.
                main_module.reset_health_observability_caches()
                replica_compat.reset_health_transition_state()
                response = Response()
                async with self.sessions() as db:
                    body = await main_module.health_check(response, db)
                # FU-4: public and internal surfaces use the exact same decision.
                public_response = Response()
                async with self.sessions() as db:
                    public_body = await main_module.public_health_check(public_response, db)
                self.assertEqual(public_response.status_code, response.status_code)
                healthy = response.status_code == 200
                self.assertEqual(public_body, {"status": "healthy" if healthy else "unhealthy"})
                self.assertEqual(body["status"], "healthy" if healthy else "degraded")
                result.append((healthy, body["replica_compatibility"]))
                for consumer in (delivery_cli, turn_worker):
                    replica_compat.reset_health_transition_state()
                    code = await consumer._health()
                    self.assertEqual(code, 0 if emitted[-1]["healthy"] else 1)
                    result.append((code == 0, emitted[-1]["replica_compatibility"]))
        return result

    async def test_replica_health_only_blocks_active_db_scheduler(self) -> None:
        # Local flag deliberately remains True for disabled: only DB state
        # decides whether incompatibility blocks any of the three consumers.
        cases = 0
        for state in ("disabled", "enabling", "enabled", "draining"):
            for mismatch in ("code_version_mismatch", "legacy_replica_detected", "scheduler_flag_mismatch"):
                await self._wipe()
                worker_id = await self._healthy_delivery_fleet()
                async with self.sessions() as db:
                    await db.execute(text("UPDATE scheduler_runtime_state SET state=:s WHERE id=1"), {"s": state})
                    await db.commit()
                if mismatch == "legacy_replica_detected":
                    await self._legacy(str(uuid4()))
                else:
                    fingerprint = replace(self.ready, **(
                        {"code_version": "old-build"} if mismatch == "code_version_mismatch"
                        else {"scheduler_enabled": False}))
                    await fx.put_replica(self.sessions, fingerprint)
                before = replica_compat.counts()["incompatible_reports"]
                lines = []
                sink = logger.add(lines.append, format="{message}", level="ERROR")
                try:
                    results = await self._probe_health_surfaces(worker_id)
                finally:
                    logger.remove(sink)
                with self.subTest(state=state, mismatch=mismatch):
                    expected = state == "disabled"
                    for healthy, block in results:
                        self.assertEqual(healthy, expected, block)
                        self.assertEqual((block["compatible"], block["healthy"], block["state"],
                                          block["blocks_health"]), (False, expected, state, not expected))
                        self.assertIn(mismatch, block["reasons"])
                        self.assertEqual(block["signal_threshold"], 1)
                    self.assertEqual(replica_compat.counts()["incompatible_reports"], before + 3)
                    signal = [line for line in lines if "Replica fleet incompatible" in line]
                    self.assertEqual(len(signal), 3)
                    self.assertTrue(all(f"state={state}" in line and "threshold=1" in line
                                        and f"blocks_health={not expected}" in line for line in signal))
                cases += len(results)
        self.assertEqual(cases, 36)

    async def test_unavailable_fleet_preserves_db_state_for_health(self) -> None:
        async def missing_table(db):
            # Real MySQL failure, not a fabricated report or exception.
            await db.execute(text("SELECT * FROM t083_missing_fleet"))

        worker_id = await self._healthy_delivery_fleet()
        cases = 0
        for state in ("disabled", "enabling", "enabled", "draining", None):
            async with self.sessions() as db:
                await db.execute(text("UPDATE scheduler_runtime_state SET state=:s WHERE id=1"),
                                 {"s": state or "disabled"})
                await db.commit()
            before = replica_compat.counts()["report_unavailable"]
            lines = []
            sink = logger.add(lines.append, format="{message}", level="ERROR")
            try:
                target = "load_fleet" if state is not None else "health_scheduler_state"
                with patch.object(replica_repo, target, missing_table):
                    results = await self._probe_health_surfaces(worker_id)
            finally:
                logger.remove(sink)
            with self.subTest(state=state):
                expected = state in ("disabled", None)
                for healthy, block in results:
                    self.assertEqual(healthy, expected, block)
                    self.assertEqual((block["compatible"], block["healthy"], block["state"],
                                      block["blocks_health"]), (False, expected, state, not expected))
                    self.assertEqual(block["reasons"], ["replica_state_unavailable"])
                self.assertEqual(replica_compat.counts()["report_unavailable"], before + 3)
                signal = [line for line in lines if "Replica compatibility unavailable" in line]
                self.assertEqual(len(signal), 3)
                self.assertTrue(all(f"state={state}" in line and "threshold=1" in line
                                    and f"blocks_health={not expected}" in line for line in signal))
            cases += len(results)
        self.assertEqual(cases, 15)

    async def test_app_health_fails_closed_on_divergent_replica(self) -> None:
        await self._healthy_delivery_fleet()
        provider = AsyncMock(return_value=SimpleNamespace(ready=True))
        with patch.object(main_module, "provider_auth_readiness", provider):
            response = Response()
            async with self.sessions() as db:
                body = await main_module.health_check(response, db)
            self.assertTrue(body["delivery_ready"], body)
            self.assertEqual(body["replica_compatibility"]["reasons"], [])
            await fx.put_replica(self.sessions, replace(self.ready, scheduler_epoch=str(uuid4())), kind="app")
            main_module.reset_health_observability_caches()  # S4: outside the cache TTL
            response = Response()
            async with self.sessions() as db:
                body = await main_module.health_check(response, db)
        self.assertEqual(response.status_code, 503)
        self.assertFalse(body["delivery_ready"])
        self.assertIn("scheduler_epoch_mismatch", body["replica_compatibility"]["reasons"])
        rendered = json.dumps(body)
        self.assertNotIn(self.scheduler_epoch, rendered)
        self.assertNotIn(self.delivery_epoch, rendered)

    async def test_delivery_cli_health_fails_closed_on_divergent_replica(self) -> None:
        worker_id = await self._healthy_delivery_fleet()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            path.write_text(json.dumps({"worker_id": worker_id, "ready": True, "provider_ready": True,
                                        "runtime_compatible": True}))
            emitted: list[dict] = []
            with patch.object(settings, "delivery_worker_health_path", str(path)), \
                    patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                    patch.object(delivery_cli, "_emit", emitted.append):
                self.assertEqual(await delivery_cli._health(), 0, emitted)
                await fx.put_replica(self.sessions, replace(self.ready, dispatch_enabled=False),
                                     kind="delivery_worker")
                self.assertEqual(await delivery_cli._health(), 1)
        self.assertFalse(emitted[-1]["healthy"])
        self.assertIn("dispatch_flag_mismatch", emitted[-1]["replica_compatibility"]["reasons"])

    async def test_turn_worker_health_cli(self) -> None:
        await self._healthy_delivery_fleet()
        replica_id = str(uuid4())
        await fx.put_replica(self.sessions, self.ready, kind="turn_worker", replica_id=replica_id)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "turn.json"
            emitted: list[dict] = []

            def snapshot(worker_id=f"turn-{replica_id}", age=0):
                path.write_text(json.dumps({"worker_id": worker_id, "updated_at": (
                    datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat(timespec="seconds")}))

            with patch.object(settings, "inbox_turn_worker_health_path", str(path)), \
                    patch.object(turn_worker, "AsyncSessionLocal", self.sessions), \
                    patch.object(turn_worker, "engine", SimpleNamespace(dispose=AsyncMock())), \
                    patch.object(turn_worker, "_emit", emitted.append):
                snapshot()
                self.assertEqual(await turn_worker._health(), 0, emitted)
                self.assertTrue(emitted[-1]["heartbeat_fresh"] and emitted[-1]["snapshot_fresh"])
                cases = 1
                for label, action, field in (
                    ("stale_snapshot", lambda: snapshot(age=replica_repo.FRESH_SECONDS + 5), "snapshot_fresh"),
                    ("foreign_snapshot", lambda: snapshot(worker_id=f"turn-{uuid4()}"), "heartbeat_fresh"),
                    ("bad_snapshot", lambda: path.write_text("{"), "reason"),
                ):
                    action()
                    with self.subTest(label=label):
                        self.assertEqual(await turn_worker._health(), 1)
                        self.assertFalse(emitted[-1].get(field, False) is True)
                    cases += 1
                snapshot()
                await fx.put_replica(self.sessions, self.ready, kind="turn_worker", replica_id=replica_id,
                                     stopped=True)
                self.assertEqual(await turn_worker._health(), 1)
                self.assertFalse(emitted[-1]["heartbeat_fresh"])
                await fx.put_replica(self.sessions, self.ready, kind="turn_worker", replica_id=replica_id)
                await fx.put_replica(self.sessions, replace(self.ready, code_version="outbox-v0"))
                self.assertEqual(await turn_worker._health(), 1)
                self.assertIn("code_version_mismatch", emitted[-1]["replica_compatibility"]["reasons"])
                with patch.object(settings, "inbox_turn_processor_enabled", False):
                    self.assertEqual(await turn_worker._health(), 1)
                self.assertEqual(emitted[-1], {"healthy": False, "reason": "turn_processor_disabled"})
                cases += 3
        self.assertEqual(cases, 7)

    # ── process hooks ───────────────────────────────────────────────────
    async def test_turn_worker_refuses_unsupported_schema_without_processor(self) -> None:
        await fx.set_schema_version(self.sessions, 1)
        with patch.object(turn_worker, "AsyncSessionLocal", self.sessions), \
                patch.object(turn_worker, "engine", SimpleNamespace(dispose=AsyncMock())), \
                patch.object(turn_worker, "TurnProcessor", side_effect=AssertionError("processor built")):
            self.assertEqual(await turn_worker._run(), turn_worker.EXIT_REFUSED)
        async with self.sessions() as db:
            self.assertEqual((await db.execute(text("SELECT COUNT(*) FROM replica_heartbeats"))).scalar_one(), 0)

    async def test_turn_worker_beats_while_running_and_marks_clean_exit(self) -> None:
        built = {}

        class Processor:
            def __init__(self, *args, worker_id, **kwargs):
                self.worker_id = built["worker_id"] = worker_id

            async def run(self, stop):
                replica_id = built["worker_id"].removeprefix("turn-")
                for _ in range(200):
                    async with turn_worker.AsyncSessionLocal() as db:
                        if await replica_repo.own_heartbeat_fresh(db, replica_id=replica_id):
                            built["seen"] = True
                            return
                    await asyncio.sleep(0.02)

            async def shutdown(self, grace):
                return None

        with patch.object(turn_worker, "AsyncSessionLocal", self.sessions), \
                patch.object(turn_worker, "engine", SimpleNamespace(dispose=AsyncMock())), \
                patch.object(turn_worker, "FinalCandidateRouter", lambda *a, **k: None), \
                patch.object(turn_worker, "TurnProcessor", Processor):
            self.assertIsNone(await turn_worker._run())
        self.assertTrue(built.get("seen"))
        replica_id = built["worker_id"].removeprefix("turn-")
        async with self.sessions() as db:
            row = await db.get(ReplicaHeartbeat, replica_id)
            self.assertEqual(row.replica_kind, "turn_worker")
            self.assertIsNotNone(row.stopped_at)

    async def test_delivery_worker_replica_beat_and_stop_marker_match_legacy_row(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        with patch.object(delivery_worker, "AsyncSessionLocal", self.sessions), \
                patch.object(worker, "_publish_health_snapshot"):
            await worker._replica_beat()
            await worker._heartbeat()
            async with self.sessions() as db:
                snapshot = await replica_repo.load_fleet(db)
            self.assertEqual(replica_repo.kind_counts(snapshot.replicas)["delivery_worker"], 1)
            self.assertEqual(snapshot.legacy_unmatched, 0)
            worker._replica_drained = True
            await worker._heartbeat()
        async with self.sessions() as db:
            row = await db.get(ReplicaHeartbeat, worker.worker_id)
            self.assertIsNotNone(row.stopped_at)
            snapshot = await replica_repo.load_fleet(db)
        # Legacy row stays fresh, but the clean exit accounts for it.
        self.assertEqual(snapshot.legacy_unmatched, 0)
        self.assertEqual(replica_repo.kind_counts(snapshot.replicas)["delivery_worker"], 0)

    async def test_delivery_worker_run_loop_beats_then_marks_clean_exit(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        with ExitStack() as stack:
            stack.enter_context(patch.object(delivery_worker, "AsyncSessionLocal", self.sessions))
            stack.enter_context(patch.object(delivery_worker, "engine", SimpleNamespace(dispose=AsyncMock())))
            stack.enter_context(patch.object(settings, "delivery_dispatch_enabled", False))
            for name in ("_check_readiness", "_maintenance", "_retention"):
                stack.enter_context(patch.object(worker, name, AsyncMock(return_value=True)))
            stack.enter_context(patch.object(worker, "_publish_health_snapshot"))
            run = asyncio.create_task(worker.run())
            live = False
            for _ in range(250):
                async with self.sessions() as db:
                    live = await replica_repo.own_heartbeat_fresh(db, replica_id=worker.worker_id)
                if live:
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(live, "loop never wrote a live replica heartbeat")
            worker.request_stop()
            await asyncio.wait_for(run, timeout=30)
        async with self.sessions() as db:
            row = await db.get(ReplicaHeartbeat, worker.worker_id)
            self.assertIsNotNone(row.stopped_at)
            self.assertEqual((await replica_repo.load_fleet(db)).legacy_unmatched, 0)

    async def test_delivery_worker_retention_purges_replica_heartbeats(self) -> None:
        expired = await fx.put_replica(self.sessions, self.ready, age_seconds=91 * 86400)
        worker = delivery_worker.DeliveryWorker()
        clean = SimpleNamespace(payloads_scrubbed=0, receipts_purged=0, heartbeats_purged=0,
                                audits_purged=0, deliveries_purged=0)
        with patch.object(delivery_worker, "AsyncSessionLocal", self.sessions), \
                patch.object(delivery_worker.delivery_worker_repo, "cleanup_retention",
                             AsyncMock(return_value=clean)), \
                patch.object(delivery_worker.scheduler_retention_repo, "cleanup_retention",
                             AsyncMock(return_value=SimpleNamespace(examined=0))):
            await worker._retention()
        async with self.sessions() as db:
            self.assertIsNone(await db.get(ReplicaHeartbeat, expired))

    async def test_app_lifespan_beats_and_marks_clean_exit(self) -> None:
        with patch.object(main_module, "AsyncSessionLocal", self.sessions):
            async with main_module.lifespan(main_module.app):
                for _ in range(200):
                    async with self.sessions() as db:
                        rows = (await db.execute(text(
                            "SELECT replica_id FROM replica_heartbeats WHERE replica_kind='app' "
                            "AND stopped_at IS NULL"))).all()
                    if rows:
                        break
                    await asyncio.sleep(0.02)
                self.assertEqual(len(rows), 1)
        async with self.sessions() as db:
            row = await db.get(ReplicaHeartbeat, rows[0][0])
            self.assertIsNotNone(row.stopped_at)

    async def test_retention_purges_only_expired_rows(self) -> None:
        fresh = await fx.put_replica(self.sessions, self.ready)
        kept = await fx.put_replica(self.sessions, self.ready, age_seconds=89 * 86400)
        expired = [await fx.put_replica(self.sessions, self.ready, age_seconds=91 * 86400) for _ in range(3)]
        async with self.sessions() as db:
            self.assertEqual(await replica_repo.purge_expired(db, limit=2), 2)
            await db.commit()
        self.assertEqual(await replica_compat.purge_expired(self.sessions), 1)
        async with self.sessions() as db:
            remaining = {r[0] for r in (await db.execute(text("SELECT replica_id FROM replica_heartbeats"))).all()}
        self.assertEqual(remaining, {fresh, kept})
        self.assertTrue(set(expired).isdisjoint(remaining))
        # Age is re-checked at delete time: a row revived after discovery stays.
        async with self.sessions() as db:
            self.assertEqual(await replica_repo.delete_if_expired(db, [fresh, kept]), 0)
            await db.commit()
        before = replica_compat.counts()["retention_failures"]

        def broken():
            raise RuntimeError("synthetic")
        self.assertEqual(await replica_compat.purge_expired(broken), 0)
        self.assertEqual(replica_compat.counts()["retention_failures"], before + 1)

    # ── contracts ───────────────────────────────────────────────────────
    async def test_orm_matches_live_schema(self) -> None:
        def live(sync_connection):
            inspector = inspect(sync_connection)
            return (
                {c["name"]: c["nullable"] for c in inspector.get_columns("replica_heartbeats")},
                {i["name"]: tuple(i["column_names"]) for i in inspector.get_indexes("replica_heartbeats")},
                {c["name"] for c in inspector.get_check_constraints("replica_heartbeats")},
                inspector.get_foreign_keys("replica_heartbeats"),
                inspector.get_pk_constraint("replica_heartbeats")["constrained_columns"],
            )

        async with self.engine.connect() as connection:
            columns, indexes, checks, fks, pk = await connection.run_sync(live)
        table = ReplicaHeartbeat.__table__
        self.assertEqual(list(columns), [c.name for c in table.columns])
        self.assertEqual(columns, {c.name: c.nullable for c in table.columns})
        self.assertEqual(indexes, {i.name: tuple(c.name for c in i.columns) for i in table.indexes})
        self.assertEqual(checks, {c.name for c in table.constraints if c.__class__.__name__ == "CheckConstraint"})
        self.assertEqual((fks, pk), ([], ["replica_id"]))

    async def test_documented_contract_parity(self) -> None:
        agents = (ROOT / "AGENTS.md").read_text()
        for reason in sorted(replica_repo.GATE_REASONS):
            self.assertIn(f"`{reason}`", agents, reason)
        for literal in ("--attest-old-replicas-removed", "replica_heartbeats",
                        f"FRESH_SECONDS={replica_repo.FRESH_SECONDS}",
                        f"HEARTBEAT_SECONDS={replica_compat.HEARTBEAT_SECONDS}",
                        f"schema_version={replica_compat.EXPECTED_SCHEMA_VERSION}",
                        "python -m src.turn_worker health",
                        "BLOCKING_STATES = " + "|".join(("enabling", "enabled", "draining")),
                        "replica_compatibility.healthy", "state=null"):
            self.assertIn(literal, agents)
        self.assertEqual(replica_compat.BLOCKING_STATES, {"enabling", "enabled", "draining"})
        init_sql = (ROOT / "db" / "init.sql").read_text()
        self.assertEqual(
            re.findall(r"UPDATE scheduler_runtime_state SET schema_version = (\d+)", init_sql),
            ["2", str(replica_compat.EXPECTED_SCHEMA_VERSION)])
        marker = "DELIVERY_WORKER_CODE_VERSION identifica o build de TODAS as replicas"
        for env in (ROOT / ".env.example", ROOT / "app" / ".env.example"):
            self.assertIn(marker, env.read_text(), env)
        compose = (ROOT / "docker-compose.yml").read_text()
        block = compose.split("  turn-worker:\n", 1)[1].split("\n\n", 1)[0]
        for literal in ('profiles: ["inbox"]', '"python", "-m", "src.turn_worker"]',
                        '["CMD", "python", "-m", "src.turn_worker", "health"]', "restart: on-failure"):
            self.assertIn(literal, block)


if __name__ == "__main__":
    unittest.main()
