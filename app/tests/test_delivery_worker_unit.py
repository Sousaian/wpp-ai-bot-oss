import asyncio
from contextlib import ExitStack
import io
import json
import os
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from loguru import logger
from sqlalchemy.exc import OperationalError

from src import delivery_cli, delivery_worker
from src.services import delivery_adapters


class _Response:
    def __init__(self, payload: object, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status
        self.request = httpx.Request("GET", "http://provider.test")
        self.headers: dict[str, str] = {}
        self.content = b"{}" if payload is not None else b""

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = httpx.Response(
                self.status_code,
                request=self.request,
                headers=self.headers,
            )
            raise httpx.HTTPStatusError(
                "provider failure",
                request=self.request,
                response=response,
            )


class _Context:
    def __init__(self, client: object) -> None:
        self.client = client

    async def __aenter__(self) -> object:
        return self.client

    async def __aexit__(self, *args: object) -> None:
        return None


def _claimed_delivery(delivery_id: int) -> object:
    return delivery_worker.delivery_worker_repo.ClaimedDelivery(
        id=delivery_id,
        lease_token=f"11111111-1111-4111-8111-{delivery_id:012d}",
        provider="evolution",
        operation="evolution.send_text.v1",
        payload_version=1,
        payload={
            "phone": f"55110000{delivery_id:04d}",
            "message": "restricted",
            "instance_name": "instance-a",
        },
        delivery_key=f"22222222-2222-4222-8222-{delivery_id:012d}",
        agente_id=1,
        sessao_id=2,
        handoff_attempt_id=None,
        attempt_count=1,
        cutover_epoch="33333333-3333-4333-8333-333333333333",
    )


class DeliveryPayloadTests(unittest.TestCase):
    def test_versioned_payload_decoders_reject_unknown_contracts(self) -> None:
        with self.assertRaises(delivery_adapters.InvalidDeliveryPayload):
            delivery_adapters.decode_evolution_text(
                "evolution.send_text.v1", 2, {"phone": "1"}
            )
        with self.assertRaises(delivery_adapters.InvalidDeliveryPayload):
            delivery_adapters.decode_chatwoot_handoff(
                "chatwoot.open_handoff.v2", 1, {}
            )

    def test_retry_backoff_is_jittered_and_retry_after_bounded(self) -> None:
        self.assertEqual(
            delivery_worker._backoff_seconds(
                1,
                retry_after=None,
                random_value=lambda: 0.5,
            ),
            2.5,
        )
        self.assertEqual(
            delivery_worker._backoff_seconds(
                9,
                retry_after=99999,
                random_value=lambda: 1.0,
            ),
            3600.0,
        )

    def test_failure_classification_preserves_provider_ambiguity_rules(self) -> None:
        request = httpx.Request("POST", "http://provider.test")
        chatwoot_500 = httpx.HTTPStatusError(
            "secret body",
            request=request,
            response=httpx.Response(500, request=request),
        )
        decision = delivery_worker._classify_exception("chatwoot", chatwoot_500)
        self.assertTrue(decision.retry)
        self.assertTrue(decision.ambiguous)
        self.assertEqual(decision.error_code, "http_500")
        connect = delivery_worker._classify_exception(
            "evolution",
            httpx.ConnectError("secret-host", request=request),
        )
        self.assertTrue(connect.retry)
        self.assertFalse(connect.ambiguous)
        duplicate = delivery_worker._classify_exception(
            "chatwoot",
            delivery_adapters.DuplicateRemoteEffect("duplicate"),
        )
        self.assertFalse(duplicate.retry)
        self.assertEqual(duplicate.error_code, "duplicate_remote_effect")

    def test_retry_after_accepts_http_date_and_caps_delay(self) -> None:
        request = httpx.Request("POST", "http://provider.test")
        response = httpx.Response(
            429,
            request=request,
            headers={
                "Retry-After": format_datetime(
                    datetime.now(timezone.utc) + timedelta(hours=2),
                    usegmt=True,
                )
            },
        )
        self.assertEqual(
            delivery_adapters.retry_after_seconds(response, maximum=60),
            60,
        )

    def test_worker_logs_never_reference_payload_or_provider_body(self) -> None:
        source = Path(delivery_worker.__file__).read_text()
        for logger_call in source.split("logger.")[1:]:
            call = logger_call.split(")", 1)[0]
            self.assertNotIn("delivery.payload", call)
            self.assertNotIn("response.text", call)
            self.assertNotIn("response.content", call)


class WorkerGatingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # T08-3: replica compatibility has its own real-MySQL suite; these
        # unit tests keep isolating the pre-existing local/DB health inputs.
        compatible = delivery_cli.replica_compat.FleetReport(True, ())
        replicas = patch.object(
            delivery_cli.replica_compat, "fleet_report", AsyncMock(return_value=compatible)
        )
        replicas.start()
        self.addCleanup(replicas.stop)

    async def test_dispatch_disabled_claims_nothing_without_database_or_provider_calls(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        with (
            patch.object(delivery_worker.settings, "delivery_dispatch_enabled", False),
            patch.object(delivery_worker, "AsyncSessionLocal") as session_factory,
        ):
            claimed = await worker._claim(8)
        self.assertEqual(claimed, [])
        session_factory.assert_not_called()

    async def test_claim_retry_exhaustion_is_bounded_and_does_not_escape_worker(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        exhausted = delivery_worker.delivery_worker_repo.ClaimRetryExhausted(
            "bounded"
        )
        with (
            patch.object(delivery_worker.settings, "delivery_dispatch_enabled", True),
            patch.object(delivery_worker.settings, "delivery_mode", "outbox"),
            patch.object(
                delivery_worker.settings,
                "delivery_cutover_epoch",
                "33333333-3333-4333-8333-333333333333",
            ),
            patch.object(worker, "_readiness_ok", True),
            patch.object(
                delivery_worker.delivery_worker_repo,
                "claim_due_with_retry",
                AsyncMock(side_effect=exhausted),
            ) as claim,
        ):
            self.assertEqual(await worker._claim(8), [])
        claim.assert_awaited_once()

    async def test_whole_claim_transaction_retries_fresh_sessions(self) -> None:
        class Deadlock(Exception):
            pass

        class Session:
            def begin(self):
                return _Context(None)

        sessions: list[Session] = []

        def factory():
            session = Session()
            sessions.append(session)
            return _Context(session)

        deadlock = OperationalError("claim", {}, Deadlock(1213, "deadlock"))
        claim = AsyncMock(side_effect=[deadlock, deadlock, []])
        sleep = AsyncMock()
        with patch.object(
            delivery_worker.delivery_worker_repo, "claim_due", claim
        ):
            rows = await delivery_worker.delivery_worker_repo.claim_due_with_retry(
                factory,
                available_slots=8,
                worker_id="worker-a",
                cutover_epoch="33333333-3333-4333-8333-333333333333",
                evolution_lease_seconds=90,
                chatwoot_lease_seconds=120,
                max_attempts=3,
                max_jitter_seconds=0,
                sleep=sleep,
            )
        self.assertEqual(rows, [])
        self.assertEqual(len(sessions), 3)
        self.assertEqual(claim.await_count, 3)
        self.assertEqual(sleep.await_count, 2)

    async def test_whole_claim_transaction_retry_exhaustion_is_bounded(self) -> None:
        class Deadlock(Exception):
            pass

        class Session:
            def begin(self):
                return _Context(None)

        sessions: list[Session] = []

        def factory():
            session = Session()
            sessions.append(session)
            return _Context(session)

        deadlock = OperationalError("claim", {}, Deadlock(1213, "deadlock"))
        sleep = AsyncMock()
        with patch.object(
            delivery_worker.delivery_worker_repo,
            "claim_due",
            AsyncMock(side_effect=deadlock),
        ):
            with self.assertRaises(
                delivery_worker.delivery_worker_repo.ClaimRetryExhausted
            ):
                await delivery_worker.delivery_worker_repo.claim_due_with_retry(
                    factory,
                    available_slots=8,
                    worker_id="worker-a",
                    cutover_epoch="33333333-3333-4333-8333-333333333333",
                    evolution_lease_seconds=90,
                    chatwoot_lease_seconds=120,
                    max_attempts=3,
                    max_jitter_seconds=0,
                    sleep=sleep,
                )
        self.assertEqual(len(sessions), 3)
        self.assertEqual(sleep.await_count, 2)

    async def test_ownership_loss_cancels_and_awaits_blocked_provider(self) -> None:
        claimed = delivery_worker.delivery_worker_repo.ClaimedDelivery(
            id=19,
            lease_token="11111111-1111-4111-8111-111111111111",
            provider="evolution",
            operation="evolution.send_text.v1",
            payload_version=1,
            payload={
                "phone": "551100000019",
                "message": "restricted",
                "instance_name": "instance-a",
            },
            delivery_key="22222222-2222-4222-8222-222222222222",
            agente_id=1,
            sessao_id=2,
            handoff_attempt_id=None,
            attempt_count=1,
            cutover_epoch="33333333-3333-4333-8333-333333333333",
        )
        provider_started = asyncio.Event()
        provider_finalized = asyncio.Event()

        async def blocked_provider(*args, **kwargs):
            provider_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                provider_finalized.set()

        async def lose_authority(self, delivery, lease_seconds, ownership_lost):
            await provider_started.wait()
            ownership_lost.set()

        complete = AsyncMock()
        failure = AsyncMock()
        with (
            patch.object(delivery_worker, "send_evolution_text", blocked_provider),
            patch.object(
                delivery_worker.DeliveryWorker,
                "_renew_loop",
                lose_authority,
            ),
            patch.object(
                delivery_worker.delivery_worker_repo,
                "complete_evolution",
                complete,
            ),
            patch.object(
                delivery_worker.delivery_worker_repo,
                "fail_processing",
                failure,
            ),
        ):
            await asyncio.wait_for(
                delivery_worker.DeliveryWorker()._dispatch_one(claimed),
                timeout=0.5,
            )
        self.assertTrue(provider_finalized.is_set())
        complete.assert_not_awaited()
        failure.assert_not_awaited()

    async def test_concurrent_local_health_publication_has_no_shared_temp_race(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            first = delivery_worker.DeliveryWorker()
            second = delivery_worker.DeliveryWorker()
            for worker in (first, second):
                worker._readiness_ok = True
                worker._provider_ready = True
                worker._runtime_compatible = True
            with patch.object(
                delivery_worker.settings, "delivery_worker_health_path", str(path)
            ):
                await asyncio.gather(
                    asyncio.to_thread(first._publish_health_snapshot),
                    asyncio.to_thread(second._publish_health_snapshot),
                )
            snapshot = json.loads(path.read_text())
            self.assertIn(snapshot["worker_id"], {first.worker_id, second.worker_id})
            self.assertEqual(list(path.parent.glob("*.tmp")), [])

    async def test_missing_provider_contract_marks_local_worker_not_ready(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        db = SimpleNamespace()
        state = SimpleNamespace(
            mode="outbox",
            cutover_epoch="33333333-3333-4333-8333-333333333333",
        )
        with (
            patch.object(delivery_worker.settings, "delivery_dispatch_enabled", True),
            patch.object(delivery_worker.settings, "delivery_mode", "outbox"),
            patch.object(
                delivery_worker.settings,
                "delivery_cutover_epoch",
                state.cutover_epoch,
            ),
            patch.object(
                delivery_worker, "AsyncSessionLocal", return_value=_Context(db)
            ),
            patch.object(
                delivery_worker.delivery_state_repo,
                "get_runtime_state",
                AsyncMock(return_value=state),
            ),
            patch.object(
                delivery_worker.agente_repo,
                "get_active_chatwoot_inbox_ids",
                AsyncMock(return_value=[7]),
            ),
            patch.object(
                delivery_worker.chatwoot_client,
                "check_delivery_contract",
                AsyncMock(side_effect=RuntimeError("missing definition")),
            ),
        ):
            self.assertFalse(await worker._check_readiness())
        self.assertFalse(worker._readiness_ok)
        self.assertFalse(worker._provider_ready)

    async def test_health_uses_exact_local_replica_and_database_utc(self) -> None:
        worker_id = "44444444-4444-4444-8444-444444444444"
        db_now = datetime(2040, 1, 1, 12, 0, 0)
        readiness = delivery_cli.delivery_state_repo.WorkerReadiness(
            db_now=db_now,
            heartbeat=db_now - timedelta(seconds=10),
            oldest_due=db_now - timedelta(seconds=20),
            runtime_mode="outbox",
            runtime_epoch="33333333-3333-4333-8333-333333333333",
        )
        db = SimpleNamespace()
        emitted: list[dict[str, object]] = []
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            path.write_text(
                json.dumps(
                    {
                        "worker_id": worker_id,
                        "ready": True,
                        "provider_ready": True,
                        "runtime_compatible": True,
                        "retention_task": {"state": "idle", "stale": False},
                        "scheduler_retention_health": {"available": True, "candidate_failures": {"count": 2}},
                    }
                )
            )
            lookup = AsyncMock(return_value=readiness)
            with (
                patch.object(
                    delivery_cli.settings, "delivery_worker_health_path", str(path)
                ),
                patch.object(
                    delivery_cli.settings, "delivery_dispatch_enabled", True
                ),
                patch.object(delivery_cli.settings, "delivery_mode", "outbox"),
                patch.object(
                    delivery_cli.settings,
                    "delivery_cutover_epoch",
                    readiness.runtime_epoch,
                ),
                patch.object(
                    delivery_cli, "AsyncSessionLocal", return_value=_Context(db)
                ),
                patch.object(
                    delivery_cli.delivery_state_repo,
                    "get_worker_readiness",
                    lookup,
                ),
                # T08-5: scheduler aggregates are covered by test_scheduler_metrics_mysql.
                patch.object(
                    delivery_cli.scheduler_observability,
                    "scheduler_health",
                    AsyncMock(return_value={"available": True, "healthy": True}),
                ),
                patch.object(delivery_cli, "_emit", emitted.append),
            ):
                self.assertEqual(await delivery_cli._health(), 0)
        self.assertEqual(lookup.await_args.kwargs["worker_id"], worker_id)
        self.assertTrue(emitted[0]["healthy"])
        self.assertEqual(emitted[0]["retention_task"]["state"], "idle")
        self.assertEqual(emitted[0]["scheduler_retention_health"]["candidate_failures"]["count"], 2)

    async def test_health_fails_when_local_provider_readiness_is_false(self) -> None:
        worker_id = "44444444-4444-4444-8444-444444444444"
        now = datetime(2040, 1, 1, 12, 0, 0)
        readiness = delivery_cli.delivery_state_repo.WorkerReadiness(
            db_now=now,
            heartbeat=now,
            oldest_due=None,
            runtime_mode="outbox",
            runtime_epoch="33333333-3333-4333-8333-333333333333",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            path.write_text(
                json.dumps(
                    {
                        "worker_id": worker_id,
                        "ready": False,
                        "provider_ready": False,
                        "runtime_compatible": True,
                    }
                )
            )
            with (
                patch.object(
                    delivery_cli.settings, "delivery_worker_health_path", str(path)
                ),
                patch.object(
                    delivery_cli.settings, "delivery_dispatch_enabled", True
                ),
                patch.object(delivery_cli.settings, "delivery_mode", "outbox"),
                patch.object(
                    delivery_cli.settings,
                    "delivery_cutover_epoch",
                    readiness.runtime_epoch,
                ),
                patch.object(
                    delivery_cli, "AsyncSessionLocal", return_value=_Context(object())
                ),
                patch.object(
                    delivery_cli.delivery_state_repo,
                    "get_worker_readiness",
                    AsyncMock(return_value=readiness),
                ),
                # T08-5: scheduler aggregates are covered by test_scheduler_metrics_mysql.
                patch.object(
                    delivery_cli.scheduler_observability,
                    "scheduler_health",
                    AsyncMock(return_value={"available": True, "healthy": True}),
                ),
                patch.object(delivery_cli, "_emit"),
            ):
                self.assertEqual(await delivery_cli._health(), 1)

    async def _health_for_snapshot(
        self,
        snapshot: dict[str, object],
        *,
        mode: str = "outbox",
    ) -> tuple[int, dict[str, object]]:
        epoch = "33333333-3333-4333-8333-333333333333"
        now = datetime(2040, 1, 1, 12, 0, 0)
        readiness = delivery_cli.delivery_state_repo.WorkerReadiness(
            db_now=now,
            heartbeat=now,
            oldest_due=None,
            runtime_mode=mode,
            runtime_epoch=epoch,
        )
        emitted: list[dict[str, object]] = []
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            path.write_text(json.dumps(snapshot))
            with (
                patch.object(
                    delivery_cli.settings, "delivery_worker_health_path", str(path)
                ),
                patch.object(
                    delivery_cli.settings,
                    "delivery_dispatch_enabled",
                    mode == "outbox",
                ),
                patch.object(delivery_cli.settings, "delivery_mode", mode),
                patch.object(
                    delivery_cli.settings,
                    "delivery_cutover_epoch",
                    epoch if mode == "outbox" else None,
                ),
                patch.object(
                    delivery_cli, "AsyncSessionLocal", return_value=_Context(object())
                ),
                patch.object(
                    delivery_cli.delivery_state_repo,
                    "get_worker_readiness",
                    AsyncMock(return_value=readiness),
                ),
                # T08-5: scheduler aggregates are covered by test_scheduler_metrics_mysql.
                patch.object(
                    delivery_cli.scheduler_observability,
                    "scheduler_health",
                    AsyncMock(return_value={"available": True, "healthy": True}),
                ),
                patch.object(delivery_cli, "_emit", emitted.append),
            ):
                code = await delivery_cli._health()
        return code, emitted[0]

    async def test_health_fails_at_local_maintenance_skip_threshold(self) -> None:
        base = {
            "worker_id": "44444444-4444-4444-8444-444444444444",
            "provider_ready": True,
            "runtime_compatible": True,
            "maintenance_skip_error_threshold": 3,
        }
        cases = {
            # Snapshot written by the worker itself at the threshold.
            "worker_marked_not_ready": (
                {"ready": False, "maintenance_consecutive_skips": 3,
                 "maintenance_healthy": False},
                1,
            ),
            # The CLI gates on maintenance on its own, not only via "ready".
            "cli_gate_independent_of_ready": (
                {"ready": True, "maintenance_consecutive_skips": 3,
                 "maintenance_healthy": False},
                1,
            ),
            "inconsistent_healthy_flag_over_threshold": (
                {"ready": True, "maintenance_consecutive_skips": 3,
                 "maintenance_healthy": True},
                1,
            ),
            "below_threshold_stays_healthy": (
                {"ready": True, "maintenance_consecutive_skips": 2,
                 "maintenance_healthy": True},
                0,
            ),
        }
        for name, (fields, expected) in cases.items():
            with self.subTest(name):
                code, emitted = await self._health_for_snapshot(base | fields)
                self.assertEqual(code, expected)
                self.assertEqual(emitted["healthy"], expected == 0)
                self.assertEqual(emitted["maintenance_healthy"], expected == 0)
                self.assertEqual(
                    emitted["maintenance_consecutive_skips"],
                    fields["maintenance_consecutive_skips"],
                )

        for name, fields in {
            "bool_skips": {"maintenance_consecutive_skips": True},
            "negative_skips": {"maintenance_consecutive_skips": -1},
            "zero_threshold": {"maintenance_skip_error_threshold": 0},
            "non_bool_flag": {"maintenance_healthy": "yes"},
        }.items():
            with self.subTest(name):
                code, emitted = await self._health_for_snapshot(
                    base | {"ready": True} | fields
                )
                self.assertEqual(code, 1)
                self.assertEqual(emitted["reason"], "local_snapshot_unavailable")

    async def test_direct_idle_worker_stays_ready_and_recovers_after_one_pass(
        self,
    ) -> None:
        """Default direct-mode idle worker: healthy maintenance never blocks
        readiness, the threshold marks only the local snapshot, claims stay
        gated by runtime readiness alone, and one completed pass recovers."""
        worker = delivery_worker.DeliveryWorker()
        state = SimpleNamespace(mode="direct", cutover_epoch=None)
        db = SimpleNamespace(commit=AsyncMock())
        lost = OperationalError("UPDATE", {}, Exception(1205, "Lock wait timeout"))
        clean = SimpleNamespace(
            recovered=0, ambiguity_counted=0, cancelled=0, expired=0,
            dependency_terminal=0,
        )
        maintenance = AsyncMock(return_value=clean)
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            path = Path(directory) / "health.json"
            for target, attribute, value in (
                (delivery_worker.settings, "delivery_worker_health_path", str(path)),
                (delivery_worker.settings, "delivery_mode", "direct"),
                (delivery_worker.settings, "delivery_dispatch_enabled", False),
                (delivery_worker.settings, "delivery_cutover_epoch", None),
                (delivery_worker.settings, "delivery_maintenance_skip_error_threshold", 2),
                (delivery_worker, "AsyncSessionLocal", lambda: _Context(db)),
                (delivery_worker.delivery_state_repo, "get_runtime_state",
                 AsyncMock(return_value=state)),
                (delivery_worker.delivery_worker_repo, "run_preclaim_maintenance",
                 maintenance),
                (delivery_worker.usage_reconciliation_repo, "reconcile_due",
                 AsyncMock(return_value=SimpleNamespace(examined=0))),
            ):
                stack.enter_context(patch.object(target, attribute, value))

            def snapshot() -> dict[str, object]:
                worker._publish_health_snapshot()
                return json.loads(path.read_text())

            self.assertTrue(await worker._check_readiness())
            await worker._maintenance()
            published = snapshot()
            self.assertTrue(published["ready"])
            self.assertTrue(published["maintenance_healthy"])
            self.assertEqual(published["maintenance_consecutive_skips"], 0)
            code, emitted = await self._health_for_snapshot(published, mode="direct")
            self.assertEqual(code, 0)
            self.assertTrue(emitted["maintenance_healthy"])

            maintenance.side_effect = lost
            for _ in range(2):
                await worker._maintenance()
            published = snapshot()
            self.assertFalse(published["ready"])
            self.assertFalse(published["maintenance_healthy"])
            self.assertEqual(published["maintenance_consecutive_skips"], 2)
            # Claims and the loop are untouched: only the local snapshot flips.
            self.assertTrue(worker._readiness_ok)
            self.assertFalse(worker._stop.is_set())
            code, _ = await self._health_for_snapshot(published, mode="direct")
            self.assertEqual(code, 1)

            maintenance.side_effect = None
            await worker._maintenance()
            published = snapshot()
            self.assertTrue(published["ready"])
            self.assertTrue(published["maintenance_healthy"])
            self.assertEqual(published["maintenance_consecutive_skips"], 0)
            code, _ = await self._health_for_snapshot(published, mode="direct")
            self.assertEqual(code, 0)

    async def test_health_fails_when_authoritative_epoch_rotates(self) -> None:
        worker_id = "44444444-4444-4444-8444-444444444444"
        now = datetime(2040, 1, 1, 12, 0, 0)
        readiness = delivery_cli.delivery_state_repo.WorkerReadiness(
            db_now=now,
            heartbeat=now,
            oldest_due=None,
            runtime_mode="outbox",
            runtime_epoch="55555555-5555-4555-8555-555555555555",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            path.write_text(
                json.dumps(
                    {
                        "worker_id": worker_id,
                        "ready": True,
                        "provider_ready": True,
                        "runtime_compatible": True,
                    }
                )
            )
            with (
                patch.object(
                    delivery_cli.settings, "delivery_worker_health_path", str(path)
                ),
                patch.object(
                    delivery_cli.settings, "delivery_dispatch_enabled", True
                ),
                patch.object(delivery_cli.settings, "delivery_mode", "outbox"),
                patch.object(
                    delivery_cli.settings,
                    "delivery_cutover_epoch",
                    "33333333-3333-4333-8333-333333333333",
                ),
                patch.object(
                    delivery_cli, "AsyncSessionLocal", return_value=_Context(object())
                ),
                patch.object(
                    delivery_cli.delivery_state_repo,
                    "get_worker_readiness",
                    AsyncMock(return_value=readiness),
                ),
                # T08-5: scheduler aggregates are covered by test_scheduler_metrics_mysql.
                patch.object(
                    delivery_cli.scheduler_observability,
                    "scheduler_health",
                    AsyncMock(return_value={"available": True, "healthy": True}),
                ),
                patch.object(delivery_cli, "_emit"),
            ):
                self.assertEqual(await delivery_cli._health(), 1)

    async def test_worker_failure_logs_omit_phone_prefix_content_and_credentials(self) -> None:
        phone = "5511987654321"
        content = "restricted-message-body"
        credential = "credential-secret"
        request = httpx.Request("POST", "http://provider.test")
        claimed = delivery_worker.delivery_worker_repo.ClaimedDelivery(
            id=17,
            lease_token="11111111-1111-4111-8111-111111111111",
            provider="evolution",
            operation="evolution.send_text.v1",
            payload_version=1,
            payload={
                "phone": phone,
                "message": content,
                "instance_name": "instance-a",
            },
            delivery_key="22222222-2222-4222-8222-222222222222",
            agente_id=1,
            sessao_id=2,
            handoff_attempt_id=None,
            attempt_count=1,
            cutover_epoch="33333333-3333-4333-8333-333333333333",
        )
        db = SimpleNamespace(commit=AsyncMock())
        logs = io.StringIO()
        sink = logger.add(logs, format="{message}\n")
        try:
            with (
                patch.object(
                    delivery_worker,
                    "send_evolution_text",
                    AsyncMock(
                        side_effect=httpx.ConnectError(
                            credential,
                            request=request,
                        )
                    ),
                ),
                patch.object(
                    delivery_worker.delivery_worker_repo,
                    "fail_processing",
                    AsyncMock(return_value="retry"),
                ),
                patch.object(
                    delivery_worker.DeliveryWorker,
                    "_renew_loop",
                    AsyncMock(return_value=None),
                ),
                patch.object(
                    delivery_worker,
                    "AsyncSessionLocal",
                    return_value=_Context(db),
                ),
            ):
                await delivery_worker.DeliveryWorker()._dispatch_one(claimed)
        finally:
            logger.remove(sink)
        rendered = logs.getvalue()
        self.assertNotIn(phone, rendered)
        for length in range(4, len(phone)):
            self.assertNotIn(phone[:length], rendered)
        self.assertNotIn(content, rendered)
        self.assertNotIn(credential, rendered)

    async def test_chatwoot_timeout_reconciles_exact_marker_without_create_retry(self) -> None:
        request = httpx.Request("POST", "http://chatwoot.test")
        claimed = delivery_worker.delivery_worker_repo.ClaimedDelivery(
            id=18,
            lease_token="11111111-1111-4111-8111-111111111111",
            provider="chatwoot",
            operation="chatwoot.open_handoff.v1",
            payload_version=1,
            payload={
                "phone": "551100000001",
                "summary": "restricted",
                "inbox_id": 7,
                "handoff_key": "handoff-key",
            },
            delivery_key="22222222-2222-4222-8222-222222222222",
            agente_id=1,
            sessao_id=2,
            handoff_attempt_id=3,
            attempt_count=1,
            cutover_epoch="33333333-3333-4333-8333-333333333333",
        )
        result = delivery_adapters.DeliveryResult(
            external_id="99",
            http_status=200,
            contact_id=88,
            conversation_id=99,
            reconciled=True,
        )
        db = SimpleNamespace(commit=AsyncMock())
        complete = AsyncMock(return_value=True)
        reconcile = AsyncMock(return_value=result)
        failure = AsyncMock(return_value="retry")
        with (
            patch.object(
                delivery_worker.DeliveryWorker,
                "_dispatch_chatwoot",
                AsyncMock(side_effect=httpx.ReadTimeout("lost ack", request=request)),
            ),
            patch.object(
                delivery_worker,
                "open_or_reconcile_chatwoot_handoff",
                reconcile,
            ),
            patch.object(
                delivery_worker.delivery_worker_repo,
                "complete_chatwoot_handoff",
                complete,
            ),
            patch.object(
                delivery_worker.delivery_worker_repo,
                "fail_processing",
                failure,
            ),
            patch.object(
                delivery_worker.DeliveryWorker,
                "_renew_loop",
                AsyncMock(return_value=None),
            ),
            patch.object(
                delivery_worker,
                "AsyncSessionLocal",
                return_value=_Context(db),
            ),
        ):
            await delivery_worker.DeliveryWorker()._dispatch_one(claimed)
        self.assertTrue(reconcile.await_args.kwargs["reconcile_only"])
        complete.assert_awaited_once()
        failure.assert_not_awaited()

    async def test_initial_dispatch_and_reconcile_share_one_absolute_deadline(self) -> None:
        request = httpx.Request("POST", "http://chatwoot.test")
        claimed = delivery_worker.delivery_worker_repo.ClaimedDelivery(
            id=20,
            lease_token="11111111-1111-4111-8111-111111111111",
            provider="chatwoot",
            operation="chatwoot.open_handoff.v1",
            payload_version=1,
            payload={
                "phone": "551100000020",
                "summary": "restricted",
                "inbox_id": 7,
                "handoff_key": "handoff-key",
            },
            delivery_key="22222222-2222-4222-8222-222222222222",
            agente_id=1,
            sessao_id=2,
            handoff_attempt_id=3,
            attempt_count=1,
            cutover_epoch="33333333-3333-4333-8333-333333333333",
        )

        async def slow_initial(*args, **kwargs):
            await asyncio.sleep(0.05)
            raise httpx.ReadTimeout("lost ack", request=request)

        async def slow_reconcile(*args, **kwargs):
            await asyncio.sleep(0.05)
            return delivery_adapters.DeliveryResult(
                external_id="99",
                http_status=200,
                contact_id=88,
                conversation_id=99,
                reconciled=True,
            )

        worker = delivery_worker.DeliveryWorker()
        started = time.monotonic()
        with (
            patch.object(worker, "_dispatch_chatwoot", slow_initial),
            patch.object(
                delivery_worker,
                "open_or_reconcile_chatwoot_handoff",
                slow_reconcile,
            ),
        ):
            result, failure = await worker._run_provider_operation(
                claimed,
                deadline_at=time.monotonic() + 0.08,
            )
        self.assertIsNone(result)
        self.assertIsNotNone(failure)
        self.assertEqual(failure.error_code, "response_ambiguous")
        self.assertLess(time.monotonic() - started, 0.12)


class GracefulDrainTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _runtime_patches(worker, claimed):
        return (
            patch.object(worker, "_check_readiness", AsyncMock(return_value=True)),
            patch.object(worker, "_maintenance", AsyncMock()),
            patch.object(worker, "_retention", AsyncMock()),
            patch.object(worker, "_heartbeat", AsyncMock()),
            patch.object(worker, "_claim", AsyncMock(return_value=claimed)),
            patch.object(
                delivery_worker,
                "engine",
                SimpleNamespace(dispose=AsyncMock()),
            ),
            patch.object(delivery_worker.settings, "delivery_poll_interval_seconds", 0.01),
        )

    async def test_main_signal_path_completes_provider_with_renewal_during_grace(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        claimed = _claimed_delivery(31)
        provider_started = asyncio.Event()
        release_provider = asyncio.Event()
        provider_cancelled = asyncio.Event()
        renewal_after_stop = asyncio.Event()
        renewal_finalized = asyncio.Event()
        signal_handlers: dict[int, object] = {}
        db = SimpleNamespace(commit=AsyncMock())
        complete = AsyncMock(return_value=True)

        async def provider(*args, **kwargs):
            provider_started.set()
            try:
                await release_provider.wait()
                return delivery_adapters.DeliveryResult(
                    external_id="remote-31",
                    http_status=200,
                )
            except asyncio.CancelledError:
                provider_cancelled.set()
                raise

        async def renew(delivery, lease_seconds, ownership_lost):
            try:
                while True:
                    await asyncio.sleep(0.005)
                    if worker._stop.is_set():
                        renewal_after_stop.set()
            finally:
                renewal_finalized.set()

        patches = self._runtime_patches(worker, [claimed])
        with ExitStack() as stack:
            runtime_mocks = [
                stack.enter_context(runtime_patch) for runtime_patch in patches
            ]
            claim = runtime_mocks[4]
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_grace_seconds", 0.2))
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_abort_seconds", 0.1))
            stack.enter_context(patch.object(worker, "_renew_loop", renew))
            stack.enter_context(patch.object(delivery_worker, "send_evolution_text", provider))
            stack.enter_context(
                patch.object(delivery_worker, "DeliveryWorker", return_value=worker)
            )
            stack.enter_context(
                patch.object(
                    delivery_worker.asyncio,
                    "get_running_loop",
                    return_value=SimpleNamespace(
                        add_signal_handler=lambda signum, callback: signal_handlers.__setitem__(
                            signum, callback
                        )
                    ),
                )
            )
            stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "complete_evolution",
                complete,
            ))
            failure = stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "fail_processing",
                AsyncMock(),
            ))
            stack.enter_context(patch.object(
                delivery_worker,
                "AsyncSessionLocal",
                return_value=_Context(db),
            ))
            run = asyncio.create_task(delivery_worker._main())
            await asyncio.wait_for(provider_started.wait(), timeout=0.5)
            self.assertIn(delivery_worker.signal.SIGTERM, signal_handlers)
            self.assertIn(delivery_worker.signal.SIGINT, signal_handlers)
            signal_handlers[delivery_worker.signal.SIGTERM]()
            await asyncio.wait_for(renewal_after_stop.wait(), timeout=0.1)
            release_provider.set()
            await asyncio.wait_for(run, timeout=0.5)

        complete.assert_awaited_once()
        failure.assert_not_awaited()
        self.assertFalse(provider_cancelled.is_set())
        self.assertTrue(renewal_finalized.is_set())
        self.assertFalse(worker._abort.is_set())
        claim.assert_awaited_once()

    async def test_run_cancels_only_after_absolute_drain_deadline(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        claimed = _claimed_delivery(32)
        provider_started = asyncio.Event()
        provider_cancelled = asyncio.Event()
        renewal_finalized = asyncio.Event()
        cancelled_at: list[float] = []

        async def provider(*args, **kwargs):
            provider_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled_at.append(time.monotonic())
                provider_cancelled.set()

        async def renew(delivery, lease_seconds, ownership_lost):
            try:
                await asyncio.Event().wait()
            finally:
                renewal_finalized.set()

        patches = self._runtime_patches(worker, [claimed])
        with ExitStack() as stack:
            for runtime_patch in patches:
                stack.enter_context(runtime_patch)
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_grace_seconds", 0.06))
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_abort_seconds", 0.1))
            stack.enter_context(patch.object(worker, "_renew_loop", renew))
            stack.enter_context(patch.object(delivery_worker, "send_evolution_text", provider))
            complete = stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "complete_evolution",
                AsyncMock(),
            ))
            failure = stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "fail_processing",
                AsyncMock(),
            ))
            run = asyncio.create_task(worker.run())
            await asyncio.wait_for(provider_started.wait(), timeout=0.5)
            stop_at = time.monotonic()
            worker.request_stop()
            await asyncio.wait_for(run, timeout=0.5)

        self.assertTrue(provider_cancelled.is_set())
        self.assertGreaterEqual(cancelled_at[0] - stop_at, 0.045)
        self.assertTrue(renewal_finalized.is_set())
        self.assertTrue(worker._abort.is_set())
        complete.assert_not_awaited()
        failure.assert_not_awaited()

    async def test_run_uses_one_deadline_for_multiple_jobs_and_repeated_signals(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        claimed = [_claimed_delivery(33), _claimed_delivery(34)]
        both_started = asyncio.Event()
        release_first = asyncio.Event()
        started: set[str] = set()
        finalized: set[str] = set()
        db = SimpleNamespace(commit=AsyncMock())
        complete = AsyncMock(return_value=True)

        async def provider(payload, **kwargs):
            started.add(payload.phone)
            if len(started) == 2:
                both_started.set()
            try:
                if payload.phone.endswith("0033"):
                    await release_first.wait()
                    return delivery_adapters.DeliveryResult(
                        external_id="remote-33",
                        http_status=200,
                    )
                await asyncio.Event().wait()
            finally:
                finalized.add(payload.phone)

        async def renew(delivery, lease_seconds, ownership_lost):
            await asyncio.Event().wait()

        patches = self._runtime_patches(worker, claimed)
        with ExitStack() as stack:
            runtime_mocks = [
                stack.enter_context(runtime_patch) for runtime_patch in patches
            ]
            claim = runtime_mocks[4]
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_grace_seconds", 0.08))
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_abort_seconds", 0.1))
            stack.enter_context(patch.object(worker, "_renew_loop", renew))
            stack.enter_context(patch.object(delivery_worker, "send_evolution_text", provider))
            stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "complete_evolution",
                complete,
            ))
            failure = stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "fail_processing",
                AsyncMock(),
            ))
            stack.enter_context(patch.object(
                delivery_worker,
                "AsyncSessionLocal",
                return_value=_Context(db),
            ))
            run = asyncio.create_task(worker.run())
            await asyncio.wait_for(both_started.wait(), timeout=0.5)
            worker.request_stop()
            original_deadline = worker._drain_deadline
            original_shutdown_deadline = worker._shutdown_deadline
            await asyncio.sleep(0.02)
            worker.request_stop()
            self.assertEqual(worker._drain_deadline, original_deadline)
            self.assertEqual(worker._shutdown_deadline, original_shutdown_deadline)
            release_first.set()
            await asyncio.wait_for(run, timeout=0.5)

        self.assertEqual(complete.await_count, 1)
        self.assertEqual(complete.await_args.kwargs["delivery_id"], 33)
        failure.assert_not_awaited()
        self.assertEqual(len(finalized), 2)
        claim.assert_awaited_once()

    async def test_ownership_loss_during_drain_still_cancels_immediately(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        claimed = _claimed_delivery(35)
        provider_started = asyncio.Event()
        provider_cancelled = asyncio.Event()

        async def provider(*args, **kwargs):
            provider_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                provider_cancelled.set()

        async def lose_during_drain(delivery, lease_seconds, ownership_lost):
            await worker._stop.wait()
            ownership_lost.set()

        patches = self._runtime_patches(worker, [claimed])
        with ExitStack() as stack:
            for runtime_patch in patches:
                stack.enter_context(runtime_patch)
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_grace_seconds", 0.3))
            stack.enter_context(patch.object(delivery_worker.settings, "delivery_shutdown_abort_seconds", 0.1))
            stack.enter_context(patch.object(worker, "_renew_loop", lose_during_drain))
            stack.enter_context(patch.object(delivery_worker, "send_evolution_text", provider))
            complete = stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "complete_evolution",
                AsyncMock(),
            ))
            failure = stack.enter_context(patch.object(
                delivery_worker.delivery_worker_repo,
                "fail_processing",
                AsyncMock(),
            ))
            run = asyncio.create_task(worker.run())
            await asyncio.wait_for(provider_started.wait(), timeout=0.5)
            stop_at = time.monotonic()
            worker.request_stop()
            await asyncio.wait_for(run, timeout=0.2)

        self.assertLess(time.monotonic() - stop_at, 0.2)
        self.assertTrue(provider_cancelled.is_set())
        self.assertFalse(worker._abort.is_set())
        complete.assert_not_awaited()
        failure.assert_not_awaited()

    async def test_real_signal_bounds_blocked_claim_and_starts_no_provider(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        claimed = _claimed_delivery(36)
        claim_started = asyncio.Event()
        release_claim = asyncio.Event()
        claim_finished = asyncio.Event()
        provider = AsyncMock()

        async def blocked_claim(capacity):
            claim_started.set()
            try:
                while not release_claim.is_set():
                    try:
                        await release_claim.wait()
                    except asyncio.CancelledError:
                        continue
                return [claimed]
            finally:
                claim_finished.set()

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(worker, "_check_readiness", AsyncMock(return_value=True))
            )
            stack.enter_context(patch.object(worker, "_maintenance", AsyncMock()))
            stack.enter_context(patch.object(worker, "_retention", AsyncMock()))
            stack.enter_context(patch.object(worker, "_heartbeat", AsyncMock()))
            stack.enter_context(patch.object(worker, "_claim", blocked_claim))
            stack.enter_context(
                patch.object(
                    delivery_worker,
                    "engine",
                    SimpleNamespace(dispose=AsyncMock()),
                )
            )
            stack.enter_context(
                patch.object(
                    delivery_worker.settings,
                    "delivery_shutdown_grace_seconds",
                    0.08,
                )
            )
            stack.enter_context(
                patch.object(
                    delivery_worker.settings,
                    "delivery_shutdown_abort_seconds",
                    0.04,
                )
            )
            stack.enter_context(
                patch.object(delivery_worker, "send_evolution_text", provider)
            )
            stack.enter_context(
                patch.object(delivery_worker, "DeliveryWorker", return_value=worker)
            )
            run = asyncio.create_task(delivery_worker._main())
            await asyncio.wait_for(claim_started.wait(), timeout=0.5)
            stopped_at = time.monotonic()
            os.kill(os.getpid(), delivery_worker.signal.SIGTERM)
            await asyncio.wait_for(run, timeout=0.25)
            elapsed = time.monotonic() - stopped_at
            release_claim.set()
            await asyncio.wait_for(claim_finished.wait(), timeout=0.1)

        self.assertLess(elapsed, 0.18)
        provider.assert_not_awaited()

    async def test_claim_returning_after_grace_never_starts_provider(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        claimed = _claimed_delivery(37)
        claim_started = asyncio.Event()
        provider = AsyncMock()

        async def late_claim(capacity):
            claim_started.set()
            returns_at = time.monotonic() + 0.12
            while time.monotonic() < returns_at:
                try:
                    await asyncio.sleep(returns_at - time.monotonic())
                except asyncio.CancelledError:
                    continue
            return [claimed]

        with ExitStack() as stack:
            for runtime_patch in self._runtime_patches(worker, []):
                stack.enter_context(runtime_patch)
            stack.enter_context(patch.object(worker, "_claim", late_claim))
            stack.enter_context(
                patch.object(
                    delivery_worker.settings,
                    "delivery_shutdown_grace_seconds",
                    0.08,
                )
            )
            stack.enter_context(
                patch.object(
                    delivery_worker.settings,
                    "delivery_shutdown_abort_seconds",
                    0.08,
                )
            )
            stack.enter_context(
                patch.object(delivery_worker, "send_evolution_text", provider)
            )
            stack.enter_context(
                patch.object(delivery_worker, "DeliveryWorker", return_value=worker)
            )
            run = asyncio.create_task(delivery_worker._main())
            await asyncio.wait_for(claim_started.wait(), timeout=0.5)
            stopped_at = time.monotonic()
            os.kill(os.getpid(), delivery_worker.signal.SIGTERM)
            await asyncio.wait_for(run, timeout=0.25)

        self.assertGreaterEqual(time.monotonic() - stopped_at, 0.1)
        provider.assert_not_awaited()
        self.assertEqual(worker.active_count, 0)

    async def test_blocked_final_heartbeat_and_disposal_share_absolute_budget(self) -> None:
        worker = delivery_worker.DeliveryWorker()
        worker._last_heartbeat = time.monotonic()
        loop_reached_claim = asyncio.Event()
        heartbeat_started = asyncio.Event()
        heartbeat_cancelled = asyncio.Event()
        disposal_started = asyncio.Event()
        disposal_cancelled = asyncio.Event()

        async def no_claims(capacity):
            loop_reached_claim.set()
            return []

        async def blocked_heartbeat():
            heartbeat_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                heartbeat_cancelled.set()

        async def blocked_dispose():
            disposal_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                disposal_cancelled.set()

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(worker, "_check_readiness", AsyncMock(return_value=True))
            )
            stack.enter_context(patch.object(worker, "_maintenance", AsyncMock()))
            stack.enter_context(patch.object(worker, "_retention", AsyncMock()))
            stack.enter_context(patch.object(worker, "_heartbeat", blocked_heartbeat))
            stack.enter_context(patch.object(worker, "_claim", no_claims))
            stack.enter_context(
                patch.object(
                    delivery_worker,
                    "engine",
                    SimpleNamespace(dispose=blocked_dispose),
                )
            )
            stack.enter_context(
                patch.object(
                    delivery_worker.settings,
                    "delivery_shutdown_grace_seconds",
                    0.08,
                )
            )
            stack.enter_context(
                patch.object(
                    delivery_worker.settings,
                    "delivery_shutdown_abort_seconds",
                    0.04,
                )
            )
            stack.enter_context(
                patch.object(delivery_worker, "DeliveryWorker", return_value=worker)
            )
            run = asyncio.create_task(delivery_worker._main())
            await asyncio.wait_for(loop_reached_claim.wait(), timeout=0.5)
            stopped_at = time.monotonic()
            os.kill(os.getpid(), delivery_worker.signal.SIGTERM)
            await asyncio.wait_for(run, timeout=0.25)
            elapsed = time.monotonic() - stopped_at

        self.assertLess(elapsed, 0.18)
        self.assertTrue(heartbeat_started.is_set())
        self.assertTrue(heartbeat_cancelled.is_set())
        self.assertTrue(disposal_started.is_set())
        self.assertTrue(disposal_cancelled.is_set())


class DeliveryCLITests(unittest.TestCase):
    def test_mutations_require_explicit_actor_and_reason(self) -> None:
        parser = delivery_cli._parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(["retry", "1"])
        parsed = parser.parse_args(
            ["retry", "1", "--actor", "operator", "--reason", "incident"]
        )
        self.assertEqual(parsed.delivery_id, 1)
        self.assertEqual(parsed.actor, "operator")
        with self.assertRaises(SystemExit):
            parser.parse_args(
                ["cancel", "1", "--actor", "   ", "--reason", "incident"]
            )
        with self.assertRaises(SystemExit):
            parser.parse_args(
                ["cancel", "1", "--actor", "operator", "--reason", "\t"]
            )
        with self.assertRaises(SystemExit):
            parser.parse_args(
                ["resolve-context-note", "1", "--resolution", "retry-safe"]
            )
        resolved = parser.parse_args(
            [
                "resolve-context-note",
                "1",
                "--resolution",
                "delivered",
                "--actor",
                "operator",
                "--reason",
                "provider verified",
            ]
        )
        self.assertEqual((resolved.delivery_id, resolved.resolution), (1, "delivered"))

    def test_cli_is_not_registered_in_public_routes(self) -> None:
        root = Path(delivery_worker.__file__).resolve().parents[1]
        public_sources = [root / "src" / "main.py", *sorted((root / "src" / "routers").glob("*.py"))]
        for source in public_sources:
            self.assertNotIn("delivery_cli", source.read_text())


class EvolutionAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_presence_uses_provider_contract_and_bounded_durable_delay(self) -> None:
        client = SimpleNamespace(post=AsyncMock(return_value=_Response({"ok": True}, 201)))
        with patch.object(
            delivery_adapters.httpx,
            "AsyncClient",
            return_value=_Context(client),
        ):
            await delivery_adapters.send_evolution_presence(
                phone="551100000001",
                instance_name="instance-a",
                presence="composing",
                delay_ms=12000,
                timeout_seconds=15,
            )
        self.assertEqual(
            client.post.await_args.kwargs["json"],
            {
                "number": "551100000001@s.whatsapp.net",
                "presence": "composing",
                "delay": 12000,
            },
        )
        self.assertTrue(
            client.post.await_args.args[0].endswith("/chat/sendPresence/instance-a")
        )
        with self.assertRaises(ValueError):
            await delivery_adapters.send_evolution_presence(
                phone="1",
                instance_name="i",
                presence="composing",
                delay_ms=12001,
                timeout_seconds=1,
            )

    async def test_validated_2xx_without_remote_id_is_delivered(self) -> None:
        client = SimpleNamespace(post=AsyncMock(return_value=_Response({"ok": True}, 201)))
        with patch.object(
            delivery_adapters.httpx,
            "AsyncClient",
            return_value=_Context(client),
        ):
            result = await delivery_adapters.send_evolution_text(
                delivery_adapters.EvolutionTextPayloadV1(
                    phone="551100000001",
                    message="restricted",
                    instance_name="instance-a",
                ),
                timeout_seconds=10,
            )
        self.assertEqual(result.http_status, 201)
        self.assertIsNone(result.external_id)
        self.assertEqual(
            client.post.await_args.kwargs["json"],
            {"number": "551100000001@s.whatsapp.net", "text": "restricted"},
        )

    async def test_nested_remote_id_is_normalized(self) -> None:
        client = SimpleNamespace(
            post=AsyncMock(return_value=_Response({"key": {"id": "remote-1"}}))
        )
        with patch.object(
            delivery_adapters.httpx,
            "AsyncClient",
            return_value=_Context(client),
        ):
            result = await delivery_adapters.send_evolution_text(
                delivery_adapters.EvolutionTextPayloadV1("1", "x", "i"),
                timeout_seconds=10,
            )
        self.assertEqual(result.external_id, "remote-1")


class ChatwootAdapterTests(unittest.IsolatedAsyncioTestCase):
    def _payload(self) -> delivery_adapters.ChatwootHandoffPayloadV1:
        return delivery_adapters.ChatwootHandoffPayloadV1(
            phone="551100000001",
            summary="restricted summary",
            inbox_id=7,
            handoff_key="handoff-key",
        )

    async def test_exact_reconciliation_verifies_account_inbox_contact_and_marker(self) -> None:
        marker = "delivery-key"
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=[
                    _Response({"id": 7, "lock_to_single_conversation": False}),
                    _Response(
                        {
                            "payload": [{"id": 9, "phone_number": "+551100000001"}],
                            "meta": {"has_more": False},
                        }
                    ),
                ]
            ),
            post=AsyncMock(),
        )
        row = {
            "id": 33,
            "account_id": delivery_adapters.settings.chatwoot_account_id,
            "inbox_id": 7,
            "contact_id": 9,
            "custom_attributes": {"wpp_delivery_key": marker},
        }
        with (
            patch.object(
                delivery_adapters.httpx,
                "AsyncClient",
                return_value=_Context(client),
            ),
            patch.object(
                delivery_adapters.chatwoot_client,
                "_filter_conversations_by_delivery_key",
                AsyncMock(return_value=[row]),
            ),
        ):
            result = await delivery_adapters.open_or_reconcile_chatwoot_handoff(
                self._payload(),
                delivery_key=marker,
                timeout_seconds=10,
            )
        self.assertTrue(result.reconciled)
        self.assertEqual(result.conversation_id, 33)
        client.post.assert_not_awaited()

    async def test_multiple_exact_markers_are_terminal(self) -> None:
        marker = "delivery-key"
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=[
                    _Response({"id": 7, "lock_to_single_conversation": False}),
                    _Response(
                        {
                            "payload": [{"id": 9, "phone_number": "+551100000001"}],
                            "meta": {"has_more": False},
                        }
                    ),
                ]
            ),
            post=AsyncMock(),
        )
        rows = [
            {
                "id": value,
                "account_id": delivery_adapters.settings.chatwoot_account_id,
                "inbox_id": 7,
                "contact_id": 9,
                "custom_attributes": {"wpp_delivery_key": marker},
            }
            for value in (33, 34)
        ]
        with (
            patch.object(
                delivery_adapters.httpx,
                "AsyncClient",
                return_value=_Context(client),
            ),
            patch.object(
                delivery_adapters.chatwoot_client,
                "_filter_conversations_by_delivery_key",
                AsyncMock(return_value=rows),
            ),
        ):
            with self.assertRaises(delivery_adapters.DuplicateRemoteEffect):
                await delivery_adapters.open_or_reconcile_chatwoot_handoff(
                    self._payload(),
                    delivery_key=marker,
                    timeout_seconds=10,
                )

    async def test_locked_inbox_fails_before_any_remote_mutation(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(return_value=_Response({"id": 7, "lock_to_single_conversation": True})),
            post=AsyncMock(),
        )
        with patch.object(
            delivery_adapters.httpx,
            "AsyncClient",
            return_value=_Context(client),
        ):
            with self.assertRaises(delivery_adapters.chatwoot_client.ChatwootContractError):
                await delivery_adapters.open_or_reconcile_chatwoot_handoff(
                    self._payload(),
                    delivery_key="delivery-key",
                    timeout_seconds=10,
                )
        client.post.assert_not_awaited()

    async def test_create_sets_exact_marker_and_private_summary_then_verifies_result(self) -> None:
        marker = "delivery-key"
        verified = {
            "id": 33,
            "account_id": delivery_adapters.settings.chatwoot_account_id,
            "inbox_id": 7,
            "contact_id": 9,
            "custom_attributes": {"wpp_delivery_key": marker},
        }
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=[
                    _Response({"id": 7, "lock_to_single_conversation": False}),
                    _Response(
                        {
                            "payload": [{"id": 9, "phone_number": "+551100000001"}],
                            "meta": {"has_more": False},
                        }
                    ),
                    _Response(verified),
                ]
            ),
            post=AsyncMock(return_value=_Response({"id": 33}, 201)),
        )
        with (
            patch.object(
                delivery_adapters.httpx,
                "AsyncClient",
                return_value=_Context(client),
            ),
            patch.object(
                delivery_adapters.chatwoot_client,
                "_filter_conversations_by_delivery_key",
                AsyncMock(return_value=[]),
            ),
        ):
            result = await delivery_adapters.open_or_reconcile_chatwoot_handoff(
                self._payload(),
                delivery_key=marker,
                timeout_seconds=10,
            )
        create_payload = client.post.await_args.kwargs["json"]
        self.assertEqual(create_payload["custom_attributes"]["wpp_delivery_key"], marker)
        self.assertTrue(create_payload["message"]["private"])
        self.assertEqual(result.conversation_id, 33)

    async def test_multi_page_reconcile_obeys_one_absolute_deadline(self) -> None:
        get = AsyncMock(
            side_effect=[
                _Response({"id": 7, "lock_to_single_conversation": False}),
                _Response(
                    {
                        "payload": [
                            {"id": 9, "phone_number": "+551100000001"}
                        ],
                        "meta": {"has_more": False},
                    }
                ),
            ]
        )

        async def slow_page(*args, **kwargs):
            await asyncio.sleep(0.04)
            page = kwargs["params"]["page"]
            return _Response(
                {
                    "payload": [
                        {
                            "id": page,
                            "account_id": delivery_adapters.settings.chatwoot_account_id,
                            "inbox_id": 7,
                            "contact_id": 9,
                            "custom_attributes": {
                                "wpp_delivery_key": "delivery-key"
                            },
                        }
                    ],
                    "meta": {"all_count": 100},
                }
            )

        client = SimpleNamespace(get=get, post=AsyncMock(side_effect=slow_page))
        started = time.monotonic()
        with patch.object(
            delivery_adapters.httpx,
            "AsyncClient",
            return_value=_Context(client),
        ):
            with self.assertRaises(asyncio.TimeoutError):
                await delivery_adapters.open_or_reconcile_chatwoot_handoff(
                    self._payload(),
                    delivery_key="delivery-key",
                    timeout_seconds=0.09,
                    reconcile_only=True,
                )
        self.assertLess(time.monotonic() - started, 0.14)
        self.assertGreaterEqual(client.post.await_count, 2)


if __name__ == "__main__":
    unittest.main()
