"""T08-5: aggregate scheduler metrics and webhook 503 codes on real MySQL.

Synthetic data only; fake providers, no LLM/WhatsApp. Every metric and every
503 code has a named test that the fix-removal mutations target.
"""
import os
import ast
import asyncio
import io
import json
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from fastapi import Response
from sqlalchemy import text
import inspect

from sqlalchemy.exc import DBAPIError, OperationalError

from src import delivery_cli, main
from src.config import settings
from src.database.models import (
    ConversationTurn, GenerationAttempt, InboundEvent, LLMUsageReconciliation, TurnRevision,
)
from src.database.repositories import (
    consumo_repo, delivery_state_repo, scheduler_metrics_repo, scheduler_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services import evolution_orchestrator, replica_compat, scheduler_observability as obs
from src.utils import logger as app_logger
from src.utils.async_ttl_cache import AsyncSingleFlightTTL
from tests import test_handoff_terminal_mysql as base

BEARER = "t085-bearer-secret"
PHONE = "5511987650085"
CONTENT = "t085-conteudo-sigiloso"
REPO_ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
REAL_FLEET_REPORT = replica_compat.fleet_report
REAL_FLEET_REPORT_DB = replica_compat.fleet_report_db
# S4/P3-8: every process entrypoint and the scope that must call setup_logger.
LOG_ENTRYPOINTS = {
    "app/src/main.py": None,  # module import (uvicorn loads src.main:app)
    "app/src/delivery_cli.py": "main",
    "app/src/provider_auth_cli.py": "main",
    "app/src/auth_cli.py": "main",
    "app/src/delivery_worker.py": "__main__",
    "app/src/turn_worker.py": "__main__",
}


class SchedulerMetricsMySQLTests(base.HandoffTerminalMySQLTests):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        main.reset_health_observability_caches()
        main.reset_health_transition_state()
        obs.reset_health_transition_state()
        replica_compat.reset_health_transition_state()
        self.patches.enter_context(patch.object(settings, "evolution_webhook_bearer_token", BEARER))

        async def uow_override():
            async with UnitOfWork(self.sessions) as uow:
                yield uow

        async def db_override():
            async with self.sessions() as db:
                yield db

        main.app.dependency_overrides[main.get_unit_of_work] = uow_override
        main.app.dependency_overrides[main.get_db] = db_override
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=main.app),
                                        base_url="http://t085.test")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        main.app.dependency_overrides.clear()
        await super().asyncTearDown()

    # ── fixtures ────────────────────────────────────────────────────────
    async def _sql(self, statement: str, **params) -> None:
        async with self.sessions.begin() as db:
            await db.execute(text(statement), params)

    async def _state(self, state: str) -> None:
        await self._sql("UPDATE scheduler_runtime_state SET state=:s WHERE id=1", s=state)

    async def _metrics(self, **kwargs) -> scheduler_metrics_repo.SchedulerMetrics:
        return await scheduler_metrics_repo.collect(self.sessions, **kwargs)

    async def _health(self, *, reset_cache: bool = True) -> tuple[int | None, dict]:
        if reset_cache:
            main.reset_health_observability_caches()
        response = Response()
        with patch.object(main, "provider_auth_readiness", AsyncMock(return_value=SimpleNamespace(ready=True))):
            async with self.sessions() as db:
                body = await main.health_check(response, db)
        return response.status_code, body

    async def _turn_graph(self, suffix: str, phones: int = 1) -> tuple[int, str, list[ConversationTurn]]:
        agent, instance = await self._agent(f"t085-{suffix}")
        for index in range(phones):
            await self._accept(instance, f"5511{agent:05d}{index:04d}", f"{suffix}-{index}", "abc")
        turns = [t for t in await self._rows(ConversationTurn, ConversationTurn.id) if t.agente_id == agent]
        return agent, instance, turns

    async def _revision(self, turn_id: int) -> TurnRevision:
        return [r for r in await self._rows(TurnRevision, TurnRevision.id) if r.turn_id == turn_id][0]

    async def _attempt(self, turn: ConversationTurn, *, outcome: str, state: str = "pending",
                       reconciled: bool = False, owner: str | None = None, drain: bool = False) -> None:
        revision = await self._revision(turn.id)
        async with self.sessions.begin() as db:
            count = len([r for r in (await db.execute(text(
                "SELECT id FROM generation_attempts WHERE revision_id=:r"), {"r": revision.id})).all()])
            db.add(GenerationAttempt(
                agente_id=turn.agente_id, sessao_id=turn.sessao_id, turn_id=turn.id,
                revision_id=revision.id, scheduler_epoch=self.scheduler_epoch,
                lane_owner_token=owner or str(uuid4()), attempt_type="response",
                attempt_ordinal=count + 1, modelo_llm=base.MODEL, outcome=outcome,
                accounting_state=state,
                outcome_recorded_at=None if outcome == "started" else datetime.utcnow(),
                accounting_drain_until=datetime.utcnow() + timedelta(hours=1) if drain else None,
                reconciled_at=datetime.utcnow() if reconciled else None,
                reconciliation_actor="t085" if reconciled else None,
                reconciliation_reason="evidence=t085" if reconciled else None))

    async def _own_lane(self, turn: ConversationTurn, *, expired: bool, token: str | None = None) -> str:
        token = token or str(uuid4())
        revision = await self._revision(turn.id)
        await self._sql(
            "UPDATE agent_service_lanes SET owner_token=:tok, owner_worker_id='t085', "
            f"lease_until=UTC_TIMESTAMP(6) {'-' if expired else '+'} INTERVAL 1 HOUR, "
            "active_turn_id=:t, active_revision_id=:r WHERE agente_id=:a",
            tok=token, t=turn.id, r=revision.id, a=turn.agente_id)
        return token

    def _payload(self, instance: str, message_id: str, phone: str = PHONE, content: str = CONTENT) -> dict:
        return {"event": "messages.upsert", "instance": instance,
                "data": {"key": {"id": message_id, "fromMe": False, "remoteJid": f"{phone}@s.whatsapp.net"},
                         "message": {"conversation": content}}}

    async def _post(self, instance: str, message_id: str, *, phone: str = PHONE,
                    content: str = CONTENT) -> httpx.Response:
        return await self.client.post("/webhook", headers={"Authorization": f"Bearer {BEARER}"},
                                      json=self._payload(instance, message_id, phone, content))

    def _assert_503(self, response: httpx.Response, code: str, *, retry_after: str | None = "5") -> None:
        self.assertEqual(response.status_code, 503, response.text)
        self.assertEqual(response.json(), {"detail": "temporarily unavailable" if retry_after
                                           else "webhook authentication unavailable", "code": code})
        self.assertEqual(response.headers.get("retry-after"), retry_after)
        for secret in (PHONE, CONTENT, BEARER, self.scheduler_epoch, self.delivery_epoch):
            self.assertNotIn(secret, response.text)

    async def _expect_code(self, code: str, call, *, cap: str | None = None, retry_after: str | None = "5"):
        before = obs.rejection_counts()
        response = await call()
        self._assert_503(response, code, retry_after=retry_after)
        after = obs.rejection_counts()
        self.assertEqual(after["by_code"].get(code, 0), before["by_code"].get(code, 0) + 1)
        if cap is not None:
            self.assertEqual(after["by_cap"].get(cap, 0), before["by_cap"].get(cap, 0) + 1)
        return response

    # ── metrics ─────────────────────────────────────────────────────────
    async def test_state_version_and_epoch_match(self) -> None:
        await self._sql("UPDATE scheduler_runtime_state SET state='draining', version=41 WHERE id=1")
        status, body = await self._health()
        block = body["scheduler"]
        self.assertTrue(block["available"])
        self.assertEqual((block["state"], block["version"]), ("draining", 41))
        self.assertEqual((block["epoch_configured"], block["epoch_matches"]), (True, True))
        self.assertNotIn("epoch", block)
        with patch.object(settings, "inbox_scheduler_epoch", str(uuid4())):
            self.assertFalse((await self._health())[1]["scheduler"]["epoch_matches"])
        with patch.object(settings, "inbox_scheduler_epoch", None):
            block = (await self._health())[1]["scheduler"]
            self.assertEqual((block["epoch_configured"], block["epoch_matches"]), (False, False))
        empty = await self._metrics()
        self.assertEqual((empty.nonterminal_turns, empty.ready_turns, empty.owned_lanes,
                          empty.outcome_unknown_attempts, empty.usage_reconciliation_backlog), (0, 0, 0, 0, 0))
        self.assertIsNone(empty.oldest_ready_age_seconds)
        self.assertFalse(block["attention_required"])

    async def test_nonterminal_turn_count_and_bytes(self) -> None:
        agent, instance = await self._agent("t085-bytes")
        await self._accept(instance, "5511000000001", "b-1", "abc")          # 3 bytes
        await self._accept(instance, "5511000000001", "b-2", "déf")          # 4 bytes
        await self._accept(instance, "5511000000002", "b-3", "xy")           # 2 bytes
        await self._accept(instance, "5511000000003", "b-4", "terminal")     # excluded below
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        await self._sql("UPDATE conversation_turns SET status='cancelled', session_slot='terminal', "
                        "terminal_reason='t085', terminal_at=UTC_TIMESTAMP(6) WHERE id=:t", t=turns[-1].id)
        metrics = await self._metrics()
        self.assertEqual((metrics.nonterminal_turns, metrics.nonterminal_bytes), (2, 9))
        self.assertEqual((metrics.cap_global_turns, metrics.cap_global_bytes), (10000, 268435456))
        block = (await self._health())[1]["scheduler"]["nonterminal_turns"]
        self.assertEqual(block, {"count": 2, "bytes": 9, "cap_turns": 10000, "cap_bytes": 268435456})

    async def test_ready_turns_and_oldest_age(self) -> None:
        _, _, turns = await self._turn_graph("ready", phones=4)
        await self._sql("UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6) - INTERVAL 30 SECOND WHERE id=:t",
                        t=turns[0].id)
        await self._sql("UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6) - INTERVAL 5 SECOND WHERE id=:t",
                        t=turns[1].id)
        await self._sql("UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6) + INTERVAL 1 HOUR WHERE id=:t",
                        t=turns[2].id)
        await self._sql("UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6) - INTERVAL 1 HOUR, status='retry', "
                        "retry_at=UTC_TIMESTAMP(6) + INTERVAL 1 HOUR WHERE id=:t", t=turns[3].id)
        metrics = await self._metrics()
        self.assertEqual(metrics.ready_turns, 2)
        self.assertGreaterEqual(metrics.oldest_ready_age_seconds, 30)
        self.assertLess(metrics.oldest_ready_age_seconds, 60)
        with patch.object(settings, "inbox_scheduler_epoch", str(uuid4())):
            # The DB scheduler epoch (not local config) defines readiness.
            self.assertEqual((await self._metrics()).ready_turns, 2)

    async def test_lane_lease_expired_and_quarantined(self) -> None:
        graphs = [await self._turn_graph(f"lane-{i}") for i in range(4)]
        await self._own_lane(graphs[0][2][0], expired=True)                       # stalled, recoverable
        token = await self._own_lane(graphs[1][2][0], expired=True)               # quarantined
        await self._attempt(graphs[1][2][0], outcome="outcome_unknown",
                            state="reconciliation_required", owner=token)
        drain = await self._own_lane(graphs[2][2][0], expired=True)               # inside accounting drain
        await self._attempt(graphs[2][2][0], outcome="started", owner=drain, drain=True)
        await self._own_lane(graphs[3][2][0], expired=False)                      # healthy owner
        metrics = await self._metrics()
        self.assertEqual((metrics.owned_lanes, metrics.lease_expired_lanes, metrics.quarantined_lanes), (4, 1, 2))
        status, body = await self._health()
        self.assertEqual(body["scheduler"]["lanes"], {"owned": 4, "lease_expired": 1, "quarantined": 2})
        self.assertTrue(body["scheduler"]["attention_required"])

    async def test_unknown_usage_attempts(self) -> None:
        _, _, turns_a = await self._turn_graph("usage-a", phones=2)
        _, _, turns_b = await self._turn_graph("usage-b")
        await self._attempt(turns_a[0], outcome="outcome_unknown", state="reconciliation_required")
        await self._attempt(turns_a[0], outcome="started")
        await self._attempt(turns_a[1], outcome="succeeded", state="reconciliation_required")
        await self._attempt(turns_a[1], outcome="failed", state="reconciliation_required", reconciled=True)
        await self._attempt(turns_b[0], outcome="succeeded", state="reconciliation_required")
        await self._attempt(turns_b[0], outcome="succeeded", state="recorded")
        metrics = await self._metrics()
        self.assertEqual((metrics.outcome_unknown_attempts, metrics.started_attempts), (1, 1))
        self.assertEqual((metrics.unreconciled_attempts, metrics.quarantined_agents), (3, 2))
        usage = (await self._health())[1]["scheduler"]["unknown_usage"]
        self.assertEqual(usage["outcome_unknown_attempts"], 1)
        self.assertEqual(usage["unreconciled_attempts"], 3)
        self.assertEqual(usage["quarantined_agents"], 2)

    async def test_usage_reconciliation_backlog_and_dead(self) -> None:
        agent, _ = await self._agent("t085-reconciliation")
        async with self.sessions.begin() as db:
            for status in ("pending", "pending", "processing", "dead", "resolved", "resolved"):
                db.add(LLMUsageReconciliation(agente_id=agent, modelo_llm=base.MODEL, tipo_chamada="resposta",
                                              safe_error_code="t085", status=status))
        metrics = await self._metrics()
        self.assertEqual((metrics.usage_reconciliation_backlog, metrics.usage_reconciliation_dead), (3, 1))
        block = (await self._health())[1]["scheduler"]
        self.assertEqual(block["unknown_usage"]["usage_reconciliation_dead"], 1)
        self.assertTrue(block["attention_required"])

    async def test_saturation_reported_not_exact(self) -> None:
        await self._turn_graph("saturated", phones=3)
        bounded = await self._metrics(bound=2)
        self.assertEqual(bounded.nonterminal_turns, 2)
        self.assertIn("nonterminal_turns", bounded.saturated)
        exact = await self._metrics(bound=3 + 1)
        self.assertEqual((exact.nonterminal_turns, exact.saturated), (3, ()))
        with patch.object(scheduler_metrics_repo.collect, "__kwdefaults__", {"bound": 2}):
            block = (await self._health())[1]["scheduler"]
        self.assertIn("nonterminal_turns", block["saturated"])
        self.assertTrue(block["attention_required"])

    async def _healthy_direct_runtime(self) -> None:
        # Direct runtime so that delivery readiness alone reports healthy.
        await self._sql("UPDATE delivery_runtime_state SET mode='direct' WHERE id=1")
        self.patches.enter_context(patch.object(settings, "delivery_mode", "direct"))
        # The global engine's SELECT 1 is bound to another test loop; DB reachability is
        # proven by the metrics queries themselves.
        self.patches.enter_context(patch.object(main, "check_db_connection", AsyncMock(return_value=True)))
        # T08-3: replica compatibility has its own real-MySQL suite; this suite
        # isolates the scheduler metrics block from the replica fleet.
        compatible = replica_compat.FleetReport(True, ())
        self.patches.enter_context(patch.object(
            replica_compat, "fleet_report_db", AsyncMock(return_value=compatible)))
        self.patches.enter_context(patch.object(
            replica_compat, "fleet_report", AsyncMock(return_value=compatible)))
        status, body = await self._health()
        self.assertEqual((status, body["status"], body["delivery_ready"]), (200, "healthy", True))
        self.assertTrue((await self._cli_health())["healthy"])

    async def test_metrics_unavailable_fail_closed(self) -> None:
        await self._healthy_direct_runtime()
        cases = 0
        for state in ("enabled", "draining", "enabling"):
            await self._state(state)
            obs.reset_health_transition_state()
            before = obs.metrics_unavailable_count()
            with patch.dict(scheduler_metrics_repo.QUERIES,
                            {"nonterminal_turns": "SELECT NULL, NULL FROM DUAL WHERE :bound > 0"}):
                with self.assertRaises(scheduler_metrics_repo.MetricsUnavailable) as caught:
                    await self._metrics()
                self.assertEqual(caught.exception.scheduler_state, state)
                status, body = await self._health()
            self.assertEqual((status, body["status"]), (503, "degraded"), state)
            self.assertEqual(set(body["scheduler"]), {"available", "healthy", "state", "reason",
                             "metrics_unavailable_count", "signal_threshold", "rejections"})
            self.assertEqual((body["scheduler"]["available"], body["scheduler"]["healthy"],
                              body["scheduler"]["state"]), (False, False, state))
            self.assertEqual(obs.metrics_unavailable_count(), before + 1)
            with patch.dict(scheduler_metrics_repo.QUERIES, {"lanes": "SELECT * FROM missing_t085_table"}):
                status, body = await self._health()
            self.assertEqual((status, body["scheduler"]["available"]), (503, False))
            obs.reset_health_transition_state()  # delivery_cli is a separate process in production.
            emitted = await self._cli_health(QUERIES={"lanes": "SELECT * FROM missing_t085_table"})
            self.assertFalse(emitted["healthy"])
            self.assertEqual((emitted["scheduler"]["available"], emitted["scheduler"]["healthy"]), (False, False))
            cases += 1
        self.assertEqual(cases, 3)

    async def test_metrics_unavailable_disabled_keeps_legacy_health(self) -> None:
        await self._healthy_direct_runtime()
        await self._state("disabled")
        sink: list[str] = []
        sink_id = obs.logger.add(lambda message: sink.append(str(message)), level="ERROR")
        cases = 0
        try:
            for broken in ({"lanes": "SELECT * FROM missing_t085_table"},
                           {"nonterminal_turns": "SELECT NULL, NULL FROM DUAL WHERE :bound > 0"},
                           # Unreadable state row: the DB check owns that failure, no second reason.
                           {"state": "SELECT NULL AS state, 1 AS version, NULL AS scheduler_epoch, "
                                     "1 AS max_global_queued_turns, 1 AS max_global_queued_bytes"}):
                before = obs.metrics_unavailable_count()
                expected_state = None if "state" in broken else "disabled"
                obs.reset_health_transition_state()
                with patch.dict(scheduler_metrics_repo.QUERIES, broken):
                    status, body = await self._health()
                self.assertEqual((status, body["status"]), (200, "healthy"), broken)
                block = body["scheduler"]
                self.assertEqual((block["available"], block["healthy"], block["state"]),
                                 (False, True, expected_state))
                self.assertNotIn("nonterminal_turns", block)
                self.assertEqual(obs.metrics_unavailable_count(), before + 1)
                obs.reset_health_transition_state()  # Simulate container-only CLI process.
                emitted = await self._cli_health(QUERIES=broken)
                self.assertTrue(emitted["healthy"], broken)
                self.assertEqual((emitted["scheduler"]["available"], emitted["scheduler"]["healthy"]),
                                 (False, True))
                self.assertEqual(obs.metrics_unavailable_count(), before + 2)
                cases += 1
        finally:
            obs.logger.remove(sink_id)
        signal = [line for line in sink if "Scheduler metrics unavailable" in line]
        self.assertEqual(len(signal), 6)
        self.assertTrue(all("threshold=1" in line and "blocks_health=False" in line for line in signal))
        self.assertEqual(cases, 3)

    async def test_public_health_singleflight_collects_once_per_ttl(self) -> None:
        """Floods coalesce; TTL expiry recollects and preserves fail-closed."""

        await self._healthy_direct_runtime()
        now = [100.0]
        scheduler_cache = AsyncSingleFlightTTL(clock=lambda: now[0])
        replica_cache = AsyncSingleFlightTTL(clock=lambda: now[0])
        original = scheduler_metrics_repo.collect
        scheduler_calls = 0
        replica_calls = 0
        compatible = replica_compat.FleetReport(True, ())

        async def counted(*args, **kwargs):
            nonlocal scheduler_calls
            scheduler_calls += 1
            await asyncio.sleep(0.05)
            return await original(*args, **kwargs)

        async def counted_replicas(*args, **kwargs):
            nonlocal replica_calls
            replica_calls += 1
            await asyncio.sleep(0.05)
            return compatible

        sink: list[str] = []
        sink_id = obs.logger.add(lambda message: sink.append(str(message)), level="ERROR")
        before = obs.metrics_unavailable_count()
        obs.reset_health_transition_state()
        try:
            with patch.object(main, "_scheduler_health_cache", scheduler_cache), \
                    patch.object(main, "_replica_health_cache", replica_cache), \
                    patch.object(settings, "health_observability_cache_seconds", 5.0), \
                    patch.object(scheduler_metrics_repo, "collect", counted), \
                    patch.object(replica_compat, "fleet_report", counted_replicas):
                results = await asyncio.gather(*(self.client.get("/internal/health") for _ in range(12)))
                self.assertTrue(all(item.json()["scheduler"]["available"] for item in results))
                self.assertEqual((scheduler_calls, replica_calls), (1, 1))

                now[0] += 4.999
                await self._health(reset_cache=False)
                self.assertEqual((scheduler_calls, replica_calls), (1, 1))
                now[0] += 0.002
                await self._health(reset_cache=False)
                self.assertEqual((scheduler_calls, replica_calls), (2, 2))

                now[0] += 5.001
                broken = {"lanes": "SELECT * FROM missing_sec_s4_scheduler_metrics"}
                with patch.dict(scheduler_metrics_repo.QUERIES, broken):
                    status, body = await self._health(reset_cache=False)
                    self.assertEqual((status, body["scheduler"]["healthy"]), (503, False))
                    # Cached unavailable stays fail-closed and does not re-log.
                    status, _ = await self._health(reset_cache=False)
                    self.assertEqual(status, 503)
                    now[0] += 5.001
                    status, _ = await self._health(reset_cache=False)
                    self.assertEqual(status, 503)
                self.assertEqual(obs.metrics_unavailable_count(), before + 1)

                # Recovery arms the next available→unavailable transition.
                now[0] += 5.001
                self.assertEqual((await self._health(reset_cache=False))[0], 200)
                now[0] += 5.001
                with patch.dict(scheduler_metrics_repo.QUERIES, broken):
                    self.assertEqual((await self._health(reset_cache=False))[0], 503)
        finally:
            obs.logger.remove(sink_id)
        self.assertEqual(obs.metrics_unavailable_count(), before + 2)
        transition_logs = [line for line in sink if "Scheduler metrics unavailable" in line]
        self.assertEqual(len(transition_logs), 2)

    async def test_health_cache_cancelled_waiter_preserves_flight(self) -> None:
        """R2-S4: cancelled initiator/follower, scheduler and replica stages."""

        await self._healthy_direct_runtime()
        original_scheduler = obs.scheduler_health
        cases = 0
        for stage in ("scheduler", "replica"):
            for cancelled_index in (0, 5):
                with self.subTest(stage=stage, cancelled_index=cancelled_index):
                    started, release = asyncio.Event(), asyncio.Event()
                    caches = (AsyncSingleFlightTTL(), AsyncSingleFlightTTL())
                    calls = {"scheduler": 0, "replica": 0}
                    loads_cancelled = []

                    async def scheduler(*args, **kwargs):
                        calls["scheduler"] += 1
                        result = await original_scheduler(*args, **kwargs)
                        if stage == "scheduler":
                            started.set()
                            try:
                                await release.wait()
                            except asyncio.CancelledError:
                                loads_cancelled.append(stage)
                                raise
                        return result

                    async def replica(*args, **kwargs):
                        calls["replica"] += 1
                        result = await REAL_FLEET_REPORT(*args, **kwargs)
                        if stage == "replica":
                            started.set()
                            try:
                                await release.wait()
                            except asyncio.CancelledError:
                                loads_cancelled.append(stage)
                                raise
                        return result

                    with patch.object(main, "_scheduler_health_cache", caches[0]), \
                            patch.object(main, "_replica_health_cache", caches[1]), \
                            patch.object(obs, "scheduler_health", scheduler), \
                            patch.object(replica_compat, "fleet_report", replica), \
                            patch.object(main, "provider_auth_readiness", AsyncMock(
                                return_value=SimpleNamespace(ready=True))):
                        waiters = [asyncio.create_task(self.client.get("/internal/health")) for _ in range(12)]
                        try:
                            await asyncio.wait_for(started.wait(), 2)
                            await asyncio.sleep(0.02)
                            flight = caches[0 if stage == "scheduler" else 1]._inflight
                            waiters[cancelled_index].cancel()
                            with self.assertRaises(asyncio.CancelledError):
                                await waiters[cancelled_index]
                            # New arrivals while the original flight is still running.
                            newcomers = [asyncio.create_task(self.client.get("/internal/health")) for _ in range(5)]
                            waiters.extend(newcomers)
                            await asyncio.sleep(0.05)
                            self.assertIs(caches[0 if stage == "scheduler" else 1]._inflight, flight)
                            self.assertEqual(loads_cancelled, [])
                            self.assertEqual(calls[stage], 1)
                            release.set()
                            remaining = [w for i, w in enumerate(waiters) if i != cancelled_index]
                            results = await asyncio.wait_for(asyncio.gather(*remaining), 3)
                            self.assertTrue(all(r.json()["scheduler"]["available"] for r in results))
                            self.assertEqual(calls, {"scheduler": 1, "replica": 1})
                        finally:
                            release.set()
                            for waiter in waiters:
                                if not waiter.done():
                                    waiter.cancel()
                            await asyncio.gather(*waiters, return_exceptions=True)
                            for cache in caches:
                                await cache.aclose()
                    cases += 1
        self.assertEqual(cases, 4)
        print("SEC2_S4_CANCEL cases=4 waiters=12 newcomers=5 loads_per_cache=1")

    async def test_health_cache_orphaned_load_publishes_with_completion_ttl(self) -> None:
        """All waiters disappear: completion still publishes with a fresh TTL."""

        now = [100.0]
        for reset_during_load in (False, True):
            with self.subTest(reset_during_load=reset_during_load):
                cache = AsyncSingleFlightTTL(clock=lambda: now[0])
                started, release = asyncio.Event(), asyncio.Event()
                calls = 0

                async def load():
                    nonlocal calls
                    calls += 1
                    result = await self._metrics()
                    started.set()
                    await release.wait()
                    return result

                waiters = [asyncio.create_task(cache.get(load, ttl_seconds=5)) for _ in range(6)]
                try:
                    await asyncio.wait_for(started.wait(), 2)
                    flight = cache._inflight
                    for waiter in waiters:
                        waiter.cancel()
                    await asyncio.gather(*waiters, return_exceptions=True)
                    self.assertIs(cache._inflight, flight)
                    if reset_during_load:
                        cache.reset()  # Reset is not process shutdown.
                    now[0] += 20  # TTL starts at completion, not task creation.
                    release.set()
                    while not flight.done():
                        await asyncio.sleep(0)
                    value = await cache.get(load, ttl_seconds=5)
                    self.assertEqual(value.state, "enabled")
                    self.assertEqual(calls, 1)
                    now[0] += 4.999
                    self.assertIs(await cache.get(load, ttl_seconds=5), value)
                    now[0] += 0.002
                    await cache.get(load, ttl_seconds=5)
                    self.assertEqual(calls, 2)
                    self.assertIsNone(cache._inflight)
                finally:
                    release.set()
                    await cache.aclose()
        print("SEC2_S4_ORPHAN cases=2 publication=task ttl=completion")

    async def test_health_cache_cancelled_waiter_caches_unavailable(self) -> None:
        """Real SQL failure after cancellation stays unavailable for the TTL."""

        await self._healthy_direct_runtime()
        now = [100.0]
        cache = AsyncSingleFlightTTL(clock=lambda: now[0])
        started, release = asyncio.Event(), asyncio.Event()
        original = scheduler_metrics_repo.collect
        calls = 0

        async def collect(*args, **kwargs):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return await original(*args, **kwargs)

        with patch.object(main, "_scheduler_health_cache", cache), \
                patch.object(scheduler_metrics_repo, "collect", collect):
            waiter = asyncio.create_task(self._health(reset_cache=False))
            try:
                await asyncio.wait_for(started.wait(), 2)
                waiter.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await waiter
                with patch.dict(scheduler_metrics_repo.QUERIES, {
                        "lanes": "SELECT * FROM missing_sec2_s4_metrics"}):
                    release.set()
                    status, body = await self._health(reset_cache=False)
                self.assertEqual((status, body["scheduler"]["available"]), (503, False))
                status, _ = await self._health(reset_cache=False)
                self.assertEqual(status, 503)  # DB fixed, failed snapshot still cached.
                self.assertEqual(calls, 1)
                now[0] += 5.001
                status, body = await self._health(reset_cache=False)
                self.assertEqual((status, body["scheduler"]["available"]), (200, True))
                self.assertEqual(calls, 2)
            finally:
                release.set()
                await cache.aclose()
        print("SEC2_S4_UNAVAILABLE sql_failure=503 cached=503 ttl_expired=200 loads=2")

    async def test_health_cache_unexpected_exception_cached_until_ttl(self) -> None:
        """Unexpected SQL exceptions never resurrect a healthy snapshot."""

        now = [100.0]
        cache = AsyncSingleFlightTTL(clock=lambda: now[0])
        calls = 0
        broken = False

        async def load():
            nonlocal calls
            calls += 1
            if broken:
                await self._sql("SELECT * FROM missing_sec2_s4_unexpected")
            return await self._metrics()

        try:
            self.assertEqual((await cache.get(load, ttl_seconds=5)).state, "enabled")
            now[0] += 5.001
            broken = True
            for _ in range(3):
                with self.assertRaises(DBAPIError):
                    await cache.get(load, ttl_seconds=5)
            self.assertEqual(calls, 2)
            broken = False
            with self.assertRaises(DBAPIError):
                await cache.get(load, ttl_seconds=5)
            now[0] += 5.001
            self.assertEqual((await cache.get(load, ttl_seconds=5)).state, "enabled")
            self.assertEqual(calls, 3)
        finally:
            await cache.aclose()
        print("SEC2_S4_EXCEPTION cached_error_reads=4 total_loads=3")

    async def test_health_cache_lifespan_shutdown_cancels_loads(self) -> None:
        """Actual app lifespan owns cancellation of both shielded loads."""

        caches = (AsyncSingleFlightTTL(), AsyncSingleFlightTTL())
        started = [asyncio.Event(), asyncio.Event()]
        cleaned = [asyncio.Event(), asyncio.Event()]
        release = asyncio.Event()
        tasks = []
        heartbeat = SimpleNamespace(start=lambda: None, stop=AsyncMock())

        async def load(index):
            try:
                await self._metrics()  # Real MySQL read, no provider.
                started[index].set()
                await release.wait()
            finally:
                cleaned[index].set()

        with patch.object(main, "_scheduler_health_cache", caches[0]), \
                patch.object(main, "_replica_health_cache", caches[1]), \
                patch.object(replica_compat, "HeartbeatLoop", return_value=heartbeat):
            lifecycle = main.lifespan(main.app)
            await lifecycle.__aenter__()
            try:
                tasks = [asyncio.create_task(cache.get(lambda i=i: load(i), ttl_seconds=5))
                         for i, cache in enumerate(caches)]
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), 2)
                began = asyncio.get_running_loop().time()
                await asyncio.wait_for(lifecycle.__aexit__(None, None, None), 1)
                elapsed = asyncio.get_running_loop().time() - began
                self.assertTrue(all(event.is_set() for event in cleaned))
                self.assertTrue(all(cache._inflight is None for cache in caches))
                results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 1)
                self.assertTrue(all(isinstance(result, asyncio.CancelledError) for result in results))
                heartbeat.stop.assert_awaited_once()
                for cache in caches:
                    with self.assertRaises(asyncio.CancelledError):
                        await cache.get(lambda: load(0), ttl_seconds=5)
                self.assertLess(elapsed, 1)
                print(f"SEC2_S4_SHUTDOWN loads=2 cancelled=2 grace=1 elapsed={elapsed:.3f}")
            finally:
                release.set()
                for cache in caches:
                    await cache.aclose()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_fu4_public_health_minimal_and_status_parity(self) -> None:
        """FU-4: real DB state + each degraded readiness input, no public details."""

        await self._healthy_direct_runtime()
        cases = 0
        for state in ("disabled", "enabling", "enabled", "draining"):
            await self._state(state)
            for fault in ("none", "metrics", "replicas", "database", "delivery"):
                with self.subTest(state=state, fault=fault):
                    main.reset_health_observability_caches()
                    report = replica_compat.FleetReport(
                        fault != "replicas", ("code_version_mismatch",) if fault == "replicas" else (),
                        scheduler_state=state)
                    queries = ({"lanes": "SELECT * FROM missing_fu4_metrics"}
                               if fault == "metrics" else {})
                    with patch.dict(scheduler_metrics_repo.QUERIES, queries), \
                            patch.object(replica_compat, "fleet_report", AsyncMock(return_value=report)), \
                            patch.object(main, "check_db_connection", AsyncMock(return_value=fault != "database")), \
                            patch.object(main, "provider_auth_readiness", AsyncMock(
                                return_value=SimpleNamespace(ready=True))):
                        # Failure after aggregates must also be redacted.
                        from contextlib import nullcontext
                        failing = (patch.object(delivery_state_repo, "get_runtime_state", AsyncMock(
                            side_effect=RuntimeError("fu4-private-readiness")))
                            if fault == "delivery" else nullcontext())
                        with failing:
                            details = await self.client.get("/internal/health")
                            public = await self.client.get("/health")
                    self.assertEqual(set(public.json()), {"status"})
                    expected = 503 if (fault in {"database", "delivery"}
                        or (state != "disabled" and fault in {"metrics", "replicas"})) else 200
                    self.assertEqual(details.status_code, expected)
                    self.assertEqual(public.status_code, details.status_code)
                    self.assertEqual(public.json(), {"status": "healthy" if expected == 200 else "unhealthy"})
                    self.assertTrue({"status", "database", "lifecycle", "delivery_ready", "scheduler"}
                                    <= set(details.json()))
                    self.assertEqual(details.json()["status"], "healthy" if expected == 200 else "degraded")
                    for secret in (PHONE, CONTENT, BEARER, self.scheduler_epoch, "fu4-private-readiness"):
                        self.assertNotIn(secret, public.text)
                    cases += 1
        self.assertEqual(cases, 20)
        print("FU4_PUBLIC_PARITY states=4 faults=5 cases=20 keys=status http=200/503")

    async def test_fu4_root_status_only(self) -> None:
        """FU-4: GET / exposes liveness only; no service name or version."""

        response = await self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "online"})
        for leaked in ("service", "version", "MVP", "0.1.0"):
            self.assertNotIn(leaked, response.text)
        print("FU4_ROOT_MINIMAL keys=status")

    async def test_fu4_public_and_internal_share_singleflight(self) -> None:
        await self._healthy_direct_runtime()
        main.reset_health_observability_caches()
        original = scheduler_metrics_repo.collect
        calls = 0

        async def collect(*args, **kwargs):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.03)
            return await original(*args, **kwargs)

        with patch.object(scheduler_metrics_repo, "collect", collect), \
                patch.object(main, "provider_auth_readiness", AsyncMock(
                    return_value=SimpleNamespace(ready=True))):
            results = await asyncio.gather(*(self.client.get(
                "/health" if i % 2 else "/internal/health") for i in range(12)))
        self.assertEqual(calls, 1)
        self.assertTrue(all(r.status_code == 200 for r in results))
        for i, result in enumerate(results):
            if i % 2:
                self.assertEqual(result.json(), {"status": "healthy"})
            else:
                self.assertTrue(result.json()["scheduler"]["available"])
        print("FU4_MIXED_SINGLEFLIGHT requests=12 loads=1")

    async def test_replica_and_public_health_log_only_on_transition(self) -> None:
        """S4: N reads in one unhealthy episode → one counter step and one ERROR."""

        await self._healthy_direct_runtime()
        sink: list[str] = []
        sink_id = obs.logger.add(lambda message: sink.append(str(message)), level="ERROR")
        counts = replica_compat.counts()
        unhealthy = main.health_unhealthy_transitions()
        try:
            with patch.object(replica_compat, "fleet_report", REAL_FLEET_REPORT), \
                    patch.object(replica_compat, "fleet_report_db", REAL_FLEET_REPORT_DB):
                for reasons in ((("code_version_mismatch",),) * 3 + ((),)
                                + (("code_version_mismatch",),) * 2):
                    with patch.object(replica_compat.replica_repo, "readiness_reasons",
                                      return_value=reasons):
                        await self._health()
                with patch.object(replica_compat.replica_repo, "load_fleet",
                                  AsyncMock(side_effect=RuntimeError("sec-s4"))):
                    for _ in range(3):
                        _, body = await self._health()
                self.assertEqual(body["replica_compatibility"]["reasons"],
                                 ["replica_state_unavailable"])
            for db_ok in (False, False, False, True, False, False):
                with patch.object(main, "check_db_connection", AsyncMock(return_value=db_ok)):
                    status, _ = await self._health()
                self.assertEqual(status, 200 if db_ok else 503)
        finally:
            obs.logger.remove(sink_id)
        after = replica_compat.counts()
        self.assertEqual(after["incompatible_reports"], counts["incompatible_reports"] + 2)
        self.assertEqual(after["report_unavailable"], counts["report_unavailable"] + 1)
        # Active DB scheduler: incompatible replicas block too (two episodes),
        # the DB outage continues the second one and the recovery arms a third.
        self.assertEqual(main.health_unhealthy_transitions(), unhealthy + 3)
        self.assertEqual(sum("Replica fleet incompatible" in line for line in sink), 2)
        self.assertEqual(sum("Replica compatibility unavailable" in line for line in sink), 1)
        self.assertEqual(sum("Public health unhealthy" in line for line in sink), 3)
        self.assertTrue(all("threshold=1" in line for line in sink if "health" in line.lower()))

    async def test_exception_logs_never_render_locals(self) -> None:
        """S4/P3-8: diagnose/backtrace off on every handler of every entrypoint."""

        secret = "5511900000085"
        stdout, stderr = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory(prefix="sec-s4-") as tmp:
            log_file = Path(tmp) / "app.log"
            with patch("sys.stdout", stdout), patch("sys.stderr", stderr):
                app_logger.setup_logger(str(log_file))

                def explode(lead_phone: str) -> None:
                    hidden_token = lead_phone + "-token"
                    raise ValueError("sec-s4 failure " + str(len(hidden_token)))

                try:
                    explode(secret)
                except ValueError:
                    app_logger.logger.exception("sec-s4 exception")
            app_logger.logger.remove()
            written = log_file.read_text()
        app_logger.setup_logger()
        for output in (stderr.getvalue(), written):
            self.assertIn("ValueError: sec-s4 failure", output)
            self.assertNotIn(secret, output)  # diagnose=True would print the local's value
        self.assertEqual(stdout.getvalue(), "")  # CLI JSON stays alone on stdout

        for relative, scope in LOG_ENTRYPOINTS.items():
            tree = ast.parse((REPO_ROOT / relative).read_text())
            if scope is None:
                nodes = [n for n in tree.body if isinstance(n, ast.Expr)]
            elif scope == "__main__":
                nodes = [n for n in tree.body if isinstance(n, ast.If)
                         and "__main__" in ast.unparse(n.test)]
            else:
                nodes = [n for n in tree.body
                         if isinstance(n, ast.FunctionDef) and n.name == scope]
            calls = [c for n in nodes for c in ast.walk(n) if isinstance(c, ast.Call)
                     and ast.unparse(c.func) == "setup_logger"]
            self.assertTrue(calls, relative)

    async def test_queries_never_lock_and_explain_indexed(self) -> None:
        for name, statement in scheduler_metrics_repo.QUERIES.items():
            self.assertNotRegex(statement.upper(), r"FOR (SHARE|UPDATE)|LOCK IN SHARE", name)
        _, _, turns = await self._turn_graph("locks", phones=2)
        token = await self._own_lane(turns[0], expired=True)
        await self._attempt(turns[0], outcome="outcome_unknown", state="reconciliation_required", owner=token)
        original = scheduler_metrics_repo._row
        observed: list[tuple[str, int, int]] = []

        async def observing(db, name, **params):
            row = await original(db, name, **params)
            locks = (await db.execute(text(
                "SELECT COUNT(*) FROM performance_schema.data_locks l JOIN performance_schema.threads t "
                "ON t.THREAD_ID=l.THREAD_ID WHERE t.PROCESSLIST_ID=CONNECTION_ID()"))).scalar_one()
            # A write probe proves the transaction itself is READ ONLY (1792).
            try:
                async with db.begin_nested():
                    await db.execute(text("UPDATE scheduler_runtime_state SET version=version WHERE id=0"))
                read_only = 0
            except DBAPIError as exc:
                read_only = int(exc.orig.args[0] == 1792)
            observed.append((name, int(locks), int(read_only)))
            return row

        # Writers hold X locks on every row the metrics read; plain reads never wait.
        async with self.engine.connect() as holder:
            transaction = await holder.begin()
            await holder.execute(text("SELECT id FROM conversation_turns FOR UPDATE"))
            await holder.execute(text("SELECT agente_id FROM agent_service_lanes FOR UPDATE"))
            await holder.execute(text("SELECT id FROM generation_attempts FOR UPDATE"))
            await holder.execute(text("SELECT id FROM scheduler_runtime_state FOR UPDATE"))
            try:
                with patch.object(scheduler_metrics_repo, "_row", observing):
                    metrics = await self._metrics()
            finally:
                await transaction.rollback()
        self.assertEqual(metrics.nonterminal_turns, 2)
        self.assertEqual(len(observed), len(scheduler_metrics_repo.QUERIES))
        self.assertEqual({(name, locks) for name, locks, _ in observed},
                         {(name, 0) for name in scheduler_metrics_repo.QUERIES})
        self.assertTrue(all(read_only == 1 for *_, read_only in observed), observed)
        # EXPLAIN evidence on a representative population (terminal history
        # dominates): logged for the matrix, asserted for the hot tables.
        async with self.sessions.begin() as db:
            await db.execute(text("SET SESSION cte_max_recursion_depth=5000"))
            await db.execute(text(
                "INSERT INTO conversation_turns (turn_key, agente_id, sessao_id, scheduler_epoch, "
                "arrival_sequence, scheduling_sequence, status, session_slot, ready_at, "
                "terminal_reason, terminal_at) "
                "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2000) "
                "SELECT UUID(), :a, :s, :e, 1000 + n, 1000 + n, 'delivered', 'terminal', "
                "UTC_TIMESTAMP(6), 't085-load', UTC_TIMESTAMP(6) FROM seq"),
                {"a": turns[0].agente_id, "s": turns[0].sessao_id, "e": self.scheduler_epoch})
            await db.execute(text(
                "INSERT INTO llm_usage_reconciliation (reconciliation_key, agente_id, modelo_llm, "
                "tipo_chamada, safe_error_code, status) "
                "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2000) "
                "SELECT UUID(), :a, 'openai/gpt-4o-mini', 'resposta', 't085', 'resolved' FROM seq"),
                {"a": turns[0].agente_id})
        self.assertEqual((await self._metrics()).nonterminal_turns, 2)
        async with self.sessions() as db:
            await db.execute(text("ANALYZE TABLE conversation_turns, generation_attempts, "
                                  "agent_service_lanes, llm_usage_reconciliation, turn_input_memberships"))
            plans: dict[str, list[dict]] = {}
            for name, statement in scheduler_metrics_repo.QUERIES.items():
                rows = (await db.execute(text("EXPLAIN " + statement),
                                         {"bound": 100000, "scheduler_epoch": self.scheduler_epoch})).mappings()
                plans[name] = [dict(r) for r in rows]
                print(f"T085_EXPLAIN {name} " + json.dumps(
                    [{k: r.get(k) for k in ("table", "type", "key", "rows", "Extra")} for r in plans[name]],
                    default=str))
        def access(name: str, table: str) -> set[tuple]:
            return {(r["type"], r["key"]) for r in plans[name] if r["table"] in (table, table[0])}
        self.assertIn(("range", "idx_turn_claim_lease"), access("nonterminal_turns", "t"))
        self.assertIn(("range", "idx_turn_claim_lease"), access("ready_turns", "t"))
        self.assertIn(("range", "idx_usage_reconciliation_due"), access("usage_reconciliation", "u"))
        self.assertIn(("ref", "idx_turn_input_range"), access("nonterminal_turns", "m"))
        self.assertTrue(any(r["key"] == "idx_generation_agent_unknown" for r in plans["attempt_outcomes"]))
        self.assertTrue(any(r["key"] == "idx_generation_agent_reconciliation"
                            for r in plans["unreconciled_attempts"]))
        self.assertTrue(any(r["key"] == "idx_usage_reconciliation_due" for r in plans["usage_reconciliation"]))
        for name in ("nonterminal_turns", "ready_turns", "attempt_outcomes", "unreconciled_attempts",
                     "usage_reconciliation"):
            hot = [r for r in plans[name] if r["table"] in ("t", "g", "u", "m")]
            self.assertFalse([r for r in hot if r["type"] == "ALL"], (name, hot))

    async def _cli_health(self, QUERIES: dict | None = None) -> dict:
        emitted: list[dict] = []
        worker_id = str(uuid4())
        async with self.sessions.begin() as db:
            await delivery_state_repo.record_heartbeat(db, worker_id=worker_id, code_version="t085",
                                                       current_lease_count=0)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "health.json"
            path.write_text(json.dumps({"worker_id": worker_id, "ready": True,
                                        "provider_ready": True, "runtime_compatible": True}))
            with patch.object(delivery_cli.settings, "delivery_worker_health_path", str(path)), \
                    patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                    patch.object(delivery_cli, "_emit", emitted.append), \
                    patch.dict(scheduler_metrics_repo.QUERIES, QUERIES or {}):
                await delivery_cli._health()
        return emitted[0]

    async def test_health_and_cli_payloads_have_no_pii(self) -> None:
        agent, instance = await self._agent("t085-pii")
        await self._accept(instance, PHONE, "pii-provider-id-t085", CONTENT)
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        token = await self._own_lane(turn, expired=True)
        await self._attempt(turn, outcome="outcome_unknown", state="reconciliation_required", owner=token)
        await self._state("draining")
        await self._post(instance, "pii-refused-t085")
        receipt = (await self._rows(InboundEvent, InboundEvent.id))[0]
        forbidden = [PHONE, CONTENT, instance, "pii-provider-id-t085", "pii-refused-t085", BEARER,
                     self.delivery_epoch, turn.turn_key, token, receipt.payload_hmac.hex()
                     if isinstance(receipt.payload_hmac, bytes) else str(receipt.payload_hmac)]
        _, body = await self._health()
        dumped = json.dumps(body, default=str)
        for value in forbidden + [self.scheduler_epoch]:
            self.assertNotIn(value, dumped)
        self.assertNotRegex(dumped, r"\d{10,}")
        emitted = await self._cli_health()
        dumped = json.dumps(emitted["scheduler"], default=str)
        for value in forbidden:
            self.assertNotIn(value, dumped)
        # Container-only CLI carries the scheduler epoch by design, only there.
        self.assertEqual(emitted["scheduler"]["epoch"], self.scheduler_epoch)
        self.assertEqual(dumped.count(self.scheduler_epoch), 1)
        allowed = set(obs.REJECTION_CODES) | set(obs.CAP_KINDS.values()) | {obs.CAP_KIND_UNKNOWN}
        self.assertTrue(set(body["scheduler"]["rejections"]["by_code"]) <= allowed)

    # ── 503 codes ───────────────────────────────────────────────────────
    async def test_503_state_codes(self) -> None:
        _, instance = await self._agent("t085-state")
        cases = 0
        for state, code in (("draining", "scheduler_draining"), ("disabled", "scheduler_not_enabled"),
                            ("enabling", "scheduler_not_enabled")):
            await self._state(state)
            before = scheduler_repo.intake_state_refusal_count()
            await self._expect_code(code, lambda: self._post(instance, f"state-{state}"))
            self.assertEqual(scheduler_repo.intake_state_refusal_count(), before + 1)
            cases += 1
        await self._state("enabled")
        self.assertEqual((await self._post(instance, "state-enabled")).status_code, 202)
        self.assertEqual(cases, 3)

    async def test_503_cap_codes_by_kind(self) -> None:
        cases = 0
        for column, kind, second_phone in (
            ("max_pending_inputs_per_session", "session_inputs", None),
            ("max_pending_input_bytes_per_session", "session_bytes", None),
            ("max_queued_turns_per_agent", "agent_turns", "5511000000777"),
            ("max_queued_bytes_per_agent", "agent_bytes", "5511000000777"),
            ("max_global_queued_turns", "global_turns", "other-agent"),
            ("max_global_queued_bytes", "global_bytes", "other-agent"),
        ):
            await self._wipe()
            await self._sql(f"UPDATE scheduler_runtime_state SET {column}=1 WHERE id=1")
            if column.endswith("bytes_per_session") or column.endswith("bytes_per_agent") \
                    or column == "max_global_queued_bytes":
                await self._sql(f"UPDATE scheduler_runtime_state SET {column}=4 WHERE id=1")
            _, instance = await self._agent(f"t085-cap-{kind}")
            self.assertEqual((await self._post(instance, f"{kind}-1", content="abc")).status_code, 202)
            if second_phone == "other-agent":
                _, target = await self._agent(f"t085-cap2-{kind}")
                call = lambda: self._post(target, f"{kind}-2", content="ab")
            else:
                call = lambda: self._post(instance, f"{kind}-2", phone=second_phone or PHONE, content="ab")
            await self._expect_code("cap_exceeded", call, cap=kind)
            await self._sql(
                "UPDATE scheduler_runtime_state SET max_pending_inputs_per_session=128, "
                "max_pending_input_bytes_per_session=262144, max_queued_turns_per_agent=1000, "
                "max_queued_bytes_per_agent=16777216, max_global_queued_turns=10000, "
                "max_global_queued_bytes=268435456 WHERE id=1")
            cases += 1
        self.assertEqual(cases, 6)

    async def test_503_epoch_codes(self) -> None:
        agent, instance = await self._agent("t085-epoch")
        cases = 0
        for name in ("inbox_scheduler_epoch", "delivery_cutover_epoch"):
            with patch.object(settings, name, str(uuid4())):
                await self._expect_code("epoch_mismatch", lambda: self._post(instance, f"epoch-{name}"))
            cases += 1
        self.assertEqual((await self._post(instance, "epoch-seed")).status_code, 202)
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        await self._own_lane(turn, expired=False)
        await self._sql("UPDATE agent_service_lanes SET scheduler_epoch=:e WHERE agente_id=:a",
                        e=str(uuid4()), a=agent)
        await self._expect_code("epoch_mismatch", lambda: self._post(instance, "epoch-lane", phone="5511000000999"))
        cases += 1
        legacy, legacy_instance = await self._agent("t085-epoch-legacy")
        await self._sql("UPDATE agentes SET inbox_scheduler_enabled=FALSE WHERE id=:a", a=legacy)
        with patch.object(settings, "delivery_cutover_epoch", str(uuid4())):
            await self._expect_code("epoch_mismatch", lambda: self._post(legacy_instance, "epoch-legacy"))
        cases += 1
        self.assertEqual(cases, 4)

    async def test_503_runtime_codes(self) -> None:
        _, instance = await self._agent("t085-runtime")
        legacy, legacy_instance = await self._agent("t085-runtime-legacy")
        await self._sql("UPDATE agentes SET inbox_scheduler_enabled=FALSE WHERE id=:a", a=legacy)
        await self._sql("UPDATE delivery_runtime_state SET mode='frozen' WHERE id=1")
        await self._expect_code("runtime_mismatch", lambda: self._post(instance, "runtime-scheduled"))
        await self._expect_code("runtime_frozen", lambda: self._post(legacy_instance, "runtime-frozen"))
        await self._sql("UPDATE delivery_runtime_state SET mode='outbox' WHERE id=1")
        with patch.object(settings, "delivery_mode", "direct"):
            await self._expect_code("runtime_mismatch", lambda: self._post(legacy_instance, "runtime-mode"))

    async def test_503_gate_codes(self) -> None:
        agent, instance = await self._agent("t085-gate")
        with patch.object(settings, "inbox_scheduler_enabled", False):
            await self._expect_code("feature_disabled", lambda: self._post(instance, "gate-feature"))
        with patch.object(settings, "provider_webhook_auth_mode", "provisioning"):
            await self._expect_code("auth_not_enforced", lambda: self._post(instance, "gate-auth"))
        with patch.object(settings, "evolution_webhook_bearer_token", None):
            await self._expect_code("auth_unavailable", lambda: self._post(instance, "gate-bearer"),
                                    retry_after=None)
        original = evolution_orchestrator.agente_repo.get_by_instance_name
        for opt_in, code in ((True, "gate_changed"), (False, "gate_changed")):
            seen = 0

            async def stale_preliminary(db, name, **kwargs):
                nonlocal seen
                seen += 1
                if seen == 1:
                    return SimpleNamespace(id=agent, inbox_scheduler_enabled=opt_in)
                return await original(db, name, **kwargs)

            if opt_in:   # scheduled path sees the agent deactivated after the gate read
                await self._sql("UPDATE agentes SET ativo=FALSE WHERE id=:a", a=agent)
            else:        # legacy path sees the opt-in appear after the gate read
                await self._sql("UPDATE agentes SET ativo=TRUE WHERE id=:a", a=agent)
            with patch.object(evolution_orchestrator.agente_repo, "get_by_instance_name", stale_preliminary):
                await self._expect_code(code, lambda: self._post(instance, f"gate-changed-{opt_in}"))

    async def test_503_internal_codes(self) -> None:
        _, instance = await self._agent("t085-internal")
        deadlock = OperationalError("SELECT 1", {}, Exception(1213, "Deadlock found"))
        cases = 0
        for error, code in (
            (evolution_orchestrator.ReceiptConfigurationError("x"), "receipt_config_unavailable"),
            (consumo_repo.UsagePersistenceInvariantError("x"), "usage_invariant"),
            (deadlock, "transaction_lost"),
            (scheduler_repo.SchedulerDeadlockRetryExhausted("x"), "transaction_lost"),
            (RuntimeError(PHONE), "internal_error"),
        ):
            with patch.object(evolution_orchestrator, "orchestrate", AsyncMock(side_effect=error)):
                await self._expect_code(code, lambda: self._post(instance, f"internal-{code}"))
            cases += 1
        for error in (scheduler_repo.SchedulerConflictError(PHONE),
                      scheduler_repo.IncompleteIntakeMembershipError("historical receipt disposition requires audited repair")):
            with patch.object(evolution_orchestrator.scheduler_repo, "lock_intake_turn_graph",
                              AsyncMock(side_effect=error)):
                await self._expect_code("conflict", lambda: self._post(instance, f"internal-conflict-{cases}"))
            cases += 1
        with patch.object(evolution_orchestrator.scheduler_repo, "lock_intake_turn_graph",
                          AsyncMock(side_effect=scheduler_repo.SchedulerNotReadyError(CONTENT))):
            await self._expect_code("scheduler_not_ready", lambda: self._post(instance, "internal-notready"))
        cases += 1
        self.assertEqual(cases, 8)

    async def test_503_bodies_have_no_pii(self) -> None:
        _, instance = await self._agent("t085-body")
        await self._state("draining")
        sink: list[str] = []
        sink_id = obs.logger.add(lambda message: sink.append(str(message)))
        try:
            responses = [await self._post(instance, "pii-body-1")]
            with patch.object(evolution_orchestrator, "orchestrate",
                              AsyncMock(side_effect=RuntimeError(f"{PHONE} {CONTENT}"))):
                responses.append(await self._post(instance, "pii-body-2"))
        finally:
            obs.logger.remove(sink_id)
        for response in responses:
            self.assertEqual(set(response.json()), {"detail", "code"})
            self.assertIn(response.json()["code"], obs.REJECTION_CODES)
            for value in (PHONE, CONTENT, instance, "pii-body-1", "pii-body-2", BEARER,
                          self.scheduler_epoch, self.delivery_epoch):
                self.assertNotIn(value, response.text)
        signal = [line for line in sink if "Evolution webhook unavailable" in line]
        self.assertEqual(len(signal), 2)
        for line in signal:
            self.assertNotIn(PHONE, line)
            self.assertNotIn(CONTENT, line)
            self.assertIn("threshold=1", line)

    async def test_contract_parity(self) -> None:
        scheduler_src = (REPO_ROOT / "app/src/database/repositories/scheduler_repo.py").read_text()
        orchestrator_src = (REPO_ROOT / "app/src/services/evolution_orchestrator.py").read_text()
        for message in list(obs.CAP_KINDS) + list(obs.SCHEDULER_MESSAGE_CODES):
            self.assertIn(f'"{message}"', scheduler_src, message)
        for message in obs.ORCHESTRATOR_MESSAGE_CODES:
            self.assertIn(f'"{message}"', orchestrator_src, message)
        caps_src = inspect.getsource(scheduler_repo.assert_intake_caps)
        raised_caps = set(re.findall(r'SchedulerCapExceeded\("([^"]+)"\)', caps_src))
        self.assertEqual(raised_caps, set(obs.CAP_KINDS))
        mapped = set(obs.CAP_KINDS.values()) | set(obs.SCHEDULER_MESSAGE_CODES.values()) | set(
            obs.ORCHESTRATOR_MESSAGE_CODES.values()) | set(obs.STATE_REFUSAL_CODES.values()) | {obs.STATE_REFUSAL_DEFAULT}
        self.assertTrue((mapped - set(obs.CAP_KINDS.values())) <= obs.REJECTION_CODES)
        doc = (REPO_ROOT / "AGENTS.md").read_text()
        section = doc[doc.index("#### T08-5"):]
        for code in obs.REJECTION_CODES:
            self.assertIn(f"`{code}`", section, code)
        for kind in obs.CAP_KINDS.values():
            self.assertIn(f"`{kind}`", section, kind)
        for name in scheduler_metrics_repo.QUERIES:
            self.assertIn(f"`{name}`", section, name)


for _name in dir(base.HandoffTerminalMySQLTests):
    if _name.startswith("test_") and _name not in SchedulerMetricsMySQLTests.__dict__:
        setattr(SchedulerMetricsMySQLTests, _name, None)
