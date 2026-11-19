"""T08-1: real MySQL state/drain barriers, fake providers only."""
import asyncio
from contextlib import ExitStack
from datetime import datetime, timedelta
import json
from pathlib import Path
import os
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from types import SimpleNamespace

from fastapi import HTTPException
from sqlalchemy import event, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import DBAPIError

from src import delivery_cli
from src.config import settings
from src.database.models import (
    AgentServiceLane, Agente, ConversationTurn, DeliveryOperatorAction,
    GenerationAttempt, InboundEvent, Mensagem, OutboxDelivery, ScheduledHandoff,
    SchedulerRuntimeState, TurnInputMembership, TurnRevision,
)
from src import main as main_module
from src.database.repositories import (
    delivery_state_repo, delivery_worker_repo, inbound_event_repo, lifecycle_repo,
    scheduler_repo, turn_delivery_repo, turn_generation_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services import evolution_orchestrator
from tests import replica_fixtures
from tests import test_turn_delivery_mysql as base


class SchedulerDrainMySQLTests(base.TurnDeliveryMySQLTests):
    async def _state(self, state: str, *, version: int = 10) -> None:
        async with self.sessions.begin() as db:
            await db.execute(text("UPDATE scheduler_runtime_state SET state=:state, version=:v WHERE id=1"),
                             {"state": state, "v": version})

    async def _transition(self, target: str, version: int = 10, *, epoch=None):
        # T08-3: enabling -> enabled also needs a compatible fleet + attestation.
        attestation = await replica_fixtures.ready_fleet(
            self.sessions, delivery_epoch=self.delivery_epoch,
            scheduler_epoch=epoch or self.scheduler_epoch) if target == "enabled" else None
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions, lambda db: scheduler_repo.transition_scheduler_state(
                db, target=target, scheduler_epoch=epoch or self.scheduler_epoch,
                expected_version=version, actor="t081", reason="synthetic state change",
                replica_attestation=attestation))

    async def _seed(self):
        agent, instance = await self._agent("drain")
        phone = "5511000000081"
        await self._accept(instance, phone, "seed", "synthetic input")
        return agent, instance, phone, await self._one(ConversationTurn), await self._one(TurnRevision)

    async def _terminalize_fixture(self) -> None:
        async with self.sessions.begin() as db:
            await db.execute(text("UPDATE conversation_turns SET status='cancelled', session_slot='terminal', "
                                  "terminal_reason='fixture', terminal_at=UTC_TIMESTAMP(6) WHERE session_slot<>'terminal'"))

    async def _attempt(self, turn, revision, *, outcome="started", reconciliation=False):
        async with self.sessions.begin() as db:
            row = GenerationAttempt(
                agente_id=turn.agente_id, sessao_id=turn.sessao_id, turn_id=turn.id,
                revision_id=revision.id, scheduler_epoch=self.scheduler_epoch,
                lane_owner_token=str(uuid4()), attempt_type="response", attempt_ordinal=1,
                modelo_llm="openai/gpt-4o-mini", outcome=outcome,
                accounting_state="reconciliation_required" if reconciliation else "pending",
                outcome_recorded_at=None if outcome == "started" else datetime.utcnow())
            db.add(row)
            await db.flush()
            self.last_attempt = row
            return row.id

    async def _blocked(self, category: str):
        before = scheduler_repo.drain_refusal_count()
        with self.assertRaises(scheduler_repo.SchedulerDrainBlocked) as caught:
            await self._transition("disabled")
        self.assertGreater(caught.exception.counts[category], 0)
        self.assertIn(category, str(caught.exception))
        self.assertEqual(scheduler_repo.drain_refusal_count(), before + 1)
        self.assertEqual(await self._count(DeliveryOperatorAction), 0)
        # S2: exact category parity against the locking guard on every fixture.
        from src.database.repositories.scheduler_diagnostics_repo import scheduler_snapshot
        async with self.sessions() as db:
            diagnostic = await scheduler_snapshot(db)
        self.assertEqual(diagnostic["clearance"], caught.exception.counts)
        self.assertEqual(diagnostic["saturated"], [])
        return caught.exception.counts

    async def _lock_wait(self) -> None:
        # No sleeps as a synchronization premise: observe an actual InnoDB wait.
        async def waiting():
            async with self.sessions() as db:
                return bool((await db.execute(text("SELECT COUNT(*) FROM performance_schema.data_lock_waits"))).scalar_one())
        await self._until(waiting, "real InnoDB lock wait")

    async def _diagnostic_barrier(self, command: list[str], scan: str) -> None:
        # S2: hold the actual CLI transaction mid-scan, not a mocked repository.
        _, instance = await self._agent("sec-s2-other-agent")
        for n in range(3):
            await self._state("enabled")
            entered, release = asyncio.Event(), asyncio.Event()
            connection_ids: list[int] = []
            original = AsyncSession.execute

            async def execute(db, statement, *args, **kwargs):
                mine = asyncio.current_task().get_name() == "sec-s2-show"
                target = mine and scan in str(statement) and not entered.is_set()
                if target:
                    connection_ids.append((await original(db, text("SELECT CONNECTION_ID()"))).scalar_one())
                result = await original(db, statement, *args, **kwargs)
                if target:
                    entered.set()
                    await release.wait()
                return result

            with patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                 patch("src.agent_scheduler_cli.provider_auth_enforced", AsyncMock(return_value=(True, []))), \
                 patch.object(delivery_cli, "_emit") as emit, \
                 patch.object(AsyncSession, "execute", execute):
                show = asyncio.create_task(delivery_cli._run(delivery_cli._parser().parse_args(command)),
                                           name="sec-s2-show")
                intake = None
                try:
                    await asyncio.wait_for(entered.wait(), 10)
                    intake = asyncio.create_task(self._accept(instance, f"5511000090{n:03}",
                                                             f"sec-s2-{n}", "synthetic"))
                    done, _ = await asyncio.wait({intake}, timeout=2)
                    async with self.sessions() as db:
                        locks = (await db.execute(text(
                            "SELECT COUNT(*) FROM performance_schema.data_locks l "
                            "JOIN performance_schema.threads t ON t.THREAD_ID=l.THREAD_ID "
                            "WHERE t.PROCESSLIST_ID=:id"), {"id": connection_ids[0]})).scalar_one()
                        waits = (await db.execute(text(
                            "SELECT COUNT(*) FROM performance_schema.data_lock_waits w "
                            "JOIN performance_schema.threads t ON t.THREAD_ID=w.BLOCKING_THREAD_ID "
                            "OR t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_ID=:id"),
                            {"id": connection_ids[0]})).scalar_one()
                    self.assertEqual((bool(done), locks, waits), (True, 0, 0),
                                     "S2 diagnostic blocked cross-agent intake or held InnoDB locks")
                    await intake
                    # A concurrent commit must not split the diagnostic read view.
                    await self._state("draining", version=11)
                finally:
                    release.set()
                    results = await asyncio.gather(show, *([intake] if intake else []), return_exceptions=True)
                self.assertEqual(results[0], 0, results)
                payload = emit.call_args.args[0]
                self.assertTrue(payload["diagnostic_only"])
                self.assertEqual(payload["consistency"], "read_only_snapshot")
                self.assertEqual(payload.get("state", payload.get("scheduler_state")), "enabled")
                if command[0] == "scheduler-state":
                    self.assertFalse(payload["clearance_proven"])
                    # New intake happened after the snapshot began.
                    self.assertEqual(payload["clearance"]["nonterminal_turns"], n + 1)
                self.assertEqual(await self._count(DeliveryOperatorAction), 0)

    async def test_scheduler_show_snapshot_does_not_block_intake(self) -> None:
        await self._seed()
        await self._diagnostic_barrier(["scheduler-state", "show"], "FROM conversation_turns")

    async def test_agent_show_snapshot_does_not_block_intake(self) -> None:
        agent, *_ = await self._seed()
        await self._diagnostic_barrier(["agent-scheduler", "show", str(agent)], "FROM agentes")

    async def test_diagnostic_readonly_and_saturation(self) -> None:
        from src.database.repositories import scheduler_diagnostics_repo, agent_scheduler_repo
        agent, *_ = await self._seed()
        for operation in (
            lambda db: scheduler_diagnostics_repo.scheduler_snapshot(db, bound=1),
            lambda db: agent_scheduler_repo.show_agent(db, agente_id=agent, provider_auth_enforced=True),
        ):
            async with self.sessions() as db:
                payload = await operation(db)
                if "clearance" in payload:
                    self.assertEqual(payload["row_bound"], 1)
                    self.assertEqual(payload["saturated"], ["nonterminal_turns"])
                    self.assertEqual(payload["clearance"]["nonterminal_turns"], 1)
                with self.assertRaises(DBAPIError) as caught:
                    await db.execute(text("UPDATE scheduler_runtime_state SET version=version WHERE id=1"))
                self.assertEqual(caught.exception.orig.args[0], 1792)
                await db.rollback()
        doc = (Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2])) / "AGENTS.md").read_text()
        for marker in ("S2", "diagnostic_only=true", "consistency=read_only_snapshot", "clearance_proven=false"):
            self.assertIn(marker, doc)

    async def test_complete_state_matrix_and_append_only_audit(self) -> None:
        allowed = {("disabled", "enabling"), ("enabling", "enabled"),
                   ("enabling", "disabled"), ("enabled", "draining"), ("draining", "disabled")}
        cases = 0
        for source in ("disabled", "enabling", "enabled", "draining"):
            for target in ("disabled", "enabling", "enabled", "draining"):
                await self._state(source)
                before = await self._count(DeliveryOperatorAction)
                with self.subTest(source=source, target=target):
                    if (source, target) in allowed:
                        row = await self._transition(target)
                        self.assertEqual((row.state, row.version), (target, 11))
                        actions = await self._rows(DeliveryOperatorAction, DeliveryOperatorAction.id)
                        self.assertEqual(len(actions), before + 1)
                        audit = actions[-1]
                        self.assertEqual(audit.action, f"scheduler:{source}:{target}")
                        expected = {
                            "v": 1, "from_version": 10, "to_version": 11,
                            "from_epoch": self.scheduler_epoch, "to_epoch": self.scheduler_epoch,
                            "delivery_epoch": self.delivery_epoch, "reason": "synthetic state change"}
                        if target == "enabled":  # T08-3 audited attestation
                            expected.update({"v": 2, "attested_old_replicas_removed": True,
                                "code_version": replica_fixtures.CODE_VERSION, "schema_version": 3,
                                "replicas": {"app": 1, "delivery_worker": 1, "turn_worker": 1}})
                        self.assertEqual(json.loads(audit.reason), expected)
                    else:
                        with self.assertRaises(scheduler_repo.SchedulerConflictError):
                            await self._transition(target)
                        self.assertEqual(await self._count(DeliveryOperatorAction), before)
                    cases += 1
        self.assertEqual(cases, 16)

    async def test_epoch_rotation_version_and_atomic_audit(self) -> None:
        cases = 0
        for source, target in (("enabling", "enabled"), ("enabling", "disabled"),
                               ("enabled", "draining"), ("draining", "disabled")):
            await self._state(source)
            with self.assertRaises(scheduler_repo.SchedulerConflictError):
                await self._transition(target, epoch=str(uuid4()))
            cases += 1
        await self._state("disabled")
        with self.assertRaises(scheduler_repo.SchedulerConflictError):
            await self._transition("enabling", version=9)
        new_epoch = str(uuid4())
        row = await self._transition("enabling", epoch=new_epoch)
        self.assertEqual(row.scheduler_epoch, new_epoch)
        # Same-UoW rollback removes state change AND appended audit.
        async with self.sessions() as db:
            await scheduler_repo.transition_scheduler_state(db, target="disabled", scheduler_epoch=new_epoch,
                expected_version=11, actor="t081", reason="rolled back")
            await db.rollback()
        self.assertEqual((await self._one(SchedulerRuntimeState)).state, "enabling")
        self.assertEqual(await self._count(DeliveryOperatorAction), 1)
        self.assertEqual(cases, 4)

    async def test_cli_show_edges_refusal_and_metadata_validation(self) -> None:
        await self._state("disabled")
        # T08-3: CLI enable needs a compatible fleet, dispatch config and the flag.
        await replica_fixtures.ready_fleet(self.sessions, delivery_epoch=self.delivery_epoch,
                                           scheduler_epoch=self.scheduler_epoch)
        self.patches.enter_context(patch.object(settings, "delivery_dispatch_enabled", True))
        with patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), patch.object(delivery_cli, "_emit") as emit:
            self.assertEqual(await delivery_cli._run(delivery_cli._parser().parse_args(["scheduler-state", "show"])), 0)
            self.assertEqual(emit.call_args.args[0]["state"], "disabled")
            self.assertEqual(await self._count(DeliveryOperatorAction), 0)
            for v, target in enumerate(("enabling", "enabled", "draining", "disabled"), 10):
                args = delivery_cli._parser().parse_args(["scheduler-state", target, "--epoch", self.scheduler_epoch,
                    "--expected-version", str(v), "--actor", "t081", "--reason", "CLI synthetic"]
                    + (["--attest-old-replicas-removed"] if target == "enabled" else []))
                self.assertEqual(await delivery_cli._run(args), 0)
                self.assertEqual(emit.call_args.args[0]["state"], target)
            self.assertEqual(await self._count(DeliveryOperatorAction), 4)
            # Refused edge: exit 1, named categories, nothing appended.
            await self._legacy_row(delivery_class="legacy_unknown")
            args = delivery_cli._parser().parse_args(["scheduler-state", "enabling", "--epoch", self.scheduler_epoch,
                "--expected-version", "14", "--actor", "t081", "--reason", "CLI refused"])
            self.assertEqual(await delivery_cli._run(args), 1)
            self.assertEqual(emit.call_args.args[0]["status"], "refused")
            self.assertEqual(emit.call_args.args[0]["clearance"]["legacy_unknown_deliveries"], 1)
            self.assertEqual(await self._count(DeliveryOperatorAction), 4)
        for actor, reason in (("", "r"), ("a", " "), ("a" * 256, "r"), ("a", "r" * 500)):
            async with self.sessions() as db:
                with self.assertRaises(ValueError):
                    await scheduler_repo.transition_scheduler_state(db, target="enabling",
                        scheduler_epoch=self.scheduler_epoch, expected_version=14, actor=actor, reason=reason)
        self.assertEqual(await self._count(DeliveryOperatorAction), 4)

    async def test_guard_nonterminal_turns_and_enabling_abort_rotation(self) -> None:
        await self._seed()
        for source, target in (("draining", "disabled"), ("enabling", "disabled"), ("disabled", "enabling")):
            await self._state(source)
            with self.assertRaises(scheduler_repo.SchedulerDrainBlocked) as caught:
                await self._transition(target, epoch=str(uuid4()) if source == "disabled" else None)
            self.assertEqual(caught.exception.counts["nonterminal_turns"], 1)

    async def test_guard_owned_lane_even_with_terminal_turn(self) -> None:
        _, _, _, turn, revision = await self._seed()
        await self._terminalize_fixture()
        async with self.sessions.begin() as db:
            await db.execute(text("UPDATE agent_service_lanes SET owner_token=:token, owner_worker_id='t081', "
                "lease_until=UTC_TIMESTAMP(6) - INTERVAL 1 DAY, active_turn_id=:t, active_revision_id=:r"),
                {"token": str(uuid4()), "t": turn.id, "r": revision.id})
        await self._state("draining")
        counts = await self._blocked("owned_lanes")
        self.assertEqual(counts["nonterminal_turns"], 0)

    async def test_guard_automated_deliveries_even_with_terminal_turn(self) -> None:
        await self._ordinary_admitted("guard-delivery")
        async with self.sessions.begin() as db:
            await db.execute(text("UPDATE agent_service_lanes SET owner_token=NULL, owner_worker_id=NULL, "
                                  "lease_until=NULL, active_turn_id=NULL, active_revision_id=NULL"))
        await self._terminalize_fixture()
        await self._state("draining")
        counts = await self._blocked("automated_deliveries")
        self.assertEqual(counts["owned_lanes"] + counts["nonterminal_turns"], 0)

    async def test_guard_legacy_unknown_deliveries(self) -> None:
        async with self.sessions.begin() as db:
            db.add(OutboxDelivery(delivery_key=str(uuid4()), dedupe_key="t081-unknown",
                cutover_epoch=self.delivery_epoch, provider="evolution", operation="evolution.send_text.v1",
                payload_version=1, payload={}, delivery_class="legacy_unknown", ordering_key=str(uuid4()),
                stream_sequence=1, status="pending", next_attempt_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(minutes=30)))
        await self._state("draining")
        await self._blocked("legacy_unknown_deliveries")

    async def test_guard_unresolved_attempt_matrix(self) -> None:
        for outcome in ("started", "outcome_unknown"):
            await self._wipe()
            await self._state("enabled")
            *_, turn, revision = await self._seed()
            await self._attempt(turn, revision, outcome=outcome)
            await self._terminalize_fixture()
            await self._state("draining")
            await self._blocked("unresolved_attempts")

    async def test_guard_unreconciled_known_attempt(self) -> None:
        *_, turn, revision = await self._seed()
        await self._attempt(turn, revision, outcome="failed", reconciliation=True)
        await self._terminalize_fixture()
        await self._state("draining")
        counts = await self._blocked("unreconciled_attempts")
        self.assertEqual(counts["unresolved_attempts"], 0)

    async def test_guard_scheduled_handoff_category(self) -> None:
        await self._run_handoff("guard-handoff")
        await self._state("draining")
        counts = await self._blocked("nonterminal_scheduled_handoffs")
        self.assertEqual(counts["nonterminal_scheduled_handoffs"], 1)

    async def test_replay_state_matrix_no_cap_or_new_work(self) -> None:
        _, instance, phone, _, _ = await self._seed()
        before = [await self._count(m) for m in (InboundEvent, Mensagem, TurnInputMembership, ConversationTurn)]
        cases = 0
        for state in ("enabled", "draining", "disabled", "enabling"):
            await self._state(state)
            with patch.object(scheduler_repo, "assert_intake_caps", AsyncMock(side_effect=AssertionError("cap on replay"))):
                result = await self._intake(instance, phone, "seed", "synthetic input")
                self.assertEqual(result.body["status"], "already_accepted")
            with self.assertRaises(inbound_event_repo.ReceiptCollisionError):
                await self._intake(instance, phone, "seed", "changed")
            if state != "enabled":
                with self.assertRaises(evolution_orchestrator.ScheduledIntakeUnavailable):
                    await self._intake(instance, phone, "new-" + state, "new")
            self.assertEqual(before, [await self._count(m) for m in (InboundEvent, Mensagem, TurnInputMembership, ConversationTurn)])
            cases += 1
        self.assertEqual(cases, 4)
        self.assertEqual(await self._count(OutboxDelivery), 0)

    async def test_replay_state_gate_matrix(self) -> None:
        agent, instance, phone, _, _ = await self._seed()
        cases = 0
        for state in ("enabled", "draining", "disabled", "enabling"):
            await self._state(state)
            for gate, value in (("provider_webhook_auth_mode", "observe"), ("inbox_scheduler_enabled", False),
                                ("inbox_scheduler_epoch", str(uuid4())), ("delivery_cutover_epoch", str(uuid4())),
                                ("delivery_mode", "direct")):
                with self.subTest(state=state, gate=gate), patch.object(settings, gate, value):
                    with self.assertRaises(evolution_orchestrator.ScheduledIntakeUnavailable):
                        await self._intake(instance, phone, "seed", "synthetic input")
                cases += 1
            # Exercise the scheduled path with stale preliminary active/opt-in hints.
            for active, optin, authenticated in ((False, True, True), (True, False, True), (True, True, False)):
                async with self.sessions.begin() as db:
                    await db.execute(text("UPDATE agentes SET ativo=:a, inbox_scheduler_enabled=:o WHERE id=:id"),
                                     {"a": active, "o": optin, "id": agent})
                async with UnitOfWork(self.sessions) as uow:
                    with self.assertRaises(evolution_orchestrator.ScheduledIntakeUnavailable):
                        await evolution_orchestrator._orchestrate_scheduled_intake(
                            event=evolution_orchestrator.EvolutionMessageEvent(instance_name=instance,
                                telefone=phone, provider_message_id="seed", texto="synthetic input"),
                            uow=uow, authenticated=authenticated, agente_id=agent)
                cases += 1
            async with self.sessions.begin() as db:
                await db.execute(text("UPDATE agentes SET ativo=TRUE, inbox_scheduler_enabled=TRUE WHERE id=:id"), {"id": agent})
        self.assertEqual(cases, 32)
        self.assertEqual(await self._count(InboundEvent), 1)

    async def test_drain_accepts_claim_generation_delivery_then_disables(self) -> None:
        await self._seed()
        await self._force_ready()
        await self._state("enabled")
        await self._transition("draining")
        llm = base.FakeLLM("synthetic reply")
        processor = self._router_processor(llm)
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._claim()
        self.assertEqual(len(claimed), 1)
        self.assertTrue(await self._preio(claimed[0]))
        self.assertTrue(await self._complete(claimed[0]))
        self.assertEqual(len(llm.calls), 1)
        self.assertEqual((await self._transition("disabled", version=11)).state, "disabled")

    async def test_enabling_disabled_refuse_claims(self) -> None:
        agent, _, _, turn, _ = await self._seed()
        await self._force_ready()
        for state in ("disabled", "enabling"):
            await self._state(state)
            async with self.sessions() as db:
                with self.assertRaises(scheduler_repo.SchedulerNotReadyError):
                    await scheduler_repo.claim_ready_turn(db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.delivery_epoch, worker_id="t081", lease_seconds=30,
                        candidate=(turn.id, turn.sessao_id))

    async def test_drain_intake_and_replay_lock_barriers_repeated(self) -> None:
        _, instance, phone, _, _ = await self._seed()
        for n in range(3):
            await self._state("enabled")
            async with self.sessions() as blocker:
                await delivery_state_repo.get_runtime_state(blocker, for_share=True)
                state = await scheduler_repo.get_scheduler_state(blocker, for_update=True)
                task = asyncio.create_task(self._intake(instance, phone, f"new-{n}", "new"))
                await self._lock_wait()
                state.state = "draining"
                await blocker.commit()
                with self.assertRaises(evolution_orchestrator.ScheduledIntakeUnavailable):
                    await task
            async with self.sessions() as blocker:
                await delivery_state_repo.get_runtime_state(blocker, for_share=True)
                state = await scheduler_repo.get_scheduler_state(blocker, for_update=True)
                task = asyncio.create_task(self._intake(instance, phone, "seed", "synthetic input"))
                await self._lock_wait()
                state.state = "disabled"
                await blocker.commit()
                self.assertEqual((await task).body["status"], "already_accepted")
        self.assertEqual(await self._count(InboundEvent), 1)

    async def test_drain_claim_barrier_repeated(self) -> None:
        for n in range(3):
            await self._wipe()
            await self._state("enabled")
            agent, _, _, turn, _ = await self._seed()
            await self._force_ready()
            async def claim():
                return await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions,
                    lambda db: scheduler_repo.claim_ready_turn(db, agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch, delivery_runtime_epoch=self.delivery_epoch,
                        worker_id="t081", lease_seconds=30, candidate=(turn.id, turn.sessao_id)))
            async with self.sessions() as blocker:
                await delivery_state_repo.get_runtime_state(blocker, for_share=True)
                state = await scheduler_repo.get_scheduler_state(blocker, for_update=True)
                task = asyncio.create_task(claim())
                await self._lock_wait()
                state.state = "draining"
                await blocker.commit()
                self.assertIsNotNone(await task)

    async def test_disable_owned_lane_barrier_repeated(self) -> None:
        *_, turn, revision = await self._seed()
        await self._terminalize_fixture()
        await self._state("draining")
        for n in range(3):
            async with self.sessions() as blocker:
                await db_execute_lane_owner(blocker, turn.id, revision.id)
                task = asyncio.create_task(self._transition("disabled"))
                await self._lock_wait()
                await blocker.commit()
                with self.assertRaises(scheduler_repo.SchedulerDrainBlocked) as caught:
                    await task
                self.assertEqual(caught.exception.counts["owned_lanes"], 1)
            async with self.sessions.begin() as db:
                await db.execute(text("UPDATE agent_service_lanes SET owner_token=NULL, owner_worker_id=NULL, "
                                      "lease_until=NULL, active_turn_id=NULL, active_revision_id=NULL"))

    # ── A1/A3: guard transaction shape, lock footprint, real-path barriers ──
    CLEARANCE_TABLES = ("agent_service_lanes", "conversation_turns", "generation_attempts",
                        "scheduled_handoffs", "outbox_deliveries")

    async def _guard(self, target: str = "disabled", version: int = 10):
        """Real guard UoW without the retry wrapper: any 1213 surfaces."""
        async with self.sessions() as db:
            try:
                row = await scheduler_repo.transition_scheduler_state(db, target=target,
                    scheduler_epoch=self.scheduler_epoch, expected_version=version,
                    actor="t081", reason="barrier")
                await db.commit()
                return ("ok", row.state)
            except scheduler_repo.SchedulerDrainBlocked as exc:
                await db.rollback()
                return ("blocked", {k: v for k, v in exc.counts.items() if v})
            except DBAPIError as exc:
                return ("dberror", scheduler_repo._mysql_error_code(exc))

    async def _record_locks(self):
        async with self.sessions() as db:
            rows = (await db.execute(text(
                "SELECT OBJECT_NAME, INDEX_NAME, LOCK_MODE, LOCK_DATA FROM performance_schema.data_locks "
                "WHERE OBJECT_SCHEMA=DATABASE() AND LOCK_TYPE='RECORD' ORDER BY OBJECT_NAME, LOCK_DATA"))).all()
        return [tuple(r) for r in rows]

    async def _no_wait(self, coro, label: str):
        # Must finish while the guard transaction is still open.
        result = await asyncio.wait_for(coro, timeout=10)
        async with self.sessions() as db:
            waits = (await db.execute(text("SELECT COUNT(*) FROM performance_schema.data_lock_waits"))).scalar_one()
        self.assertEqual(waits, 0, label)
        return result

    async def _legacy_row(self, status: str = "pending", delivery_class: str = "legacy_automated") -> int:
        async with self.sessions.begin() as db:
            row = OutboxDelivery(delivery_key=str(uuid4()), dedupe_key=f"t081-legacy-{uuid4()}",
                cutover_epoch=self.delivery_epoch, provider="evolution", operation="evolution.send_text.v1",
                payload_version=1, payload={}, delivery_class=delivery_class, ordering_key=str(uuid4()),
                stream_sequence=1, status=status, next_attempt_at=datetime.utcnow() - timedelta(seconds=5),
                expires_at=datetime.utcnow() + timedelta(minutes=30))
            db.add(row)
            await db.flush()
            return row.id

    async def _legacy_writes_do_not_wait(self, legacy_id: int) -> None:
        async def update_legacy():
            async with self.sessions.begin() as db:
                await db.execute(text("UPDATE outbox_deliveries SET priority=priority+1 WHERE id=:id"), {"id": legacy_id})
        await self._no_wait(update_legacy(), "legacy row update waited on the guard")
        await self._no_wait(self._legacy_row(), "legacy outbox insert waited on a guard gap lock")

    async def test_guard_requires_fresh_transaction_and_reads_current(self) -> None:
        *_, turn, revision = await self._seed()
        await self._attempt(turn, revision, outcome="failed", reconciliation=True)
        await self._terminalize_fixture()
        await self._state("draining")
        async with self.sessions() as stale:
            await stale.execute(select(GenerationAttempt))  # opens a read view first
            with self.assertRaises(scheduler_repo.SchedulerConflictError):
                await scheduler_repo.transition_scheduler_state(stale, target="disabled",
                    scheduler_epoch=self.scheduler_epoch, expected_version=10, actor="t081", reason="stale")
            await stale.rollback()
        async with self.sessions() as pending:
            pending.add(DeliveryOperatorAction(delivery_id=None, action="x", actor="t081", reason="x"))
            with self.assertRaises(scheduler_repo.SchedulerConflictError):
                await scheduler_repo.transition_scheduler_state(pending, target="disabled",
                    scheduler_epoch=self.scheduler_epoch, expected_version=10, actor="t081", reason="pending")
        self.assertEqual(await self._count(DeliveryOperatorAction), 0)
        # The fresh guard UoW still sees the committed quarantine.
        await self._blocked("unreconciled_attempts")

    async def test_clearance_lock_footprint_no_gap_or_secondary_locks(self) -> None:
        """A3: only S record locks on blocking clustered rows; others never wait."""
        # Clean: every category present only as non-blocking rows.
        *_, turn, revision = await self._seed()
        await self._attempt(turn, revision, outcome="failed")
        await self._terminalize_fixture()
        legacy_id = await self._legacy_row()
        await self._state("draining")
        async with self.sessions() as guard:
            await scheduler_repo.transition_scheduler_state(guard, target="disabled",
                scheduler_epoch=self.scheduler_epoch, expected_version=10, actor="t081", reason="footprint")
            locks = await self._record_locks()
            self.assertEqual([l for l in locks if l[0] in self.CLEARANCE_TABLES], [], locks)
            self.assertTrue(locks and all(l[2].endswith("REC_NOT_GAP") for l in locks), locks)
            await self._legacy_writes_do_not_wait(legacy_id)
            await guard.commit()
        self.assertEqual((await self._one(SchedulerRuntimeState)).state, "disabled")
        # Blocking: locks are exactly the blocking clustered rows.
        await self._wipe()
        await self._state("enabled")
        agent, *_ = await self._ordinary_admitted("footprint")
        legacy_id = await self._legacy_row()
        await self._state("draining")
        turn = await self._one(ConversationTurn)
        delivery = (await self._automated())[0]
        async with self.sessions() as guard:
            with self.assertRaises(scheduler_repo.SchedulerDrainBlocked):
                await scheduler_repo.transition_scheduler_state(guard, target="disabled",
                    scheduler_epoch=self.scheduler_epoch, expected_version=10, actor="t081", reason="footprint")
            locks = [l for l in await self._record_locks() if l[0] in self.CLEARANCE_TABLES]
            self.assertEqual(sorted(locks), sorted([
                ("agent_service_lanes", "PRIMARY", "S,REC_NOT_GAP", str(agent)),
                ("conversation_turns", "PRIMARY", "S,REC_NOT_GAP", str(turn.id)),
                ("outbox_deliveries", "PRIMARY", "S,REC_NOT_GAP", str(delivery.id)),
            ]), locks)
            await self._legacy_writes_do_not_wait(legacy_id)
            await guard.rollback()

    async def test_guard_vs_delivery_claim_no_deadlock_repeated(self) -> None:
        outcomes = []
        for n in range(3):
            await self._wipe()
            await self._state("enabled")
            await self._ordinary_admitted(f"claim-{n}")
            legacy_id = await self._legacy_row()
            await self._state("draining")
            # Order A: claim holds X on legacy+automated rows; guard waits, then reads current.
            async with self.sessions() as claim:
                claimed = await delivery_worker_repo.claim_due(claim, available_slots=20, worker_id="t081",
                    cutover_epoch=self.delivery_epoch, evolution_lease_seconds=90, chatwoot_lease_seconds=30)
                self.assertEqual(len(claimed), 2)
                task = asyncio.create_task(self._guard())
                await self._lock_wait()
                await claim.commit()
                outcomes.append(await task)
            # Order B: guard holds its S locks; claim skips them without waiting.
            async with self.sessions.begin() as db:
                await db.execute(text("UPDATE outbox_deliveries SET status='retry', lease_token=NULL, "
                    "lock_owner=NULL, lock_until=NULL, next_attempt_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND"))
            async with self.sessions() as guard:
                with self.assertRaises(scheduler_repo.SchedulerDrainBlocked):
                    await scheduler_repo.transition_scheduler_state(guard, target="disabled",
                        scheduler_epoch=self.scheduler_epoch, expected_version=10, actor="t081", reason="b")
                async def claim_b():
                    async with self.sessions() as db:
                        rows = await delivery_worker_repo.claim_due(db, available_slots=20, worker_id="t081",
                            cutover_epoch=self.delivery_epoch, evolution_lease_seconds=90, chatwoot_lease_seconds=30)
                        await db.commit()
                        return [r.id for r in rows]
                self.assertEqual(await self._no_wait(claim_b(), "claim waited on guard"), [legacy_id])
                await guard.rollback()
        self.assertEqual([o[0] for o in outcomes], ["blocked"] * 3, outcomes)

    async def test_guard_vs_late_accounting_no_deadlock_repeated(self) -> None:
        results = []
        for n in range(3):
            for order in ("accounting_first", "guard_first"):
                await self._wipe()
                await self._state("enabled")
                *_, turn, revision = await self._seed()
                await self._attempt(turn, revision)
                attempt = self.last_attempt
                await self._terminalize_fixture()
                await self._state("draining")
                async def account(db):
                    try:
                        return await turn_generation_repo.record_generation_accounting_only(db,
                            agente_id=attempt.agente_id, sessao_id=attempt.sessao_id,
                            attempt_key=attempt.attempt_key, lane_owner_token=attempt.lane_owner_token,
                            outcome=turn_generation_repo.GenerationOutcome(outcome="failed",
                                                                           safe_error_code="http_500"))
                    except DBAPIError as exc:
                        return ("dberror", scheduler_repo._mysql_error_code(exc))
                if order == "accounting_first":
                    async with self.sessions() as acc:
                        self.assertTrue(await account(acc))
                        task = asyncio.create_task(self._guard())
                        await self._lock_wait()
                        await acc.commit()
                        guard_result = await task
                    # Guard waited for the attempt and read its committed outcome.
                    self.assertEqual(guard_result, ("ok", "disabled"))
                else:
                    async with self.sessions() as guard:
                        with self.assertRaises(scheduler_repo.SchedulerDrainBlocked):
                            await scheduler_repo.transition_scheduler_state(guard, target="disabled",
                                scheduler_epoch=self.scheduler_epoch, expected_version=10, actor="t081",
                                reason="late accounting")
                        async def late():
                            async with self.sessions() as db:
                                ok = await account(db)
                                await db.commit()
                                return ok
                        task = asyncio.create_task(late())
                        await self._lock_wait()
                        await guard.rollback()
                        guard_result = ("blocked", "rolled back")
                        self.assertIs(await task, True)
                results.append((order, guard_result))
        self.assertEqual(len(results), 6)

    async def test_guard_vs_intake_no_deadlock_repeated(self) -> None:
        _, instance, phone, _, _ = await self._seed()
        await self._terminalize_fixture()
        version = 10
        await self._state("enabled", version=version)
        original = scheduler_repo.assert_intake_caps
        for n in range(3):
            # Order A: a new intake holds runtime..session+identity rows; guard waits.
            gate, entered = asyncio.Event(), asyncio.Event()
            async def paused(*args, **kwargs):
                entered.set()
                await gate.wait()
                return await original(*args, **kwargs)
            with patch.object(scheduler_repo, "assert_intake_caps", paused):
                intake = asyncio.create_task(self._intake(instance, phone, f"new-{n}", "new"))
                await asyncio.wait_for(entered.wait(), 10)
                guard = asyncio.create_task(self._guard("draining", version))
                await self._lock_wait()
                gate.set()
                self.assertEqual((await intake).http_status, 202)
                self.assertEqual(await guard, ("ok", "draining"))
            version += 1
            blocked = await self._guard("disabled", version)
            self.assertEqual(blocked[0], "blocked")
            self.assertIn("nonterminal_turns", blocked[1])
            # Order B: the real guard holds scheduler X; intake waits, then is refused.
            await self._terminalize_fixture()
            async with self.sessions() as guard_db:
                await scheduler_repo.transition_scheduler_state(guard_db, target="disabled",
                    scheduler_epoch=self.scheduler_epoch, expected_version=version, actor="t081", reason="b")
                new = asyncio.create_task(self._intake(instance, phone, f"late-{n}", "late"))
                replay = asyncio.create_task(self._intake(instance, phone, "seed", "synthetic input"))
                await self._lock_wait()
                await guard_db.commit()
            with self.assertRaises(evolution_orchestrator.ScheduledIntakeUnavailable):
                await new
            self.assertEqual((await replay).body["status"], "already_accepted")
            version += 1
            await self._state("enabled", version=version)
        self.assertEqual(await self._count(InboundEvent), 4)

    async def test_guard_vs_lifecycle_no_deadlock_repeated(self) -> None:
        for n in range(3):
            for order in ("guard_first", "lifecycle_first"):
                await self._wipe()
                await self._state("enabled")
                agent, _, phone, turn, _ = await self._seed()
                await self._state("draining")
                async def transfer(db):
                    try:
                        return await lifecycle_repo.transfer_session(db, agente_id=agent, telefone=phone,
                                                                     motivo="transferencia_manual")
                    except DBAPIError as exc:
                        return ("dberror", scheduler_repo._mysql_error_code(exc))
                if order == "guard_first":
                    async with self.sessions() as guard:
                        with self.assertRaises(scheduler_repo.SchedulerDrainBlocked):
                            await scheduler_repo.transition_scheduler_state(guard, target="disabled",
                                scheduler_epoch=self.scheduler_epoch, expected_version=10, actor="t081",
                                reason="lifecycle")
                        async def lifecycle():
                            async with self.sessions() as db:
                                result = await transfer(db)
                                await db.commit()
                                return result
                        task = asyncio.create_task(lifecycle())
                        await self._lock_wait()
                        await guard.rollback()
                        result = await task
                    self.assertEqual(result.cancelled_turns, (turn.id,))
                    self.assertEqual(await self._guard(), ("ok", "disabled"))
                else:
                    async with self.sessions() as life:
                        result = await transfer(life)
                        self.assertEqual(result.cancelled_turns, (turn.id,))
                        task = asyncio.create_task(self._guard())
                        await self._lock_wait()
                        await life.commit()
                    self.assertEqual(await task, ("ok", "disabled"))

    async def test_preio_refuses_claim_in_enabling_and_disabled(self) -> None:
        cases = 0
        for state in ("enabling", "disabled"):
            await self._wipe()
            await self._state("enabled")
            await self._ordinary_admitted(f"preio-{state}")
            claimed = (await self._claim())[0]
            await self._state(state)
            before = turn_delivery_repo.runtime_gate_release_count()
            self.assertFalse(await self._preio(claimed))
            self.assertEqual(turn_delivery_repo.runtime_gate_release_count(), before + 1)
            row = (await self._automated())[0]
            self.assertEqual((row.status, row.ambiguity_count, row.lease_token, row.last_error_code),
                             ("retry", 0, None, "preio_refused_runtime"))
            async with self.sessions() as db:
                now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
            self.assertGreater(row.next_attempt_at, now)
            # Zero I/O: the Evolution client patch raises if it is ever called.
            evolution_orchestrator.evolution_client.send_text_message.assert_not_called()
            cases += 1
        self.assertEqual(cases, 2)

    async def test_http_webhook_state_gate_matrix(self) -> None:
        _, instance, phone, _, _ = await self._seed()
        def request(message_id: str, content: str, token: str = "t081-bearer"):
            return SimpleNamespace(headers={"Authorization": f"Bearer {token}"}, json=AsyncMock(return_value={
                "event": "messages.upsert", "instance": instance,
                "data": {"key": {"id": message_id, "fromMe": False, "remoteJid": f"{phone}@s.whatsapp.net"},
                         "message": {"conversation": content}}}))
        async def call(req):
            response = SimpleNamespace(status_code=None)
            async with UnitOfWork(self.sessions) as uow:
                try:
                    body = await main_module.webhook_handler(req, uow, response)
                    return response.status_code, body.get("status")
                except HTTPException as exc:
                    return exc.status_code, (exc.headers or {}).get("Retry-After")
        cases = 0
        with patch.object(settings, "evolution_webhook_bearer_token", "t081-bearer"):
            for state in ("enabled", "draining", "disabled", "enabling"):
                await self._state(state)
                before = scheduler_repo.intake_state_refusal_count()
                self.assertEqual(await call(request("seed", "synthetic input")), (200, "already_accepted"))
                self.assertEqual(await call(request("seed", "changed")), (409, None))
                self.assertEqual(await call(request("seed", "synthetic input", token="wrong")), (401, None))
                new = await call(request(f"http-{state}", "new"))
                if state == "enabled":
                    self.assertEqual(new[0], 202)
                else:
                    self.assertEqual(new, (503, "5"))
                    self.assertEqual(scheduler_repo.intake_state_refusal_count(), before + 1)
                cases += 4
        self.assertEqual(cases, 16)
        self.assertEqual(await self._count(InboundEvent), 2)

    async def test_documented_contract_parity(self) -> None:
        doc = (Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2])) / "AGENTS.md").read_text()
        async with self.sessions.begin() as db:
            await delivery_state_repo.get_runtime_state(db, for_share=True)
            await scheduler_repo.get_scheduler_state(db, for_update=True)
            counts = await scheduler_repo.scheduler_clearance_counts(db)
        for category in counts:
            self.assertIn(f"`{category}=0`", doc)
        self.assertEqual(set(scheduler_repo.SCHEDULER_TRANSITIONS), {"disabled", "enabling", "enabled", "draining"})
        self.assertEqual(set(scheduler_repo.NONTERMINAL_TURN_STATUSES),
                         {"pending", "claimed", "generating", "typing", "awaiting_delivery", "retry"})
        self.assertIn("scheduler-state show", doc)


async def db_execute_lane_owner(db, turn_id, revision_id):
    await db.execute(text("UPDATE agent_service_lanes SET owner_token=:token, owner_worker_id='t081', "
        "lease_until=UTC_TIMESTAMP(6) + INTERVAL 30 SECOND, active_turn_id=:t, active_revision_id=:r"),
        {"token": str(uuid4()), "t": turn_id, "r": revision_id})


for _name in dir(base.TurnDeliveryMySQLTests):
    if _name.startswith("test_") and _name not in SchedulerDrainMySQLTests.__dict__:
        setattr(SchedulerDrainMySQLTests, _name, None)
