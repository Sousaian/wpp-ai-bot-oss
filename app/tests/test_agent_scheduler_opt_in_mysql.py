"""T08-6: guarded per-agent opt-in on real MySQL, fake providers only."""
import os
import asyncio
from decimal import Decimal
import itertools
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from src import agent_scheduler_cli, delivery_cli
from src.config import settings
from src.database.models import (
    Agente, ConversationTurn, DeliveryOperatorAction, InboundEvent, SchedulerRuntimeState,
)
from src.database.repositories import agent_scheduler_repo, scheduler_repo
from src.routers import agentes as agentes_router
from src.services import evolution_orchestrator
from src.services.provider_auth_bootstrap import ReadinessReport
from tests import test_scheduler_drain_mysql as drain


def _report(ready: bool = True, auth_mode: str = "enforced") -> ReadinessReport:
    return ReadinessReport(ready=ready, auth_mode=auth_mode, evolution_instances=1,
                           chatwoot_inboxes=1, issues=() if ready else ("synthetic_issue",))


class AgentSchedulerOptInMySQLTests(drain.SchedulerDrainMySQLTests):
    async def _plain(self, suffix: str, *, ativo=True, inbox=7, opted=False) -> tuple[int, str]:
        instance = f"t086-{suffix}-{uuid4()}"
        async with self.sessions.begin() as db:
            row = Agente(nome=f"t086 {suffix}", instance_name=instance, system_prompt="t086",
                         numero_notificacao="0", modelo_llm="openai/gpt-4o-mini",
                         chatwoot_inbox_id=inbox, ativo=ativo, inbox_scheduler_enabled=opted)
            db.add(row)
            await db.flush()
            return row.id, instance

    async def _set_agent(self, agent: int, *, ativo: bool, inbox, opted: bool) -> None:
        async with self.sessions.begin() as db:
            await db.execute(text("UPDATE agentes SET ativo=:a, chatwoot_inbox_id=:i, "
                                  "inbox_scheduler_enabled=:o WHERE id=:id"),
                             {"a": ativo, "i": inbox, "o": opted, "id": agent})

    async def _runtime(self, mode: str) -> None:
        async with self.sessions.begin() as db:
            await db.execute(text("UPDATE delivery_runtime_state SET mode=:m WHERE id=1"), {"m": mode})

    async def _flag(self, agent: int) -> bool:
        async with self.sessions() as db:
            return bool((await db.execute(select(Agente.inbox_scheduler_enabled)
                                          .where(Agente.id == agent))).scalar_one())

    async def _opt_in(self, agent: int, *, auth=True, scheduler_epoch=None, delivery_epoch=None,
                      actor="t086", reason="synthetic opt-in"):
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions, lambda db: agent_scheduler_repo.opt_in_agent(
                db, agente_id=agent, scheduler_epoch=scheduler_epoch or self.scheduler_epoch,
                delivery_epoch=delivery_epoch or self.delivery_epoch,
                provider_auth_enforced=auth, actor=actor, reason=reason))

    async def _opt_out(self, agent: int, *, actor="t086", reason="synthetic opt-out"):
        from src.database.repositories import lifecycle_repo
        return await lifecycle_repo.run_in_new_session(
            self.sessions, lambda db: agent_scheduler_repo.opt_out_agent(
                db, agente_id=agent, actor=actor, reason=reason))

    async def _refused(self, coro, action: str, reasons: tuple[str, ...]):
        before = agent_scheduler_repo.refusal_counts()[action]
        audits = await self._count(DeliveryOperatorAction)
        with self.assertRaises(agent_scheduler_repo.AgentSchedulerRefused) as caught:
            await coro
        self.assertEqual(caught.exception.reasons, reasons)
        self.assertEqual(agent_scheduler_repo.refusal_counts()[action], before + 1)
        self.assertEqual(await self._count(DeliveryOperatorAction), audits)
        return caught.exception

    # ── guard matrix ────────────────────────────────────────────────────
    async def test_opt_in_guard_matrix_complete(self) -> None:
        """Full product scheduler × runtime × ativo × inbox × auth × flag."""
        agent, _ = await self._plain("matrix")
        cases = allowed = 0
        for state, mode, ativo, inbox, auth, opted in itertools.product(
                ("disabled", "enabling", "enabled", "draining"), ("direct", "outbox", "frozen"),
                (True, False), (7, None), (True, False), (False, True)):
            await self._state(state)
            await self._runtime(mode)
            await self._set_agent(agent, ativo=ativo, inbox=inbox, opted=opted)
            expected = tuple(name for name, failed in (
                ("scheduler_not_enabled", state != "enabled"),
                ("runtime_not_outbox", mode != "outbox"),
                ("provider_auth_not_enforced", not auth),
                ("agent_inactive", not ativo),
                ("chatwoot_inbox_missing", inbox is None),
                ("agent_already_opted_in", opted)) if failed)
            with self.subTest(state=state, mode=mode, ativo=ativo, inbox=inbox, auth=auth, opted=opted):
                if expected:
                    await self._refused(self._opt_in(agent, auth=auth), "opt_in", expected)
                    self.assertEqual(await self._flag(agent), opted)
                else:
                    before = await self._count(DeliveryOperatorAction)
                    row = await self._opt_in(agent, auth=auth)
                    self.assertTrue(row.inbox_scheduler_enabled)
                    self.assertTrue(await self._flag(agent))
                    self.assertEqual(await self._count(DeliveryOperatorAction), before + 1)
                    allowed += 1
            cases += 1
        self.assertEqual((cases, allowed), (192, 1))
        # Epoch guards and missing agent (named, ordered, combined).
        await self._state("enabled")
        await self._runtime("outbox")
        await self._set_agent(agent, ativo=True, inbox=7, opted=False)
        other = str(uuid4())
        for kwargs, expected in (
                ({"scheduler_epoch": other}, ("scheduler_epoch_mismatch",)),
                ({"delivery_epoch": other}, ("delivery_epoch_mismatch",)),
                ({"scheduler_epoch": other, "delivery_epoch": other},
                 ("scheduler_epoch_mismatch", "delivery_epoch_mismatch"))):
            await self._refused(self._opt_in(agent, **kwargs), "opt_in", expected)
            cases += 1
        await self._refused(self._opt_in(agent + 999), "opt_in", ("agent_not_found",))
        self.assertFalse(await self._flag(agent))
        self.assertEqual(cases + 1, 196)
        self.assertEqual(set(agent_scheduler_repo.OPT_IN_REFUSALS), {
            "agent_not_found", "scheduler_not_enabled", "scheduler_epoch_mismatch",
            "runtime_not_outbox", "delivery_epoch_mismatch", "provider_auth_not_enforced",
            "agent_inactive", "chatwoot_inbox_missing", "agent_already_opted_in"})

    async def test_opt_in_audit_and_metadata(self) -> None:
        agent, _ = await self._plain("audit")
        for actor, reason in (("", "r"), ("a", " "), ("a" * 256, "r"), ("a", "r" * 500)):
            with self.assertRaises(ValueError):
                await self._opt_in(agent, actor=actor, reason=reason)
        self.assertEqual(await self._count(DeliveryOperatorAction), 0)
        self.assertFalse(await self._flag(agent))
        state = await self._one(SchedulerRuntimeState)
        await self._opt_in(agent, actor=" ops ", reason=" enable agent ")
        audit = (await self._rows(DeliveryOperatorAction, DeliveryOperatorAction.id))[-1]
        self.assertEqual((audit.action, audit.actor, audit.delivery_id),
                         ("agent_scheduler:opt_in", "ops", None))
        self.assertEqual(json.loads(audit.reason), {
            "v": 1, "agente_id": agent, "from": False, "to": True, "scheduler_state": "enabled",
            "scheduler_epoch": self.scheduler_epoch, "scheduler_version": state.version,
            "delivery_mode": "outbox", "delivery_epoch": self.delivery_epoch,
            "reason": "enable agent"})
        # Opt-in never changes scheduler/runtime state or version.
        self.assertEqual((await self._one(SchedulerRuntimeState)).version, state.version)

    async def test_opt_in_requires_fresh_transaction(self) -> None:
        agent, _ = await self._plain("fresh")
        async with self.sessions() as stale:
            await stale.execute(select(Agente.id))  # opens a read view first
            with self.assertRaises(scheduler_repo.SchedulerConflictError):
                await agent_scheduler_repo.opt_in_agent(
                    stale, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                    delivery_epoch=self.delivery_epoch, provider_auth_enforced=True,
                    actor="t086", reason="stale")
            await stale.rollback()
        self.assertFalse(await self._flag(agent))
        self.assertEqual(await self._count(DeliveryOperatorAction), 0)

    async def test_opt_in_enables_scheduled_intake_in_enabled(self) -> None:
        agent, instance = await self._plain("intake")
        await self._opt_in(agent)
        await self._accept(instance, "5511000000086", "t086-in", "synthetic input")
        self.assertEqual(await self._count(ConversationTurn), 1)
        receipt = await self._one(InboundEvent)
        self.assertEqual(receipt.receipt_disposition, "scheduled")

    # ── opt-out ─────────────────────────────────────────────────────────
    async def test_opt_out_reuses_lifecycle_refusal_and_audits(self) -> None:
        agent, *_ = await self._seed()  # opted-in agent with a nonterminal turn
        await self._refused(self._opt_out(agent), "opt_out", ("scheduled_work_pending",))
        self.assertTrue(await self._flag(agent))
        for state in ("draining", "enabled"):
            await self._state(state)
            await self._refused(self._opt_out(agent), "opt_out", ("scheduled_work_pending",))
        await self._terminalize_fixture()
        await self._state("draining")  # opt-out stays possible while draining
        row = await self._opt_out(agent, actor=" ops ", reason=" back to legacy ")
        self.assertFalse(row.inbox_scheduler_enabled)
        self.assertFalse(await self._flag(agent))
        audit = (await self._rows(DeliveryOperatorAction, DeliveryOperatorAction.id))[-1]
        self.assertEqual((audit.action, audit.actor), ("agent_scheduler:opt_out", "ops"))
        self.assertEqual(json.loads(audit.reason), {
            "v": 1, "agente_id": agent, "from": True, "to": False, "scheduler_state": "draining",
            "scheduler_epoch": self.scheduler_epoch, "scheduler_version": 10,
            "delivery_mode": "outbox", "delivery_epoch": self.delivery_epoch,
            "reason": "back to legacy"})
        await self._refused(self._opt_out(agent), "opt_out", ("agent_not_opted_in",))
        await self._refused(self._opt_out(agent + 999), "opt_out", ("agent_not_found",))
        with self.assertRaises(ValueError):
            await self._opt_out(agent, actor=" ")

    # ── CLI ─────────────────────────────────────────────────────────────
    async def test_cli_opt_in_out_show_and_auth_sources(self) -> None:
        agent, _ = await self._plain("cli")
        parse = delivery_cli._parser().parse_args
        opt_in = ["agent-scheduler", "opt-in", str(agent), "--scheduler-epoch", self.scheduler_epoch,
                  "--delivery-epoch", self.delivery_epoch, "--actor", "t086", "--reason", "CLI"]
        readiness = AsyncMock(return_value=_report())
        with patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                patch.object(delivery_cli, "_emit") as emit, \
                patch.object(agent_scheduler_cli, "provider_auth_readiness", readiness):
            self.assertEqual(await delivery_cli._run(parse(["agent-scheduler", "show", str(agent)])), 0)
            shown = emit.call_args.args[0]
            self.assertEqual((shown["inbox_scheduler_enabled"], shown["opt_in_blockers"]), (False, []))
            # Auth: snapshot not ready, snapshot not enforced, local mode not enforced.
            cases = 0
            for report, mode in ((_report(ready=False), "enforced"),
                                 (_report(auth_mode="provisioning"), "enforced"),
                                 (_report(), "provisioning")):
                readiness.return_value = report
                with patch.object(settings, "provider_webhook_auth_mode", mode):
                    self.assertEqual(await delivery_cli._run(parse(opt_in)), 1)
                out = emit.call_args.args[0]
                self.assertEqual((out["status"], out["reasons"]), ("refused", ["provider_auth_not_enforced"]))
                cases += 1
            self.assertEqual(cases, 3)
            self.assertFalse(await self._flag(agent))
            readiness.return_value = _report()
            self.assertEqual(await delivery_cli._run(parse(opt_in)), 0)
            self.assertEqual(emit.call_args.args[0]["status"], "opted_in")
            self.assertEqual(await delivery_cli._run(parse(opt_in)), 1)
            self.assertEqual(emit.call_args.args[0]["reasons"], ["agent_already_opted_in"])
            await self._state("draining")
            self.assertEqual(await delivery_cli._run(parse(["agent-scheduler", "show", str(agent)])), 0)
            self.assertEqual(emit.call_args.args[0]["opt_in_blockers"],
                             ["scheduler_not_enabled", "agent_already_opted_in"])
            out_args = ["agent-scheduler", "opt-out", str(agent), "--actor", "t086", "--reason", "CLI out"]
            self.assertEqual(await delivery_cli._run(parse(out_args)), 0)
            self.assertEqual(emit.call_args.args[0]["status"], "opted_out")
            self.assertEqual(await delivery_cli._run(parse(out_args)), 1)
            self.assertEqual(emit.call_args.args[0]["reasons"], ["agent_not_opted_in"])
        self.assertEqual(await self._count(DeliveryOperatorAction), 2)
        for bad in (["agent-scheduler", "opt-in", str(agent), "--delivery-epoch", self.delivery_epoch,
                     "--actor", "a", "--reason", "r"],
                    ["agent-scheduler", "opt-in", str(agent), "--scheduler-epoch", "x",
                     "--delivery-epoch", self.delivery_epoch, "--actor", "a", "--reason", "r"],
                    ["agent-scheduler", "opt-out", str(agent), "--actor", " ", "--reason", "r"]):
            with self.assertRaises(SystemExit), patch("sys.stderr"):
                parse(bad)

    async def test_cli_list_opted_in_is_paginated_read_only_and_lock_free(self) -> None:
        opted = [await self._plain(f"list-{index}", opted=True) for index in range(3)]
        await self._plain("list-disabled", opted=False)
        statements: list[str] = []

        from sqlalchemy import event

        def record_statement(_conn, _cursor, statement, _parameters, _context, _many):
            statements.append(" ".join(statement.split()))

        event.listen(self.engine.sync_engine, "before_cursor_execute", record_statement)
        try:
            parse = delivery_cli._parser().parse_args
            with patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                    patch.object(delivery_cli, "_emit") as emit:
                self.assertEqual(
                    await delivery_cli._run(parse([
                        "agent-scheduler", "list", "--after-id", "0", "--limit", "2",
                    ])),
                    0,
                )
                first = emit.call_args.args[0]
                self.assertEqual(
                    [row["agente_id"] for row in first["agents"]],
                    [opted[0][0], opted[1][0]],
                )
                self.assertTrue(first["has_more"])
                self.assertEqual(first["next_after_id"], opted[1][0])

                self.assertEqual(
                    await delivery_cli._run(parse([
                        "agent-scheduler", "list", "--after-id",
                        str(first["next_after_id"]), "--limit", "2",
                    ])),
                    0,
                )
                second = emit.call_args.args[0]
                self.assertEqual(
                    [row["agente_id"] for row in second["agents"]], [opted[2][0]]
                )
                self.assertFalse(second["has_more"])
                self.assertIsNone(second["next_after_id"])

                async with self.sessions() as holder:
                    async with holder.begin():
                        await holder.execute(
                            select(Agente).where(Agente.id == opted[0][0]).with_for_update()
                        )
                        self.assertEqual(
                            await asyncio.wait_for(
                                delivery_cli._run(parse([
                                    "agent-scheduler", "list", "--after-id", "0",
                                    "--limit", "10",
                                ])),
                                timeout=1.0,
                            ),
                            0,
                        )
                        self.assertEqual(
                            [row["agente_id"] for row in emit.call_args.args[0]["agents"]],
                            [row[0] for row in opted],
                        )
        finally:
            event.remove(self.engine.sync_engine, "before_cursor_execute", record_statement)

        self.assertIn(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY", statements
        )

    # ── public API ──────────────────────────────────────────────────────
    async def test_public_api_never_exposes_flag(self) -> None:
        for schema in (agentes_router.AgenteCreateSchema, agentes_router.AgenteUpdateSchema,
                       agentes_router.AgenteResponseSchema):
            self.assertNotIn("inbox_scheduler_enabled", schema.model_fields, schema.__name__)
        body = {"nome": "t086 api", "instance_name": f"t086-api-{uuid4()}", "system_prompt": "p",
                "numero_notificacao": "0", "chatwoot_inbox_id": 7, "inbox_scheduler_enabled": True}
        async with self.sessions() as db:
            created = await agentes_router.criar_agente(
                agentes_router.AgenteCreateSchema.model_validate(body), db)
            await db.commit()
            agent = created.id
        self.assertFalse(await self._flag(agent))
        self.assertNotIn("inbox_scheduler_enabled",
                         agentes_router.AgenteResponseSchema.model_validate(created).model_dump())
        async with self.sessions() as db:
            with self.assertRaises(HTTPException) as caught:
                await agentes_router.atualizar_agente(
                    agent, agentes_router.AgenteUpdateSchema.model_validate(
                        {"inbox_scheduler_enabled": True}), db)
            self.assertEqual(caught.exception.status_code, 400)
        async with self.sessions() as db:
            await agentes_router.atualizar_agente(agent, agentes_router.AgenteUpdateSchema.model_validate(
                {"nome": "t086 renamed", "inbox_scheduler_enabled": True}), db)
            await db.commit()
        self.assertFalse(await self._flag(agent))
        await self._opt_in(agent)
        async with self.sessions() as db:
            await agentes_router.atualizar_agente(agent, agentes_router.AgenteUpdateSchema.model_validate(
                {"limite_mensal_creditos": Decimal("1"), "inbox_scheduler_enabled": False}), db)
            await db.commit()
        self.assertTrue(await self._flag(agent))
        paths = {route.path for route in agentes_router.router.routes}
        self.assertFalse(any("scheduler" in path or "opt" in path for path in paths), paths)

    # ── race: opt-in × enabled→draining (both orders, real InnoDB waits) ─
    async def test_opt_in_vs_drain_race_both_orders_repeated(self) -> None:
        agent, instance = await self._plain("race")
        refused_before = scheduler_repo.intake_state_refusal_count()
        for n in range(3):
            # Order A: opt-in holds runtime S → scheduler X → agent X; drain waits.
            await self._state("enabled")
            await self._set_agent(agent, ativo=True, inbox=7, opted=False)
            async with self.sessions() as optin:
                await agent_scheduler_repo.opt_in_agent(
                    optin, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                    delivery_epoch=self.delivery_epoch, provider_auth_enforced=True,
                    actor="t086", reason="order A")
                guard = asyncio.create_task(self._guard("draining"))
                await self._lock_wait()
                await optin.commit()
            self.assertEqual(await guard, ("ok", "draining"))
            self.assertTrue(await self._flag(agent))
            # Committed opt-in does not reopen intake in draining.
            receipts = await self._count(InboundEvent)
            with self.assertRaises(evolution_orchestrator.ScheduledIntakeUnavailable):
                await self._intake(instance, "5511000000086", f"t086-a-{n}", "late")
            self.assertEqual(await self._count(InboundEvent), receipts)
            # Order B: drain holds scheduler X; opt-in waits and is refused after commit.
            await self._state("enabled")
            await self._set_agent(agent, ativo=True, inbox=7, opted=False)
            audits = await self._count(DeliveryOperatorAction)
            async with self.sessions() as guard_db:
                await scheduler_repo.transition_scheduler_state(
                    guard_db, target="draining", scheduler_epoch=self.scheduler_epoch,
                    expected_version=10, actor="t086", reason="order B")

                async def opt_in_without_retry():
                    async with self.sessions() as db:
                        try:
                            await agent_scheduler_repo.opt_in_agent(
                                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                                delivery_epoch=self.delivery_epoch, provider_auth_enforced=True,
                                actor="t086", reason="order B")
                            await db.commit()
                            return ("ok",)
                        except agent_scheduler_repo.AgentSchedulerRefused as exc:
                            return ("refused", exc.reasons)
                        except DBAPIError as exc:
                            return ("dberror", scheduler_repo._mysql_error_code(exc))
                task = asyncio.create_task(opt_in_without_retry())
                await self._lock_wait()
                await guard_db.commit()
            self.assertEqual(await task, ("refused", ("scheduler_not_enabled",)))
            self.assertFalse(await self._flag(agent))
            self.assertEqual(await self._count(DeliveryOperatorAction), audits + 1)
            self.assertEqual((await self._one(SchedulerRuntimeState)).state, "draining")
        self.assertEqual(scheduler_repo.intake_state_refusal_count(), refused_before + 3)

    # ── PUT /agentes inbox guard (lifecycle UoW) ────────────────────────
    async def _put(self, agent: int, body: dict) -> int:
        async with self.sessions() as db:
            try:
                await agentes_router.atualizar_agente(
                    agent, agentes_router.AgenteUpdateSchema.model_validate(body), db)
                await db.commit()
                return 200
            except HTTPException as exc:
                await db.rollback()
                return exc.status_code

    async def _inbox(self, agent: int):
        async with self.sessions() as db:
            return (await db.execute(select(Agente.chatwoot_inbox_id).where(Agente.id == agent))).scalar_one()

    async def test_put_cannot_clear_inbox_of_opted_in_agent(self) -> None:
        from src.database.repositories import lifecycle_repo
        opted, _ = await self._plain("put-opted", opted=True)
        plain, _ = await self._plain("put-plain")
        cases = 0
        for body in ({"chatwoot_inbox_id": None}, {"chatwoot_inbox_id": None, "nome": "x"},
                     {"chatwoot_inbox_id": None, "ativo": False}):
            before = lifecycle_repo.lifecycle_counts()["opted_in_inbox_clear_refused"]
            self.assertEqual(await self._put(opted, body), 409)
            self.assertEqual(lifecycle_repo.lifecycle_counts()["opted_in_inbox_clear_refused"], before + 1)
            self.assertEqual(await self._inbox(opted), 7)
            cases += 1
        async with self.sessions() as db:
            row = await db.get(Agente, opted)
            self.assertEqual((row.nome, row.ativo, row.inbox_scheduler_enabled), ("t086 put-opted", True, True))
        self.assertEqual(await self._put(opted, {"chatwoot_inbox_id": 9}), 200)  # swap allowed
        self.assertEqual(await self._inbox(opted), 9)
        self.assertEqual(await self._put(opted, {"nome": "kept"}), 200)  # inbox untouched
        self.assertEqual(await self._inbox(opted), 9)
        self.assertEqual(await self._put(plain, {"chatwoot_inbox_id": None}), 200)  # not opted in
        self.assertIsNone(await self._inbox(plain))
        # Opt out first, then clearing is allowed.
        await self._opt_out(opted)
        self.assertEqual(await self._put(opted, {"chatwoot_inbox_id": None}), 200)
        self.assertIsNone(await self._inbox(opted))
        self.assertEqual(cases + 4, 7)

    async def test_put_inbox_clear_vs_opt_in_race_both_orders_repeated(self) -> None:
        from src.database.repositories import lifecycle_repo
        agent, _ = await self._plain("put-race")
        for n in range(3):
            # Order A: opt-in holds scheduler X + agent X; PUT waits, then sees opt-in → 409.
            await self._set_agent(agent, ativo=True, inbox=7, opted=False)
            async with self.sessions() as optin:
                await agent_scheduler_repo.opt_in_agent(
                    optin, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                    delivery_epoch=self.delivery_epoch, provider_auth_enforced=True,
                    actor="t086", reason="race A")
                put = asyncio.create_task(self._put(agent, {"chatwoot_inbox_id": None}))
                await self._lock_wait()
                await optin.commit()
            self.assertEqual(await put, 409)
            self.assertEqual((await self._flag(agent), await self._inbox(agent)), (True, 7))
            # Order B: PUT clears inbox holding the lifecycle prefix; opt-in waits → refused.
            await self._set_agent(agent, ativo=True, inbox=7, opted=False)
            async with self.sessions() as put_db:
                await lifecycle_repo.update_agent(put_db, agente_id=agent,
                                                  payload={"chatwoot_inbox_id": None})

                async def opt_in_without_retry():
                    async with self.sessions() as db:
                        try:
                            await agent_scheduler_repo.opt_in_agent(
                                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                                delivery_epoch=self.delivery_epoch, provider_auth_enforced=True,
                                actor="t086", reason="race B")
                            await db.commit()
                            return ("ok",)
                        except agent_scheduler_repo.AgentSchedulerRefused as exc:
                            return ("refused", exc.reasons)
                        except DBAPIError as exc:
                            return ("dberror", scheduler_repo._mysql_error_code(exc))
                task = asyncio.create_task(opt_in_without_retry())
                await self._lock_wait()
                await put_db.commit()
            self.assertEqual(await task, ("refused", ("chatwoot_inbox_missing",)))
            self.assertEqual((await self._flag(agent), await self._inbox(agent)), (False, None))

    async def test_documented_contract_parity(self) -> None:
        doc = (Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2])) / "AGENTS.md").read_text()
        self.assertIn("agent-scheduler opt-in", doc)
        self.assertIn("opted_in_inbox_clear_refused", doc)
        for name in agent_scheduler_repo.OPT_IN_REFUSALS + agent_scheduler_repo.OPT_OUT_REFUSALS:
            self.assertIn(f"`{name}`", doc)
        self.assertEqual(agent_scheduler_repo.OPT_IN_SCHEDULER_STATES, frozenset({"enabled"}))
        self.assertEqual(agent_scheduler_repo.OPT_IN_RUNTIME_MODES, frozenset({"outbox"}))


for _name in dir(drain.SchedulerDrainMySQLTests):
    if _name.startswith("test_") and _name not in AgentSchedulerOptInMySQLTests.__dict__:
        setattr(AgentSchedulerOptInMySQLTests, _name, None)
