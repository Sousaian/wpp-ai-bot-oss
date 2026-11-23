"""Ticket 07 real-MySQL lifecycle and operator fencing.

Every lifecycle action races a real in-flight state (active generation,
typing wait, claimed send) at a deterministic barrier. Providers are
in-process fakes; helpers come from the ticket06 suite, whose own cases are
not re-run here.
"""

import asyncio
import os
from datetime import timedelta
import re
from decimal import Decimal
import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import event, func, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError

from src.database.models import (
    AgentServiceLane,
    Agente,
    Atendente,
    ConsumoLLM,
    ConversationTurn,
    DeliveryOperatorAction,
    GenerationAttempt,
    HandoffAttempt,
    OutboxDelivery,
    Sessao,
    TurnRevision,
)
from src.database.repositories import (
    consumo_repo,
    delivery_state_repo,
    delivery_worker_repo,
    lifecycle_repo,
    scheduler_repo,
    turn_delivery_repo,
    turn_generation_repo,
)
from src.routers import agentes as agentes_router
from src.routers import sessoes as sessoes_router
from src.services import llm_client
from tests import auth_fixtures
from tests import test_turn_delivery_mysql as t06


FakeLLM = t06.FakeLLM
ForbiddenLLM = t06.ForbiddenLLM
MODEL = "openai/gpt-4o-mini"


class LifecycleMySQLTests(t06.TurnDeliveryMySQLTests):
    # ── helpers ─────────────────────────────────────────────────────────
    async def _transfer(self, agent: int, phone: str) -> dict:
        async with self.sessions() as db:
            result = await sessoes_router.transferir_sessao(
                telefone=phone, agente_id=agent, motivo=None, db=db, principal=auth_fixtures.ADMIN
            )
            await db.commit()
        return result

    async def _resume(self, agent: int, phone: str) -> dict:
        async with self.sessions() as db:
            result = await sessoes_router.retomar_sessao(telefone=phone, agente_id=agent, db=db, principal=auth_fixtures.ADMIN)
            await db.commit()
        return result

    async def _disable(self, agent: int, **values) -> object:
        values = values or {"ativo": False}
        async with self.sessions() as db:
            result = await agentes_router.atualizar_agente(
                agente_id=agent, dados=agentes_router.AgenteUpdateSchema(**values), db=db, principal=auth_fixtures.ADMIN
            )
            await db.commit()
        return result

    async def _status(self, http: int, coroutine_factory, detail: str | None = None) -> None:
        async with self.sessions() as db:
            with self.assertRaises(HTTPException) as raised:
                await coroutine_factory(db)
            self.assertEqual(raised.exception.status_code, http)
            if detail is not None:
                self.assertIn(detail, raised.exception.detail)

    async def _one_turn(self) -> ConversationTurn:
        return (await self._turns())[0]

    async def _lane(self, agent: int) -> AgentServiceLane:
        async with self.sessions() as db:
            return await db.get(AgentServiceLane, agent)

    async def _session(self) -> Sessao:
        return (await self._rows(Sessao, Sessao.id))[0]

    async def _start_generation_barrier(self, suffix: str):
        """Processor holds a started generation attempt inside the fake LLM."""

        agent, instance = await self._agent(suffix)
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, f"{suffix}-1", "oi")
        await self._force_ready()
        llm = FakeLLM("resposta que chega tarde")
        llm.barrier = asyncio.Event()
        processor = self._router_processor(llm)
        tasks = await processor.run_pass()
        self.assertEqual(len(tasks), 1)
        await asyncio.wait_for(llm.entered.wait(), timeout=20)
        return agent, instance, phone, llm, tasks[0]

    async def _start_typing_barrier(self, suffix: str):
        agent, instance = await self._agent(suffix)
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, f"{suffix}-1", "oi")
        await self._force_ready()
        processor = self._router_processor(FakeLLM("resposta digitando"))
        original_wait = processor._wait
        at_typing = asyncio.Event()
        release = asyncio.Event()

        async def hold_typing(lease, step):
            if step.phase == "typing" and not at_typing.is_set():
                at_typing.set()
                await release.wait()
            await original_wait(lease, step)

        processor._wait = hold_typing
        tasks = await processor.run_pass()
        await asyncio.wait_for(at_typing.wait(), timeout=20)
        return agent, instance, phone, release, tasks[0]

    async def _assert_lost_generation_accounted(self, task) -> None:
        self.assertEqual(await asyncio.wait_for(task, timeout=30), "lost")
        attempt = (await self._rows(GenerationAttempt, GenerationAttempt.id))[0]
        self.assertEqual((attempt.outcome, attempt.accounting_state), ("succeeded", "recorded"))
        self.assertEqual(await self._count(ConsumoLLM), 1)

    async def _recover_with_hint(self, agent: int) -> str:
        async with self.sessions() as db:
            hints = await turn_generation_repo.list_expired_lane_hints(db, limit=10)
        hint = next(((t, s) for a, t, s in hints if a == agent), None)
        self.assertIsNotNone(hint, "expired lane must be discoverable")

        async def operation(db):
            now = await delivery_state_repo.get_database_now(db)
            return await scheduler_repo.recover_expired_lane(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                retry_at=now + timedelta(seconds=1),
                expected_turn_id=hint[0],
                expected_sessao_id=hint[1],
            )

        return await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, operation)

    # ── /transferir and /retomar at every barrier ───────────────────────
    async def _assert_cancelled_keeps_lane_then_recovers(self, agent: int, reason: str, llm, task):
        turn = await self._one_turn()
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason), ("cancelled", "terminal", reason)
        )
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual(revision.status, "cancelled")
        lane = await self._lane(agent)
        # In-flight generation: the owner keeps the lane for exact accounting.
        self.assertEqual(lane.owner_token, turn.claim_token)
        llm.barrier.set()
        await self._assert_lost_generation_accounted(task)
        self.assertEqual(await self._automated(), [])
        self.assertEqual(await self._count(TurnRevision, TurnRevision.status == "typing"), 0)
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_with_hint(agent), "idle")
        self.assertIsNone((await self._lane(agent)).owner_token)
        self.assertIsNone((await self._one_turn()).claim_token)

    async def test_transfer_during_active_generation_keeps_lane_for_accounting(self) -> None:
        agent, _, phone, llm, task = await self._start_generation_barrier("tr-gen")
        await self._transfer(agent, phone)
        self.assertEqual(
            ((await self._session()).handler, (await self._session()).motivo_encerramento),
            ("encerrada", "transferencia_manual"),
        )
        await self._assert_cancelled_keeps_lane_then_recovers(agent, "session_transferred", llm, task)

    async def test_resume_during_active_generation_keeps_lane_for_accounting(self) -> None:
        agent, _, phone, llm, task = await self._start_generation_barrier("rs-gen")
        await self._resume(agent, phone)
        self.assertEqual((await self._session()).handler, "bot")
        await self._assert_cancelled_keeps_lane_then_recovers(agent, "session_resumed", llm, task)

    async def test_disable_during_active_generation_keeps_lane_for_accounting(self) -> None:
        agent, _, _, llm, task = await self._start_generation_barrier("ds-gen")
        await self._disable(agent)
        await self._assert_cancelled_keeps_lane_then_recovers(agent, "agent_disabled", llm, task)

    async def _assert_typing_cancelled(self, agent: int, reason: str, release, task) -> None:
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.terminal_reason, turn.claim_token), ("cancelled", reason, None))
        self.assertIsNone((await self._lane(agent)).owner_token)
        release.set()
        self.assertEqual(await asyncio.wait_for(task, timeout=30), "lost")
        self.assertEqual(await self._automated(), [])
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual(revision.status, "cancelled")

    async def test_transfer_during_typing_cancels_without_send(self) -> None:
        agent, _, phone, release, task = await self._start_typing_barrier("tr-typ")
        await self._transfer(agent, phone)
        await self._assert_typing_cancelled(agent, "session_transferred", release, task)

    async def test_resume_during_typing_cancels_without_send(self) -> None:
        agent, _, phone, release, task = await self._start_typing_barrier("rs-typ")
        await self._resume(agent, phone)
        await self._assert_typing_cancelled(agent, "session_resumed", release, task)

    async def test_disable_during_typing_cancels_without_send(self) -> None:
        agent, _, _, release, task = await self._start_typing_barrier("ds-typ")
        await self._disable(agent)
        await self._assert_typing_cancelled(agent, "agent_disabled", release, task)

    async def _claimed_send(self, suffix: str):
        agent, instance, phone, _ = await self._ordinary_admitted(suffix)
        claimed = await self._claim()
        self.assertEqual(len(claimed), 1)
        self.assertTrue(await self._preio(claimed[0]))
        return agent, instance, phone, claimed[0]

    async def _assert_delivered_after_cancel(self, agent: int, claimed) -> None:
        row = (await self._automated())[0]
        self.assertEqual((row.status, row.lease_token is not None), ("processing", True))
        self.assertIsNotNone(row.cancel_requested_at)
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.session_slot, turn.claim_token), ("awaiting_delivery", "active", None))
        self.assertIsNone((await self._lane(agent)).owner_token)
        # The in-flight send records the truth; the turn settles; no resend.
        self.assertTrue(await self._complete(claimed))
        row = (await self._automated())[0]
        self.assertEqual((row.status, row.terminal_reason, row.ambiguity_count), ("delivered", "delivered_after_cancel", 0))
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.session_slot), ("delivered", "terminal"))
        self.assertEqual(await self._automated_claims(), [])

    async def test_transfer_during_claimed_send_fences_and_records_delivery(self) -> None:
        agent, _, phone, claimed = await self._claimed_send("tr-send")
        await self._transfer(agent, phone)
        await self._assert_delivered_after_cancel(agent, claimed)

    async def test_resume_during_claimed_send_fences_and_records_delivery(self) -> None:
        agent, _, phone, claimed = await self._claimed_send("rs-send")
        await self._resume(agent, phone)
        await self._assert_delivered_after_cancel(agent, claimed)

    async def test_disable_during_claimed_send_fences_and_records_delivery(self) -> None:
        agent, _, _, claimed = await self._claimed_send("ds-send")
        await self._disable(agent)
        await self._assert_delivered_after_cancel(agent, claimed)

    async def test_transfer_cancels_queued_reply_and_successor_in_same_uow(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("tr-queue")
        await self._accept(instance, phone, "tr-queue-2", "sucessor")
        before = lifecycle_repo.lifecycle_counts()["turns_cancelled"]
        await self._transfer(agent, phone)
        self.assertEqual(lifecycle_repo.lifecycle_counts()["turns_cancelled"], before + 2)
        self.assertEqual(
            [(t.status, t.session_slot, t.terminal_reason) for t in await self._turns()],
            [
                ("cancelled", "terminal", "session_transferred"),
                ("cancelled", "terminal", "session_transferred"),
            ],
        )
        row = (await self._automated())[0]
        self.assertEqual((row.status, row.terminal_reason), ("cancelled", "cancel_requested"))
        self.assertIsNone((await self._lane(agent)).owner_token)
        self.assertEqual(await self._automated_claims(), [])

    async def test_resume_after_admitted_handoff_settles_in_same_uow(self) -> None:
        agent, instance, phone, _, _ = await self._run_handoff("rs-handoff")
        await self._resume(agent, phone)
        turn = await self._one_turn()
        # Settled by /retomar itself, no discovery pass needed.
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("cancelled", "terminal", "handoff_superseded"),
        )
        self.assertEqual({r.status for r in await self._automated()}, {"cancelled"})
        attempt = (await self._rows(HandoffAttempt, HandoffAttempt.id))[0]
        self.assertEqual(attempt.status, "superseded")
        self.assertEqual(
            {r.status for r in await self._rows(OutboxDelivery, OutboxDelivery.id)}, {"cancelled"}
        )
        await self._answer_new_inbound(instance, phone, "rs-handoff-new")

    async def test_resume_with_handoff_reply_in_flight_settles_on_completion(self) -> None:
        agent, _, phone, _, _ = await self._run_handoff("rs-hreply")
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._automated_claims()
        self.assertTrue(await self._preio(claimed[0]))
        await self._resume(agent, phone)
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.session_slot), ("awaiting_delivery", "active"))
        reply, transition = await self._automated()
        self.assertEqual((reply.status, transition.status), ("processing", "cancelled"))
        self.assertTrue(await self._complete(claimed[0]))
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.terminal_reason), ("cancelled", "handoff_superseded"))
        reply = (await self._automated())[0]
        self.assertEqual(reply.terminal_reason, "delivered_after_cancel")

    async def test_resume_on_bot_session_matches_legacy_cancel_of_queued_work(self) -> None:
        agent, _, phone, _ = await self._ordinary_admitted("rs-bot")
        await self._resume(agent, phone)
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.terminal_reason), ("cancelled", "session_resumed"))
        self.assertEqual((await self._automated())[0].status, "cancelled")

    # ── agent update / disable ──────────────────────────────────────────
    async def test_disable_cancels_every_session_and_reenable_never_replays(self) -> None:
        agent, instance = await self._agent("ds-many")
        for index in range(2):
            await self._accept(instance, f"5511{agent:06d}{index:02d}", f"ds-many-{index}", "oi")
        await self._disable(agent)
        self.assertEqual(
            [(t.status, t.terminal_reason) for t in await self._turns()],
            [("cancelled", "agent_disabled")] * 2,
        )
        await self._disable(agent, ativo=True)
        await self._force_ready()
        self.assertEqual(await self._router_processor(ForbiddenLLM()).run_pass(), [])

    async def test_opt_out_with_pending_scheduled_work_is_refused(self) -> None:
        agent, instance = await self._agent("opt-out")
        await self._accept(instance, f"5511{agent:08d}", "opt-out-1", "oi")
        async with self.sessions() as db:
            with self.assertRaises(lifecycle_repo.LifecycleRefused):
                await lifecycle_repo.run_with_deadlock_retry(
                    db,
                    lambda tx: lifecycle_repo.update_agent(
                        tx, agente_id=agent, payload={"inbox_scheduler_enabled": False}
                    ),
                )
            await db.rollback()
        self.assertTrue((await self._rows(Agente, Agente.id))[0].inbox_scheduler_enabled)
        self.assertEqual((await self._one_turn()).status, "pending")
        # With the work settled (agent disabled), the opt-out is accepted.
        await self._disable(agent)
        async with self.sessions() as db:
            result = await lifecycle_repo.run_with_deadlock_retry(
                db,
                lambda tx: lifecycle_repo.update_agent(
                    tx, agente_id=agent, payload={"inbox_scheduler_enabled": False}
                ),
            )
            await db.commit()
        self.assertFalse(result.agent.inbox_scheduler_enabled)

    async def test_update_derives_fields_from_fresh_row_not_preloaded_identity(self) -> None:
        agent, _ = await self._agent("fresh-update")
        async with self.sessions() as db:
            preloaded = await db.get(Agente, agent)
            self.assertEqual(preloaded.modelo_openai, "gpt-4o-mini")
            async with self.sessions() as other:
                await other.execute(
                    update(Agente).where(Agente.id == agent).values(modelo_openai="gemini-2.5-flash")
                )
                await other.commit()
            # Same open transaction: the preloaded identity is stale.
            self.assertEqual(preloaded.modelo_openai, "gpt-4o-mini")

            def prepare(row, values):
                values["modelo_llm"] = "google/" + row.modelo_openai
                return values

            result = await lifecycle_repo.run_with_deadlock_retry(
                db,
                lambda tx: lifecycle_repo.update_agent(
                    tx, agente_id=agent, payload={"nome": "novo"}, prepare=prepare
                ),
            )
            await db.commit()
        self.assertEqual(result.agent.modelo_llm, "google/gemini-2.5-flash")

    async def test_pending_write_is_refused_never_overwritten(self) -> None:
        agent, _ = await self._agent("pending-write")
        async with self.sessions() as db:
            row = await db.get(Agente, agent)
            row.nome = "escrita pendente"
            with self.assertRaises(lifecycle_repo.PendingWritesError):
                await lifecycle_repo.update_agent(db, agente_id=agent, payload={"ativo": False})
            self.assertEqual(row.nome, "escrita pendente")
            await db.rollback()
        stored = (await self._rows(Agente, Agente.id))[0]
        self.assertEqual((stored.nome, stored.ativo), ("Ticket05 pending-write", True))

    # ── deletes ─────────────────────────────────────────────────────────
    async def test_delete_with_scheduler_history_returns_409_and_keeps_rows(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("del-hist")
        claimed = await self._claim()
        self.assertTrue(await self._complete(claimed[0]))
        await self._status(
            409,
            lambda db: sessoes_router.deletar_sessao(telefone=phone, agente_id=agent, db=db, principal=auth_fixtures.ADMIN),
            lifecycle_repo.SCHEDULER_HISTORY_MESSAGE,
        )
        await self._status(
            409,
            lambda db: agentes_router.deletar_agente(agente_id=agent, db=db, principal=auth_fixtures.ADMIN),
            lifecycle_repo.SCHEDULER_HISTORY_MESSAGE,
        )
        self.assertEqual(await self._count(Sessao), 1)
        self.assertEqual(await self._count(ConversationTurn), 1)

    async def test_delete_with_pending_delivery_keeps_409(self) -> None:
        agent, _, phone, _ = await self._ordinary_admitted("del-pending")
        await self._status(
            409,
            lambda db: sessoes_router.deletar_sessao(telefone=phone, agente_id=agent, db=db, principal=auth_fixtures.ADMIN),
        )

    async def test_delete_without_history_still_deletes(self) -> None:
        agent, _ = await self._agent("del-clean")
        async with self.sessions() as db:
            db.add(Sessao(agente_id=agent, telefone="5511000099", delivery_stream_key=str(uuid4())))
            await db.commit()
        async with self.sessions() as db:
            await sessoes_router.deletar_sessao(telefone="5511000099", agente_id=agent, db=db, principal=auth_fixtures.ADMIN)
            await db.commit()
        async with self.sessions() as db:
            await agentes_router.deletar_agente(agente_id=agent, db=db, principal=auth_fixtures.ADMIN)
            await db.commit()
        self.assertEqual((await self._count(Sessao), await self._count(Agente)), (0, 0))

    async def test_delete_restrict_dependent_is_409_not_500(self) -> None:
        """Any retained RESTRICT dependent (C1) refuses with 409, never 1451/500."""

        agent, _ = await self._agent("del-restrict")
        async with self.sessions() as db:
            session = Sessao(agente_id=agent, telefone="5511000098", delivery_stream_key=str(uuid4()))
            db.add(session)
            await db.flush()
            await db.execute(
                text("INSERT INTO mensagens (sessao_id, role, conteudo) VALUES (:s, 'user', 'x')"),
                {"s": session.id},
            )
            await db.execute(
                text(
                    "INSERT INTO inbound_events (provider, source_scope, provider_event_id, "
                    "event_type, payload_hmac, hmac_key_version, agente_id, sessao_id, "
                    "receipt_disposition, mensagem_id) SELECT 'evolution', 'scope', :e, "
                    "'messages.upsert', REPEAT('a', 64), 1, :a, :s, 'legacy_unknown', id "
                    "FROM mensagens WHERE sessao_id=:s"
                ),
                {"e": f"restrict-{uuid4()}", "a": agent, "s": session.id},
            )
            await db.commit()
        await self._status(
            409,
            lambda db: sessoes_router.deletar_sessao(telefone="5511000098", agente_id=agent, db=db, principal=auth_fixtures.ADMIN),
        )
        await self._status(409, lambda db: agentes_router.deletar_agente(agente_id=agent, db=db, principal=auth_fixtures.ADMIN))
        self.assertEqual((await self._count(Sessao), await self._count(Agente)), (1, 1))

    # ── audited operator paths ──────────────────────────────────────────
    async def _operator_cancel(self, delivery_id: int):
        return await lifecycle_repo.run_in_new_session(
            self.sessions,
            lambda db: lifecycle_repo.operator_cancel_automated(
                db, delivery_id=delivery_id, actor="ops", reason="ticket07 test"
            ),
        )

    async def test_operator_cancel_automated_settles_turn_and_promotes(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("op-cancel")
        await self._accept(instance, phone, "op-cancel-2", "sucessor")
        row = (await self._automated())[0]
        cancelled = await self._operator_cancel(row.id)
        self.assertEqual((cancelled.status, cancelled.terminal_reason), ("cancelled", "operator_cancelled"))
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()],
            [("dead", "terminal"), ("pending", "active")],
        )
        action = (await self._rows(DeliveryOperatorAction, DeliveryOperatorAction.id))[-1]
        self.assertEqual(
            (action.action, action.delivery_id, action.snapshot_cutover_epoch, action.snapshot_status),
            ("cancel", row.id, self.delivery_epoch, "cancelled"),
        )

    async def test_operator_cancel_automated_works_while_frozen(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("op-frozen")
        async with self.sessions() as db:
            await db.execute(text("UPDATE delivery_runtime_state SET mode='frozen'"))
            await db.commit()
        row = (await self._automated())[0]
        await self._operator_cancel(row.id)
        self.assertEqual((await self._one_turn()).status, "dead")
        async with self.sessions() as db:
            readiness = await delivery_state_repo.get_rollback_readiness(db)
        self.assertEqual(readiness.nonterminal, 0)

    async def test_operator_cancel_in_flight_only_requests_cancellation(self) -> None:
        agent, _, _, claimed = await self._claimed_send("op-live")
        row = await self._operator_cancel(claimed.id)
        self.assertEqual(row.status, "processing")
        self.assertIsNotNone(row.cancel_requested_at)
        self.assertTrue(await self._complete(claimed))
        self.assertEqual((await self._automated())[0].terminal_reason, "delivered_after_cancel")

    async def test_legacy_operator_paths_refuse_automated_rows(self) -> None:
        await self._ordinary_admitted("op-legacy")
        row = (await self._automated())[0]
        async with self.sessions() as db:
            with self.assertRaisesRegex(ValueError, "lane-fenced lifecycle"):
                await delivery_worker_repo.operator_cancel(db, delivery_id=row.id, actor="a", reason="r")
            await db.rollback()
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery).where(OutboxDelivery.id == row.id).values(
                    status="dead", terminal_at=func.utc_timestamp(6), terminal_reason="x"
                )
            )
            await db.commit()
        async with self.sessions() as db:
            with self.assertRaisesRegex(ValueError, "turn-resolve"):
                await delivery_worker_repo.operator_retry(
                    db, delivery_id=row.id, actor="a", reason="r", ttl_seconds=60
                )
            await db.rollback()

    async def _parked_handoff(self, suffix: str, *, ambiguity: int = 0):
        agent, instance, phone, _, _ = await self._run_handoff(suffix)
        reply = (await self._automated())[0]
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery).where(OutboxDelivery.id == reply.id).values(
                    status="dead", terminal_at=func.utc_timestamp(6),
                    terminal_reason="http_503", ambiguity_count=ambiguity,
                )
            )
            await db.execute(
                update(ConversationTurn).values(delivery_attempt_count=8)
            )
            await db.commit()
        return agent, instance, phone, reply

    async def _turn_resolve(self, resolution: str, **kwargs):
        turn = await self._one_turn()
        return await lifecycle_repo.run_in_new_session(
            self.sessions,
            lambda db: lifecycle_repo.resolve_parked_turn(
                db, turn_id=turn.id, resolution=resolution, actor="ops",
                reason="ticket07 test", ttl_seconds=600, **kwargs,
            ),
        )

    async def test_turn_resolve_retry_rearms_parked_handoff_reply(self) -> None:
        agent, _, _, reply = await self._parked_handoff("tr-retry")
        result = await self._turn_resolve("retry")
        self.assertEqual(result.rearmed, (reply.id,))
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.delivery_attempt_count), ("awaiting_delivery", 0))
        # The lane-owned admission sends it; no generation.
        self.assertEqual(await self._run_one(self._router_processor(ForbiddenLLM())), ["final_candidate"])
        claimed = await self._automated_claims()
        self.assertEqual([c.id for c in claimed], [reply.id])
        action = (await self._rows(DeliveryOperatorAction, DeliveryOperatorAction.id))[-1]
        self.assertEqual((action.action, action.snapshot_cutover_epoch), ("turn_resolve:retry", self.delivery_epoch))

    async def test_turn_resolve_retry_requires_current_handoff_generation(self) -> None:
        await self._parked_handoff("tr-gen")
        async with self.sessions() as db:
            await db.execute(text("UPDATE sessoes SET handoff_generation=handoff_generation+1"))
            await db.commit()
        with self.assertRaisesRegex(lifecycle_repo.LifecycleRefused, "current handoff generation"):
            await self._turn_resolve("retry")
        self.assertEqual((await self._automated())[0].status, "dead")

    async def test_turn_resolve_ambiguous_effect_requires_explicit_risk(self) -> None:
        await self._parked_handoff("tr-amb", ambiguity=1)
        with self.assertRaisesRegex(lifecycle_repo.LifecycleRefused, "duplicate-risk"):
            await self._turn_resolve("retry")
        result = await self._turn_resolve("retry", accept_duplicate_risk=True)
        self.assertEqual(len(result.rearmed), 1)

    async def test_turn_resolve_abandon_terminalizes_parked_turn(self) -> None:
        await self._parked_handoff("tr-abandon")
        result = await self._turn_resolve("abandon")
        self.assertEqual(result.status, "cancelled")
        turn = await self._one_turn()
        self.assertEqual((turn.session_slot, turn.terminal_reason), ("terminal", "operator_abandoned"))
        self.assertEqual({r.status for r in await self._automated()}, {"dead", "cancelled"})
        self.assertEqual((await self._session()).motivo_encerramento, "handoff")

    async def test_processing_cancel_matrix(self) -> None:
        cases = 0
        leases = ("live", "expired", "absent") if os.getenv("T07_ALLOW_ABSENT_LEASE") == "1" else ("live", "expired")
        for action in ("cancel", "transfer", "resume", "disable"):
            for mode in ("outbox", "frozen"):
                for lease in leases:
                    with self.subTest(action=action, mode=mode, lease=lease):
                        if cases:
                            await self.asyncTearDown()
                            await self.asyncSetUp()
                        agent, _, phone, claimed = await self._claimed_send("cancel-matrix")
                        async with self.sessions() as db:
                            deadline = {"live": "UTC_TIMESTAMP(6)+INTERVAL 120 SECOND",
                                        "expired": "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND",
                                        "absent": "NULL"}[lease]
                            await db.execute(text(f"UPDATE outbox_deliveries SET lock_until={deadline} WHERE id=:id"), {"id": claimed.id})
                            await db.execute(text("UPDATE delivery_runtime_state SET mode=:mode"), {"mode": mode})
                            await db.commit()
                        if action == "cancel":
                            await self._operator_cancel(claimed.id)
                        elif action == "transfer":
                            await self._transfer(agent, phone)
                        elif action == "resume":
                            if mode == "frozen":
                                with self.assertRaises(HTTPException) as refused:
                                    await self._resume(agent, phone)
                                self.assertEqual(refused.exception.status_code, 503)
                                await lifecycle_repo.run_in_new_session(
                                    self.sessions, lambda db: lifecycle_repo.resume_session(
                                        db, agente_id=agent, telefone=phone
                                    ),
                                )
                            else:
                                await self._resume(agent, phone)
                        else:
                            await self._disable(agent)
                        row = (await self._automated())[0]
                        turn = await self._one_turn()
                        lane = await self._lane(agent)
                        async with self.sessions() as db:
                            readiness = await delivery_state_repo.get_rollback_readiness(db)
                        self.assertIsNotNone(row.cancel_requested_at)
                        if lease == "live":
                            self.assertEqual((row.status, row.ambiguity_count), ("processing", 0))
                            self.assertEqual(turn.session_slot, "active")
                            if action == "cancel":
                                self.assertIsNotNone(lane.owner_token)
                            self.assertEqual(readiness.nonterminal, 1)
                        else:
                            self.assertEqual((row.status, row.ambiguity_count, row.last_error_code),
                                             ("cancelled", 1, "lease_expired_ambiguous"))
                            self.assertIsNone(row.lease_token)
                            self.assertIsNone(row.lock_until)
                            self.assertEqual(turn.session_slot, "terminal")
                            self.assertIsNone(lane.owner_token)
                            self.assertEqual(readiness.nonterminal, 0)
                        cases += 1
        self.assertEqual(cases, 8 * len(leases))

    async def test_live_abandon_finishes_after_owner_delivery(self) -> None:
        await self._run_handoff("live-abandon-finalize")
        self.assertEqual(await self._run_one(self._router_processor(ForbiddenLLM())), ["final_candidate"])
        claimed = await self._automated_claims()
        self.assertEqual(len(claimed), 1)
        self.assertTrue(await self._preio(claimed[0]))
        await self._turn_resolve("abandon")
        self.assertEqual((await self._one_turn()).status, "awaiting_delivery")
        async with self.sessions() as db:
            await db.execute(text("UPDATE delivery_operator_actions SET created_at=UTC_TIMESTAMP(6)-INTERVAL 100 DAY"))
            retained = await delivery_worker_repo.cleanup_retention(db)
            self.assertGreater(retained.abandon_intents_retained, 0)
            await db.commit()
        self.assertTrue(await self._complete(claimed[0]))
        turn = await self._one_turn()
        self.assertEqual((turn.status, turn.session_slot, turn.terminal_reason),
                         ("cancelled", "terminal", "operator_abandoned"))
        self.assertIsNone((await self._lane(turn.agente_id)).owner_token)

    async def test_handoff_processing_cancel_matrix(self) -> None:
        import itertools

        cases = 0
        for action, mode, live, effect in itertools.product(
            ("cancel", "abandon", "transfer", "resume", "disable"),
            ("outbox", "frozen"), (True, False), ("reply", "handoff_transition"),
        ):
            with self.subTest(action=action, mode=mode, live=live, effect=effect):
                if cases:
                    await self.asyncTearDown()
                    await self.asyncSetUp()
                agent, _, phone, _, _ = await self._run_handoff("handoff-cancel-matrix")
                target = next(r for r in await self._automated() if r.automated_effect == effect)
                async with self.sessions() as db:
                    await db.execute(text(
                        "UPDATE outbox_deliveries SET status='processing', lease_token=:token, "
                        "lock_owner='test-worker', lock_until=TIMESTAMPADD(SECOND,:seconds,UTC_TIMESTAMP(6)), "
                        "next_attempt_at=UTC_TIMESTAMP(6), expires_at=UTC_TIMESTAMP(6)+INTERVAL 1 HOUR "
                        "WHERE id=:id"
                    ), {"id": target.id, "token": str(uuid4()), "seconds": 120 if live else -1})
                    await db.execute(text("UPDATE delivery_runtime_state SET mode=:mode"), {"mode": mode})
                    await db.commit()
                if action == "cancel":
                    # Resolve each effect; cancelling one does not erase its sibling.
                    for row in await self._automated():
                        await self._operator_cancel(row.id)
                elif action == "abandon":
                    await self._turn_resolve("abandon")
                elif action == "transfer":
                    await self._transfer(agent, phone)
                elif action == "resume":
                    if mode == "frozen":
                        with self.assertRaises(HTTPException) as refused:
                            await self._resume(agent, phone)
                        self.assertEqual(refused.exception.status_code, 503)
                        await lifecycle_repo.run_in_new_session(
                            self.sessions, lambda db: lifecycle_repo.resume_session(
                                db, agente_id=agent, telefone=phone
                            ),
                        )
                    else:
                        await self._resume(agent, phone)
                else:
                    await self._disable(agent)
                target = next(r for r in await self._automated() if r.id == target.id)
                self.assertIsNotNone(target.cancel_requested_at)
                self.assertEqual(target.status, "processing" if live else "cancelled")
                self.assertEqual(target.ambiguity_count, 0 if live else 1)
                turn = await self._one_turn()
                if live:
                    self.assertEqual(turn.status, "awaiting_delivery")
                else:
                    if action == "cancel":
                        # Cancelling delivery is not abandoning the current Chatwoot
                        # handoff. Existing contract parks it for explicit resolution.
                        self.assertEqual(turn.status, "awaiting_delivery")
                        await self._turn_resolve("abandon")
                        turn = await self._one_turn()
                    self.assertEqual(turn.session_slot, "terminal")
                    self.assertIsNone((await self._lane(agent)).owner_token)
                cases += 1
        self.assertEqual(cases, 40)

    # ── generation quarantine list / inspect / resolve ──────────────────
    async def _quarantined_lane(self, suffix: str):
        """Owner crashed with a started attempt; drain lapsed; lane stays owned."""

        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_runtime_state SET accounting_drain_seconds=1"))
            await db.commit()
        agent, instance = await self._agent(suffix)
        await self._accept(instance, f"5511{agent:08d}", f"{suffix}-1", "oi")
        await self._force_ready()
        async with self.sessions() as db:
            hint = await turn_generation_repo.ready_turn_hint(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch
            )
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch, worker_id="crashed",
                lease_seconds=30, candidate=hint,
            ),
        )
        lease = turn_generation_repo.TurnLease.from_claim(claim)
        policy = self._router_processor(ForbiddenLLM()).policy
        step = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: turn_generation_repo.advance_turn(db, lease=lease, policy=policy),
        )
        self.assertIsInstance(step, turn_generation_repo.GenerateStep)
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_with_hint(agent), "quarantined")
        await asyncio.sleep(1.2)
        self.assertEqual(await self._recover_with_hint(agent), "quarantined")
        return agent, step.attempt_key

    async def _resolve(self, key: str, outcome: str, **kwargs):
        return await lifecycle_repo.run_in_new_session(
            self.sessions,
            lambda db: lifecycle_repo.resolve_generation_quarantine(
                db, attempt_key=key, outcome=outcome, evidence="provider_generation_lookup",
                actor="ops", note="ticket07 test", **kwargs,
            ),
        )

    async def test_quarantine_failed_resolution_releases_lane_only_after_resolve(self) -> None:
        agent, key = await self._quarantined_lane("q-failed")
        async with self.sessions() as db:
            rows = await lifecycle_repo.list_generation_quarantine(db)
            inspected = await lifecycle_repo.inspect_generation_attempt(db, attempt_key=key)
            hints = await turn_generation_repo.list_expired_lane_hints(db, limit=10)
        self.assertEqual([r.attempt_key for r in rows], [key])
        self.assertEqual(rows[0].outcome, "outcome_unknown")
        self.assertTrue(inspected["lane_owned_by_attempt"])
        self.assertEqual(hints, [])  # quarantined lane is not recoverable yet
        with self.assertRaisesRegex(lifecycle_repo.LifecycleRefused, "duplicate-spend-risk"):
            await self._resolve(key, "failed")
        result = await self._resolve(key, "failed", accept_duplicate_spend_risk=True)
        self.assertTrue(result.reconciled)
        attempt = (await self._rows(GenerationAttempt, GenerationAttempt.id))[0]
        # Audit kept: still reconciliation_required, decision recorded, no cost.
        self.assertEqual(
            (attempt.outcome, attempt.accounting_state, attempt.custo_creditos, attempt.consumo_llm_id),
            ("failed", "reconciliation_required", None, None),
        )
        self.assertIn("risk=duplicate_spend_accepted", attempt.reconciliation_reason)
        # Resolution never touches domain authority: the lane is still owned.
        self.assertIsNotNone((await self._lane(agent)).owner_token)
        self.assertEqual(await self._recover_with_hint(agent), "retry")
        self.assertIsNone((await self._lane(agent)).owner_token)
        # Exact replay is idempotent; a different tuple is refused.
        self.assertFalse((await self._resolve(key, "failed", accept_duplicate_spend_risk=True)).reconciled)
        with self.assertRaises(lifecycle_repo.LifecycleRefused):
            await lifecycle_repo.run_in_new_session(
                self.sessions,
                lambda db: lifecycle_repo.resolve_generation_quarantine(
                    db, attempt_key=key, outcome="failed", evidence="provider_dashboard",
                    actor="ops", note="ticket07 test", accept_duplicate_spend_risk=True,
                ),
            )

    async def test_quarantine_succeeded_requires_exact_ledger_row(self) -> None:
        agent, key = await self._quarantined_lane("q-success")
        attempt = (await self._rows(GenerationAttempt, GenerationAttempt.id))[0]

        async def ledger(sessao_id, gen_id):
            async with self.sessions() as db:
                row = await consumo_repo.registrar(
                    db, agente_id=agent, sessao_id=sessao_id, modelo_llm=MODEL,
                    tipo_chamada="resposta", tokens_input=11, tokens_output=5,
                    custo_creditos=Decimal("0.00002000"), provider_generation_id=gen_id,
                )
                await db.commit()
                return row.id

        with self.assertRaisesRegex(lifecycle_repo.LifecycleRefused, "consumo-id"):
            await self._resolve(key, "succeeded")
        other_session = Sessao(agente_id=agent, telefone="5511777", delivery_stream_key=str(uuid4()))
        async with self.sessions() as db:
            db.add(other_session)
            await db.commit()
        wrong = await ledger(other_session.id, "wrong-session")
        with self.assertRaises(lifecycle_repo.LifecycleRefused):
            await self._resolve(key, "succeeded", consumo_llm_id=wrong)
        exact = await ledger(attempt.sessao_id, "late-provider-evidence")
        result = await self._resolve(key, "succeeded", consumo_llm_id=exact)
        self.assertTrue(result.reconciled)
        attempt = (await self._rows(GenerationAttempt, GenerationAttempt.id))[0]
        self.assertEqual(
            (attempt.outcome, attempt.consumo_llm_id, attempt.custo_creditos, attempt.accounting_state),
            ("succeeded", exact, Decimal("0.00002000"), "reconciliation_required"),
        )

    async def test_one_ledger_row_cannot_reconcile_two_attempts(self):
        agent, first_key = await self._quarantined_lane("r7b-ledger-reuse")
        first = (await self._rows(GenerationAttempt, GenerationAttempt.id))[0]
        second_key = str(uuid4())
        second_owner = str(uuid4())
        async with self.sessions() as db:
            db.add(
                GenerationAttempt(
                    attempt_key=second_key,
                    agente_id=first.agente_id,
                    sessao_id=first.sessao_id,
                    turn_id=first.turn_id,
                    revision_id=first.revision_id,
                    scheduler_epoch=first.scheduler_epoch,
                    lane_owner_token=second_owner,
                    attempt_type="response",
                    attempt_ordinal=2,
                    outcome="outcome_unknown",
                    accounting_state="reconciliation_required",
                    modelo_llm=MODEL,
                    safe_error_code="lane_lease_expired_unknown_outcome",
                    outcome_recorded_at=func.utc_timestamp(6),
                )
            )
            ledger = await consumo_repo.registrar(
                db,
                agente_id=agent,
                sessao_id=first.sessao_id,
                modelo_llm=MODEL,
                tipo_chamada="resposta",
                tokens_input=11,
                tokens_output=5,
                custo_creditos=Decimal("0.00002000"),
                provider_generation_id=None,
            )
            await db.commit()
            ledger_id = ledger.id

        one = await self._resolve(first_key, "succeeded", consumo_llm_id=ledger_id)
        accepted_second = True
        try:
            two = await self._resolve(second_key, "succeeded", consumo_llm_id=ledger_id)
        except lifecycle_repo.LifecycleRefused:
            accepted_second = False
            two = None
        async with self.sessions() as db:
            links = list(
                (
                    await db.execute(
                        select(GenerationAttempt.id, GenerationAttempt.consumo_llm_id)
                        .where(GenerationAttempt.consumo_llm_id == ledger_id)
                        .order_by(GenerationAttempt.id)
                    )
                ).all()
            )
            ledger_count = int(
                (
                    await db.execute(
                        select(func.count(ConsumoLLM.id)).where(ConsumoLLM.id == ledger_id)
                    )
                ).scalar_one()
            )
        print(
            "R7B_LEDGER_REUSE",
            one.reconciled,
            accepted_second,
            None if two is None else two.reconciled,
            links,
            ledger_count,
        )
        self.assertFalse(accepted_second)
        self.assertEqual(len(links), 1)
        self.assertEqual(ledger_count, 1)
        # Independent storage fence: an otherwise identical commercial tuple
        # cannot be cloned onto another attempt, even bypassing repositories.
        columns = [column.name for column in GenerationAttempt.__table__.columns if column.name != "id"]
        selected = [":key" if name == "attempt_key" else "3" if name == "attempt_ordinal" else name for name in columns]
        async with self.sessions() as db:
            with self.assertRaises(IntegrityError) as refused:
                await db.execute(text(
                    "INSERT INTO generation_attempts (" + ",".join(columns) + ") SELECT " +
                    ",".join(selected) + " FROM generation_attempts WHERE attempt_key=:first"
                ), {"key": str(uuid4()), "first": first_key})
            self.assertIn("uq_generation_consumo_link", str(refused.exception))
            await db.rollback()


    async def test_concurrent_accounting_links_one_ledger_exactly_once(self) -> None:
        cases = 0
        for kind in ("response", "summary"):
            for provider_id in (None, "known-generation"):
                if cases:
                    await self.asyncTearDown()
                    await self.asyncSetUp()
                agent, _ = await self._quarantined_lane("concurrent-ledger")
                original = (await self._rows(GenerationAttempt, GenerationAttempt.id))[0]
                attempts = []
                async with self.sessions() as db:
                    for ordinal in (2, 3):
                        row = GenerationAttempt(
                            attempt_key=str(uuid4()), agente_id=agent, sessao_id=original.sessao_id,
                            turn_id=original.turn_id, revision_id=original.revision_id,
                            scheduler_epoch=original.scheduler_epoch, lane_owner_token=str(uuid4()),
                            attempt_type=kind, attempt_ordinal=ordinal, outcome="started",
                            accounting_state="pending", modelo_llm=MODEL,
                        )
                        db.add(row)
                        attempts.append(row)
                    ledger = await consumo_repo.registrar(
                        db, agente_id=agent, sessao_id=original.sessao_id, modelo_llm=MODEL,
                        tipo_chamada="resposta" if kind == "response" else "resumo",
                        tokens_input=11, tokens_output=5, custo_creditos=Decimal("0.00002000"),
                        provider_generation_id=provider_id,
                    )
                    await db.commit()
                async def account(attempt, *, tokens=11):
                    async def operation(db):
                        await turn_generation_repo._share_lock_accounting_parents(
                            db, agente_id=agent, sessao_id=original.sessao_id
                        )
                        return await scheduler_repo.record_generation_outcome(
                            db, attempt_key=attempt.attempt_key, lane_owner_token=attempt.lane_owner_token,
                            scheduler_epoch=attempt.scheduler_epoch, outcome="succeeded",
                            provider_generation_id=provider_id, tokens_input=tokens, tokens_output=5,
                            custo_creditos=Decimal("0.00002000"), consumo_llm_id=ledger.id,
                        )
                    return await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, operation)
                results = await asyncio.gather(*(account(attempt) for attempt in attempts))
                self.assertEqual(sorted(results), [False, True])
                winner = attempts[results.index(True)]
                self.assertTrue(await account(winner))
                self.assertFalse(await account(winner, tokens=12))
                linked = [row for row in await self._rows(GenerationAttempt, GenerationAttempt.id) if row.consumo_llm_id == ledger.id]
                self.assertEqual(len(linked), 1)
                cases += 1
        self.assertEqual(cases, 4)

    # ── runtime gate before I/O ─────────────────────────────────────────
    async def _assert_runtime_release(self, mutate_sql: str) -> None:
        agent, _, _, _ = await self._ordinary_admitted(f"gate-{uuid4().hex[:6]}")
        claimed = (await self._claim())[0]
        async with self.sessions() as db:
            await db.execute(text(mutate_sql))
            await db.commit()
        before = turn_delivery_repo.runtime_gate_release_count()
        self.assertFalse(await self._preio(claimed))
        self.assertEqual(turn_delivery_repo.runtime_gate_release_count(), before + 1)
        row = (await self._automated())[0]
        self.assertEqual(
            (row.status, row.ambiguity_count, row.lease_token, row.last_error_code),
            ("retry", 0, None, "preio_refused_runtime"),
        )
        async with self.sessions() as db:
            now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        self.assertGreater(row.next_attempt_at, now)

    async def test_preio_runtime_gate_frozen_releases_claim(self) -> None:
        await self._assert_runtime_release("UPDATE delivery_runtime_state SET mode='frozen'")

    async def test_preio_runtime_gate_divergent_epoch_releases_claim(self) -> None:
        await self._assert_runtime_release(
            f"UPDATE delivery_runtime_state SET cutover_epoch='{uuid4()}'"
        )

    async def test_preio_scheduler_gate_releases_claim(self) -> None:
        await self._assert_runtime_release("UPDATE scheduler_runtime_state SET state='enabling'")

    # ── §16.1 A1: hinted identities, no plain read before a lock wait ───
    def _record(self):
        statements: list[str] = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(self.engine.sync_engine, "before_cursor_execute", record)
        return statements, lambda: event.remove(self.engine.sync_engine, "before_cursor_execute", record)

    def _assert_only_locking_reads_before_last_lock(self, statements: list[str]) -> None:
        def locking_read(q: str) -> bool:
            return any(mark in q for mark in ("FOR UPDATE", "FOR SHARE", "LOCK IN SHARE MODE"))

        locking = [i for i, q in enumerate(statements) if locking_read(q)]
        self.assertTrue(locking)
        for statement in statements[: locking[-1]]:
            if statement.lstrip().upper().startswith("SELECT") and re.search(
                r"\bFROM\b", statement, re.IGNORECASE
            ):
                self.assertTrue(
                    locking_read(statement),
                    f"plain read before a lock wait: {statement[:160]}",
                )

    async def test_hinted_claim_recover_and_renew_take_only_locking_reads(self) -> None:
        agent, instance = await self._agent("a1-hint")
        await self._accept(instance, f"5511{agent:08d}", "a1-1", "oi")
        await self._force_ready()
        async with self.sessions() as db:
            hint = await turn_generation_repo.ready_turn_hint(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch
            )
        statements, stop = self._record()
        try:
            async with self.sessions() as db:
                async with db.begin():
                    claim = await scheduler_repo.claim_ready_turn(
                        db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.delivery_epoch, worker_id="a1",
                        lease_seconds=30, candidate=hint,
                    )
            self.assertIsNotNone(claim)
            self._assert_only_locking_reads_before_last_lock(statements)
            statements.clear()
            async with self.sessions() as db:
                async with db.begin():
                    self.assertTrue(
                        await scheduler_repo.renew_lane_lease(
                            db, turn_id=claim.turn_id, agente_id=agent,
                            scheduler_epoch=self.scheduler_epoch,
                            delivery_runtime_epoch=self.delivery_epoch,
                            owner_token=claim.claim_token, lease_seconds=30,
                            sessao_id=claim.sessao_id,
                        )
                    )
            self._assert_only_locking_reads_before_last_lock(statements)
        finally:
            stop()
        await self._expire_lane(agent)
        statements, stop = self._record()
        try:
            self.assertEqual(await self._recover_with_hint(agent), "retry")
        finally:
            stop()
        recover_tx = statements[next(i for i, q in enumerate(statements) if "delivery_runtime_state" in q):]
        self._assert_only_locking_reads_before_last_lock(recover_tx)

    async def test_hinted_claim_rechecks_under_lock_after_session_wait(self) -> None:
        agent, instance = await self._agent("a1-wait")
        await self._accept(instance, f"5511{agent:08d}", "a1w-1", "oi")
        await self._force_ready()
        async with self.sessions() as db:
            hint = await turn_generation_repo.ready_turn_hint(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch
            )
        holder = self.sessions()
        try:
            await holder.execute(select(Sessao).where(Sessao.id == hint[1]).with_for_update())
            claim_task = asyncio.create_task(
                scheduler_repo.run_transaction_with_deadlock_retry(
                    self.sessions,
                    lambda db: scheduler_repo.claim_ready_turn(
                        db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.delivery_epoch, worker_id="a1w",
                        lease_seconds=30, candidate=hint,
                    ),
                )
            )
            await asyncio.sleep(0.5)
            self.assertFalse(claim_task.done())  # waiting on the session lock
            await holder.execute(
                update(ConversationTurn).values(
                    ready_at=func.utc_timestamp(6) + text("INTERVAL 60 SECOND")
                )
            )
            await holder.commit()
        finally:
            await holder.close()
        self.assertIsNone(await asyncio.wait_for(claim_task, timeout=20))
        self.assertIsNone((await self._lane(agent)).owner_token)

    async def test_processor_passes_discovery_hints_to_every_lock_transaction(self) -> None:
        agent, instance = await self._agent("a1-proc")
        await self._accept(instance, f"5511{agent:08d}", "a1p-1", "oi")
        await self._force_ready()
        seen: dict[str, list] = {"claim": [], "renew": []}
        original_claim = scheduler_repo.claim_ready_turn
        original_renew = scheduler_repo.renew_lane_lease

        async def claim(db, **kwargs):
            seen["claim"].append(kwargs.get("candidate"))
            return await original_claim(db, **kwargs)

        async def renew(db, **kwargs):
            seen["renew"].append(kwargs.get("sessao_id"))
            return await original_renew(db, **kwargs)

        llm = FakeLLM("ok")
        llm.barrier = asyncio.Event()
        processor = self._router_processor(llm)
        processor.lease_seconds = 3
        with patch.object(scheduler_repo, "claim_ready_turn", claim), patch.object(
            scheduler_repo, "renew_lane_lease", renew
        ):
            tasks = await processor.run_pass()
            await asyncio.wait_for(llm.entered.wait(), timeout=20)
            await asyncio.sleep(1.5)  # at least one renewal while generating
            llm.barrier.set()
            await asyncio.gather(*tasks)
        self.assertTrue(seen["claim"] and all(c is not None for c in seen["claim"]))
        self.assertTrue(seen["renew"] and all(s is not None for s in seen["renew"]))

    async def test_processor_recovers_expired_lanes_with_discovery_hints(self) -> None:
        agent, instance = await self._agent("a1-recover")
        await self._accept(instance, f"5511{agent:08d}", "a1r-1", "oi")
        await self._force_ready()
        async with self.sessions() as db:
            hint = await turn_generation_repo.ready_turn_hint(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch
            )
        await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch, worker_id="crashed",
                lease_seconds=30, candidate=hint,
            ),
        )
        await self._expire_lane(agent)
        seen = []
        original = scheduler_repo.recover_expired_lane

        async def recover(db, **kwargs):
            seen.append((kwargs.get("expected_turn_id"), kwargs.get("expected_sessao_id")))
            return await original(db, **kwargs)

        processor = self._router_processor(ForbiddenLLM())
        with patch.object(scheduler_repo, "recover_expired_lane", recover), patch.object(
            turn_generation_repo, "list_ready_agent_ids", return_value=[]
        ):
            await processor.run_pass()
        self.assertEqual(seen, [hint])
        self.assertIsNone((await self._lane(agent)).owner_token)

    async def test_stale_hints_are_refused_under_locks(self) -> None:
        agent, instance = await self._agent("a1-stale")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "a1s-1", "oi")
        await self._accept(instance, f"{phone}9", "a1s-2", "outro")
        await self._force_ready()
        turns = await self._turns()
        head, second = (turns[0].id, turns[0].sessao_id), (turns[1].id, turns[1].sessao_id)

        async def claim(candidate):
            return await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions,
                lambda db: scheduler_repo.claim_ready_turn(
                    db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.delivery_epoch, worker_id="a1s",
                    lease_seconds=30, candidate=candidate,
                ),
            )

        # A candidate that is not the head of the lane is never claimed.
        self.assertIsNone(await claim(second))
        claimed = await claim(head)
        self.assertEqual(claimed.turn_id, head[0])
        # Renewal with a session identity that is not the claimed one fails.
        async with self.sessions() as db:
            async with db.begin():
                self.assertFalse(
                    await scheduler_repo.renew_lane_lease(
                        db, turn_id=claimed.turn_id, agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.delivery_epoch,
                        owner_token=claimed.claim_token, lease_seconds=30,
                        sessao_id=second[1],
                    )
                )
        await self._expire_lane(agent)

        async def recover(turn_id, sessao_id):
            async def operation(db):
                now = await delivery_state_repo.get_database_now(db)
                return await scheduler_repo.recover_expired_lane(
                    db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.delivery_epoch,
                    retry_at=now + timedelta(seconds=1),
                    expected_turn_id=turn_id, expected_sessao_id=sessao_id,
                )

            return await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, operation)

        with self.assertRaisesRegex(scheduler_repo.SchedulerConflictError, "hint is stale"):
            await recover(*second)
        with self.assertRaisesRegex(scheduler_repo.SchedulerConflictError, "stale turn identity"):
            await recover(head[0], second[1])
        self.assertEqual(await recover(*head), "retry")

    async def test_owned_lane_refuses_hinted_and_legacy_claims(self) -> None:
        agent, instance = await self._agent("a1-owned")
        await self._accept(instance, f"5511{agent:08d}", "a1o-1", "um")
        await self._accept(instance, f"5511{agent:08d}9", "a1o-2", "dois")
        await self._force_ready()

        async def claim(candidate):
            return await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions,
                lambda db: scheduler_repo.claim_ready_turn(
                    db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.delivery_epoch, worker_id=f"w-{uuid4()}",
                    lease_seconds=30, candidate=candidate,
                ),
            )

        first, second = await self._turns()
        self.assertIsNotNone(await claim((first.id, first.sessao_id)))
        # The lane is owned: neither the hinted nor the unhinted path claims.
        self.assertIsNone(await claim((second.id, second.sessao_id)))
        self.assertIsNone(await claim(None))
        self.assertEqual(
            sorted(t.status for t in await self._turns()), ["claimed", "pending"]
        )

    async def test_quarantined_agent_hint_is_not_claimed(self) -> None:
        agent, instance = await self._agent("a1-quarantine")
        await self._accept(instance, f"5511{agent:08d}", "a1q-1", "oi")
        await self._force_ready()
        unknown = FakeLLM(llm_client.LLMProviderError("provider_timeout", outcome_known=False))
        self.assertEqual(
            await self._run_one(self._router_processor(unknown)), ["generation_outcome_unknown"]
        )
        async with self.sessions() as db:
            await db.execute(text(
                "UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND, "
                "retry_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND"
            ))
            await db.commit()
            hint = await turn_generation_repo.ready_turn_hint(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch
            )
        self.assertIsNotNone(hint)
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch, worker_id="q",
                lease_seconds=30, candidate=hint,
            ),
        )
        self.assertIsNone(claim)
        async with self.sessions() as db:
            listed = await lifecycle_repo.list_generation_quarantine(db, agente_id=agent)
        self.assertEqual(len(listed), 1)

    async def test_owner_lifecycle_cancel_prelocks_successor_before_revisions(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("owner-prelock")
        await self._accept(instance, phone, "owner-prelock-2", "sucessor")
        active, successor = await self._turns()
        lease = turn_generation_repo.TurnLease(
            turn_id=active.id, agente_id=agent, sessao_id=active.sessao_id,
            owner_token=active.claim_token, scheduler_epoch=self.scheduler_epoch,
            delivery_runtime_epoch=self.delivery_epoch,
        )
        async with self.sessions() as db:
            await db.execute(text("UPDATE sessoes SET handler='encerrada'"))
            await db.commit()
        policy = self._router_processor(ForbiddenLLM()).policy
        statements, stop = self._record()
        try:
            step = await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions,
                lambda db: turn_generation_repo.advance_turn(db, lease=lease, policy=policy),
            )
        finally:
            stop()
        self.assertEqual(step.reason, "lifecycle_cancelled")
        first_revision = next(i for i, q in enumerate(statements) if "FROM turn_revisions" in q)
        successor_lock = next(
            i for i, q in enumerate(statements)
            if "FROM conversation_turns" in q
            and "session_slot" in q.split("WHERE", 1)[-1]
            and "FOR UPDATE" in q
        )
        self.assertLess(successor_lock, first_revision)
        self.assertEqual(
            [(t.status, t.terminal_reason) for t in await self._turns()],
            [("cancelled", "session_closed"), ("cancelled", "session_closed")],
        )
        self.assertEqual((await self._automated())[0].status, "cancelled")
        self.assertIsNone((await self._lane(agent)).owner_token)

    async def test_dormant_primitives_use_session_hint(self) -> None:
        import inspect
        from datetime import datetime

        agent, _, _, _ = await self._claimed_send("primitive-hints")
        turn = await self._one_turn()
        lane = await self._lane(agent)
        values = dict(
            turn_id=turn.id, sessao_id=turn.sessao_id, agente_id=agent,
            revision_id=turn.final_revision_id, scheduler_epoch=self.scheduler_epoch,
            delivery_runtime_epoch=self.delivery_epoch, owner_token=lane.owner_token,
            expected_input_version=turn.input_version, attempt_type="response", modelo_llm=MODEL,
            expected_first_input_sequence=1, expected_last_input_sequence=1,
            retry_at=datetime.utcnow() + timedelta(seconds=5), terminal_status="cancelled",
            terminal_reason="test", promote_successor=True,
        )
        for name in ("seal_revision_range", "start_generation_attempt", "admit_revision",
                     "yield_turn_for_retry", "complete_turn_and_release_lane"):
            operation = getattr(scheduler_repo, name)
            kwargs = {key: value for key, value in values.items() if key in inspect.signature(operation).parameters}
            statements, stop = self._record()
            try:
                async with self.sessions() as db:
                    try:
                        await operation(db, **kwargs)
                    except (scheduler_repo.SchedulerConflictError, ValueError):
                        pass  # Wrong lifecycle phase is expected for some primitives.
                    await db.rollback()
            finally:
                stop()
            self.assertFalse([sql for sql in statements if "select conversation_turns.sessao_id" in sql.lower()], name)
            self.assertTrue(any("sessoes" in sql.lower() and "for update" in sql.lower() for sql in statements), name)

    async def test_app_health_exposes_local_lifecycle_counts(self) -> None:
        from fastapi import Response
        from types import SimpleNamespace
        from unittest.mock import AsyncMock
        from src import main

        before = lifecycle_repo.lifecycle_counts()
        with patch.object(main, "provider_auth_readiness", AsyncMock(return_value=SimpleNamespace(ready=True))):
            async with self.sessions() as db:
                result = await main.health_check(Response(), db)
            self.assertEqual(result["lifecycle"], {"counts": before, "signal_threshold": 1})
            with patch.object(main.delivery_state_repo, "get_runtime_state", AsyncMock(side_effect=RuntimeError("test"))):
                async with self.sessions() as db:
                    result = await main.health_check(Response(), db)
                self.assertEqual(result["lifecycle"]["counts"], before)

    # ── whole-transaction retry on a real deadlock ──────────────────────
    async def test_lifecycle_transaction_retries_after_real_deadlock(self) -> None:
        agent, instance = await self._agent("deadlock")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "dl-1", "oi")
        messages = []
        sink = lifecycle_repo.logger.add(lambda message: messages.append(str(message)))
        self.addCleanup(lifecycle_repo.logger.remove, sink)
        other = self.sessions()
        try:
            await other.execute(select(Sessao).where(Sessao.telefone == phone).with_for_update())
            other.add_all(Atendente(nome="peso", numero_whatsapp=f"peso-{uuid4()}") for _ in range(40))
            await other.flush()
            before = lifecycle_repo.lifecycle_counts()["deadlock_retried"]
            transfer = asyncio.create_task(self._transfer(agent, phone))
            await asyncio.sleep(0.5)
            self.assertFalse(transfer.done())  # holds scheduler X, waits on session
            waiter = asyncio.create_task(
                other.execute(text("SELECT id FROM scheduler_runtime_state WHERE id=1 FOR UPDATE"))
            )
            await asyncio.sleep(1.0)
            await asyncio.wait_for(waiter, timeout=20)  # the light victim was the transfer
            await other.rollback()
            await asyncio.wait_for(transfer, timeout=30)
        finally:
            await other.close()
        self.assertEqual(lifecycle_repo.lifecycle_counts()["deadlock_retried"], before + 1)
        self.assertTrue(any("Lifecycle transaction lost; retrying" in message for message in messages))
        self.assertEqual((await self._one_turn()).terminal_reason, "session_transferred")

    # ── conversation turn lifecycle guard: full slot/status matrix ──────
    async def test_turn_lifecycle_guard_full_status_matrix(self) -> None:
        agent, _ = await self._agent("guard-matrix")
        active = ("pending", "claimed", "generating", "typing", "awaiting_delivery", "retry")
        terminal = ("delivered", "handoff", "cancelled", "dead")
        states = [("active", s) for s in active] + [("successor", "pending")] + [
            ("terminal", s) for s in terminal
        ]

        def allowed(source, target) -> bool:
            if source[0] == "terminal":
                return source == target
            if source[0] == "active":
                return target[0] in {"active", "terminal"}
            return True

        verdicts = {"allowed": 0, "denied": 0}
        sequence = 0
        for source in states:
            for target in states:
                sequence += 1
                async with self.sessions() as db:
                    session = Sessao(
                        agente_id=agent, telefone=f"g{sequence:04d}", delivery_stream_key=str(uuid4())
                    )
                    db.add(session)
                    await db.flush()
                    terminal_cols = (
                        ", terminal_at=UTC_TIMESTAMP(6), terminal_reason='matrix'"
                        if source[0] == "terminal" else ""
                    )
                    await db.execute(
                        text(
                            "INSERT INTO conversation_turns (turn_key, agente_id, sessao_id, "
                            "scheduler_epoch, arrival_sequence, scheduling_sequence, status, "
                            "session_slot, ready_at) VALUES (UUID(), :a, :s, :e, :n, :n, :st, :sl, "
                            "UTC_TIMESTAMP(6))"
                        ),
                        {"a": agent, "s": session.id, "e": self.scheduler_epoch, "n": sequence,
                         "st": "pending" if source[0] == "terminal" else source[1],
                         "sl": "active" if source[0] == "terminal" else source[0]},
                    )
                    turn_id = (await db.execute(text("SELECT LAST_INSERT_ID()"))).scalar_one()
                    if terminal_cols:
                        # Terminal state reached through the allowed active edge.
                        await db.execute(text(
                            f"UPDATE conversation_turns SET status='{source[1]}', "
                            f"session_slot='terminal'{terminal_cols} WHERE id=:id"
                        ), {"id": turn_id})
                    await db.commit()
                target_cols = (
                    "terminal_at=terminal_at, terminal_reason=terminal_reason"
                    if source == target
                    else "terminal_at=UTC_TIMESTAMP(6), terminal_reason='matrix-to'"
                    if target[0] == "terminal"
                    else "terminal_at=NULL, terminal_reason=NULL"
                )
                async with self.sessions() as db:
                    try:
                        await db.execute(text(
                            f"UPDATE conversation_turns SET status='{target[1]}', "
                            f"session_slot='{target[0]}', {target_cols} WHERE id=:id"
                        ), {"id": turn_id})
                        await db.commit()
                        verdict = True
                    except DBAPIError as exc:
                        await db.rollback()
                        self.assertIn("turn lifecycle transition is not allowed", str(exc))
                        verdict = False
                self.assertEqual(verdict, allowed(source, target), f"{source}->{target}")
                verdicts["allowed" if verdict else "denied"] += 1
        self.assertEqual(verdicts, {"allowed": 75, "denied": 46})
        # Same terminal status with a rewritten reason is not identical: denied.
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, "lifecycle transition is not allowed"):
                await db.execute(text(
                    "UPDATE conversation_turns SET terminal_reason='rewritten' "
                    "WHERE session_slot='terminal' LIMIT 1"
                ))
            await db.rollback()

    # ── retention never erases a nonterminal turn's automated rows ──────
    async def test_retention_skips_limit_plus_one_parked_rows_and_purges_victim(self) -> None:
        stuck = []
        for index in range(3):  # limit (2) + 1 rows that must never progress
            await self._ordinary_admitted(f"ret-stuck-{index}")
        async with self.sessions() as db:
            await db.execute(text(
                "UPDATE outbox_deliveries SET status='dead', terminal_reason='http_503', "
                "terminal_at=UTC_TIMESTAMP(6) - INTERVAL 100 DAY WHERE delivery_class='automated'"
            ))
            await db.commit()
        stuck = [r.id for r in await self._automated()]
        agent, _, _, _ = await self._ordinary_admitted("ret-victim")
        claimed = await self._claim()
        self.assertTrue(await self._complete(claimed[0]))
        async with self.sessions() as db:
            await db.execute(text(
                "UPDATE outbox_deliveries SET terminal_at=UTC_TIMESTAMP(6) - INTERVAL 100 DAY "
                "WHERE id=:id"
            ), {"id": claimed[0].id})
            await db.commit()
        for _ in range(2):
            async with self.sessions() as db:
                result = await delivery_worker_repo.cleanup_retention(db, automated_limit=2)
                self.assertEqual(result.automated_nonterminal_skipped, 3)
                self.assertGreaterEqual(delivery_worker_repo.retention_skip_count(), 3)
                await db.commit()
        rows = {r.id: r for r in await self._automated()}
        self.assertNotIn(claimed[0].id, rows)  # victim scrubbed then deleted
        self.assertEqual(sorted(rows), sorted(stuck))
        self.assertTrue(all(rows[i].payload for i in stuck))  # payload kept for turn-resolve

    # ── callbacks stay human-path only ──────────────────────────────────
    async def test_chatwoot_callback_never_takes_lane_or_unblocks_automated(self) -> None:
        agent, _, phone, _, _ = await self._run_handoff("callback")
        opened = next(
            r for r in await self._rows(OutboxDelivery, OutboxDelivery.id)
            if r.operation == t06.OPEN
        )
        attempt = (await self._rows(HandoffAttempt, HandoffAttempt.id))[0]
        statements, stop = self._record()
        try:
            async with self.sessions() as db:
                outcome = await delivery_worker_repo.complete_chatwoot_handoff_from_callback(
                    db, sessao_id=attempt.sessao_id, attempt_id=attempt.id,
                    cutover_epoch=self.delivery_epoch, contact_id=41, conversation_id=42,
                    delivery_id=opened.id, delivery_key=opened.delivery_key, inbox_id=7,
                    transition_ttl_seconds=600,
                )
                await db.commit()
        finally:
            stop()
        self.assertEqual(outcome, "completed")
        self.assertFalse([q for q in statements if "agent_service_lanes" in q or "conversation_turns" in q])
        transition = (await self._automated())[1]
        self.assertEqual(transition.status, "blocked")

    # ── FU-2: ledger gaps, savepoint loss and owned abandon ──────────────
    async def _fu2_started_attempt(self, suffix: str) -> GenerationAttempt:
        agent, instance = await self._agent(suffix)
        await self._accept(instance, f"5511{agent:08d}", suffix, "synthetic input")
        await self._force_ready()
        async with self.sessions() as db:
            hint = await turn_generation_repo.ready_turn_hint(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch)
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions, lambda db: scheduler_repo.claim_ready_turn(
                db, agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch, worker_id="fu2-worker",
                lease_seconds=30, candidate=hint))
        lease = turn_generation_repo.TurnLease.from_claim(claim)
        policy = self._router_processor(ForbiddenLLM()).policy
        step = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions, lambda db: turn_generation_repo.advance_turn(db, lease=lease, policy=policy))
        self.assertIsInstance(step, turn_generation_repo.GenerateStep)
        # The DB pre-provider path creates a genuine started attempt. Never
        # execute the GenerateStep: provider and spend stay entirely synthetic.
        async with self.sessions() as db:
            return (await db.execute(select(GenerationAttempt).where(
                GenerationAttempt.attempt_key == step.attempt_key))).scalar_one()

    async def test_two_agents_late_accounting_no_unique_gap_deadlock(self) -> None:
        # No whole-transaction retry: a 1213 may not be hidden by recovery.
        attempts = [await self._fu2_started_attempt(f"fu2-gap-{n}") for n in range(2)]
        original = scheduler_repo._lock_matching_consumption
        cases = 0
        for provider_ids in ((None, None), (f"fu2-a-{uuid4()}", f"fu2-b-{uuid4()}")):
            if cases:
                attempts = [await self._fu2_started_attempt(f"fu2-gap-{cases}-{n}") for n in range(2)]
            arrived = [asyncio.Event(), asyncio.Event()]
            async def barrier(db, **kwargs):
                matched = await original(db, **kwargs)
                n = next(i for i, row in enumerate(attempts) if row.id == kwargs["attempt"].id)
                arrived[n].set()
                await asyncio.wait_for(arrived[1-n].wait(), timeout=20)
                return matched
            async def account(n):
                row = attempts[n]
                usage = turn_generation_repo.GenerationUsage(
                    modelo_llm=MODEL, tokens_input=11, tokens_output=5,
                    tokens_reasoning=None, tokens_cache_read=None, tokens_cache_write=None,
                    custo_creditos=Decimal("0.00002000"), provider_generation_id=provider_ids[n],
                )
                async with self.sessions() as db:
                    accepted = await turn_generation_repo.record_generation_accounting_only(
                        db, agente_id=row.agente_id, sessao_id=row.sessao_id,
                        attempt_key=row.attempt_key, lane_owner_token=row.lane_owner_token,
                        outcome=turn_generation_repo.GenerationOutcome("succeeded", usage=usage),
                    )
                    await db.commit()
                    return accepted
            with patch.object(scheduler_repo, "_lock_matching_consumption", barrier):
                results = await asyncio.wait_for(asyncio.gather(account(0), account(1), return_exceptions=True), 40)
            self.assertEqual(results, [True, True], results)
            linked = [r for r in await self._rows(GenerationAttempt, GenerationAttempt.id) if r.id in [a.id for a in attempts]]
            self.assertEqual({r.outcome for r in linked}, {"succeeded"})
            self.assertEqual(len({r.consumo_llm_id for r in linked}), 2)
            self.assertTrue(all(r.consumo_llm_id is not None for r in linked))
            cases += 1
        self.assertEqual(cases, 2)

    async def test_accounting_savepoint_real_deadlock_escapes(self) -> None:
        # The lighter transaction loses while inside the new savepoint.
        # No retry wrapper: both 1213 and wrapped 1305 must escape intact.
        from src.database import db_errors
        row = await self._fu2_started_attempt("fu2-savepoint")
        async with self.sessions() as seed:
            blocker = Atendente(nome="fu2 blocker", numero_whatsapp=f"fu2-{uuid4()}")
            seed.add(blocker)
            await seed.commit()
        entered = asyncio.Event()
        async with self.sessions() as heavy, self.sessions() as victim:
            await heavy.execute(select(Atendente).where(Atendente.id == blocker.id).with_for_update())
            heavy.add_all(Atendente(nome="fu2 weight", numero_whatsapp=f"fu2-{uuid4()}") for _ in range(60))
            await heavy.flush()
            attempt = (await victim.execute(select(GenerationAttempt).where(
                GenerationAttempt.id == row.id).with_for_update())).scalar_one()
            original_flush = victim.flush
            async def flush(*args, **kwargs):
                if victim.in_nested_transaction():
                    entered.set()
                    await victim.execute(text("SELECT id FROM atendentes WHERE id=:id FOR UPDATE"), {"id": blocker.id})
                return await original_flush(*args, **kwargs)
            async def lose():
                with patch.object(victim, "flush", flush):
                    return await scheduler_repo._write_generation_accounting(
                        victim, row=attempt, values={"outcome": "failed", "outcome_recorded_at": func.utc_timestamp(6)})
            task = asyncio.create_task(lose())
            await asyncio.wait_for(entered.wait(), 20)
            try:
                await asyncio.wait_for(heavy.execute(select(GenerationAttempt.id).where(
                    GenerationAttempt.id == row.id).with_for_update()), 20)
                with self.assertRaises(BaseException) as caught:
                    await asyncio.wait_for(task, 20)
                self.assertTrue(db_errors.transaction_lost(caught.exception), repr(caught.exception))
                print("FU2_SAVEPOINT_TRANSACTION_LOST", sorted(set(db_errors.mysql_error_codes(caught.exception))))
            finally:
                await heavy.rollback()
                await victim.rollback()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        # Storage stayed unchanged: no false evidence or partial link.
        saved = next(r for r in await self._rows(GenerationAttempt, GenerationAttempt.id) if r.id == row.id)
        self.assertEqual((saved.outcome, saved.consumo_llm_id), ("started", None))
        # Driver/wrapper loss codes must not be treated as duplicate evidence.
        for code, message in (
            (1205, "uq_generation_consumo_link"),
            (1213, "uq_generation_consumo_link"),
            (1305, "uq_generation_consumo_link"),
            (1062, "Duplicate entry 'uq_generation_consumo_link' for key 'generation_attempts.uq_generation_provider_id'"),
            (1062, "Duplicate entry 'uq_generation_consumo_link' for key 'another_index'"),
        ):
            async with self.sessions() as db:
                attempt = (await db.execute(select(GenerationAttempt).where(
                    GenerationAttempt.id == row.id).with_for_update())).scalar_one()
                loss = IntegrityError("fu2 synthetic wrapped loss", {}, Exception(code, message))
                flush_original = db.flush
                async def inject(*args, **kwargs):
                    if db.in_nested_transaction():
                        raise loss
                    return await flush_original(*args, **kwargs)
                with patch.object(db, "flush", inject):
                    with self.assertRaises(IntegrityError) as propagated:
                        await scheduler_repo._write_generation_accounting(db, row=attempt, values={})
                self.assertIs(propagated.exception, loss)
                await db.rollback()

    async def test_abandon_parked_handoff_releases_owned_lane_and_claim(self) -> None:
        agent, _, _, _ = await self._parked_handoff("fu2-r5")
        turn = await self._one_turn()
        token = str(uuid4())
        async with self.sessions() as db:
            await db.execute(update(ConversationTurn).where(ConversationTurn.id == turn.id).values(
                claim_token=token, claim_owner="fu2-worker", claim_until=func.utc_timestamp(6) + text("INTERVAL 60 SECOND")))
            await db.execute(update(AgentServiceLane).where(AgentServiceLane.agente_id == agent).values(
                owner_token=token, owner_worker_id="fu2-worker", lease_until=func.utc_timestamp(6) + text("INTERVAL 60 SECOND"),
                active_turn_id=turn.id, active_revision_id=turn.final_revision_id))
            await db.commit()
        self.assertEqual((await self._lane(agent)).owner_token, token)
        result = await self._turn_resolve("abandon")
        self.assertEqual(result.status, "cancelled")
        turn, lane = await self._one_turn(), await self._lane(agent)
        self.assertEqual((turn.session_slot, turn.terminal_reason), ("terminal", "operator_abandoned"))
        self.assertEqual((turn.claim_token, turn.claim_owner, turn.claim_until), (None, None, None))
        self.assertEqual((lane.owner_token, lane.owner_worker_id, lane.lease_until, lane.active_turn_id, lane.active_revision_id), (None, None, None, None, None))
        self.assertEqual({r.status for r in await self._automated()}, {"dead", "cancelled"})
        self.assertEqual((await self._session()).motivo_encerramento, "handoff")



# Inherited ticket05/06 cases belong to their own matrices; run only ticket07.
for _name in dir(t06.TurnDeliveryMySQLTests):
    if _name.startswith("test_") and _name not in LifecycleMySQLTests.__dict__:
        setattr(LifecycleMySQLTests, _name, None)


if __name__ == "__main__":
    unittest.main()
