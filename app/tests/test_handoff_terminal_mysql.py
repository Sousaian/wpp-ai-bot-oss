"""Ticket 05 real-MySQL barriers; every provider is an in-process fake."""

import asyncio
from datetime import datetime, timedelta
from contextlib import ExitStack
from decimal import Decimal
import functools
import json
import os
import random
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from pydantic import SecretStr
from sqlalchemy import func, inspect, select, text, update
from sqlalchemy.exc import DBAPIError, OperationalError, TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.config import settings
from src.database.models import (
    AgentServiceLane,
    Agente,
    ConversationTurn,
    DeliveryOperatorAction,
    GenerationAttempt,
    HandoffAttempt,
    HandoffContextAssignment,
    InboundEvent,
    ConsumoLLM,
    Mensagem,
    OutboxDelivery,
    ScheduledHandoff,
    Sessao,
    TurnInputMembership,
    TurnRevision,
)
from src.database.repositories import (
    delivery_worker_repo,
    outbox_repo,
    scheduler_repo,
    turn_delivery_repo,
    turn_generation_repo,
    turn_handoff_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services import delivery_adapters, evolution_orchestrator, handoff_context, llm_client
from src.services.turn_handoff import (
    HandoffFinalizer,
    _deterministic_admission_failure,
)
from src.services.turn_processor import TurnProcessor


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")
MODEL = "openai/gpt-4o-mini"
NOTE = "chatwoot.add_private_note.v1"
OPEN = "chatwoot.open_handoff.v1"
SEND = "evolution.send_text.v1"
TABLES = (
    "delivery_operator_actions",
    "delivery_worker_heartbeats",
    "replica_heartbeats",  # T08-3
    "outbox_deliveries",
    "handoff_context_assignments",
    "scheduled_handoffs",
    "generation_attempts",
    "turn_input_memberships",
    "turn_revisions",
    "conversation_turns",
    "agent_service_lanes",
    "handoff_attempts",
    "inbound_events",
    "llm_usage_reconciliation",
    "consumo_llm",
    "mensagens",
    "sessoes",
    "agentes",
)


class FakeLLM:
    def __init__(self, *responses: object) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []
        self.entered = asyncio.Event()
        self.barrier: asyncio.Event | None = None

    async def __call__(self, *, modelo_llm, messages, tipo_chamada):
        self.calls.append(
            {"modelo_llm": modelo_llm, "messages": messages, "tipo_chamada": tipo_chamada}
        )
        self.entered.set()
        if self.barrier is not None:
            await self.barrier.wait()
        value = self.responses[len(self.calls) - 1]
        if isinstance(value, BaseException):
            raise value
        return llm_client.LLMResult(
            texto=value,
            modelo_llm=modelo_llm,
            tokens_input=7,
            tokens_output=3,
            tokens_reasoning=None,
            tokens_cache_read=None,
            tokens_cache_write=None,
            custo_creditos=Decimal("0.00001000"),
            provider_generation_id=f"ticket05-{uuid4()}",
            modelo_llm_reportado=MODEL,
        )


async def _no_presence(**kwargs) -> int:
    """Hermetic default: no Evolution presence HTTP from any test (A-4)."""

    return 201


class ForbiddenLLM:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, **kwargs):
        self.calls += 1
        raise AssertionError("LLM must not be called")


class HandoffTerminalMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        self.delivery_epoch = str(uuid4())
        self.scheduler_epoch = str(uuid4())
        await self._wipe()
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='outbox', "
                    "cutover_epoch=:epoch, version=version+1, changed_by='ticket05', "
                    "change_reason='disposable handoff test' WHERE id=1"
                ),
                {"epoch": self.delivery_epoch},
            )
            await connection.execute(
                text(
                    "UPDATE scheduler_runtime_state SET state='enabled', "
                    "scheduler_epoch=:epoch, version=version+1, schema_version=1, "
                    "max_pending_inputs_per_session=128, "
                    "max_pending_input_bytes_per_session=262144, "
                    "max_queued_turns_per_agent=1000, "
                    "max_queued_bytes_per_agent=16777216, "
                    "max_global_queued_turns=10000, "
                    "max_global_queued_bytes=268435456, "
                    "max_generation_attempts_per_turn=2, "
                    "max_delivery_attempts_per_turn=8, max_refreshes_per_turn=1, "
                    "accounting_drain_seconds=30, changed_by='ticket05', "
                    "change_reason='disposable handoff test' WHERE id=1"
                ),
                {"epoch": self.scheduler_epoch},
            )
        self.patches = ExitStack()
        for name, value in (
            ("provider_webhook_auth_mode", "enforced"),
            ("delivery_mode", "outbox"),
            ("delivery_cutover_epoch", self.delivery_epoch),
            ("inbox_scheduler_enabled", True),
            ("inbox_scheduler_epoch", self.scheduler_epoch),
            ("inbound_receipt_hmac_current_version", 1),
            ("inbound_receipt_hmac_keys", {1: SecretStr("ticket05-receipt")}),
        ):
            self.patches.enter_context(patch.object(settings, name, value))
        self.patches.enter_context(
            patch(
                "src.services.evolution_orchestrator.processar",
                AsyncMock(side_effect=AssertionError("legacy LLM path called")),
            )
        )
        self.patches.enter_context(
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                AsyncMock(side_effect=AssertionError("WhatsApp called")),
            )
        )
        self.patches.enter_context(
            patch(
                "src.services.llm_client._client",
                side_effect=AssertionError("real LLM client built"),
            )
        )
        self.processors: list[TurnProcessor] = []

    async def asyncTearDown(self) -> None:
        for processor in self.processors:
            await processor.shutdown(0.5)
        self.patches.close()
        await self._wipe()
        await self.engine.dispose()

    async def _wipe(self) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table_name in TABLES:
                await connection.execute(text(f"DELETE FROM {table_name}"))
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))

    # ── fixtures ────────────────────────────────────────────────────────
    async def _agent(self, suffix: str, *, inbox: int | None = 7, limit=None) -> tuple[int, str]:
        instance = f"ticket05-{suffix}-{uuid4()}"
        async with self.sessions() as db:
            row = Agente(
                nome=f"Ticket05 {suffix}",
                instance_name=instance,
                system_prompt="system-ticket05",
                numero_notificacao="0",
                modelo_llm=MODEL,
                chatwoot_inbox_id=inbox,
                inbox_scheduler_enabled=True,
                limite_mensal_creditos=limit,
            )
            db.add(row)
            await db.commit()
            return row.id, instance

    async def _intake(self, instance: str, phone: str, marker: str, content: str):
        event = evolution_orchestrator.EvolutionMessageEvent(
            instance_name=instance,
            provider_message_id=marker,
            telefone=phone,
            texto=content,
        )
        async with UnitOfWork(self.sessions) as uow:
            return await evolution_orchestrator.orchestrate(
                event=event, uow=uow, authenticated=True
            )

    async def _accept(self, instance: str, phone: str, marker: str, content: str) -> None:
        result = await self._intake(instance, phone, marker, content)
        self.assertEqual(result.http_status, 202, result.body)

    async def _force_ready(self) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND "
                    "WHERE session_slot='active' AND status IN ('pending','retry')"
                )
            )
            await db.commit()

    def _processor(self, llm, consumer) -> TurnProcessor:
        processor = TurnProcessor(
            self.sessions,
            worker_id=f"ticket05-worker-{uuid4()}",
            scheduler_epoch=self.scheduler_epoch,
            delivery_runtime_epoch=self.delivery_epoch,
            consumer=consumer,
            llm_call=llm,
            presence_call=_no_presence,
            lease_seconds=30,
            poll_interval_seconds=0.02,
            generation_timeout_seconds=10,
            typing_jitter_ms=0,
            release_backoff_seconds=1,
            concurrency=1,
            rng=random.Random(1),
        )
        self.processors.append(processor)
        return processor

    def _finalizer(self, summary, **kwargs) -> HandoffFinalizer:
        return HandoffFinalizer(
            self.sessions,
            llm_call=summary,
            generation_timeout_seconds=10,
            setup_ttl_seconds=600,
            release_backoff_seconds=1,
            **kwargs,
        )

    async def _until(self, predicate, message: str) -> None:
        deadline = asyncio.get_running_loop().time() + 20
        while not await predicate():
            if asyncio.get_running_loop().time() > deadline:
                self.fail(f"condition not reached: {message}")
            await asyncio.sleep(0.03)

    async def _run_one(self, processor: TurnProcessor) -> list[str]:
        tasks = await processor.run_pass()
        self.assertEqual(len(tasks), 1)
        return await asyncio.gather(*tasks)

    async def _expire_and_reclaim(self, processor: TurnProcessor, agente_id: int) -> None:
        async with self.sessions() as db:
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == agente_id)
                .values(lease_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.execute(
                update(ConversationTurn)
                .where(
                    ConversationTurn.agente_id == agente_id,
                    ConversationTurn.claim_token.is_not(None),
                )
                .values(claim_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        await processor._recover(agente_id)
        await asyncio.sleep(1.1)

    async def _count(self, model, *criteria) -> int:
        async with self.sessions() as db:
            column = list(inspect(model).primary_key)[0]
            return int(
                (await db.execute(select(func.count(column)).where(*criteria))).scalar_one()
            )

    async def _revision_status(self, status: str, number: int | None = None) -> bool:
        criteria = [TurnRevision.status == status]
        if number is not None:
            criteria.append(TurnRevision.revision_number == number)
        return await self._count(TurnRevision, *criteria) > 0

    async def _run_handoff(
        self,
        suffix: str,
        *,
        late_hook=None,
        response=None,
        summary=None,
        agent_kwargs=None,
    ):
        agent, instance = await self._agent(suffix, **(agent_kwargs or {}))
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, f"{suffix}-1", "preciso de atendimento")
        await self._force_ready()
        response = response or FakeLLM("Vou transferir você. [TRANSFERIR]")
        summary = summary or FakeLLM("resumo final")
        processor = self._processor(response, self._finalizer(summary))
        task = asyncio.create_task(self._run_one(processor))
        if late_hook is not None:
            await late_hook(instance, phone, summary)
        await asyncio.wait_for(task, timeout=30)
        return agent, instance, phone, response, summary

    async def _late_during_summary(self, suffix: str, *contents: str):
        release = asyncio.Event()

        async def during_summary(instance, phone, summary):
            summary.barrier = release
            await asyncio.wait_for(summary.entered.wait(), timeout=20)
            for index, content in enumerate(contents):
                await self._accept(instance, phone, f"{suffix}-late-{index}", content)
            release.set()

        return await self._run_handoff(suffix, late_hook=during_summary)

    async def _rows(self, model, *order):
        async with self.sessions() as db:
            return list(
                (await db.execute(select(model).order_by(*order))).scalars()
            )

    # ── barriers: typing / summary / terminal admission ─────────────────
    async def test_input_during_first_typing_refreshes_before_single_summary(self) -> None:
        agent, instance = await self._agent("typing")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "typing-1", "primeira")
        await self._force_ready()
        response = FakeLLM("[TRANSFERIR]", "[TRANSFERIR]")
        summary = FakeLLM("resumo com duas mensagens")
        processor = self._processor(response, self._finalizer(summary))
        task = asyncio.create_task(self._run_one(processor))
        await self._until(lambda: self._revision_status("typing", 1), "typing barrier")
        await self._accept(instance, phone, "typing-2", "durante typing")
        await asyncio.wait_for(task, timeout=30)
        self.assertEqual(len(response.calls), 2)
        self.assertEqual(len(summary.calls), 1)
        contents = [row["content"] for row in summary.calls[0]["messages"]]
        self.assertEqual(contents.count("primeira"), 1)
        self.assertEqual(contents.count("durante typing"), 1)
        self.assertEqual(await self._count(HandoffContextAssignment), 0)
        self.assertEqual(await self._count(ScheduledHandoff), 1)

    async def test_input_during_refreshed_typing_is_late_context_exactly_once(self) -> None:
        agent, instance = await self._agent("typing2")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "typing2-1", "primeira")
        await self._force_ready()
        response = FakeLLM("rascunho", "[TRANSFERIR]")
        response.barrier = asyncio.Event()
        summary = FakeLLM("resumo")
        processor = self._processor(response, self._finalizer(summary))
        task = asyncio.create_task(self._run_one(processor))
        await asyncio.wait_for(response.entered.wait(), timeout=10)
        await self._accept(instance, phone, "typing2-2", "refresh")
        response.barrier.set()
        await self._until(lambda: self._revision_status("typing", 2), "rev2 typing")
        await self._accept(instance, phone, "typing2-3", "depois do refresh")
        await asyncio.wait_for(task, timeout=30)
        contents = [row["content"] for row in summary.calls[0]["messages"]]
        self.assertIn("refresh", contents)
        self.assertNotIn("depois do refresh", contents)
        assignments = await self._rows(HandoffContextAssignment, HandoffContextAssignment.coverage_ordinal)
        late = (await self._rows(TurnInputMembership, TurnInputMembership.id))[-1]
        self.assertEqual([row.turn_input_membership_id for row in assignments], [late.id])
        self.assertEqual(late.membership_state, "post_handoff_pending")

    async def test_input_during_summary_is_captured_once_not_summarized(self) -> None:
        _, _, _, response, summary = await self._late_during_summary(
            "summary", "late exact context", "second late"
        )
        self.assertEqual(len(response.calls), 1)
        self.assertEqual(len(summary.calls), 1)
        self.assertNotIn(
            "late exact context",
            [row["content"] for row in summary.calls[0]["messages"]],
        )
        assignments = await self._rows(HandoffContextAssignment, HandoffContextAssignment.coverage_ordinal)
        memberships = await self._rows(TurnInputMembership, TurnInputMembership.id)
        deliveries = await self._rows(OutboxDelivery, OutboxDelivery.id)
        session = (await self._rows(Sessao, Sessao.id))[0]
        lane = (await self._rows(AgentServiceLane, AgentServiceLane.agente_id))[0]
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual(
            [(row.turn_input_membership_id, row.coverage_ordinal) for row in assignments],
            [(memberships[1].id, 1), (memberships[2].id, 2)],
        )
        self.assertEqual(
            [row.membership_state for row in memberships],
            ["sealed", "post_handoff_pending", "post_handoff_pending"],
        )
        self.assertEqual((session.handler, session.motivo_encerramento), ("encerrada", "handoff"))
        self.assertIsNone(lane.owner_token)
        self.assertEqual(
            [(row.session_slot, row.status, row.claim_token) for row in turns],
            [("active", "awaiting_delivery", None), ("terminal", "cancelled", None)],
        )
        # Ticket 06 stages the non-empty reply before the blocked transition.
        self.assertEqual(revision.status, "admitted")
        self.assertEqual(revision.draft_text, "Vou transferir você.")
        self.assertEqual(turns[0].final_revision_id, revision.id)
        operations = [(row.operation, row.status, row.delivery_class) for row in deliveries]
        self.assertEqual(
            operations,
            [
                (OPEN, "pending", "provider_setup_context"),
                (NOTE, "blocked", "provider_setup_context"),
                (SEND, "pending", "automated"),
                (SEND, "blocked", "automated"),
            ],
        )
        note = deliveries[1]
        self.assertEqual(
            note.payload["coverage"],
            [f"membership:{memberships[1].id}", f"membership:{memberships[2].id}"],
        )
        self.assertIn("late exact context", note.payload["content"])
        self.assertEqual(note.depends_on_delivery_id, deliveries[0].id)
        reply, transition = deliveries[2:]
        self.assertEqual((reply.automated_effect, transition.automated_effect),
                         ("reply", "handoff_transition"))
        self.assertLess(reply.stream_sequence, transition.stream_sequence)
        self.assertEqual(
            (transition.conversation_turn_id, transition.turn_revision_id, transition.depends_on_delivery_id),
            (turns[0].id, revision.id, deliveries[0].id),
        )
        self.assertEqual(await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"), 1)
        scheduled = (await self._rows(ScheduledHandoff, ScheduledHandoff.handoff_attempt_id))[0]
        self.assertEqual(scheduled.summary_kind, "generated")
        self.assertIsNotNone(scheduled.summary_generation_attempt_id)

    async def test_transfer_only_stages_transition_without_reply_or_sequence_gap(self) -> None:
        await self._run_handoff(
            "empty-reply",
            response=FakeLLM("[TRANSFERIR]"),
            summary=FakeLLM("resumo"),
        )
        automated = [
            row
            for row in await self._rows(OutboxDelivery, OutboxDelivery.id)
            if row.delivery_class == "automated"
        ]
        self.assertEqual(
            [(row.automated_effect, row.status) for row in automated],
            [("handoff_transition", "blocked")],
        )
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.next_delivery_sequence, automated[0].stream_sequence + 1)
        # Every allocated position of the session stream holds a row: no gap.
        stream = [
            row.stream_sequence
            for row in await self._rows(OutboxDelivery, OutboxDelivery.stream_sequence)
            if row.ordering_key == session.delivery_stream_key
        ]
        self.assertEqual(stream, list(range(1, session.next_delivery_sequence)))

    async def test_admitted_handoff_yield_cap_parks_without_orphaning_effects(self) -> None:
        await self._run_handoff("admitted-cap")
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        async with self.sessions() as db:
            self.assertTrue(
                await turn_delivery_repo.requeue_admitted_turn(
                    db,
                    turn_id=turn.id,
                    agente_id=turn.agente_id,
                    sessao_id=turn.sessao_id,
                    scheduler_epoch=turn.scheduler_epoch,
                    delivery_runtime_epoch=self.delivery_epoch,
                )
            )
            await db.commit()
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=turn.agente_id,
                scheduler_epoch=turn.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="ticket06-cap",
                lease_seconds=30,
            ),
        )
        self.assertIsNotNone(claim)
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        self.assertEqual(turn.status, "claimed")
        self.assertIsNotNone(turn.claim_token)
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.id == turn.id)
                .values(delivery_attempt_count=8)
            )
            await db.commit()
        before = scheduler_repo.admitted_handoff_park_signal_count()
        retry = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.yield_turn_for_retry(
                db,
                turn_id=turn.id,
                sessao_id=turn.sessao_id,
                agente_id=turn.agente_id,
                scheduler_epoch=turn.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                owner_token=turn.claim_token,
                retry_at=datetime.utcnow() + timedelta(seconds=5),
            ),
        )
        self.assertFalse(retry)
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        lane = (await self._rows(AgentServiceLane, AgentServiceLane.agente_id))[0]
        self.assertEqual((turn.status, turn.session_slot), ("awaiting_delivery", "active"))
        self.assertIsNone(turn.claim_token)
        self.assertIsNone(lane.owner_token)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(
            [(row.automated_effect, row.status) for row in await self._rows(OutboxDelivery, OutboxDelivery.id)
             if row.delivery_class == "automated"],
            [("reply", "pending"), ("handoff_transition", "blocked")],
        )
        self.assertEqual(
            scheduler_repo.admitted_handoff_park_signal_count(), before + 1
        )

    async def test_input_before_terminal_admission_locks_is_captured_once(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        original = turn_handoff_repo.complete_summary_and_finalize

        async def barrier(db, **kwargs):
            entered.set()
            await release.wait()
            return await original(db, **kwargs)

        with patch.object(turn_handoff_repo, "complete_summary_and_finalize", side_effect=barrier):
            async def during_admission(instance, phone, summary):
                await asyncio.wait_for(entered.wait(), timeout=20)
                await self._accept(instance, phone, "admit-late", "admission context")
                release.set()

            await self._run_handoff("admission", late_hook=during_admission)
        assigned = [row.turn_input_membership_id for row in await self._rows(HandoffContextAssignment, HandoffContextAssignment.coverage_ordinal)]
        payloads = [row.payload for row in await self._rows(OutboxDelivery, OutboxDelivery.id) if row.operation == NOTE]
        self.assertEqual(len(assigned), 1)
        self.assertEqual(
            [coverage for payload in payloads for coverage in payload["coverage"]],
            [f"membership:{assigned[0]}"],
        )

    async def test_input_during_locked_terminal_admission_keeps_closed_session_behavior(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        original = turn_handoff_repo.handoff_attempt_repo.create_for_locked_session

        async def barrier(db, **kwargs):
            entered.set()
            await release.wait()
            return await original(db, **kwargs)

        intake_results = []

        async def during_admission(instance, phone, summary):
            await asyncio.wait_for(entered.wait(), timeout=20)
            intake = asyncio.create_task(
                self._intake(instance, phone, "locked-late", "after cutoff")
            )
            await asyncio.sleep(0.5)
            self.assertFalse(intake.done(), "intake must wait on the locked session")
            release.set()
            intake_results.append(await asyncio.wait_for(intake, timeout=20))

        with patch.object(
            turn_handoff_repo.handoff_attempt_repo,
            "create_for_locked_session",
            side_effect=barrier,
        ):
            await self._run_handoff("locked", late_hook=during_admission)
        self.assertEqual(intake_results[0].body.get("reason"), "closed_session")
        self.assertEqual(await self._count(HandoffContextAssignment), 0)
        self.assertEqual(await self._count(TurnInputMembership), 1)
        receipts = await self._rows(InboundEvent, InboundEvent.id)
        self.assertEqual(
            [row.receipt_disposition for row in receipts], ["scheduled", "ignored_nonbot"]
        )
        self.assertEqual(await self._count(ConversationTurn), 1)

    async def test_real_deadlock_retries_whole_terminal_admission_once(self) -> None:
        """A real InnoDB 1213 inside admission retries the whole UoW once."""

        release = asyncio.Event()
        graph_locked = asyncio.Event()
        calls = []
        original = turn_handoff_repo.scheduler_repo.lock_intake_turn_graph

        async def observed(db, **kwargs):
            graph = await original(db, **kwargs)
            calls.append(1)
            if len(calls) == 2:
                graph_locked.set()
            return graph

        async def deadlock(instance, phone, summary):
            summary.barrier = release
            await asyncio.wait_for(summary.entered.wait(), timeout=20)
            await self._accept(instance, phone, "deadlock-late", "late during deadlock")
            late = (await self._rows(TurnInputMembership, TurnInputMembership.id))[-1]
            session_id = late.sessao_id
            async with self.sessions() as other:
                await other.begin()
                # Heavier transaction (undo records, no shared FK parents) so
                # InnoDB picks the admission transaction as the victim.
                for _ in range(200):
                    await other.execute(
                        text(
                            "INSERT INTO delivery_operator_actions (action, actor, reason) "
                            "VALUES ('ticket05-weight', 'test', 'deadlock weight')"
                        )
                    )
                await other.execute(
                    select(TurnInputMembership.id)
                    .where(TurnInputMembership.id == late.id)
                    .with_for_update()
                )
                release.set()
                await asyncio.wait_for(graph_locked.wait(), timeout=20)
                await asyncio.sleep(0.5)
                # Reverse edge: admission holds the session and waits on the
                # membership; this transaction now waits on the session.
                await other.execute(
                    select(Sessao.id).where(Sessao.id == session_id).with_for_update()
                )
                await other.rollback()

        with patch.object(
            turn_handoff_repo.scheduler_repo, "lock_intake_turn_graph", side_effect=observed
        ):
            _, _, _, _, summary = await self._run_handoff("deadlock", late_hook=deadlock)
        self.assertGreaterEqual(len(calls), 3, "admission must have been retried")
        self.assertEqual(len(summary.calls), 1)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(await self._count(HandoffAttempt), 1)
        self.assertEqual(await self._count(HandoffContextAssignment), 1)
        self.assertEqual(
            await self._count(GenerationAttempt, GenerationAttempt.attempt_type == "summary"), 1
        )
        self.assertEqual(await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"), 1)
        self.assertEqual(
            await self._count(
                DeliveryOperatorAction, DeliveryOperatorAction.action == "ticket05-weight"
            ),
            0,
        )

    # ── escape-heavy / oversized late input (review P1-1) ───────────────
    ESCAPE_HEAVY = "\x01" * 30000  # 6x under JSON escaping: one record > 150000

    async def _assert_escape_heavy_admitted(self, instance: str, phone: str, summary_calls: int) -> None:
        self.assertEqual(summary_calls, 1)
        self.assertEqual(await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"), 1)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual((session.handler, session.motivo_encerramento), ("encerrada", "handoff"))
        late = (await self._rows(TurnInputMembership, TurnInputMembership.id))[-1]
        assignments = await self._rows(HandoffContextAssignment, HandoffContextAssignment.coverage_ordinal)
        self.assertEqual([row.turn_input_membership_id for row in assignments], [late.id])
        self.assertEqual(late.membership_state, "post_handoff_pending")
        notes = [row for row in await self._rows(OutboxDelivery, OutboxDelivery.id) if row.operation == NOTE]
        coverage = [item for row in notes for item in row.payload["coverage"]]
        self.assertEqual(
            coverage,
            [f"membership:{late.id}#part{n:04d}/{len(notes):04d}" for n in range(1, len(notes) + 1)],
        )
        self.assertTrue(all(len(row.payload["content"]) <= 150_000 for row in notes))
        rebuilt = "".join(
            json.loads(line)["content"]
            for row in notes
            for line in row.payload["content"].split("\n")[1:]
        )
        self.assertEqual(rebuilt, self.ESCAPE_HEAVY)
        self.assertEqual({row.source_id for row in assignments}, {notes[0].payload["source_id"]})
        # Every part is dispatchable under exact ownership, in order.
        await self._deliver_open((await self._claim())[0])
        for _ in notes:
            batch = await self._claim()
            self.assertEqual([row.operation for row in batch], [NOTE])
            self.assertTrue(await self._complete_note(batch[0]))
        self.assertEqual(await self._claim(), [])
        result = await self._intake(instance, phone, f"after-{uuid4()}", "depois")
        self.assertEqual(result.body.get("reason"), "closed_session")

    async def test_escape_heavy_late_input_during_summary_is_split_and_admitted(self) -> None:
        release = asyncio.Event()
        names = {}

        async def during_summary(instance, phone, summary):
            names.update(instance=instance, phone=phone)
            summary.barrier = release
            await asyncio.wait_for(summary.entered.wait(), timeout=20)
            await self._accept(instance, phone, "escape-summary", self.ESCAPE_HEAVY)
            release.set()

        _, _, _, _, summary = await self._run_handoff("escape-summary", late_hook=during_summary)
        await self._assert_escape_heavy_admitted(names["instance"], names["phone"], len(summary.calls))

    async def test_escape_heavy_late_input_during_refreshed_typing_is_admitted(self) -> None:
        agent, instance = await self._agent("escape-typing")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "et-1", "primeira")
        await self._force_ready()
        response = FakeLLM("rascunho", "[TRANSFERIR]")
        response.barrier = asyncio.Event()
        summary = FakeLLM("resumo")
        processor = self._processor(response, self._finalizer(summary))
        task = asyncio.create_task(self._run_one(processor))
        await asyncio.wait_for(response.entered.wait(), timeout=10)
        await self._accept(instance, phone, "et-2", "refresh")
        response.barrier.set()
        await self._until(lambda: self._revision_status("typing", 2), "rev2 typing")
        await self._accept(instance, phone, "et-3", self.ESCAPE_HEAVY)
        await asyncio.wait_for(task, timeout=60)
        await self._assert_escape_heavy_admitted(instance, phone, len(summary.calls) and 1)
        self.assertEqual(len(summary.calls), 1)

    async def _late_before_begin(self, suffix: str, content: str, **agent_kwargs):
        """Late input lands after the seal, before begin_handoff takes locks."""

        gate = asyncio.Event()
        entered = asyncio.Event()
        original = turn_handoff_repo.begin_handoff

        async def held(db, **kwargs):
            entered.set()
            await gate.wait()
            return await original(db, **kwargs)

        async def hook(instance, phone, summary):
            await asyncio.wait_for(entered.wait(), timeout=20)
            await self._accept(instance, phone, f"{suffix}-late", content)
            gate.set()

        with patch.object(turn_handoff_repo, "begin_handoff", side_effect=held):
            return await self._run_handoff(suffix, late_hook=hook, agent_kwargs=agent_kwargs)

    async def test_escape_heavy_late_input_on_fallback_summary_path_is_admitted(self) -> None:
        _, instance, phone, _, summary = await self._late_before_begin(
            "escape-fallback", self.ESCAPE_HEAVY, limit=Decimal("0.000001")
        )
        self.assertEqual(len(summary.calls), 0)
        scheduled = (await self._rows(ScheduledHandoff, ScheduledHandoff.handoff_attempt_id))[0]
        self.assertEqual(scheduled.summary_kind, "soft_limit_fallback")
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        late = (await self._rows(TurnInputMembership, TurnInputMembership.id))[-1]
        self.assertEqual(late.membership_state, "post_handoff_pending")
        notes = [row for row in await self._rows(OutboxDelivery, OutboxDelivery.id) if row.operation == NOTE]
        self.assertGreater(len(notes), 1)
        result = await self._intake(instance, phone, "fallback-after", "depois")
        self.assertEqual(result.body.get("reason"), "closed_session")

    async def _assert_terminal_failure(self, instance: str, phone: str) -> None:
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.session_slot, row.status, row.terminal_reason) for row in turns],
            [
                ("terminal", "dead", "handoff_admission_failed"),
                ("terminal", "cancelled", "handoff_admission_failed"),
            ],
        )
        lane = (await self._rows(AgentServiceLane, AgentServiceLane.agente_id))[0]
        self.assertIsNone(lane.owner_token)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual((session.handler, session.motivo_encerramento), ("encerrada", "falha_tecnica"))
        self.assertEqual(await self._count(ScheduledHandoff), 0)
        self.assertEqual(await self._count(HandoffAttempt), 0)
        self.assertEqual(await self._count(OutboxDelivery), 0)
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual(revision.status, "dead")
        result = await self._intake(instance, phone, f"after-fail-{uuid4()}", "depois")
        self.assertEqual(result.body.get("reason"), "closed_session")
        # Nothing is claimable again: no reclaim can repay the summary.
        forbidden = ForbiddenLLM()
        processor = self._processor(forbidden, self._finalizer(forbidden))
        self.assertEqual(await processor.run_pass(), [])
        self.assertEqual(forbidden.calls, 0)

    async def test_part_coverage_ownership_is_exact(self) -> None:
        release = asyncio.Event()

        async def during_summary(instance, phone, summary):
            summary.barrier = release
            await asyncio.wait_for(summary.entered.wait(), timeout=20)
            await self._accept(instance, phone, "parts-late", self.ESCAPE_HEAVY)
            release.set()

        await self._run_handoff("parts", late_hook=during_summary)
        await self._deliver_open((await self._claim())[0])
        self.assertTrue(await self._complete_note((await self._claim())[0]))
        second = (await self._claim())[0]
        coverage = tuple(second.payload["coverage"])
        membership_id = int(coverage[0].split(":")[1].split("#")[0])
        self.assertTrue(coverage[0].endswith("#part0002/0002"))

        async def ownership(candidate):
            async with self.sessions() as db:
                result = await delivery_worker_repo.get_context_note_dispatch_ownership(
                    db,
                    delivery_id=second.id,
                    lease_token=second.lease_token,
                    cutover_epoch=self.delivery_epoch,
                    sessao_id=second.sessao_id,
                    attempt_id=second.handoff_attempt_id,
                    source_id=second.payload["source_id"],
                    coverage=candidate,
                )
                await db.rollback()
            return result

        self.assertIsNotNone(await ownership(coverage))
        for forged in (
            (f"membership:{membership_id}",),
            (f"membership:{membership_id}#part0001/0002",),
            (f"membership:{membership_id}#part0002/0002", f"membership:{membership_id + 1}"),
        ):
            with self.subTest(forged=forged):
                self.assertIsNone(await ownership(forged))

    async def test_deterministic_admission_failure_is_terminal_not_repaid(self) -> None:
        # Both failure kinds of the pure planner are promoted at its boundary.
        for index, planner_error in enumerate(
            (ValueError("planner validation"), AssertionError("planner invariant"))
        ):
            with self.subTest(error=type(planner_error).__name__):
                if index:
                    await self._wipe()

                def broken(*args, **kwargs):
                    raise planner_error

                release = asyncio.Event()
                names = {}

                async def during_summary(instance, phone, summary):
                    names.update(instance=instance, phone=phone)
                    summary.barrier = release
                    await asyncio.wait_for(summary.entered.wait(), timeout=20)
                    await self._accept(instance, phone, f"broken-late-{index}", "late")
                    release.set()

                with patch.object(turn_handoff_repo, "plan_context_chunks", side_effect=broken):
                    _, _, _, _, summary = await self._run_handoff(
                        f"broken-{index}", late_hook=during_summary
                    )
                self.assertEqual(len(summary.calls), 1)
                attempt = [row for row in await self._rows(GenerationAttempt, GenerationAttempt.id) if row.attempt_type == "summary"]
                self.assertEqual([(row.outcome, row.accounting_state) for row in attempt], [("succeeded", "recorded")])
                self.assertEqual(await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"), 1)
                await self._assert_terminal_failure(names["instance"], names["phone"])

    async def test_deterministic_failure_on_fallback_path_is_terminal(self) -> None:
        def broken(*args, **kwargs):
            raise ValueError("deterministic planner failure")

        with patch.object(turn_handoff_repo, "plan_context_chunks", side_effect=broken):
            _, instance, phone, _, summary = await self._late_before_begin(
                "broken-fallback", "late", limit=Decimal("0.000001")
            )
        self.assertEqual(len(summary.calls), 0)
        self.assertEqual(
            await self._count(GenerationAttempt, GenerationAttempt.attempt_type == "summary"), 0
        )
        await self._assert_terminal_failure(instance, phone)

    async def test_deterministic_admission_allowlist_is_terminal(self) -> None:
        errors = (
            turn_handoff_repo.HandoffLocalInvariantError("coverage invariant"),
            turn_handoff_repo.HandoffAdmissionValidationError("local validation"),
            outbox_repo.OutboxConflictError("outbox identity conflict"),
            scheduler_repo.SchedulerConflictError("scheduler state conflict"),
            delivery_adapters.InvalidDeliveryPayload("local payload invalid"),
        )
        for index, error in enumerate(errors):
            with self.subTest(error=type(error).__name__):
                if index:
                    await self._wipe()
                with patch.object(
                    turn_handoff_repo, "begin_handoff", side_effect=error
                ):
                    _, instance, phone, _, summary = await self._run_handoff(
                        f"allowlist-{index}"
                    )
                self.assertEqual(len(summary.calls), 0)
                turns = await self._rows(ConversationTurn, ConversationTurn.id)
                self.assertEqual(
                    [(row.session_slot, row.status, row.terminal_reason) for row in turns],
                    [("terminal", "dead", "handoff_admission_failed")],
                )
                session = (await self._rows(Sessao, Sessao.id))[0]
                self.assertEqual(
                    (session.handler, session.motivo_encerramento),
                    ("encerrada", "falha_tecnica"),
                )
                self.assertEqual(await self._count(ScheduledHandoff), 0)
                self.assertEqual(await self._count(HandoffAttempt), 0)
                self.assertEqual(await self._count(OutboxDelivery), 0)

    async def test_infrastructure_failures_before_summary_reclaim_and_admit(self) -> None:
        factories = (
            lambda: OperationalError("BEGIN", {}, Exception(1040, "too many connections")),
            lambda: OperationalError("BEGIN", {}, Exception(1290, "read only")),
            lambda: SATimeoutError("QueuePool limit reached"),
        )
        for index, factory in enumerate(factories):
            with self.subTest(error=type(factory()).__name__, case=index):
                if index:
                    await self._wipe()
                agent, instance = await self._agent(f"pre-infra-{index}")
                phone = f"5511{agent:08d}"
                await self._accept(instance, phone, f"pre-infra-{index}-1", "handoff")
                await self._force_ready()
                response = FakeLLM("[TRANSFERIR]")
                summary = FakeLLM("resumo recuperado")
                finalizer = self._finalizer(summary)
                processor = self._processor(response, finalizer)
                original = turn_handoff_repo.begin_handoff
                calls = 0

                async def fail_once(db, **kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        raise factory()
                    return await original(db, **kwargs)

                with patch.object(
                    turn_handoff_repo, "begin_handoff", side_effect=fail_once
                ):
                    self.assertEqual(await self._run_one(processor), ["error"])
                    self.assertEqual(len(summary.calls), 0)
                    await self._expire_and_reclaim(processor, agent)
                    await self._run_one(processor)
                self.assertEqual(await self._count(ScheduledHandoff), 1)
                self.assertEqual(await self._count(HandoffAttempt), 1)
                self.assertEqual(len(summary.calls), 1)
                self.assertEqual(
                    await self._count(
                        GenerationAttempt, GenerationAttempt.attempt_type == "summary"
                    ),
                    1,
                )

    async def test_infrastructure_failures_after_paid_summary_account_then_admit(self) -> None:
        factories = (
            lambda: OperationalError("COMMIT", {}, Exception(1040, "too many connections")),
            lambda: OperationalError("COMMIT", {}, Exception(1290, "read only")),
            lambda: SATimeoutError("QueuePool limit reached"),
        )
        for index, factory in enumerate(factories):
            with self.subTest(error=type(factory()).__name__, case=index):
                if index:
                    await self._wipe()
                agent, instance = await self._agent(f"post-infra-{index}")
                phone = f"5511{agent:08d}"
                await self._accept(instance, phone, f"post-infra-{index}-1", "handoff")
                await self._force_ready()
                response = FakeLLM("[TRANSFERIR]")
                summary = FakeLLM("resumo um", "resumo dois")
                finalizer = self._finalizer(summary)
                processor = self._processor(response, finalizer)
                original = turn_handoff_repo.complete_summary_and_finalize
                calls = 0

                async def fail_once(db, **kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        raise factory()
                    return await original(db, **kwargs)

                with patch.object(
                    turn_handoff_repo,
                    "complete_summary_and_finalize",
                    side_effect=fail_once,
                ):
                    await self._run_one(processor)
                    self.assertEqual(await self._count(ScheduledHandoff), 0)
                    self.assertEqual(
                        await self._count(
                            GenerationAttempt,
                            GenerationAttempt.attempt_type == "summary",
                            GenerationAttempt.accounting_state == "recorded",
                        ),
                        1,
                    )
                    await self._expire_and_reclaim(processor, agent)
                    await self._run_one(processor)
                self.assertEqual(await self._count(ScheduledHandoff), 1)
                self.assertEqual(await self._count(HandoffAttempt), 1)
                self.assertEqual(len(summary.calls), 2)
                self.assertEqual(
                    await self._count(
                        GenerationAttempt, GenerationAttempt.attempt_type == "summary"
                    ),
                    2,
                )
                self.assertEqual(
                    await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"),
                    2,
                )

    async def test_unknown_exception_is_accounted_and_reclaimed_not_terminal(self) -> None:
        agent, instance = await self._agent("unknown-admission")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "unknown-admission-1", "handoff")
        await self._force_ready()
        response = FakeLLM("[TRANSFERIR]")
        summary = FakeLLM("resumo um", "resumo dois")
        processor = self._processor(response, self._finalizer(summary))
        original = turn_handoff_repo.complete_summary_and_finalize
        calls = 0

        async def fail_once(db, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("unknown dependency failure")
            return await original(db, **kwargs)

        with patch.object(
            turn_handoff_repo, "complete_summary_and_finalize", side_effect=fail_once
        ):
            await self._run_one(processor)
            self.assertEqual(await self._count(ScheduledHandoff), 0)
            session = (await self._rows(Sessao, Sessao.id))[0]
            self.assertEqual(session.handler, "bot")
            await self._expire_and_reclaim(processor, agent)
            await self._run_one(processor)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(len(summary.calls), 2)

    async def test_terminalization_failure_falls_back_to_exact_accounting(self) -> None:
        agent, instance = await self._agent("terminal-fallback")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "terminal-fallback-1", "handoff")
        await self._force_ready()
        response = FakeLLM("[TRANSFERIR]")
        summary = FakeLLM("resumo um", "resumo dois")
        finalizer = self._finalizer(summary)
        processor = self._processor(response, finalizer)
        original = turn_handoff_repo.complete_summary_and_finalize
        calls = 0

        async def deterministic_once(db, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise turn_handoff_repo.HandoffAdmissionValidationError("local invalid")
            return await original(db, **kwargs)

        with (
            patch.object(
                turn_handoff_repo,
                "complete_summary_and_finalize",
                side_effect=deterministic_once,
            ),
            patch.object(
                turn_handoff_repo,
                "fail_handoff_terminally",
                side_effect=SATimeoutError("terminal transaction pool timeout"),
            ),
        ):
            await self._run_one(processor)
        self.assertEqual(finalizer.terminal_failure_fallback_count, 1)
        self.assertEqual(await self._count(ScheduledHandoff), 0)
        self.assertEqual(
            await self._count(
                GenerationAttempt,
                GenerationAttempt.attempt_type == "summary",
                GenerationAttempt.accounting_state == "recorded",
            ),
            1,
        )
        await self._expire_and_reclaim(processor, agent)
        await self._run_one(processor)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(len(summary.calls), 2)

    async def test_driver_assertion_mid_admission_is_accounted_then_admitted(self) -> None:
        agent, instance = await self._agent("driver-assert")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "driver-assert-1", "handoff")
        await self._force_ready()
        summary = FakeLLM("resumo um", "resumo dois")
        processor = self._processor(FakeLLM("[TRANSFERIR]"), self._finalizer(summary))
        original = turn_handoff_repo.handoff_attempt_repo.create_for_locked_session
        calls = 0

        async def desync_once(db, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                # Shape of aiomysql/pymysql wire desync inside admission.
                raise AssertionError("Protocol error, expecting EOF")
            return await original(db, **kwargs)

        with patch.object(
            turn_handoff_repo.handoff_attempt_repo,
            "create_for_locked_session",
            side_effect=desync_once,
        ):
            await self._run_one(processor)
            self.assertEqual(await self._count(ScheduledHandoff), 0)
            turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
            self.assertEqual(turn.session_slot, "active")
            self.assertEqual((await self._rows(Sessao, Sessao.id))[0].handler, "bot")
            self.assertEqual(
                await self._count(
                    GenerationAttempt,
                    GenerationAttempt.attempt_type == "summary",
                    GenerationAttempt.accounting_state == "recorded",
                ),
                1,
            )
            await self._expire_and_reclaim(processor, agent)
            await self._run_one(processor)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(await self._count(HandoffAttempt), 1)
        self.assertEqual(len(summary.calls), 2)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual((session.handler, session.motivo_encerramento), ("encerrada", "handoff"))

    async def test_cancellation_during_admission_accounts_known_paid_summary(self) -> None:
        agent, instance = await self._agent("cancel-admission")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "cancel-admission-1", "handoff")
        await self._force_ready()
        summary = FakeLLM("resumo um", "resumo dois")
        processor = self._processor(FakeLLM("[TRANSFERIR]"), self._finalizer(summary))
        original = turn_handoff_repo.complete_summary_and_finalize
        calls = 0

        async def cancel_once(db, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise asyncio.CancelledError()
            return await original(db, **kwargs)

        with patch.object(
            turn_handoff_repo, "complete_summary_and_finalize", side_effect=cancel_once
        ):
            with self.assertRaises(asyncio.CancelledError):
                await self._run_one(processor)
            attempts = [
                (row.outcome, row.accounting_state)
                for row in await self._rows(GenerationAttempt, GenerationAttempt.id)
                if row.attempt_type == "summary"
            ]
            self.assertEqual(attempts, [("succeeded", "recorded")])
            self.assertEqual(await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"), 1)
            await self._expire_and_reclaim(processor, agent)
            await self._run_one(processor)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(len(summary.calls), 2)

    async def test_cancellation_while_recording_response_accounts_attempt(self) -> None:
        agent, instance = await self._agent("cancel-response")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "cancel-response-1", "oi")
        await self._force_ready()
        processor = self._processor(FakeLLM("resposta"), lambda *a: asyncio.sleep(0))
        original = turn_generation_repo.record_generation_result
        calls = 0

        async def cancel_once(db, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise asyncio.CancelledError()
            return await original(db, **kwargs)

        with patch.object(
            turn_generation_repo, "record_generation_result", side_effect=cancel_once
        ):
            with self.assertRaises(asyncio.CancelledError):
                await self._run_one(processor)
        attempts = [
            (row.outcome, row.accounting_state)
            for row in await self._rows(GenerationAttempt, GenerationAttempt.id)
        ]
        self.assertEqual(attempts, [("succeeded", "recorded")])
        self.assertEqual(await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resposta"), 1)

    async def _prepare_cap_candidate(self, suffix: str, response_text: str):
        agent, instance = await self._agent(suffix)
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, f"{suffix}-1", "mensagem")
        await self._force_ready()
        captured = []

        async def consumer(candidate, handle):
            captured.append(candidate)

        await self._run_one(self._processor(FakeLLM(response_text), consumer))
        candidate = captured[0]
        await self._accept(instance, phone, f"{suffix}-late", "late input")
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.id == candidate.lease.turn_id)
                .values(delivery_attempt_count=8)
            )
            await db.commit()
        return agent, instance, phone, candidate

    async def _expire_cap_candidate(self, candidate) -> None:
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.id == candidate.lease.turn_id)
                .values(claim_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == candidate.lease.agente_id)
                .values(lease_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()

    async def _recover_cap(self, agent: int):
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.recover_expired_lane(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                retry_at=datetime.utcnow() + timedelta(seconds=5),
            ),
        )

    async def _start_paused_typing(self, suffix: str, *responses: str):
        """Run the real processor and pause its first committed typing wait."""

        agent, instance = await self._agent(suffix)
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, f"{suffix}-1", "primeira")
        await self._force_ready()
        llm = FakeLLM(*responses)
        captured = []

        async def consumer(candidate, handle):
            captured.append(candidate)

        processor = self._processor(llm, consumer)
        original_wait = processor._wait
        paused = asyncio.Event()
        release = asyncio.Event()

        async def pause_first_typing(lease, step):
            if step.phase == "typing" and not paused.is_set():
                paused.set()
                await release.wait()
                return
            await original_wait(lease, step)

        processor._wait = pause_first_typing
        task = asyncio.create_task(self._run_one(processor))
        return agent, instance, phone, processor, llm, captured, task, paused, release

    async def _set_active_turn_at_cap(self, agent: int, *, expire: bool) -> tuple[int, str]:
        async with self.sessions() as db:
            lane = (
                await db.execute(
                    select(AgentServiceLane)
                    .where(AgentServiceLane.agente_id == agent)
                )
            ).scalar_one()
            self.assertIsNotNone(lane.active_turn_id)
            self.assertIsNotNone(lane.owner_token)
            values = {"delivery_attempt_count": 8}
            lane_values = {}
            if expire:
                values["claim_until"] = func.utc_timestamp(6) - text(
                    "INTERVAL 1 SECOND"
                )
                lane_values["lease_until"] = func.utc_timestamp(6) - text(
                    "INTERVAL 1 SECOND"
                )
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.id == lane.active_turn_id)
                .values(**values)
            )
            if lane_values:
                await db.execute(
                    update(AgentServiceLane)
                    .where(AgentServiceLane.agente_id == agent)
                    .values(**lane_values)
                )
            await db.commit()
            return lane.active_turn_id, lane.owner_token

    async def _yield_cap(self, agent: int, turn_id: int, owner_token: str) -> bool:
        async with self.sessions() as db:
            session_id = (await db.get(ConversationTurn, turn_id)).sessao_id
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.yield_turn_for_retry(
                db,
                turn_id=turn_id,
                sessao_id=session_id,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                owner_token=owner_token,
                retry_at=datetime.utcnow() + timedelta(seconds=5),
            ),
        )

    async def _make_preserved_refresh_ready(self) -> None:
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(
                    ConversationTurn.session_slot == "active",
                    ConversationTurn.status == "retry",
                )
                .values(
                    ready_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                    retry_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                )
            )
            await db.commit()

    async def _assert_preserved_refresh_processed(
        self,
        *,
        processor,
        llm,
        captured,
        original_turn_id: int,
    ) -> None:
        await self._make_preserved_refresh_ready()
        await asyncio.sleep(0)
        await asyncio.wait_for(self._run_one(processor), timeout=30)
        revisions = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(
            [(row.revision_number, row.status, row.superseded_reason) for row in revisions],
            [
                (1, "superseded", "refresh_input_at_scheduler_cap"),
                (2, "final_candidate", None),
            ],
        )
        self.assertEqual(len(llm.calls), 2)
        self.assertEqual(len(captured), 1)
        self.assertFalse(captured[0].needs_handoff)
        self.assertEqual(
            (
                revisions[1].sealed_first_input_sequence,
                revisions[1].sealed_last_input_sequence,
                revisions[1].sealed_input_count,
            ),
            (1, 2, 2),
        )
        memberships = await self._rows(TurnInputMembership, TurnInputMembership.id)
        self.assertEqual({row.turn_id for row in memberships}, {original_turn_id})
        self.assertEqual([row.membership_state for row in memberships], ["sealed", "sealed"])
        messages = await self._rows(Mensagem, Mensagem.id)
        self.assertEqual(
            [(row.role, row.conteudo) for row in messages],
            [
                ("user", "primeira"),
                ("user", "refresh"),
                ("assistant", "resposta final"),
            ],
        )
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.handler, "bot")
        self.assertEqual(
            await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resposta"), 2
        )

    async def _run_refresh_cap_case(self, suffix: str, *, recovery: bool) -> None:
        (
            agent,
            instance,
            phone,
            processor,
            llm,
            captured,
            task,
            paused,
            release,
        ) = await self._start_paused_typing(suffix, "[TRANSFERIR]", "resposta final")
        await asyncio.wait_for(paused.wait(), timeout=20)
        await self._accept(instance, phone, f"{suffix}-refresh", "refresh")
        before_memberships = await self._rows(
            TurnInputMembership, TurnInputMembership.id
        )
        before_turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        expected_ready = min(
            before_memberships[-1].created_at + timedelta(seconds=2),
            before_memberships[0].created_at + timedelta(seconds=5),
        )
        self.assertEqual(before_turn.ready_at, expected_ready)
        turn_id, owner_token = await self._set_active_turn_at_cap(
            agent, expire=recovery
        )
        try:
            if recovery:
                self.assertEqual(await self._recover_cap(agent), "retry")
            else:
                self.assertTrue(await self._yield_cap(agent, turn_id, owner_token))
            preserved = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
            self.assertEqual(preserved.ready_at, expected_ready)
            self.assertEqual(preserved.delivery_attempt_count, 8)
        finally:
            release.set()
        await asyncio.wait_for(task, timeout=20)
        await self._assert_preserved_refresh_processed(
            processor=processor,
            llm=llm,
            captured=captured,
            original_turn_id=turn_id,
        )

    async def test_recovery_cap_preserves_pending_refresh_then_processes_rev2(self) -> None:
        await self._run_refresh_cap_case("refresh-recovery-cap", recovery=True)

    async def test_yield_cap_preserves_pending_refresh_then_processes_rev2(self) -> None:
        await self._run_refresh_cap_case("refresh-yield-cap", recovery=False)

    async def _run_rev1_without_refresh_cap_case(
        self, suffix: str, *, recovery: bool
    ) -> None:
        (
            agent,
            _,
            _,
            _,
            _,
            _,
            task,
            paused,
            release,
        ) = await self._start_paused_typing(suffix, "[TRANSFERIR]")
        await asyncio.wait_for(paused.wait(), timeout=20)
        turn_id, owner_token = await self._set_active_turn_at_cap(
            agent, expire=recovery
        )
        try:
            if recovery:
                self.assertEqual(await self._recover_cap(agent), "dead")
            else:
                self.assertFalse(await self._yield_cap(agent, turn_id, owner_token))
        finally:
            release.set()
        await asyncio.wait_for(task, timeout=20)
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual((revision.revision_number, revision.status), (1, "dead"))
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(
            (session.handler, session.motivo_encerramento),
            ("encerrada", "falha_tecnica"),
        )

    async def test_recovery_cap_terminalizes_rev1_typing_without_pending_refresh(self) -> None:
        await self._run_rev1_without_refresh_cap_case(
            "no-refresh-recovery-cap", recovery=True
        )

    async def test_yield_cap_terminalizes_rev1_typing_without_pending_refresh(self) -> None:
        await self._run_rev1_without_refresh_cap_case(
            "no-refresh-yield-cap", recovery=False
        )

    async def _start_paused_rev2_typing(
        self, suffix: str, *, second_response: str = "[TRANSFERIR]"
    ):
        agent, instance = await self._agent(suffix)
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, f"{suffix}-1", "primeira")
        await self._force_ready()
        provider_release = asyncio.Event()
        llm = FakeLLM("[TRANSFERIR]", second_response)
        llm.barrier = provider_release

        async def consumer(candidate, handle):
            raise AssertionError("rev2 must remain paused before final candidate")

        processor = self._processor(llm, consumer)
        original_wait = processor._wait
        paused = asyncio.Event()
        release = asyncio.Event()

        async def pause_typing(lease, step):
            if step.phase == "typing" and not paused.is_set():
                paused.set()
                await release.wait()
                return
            await original_wait(lease, step)

        processor._wait = pause_typing
        task = asyncio.create_task(self._run_one(processor))
        await asyncio.wait_for(llm.entered.wait(), timeout=20)
        await self._accept(instance, phone, f"{suffix}-refresh", "refresh")
        provider_release.set()
        await asyncio.wait_for(paused.wait(), timeout=20)
        revisions = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(
            [(row.revision_number, row.status) for row in revisions],
            [(1, "superseded"), (2, "typing")],
        )
        return agent, task, release

    async def _run_rev2_typing_cap_case(self, suffix: str, *, recovery: bool) -> None:
        agent, task, release = await self._start_paused_rev2_typing(suffix)
        turn_id, owner_token = await self._set_active_turn_at_cap(
            agent, expire=recovery
        )
        try:
            if recovery:
                self.assertEqual(await self._recover_cap(agent), "dead")
            else:
                self.assertFalse(await self._yield_cap(agent, turn_id, owner_token))
        finally:
            release.set()
        await asyncio.wait_for(task, timeout=20)
        revisions = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(
            [(row.revision_number, row.status) for row in revisions],
            [(1, "superseded"), (2, "dead")],
        )
        self.assertEqual(len(revisions), 2)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(
            (session.handler, session.motivo_encerramento),
            ("encerrada", "falha_tecnica"),
        )

    async def test_recovery_cap_terminalizes_rev2_typing_handoff(self) -> None:
        await self._run_rev2_typing_cap_case("rev2-recovery-cap", recovery=True)

    async def test_yield_cap_terminalizes_rev2_typing_handoff(self) -> None:
        await self._run_rev2_typing_cap_case("rev2-yield-cap", recovery=False)

    async def _run_rev2_nonhandoff_cap_case(
        self, suffix: str, *, recovery: bool
    ) -> None:
        agent, task, release = await self._start_paused_rev2_typing(
            suffix, second_response="resposta revisada"
        )
        turn_id, owner_token = await self._set_active_turn_at_cap(
            agent, expire=recovery
        )
        signal_before = scheduler_repo.handoff_cap_signal_count()
        try:
            if recovery:
                self.assertEqual(await self._recover_cap(agent), "dead")
                expected_reason = "lease_recovery_attempt_cap"
            else:
                self.assertFalse(await self._yield_cap(agent, turn_id, owner_token))
                expected_reason = "delivery_attempt_cap"
        finally:
            release.set()
        await asyncio.wait_for(task, timeout=20)
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("dead", "terminal", expected_reason),
        )
        revisions = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(
            [
                (row.revision_number, row.status, row.needs_handoff)
                for row in revisions
            ],
            [(1, "superseded", True), (2, "typing", False)],
        )
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.handler, "bot")
        self.assertEqual(
            scheduler_repo.handoff_cap_signal_count(), signal_before
        )

    async def test_recovery_cap_rev2_nonhandoff_draft_overrides_old_intent(self) -> None:
        await self._run_rev2_nonhandoff_cap_case(
            "rev2-nonhandoff-recovery", recovery=True
        )

    async def test_yield_cap_rev2_nonhandoff_draft_overrides_old_intent(self) -> None:
        await self._run_rev2_nonhandoff_cap_case(
            "rev2-nonhandoff-yield", recovery=False
        )

    async def _claim_preserved_refresh(self, agent: int):
        await self._make_preserved_refresh_ready()
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="always-fails",
                lease_seconds=30,
            ),
        )

    async def _run_every_pass_fails_case(self, suffix: str, *, recovery: bool) -> None:
        (
            agent,
            instance,
            phone,
            _,
            _,
            _,
            task,
            paused,
            release,
        ) = await self._start_paused_typing(suffix, "[TRANSFERIR]")
        await asyncio.wait_for(paused.wait(), timeout=20)
        await self._accept(instance, phone, f"{suffix}-refresh", "refresh")
        turn_id, owner_token = await self._set_active_turn_at_cap(
            agent, expire=recovery
        )
        if recovery:
            self.assertEqual(await self._recover_cap(agent), "retry")
        else:
            self.assertTrue(await self._yield_cap(agent, turn_id, owner_token))
        release.set()
        await asyncio.wait_for(task, timeout=20)
        claim = await self._claim_preserved_refresh(agent)
        self.assertIsNotNone(claim)
        signal_before = scheduler_repo.handoff_cap_signal_count()
        with patch.object(scheduler_repo.logger, "error") as signal:
            if recovery:
                await self._set_active_turn_at_cap(agent, expire=True)
                self.assertEqual(await self._recover_cap(agent), "dead")
            else:
                self.assertFalse(
                    await self._yield_cap(agent, claim.turn_id, claim.claim_token)
                )
        signal.assert_called_once()
        self.assertEqual(
            scheduler_repo.handoff_cap_signal_count(), signal_before + 1
        )
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.status, row.session_slot, row.terminal_reason) for row in turns],
            [("dead", "terminal", "handoff_recovery_cap")],
        )
        revisions = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(
            [(row.revision_number, row.status) for row in revisions],
            [(1, "superseded"), (2, "dead")],
        )
        self.assertEqual(len(revisions), 2, "the cap grants only one refresh reprieve")
        memberships = await self._rows(TurnInputMembership, TurnInputMembership.id)
        self.assertEqual(
            [row.membership_state for row in memberships], ["sealed", "active"]
        )
        self.assertEqual(await self._count(HandoffContextAssignment), 0)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(
            (session.handler, session.motivo_encerramento),
            ("encerrada", "falha_tecnica"),
        )
        closed = await self._intake(
            instance, phone, f"{suffix}-after-terminal", "depois"
        )
        self.assertEqual(closed.body.get("reason"), "closed_session")

    async def test_recovery_cap_every_pass_fails_terminates_after_one_refresh(self) -> None:
        await self._run_every_pass_fails_case("fails-recovery-cap", recovery=True)

    async def test_yield_cap_every_pass_fails_terminates_after_one_refresh(self) -> None:
        await self._run_every_pass_fails_case("fails-yield-cap", recovery=False)

    async def test_recovery_cap_does_not_carry_ordinary_supersession_reason(self) -> None:
        (
            agent,
            instance,
            phone,
            processor,
            _,
            _,
            task,
            typing_paused,
            typing_release,
        ) = await self._start_paused_typing(
            "ordinary-supersession-reason", "[TRANSFERIR]", "unused"
        )
        await asyncio.wait_for(typing_paused.wait(), timeout=20)
        await self._accept(
            instance,
            phone,
            "ordinary-supersession-reason-refresh",
            "refresh",
        )
        collecting_paused = asyncio.Event()
        collecting_release = asyncio.Event()
        previous_wait = processor._wait

        async def pause_collecting(lease, step):
            if step.phase == "collecting" and not collecting_paused.is_set():
                collecting_paused.set()
                await collecting_release.wait()
                return
            await previous_wait(lease, step)

        processor._wait = pause_collecting
        typing_release.set()
        await asyncio.wait_for(collecting_paused.wait(), timeout=20)
        revisions = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(
            [
                (row.revision_number, row.status, row.superseded_reason)
                for row in revisions
            ],
            [
                (1, "superseded", "refresh_input_during_typing"),
                (2, "collecting", None),
            ],
        )
        await self._set_active_turn_at_cap(agent, expire=True)
        signal_before = scheduler_repo.handoff_cap_signal_count()
        try:
            self.assertEqual(await self._recover_cap(agent), "dead")
        finally:
            collecting_release.set()
        await asyncio.wait_for(task, timeout=20)
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        self.assertEqual(turn.terminal_reason, "lease_recovery_attempt_cap")
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.handler, "bot")
        self.assertEqual(
            scheduler_repo.handoff_cap_signal_count(), signal_before
        )

    async def test_recovery_cap_terminalizes_decided_handoff(self) -> None:
        agent, instance = await self._agent("handoff-recovery-cap")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "handoff-recovery-cap-1", "handoff")
        await self._force_ready()
        release = asyncio.Event()
        summary = FakeLLM("resumo um", "resumo dois")
        summary.barrier = release
        processor = self._processor(FakeLLM("[TRANSFERIR]"), self._finalizer(summary))

        async def persistent(db, **kwargs):
            raise RuntimeError("persistent unknown dependency failure")

        with patch.object(
            turn_handoff_repo, "complete_summary_and_finalize", side_effect=persistent
        ):
            task = asyncio.create_task(self._run_one(processor))
            await asyncio.wait_for(summary.entered.wait(), timeout=20)
            await self._accept(instance, phone, "handoff-recovery-cap-late", "late input")
            release.set()
            await asyncio.wait_for(task, timeout=30)
            summary.barrier = None
            await self._expire_and_reclaim(processor, agent)
            await self._run_one(processor)
        self.assertEqual(len(summary.calls), 2)
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.session_slot == "active")
                .values(
                    delivery_attempt_count=8,
                    claim_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                )
            )
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == agent)
                .values(lease_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        signal_before = scheduler_repo.handoff_cap_signal_count()
        with patch.object(scheduler_repo.logger, "error") as signal:
            outcome = await self._recover_cap(agent)
        self.assertEqual(outcome, "dead")
        signal.assert_called_once()
        self.assertEqual(
            scheduler_repo.handoff_cap_signal_count(), signal_before + 1
        )
        self.assertEqual(signal.call_args.args[-2], signal_before + 1)
        self.assertEqual(signal.call_args.args[-1], 1)
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.session_slot, row.status, row.terminal_reason) for row in turns],
            [
                ("terminal", "dead", "handoff_recovery_cap"),
                ("terminal", "cancelled", "handoff_recovery_cap"),
            ],
        )
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(
            (session.handler, session.motivo_encerramento),
            ("encerrada", "falha_tecnica"),
        )
        memberships = await self._rows(TurnInputMembership, TurnInputMembership.id)
        self.assertEqual(
            [(row.turn_id, row.membership_state) for row in memberships],
            [(turns[0].id, "sealed"), (turns[1].id, "active")],
        )
        self.assertEqual(await self._count(HandoffContextAssignment), 0)
        self.assertEqual(await self._count(ScheduledHandoff), 0)
        self.assertEqual(
            await self._count(
                GenerationAttempt,
                GenerationAttempt.attempt_type == "summary",
                GenerationAttempt.accounting_state == "recorded",
            ),
            2,
        )
        self.assertEqual(
            await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"), 2
        )
        closed = await self._intake(instance, phone, "handoff-recovery-cap-after", "depois")
        self.assertEqual(closed.body.get("reason"), "closed_session")

    async def test_recovery_cap_promotes_ordinary_successor(self) -> None:
        agent, instance, phone, candidate = await self._prepare_cap_candidate(
            "ordinary-recovery-cap", "resposta comum"
        )
        await self._expire_cap_candidate(candidate)
        self.assertEqual(await self._recover_cap(agent), "dead")
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.session_slot, row.status, row.terminal_reason) for row in turns],
            [
                ("terminal", "dead", "lease_recovery_attempt_cap"),
                ("active", "pending", None),
            ],
        )
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.handler, "bot")
        await self._accept(instance, phone, "ordinary-recovery-cap-after", "depois")
        memberships = await self._rows(TurnInputMembership, TurnInputMembership.id)
        self.assertEqual(
            [row.turn_id for row in memberships[1:]], [turns[1].id, turns[1].id]
        )

    async def test_yield_cap_terminalizes_decided_handoff(self) -> None:
        _, instance, phone, candidate = await self._prepare_cap_candidate(
            "handoff-yield-cap", "[TRANSFERIR]"
        )
        lease = candidate.lease
        signal_before = scheduler_repo.handoff_cap_signal_count()
        with patch.object(scheduler_repo.logger, "error") as signal:
            retry = await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions,
                lambda db: scheduler_repo.yield_turn_for_retry(
                    db,
                    turn_id=lease.turn_id,
                    sessao_id=lease.sessao_id,
                    agente_id=lease.agente_id,
                    scheduler_epoch=lease.scheduler_epoch,
                    delivery_runtime_epoch=lease.delivery_runtime_epoch,
                    owner_token=lease.owner_token,
                    retry_at=datetime.utcnow() + timedelta(seconds=5),
                ),
            )
        self.assertFalse(retry)
        signal.assert_called_once()
        self.assertEqual(
            scheduler_repo.handoff_cap_signal_count(), signal_before + 1
        )
        self.assertEqual(signal.call_args.args[-2], signal_before + 1)
        self.assertEqual(signal.call_args.args[-1], 1)
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.session_slot, row.status, row.terminal_reason) for row in turns],
            [
                ("terminal", "dead", "handoff_recovery_cap"),
                ("terminal", "cancelled", "handoff_recovery_cap"),
            ],
        )
        memberships = await self._rows(TurnInputMembership, TurnInputMembership.id)
        self.assertEqual([row.membership_state for row in memberships], ["sealed", "active"])
        self.assertEqual(await self._count(HandoffContextAssignment), 0)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(
            (session.handler, session.motivo_encerramento),
            ("encerrada", "falha_tecnica"),
        )
        closed = await self._intake(instance, phone, "handoff-yield-cap-after", "depois")
        self.assertEqual(closed.body.get("reason"), "closed_session")

    async def test_yield_cap_promotes_ordinary_successor(self) -> None:
        _, instance, phone, candidate = await self._prepare_cap_candidate(
            "ordinary-yield-cap", "resposta comum"
        )
        lease = candidate.lease
        retry = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.yield_turn_for_retry(
                db,
                turn_id=lease.turn_id,
                sessao_id=lease.sessao_id,
                agente_id=lease.agente_id,
                scheduler_epoch=lease.scheduler_epoch,
                delivery_runtime_epoch=lease.delivery_runtime_epoch,
                owner_token=lease.owner_token,
                retry_at=datetime.utcnow() + timedelta(seconds=5),
            ),
        )
        self.assertFalse(retry)
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.session_slot, row.status, row.terminal_reason) for row in turns],
            [
                ("terminal", "dead", "delivery_attempt_cap"),
                ("active", "pending", None),
            ],
        )
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.handler, "bot")
        await self._accept(instance, phone, "ordinary-yield-cap-after", "depois")
        memberships = await self._rows(TurnInputMembership, TurnInputMembership.id)
        self.assertEqual(
            [row.turn_id for row in memberships[1:]], [turns[1].id, turns[1].id]
        )

    async def test_terminal_completion_without_promotion_refuses_to_orphan(self) -> None:
        agent, instance = await self._agent("orphan-guard")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "orphan-1", "oi")
        await self._force_ready()
        captured = []

        async def consumer(candidate, handle):
            captured.append(candidate)

        await asyncio.wait_for(
            self._run_one(self._processor(FakeLLM("resposta"), consumer)), timeout=30
        )
        candidate = captured[0]
        lease = candidate.lease
        await self._accept(instance, phone, "orphan-2", "sucessor")

        async def reclaim_and_admit(db):
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == agent)
                .values(
                    owner_token=lease.owner_token,
                    owner_worker_id="test",
                    lease_until=func.utc_timestamp(6) + text("INTERVAL 30 SECOND"),
                    active_turn_id=lease.turn_id,
                    active_revision_id=candidate.revision_id,
                )
            )
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.id == lease.turn_id)
                .values(
                    claim_token=lease.owner_token,
                    claim_owner="test",
                    claim_until=func.utc_timestamp(6) + text("INTERVAL 30 SECOND"),
                )
            )

        await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, reclaim_and_admit)
        await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.admit_revision(
                db,
                turn_id=lease.turn_id,
                sessao_id=lease.sessao_id,
                revision_id=candidate.revision_id,
                agente_id=lease.agente_id,
                scheduler_epoch=lease.scheduler_epoch,
                delivery_runtime_epoch=lease.delivery_runtime_epoch,
                owner_token=lease.owner_token,
                expected_first_input_sequence=candidate.sealed_first_input_sequence,
                expected_last_input_sequence=candidate.sealed_last_input_sequence,
            ),
        )

        def complete(promote):
            return lambda db: scheduler_repo.complete_turn_and_release_lane(
                db,
                turn_id=lease.turn_id,
                sessao_id=lease.sessao_id,
                revision_id=candidate.revision_id,
                agente_id=lease.agente_id,
                scheduler_epoch=lease.scheduler_epoch,
                delivery_runtime_epoch=lease.delivery_runtime_epoch,
                owner_token=lease.owner_token,
                terminal_status="delivered",
                terminal_reason="orphan_guard",
                promote_successor=promote,
            )

        with self.assertRaisesRegex(scheduler_repo.SchedulerConflictError, "orphan"):
            await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, complete(False))
        await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, complete(True))
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.session_slot, row.status) for row in turns],
            [("terminal", "delivered"), ("active", "pending")],
        )

    def test_deterministic_classifier_rejects_generic_and_infrastructure_errors(self) -> None:
        for error in (
            ValueError("driver value error"),
            # pymysql/aiomysql protocol desync, not wrapped by SQLAlchemy.
            AssertionError("Protocol error, expecting EOF"),
            RuntimeError("unknown"),
            OSError("network"),
            SATimeoutError("pool"),
            OperationalError("SELECT", {}, Exception(1040, "capacity")),
        ):
            with self.subTest(error=type(error).__name__):
                self.assertFalse(_deterministic_admission_failure(error))

    # ── superseded candidates never summarize or close ──────────────────
    async def _assert_not_handed_off(self, summary) -> None:
        self.assertEqual(len(summary.calls), 0)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.handler, "bot")
        self.assertEqual(await self._count(ScheduledHandoff), 0)
        self.assertEqual(await self._count(HandoffAttempt), 0)
        self.assertEqual(await self._count(OutboxDelivery), 0)
        self.assertEqual(
            await self._count(GenerationAttempt, GenerationAttempt.attempt_type == "summary"), 0
        )

    async def test_superseded_during_generation_never_summarizes_or_closes(self) -> None:
        agent, instance = await self._agent("superseded")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "superseded-1", "primeira")
        await self._force_ready()
        response = FakeLLM("[TRANSFERIR]", "sem handoff")
        response.barrier = asyncio.Event()
        summary = FakeLLM("must not run")
        finalizer = self._finalizer(summary)
        seen = []

        async def consumer(candidate, handle):
            seen.append(candidate)
            if candidate.needs_handoff:
                await finalizer(candidate, handle)

        processor = self._processor(response, consumer)
        task = asyncio.create_task(self._run_one(processor))
        await asyncio.wait_for(response.entered.wait(), timeout=10)
        await self._accept(instance, phone, "superseded-2", "segunda")
        response.barrier.set()
        await asyncio.wait_for(task, timeout=30)
        self.assertEqual(len(response.calls), 2)
        self.assertEqual([row.needs_handoff for row in seen], [False])
        await self._assert_not_handed_off(summary)

    async def test_superseded_during_typing_never_summarizes_or_closes(self) -> None:
        agent, instance = await self._agent("superseded-typing")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "st-1", "primeira")
        await self._force_ready()
        response = FakeLLM("[TRANSFERIR]", "sem handoff")
        summary = FakeLLM("must not run")
        finalizer = self._finalizer(summary)
        seen = []

        async def consumer(candidate, handle):
            seen.append(candidate)
            if candidate.needs_handoff:
                await finalizer(candidate, handle)

        processor = self._processor(response, consumer)
        task = asyncio.create_task(self._run_one(processor))
        await self._until(lambda: self._revision_status("typing", 1), "typing barrier")
        await self._accept(instance, phone, "st-2", "segunda")
        await asyncio.wait_for(task, timeout=30)
        self.assertEqual([row.needs_handoff for row in seen], [False])
        superseded = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(superseded[0].status, "superseded")
        self.assertTrue(superseded[0].needs_handoff)
        await self._assert_not_handed_off(summary)

    async def test_stale_superseded_candidate_is_rejected_under_locks(self) -> None:
        agent, instance = await self._agent("stale-candidate")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "sc-1", "primeira")
        await self._force_ready()
        captured = []

        async def consumer(candidate, handle):
            captured.append(candidate)

        processor = self._processor(FakeLLM("[TRANSFERIR]"), consumer)
        await asyncio.wait_for(self._run_one(processor), timeout=30)
        candidate = captured[0]
        async with self.sessions() as db:
            await db.execute(
                update(TurnRevision)
                .where(TurnRevision.id == candidate.revision_id)
                .values(status="cancelled")
            )
            await db.commit()
        summary = FakeLLM("must not run")
        result = await self._finalizer(summary)(candidate, _Handle(candidate.lease))
        self.assertEqual(result, "lost")
        await self._assert_not_handed_off(summary)

    # ── re-queued admitted turn never generates again ───────────────────
    async def test_requeued_admitted_handoff_turn_never_generates_again(self) -> None:
        await self._late_during_summary("requeue", "late")
        forbidden_response, forbidden_summary = ForbiddenLLM(), ForbiddenLLM()
        # Closed by this handoff (ticket06 continuation) or resumed to bot: the
        # ticket05 consumer parks the admitted turn. Session closed by another
        # lifecycle (last): ticket07 cancels the superseded handoff formally,
        # never a release or retry loop.
        for handler, motivo, expected_status in (
            ("encerrada", "handoff", "awaiting_delivery"),
            ("bot", "handoff", "awaiting_delivery"),
            ("encerrada", "transferencia_manual", "cancelled"),
        ):
            async with self.sessions() as db:
                await db.execute(
                    update(Sessao).values(handler=handler, motivo_encerramento=motivo)
                )
                await db.execute(
                    update(ConversationTurn)
                    .where(ConversationTurn.session_slot == "active")
                    .values(
                        status="retry",
                        ready_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                        retry_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                    )
                )
                await db.commit()
            processor = self._processor(
                forbidden_response, self._finalizer(forbidden_summary)
            )
            await asyncio.wait_for(self._run_one(processor), timeout=30)
            turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
            lane = (await self._rows(AgentServiceLane, AgentServiceLane.agente_id))[0]
            self.assertEqual((turn.status, turn.claim_token), (expected_status, None))
            self.assertIsNone(lane.owner_token)
        self.assertEqual((forbidden_response.calls, forbidden_summary.calls), (0, 0))
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual(revision.status, "admitted")
        self.assertEqual(await self._count(GenerationAttempt), 2)
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(await self._count(HandoffAttempt), 1)
        self.assertEqual(await self._count(HandoffContextAssignment), 1)
        self.assertEqual(await self._count(OutboxDelivery), 4)

    # ── scheduler invariants after terminal admission ───────────────────
    async def test_post_handoff_scheduler_invariants(self) -> None:
        agent, instance, phone, _, _ = await self._late_during_summary("invariants", "late")
        async with self.sessions() as db:
            ready = await turn_generation_repo.list_ready_agent_ids(
                db, scheduler_epoch=self.scheduler_epoch, limit=10
            )
            expired = await turn_generation_repo.list_expired_lane_agent_ids(db, limit=10)
        self.assertEqual((ready, expired), ([], []))
        # A claim finds no eligible turn and an expired-lane recovery has
        # nothing to recover: the waiting handoff turn never holds the lane.
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="ticket05",
                lease_seconds=30,
            ),
        )
        self.assertIsNone(claim)
        # New input after terminal admission keeps the closed-session behavior.
        result = await self._intake(instance, phone, "invariants-after", "depois")
        self.assertEqual(result.body.get("reason"), "closed_session")
        self.assertEqual(await self._count(TurnInputMembership), 2)
        self.assertEqual(await self._count(HandoffContextAssignment), 1)
        # Caps still hold: the waiting handoff turn occupies one queued slot
        # of a 2-turn agent cap, so one more contact fits and a third is
        # rejected retryably without durable acceptance.
        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_runtime_state SET max_queued_turns_per_agent=2"))
            await db.commit()
        await self._accept(instance, f"5522{agent:08d}", "invariants-other", "outro lead")
        with self.assertRaises(evolution_orchestrator.ScheduledIntakeUnavailable):
            await self._intake(instance, f"5533{agent:08d}", "invariants-cap", "terceiro")
        self.assertEqual(await self._count(ConversationTurn), 3)
        await self._force_ready()
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="ticket05",
                lease_seconds=30,
            ),
        )
        self.assertIsNotNone(claim)
        self.assertNotEqual(claim.sessao_id, (await self._rows(Sessao, Sessao.id))[0].id)

    # ── delivery authority: nothing automated becomes dispatchable ──────
    async def _claim(self) -> list:
        async with self.sessions() as db:
            claimed = await delivery_worker_repo.claim_due(
                db,
                available_slots=100,
                worker_id="ticket05",
                cutover_epoch=self.delivery_epoch,
                evolution_lease_seconds=30,
                chatwoot_lease_seconds=30,
            )
            await db.commit()
        return claimed

    async def _deliver_open(self, open_claim) -> None:
        async with self.sessions() as db:
            self.assertTrue(
                await delivery_worker_repo.complete_chatwoot_handoff(
                    db,
                    delivery_id=open_claim.id,
                    lease_token=open_claim.lease_token,
                    cutover_epoch=self.delivery_epoch,
                    contact_id=9,
                    conversation_id=33,
                    http_status=200,
                    transition_ttl_seconds=600,
                )
            )
            await db.commit()

    async def _complete_note(self, claim, *, authority="worker") -> bool:
        payload = claim.payload
        async with self.sessions() as db:
            accepted = await delivery_worker_repo.complete_chatwoot_context_note(
                db,
                delivery_id=claim.id,
                lease_token=claim.lease_token,
                cutover_epoch=self.delivery_epoch,
                sessao_id=claim.sessao_id,
                attempt_id=claim.handoff_attempt_id,
                source_id=payload["source_id"],
                coverage=tuple(payload["coverage"]),
                external_id="note-ext",
                http_status=200,
                next_chunk_ttl_seconds=600,
                authority=authority,
            )
            await db.commit()
        return accepted

    async def _multi_chunk_handoff(self, suffix: str):
        small = functools.partial(handoff_context.plan_context_chunks, maximum_characters=600)
        with patch.object(turn_handoff_repo, "plan_context_chunks", small):
            await self._late_during_summary(
                suffix, *(f"late-{index}:" + "x" * 200 for index in range(4))
            )
        notes = [row for row in await self._rows(OutboxDelivery, OutboxDelivery.id) if row.operation == NOTE]
        self.assertGreater(len(notes), 1)
        return notes

    async def test_worker_claim_never_dispatches_ticket05_automated_rows(self) -> None:
        notes = await self._multi_chunk_handoff("claim")
        claimed_ops = []
        first = await self._claim()
        self.assertEqual([row.operation for row in first], [OPEN])
        await self._deliver_open(first[0])
        for _ in range(len(notes)):
            batch = await self._claim()
            claimed_ops.extend(row.operation for row in batch)
            self.assertEqual([row.operation for row in batch], [NOTE])
            self.assertTrue(await self._complete_note(batch[0]))
        self.assertEqual(await self._claim(), [])
        self.assertEqual(claimed_ops, [NOTE] * len(notes))
        rows = await self._rows(OutboxDelivery, OutboxDelivery.id)
        automated = [row for row in rows if row.delivery_class == "automated"]
        self.assertEqual(
            [(row.operation, row.status, row.automated_effect) for row in automated],
            [(SEND, "pending", "reply"), (SEND, "blocked", "handoff_transition")],
        )
        self.assertEqual(
            {row.status for row in rows if row.delivery_class != "automated"}, {"delivered"}
        )

    async def test_callback_confirmation_never_unblocks_automated_transition(self) -> None:
        await self._late_during_summary("callback", "late")
        open_row = (await self._rows(OutboxDelivery, OutboxDelivery.id))[0]
        async with self.sessions() as db:
            outcome = await delivery_worker_repo.complete_chatwoot_handoff_from_callback(
                db,
                sessao_id=open_row.sessao_id,
                attempt_id=open_row.handoff_attempt_id,
                cutover_epoch=self.delivery_epoch,
                contact_id=9,
                conversation_id=33,
                delivery_id=open_row.id,
                delivery_key=open_row.delivery_key,
                inbox_id=7,
                transition_ttl_seconds=600,
            )
            await db.commit()
        self.assertEqual(outcome, "completed")
        statuses = {row.operation: row.status for row in await self._rows(OutboxDelivery, OutboxDelivery.id)}
        self.assertEqual(statuses[NOTE], "pending")
        self.assertEqual(statuses[SEND], "blocked")

    async def test_operator_paths_never_revive_automated_transition(self) -> None:
        notes = await self._multi_chunk_handoff("operator")
        open_claim = (await self._claim())[0]
        async with self.sessions() as db:
            outcome = await delivery_worker_repo.fail_processing(
                db,
                delivery_id=open_claim.id,
                lease_token=open_claim.lease_token,
                cutover_epoch=self.delivery_epoch,
                error_code="http_422",
                http_status=422,
                retry_delay_seconds=None,
                ambiguous=False,
            )
            await db.commit()
        self.assertEqual(outcome, "dead")
        for _ in range(len(notes) + 2):
            async with self.sessions() as db:
                await delivery_worker_repo.run_preclaim_maintenance(db)
                await db.commit()
        rows = await self._rows(OutboxDelivery, OutboxDelivery.id)
        self.assertTrue(
            all(
                row.status in {"dead", "cancelled"}
                for row in rows
                if row.delivery_class != "automated"
            )
        )
        automated = [row for row in rows if row.delivery_class == "automated"]
        self.assertEqual(
            [(row.automated_effect, row.status) for row in automated],
            [("reply", "pending"), ("handoff_transition", "blocked")],
        )
        transition = next(row for row in automated if row.automated_effect == "handoff_transition")
        reply = next(row for row in automated if row.automated_effect == "reply")
        async with self.sessions() as db:
            with self.assertRaisesRegex(ValueError, "only terminal"):
                await delivery_worker_repo.operator_retry(
                    db, delivery_id=transition.id, actor="ops", reason="try", ttl_seconds=600
                )
            await db.rollback()
        # A terminal automated row is still refused: only lane-authorized
        # admission may revive it (ticket 07 owns operator resolution).
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.id == reply.id)
                .values(status="dead", terminal_at=func.utc_timestamp(6), terminal_reason="synthetic")
            )
            await db.commit()
        async with self.sessions() as db:
            with self.assertRaisesRegex(ValueError, "lane-authorized"):
                await delivery_worker_repo.operator_retry(
                    db, delivery_id=reply.id, actor="ops", reason="try", ttl_seconds=600
                )
            await db.rollback()
        async with self.sessions() as db:
            await delivery_worker_repo.operator_retry(
                db, delivery_id=open_claim.id, actor="ops", reason="retry open", ttl_seconds=600
            )
            await db.commit()
        rows = await self._rows(OutboxDelivery, OutboxDelivery.id)
        by_id = {row.id: row for row in rows}
        self.assertEqual(by_id[open_claim.id].status, "retry")
        self.assertEqual({by_id[note.id].status for note in notes}, {"blocked"})
        self.assertEqual(by_id[transition.id].status, "blocked")
        await self._deliver_open((await self._claim())[0])
        for _ in range(len(notes)):
            batch = await self._claim()
            self.assertEqual([row.operation for row in batch], [NOTE])
            self.assertTrue(await self._complete_note(batch[0]))
        self.assertEqual(await self._claim(), [])

    # ── context-note ownership, ambiguity and operator recovery ─────────
    async def test_note_ownership_requires_exact_mapping_attempt_and_coverage(self) -> None:
        await self._late_during_summary("ownership", "late")
        await self._deliver_open((await self._claim())[0])
        note = (await self._claim())[0]
        payload = note.payload

        async def ownership(**overrides):
            args = dict(
                delivery_id=note.id,
                lease_token=note.lease_token,
                cutover_epoch=self.delivery_epoch,
                sessao_id=note.sessao_id,
                attempt_id=note.handoff_attempt_id,
                source_id=payload["source_id"],
                coverage=tuple(payload["coverage"]),
            )
            args.update(overrides)
            async with self.sessions() as db:
                result = await delivery_worker_repo.get_context_note_dispatch_ownership(db, **args)
                await db.rollback()
            return result

        exact = await ownership()
        self.assertEqual(
            (exact.inbox_id, exact.contact_id, exact.conversation_id, exact.status),
            (7, 9, 33, "ready"),
        )
        for name, override in {
            "lease": {"lease_token": str(uuid4())},
            "epoch": {"cutover_epoch": str(uuid4())},
            "attempt": {"attempt_id": note.handoff_attempt_id + 1000},
            "session": {"sessao_id": note.sessao_id + 1000},
            # Always a different marker: flip the last hex digit.
            "source": {
                "source_id": payload["source_id"][:-1]
                + ("1" if payload["source_id"][-1] == "0" else "0")
            },
            "coverage": {"coverage": ("membership:999999",)},
            "part_claims_start": {
                "coverage": tuple(
                    item.split("#")[0] if "#" in item else f"{item}#part0002/0002"
                    for item in payload["coverage"]
                )
            },
        }.items():
            with self.subTest(case=name):
                self.assertIsNone(await ownership(**override))
        async with self.sessions() as db:
            await db.execute(
                update(Sessao).values(handoff_generation=Sessao.handoff_generation + 1)
            )
            await db.commit()
        self.assertIsNone(await ownership())

    async def test_remote_accept_with_local_failure_recovers_exactly_by_operator(self) -> None:
        notes = await self._multi_chunk_handoff("local-failure")
        await self._deliver_open((await self._claim())[0])
        first = (await self._claim())[0]
        # The provider accepted the note but the worker died before the local
        # completion: the lease expires with the row still processing.
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.id == first.id)
                .values(lock_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        for _ in range(3):
            async with self.sessions() as db:
                result = await delivery_worker_repo.run_preclaim_maintenance(db)
                await db.commit()
        rows = {row.id: row for row in await self._rows(OutboxDelivery, OutboxDelivery.id)}
        self.assertEqual(
            (rows[first.id].status, rows[first.id].terminal_reason, rows[first.id].ambiguity_count),
            ("dead", "context_note_ambiguous", 1),
        )
        self.assertEqual(rows[notes[1].id].status, "dead")
        self.assertEqual(rows[notes[1].id].terminal_reason, "dependency_failed")
        self.assertEqual(await self._claim(), [])
        async with self.sessions() as db:
            with self.assertRaisesRegex(ValueError, "ambiguous context notes"):
                await delivery_worker_repo.operator_retry(
                    db, delivery_id=first.id, actor="ops", reason="blind", ttl_seconds=600
                )
            await db.rollback()
        async with self.sessions() as db:
            reconciling = await delivery_worker_repo.claim_for_operator_reconciliation(
                db, delivery_id=first.id, actor="ops", reason="exact marker", lease_seconds=60
            )
            await db.commit()
        rows = {row.id: row for row in await self._rows(OutboxDelivery, OutboxDelivery.id)}
        self.assertEqual(rows[notes[1].id].status, "blocked")
        self.assertTrue(await self._complete_note(reconciling, authority="operator"))
        rows = {row.id: row for row in await self._rows(OutboxDelivery, OutboxDelivery.id)}
        self.assertEqual(rows[first.id].status, "delivered")
        self.assertEqual(rows[notes[1].id].status, "pending")
        self.assertEqual(rows[notes[1].id].ambiguity_count, 0)
        for _ in range(len(notes) - 1):
            batch = await self._claim()
            self.assertEqual([row.operation for row in batch], [NOTE])
            self.assertTrue(await self._complete_note(batch[0]))
        self.assertEqual(await self._claim(), [])

    async def test_audited_note_resolutions_restore_the_ordered_chain(self) -> None:
        for resolution, expected_first, expected_next in (
            ("delivered", "delivered", "pending"),
            ("retry-safe", "retry", "blocked"),
            ("cancelled", "cancelled", "pending"),
        ):
            with self.subTest(resolution=resolution):
                await self._wipe()
                notes = await self._multi_chunk_handoff(f"resolve-{resolution}")
                await self._deliver_open((await self._claim())[0])
                first = (await self._claim())[0]
                async with self.sessions() as db:
                    await delivery_worker_repo.fail_processing(
                        db,
                        delivery_id=first.id,
                        lease_token=first.lease_token,
                        cutover_epoch=self.delivery_epoch,
                        error_code="context_note_ambiguous",
                        http_status=None,
                        retry_delay_seconds=None,
                        ambiguous=True,
                    )
                    await db.commit()
                async with self.sessions() as db:
                    await delivery_worker_repo.run_preclaim_maintenance(db)
                    await db.commit()
                async with self.sessions() as db:
                    await delivery_worker_repo.resolve_context_note_ambiguity(
                        db,
                        delivery_id=first.id,
                        resolution=resolution,
                        actor="ops",
                        reason=f"provider evidence {resolution}",
                        ttl_seconds=600,
                    )
                    await db.commit()
                rows = {row.id: row for row in await self._rows(OutboxDelivery, OutboxDelivery.id)}
                self.assertEqual(rows[first.id].status, expected_first)
                self.assertEqual(rows[notes[1].id].status, expected_next)
                if resolution != "retry-safe":
                    # Delivered and cancelled both continue the ordered chain:
                    # every later chunk is delivered, none stranded.
                    for _ in range(len(notes) - 1):
                        batch = await self._claim()
                        self.assertEqual([row.operation for row in batch], [NOTE])
                        self.assertTrue(await self._complete_note(batch[0]))
                    rows = {row.id: row for row in await self._rows(OutboxDelivery, OutboxDelivery.id)}
                    self.assertEqual(
                        {rows[note.id].status for note in notes[1:]}, {"delivered"}
                    )
                actions = await self._count(
                    DeliveryOperatorAction,
                    DeliveryOperatorAction.action == f"resolve_context_note_{resolution}",
                )
                self.assertEqual(actions, 1)
                self.assertEqual(
                    [row.status for row in rows.values() if row.delivery_class == "automated"],
                    ["pending", "blocked"],
                )

    async def test_processing_note_lost_by_expiry_or_cancel_is_ambiguous(self) -> None:
        for case in ("expired", "cancelled"):
            with self.subTest(case=case):
                await self._wipe()
                await self._late_during_summary(f"lost-{case}", "late")
                await self._deliver_open((await self._claim())[0])
                note = (await self._claim())[0]
                values = {"lock_until": func.utc_timestamp(6) - text("INTERVAL 1 SECOND")}
                if case == "expired":
                    values["expires_at"] = func.utc_timestamp(6) - text("INTERVAL 1 SECOND")
                else:
                    values["cancel_requested_at"] = func.utc_timestamp(6)
                async with self.sessions() as db:
                    await db.execute(
                        update(OutboxDelivery).where(OutboxDelivery.id == note.id).values(**values)
                    )
                    await db.commit()
                async with self.sessions() as db:
                    result = await delivery_worker_repo.run_preclaim_maintenance(db)
                    await db.commit()
                row = next(r for r in await self._rows(OutboxDelivery, OutboxDelivery.id) if r.id == note.id)
                self.assertEqual(result.context_note_ambiguous, 1)
                self.assertEqual(
                    (row.status, row.terminal_reason, row.ambiguity_count),
                    ("dead" if case == "expired" else "cancelled", "context_note_ambiguous", 1),
                )
                async with self.sessions() as db:
                    with self.assertRaisesRegex(ValueError, "ambiguous context notes"):
                        await delivery_worker_repo.operator_retry(
                            db, delivery_id=note.id, actor="ops", reason="blind", ttl_seconds=600
                        )
                    await db.rollback()
                async with self.sessions() as db:
                    resolved = await delivery_worker_repo.resolve_context_note_ambiguity(
                        db, delivery_id=note.id, resolution="retry-safe",
                        actor="ops", reason="provider shows no note", ttl_seconds=600,
                    )
                    await db.commit()
                self.assertEqual((resolved.status, resolved.cancel_requested_at), ("retry", None))

    async def test_operator_reconcile_without_marker_stays_resolvable(self) -> None:
        await self._late_during_summary("marker-missing", "late")
        await self._deliver_open((await self._claim())[0])
        note = (await self._claim())[0]
        async with self.sessions() as db:
            outcome = await delivery_worker_repo.fail_processing(
                db, delivery_id=note.id, lease_token=note.lease_token,
                cutover_epoch=self.delivery_epoch, error_code="marker_not_found",
                http_status=None, retry_delay_seconds=None, ambiguous=True,
                authority="operator",
            )
            await db.commit()
        row = next(r for r in await self._rows(OutboxDelivery, OutboxDelivery.id) if r.id == note.id)
        self.assertEqual(
            (outcome, row.terminal_reason, row.last_error_code),
            ("dead", "context_note_ambiguous", "marker_not_found"),
        )
        async with self.sessions() as db:
            await delivery_worker_repo.resolve_context_note_ambiguity(
                db, delivery_id=note.id, resolution="delivered",
                actor="ops", reason="found manually", ttl_seconds=600,
            )
            await db.commit()

    async def _ambiguous_first_note(self, suffix: str):
        notes = await self._multi_chunk_handoff(suffix)
        await self._deliver_open((await self._claim())[0])
        first = (await self._claim())[0]
        async with self.sessions() as db:
            await delivery_worker_repo.fail_processing(
                db, delivery_id=first.id, lease_token=first.lease_token,
                cutover_epoch=self.delivery_epoch, error_code="context_note_ambiguous",
                http_status=None, retry_delay_seconds=None, ambiguous=True,
            )
            await db.commit()
        async with self.sessions() as db:
            await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        return notes, first

    async def _age_and_clean(self) -> dict:
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.terminal_at.is_not(None))
                .values(terminal_at=func.utc_timestamp(6) - text("INTERVAL 30 DAY"))
            )
            await db.commit()
        async with self.sessions() as db:
            await delivery_worker_repo.cleanup_retention(db, payload_days=7, receipt_days=90)
            await db.commit()
        return {row.id: row for row in await self._rows(OutboxDelivery, OutboxDelivery.id)}

    async def test_retention_keeps_unresolved_note_payloads_until_resolution(self) -> None:
        notes, first = await self._ambiguous_first_note("scrub")
        rows = await self._age_and_clean()
        self.assertEqual(rows[first.id].terminal_reason, "context_note_ambiguous")
        self.assertTrue(rows[first.id].payload, "unresolved ambiguous note was scrubbed")
        self.assertTrue(rows[notes[1].id].payload, "cascade-dead chunk was scrubbed")
        open_row = next(row for row in rows.values() if row.operation == OPEN)
        self.assertEqual(open_row.payload, {})
        # Exact reconcile still works on the retained payload; once resolved
        # the chunk becomes scrubbable.
        async with self.sessions() as db:
            reconciling = await delivery_worker_repo.claim_for_operator_reconciliation(
                db, delivery_id=first.id, actor="ops", reason="exact marker", lease_seconds=60
            )
            await db.commit()
        self.assertTrue(await self._complete_note(reconciling, authority="operator"))
        rows = await self._age_and_clean()
        self.assertEqual(rows[first.id].payload, {})
        self.assertEqual(rows[notes[1].id].status, "pending")
        self.assertTrue(rows[notes[1].id].payload)

    async def test_retention_scrubs_unresolved_notes_of_superseded_generation(self) -> None:
        notes, first = await self._ambiguous_first_note("scrub-superseded")
        async with self.sessions() as db:
            await db.execute(update(Sessao).values(handoff_generation=Sessao.handoff_generation + 1))
            await db.commit()
        rows = await self._age_and_clean()
        observed = [(rows[note.id].status, rows[note.id].payload == {}) for note in notes]
        self.assertEqual(observed[:2], [("dead", True), ("dead", True)])
        # Still-blocked chunks are not terminal: never scrubbed.
        self.assertTrue(set(observed[2:]) <= {("blocked", False)})
        self.assertEqual(rows[first.id].terminal_reason, "context_note_ambiguous")

    async def test_ambiguous_note_discovery_rotates_limit_plus_one(self) -> None:
        values = ",".join(
            f"(UUID(), :epoch, 'context-stuck-{index}', 'chatwoot', '{NOTE}', 1, JSON_OBJECT(), "
            "'provider_setup_context', UUID(), 0, 'processing', 1, 0, "
            "UTC_TIMESTAMP(6) - INTERVAL 2 SECOND, UTC_TIMESTAMP(6) + INTERVAL 1 HOUR, "
            "UUID(), 'dead-worker', UTC_TIMESTAMP(6) - INTERVAL 1 SECOND)"
            for index in range(3)
        )
        async with self.sessions() as db:
            await db.execute(
                text(
                    "INSERT INTO outbox_deliveries (delivery_key, cutover_epoch, dedupe_key, "
                    "provider, operation, payload_version, payload, delivery_class, ordering_key, "
                    "stream_sequence, status, attempt_count, ambiguity_count, next_attempt_at, "
                    f"expires_at, lease_token, lock_owner, lock_until) VALUES {values}"
                ),
                {"epoch": self.delivery_epoch},
            )
            await db.commit()
        counts = []
        for _ in range(2):
            async with self.sessions() as db:
                result = await delivery_worker_repo.run_preclaim_maintenance(
                    db, context_note_discovery_limit=2
                )
                counts.append((result.context_note_ambiguous, result.recovered))
                await db.commit()
        statuses = [row.status for row in await self._rows(OutboxDelivery, OutboxDelivery.id)]
        self.assertEqual(counts, [(2, 0), (1, 0)])
        self.assertEqual(statuses, ["dead", "dead", "dead"])

    # ── summary sources ─────────────────────────────────────────────────
    async def test_soft_limit_blocks_summary_call_and_uses_fallback(self) -> None:
        summary = FakeLLM("must not run")
        await self._run_handoff(
            "softlimit",
            summary=summary,
            agent_kwargs={"limit": Decimal("0.000001")},
        )
        self.assertEqual(len(summary.calls), 0)
        scheduled = (await self._rows(ScheduledHandoff, ScheduledHandoff.handoff_attempt_id))[0]
        self.assertEqual(
            (scheduled.summary_kind, scheduled.summary_generation_attempt_id),
            ("soft_limit_fallback", None),
        )
        open_row = (await self._rows(OutboxDelivery, OutboxDelivery.id))[0]
        self.assertTrue(open_row.payload["summary"].startswith("Handoff solicitado."))

    async def test_known_summary_failure_uses_fallback_and_keeps_attempt(self) -> None:
        summary = FakeLLM(
            llm_client.LLMProviderError("provider_http_500", outcome_known=True)
        )
        await self._run_handoff("summary-fail", summary=summary)
        scheduled = (await self._rows(ScheduledHandoff, ScheduledHandoff.handoff_attempt_id))[0]
        attempt = (
            await self._rows(GenerationAttempt, GenerationAttempt.id)
        )[-1]
        self.assertEqual(scheduled.summary_kind, "technical_fallback")
        self.assertEqual(scheduled.summary_generation_attempt_id, attempt.id)
        self.assertEqual((attempt.attempt_type, attempt.outcome), ("summary", "failed"))
        self.assertEqual(await self._count(ConsumoLLM, ConsumoLLM.tipo_chamada == "resumo"), 0)

    async def test_lost_lease_during_summary_accounts_only_and_admits_nothing(self) -> None:
        release = asyncio.Event()

        async def steal(instance, phone, summary):
            summary.barrier = release
            await asyncio.wait_for(summary.entered.wait(), timeout=20)
            async with self.sessions() as db:
                await db.execute(update(AgentServiceLane).values(owner_token=str(uuid4())))
                await db.commit()
            release.set()

        await self._run_handoff("lost", late_hook=steal)
        self.assertEqual(await self._count(ScheduledHandoff), 0)
        self.assertEqual(await self._count(HandoffAttempt), 0)
        self.assertEqual(await self._count(OutboxDelivery), 0)
        attempt = [row for row in await self._rows(GenerationAttempt, GenerationAttempt.id) if row.attempt_type == "summary"]
        self.assertEqual(len(attempt), 1)
        self.assertEqual((attempt[0].outcome, attempt[0].accounting_state), ("succeeded", "recorded"))
        self.assertIsNotNone(attempt[0].consumo_llm_id)
        session = (await self._rows(Sessao, Sessao.id))[0]
        self.assertEqual(session.handler, "bot")
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual(revision.status, "final_candidate")

    async def test_unconfigured_inbox_releases_lane_without_summary(self) -> None:
        summary = FakeLLM("must not run")
        await self._run_handoff("noinbox", summary=summary, agent_kwargs={"inbox": None})
        self.assertEqual(len(summary.calls), 0)
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        lane = (await self._rows(AgentServiceLane, AgentServiceLane.agente_id))[0]
        self.assertEqual(turn.status, "retry")
        self.assertIsNone(lane.owner_token)
        self.assertEqual(await self._count(ScheduledHandoff), 0)
        self.assertEqual((await self._rows(Sessao, Sessao.id))[0].handler, "bot")

    # ── retention, FK purge paths, ORM parity ───────────────────────────
    async def test_retention_and_delete_paths_keep_handoff_audit(self) -> None:
        await self._late_during_summary("retention", "late")
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery).values(
                    status="cancelled",
                    terminal_at=func.utc_timestamp(6) - text("INTERVAL 400 DAY"),
                    terminal_reason="cancel_requested",
                    next_attempt_at=None,
                    expires_at=None,
                )
            )
            await db.commit()
        async with self.sessions() as db:
            await delivery_worker_repo.cleanup_retention(db, payload_days=0, receipt_days=0)
            await db.commit()
        revision = (await self._rows(TurnRevision, TurnRevision.id))[0]
        self.assertEqual(revision.draft_text, "Vou transferir você.")
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(await self._count(HandoffContextAssignment), 1)
        self.assertEqual(await self._count(TurnInputMembership), 2)
        self.assertEqual(await self._count(HandoffAttempt), 1)
        for statement in ("DELETE FROM sessoes", "DELETE FROM agentes", "DELETE FROM handoff_attempts"):
            with self.subTest(statement=statement):
                async with self.sessions() as db:
                    with self.assertRaisesRegex(DBAPIError, r"\((1217|1451), .Cannot delete or update a parent row"):
                        await db.execute(text(statement))
                    await db.rollback()
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(await self._count(HandoffContextAssignment), 1)

    async def test_orm_metadata_matches_ticket05_schema(self) -> None:
        tables = ("scheduled_handoffs", "handoff_context_assignments")

        def live(sync_connection):
            inspector = inspect(sync_connection)
            return {
                table: {
                    "columns": {
                        item["name"]: item["nullable"] for item in inspector.get_columns(table)
                    },
                    "fks": {
                        item["name"]: (
                            item["referred_table"],
                            tuple(item["constrained_columns"]),
                            tuple(item["referred_columns"]),
                            item["options"].get("ondelete", "RESTRICT").upper(),
                        )
                        for item in inspector.get_foreign_keys(table)
                    },
                    "uniques": {
                        item["name"]: tuple(item["column_names"])
                        for item in inspector.get_unique_constraints(table)
                    },
                    "checks": {item["name"] for item in inspector.get_check_constraints(table)},
                    "indexes": {
                        item["name"]: tuple(item["column_names"])
                        for item in inspector.get_indexes(table)
                        if not item.get("unique") and not item["name"].startswith("fk_")
                    },
                }
                for table in tables
            }

        async with self.engine.connect() as connection:
            observed = await connection.run_sync(live)
        for table in tables:
            metadata = ScheduledHandoff.metadata.tables[table]
            expected = {
                "columns": {column.name: column.nullable for column in metadata.columns},
                "fks": {
                    fk.name: (
                        next(iter(fk.elements)).column.table.name,
                        tuple(fk.column_keys),
                        tuple(element.column.name for element in fk.elements),
                        (fk.ondelete or "RESTRICT").upper(),
                    )
                    for fk in metadata.foreign_key_constraints
                },
                "uniques": {
                    constraint.name: tuple(column.name for column in constraint.columns)
                    for constraint in metadata.constraints
                    if constraint.__class__.__name__ == "UniqueConstraint"
                },
                "checks": {
                    constraint.name
                    for constraint in metadata.constraints
                    if constraint.__class__.__name__ == "CheckConstraint"
                },
                "indexes": {
                    index.name: tuple(column.name for column in index.columns)
                    for index in metadata.indexes
                },
            }
            with self.subTest(table=table):
                self.assertEqual(observed[table], expected)

    # ── guards (load-bearing; mutation-tested by the matrix) ────────────
    async def test_scheduled_handoff_guard_is_load_bearing(self) -> None:
        await self._run_handoff("guard-scheduled")
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'scheduled handoff authority is immutable'):
                await db.execute(update(ScheduledHandoff).values(summary_kind="technical_fallback"))
            await db.rollback()

    async def test_context_assignment_guard_is_load_bearing(self) -> None:
        await self._late_during_summary("guard-assignment", "late")
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'handoff context assignment is immutable'):
                await db.execute(update(HandoffContextAssignment).values(source_id="changed"))
            await db.rollback()

    async def test_post_handoff_membership_is_terminal(self) -> None:
        await self._late_during_summary("guard-membership", "late")
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'turn input membership transition is not allowed'):
                await db.execute(
                    update(TurnInputMembership)
                    .where(TurnInputMembership.membership_state == "post_handoff_pending")
                    .values(membership_state="active", assigned_revision_id=None)
                )
            await db.rollback()

    async def test_context_note_payload_guard_is_load_bearing(self) -> None:
        await self._late_during_summary("guard-payload", "late")
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'handoff context delivery payload is immutable'):
                await db.execute(
                    update(OutboxDelivery)
                    .where(OutboxDelivery.operation == NOTE)
                    .values(payload={"rewritten": True})
                )
            await db.rollback()
        # Terminal scrub to an empty object remains allowed.
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.operation == NOTE)
                .values(status="cancelled", terminal_at=func.utc_timestamp(6), terminal_reason="x")
            )
            await db.execute(
                update(OutboxDelivery).where(OutboxDelivery.operation == NOTE).values(payload={})
            )
            await db.commit()

    async def test_membership_guard_full_status_matrix(self) -> None:
        await self._late_during_summary("guard-status-matrix", "late")
        async with self.sessions() as db:
            session = (await db.execute(select(Sessao))).scalar_one()
            turn = (
                await db.execute(
                    select(ConversationTurn).where(ConversationTurn.status == "awaiting_delivery")
                )
            ).scalar_one()
            revision = (
                await db.execute(select(TurnRevision).where(TurnRevision.status == "admitted"))
            ).scalar_one()
            attempt = (await db.execute(select(HandoffAttempt))).scalar_one()
            identities = {}
            for state in ("active", "active_assigned"):
                message = Mensagem(sessao_id=session.id, role="user", conteudo=f"matrix {state}")
                db.add(message)
                await db.flush()
                event = InboundEvent(
                    provider="evolution",
                    source_scope="matrix",
                    provider_event_id=str(uuid4()),
                    event_type="messages.upsert",
                    payload_hmac="a" * 64,
                    hmac_key_version=1,
                    agente_id=session.agente_id,
                    sessao_id=session.id,
                    receipt_disposition="scheduled",
                    mensagem_id=message.id,
                )
                db.add(event)
                await db.flush()
                row = TurnInputMembership(
                    turn_id=turn.id,
                    sessao_id=session.id,
                    mensagem_id=message.id,
                    inbound_event_id=event.id,
                    input_sequence=900 + len(identities),
                    content_bytes=len(message.conteudo.encode()),
                    membership_state="active",
                    assigned_revision_id=None,
                )
                db.add(row)
                await db.flush()
                identities[state] = row.id
            db.add(
                HandoffContextAssignment(
                    turn_input_membership_id=identities["active_assigned"],
                    handoff_attempt_id=attempt.id,
                    agente_id=session.agente_id,
                    sessao_id=session.id,
                    coverage_ordinal=999,
                    source_id="matrix-source",
                )
            )
            await db.commit()
            sealed_id = (
                await db.execute(
                    select(TurnInputMembership.id)
                    .where(TurnInputMembership.membership_state == "sealed")
                    .limit(1)
                )
            ).scalar_one()
            post_id = (
                await db.execute(
                    select(TurnInputMembership.id)
                    .where(TurnInputMembership.membership_state == "post_handoff_pending")
                    .limit(1)
                )
            ).scalar_one()
        sources = {
            "active": identities["active"],
            "active_assigned": identities["active_assigned"],
            "sealed": sealed_id,
            "post_handoff_pending": post_id,
        }
        allowed = {
            ("active", "active"),
            ("active", "sealed"),
            ("active_assigned", "active"),
            ("active_assigned", "sealed"),
            ("active_assigned", "post_handoff_pending"),
            ("sealed", "sealed"),
            ("post_handoff_pending", "post_handoff_pending"),
        }
        executed = 0
        for old, membership_id in sources.items():
            for new in ("active", "sealed", "post_handoff_pending"):
                assigned = revision.id if new == "sealed" else None
                statement = (
                    update(TurnInputMembership)
                    .where(TurnInputMembership.id == membership_id)
                    .values(membership_state=new, assigned_revision_id=assigned)
                )
                async with self.sessions() as db:
                    if (old, new) in allowed:
                        await db.execute(statement)
                    else:
                        with self.assertRaises(DBAPIError, msg=f"{old}->{new}"):
                            await db.execute(statement)
                    await db.rollback()
                executed += 1
        self.assertEqual(executed, 12)


class _Handle:
    def __init__(self, lease) -> None:
        self.lease = lease
        self.lost = asyncio.Event()
