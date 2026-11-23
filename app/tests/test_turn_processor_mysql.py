"""Ticket 04 real-MySQL barriers: fenced generation, one refresh, typing.

The LLM is always a synthetic in-process fake; Evolution/Chatwoot and the
legacy synchronous runner are patched to fail if touched.
"""

import asyncio
import json
import os
from pathlib import Path
import random
import tempfile
import unittest
from types import SimpleNamespace
from contextlib import ExitStack
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from uuid import uuid4

from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import delivery_worker, main
from src.agents import agent_runner
from src.config import settings
from src.database.connection import get_db
from src.database.models import (
    AgentServiceLane,
    Agente,
    Atendente,
    ConsumoLLM,
    LLMUsageReconciliation,
    ConversationTurn,
    GenerationAttempt,
    Mensagem,
    OutboxDelivery,
    Sessao,
    TurnInputMembership,
    TurnRevision,
)
from src.database import db_errors
from src.database.repositories import (
    consumo_repo,
    scheduler_repo,
    turn_generation_repo,
    usage_reconciliation_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services import evolution_orchestrator, llm_client
from src.services.turn_processor import TurnProcessor
from tests import auth_fixtures


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")
MODEL = "openai/gpt-4o-mini"




async def _no_presence(**kwargs) -> int:
    """Hermetic: ticket06 presence never reaches Evolution from tests."""

    return 201

class FakeLLM:
    """Synthetic provider: scripted outcomes plus optional in-flight barriers."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.entered = asyncio.Event()
        self.script: list[object] = []
        self.barriers: dict[int, asyncio.Event] = {}
        self.cost = Decimal("0.00001000")
        self.reported: str | None = None

    def hold(self, call_number: int) -> asyncio.Event:
        event = asyncio.Event()
        self.barriers[call_number] = event
        return event

    async def __call__(
        self,
        *,
        modelo_llm: str,
        messages: list[dict[str, str]],
        tipo_chamada: str,
    ) -> llm_client.LLMResult:
        self.calls.append(
            {"modelo_llm": modelo_llm, "messages": messages, "tipo_chamada": tipo_chamada}
        )
        number = len(self.calls)
        self.entered.set()
        barrier = self.barriers.get(number)
        if barrier is not None:
            await barrier.wait()
        action = self.script[number - 1] if number <= len(self.script) else None
        if isinstance(action, BaseException):
            raise action
        content = action if isinstance(action, str) else f"resposta {number}"
        return llm_client.LLMResult(
            texto=content,
            modelo_llm=modelo_llm,
            tokens_input=10 + number,
            tokens_output=3 + number,
            tokens_reasoning=None,
            tokens_cache_read=None,
            tokens_cache_write=None,
            custo_creditos=self.cost,
            provider_generation_id=f"synthetic-gen-{uuid4()}",
            modelo_llm_reportado=self.reported,
        )


class RecordingConsumer:
    """Stands in for tickets 05/06: optionally admits and completes the turn."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        complete: bool = True,
        hold: asyncio.Event | None = None,
    ) -> None:
        self.sessions = sessions
        self.complete = complete
        self.hold = hold
        self.candidates: list[turn_generation_repo.FinalCandidate] = []
        self.received = asyncio.Event()

    async def __call__(self, candidate, handle) -> None:
        self.candidates.append(candidate)
        self.received.set()
        if self.hold is not None:
            await self.hold.wait()
        if not self.complete:
            return
        lease = candidate.lease

        async def finish(db: AsyncSession) -> None:
            admitted = await scheduler_repo.admit_revision(
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
            )
            assert admitted
            await scheduler_repo.complete_turn_and_release_lane(
                db,
                turn_id=lease.turn_id,
                sessao_id=lease.sessao_id,
                revision_id=candidate.revision_id,
                agente_id=lease.agente_id,
                scheduler_epoch=lease.scheduler_epoch,
                delivery_runtime_epoch=lease.delivery_runtime_epoch,
                owner_token=lease.owner_token,
                terminal_status="delivered",
                terminal_reason="synthetic_consumer",
                promote_successor=True,
            )

        await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, finish)


class TurnProcessorMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        self.delivery_epoch = str(uuid4())
        self.scheduler_epoch = str(uuid4())
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table_name in (
                "delivery_operator_actions",
                "delivery_worker_heartbeats",
                "outbox_deliveries",
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
            ):
                await connection.execute(text(f"DELETE FROM {table_name}"))
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            await connection.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='outbox', "
                    "cutover_epoch=:epoch, version=version+1, changed_by='ticket04', "
                    "change_reason='disposable processor test' WHERE id=1"
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
                    "max_delivery_attempts_per_turn=8, "
                    "max_refreshes_per_turn=1, accounting_drain_seconds=30, "
                    "changed_by='ticket04', change_reason='disposable processor test' "
                    "WHERE id=1"
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
            ("inbound_receipt_hmac_keys", {1: SecretStr("synthetic-ticket04-receipt")}),
        ):
            self.patches.enter_context(patch.object(settings, name, value))
        self.legacy_runner = self.patches.enter_context(
            patch(
                "src.services.evolution_orchestrator.processar",
                AsyncMock(side_effect=AssertionError("legacy synchronous LLM path")),
            )
        )
        self.evolution_send = self.patches.enter_context(
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                AsyncMock(side_effect=AssertionError("ticket04 must not send text")),
            )
        )
        self.real_llm = self.patches.enter_context(
            patch(
                "src.services.llm_client._client",
                side_effect=AssertionError("real provider client must not be built"),
            )
        )
        self.processors: list[TurnProcessor] = []

    async def asyncTearDown(self) -> None:
        for processor in self.processors:
            await processor.shutdown(0.5)
        self.patches.close()
        await self.engine.dispose()

    # ── fixtures ────────────────────────────────────────────────────────
    async def _agent(self, suffix: str, **kwargs: object) -> tuple[int, str]:
        instance = f"ticket04-{suffix}-{uuid4()}"
        async with self.sessions() as db:
            row = Agente(
                nome=f"Ticket04 {suffix}",
                instance_name=instance,
                system_prompt=f"system-{suffix}",
                numero_notificacao="0",
                modelo_llm=MODEL,
                max_historico=kwargs.pop("max_historico", 20),
                inbox_scheduler_enabled=True,
                **kwargs,
            )
            db.add(row)
            await db.commit()
            return row.id, instance

    async def _intake(self, instance: str, phone: str, message_id: str, content: str) -> None:
        event = evolution_orchestrator.EvolutionMessageEvent(
            instance_name=instance,
            provider_message_id=message_id,
            telefone=phone,
            texto=content,
        )
        async with UnitOfWork(self.sessions) as uow:
            result = await evolution_orchestrator.orchestrate(
                event=event, uow=uow, authenticated=True
            )
        self.assertEqual(result.http_status, 202, result.body)

    async def _force_ready(self, *, agente_id: int | None = None) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE conversation_turns "
                    "SET ready_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND "
                    "WHERE session_slot='active' "
                    "AND (:agent IS NULL OR agente_id=:agent)"
                ),
                {"agent": agente_id},
            )
            await db.commit()

    def _processor(
        self,
        llm: FakeLLM,
        consumer: RecordingConsumer,
        *,
        worker: str = "ticket04-worker",
        lease_seconds: int = 30,
        jitter_ms: int = 0,
        rng: random.Random | None = None,
        generation_timeout: float = 10,
    ) -> TurnProcessor:
        processor = TurnProcessor(
            self.sessions,
            worker_id=worker,
            scheduler_epoch=self.scheduler_epoch,
            delivery_runtime_epoch=self.delivery_epoch,
            consumer=consumer,
            llm_call=llm,
            presence_call=_no_presence,
            lease_seconds=lease_seconds,
            poll_interval_seconds=0.05,
            generation_timeout_seconds=generation_timeout,
            typing_jitter_ms=jitter_ms,
            release_backoff_seconds=1,
            concurrency=4,
            rng=rng,
        )
        self.processors.append(processor)
        return processor

    async def _until(self, predicate, *, timeout: float = 15.0, message: str = "") -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            if await predicate():
                return
            if loop.time() > deadline:
                self.fail(f"condition not reached: {message}")
            await asyncio.sleep(0.05)

    async def _scalar(self, statement):
        async with self.sessions() as db:
            return (await db.execute(statement)).scalar()

    async def _revision_status(self, turn_number: int = 1) -> list[str]:
        async with self.sessions() as db:
            rows = (
                await db.execute(select(TurnRevision.status).order_by(TurnRevision.id))
            ).scalars()
            return list(rows)

    async def _run_background(self, processor: TurnProcessor) -> tuple[asyncio.Event, asyncio.Task]:
        stop = asyncio.Event()
        task = asyncio.create_task(processor.run(stop))
        return stop, task

    async def _stop(self, processor: TurnProcessor, stop: asyncio.Event, task: asyncio.Task) -> None:
        stop.set()
        await task
        await processor.shutdown(5)

    async def _assistant_texts(self) -> list[str]:
        async with self.sessions() as db:
            return list(
                (
                    await db.execute(
                        select(Mensagem.conteudo)
                        .where(Mensagem.role == "assistant")
                        .order_by(Mensagem.id)
                    )
                ).scalars()
            )

    async def _total_mensagens(self) -> list[int]:
        async with self.sessions() as db:
            return list(
                (await db.execute(select(Sessao.total_mensagens).order_by(Sessao.id))).scalars()
            )

    # ── tests ───────────────────────────────────────────────────────────
    async def test_burst_one_generation_persisted_typing_and_final_seal(self) -> None:
        agent, instance = await self._agent("burst", max_historico=1)
        phone = "5511990000001"
        # Prior accepted history: a legacy pair without membership.
        async with self.sessions() as db:
            session = Sessao(agente_id=agent, telefone=phone)
            db.add(session)
            await db.flush()
            db.add_all(
                [
                    Mensagem(sessao_id=session.id, role="user", conteudo="antigo"),
                    Mensagem(sessao_id=session.id, role="assistant", conteudo="anterior"),
                ]
            )
            await db.commit()
        for index, content in enumerate(("a1", "a2", "a2")):
            await self._intake(instance, phone, f"burst-{index}", content)
        await self._force_ready()
        llm = FakeLLM()
        llm.script = ["Olá, tudo certo"]
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        await processor.run_pass()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)

        self.assertEqual(len(llm.calls), 1)
        messages = llm.calls[0]["messages"]
        # max_historico=1 bounds only prior history; the full batch follows once.
        self.assertEqual(
            messages,
            [
                {"role": "system", "content": "system-burst"},
                {"role": "assistant", "content": "anterior"},
                {"role": "user", "content": "a1"},
                {"role": "user", "content": "a2"},
                {"role": "user", "content": "a2"},
            ],
        )
        candidate = consumer.candidates[0]
        self.assertEqual(
            (candidate.revision_number, candidate.sealed_first_input_sequence,
             candidate.sealed_last_input_sequence, candidate.draft_kind,
             candidate.needs_handoff, candidate.revision_status),
            (1, 1, 3, "generated", False, "final_candidate"),
        )
        async with self.sessions() as db:
            revision = (await db.execute(select(TurnRevision))).scalar_one()
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
            consumo = (await db.execute(select(ConsumoLLM))).scalar_one()
        expected_delay = round((2 + len("Olá, tudo certo") / 25) * 1000)
        self.assertEqual(revision.typing_delay_ms, expected_delay)
        self.assertEqual(revision.typing_jitter_ms, 0)
        self.assertEqual(
            revision.typing_deadline_at,
            revision.generation_completed_at + timedelta(milliseconds=expected_delay),
        )
        self.assertEqual(
            (attempt.outcome, attempt.accounting_state, attempt.consumo_llm_id),
            ("succeeded", "recorded", consumo.id),
        )
        self.assertEqual(consumo.tipo_chamada, "resposta")
        self.assertEqual(await self._assistant_texts(), ["anterior", "Olá, tudo certo"])
        self.assertEqual(await self._total_mensagens(), [1])
        self.assertEqual(await self._scalar(select(func.count(OutboxDelivery.id))), 0)
        self.legacy_runner.assert_not_awaited()
        self.evolution_send.assert_not_awaited()

    async def test_input_during_generation_refreshes_once_and_accounts_both(self) -> None:
        agent, instance = await self._agent("gen-refresh")
        phone = "5511990000002"
        await self._intake(instance, phone, "gr-1", "primeira")
        await self._force_ready()
        llm = FakeLLM()
        llm.script = ["rascunho obsoleto", "resposta final"]
        barrier = llm.hold(1)
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        stop, task = await self._run_background(processor)
        await asyncio.wait_for(llm.entered.wait(), timeout=10)
        # Barrier: new input while generation 1 is in flight.
        await self._intake(instance, phone, "gr-2", "segunda")
        barrier.set()
        await self._until(
            lambda: self._has_revision_two(), message="replacement revision"
        )
        await self._force_ready()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await self._stop(processor, stop, task)

        self.assertEqual(len(llm.calls), 2)
        self.assertEqual(
            [m["content"] for m in llm.calls[1]["messages"][1:]], ["primeira", "segunda"]
        )
        async with self.sessions() as db:
            revisions = list((await db.execute(select(TurnRevision).order_by(TurnRevision.id))).scalars())
            attempts = list((await db.execute(select(GenerationAttempt).order_by(GenerationAttempt.id))).scalars())
            consumos = (await db.execute(select(func.count(ConsumoLLM.id)))).scalar_one()
            memberships = list(
                (await db.execute(select(TurnInputMembership).order_by(TurnInputMembership.input_sequence))).scalars()
            )
        self.assertEqual(
            [(r.revision_number, r.status, r.superseded_reason) for r in revisions],
            [(1, "superseded", "refresh_input_during_generation"), (2, "final_candidate", None)],
        )
        self.assertEqual(revisions[0].draft_text, "rascunho obsoleto")
        self.assertIsNone(revisions[0].typing_deadline_at)
        self.assertEqual(
            (revisions[1].sealed_first_input_sequence, revisions[1].sealed_last_input_sequence),
            (1, 2),
        )
        self.assertEqual([m.assigned_revision_id for m in memberships], [revisions[0].id, revisions[1].id])
        self.assertEqual([(a.outcome, a.accounting_state) for a in attempts], [("succeeded", "recorded")] * 2)
        self.assertEqual(consumos, 2)
        self.assertEqual(await self._assistant_texts(), ["resposta final"])
        self.assertEqual(await self._total_mensagens(), [1])

    async def _has_revision_two(self) -> bool:
        async with self.sessions() as db:
            return bool(
                (
                    await db.execute(
                        select(func.count(TurnRevision.id)).where(TurnRevision.revision_number == 2)
                    )
                ).scalar_one()
            )

    async def _revision_in(self, number: int, status: str) -> bool:
        async with self.sessions() as db:
            return bool(
                (
                    await db.execute(
                        select(func.count(TurnRevision.id)).where(
                            TurnRevision.revision_number == number,
                            TurnRevision.status == status,
                        )
                    )
                ).scalar_one()
            )

    async def test_input_during_typing_refreshes_and_later_input_is_successor(self) -> None:
        agent, instance = await self._agent("typing-refresh")
        phone = "5511990000003"
        await self._intake(instance, phone, "tr-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        llm.script = ["primeiro rascunho", "x" * 25]
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        stop, task = await self._run_background(processor)
        await self._until(lambda: self._revision_in(1, "typing"), message="rev1 typing")
        await self._intake(instance, phone, "tr-2", "dois")
        await self._until(lambda: self._has_revision_two(), message="rev2")
        await self._force_ready()
        await self._until(lambda: self._revision_in(2, "typing"), message="rev2 typing")
        # Refresh budget spent: this input belongs to the sole successor and
        # must not touch the sealed replacement range.
        await self._intake(instance, phone, "tr-3", "tres")
        await asyncio.wait_for(consumer.received.wait(), timeout=25)
        await self._stop(processor, stop, task)

        self.assertEqual(len(llm.calls), 2)
        async with self.sessions() as db:
            turns = list((await db.execute(select(ConversationTurn).order_by(ConversationTurn.id))).scalars())
            revisions = list((await db.execute(select(TurnRevision).order_by(TurnRevision.id))).scalars())
            memberships = list(
                (await db.execute(select(TurnInputMembership).order_by(TurnInputMembership.input_sequence))).scalars()
            )
        self.assertEqual([t.session_slot for t in turns], ["active", "successor"])
        self.assertEqual(
            [(r.turn_id, r.revision_number, r.status) for r in revisions],
            [
                (turns[0].id, 1, "superseded"),
                (turns[0].id, 2, "final_candidate"),
                (turns[1].id, 1, "collecting"),
            ],
        )
        self.assertEqual(revisions[0].superseded_reason, "refresh_input_during_typing")
        self.assertIsNotNone(revisions[0].typing_deadline_at)
        self.assertEqual(
            (revisions[1].sealed_first_input_sequence, revisions[1].sealed_last_input_sequence),
            (1, 2),
        )
        self.assertEqual(
            [(m.turn_id, m.membership_state) for m in memberships],
            [(turns[0].id, "sealed"), (turns[0].id, "sealed"), (turns[1].id, "active")],
        )
        self.assertEqual(await self._assistant_texts(), ["x" * 25])
        self.assertEqual(revisions[1].typing_delay_ms, 3000)

    async def test_input_after_final_seal_goes_to_successor(self) -> None:
        agent, instance = await self._agent("after-seal")
        phone = "5511990000004"
        await self._intake(instance, phone, "as-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        hold = asyncio.Event()
        consumer = RecordingConsumer(self.sessions, complete=False, hold=hold)
        processor = self._processor(llm, consumer)
        await processor.run_pass()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await self._intake(instance, phone, "as-2", "dois")
        hold.set()
        await processor.shutdown(5)
        async with self.sessions() as db:
            revision = (
                await db.execute(select(TurnRevision).order_by(TurnRevision.id).limit(1))
            ).scalar_one()
            slots = list(
                (await db.execute(select(ConversationTurn.session_slot).order_by(ConversationTurn.id))).scalars()
            )
        self.assertEqual(revision.status, "final_candidate")
        self.assertEqual((revision.sealed_first_input_sequence, revision.sealed_last_input_sequence), (1, 1))
        self.assertEqual(slots, ["active", "successor"])
        self.assertEqual(len(llm.calls), 1)

    async def test_one_generation_per_number_and_distinct_numbers_concurrent(self) -> None:
        agent_x, instance_x = await self._agent("lane-x")
        agent_y, instance_y = await self._agent("lane-y")
        await self._intake(instance_x, "5511990000010", "x-a", "contato A")
        await self._intake(instance_x, "5511990000011", "x-b", "contato B")
        await self._intake(instance_y, "5511990000012", "y-c", "contato C")
        await self._force_ready()
        llm = FakeLLM()
        barriers = [llm.hold(1), llm.hold(2), llm.hold(3)]
        consumer = RecordingConsumer(self.sessions, complete=True)
        # Two independent processors (replicas): only the DB lane fence, not
        # local single-flight, can keep agent X to one in-flight generation.
        processor = self._processor(llm, consumer)
        replica = self._processor(llm, consumer, worker="ticket04-replica")
        stop, task = await self._run_background(processor)
        replica_stop, replica_task = await self._run_background(replica)
        await self._until(lambda: self._calls_at_least(llm, 2), message="two concurrent calls")
        await asyncio.sleep(0.75)
        # X has exactly one in-flight generation; Y progresses concurrently.
        self.assertEqual(len(llm.calls), 2)
        self.assertEqual(
            sorted(c["messages"][-1]["content"] for c in llm.calls),
            ["contato A", "contato C"],
        )
        async with self.sessions() as db:
            lane_x = await db.get(AgentServiceLane, agent_x)
            lane_y = await db.get(AgentServiceLane, agent_y)
        self.assertIsNotNone(lane_x.owner_token)
        self.assertIsNotNone(lane_y.owner_token)
        for barrier in barriers:
            barrier.set()
        await self._until(lambda: self._calls_at_least(llm, 3), message="B after A")
        await self._until(
            lambda: self._consumer_count(consumer, 3), timeout=30, message="three candidates"
        )
        await self._stop(processor, stop, task)
        await self._stop(replica, replica_stop, replica_task)
        self.assertEqual(len(llm.calls), 3)
        self.assertEqual(llm.calls[2]["messages"][-1]["content"], "contato B")

    async def _calls_at_least(self, llm: FakeLLM, count: int) -> bool:
        return len(llm.calls) >= count

    async def _consumer_count(self, consumer: RecordingConsumer, count: int) -> bool:
        return len(consumer.candidates) >= count

    async def test_soft_limit_rechecked_before_refresh_generation(self) -> None:
        agent, instance = await self._agent(
            "soft-limit", limite_mensal_creditos=Decimal("0.000015")
        )
        phone = "5511990000005"
        await self._intake(instance, phone, "sl-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        llm.cost = Decimal("0.00002000")
        barrier = llm.hold(1)
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        stop, task = await self._run_background(processor)
        await asyncio.wait_for(llm.entered.wait(), timeout=10)
        await self._intake(instance, phone, "sl-2", "dois")
        barrier.set()
        await self._until(lambda: self._has_revision_two(), message="rev2")
        await self._force_ready()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await self._stop(processor, stop, task)

        self.assertEqual(len(llm.calls), 1)
        candidate = consumer.candidates[0]
        self.assertEqual((candidate.revision_number, candidate.draft_kind), (2, "soft_limit_fallback"))
        async with self.sessions() as db:
            revision_two = (
                await db.execute(select(TurnRevision).where(TurnRevision.revision_number == 2))
            ).scalar_one()
        self.assertEqual(revision_two.draft_text, evolution_orchestrator.MENSAGEM_LIMITE_ATINGIDO)
        self.assertEqual(await self._scalar(select(func.count(ConsumoLLM.id))), 1)
        self.assertEqual(await self._assistant_texts(), [])
        self.assertEqual(await self._total_mensagens(), [0])

    async def test_lease_loss_in_flight_finalizes_only_exact_accounting(self) -> None:
        # Renewal interval (10s) is longer than the barrier window, so the
        # owner-path fence in the result transaction is what rejects the draft.
        await self._lease_loss_case(lease_seconds=30, wait_for_renewer=False)

    async def test_renewer_detected_lease_loss_drains_then_accounts_only(self) -> None:
        # Renewal (every 2s) observes the loss first; the in-flight call may
        # finish inside the accounting-drain budget and is finalized only for
        # its exact attempt.
        await self._lease_loss_case(lease_seconds=6, wait_for_renewer=True)

    async def _lease_loss_case(self, *, lease_seconds: int, wait_for_renewer: bool) -> None:
        agent, instance = await self._agent(f"lease-loss-{lease_seconds}")
        await self._intake(instance, "5511990000006", "ll-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        barrier = llm.hold(1)
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer, lease_seconds=lease_seconds)
        owner_finalize = self.patches.enter_context(
            patch.object(
                turn_generation_repo,
                "record_generation_result",
                wraps=turn_generation_repo.record_generation_result,
            )
        )
        claim_task = asyncio.create_task(processor.process_agent(agent))
        await asyncio.wait_for(llm.entered.wait(), timeout=10)
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
            stale_token = attempt.lane_owner_token
            await db.execute(
                text(
                    "UPDATE agent_service_lanes SET lease_until=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND "
                    "WHERE agente_id=:agent"
                ),
                {"agent": agent},
            )
            await db.execute(
                text(
                    "UPDATE conversation_turns SET claim_until=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND "
                    "WHERE agente_id=:agent"
                ),
                {"agent": agent},
            )
            await db.commit()
        recovery = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.recover_expired_lane(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                retry_at=datetime.utcnow() + timedelta(seconds=60),
            ),
        )
        self.assertEqual(recovery, "quarantined")
        # A stale task cannot finalize anything but its own exact attempt.
        wrong = turn_generation_repo.GenerationOutcome("failed", safe_error_code="probe")
        self.assertFalse(
            await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions,
                lambda db: turn_generation_repo.record_generation_accounting_only(
                    db,
                    agente_id=agent,
                    sessao_id=attempt.sessao_id,
                    attempt_key=attempt.attempt_key,
                    lane_owner_token=str(uuid4()),
                    outcome=wrong,
                ),
            )
        )
        if wait_for_renewer:
            await asyncio.sleep(2.5)
            self.assertFalse(claim_task.done())
        barrier.set()
        result = await asyncio.wait_for(claim_task, timeout=20)
        self.assertEqual(result, "lost")
        # Renewer-detected loss never even attempts domain finalization.
        self.assertEqual(owner_finalize.called, not wait_for_renewer)
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
            revision = (await db.execute(select(TurnRevision))).scalar_one()
            consumo = (await db.execute(select(ConsumoLLM))).scalar_one()
        self.assertEqual(attempt.lane_owner_token, stale_token)
        self.assertEqual(
            (attempt.outcome, attempt.accounting_state, attempt.consumo_llm_id),
            ("succeeded", "reconciliation_required", consumo.id),
        )
        self.assertEqual(revision.status, "generating")
        self.assertIsNone(revision.draft_text)
        self.assertEqual(await self._assistant_texts(), [])
        self.assertEqual(await self._total_mensagens(), [0])
        self.assertEqual(consumer.candidates, [])
        # Quarantined agent admits no new generation.
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="other",
                lease_seconds=30,
            ),
        )
        self.assertIsNone(claim)

    async def test_unknown_outcome_quarantines_agent_while_others_progress(self) -> None:
        agent_x, instance_x = await self._agent("unknown-x")
        agent_y, instance_y = await self._agent("unknown-y")
        await self._intake(instance_x, "5511990000020", "ux-1", "x")
        await self._force_ready()

        class RoutedLLM(FakeLLM):
            async def __call__(self, *, modelo_llm, messages, tipo_chamada):
                if messages[0]["content"] == "system-unknown-x":
                    self.calls.append({"messages": messages})
                    raise llm_client.LLMProviderError(
                        "provider_outcome_unknown", outcome_known=False
                    )
                return await super().__call__(
                    modelo_llm=modelo_llm, messages=messages, tipo_chamada=tipo_chamada
                )

        llm = RoutedLLM()
        consumer = RecordingConsumer(self.sessions, complete=True)
        processor = self._processor(llm, consumer)
        result = await processor.process_agent(agent_x)
        self.assertEqual(result, "generation_outcome_unknown")
        await self._intake(instance_x, "5511990000021", "ux-2", "x2")
        await self._intake(instance_y, "5511990000022", "uy-1", "y")
        await self._force_ready()
        await asyncio.sleep(1.1)
        stop, task = await self._run_background(processor)
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await asyncio.sleep(0.5)
        await self._stop(processor, stop, task)
        self.assertEqual([c.lease.agente_id for c in consumer.candidates], [agent_y])
        self.assertEqual(len(llm.calls), 2)
        async with self.sessions() as db:
            attempt_x = (
                await db.execute(select(GenerationAttempt).where(GenerationAttempt.agente_id == agent_x))
            ).scalar_one()
            turn_x = (
                await db.execute(
                    select(ConversationTurn)
                    .where(ConversationTurn.agente_id == agent_x)
                    .order_by(ConversationTurn.id)
                    .limit(1)
                )
            ).scalar_one()
            lane_x = await db.get(AgentServiceLane, agent_x)
        self.assertEqual(
            (attempt_x.outcome, attempt_x.accounting_state, attempt_x.safe_error_code),
            ("outcome_unknown", "reconciliation_required", "provider_outcome_unknown"),
        )
        self.assertEqual(attempt_x.custo_creditos, None)
        self.assertEqual(turn_x.status, "retry")
        self.assertIsNone(lane_x.owner_token)

    async def test_known_failure_uses_technical_fallback_without_retry(self) -> None:
        agent, instance = await self._agent("known-failure")
        await self._intake(instance, "5511990000007", "kf-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        llm.script = [llm_client.LLMProviderError("provider_http_400", outcome_known=True)]
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        await processor.run_pass()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await processor.shutdown(5)
        self.assertEqual(len(llm.calls), 1)
        self.assertEqual(consumer.candidates[0].draft_kind, "technical_fallback")
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
        self.assertEqual((attempt.outcome, attempt.accounting_state), ("failed", "pending"))
        self.assertEqual(await self._scalar(select(func.count(ConsumoLLM.id))), 0)
        self.assertEqual(await self._assistant_texts(), [])

    async def test_restart_resumes_persisted_typing_without_reroll(self) -> None:
        agent, instance = await self._agent("restart")
        await self._intake(instance, "5511990000008", "rs-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        llm.script = ["resposta persistida"]
        consumer = RecordingConsumer(self.sessions, complete=False)
        first = self._processor(llm, consumer, jitter_ms=1000, rng=random.Random(7))
        stop, task = await self._run_background(first)
        await self._until(lambda: self._revision_in(1, "typing"), message="typing")
        async with self.sessions() as db:
            before = (await db.execute(select(TurnRevision))).scalar_one()
        # Simulated crash: cancel without graceful release.
        stop.set()
        actives = list(first._active.values())
        for active in actives:
            active.cancel()
        await asyncio.gather(task, *actives, return_exceptions=True)
        self.processors.remove(first)
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE agent_service_lanes SET lease_until=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND "
                    "WHERE agente_id=:agent"
                ),
                {"agent": agent},
            )
            await db.execute(
                text(
                    "UPDATE conversation_turns SET claim_until=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND "
                    "WHERE agente_id=:agent"
                ),
                {"agent": agent},
            )
            await db.commit()
        second = self._processor(
            llm, consumer, worker="ticket04-restarted", jitter_ms=1000, rng=random.Random(99)
        )
        stop, task = await self._run_background(second)
        await asyncio.wait_for(consumer.received.wait(), timeout=25)
        await self._stop(second, stop, task)
        async with self.sessions() as db:
            after = (await db.execute(select(TurnRevision))).scalar_one()
        self.assertEqual(len(llm.calls), 1)
        self.assertEqual(
            (after.typing_delay_ms, after.typing_jitter_ms, after.typing_deadline_at,
             after.generation_completed_at),
            (before.typing_delay_ms, before.typing_jitter_ms, before.typing_deadline_at,
             before.generation_completed_at),
        )
        self.assertEqual(after.status, "final_candidate")
        self.assertEqual(consumer.candidates[0].typing_deadline_at, before.typing_deadline_at)

    async def test_graceful_shutdown_drains_generation_then_releases(self) -> None:
        agent, instance = await self._agent("shutdown")
        await self._intake(instance, "5511990000009", "sd-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        barrier = llm.hold(1)
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        await processor.run_pass()
        await asyncio.wait_for(llm.entered.wait(), timeout=10)
        shutdown = asyncio.create_task(processor.shutdown(10))
        await asyncio.sleep(0.2)
        self.assertFalse(shutdown.done())
        barrier.set()
        await asyncio.wait_for(shutdown, timeout=15)
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
            revision = (await db.execute(select(TurnRevision))).scalar_one()
            turn = (await db.execute(select(ConversationTurn))).scalar_one()
            lane = await db.get(AgentServiceLane, agent)
        self.assertEqual((attempt.outcome, attempt.accounting_state), ("succeeded", "recorded"))
        self.assertEqual(revision.status, "typing")
        self.assertEqual((turn.status, turn.delivery_attempt_count), ("retry", 0))
        self.assertIsNone(lane.owner_token)
        self.assertEqual(consumer.candidates, [])

    async def test_forced_shutdown_records_unknown_and_quarantines(self) -> None:
        agent, instance = await self._agent("forced")
        await self._intake(instance, "5511990000030", "fs-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        llm.hold(1)
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        await processor.run_pass()
        await asyncio.wait_for(llm.entered.wait(), timeout=10)
        await processor.shutdown(0.2)
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
        self.assertEqual(
            (attempt.outcome, attempt.accounting_state, attempt.safe_error_code),
            ("outcome_unknown", "reconciliation_required", "processor_cancelled"),
        )
        self.assertEqual(await self._scalar(select(func.count(ConsumoLLM.id))), 0)

    async def test_lifecycle_refusal_makes_no_provider_call(self) -> None:
        agent, instance = await self._agent("lifecycle")
        await self._intake(instance, "5511990000040", "lc-1", "um")
        await self._force_ready()
        async with self.sessions() as db:
            await db.execute(text("UPDATE sessoes SET handler='encerrada'"))
            await db.commit()
        llm = FakeLLM()
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        result = await processor.process_agent(agent)
        # Ticket07: formal cancellation, never a release/backoff loop.
        self.assertEqual(result, "lifecycle_cancelled")
        self.assertEqual(llm.calls, [])
        async with self.sessions() as db:
            turn = (await db.execute(select(ConversationTurn))).scalar_one()
            lane = await db.get(AgentServiceLane, agent)
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason, turn.claim_token),
            ("cancelled", "terminal", "session_closed", None),
        )
        self.assertIsNone(lane.owner_token)

    async def test_preloaded_identity_map_cannot_grant_stale_authority(self) -> None:
        agent, instance = await self._agent("preload")
        await self._intake(instance, "5511990000050", "pl-1", "um")
        await self._force_ready()
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="preload",
                lease_seconds=30,
            ),
        )
        lease = turn_generation_repo.TurnLease.from_claim(claim)
        policy = self._processor(FakeLLM(), RecordingConsumer(self.sessions)).policy
        async with self.sessions() as stale_db:
            async with stale_db.begin():
                # Preload current-owner identities into this session's map.
                # Strong references matter: the identity map is weak-keyed.
                preloaded_lane = await stale_db.get(AgentServiceLane, agent)
                preloaded_turn = await stale_db.get(ConversationTurn, lease.turn_id)
                self.assertEqual(preloaded_lane.owner_token, lease.owner_token)
                self.assertEqual(preloaded_turn.claim_token, lease.owner_token)
                async with self.sessions() as other:
                    await other.execute(
                        text(
                            "UPDATE agent_service_lanes SET owner_token=:token, "
                            "owner_worker_id='usurper' WHERE agente_id=:agent"
                        ),
                        {"token": str(uuid4()), "agent": agent},
                    )
                    await other.commit()
                with self.assertRaises(turn_generation_repo.StaleTurnOwnership):
                    await turn_generation_repo.advance_turn(
                        stale_db, lease=lease, policy=policy
                    )
                # The locked read refreshed the very preloaded instance.
                self.assertNotEqual(preloaded_lane.owner_token, lease.owner_token)
        self.assertEqual(await self._scalar(select(func.count(GenerationAttempt.id))), 0)

    async def test_draft_and_typing_guard_rejects_reroll_and_reopen(self) -> None:
        agent, instance = await self._agent("guard")
        await self._intake(instance, "5511990000060", "gd-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        await processor.run_pass()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await processor.shutdown(5)
        for statement, message in (
            ("UPDATE turn_revisions SET typing_delay_ms=typing_delay_ms+1", "typing schedule is immutable"),
            ("UPDATE turn_revisions SET draft_text='outro'", "generated revision draft is immutable"),
            ("UPDATE turn_revisions SET status='typing'", "sealed final candidate cannot be reopened"),
        ):
            async with self.sessions() as db:
                with self.assertRaises(DBAPIError) as raised:
                    await db.execute(text(statement))
                self.assertIn(message, str(raised.exception))
                await db.rollback()
        async with self.sessions() as db:
            await db.execute(text("UPDATE turn_revisions SET draft_text=NULL"))
            await db.commit()
        self.assertIsNone(await self._scalar(select(TurnRevision.draft_text)))

    async def test_admitted_revision_status_is_terminal(self) -> None:
        agent, instance = await self._agent("admitted-guard")
        await self._intake(instance, "5511990000061", "ag-1", "um")
        await self._force_ready()
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(FakeLLM(), consumer)
        await processor.run_pass()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await processor.shutdown(5)
        candidate = consumer.candidates[0]
        lease = candidate.lease

        async def admit(db: AsyncSession) -> bool:
            return await scheduler_repo.admit_revision(
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
            )

        self.assertTrue(
            await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, admit)
        )
        for target in ("final_candidate", "cancelled", "dead"):
            async with self.sessions() as db:
                with self.assertRaises(DBAPIError) as raised:
                    await db.execute(
                        text("UPDATE turn_revisions SET status=:status WHERE id=:id"),
                        {"status": target, "id": candidate.revision_id},
                    )
                self.assertIn("admitted revision status is immutable", str(raised.exception))
                await db.rollback()

        async def complete(db: AsyncSession) -> None:
            await scheduler_repo.complete_turn_and_release_lane(
                db,
                turn_id=lease.turn_id,
                sessao_id=lease.sessao_id,
                revision_id=candidate.revision_id,
                agente_id=lease.agente_id,
                scheduler_epoch=lease.scheduler_epoch,
                delivery_runtime_epoch=lease.delivery_runtime_epoch,
                owner_token=lease.owner_token,
                terminal_status="delivered",
                terminal_reason="admitted_guard_control",
                promote_successor=False,
            )

        await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, complete)
        async with self.sessions() as db:
            revision_status = await db.scalar(
                select(TurnRevision.status).where(TurnRevision.id == candidate.revision_id)
            )
            turn_status = await db.scalar(
                select(ConversationTurn.status).where(ConversationTurn.id == lease.turn_id)
            )
        self.assertEqual((revision_status, turn_status), ("admitted", "delivered"))


    # ── review follow-up: lock order, lost transactions, recovery liveness ──
    async def _started_attempt(self, suffix: str, phone: str) -> tuple[int, turn_generation_repo.TurnLease, str]:
        """Claimed turn with a committed started attempt; returns agent, lease, attempt key."""

        agent, instance = await self._agent(suffix)
        await self._intake(instance, phone, f"{suffix}-1", "um")
        await self._force_ready(agente_id=agent)
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id=f"crashed-{suffix}",
                lease_seconds=30,
            ),
        )
        lease = turn_generation_repo.TurnLease.from_claim(claim)
        policy = TurnProcessor(
            self.sessions,
            worker_id="policy",
            scheduler_epoch=self.scheduler_epoch,
            delivery_runtime_epoch=self.delivery_epoch,
            consumer=RecordingConsumer(self.sessions),
            llm_call=FakeLLM(),
            typing_jitter_ms=0,
        ).policy
        step = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: turn_generation_repo.advance_turn(db, lease=lease, policy=policy),
        )
        self.assertIsInstance(step, turn_generation_repo.GenerateStep)
        return agent, lease, step.attempt_key

    async def _expire_lane(self, agent: int, seconds_ago: int) -> None:
        async with self.sessions() as db:
            for table, column in (
                ("agent_service_lanes", "lease_until"),
                ("conversation_turns", "claim_until"),
            ):
                await db.execute(
                    text(
                        f"UPDATE {table} SET {column}=UTC_TIMESTAMP(6) - INTERVAL :ago SECOND "
                        f"WHERE agente_id=:agent AND {column} IS NOT NULL"
                    ),
                    {"agent": agent, "ago": seconds_ago},
                )
            await db.commit()

    async def test_accounting_only_overlapping_recovery_keeps_lock_order(self) -> None:
        agent, lease, attempt_key = await self._started_attempt("overlap", "5511990000070")
        await self._expire_lane(agent, 5)
        locked = asyncio.Event()
        release = asyncio.Event()
        original_lock_attempt = turn_generation_repo._lock_attempt

        async def lock_then_wait(db, key):
            row = await original_lock_attempt(db, key)
            locked.set()
            await release.wait()
            return row

        retries: list[str] = []

        def counting(label: str):
            async def sleep(_seconds: float) -> None:
                retries.append(label)

            return sleep

        usage = turn_generation_repo.GenerationUsage(
            modelo_llm=MODEL,
            tokens_input=12,
            tokens_output=4,
            tokens_reasoning=None,
            tokens_cache_read=None,
            tokens_cache_write=None,
            custo_creditos=Decimal("0.00001000"),
            provider_generation_id="overlap-generation",
        )
        outcome = turn_generation_repo.GenerationOutcome("succeeded", usage=usage)
        with patch.object(turn_generation_repo, "_lock_attempt", lock_then_wait):
            accounting = asyncio.create_task(
                scheduler_repo.run_transaction_with_deadlock_retry(
                    self.sessions,
                    lambda db: turn_generation_repo.record_generation_accounting_only(
                        db,
                        agente_id=agent,
                        sessao_id=lease.sessao_id,
                        attempt_key=attempt_key,
                        lane_owner_token=lease.owner_token,
                        outcome=outcome,
                    ),
                    sleep=counting("accounting"),
                )
            )
            await asyncio.wait_for(locked.wait(), timeout=10)
            recovery = asyncio.create_task(
                scheduler_repo.run_transaction_with_deadlock_retry(
                    self.sessions,
                    lambda db: scheduler_repo.recover_expired_lane(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.delivery_epoch,
                        retry_at=datetime.utcnow() + timedelta(seconds=60),
                    ),
                    sleep=counting("recovery"),
                )
            )
            # True overlap: recovery is in flight while the stale task holds
            # its attempt lock and has not yet inserted consumption.
            await asyncio.sleep(1.0)
            self.assertFalse(recovery.done())
            release.set()
            accounted, recovered = await asyncio.wait_for(
                asyncio.gather(accounting, recovery), timeout=30
            )
        self.assertEqual(retries, [])
        self.assertTrue(accounted)
        self.assertEqual(recovered, "retry")
        async with self.sessions() as db:
            attempt = (
                await db.execute(
                    select(GenerationAttempt).where(GenerationAttempt.attempt_key == attempt_key)
                )
            ).scalar_one()
            consumo = (await db.execute(select(ConsumoLLM))).scalar_one()
            pending = (await db.execute(select(func.count(LLMUsageReconciliation.id)))).scalar_one()
        self.assertEqual(
            (attempt.outcome, attempt.accounting_state, attempt.consumo_llm_id),
            ("succeeded", "recorded", consumo.id),
        )
        self.assertEqual(pending, 0)

    async def _deadlock_inside(self, operation, prepare=None) -> BaseException | object:
        """Run ``operation`` so its consumption insert deadlocks as the light victim."""

        agent, instance = await self._agent("deadlock")
        await self._intake(instance, "5511990000080", "deadlock-1", "um")
        async with self.sessions() as db:
            sessao_id = (await db.execute(select(Sessao.id))).scalar_one()
        if prepare is not None:
            await prepare(agent, sessao_id)
        other = self.sessions()
        main_db = self.sessions()
        try:
            await main_db.execute(
                select(Sessao.id).where(Sessao.id == sessao_id).with_for_update()
            )
            # The competing transaction modifies many rows so InnoDB picks the
            # operation under test (no undo yet) as the deadlock victim.
            await other.execute(
                text("UPDATE agentes SET nome=CONCAT(nome,'!') WHERE id=:agent"),
                {"agent": agent},
            )
            other.add_all(
                Atendente(nome="peso", numero_whatsapp=f"peso-{uuid4()}") for _ in range(40)
            )
            await other.flush()
            waiter = asyncio.create_task(
                other.execute(select(Sessao.id).where(Sessao.id == sessao_id).with_for_update())
            )
            await asyncio.sleep(0.5)
            self.assertFalse(waiter.done())
            try:
                result = await operation(main_db, agent, sessao_id)
            except BaseException as exc:
                result = exc
            await main_db.rollback()
            await asyncio.wait_for(waiter, timeout=20)
            await other.rollback()
            return result
        finally:
            await main_db.close()
            await other.close()

    async def test_consumption_insert_reraises_lost_transaction(self) -> None:
        async def operation(db, agent, sessao_id):
            return await consumo_repo.registrar_ou_reconciliar(
                db,
                agente_id=agent,
                sessao_id=sessao_id,
                modelo_llm=MODEL,
                tipo_chamada="resposta",
                tokens_input=1,
                tokens_output=1,
                custo_creditos=Decimal("0.00000100"),
                provider_generation_id="deadlock-generation",
            )

        result = await self._deadlock_inside(operation)
        self.assertIsInstance(result, DBAPIError)
        self.assertTrue(db_errors.transaction_lost(result))
        self.assertIn(1213, db_errors.mysql_error_codes(result))
        self.assertEqual(await self._scalar(select(func.count(LLMUsageReconciliation.id))), 0)
        self.assertEqual(await self._scalar(select(func.count(ConsumoLLM.id))), 0)

    async def test_usage_reconciliation_batch_reraises_lost_transaction(self) -> None:
        async def prepare(agent, sessao_id):
            # Due reconciliation row, committed before any harness lock.
            async with self.sessions() as seed:
                await usage_reconciliation_repo.record(
                    seed,
                    agente_id=agent,
                    sessao_id=sessao_id,
                    modelo_llm=MODEL,
                    tipo_chamada="resposta",
                    tokens_input=1,
                    tokens_output=1,
                    custo_creditos=Decimal("0.00000100"),
                    safe_error_code="probe",
                    provider_generation_id="deadlock-reconcile",
                )
                await seed.commit()

        async def operation(db, agent, sessao_id):
            return await usage_reconciliation_repo.reconcile_due(db, limit=10)

        result = await self._deadlock_inside(operation, prepare)
        self.assertIsInstance(result, DBAPIError)
        self.assertTrue(db_errors.transaction_lost(result))
        async with self.sessions() as db:
            row = (await db.execute(select(LLMUsageReconciliation))).scalar_one()
        self.assertEqual((row.status, row.attempt_count, row.last_error_code), ("pending", 0, None))

    async def test_maintenance_real_lock_timeout_counts_and_recovers(self) -> None:
        agent, _ = await self._agent("maintenance-skip")
        blocker = self.sessions()
        worker = delivery_worker.DeliveryWorker()
        worker._readiness_ok = True
        worker._provider_ready = True
        worker._runtime_compatible = True
        original_maintenance = (
            delivery_worker.delivery_worker_repo.run_preclaim_maintenance
        )

        async def lock_wait_timeout(db: AsyncSession):
            await db.execute(text("SET SESSION innodb_lock_wait_timeout=1"))
            await db.execute(
                text("UPDATE agentes SET nome=nome WHERE id=:agent"),
                {"agent": agent},
            )

        try:
            await blocker.execute(
                text("UPDATE agentes SET nome=CONCAT(nome,'!') WHERE id=:agent"),
                {"agent": agent},
            )
            with tempfile.TemporaryDirectory() as directory, patch.object(
                delivery_worker, "AsyncSessionLocal", self.sessions
            ), patch.object(
                delivery_worker.settings, "delivery_worker_health_path", str(Path(directory) / "health.json")
            ), patch.object(
                delivery_worker.settings, "delivery_maintenance_skip_error_threshold", 1
            ), patch.object(
                delivery_worker.delivery_worker_repo,
                "run_preclaim_maintenance",
                lock_wait_timeout,
            ), patch.object(delivery_worker.logger, "error") as error:
                self.assertIsNone(await worker._maintenance())
                self.assertEqual(worker._maintenance_consecutive_skips, 1)
                self.assertFalse(worker.maintenance_healthy)
                self.assertFalse(worker._stop.is_set())
                self.assertEqual(error.call_count, 1)
                worker._publish_health_snapshot()
                snapshot = json.loads(
                    Path(delivery_worker.settings.delivery_worker_health_path).read_text()
                )
                self.assertEqual(snapshot["maintenance_consecutive_skips"], 1)
                self.assertFalse(snapshot["maintenance_healthy"])

                await blocker.rollback()
                with patch.object(
                    delivery_worker.delivery_worker_repo,
                    "run_preclaim_maintenance",
                    original_maintenance,
                ):
                    self.assertIsNone(await worker._maintenance())
                self.assertEqual(worker._maintenance_consecutive_skips, 0)
                self.assertTrue(worker.maintenance_healthy)
        finally:
            await blocker.rollback()
            await blocker.close()

    async def test_quarantined_expired_lanes_do_not_starve_crash_recovery(self) -> None:
        quarantined: list[tuple[int, str]] = []
        for index in range(5):
            agent, lease, _ = await self._started_attempt(
                f"quarantine-{index}", f"55119900001{index:02d}"
            )
            await self._expire_lane(agent, 60)
            outcome = await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions,
                lambda db, agent=agent: scheduler_repo.recover_expired_lane(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.delivery_epoch,
                    retry_at=datetime.utcnow() + timedelta(seconds=60),
                ),
            )
            self.assertEqual(outcome, "quarantined")
            quarantined.append((agent, lease.owner_token))
        victim, victim_instance = await self._agent("victim")
        await self._intake(victim_instance, "5511990000199", "victim-1", "um")
        await self._force_ready(agente_id=victim)
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=victim,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="crashed-victim",
                lease_seconds=30,
            ),
        )
        await self._expire_lane(victim, 1)
        # Both discovery APIs must exclude unresolved quarantine before LIMIT.
        # The processor uses hints; compatibility callers still use agent IDs.
        async with self.sessions() as db:
            agent_ids = await turn_generation_repo.list_expired_lane_agent_ids(db, limit=4)
        async with self.sessions() as db:
            hints = await turn_generation_repo.list_expired_lane_hints(db, limit=4)
        self.assertEqual(agent_ids, [victim])
        self.assertEqual([hint[0] for hint in hints], [victim])
        processor = self._processor(FakeLLM(), RecordingConsumer(self.sessions, complete=False))
        processor.concurrency = 4
        recovered: list[int] = []
        original_recover = processor._recover

        async def tracking(agente_id: int, **kwargs) -> None:
            recovered.append(agente_id)
            await original_recover(agente_id, **kwargs)

        processor._recover = tracking
        with patch.object(turn_generation_repo, "list_ready_agent_ids", AsyncMock(return_value=[])):
            await processor.run_pass()
        self.assertEqual(recovered, [victim])
        async with self.sessions() as db:
            lanes = {
                row.agente_id: row
                for row in (await db.execute(select(AgentServiceLane))).scalars()
            }
            victim_turn = (
                await db.execute(select(ConversationTurn).where(ConversationTurn.agente_id == victim))
            ).scalar_one()
        self.assertIsNone(lanes[victim].owner_token)
        self.assertEqual(victim_turn.status, "retry")
        self.assertNotEqual(victim_turn.claim_token, claim.claim_token)
        for agent, token in quarantined:
            self.assertEqual(lanes[agent].owner_token, token)


    # ── Q7: requested model is the billing/link key; provider echo kept apart ──
    async def test_dated_provider_echo_links_exactly_without_quarantine(self) -> None:
        agent, instance = await self._agent("echo")
        await self._intake(instance, "5511990000090", "echo-1", "um")
        await self._force_ready()
        llm = FakeLLM()
        llm.reported = "openai/gpt-4o-mini-2024-07-18"
        consumer = RecordingConsumer(self.sessions, complete=False)
        processor = self._processor(llm, consumer)
        await processor.run_pass()
        await asyncio.wait_for(consumer.received.wait(), timeout=20)
        await processor.shutdown(5)
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
            consumo = (await db.execute(select(ConsumoLLM))).scalar_one()
        self.assertEqual(
            (attempt.modelo_llm, attempt.outcome, attempt.accounting_state, attempt.consumo_llm_id),
            (MODEL, "succeeded", "recorded", consumo.id),
        )
        self.assertEqual(
            (consumo.modelo_llm, consumo.modelo_llm_reportado),
            (MODEL, "openai/gpt-4o-mini-2024-07-18"),
        )
        self.assertEqual(consumer.candidates[0].draft_kind, "generated")
        quarantined = await self._scalar(
            select(func.count(GenerationAttempt.id)).where(
                GenerationAttempt.accounting_state == "reconciliation_required"
            )
        )
        self.assertEqual(quarantined, 0)

    async def test_legacy_runner_persists_requested_and_reported_models(self) -> None:
        agent, _ = await self._agent("legacy-echo")
        completion = SimpleNamespace(
            id=None,
            model="openai/gpt-4o-mini-2024-07-18",
            choices=[SimpleNamespace(message=SimpleNamespace(content="resposta legada"))],
            usage=SimpleNamespace(prompt_tokens=7, completion_tokens=3, model_extra={"cost": "0.0000021"}),
            model_extra={},
        )
        client = MagicMock()
        client.chat.completions.create = AsyncMock(return_value=completion)
        with patch("src.services.llm_client._client", return_value=client):
            async with self.sessions() as db:
                agente = await db.get(Agente, agent)
                sessao = Sessao(agente_id=agent, telefone="5511990000091")
                db.add(sessao)
                await db.flush()
                texto, transfer = await agent_runner.processar(agente, sessao.id, "oi", db)
                resumo = await agent_runner.gerar_resumo(sessao.id, agente, db)
                await db.commit()
        self.assertEqual((texto, transfer, resumo), ("resposta legada", False, "resposta legada"))
        async with self.sessions() as db:
            rows = list(
                (await db.execute(select(ConsumoLLM).order_by(ConsumoLLM.id))).scalars()
            )
        self.assertEqual(
            [(r.tipo_chamada, r.modelo_llm, r.modelo_llm_reportado) for r in rows],
            [
                ("resposta", MODEL, "openai/gpt-4o-mini-2024-07-18"),
                ("resumo", MODEL, "openai/gpt-4o-mini-2024-07-18"),
            ],
        )

    async def test_consumption_endpoint_totals_ignore_reported_model(self) -> None:
        agent, _ = await self._agent("totals")
        # DATETIME(0) rounds fractional seconds; keep rows strictly in the past.
        created = datetime.utcnow().replace(microsecond=0) - timedelta(minutes=1)
        async with self.sessions() as db:
            db.add_all(
                [
                    ConsumoLLM(
                        agente_id=agent, modelo_llm=MODEL, modelo_llm_reportado=None,
                        tipo_chamada="resposta", tokens_input=10, tokens_output=2,
                        custo_creditos=Decimal("0.00001000"), criado_em=created,
                    ),
                    ConsumoLLM(
                        agente_id=agent, modelo_llm=MODEL,
                        modelo_llm_reportado="openai/gpt-4o-mini-2024-07-18",
                        tipo_chamada="resumo", tokens_input=5, tokens_output=1,
                        custo_creditos=Decimal("0.00000500"), criado_em=created,
                    ),
                ]
            )
            await db.commit()

        async def db_override():
            async with self.sessions() as session:
                yield session
                await session.commit()

        main.app.dependency_overrides[get_db] = db_override
        auth_fixtures.autenticar_como(main.app)  # AUTH-1: admin principal
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app), base_url="http://ticket04.test"
            ) as client:
                response = await client.get(
                    f"/agentes/{agent}/consumo",
                    params={"inicio": (created - timedelta(days=1)).isoformat()},
                )
        finally:
            main.app.dependency_overrides.clear()
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(
            (
                body["chamadas_total"], body["chamadas_resposta"], body["chamadas_resumo"],
                body["tokens_input_total"], body["tokens_output_total"],
                Decimal(str(body["custo_total_creditos"])),
            ),
            (2, 1, 1, 15, 3, Decimal("0.000015")),
        )


if __name__ == "__main__":
    unittest.main()
