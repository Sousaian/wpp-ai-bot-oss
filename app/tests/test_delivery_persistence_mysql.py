import ast
import asyncio
import os
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import ForeignKeyConstraint, UniqueConstraint, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.database.models import (
    Agente,
    Base,
    ConsumoLLM,
    DeliveryOperatorAction,
    DeliveryWorkerHeartbeat,
    HandoffAttempt,
    InboundEvent,
    LLMUsageReconciliation,
    Mensagem,
    OutboxDelivery,
    Sessao,
)
from src.services.llm_client import LLMResult
from src.database.unit_of_work import UnitOfWork
from src.database.repositories import (
    consumo_repo,
    delivery_state_repo,
    handoff_attempt_repo,
    inbound_event_repo,
    outbox_repo,
    sessao_repo,
    usage_reconciliation_repo,
)
from tests import auth_fixtures


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")


class DeliveryPersistenceMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail(
                "OUTBOX_TEST_DATABASE_URL is required for the mandatory "
                "persistence integration suite"
            )
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table in (
                "delivery_operator_actions",
                "outbox_deliveries",
                "handoff_attempts",
                "inbound_events",
                "llm_usage_reconciliation",
                "consumo_llm",
                "mensagens",
                "sessoes",
                "agentes",
            ):
                await connection.execute(text(f"DELETE FROM {table}"))
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            await connection.execute(
                text("UPDATE delivery_runtime_state SET mode='direct'")
            )

        async with self.sessions() as db:
            agente = Agente(
                nome="Persistence test",
                instance_name=f"test-{uuid4()}",
                system_prompt="test",
                numero_notificacao="0",
            )
            db.add(agente)
            await db.commit()
            self.agente_id = agente.id

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def test_new_outbox_delivery_requires_explicit_nonlegacy_class(self) -> None:
        repo = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        production_paths = (
            "app/src/services/handoff_manager.py",
            "app/src/services/chatwoot_orchestrator.py",
            "app/src/services/evolution_orchestrator.py",
            "app/src/database/repositories/turn_handoff_repo.py",
            "app/src/database/repositories/turn_delivery_repo.py",
        )
        production_calls: list[tuple[str, int, set[str | None]]] = []
        for relative in production_paths:
            tree = ast.parse((repo / relative).read_text(), filename=relative)
            for node in ast.walk(tree):
                if not (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "enqueue"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "outbox_repo"
                ):
                    continue
                production_calls.append(
                    (relative, node.lineno, {keyword.arg for keyword in node.keywords})
                )
        self.assertEqual(len(production_calls), 8)
        self.assertEqual(
            [(relative, line) for relative, line, keywords in production_calls
             if "_delivery_class" not in keywords],
            [],
        )

        async with self.sessions.begin() as db:
            session = await sessao_repo.get_or_create_for_update(
                db, self.agente_id, "551199999999"
            )
            sequence = (await sessao_repo.allocate_delivery_sequences(db, session))[0]
            common = {
                "cutover_epoch": str(uuid4()),
                "provider": "evolution",
                "operation": "evolution.send_text.v1",
                "payload_version": 1,
                "payload": {"synthetic": True},
                "ordering_key": session.delivery_stream_key,
                "stream_sequence": sequence,
                "next_attempt_at": datetime.utcnow(),
                "expires_at": datetime.utcnow() + timedelta(hours=1),
                "agente_id": self.agente_id,
                "sessao_id": session.id,
            }
            for delivery_class in (None, "legacy_unknown"):
                kwargs = {} if delivery_class is None else {
                    "_delivery_class": delivery_class
                }
                with self.subTest(delivery_class=delivery_class), self.assertRaises(
                    outbox_repo.OutboxDeliveryClassRequiredError
                ):
                    await outbox_repo.enqueue(
                        db, dedupe_key=f"unclassified:{uuid4()}", **common, **kwargs
                    )

        async with self.sessions() as db:
            unknown = await db.scalar(
                select(OutboxDelivery.id).where(
                    OutboxDelivery.delivery_class == "legacy_unknown"
                )
            )
        self.assertIsNone(unknown)

    async def _create_handoff_delivery(
        self,
        *,
        telefone: str,
        status: str,
    ) -> tuple[int, int, int]:
        now = datetime.utcnow()
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, telefone
                )
                attempt = await handoff_attempt_repo.create_for_locked_session(
                    db,
                    sessao=sessao,
                    chatwoot_inbox_id=7,
                )
                await handoff_attempt_repo.mark_ready(
                    db,
                    sessao=sessao,
                    attempt_id=attempt.id,
                    contact_id=1000 + attempt.id,
                    conversation_id=2000 + attempt.id,
                )
                sessao.handler = "encerrada"
                sessao.encerrada_em = now
                sequence = (
                    await sessao_repo.allocate_delivery_sequences(db, sessao)
                )[0]
                delivery, _ = await outbox_repo.enqueue(
                    db,
                    cutover_epoch=str(uuid4()),
                    dedupe_key=f"route-test:{uuid4()}",
                    provider="evolution",
                    operation="evolution.send_text.v1",
                    payload_version=1,
                    payload={"text": "restricted"},
                    ordering_key=sessao.delivery_stream_key,
                    stream_sequence=sequence,
                    next_attempt_at=now,
                    expires_at=now + timedelta(minutes=30),
                    agente_id=self.agente_id,
                    sessao_id=sessao.id,
                    handoff_attempt_id=attempt.id,
                    _delivery_class="legacy_automated",
                )
                if status == "processing":
                    delivery.status = "processing"
                    delivery.lease_token = str(uuid4())
                    delivery.lock_owner = "route-test-worker"
                    delivery.lock_until = now + timedelta(minutes=1)
                    delivery.attempt_count = 1
                    await db.flush()
                elif status != "pending":
                    raise ValueError("unsupported test delivery status")
                return sessao.id, attempt.id, delivery.id

    async def test_concurrent_first_creation_returns_one_usable_session(self) -> None:
        first_locked = asyncio.Event()
        release_first = asyncio.Event()

        async def first_transaction() -> int:
            async with self.sessions() as db:
                async with db.begin():
                    session = await sessao_repo.get_or_create_for_update(
                        db, self.agente_id, "551100000001"
                    )
                    first_locked.set()
                    await release_first.wait()
                await db.execute(text("SELECT 1"))
                return session.id

        async def second_transaction() -> int:
            await first_locked.wait()
            async with self.sessions() as db:
                async with db.begin():
                    session = await sessao_repo.get_or_create_for_update(
                        db, self.agente_id, "551100000001"
                    )
                await db.execute(text("SELECT 1"))
                return session.id

        first_task = asyncio.create_task(first_transaction())
        second_task = asyncio.create_task(second_transaction())
        await first_locked.wait()
        await asyncio.sleep(0.1)
        self.assertFalse(second_task.done())
        release_first.set()
        first_id, second_id = await asyncio.gather(first_task, second_task)

        self.assertEqual(first_id, second_id)
        async with self.sessions() as db:
            rows = await db.execute(
                select(Sessao).where(
                    Sessao.agente_id == self.agente_id,
                    Sessao.telefone == "551100000001",
                )
            )
            self.assertEqual(len(rows.scalars().all()), 1)

    async def test_locked_sequence_allocation_serializes_one_stream(self) -> None:
        async with self.sessions() as db:
            async with db.begin():
                created = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551100000002"
                )
                session_id = created.id

        first_locked = asyncio.Event()
        release_first = asyncio.Event()

        async def allocate_first() -> int:
            async with self.sessions() as db:
                async with db.begin():
                    locked = await sessao_repo.lock_by_id(db, session_id)
                    self.assertIsNotNone(locked)
                    first_locked.set()
                    await release_first.wait()
                    return (await sessao_repo.allocate_delivery_sequences(db, locked))[0]

        async def allocate_second() -> int:
            await first_locked.wait()
            async with self.sessions() as db:
                async with db.begin():
                    locked = await sessao_repo.lock_by_id(db, session_id)
                    self.assertIsNotNone(locked)
                    return (await sessao_repo.allocate_delivery_sequences(db, locked))[0]

        task_one = asyncio.create_task(allocate_first())
        task_two = asyncio.create_task(allocate_second())
        await first_locked.wait()
        await asyncio.sleep(0.1)
        self.assertFalse(task_two.done())
        release_first.set()
        sequences = await asyncio.gather(task_one, task_two)
        self.assertEqual(sequences, [1, 2])

        async with self.sessions() as db:
            stored = await db.get(Sessao, session_id)
            self.assertEqual(stored.next_delivery_sequence, 3)

    async def test_receipt_replay_and_collision_leave_session_usable(self) -> None:
        identity = {
            "provider": "evolution",
            "source_scope": "instance-a",
            "provider_event_id": "message-a",
            "event_type": "messages.upsert",
        }
        actionable = {
            "instance_name": "instance-a",
            "remote_jid": "551100000001@s.whatsapp.net",
            "text": "quero atendimento",
        }
        first_keys = inbound_event_repo.ReceiptHMACKeyRing(
            current_version=1,
            keys={1: b"old-receipt-secret"},
        )
        rotated_keys = inbound_event_repo.ReceiptHMACKeyRing(
            current_version=2,
            keys={1: b"old-receipt-secret", 2: b"new-receipt-secret"},
        )
        async with self.sessions() as db:
            first = await inbound_event_repo.record(
                db,
                actionable_fields=actionable,
                keyring=first_keys,
                **identity,
            )
            await db.commit()
            self.assertTrue(first.created)
            self.assertEqual(first.event.hmac_key_version, 1)

        async with self.sessions() as db:
            replay = await inbound_event_repo.record(
                db,
                actionable_fields=dict(reversed(list(actionable.items()))),
                keyring=rotated_keys,
                **identity,
            )
            self.assertTrue(replay.replay)
            self.assertEqual(replay.event.hmac_key_version, 1)
            with self.assertRaises(inbound_event_repo.ReceiptCollisionError):
                await inbound_event_repo.record(
                    db,
                    actionable_fields={**actionable, "text": "conteúdo alterado"},
                    keyring=rotated_keys,
                    **identity,
                )
            self.assertEqual((await db.execute(text("SELECT 1"))).scalar_one(), 1)

    async def test_receipt_rotation_rejects_corrupt_or_retired_metadata(self) -> None:
        actionable = {
            "instance_name": "instance-a",
            "remote_jid": "551100000099@s.whatsapp.net",
            "text": "olá",
        }
        identity = {
            "provider": "evolution",
            "source_scope": "instance-a",
            "provider_event_id": "message-corrupt",
            "event_type": "messages.upsert",
        }
        old_keys = inbound_event_repo.ReceiptHMACKeyRing(
            current_version=1,
            keys={1: "old-secret"},
        )
        rotated_keys = inbound_event_repo.ReceiptHMACKeyRing(
            current_version=2,
            keys={1: "old-secret", 2: "new-secret"},
        )
        async with self.sessions() as db:
            created = await inbound_event_repo.record(
                db,
                actionable_fields=actionable,
                keyring=old_keys,
                **identity,
            )
            await db.commit()
            event_id = created.event.id

        async with self.sessions() as db:
            event = await db.get(InboundEvent, event_id)
            event.payload_hmac = "A" * 64
            await db.commit()
        async with self.sessions() as db:
            with self.assertRaises(inbound_event_repo.MalformedReceiptDigestError):
                await inbound_event_repo.record(
                    db,
                    actionable_fields=actionable,
                    keyring=rotated_keys,
                    **identity,
                )
            self.assertEqual((await db.execute(text("SELECT 1"))).scalar_one(), 1)

        async with self.sessions() as db:
            event = await db.get(InboundEvent, event_id)
            event.payload_hmac = old_keys.digest(actionable)
            event.hmac_key_version = 2
            await db.commit()
        async with self.sessions() as db:
            with self.assertRaises(inbound_event_repo.ReceiptCollisionError):
                await inbound_event_repo.record(
                    db,
                    actionable_fields=actionable,
                    keyring=rotated_keys,
                    **identity,
                )
            self.assertEqual((await db.execute(text("SELECT 1"))).scalar_one(), 1)

        async with self.sessions() as db:
            event = await db.get(InboundEvent, event_id)
            event.hmac_key_version = 99
            await db.commit()
        async with self.sessions() as db:
            with self.assertRaises(inbound_event_repo.RetiredReceiptKeyError):
                await inbound_event_repo.record(
                    db,
                    actionable_fields=actionable,
                    keyring=rotated_keys,
                    **identity,
                )
            self.assertEqual((await db.execute(text("SELECT 1"))).scalar_one(), 1)

        with self.assertRaises(inbound_event_repo.ReceiptKeyVersionError):
            rotated_keys.digest(actionable, version=0)
        with self.assertRaises(inbound_event_repo.ReceiptKeyVersionError):
            inbound_event_repo.ReceiptHMACKeyRing(
                current_version="1",  # type: ignore[arg-type]
                keys={1: "old-secret"},
            )

    async def test_resume_preserves_history_and_second_handoff_has_new_identity(self) -> None:
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551100000003"
                )
                session_id = sessao.id
                first = await handoff_attempt_repo.create_for_locked_session(
                    db, sessao=sessao, chatwoot_inbox_id=7
                )
                first_id = first.id
            async with db.begin():
                locked = await sessao_repo.lock_by_id(db, session_id)
                await handoff_attempt_repo.mark_ready(
                    db,
                    sessao=locked,
                    attempt_id=first_id,
                    contact_id=10,
                    conversation_id=20,
                )
                locked.handler = "encerrada"
            async with db.begin():
                locked = await sessao_repo.lock_by_id(db, session_id)
                superseded = await handoff_attempt_repo.supersede_current_and_resume(
                    db, sessao=locked
                )
                self.assertEqual(superseded.id, first_id)
                self.assertIsNone(locked.chatwoot_conversation_id)
                second = await handoff_attempt_repo.create_for_locked_session(
                    db, sessao=locked, chatwoot_inbox_id=7
                )
                second_id = second.id
                self.assertNotEqual(second.handoff_key, first.handoff_key)
                self.assertEqual(second.generation, 2)

        async with self.sessions() as db:
            attempts = (
                await db.execute(
                    select(HandoffAttempt)
                    .where(HandoffAttempt.sessao_id == session_id)
                    .order_by(HandoffAttempt.generation)
                )
            ).scalars().all()
            self.assertEqual([row.id for row in attempts], [first_id, second_id])
            self.assertEqual([row.status for row in attempts], ["superseded", "pending"])

    async def test_ready_and_resume_share_session_first_lock_order(self) -> None:
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551100000033"
                )
                attempt = await handoff_attempt_repo.create_for_locked_session(
                    db, sessao=sessao, chatwoot_inbox_id=7
                )
                session_id = sessao.id
                attempt_id = attempt.id

        ready_has_session_lock = asyncio.Event()
        release_ready = asyncio.Event()

        async def ready_transaction() -> None:
            async with self.sessions() as db:
                async with db.begin():
                    locked = await sessao_repo.lock_by_id(db, session_id)
                    self.assertIsNotNone(locked)
                    ready_has_session_lock.set()
                    await release_ready.wait()
                    await handoff_attempt_repo.mark_ready(
                        db,
                        sessao=locked,
                        attempt_id=attempt_id,
                        contact_id=91,
                        conversation_id=92,
                    )

        async def resume_transaction() -> None:
            await ready_has_session_lock.wait()
            async with self.sessions() as db:
                async with db.begin():
                    locked = await sessao_repo.lock_by_id(db, session_id)
                    self.assertIsNotNone(locked)
                    await handoff_attempt_repo.supersede_current_and_resume(
                        db, sessao=locked
                    )

        ready_task = asyncio.create_task(ready_transaction())
        resume_task = asyncio.create_task(resume_transaction())
        await ready_has_session_lock.wait()
        await asyncio.sleep(0.1)
        self.assertFalse(resume_task.done())
        release_ready.set()
        await asyncio.wait_for(
            asyncio.gather(ready_task, resume_task), timeout=5
        )

        async with self.sessions() as db:
            sessao = await db.get(Sessao, session_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            self.assertEqual(sessao.handler, "bot")
            self.assertIsNone(sessao.chatwoot_conversation_id)
            self.assertEqual(attempt.status, "superseded")
            self.assertEqual(attempt.chatwoot_conversation_id, 92)
            self.assertIsNotNone(attempt.ready_at)
            self.assertIsNotNone(attempt.terminal_at)

    async def test_usage_insert_failure_creates_durable_reconciliation(self) -> None:
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551100000004"
                )
                session_id = sessao.id

        with patch.object(
            consumo_repo,
            "registrar",
            AsyncMock(side_effect=SQLAlchemyError("forced consumption failure")),
        ):
            async with self.sessions() as db:
                result = await consumo_repo.registrar_ou_reconciliar(
                    db,
                    agente_id=self.agente_id,
                    sessao_id=session_id,
                    modelo_llm="openai/gpt-4o-mini",
                    tipo_chamada="resposta",
                    tokens_input=10,
                    tokens_output=5,
                    custo_creditos=Decimal("0.00000100"),
                    provider_generation_id=f"gen-{uuid4()}",
                )
                await db.commit()
                self.assertIsNone(result.consumo)
                self.assertIsNotNone(result.reconciliation)

            async with self.sessions() as db:
                rows = (
                    await db.execute(select(LLMUsageReconciliation))
                ).scalars().all()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0].safe_error_code, "consumo_insert_failed")

    async def test_reconciliation_is_bounded_and_pending_spend_is_visible(self) -> None:
        generation_id = f"gen-{uuid4()}"
        cost = Decimal("1.25000000")
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551100000044"
                )
                agente = await db.get(Agente, self.agente_id)
                agente.limite_mensal_creditos = Decimal("1.000000")
                await usage_reconciliation_repo.record(
                    db,
                    agente_id=self.agente_id,
                    sessao_id=sessao.id,
                    modelo_llm="openai/gpt-4o-mini",
                    tipo_chamada="resposta",
                    tokens_input=100,
                    tokens_output=50,
                    custo_creditos=cost,
                    provider_generation_id=generation_id,
                    safe_error_code="consumo_insert_failed",
                )

        async with self.sessions() as db:
            self.assertEqual(
                await consumo_repo.total_do_mes(self.agente_id, db), cost
            )
            self.assertTrue(
                await consumo_repo.atingiu_limite_mensal(self.agente_id, db)
            )
            summary = await consumo_repo.resumo_por_periodo(
                self.agente_id,
                db,
                datetime.utcnow() - timedelta(days=1),
                datetime.utcnow() + timedelta(days=1),
            )
            self.assertEqual(summary["reconciliacoes_pendentes"], 1)
            self.assertEqual(
                summary["custo_pendente_reconciliacao_creditos"], cost
            )

        async with self.sessions() as db:
            async with db.begin():
                batch = await usage_reconciliation_repo.reconcile_due(
                    db, limit=1
                )
            self.assertEqual(batch.examined, 1)
            self.assertEqual(batch.resolved, 1)

        async with self.sessions() as db:
            reconciliation = (
                await db.execute(
                    select(LLMUsageReconciliation).where(
                        LLMUsageReconciliation.provider_generation_id
                        == generation_id
                    )
                )
            ).scalar_one()
            consumptions = (
                await db.execute(
                    select(ConsumoLLM).where(
                        ConsumoLLM.provider_generation_id == generation_id
                    )
                )
            ).scalars().all()
            self.assertEqual(reconciliation.status, "resolved")
            self.assertEqual(len(consumptions), 1)
            self.assertEqual(
                await consumo_repo.total_do_mes(self.agente_id, db), cost
            )

    async def test_double_usage_persistence_failure_raises_invariant(self) -> None:
        async with self.sessions() as db:
            with (
                patch.object(
                    consumo_repo,
                    "registrar",
                    AsyncMock(side_effect=SQLAlchemyError("primary failed")),
                ),
                patch.object(
                    usage_reconciliation_repo,
                    "record",
                    AsyncMock(side_effect=SQLAlchemyError("fallback failed")),
                ),
            ):
                with self.assertRaises(
                    consumo_repo.UsagePersistenceInvariantError
                ):
                    await consumo_repo.registrar_ou_reconciliar(
                        db,
                        agente_id=self.agente_id,
                        sessao_id=None,
                        modelo_llm="openai/gpt-4o-mini",
                        tipo_chamada="resposta",
                        tokens_input=1,
                        tokens_output=1,
                        custo_creditos=Decimal("0.1"),
                        provider_generation_id=f"gen-{uuid4()}",
                    )
                self.assertEqual(
                    (await db.execute(text("SELECT 1"))).scalar_one(), 1
                )

    async def test_usage_generation_replay_is_verified_without_duplicate(self) -> None:
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551100000006"
                )
                session_id = sessao.id

        generation_id = f"gen-{uuid4()}"
        values = {
            "agente_id": self.agente_id,
            "sessao_id": session_id,
            "modelo_llm": "openai/gpt-4o-mini",
            "tipo_chamada": "resposta",
            "tokens_input": 10,
            "tokens_output": 5,
            "custo_creditos": Decimal("0.00000100"),
            "provider_generation_id": generation_id,
        }
        async with self.sessions() as db:
            first = await consumo_repo.registrar_ou_reconciliar(db, **values)
            await db.commit()
            self.assertIsNotNone(first.consumo)

        async with self.sessions() as db:
            replay = await consumo_repo.registrar_ou_reconciliar(db, **values)
            self.assertEqual(replay.consumo.id, first.consumo.id)
            with self.assertRaises(consumo_repo.UsageConflictError):
                await consumo_repo.registrar_ou_reconciliar(
                    db, **{**values, "tokens_output": 6}
                )
            self.assertEqual((await db.execute(text("SELECT 1"))).scalar_one(), 1)

    async def test_outbox_reservation_idempotency_cancellation_and_delete_guard(self) -> None:
        now = datetime.utcnow()
        epoch = str(uuid4())
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551100000005"
                )
                sequence = (await sessao_repo.allocate_delivery_sequences(db, sessao))[0]
                attempt = await handoff_attempt_repo.create_for_locked_session(
                    db, sessao=sessao, chatwoot_inbox_id=7
                )
                chatwoot_job, created = await outbox_repo.enqueue(
                    db,
                    cutover_epoch=epoch,
                    dedupe_key=f"handoff:{attempt.handoff_key}:open",
                    provider="chatwoot",
                    operation="chatwoot.open_handoff.v1",
                    payload_version=1,
                    payload={"summary": "restricted"},
                    ordering_key=attempt.handoff_key,
                    stream_sequence=0,
                    next_attempt_at=now,
                    expires_at=now + timedelta(hours=24),
                    agente_id=self.agente_id,
                    sessao_id=sessao.id,
                    handoff_attempt_id=attempt.id,
                    _delivery_class="provider_setup_context",
                )
                self.assertTrue(created)
                transition, transition_created = await outbox_repo.reserve_transition(
                    db,
                    dependency=chatwoot_job,
                    cutover_epoch=epoch,
                    dedupe_key=f"handoff:{attempt.handoff_key}:transition",
                    payload_version=1,
                    payload={"text": "restricted"},
                    ordering_key=sessao.delivery_stream_key,
                    stream_sequence=sequence,
                    agente_id=self.agente_id,
                    sessao_id=sessao.id,
                    handoff_attempt_id=attempt.id,
                    inbound_event_id=None,
                )
                self.assertTrue(transition_created)
                self.assertEqual(transition.status, "blocked")
                with self.assertRaises(outbox_repo.ActiveDeliveriesError):
                    await outbox_repo.assert_session_deletable(db, sessao.id)
                queued, processing = await outbox_repo.request_cancellation_for_session(
                    db, sessao_id=sessao.id, requested_at=now
                )
                self.assertEqual((queued, processing), (2, 0))
                session_id = sessao.id

        async with self.sessions() as db:
            deliveries = (
                await db.execute(
                    select(OutboxDelivery)
                    .where(OutboxDelivery.sessao_id == session_id)
                    .order_by(OutboxDelivery.id)
                )
            ).scalars().all()
            self.assertEqual([row.status for row in deliveries], ["cancelled", "cancelled"])
            await outbox_repo.assert_session_deletable(db, session_id)

    async def test_database_rejects_processing_without_complete_lease(self) -> None:
        async with self.sessions() as db:
            invalid = OutboxDelivery(
                delivery_key=str(uuid4()),
                cutover_epoch=str(uuid4()),
                dedupe_key=f"invalid:{uuid4()}",
                provider="evolution",
                operation="evolution.send_text.v1",
                payload_version=1,
                payload={"text": "restricted"},
                agente_id=self.agente_id,
                ordering_key=str(uuid4()),
                stream_sequence=1,
                priority=0,
                status="processing",
                next_attempt_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(minutes=30),
            )
            db.add(invalid)
            with self.assertRaises(SQLAlchemyError):
                await db.flush()
            await db.rollback()
            self.assertEqual((await db.execute(text("SELECT 1"))).scalar_one(), 1)

    async def test_heartbeat_runtime_state_and_operator_audit_persist(self) -> None:
        worker_id = str(uuid4())
        async with self.sessions() as db:
            heartbeat = await delivery_state_repo.record_heartbeat(
                db,
                worker_id=worker_id,
                code_version="test-version",
                current_lease_count=2,
                dispatch_enabled=True,
                configured_mode="outbox",
                configured_epoch=str(uuid4()),
                provider_ready=True,
                runtime_compatible=True,
            )
            state = await delivery_state_repo.get_runtime_state(db, for_update=True)
            new_epoch = str(uuid4())
            updated = await delivery_state_repo.change_runtime_state(
                db,
                mode="frozen",
                cutover_epoch=new_epoch,
                actor="integration-test",
                reason="verify durable state",
                expected_version=state.version,
            )
            action = await delivery_state_repo.record_operator_action(
                db,
                delivery_id=None,
                action="freeze",
                actor="integration-test",
                reason="verify audit",
            )
            await db.commit()
            self.assertEqual(heartbeat.current_lease_count, 2)
            self.assertTrue(heartbeat.dispatch_enabled)
            self.assertTrue(heartbeat.provider_ready)
            self.assertEqual(updated.cutover_epoch, new_epoch)
            action_id = action.id

        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(DeliveryWorkerHeartbeat, worker_id))
            self.assertIsNotNone(await db.get(DeliveryOperatorAction, action_id))

    async def test_direct_rollback_requires_terminal_ambiguity_audit(self) -> None:
        now = datetime.utcnow()
        epoch = str(uuid4())
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, "551199999919"
                )
                sequence = (await sessao_repo.allocate_delivery_sequences(db, sessao))[0]
                delivery, _ = await outbox_repo.enqueue(
                    db,
                    cutover_epoch=epoch,
                    dedupe_key=f"rollback:{uuid4()}",
                    provider="evolution",
                    operation="evolution.send_text.v1",
                    payload_version=1,
                    payload={"phone": sessao.telefone, "message": "x", "instance_name": "x"},
                    ordering_key=sessao.delivery_stream_key,
                    stream_sequence=sequence,
                    next_attempt_at=now,
                    expires_at=now + timedelta(minutes=1),
                    agente_id=self.agente_id,
                    sessao_id=sessao.id,
                    _delivery_class="legacy_automated",
                )
                delivery.status = "dead"
                delivery.ambiguity_count = 1
                delivery.terminal_at = now
                delivery.terminal_reason = "provider_result_ambiguous"
                delivery_id = delivery.id

        async with self.sessions() as db:
            blocked = await delivery_state_repo.get_rollback_readiness(
                db, cutover_epoch=epoch
            )
            self.assertFalse(blocked.ready)
            self.assertEqual(blocked.ambiguous_unresolved, 1)
            self.assertEqual(blocked.terminal_unresolved, 1)
            await db.execute(
                text("UPDATE delivery_runtime_state SET mode='frozen' WHERE id=1")
            )
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=delivery_id,
                resolution="duplicate-risk-accepted",
                actor="integration-test",
                reason="synthetic provider accepted before acknowledgement loss",
            )
            await db.commit()

        async with self.sessions() as db:
            ready = await delivery_state_repo.get_rollback_readiness(
                db, cutover_epoch=epoch
            )
            self.assertTrue(ready.ready)

    async def test_resume_route_supersedes_handoff_and_cancels_queued_work(self) -> None:
        from src.routers import sessoes as sessoes_router

        telefone = "551199999911"
        session_id, attempt_id, delivery_id = await self._create_handoff_delivery(
            telefone=telefone,
            status="pending",
        )
        async with self.sessions() as db:
            response = await sessoes_router.retomar_sessao(
                telefone=telefone,
                agente_id=self.agente_id,
                db=db, principal=auth_fixtures.ADMIN
            )
            await db.commit()
        self.assertEqual(response["status"], "resumed")

        async with self.sessions() as db:
            sessao = await db.get(Sessao, session_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            delivery = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(sessao.handler, "bot")
            self.assertIsNone(sessao.chatwoot_conversation_id)
            self.assertIsNone(sessao.encerrada_em)
            self.assertIsNone(sessao.motivo_encerramento)
            self.assertEqual(attempt.status, "superseded")
            self.assertIsNotNone(attempt.terminal_at)
            self.assertIsNotNone(attempt.chatwoot_conversation_id)
            self.assertEqual(delivery.status, "cancelled")
            self.assertEqual(delivery.terminal_reason, "cancel_requested")
            self.assertIsNotNone(delivery.cancel_requested_at)

    async def test_resume_route_fences_processing_work(self) -> None:
        from src.routers import sessoes as sessoes_router

        telefone = "551199999912"
        session_id, attempt_id, delivery_id = await self._create_handoff_delivery(
            telefone=telefone,
            status="processing",
        )
        async with self.sessions() as db:
            await sessoes_router.retomar_sessao(
                telefone=telefone,
                agente_id=self.agente_id,
                db=db, principal=auth_fixtures.ADMIN
            )
            await db.commit()

        async with self.sessions() as db:
            sessao = await db.get(Sessao, session_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            delivery = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(sessao.handler, "bot")
            self.assertIsNone(sessao.chatwoot_conversation_id)
            self.assertEqual(attempt.status, "superseded")
            self.assertEqual(delivery.status, "processing")
            self.assertIsNotNone(delivery.cancel_requested_at)
            self.assertIsNotNone(delivery.lease_token)
            self.assertIsNotNone(delivery.lock_until)

    async def test_session_delete_route_returns_409_for_queued_work(self) -> None:
        from src.routers import sessoes as sessoes_router

        telefone = "551199999913"
        session_id, _, delivery_id = await self._create_handoff_delivery(
            telefone=telefone,
            status="pending",
        )
        async with self.sessions() as db:
            with self.assertRaises(HTTPException) as raised:
                await sessoes_router.deletar_sessao(
                    telefone=telefone,
                    agente_id=self.agente_id,
                    db=db, principal=auth_fixtures.ADMIN
                )
            self.assertEqual(raised.exception.status_code, 409)

        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(Sessao, session_id))
            self.assertIsNotNone(await db.get(OutboxDelivery, delivery_id))

    async def test_agent_delete_route_returns_409_for_processing_work(self) -> None:
        from src.routers import agentes as agentes_router

        _, _, delivery_id = await self._create_handoff_delivery(
            telefone="551199999914",
            status="processing",
        )
        async with self.sessions() as db:
            with self.assertRaises(HTTPException) as raised:
                await agentes_router.deletar_agente(
                    agente_id=self.agente_id,
                    db=db, principal=auth_fixtures.ADMIN
                )
            self.assertEqual(raised.exception.status_code, 409)

        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(Agente, self.agente_id))
            delivery = await db.get(OutboxDelivery, delivery_id)
            self.assertIsNotNone(delivery)
            self.assertEqual(delivery.status, "processing")

    async def test_response_usage_invariant_rolls_back_route_before_delivery(self) -> None:
        from src import main as main_module

        request = SimpleNamespace(
            headers={},
            json=AsyncMock(
                return_value={
                    "event": "messages.upsert",
                    "instance": "unused",
                    "data": {
                        "instance": None,
                        "key": {
                            "id": "usage-route-response",
                            "fromMe": False,
                            "remoteJid": "551199999901@s.whatsapp.net",
                        },
                        "message": {"conversation": "Olá"},
                    },
                }
            ),
        )
        async with self.sessions() as db:
            agente = await db.get(Agente, self.agente_id)
            request.json.return_value["instance"] = agente.instance_name
        async with UnitOfWork(self.sessions) as uow:
            with (
                patch.object(
                    main_module.settings,
                    "provider_webhook_auth_mode",
                    "provisioning",
                ),
                patch(
                    "src.agents.agent_runner.llm_client.completar",
                    AsyncMock(
                        return_value=LLMResult(
                            texto="Resposta",
                            modelo_llm="openai/gpt-4o-mini",
                            tokens_input=10,
                            tokens_output=5,
                            tokens_reasoning=None,
                            tokens_cache_read=None,
                            tokens_cache_write=None,
                            custo_creditos=Decimal("0.001"),
                            provider_generation_id=f"gen-{uuid4()}",
                        )
                    ),
                ),
                patch.object(
                    consumo_repo,
                    "registrar",
                    AsyncMock(side_effect=SQLAlchemyError("primary failed")),
                ),
                patch.object(
                    usage_reconciliation_repo,
                    "record",
                    AsyncMock(side_effect=SQLAlchemyError("fallback failed")),
                ),
                patch.object(
                    main_module.evolution_orchestrator,
                    "_send_direct",
                    AsyncMock(),
                ) as send,
            ):
                with self.assertRaisesRegex(Exception, "503") as raised:
                    await main_module.webhook_handler(request, uow)
                self.assertEqual(raised.exception.status_code, 503)
                send.assert_not_awaited()

        await self._assert_no_conversation_side_effects("551199999901")

    async def test_summary_usage_invariant_rolls_back_before_any_delivery(self) -> None:
        from src import main as main_module

        generation_prefix = str(uuid4())
        llm_results = [
            LLMResult(
                texto="Vou transferir. [TRANSFERIR]",
                modelo_llm="openai/gpt-4o-mini",
                tokens_input=10,
                tokens_output=5,
                tokens_reasoning=None,
                tokens_cache_read=None,
                tokens_cache_write=None,
                custo_creditos=Decimal("0.001"),
                provider_generation_id=f"{generation_prefix}-response",
            ),
            LLMResult(
                texto="Resumo",
                modelo_llm="openai/gpt-4o-mini",
                tokens_input=20,
                tokens_output=7,
                tokens_reasoning=None,
                tokens_cache_read=None,
                tokens_cache_write=None,
                custo_creditos=Decimal("0.002"),
                provider_generation_id=f"{generation_prefix}-summary",
            ),
        ]
        original_persist = consumo_repo.registrar_ou_reconciliar

        async def fail_summary_only(*args, **kwargs):
            if kwargs["tipo_chamada"] == "resumo":
                raise consumo_repo.UsagePersistenceInvariantError(
                    "summary accounting unavailable"
                )
            return await original_persist(*args, **kwargs)

        request = SimpleNamespace(
            headers={},
            json=AsyncMock(
                return_value={
                    "event": "messages.upsert",
                    "instance": "unused",
                    "data": {
                        "key": {
                            "id": "usage-route-summary",
                            "fromMe": False,
                            "remoteJid": "551199999902@s.whatsapp.net",
                        },
                        "message": {"conversation": "Preciso de uma pessoa"},
                    },
                }
            ),
        )
        async with self.sessions() as db:
            agente = await db.get(Agente, self.agente_id)
            request.json.return_value["instance"] = agente.instance_name
        async with UnitOfWork(self.sessions) as uow:
            with (
                patch.object(
                    main_module.settings,
                    "provider_webhook_auth_mode",
                    "provisioning",
                ),
                patch(
                    "src.agents.agent_runner.llm_client.completar",
                    AsyncMock(side_effect=llm_results),
                ),
                patch.object(
                    consumo_repo,
                    "registrar_ou_reconciliar",
                    side_effect=fail_summary_only,
                ),
                patch.object(
                    main_module.evolution_orchestrator,
                    "_send_direct",
                    AsyncMock(),
                ) as send,
                patch.object(
                    main_module.evolution_orchestrator.handoff_manager,
                    "executar_handoff_direto",
                    AsyncMock(),
                ) as handoff,
            ):
                with self.assertRaisesRegex(Exception, "503") as raised:
                    await main_module.webhook_handler(request, uow)
                self.assertEqual(raised.exception.status_code, 503)
                send.assert_not_awaited()
                handoff.assert_not_awaited()

        await self._assert_no_conversation_side_effects("551199999902")

    async def _assert_no_conversation_side_effects(self, telefone: str) -> None:
        async with self.sessions() as db:
            sessions = (
                await db.execute(
                    select(Sessao).where(
                        Sessao.agente_id == self.agente_id,
                        Sessao.telefone == telefone,
                    )
                )
            ).scalars().all()
            self.assertEqual(sessions, [])
            self.assertEqual(
                (await db.execute(select(Mensagem))).scalars().all(), []
            )
            self.assertEqual(
                (await db.execute(select(ConsumoLLM))).scalars().all(), []
            )
            self.assertEqual(
                (
                    await db.execute(select(LLMUsageReconciliation))
                ).scalars().all(),
                [],
            )

    async def test_orm_metadata_matches_migrated_delivery_schema(self) -> None:
        expected_uniques = {
            "sessoes": {
                "uq_agente_telefone",
                "uq_sessoes_delivery_stream",
                "uq_sessoes_chatwoot_conversation",
            },
            "inbound_events": {
                "uq_inbound_event_provider_scope_id",
                "uq_inbound_event_mensagem",
            },
            "handoff_attempts": {
                "uq_handoff_attempt_key",
                "uq_handoff_session_generation",
                "uq_handoff_chatwoot_conversation",
                "uq_handoff_authority_identity",
            },
            "outbox_deliveries": {
                "uq_outbox_delivery_key",
                "uq_outbox_dedupe_key",
                "uq_outbox_stream_sequence",
                "uq_outbox_turn_revision_effect",
            },
            "llm_usage_reconciliation": {
                "uq_usage_reconciliation_key",
                "uq_usage_reconciliation_generation",
            },
        }
        expected_fks = {
            "inbound_events": {
                "fk_inbound_event_agente",
                "fk_inbound_event_sessao",
                "fk_inbound_event_mensagem",
            },
            "handoff_attempts": {
                "fk_handoff_attempt_agente",
                "fk_handoff_attempt_sessao",
            },
            "outbox_deliveries": {
                "fk_outbox_agente",
                "fk_outbox_sessao",
                "fk_outbox_handoff",
                "fk_outbox_inbound",
                "fk_outbox_dependency",
                "fk_outbox_conversation_turn",
                "fk_outbox_turn_revision",
                "fk_outbox_turn_authority",
                "fk_outbox_revision_authority",
                "fk_outbox_handoff_authority",
            },
            "llm_usage_reconciliation": {
                "fk_usage_reconciliation_agente",
                "fk_usage_reconciliation_sessao",
                "fk_usage_reconciliation_consumo",
            },
            "delivery_operator_actions": {"fk_operator_action_delivery"},
        }

        def inspect_live(sync_connection):
            inspector = inspect(sync_connection)
            return {
                "uniques": {
                    table: {
                        item["name"]
                        for item in inspector.get_unique_constraints(table)
                    }
                    for table in expected_uniques
                },
                "fks": {
                    table: {
                        item["name"] for item in inspector.get_foreign_keys(table)
                    }
                    for table in expected_fks
                },
            }

        async with self.engine.connect() as connection:
            live = await connection.run_sync(inspect_live)
            raw_precision_rows = (
                await connection.execute(
                    text(
                        "SELECT table_name, column_name, data_type, "
                        "datetime_precision, collation_name, "
                        "character_maximum_length, extra "
                        "FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() AND ("
                        "(data_type = 'datetime' AND table_name IN ("
                        "'inbound_events','handoff_attempts','outbox_deliveries',"
                        "'llm_usage_reconciliation','delivery_worker_heartbeats',"
                        "'delivery_operator_actions','delivery_runtime_state')) OR "
                        "(data_type = 'char' AND table_name IN ("
                        "'sessoes','inbound_events','handoff_attempts',"
                        "'outbox_deliveries','llm_usage_reconciliation',"
                        "'delivery_worker_heartbeats','delivery_runtime_state'))"
                        ")"
                    )
                )
            ).all()
            precision_rows = [
                {
                    "table_name": row[0],
                    "column_name": row[1],
                    "data_type": row[2],
                    "datetime_precision": row[3],
                    "collation_name": row[4],
                    "character_maximum_length": row[5],
                    "extra": row[6],
                }
                for row in raw_precision_rows
            ]

        for table, names in expected_uniques.items():
            model_names = {
                constraint.name
                for constraint in Base.metadata.tables[table].constraints
                if isinstance(constraint, UniqueConstraint)
            }
            self.assertEqual(model_names, names)
            self.assertEqual(live["uniques"][table], names)
        for table, names in expected_fks.items():
            model_names = {
                constraint.name
                for constraint in Base.metadata.tables[table].constraints
                if isinstance(constraint, ForeignKeyConstraint)
            }
            self.assertEqual(model_names, names)
            self.assertEqual(live["fks"][table], names)

        delivery_stream = next(
            row
            for row in precision_rows
            if row["table_name"] == "sessoes"
            and row["column_name"] == "delivery_stream_key"
        )
        self.assertEqual(delivery_stream["data_type"], "char")
        self.assertEqual(delivery_stream["collation_name"], "ascii_bin")
        for row in precision_rows:
            model_column = Base.metadata.tables[row["table_name"]].c[
                row["column_name"]
            ]
            if row["data_type"] == "datetime":
                self.assertEqual(
                    row["datetime_precision"],
                    6,
                    f"{row['table_name']}.{row['column_name']}",
                )
                self.assertEqual(
                    getattr(model_column.type, "fsp", None),
                    6,
                    f"ORM {row['table_name']}.{row['column_name']}",
                )
            if row["data_type"] == "char":
                self.assertEqual(model_column.type.__class__.__name__, "CHAR")
                self.assertEqual(
                    model_column.type.length,
                    row["character_maximum_length"],
                )
                self.assertEqual(model_column.type.collation, "ascii_bin")
            if row["column_name"] == "updated_at":
                self.assertIn("on update CURRENT_TIMESTAMP(6)", row["extra"])
                self.assertIn(
                    "ON UPDATE CURRENT_TIMESTAMP(6)",
                    str(model_column.server_default.arg),
                )


class UnitOfWorkTests(unittest.IsolatedAsyncioTestCase):
    async def test_commit_failure_rolls_back_and_is_not_marked_committed(self) -> None:
        from src.database.unit_of_work import UnitOfWork

        session = Mock()
        session.commit = AsyncMock(side_effect=RuntimeError("commit failed"))
        session.rollback = AsyncMock()
        session.close = AsyncMock()
        unit_of_work = UnitOfWork(session_factory=lambda: session)

        with self.assertRaises(RuntimeError):
            async with unit_of_work:
                await unit_of_work.commit()

        self.assertFalse(unit_of_work.committed)
        session.rollback.assert_awaited_once()
        session.close.assert_awaited_once()
