import asyncio
import os
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import main
from src.config import settings
from src.database.models import (
    Agente,
    ConversationTurn,
    DeliveryOperatorAction,
    DeliveryWorkerHeartbeat,
    InboundEvent,
    Mensagem,
    Sessao,
    TurnInputMembership,
    TurnRevision,
)
from src.database.repositories import delivery_worker_repo, inbound_event_repo
from src.database.unit_of_work import UnitOfWork
from src.services import evolution_orchestrator


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")


class _FailingCommitUnitOfWork(UnitOfWork):
    async def commit(self) -> None:
        await self.session.rollback()
        self._finished = True
        raise RuntimeError("synthetic commit failure")


class SchedulerIntakeMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        self.delivery_epoch = str(uuid4())
        self.scheduler_epoch = str(uuid4())
        self.instance_name = f"intake-{uuid4()}"
        self.bearer = "synthetic-ticket03-bearer"
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
                    "cutover_epoch=:epoch, version=version+1, changed_by='ticket03', "
                    "change_reason='disposable intake test' WHERE id=1"
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
                    "changed_by='ticket03', change_reason='disposable intake test' "
                    "WHERE id=1"
                ),
                {"epoch": self.scheduler_epoch},
            )
        async with self.sessions() as db:
            agente = Agente(
                nome="Ticket03 intake",
                instance_name=self.instance_name,
                system_prompt="synthetic",
                numero_notificacao="0",
                modelo_llm="openai/gpt-4o-mini",
                max_historico=1,
                inbox_scheduler_enabled=True,
            )
            db.add(agente)
            await db.commit()
            self.agente_id = agente.id

        self.patches = ExitStack()
        self.patches.enter_context(
            patch.object(settings, "provider_webhook_auth_mode", "enforced")
        )
        self.patches.enter_context(
            patch.object(settings, "evolution_webhook_bearer_token", self.bearer)
        )
        self.patches.enter_context(patch.object(settings, "delivery_mode", "outbox"))
        self.patches.enter_context(
            patch.object(settings, "delivery_cutover_epoch", self.delivery_epoch)
        )
        self.patches.enter_context(
            patch.object(settings, "inbox_scheduler_enabled", True)
        )
        self.patches.enter_context(
            patch.object(settings, "inbox_scheduler_epoch", self.scheduler_epoch)
        )
        self.patches.enter_context(
            patch.object(settings, "inbound_receipt_hmac_current_version", 1)
        )
        self.patches.enter_context(
            patch.object(
                settings,
                "inbound_receipt_hmac_keys",
                {1: SecretStr("synthetic-ticket03-receipt-key")},
            )
        )
        self.llm = self.patches.enter_context(
            patch(
                "src.services.evolution_orchestrator.processar",
                AsyncMock(side_effect=AssertionError("scheduled intake called the LLM")),
            )
        )
        self.evolution_send = self.patches.enter_context(
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                AsyncMock(side_effect=AssertionError("scheduled intake called Evolution")),
            )
        )

        async def uow_override():
            async with UnitOfWork(self.sessions) as uow:
                yield uow

        main.app.dependency_overrides[main.get_unit_of_work] = uow_override
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app),
            base_url="http://ticket03.test",
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        main.app.dependency_overrides.clear()
        self.patches.close()
        await self.engine.dispose()

    def _payload(
        self,
        message_id: str,
        content: str,
        *,
        phone: str = "5511999999003",
    ) -> dict[str, object]:
        return {
            "event": "messages.upsert",
            "instance": self.instance_name,
            "data": {
                "key": {
                    "id": message_id,
                    "fromMe": False,
                    "remoteJid": f"{phone}@s.whatsapp.net",
                },
                "message": {"conversation": content},
            },
        }

    async def _post(self, message_id: str, content: str, **kwargs: object) -> httpx.Response:
        return await self.client.post(
            "/webhook",
            headers={"Authorization": f"Bearer {self.bearer}"},
            json=self._payload(message_id, content, **kwargs),
        )

    async def _counts(self) -> tuple[int, int, int, int]:
        async with self.sessions() as db:
            values = []
            for model in (InboundEvent, Mensagem, TurnInputMembership, ConversationTurn):
                values.append(int((await db.execute(select(func.count(model.id)))).scalar_one()))
            return tuple(values)

    async def test_signed_route_concurrent_duplicate_and_changed_payload(self) -> None:
        first, duplicate = await asyncio.gather(
            self._post("same-id", "same"),
            self._post("same-id", "same"),
        )
        self.assertEqual(sorted((first.status_code, duplicate.status_code)), [200, 202])
        self.assertEqual({first.json()["status"], duplicate.json()["status"]}, {"accepted", "already_accepted"})
        changed = await self._post("same-id", "changed")
        self.assertEqual(changed.status_code, 409)
        self.assertEqual(await self._counts(), (1, 1, 1, 1))
        self.llm.assert_not_awaited()
        self.evolution_send.assert_not_awaited()

    async def test_distinct_equal_text_events_have_immutable_distinct_sequences(self) -> None:
        self.assertEqual((await self._post("equal-1", "same text")).status_code, 202)
        self.assertEqual((await self._post("equal-2", "same text")).status_code, 202)
        async with self.sessions() as db:
            messages = list((await db.execute(select(Mensagem).order_by(Mensagem.id))).scalars())
            memberships = list(
                (await db.execute(select(TurnInputMembership).order_by(TurnInputMembership.input_sequence))).scalars()
            )
            session = (await db.execute(select(Sessao))).scalar_one()
        self.assertEqual([row.conteudo for row in messages], ["same text", "same text"])
        self.assertEqual([row.input_sequence for row in memberships], [1, 2])
        self.assertEqual(len({row.inbound_event_id for row in memberships}), 2)
        self.assertEqual(session.total_mensagens, 0)

    async def test_replay_does_not_change_timer_or_capacity_at_the_limit(self) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE scheduler_runtime_state "
                    "SET max_pending_inputs_per_session=1 WHERE id=1"
                )
            )
            await db.commit()
        self.assertEqual((await self._post("capacity-1", "accepted")).status_code, 202)
        async with self.sessions() as db:
            before = (await db.execute(select(ConversationTurn.ready_at))).scalar_one()
        replay = await self._post("capacity-1", "accepted")
        self.assertEqual(replay.status_code, 200)
        async with self.sessions() as db:
            after = (await db.execute(select(ConversationTurn.ready_at))).scalar_one()
        self.assertEqual(before, after)
        rejected = await self._post("capacity-2", "rejected")
        self.assertEqual(rejected.status_code, 503)
        self.assertEqual(await self._counts(), (1, 1, 1, 1))

    async def test_collection_uses_two_second_silence_and_five_second_cap(self) -> None:
        self.assertEqual((await self._post("timer-1", "first")).status_code, 202)
        async with self.sessions() as db:
            membership = (await db.execute(select(TurnInputMembership))).scalar_one()
            first_at = (datetime.utcnow() - timedelta(seconds=4)).replace(microsecond=123456)
            await db.execute(
                text(
                    "UPDATE turn_input_memberships SET created_at=:created "
                    "WHERE id=:membership_id"
                ),
                {"created": first_at, "membership_id": membership.id},
            )
            await db.commit()
        self.assertEqual((await self._post("timer-2", "second")).status_code, 202)
        async with self.sessions() as db:
            turn = (await db.execute(select(ConversationTurn))).scalar_one()
        self.assertEqual(turn.ready_at, first_at + timedelta(seconds=5))

    async def test_entire_batch_is_stored_beyond_max_history(self) -> None:
        for index in range(4):
            self.assertEqual(
                (await self._post(f"batch-{index}", f"message-{index}")).status_code,
                202,
            )
        async with self.sessions() as db:
            messages = list((await db.execute(select(Mensagem).order_by(Mensagem.id))).scalars())
            memberships = list((await db.execute(select(TurnInputMembership))).scalars())
        self.assertEqual(len(messages), 4)
        self.assertEqual(len(memberships), 4)
        self.assertEqual([row.conteudo for row in messages], [f"message-{i}" for i in range(4)])

    async def test_commit_failure_rolls_back_and_returns_non_2xx(self) -> None:
        async def failing_override():
            async with _FailingCommitUnitOfWork(self.sessions) as uow:
                yield uow

        main.app.dependency_overrides[main.get_unit_of_work] = failing_override
        response = await self._post("commit-failure", "must roll back")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(await self._counts(), (0, 0, 0, 0))

    async def test_accepted_intake_survives_engine_restart(self) -> None:
        self.assertEqual((await self._post("restart", "durable")).status_code, 202)
        await self.client.aclose()
        await self.engine.dispose()
        restarted_engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        restarted_sessions = async_sessionmaker(restarted_engine, class_=AsyncSession)
        try:
            async with restarted_sessions() as db:
                receipt_count = (await db.execute(select(func.count(InboundEvent.id)))).scalar_one()
                membership_count = (await db.execute(select(func.count(TurnInputMembership.id)))).scalar_one()
                content = (await db.execute(select(Mensagem.conteudo))).scalar_one()
            self.assertEqual((receipt_count, membership_count, content), (1, 1, "durable"))
        finally:
            await restarted_engine.dispose()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app),
            base_url="http://ticket03.test",
        )

    async def test_sealed_refresh_routes_later_input_to_one_successor(self) -> None:
        await self._post("active-1", "one")
        await self._post("active-2", "two")
        async with self.sessions() as db:
            turn = (await db.execute(select(ConversationTurn))).scalar_one()
            revision_one = (await db.execute(select(TurnRevision))).scalar_one()
            revision_two = TurnRevision(
                turn_id=turn.id,
                scheduler_epoch=self.scheduler_epoch,
                revision_number=2,
                refresh_ordinal=1,
                replacement_of_revision_id=revision_one.id,
                status="final_candidate",
                sealed_first_input_sequence=1,
                sealed_last_input_sequence=2,
                sealed_input_count=2,
                sealed_input_bytes=6,
                sealed_at=datetime.utcnow(),
            )
            db.add(revision_two)
            await db.flush()
            turn.current_revision_id = revision_two.id
            turn.refresh_count = 1
            await db.commit()
            sealed = (revision_two.sealed_first_input_sequence, revision_two.sealed_last_input_sequence)

        await self._post("late-1", "three")
        await self._post("late-2", "four")
        async with self.sessions() as db:
            turns = list((await db.execute(select(ConversationTurn).order_by(ConversationTurn.id))).scalars())
            memberships = list(
                (await db.execute(select(TurnInputMembership).order_by(TurnInputMembership.input_sequence))).scalars()
            )
            revision_two = await db.get(TurnRevision, revision_two.id)
        self.assertEqual([(row.session_slot, row.status) for row in turns], [("active", "pending"), ("successor", "pending")])
        self.assertEqual([row.turn_id for row in memberships[:2]], [turns[0].id, turns[0].id])
        self.assertEqual([row.turn_id for row in memberships[2:]], [turns[1].id, turns[1].id])
        self.assertEqual((revision_two.sealed_first_input_sequence, revision_two.sealed_last_input_sequence), sealed)

    async def test_incomplete_historical_membership_fails_closed(self) -> None:
        payload = self._payload("incomplete", "orphan")
        parsed = evolution_orchestrator.parse_event(payload)
        self.assertIsInstance(parsed, evolution_orchestrator.EvolutionMessageEvent)
        async with self.sessions() as db:
            session = Sessao(agente_id=self.agente_id, telefone=parsed.telefone)
            db.add(session)
            await db.flush()
            keyring = inbound_event_repo.ReceiptHMACKeyRing(
                current_version=1,
                keys={1: b"synthetic-ticket03-receipt-key"},
            )
            db.add(
                InboundEvent(
                    provider="evolution",
                    source_scope=self.instance_name,
                    provider_event_id="incomplete",
                    event_type="messages.upsert",
                    payload_hmac=keyring.digest(parsed.actionable_fields),
                    hmac_key_version=1,
                    agente_id=self.agente_id,
                    sessao_id=session.id,
                    processed_at=datetime.utcnow(),
                )
            )
            await db.commit()
        response = await self._post("incomplete", "orphan")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(await self._counts(), (1, 0, 0, 0))

    async def test_replay_rejects_disposition_that_contradicts_message_evidence(self) -> None:
        payload = self._payload("corrupted-disposition", "durable evidence")
        parsed = evolution_orchestrator.parse_event(payload)
        self.assertIsInstance(parsed, evolution_orchestrator.EvolutionMessageEvent)
        async with self.sessions() as db:
            session = Sessao(agente_id=self.agente_id, telefone=parsed.telefone)
            db.add(session)
            await db.flush()
            message = Mensagem(
                sessao_id=session.id,
                role="user",
                conteudo="durable evidence",
            )
            db.add(message)
            await db.flush()
            keyring = inbound_event_repo.ReceiptHMACKeyRing(
                current_version=1,
                keys={1: b"synthetic-ticket03-receipt-key"},
            )
            db.add(
                InboundEvent(
                    provider="evolution",
                    source_scope=self.instance_name,
                    provider_event_id="corrupted-disposition",
                    event_type="messages.upsert",
                    payload_hmac=keyring.digest(parsed.actionable_fields),
                    hmac_key_version=1,
                    agente_id=self.agente_id,
                    sessao_id=session.id,
                    mensagem_id=message.id,
                    receipt_disposition="ignored_nonbot",
                    processed_at=datetime.utcnow(),
                )
            )
            await db.commit()

        response = await self._post("corrupted-disposition", "durable evidence")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(await self._counts(), (1, 1, 0, 0))

    async def test_replay_rejects_disposition_that_contradicts_membership_evidence(self) -> None:
        payload = self._payload("corrupted-membership-disposition", "membership evidence")
        parsed = evolution_orchestrator.parse_event(payload)
        self.assertIsInstance(parsed, evolution_orchestrator.EvolutionMessageEvent)
        async with self.sessions() as db:
            session = Sessao(agente_id=self.agente_id, telefone=parsed.telefone)
            db.add(session)
            await db.flush()
            message = Mensagem(
                sessao_id=session.id,
                role="user",
                conteudo="membership evidence",
            )
            db.add(message)
            await db.flush()
            keyring = inbound_event_repo.ReceiptHMACKeyRing(
                current_version=1,
                keys={1: b"synthetic-ticket03-receipt-key"},
            )
            receipt = InboundEvent(
                provider="evolution",
                source_scope=self.instance_name,
                provider_event_id="corrupted-membership-disposition",
                event_type="messages.upsert",
                payload_hmac=keyring.digest(parsed.actionable_fields),
                hmac_key_version=1,
                agente_id=self.agente_id,
                sessao_id=session.id,
                receipt_disposition="ignored_nonbot",
                processed_at=datetime.utcnow(),
            )
            db.add(receipt)
            turn = ConversationTurn(
                turn_key=str(uuid4()),
                agente_id=self.agente_id,
                sessao_id=session.id,
                scheduler_epoch=self.scheduler_epoch,
                arrival_sequence=1,
                scheduling_sequence=1,
                ready_at=datetime.utcnow(),
            )
            db.add(turn)
            await db.flush()
            revision = TurnRevision(
                turn_id=turn.id,
                scheduler_epoch=self.scheduler_epoch,
                revision_number=1,
                refresh_ordinal=0,
            )
            db.add(revision)
            await db.flush()
            turn.current_revision_id = revision.id
            db.add(
                TurnInputMembership(
                    turn_id=turn.id,
                    sessao_id=session.id,
                    mensagem_id=message.id,
                    inbound_event_id=receipt.id,
                    input_sequence=1,
                    content_bytes=len(b"membership evidence"),
                )
            )
            await db.commit()

        response = await self._post(
            "corrupted-membership-disposition",
            "membership evidence",
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(await self._counts(), (1, 1, 1, 1))

    async def test_incomplete_scheduled_replay_fails_closed_after_handler_change(self) -> None:
        for index, handler in enumerate(("humano", "encerrada"), start=1):
            phone = f"55119999991{index:02d}"
            message_id = f"incomplete-{handler}"
            accepted = await self._post(
                message_id,
                f"accepted before {handler}",
                phone=phone,
            )
            self.assertEqual(accepted.status_code, 202)
            async with self.sessions() as db:
                session = (
                    await db.execute(select(Sessao).where(Sessao.telefone == phone))
                ).scalar_one()
                receipt = (
                    await db.execute(
                        select(InboundEvent).where(
                            InboundEvent.provider_event_id == message_id
                        )
                    )
                ).scalar_one()
                await db.execute(
                    text(
                        "DELETE FROM turn_input_memberships "
                        "WHERE inbound_event_id=:receipt_id"
                    ),
                    {"receipt_id": receipt.id},
                )
                session.handler = handler
                await db.commit()

            replay = await self._post(
                message_id,
                f"accepted before {handler}",
                phone=phone,
            )
            self.assertEqual(replay.status_code, 503, handler)

    async def test_scheduled_replay_missing_only_receipt_link_fails_closed(self) -> None:
        message_id = "missing-receipt-link"
        self.assertEqual(
            (await self._post(message_id, "durable scheduled outcome")).status_code,
            202,
        )
        async with self.sessions() as db:
            receipt = (
                await db.execute(
                    select(InboundEvent).where(
                        InboundEvent.provider_event_id == message_id
                    )
                )
            ).scalar_one()
            self.assertEqual(receipt.receipt_disposition, "scheduled")
            with self.assertRaisesRegex(DBAPIError, 'inbound receipt disposition is immutable'):
                await db.execute(
                    text(
                        "UPDATE inbound_events "
                        "SET receipt_disposition='ignored_nonbot' WHERE id=:receipt_id"
                    ),
                    {"receipt_id": receipt.id},
                )
                await db.commit()
            await db.rollback()
        async with self.sessions() as db:
            receipt = (
                await db.execute(
                    select(InboundEvent).where(
                        InboundEvent.provider_event_id == message_id
                    )
                )
            ).scalar_one()
            session = await db.get(Sessao, receipt.sessao_id)
            await db.execute(
                text(
                    "UPDATE inbound_events SET mensagem_id=NULL WHERE id=:receipt_id"
                ),
                {"receipt_id": receipt.id},
            )
            session.handler = "encerrada"
            await db.commit()
            receipt_id = receipt.id

        replay = await self._post(message_id, "durable scheduled outcome")
        self.assertEqual(replay.status_code, 503)
        async with self.sessions() as db:
            receipt = await db.get(InboundEvent, receipt_id)
            memberships = int(
                (
                    await db.execute(
                        select(func.count(TurnInputMembership.id)).where(
                            TurnInputMembership.inbound_event_id == receipt_id
                        )
                    )
                ).scalar_one()
            )
        self.assertEqual(receipt.receipt_disposition, "scheduled")
        self.assertEqual(memberships, 1)

    async def test_scheduled_replay_missing_all_is_audit_retained_and_fails_closed(self) -> None:
        message_id = "missing-all-scheduled-identity"
        self.assertEqual(
            (await self._post(message_id, "never infer ignored")).status_code,
            202,
        )
        old = datetime.utcnow() - timedelta(days=91)
        async with self.sessions() as db:
            receipt = (
                await db.execute(
                    select(InboundEvent).where(
                        InboundEvent.provider_event_id == message_id
                    )
                )
            ).scalar_one()
            session = await db.get(Sessao, receipt.sessao_id)
            await db.execute(
                text(
                    "DELETE FROM turn_input_memberships "
                    "WHERE inbound_event_id=:receipt_id"
                ),
                {"receipt_id": receipt.id},
            )
            await db.execute(
                text(
                    "UPDATE inbound_events SET mensagem_id=NULL, received_at=:old "
                    "WHERE id=:receipt_id"
                ),
                {"receipt_id": receipt.id, "old": old},
            )
            session.handler = "humano"
            await db.commit()
            receipt_id = receipt.id

        replay = await self._post(message_id, "never infer ignored")
        self.assertEqual(replay.status_code, 503)
        async with self.sessions() as db:
            cleanup = await delivery_worker_repo.cleanup_retention(db)
            await db.commit()
        self.assertEqual(cleanup.receipts_purged, 0)
        async with self.sessions() as db:
            receipt = await db.get(InboundEvent, receipt_id)
        self.assertIsNotNone(receipt)
        self.assertEqual(receipt.receipt_disposition, "scheduled")

    async def test_intact_and_legitimately_ignored_nonbot_replays_remain_idempotent(self) -> None:
        for index, handler in enumerate(("humano", "encerrada"), start=1):
            intact_phone = f"55119999992{index:02d}"
            intact_id = f"intact-{handler}"
            self.assertEqual(
                (
                    await self._post(
                        intact_id,
                        "intact scheduled input",
                        phone=intact_phone,
                    )
                ).status_code,
                202,
            )
            async with self.sessions() as db:
                intact_session = (
                    await db.execute(
                        select(Sessao).where(Sessao.telefone == intact_phone)
                    )
                ).scalar_one()
                intact_session.handler = handler
                await db.commit()
            intact_replay = await self._post(
                intact_id,
                "intact scheduled input",
                phone=intact_phone,
            )
            self.assertEqual(intact_replay.status_code, 200, handler)

            ignored_phone = f"55119999993{index:02d}"
            async with self.sessions() as db:
                ignored_session = Sessao(
                    agente_id=self.agente_id,
                    telefone=ignored_phone,
                    handler=handler,
                )
                db.add(ignored_session)
                await db.commit()
            ignored_id = f"ignored-{handler}"
            ignored = await self._post(
                ignored_id,
                "legitimately ignored",
                phone=ignored_phone,
            )
            self.assertEqual(ignored.status_code, 200, handler)
            ignored_replay = await self._post(
                ignored_id,
                "legitimately ignored",
                phone=ignored_phone,
            )
            self.assertEqual(ignored_replay.status_code, 200, handler)
            self.assertEqual(ignored_replay.json()["status"], "already_accepted")

    async def test_retention_skips_scheduled_receipt_and_purges_unrelated_rows(self) -> None:
        self.assertEqual(
            (await self._post("retention-linked", "retained work")).status_code,
            202,
        )
        old = datetime.utcnow() - timedelta(days=91)
        old_worker = str(uuid4())
        async with self.sessions() as db:
            protected = (
                await db.execute(
                    select(InboundEvent).where(
                        InboundEvent.provider_event_id == "retention-linked"
                    )
                )
            ).scalar_one()
            protected.received_at = old
            db.add(
                InboundEvent(
                    provider="evolution",
                    source_scope=self.instance_name,
                    provider_event_id="retention-unlinked",
                    event_type="messages.upsert",
                    payload_hmac="f" * 64,
                    hmac_key_version=1,
                    receipt_disposition="legacy_evolution",
                    received_at=old,
                    processed_at=old,
                )
            )
            db.add(
                DeliveryWorkerHeartbeat(
                    worker_id=old_worker,
                    code_version="ticket03",
                    started_at=old,
                    last_seen_at=old,
                    current_lease_count=0,
                    dispatch_enabled=False,
                    configured_mode="direct",
                    provider_ready=False,
                    runtime_compatible=False,
                )
            )
            db.add(
                DeliveryOperatorAction(
                    delivery_id=None,
                    action="retention-probe",
                    actor="ticket03",
                    reason="unrelated maintenance must commit",
                    created_at=old,
                )
            )
            await db.commit()
            protected_id = protected.id

        async def cleanup_once():
            async with self.sessions() as db:
                result = await delivery_worker_repo.cleanup_retention(db)
                await db.commit()
                return result

        first, second = await asyncio.gather(cleanup_once(), cleanup_once())
        self.assertEqual(first.receipts_purged + second.receipts_purged, 1)
        self.assertEqual(first.heartbeats_purged + second.heartbeats_purged, 1)
        self.assertEqual(first.audits_purged + second.audits_purged, 1)
        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(InboundEvent, protected_id))
            self.assertEqual(
                int(
                    (
                        await db.execute(
                            select(func.count(TurnInputMembership.id)).where(
                                TurnInputMembership.inbound_event_id == protected_id
                            )
                        )
                    ).scalar_one()
                ),
                1,
            )
            self.assertIsNone(await db.get(DeliveryWorkerHeartbeat, old_worker))
            self.assertEqual(
                int(
                    (
                        await db.execute(
                            select(func.count(InboundEvent.id)).where(
                                InboundEvent.provider_event_id == "retention-unlinked"
                            )
                        )
                    ).scalar_one()
                ),
                0,
            )

    async def test_gate_mismatch_fails_closed_without_receipt(self) -> None:
        with patch.object(settings, "inbox_scheduler_epoch", str(uuid4())):
            mismatch = await self._post("wrong-epoch", "no admission")
        self.assertEqual(mismatch.status_code, 503)
        self.assertEqual(await self._counts(), (0, 0, 0, 0))
        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_runtime_state SET state='draining' WHERE id=1"))
            await db.commit()
        draining = await self._post("draining", "no admission")
        self.assertEqual(draining.status_code, 503)
        self.assertEqual(await self._counts(), (0, 0, 0, 0))

    async def test_concurrent_agent_opt_in_never_falls_back_to_sync(self) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE agentes SET inbox_scheduler_enabled=FALSE "
                    "WHERE id=:agente_id"
                ),
                {"agente_id": self.agente_id},
            )
            await db.commit()

        original_lookup = evolution_orchestrator.agente_repo.get_by_instance_name
        lookup_count = 0

        async def flip_after_preliminary_read(*args: object, **kwargs: object):
            nonlocal lookup_count
            row = await original_lookup(*args, **kwargs)
            lookup_count += 1
            if lookup_count == 1:
                async with self.sessions() as db:
                    await db.execute(
                        text(
                            "UPDATE agentes SET inbox_scheduler_enabled=TRUE "
                            "WHERE id=:agente_id"
                        ),
                        {"agente_id": self.agente_id},
                    )
                    await db.commit()
            return row

        with patch.object(
            evolution_orchestrator.agente_repo,
            "get_by_instance_name",
            side_effect=flip_after_preliminary_read,
        ):
            response = await self._post("concurrent-opt-in", "no sync fallback")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(await self._counts(), (0, 0, 0, 0))
        self.llm.assert_not_awaited()
        self.evolution_send.assert_not_awaited()

    async def _post_concurrently_behind_scheduler_mutex(
        self,
        posts: list[tuple[str, str, str]],
    ) -> list[httpx.Response]:
        """Hold the scheduler mutex until every webhook has passed its gate read.

        Ordering is forced by a hook, not by lock-wait timing: the preliminary
        gate read of every request completes before the holder commits.
        """

        original = evolution_orchestrator.scheduler_repo.lock_authority_prefix
        entered = 0
        all_entered = asyncio.Event()

        async def counting_lock_authority_prefix(*args: object, **kwargs: object):
            nonlocal entered
            entered += 1
            if entered == len(posts):
                all_entered.set()
            return await original(*args, **kwargs)

        async with self.engine.connect() as holder:
            transaction = await holder.begin()
            try:
                await holder.execute(
                    text("SELECT id FROM scheduler_runtime_state WHERE id=1 FOR UPDATE")
                )
                with patch.object(
                    evolution_orchestrator.scheduler_repo,
                    "lock_authority_prefix",
                    side_effect=counting_lock_authority_prefix,
                ):
                    tasks = [
                        asyncio.create_task(self._post(message_id, content, phone=phone))
                        for message_id, content, phone in posts
                    ]
                    await asyncio.wait_for(all_entered.wait(), timeout=10)
                    await asyncio.sleep(0.2)
                    await transaction.commit()
                    return list(await asyncio.gather(*tasks))
            finally:
                if transaction.is_active:
                    await transaction.rollback()

    async def test_distinct_concurrent_same_session_intake_reads_after_mutex(self) -> None:
        responses = await self._post_concurrently_behind_scheduler_mutex(
            [
                ("mutex-distinct-1", "first", "5511999999003"),
                ("mutex-distinct-2", "second", "5511999999003"),
            ]
        )

        self.assertEqual(
            [response.status_code for response in responses],
            [202, 202],
            [response.text for response in responses],
        )
        async with self.sessions() as db:
            memberships = list(
                (
                    await db.execute(
                        select(TurnInputMembership).order_by(
                            TurnInputMembership.input_sequence
                        )
                    )
                ).scalars()
            )
            receipt_count = int(
                (await db.execute(select(func.count(InboundEvent.id)))).scalar_one()
            )
            turns = list((await db.execute(select(ConversationTurn))).scalars())
        self.assertEqual([row.input_sequence for row in memberships], [1, 2])
        self.assertEqual(len({row.inbound_event_id for row in memberships}), 2)
        self.assertEqual(receipt_count, 2)
        self.assertEqual(len(turns), 1)
        self.assertEqual(turns[0].input_version, 2)
        self.llm.assert_not_awaited()
        self.evolution_send.assert_not_awaited()

    async def test_concurrent_intake_caps_count_inputs_committed_during_mutex_wait(
        self,
    ) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE scheduler_runtime_state "
                    "SET max_queued_turns_per_agent=1 WHERE id=1"
                )
            )
            await db.commit()

        responses = await self._post_concurrently_behind_scheduler_mutex(
            [
                ("mutex-cap-1", "first lead", "5511999999003"),
                ("mutex-cap-2", "second lead", "5511999999004"),
            ]
        )

        self.assertEqual(
            sorted(response.status_code for response in responses),
            [202, 503],
            [response.text for response in responses],
        )
        self.assertEqual(await self._counts(), (1, 1, 1, 1))
        self.llm.assert_not_awaited()
        self.evolution_send.assert_not_awaited()

    async def test_default_off_soft_limit_reads_spend_committed_during_session_wait(
        self,
    ) -> None:
        telefone = "5511999999003"
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='direct', "
                    "version=version+1, changed_by='ticket03', "
                    "change_reason='disposable default-off test' WHERE id=1"
                )
            )
            await db.execute(
                text("UPDATE scheduler_runtime_state SET state='disabled' WHERE id=1")
            )
            await db.execute(
                text(
                    "UPDATE agentes SET inbox_scheduler_enabled=FALSE, "
                    "limite_mensal_creditos=1.000000 WHERE id=:agente_id"
                ),
                {"agente_id": self.agente_id},
            )
            await db.execute(
                text(
                    "INSERT INTO sessoes (agente_id,telefone,delivery_stream_key) "
                    "VALUES (:agente_id,:telefone,UUID())"
                ),
                {"agente_id": self.agente_id, "telefone": telefone},
            )
            sessao_id = int(
                (
                    await db.execute(
                        select(Sessao.id).where(
                            Sessao.agente_id == self.agente_id,
                            Sessao.telefone == telefone,
                        )
                    )
                ).scalar_one()
            )
            await db.execute(
                text(
                    "INSERT INTO consumo_llm (agente_id,sessao_id,modelo_llm,"
                    "tipo_chamada,tokens_input,tokens_output,custo_creditos) "
                    "VALUES (:agente_id,:sessao_id,'openai/gpt-4o-mini',"
                    "'resposta',1,1,0.50000000)"
                ),
                {"agente_id": self.agente_id, "sessao_id": sessao_id},
            )
            await db.commit()

        self.llm.side_effect = None
        self.llm.return_value = ("synthetic answer", False)
        self.evolution_send.side_effect = None
        self.evolution_send.return_value = None

        original = evolution_orchestrator.sessao_repo.get_or_create_for_update
        reached_session_lock = asyncio.Event()

        async def signal_before_session_lock(*args: object, **kwargs: object):
            reached_session_lock.set()
            return await original(*args, **kwargs)

        with ExitStack() as stack:
            stack.enter_context(patch.object(settings, "delivery_mode", "direct"))
            stack.enter_context(patch.object(settings, "inbox_scheduler_enabled", False))
            stack.enter_context(
                patch.object(
                    evolution_orchestrator.sessao_repo,
                    "get_or_create_for_update",
                    side_effect=signal_before_session_lock,
                )
            )
            async with self.engine.connect() as holder:
                transaction = await holder.begin()
                try:
                    await holder.execute(
                        text("SELECT id FROM sessoes WHERE id=:sessao_id FOR UPDATE"),
                        {"sessao_id": sessao_id},
                    )
                    task = asyncio.create_task(self._post("soft-limit-wait", "hello"))
                    await asyncio.wait_for(reached_session_lock.wait(), timeout=10)
                    await asyncio.sleep(0.2)
                    await holder.execute(
                        text(
                            "INSERT INTO consumo_llm (agente_id,sessao_id,modelo_llm,"
                            "tipo_chamada,tokens_input,tokens_output,custo_creditos) "
                            "VALUES (:agente_id,:sessao_id,'openai/gpt-4o-mini',"
                            "'resposta',1,1,0.60000000)"
                        ),
                        {"agente_id": self.agente_id, "sessao_id": sessao_id},
                    )
                    await transaction.commit()
                    response = await task
                finally:
                    if transaction.is_active:
                        await transaction.rollback()

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "accepted")
        self.assertEqual(response.json()["reason"], "temporarily_unavailable")
        self.llm.assert_not_awaited()
        self.evolution_send.assert_awaited_once()

    REPLAY_ROUNDS = 20
    REPLAY_CONCURRENCY = 6

    async def _use_legacy_outbox_path(self) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE agentes SET inbox_scheduler_enabled=FALSE "
                    "WHERE id=:agente_id"
                ),
                {"agente_id": self.agente_id},
            )
            await db.commit()
        self.patches.enter_context(
            patch.object(settings, "inbox_scheduler_enabled", False)
        )
        self.llm.side_effect = None
        self.llm.return_value = ("synthetic answer", False)

    async def _outbox_count(self) -> int:
        async with self.sessions() as db:
            return int(
                (
                    await db.execute(text("SELECT COUNT(*) FROM outbox_deliveries"))
                ).scalar_one()
            )

    def _assert_no_retryable_failure(self, responses: list[httpx.Response]) -> None:
        bodies = [response.text for response in responses]
        self.assertNotIn(503, [response.status_code for response in responses], bodies)
        self.assertFalse(any("1213" in body for body in bodies), bodies)

    async def test_legacy_outbox_concurrent_exact_replays_are_already_accepted(
        self,
    ) -> None:
        await self._use_legacy_outbox_path()
        for round_number in range(self.REPLAY_ROUNDS):
            message_id = f"legacy-replay-{round_number}"
            first = await self._post(message_id, "replayed")
            self.assertEqual(first.status_code, 200, first.text)
            self.assertEqual(first.json()["delivery_status"], "queued")
            replays = await asyncio.gather(
                *(
                    self._post(message_id, "replayed")
                    for _ in range(self.REPLAY_CONCURRENCY)
                )
            )
            self._assert_no_retryable_failure(replays)
            self.assertEqual(
                [response.json()["status"] for response in replays],
                ["already_accepted"] * self.REPLAY_CONCURRENCY,
            )
        self.assertEqual(await self._outbox_count(), self.REPLAY_ROUNDS)
        self.assertEqual(self.llm.await_count, self.REPLAY_ROUNDS)

    async def test_legacy_outbox_first_delivery_racing_duplicates_enqueues_once(
        self,
    ) -> None:
        await self._use_legacy_outbox_path()
        for round_number in range(self.REPLAY_ROUNDS):
            message_id = f"legacy-race-{round_number}"
            responses = await asyncio.gather(
                *(
                    self._post(message_id, "raced")
                    for _ in range(self.REPLAY_CONCURRENCY)
                )
            )
            self._assert_no_retryable_failure(responses)
            statuses = sorted(
                response.json().get("delivery_status") for response in responses
            )
            self.assertEqual(
                statuses,
                ["already_accepted"] * (self.REPLAY_CONCURRENCY - 1) + ["queued"],
            )
            self.assertEqual(await self._outbox_count(), round_number + 1)
        self.assertEqual(self.llm.await_count, self.REPLAY_ROUNDS)

    async def test_scheduled_concurrent_exact_replays_and_first_delivery_race(
        self,
    ) -> None:
        for round_number in range(self.REPLAY_ROUNDS):
            replayed_id = f"scheduled-replay-{round_number}"
            first = await self._post(replayed_id, f"replayed {round_number}")
            self.assertEqual(first.status_code, 202, first.text)
            replays = await asyncio.gather(
                *(
                    self._post(replayed_id, f"replayed {round_number}")
                    for _ in range(self.REPLAY_CONCURRENCY)
                )
            )
            self._assert_no_retryable_failure(replays)
            self.assertEqual(
                [response.status_code for response in replays],
                [200] * self.REPLAY_CONCURRENCY,
            )
            raced_id = f"scheduled-race-{round_number}"
            raced = await asyncio.gather(
                *(
                    self._post(raced_id, f"raced {round_number}")
                    for _ in range(self.REPLAY_CONCURRENCY)
                )
            )
            self._assert_no_retryable_failure(raced)
            self.assertEqual(
                sorted(response.status_code for response in raced),
                [200] * (self.REPLAY_CONCURRENCY - 1) + [202],
            )
        receipts, messages, memberships, _ = await self._counts()
        self.assertEqual((receipts, messages, memberships), (40, 40, 40))
        self.llm.assert_not_awaited()

    async def test_legacy_outbox_concurrent_payload_collision_returns_409(
        self,
    ) -> None:
        await self._use_legacy_outbox_path()
        for round_number in range(self.REPLAY_ROUNDS):
            message_id = f"legacy-collision-{round_number}"
            first = await self._post(message_id, "original")
            self.assertEqual(first.status_code, 200, first.text)
            half = self.REPLAY_CONCURRENCY // 2
            responses = await asyncio.gather(
                *(self._post(message_id, "original") for _ in range(half)),
                *(self._post(message_id, "changed") for _ in range(half)),
            )
            self._assert_no_retryable_failure(responses)
            self.assertEqual(
                [response.status_code for response in responses],
                [200] * half + [409] * half,
            )
            self.assertEqual(
                [response.json()["status"] for response in responses[:half]],
                ["already_accepted"] * half,
            )
        self.assertEqual(await self._outbox_count(), self.REPLAY_ROUNDS)

    async def test_chatwoot_receipt_concurrent_exact_replays_do_not_deadlock(
        self,
    ) -> None:
        keyring = inbound_event_repo.ReceiptHMACKeyRing(
            current_version=1,
            keys={1: b"synthetic-ticket03-receipt-key"},
        )

        barrier: asyncio.Barrier | None = None

        async def record(provider_event_id: str) -> bool:
            async with self.sessions() as db:
                if barrier is not None:
                    # Every duplicate holds the shared unique-record lock that a
                    # failed duplicate INSERT takes, before any recovery read.
                    await db.execute(
                        text(
                            "SELECT id FROM inbound_events WHERE provider='chatwoot' "
                            "AND source_scope='1' AND provider_event_id=:event_id "
                            "FOR SHARE"
                        ),
                        {"event_id": provider_event_id},
                    )
                    await barrier.wait()
                result = await inbound_event_repo.record(
                    db,
                    provider="chatwoot",
                    source_scope="1",
                    provider_event_id=provider_event_id,
                    event_type="message_created",
                    actionable_fields={"message_id": provider_event_id},
                    keyring=keyring,
                    receipt_disposition="chatwoot",
                )
                await db.commit()
                return result.created

        for round_number in range(self.REPLAY_ROUNDS):
            provider_event_id = f"chatwoot-replay-{round_number}"
            barrier = None
            self.assertTrue(await record(provider_event_id))
            barrier = asyncio.Barrier(self.REPLAY_CONCURRENCY)
            created = await asyncio.gather(
                *(record(provider_event_id) for _ in range(self.REPLAY_CONCURRENCY)),
                return_exceptions=True,
            )
            self.assertEqual(created, [False] * self.REPLAY_CONCURRENCY)


if __name__ == "__main__":
    unittest.main()
