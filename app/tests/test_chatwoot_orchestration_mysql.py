import asyncio
import hashlib
import hmac
import json
import os
import time
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from fastapi import HTTPException, Response
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.database.models import Agente, HandoffAttempt, InboundEvent, OutboxDelivery, Sessao
from src.database.repositories import (
    delivery_worker_repo,
    handoff_attempt_repo,
    sessao_repo,
)
from src.database.unit_of_work import UnitOfWork
from src import delivery_worker as worker_module
from src.routers import chatwoot as chatwoot_router
from src.services import chatwoot_orchestrator, handoff_manager
from src.services.delivery_adapters import DeliveryResult


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")


class ChatwootOrchestrationMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table in (
                "delivery_operator_actions",
                "outbox_deliveries",
                "handoff_attempts",
                "inbound_events",
                "mensagens",
                "sessoes",
                "agentes",
            ):
                await connection.execute(text(f"DELETE FROM {table}"))
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        self.epoch = str(uuid4())
        async with self.sessions() as db:
            agente = Agente(
                nome="Chatwoot lifecycle",
                instance_name=f"chatwoot-{uuid4()}",
                system_prompt="test",
                numero_notificacao="0",
                chatwoot_inbox_id=7,
            )
            db.add(agente)
            await db.flush()
            self.agente_id = agente.id
            self.instance_name = agente.instance_name
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='outbox', "
                    "cutover_epoch=:epoch, version=version+1"
                ),
                {"epoch": self.epoch},
            )
            await db.commit()

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def _plan_handoff(self, phone: str) -> tuple[int, int, str]:
        async with self.sessions() as db:
            async with db.begin():
                agente = await db.get(Agente, self.agente_id)
                sessao = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, phone
                )
                inbound = InboundEvent(
                    provider="evolution",
                    source_scope=self.instance_name,
                    provider_event_id=str(uuid4()),
                    event_type="messages.upsert",
                    payload_hmac="a" * 64,
                    hmac_key_version=1,
                    agente_id=self.agente_id,
                    sessao_id=sessao.id,
                    processed_at=datetime.utcnow(),
                )
                db.add(inbound)
                await db.flush()
                open_job, transition = await handoff_manager.planejar_handoff(
                    agente=agente,
                    sessao=sessao,
                    telefone_lead=phone,
                    resumo="summary",
                    inbound_event=inbound,
                    cutover_epoch=self.epoch,
                    db=db,
                    now=datetime.utcnow(),
                    setup_ttl_seconds=86400,
                )
                return sessao.id, transition.id, open_job.delivery_key

    def _settings(self):
        return (
            patch.object(chatwoot_orchestrator.settings, "delivery_mode", "outbox"),
            patch.object(
                chatwoot_orchestrator.settings,
                "delivery_cutover_epoch",
                self.epoch,
            ),
            patch.object(
                chatwoot_orchestrator.settings,
                "inbound_receipt_hmac_keys",
                {1: SecretStr("chatwoot-receipt-key")},
            ),
            patch.object(
                chatwoot_orchestrator.settings,
                "inbound_receipt_hmac_current_version",
                1,
            ),
        )

    async def _orchestrate(self, event: chatwoot_orchestrator.ChatwootReplyEvent):
        patches = self._settings()
        for item in patches:
            item.start()
        try:
            async with UnitOfWork(self.sessions) as uow:
                return await chatwoot_orchestrator.orchestrate(
                    event=event, uow=uow, authenticated=True
                )
        finally:
            for item in reversed(patches):
                item.stop()

    async def test_fast_marker_reply_completes_open_and_stays_after_transition(self) -> None:
        session_id, transition_id, marker = await self._plan_handoff("551100000070")
        async with self.sessions() as db:
            open_job = (
                await db.execute(
                    select(OutboxDelivery).where(
                        OutboxDelivery.delivery_key == marker
                    )
                )
            ).scalar_one()
            attempt = await db.get(HandoffAttempt, open_job.handoff_attempt_id)
            self.assertNotEqual(marker, attempt.handoff_key)
        event = chatwoot_orchestrator.ChatwootReplyEvent(
            account_id=1,
            remote_account_id=1,
            message_id=7001,
            conversation_id=8001,
            content="human reply",
            inbox_id=7,
            contact_id=9001,
            handoff_key=marker,
        )
        result = await self._orchestrate(event)
        self.assertEqual(result.http_status, 202)
        self.assertEqual(result.body["delivery_status"], "queued")

        async with self.sessions() as db:
            session = await db.get(Sessao, session_id)
            attempt = (
                await db.execute(
                    select(HandoffAttempt).where(
                        HandoffAttempt.sessao_id == session_id
                    )
                )
            ).scalar_one()
            transition = await db.get(OutboxDelivery, transition_id)
            replies = (
                await db.execute(
                    select(OutboxDelivery).where(
                        OutboxDelivery.inbound_event_id.in_(
                            select(InboundEvent.id).where(
                                InboundEvent.provider == "chatwoot"
                            )
                        )
                    )
                )
            ).scalars().all()
            open_job = (
                await db.execute(
                    select(OutboxDelivery).where(
                        OutboxDelivery.handoff_attempt_id == attempt.id,
                        OutboxDelivery.provider == "chatwoot",
                    )
                )
            ).scalar_one()
            self.assertEqual(attempt.status, "ready")
            self.assertEqual(attempt.chatwoot_conversation_id, 8001)
            self.assertEqual(session.chatwoot_conversation_id, 8001)
            self.assertEqual(open_job.status, "delivered")
            self.assertEqual(open_job.terminal_reason, "confirmed_by_callback")
            self.assertEqual(transition.status, "pending")
            self.assertIsNotNone(transition.expires_at)
            self.assertEqual(len(replies), 1)
            self.assertLess(transition.stream_sequence, replies[0].stream_sequence)

        replay = await self._orchestrate(event)
        self.assertEqual(replay.body["status"], "already_accepted")

    async def test_live_worker_fence_forces_callback_retry_without_receipt_commit(self) -> None:
        session_id, _, marker = await self._plan_handoff("551100000072")
        claimed = await delivery_worker_repo.claim_due_with_retry(
            self.sessions,
            available_slots=1,
            worker_id="callback-fence-worker",
            cutover_epoch=self.epoch,
            evolution_lease_seconds=90,
            chatwoot_lease_seconds=120,
            max_attempts=3,
            max_jitter_seconds=0.01,
        )
        self.assertEqual([row.delivery_key for row in claimed], [marker])
        event = chatwoot_orchestrator.ChatwootReplyEvent(
            account_id=1,
            remote_account_id=1,
            message_id=7201,
            conversation_id=8201,
            content="reply while provider call is in flight",
            inbox_id=7,
            contact_id=9201,
            handoff_key=marker,
        )
        provider_started = asyncio.Event()
        release_provider = asyncio.Event()

        async def blocked_provider(
            payload, *, delivery_key, deadline_at, reconcile_only=False
        ):
            provider_started.set()
            await release_provider.wait()
            return DeliveryResult(
                external_id="8201",
                http_status=201,
                contact_id=9201,
                conversation_id=8201,
                reconciled=False,
            )

        with (
            patch.object(worker_module, "AsyncSessionLocal", self.sessions),
            patch.object(
                worker_module,
                "open_or_reconcile_chatwoot_handoff",
                blocked_provider,
            ),
        ):
            dispatch = asyncio.create_task(
                worker_module.DeliveryWorker()._dispatch_one(claimed[0])
            )
            await asyncio.wait_for(provider_started.wait(), timeout=1)
            with self.assertRaisesRegex(
                chatwoot_orchestrator.ChatwootOwnershipError,
                "live worker",
            ):
                await self._orchestrate(event)

            async with self.sessions() as db:
                row = await db.get(OutboxDelivery, claimed[0].id)
                self.assertEqual(row.status, "processing")
                self.assertEqual(row.lease_token, claimed[0].lease_token)
                self.assertIsNotNone(row.lock_until)
                receipts = (
                    await db.execute(
                        select(InboundEvent).where(
                            InboundEvent.provider == "chatwoot",
                            InboundEvent.provider_event_id == "7201",
                        )
                    )
                ).scalars().all()
                self.assertEqual(receipts, [])
                self.assertTrue(
                    await delivery_worker_repo.renew_lease(
                        db,
                        delivery_id=claimed[0].id,
                        lease_token=claimed[0].lease_token,
                        cutover_epoch=self.epoch,
                        lease_seconds=120,
                    )
                )
                await db.commit()
            release_provider.set()
            await asyncio.wait_for(dispatch, timeout=2)

        result = await self._orchestrate(event)
        self.assertEqual(result.body["status"], "queued")
        async with self.sessions() as db:
            session = await db.get(Sessao, session_id)
            self.assertEqual(session.chatwoot_conversation_id, 8201)

    async def test_expired_worker_lease_allows_callback_takeover_and_rejects_old_token(self) -> None:
        _, transition_id, marker = await self._plan_handoff("551100000074")
        claimed = (
            await delivery_worker_repo.claim_due_with_retry(
                self.sessions,
                available_slots=1,
                worker_id="expired-callback-worker",
                cutover_epoch=self.epoch,
                evolution_lease_seconds=90,
                chatwoot_lease_seconds=120,
                max_attempts=3,
                max_jitter_seconds=0.01,
            )
        )[0]
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, claimed.id)
            row.lock_until = datetime.utcnow() - timedelta(seconds=1)
            await db.commit()

        event = chatwoot_orchestrator.ChatwootReplyEvent(
            account_id=1,
            remote_account_id=1,
            message_id=7401,
            conversation_id=8401,
            content="reply after owner expiry",
            inbox_id=7,
            contact_id=9401,
            handoff_key=marker,
        )
        result = await self._orchestrate(event)
        self.assertEqual(result.body["status"], "queued")

        async with self.sessions() as db:
            self.assertFalse(
                await delivery_worker_repo.renew_lease(
                    db,
                    delivery_id=claimed.id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=self.epoch,
                    lease_seconds=120,
                )
            )
            await db.commit()
        async with self.sessions() as db:
            self.assertFalse(
                await delivery_worker_repo.complete_chatwoot_handoff(
                    db,
                    delivery_id=claimed.id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=self.epoch,
                    contact_id=9401,
                    conversation_id=8401,
                    http_status=201,
                    transition_ttl_seconds=1800,
                )
            )
            await db.commit()

        async with self.sessions() as db:
            parent = await db.get(OutboxDelivery, claimed.id)
            transition = await db.get(OutboxDelivery, transition_id)
            replies = (
                await db.execute(
                    select(OutboxDelivery).where(
                        OutboxDelivery.dedupe_key == "evolution:chatwoot:1:7401"
                    )
                )
            ).scalars().all()
            self.assertEqual(parent.status, "delivered")
            self.assertEqual(parent.terminal_reason, "confirmed_by_callback")
            self.assertIsNone(parent.lease_token)
            self.assertEqual(transition.status, "pending")
            self.assertEqual(len(replies), 1)
            self.assertLess(transition.stream_sequence, replies[0].stream_sequence)

    async def test_committed_renewal_after_marker_read_keeps_callback_busy(self) -> None:
        _, transition_id, marker = await self._plan_handoff("551100000075")
        claimed = (
            await delivery_worker_repo.claim_due_with_retry(
                self.sessions,
                available_slots=1,
                worker_id="expired-race-worker",
                cutover_epoch=self.epoch,
                evolution_lease_seconds=90,
                chatwoot_lease_seconds=120,
                max_attempts=3,
                max_jitter_seconds=0.01,
            )
        )[0]
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, claimed.id)
            row.lock_until = datetime.utcnow() - timedelta(seconds=1)
            await db.commit()

        event = chatwoot_orchestrator.ChatwootReplyEvent(
            account_id=1,
            remote_account_id=1,
            message_id=7501,
            conversation_id=8501,
            content="reply in authority race",
            inbox_id=7,
            contact_id=9501,
            handoff_key=marker,
        )
        payload = {
            "event": "message_created",
            "account": {"id": 1},
            "id": event.message_id,
            "content": event.content,
            "message_type": "outgoing",
            "sender": {"type": "user"},
            "conversation": {
                "id": event.conversation_id,
                "inbox_id": event.inbox_id,
                "contact_id": event.contact_id,
                "custom_attributes": {"wpp_delivery_key": marker},
            },
        }
        secret = "deterministic-renewal-race-secret"

        async def call_signed_route():
            raw = json.dumps(payload, separators=(",", ":")).encode()
            timestamp = str(int(time.time()))
            digest = hmac.new(
                secret.encode(), timestamp.encode() + b"." + raw, hashlib.sha256
            ).hexdigest()
            request = SimpleNamespace(
                body=unittest.mock.AsyncMock(return_value=raw),
                headers={
                    "X-Chatwoot-Timestamp": timestamp,
                    "X-Chatwoot-Signature": f"sha256={digest}",
                },
            )
            async with UnitOfWork(self.sessions) as uow:
                return await chatwoot_router.chatwoot_webhook(
                    request, Response(), uow
                )

        marker_read = asyncio.Event()
        release_callback = asyncio.Event()
        original_resolve = chatwoot_orchestrator._resolve_attempt

        async def pause_after_marker_read(db, candidate_event):
            resolved = await original_resolve(db, candidate_event)
            marker_read.set()
            await release_callback.wait()
            return resolved

        patches = self._settings() + (
            patch.object(
                chatwoot_router.settings,
                "provider_webhook_auth_mode",
                "enforced",
            ),
            patch.object(chatwoot_router.settings, "chatwoot_webhook_secret", secret),
            patch.object(chatwoot_router.settings, "chatwoot_account_id", 1),
            patch.object(
                chatwoot_orchestrator,
                "_resolve_attempt",
                pause_after_marker_read,
            ),
        )
        for item in patches:
            item.start()
        try:
            callback_task = asyncio.create_task(call_signed_route())
            await asyncio.wait_for(marker_read.wait(), timeout=1)
            async with self.sessions() as db:
                self.assertTrue(
                    await delivery_worker_repo.renew_lease(
                        db,
                        delivery_id=claimed.id,
                        lease_token=claimed.lease_token,
                        cutover_epoch=self.epoch,
                        lease_seconds=120,
                    )
                )
                await db.commit()
            async with self.sessions() as db:
                renewed = await db.get(OutboxDelivery, claimed.id)
                renewed_until = renewed.lock_until
                self.assertEqual(renewed.lease_token, claimed.lease_token)
            release_callback.set()
            with self.assertRaises(HTTPException) as raised:
                await asyncio.wait_for(callback_task, timeout=2)
            self.assertEqual(raised.exception.status_code, 503)
            self.assertEqual(raised.exception.headers, {"Retry-After": "5"})
        finally:
            release_callback.set()
            for item in reversed(patches):
                item.stop()

        async with self.sessions() as db:
            parent = await db.get(OutboxDelivery, claimed.id)
            transition = await db.get(OutboxDelivery, transition_id)
            receipts = (
                await db.execute(
                    select(InboundEvent).where(
                        InboundEvent.provider == "chatwoot",
                        InboundEvent.provider_event_id == "7501",
                    )
                )
            ).scalars().all()
            replies = (
                await db.execute(
                    select(OutboxDelivery).where(
                        OutboxDelivery.dedupe_key == "evolution:chatwoot:1:7501"
                    )
                )
            ).scalars().all()
            self.assertEqual(parent.status, "processing")
            self.assertEqual(parent.lease_token, claimed.lease_token)
            self.assertEqual(parent.lock_until, renewed_until)
            self.assertEqual(transition.status, "blocked")
            self.assertEqual(receipts, [])
            self.assertEqual(replies, [])

        async with self.sessions() as db:
            self.assertTrue(
                await delivery_worker_repo.complete_chatwoot_handoff(
                    db,
                    delivery_id=claimed.id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=self.epoch,
                    contact_id=9501,
                    conversation_id=8501,
                    http_status=201,
                    transition_ttl_seconds=1800,
                )
            )
            await db.commit()

        final = await self._orchestrate(event)
        self.assertEqual(final.body["status"], "queued")

        replay = await self._orchestrate(event)
        self.assertEqual(replay.body["status"], "already_accepted")
        async with self.sessions() as db:
            parent = await db.get(OutboxDelivery, claimed.id)
            transition = await db.get(OutboxDelivery, transition_id)
            replies = (
                await db.execute(
                    select(OutboxDelivery).where(
                        OutboxDelivery.dedupe_key == "evolution:chatwoot:1:7501"
                    )
                )
            ).scalars().all()
            receipts = (
                await db.execute(
                    select(InboundEvent).where(
                        InboundEvent.provider == "chatwoot",
                        InboundEvent.provider_event_id == "7501",
                    )
                )
            ).scalars().all()
            self.assertEqual(parent.status, "delivered")
            self.assertEqual(transition.status, "pending")
            self.assertEqual(len(replies), 1)
            self.assertEqual(len(receipts), 1)
            self.assertLess(transition.stream_sequence, replies[0].stream_sequence)

    async def test_signed_route_concurrent_replay_and_collision_use_one_uow_effect(self) -> None:
        _, _, marker = await self._plan_handoff("551100000073")
        payload = {
            "event": "message_created",
            "account": {"id": 1},
            "id": 7301,
            "content": "signed concurrent reply",
            "message_type": "outgoing",
            "sender": {"type": "user"},
            "conversation": {
                "id": 8301,
                "inbox_id": 7,
                "contact_id": 9301,
                "custom_attributes": {"wpp_delivery_key": marker},
            },
        }
        secret = "signed-route-secret"

        async def call(body: dict[str, object]):
            raw = json.dumps(body, separators=(",", ":")).encode()
            timestamp = str(int(time.time()))
            digest = hmac.new(
                secret.encode(), timestamp.encode() + b"." + raw, hashlib.sha256
            ).hexdigest()
            request = SimpleNamespace(
                body=unittest.mock.AsyncMock(return_value=raw),
                headers={
                    "X-Chatwoot-Timestamp": timestamp,
                    "X-Chatwoot-Signature": f"sha256={digest}",
                },
            )
            async with UnitOfWork(self.sessions) as uow:
                return await chatwoot_router.chatwoot_webhook(
                    request, Response(), uow
                )

        patches = self._settings() + (
            patch.object(chatwoot_router.settings, "provider_webhook_auth_mode", "enforced"),
            patch.object(chatwoot_router.settings, "chatwoot_webhook_secret", secret),
            patch.object(chatwoot_router.settings, "chatwoot_account_id", 1),
        )
        for item in patches:
            item.start()
        try:
            results = await asyncio.gather(call(payload), call(payload))
            self.assertEqual(
                {result["status"] for result in results},
                {"queued", "already_accepted"},
            )
            collision = dict(payload)
            collision["content"] = "changed body under same message id"
            with self.assertRaises(HTTPException) as raised:
                await call(collision)
            self.assertEqual(raised.exception.status_code, 409)
        finally:
            for item in reversed(patches):
                item.stop()

        async with self.sessions() as db:
            replies = (
                await db.execute(
                    select(OutboxDelivery).where(
                        OutboxDelivery.dedupe_key == "evolution:chatwoot:1:7301"
                    )
                )
            ).scalars().all()
            self.assertEqual(len(replies), 1)

    async def test_resume_ignores_old_conversation_and_second_generation_queues(self) -> None:
        session_id, _, marker = await self._plan_handoff("551100000071")
        first = chatwoot_orchestrator.ChatwootReplyEvent(
            account_id=1,
            remote_account_id=1,
            message_id=7101,
            conversation_id=8101,
            content="first",
            inbox_id=7,
            contact_id=9101,
            handoff_key=marker,
        )
        await self._orchestrate(first)
        async with self.sessions() as db:
            async with db.begin():
                session = await sessao_repo.lock_by_id(db, session_id)
                await handoff_attempt_repo.supersede_current_and_resume(
                    db, sessao=session
                )

        stale = await self._orchestrate(
            chatwoot_orchestrator.ChatwootReplyEvent(
                account_id=1,
                remote_account_id=1,
                message_id=7102,
                conversation_id=8101,
                content="late old reply",
                inbox_id=7,
                contact_id=9101,
                handoff_key=marker,
            )
        )
        self.assertEqual(stale.body, {"status": "ignored", "reason": "stale_handoff"})

        _, _, second_marker = await self._plan_handoff("551100000071")
        self.assertNotEqual(marker, second_marker)
        current = await self._orchestrate(
            chatwoot_orchestrator.ChatwootReplyEvent(
                account_id=1,
                remote_account_id=1,
                message_id=7103,
                conversation_id=8102,
                content="current reply",
                inbox_id=7,
                contact_id=9101,
                handoff_key=second_marker,
            )
        )
        self.assertEqual(current.body["status"], "queued")


if __name__ == "__main__":
    unittest.main()
