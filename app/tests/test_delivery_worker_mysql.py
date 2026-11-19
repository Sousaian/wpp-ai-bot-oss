import asyncio
from datetime import datetime, timedelta
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.database.models import (
    Agente,
    DeliveryOperatorAction,
    HandoffAttempt,
    InboundEvent,
    OutboxDelivery,
    Sessao,
)
from src import delivery_worker as worker_module
from src.database.repositories import (
    delivery_state_repo,
    delivery_worker_repo,
    handoff_attempt_repo,
    inbound_event_repo,
    outbox_repo,
    sessao_repo,
)
from src.services.delivery_adapters import DeliveryResult


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")


class DeliveryWorkerMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required for worker integration tests")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        self.epoch = str(uuid4())
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table_name in (
                "delivery_operator_actions",
                "delivery_worker_heartbeats",
                "outbox_deliveries",
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
        async with self.sessions() as db:
            agente = Agente(
                nome="worker-test",
                instance_name=f"worker-{uuid4()}",
                system_prompt="test",
                numero_notificacao="0",
                chatwoot_inbox_id=7,
            )
            db.add(agente)
            await db.commit()
            self.agente_id = agente.id
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state "
                    "SET mode='outbox', cutover_epoch=:epoch, version=version+1, "
                    "changed_by='worker-test', change_reason='isolated test' "
                    "WHERE id=1"
                ),
                {"epoch": self.epoch},
            )
            await db.commit()

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def _session(self, phone: str) -> Sessao:
        async with self.sessions() as db:
            async with db.begin():
                session = await sessao_repo.get_or_create_for_update(
                    db,
                    self.agente_id,
                    phone,
                )
                session_id = session.id
            return await db.get(Sessao, session_id)

    async def _evolution(
        self,
        session_id: int,
        *,
        sequence: int | None = None,
        due: datetime | None = None,
        expires: datetime | None = None,
        ordering_key: str | None = None,
    ) -> int:
        now = datetime.utcnow()
        async with self.sessions() as db:
            async with db.begin():
                session = await sessao_repo.lock_by_id(db, session_id)
                if sequence is None:
                    sequence = (
                        await sessao_repo.allocate_delivery_sequences(db, session)
                    )[0]
                delivery, _ = await outbox_repo.enqueue(
                    db,
                    cutover_epoch=self.epoch,
                    dedupe_key=f"worker:{uuid4()}",
                    provider="evolution",
                    operation="evolution.send_text.v1",
                    payload_version=1,
                    payload={
                        "phone": session.telefone,
                        "message": "restricted",
                        "instance_name": "worker-test",
                    },
                    ordering_key=ordering_key or session.delivery_stream_key,
                    stream_sequence=sequence,
                    next_attempt_at=due or now,
                    expires_at=expires or now + timedelta(hours=1),
                    agente_id=self.agente_id,
                    sessao_id=session.id,
                    _delivery_class="legacy_automated",
                )
                return delivery.id

    async def _claim(self, worker: str, capacity: int = 8):
        return await delivery_worker_repo.claim_due_with_retry(
            self.sessions,
            available_slots=capacity,
            worker_id=worker,
            cutover_epoch=self.epoch,
            evolution_lease_seconds=90,
            chatwoot_lease_seconds=120,
            max_attempts=6,
            max_jitter_seconds=0.02,
        )

    async def _handoff(self, phone: str) -> tuple[int, int, int, int]:
        now = datetime.utcnow()
        async with self.sessions() as db:
            async with db.begin():
                session = await sessao_repo.get_or_create_for_update(
                    db,
                    self.agente_id,
                    phone,
                )
                attempt = await handoff_attempt_repo.create_for_locked_session(
                    db,
                    sessao=session,
                    chatwoot_inbox_id=7,
                )
                open_job, _ = await outbox_repo.enqueue(
                    db,
                    cutover_epoch=self.epoch,
                    dedupe_key=f"handoff-open:{attempt.handoff_key}",
                    provider="chatwoot",
                    operation="chatwoot.open_handoff.v1",
                    payload_version=1,
                    payload={
                        "phone": phone,
                        "summary": "restricted",
                        "inbox_id": 7,
                        "handoff_key": attempt.handoff_key,
                    },
                    ordering_key=attempt.handoff_key,
                    stream_sequence=0,
                    next_attempt_at=now,
                    expires_at=now + timedelta(hours=24),
                    agente_id=self.agente_id,
                    sessao_id=session.id,
                    handoff_attempt_id=attempt.id,
                    _delivery_class="provider_setup_context",
                )
                sequence = (
                    await sessao_repo.allocate_delivery_sequences(db, session)
                )[0]
                transition, _ = await outbox_repo.reserve_transition(
                    db,
                    dependency=open_job,
                    cutover_epoch=self.epoch,
                    dedupe_key=f"handoff-transition:{attempt.handoff_key}",
                    payload_version=1,
                    payload={
                        "phone": phone,
                        "message": "transition",
                        "instance_name": "worker-test",
                    },
                    ordering_key=session.delivery_stream_key,
                    stream_sequence=sequence,
                    agente_id=self.agente_id,
                    sessao_id=session.id,
                    handoff_attempt_id=attempt.id,
                    inbound_event_id=None,
                )
                return session.id, attempt.id, open_job.id, transition.id

    async def test_capacity_claim_respects_fifo_and_two_workers_never_share_a_job(self) -> None:
        session_a = await self._session("551100000001")
        session_b = await self._session("551100000002")
        first_a = await self._evolution(session_a.id)
        second_a = await self._evolution(session_a.id)
        first_b = await self._evolution(session_b.id)

        first_claim, second_claim = await asyncio.gather(
            self._claim("worker-a", capacity=8),
            self._claim("worker-b", capacity=8),
        )
        claimed = first_claim + second_claim
        self.assertEqual({row.id for row in claimed}, {first_a, first_b})
        self.assertEqual(len({row.id for row in claimed}), len(claimed))
        self.assertNotIn(second_a, {row.id for row in claimed})

    async def test_repeated_barrier_two_worker_claims_never_escape_or_duplicate(self) -> None:
        for round_number in range(20):
            session_a = await self._session(f"5521{round_number:09d}1")
            session_b = await self._session(f"5521{round_number:09d}2")
            first_a = await self._evolution(session_a.id)
            second_a = await self._evolution(session_a.id)
            first_b = await self._evolution(session_b.id)
            barrier = asyncio.Barrier(3)

            async def contender(worker: str):
                await barrier.wait()
                return await self._claim(worker, capacity=8)

            one = asyncio.create_task(contender(f"worker-a-{round_number}"))
            two = asyncio.create_task(contender(f"worker-b-{round_number}"))
            await barrier.wait()
            first_claim, second_claim = await asyncio.gather(one, two)
            claimed = first_claim + second_claim
            self.assertEqual({row.id for row in claimed}, {first_a, first_b})
            self.assertEqual(len({row.id for row in claimed}), len(claimed))
            self.assertNotIn(second_a, {row.id for row in claimed})

    async def test_renewal_and_fence_reject_stale_completion(self) -> None:
        session = await self._session("551100000003")
        delivery_id = await self._evolution(session.id)
        claimed = (await self._claim("worker-a", capacity=1))[0]
        async with self.sessions() as db:
            self.assertTrue(
                await delivery_worker_repo.renew_lease(
                    db,
                    delivery_id=delivery_id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=claimed.cutover_epoch,
                    lease_seconds=90,
                )
            )
            await db.commit()
        self.assertEqual(await self._claim("worker-b", capacity=1), [])
        async with self.sessions() as db:
            self.assertFalse(
                await delivery_worker_repo.complete_evolution(
                    db,
                    delivery_id=delivery_id,
                    lease_token=str(uuid4()),
                    cutover_epoch=claimed.cutover_epoch,
                    external_id="stale",
                    http_status=200,
                )
            )
            await db.commit()
        async with self.sessions() as db:
            self.assertTrue(
                await delivery_worker_repo.complete_evolution(
                    db,
                    delivery_id=delivery_id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=claimed.cutover_epoch,
                    external_id="remote-1",
                    http_status=200,
                )
            )
            await db.commit()
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(row.status, "delivered")
            self.assertEqual(row.external_id, "remote-1")

    async def test_freeze_and_epoch_rotation_reject_old_renewal_and_completion(self) -> None:
        session = await self._session("551100000013")
        delivery_id = await self._evolution(session.id)
        claimed = (await self._claim("worker-a", capacity=1))[0]
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='frozen', version=version+1, "
                    "changed_by='worker-test', change_reason='freeze test' WHERE id=1"
                )
            )
            await db.commit()
        async with self.sessions() as db:
            self.assertFalse(
                await delivery_worker_repo.renew_lease(
                    db,
                    delivery_id=delivery_id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=claimed.cutover_epoch,
                    lease_seconds=90,
                )
            )
            await db.commit()
        async with self.sessions() as db:
            self.assertFalse(
                await delivery_worker_repo.complete_evolution(
                    db,
                    delivery_id=delivery_id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=claimed.cutover_epoch,
                    external_id="remote-uncertain",
                    http_status=200,
                )
            )
            await db.commit()
        rotated = str(uuid4())
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='outbox', cutover_epoch=:epoch, "
                    "version=version+1, changed_by='worker-test', "
                    "change_reason='epoch rotation' WHERE id=1"
                ),
                {"epoch": rotated},
            )
            await db.commit()
        async with self.sessions() as db:
            self.assertFalse(
                await delivery_worker_repo.complete_evolution(
                    db,
                    delivery_id=delivery_id,
                    lease_token=claimed.lease_token,
                    cutover_epoch=claimed.cutover_epoch,
                    external_id="remote-still-uncertain",
                    http_status=200,
                )
            )
            await db.commit()
            row = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(row.status, "processing")
            self.assertIsNone(row.external_id)

    async def test_replacement_claim_cancels_old_blocked_provider_after_lease_loss(self) -> None:
        session = await self._session("551100000017")
        delivery_id = await self._evolution(session.id)
        claimed = (await self._claim("worker-a", capacity=1))[0]
        provider_started = asyncio.Event()
        provider_finalized = asyncio.Event()

        async def blocked_provider(*args, **kwargs):
            provider_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                provider_finalized.set()

        worker = worker_module.DeliveryWorker()
        with (
            patch.object(worker_module, "AsyncSessionLocal", self.sessions),
            patch.object(worker_module, "send_evolution_text", blocked_provider),
            patch.object(
                worker_module.settings,
                "delivery_lease_renew_interval_seconds",
                0.2,
            ),
        ):
            dispatch = asyncio.create_task(worker._dispatch_one(claimed))
            await asyncio.wait_for(provider_started.wait(), timeout=1)
            async with self.sessions() as db:
                row = await db.get(OutboxDelivery, delivery_id)
                row.lock_until = datetime.utcnow() - timedelta(seconds=1)
                await db.commit()
            async with self.sessions() as db:
                await delivery_worker_repo.run_preclaim_maintenance(db)
                await db.commit()
            replacement = (await self._claim("worker-b", capacity=1))[0]
            await asyncio.wait_for(dispatch, timeout=1)
        self.assertNotEqual(replacement.lease_token, claimed.lease_token)
        self.assertTrue(provider_finalized.is_set())
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(row.status, "processing")
            self.assertEqual(row.lock_owner, "worker-b")
            self.assertEqual(row.lease_token, replacement.lease_token)
            self.assertEqual(row.ambiguity_count, 1)

    async def test_freeze_during_blocked_provider_cancels_io_and_recovers_ambiguity(self) -> None:
        session = await self._session("551100000018")
        delivery_id = await self._evolution(session.id)
        claimed = (await self._claim("worker-a", capacity=1))[0]
        provider_started = asyncio.Event()
        provider_finalized = asyncio.Event()

        async def blocked_provider(*args, **kwargs):
            provider_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                provider_finalized.set()

        worker = worker_module.DeliveryWorker()
        with (
            patch.object(worker_module, "AsyncSessionLocal", self.sessions),
            patch.object(worker_module, "send_evolution_text", blocked_provider),
            patch.object(
                worker_module.settings,
                "delivery_lease_renew_interval_seconds",
                0.1,
            ),
        ):
            dispatch = asyncio.create_task(worker._dispatch_one(claimed))
            await asyncio.wait_for(provider_started.wait(), timeout=1)
            async with self.sessions() as db:
                await db.execute(
                    text(
                        "UPDATE delivery_runtime_state SET mode='frozen', "
                        "version=version+1, changed_by='worker-test', "
                        "change_reason='freeze in flight' WHERE id=1"
                    )
                )
                await db.commit()
            await asyncio.wait_for(dispatch, timeout=1)
        self.assertTrue(provider_finalized.is_set())
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(row.status, "processing")
            self.assertIsNone(row.external_id)
            row.lock_until = datetime.utcnow() - timedelta(seconds=1)
            await db.commit()
        async with self.sessions() as db:
            result = await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        self.assertEqual(result.ambiguity_counted, 1)
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(row.status, "retry")
            self.assertEqual(row.ambiguity_count, 1)
            self.assertEqual(row.last_error_code, "lease_expired_ambiguous")

    async def test_db_utc_maintenance_recovers_cancels_expires_and_dependencies(self) -> None:
        session = await self._session("551100000004")
        recovery_id = await self._evolution(session.id)
        recovery = (await self._claim("worker-a", capacity=1))[0]
        cancelled_id = await self._evolution(session.id)
        expired_id = await self._evolution(
            session.id,
            due=datetime.utcnow() - timedelta(minutes=2),
            expires=datetime.utcnow() - timedelta(minutes=1),
        )
        _, _, parent_id, child_id = await self._handoff("551100000005")
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, recovery_id)
            row.lock_until = datetime.utcnow() - timedelta(seconds=1)
            cancelled = await db.get(OutboxDelivery, cancelled_id)
            cancelled.status = "processing"
            cancelled.attempt_count = 1
            cancelled.lease_token = str(uuid4())
            cancelled.lock_owner = "worker-z"
            cancelled.lock_until = datetime.utcnow() - timedelta(seconds=1)
            cancelled.cancel_requested_at = datetime.utcnow() - timedelta(seconds=2)
            parent = await db.get(OutboxDelivery, parent_id)
            parent.status = "dead"
            parent.terminal_at = datetime.utcnow()
            parent.terminal_reason = "permanent"
            await db.commit()
        async with self.sessions() as db:
            result = await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        self.assertEqual(result.recovered, 1)
        self.assertEqual(result.ambiguity_counted, 1)
        self.assertEqual(result.cancelled, 1)
        self.assertEqual(result.expired, 1)
        self.assertEqual(result.dependency_terminal, 1)
        async with self.sessions() as db:
            recovered = await db.get(OutboxDelivery, recovery.id)
            cancelled = await db.get(OutboxDelivery, cancelled_id)
            expired = await db.get(OutboxDelivery, expired_id)
            child = await db.get(OutboxDelivery, child_id)
            self.assertEqual(recovered.status, "retry")
            self.assertEqual(recovered.ambiguity_count, 1)
            self.assertEqual(cancelled.status, "cancelled")
            self.assertEqual(expired.status, "dead")
            self.assertEqual(child.status, "dead")

    async def test_chatwoot_completion_maps_attempt_and_unblocks_transition(self) -> None:
        session_id, attempt_id, open_id, transition_id = await self._handoff(
            "551100000006"
        )
        claimed = (await self._claim("worker-a"))[0]
        self.assertEqual(claimed.id, open_id)
        async with self.sessions() as db:
            accepted = await delivery_worker_repo.complete_chatwoot_handoff(
                db,
                delivery_id=open_id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                contact_id=91,
                conversation_id=92,
                http_status=201,
                transition_ttl_seconds=1800,
            )
            await db.commit()
        self.assertTrue(accepted)
        async with self.sessions() as db:
            session = await db.get(Sessao, session_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            transition = await db.get(OutboxDelivery, transition_id)
            self.assertEqual(session.chatwoot_conversation_id, 92)
            self.assertEqual(attempt.status, "ready")
            self.assertEqual(transition.status, "pending")
            self.assertIsNotNone(transition.expires_at)

    async def test_freeze_rejects_chatwoot_mapping_and_dependency_unblock(self) -> None:
        session_id, attempt_id, open_id, transition_id = await self._handoff(
            "551100000016"
        )
        claimed = (await self._claim("worker-a", capacity=1))[0]
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='frozen', version=version+1, "
                    "changed_by='worker-test', change_reason='freeze mapping test' "
                    "WHERE id=1"
                )
            )
            await db.commit()
        async with self.sessions() as db:
            accepted = await delivery_worker_repo.complete_chatwoot_handoff(
                db,
                delivery_id=open_id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                contact_id=91,
                conversation_id=94,
                http_status=201,
                transition_ttl_seconds=1800,
            )
            await db.commit()
        self.assertFalse(accepted)
        async with self.sessions() as db:
            session = await db.get(Sessao, session_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            parent = await db.get(OutboxDelivery, open_id)
            child = await db.get(OutboxDelivery, transition_id)
            self.assertIsNone(session.chatwoot_conversation_id)
            self.assertEqual(attempt.status, "pending")
            self.assertEqual(parent.status, "processing")
            self.assertEqual(child.status, "blocked")

    async def test_expired_chatwoot_processing_lease_and_ttl_terminalize_same_pass(self) -> None:
        _, attempt_id, open_id, transition_id = await self._handoff("551100000014")
        claimed = (await self._claim("worker-a", capacity=1))[0]
        self.assertEqual(claimed.id, open_id)
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, open_id)
            row.lock_until = datetime.utcnow() - timedelta(seconds=2)
            row.expires_at = datetime.utcnow() - timedelta(seconds=1)
            await db.commit()
        async with self.sessions() as db:
            result = await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        self.assertEqual(result.expired, 1)
        async with self.sessions() as db:
            parent = await db.get(OutboxDelivery, open_id)
            child = await db.get(OutboxDelivery, transition_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            self.assertEqual(parent.status, "dead")
            self.assertEqual(parent.terminal_reason, "expired")
            self.assertIsNone(parent.lease_token)
            self.assertEqual(child.status, "dead")
            self.assertEqual(child.terminal_reason, "dependency_failed")
            self.assertEqual(attempt.status, "failed")

    async def test_expired_fifo_head_is_terminalized_before_successor_claim(self) -> None:
        session = await self._session("551100000010")
        expired_id = await self._evolution(
            session.id,
            due=datetime.utcnow() - timedelta(minutes=2),
            expires=datetime.utcnow() - timedelta(minutes=1),
        )
        successor_id = await self._evolution(session.id)
        self.assertEqual(await self._claim("worker-a"), [])
        async with self.sessions() as db:
            await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        claimed = await self._claim("worker-a")
        self.assertEqual([row.id for row in claimed], [successor_id])
        async with self.sessions() as db:
            expired = await db.get(OutboxDelivery, expired_id)
            self.assertEqual(expired.status, "dead")
            self.assertEqual(expired.terminal_reason, "expired")

    async def test_confirmed_chatwoot_success_after_cancel_never_unblocks(self) -> None:
        _, attempt_id, open_id, transition_id = await self._handoff("551100000007")
        claimed = (await self._claim("worker-a"))[0]
        async with self.sessions() as db:
            await delivery_worker_repo.operator_cancel(
                db,
                delivery_id=open_id,
                actor="operator",
                reason="test cancellation",
            )
            await db.commit()
        async with self.sessions() as db:
            accepted = await delivery_worker_repo.complete_chatwoot_handoff(
                db,
                delivery_id=open_id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                contact_id=91,
                conversation_id=93,
                http_status=201,
                transition_ttl_seconds=1800,
            )
            await db.commit()
        self.assertTrue(accepted)
        async with self.sessions() as db:
            open_job = await db.get(OutboxDelivery, open_id)
            transition = await db.get(OutboxDelivery, transition_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            self.assertEqual(open_job.terminal_reason, "delivered_after_cancel")
            self.assertEqual(transition.status, "cancelled")
            self.assertEqual(attempt.status, "cancelled")

    async def test_provider_accept_finalize_rollback_restart_reconciles_exact_marker(self) -> None:
        session_id, attempt_id, open_id, transition_id = await self._handoff(
            "551100000027"
        )
        first_claim = (await self._claim("worker-before-restart", capacity=1))[0]
        remote: dict[str, tuple[int, int]] = {}
        creates = 0
        calls = 0

        async def provider(payload, *, delivery_key, deadline_at, reconcile_only=False):
            nonlocal creates, calls
            calls += 1
            if delivery_key not in remote:
                self.assertFalse(reconcile_only)
                creates += 1
                remote[delivery_key] = (9701, 8701)
                return DeliveryResult(
                    external_id="8701",
                    http_status=201,
                    contact_id=9701,
                    conversation_id=8701,
                    reconciled=False,
                )
            contact_id, conversation_id = remote[delivery_key]
            return DeliveryResult(
                external_id=str(conversation_id),
                http_status=200,
                contact_id=contact_id,
                conversation_id=conversation_id,
                reconciled=True,
            )

        real_complete = delivery_worker_repo.complete_chatwoot_handoff

        async def finalize_then_fail(db, **kwargs):
            accepted = await real_complete(db, **kwargs)
            self.assertTrue(accepted)
            raise RuntimeError("synthetic local finalize failure")

        with (
            patch.object(worker_module, "AsyncSessionLocal", self.sessions),
            patch.object(
                worker_module,
                "open_or_reconcile_chatwoot_handoff",
                provider,
            ),
            patch.object(
                delivery_worker_repo,
                "complete_chatwoot_handoff",
                finalize_then_fail,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "local finalize failure"):
                await worker_module.DeliveryWorker()._dispatch_one(first_claim)

        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, open_id)
            self.assertEqual(row.status, "processing")
            self.assertEqual(row.lease_token, first_claim.lease_token)
            self.assertIsNone(row.external_id)
            row.lock_until = datetime.utcnow() - timedelta(seconds=1)
            await db.commit()
        async with self.sessions() as db:
            recovery = await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        self.assertEqual(recovery.recovered, 1)

        second_claim = (await self._claim("worker-after-restart", capacity=1))[0]
        self.assertEqual(second_claim.id, open_id)
        self.assertNotEqual(second_claim.lease_token, first_claim.lease_token)
        with (
            patch.object(worker_module, "AsyncSessionLocal", self.sessions),
            patch.object(
                worker_module,
                "open_or_reconcile_chatwoot_handoff",
                provider,
            ),
        ):
            await worker_module.DeliveryWorker()._dispatch_one(second_claim)

        self.assertEqual(creates, 1)
        self.assertEqual(calls, 2)
        async with self.sessions() as db:
            session = await db.get(Sessao, session_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            parent = await db.get(OutboxDelivery, open_id)
            child = await db.get(OutboxDelivery, transition_id)
            self.assertEqual(parent.status, "delivered")
            self.assertEqual(parent.external_id, "8701")
            self.assertEqual(attempt.status, "ready")
            self.assertEqual(session.chatwoot_conversation_id, 8701)
            self.assertEqual(child.status, "pending")

    async def test_dispatch_barrier_resume_delete_and_cancel_preserve_fence(self) -> None:
        from src.routers import sessoes as sessoes_router

        phone = "551100000028"
        session_id, attempt_id, open_id, transition_id = await self._handoff(phone)
        claimed = (await self._claim("barrier-worker", capacity=1))[0]
        provider_started = asyncio.Event()
        release_provider = asyncio.Event()

        async def blocked_provider(
            payload, *, delivery_key, deadline_at, reconcile_only=False
        ):
            provider_started.set()
            await release_provider.wait()
            return DeliveryResult(
                external_id="8801",
                http_status=201,
                contact_id=9801,
                conversation_id=8801,
                reconciled=False,
            )

        with (
            patch.object(worker_module, "AsyncSessionLocal", self.sessions),
            patch.object(
                worker_module,
                "open_or_reconcile_chatwoot_handoff",
                blocked_provider,
            ),
            patch.object(sessoes_router.settings, "delivery_mode", "outbox"),
            patch.object(
                sessoes_router.settings,
                "delivery_cutover_epoch",
                self.epoch,
            ),
        ):
            dispatch = asyncio.create_task(
                worker_module.DeliveryWorker()._dispatch_one(claimed)
            )
            await asyncio.wait_for(provider_started.wait(), timeout=1)

            async with self.sessions() as db:
                with self.assertRaises(HTTPException) as raised:
                    await sessoes_router.deletar_sessao(
                        telefone=phone,
                        agente_id=self.agente_id,
                        db=db,
                    )
                self.assertEqual(raised.exception.status_code, 409)
                await db.rollback()

            async with self.sessions() as db:
                resumed = await sessoes_router.retomar_sessao(
                    telefone=phone,
                    agente_id=self.agente_id,
                    db=db,
                )
                await db.commit()
            self.assertEqual(resumed["status"], "resumed")

            async with self.sessions() as db:
                row = await delivery_worker_repo.operator_cancel(
                    db,
                    delivery_id=open_id,
                    actor="barrier-operator",
                    reason="confirm cancellation while provider is active",
                )
                self.assertEqual(row.status, "processing")
                self.assertEqual(row.lease_token, claimed.lease_token)
                await db.commit()

            release_provider.set()
            await asyncio.wait_for(dispatch, timeout=2)

        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(Sessao, session_id))
            session = await db.get(Sessao, session_id)
            attempt = await db.get(HandoffAttempt, attempt_id)
            parent = await db.get(OutboxDelivery, open_id)
            child = await db.get(OutboxDelivery, transition_id)
            self.assertEqual(session.handler, "bot")
            self.assertEqual(attempt.status, "superseded")
            self.assertEqual(parent.status, "delivered")
            self.assertEqual(parent.terminal_reason, "delivered_after_cancel")
            self.assertEqual(child.status, "cancelled")

    async def test_retention_scrubs_payload_and_purges_old_receipts(self) -> None:
        session = await self._session("551100000008")
        delivery_id = await self._evolution(session.id)
        purged_delivery_id = await self._evolution(session.id)
        keyring = inbound_event_repo.ReceiptHMACKeyRing(
            current_version=1,
            keys={1: "test-secret"},
        )
        async with self.sessions() as db:
            receipt = await inbound_event_repo.record(
                db,
                provider="evolution",
                source_scope="test",
                provider_event_id="old-event",
                event_type="messages.upsert",
                actionable_fields={"id": "old-event"},
                keyring=keyring,
                receipt_disposition="legacy_evolution",
            )
            row = await db.get(OutboxDelivery, delivery_id)
            row.status = "dead"
            row.terminal_at = datetime.utcnow() - timedelta(days=8)
            row.terminal_reason = "permanent"
            purged = await db.get(OutboxDelivery, purged_delivery_id)
            purged.status = "dead"
            purged.terminal_at = datetime.utcnow() - timedelta(days=91)
            purged.terminal_reason = "permanent"
            receipt.event.received_at = datetime.utcnow() - timedelta(days=91)
            await db.commit()
        async with self.sessions() as db:
            result = await delivery_worker_repo.cleanup_retention(db)
            await db.commit()
        self.assertEqual(result.payloads_scrubbed, 2)
        self.assertEqual(result.receipts_purged, 1)
        self.assertEqual(result.deliveries_purged, 1)
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(row.payload, {})
            self.assertIsNone(await db.get(InboundEvent, receipt.event.id))
            self.assertIsNone(await db.get(OutboxDelivery, purged_delivery_id))

    async def test_operator_retry_and_cancel_are_audited_without_payload_copy(self) -> None:
        session = await self._session("551100000009")
        delivery_id = await self._evolution(session.id)
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            row.status = "dead"
            row.terminal_at = datetime.utcnow()
            row.terminal_reason = "synthetic"
            row.last_error_code = "http_400"
            await db.commit()
        async with self.sessions() as db:
            retried = await delivery_worker_repo.operator_retry(
                db,
                delivery_id=delivery_id,
                actor="operator-a",
                reason="approved retry",
                ttl_seconds=1800,
            )
            await db.commit()
            self.assertEqual(retried.status, "retry")
        async with self.sessions() as db:
            cancelled = await delivery_worker_repo.operator_cancel(
                db,
                delivery_id=delivery_id,
                actor="operator-b",
                reason="stop retry",
            )
            await db.commit()
            self.assertEqual(cancelled.status, "cancelled")
        async with self.sessions() as db:
            actions = (
                await db.execute(
                    select(DeliveryOperatorAction)
                    .where(DeliveryOperatorAction.delivery_id == delivery_id)
                    .order_by(DeliveryOperatorAction.id)
                )
            ).scalars().all()
            self.assertEqual([row.action for row in actions], ["retry", "cancel"])
            for action in actions:
                self.assertFalse(hasattr(action, "payload"))

    async def test_blank_operator_metadata_has_no_effect(self) -> None:
        session = await self._session("551100000015")
        delivery_id = await self._evolution(session.id)
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            row.status = "dead"
            row.terminal_at = datetime.utcnow()
            row.terminal_reason = "synthetic"
            await db.commit()
        async with self.sessions() as db:
            with self.assertRaisesRegex(ValueError, "actor must not be blank"):
                await delivery_worker_repo.operator_retry(
                    db,
                    delivery_id=delivery_id,
                    actor="   ",
                    reason="approved",
                    ttl_seconds=1800,
                )
            await db.rollback()
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            actions = (
                await db.execute(
                    select(DeliveryOperatorAction).where(
                        DeliveryOperatorAction.delivery_id == delivery_id
                    )
                )
            ).scalars().all()
            self.assertEqual(row.status, "dead")
            self.assertEqual(actions, [])

    async def test_worker_readiness_is_replica_specific_and_uses_database_time(self) -> None:
        stalled_id = str(uuid4())
        healthy_id = str(uuid4())
        async with self.sessions() as db:
            await delivery_state_repo.record_heartbeat(
                db,
                worker_id=stalled_id,
                code_version="test",
                current_lease_count=0,
            )
            await delivery_state_repo.record_heartbeat(
                db,
                worker_id=healthy_id,
                code_version="test",
                current_lease_count=0,
            )
            await db.execute(
                text(
                    "UPDATE delivery_worker_heartbeats "
                    "SET last_seen_at=TIMESTAMPADD(SECOND, -120, UTC_TIMESTAMP(6)) "
                    "WHERE worker_id=:worker_id"
                ),
                {"worker_id": stalled_id},
            )
            await db.commit()
        async with self.sessions() as db:
            stalled = await delivery_state_repo.get_worker_readiness(
                db,
                worker_id=stalled_id,
                cutover_epoch=self.epoch,
            )
            healthy = await delivery_state_repo.get_worker_readiness(
                db,
                worker_id=healthy_id,
                cutover_epoch=self.epoch,
            )
        self.assertGreater(
            stalled.db_now - stalled.heartbeat,
            timedelta(seconds=60),
        )
        self.assertLess(
            healthy.db_now - healthy.heartbeat,
            timedelta(seconds=60),
        )


if __name__ == "__main__":
    unittest.main()
