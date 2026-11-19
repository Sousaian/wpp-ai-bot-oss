import asyncio
import os
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from datetime import datetime, timedelta
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import delivery_cli
from src.database.models import (
    Agente,
    DeliveryOperatorAction,
    DeliveryRuntimeState,
    OutboxDelivery,
)
from src.database.repositories import (
    delivery_state_repo,
    delivery_worker_repo,
    outbox_repo,
    sessao_repo,
)


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")


class DeliveryCutoverMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        self.direct_epoch = str(uuid4())
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table in (
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
                await connection.execute(text(f"DELETE FROM {table}"))
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            await connection.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='direct', "
                    "cutover_epoch=:epoch, version=1, changed_by='test', "
                    "change_reason='isolated drill setup' WHERE id=1"
                ),
                {"epoch": self.direct_epoch},
            )
        async with self.sessions() as db:
            agente = Agente(
                nome="Cutover drill",
                instance_name=f"cutover-{uuid4()}",
                system_prompt="test",
                numero_notificacao="0",
            )
            db.add(agente)
            await db.commit()
            self.agente_id = agente.id
        self.worker_id = str(uuid4())

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def _heartbeat(
        self,
        *,
        mode: str,
        epoch: str | None,
        dispatch: bool,
        leases: int,
    ) -> None:
        async with self.sessions() as db:
            await delivery_state_repo.record_heartbeat(
                db,
                worker_id=self.worker_id,
                code_version="isolated-cutover-drill",
                current_lease_count=leases,
                dispatch_enabled=dispatch,
                configured_mode=mode,
                configured_epoch=epoch,
                provider_ready=True,
                runtime_compatible=True,
            )
            await db.commit()

    async def _delivery(self, *, epoch: str, suffix: str) -> int:
        now = datetime.utcnow()
        async with self.sessions() as db:
            async with db.begin():
                session = await sessao_repo.get_or_create_for_update(
                    db, self.agente_id, f"55117777{suffix:0>4}"
                )
                sequence = (await sessao_repo.allocate_delivery_sequences(db, session))[0]
                row, _ = await outbox_repo.enqueue(
                    db,
                    cutover_epoch=epoch,
                    dedupe_key=f"isolated-drill:{suffix}:{uuid4()}",
                    provider="evolution",
                    operation="evolution.send_text.v1",
                    payload_version=1,
                    payload={
                        "phone": session.telefone,
                        "message": "synthetic effect",
                        "instance_name": "synthetic-instance",
                    },
                    ordering_key=session.delivery_stream_key,
                    stream_sequence=sequence,
                    next_attempt_at=now,
                    expires_at=now + timedelta(hours=1),
                    agente_id=self.agente_id,
                    sessao_id=session.id,
                    _delivery_class="legacy_automated",
                )
                return row.id

    async def test_isolated_cutover_freeze_resolution_and_direct_restore(self) -> None:
        outbox_epoch = str(uuid4())
        direct_restore_epoch = str(uuid4())
        await self._heartbeat(
            mode="direct", epoch=None, dispatch=False, leases=0
        )
        provider_ready = SimpleNamespace(ready=True)
        cutover_args = Namespace(
            epoch=outbox_epoch,
            expected_version=1,
            actor="isolated-test",
            reason="synthetic cutover rehearsal",
        )
        with (
            patch.object(delivery_cli, "AsyncSessionLocal", self.sessions),
            patch.object(
                delivery_cli, "provider_auth_readiness", AsyncMock(return_value=provider_ready)
            ),
            patch.object(delivery_cli.settings, "delivery_mode", "direct"),
            patch.object(delivery_cli.settings, "delivery_dispatch_enabled", False),
            redirect_stdout(StringIO()),
        ):
            self.assertEqual(await delivery_cli._cutover(cutover_args), 0)

        async with self.sessions() as db:
            state = await db.get(DeliveryRuntimeState, 1)
            self.assertEqual((state.mode, state.cutover_epoch, state.version), ("outbox", outbox_epoch, 2))

        queued_id = await self._delivery(epoch=outbox_epoch, suffix="1")
        processing_id = await self._delivery(epoch=outbox_epoch, suffix="2")
        dead_id = await self._delivery(epoch=outbox_epoch, suffix="3")
        cancelled_id = await self._delivery(epoch=outbox_epoch, suffix="4")
        now = datetime.utcnow()
        async with self.sessions() as db:
            processing = await db.get(OutboxDelivery, processing_id)
            processing.status = "processing"
            processing.lease_token = str(uuid4())
            processing.lock_owner = self.worker_id
            processing.lock_until = now + timedelta(minutes=1)
            processing.attempt_count = 1
            dead = await db.get(OutboxDelivery, dead_id)
            dead.status = "dead"
            dead.terminal_at = now
            dead.terminal_reason = "permanent_failure"
            cancelled = await db.get(OutboxDelivery, cancelled_id)
            cancelled.status = "cancelled"
            cancelled.cancel_requested_at = now
            cancelled.terminal_at = now
            cancelled.terminal_reason = "cancel_requested"
            await db.commit()

        await self._heartbeat(
            mode="outbox", epoch=outbox_epoch, dispatch=False, leases=1
        )
        freeze_args = Namespace(
            expected_version=2,
            actor="isolated-test",
            reason="synthetic rollback rehearsal",
        )
        with (
            patch.object(delivery_cli, "AsyncSessionLocal", self.sessions),
            patch.object(delivery_cli.settings, "delivery_dispatch_enabled", False),
            redirect_stdout(StringIO()),
        ):
            with self.assertRaisesRegex(ValueError, "no leases"):
                await delivery_cli._freeze(freeze_args)

        async with self.sessions() as db:
            processing = await db.get(OutboxDelivery, processing_id)
            processing.status = "dead"
            processing.ambiguity_count = 1
            processing.lease_token = None
            processing.lock_owner = None
            processing.lock_until = None
            processing.terminal_at = datetime.utcnow()
            processing.terminal_reason = "lease_expired_ambiguous"
            await db.commit()
        await self._heartbeat(
            mode="outbox", epoch=outbox_epoch, dispatch=False, leases=0
        )
        with (
            patch.object(delivery_cli, "AsyncSessionLocal", self.sessions),
            patch.object(delivery_cli.settings, "delivery_dispatch_enabled", False),
            redirect_stdout(StringIO()),
        ):
            self.assertEqual(await delivery_cli._freeze(freeze_args), 0)

        async with self.sessions() as db:
            state = await db.get(DeliveryRuntimeState, 1)
            self.assertEqual((state.mode, state.version), ("frozen", 3))
            blocked = await delivery_state_repo.get_rollback_readiness(
                db, cutover_epoch=outbox_epoch
            )
            self.assertGreater(blocked.nonterminal, 0)
            self.assertGreater(blocked.ambiguous_unresolved, 0)
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=processing_id,
                resolution="duplicate-risk-accepted",
                actor="isolated-test",
                reason="synthetic accepted effect may have duplicated",
            )
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=dead_id,
                resolution="cancelled",
                actor="isolated-test",
                reason="synthetic permanent failure cancelled",
            )
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=cancelled_id,
                resolution="cancelled",
                actor="isolated-test",
                reason="synthetic cancellation confirmed",
            )
            await db.commit()

        with patch.object(delivery_cli, "AsyncSessionLocal", self.sessions):
            async with self.sessions() as db:
                await delivery_state_repo.record_operator_action(
                    db,
                    delivery_id=queued_id,
                    action="cancel",
                    actor="isolated-test",
                    reason="synthetic queued effect cancelled",
                )
                queued = await db.get(OutboxDelivery, queued_id)
                queued.status = "cancelled"
                queued.cancel_requested_at = datetime.utcnow()
                queued.terminal_at = datetime.utcnow()
                queued.terminal_reason = "operator_cancelled"
                await db.commit()

        async with self.sessions() as db:
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=queued_id,
                resolution="cancelled",
                actor="isolated-test",
                reason="synthetic queued effect explicitly reconciled",
            )
            await db.commit()

        stale_epoch_id = await self._delivery(epoch=self.direct_epoch, suffix="5")
        async with self.sessions() as db:
            stale = await db.get(OutboxDelivery, stale_epoch_id)
            stale.status = "dead"
            stale.terminal_at = datetime.utcnow()
            stale.terminal_reason = "stale_epoch_terminal"
            await db.commit()

        restore_args = Namespace(
            epoch=direct_restore_epoch,
            expected_version=3,
            actor="isolated-test",
            reason="synthetic guarded direct restoration",
        )
        with (
            patch.object(delivery_cli, "AsyncSessionLocal", self.sessions),
            patch.object(delivery_cli.settings, "delivery_dispatch_enabled", False),
            redirect_stdout(StringIO()),
        ):
            with self.assertRaisesRegex(ValueError, "unresolved delivery state"):
                await delivery_cli._restore_direct(restore_args)

        async with self.sessions() as db:
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=stale_epoch_id,
                resolution="cancelled",
                actor="isolated-test",
                reason="stale epoch terminal explicitly reconciled",
            )
            await db.commit()

        with (
            patch.object(delivery_cli, "AsyncSessionLocal", self.sessions),
            patch.object(delivery_cli.settings, "delivery_dispatch_enabled", False),
            redirect_stdout(StringIO()),
        ):
            self.assertEqual(await delivery_cli._restore_direct(restore_args), 0)

        async with self.sessions() as db:
            state = await db.get(DeliveryRuntimeState, 1)
            self.assertEqual(
                (state.mode, state.cutover_epoch, state.version),
                ("direct", direct_restore_epoch, 4),
            )
            actions = tuple(
                (
                    await db.execute(
                        select(DeliveryOperatorAction.action).order_by(
                            DeliveryOperatorAction.id
                        )
                    )
                ).scalars()
            )
            self.assertIn("cutover", actions)
            self.assertIn("freeze", actions)
            self.assertIn("restore_direct", actions)
            self.assertIn("rollback_resolve:duplicate-risk-accepted", actions)

    async def test_stale_resolution_never_authorizes_retried_incarnation(self) -> None:
        epoch = str(uuid4())
        delivery_id = await self._delivery(epoch=epoch, suffix="21")
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            row.status = "dead"
            row.ambiguity_count = 1
            row.terminal_at = datetime.utcnow()
            row.terminal_reason = "first_ambiguous_incarnation"
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='frozen', "
                    "cutover_epoch=:epoch WHERE id=1"
                ),
                {"epoch": epoch},
            )
            await db.commit()

        async with self.sessions() as db:
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=delivery_id,
                resolution="duplicate-risk-accepted",
                actor="isolated-test",
                reason="first terminal incarnation reconciled",
            )
            await db.commit()

        async with self.sessions() as db:
            await delivery_worker_repo.operator_retry(
                db,
                delivery_id=delivery_id,
                actor="isolated-test",
                reason="exercise a new remote attempt",
                ttl_seconds=60,
            )
            await db.commit()
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            row.status = "dead"
            row.attempt_count += 1
            row.ambiguity_count += 1
            row.terminal_at = datetime.utcnow()
            row.terminal_reason = "second_ambiguous_incarnation"
            await db.commit()

        async with self.sessions() as db:
            readiness = await delivery_state_repo.get_rollback_readiness(db)
            self.assertEqual(readiness.ambiguous_unresolved, 1)
            self.assertEqual(readiness.terminal_unresolved, 1)
            await delivery_state_repo.record_rollback_resolution(
                db,
                delivery_id=delivery_id,
                resolution="duplicate-risk-accepted",
                actor="isolated-test",
                reason="second terminal incarnation reconciled",
            )
            await db.commit()
        async with self.sessions() as db:
            self.assertTrue((await delivery_state_repo.get_rollback_readiness(db)).ready)

    async def test_concurrent_resolution_and_retry_cannot_leave_live_proof(self) -> None:
        epoch = str(uuid4())
        delivery_id = await self._delivery(epoch=epoch, suffix="22")
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            row.status = "dead"
            row.terminal_at = datetime.utcnow()
            row.terminal_reason = "race_terminal"
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state SET mode='frozen', "
                    "cutover_epoch=:epoch WHERE id=1"
                ),
                {"epoch": epoch},
            )
            await db.commit()

        async def resolve() -> object:
            try:
                async with self.sessions() as db:
                    await delivery_state_repo.record_rollback_resolution(
                        db,
                        delivery_id=delivery_id,
                        resolution="cancelled",
                        actor="race-resolver",
                        reason="terminal state reviewed",
                    )
                    await db.commit()
                return "resolved"
            except ValueError as exc:
                return exc

        async def retry() -> object:
            try:
                async with self.sessions() as db:
                    await delivery_worker_repo.operator_retry(
                        db,
                        delivery_id=delivery_id,
                        actor="race-retry",
                        reason="retry after review",
                        ttl_seconds=60,
                    )
                    await db.commit()
                return "retried"
            except ValueError as exc:
                return exc

        results = await asyncio.gather(resolve(), retry())
        self.assertIn("retried", results)
        async with self.sessions() as db:
            row = await db.get(OutboxDelivery, delivery_id)
            self.assertEqual(row.status, "retry")
            readiness = await delivery_state_repo.get_rollback_readiness(db)
            self.assertGreater(readiness.nonterminal, 0)
            self.assertFalse(readiness.ready)


if __name__ == "__main__":
    unittest.main()
