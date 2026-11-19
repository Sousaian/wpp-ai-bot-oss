"""Signal-anchored turn-worker drain over real claims and paid accounting.

Reuse the existing disposable fixtures, not a second implementation of claims.
load_tests selects only these integration cases (not inherited tests).
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy import func, select, text

from src import turn_worker
from src.config import settings
from src.database.models import AgentServiceLane, ConsumoLLM, GenerationAttempt
from src.database.repositories import scheduler_repo, turn_generation_repo
from src.services import replica_compat
from tests.test_turn_processor_mysql import TurnProcessorMySQLTests, FakeLLM, RecordingConsumer
from tests.disposable_db import assert_disposable_database


class TurnShutdownMySQLTests(TurnProcessorMySQLTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await assert_disposable_database(self, self.engine)

    async def start_worker(self, processor):
        callbacks = {}
        for name, value in (("delivery_shutdown_grace_seconds", .25),
                            ("delivery_shutdown_abort_seconds", .4)):
            self.patches.enter_context(patch.object(settings, name, value))
        self.patches.enter_context(patch.object(turn_worker, "TurnProcessor", return_value=processor))
        self.patches.enter_context(patch.object(replica_compat, "schema_supported", AsyncMock(return_value=True)))
        self.patches.enter_context(patch.object(replica_compat, "HeartbeatLoop", return_value=SimpleNamespace(
            start=lambda: None, stop=AsyncMock())))
        dispose = AsyncMock(wraps=self.engine.dispose)
        self.patches.enter_context(patch.object(turn_worker, "engine", SimpleNamespace(dispose=dispose)))
        self.patches.enter_context(patch.object(asyncio.get_running_loop(), "add_signal_handler",
            lambda sig, callback: callbacks.setdefault(sig, callback)))
        task = asyncio.create_task(turn_worker._run())
        return task, callbacks, dispose

    async def claim_case(self, release_in_drain):
        agent, instance = await self._agent("signal-claim")
        await self._intake(instance, "5511990000091", "signal-claim", "hello")
        await self._force_ready()
        llm = FakeLLM()
        consumer = RecordingConsumer(self.sessions)
        processor = self._processor(llm, consumer)
        entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
        original = scheduler_repo.claim_ready_turn
        async def claim(db, **kwargs):
            result = await original(db, **kwargs)
            if result is not None:
                entered.set()  # real claim has X locks, transaction not committed
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
            return result
        with patch.object(scheduler_repo, "claim_ready_turn", claim):
            worker, callbacks, dispose = await self.start_worker(processor)
            try:
                await asyncio.wait_for(entered.wait(), 5)
                started = asyncio.get_running_loop().time()
                callbacks[turn_worker.signal.SIGTERM]()
                # Generation/claim is an independent active task, not run().
                await asyncio.sleep(.04)
                self.assertFalse(cancelled.is_set(), "signal cancelled claim before drain")
                self.assertFalse(worker.done())
                if release_in_drain:
                    release.set()
                done, _ = await asyncio.wait({worker}, timeout=.9)
                self.assertIn(worker, done)
                worker.result()
                self.assertLess(asyncio.get_running_loop().time() - started, .9)
                dispose.assert_awaited_once()
                if not release_in_drain:
                    self.assertTrue(cancelled.is_set())
            finally:
                release.set()
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
        self.assertEqual(len(llm.calls), 0)
        self.assertEqual(await self._scalar(select(func.count(GenerationAttempt.id))), 0)
        self.assertEqual(await self._scalar(select(func.count(AgentServiceLane.agente_id)).where(
            AgentServiceLane.active_turn_id.is_not(None))), 0)
        # Existing release/rollback paths made the turn reclaimable; no new
        # force-release, no orphan attempt, no need to wait beyond its lease.
        await self._force_ready()
        fresh = self._processor(llm, consumer, worker="after-shutdown")
        stop, run = await self._run_background(fresh)
        try:
            await asyncio.wait_for(consumer.received.wait(), 10)
        finally:
            await self._stop(fresh, stop, run)
        self.assertEqual(len(llm.calls), 1)
        self.assertEqual(await self._scalar(select(func.count(ConsumoLLM.id))), 1)
        print("F0T5_CLAIM → signal_in_transaction=1 early_cancel=0 lane_orphans=0 "
              "started_orphans=0 normal_reclaim=1", flush=True)

    async def test_signal_during_claim_keeps_transaction_in_drain(self):
        await self.claim_case(True)

    async def test_signal_claim_aborts_at_drain_then_reclaims_normally(self):
        await self.claim_case(False)

    @asynccontextmanager
    async def paid_abort(self):
        agent, instance = await self._agent("signal-paid")
        await self._intake(instance, "5511990000092", "signal-paid", "hello")
        await self._force_ready()
        llm = FakeLLM()
        processor = self._processor(llm, RecordingConsumer(self.sessions))
        entered, release = asyncio.Event(), asyncio.Event()
        accounting_entered, accounting_release = asyncio.Event(), asyncio.Event()
        controllers = set(asyncio.all_tasks())
        writers, owners = [], []
        original_record = turn_generation_repo.record_generation_result
        original_account = processor._account_only

        async def recording(db, **kwargs):
            owners.append(asyncio.current_task())
            entered.set()  # LLM already returned exact usage; abort admission.
            await release.wait()
            return await original_record(db, **kwargs)

        async def accounting(*args, **kwargs):
            writers.append(asyncio.current_task())
            accounting_entered.set()
            await accounting_release.wait()  # Deterministic, not CPU timing.
            return await original_account(*args, **kwargs)

        worker = None
        try:
            with patch.object(turn_generation_repo, "record_generation_result", recording), \
                    patch.object(processor, "_account_only", accounting):
                worker, callbacks, dispose = await self.start_worker(processor)
                record_ready = asyncio.create_task(entered.wait())
                done, _ = await asyncio.wait({record_ready}, timeout=5)
                self.assertIn(record_ready, done, "paid result record barrier not reached")
                callbacks[turn_worker.signal.SIGTERM]()
                accounting_ready = asyncio.create_task(accounting_entered.wait())
                done, _ = await asyncio.wait({accounting_ready}, timeout=5)
                self.assertIn(accounting_ready, done, "shielded accounting did not start after signal")
                done, _ = await asyncio.wait({worker}, timeout=.9)
                self.assertIn(worker, done)
                worker.result()
                dispose.assert_awaited_once()
                self.assertEqual(len(llm.calls), 1)
                self.assertEqual(len(writers), 1)
                self.assertIsNot(writers[0], owners[0], "accounting must be shielded")
                self.assertFalse(writers[0].done(), "writer must outlive mocked stop")
                yield SimpleNamespace(agent=agent, llm=llm, writer=writers[0],
                                      release=accounting_release, controllers=controllers)
        finally:
            release.set()
            accounting_release.set()
            # A failed assertion must not leak writers into the next DB fixture.
            pending = set(asyncio.all_tasks()) - controllers - {asyncio.current_task()}
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def join_writers(self, state):
        # The fixture mocks HeartbeatLoop.stop, so its before_marker never runs.
        # Explicitly release and join ALL possible writers, like writers_quiescent,
        # before reading accounting. No larger drain/abort budget or sleep.
        state.release.set()
        while pending := set(asyncio.all_tasks()) - state.controllers - {asyncio.current_task()}:
            results = await asyncio.gather(*pending, return_exceptions=True)
            self.assertEqual([r for r in results if isinstance(r, BaseException)
                              and not isinstance(r, asyncio.CancelledError)], [])

    async def accounting_rows(self):
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
            ledger = list((await db.execute(select(ConsumoLLM))).scalars())
        return attempt, ledger

    def assert_settled_accounting(self, attempt, ledger, llm):
        if attempt.outcome == "succeeded":
            self.assertEqual(len(ledger), 1)
            row = ledger[0]
            self.assertEqual((attempt.accounting_state, attempt.consumo_llm_id),
                             ("recorded", row.id))
            self.assertEqual((attempt.custo_creditos, row.custo_creditos), (llm.cost, llm.cost))
            self.assertGreater(row.custo_creditos, 0)
            self.assertEqual((attempt.tokens_input, attempt.tokens_output), (11, 4))
            self.assertEqual((row.tokens_input, row.tokens_output), (11, 4))
            self.assertEqual(attempt.provider_generation_id, row.provider_generation_id)
            self.assertIsNotNone(row.provider_generation_id)
            self.assertEqual(attempt.modelo_llm, row.modelo_llm)
            return "recorded"
        if attempt.outcome == "outcome_unknown":
            self.assertEqual(attempt.accounting_state, "reconciliation_required")
            self.assertEqual(ledger, [])
            self.assertEqual((attempt.consumo_llm_id, attempt.custo_creditos,
                              attempt.tokens_input, attempt.tokens_output, attempt.reconciled_at),
                             (None, None, None, None, None))
            self.assertEqual(attempt.safe_error_code, "lane_lease_expired_unknown_outcome")
            return "unknown"
        self.fail("neither exact ledger nor recovered unknown accounting")

    async def test_abort_paid_response_preserves_shielded_accounting(self):
        async with self.paid_abort() as state:
            await self.join_writers(state)
            attempt, ledger = await self.accounting_rows()
            self.assertEqual(self.assert_settled_accounting(attempt, ledger, state.llm), "recorded")
        print("F0T5_ACCOUNTING → abort_after_paid_result=1 shielded_exact_ledger=1 "
              "writers_joined=1", flush=True)

    async def test_abort_expired_accounting_writer_recovers_to_quarantine(self):
        async with self.paid_abort() as state:
            # _run's fixture budget has expired, but the shield is still blocked.
            # Lose that writer explicitly, as crash/loop teardown can do.
            state.writer.cancel()
            await asyncio.gather(state.writer, return_exceptions=True)
            self.assertTrue(state.writer.cancelled())
            await self.join_writers(state)
            attempt, ledger = await self.accounting_rows()
            self.assertEqual((attempt.outcome, attempt.accounting_state), ("started", "pending"))
            self.assertEqual(ledger, [])
            self.assertEqual((attempt.consumo_llm_id, attempt.custo_creditos), (None, None))
            # Real DB intermediate state is NOT an acceptable terminal result.
            with self.assertRaisesRegex(AssertionError, "neither exact ledger nor recovered unknown"):
                self.assert_settled_accounting(attempt, ledger, state.llm)
            async with self.sessions.begin() as db:
                # Disposable DB configuration; never rewrite an attempt's drain.
                await db.execute(text("UPDATE scheduler_runtime_state "
                                      "SET accounting_drain_seconds=1 WHERE id=1"))
            await self._expire_lane(state.agent, 1)

            async def recover():
                async def operation(db):
                    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
                    return await scheduler_repo.recover_expired_lane(
                        db, agente_id=state.agent, scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.delivery_epoch,
                        retry_at=now + timedelta(seconds=1),
                        expected_turn_id=attempt.turn_id, expected_sessao_id=attempt.sessao_id)
                return await scheduler_repo.run_transaction_with_deadlock_retry(self.sessions, operation)

            self.assertEqual(await recover(), "quarantined")
            draining, ledger = await self.accounting_rows()
            self.assertEqual((draining.outcome, draining.accounting_state), ("started", "pending"))
            self.assertEqual(ledger, [])
            self.assertIsNotNone(draining.accounting_drain_until)

            async def drain_elapsed():
                return await self._scalar(select(GenerationAttempt.accounting_drain_until <=
                                                func.utc_timestamp(6)).where(
                                                    GenerationAttempt.id == attempt.id))

            await self._until(drain_elapsed, message="real DB accounting drain")
            self.assertEqual(await recover(), "quarantined")
            recovered, ledger = await self.accounting_rows()
            self.assertEqual(self.assert_settled_accounting(recovered, ledger, state.llm), "unknown")
            self.assertEqual(await self._scalar(select(AgentServiceLane.active_turn_id).where(
                AgentServiceLane.agente_id == state.agent)), attempt.turn_id)
        print("F0T5_ACCOUNTING → expired_writer=1 started_recovered=1 "
              "quarantined_unknown=1 phantom_ledger=0 zero_cost=0", flush=True)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(TurnShutdownMySQLTests(name) for name in (
        "test_signal_during_claim_keeps_transaction_in_drain",
        "test_signal_claim_aborts_at_drain_then_reclaims_normally",
        "test_abort_paid_response_preserves_shielded_accounting",
        "test_abort_expired_accounting_writer_recovers_to_quarantine",
    ))
