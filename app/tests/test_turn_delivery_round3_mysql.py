"""Ticket 06 review round 3: real-MySQL tests for surviving mutations.

R2M2 (finalization after the delivery lease lapsed), R2M18/R2M19 (requeue
re-checks FIFO and epoch under locks; discovery is only a hint) and R2M20
(settlement releases the lane only when this turn owns it). Every provider is
an in-process fake; helpers come from the ticket06 suite, whose own cases are
not re-run here.
"""

import unittest
from uuid import uuid4

from sqlalchemy import func, select, text, update

from src.database.models import AgentServiceLane, ConversationTurn, OutboxDelivery, Sessao
from src.database.repositories import delivery_worker_repo, turn_delivery_repo
from tests import test_turn_delivery_mysql as t06


FakeLLM = t06.FakeLLM


class TurnDeliveryRound3MySQLTests(t06.TurnDeliveryMySQLTests):
    # ── helpers ─────────────────────────────────────────────────────────
    async def _lapse_delivery_lease(self, delivery_id: int) -> None:
        """A stalled worker: its delivery lease ran out, no recovery ran yet."""

        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.id == delivery_id)
                .values(lock_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()

    async def _requeue(self, turn: ConversationTurn) -> bool:
        async with self.sessions() as db:
            requeued = await turn_delivery_repo.requeue_admitted_turn(
                db,
                turn_id=turn.id,
                agente_id=turn.agente_id,
                sessao_id=turn.sessao_id,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
            )
            await db.commit()
        return requeued

    async def _discovered(self) -> list[int]:
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=10
            )
        return [turn_id for turn_id, _, _ in ready]

    async def _waiting_on_free_lane(self, suffix: str):
        """Ordinary admitted turn whose lane expired and was released."""

        agent, instance, phone, _ = await self._ordinary_admitted(suffix)
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        turn = (await self._turns())[0]
        self.assertEqual((turn.status, turn.claim_token), ("awaiting_delivery", None))
        return agent, instance, phone, turn

    async def _lane_taken_by_other_session(self, suffix: str):
        """T1's reply is in I/O; T1's lane lapsed and T2 (same agent) owns it."""

        agent, instance, phone, _ = await self._ordinary_admitted(suffix)
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        other_phone = f"5512{agent:08d}"
        await self._accept(instance, other_phone, f"{suffix}-other", "outra conversa")
        await self._force_ready()
        processor = self._router_processor(FakeLLM("resposta da outra"))
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        first, second = await self._turns()
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertEqual(first.status, "awaiting_delivery")
        self.assertEqual(second.status, "awaiting_delivery")
        self.assertIsNotNone(lane.owner_token)
        self.assertEqual(lane.active_turn_id, second.id)
        self.assertEqual(second.claim_token, lane.owner_token)
        return claimed[0], first, second, lane

    async def _assert_lane_kept(self, owner: ConversationTurn, lane: AgentServiceLane) -> None:
        after = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertEqual(
            (after.owner_token, after.active_turn_id, after.active_revision_id, after.lease_until),
            (lane.owner_token, lane.active_turn_id, lane.active_revision_id, lane.lease_until),
        )
        kept = {t.id: t for t in await self._turns()}[owner.id]
        self.assertEqual(kept.claim_token, lane.owner_token)

    # ── R2M2: finalization records the truth after the lease lapsed ─────
    async def test_completion_after_delivery_lease_lapsed_records_delivered(self) -> None:
        await self._ordinary_admitted("r2m2-complete")
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        await self._lapse_delivery_lease(claimed[0].id)
        # The provider accepted the send; the lease lapsed before recording it.
        self.assertTrue(await self._complete(claimed[0]))
        row = (await self._automated())[0]
        self.assertEqual(
            (row.status, row.terminal_reason, row.ambiguity_count, row.lease_token),
            ("delivered", "delivered", 0, None),
        )
        turn = (await self._turns())[0]
        self.assertEqual((turn.status, turn.session_slot), ("delivered", "terminal"))

    async def test_failure_after_delivery_lease_lapsed_is_recorded_once(self) -> None:
        await self._ordinary_admitted("r2m2-fail")
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        await self._lapse_delivery_lease(claimed[0].id)
        self.assertEqual(await self._fail(claimed[0], 5), "retry")
        row = (await self._automated())[0]
        self.assertEqual(
            (row.status, row.last_error_code, row.ambiguity_count, row.lease_token),
            ("retry", "http_503", 0, None),
        )
        self.assertEqual((await self._turns())[0].delivery_attempt_count, 1)

    # ── R2M18: requeue re-checks FIFO under locks ───────────────────────
    async def test_requeue_rechecks_fifo_after_discovery(self) -> None:
        agent, instance = await self._agent("r2m18")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "r2m18-1", "oi")
        async with self.sessions() as db:
            session = (await db.execute(select(Sessao).with_for_update())).scalar_one()
            # An earlier human reply in the same stream that already died.
            await db.execute(
                text(
                    "INSERT INTO outbox_deliveries (delivery_key,cutover_epoch,dedupe_key,"
                    "provider,operation,payload_version,payload,delivery_class,sessao_id,"
                    "ordering_key,stream_sequence,status,terminal_at,terminal_reason,"
                    "last_error_code) VALUES (UUID(),:epoch,UUID(),'evolution',"
                    "'evolution.send_text.v1',1,JSON_OBJECT('synthetic',TRUE),'human',"
                    ":session,:stream,:seq,'dead',UTC_TIMESTAMP(6),'http_500','http_500')"
                ),
                {
                    "epoch": self.delivery_epoch,
                    "session": session.id,
                    "stream": session.delivery_stream_key,
                    "seq": session.next_delivery_sequence,
                },
            )
            session.next_delivery_sequence += 1
            await db.commit()
        await self._force_ready()
        processor = self._router_processor(FakeLLM("depois do humano"))
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        turn = (await self._turns())[0]
        # Discovery (a consistent-read hint) sees a clear stream and lists it.
        self.assertEqual(await self._discovered(), [turn.id])
        human = next(
            row
            for row in await self._rows(OutboxDelivery, OutboxDelivery.id)
            if row.delivery_class == "human"
        )
        # Before the requeue takes its locks, the operator revives the
        # predecessor (sanctioned path: manual retry of a dead human row).
        async with self.sessions() as db:
            await delivery_worker_repo.operator_retry(
                db,
                delivery_id=human.id,
                actor="r3b-operator",
                reason="retry dead human reply",
                ttl_seconds=3600,
            )
            await db.commit()
        self.assertFalse(await self._requeue(turn))
        after = (await self._turns())[0]
        self.assertEqual(after.status, "awaiting_delivery")
        self.assertGreater(after.scheduling_sequence, turn.scheduling_sequence)
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertIsNone(lane.owner_token)
        self.assertEqual(await self._discovered(), [])

    # ── R2M19: requeue re-checks the delivery epoch under locks ─────────
    async def test_requeue_rechecks_delivery_epoch_after_discovery(self) -> None:
        _, _, _, turn = await self._waiting_on_free_lane("r2m19")
        self.assertEqual(await self._discovered(), [turn.id])
        # Rollover between discovery and requeue. Unreachable through the
        # sanctioned cutover (it needs zero non-terminal rows); forced here as
        # in test_stale_delivery_epoch_effect_is_deferred_not_held.
        new_epoch = str(uuid4())
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE delivery_runtime_state SET cutover_epoch=:epoch, "
                    "version=version+1, changed_by='r3b', change_reason='rollover test' "
                    "WHERE id=1"
                ),
                {"epoch": new_epoch},
            )
        self.delivery_epoch = new_epoch
        self.assertFalse(await self._requeue(turn))
        after = (await self._turns())[0]
        self.assertEqual(after.status, "awaiting_delivery")
        self.assertGreater(after.scheduling_sequence, turn.scheduling_sequence)
        self.assertEqual(await self._discovered(), [])

    # ── R2M20: settlement never releases another turn's lane ────────────
    async def test_late_completion_keeps_the_lane_of_another_turn(self) -> None:
        claimed, first, second, lane = await self._lane_taken_by_other_session("r2m20-complete")
        self.assertTrue(await self._complete(claimed))
        settled = {t.id: t for t in await self._turns()}[first.id]
        self.assertEqual((settled.status, settled.session_slot), ("delivered", "terminal"))
        await self._assert_lane_kept(second, lane)

    async def test_late_failure_keeps_the_lane_of_another_turn(self) -> None:
        claimed, first, second, lane = await self._lane_taken_by_other_session("r2m20-fail")
        self.assertEqual(await self._fail(claimed, 5), "retry")
        waiting = {t.id: t for t in await self._turns()}[first.id]
        self.assertEqual(
            (waiting.status, waiting.claim_token, waiting.scheduling_sequence),
            ("awaiting_delivery", None, first.scheduling_sequence),
        )
        await self._assert_lane_kept(second, lane)


# Inherited ticket06/ticket05 cases belong to their own matrices.
for _name in dir(t06.TurnDeliveryMySQLTests):
    if _name.startswith("test_") and _name not in TurnDeliveryRound3MySQLTests.__dict__:
        setattr(TurnDeliveryRound3MySQLTests, _name, None)


if __name__ == "__main__":
    unittest.main()
