"""Ticket 06 real-MySQL barriers: presence, automated admission and caps.

Every provider is an in-process fake. Helpers come from the ticket05 suite;
its own cases are not re-run here.
"""

import asyncio
from datetime import datetime, timedelta
import random
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import event, func, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError

from src.database.models import (
    AgentServiceLane,
    Agente,
    ConversationTurn,
    HandoffAttempt,
    OutboxDelivery,
    ScheduledHandoff,
    Sessao,
    TurnRevision,
)
from src.database.repositories import (
    delivery_worker_repo,
    scheduler_repo,
    turn_delivery_repo,
)
from src.delivery_worker import _automated_hint
from src.services.turn_delivery import FinalCandidateRouter
from src.services.turn_processor import TurnProcessor
from tests import test_handoff_terminal_mysql as base


FakeLLM = base.FakeLLM
ForbiddenLLM = base.ForbiddenLLM
OPEN = base.OPEN
SEND = base.SEND


class FakePresence:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[dict[str, object]] = []
        self.fail = fail

    async def __call__(self, **kwargs) -> int:
        self.calls.append(kwargs)
        if self.fail:
            raise OSError("synthetic presence outage")
        return 201


class TurnDeliveryMySQLTests(base.HandoffTerminalMySQLTests):
    # ── helpers ─────────────────────────────────────────────────────────
    def _router_processor(self, llm, *, summary=None, presence=None) -> TurnProcessor:
        self.presence = presence or FakePresence()
        processor = TurnProcessor(
            self.sessions,
            worker_id=f"ticket06-worker-{uuid4()}",
            scheduler_epoch=self.scheduler_epoch,
            delivery_runtime_epoch=self.delivery_epoch,
            consumer=FinalCandidateRouter(
                self.sessions,
                handoff=self._finalizer(summary or FakeLLM("resumo")),
                ttl_seconds=1800,
            ),
            llm_call=llm,
            presence_call=self.presence,
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

    async def _claim(self) -> list[delivery_worker_repo.ClaimedDelivery]:
        async with self.sessions() as db:
            claimed = await delivery_worker_repo.claim_due(
                db,
                available_slots=20,
                worker_id="ticket06-delivery",
                cutover_epoch=self.delivery_epoch,
                # Production default: longer than the 60s deadline + margin.
                evolution_lease_seconds=90,
                chatwoot_lease_seconds=30,
            )
            await db.commit()
        return claimed

    async def _authorize(self, claimed):
        async with self.sessions() as db:
            authority = await scheduler_repo.lock_automated_delivery_authority(
                db,
                delivery_id=claimed.id,
                delivery_lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                hint=_automated_hint(claimed),
            )
            await db.commit()
        return authority

    async def _preio(self, claimed, *, budget: float = 65) -> bool:
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: turn_delivery_repo.authorize_before_io(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                hint=_automated_hint(claimed),
                io_budget_seconds=budget,
            ),
        )

    async def _complete(self, claimed) -> bool:
        async with self.sessions() as db:
            accepted = await turn_delivery_repo.complete_automated(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                hint=_automated_hint(claimed),
                external_id=f"wamid-{claimed.id}",
                http_status=201,
            )
            await db.commit()
        return accepted

    async def _fail(self, claimed, delay: float | None) -> str:
        async with self.sessions() as db:
            outcome = await turn_delivery_repo.fail_automated(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                hint=_automated_hint(claimed),
                error_code="http_503",
                http_status=503,
                retry_delay_seconds=delay,
                ambiguous=False,
            )
            await db.commit()
        return outcome

    async def _automated(self) -> list[OutboxDelivery]:
        return [
            row
            for row in await self._rows(OutboxDelivery, OutboxDelivery.id)
            if row.delivery_class == "automated"
        ]

    async def _one(self, model, *order):
        rows = await self._rows(model, *order)
        return rows[0]

    async def _ordinary_admitted(self, suffix: str, text_reply: str = "Olá!"):
        agent, instance = await self._agent(suffix)
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, f"{suffix}-1", "oi")
        await self._force_ready()
        processor = self._router_processor(FakeLLM(text_reply))
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        return agent, instance, phone, processor

    async def _make_retry_due(self) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND, "
                    "retry_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE status='retry'"
                )
            )
            await db.execute(
                text(
                    "UPDATE outbox_deliveries SET next_attempt_at=UTC_TIMESTAMP(6) - INTERVAL 1 SECOND "
                    "WHERE status='retry'"
                )
            )
            await db.commit()

    # ── ordinary automated admission ────────────────────────────────────
    async def test_ordinary_reply_admitted_once_and_sent_only_under_lane_fence(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("ordinary", "Olá! Ajudo?")
        rows = await self._automated()
        self.assertEqual(
            [(row.automated_effect, row.status, row.payload["message"]) for row in rows],
            [("reply", "pending", "Olá! Ajudo?")],
        )
        turn = await self._one(ConversationTurn, ConversationTurn.id)
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        revision = await self._one(TurnRevision, TurnRevision.id)
        self.assertEqual((turn.status, revision.status), ("awaiting_delivery", "admitted"))
        self.assertIsNotNone(lane.owner_token)
        self.assertEqual(lane.owner_token, turn.claim_token)
        composing = [c for c in self.presence.calls if c["presence"] == "composing"]
        self.assertEqual(len(composing), 1)
        self.assertTrue(0 < composing[0]["delay_ms"] <= revision.typing_delay_ms + 1000)
        # Arrival after admission joins a successor; the admitted send stands.
        await self._accept(instance, phone, "ordinary-after", "depois da admissão")
        claimed = await self._claim()
        self.assertEqual([row.id for row in claimed], [rows[0].id])
        self.assertIsNotNone(await self._authorize(claimed[0]))
        self.assertTrue(await self._complete(claimed[0]))
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(row.status, row.session_slot) for row in turns],
            [("delivered", "terminal"), ("pending", "active")],
        )
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertIsNone(lane.owner_token)
        self.assertEqual((await self._automated())[0].status, "delivered")

    async def test_effect_identity_is_immutable_and_unique_per_revision(self) -> None:
        await self._ordinary_admitted("identity")
        reply = (await self._automated())[0]
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, "authority is immutable"):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE outbox_deliveries SET automated_effect='handoff_transition' "
                            "WHERE id=:id"
                        ),
                        {"id": reply.id},
                    )

        async def copy(effect, suffix: str) -> None:
            async with self.sessions() as db:
                async with db.begin():
                    await db.execute(
                        text(
                            "INSERT INTO outbox_deliveries (delivery_key,cutover_epoch,dedupe_key,"
                            "provider,operation,payload_version,payload,delivery_class,"
                            "automated_effect,scheduler_epoch,conversation_turn_id,"
                            "turn_revision_id,agente_id,sessao_id,ordering_key,stream_sequence,"
                            "status,next_attempt_at,expires_at) "
                            "SELECT UUID(),cutover_epoch,CONCAT(dedupe_key,:suffix),provider,"
                            "operation,payload_version,payload,delivery_class,:effect,"
                            "scheduler_epoch,conversation_turn_id,turn_revision_id,agente_id,"
                            "sessao_id,ordering_key,stream_sequence+:offset,status,"
                            "next_attempt_at,expires_at FROM outbox_deliveries WHERE id=:id"
                        ),
                        {
                            "suffix": suffix,
                            "effect": effect,
                            "offset": 10 + len(suffix),
                            "id": reply.id,
                        },
                    )

        with self.assertRaisesRegex(IntegrityError, r"1062.*uq_outbox_turn_revision_effect"):
            await copy("reply", ":dup")
        with self.assertRaisesRegex(DBAPIError, r'3819.*ck_outbox_delivery_authority_v2'):
            await copy(None, ":null")
        await copy("handoff_transition", ":transition")
        self.assertEqual(
            sorted(r.automated_effect for r in await self._automated()),
            ["handoff_transition", "reply"],
        )

    async def test_claim_skips_automated_rows_without_live_lane_fence(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("claim-fence")
        async with self.sessions() as db:
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == agent)
                .values(lease_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        self.assertEqual(await self._claim(), [])
        self.assertEqual([row.status for row in await self._automated()], ["pending"])

    async def test_committed_claim_is_rejected_before_io_after_lane_loss(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("preio")
        claimed = await self._claim()
        self.assertEqual(len(claimed), 1)
        async with self.sessions() as db:
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == agent)
                .values(owner_token=str(uuid4()))
            )
            await db.commit()
        self.assertIsNone(await self._authorize(claimed[0]))
        before = turn_delivery_repo.preio_release_count()
        self.assertFalse(await self._preio(claimed[0]))
        self.assertEqual(turn_delivery_repo.preio_release_count(), before + 1)
        self.assertFalse(await self._complete(claimed[0]))
        row = (await self._automated())[0]
        # No I/O happened: released, never counted as an ambiguous send (A-5).
        self.assertEqual(
            (row.status, row.ambiguity_count, row.lease_token, row.last_error_code),
            ("retry", 0, None, "preio_refused_lane"),
        )
        async with self.sessions() as db:
            now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        # X4: positive backoff, never an immediate re-claim.
        self.assertGreater(row.next_attempt_at, now)
        turn = await self._one(ConversationTurn, ConversationTurn.id)
        self.assertEqual(turn.status, "awaiting_delivery")

    async def test_failed_send_yields_to_tail_and_other_agents_progress(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("fail-a")
        await self._accept(instance, phone, "fail-a-after", "sucessor")
        before = await self._one(ConversationTurn, ConversationTurn.id)
        lane_before = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        claimed = await self._claim()
        async with self.sessions() as db:
            failed_at = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        # Zero backoff is clamped: a failed send never retries immediately.
        self.assertEqual(await self._fail(claimed[0], 0), "retry")
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        failed, successor = turns
        self.assertEqual(failed.status, "retry")
        self.assertGreaterEqual(failed.retry_at, failed_at + timedelta(seconds=1))
        self.assertGreater(failed.scheduling_sequence, before.scheduling_sequence)
        self.assertEqual(failed.delivery_attempt_count, before.delivery_attempt_count + 1)
        self.assertEqual((successor.status, successor.session_slot), ("pending", "successor"))
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertIsNone(lane.owner_token)
        self.assertGreater(lane.next_scheduling_sequence, lane_before.next_scheduling_sequence)
        row = (await self._automated())[0]
        self.assertEqual(row.status, "retry")
        self.assertEqual(row.next_attempt_at, failed.retry_at)
        # The failed contact's successor is ineligible while it backs off.
        claim = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.claim_ready_turn(
                db,
                agente_id=agent,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
                worker_id="ticket06-successor",
                lease_seconds=30,
            ),
        )
        self.assertIsNone(claim)
        # An independent agent progresses while agent A backs off.
        other, other_instance = await self._agent("fail-b")
        await self._accept(other_instance, f"5511{other:08d}", "fail-b-1", "oi")
        await self._force_ready()
        processor_b = self._router_processor(FakeLLM("resposta B"))
        self.assertEqual(await self._run_one(processor_b), ["final_candidate"])
        claimed_b = await self._claim()
        self.assertEqual([row.agente_id for row in claimed_b], [other])
        self.assertTrue(await self._complete(claimed_b[0]))
        # After the backoff the same revision is re-admitted, never regenerated.
        await self._make_retry_due()
        processor_a = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor_a), ["final_candidate"])
        claimed = await self._claim()
        self.assertEqual([row.id for row in claimed], [row.id])
        self.assertTrue(await self._complete(claimed[0]))
        turns = await self._rows(ConversationTurn, ConversationTurn.agente_id, ConversationTurn.id)
        self.assertEqual(
            [(t.agente_id, t.status, t.session_slot) for t in turns],
            [
                (agent, "delivered", "terminal"),
                (agent, "pending", "active"),
                (other, "delivered", "terminal"),
            ],
        )

    async def test_presence_failure_preserves_durable_text(self) -> None:
        agent, instance = await self._agent("presence-fail")
        await self._accept(instance, f"5511{agent:08d}", "presence-1", "oi")
        await self._force_ready()
        processor = self._router_processor(
            FakeLLM("texto durável"), presence=FakePresence(fail=True)
        )
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        self.assertEqual(processor.presence_failure_count, 1)
        self.assertEqual(
            [(r.status, r.payload["message"]) for r in await self._automated()],
            [("pending", "texto durável")],
        )

    async def test_restart_reuses_persisted_typing_deadline(self) -> None:
        agent, instance = await self._agent("restart")
        await self._accept(instance, f"5511{agent:08d}", "restart-1", "oi")
        await self._force_ready()
        first = self._router_processor(FakeLLM("resposta"))
        original_wait = first._wait
        paused = asyncio.Event()

        async def stop_at_typing(lease, step):
            if step.phase == "typing":
                paused.set()
                first._stopping.set()
                return
            await original_wait(lease, step)

        first._wait = stop_at_typing
        result = await self._run_one(first)
        self.assertTrue(paused.is_set())
        self.assertEqual(result, ["released_shutdown"])
        revision = await self._one(TurnRevision, TurnRevision.id)
        persisted = (revision.typing_delay_ms, revision.typing_jitter_ms, revision.typing_deadline_at)
        await self._make_retry_due()
        second = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(second), ["final_candidate"])
        revision = await self._one(TurnRevision, TurnRevision.id)
        self.assertEqual(
            (revision.typing_delay_ms, revision.typing_jitter_ms, revision.typing_deadline_at),
            persisted,
        )
        composing = [c for c in self.presence.calls if c["presence"] == "composing"]
        self.assertTrue(all(c["delay_ms"] <= 12000 for c in composing))

    # ── admission recovery under the lane ───────────────────────────────
    async def test_expired_processing_effect_is_recovered_by_lane_admission(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("expired-claim")
        claimed = await self._claim()
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.id == claimed[0].id)
                .values(lock_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        # Generic maintenance never touches automated rows; it only signals.
        async with self.sessions() as db:
            maintenance = await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        self.assertEqual(maintenance.automated_skipped, 1)
        self.assertEqual([r.status for r in await self._automated()], ["processing"])
        await self._expire_and_reclaim(self._router_processor(ForbiddenLLM()), agent)
        await self._make_retry_due()
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        row = (await self._automated())[0]
        self.assertEqual(
            (row.status, row.ambiguity_count, row.last_error_code, row.lease_token),
            ("retry", 1, "lease_expired_ambiguous", None),
        )
        claimed = await self._claim()
        self.assertEqual([c.id for c in claimed], [row.id])
        self.assertTrue(await self._complete(claimed[0]))

    async def test_expired_effect_at_admission_terminalizes_ordinary_turn(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("expired-effect")
        await self._accept(instance, phone, "expired-after", "sucessor")
        await self._expire_and_reclaim(self._router_processor(ForbiddenLLM()), agent)
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.delivery_class == "automated")
                .values(expires_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        await self._make_retry_due()
        before = turn_delivery_repo.terminal_at_admission_count()
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        self.assertEqual(turn_delivery_repo.terminal_at_admission_count(), before + 1)
        row = (await self._automated())[0]
        self.assertEqual((row.status, row.terminal_reason), ("dead", "expired"))
        turns = await self._rows(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            [(t.status, t.session_slot) for t in turns],
            [("dead", "terminal"), ("pending", "active")],
        )
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertIsNone(lane.owner_token)

    # ── option C: handoff reply then transition ─────────────────────────
    async def _admit_open(self, open_claim) -> None:
        async with self.sessions() as db:
            accepted = await delivery_worker_repo.complete_chatwoot_handoff(
                db,
                delivery_id=open_claim.id,
                lease_token=open_claim.lease_token,
                cutover_epoch=open_claim.cutover_epoch,
                contact_id=41,
                conversation_id=42,
                http_status=200,
                transition_ttl_seconds=1800,
            )
            await db.commit()
        self.assertTrue(accepted)

    async def test_handoff_reply_precedes_transition_released_only_after_open(self) -> None:
        agent, _, _, _, _ = await self._run_handoff("option-c")
        reply, transition = await self._automated()
        self.assertEqual(
            [(r.automated_effect, r.status) for r in (reply, transition)],
            [("reply", "pending"), ("handoff_transition", "blocked")],
        )
        self.assertEqual(reply.payload["message"], "Vou transferir você.")
        self.assertLess(reply.stream_sequence, transition.stream_sequence)
        self.assertNotIn("[TRANSFERIR]", reply.payload["message"])
        # Lane released by ticket05 admission: only the open is claimable.
        claimed = await self._claim()
        self.assertEqual([c.operation for c in claimed], [OPEN])
        open_claim = claimed[0]
        # The reply is admitted under the lane without waiting for the open.
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._claim()
        self.assertEqual([(c.id, c.automated_effect) for c in claimed], [(reply.id, "reply")])
        self.assertTrue(await self._complete(claimed[0]))
        turn = await self._one(ConversationTurn, ConversationTurn.id)
        self.assertEqual(turn.status, "awaiting_delivery")
        # Transition is not discoverable while the open is still processing.
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=10
            )
        self.assertEqual(ready, [])
        self.assertEqual((await self._automated())[1].status, "blocked")
        await self._admit_open(open_claim)
        self.assertEqual((await self._automated())[1].status, "blocked")
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        rows = await self._automated()
        self.assertEqual([r.status for r in rows], ["delivered", "pending"])
        claimed = [c for c in await self._claim() if c.delivery_class == "automated"]
        self.assertEqual([c.automated_effect for c in claimed], ["handoff_transition"])
        self.assertTrue(await self._complete(claimed[0]))
        turn = await self._one(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("handoff", "terminal", "handoff_delivered"),
        )
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        session = await self._one(Sessao, Sessao.id)
        self.assertEqual(session.handler, "encerrada")

    async def test_handoff_transition_waits_for_reply_delivery(self) -> None:
        await self._run_handoff("reply-first")
        open_claim = (await self._claim())[0]
        await self._admit_open(open_claim)
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = [c for c in await self._claim() if c.delivery_class == "automated"]
        self.assertEqual([c.automated_effect for c in claimed], ["reply"])
        self.assertEqual(await self._fail(claimed[0], 5), "retry")
        rows = await self._automated()
        self.assertEqual([r.status for r in rows], ["retry", "blocked"])
        await self._make_retry_due()
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = [c for c in await self._claim() if c.delivery_class == "automated"]
        self.assertEqual([c.automated_effect for c in claimed], ["reply"])

    async def test_transfer_only_handoff_delivers_transition_without_reply(self) -> None:
        await self._run_handoff("transfer-only", response=FakeLLM("[TRANSFERIR]"))
        rows = await self._automated()
        self.assertEqual([r.automated_effect for r in rows], ["handoff_transition"])
        open_claim = (await self._claim())[0]
        await self._admit_open(open_claim)
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = [c for c in await self._claim() if c.delivery_class == "automated"]
        self.assertEqual([c.automated_effect for c in claimed], ["handoff_transition"])
        self.assertTrue(await self._complete(claimed[0]))

    async def test_dead_handoff_reply_parks_and_never_releases_transition(self) -> None:
        await self._run_handoff("reply-dead")
        open_claim = (await self._claim())[0]
        await self._admit_open(open_claim)
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = [c for c in await self._claim() if c.delivery_class == "automated"]
        self.assertEqual(await self._fail(claimed[0], None), "parked")
        rows = await self._automated()
        self.assertEqual([r.status for r in rows], ["dead", "blocked"])
        turn = await self._one(ConversationTurn, ConversationTurn.id)
        self.assertEqual((turn.status, turn.session_slot), ("awaiting_delivery", "active"))
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=10
            )
        self.assertEqual(ready, [])
        async with self.sessions() as db:
            with self.assertRaisesRegex(ValueError, "lane-authorized"):
                await delivery_worker_repo.operator_retry(
                    db, delivery_id=rows[0].id, actor="ops", reason="try", ttl_seconds=600
                )

    async def test_handoff_discovery_is_bounded_and_skips_stuck_rows(self) -> None:
        # limit+1 parked handoffs whose reply is dead never fill the page;
        # the eligible victim is still discovered.
        for index in range(3):
            await self._run_handoff(f"stuck-{index}")
            if index < 2:
                async with self.sessions() as db:
                    await db.execute(
                        update(OutboxDelivery)
                        .where(
                            OutboxDelivery.automated_effect == "reply",
                            OutboxDelivery.status == "pending",
                        )
                        .values(
                            status="dead",
                            terminal_at=func.utc_timestamp(6),
                            terminal_reason="synthetic",
                        )
                    )
                    await db.commit()
        rows = await self._automated()
        replies = [r for r in rows if r.automated_effect == "reply"]
        self.assertEqual([r.status for r in replies], ["dead", "dead", "pending"])
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=2
            )
        # Exclusion, not ordering: the dead-reply turns never appear at all.
        self.assertEqual([turn_id for turn_id, _, _ in ready], [replies[2].conversation_turn_id])

    # ── R5-a: admitted turns at the recovery cap ────────────────────────
    async def test_admitted_handoff_expired_lane_cap_parks_without_orphaning(self) -> None:
        agent, _, _, _, _ = await self._run_handoff("expired-cap")
        processor = self._router_processor(ForbiddenLLM())
        original = processor._consumer

        async def hold(candidate, handle):
            return None

        processor._consumer = hold
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        processor._consumer = original
        await self._set_active_turn_at_cap(agent, expire=True)
        before = scheduler_repo.admitted_handoff_park_signal_count()
        self.assertEqual(await self._recover_cap(agent), "idle")
        self.assertEqual(scheduler_repo.admitted_handoff_park_signal_count(), before + 1)
        turn = await self._one(ConversationTurn, ConversationTurn.id)
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("awaiting_delivery", "active", None),
        )
        self.assertEqual((await self._one(TurnRevision, TurnRevision.id)).status, "admitted")
        self.assertEqual(await self._count(ScheduledHandoff), 1)
        self.assertEqual(
            [(r.automated_effect, r.status) for r in await self._automated()],
            [("reply", "pending"), ("handoff_transition", "blocked")],
        )
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertIsNone(lane.owner_token)

    # ── P3-R7-a: only a generated rev2 overrides carried handoff intent ─
    async def _cap_refresh_then_rev2_draft(
        self, suffix: str, *, recovery: bool, draft_kind: str
    ):
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
        turn_id, owner_token = await self._set_active_turn_at_cap(agent, expire=recovery)
        if recovery:
            self.assertEqual(await self._recover_cap(agent), "retry")
        else:
            self.assertTrue(await self._yield_cap(agent, turn_id, owner_token))
        release.set()
        await asyncio.wait_for(task, timeout=20)
        claim = await self._claim_preserved_refresh(agent)
        self.assertIsNotNone(claim)
        async with self.sessions() as db:
            await db.execute(
                update(TurnRevision)
                .where(TurnRevision.revision_number == 2)
                .values(
                    status="draft",
                    draft_kind=draft_kind,
                    draft_text="fallback ou novo texto",
                    draft_text_bytes=len("fallback ou novo texto".encode()),
                    needs_handoff=False,
                    generation_completed_at=func.utc_timestamp(6),
                )
            )
            await db.commit()
        revisions = await self._rows(TurnRevision, TurnRevision.id)
        self.assertEqual(
            [(r.revision_number, r.status, r.superseded_reason) for r in revisions],
            [(1, "superseded", "refresh_input_at_scheduler_cap"), (2, "draft", None)],
        )
        if recovery:
            await self._set_active_turn_at_cap(agent, expire=True)
            outcome = await self._recover_cap(agent)
            self.assertEqual(outcome, "dead")
        else:
            self.assertFalse(await self._yield_cap(agent, claim.turn_id, claim.claim_token))
        return await self._one(ConversationTurn, ConversationTurn.id), await self._one(
            Sessao, Sessao.id
        )

    async def _assert_fallback_keeps_handoff(self, recovery: bool, kind: str) -> None:
        turn, session = await self._cap_refresh_then_rev2_draft(
            f"p3-{kind}-{'rec' if recovery else 'yield'}", recovery=recovery, draft_kind=kind
        )
        self.assertEqual(
            (turn.status, turn.terminal_reason), ("dead", "handoff_recovery_cap")
        )
        self.assertEqual(
            (session.handler, session.motivo_encerramento), ("encerrada", "falha_tecnica")
        )

    async def test_recovery_cap_rev2_technical_fallback_keeps_handoff(self) -> None:
        await self._assert_fallback_keeps_handoff(True, "technical_fallback")

    async def test_recovery_cap_rev2_soft_limit_fallback_keeps_handoff(self) -> None:
        await self._assert_fallback_keeps_handoff(True, "soft_limit_fallback")

    async def test_yield_cap_rev2_technical_fallback_keeps_handoff(self) -> None:
        await self._assert_fallback_keeps_handoff(False, "technical_fallback")

    async def test_yield_cap_rev2_soft_limit_fallback_keeps_handoff(self) -> None:
        await self._assert_fallback_keeps_handoff(False, "soft_limit_fallback")

    async def test_recovery_cap_rev2_generated_overrides_carried_handoff(self) -> None:
        turn, session = await self._cap_refresh_then_rev2_draft(
            "p3-generated-rec", recovery=True, draft_kind="generated"
        )
        self.assertEqual(
            (turn.status, turn.terminal_reason), ("dead", "lease_recovery_attempt_cap")
        )
        self.assertEqual(session.handler, "bot")

    async def test_yield_cap_rev2_generated_overrides_carried_handoff(self) -> None:
        turn, session = await self._cap_refresh_then_rev2_draft(
            "p3-generated-yield", recovery=False, draft_kind="generated"
        )
        self.assertEqual((turn.status, turn.terminal_reason), ("dead", "delivery_attempt_cap"))
        self.assertEqual(session.handler, "bot")


    # ── review round 1 (P2-1..P2-4, review-B mutations) ─────────────────
    async def _expire_lane(self, agente_id: int) -> None:
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

    async def _turns(self) -> list[ConversationTurn]:
        return await self._rows(ConversationTurn, ConversationTurn.id)

    async def test_lane_expiries_waiting_for_claim_never_spend_the_cap(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("wait-claim")
        await self._accept(instance, phone, "wait-claim-after", "sucessor")
        processor = self._router_processor(ForbiddenLLM())
        for _ in range(10):  # more expiries than max_delivery_attempts_per_turn=8
            await self._expire_lane(agent)
            self.assertEqual(await self._recover_cap(agent), "idle")
            turn = (await self._turns())[0]
            self.assertEqual(
                (turn.status, turn.delivery_attempt_count, turn.claim_token),
                ("awaiting_delivery", 0, None),
            )
            # Discovery re-admits the waiting turn; nothing is regenerated.
            self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        self.assertTrue(await self._complete(claimed[0]))
        self.assertEqual(
            [(t.status, t.session_slot, t.delivery_attempt_count) for t in await self._turns()],
            [("delivered", "terminal", 0), ("pending", "active", 0)],
        )

    async def test_real_send_failures_at_cap_kill_ordinary_turn_and_promote(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("fail-cap")
        await self._accept(instance, phone, "fail-cap-after", "sucessor")
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.session_slot == "active")
                .values(delivery_attempt_count=6)
            )
            await db.commit()
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        self.assertEqual(await self._fail(claimed[0], 5), "retry")  # count 7 < 8
        self.assertEqual((await self._turns())[0].delivery_attempt_count, 7)
        await self._make_retry_due()
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        self.assertEqual(await self._fail(claimed[0], 5), "dead")  # count 8 == cap
        self.assertEqual(
            [(t.status, t.session_slot, t.terminal_reason) for t in await self._turns()],
            [("dead", "terminal", "delivery_attempt_cap"), ("pending", "active", None)],
        )
        row = (await self._automated())[0]
        self.assertEqual((row.status, row.terminal_reason), ("dead", "automated_delivery_attempt_cap"))

    async def test_discovery_skips_busy_lanes_and_finds_the_victim(self) -> None:
        busy = []
        for index in range(3):  # limit + 1 rows that cannot progress
            agent, _, _, _ = await self._ordinary_admitted(f"busy-{index}")
            busy.append(agent)
        victim, _, _, _ = await self._ordinary_admitted("victim")
        await self._expire_lane(victim)
        self.assertEqual(await self._recover_cap(victim), "idle")
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=2
            )
        self.assertEqual([agente for _, agente, _ in ready], [victim])
        turns = {t.agente_id: t for t in await self._turns()}
        before = turns[busy[0]].scheduling_sequence
        async with self.sessions() as db:
            requeued = await turn_delivery_repo.requeue_admitted_turn(
                db,
                turn_id=turns[busy[0]].id,
                agente_id=busy[0],
                sessao_id=turns[busy[0]].sessao_id,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
            )
            await db.commit()
        self.assertFalse(requeued)
        after = {t.agente_id: t for t in await self._turns()}[busy[0]]
        self.assertEqual((after.status, after.scheduling_sequence), ("awaiting_delivery", before))

    async def test_requeue_rotates_an_ineligible_turn_on_a_free_lane(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("rotate")
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.delivery_class == "automated")
                .values(
                    status="processing",
                    lease_token=str(uuid4()),
                    lock_owner="other-worker",
                    lock_until=func.utc_timestamp(6) + text("INTERVAL 60 SECOND"),
                )
            )
            await db.commit()
        turn = (await self._turns())[0]
        async with self.sessions() as db:
            requeued = await turn_delivery_repo.requeue_admitted_turn(
                db,
                turn_id=turn.id,
                agente_id=agent,
                sessao_id=turn.sessao_id,
                scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch,
            )
            await db.commit()
        self.assertFalse(requeued)
        rotated = (await self._turns())[0]
        self.assertEqual(rotated.status, "awaiting_delivery")
        self.assertGreater(rotated.scheduling_sequence, turn.scheduling_sequence)

    async def _resume_session(self) -> None:
        from src.database.repositories import handoff_attempt_repo, outbox_repo

        async with self.sessions() as db:
            session = (
                await db.execute(select(Sessao).with_for_update())
            ).scalar_one()
            await handoff_attempt_repo.supersede_current_and_resume(db, sessao=session)
            await outbox_repo.request_cancellation_for_session(db, sessao_id=session.id)
            await db.commit()

    async def test_resume_between_claim_and_preio_never_sends_the_reply(self) -> None:
        await self._run_handoff("resume-reply")
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = [c for c in await self._claim() if c.delivery_class == "automated"]
        self.assertEqual([c.automated_effect for c in claimed], ["reply"])
        await self._resume_session()
        self.assertFalse(await self._preio(claimed[0]))
        self.assertFalse(await self._complete(claimed[0]))
        reply = (await self._automated())[0]
        self.assertEqual(
            (reply.status, reply.terminal_reason, reply.ambiguity_count),
            ("cancelled", "cancel_requested", 0),
        )

    async def test_preio_refuses_every_lifecycle_change(self) -> None:
        cases = {
            "ordinary-cancel": "UPDATE outbox_deliveries SET cancel_requested_at=UTC_TIMESTAMP(6)",
            "ordinary-session-closed": (
                "UPDATE sessoes SET handler='encerrada', "
                "motivo_encerramento='transferencia_manual', encerrada_em=UTC_TIMESTAMP(6)"
            ),
            "ordinary-agent-inactive": "UPDATE agentes SET ativo=FALSE",
        }
        for label, statement in cases.items():
            with self.subTest(case=label):
                await self._wipe_and_reset()
                await self._ordinary_admitted(label)
                claimed = await self._claim()
                async with self.sessions() as db:
                    await db.execute(text(statement))
                    await db.commit()
                self.assertFalse(await self._preio(claimed[0]))
                row = (await self._automated())[0]
                self.assertEqual((row.status, row.ambiguity_count), ("cancelled", 0))

    async def _wipe_and_reset(self) -> None:
        for processor in self.processors:
            await processor.shutdown(0.5)
        self.processors.clear()
        await self._wipe()

    async def test_preio_extends_lane_past_the_io_deadline(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("lane-extend")
        async with self.sessions() as db:
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == agent)
                .values(lease_until=func.utc_timestamp(6) + text("INTERVAL 2 SECOND"))
            )
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.agente_id == agent)
                .values(claim_until=func.utc_timestamp(6) + text("INTERVAL 2 SECOND"))
            )
            await db.commit()
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0], budget=30))
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        turn = (await self._turns())[0]
        async with self.sessions() as db:
            now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        self.assertGreaterEqual(lane.lease_until, now + timedelta(seconds=29))
        self.assertEqual(turn.claim_until, lane.lease_until)
        await asyncio.sleep(2.2)  # I/O outlives the lease left at claim time
        with self.assertRaises(scheduler_repo.SchedulerConflictError):
            await self._recover_cap(agent)
        self.assertTrue(await self._complete(claimed[0]))
        rows = await self._automated()
        self.assertEqual([(r.status, r.ambiguity_count) for r in rows], [("delivered", 0)])

    async def test_renewal_extends_lane_and_never_shortens_it(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("renew")
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0], budget=30))
        async with self.sessions() as db:
            await db.execute(
                update(AgentServiceLane)
                .where(AgentServiceLane.agente_id == agent)
                .values(lease_until=func.utc_timestamp(6) + text("INTERVAL 1 SECOND"))
            )
            await db.commit()

        async def renew(lane_seconds: int) -> bool:
            return await scheduler_repo.run_transaction_with_deadlock_retry(
                self.sessions,
                lambda db: turn_delivery_repo.renew_automated_authority(
                    db,
                    delivery_id=claimed[0].id,
                    lease_token=claimed[0].lease_token,
                    cutover_epoch=claimed[0].cutover_epoch,
                    hint=_automated_hint(claimed[0]),
                    delivery_lease_seconds=90,
                    lane_lease_seconds=lane_seconds,
                ),
            )

        self.assertTrue(await renew(30))
        async with self.sessions() as db:
            now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        extended = (await self._one(AgentServiceLane, AgentServiceLane.agente_id)).lease_until
        self.assertGreaterEqual(extended, now + timedelta(seconds=29))
        self.assertTrue(await renew(5))
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertGreaterEqual(lane.lease_until, extended)
        self.assertEqual((await self._turns())[0].claim_until, lane.lease_until)

    async def test_authority_requires_awaiting_delivery_and_matching_effect(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("fence-status")
        claimed = await self._claim()
        wrong = _automated_hint(claimed[0])
        from dataclasses import replace

        async with self.sessions() as db:
            self.assertIsNone(
                await scheduler_repo.lock_automated_delivery_authority(
                    db,
                    delivery_id=claimed[0].id,
                    delivery_lease_token=claimed[0].lease_token,
                    cutover_epoch=claimed[0].cutover_epoch,
                    hint=replace(wrong, automated_effect="handoff_transition"),
                )
            )
            await db.rollback()
        self.assertIsNotNone(await self._authorize(claimed[0]))
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.agente_id == agent)
                .values(status="claimed")
            )
            await db.commit()
        self.assertIsNone(await self._authorize(claimed[0]))

    async def test_claim_requires_live_turn_claim(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("claim-until")
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.agente_id == agent)
                .values(claim_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        self.assertEqual(await self._claim(), [])

    async def _claimed_transition(self, suffix: str):
        await self._run_handoff(suffix)
        open_claim = (await self._claim())[0]
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        reply = [c for c in await self._claim() if c.delivery_class == "automated"][0]
        self.assertTrue(await self._preio(reply))
        self.assertTrue(await self._complete(reply))
        await self._admit_open(open_claim)
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        transition = [c for c in await self._claim() if c.delivery_class == "automated"][0]
        self.assertEqual(transition.automated_effect, "handoff_transition")
        return transition, open_claim

    async def test_transition_authority_requires_open_delivered(self) -> None:
        transition, open_claim = await self._claimed_transition("fence-open")
        self.assertIsNotNone(await self._authorize(transition))
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.id == open_claim.id)
                .values(status="dead", terminal_reason="synthetic")
            )
            await db.commit()
        self.assertIsNone(await self._authorize(transition))

    async def test_transition_authority_requires_ready_attempt(self) -> None:
        transition, _ = await self._claimed_transition("fence-ready")
        async with self.sessions() as db:
            await db.execute(
                update(HandoffAttempt).values(status="pending", ready_at=None)
            )
            await db.commit()
        self.assertIsNone(await self._authorize(transition))

    async def _admitted_handoff_reclaim(self, suffix: str, statement: str) -> list[str]:
        await self._run_handoff(suffix)
        async with self.sessions() as db:
            await db.execute(text(statement))
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
        return await self._run_one(self._router_processor(ForbiddenLLM()))

    async def test_bypass_requires_current_handoff_generation(self) -> None:
        self.assertEqual(
            await self._admitted_handoff_reclaim(
                "bypass-gen", "UPDATE sessoes SET handoff_generation=handoff_generation+1"
            ),
            ["lifecycle_cancelled"],
        )
        # Ticket07: the superseded handoff is cancelled formally, not released.
        self.assertEqual([r.status for r in await self._automated()], ["cancelled", "cancelled"])
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        self.assertEqual((turn.status, turn.terminal_reason), ("cancelled", "handoff_superseded"))

    async def test_bypass_requires_pending_or_ready_attempt(self) -> None:
        self.assertEqual(
            await self._admitted_handoff_reclaim(
                "bypass-attempt",
                "UPDATE handoff_attempts SET status='superseded', terminal_at=UTC_TIMESTAMP(6)",
            ),
            ["lifecycle_cancelled"],
        )
        # Ticket07: the superseded handoff is cancelled formally, not released.
        self.assertEqual([r.status for r in await self._automated()], ["cancelled", "cancelled"])
        turn = (await self._rows(ConversationTurn, ConversationTurn.id))[0]
        self.assertEqual((turn.status, turn.terminal_reason), ("cancelled", "handoff_superseded"))

    async def test_maintenance_never_expires_or_cascades_automated_rows(self) -> None:
        await self._run_handoff("maintenance")
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.automated_effect == "reply")
                .values(expires_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.operation == OPEN)
                .values(status="dead", terminal_at=func.utc_timestamp(6), terminal_reason="synthetic")
            )
            await db.commit()
        async with self.sessions() as db:
            result = await delivery_worker_repo.run_preclaim_maintenance(db)
            await db.commit()
        self.assertEqual(
            [(r.automated_effect, r.status) for r in await self._automated()],
            [("reply", "pending"), ("handoff_transition", "blocked")],
        )
        self.assertEqual(result.automated_skipped, 2)

    async def test_terminal_effect_at_handoff_admission_parks_not_kills(self) -> None:
        await self._run_handoff("handoff-expired")
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.automated_effect == "reply")
                .values(expires_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()
        before = turn_delivery_repo.terminal_at_admission_count()
        self.assertEqual(
            await self._run_one(self._router_processor(ForbiddenLLM())), ["final_candidate"]
        )
        self.assertEqual(turn_delivery_repo.terminal_at_admission_count(), before + 1)
        turn = (await self._turns())[0]
        self.assertEqual((turn.status, turn.session_slot), ("awaiting_delivery", "active"))
        self.assertEqual(
            [(r.automated_effect, r.status) for r in await self._automated()],
            [("reply", "dead"), ("handoff_transition", "blocked")],
        )
        self.assertEqual(await self._count(ScheduledHandoff), 1)

    async def test_ordinary_admitted_at_cap_on_lane_expiry_dies_and_promotes(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("expiry-cap")
        await self._accept(instance, phone, "expiry-cap-after", "sucessor")
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.session_slot == "active")
                .values(delivery_attempt_count=8)
            )
            await db.commit()
        await self._expire_lane(agent)
        before = scheduler_repo.admitted_handoff_park_signal_count()
        self.assertEqual(await self._recover_cap(agent), "dead")
        # Never parked: no park signal, reply terminal, successor promoted.
        self.assertEqual(scheduler_repo.admitted_handoff_park_signal_count(), before)
        self.assertEqual(
            [(t.status, t.session_slot, t.terminal_reason) for t in await self._turns()],
            [("dead", "terminal", "delivery_attempt_cap"), ("pending", "active", None)],
        )
        self.assertEqual(
            [(r.status, r.terminal_reason) for r in await self._automated()],
            [("cancelled", "delivery_attempt_cap")],
        )

    async def test_preio_refuses_superseded_handoff_reply(self) -> None:
        cases = {
            "attempt-superseded": (
                "UPDATE handoff_attempts SET status='superseded', terminal_at=UTC_TIMESTAMP(6)"
            ),
            "generation-bumped": "UPDATE sessoes SET handoff_generation=handoff_generation+1",
            "session-resumed": (
                "UPDATE sessoes SET handler='bot', motivo_encerramento=NULL, encerrada_em=NULL"
            ),
        }
        for label, statement in cases.items():
            with self.subTest(case=label):
                await self._wipe_and_reset()
                await self._run_handoff(f"reply-{label}")
                processor = self._router_processor(ForbiddenLLM())
                self.assertEqual(await self._run_one(processor), ["final_candidate"])
                claimed = [c for c in await self._claim() if c.delivery_class == "automated"]
                self.assertEqual([c.automated_effect for c in claimed], ["reply"])
                async with self.sessions() as db:
                    await db.execute(text(statement))
                    await db.commit()
                self.assertFalse(await self._preio(claimed[0]))
                reply = (await self._automated())[0]
                self.assertEqual(
                    (reply.status, reply.terminal_reason, reply.ambiguity_count),
                    ("cancelled", "lifecycle_refused", 0),
                )

    async def test_admission_defers_instead_of_holding_the_lane(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("defer")
        claimed = await self._claim()  # a delivery worker owns the reply lease
        self.assertEqual(len(claimed), 1)
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.agente_id == agent)
                .values(
                    status="retry",
                    ready_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                    retry_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                )
            )
            await db.commit()
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        turn = (await self._turns())[0]
        self.assertIsNone(lane.owner_token)
        self.assertEqual((turn.status, turn.delivery_attempt_count), ("awaiting_delivery", 0))
        self.assertEqual((await self._automated())[0].status, "processing")

    async def test_discovery_requires_open_delivered_for_transition(self) -> None:
        await self._run_handoff("discover-open")
        open_claim = (await self._claim())[0]
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        reply = [c for c in await self._claim() if c.delivery_class == "automated"][0]
        self.assertTrue(await self._preio(reply))
        self.assertTrue(await self._complete(reply))
        await self._admit_open(open_claim)

        async def discovered() -> list[int]:
            async with self.sessions() as db:
                return [
                    turn_id
                    for turn_id, _, _ in await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                        db, scheduler_epoch=self.scheduler_epoch, limit=10
                    )
                ]

        turn = (await self._turns())[0]
        self.assertEqual(await discovered(), [turn.id])
        # Attempt stays ready; only the open's own status changes.
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.id == open_claim.id)
                .values(status="dead", terminal_reason="synthetic")
            )
            await db.commit()
        self.assertEqual(await discovered(), [])

    async def test_transition_authority_refuses_a_reply_hint(self) -> None:
        transition, _ = await self._claimed_transition("hint-effect")
        from dataclasses import replace

        self.assertIsNotNone(await self._authorize(transition))
        async with self.sessions() as db:
            self.assertIsNone(
                await scheduler_repo.lock_automated_delivery_authority(
                    db,
                    delivery_id=transition.id,
                    delivery_lease_token=transition.lease_token,
                    cutover_epoch=transition.cutover_epoch,
                    hint=replace(_automated_hint(transition), automated_effect="reply"),
                )
            )
            await db.rollback()

    # ── review round 2: X1/X2/X4, N6/N12, B suspicions ──────────────────
    async def _transfer_session(self) -> None:
        from src.database.repositories import sessao_repo

        async with self.sessions() as db:
            session = (await db.execute(select(Sessao))).scalar_one()
            await sessao_repo.encerrar_sessao(db, session.id, "transferencia_manual")
            await db.commit()

    async def _set_agent(self, **values) -> None:
        async with self.sessions() as db:
            await db.execute(update(Agente).values(**values))
            await db.commit()

    async def _automated_claims(self) -> list:
        return [c for c in await self._claim() if c.delivery_class == "automated"]

    async def _answer_new_inbound(self, instance: str, phone: str, marker: str) -> None:
        """After settlement, a fresh inbound is generated and delivered by fake I/O."""

        await self._accept(instance, phone, marker, "nova mensagem")
        await self._force_ready()
        processor = self._router_processor(FakeLLM("resposta nova"))
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._automated_claims()
        self.assertEqual([c.payload["message"] for c in claimed], ["resposta nova"])
        signals = turn_delivery_repo.lifecycle_settle_count()
        self.assertTrue(await self._preio(claimed[0]))
        self.assertTrue(await self._complete(claimed[0]))
        self.assertEqual(turn_delivery_repo.lifecycle_settle_count(), signals)
        self.assertEqual((await self._automated())[-1].status, "delivered")

    async def test_resume_during_handoff_reply_io_records_single_delivery(self) -> None:
        agent, instance, phone, _, _ = await self._run_handoff("x1a")
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._automated_claims()
        self.assertEqual([c.automated_effect for c in claimed], ["reply"])
        self.assertTrue(await self._preio(claimed[0]))
        await self._resume_session()  # /retomar while the reply is in flight
        self.assertTrue(await self._complete(claimed[0]))
        reply, transition = await self._automated()
        self.assertEqual(
            (reply.status, reply.terminal_reason, reply.ambiguity_count),
            ("delivered", "delivered_after_cancel", 0),
        )
        self.assertEqual(transition.status, "cancelled")
        turn = (await self._turns())[0]
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("cancelled", "terminal", "handoff_superseded"),
        )
        self.assertEqual(await self._automated_claims(), [])
        await self._answer_new_inbound(instance, phone, "x1a-new")

    async def test_agent_toggle_during_ordinary_io_records_single_delivery(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("x1b")
        await self._accept(instance, phone, "x1b-after", "sucessor")
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        await self._set_agent(ativo=False)
        signals = turn_delivery_repo.lifecycle_settle_count()
        self.assertTrue(await self._complete(claimed[0]))
        self.assertEqual(turn_delivery_repo.lifecycle_settle_count(), signals + 1)
        await self._set_agent(ativo=True)
        row = (await self._automated())[0]
        self.assertEqual((row.status, row.attempt_count, row.ambiguity_count), ("delivered", 1, 0))
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()],
            [("delivered", "terminal"), ("pending", "active")],
        )
        # No second authorized send for the delivered reply.
        self.assertEqual([c.id for c in await self._claim() if c.id == row.id], [])

    async def test_transfer_during_ordinary_io_records_single_delivery(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("x1c")
        await self._accept(instance, phone, "x1c-after", "sucessor")
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        await self._transfer_session()
        self.assertTrue(await self._complete(claimed[0]))
        self.assertEqual([r.status for r in await self._automated()], ["delivered"])
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()],
            [("delivered", "terminal"), ("pending", "active")],
        )

    async def test_failure_after_lifecycle_change_is_recorded_and_settles(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("x1d")
        await self._accept(instance, phone, "x1d-after", "sucessor")
        claimed = await self._claim()
        self.assertTrue(await self._preio(claimed[0]))
        await self._transfer_session()
        # Retryable failure, but the lifecycle no longer authorizes a retry.
        self.assertEqual(await self._fail(claimed[0], 5), "dead")
        row = (await self._automated())[0]
        self.assertEqual((row.status, row.last_error_code), ("dead", "http_503"))
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()],
            [("dead", "terminal"), ("pending", "active")],
        )

    async def test_preio_lifecycle_refusal_settles_and_bot_answers_after_reactivation(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("x2a")
        await self._accept(instance, phone, "x2a-after", "sucessor")
        claimed = await self._claim()
        await self._set_agent(ativo=False)
        self.assertFalse(await self._preio(claimed[0]))
        self.assertEqual(
            [(r.status, r.terminal_reason) for r in await self._automated()],
            [("cancelled", "lifecycle_refused")],
        )
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()],
            [("dead", "terminal"), ("pending", "active")],
        )
        await self._set_agent(ativo=True)
        await self._force_ready()
        processor = self._router_processor(FakeLLM("resposta ao sucessor"))
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        self.assertEqual(
            [c.payload["message"] for c in await self._automated_claims()],
            ["resposta ao sucessor"],
        )

    async def test_preio_refusal_parks_a_current_handoff(self) -> None:
        await self._run_handoff("x2-handoff")
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = await self._automated_claims()
        await self._set_agent(ativo=False)
        before = turn_delivery_repo.handoff_effect_terminal_count()
        self.assertFalse(await self._preio(claimed[0]))
        self.assertEqual(turn_delivery_repo.handoff_effect_terminal_count(), before + 1)
        turn = (await self._turns())[0]
        self.assertEqual((turn.status, turn.session_slot), ("awaiting_delivery", "active"))
        self.assertEqual(
            [(r.automated_effect, r.status) for r in await self._automated()],
            [("reply", "cancelled"), ("handoff_transition", "blocked")],
        )

    async def test_transfer_then_resume_with_pending_reply_settles_orphan(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("x2b")
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        await self._accept(instance, phone, "x2b-after", "sucessor")
        await self._transfer_session()
        await self._resume_session()  # /retomar cancels the pending reply
        processor = self._router_processor(FakeLLM("resposta ao sucessor"))
        await processor.run_pass()  # discovery settles the orphaned turn
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()][:2],
            [("dead", "terminal"), ("pending", "active")],
        )
        await self._force_ready()
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        self.assertEqual(
            [c.payload["message"] for c in await self._automated_claims()],
            ["resposta ao sucessor"],
        )

    async def test_resume_with_pending_handoff_settles_and_bot_answers(self) -> None:
        agent, instance, phone, _, _ = await self._run_handoff("r3b")
        await self._resume_session()
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await processor.run_pass(), [])
        turn = (await self._turns())[0]
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("cancelled", "terminal", "handoff_superseded"),
        )
        self.assertEqual(
            {r.status for r in await self._automated()}, {"cancelled"}
        )
        await self._answer_new_inbound(instance, phone, "r3b-new")

    async def test_preio_extends_a_short_delivery_lease(self) -> None:
        await self._ordinary_admitted("n12")
        claimed = await self._claim()
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.id == claimed[0].id)
                .values(lock_until=func.utc_timestamp(6) + text("INTERVAL 2 SECOND"))
            )
            await db.commit()
        self.assertTrue(await self._preio(claimed[0], budget=65))
        async with self.sessions() as db:
            now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        row = (await self._automated())[0]
        self.assertGreaterEqual(row.lock_until, now + timedelta(seconds=64))
        self.assertEqual(row.status, "processing")

    async def test_admission_defers_behind_open_stream_predecessor(self) -> None:
        agent, instance = await self._agent("fifo")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "fifo-1", "oi")
        await self._force_ready()
        async with self.sessions() as db:
            session = (await db.execute(select(Sessao).with_for_update())).scalar_one()
            await db.execute(
                text(
                    "INSERT INTO outbox_deliveries (delivery_key,cutover_epoch,dedupe_key,"
                    "provider,operation,payload_version,payload,delivery_class,ordering_key,"
                    "stream_sequence,status,next_attempt_at,expires_at) VALUES (UUID(),:epoch,"
                    "UUID(),'evolution','evolution.send_text.v1',1,JSON_OBJECT('synthetic',TRUE),"
                    "'human',:stream,:seq,'retry',UTC_TIMESTAMP(6)+INTERVAL 1 HOUR,"
                    "UTC_TIMESTAMP(6)+INTERVAL 2 HOUR)"
                ),
                {
                    "epoch": self.delivery_epoch,
                    "stream": session.delivery_stream_key,
                    "seq": session.next_delivery_sequence,
                },
            )
            session.next_delivery_sequence += 1
            await db.commit()
        processor = self._router_processor(FakeLLM("atrás do humano"))
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        turn = (await self._turns())[0]
        self.assertIsNone(lane.owner_token)  # deferred, lane not held
        self.assertEqual(turn.status, "awaiting_delivery")
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=10
            )
        self.assertEqual(ready, [])
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(OutboxDelivery.delivery_class == "human")
                .values(status="delivered", terminal_at=func.utc_timestamp(6),
                        terminal_reason="delivered", delivered_at=func.utc_timestamp(6))
            )
            await db.commit()
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=10
            )
        self.assertEqual([turn_id for turn_id, _, _ in ready], [turn.id])

    async def test_stale_delivery_epoch_effect_is_deferred_not_held(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("epoch")
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        new_epoch = str(uuid4())
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE delivery_runtime_state SET cutover_epoch=:epoch, "
                    "version=version+1, changed_by='ticket06', change_reason='rollover test' "
                    "WHERE id=1"
                ),
                {"epoch": new_epoch},
            )
        async with self.sessions() as db:
            ready = await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                db, scheduler_epoch=self.scheduler_epoch, limit=10
            )
        self.assertEqual(ready, [])
        async with self.sessions() as db:
            await db.execute(
                update(ConversationTurn)
                .where(ConversationTurn.agente_id == agent)
                .values(
                    status="retry",
                    ready_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                    retry_at=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"),
                )
            )
            await db.commit()
        self.delivery_epoch = new_epoch
        before = turn_delivery_repo.epoch_mismatch_count()
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        self.assertEqual(turn_delivery_repo.epoch_mismatch_count(), before + 1)
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        self.assertIsNone(lane.owner_token)
        self.assertEqual((await self._turns())[0].status, "awaiting_delivery")

    async def test_ordinary_admission_resets_generation_recovery_count(self) -> None:
        agent, instance = await self._agent("reset")
        await self._accept(instance, f"5511{agent:08d}", "reset-1", "oi")
        await self._force_ready()
        async with self.sessions() as db:
            await db.execute(update(ConversationTurn).values(delivery_attempt_count=7))
            await db.commit()
        processor = self._router_processor(FakeLLM("olá"))
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        turn = (await self._turns())[0]
        self.assertEqual((turn.status, turn.delivery_attempt_count), ("awaiting_delivery", 0))

    async def test_handoff_admission_resets_generation_recovery_count(self) -> None:
        agent, instance = await self._agent("handoff-reset")
        phone = f"5511{agent:08d}"
        await self._accept(instance, phone, "handoff-reset-1", "preciso de atendimento")
        await self._force_ready()
        async with self.sessions() as db:
            # Generation recoveries already near the cap (max 8).
            await db.execute(update(ConversationTurn).values(delivery_attempt_count=7))
            await db.commit()
        processor = self._processor(
            FakeLLM("Vou transferir você. [TRANSFERIR]"), self._finalizer(FakeLLM("resumo"))
        )
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        turn = (await self._turns())[0]
        self.assertEqual(
            (turn.status, turn.delivery_attempt_count), ("awaiting_delivery", 0)
        )
        router = self._router_processor(ForbiddenLLM())
        outcomes = []
        for _ in range(8):  # full send budget after admission
            await self._make_retry_due()
            self.assertEqual(await self._run_one(router), ["final_candidate"])
            claimed = await self._automated_claims()
            self.assertEqual([c.automated_effect for c in claimed], ["reply"])
            self.assertTrue(await self._preio(claimed[0]))
            outcomes.append(await self._fail(claimed[0], 5))
        self.assertEqual(outcomes, ["retry"] * 7 + ["parked"])
        turn = (await self._turns())[0]
        self.assertEqual(
            (turn.status, turn.session_slot, turn.delivery_attempt_count),
            ("awaiting_delivery", "active", 8),
        )

    # ── review round 3: Y1 no processing effect is ever orphaned ────────
    async def _expire_automated_leases(self) -> None:
        async with self.sessions() as db:
            await db.execute(
                update(OutboxDelivery)
                .where(
                    OutboxDelivery.delivery_class == "automated",
                    OutboxDelivery.status == "processing",
                )
                .values(lock_until=func.utc_timestamp(6) - text("INTERVAL 1 SECOND"))
            )
            await db.commit()

    async def _renew(self, claimed) -> bool:
        async with self.sessions() as db:
            renewed = await turn_delivery_repo.renew_automated_authority(
                db,
                delivery_id=claimed.id,
                lease_token=claimed.lease_token,
                cutover_epoch=claimed.cutover_epoch,
                hint=_automated_hint(claimed),
                delivery_lease_seconds=90,
                lane_lease_seconds=30,
            )
            await db.commit()
        return renewed

    async def _settle_candidates(self) -> list[int]:
        async with self.sessions() as db:
            return [
                turn_id
                for turn_id, _, _ in await turn_delivery_repo.list_admitted_turns_ready_for_admission(
                    db, scheduler_epoch=self.scheduler_epoch, limit=10
                )
            ]

    async def _requeue_first(self) -> bool:
        turn = (await self._turns())[0]
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

    async def _orphan_after_resume(self, suffix: str, effect: str, mode: str):
        """Effect in I/O, /retomar, then the owner never finalizes."""

        if effect == "reply":
            agent, instance, phone, _, _ = await self._run_handoff(suffix)
            processor = self._router_processor(ForbiddenLLM())
            self.assertEqual(await self._run_one(processor), ["final_candidate"])
            claimed = (await self._automated_claims())[0]
            self.assertTrue(await self._preio(claimed))
        else:
            claimed, _ = await self._claimed_transition(suffix)
            self.assertTrue(await self._preio(claimed))
            turn = (await self._turns())[0]
            agent = turn.agente_id
            session = await self._one(Sessao, Sessao.id)
            phone = session.telefone
            instance = (await self._one(Agente, Agente.id)).instance_name
        self.assertEqual(claimed.automated_effect, effect)
        await self._resume_session()  # /retomar while the effect is in flight
        if mode == "renew_abort":
            # The renewal sees the closed lifecycle and the worker aborts the
            # I/O without recording anything.
            self.assertFalse(await self._renew(claimed))
        return agent, instance, phone, claimed

    async def _assert_orphan_resolved_and_bot_answers(
        self, agent: int, instance: str, phone: str, claimed, marker: str
    ) -> None:
        # Live lease: never killed, the turn waits and is not listed.
        row = next(r for r in await self._automated() if r.id == claimed.id)
        self.assertEqual((row.status, row.ambiguity_count), ("processing", 0))
        self.assertEqual(await self._settle_candidates(), [])
        self.assertEqual(await self._requeue_first(), False)
        row = next(r for r in await self._automated() if r.id == claimed.id)
        self.assertEqual((row.status, row.ambiguity_count), ("processing", 0))
        self.assertEqual((await self._turns())[0].session_slot, "active")
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        # Free lane, live delivery lease: still never listed (no hot loop).
        self.assertEqual(await self._settle_candidates(), [])
        await self._expire_automated_leases()
        before = scheduler_repo.orphan_effect_resolved_count()
        processor = self._router_processor(ForbiddenLLM())
        await processor.run_pass()  # discovery settle branch
        self.assertEqual(scheduler_repo.orphan_effect_resolved_count(), before + 1)
        row = next(r for r in await self._automated() if r.id == claimed.id)
        self.assertEqual(
            (row.status, row.terminal_reason, row.ambiguity_count, row.last_error_code,
             row.lease_token),
            ("cancelled", "cancel_requested", 1, "lease_expired_ambiguous", None),
        )
        self.assertTrue(
            all(r.status in {"delivered", "dead", "cancelled"} for r in await self._automated())
        )
        turn = (await self._turns())[0]
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("cancelled", "terminal", "handoff_superseded"),
        )
        await self._answer_new_inbound(instance, phone, marker)

    async def test_y1_orphan_effect_after_resume_resolves_and_bot_answers(self) -> None:
        for effect in ("reply", "handoff_transition"):
            for mode in ("crash", "renew_abort"):
                with self.subTest(effect=effect, mode=mode):
                    await self._wipe_and_reset()
                    suffix = f"y1-{effect[:5]}-{mode}"
                    agent, instance, phone, claimed = await self._orphan_after_resume(
                        suffix, effect, mode
                    )
                    await self._assert_orphan_resolved_and_bot_answers(
                        agent, instance, phone, claimed, f"{suffix}-new"
                    )

    async def test_y1_superseded_settle_waits_for_live_sibling_then_owner_settles(self) -> None:
        agent, instance, phone, _, _ = await self._run_handoff("y1-live")
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = (await self._automated_claims())[0]
        self.assertTrue(await self._preio(claimed))
        async with self.sessions() as db:
            # Superseded without the session-wide cancellation: the transition
            # stays blocked and the reply keeps a live lease.
            await db.execute(
                text("UPDATE handoff_attempts SET status='superseded', terminal_at=UTC_TIMESTAMP(6)")
            )
            await db.commit()
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        before = scheduler_repo.settle_waiting_live_effect_count()
        self.assertFalse(await self._requeue_first())
        self.assertEqual(scheduler_repo.settle_waiting_live_effect_count(), before + 1)
        self.assertEqual(
            [(r.automated_effect, r.status, r.ambiguity_count) for r in await self._automated()],
            [("reply", "processing", 0), ("handoff_transition", "blocked", 0)],
        )
        turn = (await self._turns())[0]
        self.assertEqual((turn.status, turn.session_slot), ("awaiting_delivery", "active"))
        # The live owner's own finalization settles everything (R2M3).
        self.assertTrue(await self._complete(claimed))
        self.assertEqual(
            [(r.automated_effect, r.status, r.terminal_reason) for r in await self._automated()],
            [("reply", "delivered", "delivered"),
             ("handoff_transition", "cancelled", "handoff_superseded")],
        )
        turn = (await self._turns())[0]
        self.assertEqual(
            (turn.status, turn.session_slot, turn.terminal_reason),
            ("cancelled", "terminal", "handoff_superseded"),
        )

    async def test_y1_superseded_handoff_promotes_waiting_successor(self) -> None:
        agent, instance, phone, _, _ = await self._run_handoff("y1-succ")
        await self._resume_session()
        # Inbound after /retomar waits as successor behind the admitted turn.
        await self._accept(instance, phone, "y1-succ-after", "voltei")
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()],
            [("awaiting_delivery", "active"), ("pending", "successor")],
        )
        processor = self._router_processor(FakeLLM("resposta ao sucessor"))
        await processor.run_pass()
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()][:2],
            [("cancelled", "terminal"), ("pending", "active")],
        )
        await self._force_ready()
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        self.assertEqual(
            [c.payload["message"] for c in await self._automated_claims()],
            ["resposta ao sucessor"],
        )

    async def test_y1_cap_never_kills_a_live_lease_and_resolves_an_expired_one(self) -> None:
        for path in ("recovery", "yield"):
            with self.subTest(path=path):
                await self._wipe_and_reset()
                agent, instance, phone, _ = await self._ordinary_admitted("y1-cap")
                await self._accept(instance, phone, "y1-cap-after", "sucessor")
                claimed = (await self._claim())[0]
                self.assertTrue(await self._preio(claimed))
                async with self.sessions() as db:
                    await db.execute(
                        update(ConversationTurn)
                        .where(ConversationTurn.session_slot == "active")
                        .values(delivery_attempt_count=8)
                    )
                    await db.commit()
                if path == "recovery":
                    await self._expire_lane(agent)
                    self.assertEqual(await self._recover_cap(agent), "idle")
                else:
                    lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
                    self.assertFalse(await self._yield_cap(agent, lane.active_turn_id, lane.owner_token))
                row = (await self._automated())[0]
                self.assertEqual((row.status, row.ambiguity_count), ("processing", 0))
                self.assertEqual(
                    [(t.status, t.session_slot) for t in await self._turns()],
                    [("awaiting_delivery", "active"), ("pending", "successor")],
                )
                # The public discovery/requeue path must settle without another claim,
                # even with the agent disabled. No synthetic lane resurrection.
                await self._set_agent(ativo=False)
                await self._expire_automated_leases()
                self.assertEqual(await self._settle_candidates(), [(await self._turns())[0].id])
                before = scheduler_repo.orphan_effect_resolved_count()
                self.assertFalse(await self._requeue_first())
                self.assertEqual(scheduler_repo.orphan_effect_resolved_count(), before + 1)
                row = (await self._automated())[0]
                self.assertEqual(
                    (row.status, row.terminal_reason, row.ambiguity_count, row.last_error_code),
                    ("dead", "delivery_attempt_cap", 1, "lease_expired_ambiguous"),
                )
                self.assertEqual(
                    [(t.status, t.session_slot) for t in await self._turns()],
                    [("dead", "terminal"), ("pending", "active")],
                )

        # Cap reached while a queued effect is in backoff: neither normal
        # dispatch discovery nor the processing/lifecycle branch can help.
        await self._wipe_and_reset()
        agent, instance, phone, _ = await self._ordinary_admitted("y1-cap-queued")
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        async with self.sessions() as db:
            await db.execute(update(ConversationTurn).values(delivery_attempt_count=8))
            await db.execute(update(OutboxDelivery).where(
                OutboxDelivery.delivery_class == "automated"
            ).values(next_attempt_at=func.utc_timestamp(6) + text("INTERVAL 1 HOUR")))
            await db.commit()
        self.assertEqual(await self._settle_candidates(), [(await self._turns())[0].id])
        self.assertFalse(await self._requeue_first())
        self.assertEqual((await self._automated())[0].status, "cancelled")
        self.assertEqual((await self._turns())[0].status, "dead")

    async def test_y1_cancelled_orphan_of_inactive_agent_is_settled(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("y1-inactive")
        await self._accept(instance, phone, "y1-inactive-after", "sucessor")
        claimed = (await self._claim())[0]
        self.assertTrue(await self._preio(claimed))
        await self._transfer_session()
        await self._resume_session()  # cancel requested on the in-flight reply
        await self._set_agent(ativo=False)
        await self._expire_lane(agent)
        self.assertEqual(await self._recover_cap(agent), "idle")
        self.assertEqual(await self._settle_candidates(), [])  # lease still live
        await self._expire_automated_leases()
        self.assertEqual(await self._settle_candidates(), [(await self._turns())[0].id])
        self.assertFalse(await self._requeue_first())
        row = (await self._automated())[0]
        self.assertEqual(
            (row.status, row.terminal_reason, row.ambiguity_count),
            ("cancelled", "cancel_requested", 1),
        )
        self.assertEqual(
            [(t.status, t.session_slot) for t in await self._turns()],
            [("dead", "terminal"), ("pending", "active")],
        )

    async def test_y1_preio_refusal_leaves_no_open_effect(self) -> None:
        await self._run_handoff("y1-preio")
        processor = self._router_processor(ForbiddenLLM())
        self.assertEqual(await self._run_one(processor), ["final_candidate"])
        claimed = (await self._automated_claims())[0]
        async with self.sessions() as db:
            await db.execute(
                text("UPDATE handoff_attempts SET status='superseded', terminal_at=UTC_TIMESTAMP(6)")
            )
            await db.commit()
        self.assertFalse(await self._preio(claimed))
        self.assertEqual(
            [(r.automated_effect, r.status) for r in await self._automated()],
            [("reply", "cancelled"), ("handoff_transition", "cancelled")],
        )
        turn = (await self._turns())[0]
        self.assertEqual((turn.status, turn.session_slot), ("cancelled", "terminal"))

    async def test_completion_terminal_turn_refuses_open_automated_effect(self) -> None:
        agent, _, _, _ = await self._ordinary_admitted("y1-complete")
        lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
        turn = (await self._turns())[0]
        async with self.sessions() as db:
            with self.assertRaisesRegex(
                scheduler_repo.SchedulerConflictError, "orphan an automated effect"
            ):
                await scheduler_repo.complete_turn_and_release_lane(
                    db,
                    turn_id=turn.id,
                    sessao_id=turn.sessao_id,
                    revision_id=turn.final_revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.delivery_epoch,
                    owner_token=lane.owner_token,
                    terminal_status="delivered",
                    terminal_reason="synthetic",
                    promote_successor=True,
                )
            await db.rollback()

    async def test_y1_ordinary_aborted_renewal_settles_without_cancel_request(self) -> None:
        for change in ("inactive", "optout", "closed"):
            with self.subTest(change=change):
                await self._wipe_and_reset()
                agent, instance, phone, _ = await self._ordinary_admitted(f"y1-{change}")
                claimed = (await self._claim())[0]
                self.assertTrue(await self._preio(claimed))
                if change == "inactive":
                    await self._set_agent(ativo=False)
                elif change == "optout":
                    await self._set_agent(inbox_scheduler_enabled=False)
                else:
                    async with self.sessions() as db:
                        await db.execute(update(Sessao).values(handler="encerrada"))
                        await db.commit()
                self.assertFalse(await self._renew(claimed))
                await self._expire_lane(agent)
                self.assertEqual(await self._recover_cap(agent), "idle")
                self.assertEqual(await self._settle_candidates(), [])
                self.assertFalse(await self._requeue_first())
                self.assertEqual((await self._automated())[0].status, "processing")
                await self._expire_automated_leases()
                self.assertEqual(await self._settle_candidates(), [(await self._turns())[0].id])
                self.assertFalse(await self._requeue_first())
                row = (await self._automated())[0]
                self.assertEqual(
                    (row.status, row.ambiguity_count, row.last_error_code),
                    ("dead", 1, "lease_expired_ambiguous"),
                )
                self.assertEqual((await self._turns())[0].session_slot, "terminal")
                await self._set_agent(ativo=True, inbox_scheduler_enabled=True)
                if change == "closed":
                    await self._resume_session()
                await self._answer_new_inbound(instance, phone, f"y1-{change}-new")

    async def test_y1_absent_lease_is_defensively_resolved_without_weakening_check(self) -> None:
        await self._ordinary_admitted("y1-null")
        claimed = (await self._claim())[0]
        # Absent processing leases are impossible under the production CHECK.
        # Prove that invariant on MySQL; exercise defensive handling only on
        # the detached ORM object, never by dropping or disabling the guard.
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, "ck_outbox_lease_tuple"):
                await db.execute(update(OutboxDelivery).where(
                    OutboxDelivery.id == claimed.id
                ).values(lock_until=None))
            await db.rollback()
        row = (await self._automated())[0]
        row.lock_until = None
        self.assertTrue(scheduler_repo.resolve_open_automated_effects_locked(
            (row,), now=datetime.utcnow(), reason="defensive_absent_lease",
            processing_status="dead", turn_id=row.conversation_turn_id,
        ))
        self.assertEqual(
            (row.status, row.ambiguity_count, row.last_error_code, row.lease_token),
            ("dead", 1, "lease_expired_ambiguous", None),
        )

    async def test_y1_cap_recovery_and_yield_resolve_expired_processing(self) -> None:
        for path in ("recovery", "yield"):
            with self.subTest(path=path):
                await self._wipe_and_reset()
                agent, instance, phone, _ = await self._ordinary_admitted(f"y1-{path}")
                await self._accept(instance, phone, "y1-cap-successor", "sucessor")
                claimed = (await self._claim())[0]
                self.assertTrue(await self._preio(claimed))
                lane = await self._one(AgentServiceLane, AgentServiceLane.agente_id)
                turn = (await self._turns())[0]
                async with self.sessions() as db:
                    await db.execute(update(ConversationTurn).where(
                        ConversationTurn.id == turn.id
                    ).values(delivery_attempt_count=8))
                    await db.commit()
                await self._expire_automated_leases()
                if path == "recovery":
                    await self._expire_lane(agent)
                    self.assertEqual(await self._recover_cap(agent), "dead")
                else:
                    self.assertFalse(await self._yield_cap(agent, turn.id, lane.owner_token))
                row = (await self._automated())[0]
                self.assertEqual(
                    (row.status, row.ambiguity_count, row.last_error_code),
                    ("dead", 1, "lease_expired_ambiguous"),
                )
                self.assertEqual(
                    [(t.status, t.session_slot) for t in await self._turns()],
                    [("dead", "terminal"), ("pending", "active")],
                )
                self.assertFalse(await self._complete(claimed))

    async def test_y1_settle_prelocks_successor_before_revision_and_effects(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("y1-lock-order")
        await self._accept(instance, phone, "y1-lock-successor", "sucessor")
        claimed = (await self._claim())[0]
        statements = []

        def record(conn, cursor, statement, parameters, context, executemany):
            if "FOR UPDATE" in statement:
                statements.append(statement)

        def assert_order():
            successors = [i for i, q in enumerate(statements)
                          if "FROM conversation_turns" in q and "conversation_turns.session_slot =" in q]
            self.assertTrue(successors, "successor must be prelocked")
            successor = successors[0]
            revision = next(i for i, q in enumerate(statements) if "FROM turn_revisions" in q)
            effect = next(i for i, q in enumerate(statements) if "FROM outbox_deliveries" in q)
            self.assertLess(successor, revision)
            self.assertLess(revision, effect)

        event.listen(self.engine.sync_engine, "before_cursor_execute", record)
        try:
            self.assertTrue(await self._preio(claimed))
            assert_order()
            await self._transfer_session()
            await self._resume_session()
            await self._expire_lane(agent)
            await self._recover_cap(agent)
            await self._expire_automated_leases()
            statements.clear()
            self.assertFalse(await self._requeue_first())
            assert_order()
        finally:
            event.remove(self.engine.sync_engine, "before_cursor_execute", record)

# Inherited ticket05 cases belong to their own matrix; run only ticket06 here.
for _name in dir(base.HandoffTerminalMySQLTests):
    if _name.startswith("test_") and _name not in TurnDeliveryMySQLTests.__dict__:
        setattr(TurnDeliveryMySQLTests, _name, None)


if __name__ == "__main__":
    unittest.main()
