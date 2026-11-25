"""T08-7 isolated signed-ingress/fake-provider lifecycle rehearsal on MySQL."""
import os
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import delivery_cli, main
from src.config import settings
from src.database.connection import get_db
from src.database.models import (
    Agente, ConversationTurn, DeliveryOperatorAction, GenerationAttempt,
    InboundEvent, ReplicaHeartbeat, SchedulerRetentionProgress, SchedulerRuntimeState,
)
from src.database.repositories import (
    agent_scheduler_repo, lifecycle_repo, replica_repo, scheduler_repo,
    scheduler_retention_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services import evolution_orchestrator, llm_client, scheduler_observability
from src.services.turn_processor import TurnProcessor
from tests import auth_fixtures, replica_fixtures
from tests.disposable_db import assert_disposable_database
from tests.test_handoff_terminal_mysql import TABLES
from tests.test_turn_processor_mysql import FakeLLM, RecordingConsumer, _no_presence


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")
PHONE = "5511987654321"
CONTENT = "t087-private-content-never-log"
BEARER = "t087-secret-bearer-never-log"
RECEIPT_SECRET = "t087-secret-receipt-never-log"


class Ticket08RehearsalMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        # Owns the whole disposable DB; require the runner's exact shared marker.
        await assert_disposable_database(self, self.engine)
        self.sessions = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.delivery_epoch = str(uuid4())
        self.scheduler_epoch = str(uuid4())
        self.instance = f"t087-{uuid4()}"
        async with self.engine.begin() as conn:
            await conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table_name in TABLES:
                await conn.execute(text(f"DELETE FROM {table_name}"))
            await conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            await conn.execute(text(
                "UPDATE delivery_runtime_state SET mode='outbox', cutover_epoch=:e, "
                "version=10, changed_by='t087', change_reason='isolated rehearsal' WHERE id=1"
            ), {"e": self.delivery_epoch})
            await conn.execute(text(
                "UPDATE scheduler_runtime_state SET state='disabled', scheduler_epoch=:e, "
                "version=10, schema_version=3, changed_by='t087', "
                "change_reason='isolated rehearsal' WHERE id=1"
            ), {"e": self.scheduler_epoch})
        async with self.sessions.begin() as db:
            agent = Agente(
                nome="T087 isolated", instance_name=self.instance, system_prompt="synthetic-system",
                numero_notificacao="0", modelo_llm="openai/gpt-4o-mini",
                chatwoot_inbox_id=7, inbox_scheduler_enabled=False,
            )
            db.add(agent)
            await db.flush()
            self.agent_id = agent.id
        self.patches = ExitStack()
        for name, value in (
            ("provider_webhook_auth_mode", "enforced"),
            ("evolution_webhook_bearer_token", BEARER),
            ("delivery_mode", "outbox"),
            ("delivery_cutover_epoch", self.delivery_epoch),
            ("delivery_dispatch_enabled", True),
            ("inbox_scheduler_enabled", True),
            ("inbox_scheduler_epoch", self.scheduler_epoch),
            ("inbox_turn_processor_enabled", True),
            ("delivery_worker_code_version", replica_fixtures.CODE_VERSION),
            ("inbound_receipt_hmac_current_version", 1),
            ("inbound_receipt_hmac_keys", {1: SecretStr(RECEIPT_SECRET)}),
        ):
            self.patches.enter_context(patch.object(settings, name, value))
        self.patches.enter_context(patch(
            "src.services.evolution_orchestrator.processar",
            AsyncMock(side_effect=AssertionError("legacy LLM path reached")),
        ))
        self.patches.enter_context(patch.object(
            evolution_orchestrator.evolution_client, "send_text_message",
            AsyncMock(side_effect=AssertionError("real Evolution transport reached")),
        ))
        self.patches.enter_context(patch(
            "src.services.llm_client._client",
            side_effect=AssertionError("real LLM provider reached"),
        ))

        async def uow_override():
            async with UnitOfWork(self.sessions) as uow:
                yield uow
        main.app.dependency_overrides[main.get_unit_of_work] = uow_override

        async def db_override():
            async with self.sessions() as db:
                yield db
        main.app.dependency_overrides[get_db] = db_override
        auth_fixtures.autenticar_como(main.app)  # AUTH-1: admin principal
        main.reset_health_observability_caches()

        async def check_db_connection() -> bool:
            # Same NullPool engine: no pooled connection outlives this loop.
            async with self.sessions() as db:
                await db.execute(text("SELECT 1"))
            return True
        self.patches.enter_context(
            patch.object(main, "check_db_connection", check_db_connection)
        )
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app), base_url="http://t087.invalid"
        )
        self.processors: list[TurnProcessor] = []

    async def asyncTearDown(self) -> None:
        for processor in self.processors:
            await processor.shutdown(2)
        await self.client.aclose()
        main.app.dependency_overrides.clear()
        self.patches.close()
        await self.engine.dispose()

    async def _transition(self, target: str, version: int, *, attestation=None):
        return await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: scheduler_repo.transition_scheduler_state(
                db, target=target, scheduler_epoch=self.scheduler_epoch,
                expected_version=version, actor="t087", reason="isolated rehearsal",
                replica_attestation=attestation,
            ),
        )

    async def _enable_and_opt_in(self) -> None:
        await self._transition("enabling", 10)
        attestation = await replica_fixtures.ready_fleet(
            self.sessions, delivery_epoch=self.delivery_epoch,
            scheduler_epoch=self.scheduler_epoch,
        )
        await self._transition("enabled", 11, attestation=attestation)
        result = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            lambda db: agent_scheduler_repo.opt_in_agent(
                db, agente_id=self.agent_id, scheduler_epoch=self.scheduler_epoch,
                delivery_epoch=self.delivery_epoch, provider_auth_enforced=True,
                actor="t087", reason="canary opt-in",
            ),
        )
        self.assertTrue(result.inbox_scheduler_enabled)

    def _payload(self, message_id: str, content: str = CONTENT) -> dict[str, object]:
        return {
            "event": "messages.upsert", "instance": self.instance,
            "data": {"key": {"id": message_id, "fromMe": False,
                              "remoteJid": f"{PHONE}@s.whatsapp.net"},
                     "message": {"conversation": content}},
        }

    async def _post(self, message_id: str, content: str = CONTENT) -> httpx.Response:
        return await self.client.post(
            "/webhook", headers={"Authorization": f"Bearer {BEARER}"},
            json=self._payload(message_id, content),
        )

    async def _force_ready(self) -> None:
        async with self.sessions.begin() as db:
            await db.execute(text(
                "UPDATE conversation_turns SET ready_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND "
                "WHERE agente_id=:a AND session_slot='active'"
            ), {"a": self.agent_id})

    def _processor(self, llm, consumer) -> TurnProcessor:
        processor = TurnProcessor(
            self.sessions, worker_id=f"t087-turn-{uuid4()}",
            scheduler_epoch=self.scheduler_epoch, delivery_runtime_epoch=self.delivery_epoch,
            consumer=consumer, llm_call=llm, presence_call=_no_presence,
            lease_seconds=30, poll_interval_seconds=0.05,
            generation_timeout_seconds=5, typing_jitter_ms=0,
            release_backoff_seconds=1, concurrency=1,
        )
        self.processors.append(processor)
        return processor

    async def test_two_stage_canary_drain_replay_and_fake_delivery(self) -> None:
        await self._enable_and_opt_in()
        accepted = await self._post("accepted-before-drain")
        self.assertEqual((accepted.status_code, accepted.json()["status"]), (202, "accepted"))
        async with self.sessions() as db:
            receipt = (await db.execute(select(InboundEvent))).scalar_one()
        self.assertEqual(receipt.hmac_key_version, 1)
        self.assertRegex(receipt.payload_hmac, r"^[0-9a-f]{64}$")
        await self._transition("draining", 12)
        replay = await self._post("accepted-before-drain")
        self.assertEqual((replay.status_code, replay.json()["status"]), (200, "already_accepted"))
        refused = await self._post("new-during-drain", "new-private-content-never-log")
        self.assertEqual(refused.status_code, 503)
        self.assertEqual(refused.json()["code"], "scheduler_draining")
        await self._force_ready()
        llm = FakeLLM()
        llm.script = ["synthetic reply"]
        transport = RecordingConsumer(self.sessions, complete=True)
        result = await self._processor(llm, transport).process_agent(self.agent_id)
        self.assertEqual(result, "final_candidate")
        self.assertEqual(len(llm.calls), 1)
        self.assertEqual(len(transport.candidates), 1)
        async with self.sessions() as db:
            turn = (await db.execute(select(ConversationTurn))).scalar_one()
            agent = await db.get(Agente, self.agent_id)
        self.assertEqual((turn.status, turn.session_slot), ("delivered", "terminal"))
        self.assertTrue(agent.inbox_scheduler_enabled)
        await lifecycle_repo.run_in_new_session(
            self.sessions,
            lambda db: agent_scheduler_repo.opt_out_agent(
                db, agente_id=self.agent_id, actor="t087", reason="canary complete"
            ),
        )
        disabled = await self._transition("disabled", 13)
        self.assertEqual(disabled.state, "disabled")

    async def test_unknown_generation_blocks_clearance_until_audited_resolution(self) -> None:
        await self._enable_and_opt_in()
        self.assertEqual((await self._post("unknown-generation")).status_code, 202)
        await self._force_ready()
        llm = FakeLLM()
        llm.script = [llm_client.LLMProviderError("provider_transport_unknown", outcome_known=False)]
        transport = RecordingConsumer(self.sessions, complete=True)
        result = await self._processor(llm, transport).process_agent(self.agent_id)
        self.assertEqual(result, "generation_outcome_unknown")
        await self._transition("draining", 12)
        with self.assertRaises(scheduler_repo.SchedulerDrainBlocked) as blocked:
            await self._transition("disabled", 13)
        self.assertEqual(blocked.exception.counts["unresolved_attempts"], 1)
        async with self.sessions() as db:
            attempt = (await db.execute(select(GenerationAttempt))).scalar_one()
        resolved = await lifecycle_repo.run_in_new_session(
            self.sessions,
            lambda db: lifecycle_repo.resolve_generation_quarantine(
                db, attempt_key=attempt.attempt_key, outcome="failed",
                evidence="provider_dashboard", actor="t087", note="isolated evidence",
                accept_duplicate_spend_risk=True,
            ),
        )
        self.assertTrue(resolved.reconciled)
        async with self.sessions() as db:
            attempt = await db.get(GenerationAttempt, attempt.id)
        self.assertIsNotNone(attempt.reconciled_at)
        self.assertIn("risk=duplicate_spend_accepted", attempt.reconciliation_reason)
        await lifecycle_repo.run_in_new_session(
            self.sessions,
            lambda db: lifecycle_repo.update_agent(
                db, agente_id=self.agent_id, payload={"ativo": False}
            ),
        )
        disabled = await self._transition("disabled", 13)
        self.assertEqual(disabled.state, "disabled")

    async def test_mixed_version_fails_readiness_and_enable_gate(self) -> None:
        await self._transition("enabling", 10)
        attestation = await replica_fixtures.ready_fleet(
            self.sessions, delivery_epoch=self.delivery_epoch,
            scheduler_epoch=self.scheduler_epoch,
        )
        async with self.sessions.begin() as db:
            replica = (await db.execute(
                select(ReplicaHeartbeat).where(ReplicaHeartbeat.replica_kind == "app")
            )).scalars().first()
            replica.code_version = "old-code"
        async with self.sessions() as db:
            fleet = await replica_repo.load_fleet(db)
        reasons = replica_repo.readiness_reasons(fleet, reader=attestation.operator)
        self.assertIn("code_version_mismatch", reasons)
        with self.assertRaises(scheduler_repo.ReplicaGateRefused) as refused:
            await self._transition("enabled", 11, attestation=attestation)
        self.assertIn("code_version_mismatch", refused.exception.reasons)
        async with self.sessions() as db:
            state = await db.get(SchedulerRuntimeState, 1)
            audit_count = (await db.execute(select(DeliveryOperatorAction))).scalars().all()
        self.assertEqual(state.state, "enabling")
        self.assertEqual(len(audit_count), 1)  # disabled->enabling only

    async def test_security_paths_health_cache_list_delete_and_checkpoint(self) -> None:
        """Security fixes S1–S5 on one isolated lifetime, scanned for privacy."""
        await self._enable_and_opt_in()
        self.assertEqual((await self._post("security-paths")).status_code, 202)

        # FU-4: public redaction and internal details share the S4 cache.
        loads = AsyncMock(wraps=scheduler_observability.scheduler_health)
        with patch.object(settings, "health_observability_cache_seconds", 60), \
                patch.object(scheduler_observability, "scheduler_health", loads):
            public = await self.client.get("/health")
            first = await self.client.get("/internal/health")
            second = await self.client.get("/internal/health")
        self.assertEqual(loads.await_count, 1)
        self.assertEqual(public.status_code, first.status_code)
        self.assertEqual(set(public.json()), {"status"})
        self.assertEqual(public.json()["status"],
                         "healthy" if first.json()["status"] == "healthy" else "unhealthy")
        self.assertEqual(first.status_code, second.status_code)
        self.assertEqual(first.json()["scheduler"], second.json()["scheduler"])
        for body in (public.text, first.text, second.text):
            for secret in (PHONE, CONTENT, BEARER, RECEIPT_SECRET, self.scheduler_epoch):
                self.assertNotIn(secret, body)

        # S5: drain discovery lists the opted-in agent without exposing data.
        with patch.object(delivery_cli, "AsyncSessionLocal", self.sessions), \
                patch.object(delivery_cli, "_emit") as emit:
            self.assertEqual(await delivery_cli._run(delivery_cli._parser().parse_args(
                ["agent-scheduler", "list", "--after-id", "0", "--limit", "10"]
            )), 0)
        listing = emit.call_args.args[0]
        self.assertEqual([row["agente_id"] for row in listing["agents"]], [self.agent_id])
        self.assertNotIn(self.instance, repr(listing))

        # S3: retained scheduler history refuses agent delete with 409.
        refused = await self.client.delete(f"/agentes/{self.agent_id}")
        self.assertEqual(refused.status_code, 409)
        for secret in (PHONE, CONTENT, self.instance):
            self.assertNotIn(secret, refused.text)
        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(Agente, self.agent_id))

        # S1: a pass over a young head checkpoints durably (CAS version + 1).
        async with self.sessions() as db:
            before = await db.get(SchedulerRetentionProgress, 1)
        result = await scheduler_retention_repo.cleanup_retention(self.sessions)
        async with self.sessions() as db:
            after = await db.get(SchedulerRetentionProgress, 1)
        self.assertEqual(after.version, before.version + 1)
        self.assertEqual(after.last_turn_id, 0)
        self.assertEqual((result.scanned, result.drafts_scrubbed, result.turns_purged), (1, 0, 0))
        async with self.sessions() as db:
            self.assertEqual(len((await db.execute(select(ConversationTurn))).scalars().all()), 1)
