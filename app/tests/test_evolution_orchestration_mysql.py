import asyncio
import os
import unittest
from contextlib import ExitStack
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.config import settings
from src.database.models import (
    Agente,
    ConsumoLLM,
    HandoffAttempt,
    InboundEvent,
    LLMUsageReconciliation,
    Mensagem,
    OutboxDelivery,
    Sessao,
)
from src.database.repositories import (
    consumo_repo,
    handoff_attempt_repo,
    outbox_repo,
    sessao_repo,
)
from src.database.unit_of_work import UnitOfWork
from src.services import evolution_orchestrator
from src.services.delivery_adapters import (
    decode_chatwoot_handoff,
    decode_evolution_text,
)
from src.services.llm_client import LLMResult


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")


def _llm_result(texto: str, generation_id: str | None = None) -> LLMResult:
    return LLMResult(
        texto=texto,
        modelo_llm="openai/gpt-4o-mini",
        tokens_input=10,
        tokens_output=5,
        tokens_reasoning=None,
        tokens_cache_read=None,
        tokens_cache_write=None,
        custo_creditos=Decimal("0.001"),
        provider_generation_id=generation_id or f"gen-{uuid4()}",
    )


class EvolutionOrchestrationMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table_name in (
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
                await connection.execute(text(f"DELETE FROM {table_name}"))
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))

        self.epoch = str(uuid4())
        self.instance_name = f"orchestration-{uuid4()}"
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state "
                    "SET mode='outbox', cutover_epoch=:epoch, version=version+1"
                ),
                {"epoch": self.epoch},
            )
            agente = Agente(
                nome="Orchestration test",
                instance_name=self.instance_name,
                system_prompt="test prompt",
                modelo_llm="openai/gpt-4o-mini",
                numero_notificacao="0",
                chatwoot_inbox_id=7,
            )
            db.add(agente)
            await db.commit()
            self.agente_id = agente.id

    async def asyncTearDown(self) -> None:
        # The actual worker contracts, not test-only shape checks, must accept
        # every payload committed by every orchestration test.
        async with self.sessions() as db:
            rows = list((await db.execute(select(OutboxDelivery))).scalars())
            for row in rows:
                if row.provider == "evolution":
                    decode_evolution_text(
                        row.operation,
                        row.payload_version,
                        row.payload,
                    )
                elif row.provider == "chatwoot":
                    decode_chatwoot_handoff(
                        row.operation,
                        row.payload_version,
                        row.payload,
                    )
                else:
                    self.fail(f"unexpected staged provider: {row.provider}")
        await self.engine.dispose()

    def _event(
        self,
        message_id: str,
        text_value: str,
        *,
        phone: str = "5511999999001",
    ) -> evolution_orchestrator.EvolutionMessageEvent:
        return evolution_orchestrator.EvolutionMessageEvent(
            instance_name=self.instance_name,
            provider_message_id=message_id,
            telefone=phone,
            texto=text_value,
        )

    def _settings(self, mode: str = "outbox") -> ExitStack:
        stack = ExitStack()
        stack.enter_context(patch.object(settings, "delivery_mode", mode))
        stack.enter_context(
            patch.object(
                settings,
                "delivery_cutover_epoch",
                self.epoch if mode == "outbox" else None,
            )
        )
        stack.enter_context(
            patch.object(settings, "inbound_receipt_hmac_current_version", 2)
        )
        stack.enter_context(
            patch.object(
                settings,
                "inbound_receipt_hmac_keys",
                {1: SecretStr("retained-key"), 2: SecretStr("current-key")},
            )
        )
        return stack

    async def _orchestrate(
        self,
        event: evolution_orchestrator.EvolutionMessageEvent,
    ) -> evolution_orchestrator.OrchestrationResult:
        async with UnitOfWork(self.sessions) as uow:
            return await evolution_orchestrator.orchestrate(
                event=event,
                uow=uow,
                authenticated=True,
            )

    async def _set_runtime(self, mode: str) -> None:
        async with self.sessions() as db:
            await db.execute(
                text(
                    "UPDATE delivery_runtime_state "
                    "SET mode=:mode, cutover_epoch=:epoch, version=version+1"
                ),
                {"mode": mode, "epoch": self.epoch},
            )
            await db.commit()

    async def test_two_distinct_messages_serialize_history_count_and_sequences(self) -> None:
        first_inside_llm = asyncio.Event()
        release_first = asyncio.Event()
        observed_histories: list[list[str]] = []

        async def complete(**kwargs):
            contents = [item["content"] for item in kwargs["messages"]]
            observed_histories.append(contents)
            if len(observed_histories) == 1:
                first_inside_llm.set()
                await release_first.wait()
            return _llm_result(f"reply-{len(observed_histories)}")

        with (
            self._settings(),
            patch(
                "src.agents.agent_runner.llm_client.completar",
                AsyncMock(side_effect=complete),
            ),
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                AsyncMock(),
            ) as direct_send,
        ):
            first = asyncio.create_task(self._orchestrate(self._event("m-1", "one")))
            await asyncio.wait_for(first_inside_llm.wait(), timeout=2)
            second = asyncio.create_task(self._orchestrate(self._event("m-2", "two")))
            await asyncio.sleep(0.05)
            self.assertFalse(second.done())
            release_first.set()
            results = await asyncio.gather(first, second)

        self.assertEqual([result.body["status"] for result in results], ["queued", "queued"])
        direct_send.assert_not_awaited()
        self.assertIn("one", observed_histories[0])
        self.assertIn("reply-1", observed_histories[1])
        self.assertIn("two", observed_histories[1])

        async with self.sessions() as db:
            sessao = (await db.execute(select(Sessao))).scalar_one()
            self.assertEqual(sessao.total_mensagens, 2)
            self.assertEqual(sessao.next_delivery_sequence, 3)
            messages = list(
                (
                    await db.execute(select(Mensagem).order_by(Mensagem.id))
                ).scalars()
            )
            self.assertEqual(
                [(row.role, row.conteudo) for row in messages],
                [("user", "one"), ("assistant", "reply-1"), ("user", "two"), ("assistant", "reply-2")],
            )
            deliveries = list(
                (
                    await db.execute(
                        select(OutboxDelivery).order_by(OutboxDelivery.stream_sequence)
                    )
                ).scalars()
            )
            self.assertEqual([row.stream_sequence for row in deliveries], [1, 2])

    async def test_exact_replay_is_idempotent_and_changed_payload_is_rejected(self) -> None:
        first_inside_llm = asyncio.Event()
        release_first = asyncio.Event()

        async def complete(**kwargs):
            first_inside_llm.set()
            await release_first.wait()
            return _llm_result("reply")

        llm = AsyncMock(side_effect=complete)
        with self._settings(), patch(
            "src.agents.agent_runner.llm_client.completar", llm
        ):
            first_task = asyncio.create_task(
                self._orchestrate(self._event("same-id", "same"))
            )
            await asyncio.wait_for(first_inside_llm.wait(), timeout=2)
            replay_task = asyncio.create_task(
                self._orchestrate(self._event("same-id", "same"))
            )
            await asyncio.sleep(0.05)
            self.assertFalse(replay_task.done())
            release_first.set()
            first, replay = await asyncio.gather(first_task, replay_task)
            with self.assertRaises(evolution_orchestrator.inbound_event_repo.ReceiptCollisionError):
                await self._orchestrate(self._event("same-id", "changed"))

        self.assertEqual(first.body["status"], "queued")
        self.assertEqual(replay.body["status"], "already_accepted")
        self.assertEqual(llm.await_count, 1)
        async with self.sessions() as db:
            self.assertEqual(len((await db.execute(select(InboundEvent))).scalars().all()), 1)
            self.assertEqual(len((await db.execute(select(OutboxDelivery))).scalars().all()), 1)
            self.assertEqual(len((await db.execute(select(ConsumoLLM))).scalars().all()), 1)

    async def test_replay_uses_retained_receipt_key_after_rotation(self) -> None:
        event = self._event("rotated-replay", "same")
        llm = AsyncMock(return_value=_llm_result("reply"))
        with (
            self._settings(),
            patch.object(settings, "inbound_receipt_hmac_current_version", 1),
            patch.object(
                settings,
                "inbound_receipt_hmac_keys",
                {1: SecretStr("retained-key")},
            ),
            patch("src.agents.agent_runner.llm_client.completar", llm),
        ):
            first = await self._orchestrate(event)

        with (
            self._settings(),
            patch.object(settings, "inbound_receipt_hmac_current_version", 2),
            patch.object(
                settings,
                "inbound_receipt_hmac_keys",
                {1: SecretStr("retained-key"), 2: SecretStr("new-key")},
            ),
            patch("src.agents.agent_runner.llm_client.completar", llm),
        ):
            replay = await self._orchestrate(event)

        self.assertEqual(first.body["status"], "queued")
        self.assertEqual(replay.body["status"], "already_accepted")
        llm.assert_awaited_once()

    async def test_transfer_serializes_against_normal_message_and_reserves_transition(self) -> None:
        transfer_inside_llm = asyncio.Event()
        release_transfer = asyncio.Event()
        llm_calls = 0

        async def complete(**kwargs):
            nonlocal llm_calls
            llm_calls += 1
            if kwargs["tipo_chamada"] == "resposta":
                transfer_inside_llm.set()
                await release_transfer.wait()
                return _llm_result("transfer now [TRANSFERIR]")
            return _llm_result("private summary")

        with self._settings(), patch(
            "src.agents.agent_runner.llm_client.completar",
            AsyncMock(side_effect=complete),
        ), patch.object(
            evolution_orchestrator.evolution_client,
            "send_text_message",
            AsyncMock(side_effect=AssertionError("Evolution I/O in outbox mode")),
        ) as evolution_send, patch.object(
            evolution_orchestrator.handoff_manager.chatwoot_client,
            "get_or_create_contact",
            AsyncMock(side_effect=AssertionError("Chatwoot I/O in outbox mode")),
        ) as chatwoot_contact, patch.object(
            evolution_orchestrator.handoff_manager.chatwoot_client,
            "create_conversation",
            AsyncMock(side_effect=AssertionError("Chatwoot I/O in outbox mode")),
        ) as chatwoot_conversation:
            transfer = asyncio.create_task(
                self._orchestrate(self._event("transfer", "need human"))
            )
            await asyncio.wait_for(transfer_inside_llm.wait(), timeout=2)
            normal = asyncio.create_task(
                self._orchestrate(self._event("normal", "late message"))
            )
            await asyncio.sleep(0.05)
            self.assertFalse(normal.done())
            release_transfer.set()
            transfer_result, normal_result = await asyncio.gather(transfer, normal)

        evolution_send.assert_not_awaited()
        chatwoot_contact.assert_not_awaited()
        chatwoot_conversation.assert_not_awaited()
        self.assertEqual(transfer_result.body["status"], "queued")
        self.assertEqual(normal_result.body, {"status": "ignored", "reason": "closed_session"})
        self.assertEqual(llm_calls, 2)
        async with self.sessions() as db:
            sessao = (await db.execute(select(Sessao))).scalar_one()
            self.assertEqual(sessao.handler, "encerrada")
            self.assertEqual(sessao.next_delivery_sequence, 3)
            attempt = (await db.execute(select(HandoffAttempt))).scalar_one()
            self.assertEqual(attempt.status, "pending")
            rows = list((await db.execute(select(OutboxDelivery))).scalars())
            self.assertEqual(len(rows), 3)
            open_job = next(row for row in rows if row.provider == "chatwoot")
            response = next(
                row
                for row in rows
                if row.provider == "evolution" and row.depends_on_delivery_id is None
            )
            transition = next(row for row in rows if row.status == "blocked")
            self.assertEqual(response.stream_sequence, 1)
            self.assertEqual(transition.stream_sequence, 2)
            self.assertEqual(transition.depends_on_delivery_id, open_job.id)
            self.assertIsNone(transition.expires_at)
            self.assertIsNone(transition.next_attempt_at)

    async def test_transfer_only_skips_empty_response_and_preserves_handoff_sequence(self) -> None:
        async def complete(**kwargs):
            if kwargs["tipo_chamada"] == "resposta":
                return _llm_result("  [TRANSFERIR]  ")
            return _llm_result("summary")

        with (
            self._settings(),
            patch(
                "src.agents.agent_runner.llm_client.completar",
                AsyncMock(side_effect=complete),
            ),
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                AsyncMock(side_effect=AssertionError("direct send in outbox mode")),
            ) as direct_send,
        ):
            result = await self._orchestrate(
                self._event("transfer-only", "human please")
            )

        self.assertEqual(result.body["status"], "queued")
        direct_send.assert_not_awaited()
        async with self.sessions() as db:
            sessao = (await db.execute(select(Sessao))).scalar_one()
            rows = list((await db.execute(select(OutboxDelivery))).scalars())
            self.assertEqual(sessao.handler, "encerrada")
            self.assertEqual(sessao.next_delivery_sequence, 2)
            self.assertEqual(len(rows), 2)
            self.assertFalse(
                any(
                    row.provider == "evolution"
                    and row.depends_on_delivery_id is None
                    for row in rows
                )
            )
            transition = next(row for row in rows if row.provider == "evolution")
            open_job = next(row for row in rows if row.provider == "chatwoot")
            self.assertEqual(transition.stream_sequence, 1)
            self.assertEqual(transition.status, "blocked")
            self.assertEqual(transition.depends_on_delivery_id, open_job.id)
            self.assertEqual(
                len((await db.execute(select(ConsumoLLM))).scalars().all()),
                2,
            )

    async def test_whitespace_summary_uses_fallback_and_keeps_billable_usage(self) -> None:
        async def complete(**kwargs):
            if kwargs["tipo_chamada"] == "resposta":
                return _llm_result("visible response [TRANSFERIR]")
            return _llm_result("   ")

        with self._settings(), patch(
            "src.agents.agent_runner.llm_client.completar",
            AsyncMock(side_effect=complete),
        ):
            result = await self._orchestrate(
                self._event("blank-summary", "human please")
            )

        self.assertEqual(result.body["status"], "queued")
        async with self.sessions() as db:
            open_job = (
                await db.execute(
                    select(OutboxDelivery).where(OutboxDelivery.provider == "chatwoot")
                )
            ).scalar_one()
            self.assertTrue(open_job.payload["summary"].startswith("Handoff solicitado."))
            self.assertEqual(
                len((await db.execute(select(ConsumoLLM))).scalars().all()),
                2,
            )

    async def test_whitespace_response_commits_usage_with_controlled_error_only(self) -> None:
        with self._settings(), patch(
            "src.agents.agent_runner.llm_client.completar",
            AsyncMock(return_value=_llm_result("   ")),
        ):
            result = await self._orchestrate(
                self._event("blank-response", "hello")
            )

        self.assertEqual(result.body["reason"], "processing_error")
        async with self.sessions() as db:
            rows = list((await db.execute(select(OutboxDelivery))).scalars())
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].payload["message"], evolution_orchestrator.MENSAGEM_ERRO_TECNICO)
            self.assertEqual(
                len((await db.execute(select(ConsumoLLM))).scalars().all()),
                1,
            )
            messages = list((await db.execute(select(Mensagem))).scalars())
            self.assertEqual([(row.role, row.conteudo) for row in messages], [("user", "hello")])

    async def test_direct_transfer_only_never_sends_empty_response(self) -> None:
        await self._set_runtime("direct")

        async def complete(**kwargs):
            if kwargs["tipo_chamada"] == "resposta":
                return _llm_result("[TRANSFERIR]")
            return _llm_result("summary")

        evolution_send = AsyncMock()
        with (
            self._settings("direct"),
            patch(
                "src.agents.agent_runner.llm_client.completar",
                AsyncMock(side_effect=complete),
            ),
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                evolution_send,
            ),
            patch.object(
                evolution_orchestrator.handoff_manager.chatwoot_client,
                "get_or_create_contact",
                AsyncMock(return_value=41),
            ),
            patch.object(
                evolution_orchestrator.handoff_manager.chatwoot_client,
                "create_conversation",
                AsyncMock(return_value=42),
            ),
        ):
            result = await self._orchestrate(
                self._event("direct-transfer-only", "human please")
            )

        self.assertEqual(result.body["status"], "success")
        self.assertEqual(
            result.body["delivery_status"],
            "skipped_empty_transfer_response",
        )
        evolution_send.assert_awaited_once()
        self.assertTrue(evolution_send.await_args.args[1].strip())
        async with self.sessions() as db:
            self.assertEqual(len((await db.execute(select(OutboxDelivery))).scalars().all()), 0)
            self.assertEqual(
                len((await db.execute(select(ConsumoLLM))).scalars().all()),
                2,
            )

    async def test_soft_limit_and_technical_error_are_atomic_local_effects(self) -> None:
        async with self.sessions() as db:
            agente = await db.get(Agente, self.agente_id)
            agente.limite_mensal_creditos = Decimal("0")
            await db.commit()

        with self._settings(), patch(
            "src.agents.agent_runner.llm_client.completar", AsyncMock()
        ) as llm:
            result = await self._orchestrate(self._event("limit", "hello"))
        self.assertEqual(result.body["delivery_status"], "queued")
        llm.assert_not_awaited()

        async with self.sessions() as db:
            agente = await db.get(Agente, self.agente_id)
            agente.limite_mensal_creditos = None
            await db.commit()

        with self._settings(), patch(
            "src.agents.agent_runner.llm_client.completar",
            AsyncMock(side_effect=RuntimeError("provider unavailable secret body")),
        ):
            result = await self._orchestrate(
                self._event("error", "hello", phone="5511999999002")
            )
        self.assertEqual(result.body["delivery_status"], "queued")
        async with self.sessions() as db:
            deliveries = list((await db.execute(select(OutboxDelivery))).scalars())
            self.assertEqual(len(deliveries), 2)
            self.assertTrue(
                any(row.payload["message"] == evolution_orchestrator.MENSAGEM_LIMITE_ATINGIDO for row in deliveries)
            )
            error_session = (
                await db.execute(
                    select(Sessao).where(Sessao.telefone == "5511999999002")
                )
            ).scalar_one()
            self.assertEqual(error_session.handler, "encerrada")
            self.assertEqual(error_session.motivo_encerramento, "falha_tecnica")

    async def test_usage_and_commit_failures_roll_back_every_effect(self) -> None:
        with (
            self._settings(),
            patch(
                "src.agents.agent_runner.llm_client.completar",
                AsyncMock(return_value=_llm_result("reply")),
            ),
            patch.object(
                consumo_repo,
                "registrar_ou_reconciliar",
                AsyncMock(
                    side_effect=consumo_repo.UsagePersistenceInvariantError(
                        "no durable accounting"
                    )
                ),
            ),
        ):
            with self.assertRaises(consumo_repo.UsagePersistenceInvariantError):
                await self._orchestrate(self._event("usage-fail", "hello"))

        class FailingCommitUnitOfWork(UnitOfWork):
            async def commit(self) -> None:
                await self.rollback()
                raise RuntimeError("synthetic commit failure")

        from fastapi import HTTPException
        from src import main as main_module

        request = SimpleNamespace(
            headers={"Authorization": "Bearer route-secret"},
            json=AsyncMock(
                return_value={
                    "event": "messages.upsert",
                    "instance": self.instance_name,
                    "data": {
                        "key": {
                            "id": "commit-fail",
                            "fromMe": False,
                            "remoteJid": "5511999999001@s.whatsapp.net",
                        },
                        "message": {"conversation": "hello"},
                    },
                }
            ),
        )
        with self._settings(), patch.object(
            settings,
            "provider_webhook_auth_mode",
            "enforced",
        ), patch.object(
            settings,
            "evolution_webhook_bearer_token",
            "route-secret",
        ), patch(
            "src.agents.agent_runner.llm_client.completar",
            AsyncMock(return_value=_llm_result("reply")),
        ):
            async with FailingCommitUnitOfWork(self.sessions) as uow:
                with self.assertRaises(HTTPException) as raised:
                    await main_module.webhook_handler(request, uow)
                self.assertEqual(raised.exception.status_code, 503)

        async with self.sessions() as db:
            for model in (
                Sessao,
                Mensagem,
                ConsumoLLM,
                LLMUsageReconciliation,
                InboundEvent,
                OutboxDelivery,
            ):
                self.assertEqual(len((await db.execute(select(model))).scalars().all()), 0)

    async def test_consumption_insert_failure_commits_reconciliation_with_delivery(self) -> None:
        with (
            self._settings(),
            patch(
                "src.agents.agent_runner.llm_client.completar",
                AsyncMock(return_value=_llm_result("reply", "reconcile-generation")),
            ),
            patch.object(
                consumo_repo,
                "registrar",
                AsyncMock(side_effect=SQLAlchemyError("synthetic row failure")),
            ),
        ):
            result = await self._orchestrate(self._event("reconcile", "hello"))

        self.assertEqual(result.body["delivery_status"], "queued")
        async with self.sessions() as db:
            self.assertEqual(len((await db.execute(select(ConsumoLLM))).scalars().all()), 0)
            reconciliation = (
                await db.execute(select(LLMUsageReconciliation))
            ).scalar_one()
            self.assertEqual(reconciliation.provider_generation_id, "reconcile-generation")
            self.assertEqual(reconciliation.status, "pending")
            self.assertEqual(len((await db.execute(select(OutboxDelivery))).scalars().all()), 1)

    async def test_resume_commit_is_observed_before_message_rechecks_handler(self) -> None:
        phone = "5511999999003"
        async with self.sessions() as db:
            async with db.begin():
                sessao = await sessao_repo.get_or_create_for_update(
                    db,
                    self.agente_id,
                    phone,
                )
                sessao.handler = "encerrada"
                sessao.motivo_encerramento = "handoff"

        resume_has_lock = asyncio.Event()
        release_resume = asyncio.Event()

        async def resume() -> None:
            async with self.sessions() as db:
                async with db.begin():
                    sessao = await sessao_repo.lock_by_agente_telefone(
                        db,
                        self.agente_id,
                        phone,
                    )
                    assert sessao is not None
                    await handoff_attempt_repo.supersede_current_and_resume(
                        db,
                        sessao=sessao,
                    )
                    await outbox_repo.request_cancellation_for_session(
                        db,
                        sessao_id=sessao.id,
                    )
                    resume_has_lock.set()
                    await release_resume.wait()

        llm = AsyncMock(return_value=_llm_result("after resume"))
        with self._settings(), patch(
            "src.agents.agent_runner.llm_client.completar",
            llm,
        ):
            resume_task = asyncio.create_task(resume())
            await asyncio.wait_for(resume_has_lock.wait(), timeout=2)
            message_task = asyncio.create_task(
                self._orchestrate(self._event("after-resume", "hello", phone=phone))
            )
            await asyncio.sleep(0.05)
            self.assertFalse(message_task.done())
            release_resume.set()
            await resume_task
            result = await message_task

        self.assertEqual(result.body["status"], "queued")
        llm.assert_awaited_once()
        async with self.sessions() as db:
            sessao = (
                await db.execute(select(Sessao).where(Sessao.telefone == phone))
            ).scalar_one()
            self.assertEqual(sessao.handler, "bot")
            self.assertEqual(sessao.total_mensagens, 1)
            self.assertEqual(len((await db.execute(select(OutboxDelivery))).scalars().all()), 1)

    async def test_direct_and_outbox_ownership_never_double_send(self) -> None:
        await self._set_runtime("direct")
        direct_send = AsyncMock(return_value=None)
        llm = AsyncMock(return_value=_llm_result("direct reply"))
        with (
            self._settings("direct"),
            patch(
                "src.agents.agent_runner.llm_client.completar",
                llm,
            ),
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                direct_send,
            ),
        ):
            direct = await self._orchestrate(self._event("direct", "hello"))
            replay = await self._orchestrate(self._event("direct", "hello"))
        self.assertEqual(direct.body["delivery_status"], "sent")
        self.assertEqual(replay.body["status"], "already_accepted")
        direct_send.assert_awaited_once()

        await self._set_runtime("outbox")
        with (
            self._settings("outbox"),
            patch(
                "src.agents.agent_runner.llm_client.completar",
                AsyncMock(return_value=_llm_result("queued reply")),
            ),
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                AsyncMock(side_effect=AssertionError("direct send in outbox mode")),
            ) as forbidden_send,
        ):
            queued = await self._orchestrate(
                self._event("outbox", "hello", phone="5511999999002")
            )
        self.assertEqual(queued.body["delivery_status"], "queued")
        forbidden_send.assert_not_awaited()
        async with self.sessions() as db:
            rows = list((await db.execute(select(OutboxDelivery))).scalars())
            self.assertEqual(len(rows), 1)

    async def test_runtime_mismatch_and_frozen_state_fail_without_direct_fallback(self) -> None:
        direct_send = AsyncMock()
        with (
            self._settings("direct"),
            patch.object(
                evolution_orchestrator.evolution_client,
                "send_text_message",
                direct_send,
            ),
        ):
            with self.assertRaises(evolution_orchestrator.DeliveryOwnershipError):
                await self._orchestrate(self._event("mismatch", "hello"))
        direct_send.assert_not_awaited()

        await self._set_runtime("frozen")
        with self._settings("outbox"):
            with self.assertRaises(evolution_orchestrator.DeliveryOwnershipError):
                await self._orchestrate(self._event("frozen", "hello"))

        async with self.sessions() as db:
            self.assertEqual(len((await db.execute(select(InboundEvent))).scalars().all()), 0)
            self.assertEqual(len((await db.execute(select(Sessao))).scalars().all()), 0)


if __name__ == "__main__":
    unittest.main()
