import random
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import ExitStack
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from openai import APIStatusError, APITimeoutError
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, OperationalError

from src import delivery_worker, turn_worker
from src.database import db_errors
from src.agents import agent_runner
from src.config import Settings, settings
from src.services import llm_client
from src.services.evolution_orchestrator import MENSAGEM_ERRO_TECNICO
from src.services.turn_processor import draft_for, outcome_from_result, typing_schedule
from src.database.repositories import consumo_repo, scheduler_repo, turn_generation_repo


def _result(texto: str) -> llm_client.LLMResult:
    return llm_client.LLMResult(
        texto=texto,
        modelo_llm="openai/gpt-4o-mini",
        tokens_input=1,
        tokens_output=1,
        tokens_reasoning=None,
        tokens_cache_read=None,
        tokens_cache_write=None,
        custo_creditos=Decimal("0.00000100"),
        provider_generation_id="gen-unit",
    )


def _completion(content: str | None) -> SimpleNamespace:
    return SimpleNamespace(
        id="gen-unit",
        model="openai/gpt-4o-mini",
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
        usage=SimpleNamespace(prompt_tokens=3, completion_tokens=0, model_extra={"cost": "0.0000002"}),
        model_extra={},
    )


class TypingScheduleTests(unittest.TestCase):
    def test_bounds_and_persisted_jitter(self) -> None:
        rng = random.Random(1)
        self.assertEqual(typing_schedule("", jitter_max_ms=0, rng=rng), (2000, 0))
        self.assertEqual(typing_schedule("x" * 25, jitter_max_ms=0, rng=rng), (3000, 0))
        self.assertEqual(typing_schedule("x" * 1000, jitter_max_ms=0, rng=rng), (12000, 0))
        for text in ("", "x" * 100, "x" * 1000):
            for _ in range(200):
                delay, jitter = typing_schedule(text, jitter_max_ms=1000, rng=rng)
                self.assertTrue(2000 <= delay <= 12000)
                self.assertTrue(-1000 <= jitter <= 1000)
        with self.assertRaises(ValueError):
            typing_schedule("x", jitter_max_ms=1001, rng=rng)

    def test_same_seed_is_deterministic_but_restart_never_recomputes(self) -> None:
        # Recomputation would differ by rng state; the repository stores the
        # first draw and the guard trigger forbids rewriting it.
        first = typing_schedule("x" * 50, jitter_max_ms=1000, rng=random.Random(5))
        again = typing_schedule("x" * 50, jitter_max_ms=1000, rng=random.Random(5))
        self.assertEqual(first, again)


class DraftClassificationTests(unittest.TestCase):
    def test_only_generated_rev2_overrides_carried_handoff_intent_at_cap(self) -> None:
        predecessor = SimpleNamespace(
            id=10,
            status="superseded",
            superseded_reason="refresh_input_at_scheduler_cap",
            draft_kind="generated",
            needs_handoff=True,
        )
        for kind, expected in (
            (None, "decided"),
            ("technical_fallback", "decided"),
            ("soft_limit_fallback", "decided"),
            ("generated", "ordinary"),
        ):
            revision = SimpleNamespace(
                revision_number=2,
                draft_kind=kind,
                replacement_of_revision_id=10,
                needs_handoff=False,
                status="draft",
            )
            with self.subTest(kind=kind):
                self.assertEqual(
                    scheduler_repo._handoff_cap_disposition(
                        revision, predecessor, ()
                    ),
                    expected,
                )

    def test_transfer_only_is_a_generated_handoff_draft(self) -> None:
        result = _result(" [TRANSFERIR] ")
        draft = draft_for(outcome_from_result(result), result)
        self.assertEqual(draft, turn_generation_repo.DraftInput("generated", "", True))

    def test_empty_success_is_accounted_but_technical_fallback(self) -> None:
        result = _result("   ")
        outcome = outcome_from_result(result)
        self.assertEqual(outcome.usage.custo_creditos, Decimal("0.00000100"))
        self.assertEqual(
            draft_for(outcome, result),
            turn_generation_repo.DraftInput("technical_fallback", MENSAGEM_ERRO_TECNICO, False),
        )

    def test_known_failure_and_unknown(self) -> None:
        failed = turn_generation_repo.GenerationOutcome("failed", safe_error_code="provider_http_400")
        unknown = turn_generation_repo.GenerationOutcome(
            "outcome_unknown", safe_error_code="provider_outcome_unknown"
        )
        self.assertEqual(draft_for(failed, None).kind, "technical_fallback")
        self.assertIsNone(draft_for(unknown, None))
        with self.assertRaises(ValueError):
            turn_generation_repo.GenerationOutcome("outcome_unknown")


class ContextTests(unittest.TestCase):
    def test_batch_appended_once_after_bounded_history(self) -> None:
        messages = agent_runner.montar_contexto_turno(
            "sys",
            [("user", "antes"), ("assistant", "resposta")],
            [(10, "a"), (11, "a"), (12, "b")],
        )
        self.assertEqual(
            [m["content"] for m in messages], ["sys", "antes", "resposta", "a", "a", "b"]
        )
        with self.assertRaises(ValueError):
            agent_runner.montar_contexto_turno("sys", [], [(1, "a"), (1, "a")])
        with self.assertRaises(ValueError):
            agent_runner.montar_contexto_turno("sys", [], [])


class SingleAttemptProviderTests(unittest.IsolatedAsyncioTestCase):
    def _client(self, create: AsyncMock) -> MagicMock:
        client = MagicMock()
        client.chat.completions.create = create
        return client

    async def test_timeout_is_unknown_and_never_retried(self) -> None:
        request = httpx.Request("POST", "https://openrouter.invalid/v1/chat/completions")
        create = AsyncMock(side_effect=APITimeoutError(request=request))
        with patch.object(llm_client, "_client", return_value=self._client(create)):
            with self.assertRaises(llm_client.LLMProviderError) as raised:
                await llm_client.completar_tentativa_unica(
                    modelo_llm="m", messages=[], tipo_chamada="resposta"
                )
        self.assertFalse(raised.exception.outcome_known)
        self.assertEqual(create.await_count, 1)

    async def test_http_status_is_known_failure(self) -> None:
        request = httpx.Request("POST", "https://openrouter.invalid/v1/chat/completions")
        response = httpx.Response(400, request=request)
        create = AsyncMock(
            side_effect=APIStatusError("bad request", response=response, body=None)
        )
        with patch.object(llm_client, "_client", return_value=self._client(create)):
            with self.assertRaises(llm_client.LLMProviderError) as raised:
                await llm_client.completar_tentativa_unica(
                    modelo_llm="m", messages=[], tipo_chamada="resposta"
                )
        self.assertTrue(raised.exception.outcome_known)
        self.assertEqual(raised.exception.safe_error_code, "provider_http_400")

    async def test_empty_content_returns_usage(self) -> None:
        create = AsyncMock(return_value=_completion(None))
        with patch.object(llm_client, "_client", return_value=self._client(create)):
            result = await llm_client.completar_tentativa_unica(
                modelo_llm="m", messages=[], tipo_chamada="resposta"
            )
        self.assertEqual((result.texto, result.tokens_input), ("", 3))
        self.assertEqual(result.custo_creditos, Decimal("0.0000002"))

    async def test_missing_usage_or_choices_is_unknown_not_zero_cost(self) -> None:
        no_usage = _completion("texto")
        no_usage.usage = None
        no_choices = _completion("texto")
        no_choices.choices = []
        for completion in (no_usage, no_choices):
            create = AsyncMock(return_value=completion)
            with patch.object(llm_client, "_client", return_value=self._client(create)):
                with self.assertRaises(llm_client.LLMProviderError) as raised:
                    await llm_client.completar_tentativa_unica(
                        modelo_llm="m", messages=[], tipo_chamada="resposta"
                    )
            self.assertFalse(raised.exception.outcome_known)
            self.assertEqual(raised.exception.safe_error_code, "provider_response_unparseable")
            self.assertEqual(create.await_count, 1)

    async def test_legacy_completar_keeps_its_bounded_retry(self) -> None:
        request = httpx.Request("POST", "https://openrouter.invalid/v1/chat/completions")
        create = AsyncMock(side_effect=APITimeoutError(request=request))
        with patch.object(llm_client, "_client", return_value=self._client(create)):
            with self.assertRaises(RuntimeError):
                await llm_client.completar(modelo_llm="m", messages=[], tipo_chamada="resposta")
        self.assertEqual(create.await_count, 2)


class TransactionLostClassificationTests(unittest.IsolatedAsyncioTestCase):
    def test_lost_savepoint_chain_is_detected(self) -> None:
        deadlock = OperationalError("INSERT", {}, Exception(1213, "Deadlock found"))
        try:
            try:
                raise deadlock
            except OperationalError:
                raise OperationalError(
                    "ROLLBACK TO SAVEPOINT", {}, Exception(1305, "SAVEPOINT does not exist")
                )
        except OperationalError as lost:
            chained = lost
        self.assertEqual(sorted(db_errors.mysql_error_codes(chained)), [1213, 1305])
        self.assertTrue(db_errors.transaction_lost(chained))
        duplicate = IntegrityError("INSERT", {}, Exception(1062, "Duplicate entry"))
        self.assertFalse(db_errors.transaction_lost(duplicate))

    async def test_lost_savepoint_restarts_the_whole_transaction(self) -> None:
        class _Txn:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *exc):
                return False

        class _Session:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            def begin(self):
                return _Txn()

        calls: list[int] = []
        sleeps: list[float] = []

        async def operation(_db):
            calls.append(len(calls))
            if len(calls) == 1:
                try:
                    raise OperationalError("INSERT", {}, Exception(1213, "Deadlock found"))
                except OperationalError:
                    raise OperationalError(
                        "ROLLBACK TO SAVEPOINT", {}, Exception(1305, "SAVEPOINT does not exist")
                    )
            return "ok"

        async def sleep(seconds: float) -> None:
            sleeps.append(seconds)

        result = await scheduler_repo.run_transaction_with_deadlock_retry(
            _Session, operation, sleep=sleep
        )
        self.assertEqual((result, len(calls), len(sleeps)), ("ok", 2, 1))

    async def test_reconciliation_wrapper_reraises_lost_db_error_for_retry(self) -> None:
        class _Txn:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *exc):
                return False

        class _Session:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            def begin(self):
                return _Txn()

            def begin_nested(self):
                return _Txn()

        deadlock = OperationalError("INSERT", {}, Exception(1213, "Deadlock found"))
        wrapped = consumo_repo.UsagePersistenceInvariantError("fallback failed")
        wrapped.__cause__ = deadlock
        duplicate = IntegrityError("INSERT", {}, Exception(1062, "Duplicate entry"))
        persisted = SimpleNamespace(id=1)
        calls: list[int] = []

        async def operation(db):
            calls.append(len(calls))
            return await consumo_repo.registrar_ou_reconciliar(
                db,
                agente_id=1,
                sessao_id=2,
                modelo_llm="openai/gpt-4o-mini",
                tipo_chamada="resposta",
                tokens_input=1,
                tokens_output=1,
                custo_creditos=Decimal("0.00000100"),
            )

        with patch.object(
            consumo_repo,
            "registrar",
            AsyncMock(side_effect=[duplicate, persisted]),
        ), patch.object(
            consumo_repo.usage_reconciliation_repo,
            "record",
            AsyncMock(side_effect=wrapped),
        ):
            result = await scheduler_repo.run_transaction_with_deadlock_retry(
                _Session,
                operation,
                sleep=AsyncMock(),
            )

        self.assertIs(result.consumo, persisted)
        self.assertEqual(calls, [0, 1])

    async def test_consumption_insert_loss_never_enters_reconciliation_fallback(self) -> None:
        class _Txn:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *exc):
                return False

        db = MagicMock()
        db.begin_nested.return_value = _Txn()
        deadlock = OperationalError("INSERT", {}, Exception(1213, "Deadlock found"))
        record = AsyncMock(return_value=SimpleNamespace(id=1))
        with patch.object(
            consumo_repo,
            "registrar",
            AsyncMock(side_effect=deadlock),
        ), patch.object(consumo_repo.usage_reconciliation_repo, "record", record):
            with self.assertRaises(OperationalError):
                await consumo_repo.registrar_ou_reconciliar(
                    db,
                    agente_id=1,
                    sessao_id=2,
                    modelo_llm="openai/gpt-4o-mini",
                    tipo_chamada="resposta",
                    tokens_input=1,
                    tokens_output=1,
                    custo_creditos=Decimal("0.00000100"),
                )
        record.assert_not_awaited()

    async def test_maintenance_skips_a_lost_transaction_but_raises_others(self) -> None:
        class _Session:
            async def __aenter__(self):
                return AsyncMock()

            async def __aexit__(self, *exc):
                return False

        worker = delivery_worker.DeliveryWorker()
        worker._readiness_ok = True
        worker._provider_ready = True
        worker._runtime_compatible = True
        lost = OperationalError("UPDATE", {}, Exception(1213, "Deadlock found"))
        other = OperationalError("UPDATE", {}, Exception(1146, "Table missing"))
        maintenance = SimpleNamespace(
            recovered=0,
            ambiguity_counted=0,
            cancelled=0,
            expired=0,
            dependency_terminal=0,
        )
        usage = SimpleNamespace(examined=0)
        with tempfile.TemporaryDirectory() as directory, patch.object(
            delivery_worker, "AsyncSessionLocal", _Session
        ), patch.object(
            delivery_worker.settings, "delivery_worker_health_path", str(Path(directory) / "health.json")
        ), patch.object(
            delivery_worker.settings, "delivery_maintenance_skip_error_threshold", 2
        ), patch.object(delivery_worker.logger, "warning") as warning, patch.object(
            delivery_worker.logger, "error"
        ) as error:
            with patch.object(
                delivery_worker.delivery_worker_repo,
                "run_preclaim_maintenance",
                AsyncMock(side_effect=lost),
            ):
                self.assertIsNone(await worker._maintenance())
                worker._publish_health_snapshot()
                snapshot = json.loads(
                    Path(delivery_worker.settings.delivery_worker_health_path).read_text()
                )
                self.assertEqual(snapshot["maintenance_consecutive_skips"], 1)
                self.assertTrue(snapshot["maintenance_healthy"])
                self.assertTrue(snapshot["ready"])
                self.assertEqual(warning.call_count, 1)
                error.assert_not_called()

                self.assertIsNone(await worker._maintenance())
                worker._publish_health_snapshot()
                snapshot = json.loads(
                    Path(delivery_worker.settings.delivery_worker_health_path).read_text()
                )
                self.assertEqual(snapshot["maintenance_consecutive_skips"], 2)
                self.assertFalse(snapshot["maintenance_healthy"])
                self.assertFalse(snapshot["ready"])
                self.assertEqual(error.call_count, 1)
                self.assertFalse(worker._stop.is_set())

            with patch.object(
                delivery_worker.delivery_worker_repo,
                "run_preclaim_maintenance",
                AsyncMock(return_value=maintenance),
            ), patch.object(
                delivery_worker.usage_reconciliation_repo,
                "reconcile_due",
                AsyncMock(return_value=usage),
            ):
                self.assertIsNone(await worker._maintenance())
            worker._publish_health_snapshot()
            snapshot = json.loads(
                Path(delivery_worker.settings.delivery_worker_health_path).read_text()
            )
            self.assertEqual(snapshot["maintenance_consecutive_skips"], 0)
            self.assertTrue(snapshot["maintenance_healthy"])
            self.assertTrue(snapshot["ready"])

            with patch.object(
                delivery_worker.delivery_worker_repo,
                "run_preclaim_maintenance",
                AsyncMock(side_effect=other),
            ):
                with self.assertRaises(OperationalError):
                    await worker._maintenance()


class FeatureGateTests(unittest.TestCase):
    _base = {
        "openrouter_api_key": "synthetic",
        "evolution_api_url": "http://evolution.invalid",
        "evolution_api_key": "synthetic",
        "database_url": "mysql+aiomysql://u:p@db.invalid/x",
        "_env_file": None,
    }

    def test_processor_is_off_by_default_and_requires_scheduler_gate(self) -> None:
        self.assertFalse(Settings(**self._base).inbox_turn_processor_enabled)
        with self.assertRaises(ValidationError):
            Settings(**self._base, inbox_turn_processor_enabled=True)

    def test_entrypoint_idles_when_disabled_and_refuses_when_enabled(self) -> None:
        with patch.object(settings, "inbox_turn_processor_enabled", False):
            self.assertEqual(turn_worker.main(), turn_worker.EXIT_DISABLED)
        with patch.object(settings, "inbox_turn_processor_enabled", True):
            self.assertEqual(turn_worker.main(), turn_worker.EXIT_REFUSED)

    def test_scheduler_requires_evolution_lease_beyond_io_deadline(self) -> None:
        epoch = "11111111-1111-4111-8111-111111111111"
        enabled = {
            "inbox_scheduler_enabled": True,
            "inbox_scheduler_epoch": epoch,
            "delivery_cutover_epoch": epoch,
            "delivery_mode": "outbox",
            "provider_webhook_auth_mode": "enforced",
            "inbound_receipt_hmac_current_version": 1,
            "inbound_receipt_hmac_keys": {"1": "synthetic"},
        }
        with self.assertRaises(ValidationError):
            Settings(
                **self._base,
                **enabled,
                delivery_evolution_lease_seconds=65,
                delivery_evolution_deadline_seconds=60,
            )
        Settings(
            **self._base,
            **enabled,
            delivery_evolution_lease_seconds=66,
            delivery_evolution_deadline_seconds=60,
        )

    def test_dispatch_only_worker_requires_evolution_lease_beyond_io_deadline(self) -> None:
        # X4: a delivery worker with dispatch on but the scheduler off still
        # claims automated rows; the lease rule applies to it independently.
        epoch = "11111111-1111-4111-8111-111111111111"
        dispatch_only = {
            "delivery_dispatch_enabled": True,
            "delivery_mode": "outbox",
            "delivery_cutover_epoch": epoch,
            "provider_webhook_auth_mode": "enforced",
            "inbound_receipt_hmac_current_version": 1,
            "inbound_receipt_hmac_keys": {"1": "synthetic"},
        }
        for lease in (60, 65):
            with self.subTest(lease=lease), self.assertRaises(ValidationError):
                Settings(
                    **self._base,
                    **dispatch_only,
                    delivery_evolution_lease_seconds=lease,
                    delivery_evolution_deadline_seconds=60,
                )
        self.assertFalse(
            Settings(
                **self._base,
                **dispatch_only,
                delivery_evolution_lease_seconds=66,
                delivery_evolution_deadline_seconds=60,
            ).inbox_scheduler_enabled
        )

    def test_entrypoint_runs_only_with_scheduler_gate_and_both_epochs(self) -> None:
        epoch = "11111111-1111-4111-8111-111111111111"
        complete = {
            "inbox_turn_processor_enabled": True,
            "inbox_scheduler_enabled": True,
            "inbox_scheduler_epoch": epoch,
            "delivery_cutover_epoch": epoch,
        }
        for missing in (
            "inbox_scheduler_enabled",
            "inbox_scheduler_epoch",
            "delivery_cutover_epoch",
        ):
            values = complete | {missing: False if missing.endswith("enabled") else None}
            with ExitStack() as stack:
                for name, value in values.items():
                    stack.enter_context(patch.object(settings, name, value))
                run = stack.enter_context(patch.object(turn_worker.asyncio, "run"))
                with self.subTest(missing=missing):
                    self.assertEqual(turn_worker.main(), turn_worker.EXIT_REFUSED)
                    run.assert_not_called()
        with ExitStack() as stack:
            for name, value in complete.items():
                stack.enter_context(patch.object(settings, name, value))
            run = stack.enter_context(
                patch.object(turn_worker.asyncio, "run", side_effect=lambda coro: coro.close())
            )
            self.assertEqual(turn_worker.main(), 0)
            run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
