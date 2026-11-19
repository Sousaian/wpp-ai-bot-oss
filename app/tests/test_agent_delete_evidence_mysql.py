"""S3 (P2-2): agent delete never cascades away usage or handoff evidence.

Real disposable MySQL (whole-DB fixture of the T08-2 suite), fake providers only.
"""
import os
import asyncio
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from src.database.models import (
    Agente, ConsumoLLM, ConversationTurn, HandoffAttempt, LLMUsageReconciliation, Mensagem,
    OutboxDelivery, Sessao,
)
from src.database.repositories import lifecycle_repo, scheduler_retention_repo as retention
from src.routers import agentes as agentes_router, sessoes as sessoes_router
from tests import test_scheduler_retention_mysql as base


# Contract literal (not imported) so the suite also runs, and fails, on the pre-fix base.
EVIDENCE = "evidencia de uso ou handoff retida; desative o agente (ativo=false) em vez de excluir"
SESSION_HANDOFF = "historico de handoff retido; a sessao nao pode ser excluida"


class AgentDeleteEvidenceMySQLTests(base.SchedulerRetentionMySQLTests):
    # ── fixtures ────────────────────────────────────────────────────────
    async def _agent(self, label: str) -> tuple[int, int]:
        async with self.sessions() as db:
            agent = Agente(nome=f"sec-s3 {label}", instance_name=f"sec-s3-{uuid4()}",
                           system_prompt="synthetic", numero_notificacao="0")
            db.add(agent)
            await db.flush()
            session = Sessao(agente_id=agent.id, telefone=f"synthetic-s3-{label}", handler="bot")
            db.add(session)
            await db.flush()
            db.add(Mensagem(sessao_id=session.id, role="user", conteudo="kept"))
            await db.commit()
            return agent.id, session.id

    async def _consumo(self, agent: int, session: int | None, db=None) -> None:
        row = ConsumoLLM(agente_id=agent, sessao_id=session, modelo_llm="openai/gpt-4o-mini",
                         tipo_chamada="resposta", tokens_input=1, tokens_output=1,
                         custo_creditos=Decimal("0.0001"))
        if db is not None:
            db.add(row)
            await db.flush()
            return
        async with self.sessions() as own:
            own.add(row)
            await own.commit()

    async def _handoff(self, agent: int, session: int) -> None:
        async with self.sessions() as db:
            db.add(HandoffAttempt(handoff_key=str(uuid4()), agente_id=agent, sessao_id=session,
                                  generation=1, status="failed", terminal_at=datetime.utcnow(),
                                  chatwoot_inbox_id=1))
            await db.commit()

    async def _reconciliation(self, agent: int, session: int) -> None:
        async with self.sessions() as db:
            db.add(LLMUsageReconciliation(reconciliation_key=str(uuid4()), agente_id=agent,
                                          sessao_id=session, modelo_llm="openai/gpt-4o-mini",
                                          tipo_chamada="resposta", safe_error_code="synthetic"))
            await db.commit()

    async def _http_delete(self, agent: int) -> tuple[int, str | None]:
        async with self.sessions() as db:
            try:
                await agentes_router.deletar_agente(agente_id=agent, db=db)
                await db.commit()
                return 204, None
            except HTTPException as exc:
                return exc.status_code, exc.detail

    async def _agent_rows(self, model, agent: int) -> int:
        async with self.sessions() as db:
            return len((await db.execute(select(model.id).where(model.agente_id == agent))).all())

    async def _snapshot(self) -> tuple[int, ...]:
        return tuple([await self.count(model) for model in
                      (Agente, Sessao, Mensagem, ConsumoLLM, HandoffAttempt, LLMUsageReconciliation)])

    async def _refused(self, agent: int) -> None:
        before = await self._snapshot()
        signals = lifecycle_repo.lifecycle_counts()["agent_delete_evidence_refused"]
        self.assertEqual(await self._http_delete(agent), (409, f"Agente: {EVIDENCE}"))
        self.assertEqual(await self._snapshot(), before)
        self.assertEqual(lifecycle_repo.lifecycle_counts()["agent_delete_evidence_refused"], signals + 1)

    # ── one test per evidence table ─────────────────────────────────────
    async def test_legacy_agent_with_consumo_refused(self) -> None:
        agent, session = await self._agent("consumo")
        await self._consumo(agent, session)
        await self._refused(agent)
        # Ledger row without session (administrative usage) also counts.
        other, _ = await self._agent("consumo-null")
        await self._consumo(other, None)
        await self._refused(other)

    async def test_agent_with_handoff_attempt_refused(self) -> None:
        agent, session = await self._agent("handoff")
        await self._handoff(agent, session)
        await self._refused(agent)

    async def test_agent_with_usage_reconciliation_refused(self) -> None:
        agent, session = await self._agent("reconciliation")
        await self._reconciliation(agent, session)
        await self._refused(agent)

    async def test_agent_without_evidence_still_deletes(self) -> None:
        keep, session = await self._agent("keep")
        await self._consumo(keep, session)  # another tenant's evidence is irrelevant
        agent, _ = await self._agent("clean")
        self.assertEqual(await self._http_delete(agent), (204, None))
        self.assertEqual(await self._snapshot(), (1, 1, 1, 1, 0, 0))
        self.assertEqual(await self._http_delete(agent), (404, "Agente nao encontrado"))

    async def test_scheduler_agent_after_t082_purge_still_refused(self) -> None:
        graph = await self.graph(handoff=True)
        self.assertEqual(await self._http_delete(graph["agent"]),
                         (409, f"Agente: {lifecycle_repo.SCHEDULER_HISTORY_MESSAGE}"))
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual(result.turns_purged, 1)
        self.assertEqual((await self.count(ConsumoLLM), await self.count(HandoffAttempt)), (1, 1))
        await self._refused(graph["agent"])
        self.assertEqual((await self.count(ConsumoLLM), await self.count(HandoffAttempt)), (1, 1))

    async def test_runtime_guard_runs_on_locked_row_before_evidence(self) -> None:
        agent, session = await self._agent("runtime")
        await self._consumo(agent, session)
        async with self.sessions.begin() as db:
            await db.execute(text("UPDATE delivery_runtime_state SET mode='frozen'"))
        self.assertEqual((await self._http_delete(agent))[0], 503)
        self.assertEqual(await self.count(ConsumoLLM), 1)

    async def test_stale_transaction_fails_closed(self) -> None:
        agent, _ = await self._agent("stale")
        async with self.sessions() as db:
            await db.execute(select(Agente.id))  # opens a read view first
            with self.assertRaises(lifecycle_repo.StaleTransactionError):
                await lifecycle_repo.delete_agent(db, agente_id=agent)
            await db.rollback()
        self.assertEqual(await self.count(Agente), 1)

    # ── A1/A3: footprint and races ──────────────────────────────────────
    async def _waits(self) -> int:
        async with self.sessions() as db:
            return int((await db.execute(text(
                "SELECT COUNT(*) FROM performance_schema.data_lock_waits"))).scalar_one())

    async def _locks(self) -> list[tuple]:
        async with self.sessions() as db:
            return [tuple(r) for r in (await db.execute(text(
                "SELECT OBJECT_NAME, INDEX_NAME, LOCK_MODE FROM performance_schema.data_locks "
                "WHERE OBJECT_SCHEMA=DATABASE() AND LOCK_TYPE='RECORD'"))).all()]

    async def _other_tenant_writes_never_wait(self, others: list[tuple[int, int]]) -> None:
        async def insert_all():
            for agent, session in others:
                async with self.sessions() as db:
                    await self._consumo(agent, session, db)
                    db.add(HandoffAttempt(handoff_key=str(uuid4()), agente_id=agent, sessao_id=session,
                                          generation=1 + await self.count(HandoffAttempt),
                                          status="failed", terminal_at=datetime.utcnow(),
                                          chatwoot_inbox_id=1))
                    db.add(LLMUsageReconciliation(reconciliation_key=str(uuid4()), agente_id=agent,
                        sessao_id=session, modelo_llm="openai/gpt-4o-mini",
                        tipo_chamada="resposta", safe_error_code="synthetic"))
                    db.add(Sessao(agente_id=agent, telefone=f"synthetic-s3-{uuid4()}", handler="bot"))
                    await db.commit()
        await asyncio.wait_for(insert_all(), timeout=10)
        self.assertEqual(await self._waits(), 0)

    async def test_evidence_check_footprint_other_tenants_never_wait(self) -> None:
        """Paused right after the check, both branches: record locks only,
        none on evidence tables; neighbouring tenants insert without waiting."""

        low = await self._agent("low")
        target = await self._agent("target")
        high = await self._agent("high")
        await self._consumo(*target)
        original = lifecycle_repo._has_agent_evidence
        cases = 0
        for has_evidence in (True, False):
            if not has_evidence:
                async with self.sessions.begin() as db:
                    await db.execute(text("DELETE FROM consumo_llm WHERE agente_id=:a"), {"a": target[0]})
            paused, release = asyncio.Event(), asyncio.Event()

            async def held(db, agente_id):
                found = await original(db, agente_id)
                paused.set()
                await release.wait()
                return found

            async def deleter():
                async with self.sessions() as db:
                    try:
                        return await lifecycle_repo.delete_agent(db, agente_id=target[0])
                    except lifecycle_repo.LifecycleRefused:
                        return "refused"
                    finally:
                        await db.rollback()

            with patch.object(lifecycle_repo, "_has_agent_evidence", held):
                task = asyncio.create_task(deleter())
                await asyncio.wait_for(paused.wait(), 10)
                locks = await self._locks()
                self.assertTrue(locks, "delete transaction holds its canonical locks")
                self.assertTrue(all(mode.endswith("REC_NOT_GAP") for _, _, mode in locks), locks)
                evidence = {m.__tablename__ for m in lifecycle_repo.AGENT_DELETE_EVIDENCE_TABLES}
                self.assertEqual([l for l in locks if l[0] in evidence], [], locks)
                await self._other_tenant_writes_never_wait([low, high])
                release.set()
                self.assertEqual(await task, "refused" if has_evidence else "deleted")
            cases += 1
        self.assertEqual(cases, 2)

    async def test_delete_vs_consumo_insert_race_both_orders_repeated(self) -> None:
        for n in range(3):
            # Order A: delete holds agent X; the insert waits on the FK S lock,
            # then fails (agent gone) — no ledger row is ever cascaded away.
            agent, session = await self._agent(f"race-a-{n}")
            paused, release = asyncio.Event(), asyncio.Event()
            original = lifecycle_repo._has_agent_evidence

            async def held(db, agente_id):
                found = await original(db, agente_id)
                paused.set()
                await release.wait()
                return found

            async def insert():
                async with self.sessions() as db:
                    try:
                        await self._consumo(agent, session, db)
                        await db.commit()
                        return "inserted"
                    except IntegrityError as exc:
                        return ("integrity", lifecycle_repo.db_errors.mysql_error_codes(exc), str(exc.orig)[:200])
                    except DBAPIError as exc:
                        return ("dberror", lifecycle_repo.db_errors.mysql_error_codes(exc))

            with patch.object(lifecycle_repo, "_has_agent_evidence", held):
                async with self.sessions() as deleter:
                    delete = asyncio.create_task(lifecycle_repo.delete_agent(deleter, agente_id=agent))
                    await asyncio.wait_for(paused.wait(), 10)
                    pending = asyncio.create_task(insert())
                    await self._lock_wait()
                    release.set()
                    self.assertEqual(await delete, "deleted")
                    await deleter.commit()
            outcome = await pending
            # 1216/1452: child FK refused (1216 is the detail-less form for a DML-only user).
            self.assertEqual(outcome[0], "integrity", outcome)
            self.assertTrue({1216, 1452} & set(outcome[1]), outcome)
            self.assertEqual(await self._agent_rows(ConsumoLLM, agent), 0)
            # Order B: an open insert holds the FK S lock; delete waits on agent
            # X, then sees the committed ledger row and refuses.
            agent, session = await self._agent(f"race-b-{n}")
            async with self.sessions() as writer:
                await self._consumo(agent, session, writer)
                delete = asyncio.create_task(self._http_delete(agent))
                await self._lock_wait()
                await writer.commit()
            self.assertEqual(await delete, (409, f"Agente: {EVIDENCE}"))
            self.assertEqual(await self._agent_rows(ConsumoLLM, agent), 1)

    async def _lock_wait(self) -> None:
        deadline = asyncio.get_running_loop().time() + 10
        while not await self._waits():
            if asyncio.get_running_loop().time() > deadline:
                self.fail("no real InnoDB lock wait observed")
            await asyncio.sleep(0.02)

    # ── DELETE /sessoes/{telefone} (user decision (a)) ──────────────────
    async def _session_delete(self, agent: int, phone: str) -> tuple[int, str | None]:
        async with self.sessions() as db:
            try:
                await sessoes_router.deletar_sessao(telefone=phone, agente_id=agent, db=db)
                await db.commit()
                return 204, None
            except HTTPException as exc:
                return exc.status_code, exc.detail

    async def _session_refused(self, agent: int, phone: str) -> None:
        before = await self._snapshot()
        signals = lifecycle_repo.lifecycle_counts()["session_delete_handoff_refused"]
        self.assertEqual(await self._session_delete(agent, phone), (409, f"Sessao: {SESSION_HANDOFF}"))
        self.assertEqual(await self._snapshot(), before)
        self.assertEqual(lifecycle_repo.lifecycle_counts()["session_delete_handoff_refused"], signals + 1)

    async def test_session_with_handoff_refused(self) -> None:
        agent, session = await self._agent("s-handoff")
        await self._handoff(agent, session)
        await self._session_refused(agent, "synthetic-s3-s-handoff")

    async def test_legacy_v4_session_with_handoff_refused_after_outbox_cleanup(self) -> None:
        agent, session = await self._agent("s-legacy")
        async with self.sessions() as db:
            await db.execute(text("UPDATE sessoes SET handler='encerrada', motivo_encerramento='handoff', "
                                  "encerrada_em=UTC_TIMESTAMP(), handoff_generation=1 WHERE id=:s"),
                             {"s": session})
            attempt = HandoffAttempt(handoff_key=str(uuid4()), agente_id=agent, sessao_id=session,
                                     generation=1, status="ready", ready_at=datetime.utcnow(),
                                     chatwoot_inbox_id=1, chatwoot_contact_id=1,
                                     chatwoot_conversation_id=900000 + agent)
            db.add(attempt)
            await db.commit()
        await self._session_refused(agent, "synthetic-s3-s-legacy")
        # Without any outbox/scheduled dependent (legacy cleanup done) the
        # guard alone keeps the attempt and its Chatwoot mapping.
        self.assertEqual(await self.count(OutboxDelivery), 0)
        await self._session_refused(agent, "synthetic-s3-s-legacy")
        self.assertEqual(await self._agent_rows(HandoffAttempt, agent), 1)

    async def test_session_without_handoff_deletes_and_keeps_ledger(self) -> None:
        agent, session = await self._agent("s-clean")
        await self._consumo(agent, session)
        await self._reconciliation(agent, session)
        self.assertEqual(await self._session_delete(agent, "synthetic-s3-s-clean"), (204, None))
        async with self.sessions() as db:
            consumo = (await db.execute(select(ConsumoLLM.sessao_id).where(
                ConsumoLLM.agente_id == agent))).scalars().all()
            reconciliation = (await db.execute(select(LLMUsageReconciliation.sessao_id).where(
                LLMUsageReconciliation.agente_id == agent))).scalars().all()
        self.assertEqual((consumo, reconciliation), ([None], [None]))
        self.assertEqual(await self.count(Sessao), 0)
        self.assertEqual(await self.count(Mensagem), 0)
        self.assertEqual(await self._session_delete(agent, "synthetic-s3-s-clean"),
                         (404, "Sessao nao encontrada"))

    async def test_session_after_t082_purge_and_legacy_cleanup_still_refused(self) -> None:
        graph = await self.graph(handoff=True)
        await self.handoff_effects(graph)
        self.assertEqual(await self._session_delete(graph["agent"], "synthetic-t082"),
                         (409, f"Sessao: {lifecycle_repo.SCHEDULER_HISTORY_MESSAGE}"))
        passes = 0
        while await self.count(ConversationTurn) or await self.count(OutboxDelivery):
            passes += 1
            self.assertLessEqual(passes, 8, "retention did not converge")
            await self.worker_pass()
        self.assertEqual((await self.count(ConsumoLLM), await self.count(HandoffAttempt)), (1, 1))
        await self._session_refused(graph["agent"], "synthetic-t082")
        await self._refused(graph["agent"])

    async def test_session_stale_transaction_fails_closed(self) -> None:
        agent, _ = await self._agent("s-stale")
        async with self.sessions() as db:
            await db.execute(select(Sessao.id))
            with self.assertRaises(lifecycle_repo.StaleTransactionError):
                await lifecycle_repo.delete_session(db, agente_id=agent, telefone="synthetic-s3-s-stale")
            await db.rollback()
        self.assertEqual(await self.count(Sessao), 1)

    async def test_documented_contract_parity(self) -> None:
        doc = (Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2])) / "AGENTS.md").read_text()
        self.assertIn("agent_delete_evidence_refused", doc)
        self.assertIn("session_delete_handoff_refused", doc)
        self.assertEqual(lifecycle_repo.SESSION_HANDOFF_MESSAGE, SESSION_HANDOFF)
        for model in lifecycle_repo.AGENT_DELETE_EVIDENCE_TABLES:
            self.assertIn(f"`{model.__tablename__}`", doc)
        self.assertEqual(lifecycle_repo.AGENT_EVIDENCE_MESSAGE, EVIDENCE)
        self.assertEqual({m.__tablename__ for m in lifecycle_repo.AGENT_DELETE_EVIDENCE_TABLES},
                         {"consumo_llm", "handoff_attempts", "llm_usage_reconciliation"})


for _name in dir(base.SchedulerRetentionMySQLTests):
    if _name.startswith("test_") and _name not in AgentDeleteEvidenceMySQLTests.__dict__:
        setattr(AgentDeleteEvidenceMySQLTests, _name, None)
