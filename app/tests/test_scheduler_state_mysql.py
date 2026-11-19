import asyncio
import os
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    UniqueConstraint,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.database.models import (
    AgentServiceLane,
    Agente,
    ConversationTurn,
    ConsumoLLM,
    GenerationAttempt,
    HandoffAttempt,
    InboundEvent,
    Mensagem,
    OutboxDelivery,
    SchedulerRuntimeState,
    TurnInputMembership,
    TurnRevision,
)
from src.database.repositories import (
    inbound_event_repo,
    outbox_repo,
    scheduler_repo,
    sessao_repo,
)


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")


class SchedulerStateMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        self.sessions = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
        self.scheduler_epoch = str(uuid4())
        self.cutover_epoch = str(uuid4())
        async with self.engine.begin() as connection:
            await connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))
            for table in (
                "delivery_operator_actions",
                "outbox_deliveries",
                "generation_attempts",
                "turn_input_memberships",
                "turn_revisions",
                "conversation_turns",
                "agent_service_lanes",
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
                    "UPDATE delivery_runtime_state SET mode='outbox', "
                    "cutover_epoch=:epoch, version=1, changed_by='test', "
                    "change_reason='disposable scheduler test' WHERE id=1"
                ),
                {"epoch": self.cutover_epoch},
            )
            await connection.execute(
                text(
                    "UPDATE scheduler_runtime_state SET state='enabled', "
                    "scheduler_epoch=:epoch, version=3, schema_version=1, "
                    "max_pending_inputs_per_session=128, "
                    "max_pending_input_bytes_per_session=262144, "
                    "max_queued_turns_per_agent=1000, "
                    "max_queued_bytes_per_agent=16777216, "
                    "max_global_queued_turns=10000, "
                    "max_global_queued_bytes=268435456, "
                    "max_generation_attempts_per_turn=2, "
                    "max_delivery_attempts_per_turn=8, "
                    "max_refreshes_per_turn=1, accounting_drain_seconds=30, "
                    "changed_by='test', change_reason='disposable scheduler test' "
                    "WHERE id=1"
                ),
                {"epoch": self.scheduler_epoch},
            )

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def _agent(self, suffix: str = "a") -> int:
        async with self.sessions() as db:
            row = Agente(
                nome=f"Scheduler {suffix}",
                instance_name=f"scheduler-{suffix}-{uuid4()}",
                system_prompt="synthetic",
                numero_notificacao="0",
            )
            db.add(row)
            await db.commit()
            return row.id

    async def _turn(
        self,
        *,
        agente_id: int,
        telefone: str,
        successor: bool = False,
        ready_at: datetime | None = None,
    ) -> tuple[int, int, int]:
        async with self.sessions() as db:
            async with db.begin():
                _, _, _, lane = await scheduler_repo.lock_authority_prefix(
                    db,
                    agente_id=agente_id,
                    scheduler_epoch=self.scheduler_epoch,
                    create_lane=True,
                )
                session = await sessao_repo.get_or_create_for_update(
                    db, agente_id, telefone
                )
                turn = await scheduler_repo.create_turn(
                    db,
                    lane=lane,
                    sessao=session,
                    scheduler_epoch=self.scheduler_epoch,
                    ready_at=ready_at or datetime.utcnow() - timedelta(seconds=1),
                    successor=successor,
                )
                revision = await scheduler_repo.create_revision(
                    db,
                    turn=turn,
                    scheduler_epoch=self.scheduler_epoch,
                )
                return session.id, turn.id, revision.id

    async def _membership(
        self,
        *,
        agente_id: int,
        sessao_id: int,
        turn_id: int,
        provider_event_id: str,
        content: str,
    ) -> int:
        async with self.sessions() as db:
            async with db.begin():
                _, scheduler, _, _ = await scheduler_repo.lock_authority_prefix(
                    db,
                    agente_id=agente_id,
                    scheduler_epoch=self.scheduler_epoch,
                )
                receipt = InboundEvent(
                    provider="evolution",
                    source_scope="ticket02",
                    provider_event_id=provider_event_id,
                    event_type="messages.upsert",
                    payload_hmac=("a" * 64),
                    hmac_key_version=1,
                )
                message = Mensagem(sessao_id=sessao_id, role="user", conteudo=content)
                db.add_all((receipt, message))
                await db.flush()
                membership, created = await scheduler_repo.append_input_membership(
                    db,
                    scheduler=scheduler,
                    turn_id=turn_id,
                    sessao_id=sessao_id,
                    mensagem_id=message.id,
                    inbound_event_id=receipt.id,
                    content_bytes=len(content.encode("utf-8")),
                )
                self.assertTrue(created)
                return membership.id

    async def _add_consumption(
        self,
        db: AsyncSession,
        *,
        agente_id: int,
        sessao_id: int,
        provider_generation_id: str,
        tokens_input: int,
        tokens_output: int,
        custo_creditos: Decimal,
        modelo_llm: str = "synthetic/model",
        tipo_chamada: str = "resposta",
        tokens_reasoning: int | None = None,
        tokens_cache_read: int | None = None,
        tokens_cache_write: int | None = None,
    ) -> ConsumoLLM:
        row = ConsumoLLM(
            agente_id=agente_id,
            sessao_id=sessao_id,
            modelo_llm=modelo_llm,
            tipo_chamada=tipo_chamada,
            tokens_input=tokens_input,
            tokens_output=tokens_output,
            tokens_reasoning=tokens_reasoning,
            tokens_cache_read=tokens_cache_read,
            tokens_cache_write=tokens_cache_write,
            custo_creditos=custo_creditos,
            provider_generation_id=provider_generation_id,
        )
        db.add(row)
        await db.flush()
        return row

    async def test_schema_matches_orm_authority_contract(self) -> None:
        authority_tables = (
            "scheduler_runtime_state",
            "agent_service_lanes",
            "conversation_turns",
            "turn_revisions",
            "turn_input_memberships",
            "generation_attempts",
        )

        def live_authority(sync_connection):
            inspector = inspect(sync_connection)
            def normalized_type(value) -> str:
                return value.compile(dialect=sync_connection.dialect).lower().replace(
                    " collate utf8mb4_unicode_ci", ""
                ).replace(" character set ascii", "").replace(
                    "tinyint(1)", "bool"
                ).replace("numeric(", "decimal(")
            return {
                table: {
                    "columns": {
                        item["name"]: (
                            item["nullable"],
                            normalized_type(item["type"]),
                        )
                        for item in inspector.get_columns(table)
                    },
                    "fks": {item["name"] for item in inspector.get_foreign_keys(table)},
                    "uniques": {
                        item["name"] for item in inspector.get_unique_constraints(table)
                    },
                    "checks": {
                        item["name"] for item in inspector.get_check_constraints(table)
                    },
                    "indexes": {
                        item["name"]: tuple(item["column_names"])
                        for item in inspector.get_indexes(table)
                        if not item.get("unique") and not item["name"].startswith("fk_")
                    },
                }
                for table in authority_tables
            }

        async with self.engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
            self.assertTrue(
                {
                    "scheduler_runtime_state",
                    "agent_service_lanes",
                    "conversation_turns",
                    "turn_revisions",
                    "turn_input_memberships",
                    "generation_attempts",
                }.issubset(tables)
            )
            outbox_columns = await connection.run_sync(
                lambda sync: {c["name"] for c in inspect(sync).get_columns("outbox_deliveries")}
            )
            self.assertTrue(
                {"delivery_class", "scheduler_epoch", "conversation_turn_id", "turn_revision_id"}
                .issubset(outbox_columns)
            )
            live = await connection.run_sync(live_authority)
            generation_consumption_delete_rule = (
                await connection.execute(
                    text(
                        "SELECT delete_rule FROM information_schema.referential_constraints "
                        "WHERE constraint_schema=DATABASE() "
                        "AND table_name='generation_attempts' "
                        "AND constraint_name='fk_generation_consumo'"
                    )
                )
            ).scalar_one()
            self.assertEqual(generation_consumption_delete_rule, "RESTRICT")
        for table in authority_tables:
            metadata = ConversationTurn.metadata.tables[table]
            self.assertEqual(
                {
                    column.name: (
                        column.nullable,
                        column.type.compile(dialect=self.engine.dialect).lower().replace(
                            " collate utf8mb4_unicode_ci", ""
                        ).replace(" character set ascii", "").replace(
                            "tinyint(1)", "bool"
                        ).replace("numeric(", "decimal("),
                    )
                    for column in metadata.columns
                },
                live[table]["columns"],
            )
            self.assertEqual(
                {
                    constraint.name
                    for constraint in metadata.constraints
                    if isinstance(constraint, ForeignKeyConstraint)
                },
                live[table]["fks"],
            )
            self.assertEqual(
                {
                    constraint.name
                    for constraint in metadata.constraints
                    if isinstance(constraint, UniqueConstraint)
                },
                live[table]["uniques"],
            )
            self.assertEqual(
                {
                    constraint.name
                    for constraint in metadata.constraints
                    if isinstance(constraint, CheckConstraint)
                },
                live[table]["checks"],
            )
            self.assertEqual(
                {
                    index.name: tuple(column.name for column in index.columns)
                    for index in metadata.indexes
                    if not index.unique
                },
                live[table]["indexes"],
            )

    async def test_unique_active_and_pending_successor_barriers(self) -> None:
        agent = await self._agent()
        session_id, _, _ = await self._turn(agente_id=agent, telefone="55110001")
        await self._turn(
            agente_id=agent, telefone="55110001", successor=True
        )
        async with self.sessions() as db:
            async with db.begin():
                _, _, _, lane = await scheduler_repo.lock_authority_prefix(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                )
                session = await sessao_repo.lock_by_id(db, session_id, refresh_existing=True)
                with self.assertRaises(IntegrityError):
                    async with db.begin_nested():
                        await scheduler_repo.create_turn(
                            db,
                            lane=lane,
                            sessao=session,
                            scheduler_epoch=self.scheduler_epoch,
                            ready_at=datetime.utcnow(),
                            successor=True,
                        )

    async def test_two_claimers_serialize_on_one_agent_lane(self) -> None:
        agent = await self._agent()
        await self._turn(agente_id=agent, telefone="55110002")
        await self._turn(agente_id=agent, telefone="55110003")
        first_claimed = asyncio.Event()
        release = asyncio.Event()

        async def first() -> scheduler_repo.ClaimedTurn | None:
            async with self.sessions() as db:
                async with db.begin():
                    claim = await scheduler_repo.claim_ready_turn(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        worker_id="worker-first",
                        lease_seconds=30,
                    )
                    first_claimed.set()
                    await release.wait()
                    return claim

        async def second() -> scheduler_repo.ClaimedTurn | None:
            await first_claimed.wait()
            async with self.sessions() as db:
                async with db.begin():
                    return await scheduler_repo.claim_ready_turn(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        worker_id="worker-second",
                        lease_seconds=30,
                    )

        one = asyncio.create_task(first())
        two = asyncio.create_task(second())
        await first_claimed.wait()
        await asyncio.sleep(0.1)
        self.assertFalse(two.done())
        release.set()
        first_result, second_result = await asyncio.gather(one, two)
        self.assertIsNotNone(first_result)
        self.assertIsNone(second_result)

    async def test_expired_owner_requires_recovery_before_replacement_claim(self) -> None:
        agent = await self._agent("expired")
        first_session, first_turn, _ = await self._turn(
            agente_id=agent, telefone="55110101"
        )
        _, second_turn, _ = await self._turn(
            agente_id=agent, telefone="55110102"
        )
        async with self.sessions() as db:
            async with db.begin():
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="expired-owner",
                    lease_seconds=30,
                )
        self.assertEqual(claim.turn_id, first_turn)
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE agent_service_lanes SET lease_until="
                    "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE agente_id=:agent"
                ),
                {"agent": agent},
            )
            await connection.execute(
                text(
                    "UPDATE conversation_turns SET claim_until="
                    "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=:turn"
                ),
                {"turn": first_turn},
            )
        async with self.sessions() as db:
            async with db.begin():
                self.assertFalse(
                    await scheduler_repo.renew_lane_lease(
                        db,
                        turn_id=first_turn,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        owner_token=claim.claim_token,
                        lease_seconds=30,
                    )
                )
        async with self.sessions() as db:
            async with db.begin():
                self.assertIsNone(
                    await scheduler_repo.claim_ready_turn(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        worker_id="must-not-overwrite",
                        lease_seconds=30,
                    )
                )

        recovery_locked = asyncio.Event()
        release_recovery = asyncio.Event()

        async def recover() -> str:
            async with self.sessions() as db:
                async with db.begin():
                    await scheduler_repo.lock_authority_prefix(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        require_enabled=False,
                    )
                    recovery_locked.set()
                    await release_recovery.wait()
                    return await scheduler_repo.recover_expired_lane(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        retry_at=datetime.utcnow() + timedelta(seconds=30),
                    )

        async def replacement_claim():
            await recovery_locked.wait()
            async with self.sessions() as db:
                async with db.begin():
                    return await scheduler_repo.claim_ready_turn(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        worker_id="replacement-owner",
                        lease_seconds=30,
                    )

        recovering = asyncio.create_task(recover())
        replacement = asyncio.create_task(replacement_claim())
        await recovery_locked.wait()
        await asyncio.sleep(0.1)
        self.assertFalse(replacement.done())
        release_recovery.set()
        recovery_result, replacement_result = await asyncio.gather(
            recovering, replacement
        )
        self.assertEqual(recovery_result, "retry")
        self.assertEqual(replacement_result.turn_id, second_turn)
        async with self.sessions() as db:
            stale = await db.get(ConversationTurn, first_turn)
            self.assertEqual(stale.sessao_id, first_session)
            self.assertEqual(stale.status, "retry")
            self.assertIsNone(stale.claim_token)

    async def test_late_known_generation_requires_audited_resolution(self) -> None:
        agent = await self._agent("late-known")
        session_id, turn_id, revision_id = await self._turn(
            agente_id=agent, telefone="55110103"
        )
        await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=turn_id,
            provider_event_id="late-known-1",
            content="contabilize",
        )
        async with self.sessions() as db:
            async with db.begin():
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="late-known-worker",
                    lease_seconds=30,
                )
        async with self.sessions() as db:
            async with db.begin():
                await scheduler_repo.seal_revision_range(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    expected_input_version=1,
                )
                attempt = await scheduler_repo.start_generation_attempt(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    attempt_type="response",
                    modelo_llm="synthetic/model",
                )
                attempt_key = attempt.attempt_key
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'generation success requires durable accounting state'):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE generation_attempts SET outcome='succeeded', "
                            "accounting_state='pending', "
                            "provider_generation_id='raw-unaccounted-success', "
                            "tokens_input=1,tokens_output=1,custo_creditos=0.00000100, "
                            "outcome_recorded_at=UTC_TIMESTAMP(6) "
                            "WHERE attempt_key=:attempt_key"
                        ),
                        {"attempt_key": attempt_key},
                    )
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE agent_service_lanes SET lease_until="
                    "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE agente_id=:agent"
                ),
                {"agent": agent},
            )
            await connection.execute(
                text(
                    "UPDATE conversation_turns SET claim_until="
                    "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=:turn"
                ),
                {"turn": turn_id},
            )
        async with self.sessions() as db:
            async with db.begin():
                self.assertEqual(
                    await scheduler_repo.recover_expired_lane(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        retry_at=datetime.utcnow() + timedelta(seconds=30),
                    ),
                    "quarantined",
                )
        async with self.sessions() as db:
            async with db.begin():
                self.assertTrue(
                    await scheduler_repo.record_generation_outcome(
                        db,
                        attempt_key=attempt_key,
                        lane_owner_token=claim.claim_token,
                        scheduler_epoch=self.scheduler_epoch,
                        outcome="succeeded",
                        provider_generation_id="late-known-generation",
                        tokens_input=7,
                        tokens_output=3,
                        custo_creditos=Decimal("0.00000300"),
                    )
                )
                self.assertTrue(
                    await scheduler_repo.record_generation_outcome(
                        db,
                        attempt_key=attempt_key,
                        lane_owner_token=claim.claim_token,
                        scheduler_epoch=self.scheduler_epoch,
                        outcome="succeeded",
                        provider_generation_id="late-known-generation",
                        tokens_input=7,
                        tokens_output=3,
                        custo_creditos=Decimal("0.00000300"),
                    )
                )
                for conflict in (
                    {"tokens_input": 999},
                    {"custo_creditos": Decimal("9.00000000")},
                    {"safe_error_code": "conflicting-error"},
                ):
                    values = {
                        "tokens_input": 7,
                        "tokens_output": 3,
                        "custo_creditos": Decimal("0.00000300"),
                        **conflict,
                    }
                    self.assertFalse(
                        await scheduler_repo.record_generation_outcome(
                            db,
                            attempt_key=attempt_key,
                            lane_owner_token=claim.claim_token,
                            scheduler_epoch=self.scheduler_epoch,
                            outcome="succeeded",
                            provider_generation_id="late-known-generation",
                            **values,
                        )
                    )
        async with self.sessions() as db:
            async with db.begin():
                self.assertEqual(
                    await scheduler_repo.recover_expired_lane(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        retry_at=datetime.utcnow() + timedelta(seconds=30),
                    ),
                    "quarantined",
                )
                self.assertFalse(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="operator-test",
                        reason="known late provider completion reviewed",
                        resolved_outcome="succeeded",
                        provider_generation_id="late-known-generation",
                        tokens_input=7,
                        tokens_output=3,
                        custo_creditos=Decimal("0.00000300"),
                    )
                )
        other_agent = await self._agent("wrong-consumption-owner")
        other_session_id, _, _ = await self._turn(
            agente_id=other_agent, telefone="55110130"
        )
        mismatched_rows = (
            {"agente_id": other_agent, "sessao_id": other_session_id},
            {"modelo_llm": "synthetic/wrong-model"},
            {"tipo_chamada": "resumo"},
            {"tokens_input": 8},
            {"custo_creditos": Decimal("0.00000900")},
        )
        for mismatch in mismatched_rows:
            async with self.sessions() as db:
                async with db.begin():
                    values = {
                        "agente_id": agent,
                        "sessao_id": session_id,
                        "provider_generation_id": "late-known-generation",
                        "tokens_input": 7,
                        "tokens_output": 3,
                        "custo_creditos": Decimal("0.00000300"),
                        **mismatch,
                    }
                    mismatched_consumption = await self._add_consumption(db, **values)
                    self.assertFalse(
                        await scheduler_repo.resolve_generation_reconciliation(
                            db,
                            attempt_key=attempt_key,
                            scheduler_epoch=self.scheduler_epoch,
                            lane_owner_token=claim.claim_token,
                            actor="operator-test",
                            reason="known late provider completion reviewed",
                            resolved_outcome="succeeded",
                            provider_generation_id="late-known-generation",
                            tokens_input=7,
                            tokens_output=3,
                            custo_creditos=Decimal("0.00000300"),
                            consumo_llm_id=mismatched_consumption.id,
                        )
                    )
                    await db.delete(mismatched_consumption)
                    await db.flush()
        async with self.sessions() as db:
            async with db.begin():
                wrong_consumption = await self._add_consumption(
                    db,
                    agente_id=agent,
                    sessao_id=session_id,
                    provider_generation_id="wrong-generation-link",
                    tokens_input=7,
                    tokens_output=3,
                    custo_creditos=Decimal("0.00000300"),
                )
                wrong_consumption_id = wrong_consumption.id
                self.assertFalse(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="operator-test",
                        reason="known late provider completion reviewed",
                        resolved_outcome="succeeded",
                        provider_generation_id="late-known-generation",
                        tokens_input=7,
                        tokens_output=3,
                        custo_creditos=Decimal("0.00000300"),
                        consumo_llm_id=wrong_consumption_id,
                    )
                )
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'accounted generation success requires consumption link'):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE generation_attempts SET reconciled_at=UTC_TIMESTAMP(6), "
                            "reconciliation_actor='raw-test', "
                            "reconciliation_reason='missing commercial link' "
                            "WHERE attempt_key=:attempt_key"
                        ),
                        {"attempt_key": attempt_key},
                    )
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'generation consumption link does not match evidence'):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE generation_attempts SET consumo_llm_id=:consumo, "
                            "reconciled_at=UTC_TIMESTAMP(6), "
                            "reconciliation_actor='raw-test', "
                            "reconciliation_reason='wrong commercial link' "
                            "WHERE attempt_key=:attempt_key"
                        ),
                        {
                            "attempt_key": attempt_key,
                            "consumo": wrong_consumption_id,
                        },
                    )
        with self.assertRaises(RuntimeError):
            async with self.sessions() as db:
                async with db.begin():
                    rolled_back = await self._add_consumption(
                        db,
                        agente_id=agent,
                        sessao_id=session_id,
                        provider_generation_id="late-known-generation",
                        tokens_input=7,
                        tokens_output=3,
                        custo_creditos=Decimal("0.00000300"),
                    )
                    self.assertTrue(
                        await scheduler_repo.resolve_generation_reconciliation(
                            db,
                            attempt_key=attempt_key,
                            scheduler_epoch=self.scheduler_epoch,
                            lane_owner_token=claim.claim_token,
                            actor="operator-test",
                            reason="known late provider completion reviewed",
                            resolved_outcome="succeeded",
                            provider_generation_id="late-known-generation",
                            tokens_input=7,
                            tokens_output=3,
                            custo_creditos=Decimal("0.00000300"),
                            consumo_llm_id=rolled_back.id,
                        )
                    )
                    raise RuntimeError("force reconciliation rollback")
        async with self.sessions() as db:
            async with db.begin():
                attempt_after_rollback = (
                    await db.execute(
                        select(GenerationAttempt)
                        .where(GenerationAttempt.attempt_key == attempt_key)
                        .with_for_update()
                    )
                ).scalar_one()
                self.assertIsNone(attempt_after_rollback.reconciled_at)
                self.assertIsNone(attempt_after_rollback.consumo_llm_id)
                self.assertEqual(
                    (
                        await db.execute(
                            select(func.count(ConsumoLLM.id)).where(
                                ConsumoLLM.provider_generation_id
                                == "late-known-generation"
                            )
                        )
                    ).scalar_one(),
                    0,
                )
                consumption = await self._add_consumption(
                    db,
                    agente_id=agent,
                    sessao_id=session_id,
                    provider_generation_id="late-known-generation",
                    tokens_input=7,
                    tokens_output=3,
                    custo_creditos=Decimal("0.00000300"),
                )
                consumption_id = consumption.id
                self.assertTrue(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="operator-test",
                        reason="known late provider completion reviewed",
                        resolved_outcome="succeeded",
                        provider_generation_id="late-known-generation",
                        tokens_input=7,
                        tokens_output=3,
                        custo_creditos=Decimal("0.00000300"),
                        consumo_llm_id=consumption_id,
                    )
                )
        async with self.sessions() as db:
            async with db.begin():
                exact = {
                    "attempt_key": attempt_key,
                    "scheduler_epoch": self.scheduler_epoch,
                    "lane_owner_token": claim.claim_token,
                    "actor": "operator-test",
                    "reason": "known late provider completion reviewed",
                    "resolved_outcome": "succeeded",
                    "provider_generation_id": "late-known-generation",
                    "tokens_input": 7,
                    "tokens_output": 3,
                    "custo_creditos": Decimal("0.00000300"),
                    "consumo_llm_id": consumption_id,
                }
                self.assertTrue(
                    await scheduler_repo.resolve_generation_reconciliation(db, **exact)
                )
                self.assertTrue(
                    await scheduler_repo.record_generation_outcome(
                        db,
                        attempt_key=attempt_key,
                        lane_owner_token=claim.claim_token,
                        scheduler_epoch=self.scheduler_epoch,
                        outcome="succeeded",
                        provider_generation_id="late-known-generation",
                        tokens_input=7,
                        tokens_output=3,
                        custo_creditos=Decimal("0.00000300"),
                        consumo_llm_id=consumption_id,
                    )
                )
                self.assertFalse(
                    await scheduler_repo.record_generation_outcome(
                        db,
                        attempt_key=attempt_key,
                        lane_owner_token=claim.claim_token,
                        scheduler_epoch=self.scheduler_epoch,
                        outcome="succeeded",
                        provider_generation_id="late-known-generation",
                        tokens_input=999,
                        tokens_output=3,
                        custo_creditos=Decimal("9.00000000"),
                        consumo_llm_id=consumption_id,
                    )
                )
                incomplete = dict(exact)
                incomplete["tokens_input"] = None
                self.assertFalse(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db, **incomplete
                    )
                )
                for field, value in (
                    ("tokens_input", 999),
                    ("custo_creditos", Decimal("9.00000000")),
                    ("safe_error_code", "conflicting-error"),
                    ("consumo_llm_id", wrong_consumption_id),
                ):
                    conflict = dict(exact)
                    conflict[field] = value
                    self.assertFalse(
                        await scheduler_repo.resolve_generation_reconciliation(
                            db, **conflict
                        )
                    )
        async with self.sessions() as db:
            with self.assertRaises(IntegrityError):
                async with db.begin():
                    linked = await db.get(ConsumoLLM, consumption_id)
                    await db.delete(linked)
                    await db.flush()
        async with self.sessions() as db:
            async with db.begin():
                self.assertEqual(
                    await scheduler_repo.recover_expired_lane(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        retry_at=datetime.utcnow() + timedelta(seconds=30),
                    ),
                    "retry",
                )

    async def test_genuine_unknown_can_be_resolved_once_without_domain_authority(self) -> None:
        agent = await self._agent("unknown-generation")
        session_id, turn_id, revision_id = await self._turn(
            agente_id=agent, telefone="55110104"
        )
        await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=turn_id,
            provider_event_id="unknown-generation-1",
            content="ambíguo",
        )
        async with self.sessions() as db:
            async with db.begin():
                state = await scheduler_repo.get_scheduler_state(db, for_update=True)
                state.accounting_drain_seconds = 1
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="unknown-worker",
                    lease_seconds=30,
                )
        async with self.sessions() as db:
            async with db.begin():
                await scheduler_repo.seal_revision_range(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    expected_input_version=1,
                )
                attempt = await scheduler_repo.start_generation_attempt(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    attempt_type="response",
                    modelo_llm="synthetic/model",
                )
                attempt_key = attempt.attempt_key
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE agent_service_lanes SET lease_until="
                    "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE agente_id=:agent"
                ),
                {"agent": agent},
            )
            await connection.execute(
                text(
                    "UPDATE conversation_turns SET claim_until="
                    "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=:turn"
                ),
                {"turn": turn_id},
            )
        async with self.sessions() as db:
            async with db.begin():
                self.assertEqual(
                    await scheduler_repo.recover_expired_lane(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        retry_at=datetime.utcnow() + timedelta(seconds=30),
                    ),
                    "quarantined",
                )
        await asyncio.sleep(1.05)
        async with self.sessions() as db:
            async with db.begin():
                self.assertEqual(
                    await scheduler_repo.recover_expired_lane(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        retry_at=datetime.utcnow() + timedelta(seconds=30),
                    ),
                    "quarantined",
                )
                self.assertIsNone(
                    await scheduler_repo.claim_ready_turn(
                        db,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        worker_id="blocked-by-unknown",
                        lease_seconds=30,
                    )
                )
        async with self.sessions() as db:
            async with db.begin():
                self.assertFalse(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="operator-test",
                        reason="provider audit proved success",
                        resolved_outcome="succeeded",
                        provider_generation_id="reconciled-generation",
                        tokens_input=8,
                        tokens_output=5,
                        custo_creditos=Decimal("0.00000400"),
                        safe_error_code="lane_lease_expired_unknown_outcome",
                    )
                )
                consumption = await self._add_consumption(
                    db,
                    agente_id=agent,
                    sessao_id=session_id,
                    provider_generation_id="reconciled-generation",
                    tokens_input=8,
                    tokens_output=5,
                    custo_creditos=Decimal("0.00000400"),
                )
                consumption_id = consumption.id
                self.assertTrue(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="operator-test",
                        reason="provider audit proved success",
                        resolved_outcome="succeeded",
                        provider_generation_id="reconciled-generation",
                        tokens_input=8,
                        tokens_output=5,
                        custo_creditos=Decimal("0.00000400"),
                        consumo_llm_id=consumption_id,
                        safe_error_code="lane_lease_expired_unknown_outcome",
                    )
                )
                self.assertTrue(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="operator-test",
                        reason="provider audit proved success",
                        resolved_outcome="succeeded",
                        provider_generation_id="reconciled-generation",
                        tokens_input=8,
                        tokens_output=5,
                        custo_creditos=Decimal("0.00000400"),
                        consumo_llm_id=consumption_id,
                        safe_error_code="lane_lease_expired_unknown_outcome",
                    )
                )
                self.assertFalse(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="operator-test",
                        reason="provider audit proved success",
                        resolved_outcome="succeeded",
                        provider_generation_id="reconciled-generation",
                        tokens_input=999,
                        tokens_output=5,
                        custo_creditos=Decimal("9.00000000"),
                        consumo_llm_id=consumption_id,
                        safe_error_code="lane_lease_expired_unknown_outcome",
                    )
                )
                self.assertFalse(
                    await scheduler_repo.resolve_generation_reconciliation(
                        db,
                        attempt_key=attempt_key,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        actor="other-operator",
                        reason="conflicting audit",
                        resolved_outcome="failed",
                        safe_error_code="lane_lease_expired_unknown_outcome",
                    )
                )
        async with self.sessions() as db:
            row = (
                await db.execute(
                    select(GenerationAttempt).where(
                        GenerationAttempt.attempt_key == attempt_key
                    )
                )
            ).scalar_one()
            lane = await db.get(AgentServiceLane, agent)
            self.assertEqual(row.outcome, "succeeded")
            self.assertEqual(row.safe_error_code, "lane_lease_expired_unknown_outcome")
            self.assertIsNotNone(row.reconciled_at)
            self.assertEqual(lane.owner_token, claim.claim_token)

    async def test_sealed_range_and_membership_identity_are_immutable(self) -> None:
        agent = await self._agent()
        session_id, turn_id, revision_id = await self._turn(
            agente_id=agent, telefone="55110004"
        )
        membership_id = await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=turn_id,
            provider_event_id="range-1",
            content="olá 🌎",
        )
        async with self.sessions() as db:
            async with db.begin():
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="range-worker",
                    lease_seconds=30,
                )
            self.assertIsNotNone(claim)
        async with self.sessions() as db:
            async with db.begin():
                revision = await scheduler_repo.seal_revision_range(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    expected_input_version=1,
                )
                self.assertEqual(
                    (revision.sealed_first_input_sequence, revision.sealed_last_input_sequence),
                    (1, 1),
                )
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'sealed revision input range is immutable'):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE turn_revisions SET sealed_last_input_sequence=2 "
                            "WHERE id=:id"
                        ),
                        {"id": revision_id},
                    )
            with self.assertRaisesRegex(DBAPIError, 'turn input identity is immutable'):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE turn_input_memberships SET input_sequence=2 "
                            "WHERE id=:id"
                        ),
                        {"id": membership_id},
                    )

    async def test_refresh_seals_original_and_late_input_once(self) -> None:
        agent = await self._agent("refresh")
        session_id, turn_id, revision_one_id = await self._turn(
            agente_id=agent, telefone="55110105"
        )
        await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=turn_id,
            provider_event_id="refresh-original",
            content="original",
        )
        async with self.sessions() as db:
            async with db.begin():
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="refresh-worker",
                    lease_seconds=30,
                )
        async with self.sessions() as db:
            async with db.begin():
                await scheduler_repo.seal_revision_range(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_one_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    expected_input_version=1,
                )
        await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=turn_id,
            provider_event_id="refresh-late",
            content="late",
        )
        async with self.sessions() as db:
            async with db.begin():
                _, _, lane, _, turn = await scheduler_repo._lock_owned_turn(
                    db,
                    turn_id=turn_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                )
                revision_two = await scheduler_repo.create_revision(
                    db,
                    turn=turn,
                    scheduler_epoch=self.scheduler_epoch,
                    replacement_of_revision_id=revision_one_id,
                )
                lane.active_revision_id = revision_two.id
                revision_two = await scheduler_repo.seal_revision_range(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_two.id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    expected_input_version=2,
                )
                revision_two_id = revision_two.id
                self.assertEqual(
                    (
                        revision_two.sealed_first_input_sequence,
                        revision_two.sealed_last_input_sequence,
                        revision_two.sealed_input_count,
                        revision_two.sealed_input_bytes,
                    ),
                    (1, 2, 2, len("original".encode()) + len("late".encode())),
                )
        _, successor_turn, _ = await self._turn(
            agente_id=agent, telefone="55110105", successor=True
        )
        await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=successor_turn,
            provider_event_id="refresh-successor",
            content="successor",
        )
        async with self.sessions() as db:
            revision_two = await db.get(TurnRevision, revision_two_id)
            memberships = tuple(
                (
                    await db.execute(
                        select(TurnInputMembership)
                        .where(TurnInputMembership.sessao_id == session_id)
                        .order_by(TurnInputMembership.input_sequence)
                    )
                )
                .scalars()
                .all()
            )
            self.assertEqual(
                [row.assigned_revision_id for row in memberships],
                [revision_one_id, revision_two_id, None],
            )
            self.assertEqual(
                (
                    revision_two.sealed_first_input_sequence,
                    revision_two.sealed_last_input_sequence,
                    revision_two.sealed_input_count,
                ),
                (1, 2, 2),
            )

    async def test_stale_lease_epoch_and_exact_attempt_accounting(self) -> None:
        agent = await self._agent()
        session_id, turn_id, revision_id = await self._turn(
            agente_id=agent, telefone="55110005"
        )
        await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=turn_id,
            provider_event_id="attempt-1",
            content="teste",
        )
        async with self.sessions() as db:
            async with db.begin():
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="attempt-worker",
                    lease_seconds=30,
                )
        async with self.sessions() as db:
            async with db.begin():
                await scheduler_repo.seal_revision_range(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    expected_input_version=1,
                )
                attempt = await scheduler_repo.start_generation_attempt(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    attempt_type="response",
                    modelo_llm="synthetic/model",
                )
                attempt_key = attempt.attempt_key
        async with self.sessions() as db:
            async with db.begin():
                lane = await db.get(AgentServiceLane, agent)
                turn = await db.get(ConversationTurn, turn_id)
                lane.owner_token = None
                lane.owner_worker_id = None
                lane.lease_until = None
                lane.active_turn_id = None
                lane.active_revision_id = None
                turn.claim_token = None
                turn.claim_owner = None
                turn.claim_until = None
        async with self.sessions() as db:
            async with db.begin():
                self.assertFalse(
                    await scheduler_repo.record_generation_outcome(
                        db,
                        attempt_key=attempt_key,
                        lane_owner_token=str(uuid4()),
                        scheduler_epoch=self.scheduler_epoch,
                        outcome="outcome_unknown",
                    )
                )
        async with self.sessions() as db:
            async with db.begin():
                self.assertTrue(
                    await scheduler_repo.record_generation_outcome(
                        db,
                        attempt_key=attempt_key,
                        lane_owner_token=claim.claim_token,
                        scheduler_epoch=self.scheduler_epoch,
                        outcome="succeeded",
                        provider_generation_id="synthetic-generation-1",
                        tokens_input=10,
                        tokens_output=4,
                        custo_creditos=Decimal("0.00000100"),
                    )
                )
        async with self.sessions() as db:
            async with db.begin():
                self.assertFalse(
                    await scheduler_repo.renew_lane_lease(
                        db,
                        turn_id=turn_id,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        owner_token=claim.claim_token,
                        lease_seconds=30,
                    )
                )

    async def test_tail_retry_yields_fifo_to_other_contact(self) -> None:
        agent = await self._agent()
        _, first_turn, _ = await self._turn(agente_id=agent, telefone="55110006")
        _, second_turn, _ = await self._turn(agente_id=agent, telefone="55110007")
        async with self.sessions() as db:
            async with db.begin():
                first = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="retry-worker",
                    lease_seconds=30,
                )
        self.assertEqual(first.turn_id, first_turn)
        async with self.sessions() as db:
            async with db.begin():
                self.assertTrue(
                    await scheduler_repo.yield_turn_for_retry(
                        db,
                        turn_id=first_turn,
                        sessao_id=first.sessao_id,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        owner_token=first.claim_token,
                        retry_at=datetime.utcnow() + timedelta(seconds=30),
                    )
                )
        async with self.sessions() as db:
            async with db.begin():
                second = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="second-worker",
                    lease_seconds=30,
                )
        self.assertEqual(second.turn_id, second_turn)
        async with self.sessions() as db:
            first_row = await db.get(ConversationTurn, first_turn)
            second_row = await db.get(ConversationTurn, second_turn)
            self.assertGreater(first_row.scheduling_sequence, second_row.scheduling_sequence)

    async def test_unknown_legacy_delivery_blocks_enablement(self) -> None:
        agent = await self._agent()
        async with self.sessions() as db:
            async with db.begin():
                session = await sessao_repo.get_or_create_for_update(db, agent, "55110008")
                sequence = (await sessao_repo.allocate_delivery_sequences(db, session))[0]
                common = {
                    "cutover_epoch": self.cutover_epoch,
                    "provider": "evolution",
                    "operation": "evolution.send_text.v1",
                    "payload_version": 1,
                    "payload": {"synthetic": True},
                    "ordering_key": session.delivery_stream_key,
                    "stream_sequence": sequence,
                    "next_attempt_at": datetime.utcnow(),
                    "expires_at": datetime.utcnow() + timedelta(hours=1),
                    "agente_id": agent,
                    "sessao_id": session.id,
                }
                delivery = OutboxDelivery(
                    delivery_key=str(uuid4()),
                    dedupe_key=f"retained-legacy:{uuid4()}",
                    delivery_class="legacy_unknown",
                    status="pending",
                    **common,
                )
                db.add(delivery)
                await db.flush()
                self.assertEqual(delivery.delivery_class, "legacy_unknown")
        async with self.sessions() as db:
            async with db.begin():
                state = await db.get(SchedulerRuntimeState, 1)
                state.state = "disabled"
                state.version = 1
                await db.flush()
        async with self.sessions() as db:
            # T08-1: disabled→enabling runs the full clearance guard first,
            # whose refusal names the category (SchedulerNotReadyError subclass).
            with self.assertRaisesRegex(
                scheduler_repo.SchedulerDrainBlocked,
                "legacy_unknown_deliveries=1",
            ):
                async with db.begin():
                    await scheduler_repo.transition_scheduler_state(
                        db,
                        target="enabling",
                        scheduler_epoch=self.scheduler_epoch,
                        expected_version=1,
                        actor="test",
                        reason="must fail closed",
                    )

    async def test_global_caps_serialize_cross_agent_admission_and_preserve_replay(self) -> None:
        first_agent = await self._agent("global-cap-a")
        second_agent = await self._agent("global-cap-b")
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE scheduler_runtime_state SET max_global_queued_turns=1, "
                    "max_global_queued_bytes=4 WHERE id=1"
                )
            )
        first_locked = asyncio.Event()
        release_first = asyncio.Event()
        first_identity: dict[str, int] = {}

        async def admit(
            agente_id: int,
            telefone: str,
            provider_event_id: str,
            *,
            hold: bool,
        ) -> bool:
            async with self.sessions() as db:
                try:
                    async with db.begin():
                        _, scheduler, _, lane = await scheduler_repo.lock_authority_prefix(
                            db,
                            agente_id=agente_id,
                            scheduler_epoch=self.scheduler_epoch,
                            create_lane=True,
                        )
                        if hold:
                            first_locked.set()
                            await release_first.wait()
                        await scheduler_repo.assert_intake_caps(
                            db,
                            scheduler=scheduler,
                            agente_id=agente_id,
                            sessao_id=-1,
                            incoming_bytes=4,
                            additional_turns=1,
                        )
                        session = await sessao_repo.get_or_create_for_update(
                            db, agente_id, telefone
                        )
                        turn = await scheduler_repo.create_turn(
                            db,
                            lane=lane,
                            sessao=session,
                            scheduler_epoch=self.scheduler_epoch,
                            ready_at=datetime.utcnow(),
                            successor=False,
                        )
                        await scheduler_repo.create_revision(
                            db, turn=turn, scheduler_epoch=self.scheduler_epoch
                        )
                        receipt = InboundEvent(
                            provider="evolution",
                            source_scope="ticket02-cap",
                            provider_event_id=provider_event_id,
                            event_type="messages.upsert",
                            payload_hmac=("b" * 64),
                            hmac_key_version=1,
                        )
                        message = Mensagem(
                            sessao_id=session.id, role="user", conteudo="1234"
                        )
                        db.add_all((receipt, message))
                        await db.flush()
                        membership, created = await scheduler_repo.append_input_membership(
                            db,
                            scheduler=scheduler,
                            turn_id=turn.id,
                            sessao_id=session.id,
                            mensagem_id=message.id,
                            inbound_event_id=receipt.id,
                            content_bytes=4,
                        )
                        self.assertTrue(created)
                        if hold:
                            first_identity.update(
                                membership=membership.id,
                                message=message.id,
                                receipt=receipt.id,
                                turn=turn.id,
                                session=session.id,
                            )
                        return True
                except scheduler_repo.SchedulerCapExceeded:
                    return False

        first = asyncio.create_task(
            admit(first_agent, "55110106", "global-cap-first", hold=True)
        )
        await first_locked.wait()
        second = asyncio.create_task(
            admit(second_agent, "55110107", "global-cap-second", hold=False)
        )
        await asyncio.sleep(0.1)
        self.assertFalse(second.done())
        release_first.set()
        self.assertEqual(await asyncio.gather(first, second), [True, False])
        async with self.sessions() as db:
            async with db.begin():
                self.assertIsNone(
                    (
                        await db.execute(
                            select(InboundEvent).where(
                                InboundEvent.provider_event_id == "global-cap-second"
                            )
                        )
                    ).scalar_one_or_none()
                )
                _, scheduler, _, _ = await scheduler_repo.lock_authority_prefix(
                    db,
                    agente_id=first_agent,
                    scheduler_epoch=self.scheduler_epoch,
                )
                replay, created = await scheduler_repo.append_input_membership(
                    db,
                    scheduler=scheduler,
                    turn_id=first_identity["turn"],
                    sessao_id=first_identity["session"],
                    mensagem_id=first_identity["message"],
                    inbound_event_id=first_identity["receipt"],
                    content_bytes=4,
                )
                self.assertFalse(created)
                self.assertEqual(replay.id, first_identity["membership"])

    async def test_receipt_savepoint_lock_survives_until_outer_commit(self) -> None:
        keyring = inbound_event_repo.ReceiptHMACKeyRing(
            current_version=1, keys={1: "synthetic-ticket02-receipt-key"}
        )
        inserted = asyncio.Event()
        release = asyncio.Event()

        async def first_insert() -> None:
            async with self.sessions() as db:
                async with db.begin():
                    result = await inbound_event_repo.record(
                        db,
                        provider="evolution",
                        source_scope="ticket02-savepoint",
                        provider_event_id="same-event",
                        event_type="messages.upsert",
                        actionable_fields={"message": "same"},
                        keyring=keyring,
                    )
                    self.assertTrue(result.created)
                    inserted.set()
                    await release.wait()

        async def replay_insert() -> bool:
            await inserted.wait()
            async with self.sessions() as db:
                async with db.begin():
                    result = await inbound_event_repo.record(
                        db,
                        provider="evolution",
                        source_scope="ticket02-savepoint",
                        provider_event_id="same-event",
                        event_type="messages.upsert",
                        actionable_fields={"message": "same"},
                        keyring=keyring,
                    )
                    return result.replay

        first = asyncio.create_task(first_insert())
        second = asyncio.create_task(replay_insert())
        await inserted.wait()
        await asyncio.sleep(0.1)
        self.assertFalse(second.done())
        release.set()
        _, replayed = await asyncio.gather(first, second)
        self.assertTrue(replayed)

    async def test_automated_delivery_exact_authority_and_immutability(self) -> None:
        agent = await self._agent()
        # Automated authority requires the agent's explicit scheduler opt-in.
        async with self.sessions() as db:
            await db.execute(
                text("UPDATE agentes SET inbox_scheduler_enabled=TRUE WHERE id=:id"),
                {"id": agent},
            )
            await db.commit()
        session_id, turn_id, revision_id = await self._turn(
            agente_id=agent, telefone="55110009"
        )
        await self._membership(
            agente_id=agent,
            sessao_id=session_id,
            turn_id=turn_id,
            provider_event_id="delivery-1",
            content="envie",
        )
        async with self.sessions() as db:
            async with db.begin():
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="delivery-worker",
                    lease_seconds=30,
                )
        async with self.sessions() as db:
            async with db.begin():
                revision = await scheduler_repo.seal_revision_range(
                    db,
                    turn_id=turn_id,
                    sessao_id=session_id,
                    revision_id=revision_id,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    owner_token=claim.claim_token,
                    expected_input_version=1,
                )
                completed_at = datetime.utcnow()
                revision.status = "final_candidate"
                revision.draft_text = "resposta"
                revision.draft_text_bytes = 8
                revision.needs_handoff = False
                revision.draft_kind = "generated"
                revision.generation_completed_at = completed_at
                # Ticket 04 guard: a final candidate carries its persisted,
                # internally consistent typing schedule.
                revision.typing_delay_ms = 2000
                revision.typing_jitter_ms = 0
                revision.typing_deadline_at = completed_at + timedelta(milliseconds=2000)
                with self.assertRaises(scheduler_repo.SchedulerConflictError):
                    await scheduler_repo.admit_revision(
                        db,
                        turn_id=turn_id,
                        sessao_id=session_id,
                        revision_id=revision_id,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=str(uuid4()),
                        owner_token=claim.claim_token,
                        expected_first_input_sequence=1,
                        expected_last_input_sequence=1,
                    )
                self.assertTrue(
                    await scheduler_repo.admit_revision(
                        db,
                        turn_id=turn_id,
                        sessao_id=session_id,
                        revision_id=revision_id,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        owner_token=claim.claim_token,
                        expected_first_input_sequence=1,
                        expected_last_input_sequence=1,
                    )
                )
                session = await sessao_repo.lock_by_id(db, session_id, refresh_existing=True)
                sequence = (await sessao_repo.allocate_delivery_sequences(db, session))[0]
                delivery, _ = await outbox_repo.enqueue_automated(
                    db,
                    scheduler_epoch=self.scheduler_epoch,
                    lane_owner_token=claim.claim_token,
                    conversation_turn_id=turn_id,
                    turn_revision_id=revision_id,
                    automated_effect="reply",
                    cutover_epoch=self.cutover_epoch,
                    dedupe_key=f"automated:{uuid4()}",
                    payload_version=1,
                    payload={"synthetic": True},
                    ordering_key=session.delivery_stream_key,
                    stream_sequence=sequence,
                    next_attempt_at=datetime.utcnow(),
                    expires_at=datetime.utcnow() + timedelta(hours=1),
                    agente_id=agent,
                    sessao_id=session_id,
                )
                delivery.status = "processing"
                delivery.lease_token = str(uuid4())
                delivery.lock_owner = "outbox-worker"
                delivery.lock_until = datetime.utcnow() + timedelta(seconds=30)
                delivery.attempt_count = 1
                delivery_id = delivery.id
                delivery_token = delivery.lease_token
        authority_hint = scheduler_repo.AutomatedDeliveryHint(
            delivery_class="automated",
            automated_effect="reply",
            agente_id=agent,
            sessao_id=session_id,
            conversation_turn_id=turn_id,
            turn_revision_id=revision_id,
            handoff_attempt_id=None,
            scheduler_epoch=self.scheduler_epoch,
        )
        other_session_id, other_turn_id, other_revision_id = await self._turn(
            agente_id=agent, telefone="55110108"
        )
        async with self.sessions() as db:
            async with db.begin():
                other_handoff = HandoffAttempt(
                    handoff_key=str(uuid4()),
                    sessao_id=other_session_id,
                    agente_id=agent,
                    generation=1,
                    status="pending",
                    chatwoot_inbox_id=1,
                )
                db.add(other_handoff)
                await db.flush()
                other_handoff_id = other_handoff.id
        async with self.sessions() as db:
            with self.assertRaises(outbox_repo.OutboxConflictError):
                async with db.begin():
                    session = await sessao_repo.lock_by_id(
                        db, session_id, refresh_existing=True
                    )
                    await outbox_repo.enqueue_automated(
                        db,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=claim.claim_token,
                        conversation_turn_id=turn_id,
                        turn_revision_id=other_revision_id,
                        automated_effect="reply",
                        cutover_epoch=self.cutover_epoch,
                        dedupe_key=f"poisoned:{uuid4()}",
                        payload_version=1,
                        payload={"synthetic": True},
                        ordering_key=session.delivery_stream_key,
                        stream_sequence=session.next_delivery_sequence,
                        next_attempt_at=datetime.utcnow(),
                        expires_at=datetime.utcnow() + timedelta(hours=1),
                        agente_id=agent,
                        sessao_id=session_id,
                    )
        async with self.sessions() as db:
            with self.assertRaises(outbox_repo.OutboxConflictError):
                async with db.begin():
                    session = await sessao_repo.lock_by_id(
                        db, session_id, refresh_existing=True
                    )
                    await outbox_repo.enqueue_automated(
                        db,
                        scheduler_epoch=self.scheduler_epoch,
                        lane_owner_token=str(uuid4()),
                        conversation_turn_id=turn_id,
                        turn_revision_id=revision_id,
                        automated_effect="reply",
                        cutover_epoch=self.cutover_epoch,
                        dedupe_key=f"poisoned-token:{uuid4()}",
                        payload_version=1,
                        payload={"synthetic": True},
                        ordering_key=session.delivery_stream_key,
                        stream_sequence=session.next_delivery_sequence,
                        next_attempt_at=datetime.utcnow(),
                        expires_at=datetime.utcnow() + timedelta(hours=1),
                        agente_id=agent,
                        sessao_id=session_id,
                    )
        # Valid effect identity: only the composite FK may reject these rows
        # (errno 1452 naming the exact constraint), never the authority CHECK.
        async with self.sessions() as db:
            # 1452 names the FK; a DML-only user gets 1216 (no parent
            # visibility). Which FK rejects it is proven by the matrix FK drop.
            with self.assertRaisesRegex(
                IntegrityError, r"\((1216|1452), .Cannot add or update a child row"
            ):
                async with db.begin():
                    await db.execute(
                        text(
                            "INSERT INTO outbox_deliveries ("
                            "delivery_key,cutover_epoch,dedupe_key,provider,operation,"
                            "payload_version,payload,delivery_class,automated_effect,"
                            "scheduler_epoch,"
                            "conversation_turn_id,turn_revision_id,agente_id,sessao_id,"
                            "ordering_key,stream_sequence,status,next_attempt_at,expires_at"
                            ") VALUES (UUID(),:cutover,UUID(),'evolution',"
                            "'evolution.send_text.v1',1,JSON_OBJECT('synthetic',TRUE),"
                            "'automated','reply',:scheduler,:turn,:wrong_revision,:agent,:session,"
                            "UUID(),1,'pending',UTC_TIMESTAMP(6),"
                            "UTC_TIMESTAMP(6)+INTERVAL 1 HOUR)"
                        ),
                        {
                            "cutover": self.cutover_epoch,
                            "scheduler": self.scheduler_epoch,
                            "turn": turn_id,
                            "wrong_revision": other_revision_id,
                            "agent": agent,
                            "session": session_id,
                        },
                    )
        async with self.sessions() as db:
            with self.assertRaisesRegex(
                IntegrityError, r"\((1216|1452), .Cannot add or update a child row"
            ):
                async with db.begin():
                    await db.execute(
                        text(
                            "INSERT INTO outbox_deliveries ("
                            "delivery_key,cutover_epoch,dedupe_key,provider,operation,"
                            "payload_version,payload,delivery_class,automated_effect,"
                            "scheduler_epoch,"
                            "conversation_turn_id,turn_revision_id,agente_id,sessao_id,"
                            "handoff_attempt_id,ordering_key,stream_sequence,status,"
                            "next_attempt_at,expires_at) VALUES (UUID(),:cutover,UUID(),"
                            "'evolution','evolution.send_text.v1',1,"
                            "JSON_OBJECT('synthetic',TRUE),'automated','handoff_transition',:scheduler,:turn,"
                            ":revision,:agent,:session,:wrong_handoff,UUID(),1,'pending',"
                            "UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)+INTERVAL 1 HOUR)"
                        ),
                        {
                            "cutover": self.cutover_epoch,
                            "scheduler": self.scheduler_epoch,
                            "turn": turn_id,
                            "revision": revision_id,
                            "agent": agent,
                            "session": session_id,
                            "wrong_handoff": other_handoff_id,
                        },
                    )
        self.assertNotEqual(other_turn_id, turn_id)
        async with self.sessions() as db:
            async with db.begin():
                authority = await scheduler_repo.lock_automated_delivery_authority(
                    db,
                    delivery_id=delivery_id,
                    delivery_lease_token=delivery_token,
                    cutover_epoch=self.cutover_epoch,
                    hint=authority_hint,
                )
                self.assertIsNotNone(authority)
        async with self.sessions() as db:
            async with db.begin():
                self.assertIsNone(
                    await scheduler_repo.lock_automated_delivery_authority(
                        db,
                        delivery_id=delivery_id,
                        delivery_lease_token=str(uuid4()),
                        cutover_epoch=self.cutover_epoch,
                        hint=authority_hint,
                    )
                )
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'outbox delivery authority is immutable'):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE outbox_deliveries SET delivery_class='human' WHERE id=:id"
                        ),
                        {"id": delivery_id},
                    )
        async with self.sessions() as db:
            with self.assertRaisesRegex(DBAPIError, 'outbox delivery authority is immutable'):
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE outbox_deliveries "
                            "SET automated_effect='handoff_transition' WHERE id=:id"
                        ),
                        {"id": delivery_id},
                    )
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE outbox_deliveries SET lock_until="
                    "UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=:id"
                ),
                {"id": delivery_id},
            )
        async with self.sessions() as db:
            async with db.begin():
                self.assertIsNone(
                    await scheduler_repo.lock_automated_delivery_authority(
                        db,
                        delivery_id=delivery_id,
                        delivery_lease_token=delivery_token,
                        cutover_epoch=self.cutover_epoch,
                        hint=authority_hint,
                    )
                )

    async def test_bounded_whole_transaction_retry(self) -> None:
        attempts = 0

        async def operation(db: AsyncSession) -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise DBAPIError.instance(
                    "synthetic statement",
                    {},
                    Exception(1213, "synthetic deadlock"),
                    Exception,
                )
            await db.execute(text("SELECT 1"))
            return "ok"

        result = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            operation,
            max_attempts=3,
            max_jitter_seconds=0,
        )
        self.assertEqual((result, attempts), ("ok", 3))

    async def test_real_mysql_deadlock_retries_entire_transaction(self) -> None:
        first_agent = await self._agent("deadlock-a")
        second_agent = await self._agent("deadlock-b")
        second_locked = asyncio.Event()
        first_locked = asyncio.Event()
        blocker_deadlocked = False

        async def blocker() -> None:
            nonlocal blocker_deadlocked
            try:
                async with self.sessions() as db:
                    async with db.begin():
                        await db.execute(
                            text(
                                "UPDATE agentes SET nome=CONCAT(nome, '-held') WHERE id=:id"
                            ),
                            {"id": second_agent},
                        )
                        second_locked.set()
                        await first_locked.wait()
                        await db.execute(
                            select(Agente).where(Agente.id == first_agent).with_for_update()
                        )
            except DBAPIError as exc:
                if scheduler_repo._mysql_error_code(exc) != 1213:
                    raise
                blocker_deadlocked = True

        attempts = 0

        async def operation(db: AsyncSession) -> int:
            nonlocal attempts
            attempts += 1
            await db.execute(
                select(Agente).where(Agente.id == first_agent).with_for_update()
            )
            if attempts == 1:
                first_locked.set()
                await asyncio.sleep(0.05)
            await db.execute(
                select(Agente).where(Agente.id == second_agent).with_for_update()
            )
            return attempts

        blocker_task = asyncio.create_task(blocker())
        await second_locked.wait()
        result = await scheduler_repo.run_transaction_with_deadlock_retry(
            self.sessions,
            operation,
            max_attempts=3,
            max_jitter_seconds=0,
        )
        await blocker_task
        self.assertFalse(blocker_deadlocked)
        self.assertEqual((result, attempts), (2, 2))

    async def test_scheduler_epoch_rotation_fences_preloaded_owner(self) -> None:
        agent = await self._agent("epoch")
        _, turn_id, _ = await self._turn(agente_id=agent, telefone="55110010")
        async with self.sessions() as db:
            async with db.begin():
                claim = await scheduler_repo.claim_ready_turn(
                    db,
                    agente_id=agent,
                    scheduler_epoch=self.scheduler_epoch,
                    delivery_runtime_epoch=self.cutover_epoch,
                    worker_id="epoch-worker",
                    lease_seconds=30,
                )
        stale_session = self.sessions()
        try:
            preloaded = await stale_session.get(AgentServiceLane, agent)
            preloaded_scheduler = await stale_session.get(SchedulerRuntimeState, 1)
            self.assertEqual(preloaded.owner_token, claim.claim_token)
            self.assertEqual(preloaded_scheduler.scheduler_epoch, self.scheduler_epoch)
            await stale_session.commit()
            rotated_epoch = str(uuid4())
            async with self.sessions() as db:
                async with db.begin():
                    await db.execute(
                        text(
                            "UPDATE scheduler_runtime_state SET scheduler_epoch=:epoch, "
                            "version=version+1 WHERE id=1"
                        ),
                        {"epoch": rotated_epoch},
                    )
            async with stale_session.begin():
                self.assertFalse(
                    await scheduler_repo.renew_lane_lease(
                        stale_session,
                        turn_id=turn_id,
                        agente_id=agent,
                        scheduler_epoch=self.scheduler_epoch,
                        delivery_runtime_epoch=self.cutover_epoch,
                        owner_token=claim.claim_token,
                        lease_seconds=30,
                    )
                )
                self.assertEqual(preloaded_scheduler.scheduler_epoch, rotated_epoch)
        finally:
            await stale_session.close()
