"""FU-2: skewed turn history must not widen session locking to other tenants."""
import unittest

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession
from unittest.mock import patch
from src.database.models import ConversationTurn, OutboxDelivery
from src.database.repositories import lifecycle_repo, scheduler_repo, scheduler_retention_repo
from tests import test_lifecycle_mysql as t07


class LockFootprintMySQLTests(t07.LifecycleMySQLTests):
    async def test_session_locking_paths_under_skewed_history(self) -> None:
        agent, instance, phone, _ = await self._ordinary_admitted("fu2-footprint")
        active = await self._one_turn()
        await self._accept(instance, phone, "fu2-successor", "successor")
        for n in range(3):
            foreign, name = await self._agent(f"fu2-foreign-{n}")
            await self._accept(name, f"5511{foreign:08d}", f"fu2-f-{n}", "other tenant")
        # Long session (5000 terminal turns), only five globally open turns.
        async with self.sessions() as db:
            await db.execute(text("SET SESSION cte_max_recursion_depth=6000"))
            await db.execute(text("""
                INSERT INTO conversation_turns
                  (turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,
                   scheduling_sequence,status,session_slot,ready_at,terminal_at,terminal_reason)
                WITH RECURSIVE n AS (SELECT 1 AS i UNION ALL SELECT i+1 FROM n WHERE i<5000)
                SELECT UUID(),:a,:s,:e,10000+i,10000+i,'delivered','terminal',
                       UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),'fu2-skew' FROM n
            """), {"a": agent, "s": active.sessao_id, "e": self.scheduler_epoch})
            await db.commit()
            await db.execute(text("ANALYZE TABLE conversation_turns"))
            await db.commit()
        async with self.sessions() as db:
            own = set((await db.execute(select(ConversationTurn.id).where(
                ConversationTurn.sessao_id == active.sessao_id))).scalars())
        import inspect
        history_kwargs = {"index": "idx_turn_session_status"} if "index" in inspect.signature(lifecycle_repo._has_scheduler_history).parameters else {}
        paths = (
            ("intake", lambda db: scheduler_repo.lock_intake_turn_graph(db, sessao_id=active.sessao_id)),
            ("lifecycle", lambda db: lifecycle_repo.lock_scheduled_graph(db, session_ids=[active.sessao_id])),
            ("successor", lambda db: scheduler_repo.lock_pending_successor(db, sessao_id=active.sessao_id)),
            ("promotion", lambda db: scheduler_repo._promote_successor_after_terminal_active(db, sessao_id=active.sessao_id)),
            ("session_history", lambda db: lifecycle_repo._has_scheduler_history(db,
                ConversationTurn.sessao_id == active.sessao_id, **history_kwargs)),
            ("retention", lambda db: scheduler_retention_repo._retain_one(db,
                turn_id=active.id, agente_id=agent, sessao_id=active.sessao_id, payload_days=7, receipt_days=90)),
            ("orphan", lambda db: scheduler_repo.complete_turn_and_release_lane(db,
                turn_id=active.id, sessao_id=active.sessao_id, revision_id=active.final_revision_id,
                agente_id=agent, scheduler_epoch=self.scheduler_epoch,
                delivery_runtime_epoch=self.delivery_epoch, owner_token=active.claim_token,
                terminal_status="delivered", terminal_reason="fu2-test", promote_successor=False)),
        )
        seen = 0
        for name, operation in paths:
            with self.subTest(path=name):
                async with self.sessions() as db:
                    if name in {"session_history", "retention"}:
                        # Both history callers open their own RC transaction.
                        await db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                    cid = (await db.execute(text("SELECT CONNECTION_ID()"))).scalar_one()
                    # Production callers own the session before the turn graph.
                    if name not in {"orphan", "retention"}:
                        await db.execute(text("SELECT id FROM sessoes WHERE id=:s FOR UPDATE"), {"s": active.sessao_id})
                    if name == "promotion":
                        await db.execute(text("UPDATE conversation_turns SET status='delivered', "
                            "session_slot='terminal',terminal_at=UTC_TIMESTAMP(6),terminal_reason='fu2' WHERE id=:t"), {"t": active.id})
                    if name == "orphan":
                        await db.execute(update(OutboxDelivery).values(status="cancelled",
                            terminal_at=text("UTC_TIMESTAMP(6)"), terminal_reason="fu2"))
                        # End the fixture transaction: canonical runtime locks must
                        # not be acquired while holding a delivery lock.
                        await db.commit()
                        cid = (await db.execute(text("SELECT CONNECTION_ID()"))).scalar_one()
                    original = db.execute
                    snapshots = []
                    async def execute(statement, *args, **kwargs):
                        result = await original(statement, *args, **kwargs)
                        sql = str(statement.compile(dialect=self.engine.dialect))
                        if "FROM conversation_turns" in sql and "FOR UPDATE" in sql:
                            async with self.sessions() as observer:
                                locks = (await observer.execute(text("""
                                    SELECT INDEX_NAME,LOCK_MODE,LOCK_DATA
                                    FROM performance_schema.data_locks l
                                    JOIN performance_schema.threads t ON t.THREAD_ID=l.THREAD_ID
                                    WHERE t.PROCESSLIST_ID=:cid AND OBJECT_SCHEMA=DATABASE()
                                      AND OBJECT_NAME='conversation_turns' AND LOCK_TYPE='RECORD'
                                """), {"cid": cid})).all()
                            snapshots.append((sql, [tuple(r) for r in locks]))
                        return result
                    with patch.object(db, "execute", execute):
                        try:
                            await operation(db)
                        except scheduler_repo.SchedulerConflictError as exc:
                            self.assertEqual(name, "orphan")
                            self.assertIn("orphan a successor", str(exc))
                    self.assertTrue(snapshots, name)
                    for sql, locks in snapshots:
                        self.assertTrue(locks, name)
                        foreign_primary = [r for r in locks if r[0] == "PRIMARY" and r[2] != "supremum pseudo-record" and int(r[2]) not in own]
                        self.assertEqual(foreign_primary, [], (name, len(locks), locks[:12]))
                        # Even gap-boundary records must not name a different
                        # session; terminal history provides an in-session bound.
                        foreign_session_index = [r for r in locks if r[0] == "idx_turn_session_status"
                            and r[2] != "supremum pseudo-record" and int(r[2].split(",")[0]) != active.sessao_id]
                        self.assertEqual(foreign_session_index, [], (name, len(locks), locks[:12]))
                        foreign_retention = [r for r in locks if r[0] == "idx_turn_retention_terminal"
                            and (r[2] == "supremum pseudo-record" or int(r[2].rsplit(",", 1)[-1]) not in own)]
                        self.assertEqual(foreign_retention, [], (name, len(locks), locks[:12]))
                        where = sql.split("WHERE", 1)[1].lstrip()
                        if where.startswith("conversation_turns.sessao_id"):
                            self.assertIn("FORCE INDEX (idx_turn_session_status)", sql)
                    print("FU2_FOOTPRINT", name, "own_session", active.sessao_id,
                          "snapshots", len(snapshots), "records", [len(r[1]) for r in snapshots], "foreign=0")
                    await db.rollback()
                    seen += 1
        self.assertEqual(seen, 7)

    async def test_settlement_absent_operator_intent_takes_no_gap_lock(self) -> None:
        await self._ordinary_admitted("fu2-audit-gap")
        claimed = (await self._claim())[0]
        original = AsyncSession.execute
        captured = []
        async def execute(db, statement, *args, **kwargs):
            result = await original(db, statement, *args, **kwargs)
            sql = str(statement)
            if "FROM delivery_operator_actions" in sql:
                cid = (await original(db, text("SELECT CONNECTION_ID()"))).scalar_one()
                async with self.sessions() as observer:
                    locks = (await original(observer, text(
                        "SELECT INDEX_NAME,LOCK_MODE,LOCK_DATA FROM performance_schema.data_locks l "
                        "JOIN performance_schema.threads t ON t.THREAD_ID=l.THREAD_ID "
                        "WHERE t.PROCESSLIST_ID=:cid AND OBJECT_SCHEMA=DATABASE() "
                        "AND OBJECT_NAME='delivery_operator_actions' AND LOCK_TYPE='RECORD'"), {"cid": cid})).all()
                captured.append([tuple(r) for r in locks])
            return result
        with patch.object(AsyncSession, "execute", execute):
            self.assertTrue(await self._complete(claimed))
        self.assertEqual(captured, [[]])



for _name in dir(t07.LifecycleMySQLTests):
    if _name.startswith("test_") and _name not in LockFootprintMySQLTests.__dict__:
        setattr(LockFootprintMySQLTests, _name, None)

if __name__ == "__main__":
    unittest.main()
