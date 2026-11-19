#!/usr/bin/env python3
"""R2-S1 disposable-root plan comparison; never run against a real database.

OUTBOX_TEST_DATABASE_URL + WPP_DISPOSABLE_TEST_DATABASE_MARKER are required.
Builds the existing 10k-active claim fixture plus 10k terminal history, then
compares production query access paths with the new index invisible/visible.
The separate T081 test asserts actual locking footprint, not just EXPLAIN.
"""
import asyncio
import json
import os
from pathlib import Path
import unittest

from sqlalchemy import text
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.asyncio import create_async_engine

from src.database.repositories import scheduler_repo, scheduler_metrics_repo, turn_generation_repo
from tests.disposable_db import assert_disposable_database


async def main() -> None:
    engine = create_async_engine(os.environ['OUTBOX_TEST_DATABASE_URL'])
    await assert_disposable_database(unittest.TestCase(), engine)
    root = Path(os.environ.get('WPP_REPO_ROOT', '/workspace'))
    fixture = (root / 'scripts/measure_scheduler_claim_plan.sql').read_text()
    seed, rest = fixture.split('EXPLAIN ANALYZE', 1)
    forced_claim = rest.split('DELETE FROM conversation_turns', 1)[0].strip().rstrip(';')
    try:
        async with engine.begin() as db:
            # Script owns a dedicated disposable schema, no other suite shares it.
            for statement in seed.split(';'):
                if statement.strip():
                    await db.execute(text(statement))
            await db.execute(text("INSERT INTO conversation_turns "
                "(turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,"
                "status,session_slot,ready_at,terminal_at,terminal_reason) "
                "SELECT UUID(),agente_id,sessao_id,scheduler_epoch,arrival_sequence+20000,"
                "scheduling_sequence+20000,'delivered','terminal',ready_at,UTC_TIMESTAMP(6),"
                "'sec2-s1-plans' FROM conversation_turns WHERE agente_id=@plan_agent"))
            agent = (await db.execute(text('SELECT @plan_agent'))).scalar_one()
            await db.execute(text('ANALYZE TABLE conversation_turns'))
            statements = {'measure_scheduler_claim_plan.sql': forced_claim}
            class Result:
                def scalar_one(self): return 0
                def one(self): return (0, 0)
                def one_or_none(self): return None
            class Capture:
                async def execute(self, statement):
                    captured.append(str(statement) if isinstance(statement, str) else
                                    str(statement.compile(dialect=mysql.dialect(),
                                        compile_kwargs={'literal_binds': True})))
                    return Result()
            captured = []
            await turn_generation_repo.ready_turn_hint(Capture(), agente_id=int(agent),
                scheduler_epoch='22222222-2222-4222-8222-222222222222')
            statements['actual_ready_turn_hint'] = captured.pop()
            await scheduler_repo.scheduler_clearance_counts(Capture())
            statements.update({f'T081_clearance_{i}': value for i, value in enumerate(captured)})
            statements.update({f'T085_{name}': value.replace(':bound', '100000').replace(
                ':scheduler_epoch', "'22222222-2222-4222-8222-222222222222'")
                for name, value in scheduler_metrics_repo.QUERIES.items()})
            paths = {}
            for visibility in ('INVISIBLE', 'VISIBLE'):
                await db.execute(text('ALTER TABLE conversation_turns ALTER INDEX '
                                      'idx_turn_retention_terminal '+visibility))
                plans = {}
                for name, statement in statements.items():
                    rows = [dict(r) for r in (await db.execute(text('EXPLAIN '+statement))).mappings()]
                    plans[name] = [(r['select_type'], r['table'], r['type'], r['key']) for r in rows]
                    print('SEC2_S1_PLAN', visibility, name, json.dumps(rows, default=str))
                    assert not any(r['key'] == 'idx_turn_retention_terminal' for r in rows), (name, rows)
                paths[visibility] = plans
            assert paths['INVISIBLE'] == paths['VISIBLE'], paths
            print('SEC2_S1_PLAN_PARITY PASS', len(statements), 'queries active=10000 terminal=10000')
            # Required existing measurement, including actual rows/timing.
            measured = (await db.execute(text('EXPLAIN ANALYZE '+forced_claim))).scalars().all()
            print('SEC2_S1_MEASURE_SCHEDULER', '\n'.join(measured))
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
