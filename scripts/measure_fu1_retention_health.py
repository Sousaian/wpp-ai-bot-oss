#!/usr/bin/env python3
"""FU-1 disposable-root measurement; /workspace read-only, runtime tmpfs.

Creates 1M terminal graphs with scrubbed revisions plus one retained draft.
Uses only the existing schema. Database marker guard is mandatory. Never run
on a real DB; tables are populated with a distinct synthetic agent/session.
"""
import asyncio
import json
import os
from pathlib import Path
import statistics
import time
import unittest
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.database.repositories import scheduler_retention_repo as retention
from tests.disposable_db import assert_disposable_database


async def main() -> None:
    engine = create_async_engine(os.environ['OUTBOX_TEST_DATABASE_URL'])
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await assert_disposable_database(unittest.TestCase(), engine)
    history = 1_000_000
    epoch = str(uuid4())
    try:
        async with sessions() as db:
            await db.execute(text("INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao) "
                                  "VALUES ('fu1 synthetic',:instance,'synthetic','0')"), {'instance': str(uuid4())})
            agent = (await db.execute(text('SELECT LAST_INSERT_ID()'))).scalar_one()
            await db.execute(text("INSERT INTO sessoes (agente_id,telefone,delivery_stream_key) VALUES (:a,'fu1-synthetic',UUID())"), {'a': agent})
            session = (await db.execute(text('SELECT LAST_INSERT_ID()'))).scalar_one()
            await db.commit()
            started = time.monotonic()
            for chunk in range(10):
                await db.execute(text("INSERT INTO conversation_turns "
                    "(turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,"
                    "status,session_slot,ready_at,terminal_at,terminal_reason,created_at) "
                    "WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n<999) "
                    "SELECT UUID(),:a,:s,:e,:offset+x.n*100+y.n+1,:offset+x.n*100+y.n+1,"
                    "'delivered','terminal',UTC_TIMESTAMP(6)-INTERVAL 100 DAY,"
                    "UTC_TIMESTAMP(6)-INTERVAL 100 DAY,'fu1-history',UTC_TIMESTAMP(6)-INTERVAL 100 DAY "
                    "FROM seq x JOIN seq y ON y.n<100"),
                    {'a': agent, 's': session, 'e': epoch, 'offset': chunk * 100000})
                await db.commit()
                print('FU1_POPULATE', (chunk+1)*100000, flush=True)
            await db.execute(text("INSERT INTO turn_revisions (revision_key,turn_id,scheduler_epoch,revision_number,status,"
                "draft_text,created_at) SELECT UUID(),id,scheduler_epoch,1,'admitted',NULL,created_at "
                "FROM conversation_turns WHERE agente_id=:a"), {'a': agent})
            await db.commit()
            await db.execute(text("INSERT INTO conversation_turns "
                "(turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,"
                "status,session_slot,ready_at,terminal_at,terminal_reason,created_at) "
                "VALUES (UUID(),:a,:s,:e,1000001,1000001,'delivered','terminal',UTC_TIMESTAMP(6)-INTERVAL 14 DAY,"
                "UTC_TIMESTAMP(6)-INTERVAL 14 DAY,'fu1-draft',UTC_TIMESTAMP(6)-INTERVAL 14 DAY)"),
                {'a': agent, 's': session, 'e': epoch})
            last = (await db.execute(text('SELECT LAST_INSERT_ID()'))).scalar_one()
            await db.execute(text("INSERT INTO turn_revisions (revision_key,turn_id,scheduler_epoch,revision_number,status,draft_text) "
                                  "VALUES (UUID(),:id,:e,1,'admitted','synthetic retained draft')"), {'id': last, 'e': epoch})
            await db.commit()
            await db.execute(text('ANALYZE TABLE conversation_turns, turn_revisions'))
            print('FU1_VOLUME', {'scrubbed_turns': history, 'scrubbed_revisions': history,
                                 'retained_drafts': 1, 'populate_seconds': time.monotonic()-started}, flush=True)
            for size in (1, 100, 1000):
                statement = retention.health_query(draft=True, turn_ids=tuple(range(last-size+1, last+1)))
                sql = str(statement.compile(dialect=mysql.dialect(), compile_kwargs={'literal_binds': True}))
                plan = [dict(row) for row in (await db.execute(text('EXPLAIN '+sql))).mappings()]
                assert not any(row['type'] == 'ALL' for row in plan), plan
                assert all(row['key'] in ('PRIMARY', 'uq_turn_revision_number') for row in plan), plan
                analysis = (await db.execute(text('EXPLAIN ANALYZE '+sql))).scalar_one()
                timings = []
                for _ in range(10):
                    before = time.monotonic()
                    value = (await db.execute(text(sql))).scalar_one()
                    assert value is not None
                    timings.append((time.monotonic()-before)*1000)
                print('FU1_BOUNDED_EXPLAIN', size, json.dumps(plan, default=str), flush=True)
                print('FU1_BOUNDED_ANALYZE', size, analysis, flush=True)
                print('FU1_BOUNDED_MS', size, {'min': min(timings), 'median': statistics.median(timings),
                                             'max': max(timings)}, flush=True)
            old = ("SELECT /*+ MAX_EXECUTION_TIME(2000) */ t.terminal_at "
                   "FROM conversation_turns t FORCE INDEX (idx_turn_retention_terminal) "
                   "WHERE t.session_slot='terminal' AND (SELECT r.id FROM turn_revisions r "
                   "WHERE r.turn_id=t.id AND r.draft_text IS NOT NULL LIMIT 1) IS NOT NULL "
                   "ORDER BY t.terminal_at LIMIT 1")
            before = time.monotonic()
            try:
                await db.execute(text(old))
                outcome = 'returned'
            except DBAPIError as exc:
                outcome = {'db_error_code': exc.orig.args[0]}
            print('FU1_BASELINE_GLOBAL', {'seconds': time.monotonic()-before, 'outcome': outcome}, flush=True)
    finally:
        await engine.dispose()


if __name__ == '__main__':
    asyncio.run(main())
