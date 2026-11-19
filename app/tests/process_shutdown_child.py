"""Subprocess-only harness: production main()/Uvicorn + real DB, synthetic LLM.

Only cancellation resistance is injected, around real heartbeat I/O. It repeats
shielding to exercise asyncio.run's second cancellation at teardown. No mocked
DB, no provider network, no replacement of accounting/recovery implementations.
"""
import asyncio
import os
from uuid import UUID

from sqlalchemy import event

from src.database.connection import engine
from src.services import replica_compat as rc


scenario = os.environ["P38_SCENARIO"]
replica_id = UUID(os.environ["P38_REPLICA_ID"])
rc.uuid4 = lambda: replica_id
seen = set()
marked = False


@event.listens_for(engine.sync_engine, "before_cursor_execute")
def sql_barrier(conn, cursor, statement, parameters, context, executemany):
    if marked and not statement.lstrip().upper().startswith("SELECT"):
        print("PROOF_LATE_SQL", flush=True)
    for table in ("replica_heartbeats", "consumo_llm", "scheduler_runtime_state"):
        if table in statement:
            cid = conn.connection.driver_connection.thread_id()
            if (table, cid) not in seen:
                seen.add((table, cid))
                print("PROOF_SQL", table, cid, flush=True)


real_beat = rc.beat
ready = False
async def observed_beat(*args, **kwargs):
    global ready, marked
    result = await real_beat(*args, **kwargs)
    if kwargs.get("stopped") and result:
        marked = True
        print("PROOF_MARKER", flush=True)
    if not kwargs.get("stopped") and result and not ready:
        ready = True
        print("PROOF_READY", flush=True)
    return result
rc.beat = observed_beat

if scenario in {"worker_beat", "app_beat", "worker_socket"}:
    async def resistant_beat(self):
        inner = None
        while True:
            if inner is None or inner.done():
                inner = asyncio.create_task(rc.beat(self.sessions, kind=self.kind, replica_id=self.replica_id))
            try:
                await asyncio.shield(inner)
                await asyncio.sleep(.05)
                inner = None
            except asyncio.CancelledError:
                # Deliberately resistant REAL SQL wait. The inner SQL task may
                # itself be cancelled by asyncio.run; start another shield so
                # its unbounded gather cannot accidentally make this test pass.
                pass
    rc.HeartbeatLoop._run = resistant_beat

if scenario.startswith("worker") or scenario == "startup":
    from src import turn_worker
    turn_worker.uuid4 = lambda: replica_id
    if scenario == "worker_accounting":
        from src.services.turn_processor import TurnProcessor
        from tests.test_turn_processor_mysql import FakeLLM, RecordingConsumer, _no_presence
        fake = FakeLLM()
        async def paid(**kwargs):
            result = await fake(**kwargs)
            print("PROOF_PAID", flush=True)
            from pathlib import Path
            while not Path(os.environ["P38_RELEASE_FILE"]).exists():
                await asyncio.sleep(.01)
            return result
        def processor(sessions, **kwargs):
            kwargs["consumer"] = RecordingConsumer(sessions, complete=False)
            return TurnProcessor(sessions, **kwargs, llm_call=paid, presence_call=_no_presence,
                                 poll_interval_seconds=.05)
        turn_worker.TurnProcessor = processor
    from src.utils.logger import setup_logger
    setup_logger()
    raise SystemExit(turn_worker.main())
else:
    from src.main import app
    from src import app_server
    if scenario in {"app_long", "app_busy"}:
        @app.get("/proof/long")
        async def long_request():
            print("PROOF_HTTP", flush=True)
            if scenario == "app_busy":
                import time
                time.sleep(60)  # Occupied event loop; independent timer must fire.
            else:
                await asyncio.sleep(60)
            return {"ok": True}
    if scenario == "app_direct":
        from src.services import llm_client, evolution_orchestrator
        from tests.test_turn_processor_mysql import FakeLLM
        from unittest.mock import AsyncMock
        fake = FakeLLM()
        async def paid(**kwargs):
            result = await fake(**kwargs)
            print("PROOF_PAID", flush=True)
            from pathlib import Path
            while not Path(os.environ["P38_RELEASE_FILE"]).exists():
                await asyncio.sleep(.01)
            return result
        llm_client.completar = paid
        from src.agents import agent_runner
        real_record_usage = agent_runner._registrar_consumo_seguro
        async def before_commit(**kwargs):
            # Deterministic cancellation boundary AFTER the real runner has
            # received a paid result and flushed assistant, BEFORE usage/UoW
            # commit. No accounting implementation is replaced or resolved.
            print("PROOF_DIRECT_COMMIT_BARRIER", flush=True)
            await asyncio.Event().wait()
            return await real_record_usage(**kwargs)
        agent_runner._registrar_consumo_seguro = before_commit
        evolution_orchestrator.send_text_message = AsyncMock(return_value=True)
    app_server.main(app, port=int(os.environ["P38_PORT"]))
