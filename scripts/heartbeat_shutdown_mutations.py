#!/usr/bin/env python3
"""Executable F0-T5 mutants; exact edits of production code, never test edits."""
from pathlib import Path
import shutil
import sys

RC = "app/src/services/replica_compat.py"
WORKER = "app/src/turn_worker.py"
UTIL = "app/src/utils/shutdown.py"
MUTATIONS = {
    "gather_without_cancel": (RC, '''                task.cancel()
                if not await wait_until(task, deadline):
                    self._stop_timeout()
                    return  # Never race a marker against unfinished DB cleanup.''',
        '''                await asyncio.gather(task, return_exceptions=True)'''),
    "no_total_deadline": (UTIL,
        '''{task}, timeout=max(0.0, deadline - asyncio.get_running_loop().time())''',
        '''{task}, timeout=None'''),
    "marker_fixed_five_seconds": (RC,
        '''            if not await wait_until(task, deadline):
                self._stop_timeout()''',
        '''            if not await wait_until(task, asyncio.get_running_loop().time() + 5):
                self._stop_timeout()'''),
    "cancel_shared_shutdown": (RC,
        '''            # The first caller does not own shared process shutdown.
            raise''',
        '''            self._shutdown_task.cancel()
            raise'''),
    "swallow_caller_cancellation": (RC,
        '''            # The first caller does not own shared process shutdown.
            raise''',
        '''            # MUTANT: caller cancellation swallowed.
            return'''),
    "no_cancelled_connection_invalidation": (RC,
        '''                    await db.invalidate()''',
        '''                    pass  # MUTANT: reuse interrupted connection'''),
    "dispose_outside_finally": (WORKER,
        '''            await _cleanup_stage(engine.dispose(), deadline)
        logger.info''',
        '''            pass
        await _cleanup_stage(engine.dispose(), deadline)
        logger.info'''),
    "no_signal_watchdog": (WORKER,
        '''        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)''',
        '''        await asyncio.gather(run_task, return_exceptions=True)'''),
    "unbounded_worker_cleanup": (WORKER,
        '''    finished = await wait_until(task, deadline)''',
        '''    await asyncio.gather(task, return_exceptions=True)
    finished = True'''),
    "force_exit_removed": ("app/src/utils/process_shutdown.py",
        "        self._exit(EXIT_FORCED)", "        return  # MUTANT: no process exit"),
    "marker_with_pending_writer": (RC,
        "            if before_marker is not None and not await before_marker():",
        "            if False:  # MUTANT: skip writer quiescence"),
    "no_startup_watch": (WORKER,
        "        await asyncio.wait({schema_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)",
        "        await schema_task"),
    "no_uvicorn_http_timeout": ("app/src/app_server.py",
        "        timeout_graceful_shutdown=APP_HTTP_DRAIN_SECONDS,",
        "        timeout_graceful_shutdown=None,"),
    "short_process_budget": ("app/src/utils/process_shutdown.py",
        "        self.seconds = grace_seconds - PROCESS_MARGIN_SECONDS",
        "        self.seconds = min(5.0, grace_seconds - PROCESS_MARGIN_SECONDS)"),
    "no_direct_cancellation_signal": ("app/src/app_server.py",
        '                signal("direct_shutdown_cancellations")',
        '                pass  # MUTANT: silent direct usage loss'),
    "event_loop_only_signal": ("app/src/app_server.py",
        "                self.previous_handlers[signum] = process_signal.signal(signum, self.handle_exit)",
        "                asyncio.get_running_loop().add_signal_handler(signum, self.handle_exit, signum, None)"),

    "stale_cache_docs": ("AGENTS.md",
        "vencido. Shutdown do app cancela concorrentemente ambas as cargas e o beat;",
        "vencido. Shutdown do app cancela e aguarda ambas as cargas antes de parar\n  heartbeat;"),

    "fixed_entrypoint_host": ("app/src/app_server.py",
        "        host=settings.app_host if host is None else host,",
        '        host="0.0.0.0",'),
    "marker_failure_as_unsafe": ("app/src/utils/process_shutdown.py",
        '    signal("marker_failed")', '    guard.mark_unsafe()'),
    "silent_debug_reload": ("app/src/app_server.py",
        "    if settings.debug:", "    if False:  # MUTANT: silently ignore reload"),

}


PROCESS_CLASS = "tests.test_process_shutdown_mysql.ProcessShutdownMySQLTests."
PROCESS_UNIT = "tests.test_process_shutdown_unit.ProcessShutdownUnitTests."
MUTATION_TESTS = {
    "force_exit_removed": (PROCESS_CLASS + "test_worker_resistant_real_beat_exits_process",),
    "marker_with_pending_writer": (PROCESS_UNIT + "test_marker_withheld_when_gate_finds_pending_writer",
        PROCESS_CLASS + "test_accounting_real_mdl_abandoned_then_existing_recovery"),
    "no_startup_watch": (PROCESS_CLASS + "test_startup_schema_real_mdl_signal_is_watched",),
    "no_uvicorn_http_timeout": (PROCESS_CLASS + "test_app_long_http_drain_is_bounded_and_clean_marker_present",),
    "short_process_budget": (PROCESS_UNIT + "test_budget_uses_real_compose_grace_and_http_drain",
        PROCESS_CLASS + "test_worker_resistant_real_beat_exits_process"),
    "no_direct_cancellation_signal": (PROCESS_CLASS + "test_direct_paid_result_cancelled_by_http_drain_rolls_back_with_signal",),
    "event_loop_only_signal": (PROCESS_CLASS + "test_app_occupied_event_loop_still_exits_from_thread",),
    "stale_cache_docs": (PROCESS_UNIT + "test_entrypoint_docs_http_and_concurrent_cache_contract_parity",
        PROCESS_CLASS + "test_clean_worker_process_marks_stopped"),

    "fixed_entrypoint_host": (PROCESS_UNIT + "test_entrypoints_preserve_configured_host_port_and_docker_defaults",
        PROCESS_CLASS + "test_clean_worker_process_marks_stopped"),
    "marker_failure_as_unsafe": (PROCESS_CLASS + "test_worker_db_down_without_writer_is_marker_failed_not_unsafe",
        PROCESS_CLASS + "test_app_db_down_without_writer_is_marker_failed_not_unsafe"),
    "silent_debug_reload": (PROCESS_UNIT + "test_debug_reload_is_refused_with_named_error_before_server_start",
        PROCESS_CLASS + "test_clean_worker_process_marks_stopped"),

}


def apply(name: str, source: Path, destination: Path) -> None:
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns(".git", "__pycache__", "*.pyc"))
    file, old, new = MUTATIONS[name]
    path = destination / file
    body = path.read_text()
    assert body.count(old) == 1, (name, body.count(old))
    path.write_text(body.replace(old, new, 1))


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "list":
        print("\n".join(MUTATIONS))
    elif command == "matrix":
        for name in MUTATIONS:
            tests = MUTATION_TESTS.get(name, ("tests.test_heartbeat_shutdown_unit", "tests.test_heartbeat_shutdown_mysql", "tests.test_heartbeat_turn_shutdown_mysql"))
            count = len(tests) if name in MUTATION_TESTS else 22
            print(name + "|" + str(count) + "|" + " ".join(tests))
    elif command == "count":
        print(len(MUTATIONS))
    elif command == "apply":
        apply(sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]))
    else:
        raise SystemExit("expected list, count or apply NAME SOURCE DESTINATION")
