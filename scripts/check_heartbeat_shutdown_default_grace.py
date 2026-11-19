#!/usr/bin/env python3
"""Actual default PROCESS-grace checks (not a direct _run coroutine probe).

Requires the same disposable MySQL/environment as the counted matrix. Production
main(), asyncio.run teardown, real SIGTERM, resistant MDL and exit code checked.
"""
import unittest


if __name__ == "__main__":
    loader = unittest.defaultTestLoader
    suite = loader.loadTestsFromNames([
        "tests.test_process_shutdown_mysql.ProcessShutdownMySQLTests.test_worker_resistant_real_beat_exits_process",
        "tests.test_process_shutdown_mysql.ProcessShutdownMySQLTests.test_app_resistant_real_mdl_exits_process_without_marker",
    ])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
