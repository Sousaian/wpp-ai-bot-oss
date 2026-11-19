#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the T08-5 observability matrix.

Each case removes exactly one protection (metric predicate, aggregate or 503
code mapping) from a copy of ``app/`` and names the test that must fail
against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T = "tests.test_scheduler_metrics_mysql.SchedulerMetricsMySQLTests."
METRICS = "app/src/database/repositories/scheduler_metrics_repo.py"
OBS = "app/src/services/scheduler_observability.py"
MAIN = "app/src/main.py"
SCHEDULER = "app/src/database/repositories/scheduler_repo.py"
CLI = "app/src/delivery_cli.py"
CACHE = "app/src/utils/async_ttl_cache.py"
REPLICA = "app/src/services/replica_compat.py"
LOGGER = "app/src/utils/logger.py"


def _drop(path: str, line: str, test: str) -> tuple[str, str, str, str]:
    return (T + test, path, line, "")


CAP_LINES = {
    "session_inputs": '    "session pending input count cap exceeded": "session_inputs",\n',
    "session_bytes": '    "session pending input byte cap exceeded": "session_bytes",\n',
    "agent_turns": '    "agent queued turn cap exceeded": "agent_turns",\n',
    "agent_bytes": '    "agent queued byte cap exceeded": "agent_bytes",\n',
    "global_turns": '    "global queued turn cap exceeded": "global_turns",\n',
    "global_bytes": '    "global queued byte cap exceeded": "global_bytes",\n',
}

CASES = {
    # ── metrics ─────────────────────────────────────────────────────────
    "state_value_wrong": (
        T + "test_state_version_and_epoch_match", METRICS,
        "        state=str(state.state),\n", '        state="enabled",\n',
    ),
    "version_value_wrong": (
        T + "test_state_version_and_epoch_match", METRICS,
        '        version=_int(state.version, "version"),\n', "        version=1,\n",
    ),
    "epoch_match_ignores_value": (
        T + "test_state_version_and_epoch_match", OBS,
        '"epoch_matches": bool(configured_epoch and configured_epoch == metrics.scheduler_epoch),',
        '"epoch_matches": bool(configured_epoch),',
    ),
    "public_health_exposes_epoch": (
        T + "test_health_and_cli_payloads_have_no_pii", MAIN,
        "            configured_epoch=settings.inbox_scheduler_epoch,\n",
        "            configured_epoch=settings.inbox_scheduler_epoch, include_epoch=True,\n",
    ),
    "nonterminal_filter_removed": (
        T + "test_nonterminal_turn_count_and_bytes", METRICS,
        'f"WHERE t.status IN {_NONTERMINAL} LIMIT :bound) b"',
        'f"WHERE t.status IS NOT NULL LIMIT :bound) b"',
    ),
    "bytes_not_summed": (
        T + "test_nonterminal_turn_count_and_bytes", METRICS,
        'nonterminal_bytes=_int(turns[1], "nonterminal_bytes"),',
        'nonterminal_bytes=_int(turns[0], "nonterminal_bytes"),',
    ),
    "ready_deadline_ignored": (
        T + "test_ready_turns_and_oldest_age", METRICS,
        '"AND t.scheduler_epoch = :scheduler_epoch AND t.ready_at <= UTC_TIMESTAMP(6) "',
        '"AND t.scheduler_epoch = :scheduler_epoch "',
    ),
    "retry_backoff_ignored": (
        T + "test_ready_turns_and_oldest_age", METRICS,
        '"AND (t.retry_at IS NULL OR t.retry_at <= UTC_TIMESTAMP(6)) LIMIT :bound) r"',
        '"LIMIT :bound) r"',
    ),
    "oldest_uses_newest": (
        T + "test_ready_turns_and_oldest_age", METRICS,
        "MIN(r.ready_at)", "MAX(r.ready_at)",
    ),
    "lease_expiry_ignored": (
        T + "test_lane_lease_expired_and_quarantined", METRICS,
        '"FROM (SELECT l.lease_until <= UTC_TIMESTAMP(6) AS expired, "',
        '"FROM (SELECT l.lease_until IS NOT NULL AS expired, "',
    ),
    "quarantine_predicate_dropped": (
        T + "test_lane_lease_expired_and_quarantined", METRICS,
        "\"AND ((g.accounting_state = 'reconciliation_required' AND g.reconciled_at IS NULL) \"",
        '"AND ((FALSE) "',
    ),
    "drain_predicate_dropped": (
        T + "test_lane_lease_expired_and_quarantined", METRICS,
        "\"OR (g.outcome = 'started' AND g.accounting_drain_until > UTC_TIMESTAMP(6))))\"",
        '"OR (FALSE)))"',
    ),
    "outcome_unknown_uncounted": (
        T + "test_unknown_usage_attempts", METRICS,
        "COALESCE(SUM(x.outcome = 'outcome_unknown'), 0)", "COALESCE(SUM(x.outcome = 'failed'), 0)",
    ),
    "started_uncounted": (
        T + "test_unknown_usage_attempts", METRICS,
        "COALESCE(SUM(x.outcome = 'started'), 0)", "COALESCE(SUM(x.outcome = 'failed'), 0)",
    ),
    "reconciled_counted_as_unknown": (
        T + "test_unknown_usage_attempts", METRICS,
        "\"AND g.accounting_state = 'reconciliation_required' AND g.reconciled_at IS NULL \"",
        "\"AND g.accounting_state = 'reconciliation_required' \"",
    ),
    "quarantined_agents_not_distinct": (
        T + "test_unknown_usage_attempts", METRICS,
        "COUNT(DISTINCT x.agente_id)", "COUNT(x.agente_id)",
    ),
    "reconciliation_backlog_partial": (
        T + "test_usage_reconciliation_backlog_and_dead", METRICS,
        "COALESCE(SUM(x.status IN ('pending','processing')), 0)", "COALESCE(SUM(x.status IN ('pending')), 0)",
    ),
    "reconciliation_dead_uncounted": (
        T + "test_usage_reconciliation_backlog_and_dead", METRICS,
        "COALESCE(SUM(x.status = 'dead'), 0)", "COALESCE(SUM(x.status = 'resolved'), 0)",
    ),
    "attention_ignores_dead": _drop(
        OBS, "        metrics.usage_reconciliation_dead,\n", "test_usage_reconciliation_backlog_and_dead"),
    "saturation_undetected": (
        T + "test_saturation_reported_not_exact", METRICS,
        "if count is not None and int(count) >= bound", "if count is not None and int(count) > bound",
    ),
    "attention_ignores_saturation": _drop(OBS, "        metrics.saturated,\n", "test_saturation_reported_not_exact"),
    "unreadable_becomes_zero": (
        T + "test_metrics_unavailable_fail_closed", METRICS,
        '        raise ValueError(f"metric {name} is unreadable")\n', "        return 0\n",
    ),
    "unavailable_reads_healthy": (
        T + "test_metrics_unavailable_fail_closed", MAIN,
        'healthy = bool(delivery_ready and scheduler["healthy"])', "healthy = bool(delivery_ready)",
    ),
    "cli_unavailable_reads_healthy": (
        T + "test_metrics_unavailable_fail_closed", CLI,
        'heartbeat_ok and backlog_ok and runtime_ok and local_ready and scheduler["healthy"]',
        "heartbeat_ok and backlog_ok and runtime_ok and local_ready",
    ),
    "disabled_state_fails_closed": (
        T + "test_metrics_unavailable_disabled_keeps_legacy_health", OBS,
        "        blocks_health = state in BLOCKING_STATES\n", "        blocks_health = True\n",
    ),
    "active_state_fails_open": (
        T + "test_metrics_unavailable_fail_closed", OBS,
        "        blocks_health = state in BLOCKING_STATES\n", "        blocks_health = False\n",
    ),
    "draining_not_blocking": (
        T + "test_metrics_unavailable_fail_closed", OBS,
        'BLOCKING_STATES = frozenset({"enabling", "enabled", "draining"})',
        'BLOCKING_STATES = frozenset({"enabling", "enabled"})',
    ),
    "failed_state_not_propagated": (
        T + "test_metrics_unavailable_fail_closed", METRICS,
        "            known_state.append(str(state.state))\n", "",
    ),
    "metrics_unavailable_uncounted": (
        T + "test_metrics_unavailable_fail_closed", OBS,
        "        _metrics_unavailable_count += 1\n", "        _metrics_unavailable_count += 0\n",
    ),
    "metrics_query_locks": (
        T + "test_queries_never_lock_and_explain_indexed", METRICS,
        '"WHERE l.owner_token IS NOT NULL LIMIT :bound) x"',
        '"WHERE l.owner_token IS NOT NULL LIMIT :bound FOR SHARE) x"',
    ),
    "transaction_not_read_only": (
        T + "test_queries_never_lock_and_explain_indexed", METRICS,
        '"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"',
        '"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"',
    ),
    "turn_scan_unindexed": (
        T + "test_queries_never_lock_and_explain_indexed", METRICS,
        'f"FROM conversation_turns t FORCE INDEX (idx_turn_claim_lease) "',
        'f"FROM conversation_turns t IGNORE INDEX (idx_turn_claim_lease, idx_turn_ready_claim) "',
    ),
    # ── 503 codes ───────────────────────────────────────────────────────
    "draining_code_lost": (
        T + "test_503_state_codes", OBS,
        'STATE_REFUSAL_CODES = {"draining": "scheduler_draining"}', "STATE_REFUSAL_CODES = {}",
    ),
    "not_enabled_code_lost": (
        T + "test_503_state_codes", OBS,
        'STATE_REFUSAL_DEFAULT = "scheduler_not_enabled"', 'STATE_REFUSAL_DEFAULT = "scheduler_draining"',
    ),
    "state_hook_removed": (
        T + "test_503_state_codes", SCHEDULER,
        "        refusal.scheduler_state = scheduler.state  # T08-5: technical 503 code\n", "",
    ),
    "retry_after_dropped": (
        T + "test_503_state_codes", OBS,
        "        retry_after: str | None = RETRY_AFTER_SECONDS,\n", "        retry_after: str | None = None,\n",
    ),
    "code_missing_from_body": (
        T + "test_503_state_codes", OBS,
        'content={"detail": exc.detail, "code": exc.code},', 'content={"detail": exc.detail},',
    ),
    "handler_not_registered": (
        T + "test_503_bodies_have_no_pii", MAIN,
        "app.add_exception_handler(\n    scheduler_observability.TechnicalUnavailable,",
        "(lambda *args: None)(\n    scheduler_observability.TechnicalUnavailable,",
    ),
    "rejection_uncounted": (
        T + "test_503_state_codes", OBS,
        "    _rejections[code] = _rejections.get(code, 0) + 1\n",
        "    _rejections[code] = _rejections.get(code, 0) + 0\n",
    ),
    "cap_rejection_uncounted": (
        T + "test_503_cap_codes_by_kind", OBS,
        "        _cap_rejections[cap_kind] = _cap_rejections.get(cap_kind, 0) + 1\n",
        "        _cap_rejections[cap_kind] = _cap_rejections.get(cap_kind, 0) + 0\n",
    ),
    "cap_code_lost": (
        T + "test_503_cap_codes_by_kind", OBS,
        'return "cap_exceeded", CAP_KINDS.get(str(item), CAP_KIND_UNKNOWN)',
        'return "conflict", CAP_KINDS.get(str(item), CAP_KIND_UNKNOWN)',
    ),
    **{f"cap_kind_{kind}_lost": _drop(OBS, line, "test_503_cap_codes_by_kind") for kind, line in CAP_LINES.items()},
    "scheduler_epoch_code_lost": _drop(OBS, '    "scheduler epoch changed": "epoch_mismatch",\n', "test_503_epoch_codes"),
    "delivery_epoch_code_lost": _drop(
        OBS, '    "delivery runtime epoch changed": "epoch_mismatch",\n', "test_503_epoch_codes"),
    "lane_epoch_code_lost": _drop(
        OBS, '    "owned lane belongs to another scheduler epoch": "epoch_mismatch",\n', "test_503_epoch_codes"),
    "legacy_epoch_code_lost": _drop(
        OBS, '    "configured delivery epoch does not match database": "epoch_mismatch",\n', "test_503_epoch_codes"),
    "runtime_not_outbox_code_lost": _drop(
        OBS, '    "delivery runtime is not outbox": "runtime_mismatch",\n', "test_503_runtime_codes"),
    "runtime_frozen_code_lost": _drop(
        OBS, '    "delivery runtime is frozen": "runtime_frozen",\n', "test_503_runtime_codes"),
    "runtime_mode_code_lost": _drop(
        OBS, '    "configured delivery mode does not match database": "runtime_mismatch",\n', "test_503_runtime_codes"),
    "feature_disabled_code_lost": _drop(
        OBS, '    "scheduled intake feature gate is disabled": "feature_disabled",\n', "test_503_gate_codes"),
    "auth_not_enforced_code_lost": _drop(
        OBS, '    "scheduled intake requires enforced auth": "auth_not_enforced",\n', "test_503_gate_codes"),
    "auth_unavailable_code_lost": (
        T + "test_503_gate_codes", MAIN,
        'scheduler_observability.record_rejection("auth_unavailable"),',
        'scheduler_observability.record_rejection("internal_error"),',
    ),
    "scheduled_gate_changed_code_lost": _drop(
        OBS, '    "scheduled intake gate changed": "gate_changed",\n', "test_503_gate_codes"),
    "legacy_gate_changed_code_lost": _drop(
        OBS, '    "agent scheduler opt-in changed": "gate_changed",\n', "test_503_gate_codes"),
    "receipt_config_code_lost": (
        T + "test_503_internal_codes", OBS,
        '        return "receipt_config_unavailable", None\n', '        return "internal_error", None\n',
    ),
    "usage_invariant_code_lost": (
        T + "test_503_internal_codes", OBS,
        '        return "usage_invariant", None\n', '        return "internal_error", None\n',
    ),
    "transaction_lost_code_lost": (
        T + "test_503_internal_codes", OBS,
        "    if isinstance(exc, scheduler_repo.SchedulerDeadlockRetryExhausted) or db_errors.transaction_lost(exc):\n",
        "    if False:\n",
    ),
    "conflict_code_lost": (
        T + "test_503_internal_codes", OBS,
        'return SCHEDULER_MESSAGE_CODES.get(str(item), "conflict"), None',
        'return SCHEDULER_MESSAGE_CODES.get(str(item), "internal_error"), None',
    ),
    "not_ready_code_lost": (
        T + "test_503_internal_codes", OBS,
        'return SCHEDULER_MESSAGE_CODES.get(str(item), "scheduler_not_ready"), None',
        'return SCHEDULER_MESSAGE_CODES.get(str(item), "conflict"), None',
    ),
    "internal_error_code_lost": (
        T + "test_503_internal_codes", OBS,
        '    return "internal_error", None\n', '    return "conflict", None\n',
    ),
    # FU-4: exact public allowlist and shared readiness decision.
    "fu4_public_body_leaks_scheduler": (
        T + "test_fu4_public_health_minimal_and_status_parity", MAIN,
        '    return {"status": "healthy" if details["status"] == "healthy" else "unhealthy"}\n',
        '    return {"status": "healthy" if details["status"] == "healthy" else "unhealthy", "scheduler": details["scheduler"]}\n',
    ),
    "fu4_public_http_ignores_readiness": (
        T + "test_fu4_public_health_minimal_and_status_parity", MAIN,
        '    details = await health_check(response, db)\n',
        '    details = await health_check(Response(), db)\n',
    ),
    "fu4_public_status_always_healthy": (
        T + "test_fu4_public_health_minimal_and_status_parity", MAIN,
        '    return {"status": "healthy" if details["status"] == "healthy" else "unhealthy"}\n',
        '    return {"status": "healthy"}\n',
    ),
    # FU-4 follow-up: GET / keeps a one-key liveness body.
    "fu4_root_leaks_version": (
        T + "test_fu4_root_status_only", MAIN,
        '    return {"status": "online"}\n',
        '    return {"status": "online", "version": "0.1.0"}\n',
    ),
    "fu4_root_leaks_service": (
        T + "test_fu4_root_status_only", MAIN,
        '    return {"status": "online"}\n',
        '    return {"status": "online", "service": "MVP Atendimento IA"}\n',
    ),
    # R2-S4: cancellation ownership belongs to the shared task and lifespan.
    "cache_waiter_cancels_shared_load": (
        T + "test_health_cache_cancelled_waiter_preserves_flight", CACHE,
        "        return await asyncio.shield(task)\n",
        "        return await task\n",
    ),
    "cache_waiter_detaches_shared_load": (
        T + "test_health_cache_cancelled_waiter_preserves_flight", CACHE,
        "        return await asyncio.shield(task)\n",
        "        try:\n            return await asyncio.shield(task)\n"
        "        except BaseException:\n            self._inflight = None\n            raise\n",
    ),
    "cache_orphan_does_not_publish": (
        T + "test_health_cache_orphaned_load_publishes_with_completion_ttl", CACHE,
        "            self._value = value\n", "            self._value = _MISSING\n",
    ),
    "cache_unexpected_error_not_cached": (
        T + "test_health_cache_unexpected_exception_cached_until_ttl", CACHE,
        "            self._error = exc\n", "            self._error = None\n",
    ),
    "scheduler_cache_skips_shutdown": (
        T + "test_health_cache_lifespan_shutdown_cancels_loads", MAIN,
        "        await _scheduler_health_cache.aclose()\n", "        pass  # missing scheduler cache shutdown\n",
    ),
    "replica_cache_skips_shutdown": (
        T + "test_health_cache_lifespan_shutdown_cancels_loads", MAIN,
        "        await _replica_health_cache.aclose()\n", "        pass  # missing replica cache shutdown\n",
    ),
    # ── S4: /health single-flight cache, transition logs, safe Loguru ───
    "cache_ignores_ttl": (
        T + "test_public_health_singleflight_collects_once_per_ttl", CACHE,
        "        now = self._clock()\n"
        "        if self._value is not _MISSING and now < self._expires_at:\n",
        "        now = self._clock()\n"
        "        if self._value is not _MISSING:\n",
    ),
    "cache_not_single_flight": (
        T + "test_public_health_singleflight_collects_once_per_ttl", CACHE,
        "            if task is None:\n", "            if True:\n",
    ),
    "metrics_unavailable_logs_per_request": (
        T + "test_public_health_singleflight_collects_once_per_ttl", OBS,
        "        if _metrics_available:\n", "        if True:\n",
    ),
    "replica_incompatible_logs_per_request": (
        T + "test_replica_and_public_health_log_only_on_transition", REPLICA,
        "    if not compatible and _report_compatible:\n", "    if not compatible:\n",
    ),
    "replica_unavailable_logs_per_request": (
        T + "test_replica_and_public_health_log_only_on_transition", REPLICA,
        "    if _report_available:\n", "    if True:\n",
    ),
    "public_health_logs_per_request": (
        T + "test_replica_and_public_health_log_only_on_transition", MAIN,
        "    if not healthy and _health_was_healthy:\n", "    if not healthy:\n",
    ),
    "loguru_diagnose_enabled": (
        T + "test_exception_logs_never_render_locals", LOGGER,
        'SAFE_HANDLER_OPTIONS = {"backtrace": False, "diagnose": False}',
        'SAFE_HANDLER_OPTIONS = {"backtrace": False, "diagnose": True}',
    ),
    "main_skips_safe_logger": _drop(
        MAIN, 'setup_logger("logs/app.log")\n', "test_exception_logs_never_render_locals"),
    "delivery_cli_skips_safe_logger": (
        T + "test_exception_logs_never_render_locals", CLI,
        "    setup_logger()\n    args = _parser().parse_args()\n",
        "    args = _parser().parse_args()\n"),
    "provider_auth_cli_skips_safe_logger": (
        T + "test_exception_logs_never_render_locals", "app/src/provider_auth_cli.py",
        "    setup_logger()\n    parser = argparse.ArgumentParser(\n",
        "    parser = argparse.ArgumentParser(\n"),
    "delivery_worker_skips_safe_logger": (
        T + "test_exception_logs_never_render_locals", "app/src/delivery_worker.py",
        "    setup_logger()\n    asyncio.run(_main())\n", "    asyncio.run(_main())\n"),
    "turn_worker_skips_safe_logger": (
        T + "test_exception_logs_never_render_locals", "app/src/turn_worker.py",
        "    setup_logger()\n    sys.exit(main(sys.argv[1:]))\n",
        "    sys.exit(main(sys.argv[1:]))\n"),
}


def apply(root: Path, name: str) -> None:
    if name not in CASES:
        raise SystemExit(f"unknown mutation: {name}")
    _, relative, old, new = CASES[name]
    path = root / relative
    content = path.read_text()
    count = content.count(old)
    if count != 1:
        raise SystemExit(f"{name}: expected one mutation target, found {count}")
    path.write_text(content.replace(old, new, 1))


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "list":
        for name, (test, *_rest) in CASES.items():
            print(f"{name} {test}")
        return 0
    if len(argv) == 2 and argv[1] == "count":
        print(len(CASES))
        return 0
    if len(argv) == 2 and argv[1] == "check":
        root = Path(__file__).resolve().parent.parent
        for name, (_, relative, old, new) in CASES.items():
            content = (root / relative).read_text()
            count = content.count(old)
            if count != 1:
                raise SystemExit(f"{name}: expected one mutation target, found {count}")
            compile(content.replace(old, new, 1), str(relative), "exec")
        print(f"targets and syntax ok: {len(CASES)}")
        return 0
    if len(argv) == 4 and argv[1] == "apply":
        apply(Path(argv[2]), argv[3])
        return 0
    raise SystemExit("usage: inbox_t085_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
