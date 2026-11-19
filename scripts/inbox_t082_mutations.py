#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the T08-2 retention matrix.

Each case removes exactly one safety exclusion from a copy of ``app/`` and
names the test that must fail against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T = "tests.test_scheduler_retention_mysql.SchedulerRetentionMySQLTests."
REPO = "app/src/database/repositories/scheduler_retention_repo.py"
WORKER = "app/src/delivery_worker.py"

_P32_NONTERMINAL_FINALIZER = '''        if result.nonterminal_observed:
            _counts["nonterminal"] = _counts.get("nonterminal", 0) + result.nonterminal_observed
            logger.error("Scheduler retention nonterminal aggregate | observed={} count={} threshold={}",
                         result.nonterminal_observed, _counts["nonterminal"], SIGNAL_THRESHOLD)
'''

CASES = {
    # P3-2: discard the pending aggregate only on the selected losing exit;
    # last_pass observations remain intact, exposing the original contradiction.
    "p32_version_exit_aggregate_lost": (
        T + "test_p32_nonterminal_version_loser_finalizes_once", REPO,
        '                        return result\n                    turn_id, agente_id, sessao_id = row.id, row.agente_id, row.sessao_id',
        '                        result.nonterminal_observed = 0  # mutation: skip losing-version aggregate\n'
        '                        return result\n                    turn_id, agente_id, sessao_id = row.id, row.agente_id, row.sessao_id',
    ),
    "p32_cas_exit_aggregate_lost": (
        T + "test_p32_nonterminal_checkpoint_cas_loser_finalizes_once", REPO,
        '            return result  # R2-S1: no stale SLA from a losing sweep',
        '            result.nonterminal_observed = 0  # mutation: skip losing-CAS aggregate\n'
        '            return result  # R2-S1: no stale SLA from a losing sweep',
    ),
    "p32_aggregate_double_counted": (
        T + "test_p32_nonterminal_version_loser_finalizes_once", REPO,
        _P32_NONTERMINAL_FINALIZER,
        _P32_NONTERMINAL_FINALIZER + _P32_NONTERMINAL_FINALIZER,
    ),
    # P3-1: candidate-local failures are not failed measurement/task completion.
    "p31_candidate_as_measurement": (
        T + "test_p31_persistent_poison_rotates_and_completes_real_worker_loop", REPO,
        '        "checkpoint_failed", "discovery_failed"))  # P3-1: measurement only',
        '        "checkpoint_failed", "discovery_failed", "candidate_error", "foreign_key_reference", "transaction_lost"))',
    ),
    "p31_candidate_signal_suppressed": (
        T + "test_p31_persistent_poison_rotates_and_completes_real_worker_loop", REPO,
        '    if reason in {"discovery_changed", "checkpoint_contended", "budget_exhausted"}:',
        '    if reason in CANDIDATE_REASONS or reason in {"discovery_changed", "checkpoint_contended", "budget_exhausted"}:',
    ),
    "p31_empty_partial_erases_failure": (
        T + "test_p31_candidate_taxonomy_and_nonempty_signal_survives_partial_pass", REPO,
        '        elif summary.clean_sweep and completed:',
        '        else:',
    ),
    "p31_sweep_forgets_candidate": (
        T + "test_p31_candidate_taxonomy_and_nonempty_signal_survives_partial_pass", REPO,
        '                candidate_pass.clean_sweep = not scan.candidate_failed',
        '                candidate_pass.clean_sweep = True',
    ),

    "disposable_db_marker_guard_removed": (
        T + "test_disposable_database_marker_guard", "app/tests/disposable_db.py",
        "    test_case.assertEqual(current_database, marker)",
        "    test_case.assertEqual(current_database, current_database)",
    ),
    "nonterminal_purged": (
        T + "test_nonterminal_old_turn_keeps_everything", REPO,
        '    if turn.session_slot != "terminal":\n        return retain("nonterminal")',
        '    if False:\n        return retain("nonterminal")',
    ),
    "payload_horizon_ignored": (
        T + "test_locked_path_rechecks_ages_directly", REPO,
        "    if not _old(turn.terminal_at, payload_cutoff):\n        return result",
        "    if False:\n        return result",
    ),
    "audit_horizon_ignored": (
        T + "test_locked_path_rechecks_ages_directly", REPO,
        "    if not _old(turn.terminal_at, audit_cutoff):\n        return result",
        "    if False:\n        return result",
    ),
    "unknown_outcome_purged": (
        T + "test_started_and_unknown_and_unreconciled_retained", REPO,
        '    return (attempt.outcome in {"succeeded", "failed"}',
        '    return (attempt.outcome in {"succeeded", "failed", "started", "outcome_unknown"}',
    ),
    "unreconciled_quarantine_purged": (
        T + "test_started_and_unknown_and_unreconciled_retained", REPO,
        '            and not (attempt.accounting_state == "reconciliation_required"\n'
        "                     and attempt.reconciled_at is None)",
        "            and True",
    ),
    "success_without_ledger_purged": (
        T + "test_succeeded_without_exact_ledger_retained", REPO,
        '            and (attempt.outcome != "succeeded" or attempt.consumo_llm_id is not None))',
        "            and True)",
    ),
    "pending_reconciliation_purged": (
        T + "test_pending_reconciliation_keeps_evidence", REPO,
        ' or any(r.status != "resolved" for r in reconciliation):',
        ":",
    ),
    "unsent_effect_scrubbed": (
        T + "test_unsent_effect_keeps_text_and_graph", REPO,
        '    if any(e.status not in {"delivered", "dead", "cancelled"} for e in linked):',
        "    if False:",
    ),
    "lane_reference_purged": (
        T + "test_lane_referencing_terminal_turn_retains_graph", REPO,
        "    if prefix.lane is not None and (prefix.lane.active_turn_id == turn_id",
        "    if False and (prefix.lane.active_turn_id == turn_id",
    ),
    "delivery_reference_ignored": (
        T + "test_handoff_notes_cleanup_then_graph_mapping_survives", REPO,
        '    if linked:\n        return retain("delivery_reference")',
        '    if False:\n        return retain("delivery_reference")',
    ),
    "recent_accounting_purged": (
        T + "test_late_accounting_lock_wait_reads_fresh_state", REPO,
        "    if any(not _old(a.outcome_recorded_at, audit_cutoff)",
        "    if False and any(not _old(a.outcome_recorded_at, audit_cutoff)",
    ),
    "recent_receipt_purged": (
        T + "test_recent_receipt_blocks_purge_not_scrub", REPO,
        "           or not _old(receipt_map[m.inbound_event_id].received_at, audit_cutoff)\n",
        "",
    ),
    "context_reference_ignored": (
        T + "test_successor_context_assignment_waits_for_parent_handoff", REPO,
        '            return retain("context_reference")',
        "            pass",
    ),
    "revision_order_reversed": (
        T + "test_refresh_cycle_deleted_replacement_first", REPO,
        "key=lambda r: r.revision_number, reverse=True)",
        "key=lambda r: r.revision_number)",
    ),
    "turn_backlinks_kept": (
        T + "test_ordinary_full_purge_preserves_ledger_and_messages", REPO,
        "                     .values(current_revision_id=None, final_revision_id=None))",
        "                     .values(terminal_reason=ConversationTurn.terminal_reason))",
    ),
    "receipt_kept": (
        T + "test_ordinary_full_purge_preserves_ledger_and_messages", REPO,
        "        await db.execute(delete(InboundEvent).where(InboundEvent.id == receipt.id))",
        "        pass",
    ),
    "cursor_never_advances": (
        T + "test_limit_plus_one_blocked_rows_rotate_to_victim", REPO,
        "        ).values(last_turn_id=cursor, version=version + 1))",
        "        ).values(last_turn_id=0, version=version + 1))",
    ),
    "young_page_never_wraps": (
        T + "test_young_page_ends_sweep_and_wraps_to_victim", REPO,
        "                    if cursor and not scan.prefix_known and result.scanned == 0 and (not rows or rows[0].created_at >= before):",
        "                    if cursor and result.scanned == 0 and not rows:",
    ),
    "candidate_error_blocks_pass": (
        T + "test_unexpected_restrict_and_poison_candidate_do_not_block_rotation", REPO,
        '                _signal("candidate_error")',
        "                raise",
    ),
    "restrict_violation_raises": (
        T + "test_unexpected_restrict_and_poison_candidate_do_not_block_rotation", REPO,
        '                _signal("foreign_key_reference")',
        "                raise",
    ),
    "scrubbed_turn_relocked": (
        T + "test_scrubbed_terminal_waits_without_locks", REPO,
        "    return bool(row.has_draft) or row.terminal_at < audit_cutoff",
        "    return True",
    ),
    "worker_scheduler_before_legacy": (
        T + "test_handoff_notes_cleanup_then_graph_mapping_survives", WORKER,
        '        result = await self._retention_stage("legacy", self._legacy_retention())\n'
        '        scheduler = await self._retention_stage("scheduler",\n'
        '            scheduler_retention_repo.cleanup_retention(AsyncSessionLocal))',
        '        scheduler = await self._retention_stage("scheduler",\n'
        '            scheduler_retention_repo.cleanup_retention(AsyncSessionLocal))\n'
        '        result = await self._retention_stage("legacy", self._legacy_retention())',
    ),
    "sec_s1_single_page": (
        T + "test_sec_s1_many_scrubbed_pages_reach_victim_in_one_interval", REPO,
        "        if end:\n            break",
        "        if True:\n            break",
    ),
    "sec_s1_volatile_cursor": (
        T + "test_sec_s1_restart_every_pass_and_cas_aba", REPO,
        "            cursor, version = progress.last_turn_id, progress.version",
        "            cursor, version = 0, progress.version",
    ),
    "sec_s1_cas_removed": (
        T + "test_sec_s1_restart_every_pass_and_cas_aba", REPO,
        "            SchedulerRetentionProgress.version == version,",
        "            SchedulerRetentionProgress.version >= version,",
    ),
    "sec_s1_deadline_removed": (
        T + "test_sec_s1_time_budget_rolls_back_and_advances", REPO,
        "            async with asyncio.timeout_at(deadline):",
        "            async with asyncio.timeout_at(deadline + 100):",
    ),
    "sec_s1_repeatable_read": (
        T + "test_sec_s1_cross_tenant", REPO,
        "                        await scheduler_repo.begin_scheduler_control(db)",
        "                        pass  # removed READ COMMITTED",
    ),
    "sec_s1_nonterminal_locked": (
        T + "test_sec_s1_nonterminal_aggregate_without_lifecycle", REPO,
        '                    if row.session_slot != "terminal":',
        '                    if row.session_slot != "terminal":\n                        await lifecycle_repo.run_in_new_session(session_factory, lambda db: lifecycle_repo.lock_lifecycle_prefix(db, agente_id=row.agente_id))',
    ),
    "sec_s1_nonterminal_per_row": (
        T + "test_sec_s1_nonterminal_aggregate_without_lifecycle", REPO,
        "                        result.nonterminal_observed += 1",
        '                        result.nonterminal_observed += 1\n                        _signal("nonterminal")',
    ),
    "sec_s1_traceback_restored": (
        T + "test_sec_s1_sla_health_and_safe_error", REPO,
        '                            logger.error("Scheduler retention candidate failed | erro_tipo={}", type(exc).__name__)',
        '                            logger.opt(exception=exc).error("Scheduler retention candidate failed | erro_tipo={}", type(exc).__name__)',
    ),
    "sec_s1_sla_signal_removed": (
        T + "test_sec_s1_sla_health_and_safe_error", REPO,
        '        _signal(f"{key}_sla_exceeded")',
        '        pass',
    ),
    "sec_s1_checkpoint_failure_crashes": (
        T + "test_sec_s1_checkpoint_failure_degrades_with_signal", REPO,
        '            _signal("checkpoint_failed")\n            break',
        '            raise',
    ),
    "sec_s1_progress_read_crashes": (
        T + "test_sec_s1_checkpoint_failure_degrades_with_signal", REPO,
        '        _signal("checkpoint_unavailable")\n        return result',
        '        raise',
    ),
    "sec_s1_health_hidden": (
        T + "test_sec_s1_sla_health_and_safe_error", WORKER,
        '            "scheduler_retention_health": scheduler_retention_repo.retention_health(),  # S1',
        '            "scheduler_retention_health": {},  # S1 removed',
    ),
    "deadlock_not_retried": (
        T + "test_real_deadlock_production_path_retries_whole_graph", REPO,
        "            item = await lifecycle_repo.run_in_new_session(session_factory, operation)",
        "            item = await lifecycle_repo.run_in_new_session(session_factory, operation,"
        " max_attempts=1)",
    ),
    "sec2_s1_inline_retention": (
        T + "test_sec2_s1_worker_loop_never_waits_for_retention", WORKER,
        '                self._schedule_retention(now)  # R2-S1: never await retention here',
        '                await self._retention()  # mutation: inline stalls the real loop',
    ),
    "sec2_s1_discovery_crashes": (
        T + "test_sec2_s1_discovery_and_legacy_errors_do_not_kill_worker", REPO,
        '            _signal("discovery_failed")\n            return result',
        '            raise',
    ),
    "sec2_s1_legacy_crashes": (
        T + "test_sec2_s1_discovery_and_legacy_errors_do_not_kill_worker", WORKER,
        '            self._retention_errors[stage] = self._retention_errors.get(stage, 0) + 1',
        '            raise  # mutation: retention exception kills the pass\n'
        '            self._retention_errors[stage] = self._retention_errors.get(stage, 0) + 1',
    ),
    "sec2_s1_contender_errors": (
        T + "test_sec2_s1_two_replicas_recheck_checkpoint_without_error_or_sla", REPO,
        '    if reason in {"discovery_changed", "checkpoint_contended", "budget_exhausted"}:',
        '    if False:',
    ),
    "sec2_s1_stale_page_repeated": (
        T + "test_sec2_s1_two_replicas_recheck_checkpoint_without_error_or_sla", REPO,
        '                    if await _progress_version(session_factory) != version:',
        '                    if False:',
    ),
    "sec2_s1_health_full_scan": (
        T + "test_sec2_s1_health_explain_indexed_with_synthetic_volume", REPO,
        '"FORCE INDEX (idx_turn_retention_terminal)"',
        '"IGNORE INDEX (idx_turn_retention_terminal)"',
    ),

    "sec2_s1_freshness_validation_removed": (
        T + "test_sec_s1_validated_budgets_and_contract", "app/src/config.py",
        '        if self.delivery_heartbeat_interval_seconds + self.delivery_poll_interval_seconds >= 60:',
        '        if False:',
    ),

    "sec2_s1_overlapping_passes": (
        T + "test_sec2_s1_worker_loop_never_waits_for_retention", WORKER,
        '        if self._retention_task is not None and not self._retention_task.done():',
        '        if False:',
    ),
    "sec2_s1_shutdown_leaves_retention": (
        T + "test_sec2_s1_worker_loop_never_waits_for_retention", WORKER,
        '                self._retention_task.cancel()',
        '                pass  # mutation: retention survives stop',
    ),

    "fu1_task_overrun_not_signaled": (
        T + "test_fu1_stuck_task_signals_stale_and_recovers_without_blocking_dispatch", WORKER,
        '"running_over_budget": running_seconds is not None and running_seconds >',
        '"running_over_budget": False and running_seconds is not None and running_seconds >',
    ),
    "fu1_completion_overdue_not_signaled": (
        T + "test_fu1_stuck_task_signals_stale_and_recovers_without_blocking_dispatch", WORKER,
        '"completion_overdue": now - last_good >',
        '"completion_overdue": False and now - last_good >',
    ),
    "fu1_stale_hidden": (
        T + "test_fu1_stuck_task_signals_stale_and_recovers_without_blocking_dispatch", WORKER,
        '        if retention_task["stale"]:',
        '        if False:',
    ),
    "fu1_watchdog_error_removed": (
        T + "test_fu1_stuck_task_signals_stale_and_recovers_without_blocking_dispatch", WORKER,
        '                logger.error("Delivery retention watchdog | reason={} count={} threshold=1", reason,',
        '                logger.debug("Delivery retention watchdog | reason={} count={} threshold=1", reason,',
    ),
    "fu1_sweep_watchdog_removed": (
        T + "test_fu1_partial_coverage_continuity_restart_and_sweep_watchdog", REPO,
        '    if overdue and reason not in _sla_episodes:',
        '    if False:',
    ),
    "fu1_partial_coverage_claimed_complete": (
        T + "test_fu1_partial_coverage_continuity_restart_and_sweep_watchdog", REPO,
        '        complete = draft_scan is not None and draft_scan.complete',
        '        complete = draft_scan is not None',
    ),
    "fu1_cursor_continuity_ignored": (
        T + "test_fu1_partial_coverage_continuity_restart_and_sweep_watchdog", REPO,
        '    if _coverage is not None and (_coverage.cursor, _coverage.version) == (cursor, version):',
        '    if _coverage is not None:',
    ),
    "fu1_draft_sla_deferred_until_pass_end": (
        T + "test_fu1_partial_scan_signals_positive_sla_once_per_episode", REPO,
        '        if not result.concurrent_progress and scan.oldest is not None:',
        '        if False:',
    ),
    "fu1_probe_failure_hidden": (
        T + "test_fu1_probe_failure_is_unavailable_not_empty", REPO,
        '        if before != measurement_failure_count():',
        '        if False:',
    ),
    "fu1_global_draft_probe_restored": (
        T + "test_fu1_observation_never_issues_global_draft_query", REPO,
        '                oldest_purge = (await db.execute(health_query(draft=False).where(',
        '                await db.execute(select(TurnRevision.id).where(TurnRevision.draft_text.is_not(None)))\n'
        '                oldest_purge = (await db.execute(health_query(draft=False).where(',
    ),
    "fu1_watchdog_config_relation_removed": (
        T + "test_fu1_watchdog_config_bounds_and_parity", "app/src/config.py",
        '        if (self.delivery_retention_interval_seconds * self.delivery_retention_completion_max_intervals',
        '        if (False and self.delivery_retention_interval_seconds * self.delivery_retention_completion_max_intervals',
    ),

    "fu1_first_pass_waits_for_host_uptime": (
        T + "test_fu1_failed_task_state_and_next_completion", WORKER,
        '        if (self._retention_started_at is not None',
        '        if (True',
    ),

    "fu1_contended_coverage_keeps_old_global_minimum": (
        T + "test_sec2_s1_two_replicas_recheck_checkpoint_without_error_or_sla", REPO,
        '                        _invalidate_draft_coverage(scan)',
        '                        pass  # mutation: old complete measurement kept current',
    ),

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
        return 0
    if len(argv) == 4 and argv[1] == "apply":
        apply(Path(argv[2]), argv[3])
        return 0
    raise SystemExit("usage: inbox_t082_mutations.py list|count|check|apply <root> <name>")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
