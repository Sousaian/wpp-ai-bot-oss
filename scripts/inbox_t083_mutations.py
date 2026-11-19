#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the T08-3 replica matrix.

Each case removes exactly one protection from a copy of ``app/`` (one or more
exact, single-occurrence replacements) and names the test that must FAIL
against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T = "tests.test_replica_compat_mysql.ReplicaCompatMySQLTests."
REPO = "app/src/database/repositories/replica_repo.py"
COMPAT = "app/src/services/replica_compat.py"
SCHED = "app/src/database/repositories/scheduler_repo.py"
CLI = "app/src/delivery_cli.py"
MAIN = "app/src/main.py"
TURN = "app/src/turn_worker.py"
WORKER = "app/src/delivery_worker.py"

MATRIX = T + "test_mixed_version_epoch_flag_matrix"
GATE = T + "test_enable_gate_reason_matrix"
ATTEST = T + "test_enable_gate_attestation_kinds_and_audit"
LEGACY = T + "test_stale_stopped_rows_and_unmatched_legacy_heartbeats"
CURRENCY = T + "test_db_authority_currency_rules"


def _shared(field: str, reason: str):
    return (MATRIX, [(REPO, f'    ("{field}", "{reason}"),\n', "")])


def _gate(reason: str, condition: str, indent: str = "        "):
    return (GATE, [(REPO, f'{indent}if {condition}:\n{indent}    reasons.add("{reason}")',
                    f'{indent}if False:\n{indent}    reasons.add("{reason}")')])


CASES = {
    # ── readiness: every shared field ───────────────────────────────────
    "code_version_unchecked": _shared("code_version", "code_version_mismatch"),
    "schema_field_unchecked": (T + "test_mixed_version_epoch_flag_matrix", [
        (REPO, '    ("schema_version", "schema_version_mismatch"),\n', ""),
        (REPO, "    if any(\n        row.fingerprint.schema_version != snapshot.db_schema_version\n",
               "    if False and any(\n        row.fingerprint.schema_version != snapshot.db_schema_version\n"),
    ]),
    "delivery_mode_unchecked": _shared("delivery_mode", "delivery_mode_mismatch"),
    "delivery_epoch_unchecked": _shared("delivery_epoch", "delivery_epoch_mismatch"),
    "scheduler_flag_unchecked": _shared("scheduler_enabled", "scheduler_flag_mismatch"),
    "scheduler_epoch_unchecked": _shared("scheduler_epoch", "scheduler_epoch_mismatch"),
    "webhook_auth_unchecked": _shared("webhook_auth_mode", "webhook_auth_mismatch"),
    "dispatch_flag_unchecked": _shared("dispatch_enabled", "dispatch_flag_mismatch"),
    "reader_not_member": (MATRIX, [(REPO, "    if reader is not None:\n        members.append(reader)",
                                     "    if False:\n        members.append(reader)")]),
    # ── readiness: rows, schema, legacy, currency ───────────────────────
    "schema_unsupported_ignored": (T + "test_schema_version_supported_mismatch_and_never_lowered", [
        (REPO, "    if reader is not None and reader.schema_version != snapshot.db_schema_version:",
               "    if False:")]),
    "schema_row_mismatch_ignored": (T + "test_schema_version_supported_mismatch_and_never_lowered", [
        (REPO, "    if any(\n        row.fingerprint.schema_version != snapshot.db_schema_version\n",
               "    if False and any(\n        row.fingerprint.schema_version != snapshot.db_schema_version\n")]),
    "invalid_rows_ignored": (T + "test_invalid_stored_row_is_incompatible", [
        (REPO, "    if snapshot.invalid_rows:", "    if False:")]),
    "row_decode_lenient": (T + "test_invalid_stored_row_is_incompatible", [
        (REPO, "        validate_fingerprint(fingerprint)\n    except (ReplicaHeartbeatError, TypeError, ValueError):",
               "        pass\n    except (ReplicaHeartbeatError, TypeError, ValueError):")]),
    "turn_processor_flag_ignored": (T + "test_turn_processor_flag_only_binds_turn_workers", [
        (REPO, '        row.replica_kind == "turn_worker" and not row.fingerprint.turn_processor_enabled',
               '        False and not row.fingerprint.turn_processor_enabled')]),
    "legacy_unmatched_ignored": (LEGACY, [(REPO, "    if snapshot.legacy_unmatched:", "    if False:")]),
    "legacy_stale_replica_accounts": (LEGACY, [
        (REPO, '"AND (r.stopped_at IS NOT NULL OR r.last_seen_at >= "\n'
               '                    "TIMESTAMPADD(SECOND, -:fresh, :now)))"',
               '"AND (r.stopped_at IS NOT NULL OR r.last_seen_at IS NOT NULL "\n'
               '                    "OR :fresh = :now))"')]),
    "legacy_any_kind_accounts": (LEGACY, [
        (REPO, "                    \"AND r.replica_kind = 'delivery_worker' \"\n", "")]),
    "stopped_rows_counted": (LEGACY, [
        (REPO, '"WHERE stopped_at IS NULL AND last_seen_at >= "\n                "TIMESTAMPADD(SECOND, -:fresh, :now) ORDER BY replica_id"',
               '"WHERE last_seen_at >= "\n                "TIMESTAMPADD(SECOND, -:fresh, :now) ORDER BY replica_id"')]),
    "stale_rows_counted": (LEGACY, [
        (REPO, '"WHERE stopped_at IS NULL AND last_seen_at >= "\n                "TIMESTAMPADD(SECOND, -:fresh, :now) ORDER BY replica_id"',
               '"WHERE stopped_at IS NULL AND last_seen_at >= "\n                "TIMESTAMPADD(SECOND, -:fresh * 100000, :now) ORDER BY replica_id"')]),
    "delivery_currency_ignored": (CURRENCY, [
        (REPO, '    if snapshot.runtime_mode == "outbox" and any(', '    if False and any(')]),
    "scheduler_epoch_currency_ignored": (CURRENCY, [
        (REPO, '    if snapshot.scheduler_state != "disabled" and any(', '    if False and any(')]),
    "scheduler_flag_currency_ignored": (CURRENCY, [
        (REPO, '    if snapshot.scheduler_state in {"enabled", "draining"} and any(', '    if False and any(')]),
    "report_error_reads_compatible": (T + "test_report_unavailable_fails_closed", [
        (COMPAT, '        return FleetReport(False, ("replica_state_unavailable",), scheduler_state=scheduler_state)\n    reasons =',
                 '        return FleetReport(True, ())\n    reasons =')]),
    "report_session_error_reads_compatible": (T + "test_report_unavailable_fails_closed", [
        (COMPAT, '        return FleetReport(False, ("replica_state_unavailable",))\n\n\nasync def schema_supported',
                 '        return FleetReport(True, ())\n\n\nasync def schema_supported')]),
    "schema_supported_fail_open": (T + "test_report_unavailable_fails_closed", [
        (COMPAT, '            "Replica schema version unavailable | erro_tipo={}",\n            type(exc).__name__,\n        )\n        return False',
                 '            "Replica schema version unavailable | erro_tipo={}",\n            type(exc).__name__,\n        )\n        return True')]),
    "disabled_replica_health_blocks": (T + "test_replica_health_only_blocks_active_db_scheduler", [
        (COMPAT, "return not self.compatible and self.scheduler_state in BLOCKING_STATES",
                 "return not self.compatible")]),
    "active_replica_health_ignored": (T + "test_replica_health_only_blocks_active_db_scheduler", [
        (COMPAT, "return not self.compatible and self.scheduler_state in BLOCKING_STATES",
                 "return False")]),
    "draining_replica_health_ignored": (T + "test_replica_health_only_blocks_active_db_scheduler", [
        (COMPAT, 'BLOCKING_STATES = frozenset({"enabling", "enabled", "draining"})',
                 'BLOCKING_STATES = frozenset({"enabling", "enabled"})')]),
    "replica_health_state_not_propagated": (T + "test_replica_health_only_blocks_active_db_scheduler", [
        (COMPAT, "        scheduler_state=snapshot.scheduler_state,", "")]),
    "unavailable_replica_health_state_lost": (T + "test_unavailable_fleet_preserves_db_state_for_health", [
        (COMPAT, 'return FleetReport(False, ("replica_state_unavailable",), scheduler_state=scheduler_state)',
                 'return FleetReport(False, ("replica_state_unavailable",))')]),
    "unknown_replica_health_blocks": (T + "test_unavailable_fleet_preserves_db_state_for_health", [
        (COMPAT, "return not self.compatible and self.scheduler_state in BLOCKING_STATES",
                 'return not self.compatible and self.scheduler_state != "disabled"')]),
    # ── heartbeat row ───────────────────────────────────────────────────
    "stop_marker_never_written": (T + "test_heartbeat_roundtrip_stop_marker_and_revival", [
        (REPO, '    stopped_sql = "UTC_TIMESTAMP(6)" if stopped else "NULL"', '    stopped_sql = "NULL"')]),
    "revival_keeps_stop_marker": (T + "test_heartbeat_roundtrip_stop_marker_and_revival", [
        (REPO, '"last_seen_at=new.last_seen_at, stopped_at=new.stopped_at"',
               '"last_seen_at=new.last_seen_at, stopped_at=COALESCE(stopped_at, new.stopped_at)"')]),
    "fingerprint_epoch_unvalidated": (T + "test_missing_configuration_value_never_defaults_compatible", [
        (REPO, '    _check_uuid(fingerprint.delivery_epoch, "delivery_epoch")\n', "")]),
    "failure_not_counted": (T + "test_report_unavailable_fails_closed", [
        (COMPAT, "    _counts[name] += 1\n", "")]),
    # ── enable gate ─────────────────────────────────────────────────────
    "gate_not_applied": (ATTEST, [(SCHED, '    if target == "enabled":\n        fleet = await replica_repo.load_fleet(db)',
                                   '    if False:\n        fleet = await replica_repo.load_fleet(db)')]),
    "gate_refusal_not_counted": (ATTEST, [(SCHED, "            _replica_gate_refusal_count += 1\n", "")]),
    "gate_reads_before_scheduler_lock": (T + "test_gate_reads_committed_heartbeats_after_waiting_for_scheduler_lock", [
        (SCHED, "    await begin_scheduler_control(db)\n    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)\n    state = await get_scheduler_state(db, for_update=True)\n    if state.version != expected_version:",
                "    await begin_scheduler_control(db)\n    early_fleet = await replica_repo.load_fleet(db)\n    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)\n    state = await get_scheduler_state(db, for_update=True)\n    if state.version != expected_version:"),
        (SCHED, "        fleet = await replica_repo.load_fleet(db)\n        refused = replica_repo.enable_gate_reasons(",
                "        fleet = early_fleet\n        refused = replica_repo.enable_gate_reasons("),
    ]),
    "attestation_flag_ignored": (ATTEST, [
        (REPO, "    if attestation is None or attestation.old_replicas_removed is not True:",
               "    if attestation is None:")]),
    "kind_presence_ignored": (ATTEST, [
        (REPO, '            reasons.add(f"replica_kind_missing:{kind}")', '            pass')]),
    "gate_runtime_outbox_ignored": _gate("runtime_not_outbox", 'snapshot.runtime_mode != "outbox"', "    "),
    "gate_delivery_mode_ignored": _gate("delivery_mode_not_outbox", 'member.delivery_mode != "outbox"'),
    "gate_delivery_epoch_ignored": _gate("delivery_epoch_not_runtime", "member.delivery_epoch != snapshot.runtime_epoch"),
    "gate_scheduler_flag_ignored": _gate("scheduler_flag_disabled", "not member.scheduler_enabled"),
    "gate_target_epoch_ignored": _gate("scheduler_epoch_not_target", "member.scheduler_epoch != target_epoch"),
    "gate_auth_ignored": _gate("webhook_auth_not_enforced", 'member.webhook_auth_mode != "enforced"'),
    "gate_dispatch_ignored": _gate("dispatch_disabled", "not member.dispatch_enabled"),
    # Not listed: excluding the operator from the gate's per-member loop is an
    # equivalent mutation (the operator is already a readiness reader, so any
    # divergence of it refuses through a *_mismatch reason).
    # ── CLI and health ──────────────────────────────────────────────────
    "cli_attestation_forced": (T + "test_cli_enabled_requires_flag_reports_reasons_and_audits", [
        (CLI, "old_replicas_removed=bool(args.attest_old_replicas_removed),", "old_replicas_removed=True,")]),
    "cli_refusal_exit_zero": (T + "test_cli_enabled_requires_flag_reports_reasons_and_audits", [
        (CLI, '"replica_reasons": list(exc.reasons)})\n        return 1', '"replica_reasons": list(exc.reasons)})\n        return 0')]),
    "app_health_ignores_replicas": (T + "test_app_health_fails_closed_on_divergent_replica", [
        (MAIN, "        and replicas.healthy\n", "")]),
    "cli_health_ignores_replicas": (T + "test_delivery_cli_health_fails_closed_on_divergent_replica", [
        (CLI, "    healthy = healthy and replicas.healthy\n", "")]),
    "turn_health_ignores_replicas": (T + "test_turn_worker_health_cli", [
        (TURN, "and heartbeat_fresh and replicas.healthy)", "and heartbeat_fresh)")]),
    "turn_health_ignores_heartbeat": (T + "test_turn_worker_health_cli", [
        (TURN, "snapshot_fresh and heartbeat_fresh and replicas", "snapshot_fresh and replicas")]),
    "turn_health_ignores_snapshot": (T + "test_turn_worker_health_cli", [
        (TURN, "bool(snapshot_fresh and heartbeat_fresh", "bool(heartbeat_fresh")]),
    # ── process hooks ───────────────────────────────────────────────────
    "turn_schema_refusal_removed": (T + "test_turn_worker_refuses_unsupported_schema_without_processor", [
        (TURN, "    if not await replica_compat.schema_supported(AsyncSessionLocal):", "    if False:")]),
    "turn_heartbeat_not_started": (T + "test_turn_worker_beats_while_running_and_marks_clean_exit", [
        (TURN, "    heartbeat.start()\n", "")]),
    "turn_stop_marker_skipped": (T + "test_turn_worker_beats_while_running_and_marks_clean_exit", [
        (TURN, "        await heartbeat.stop()\n", "")]),
    "app_heartbeat_not_started": (T + "test_app_lifespan_beats_and_marks_clean_exit", [
        (MAIN, "    replica_heartbeat.start()\n", "")]),
    "worker_loop_beat_removed": (T + "test_delivery_worker_run_loop_beats_then_marks_clean_exit", [
        (WORKER, "                if now - self._last_replica_beat >= replica_compat.HEARTBEAT_SECONDS:",
                 "                if False:")]),
    "worker_stop_marker_skipped": (T + "test_delivery_worker_replica_beat_and_stop_marker_match_legacy_row", [
        (WORKER, "        if self._replica_drained:  # T08-3", "        if False:  # T08-3")]),
    "worker_retention_hook_removed": (T + "test_delivery_worker_retention_purges_replica_heartbeats", [
        (WORKER, '        await self._retention_stage("replicas", replica_compat.purge_expired(AsyncSessionLocal))\n', "")]),
    "retention_age_not_rechecked": (T + "test_retention_purges_only_expired_rows", [
        (REPO, '"DELETE FROM replica_heartbeats WHERE replica_id = :id AND "\n'
               '                "last_seen_at < TIMESTAMPADD(DAY, -:days, UTC_TIMESTAMP(6))"',
               '"DELETE FROM replica_heartbeats WHERE replica_id = :id AND "\n'
               '                ":days IS NOT NULL"')]),
    "retention_unbounded": (T + "test_retention_purges_only_expired_rows", [
        (REPO, '"ORDER BY last_seen_at, replica_id LIMIT :limit"',
               '"ORDER BY last_seen_at, replica_id LIMIT 1000000 OFFSET 0 -- :limit"')]),
}


def _edits(name: str):
    if name not in CASES:
        raise SystemExit(f"unknown mutation: {name}")
    return CASES[name][1]


def apply(root: Path, name: str) -> None:
    contents: dict[str, str] = {}
    for relative, old, new in _edits(name):
        content = contents.get(relative, (root / relative).read_text())
        count = content.count(old)
        if count != 1:
            raise SystemExit(f"{name}: expected one mutation target in {relative}, found {count}")
        contents[relative] = content.replace(old, new, 1)
    for relative, content in contents.items():
        (root / relative).write_text(content)


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "list":
        for name, (test, _) in CASES.items():
            print(f"{name} {test}")
        return 0
    if len(argv) == 2 and argv[1] == "count":
        print(len(CASES))
        return 0
    if len(argv) == 2 and argv[1] == "check":
        root = Path(__file__).resolve().parent.parent
        for name in CASES:
            contents: dict[str, str] = {}
            for relative, old, new in _edits(name):
                content = contents.get(relative, (root / relative).read_text())
                if content.count(old) != 1:
                    raise SystemExit(f"{name}: expected one mutation target in {relative}")
                contents[relative] = content.replace(old, new, 1)
            for relative, content in contents.items():
                compile(content, relative, "exec")
        print(f"targets and syntax ok: {len(CASES)}")
        return 0
    if len(argv) == 4 and argv[1] == "apply":
        apply(Path(argv[2]), argv[3])
        return 0
    raise SystemExit("usage: inbox_t083_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
