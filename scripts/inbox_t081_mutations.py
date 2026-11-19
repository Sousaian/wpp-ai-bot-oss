#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the T08-1 state/drain matrix.

Each case removes exactly one protection from a copy of ``app/`` and names the
test that must fail against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T = "tests.test_scheduler_drain_mysql.SchedulerDrainMySQLTests."
SCHEDULER = "app/src/database/repositories/scheduler_repo.py"
ORCHESTRATOR = "app/src/services/evolution_orchestrator.py"
CLI = "app/src/delivery_cli.py"
OUTBOX = "app/src/database/repositories/outbox_repo.py"


def _zero_category(category: str, test: str) -> tuple[str, str, str, str]:
    return (
        T + test, SCHEDULER,
        f'    counts["{category}"] = int(',
        f'    counts["{category}"] = 0 * int(',
    )


CASES = {
    # S2: restoring either diagnostic's locking path must block real intake.
    "scheduler_show_locking_path": (
        T + "test_scheduler_show_snapshot_does_not_block_intake", CLI,
        "            return await scheduler_diagnostics_repo.scheduler_snapshot(db)",
        "            await scheduler_repo.begin_scheduler_control(db)\n"
        "            await delivery_state_repo.get_runtime_state(db, for_share=True)\n"
        "            row = await scheduler_repo.get_scheduler_state(db, for_share=True)",
    ),
    "agent_show_locking_path": (
        T + "test_agent_show_snapshot_does_not_block_intake",
        "app/src/database/repositories/agent_scheduler_repo.py",
        "    await begin_snapshot(db)\n"
        "    runtime = await delivery_state_repo.get_runtime_state(db)\n"
        "    scheduler = await scheduler_repo.get_scheduler_state(db)",
        "    await scheduler_repo.begin_scheduler_control(db)\n"
        "    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)\n"
        "    scheduler = await scheduler_repo.get_scheduler_state(db, for_share=True)",
    ),
    "diagnostic_readonly_removed": (
        T + "test_diagnostic_readonly_and_saturation",
        "app/src/database/repositories/scheduler_diagnostics_repo.py",
        'text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")',
        'text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")',
    ),
    "diagnostic_saturation_hidden": (
        T + "test_diagnostic_readonly_and_saturation",
        "app/src/database/repositories/scheduler_diagnostics_repo.py",
        '"saturated": [name for name, count in counts.items() if count >= bound]',
        '"saturated": []',
    ),
    # ── S5: new outbox rows never enter legacy_unknown ─────────────────
    "new_delivery_default_reopened": (
        "tests.test_delivery_persistence_mysql.DeliveryPersistenceMySQLTests."
        "test_new_outbox_delivery_requires_explicit_nonlegacy_class",
        OUTBOX,
        '    if delivery_class == "legacy_unknown":\n'
        "        raise OutboxDeliveryClassRequiredError(\n"
        '            "explicit delivery class is required for new outbox deliveries"\n'
        "        )",
        "    if False:\n"
        "        raise OutboxDeliveryClassRequiredError(\n"
        '            "explicit delivery class is required for new outbox deliveries"\n'
        "        )",
    ),
    # ── state machine and audit ─────────────────────────────────────────
    "self_transition_allowed": (
        T + "test_complete_state_matrix_and_append_only_audit", SCHEDULER,
        '    "enabled": frozenset({"draining"}),',
        '    "enabled": frozenset({"draining", "enabled"}),',
    ),
    "audit_not_written": (
        T + "test_complete_state_matrix_and_append_only_audit", SCHEDULER,
        "    db.add(DeliveryOperatorAction(\n        delivery_id=None,",
        "    (DeliveryOperatorAction(\n        delivery_id=None,",
    ),
    "expected_version_ignored": (
        T + "test_epoch_rotation_version_and_atomic_audit", SCHEDULER,
        "    if state.version != expected_version:\n        raise SchedulerConflictError",
        "    if False:\n        raise SchedulerConflictError",
    ),
    "epoch_rotates_while_active": (
        T + "test_epoch_rotation_version_and_atomic_audit", SCHEDULER,
        '    if state.state != "disabled" and state.scheduler_epoch != scheduler_epoch:',
        '    if False:',
    ),
    "audit_reason_truncation_unchecked": (
        T + "test_cli_show_edges_refusal_and_metadata_validation", SCHEDULER,
        "    if len(audit_reason) > 500:",
        "    if False:",
    ),
    # ── clearance guard ─────────────────────────────────────────────────
    "clearance_only_from_draining": (
        T + "test_guard_nonterminal_turns_and_enabling_abort_rotation", SCHEDULER,
        '    if target == "disabled" or state.state == "disabled":\n        await _require_scheduler_clearance(db)',
        '    if target == "disabled" and state.state == "draining":\n        await _require_scheduler_clearance(db)',
    ),
    "owned_lanes_uncounted": _zero_category("owned_lanes", "test_guard_owned_lane_even_with_terminal_turn"),
    "nonterminal_turns_uncounted": _zero_category(
        "nonterminal_turns", "test_guard_nonterminal_turns_and_enabling_abort_rotation"
    ),
    "unresolved_attempts_uncounted": (
        T + "test_guard_unresolved_attempt_matrix", SCHEDULER,
        '    counts["unresolved_attempts"] = int(attempts[0])',
        '    counts["unresolved_attempts"] = 0 * int(attempts[0])',
    ),
    "unreconciled_attempts_uncounted": (
        T + "test_guard_unreconciled_known_attempt", SCHEDULER,
        '    counts["unreconciled_attempts"] = int(attempts[1])',
        '    counts["unreconciled_attempts"] = 0 * int(attempts[1])',
    ),
    "scheduled_handoffs_uncounted": _zero_category(
        "nonterminal_scheduled_handoffs", "test_guard_scheduled_handoff_category"
    ),
    "automated_deliveries_uncounted": (
        T + "test_guard_automated_deliveries_even_with_terminal_turn", SCHEDULER,
        '    counts["automated_deliveries"] = int(deliveries[0])',
        '    counts["automated_deliveries"] = 0 * int(deliveries[0])',
    ),
    "legacy_unknown_uncounted": (
        T + "test_guard_legacy_unknown_deliveries", SCHEDULER,
        '    counts["legacy_unknown_deliveries"] = int(deliveries[1])',
        '    counts["legacy_unknown_deliveries"] = 0 * int(deliveries[1])',
    ),
    "drain_refusal_not_counted": (
        T + "test_guard_legacy_unknown_deliveries", SCHEDULER,
        "        _drain_refusal_count += 1",
        "        _drain_refusal_count += 0",
    ),
    # ── A1/A3: guard UoW shape and lock footprint ───────────────────────
    "repeatable_read_guard": (
        T + "test_clearance_lock_footprint_no_gap_or_secondary_locks", SCHEDULER,
        '        await db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))',
        '        await db.execute(text("DO 0"))',
    ),
    "outbox_scan_secondary_index": (
        T + "test_clearance_lock_footprint_no_gap_or_secondary_locks", SCHEDULER,
        '        "FROM outbox_deliveries FORCE INDEX (PRIMARY) "',
        '        "FROM outbox_deliveries "',
    ),
    "turn_scan_secondary_index": (
        T + "test_clearance_lock_footprint_no_gap_or_secondary_locks", SCHEDULER,
        '        "SELECT COUNT(*) FROM conversation_turns FORCE INDEX (PRIMARY) "',
        '        "SELECT COUNT(*) FROM conversation_turns "',
    ),
    "outbox_scan_locks_every_row": (
        T + "test_clearance_lock_footprint_no_gap_or_secondary_locks", SCHEDULER,
        "        \"WHERE delivery_class IN ('automated','legacy_unknown') \"\n"
        "        f\"AND status IN {_NONTERMINAL_DELIVERY_SQL} FOR SHARE\"",
        "        \"WHERE delivery_class IS NOT NULL \"\n"
        "        f\"AND status IS NOT NULL FOR SHARE\"",
    ),
    "stale_transaction_accepted": (
        T + "test_guard_requires_fresh_transaction_and_reads_current", SCHEDULER,
        "        if _mysql_error_code(exc) == 1568:",
        "        if False:",
    ),
    # ── intake / replay ─────────────────────────────────────────────────
    "replay_requires_enabled": (
        T + "test_replay_state_matrix_no_cap_or_new_work", ORCHESTRATOR,
        "            delivery_runtime_epoch=settings.delivery_cutover_epoch,\n"
        "            require_enabled=False,\n"
        "            create_lane=True,",
        "            delivery_runtime_epoch=settings.delivery_cutover_epoch,\n"
        "            require_enabled=True,\n"
        "            create_lane=True,",
    ),
    "new_intake_state_gate_removed": (
        T + "test_http_webhook_state_gate_matrix", ORCHESTRATOR,
        "        scheduler_repo.require_new_intake_enabled(scheduler)\n",
        "",
    ),
    "intake_refusal_not_counted": (
        T + "test_http_webhook_state_gate_matrix", SCHEDULER,
        "        _intake_state_refusal_count += 1",
        "        _intake_state_refusal_count += 0",
    ),
    # ── drain / claims / pre-I/O ────────────────────────────────────────
    "draining_refuses_claims": (
        T + "test_drain_accepts_claim_generation_delivery_then_disables", SCHEDULER,
        '    if scheduler.state not in {"enabled", "draining"}:\n'
        '        raise SchedulerNotReadyError("scheduler does not permit accepted-work claims")',
        '    if scheduler.state not in {"enabled"}:\n'
        '        raise SchedulerNotReadyError("scheduler does not permit accepted-work claims")',
    ),
    "enabling_permits_claims": (
        T + "test_enabling_disabled_refuse_claims", SCHEDULER,
        '    if scheduler.state not in {"enabled", "draining"}:\n'
        '        raise SchedulerNotReadyError("scheduler does not permit accepted-work claims")',
        '    if False:\n'
        '        raise SchedulerNotReadyError("scheduler does not permit accepted-work claims")',
    ),
    "preio_scheduler_gate_removed": (
        T + "test_preio_refuses_claim_in_enabling_and_disabled", SCHEDULER,
        "    if (\n"
        '        scheduler.state not in {"enabled", "draining"}\n'
        "        or graph.runtime.cutover_epoch != cutover_epoch\n"
        "    ):",
        "    if (\n"
        "        False\n"
        "        or graph.runtime.cutover_epoch != cutover_epoch\n"
        "    ):",
    ),
    # ── CLI ─────────────────────────────────────────────────────────────
    "cli_refusal_reported_success": (
        T + "test_cli_show_edges_refusal_and_metadata_validation", CLI,
        '        _emit({"status": "refused", "error_type": type(exc).__name__, "clearance": exc.counts})\n'
        "        return 1",
        '        _emit({"status": "refused", "error_type": type(exc).__name__, "clearance": exc.counts})\n'
        "        return 0",
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
        print(f"targets and syntax ok: {len(CASES)}")
        return 0
    if len(argv) == 4 and argv[1] == "apply":
        apply(Path(argv[2]), argv[3])
        return 0
    raise SystemExit("usage: inbox_t081_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
