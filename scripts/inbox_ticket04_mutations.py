#!/usr/bin/env python3
"""Ticket 04 fix-removal mutations for the barrier and unit tests.

Each mutation removes exactly one protection from a COPY of ``app/`` and
names the test that must fail without it. ``apply`` asserts the original text
occurs exactly once so a drifted source cannot silently produce a no-op.

Migration mutations remove one exact-shape predicate from a COPY of
``db/init.sql``; the matrix proves the named fixture is then accepted while the
unmutated migration fails closed on it without moving the column.

Usage:
  inbox_ticket04_mutations.py list
  inbox_ticket04_mutations.py apply <copied_repo_root> <mutation_id>
  inbox_ticket04_mutations.py list-migration
  inbox_ticket04_mutations.py migration-fixture <mutation_id>
  inbox_ticket04_mutations.py apply-migration <init_sql_copy> <mutation_id>
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = "app/src/database/repositories/turn_generation_repo.py"
SCHED = "app/src/database/repositories/scheduler_repo.py"
PROC = "app/src/services/turn_processor.py"
CONSUMO = "app/src/database/repositories/consumo_repo.py"
RECON = "app/src/database/repositories/usage_reconciliation_repo.py"
LLM = "app/src/services/llm_client.py"
WORKER = "app/src/delivery_worker.py"
CLI = "app/src/delivery_cli.py"
MYSQL_DRAFT_GUARD = "__mysql_draft_guard__"
T = "tests.test_turn_processor_mysql.TurnProcessorMySQLTests."
U = "tests.test_turn_processor_unit."
W = "tests.test_delivery_worker_unit.WorkerGatingTests."

MUTATIONS: dict[str, tuple[str, str, str, str]] = {
    "refresh_during_generation": (
        REPO,
        "    _store_draft(graph, revision, draft)\n"
        "    if revision.revision_number == 1 and await _active_input_count(db, graph.turn.id):",
        "    _store_draft(graph, revision, draft)\n"
        "    if False and revision.revision_number == 1 and await _active_input_count(db, graph.turn.id):",
        T + "test_input_during_generation_refreshes_once_and_accounts_both",
    ),
    "refresh_during_typing": (
        REPO,
        '        if revision.status == "typing":\n'
        "            if revision.revision_number == 1 and await _active_input_count(",
        '        if revision.status == "typing":\n'
        "            if False and revision.revision_number == 1 and await _active_input_count(",
        T + "test_input_during_typing_refreshes_and_later_input_is_successor",
    ),
    "owner_lease_fence": (
        REPO,
        "        or turn.claim_until <= now\n"
        "        or lane.owner_token != lease.owner_token\n"
        "        or lane.active_turn_id != lease.turn_id\n"
        "        or lane.lease_until is None\n"
        "        or lane.lease_until <= now\n",
        "        or lane.owner_token != lease.owner_token\n"
        "        or lane.active_turn_id != lease.turn_id\n"
        "        or lane.lease_until is None\n",
        T + "test_lease_loss_in_flight_finalizes_only_exact_accounting",
    ),
    # "lane_owner_fence_across_replicas" moved to the ticket07 list
    # (legacy_claim_owner_fence / hinted_claim_owner_fence): replicas now claim
    # through a committed discovery hint, so the owner fence, the eligibility
    # predicate and the discovery owned-lane filter are independent defenses;
    # each fence has its own deterministic ticket07 test.
    "unknown_outcome_quarantine": (
        PROC,
        'kind = "failed" if exc.outcome_known else "outcome_unknown"',
        'kind = "failed"',
        T + "test_unknown_outcome_quarantines_agent_while_others_progress",
    ),
    "soft_limit_recheck": (
        REPO,
        "            if await consumo_repo.atingiu_limite_mensal(graph.agent.id, db):",
        "            if False:",
        T + "test_soft_limit_rechecked_before_refresh_generation",
    ),
    "forced_shutdown_unknown_accounting": (
        PROC,
        "            await asyncio.shield(self._finish(lease, step, unknown, None, handle))",
        "            pass",
        T + "test_forced_shutdown_records_unknown_and_quarantines",
    ),
    "graceful_shutdown_drain": (
        PROC,
        "        _, pending = await asyncio.wait(tasks, timeout=grace_seconds)",
        "        _, pending = await asyncio.wait(tasks, timeout=0)",
        T + "test_graceful_shutdown_drains_generation_then_releases",
    ),
    "restart_reroll": (
        REPO,
        "            if graph.now >= revision.typing_deadline_at:\n"
        "                await _seal_final_candidate(db, graph, revision)",
        "            _schedule_typing(graph, revision, policy)\n"
        "            if graph.now >= revision.typing_deadline_at:\n"
        "                await _seal_final_candidate(db, graph, revision)",
        T + "test_restart_resumes_persisted_typing_without_reroll",
    ),
    "lifecycle_refusal": (
        REPO,
        '        or graph.session.handler != "bot"\n',
        "        or False\n",
        T + "test_lifecycle_refusal_makes_no_provider_call",
    ),
    "fresh_lane_identity": (
        SCHED,
        "        select(AgentServiceLane)\n"
        "        .where(AgentServiceLane.agente_id == agente_id)\n"
        "        .with_for_update()\n"
        "        .execution_options(populate_existing=True)\n",
        "        select(AgentServiceLane)\n"
        "        .where(AgentServiceLane.agente_id == agente_id)\n"
        "        .with_for_update()\n",
        T + "test_preloaded_identity_map_cannot_grant_stale_authority",
    ),
    "accounting_parent_share_locks": (
        REPO,
        "    if not await _share_lock_accounting_parents(\n"
        "        db, agente_id=agente_id, sessao_id=sessao_id\n"
        "    ):\n"
        "        return False\n",
        "",
        T + "test_accounting_only_overlapping_recovery_keeps_lock_order",
    ),
    "consumption_lost_transaction_reraise": (
        CONSUMO,
        "        if db_errors.transaction_lost(exc):\n"
        "            raise\n"
        "        if provider_generation_id is not None:",
        "        if provider_generation_id is not None:",
        U
        + "TransactionLostClassificationTests.test_consumption_insert_loss_never_enters_reconciliation_fallback",
    ),
    "reconciliation_fallback_lost_transaction_reraise": (
        CONSUMO,
        "            lost_transaction = db_errors.transaction_lost_dbapi_error(exc)\n"
        "            if lost_transaction is not None:\n"
        "                raise lost_transaction\n",
        "",
        U
        + "TransactionLostClassificationTests.test_reconciliation_wrapper_reraises_lost_db_error_for_retry",
    ),
    "reconciliation_lost_transaction_reraise": (
        RECON,
        "            if db_errors.transaction_lost(exc):\n"
        "                raise\n"
        "            row.last_error_code",
        "            row.last_error_code",
        T + "test_usage_reconciliation_batch_reraises_lost_transaction",
    ),
    "lost_savepoint_whole_retry": (
        SCHED,
        "    return isinstance(exc, DBAPIError) and db_errors.transaction_lost(exc)",
        "    return isinstance(exc, DBAPIError) and _mysql_error_code(exc) in {1205, 1213}",
        U + "TransactionLostClassificationTests.test_lost_savepoint_restarts_the_whole_transaction",
    ),
    "maintenance_lost_transaction_skip": (
        WORKER,
        "            if not db_errors.transaction_lost(exc):\n"
        "                raise\n",
        "            raise\n",
        U + "TransactionLostClassificationTests.test_maintenance_skips_a_lost_transaction_but_raises_others",
    ),
    "maintenance_consecutive_skip_counter": (
        WORKER,
        "            self._maintenance_consecutive_skips += 1\n",
        "            pass\n",
        U
        + "TransactionLostClassificationTests.test_maintenance_skips_a_lost_transaction_but_raises_others",
    ),
    "maintenance_consecutive_skip_counter_mysql": (
        WORKER,
        "            self._maintenance_consecutive_skips += 1\n",
        "            pass\n",
        T + "test_maintenance_real_lock_timeout_counts_and_recovers",
    ),
    "maintenance_skip_reset": (
        WORKER,
        "        previous_skips = self._maintenance_consecutive_skips\n"
        "        self._maintenance_consecutive_skips = 0\n",
        "        previous_skips = self._maintenance_consecutive_skips\n",
        W + "test_direct_idle_worker_stays_ready_and_recovers_after_one_pass",
    ),
    "maintenance_skip_error_escalation": (
        WORKER,
        "            log = logger.error if not self.maintenance_healthy else logger.warning\n",
        "            log = logger.warning\n",
        U
        + "TransactionLostClassificationTests.test_maintenance_skips_a_lost_transaction_but_raises_others",
    ),
    "maintenance_snapshot_not_ready": (
        WORKER,
        '            "ready": self._readiness_ok and self.maintenance_healthy,\n',
        '            "ready": self._readiness_ok,\n',
        W + "test_direct_idle_worker_stays_ready_and_recovers_after_one_pass",
    ),
    "health_cli_maintenance_gate": (
        CLI,
        "        and provider_ok\n"
        "        and maintenance_ok\n",
        "        and provider_ok\n",
        W + "test_health_fails_at_local_maintenance_skip_threshold",
    ),
    "expired_hint_discovery_skips_quarantine": (
        REPO,
        "\n            ~held_by_quarantine,\n",
        "\n",
        T + "test_quarantined_expired_lanes_do_not_starve_crash_recovery",
    ),
    "expired_discovery_skips_quarantine": (
        REPO,
        "                ~held_by_quarantine,\n",
        "",
        T + "test_quarantined_expired_lanes_do_not_starve_crash_recovery",
    ),
    "malformed_completion_unknown": (
        LLM,
        '        if not getattr(completion, "choices", None) or getattr(completion, "usage", None) is None:\n'
        '            raise ValueError("provider completion has no choices or usage")\n',
        "",
        U + "SingleAttemptProviderTests.test_missing_usage_or_choices_is_unknown_not_zero_cost",
    ),
    "processor_requested_model_link": (
        REPO,
        "            modelo_llm=attempt.modelo_llm,\n"
        "            modelo_llm_reportado=usage.modelo_llm_reportado,",
        "            modelo_llm=usage.modelo_llm_reportado or attempt.modelo_llm,\n"
        "            modelo_llm_reportado=usage.modelo_llm_reportado,",
        T + "test_dated_provider_echo_links_exactly_without_quarantine",
    ),
    "legacy_requested_model": (
        LLM,
        "        modelo_llm=modelo_llm,\n"
        '        modelo_llm_reportado=getattr(completion, "model", None) or None,',
        "        modelo_llm=completion.model or modelo_llm,\n"
        '        modelo_llm_reportado=getattr(completion, "model", None) or None,',
        T + "test_legacy_runner_persists_requested_and_reported_models",
    ),
    "context_excludes_pending_inputs": (
        REPO,
        '                        ConversationTurn.session_slot == "terminal",\n',
        "                        True,\n",
        T + "test_burst_one_generation_persisted_typing_and_final_seal",
    ),
    "admitted_revision_terminal": (
        MYSQL_DRAFT_GUARD,
        "",
        "",
        T + "test_admitted_revision_status_is_terminal",
    ),
}

# name → (anchor, replacement, table, column, fixture DDL, expected error).
# Each fixture is a correctly typed column in the wrong ordinal position on the
# merged ticket 03 predecessor schema.
MIGRATION_MUTATIONS: dict[str, tuple[str, str, str, str, str, str]] = {
    "typing_jitter_exact_position": (
        "          AND typing_jitter.ordinal_position = typing_delay.ordinal_position + 1\n",
        "",
        "turn_revisions",
        "typing_jitter_ms",
        "ALTER TABLE turn_revisions ADD COLUMN typing_jitter_ms SMALLINT NULL AFTER draft_text;",
        "turn revision typing jitter column requires audited repair",
    ),
    "draft_kind_exact_position": (
        "          AND draft_kind.ordinal_position = needs_handoff.ordinal_position + 1\n",
        "",
        "turn_revisions",
        "draft_kind",
        "ALTER TABLE turn_revisions ADD COLUMN draft_kind "
        "ENUM('generated','soft_limit_fallback','technical_fallback') NULL AFTER typing_delay_ms;",
        "turn revision draft kind column requires audited repair",
    ),
    "consumption_reported_model_exact_position": (
        "          AND reported.table_name = 'consumo_llm'\n"
        "          AND reported.column_name = 'modelo_llm_reportado'\n"
        "          AND reported.column_type = 'varchar(255)'\n"
        "          AND reported.is_nullable = 'YES'\n"
        "          AND reported.column_default IS NULL\n"
        "          AND reported.collation_name <=> requested.collation_name\n"
        "          AND reported.ordinal_position = requested.ordinal_position + 1\n",
        "          AND reported.table_name = 'consumo_llm'\n"
        "          AND reported.column_name = 'modelo_llm_reportado'\n"
        "          AND reported.column_type = 'varchar(255)'\n"
        "          AND reported.is_nullable = 'YES'\n"
        "          AND reported.column_default IS NULL\n"
        "          AND reported.collation_name <=> requested.collation_name\n",
        "consumo_llm",
        "modelo_llm_reportado",
        "ALTER TABLE consumo_llm ADD COLUMN modelo_llm_reportado VARCHAR(255) NULL AFTER tipo_chamada;",
        "consumption reported model column requires audited repair",
    ),
    "reconciliation_reported_model_exact_position": (
        "          AND reported.table_name = 'llm_usage_reconciliation'\n"
        "          AND reported.column_name = 'modelo_llm_reportado'\n"
        "          AND reported.column_type = 'varchar(255)'\n"
        "          AND reported.is_nullable = 'YES'\n"
        "          AND reported.column_default IS NULL\n"
        "          AND reported.collation_name <=> requested.collation_name\n"
        "          AND reported.ordinal_position = requested.ordinal_position + 1\n",
        "          AND reported.table_name = 'llm_usage_reconciliation'\n"
        "          AND reported.column_name = 'modelo_llm_reportado'\n"
        "          AND reported.column_type = 'varchar(255)'\n"
        "          AND reported.is_nullable = 'YES'\n"
        "          AND reported.column_default IS NULL\n"
        "          AND reported.collation_name <=> requested.collation_name\n",
        "llm_usage_reconciliation",
        "modelo_llm_reportado",
        "ALTER TABLE llm_usage_reconciliation ADD COLUMN modelo_llm_reportado "
        "VARCHAR(255) NULL AFTER tipo_chamada;",
        "usage reconciliation reported model column requires audited repair",
    ),
}


def _replace_once(target: Path, old: str, new: str, name: str) -> int:
    source = target.read_text()
    if source.count(old) != 1:
        print(f"MUTATION → FAIL ({name}): anchor count {source.count(old)}", file=sys.stderr)
        return 3
    target.write_text(source.replace(old, new))
    return 0


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "list":
        for name, (_, _, _, test) in MUTATIONS.items():
            print(f"{name} {test}")
        return 0
    if len(argv) == 4 and argv[1] == "apply":
        root = Path(argv[2])
        path, old, new, _ = MUTATIONS[argv[3]]
        if path == MYSQL_DRAFT_GUARD:
            (root / "ticket04_weaken_admitted_guard.sql").write_text(
                """DROP TRIGGER trg_turn_revision_draft_immutable_v2;
CREATE TRIGGER trg_turn_revision_draft_immutable_v2 BEFORE UPDATE ON turn_revisions
FOR EACH ROW SET @ticket04_weakened_admitted_guard = 1;
"""
            )
            return 0
        return _replace_once(root / path, old, new, argv[3])
    if len(argv) == 2 and argv[1] == "list-migration":
        for name, (_, _, table, column, _, error) in MIGRATION_MUTATIONS.items():
            print(f"{name} {table} {column} {error}")
        return 0
    if len(argv) == 3 and argv[1] == "migration-fixture":
        print(MIGRATION_MUTATIONS[argv[2]][4])
        return 0
    if len(argv) == 4 and argv[1] == "apply-migration":
        old, new, *_ = MIGRATION_MUTATIONS[argv[3]]
        return _replace_once(Path(argv[2]), old, new, argv[3])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
