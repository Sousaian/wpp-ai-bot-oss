#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the T08-6 guarded opt-in matrix.

Each case removes exactly one protection from a copy of ``app/`` and names the
test that must fail against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T = "tests.test_agent_scheduler_opt_in_mysql.AgentSchedulerOptInMySQLTests."
REPO = "app/src/database/repositories/agent_scheduler_repo.py"
LIFECYCLE = "app/src/database/repositories/lifecycle_repo.py"
CLI = "app/src/agent_scheduler_cli.py"
ROUTER = "app/src/routers/agentes.py"
MATRIX = T + "test_opt_in_guard_matrix_complete"


def _guard(condition: str, test: str = MATRIX) -> tuple[str, str, str, str]:
    return (test, REPO, f"    if {condition}:\n        failed.append(", "    if False:\n        failed.append(")


CASES = {
    # ── S5: drain discovery is a bounded lock-free READ ONLY snapshot ──
    "list_snapshot_not_read_only": (
        T + "test_cli_list_opted_in_is_paginated_read_only_and_lock_free", REPO,
        '    await db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"))',
        '    await db.execute(text("SELECT 1"))',
    ),
    "list_takes_share_locks": (
        T + "test_cli_list_opted_in_is_paginated_read_only_and_lock_free", REPO,
        "            .order_by(Agente.id)\n            .limit(limit + 1)",
        "            .order_by(Agente.id)\n"
        "            .with_for_update(read=True)\n"
        "            .limit(limit + 1)",
    ),
    # ── guards (allowlist on locked rows) ───────────────────────────────
    "scheduler_state_guard_removed": _guard("scheduler.state not in OPT_IN_SCHEDULER_STATES"),
    "scheduler_epoch_guard_removed": _guard("scheduler.scheduler_epoch != scheduler_epoch"),
    "runtime_mode_guard_removed": _guard("runtime.mode not in OPT_IN_RUNTIME_MODES"),
    "delivery_epoch_guard_removed": _guard("runtime.cutover_epoch != delivery_epoch"),
    "auth_guard_removed": _guard("provider_auth_enforced is not True"),
    "agent_active_guard_removed": _guard("agent.ativo is not True"),
    "chatwoot_inbox_guard_removed": _guard("agent.chatwoot_inbox_id is None"),
    "already_opted_in_guard_removed": _guard("agent.inbox_scheduler_enabled is not False"),
    "drain_race_scheduler_guard_removed": _guard(
        "scheduler.state not in OPT_IN_SCHEDULER_STATES",
        T + "test_opt_in_vs_drain_race_both_orders_repeated",
    ),
    # ── lock order / transaction shape ──────────────────────────────────
    "scheduler_read_unlocked": (
        T + "test_opt_in_vs_drain_race_both_orders_repeated", REPO,
        "    scheduler = await scheduler_repo.get_scheduler_state(db, for_update=True)\n"
        "    agent = await _lock_agent(db, agente_id, share=False)",
        "    scheduler = await scheduler_repo.get_scheduler_state(db)\n"
        "    agent = await _lock_agent(db, agente_id, share=False)",
    ),
    "fresh_transaction_not_required": (
        T + "test_opt_in_requires_fresh_transaction", REPO,
        "    await scheduler_repo.begin_scheduler_control(db)\n"
        "    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)\n"
        "    scheduler = await scheduler_repo.get_scheduler_state(db, for_update=True)",
        "    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)\n"
        "    scheduler = await scheduler_repo.get_scheduler_state(db, for_update=True)",
    ),
    # ── signal and audit ────────────────────────────────────────────────
    "refusal_not_counted": (
        MATRIX, REPO, "    _refusal_counts[action] += 1", "    _refusal_counts[action] += 0",
    ),
    "opt_in_audit_not_written": (
        T + "test_opt_in_audit_and_metadata", REPO,
        "    db.add(DeliveryOperatorAction(\n        delivery_id=None, action=\"agent_scheduler:opt_in\"",
        "    (DeliveryOperatorAction(\n        delivery_id=None, action=\"agent_scheduler:opt_in\"",
    ),
    "opt_out_audit_not_written": (
        T + "test_opt_out_reuses_lifecycle_refusal_and_audits", REPO,
        "    db.add(DeliveryOperatorAction(\n        delivery_id=None, action=\"agent_scheduler:opt_out\"",
        "    (DeliveryOperatorAction(\n        delivery_id=None, action=\"agent_scheduler:opt_out\"",
    ),
    "audit_reason_length_unchecked": (
        T + "test_opt_in_audit_and_metadata", REPO,
        "    if len(value) > AUDIT_REASON_MAX:", "    if False:",
    ),
    # ── opt-out through lifecycle ───────────────────────────────────────
    "opt_out_lifecycle_guard_removed": (
        T + "test_opt_out_reuses_lifecycle_refusal_and_audits", LIFECYCLE,
        "        if opting_out and not disabling and graph.turns:",
        "        if False:",
    ),
    "opt_out_not_opted_in_accepted": (
        T + "test_opt_out_reuses_lifecycle_refusal_and_audits", REPO,
        "        if agent.inbox_scheduler_enabled is not True:", "        if False:",
    ),
    # ── CLI ─────────────────────────────────────────────────────────────
    "cli_refusal_reported_success": (
        T + "test_cli_opt_in_out_show_and_auth_sources", CLI,
        '            "reasons": list(exc.reasons), "provider_auth_issues": issues,\n'
        "        })\n        return 1",
        '            "reasons": list(exc.reasons), "provider_auth_issues": issues,\n'
        "        })\n        return 0",
    ),
    "cli_auth_snapshot_ignored": (
        T + "test_cli_opt_in_out_show_and_auth_sources", CLI,
        "        and report.ready\n", "        and True\n",
    ),
    "cli_auth_snapshot_mode_ignored": (
        T + "test_cli_opt_in_out_show_and_auth_sources", CLI,
        '        and report.auth_mode == "enforced"\n', "        and True\n",
    ),
    "cli_auth_local_mode_ignored": (
        T + "test_cli_opt_in_out_show_and_auth_sources", CLI,
        '        settings.provider_webhook_auth_mode == "enforced"\n', "        True\n",
    ),
    # ── PUT /agentes inbox guard (lifecycle UoW) ────────────────────────
    "put_inbox_clear_guard_removed": (
        T + "test_put_cannot_clear_inbox_of_opted_in_agent", LIFECYCLE,
        '        and payload["chatwoot_inbox_id"] is None\n        and payload.get(',
        '        and False\n        and payload.get(',
    ),
    "put_inbox_race_guard_removed": (
        T + "test_put_inbox_clear_vs_opt_in_race_both_orders_repeated", LIFECYCLE,
        '        and payload["chatwoot_inbox_id"] is None\n        and payload.get(',
        '        and False\n        and payload.get(',
    ),
    "put_inbox_refusal_not_counted": (
        T + "test_put_cannot_clear_inbox_of_opted_in_agent", LIFECYCLE,
        '            "opted_in_inbox_clear_refused",\n            "Opted-in',
        '            "rediscovered",\n            "Opted-in',
    ),
    # ── public API ──────────────────────────────────────────────────────
    "public_update_schema_exposes_flag": (
        T + "test_public_api_never_exposes_flag", ROUTER,
        "    ativo: bool | None = None\n",
        "    ativo: bool | None = None\n    inbox_scheduler_enabled: bool | None = None\n",
    ),
    "public_create_schema_exposes_flag": (
        T + "test_public_api_never_exposes_flag", ROUTER,
        "class AgenteCreateSchema(AgenteBaseSchema):\n    pass\n",
        "class AgenteCreateSchema(AgenteBaseSchema):\n    inbox_scheduler_enabled: bool = False\n",
    ),
    "public_response_schema_exposes_flag": (
        T + "test_public_api_never_exposes_flag", ROUTER,
        "    id: int\n    chatwoot_inbox_id: int | None = None\n",
        "    id: int\n    inbox_scheduler_enabled: bool = False\n    chatwoot_inbox_id: int | None = None\n",
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
    raise SystemExit("usage: inbox_t086_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
