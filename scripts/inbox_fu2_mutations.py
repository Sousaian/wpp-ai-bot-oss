#!/usr/bin/env python3
"""FU-2 fix-removal mutations; each named test must fail on disposable MySQL."""
import ast
from pathlib import Path
import sys
from typing import NamedTuple

SCHEDULER = "app/src/database/repositories/scheduler_repo.py"
LIFECYCLE = "app/src/database/repositories/lifecycle_repo.py"
RETENTION = "app/src/database/repositories/scheduler_retention_repo.py"
DELIVERY = "app/src/database/repositories/turn_delivery_repo.py"
FOOTPRINT = "tests.test_lock_footprint_mysql.LockFootprintMySQLTests."
T07 = "tests.test_lifecycle_mysql.LifecycleMySQLTests."
SESSION_TEST = FOOTPRINT + "test_session_locking_paths_under_skewed_history"
HINT = '.with_hint(ConversationTurn, "FORCE INDEX (idx_turn_session_status)", "mysql")'


class Mutation(NamedTuple):
    test: str
    path: str
    function: str
    old: str
    new: str


CASES = {
    **{
        "unhinted_" + function: Mutation(SESSION_TEST, SCHEDULER, function, HINT, "")
        for function in (
            "lock_intake_turn_graph", "lock_pending_successor",
            "_promote_successor_after_terminal_active", "complete_turn_and_release_lane",
        )
    },
    "unhinted_lifecycle_graph": Mutation(
        SESSION_TEST, LIFECYCLE, "lock_scheduled_graph",
        '            index="idx_turn_session_status",  # FU-2\n', "",
    ),
    "unhinted_history": Mutation(
        SESSION_TEST, LIFECYCLE, "_has_scheduler_history",
        '.with_hint(ConversationTurn, f"FORCE INDEX ({index})", "mysql")', "",
    ),
    "unhinted_retention": Mutation(
        SESSION_TEST, RETENTION, "_lock",
        'query = query.with_hint(model, "FORCE INDEX (idx_turn_session_status)", "mysql")',
        "pass",
    ),
    "ledger_link_speculative_share": Mutation(
        T07 + "test_two_agents_late_accounting_no_unique_gap_deadlock",
        SCHEDULER, "_lock_matching_consumption",
        '    expected_call_type = "resposta" if attempt.attempt_type == "response" else "resumo"',
        '    await db.execute(select(GenerationAttempt.id).where(\n'
        '        GenerationAttempt.consumo_llm_id == consumo.id,\n'
        '        GenerationAttempt.id != attempt.id,\n'
        '    ).limit(1).with_for_update(read=True))\n'
        '    expected_call_type = "resposta" if attempt.attempt_type == "response" else "resumo"',
    ),
    "savepoint_loss_swallowed": Mutation(
        T07 + "test_accounting_savepoint_real_deadlock_escapes",
        SCHEDULER, "_write_generation_accounting",
        "        if db_errors.transaction_lost(exc):\n            raise\n",
        "        if db_errors.transaction_lost(exc):\n            return False\n",
    ),
    "operator_intent_speculative_share": Mutation(
        FOOTPRINT + "test_settlement_absent_operator_intent_takes_no_gap_lock",
        DELIVERY, "_settle_turn_after_effect",
        '            DeliveryOperatorAction.action == "turn_resolve:abandon",\n'
        '        )\n        .limit(1)\n',
        '            DeliveryOperatorAction.action == "turn_resolve:abandon",\n'
        '        )\n        .limit(1)\n        .with_for_update(read=True)\n',
    ),
}


def mutated(content: str, case: Mutation) -> str:
    """Scope targets to one top-level function, without copying its whole body."""
    functions = [node for node in ast.parse(content).body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and node.name == case.function]
    if len(functions) != 1:
        raise SystemExit(f"expected one function: {case.function}; found {len(functions)}")
    node = functions[0]
    lines = content.splitlines(keepends=True)
    body = "".join(lines[node.lineno - 1:node.end_lineno])
    if body.count(case.old) != 1:
        raise SystemExit(f"{case.function}: expected one mutation target, found {body.count(case.old)}")
    # Replacing a whole fluent method with nothing must not leave a bare dot
    # line. Its comment, if any, is allowed to remain as its own Python line.
    result = ("".join(lines[:node.lineno - 1]) + body.replace(case.old, case.new, 1)
              + "".join(lines[node.end_lineno:]))
    compile(result, case.path, "exec")
    return result


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "list":
        for name, case in CASES.items():
            print(name, case.test)
        return 0
    if len(argv) == 2 and argv[1] == "count":
        print(len(CASES))
        return 0
    if len(argv) == 2 and argv[1] == "check":
        root = Path(__file__).resolve().parent.parent
        for case in CASES.values():
            mutated((root / case.path).read_text(), case)
        print(f"targets and syntax ok: {len(CASES)}")
        return 0
    if len(argv) == 4 and argv[1] == "apply":
        case = CASES[argv[3]]
        path = Path(argv[2]) / case.path
        path.write_text(mutated(path.read_text(), case))
        return 0
    raise SystemExit("usage: inbox_fu2_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
