#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the S3 agent-delete evidence matrix.

Each case removes exactly one protection from a copy of ``app/`` and names the
test that must fail against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T = "tests.test_agent_delete_evidence_mysql.AgentDeleteEvidenceMySQLTests."
LIFECYCLE = "app/src/database/repositories/lifecycle_repo.py"
ROUTER = "app/src/routers/agentes.py"
AGENTE_REPO = "app/src/database/repositories/agente_repo.py"
SESSAO_REPO = "app/src/database/repositories/sessao_repo.py"
STATIC = "tests.test_delete_paths_static.DeletePathsStaticTests.test_no_agent_or_session_delete_outside_lifecycle_repo"
TABLES = "AGENT_DELETE_EVIDENCE_TABLES = (ConsumoLLM, HandoffAttempt, LLMUsageReconciliation)"
EVIDENCE_READ = "        query = select(model.id).where(model.agente_id == agente_id).limit(1)\n"


def _drop_table(name: str, test: str) -> tuple[str, str, str, str]:
    remaining = ", ".join(t for t in ("ConsumoLLM", "HandoffAttempt", "LLMUsageReconciliation") if t != name)
    return (T + test, LIFECYCLE, TABLES, f"AGENT_DELETE_EVIDENCE_TABLES = ({remaining},)")


CASES = {
    # ── guard ───────────────────────────────────────────────────────────
    "evidence_guard_removed": (
        T + "test_legacy_agent_with_consumo_refused", LIFECYCLE,
        "    if evidence is not None:\n        _signal(", "    if False:\n        _signal(",
    ),
    "evidence_guard_removed_after_purge": (
        T + "test_scheduler_agent_after_t082_purge_still_refused", LIFECYCLE,
        "    if evidence is not None:\n        _signal(", "    if False:\n        _signal(",
    ),
    "consumo_table_unchecked": _drop_table("ConsumoLLM", "test_legacy_agent_with_consumo_refused"),
    "handoff_table_unchecked": _drop_table("HandoffAttempt", "test_agent_with_handoff_attempt_refused"),
    "reconciliation_table_unchecked": _drop_table(
        "LLMUsageReconciliation", "test_agent_with_usage_reconciliation_refused"
    ),
    "refusal_not_counted": (
        T + "test_agent_with_handoff_attempt_refused", LIFECYCLE,
        '            "agent_delete_evidence_refused",\n            "Agent delete refused',
        '            "rediscovered",\n            "Agent delete refused',
    ),
    # ── isolation / lock footprint (A1/A3) ──────────────────────────────
    "repeatable_read_delete": (
        T + "test_evidence_check_footprint_other_tenants_never_wait", LIFECYCLE,
        '        await db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))',
        '        await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))',
    ),
    "evidence_read_for_share": (
        T + "test_evidence_check_footprint_other_tenants_never_wait", LIFECYCLE, EVIDENCE_READ,
        "        query = select(model.id).where(model.agente_id == agente_id).limit(1)"
        ".with_for_update(read=True)\n",
    ),
    "stale_transaction_accepted": (
        T + "test_stale_transaction_fails_closed", LIFECYCLE,
        "        if 1568 in db_errors.mysql_error_codes(exc):\n            raise StaleTransactionError",
        "        if False:\n            raise StaleTransactionError",
    ),
    "evidence_checked_before_agent_lock": (
        T + "test_delete_vs_consumo_insert_race_both_orders_repeated", LIFECYCLE,
        '    await _begin_read_committed(db)\n    prefix = await lock_lifecycle_prefix(db, agente_id=agente_id)\n    if runtime_guard is not None:\n        runtime_guard(prefix.runtime)\n    if prefix.agent is None:\n        return "not_found"\n    await _locked(db, Sessao, Sessao.agente_id == agente_id, order=(Sessao.id,))\n    if await _has_scheduler_history(db, ConversationTurn.agente_id == agente_id, index="idx_turn_ready_claim"):\n        raise LifecycleRefused(SCHEDULER_HISTORY_MESSAGE)\n    await outbox_repo.assert_agent_deletable(db, agente_id)\n    evidence = await _has_agent_evidence(db, agente_id)\n',
        '    await _begin_read_committed(db)\n    evidence = await _has_agent_evidence(db, agente_id)\n    prefix = await lock_lifecycle_prefix(db, agente_id=agente_id)\n    if runtime_guard is not None:\n        runtime_guard(prefix.runtime)\n    if prefix.agent is None:\n        return "not_found"\n    await _locked(db, Sessao, Sessao.agente_id == agente_id, order=(Sessao.id,))\n    if await _has_scheduler_history(db, ConversationTurn.agente_id == agente_id, index="idx_turn_ready_claim"):\n        raise LifecycleRefused(SCHEDULER_HISTORY_MESSAGE)\n    await outbox_repo.assert_agent_deletable(db, agente_id)\n',
    ),
    # ── DELETE /sessoes/{telefone} ──────────────────────────────────────
    "session_handoff_guard_removed": (
        T + "test_session_with_handoff_refused", LIFECYCLE,
        "    if (await db.execute(handoff)).scalar_one_or_none() is not None:",
        "    if False:",
    ),
    "session_handoff_guard_removed_after_cleanup": (
        T + "test_session_after_t082_purge_and_legacy_cleanup_still_refused", LIFECYCLE,
        "    if (await db.execute(handoff)).scalar_one_or_none() is not None:",
        "    if False:",
    ),
    "session_refusal_not_counted": (
        T + "test_session_with_handoff_refused", LIFECYCLE,
        '            "session_delete_handoff_refused",\n            "Session delete refused',
        '            "rediscovered",\n            "Session delete refused',
    ),
    # ── class guard: no delete of agentes/sessoes outside lifecycle_repo ─
    "legacy_agent_orm_delete_reintroduced": (
        STATIC, AGENTE_REPO,
        '    logger.info(f"Agente atualizado: {agente.nome}")\n    return agente\n',
        '    logger.info(f"Agente atualizado: {agente.nome}")\n    return agente\n\n\n'
        'async def delete(db: AsyncSession, agente: Agente) -> None:\n'
        '    await db.delete(agente)\n',
    ),
    "legacy_session_orm_delete_reintroduced": (
        STATIC, SESSAO_REPO,
        '        .where(Sessao.chatwoot_conversation_id == conversation_id)\n    )\n'
        '    return result.scalar_one_or_none()\n',
        '        .where(Sessao.chatwoot_conversation_id == conversation_id)\n    )\n'
        '    return result.scalar_one_or_none()\n\n\n'
        'async def delete(db: AsyncSession, sessao: Sessao) -> None:\n'
        '    await db.delete(sessao)\n',
    ),
    "legacy_session_sql_delete_reintroduced": (
        STATIC, SESSAO_REPO,
        '        .where(Sessao.chatwoot_conversation_id == conversation_id)\n    )\n'
        '    return result.scalar_one_or_none()\n',
        '        .where(Sessao.chatwoot_conversation_id == conversation_id)\n    )\n'
        '    return result.scalar_one_or_none()\n\n\n'
        'PURGE_SQL = "DELETE FROM sessoes WHERE id = :id"\n',
    ),
    # ── router ──────────────────────────────────────────────────────────
    "router_runtime_guard_dropped": (
        T + "test_runtime_guard_runs_on_locked_row_before_evidence", ROUTER,
        "            tx, agente_id=agente_id, runtime_guard=runtime_guard\n",
        "            tx, agente_id=agente_id\n",
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
    raise SystemExit("usage: inbox_sec_s3_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
