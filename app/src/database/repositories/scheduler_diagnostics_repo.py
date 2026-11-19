"""T08-1 / S2: non-authoritative, bounded READ ONLY clearance snapshots.

Never used by transitions. Counts share one MVCC read view; reaching the row
bound is explicitly reported as saturation, not exact clearance. Each query
has the same execution-time budget as the T08-5 metrics.
"""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.repositories import scheduler_repo, scheduler_metrics_repo

# Shared allowlists with the locking guard. No forced PRIMARY scans or locks.
_TURNS = scheduler_repo._NONTERMINAL_TURN_SQL
_DELIVERIES = scheduler_repo._NONTERMINAL_DELIVERY_SQL
SOURCES = {
    "owned_lanes": "agent_service_lanes WHERE owner_token IS NOT NULL",
    "nonterminal_turns": f"conversation_turns WHERE status IN {_TURNS}",
    "unresolved_attempts": "generation_attempts WHERE outcome IN ('started','outcome_unknown')",
    "unreconciled_attempts": (
        "generation_attempts WHERE accounting_state='reconciliation_required' AND reconciled_at IS NULL"
    ),
    "nonterminal_scheduled_handoffs": (
        "scheduled_handoffs sh JOIN conversation_turns t ON t.id=sh.conversation_turn_id "
        f"WHERE t.status IN {_TURNS}"
    ),
    "automated_deliveries": f"outbox_deliveries WHERE delivery_class='automated' AND status IN {_DELIVERIES}",
    "legacy_unknown_deliveries": f"outbox_deliveries WHERE delivery_class='legacy_unknown' AND status IN {_DELIVERIES}",
}
_HINT = f"/*+ MAX_EXECUTION_TIME({scheduler_metrics_repo.QUERY_TIMEOUT_MS}) */"


async def begin_snapshot(db: AsyncSession) -> None:
    # First statement only: MySQL rejects a pre-existing read view (1568).
    if db.new or db.dirty or db.deleted:
        raise scheduler_repo.SchedulerConflictError("scheduler snapshot requires no pending writes")
    await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))


async def scheduler_snapshot(
    db: AsyncSession, *, bound: int = scheduler_metrics_repo.METRIC_ROW_BOUND,
) -> dict[str, object]:
    if bound < 1:
        raise ValueError("bound must be positive")
    await begin_snapshot(db)
    state = (await db.execute(text(
        f"SELECT {_HINT} state, scheduler_epoch, version FROM scheduler_runtime_state WHERE id=1"
    ))).one()
    counts: dict[str, int] = {}
    for category, source in SOURCES.items():
        value = (await db.execute(text(
            f"SELECT {_HINT} COUNT(*) FROM (SELECT 1 FROM {source} LIMIT :bound) snapshot_rows"
        ), {"bound": bound})).scalar_one()
        counts[category] = scheduler_metrics_repo._int(value, category)
    return {
        "state": state.state, "epoch": state.scheduler_epoch, "version": state.version,
        "clearance": counts,
        "diagnostic_only": True,
        "consistency": "read_only_snapshot",
        "clearance_proven": False,
        "row_bound": bound,
        "saturated": [name for name, count in counts.items() if count >= bound],
    }
