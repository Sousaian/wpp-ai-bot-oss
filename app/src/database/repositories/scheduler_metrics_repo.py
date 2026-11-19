"""T08-5: bounded, non-locking aggregate scheduler metrics.

Every query runs in one short ``READ ONLY`` REPEATABLE READ transaction of its
own: plain consistent reads only (no ``FOR SHARE``/``FOR UPDATE``), so the
metrics never take record, gap or next-key locks and never wait on writers.
Each scan is bounded by ``METRIC_ROW_BOUND`` (``saturated`` reports when a
bound was reached, so a lower bound is never presented as exact) and by a
``MAX_EXECUTION_TIME`` hint. The output carries counts, bytes and ages only:
no phone, content, hash, token, epoch or lead identity.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.repositories.scheduler_repo import NONTERMINAL_TURN_STATUSES

METRIC_ROW_BOUND = 100_000
QUERY_TIMEOUT_MS = 2_000
_HINT = f"/*+ MAX_EXECUTION_TIME({QUERY_TIMEOUT_MS}) */"
_NONTERMINAL = "(" + ",".join(f"'{s}'" for s in NONTERMINAL_TURN_STATUSES) + ")"

# Same predicate as turn_generation_repo.list_expired_lane_agent_ids: an
# expired owner held by an unresolved quarantine or a started attempt still
# inside its accounting drain.
_HELD_BY_QUARANTINE = (
    "EXISTS (SELECT 1 FROM generation_attempts g "
    "WHERE g.turn_id = l.active_turn_id AND g.lane_owner_token = l.owner_token "
    "AND ((g.accounting_state = 'reconciliation_required' AND g.reconciled_at IS NULL) "
    "OR (g.outcome = 'started' AND g.accounting_drain_until > UTC_TIMESTAMP(6))))"
)

# Named statements: the matrix runs EXPLAIN on each one (no locking clause).
# Hot tables use forced range/ref access on existing indexes; lanes (PK =
# agente_id) and agentes are scanned whole, both bounded by the agent count.
QUERIES: dict[str, str] = {
    "state": (
        f"SELECT {_HINT} state, version, scheduler_epoch, max_global_queued_turns, "
        "max_global_queued_bytes FROM scheduler_runtime_state WHERE id = 1"
    ),
    "nonterminal_turns": (
        f"SELECT {_HINT} COUNT(*), COALESCE(SUM(b.bytes), 0) FROM ("
        "SELECT (SELECT COALESCE(SUM(m.content_bytes), 0) FROM turn_input_memberships m "
        "WHERE m.turn_id = t.id) AS bytes "
        f"FROM conversation_turns t FORCE INDEX (idx_turn_claim_lease) "
        f"WHERE t.status IN {_NONTERMINAL} LIMIT :bound) b"
    ),
    "ready_turns": (
        f"SELECT {_HINT} COUNT(*), "
        "TIMESTAMPDIFF(MICROSECOND, MIN(r.ready_at), UTC_TIMESTAMP(6)) FROM ("
        "SELECT t.ready_at FROM conversation_turns t FORCE INDEX (idx_turn_claim_lease) "
        "WHERE t.status IN ('pending','retry') AND t.session_slot = 'active' "
        "AND t.scheduler_epoch = :scheduler_epoch AND t.ready_at <= UTC_TIMESTAMP(6) "
        "AND (t.retry_at IS NULL OR t.retry_at <= UTC_TIMESTAMP(6)) LIMIT :bound) r"
    ),
    "lanes": (
        f"SELECT {_HINT} COUNT(*), "
        "COALESCE(SUM(x.expired AND NOT x.held), 0), COALESCE(SUM(x.expired AND x.held), 0) "
        "FROM (SELECT l.lease_until <= UTC_TIMESTAMP(6) AS expired, "
        f"{_HELD_BY_QUARANTINE} AS held FROM agent_service_lanes l "
        "WHERE l.owner_token IS NOT NULL LIMIT :bound) x"
    ),
    "attempt_outcomes": (
        f"SELECT {_HINT} COUNT(*), COALESCE(SUM(x.outcome = 'outcome_unknown'), 0), "
        "COALESCE(SUM(x.outcome = 'started'), 0) FROM ("
        "SELECT g.outcome FROM agentes a STRAIGHT_JOIN generation_attempts g "
        "FORCE INDEX (idx_generation_agent_unknown) ON g.agente_id = a.id "
        "AND g.outcome IN ('started','outcome_unknown') LIMIT :bound) x"
    ),
    "unreconciled_attempts": (
        f"SELECT {_HINT} COUNT(*), COUNT(DISTINCT x.agente_id) FROM ("
        "SELECT g.agente_id FROM agentes a STRAIGHT_JOIN generation_attempts g "
        "FORCE INDEX (idx_generation_agent_reconciliation) ON g.agente_id = a.id "
        "AND g.accounting_state = 'reconciliation_required' AND g.reconciled_at IS NULL "
        "LIMIT :bound) x"
    ),
    "usage_reconciliation": (
        f"SELECT {_HINT} COUNT(*), COALESCE(SUM(x.status IN ('pending','processing')), 0), "
        "COALESCE(SUM(x.status = 'dead'), 0) FROM ("
        "SELECT u.status FROM llm_usage_reconciliation u FORCE INDEX (idx_usage_reconciliation_due) "
        "WHERE u.status IN ('pending','processing','dead') LIMIT :bound) x"
    ),
}


class MetricsUnavailable(ValueError):
    """An aggregate was unreadable. ``scheduler_state`` is the state read from
    the DB in the same snapshot, or ``None`` when the state row itself was not
    readable."""

    def __init__(self, scheduler_state: str | None) -> None:
        super().__init__("scheduler metrics unavailable")
        self.scheduler_state = scheduler_state


@dataclass(frozen=True)
class SchedulerMetrics:
    state: str
    version: int
    scheduler_epoch: str
    cap_global_turns: int
    cap_global_bytes: int
    nonterminal_turns: int
    nonterminal_bytes: int
    ready_turns: int
    oldest_ready_age_seconds: float | None
    owned_lanes: int
    lease_expired_lanes: int
    quarantined_lanes: int
    outcome_unknown_attempts: int
    started_attempts: int
    unreconciled_attempts: int
    quarantined_agents: int
    usage_reconciliation_backlog: int
    usage_reconciliation_dead: int
    saturated: tuple[str, ...]


def _int(value: object, name: str) -> int:
    # Missing/unreadable aggregate is an error, never a silent zero (J).
    if value is None or isinstance(value, bool):
        raise ValueError(f"metric {name} is unreadable")
    number = int(value)
    if number < 0:
        raise ValueError(f"metric {name} is negative")
    return number


async def _row(db: AsyncSession, name: str, **params: object):
    row = (await db.execute(text(QUERIES[name]), params)).one_or_none()
    if row is None:
        raise ValueError(f"metric {name} returned no row")
    return row


async def collect(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    bound: int = METRIC_ROW_BOUND,
) -> SchedulerMetrics:
    if bound < 1:
        raise ValueError("bound must be positive")
    known_state: list[str] = []
    try:
        return await _collect(session_factory, bound=bound, known_state=known_state)
    except Exception as exc:
        raise MetricsUnavailable(known_state[0] if known_state else None) from exc


async def _collect(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    bound: int,
    known_state: list[str],
) -> SchedulerMetrics:
    async with session_factory() as db:
        try:
            # Not a transactional statement: applies to the next transaction,
            # which the first SELECT below opens (consistent snapshot, no locks).
            await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            state = await _row(db, "state")
            if state.state is None or state.scheduler_epoch is None:
                raise ValueError("scheduler state is unreadable")
            known_state.append(str(state.state))
            turns = await _row(db, "nonterminal_turns", bound=bound)
            ready = await _row(
                db, "ready_turns", bound=bound, scheduler_epoch=state.scheduler_epoch
            )
            lanes = await _row(db, "lanes", bound=bound)
            outcomes = await _row(db, "attempt_outcomes", bound=bound)
            unreconciled = await _row(db, "unreconciled_attempts", bound=bound)
            usage = await _row(db, "usage_reconciliation", bound=bound)
        finally:
            await db.rollback()

    saturated = tuple(
        name
        for name, count in (
            ("nonterminal_turns", turns[0]),
            ("ready_turns", ready[0]),
            ("lanes", lanes[0]),
            ("attempt_outcomes", outcomes[0]),
            ("unreconciled_attempts", unreconciled[0]),
            ("usage_reconciliation", usage[0]),
        )
        if count is not None and int(count) >= bound
    )
    ready_count = _int(ready[0], "ready_turns")
    if ready_count and ready[1] is None:
        raise ValueError("metric oldest_ready_age is unreadable")
    return SchedulerMetrics(
        state=str(state.state),
        version=_int(state.version, "version"),
        scheduler_epoch=str(state.scheduler_epoch),
        cap_global_turns=_int(state.max_global_queued_turns, "cap_global_turns"),
        cap_global_bytes=_int(state.max_global_queued_bytes, "cap_global_bytes"),
        nonterminal_turns=_int(turns[0], "nonterminal_turns"),
        nonterminal_bytes=_int(turns[1], "nonterminal_bytes"),
        ready_turns=ready_count,
        oldest_ready_age_seconds=(
            max(int(ready[1]), 0) / 1_000_000 if ready_count else None
        ),
        owned_lanes=_int(lanes[0], "owned_lanes"),
        lease_expired_lanes=_int(lanes[1], "lease_expired_lanes"),
        quarantined_lanes=_int(lanes[2], "quarantined_lanes"),
        outcome_unknown_attempts=_int(outcomes[1], "outcome_unknown_attempts"),
        started_attempts=_int(outcomes[2], "started_attempts"),
        unreconciled_attempts=_int(unreconciled[0], "unreconciled_attempts"),
        quarantined_agents=_int(unreconciled[1], "quarantined_agents"),
        usage_reconciliation_backlog=_int(usage[1], "usage_reconciliation_backlog"),
        usage_reconciliation_dead=_int(usage[2], "usage_reconciliation_dead"),
        saturated=saturated,
    )
