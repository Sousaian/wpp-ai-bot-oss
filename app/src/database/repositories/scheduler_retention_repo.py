"""T08-2: bounded, FK-safe terminal history retention (never commercial usage).

Separate transactions from delivery retention: discovery ends before canonical
locks; each candidate commits independently. The keyset cursor rotates past
blocked graphs, including unknown accounting, without new quarantine writes.
No savepoint and no external I/O. Existing outbox retention removes effects first.

S1: multipage sweeps with finite row/time budgets and a durable CAS checkpoint.
Checkpoint transactions never overlap lifecycle locks. Duplicate work by replicas
is safe/idempotent; stale CAS stops a replica, including across a sweep wrap (ABA).
Nonterminal observations are aggregated without taking lifecycle authority.

FU-1: bounded post-page draft probes feed an O(1), process-local sweep minimum.
Cursor/version discontinuity loses coverage, never turns it into an empty queue.
Heartbeat evaluates sweep progress independently from a potentially stuck pass.
"""
from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timedelta

from loguru import logger
from sqlalchemy import Select, delete, exists, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    ConversationTurn, GenerationAttempt, HandoffAttempt,
    HandoffContextAssignment, InboundEvent, LLMUsageReconciliation,
    OutboxDelivery, ScheduledHandoff, Sessao, TurnInputMembership, TurnRevision,
    SchedulerRetentionProgress,
)
from src.config import settings
from src.database import db_errors
from src.database.repositories import lifecycle_repo, scheduler_repo

SIGNAL_THRESHOLD = 1
_health: dict = {"available": False}
_counts: dict[str, int] = {}
# FU-1: process-local O(1) coverage. Never pretends a partial scan is global.
_coverage: DraftScan | None = None
_observing_since = time.monotonic()
_last_complete_scan_at: str | None = None
_last_complete_scan_monotonic: float | None = None
_sla_episodes: set[str] = set()
# P3-1: bounded diagnostic state, not a global poison-graph inventory.
CANDIDATE_REASONS = frozenset({
    "candidate_error", "foreign_key_reference", "transaction_lost", "nonterminal",
    "accounting_identity", "accounting_evidence", "unsent_effect", "lane_reference",
    "delivery_reference", "recent_accounting", "recent_identity", "receipt_identity",
    "receipt_reference", "context_reference",
})
_candidate_last_pass: dict | None = None
_candidate_last_failure: dict | None = None


def retention_counts() -> dict[str, int]:
    return dict(_counts)


def retention_health() -> dict:
    now = time.monotonic()
    health = {**_health, "stale": False, "candidate_failures": candidate_failure_health(),
              "last_complete_scan_at": _last_complete_scan_at,
              "last_complete_scan_age_seconds": (max(0.0, now - _last_complete_scan_monotonic)
                  if _last_complete_scan_monotonic is not None else None)}
    baseline = _last_complete_scan_monotonic if _last_complete_scan_monotonic is not None else _observing_since
    if now - baseline > settings.scheduler_retention_sweep_max_age_seconds:
        health.update(stale=True, available=False, draft_available=False)
    return health


def measurement_failure_count() -> int:
    return sum(_counts.get(key, 0) for key in (
        "health_unavailable", "draft_probe_failed", "checkpoint_unavailable",
        "checkpoint_failed", "discovery_failed"))  # P3-1: measurement only


def candidate_failure_health() -> dict:
    counts = {reason: _counts[reason] for reason in sorted(CANDIDATE_REASONS) if _counts.get(reason)}
    last = _candidate_last_failure
    terminal = last["oldest_terminal_at"] if last is not None else None
    return {"count": sum(counts.values()), "counts": counts, "error_threshold": SIGNAL_THRESHOLD,
            "last_pass": dict(_candidate_last_pass) if _candidate_last_pass is not None else None,
            "last_failure_observed_at": last["observed_at"] if last else None,
            "last_failure_count": last["count"] if last else None,
            "last_failure_reasons": dict(last["reasons"]) if last else None,
            "oldest_terminal_at": terminal,
            "oldest_terminal_age_seconds": max(0.0, (datetime.utcnow() - datetime.fromisoformat(terminal)).total_seconds())
                if terminal is not None else None}


@dataclass
class CandidatePass:
    reasons: dict[str, int] = field(default_factory=dict)
    oldest: datetime | None = None
    clean_sweep: bool = False

    def observe(self, reason: str | None, terminal_at: datetime | None) -> None:
        if reason is None:
            return
        self.reasons[reason] = self.reasons.get(reason, 0) + 1
        if terminal_at is not None and (self.oldest is None or terminal_at < self.oldest):
            self.oldest = terminal_at


def observe_watchdog() -> None:
    # Called by heartbeat, not by the potentially stalled retention task.
    baseline = _last_complete_scan_monotonic if _last_complete_scan_monotonic is not None else _observing_since
    overdue = time.monotonic() - baseline > settings.scheduler_retention_sweep_max_age_seconds
    reason = "sweep_completion_overdue"
    if overdue and reason not in _sla_episodes:
        _signal(reason)
        _sla_episodes.add(reason)
    elif not overdue:
        _sla_episodes.discard(reason)


def _sla_episode(key: str, exceeded: bool | None) -> None:
    reason = f"{key}_sla_exceeded"
    if exceeded and reason not in _sla_episodes:
        _signal(f"{key}_sla_exceeded")
        _sla_episodes.add(reason)
    elif exceeded is False:
        _sla_episodes.discard(reason)


def _signal(reason: str) -> None:
    _counts[reason] = _counts.get(reason, 0) + 1
    if reason in {"discovery_changed", "checkpoint_contended", "budget_exhausted"}:
        return  # R2-S1: expected concurrent progress, not retained evidence
    logger.error("Scheduler retention retained graph | reason={} count={} threshold={}",
                 reason, _counts[reason], SIGNAL_THRESHOLD)


@dataclass
class DraftScan:
    started_at: str
    cursor: int
    version: int
    prefix_known: bool
    scanned: int = 0
    start_cursor: int = 0
    start_version: int = 0
    oldest: datetime | None = None
    complete: bool = False
    candidate_failed: bool = False  # P3-1: carried only with cursor+version continuity


def _scan_for(cursor: int, version: int) -> DraftScan:
    if _coverage is not None and (_coverage.cursor, _coverage.version) == (cursor, version):
        return replace(_coverage)
    return DraftScan(datetime.utcnow().isoformat(), cursor, version, cursor == 0,
                     start_cursor=cursor, start_version=version)


@dataclass
class RetentionResult:
    scanned: int = 0
    examined: int = 0
    drafts_scrubbed: int = 0
    turns_purged: int = 0
    revisions_purged: int = 0
    attempts_purged: int = 0
    memberships_purged: int = 0
    assignments_purged: int = 0
    scheduled_handoffs_purged: int = 0
    receipts_purged: int = 0
    retained: int = 0
    nonterminal_observed: int = 0
    budget_exhausted: int = 0
    concurrent_progress: int = 0  # R2-S1: disappeared under a winning replica
    retained_reason: str | None = None  # P3-1: diagnostic, not an additive counter

    def add(self, other: RetentionResult) -> None:
        for field in fields(self):
            if field.name != "retained_reason":
                setattr(self, field.name, getattr(self, field.name) + getattr(other, field.name))


def discovery_query(after_id: int, limit: int):
    # PRIMARY range/order with LIMIT: at most ``limit`` rows returned per page. No
    # new index or DDL. The draft probe uses the revision turn_id index.
    has_draft = exists().where(TurnRevision.turn_id == ConversationTurn.id,
                               TurnRevision.draft_text.is_not(None))
    return (select(ConversationTurn.id, ConversationTurn.agente_id, ConversationTurn.sessao_id,
                   ConversationTurn.created_at, ConversationTurn.session_slot,
                   ConversationTurn.terminal_at, has_draft.label('has_draft'))
            .where(ConversationTurn.id > after_id)
            .order_by(ConversationTurn.id).limit(limit))


def _actionable(row, *, before: datetime, audit_cutoff: datetime) -> bool:
    if row.session_slot != "terminal":
        return False  # S1: no lifecycle lock for observational nonterminal signal
    if row.terminal_at >= before:
        return False
    return bool(row.has_draft) or row.terminal_at < audit_cutoff


async def _lock(db: AsyncSession, model, *criteria, key=None) -> list:
    query = select(model)
    if model is ConversationTurn:  # FU-2: all-turn session graph, RC at caller.
        query = query.with_hint(model, "FORCE INDEX (idx_turn_session_status)", "mysql")
    return list((await db.execute(query.where(*criteria)
        .order_by(key if key is not None else model.id).with_for_update()
        .execution_options(populate_existing=True))).scalars())


def _old(value: datetime | None, cutoff: datetime) -> bool:
    return value is not None and value < cutoff


def _known(attempt: GenerationAttempt) -> bool:
    return (attempt.outcome in {"succeeded", "failed"}
            and not (attempt.accounting_state == "reconciliation_required"
                     and attempt.reconciled_at is None)
            and (attempt.outcome != "succeeded" or attempt.consumo_llm_id is not None))


async def _retain_one(db: AsyncSession, *, turn_id: int, agente_id: int,
                      sessao_id: int, payload_days: int, receipt_days: int) -> RetentionResult:
    result = RetentionResult(examined=1)

    def retain(reason: str) -> RetentionResult:
        result.retained = int(reason != "discovery_changed")
        result.concurrent_progress = int(reason == "discovery_changed")
        result.retained_reason = reason if result.retained else None
        _signal(reason)
        return result

    # No consistent read before the entire canonical authority prefix.
    prefix = await lifecycle_repo.lock_lifecycle_prefix(db, agente_id=agente_id)
    sessions = await _lock(db, Sessao, Sessao.id == sessao_id, Sessao.agente_id == agente_id)
    if prefix.agent is None or not sessions:
        return retain("discovery_changed")
    turns = await _lock(db, ConversationTurn, ConversationTurn.sessao_id == sessao_id)
    turn_map = {turn.id: turn for turn in turns}
    turn = turn_map.get(turn_id)
    if turn is None:
        return retain("discovery_changed")
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    payload_cutoff = now - timedelta(days=payload_days)
    audit_cutoff = now - timedelta(days=receipt_days)
    if turn.session_slot != "terminal":
        return retain("nonterminal")
    if not _old(turn.terminal_at, payload_cutoff):
        return result
    turn_ids = list(turn_map)
    revisions = await _lock(db, TurnRevision, TurnRevision.turn_id.in_(turn_ids))
    attempts = await _lock(db, GenerationAttempt, or_(
        GenerationAttempt.sessao_id == sessao_id, GenerationAttempt.turn_id.in_(turn_ids),
        GenerationAttempt.revision_id.in_([r.id for r in revisions])))
    scheduled = await _lock(db, ScheduledHandoff, ScheduledHandoff.sessao_id == sessao_id,
                            key=ScheduledHandoff.handoff_attempt_id)
    await _lock(db, HandoffAttempt, HandoffAttempt.sessao_id == sessao_id)
    effects = await _lock(db, OutboxDelivery, OutboxDelivery.sessao_id == sessao_id)
    # These are identity children, acquired only after lifecycle authority.
    memberships = await _lock(db, TurnInputMembership, TurnInputMembership.turn_id == turn_id)
    assignments = await _lock(db, HandoffContextAssignment,
                              HandoffContextAssignment.sessao_id == sessao_id,
                              key=HandoffContextAssignment.turn_input_membership_id)
    receipts = await _lock(db, InboundEvent,
                           InboundEvent.id.in_([m.inbound_event_id for m in memberships]))
    # Existing accounting-only writers take agent S/session S before attempt X;
    # the prefix above excludes late completions before these decisions.
    reconciliation = await _lock(db, LLMUsageReconciliation,
        LLMUsageReconciliation.agente_id == agente_id,
        or_(LLMUsageReconciliation.sessao_id == sessao_id,
            LLMUsageReconciliation.sessao_id.is_(None)))
    ours = [r for r in revisions if r.turn_id == turn_id]
    our_attempts = [a for a in attempts if a.turn_id == turn_id]
    if any(a.agente_id != agente_id or a.sessao_id != sessao_id
           or a.turn_id not in turn_map
           or not any(r.id == a.revision_id and r.turn_id == a.turn_id for r in revisions)
           for a in attempts):
        return retain("accounting_identity")
    our_scheduled = [s for s in scheduled if s.conversation_turn_id == turn_id]
    handoff_ids = {s.handoff_attempt_id for s in our_scheduled}
    revision_ids = {r.id for r in ours}
    linked = [e for e in effects if e.conversation_turn_id == turn_id
              or e.turn_revision_id in revision_ids or e.handoff_attempt_id in handoff_ids]
    if any(not _known(a) for a in our_attempts) or any(r.status != "resolved" for r in reconciliation):
        return retain("accounting_evidence")
    if any(e.status not in {"delivered", "dead", "cancelled"} for e in linked):
        return retain("unsent_effect")
    if prefix.lane is not None and (prefix.lane.active_turn_id == turn_id
                                    or prefix.lane.active_revision_id in revision_ids):
        return retain("lane_reference")
    # Text in a terminal revision is no longer a draft source. Preserve bytes,
    # timing, model/usage identities and every lead/assistant message.
    for revision in ours:
        if revision.draft_text is not None:
            revision.draft_text = None
            result.drafts_scrubbed += 1
    await db.flush()
    if not _old(turn.terminal_at, audit_cutoff):
        return result
    if linked:
        return retain("delivery_reference")
    if any(not _old(a.outcome_recorded_at, audit_cutoff)
           or (a.reconciled_at is not None and not _old(a.reconciled_at, audit_cutoff))
           or (a.accounting_drain_until is not None and a.accounting_drain_until >= now)
           for a in our_attempts):
        return retain("recent_accounting")
    if (any(not _old(r.created_at, audit_cutoff) for r in ours + our_scheduled + memberships)
        or any(r.generation_completed_at is not None
               and not _old(r.generation_completed_at, audit_cutoff) for r in ours)):
        return retain("recent_identity")
    receipt_map = {r.id: r for r in receipts}
    if any(m.inbound_event_id not in receipt_map
           or receipt_map[m.inbound_event_id].receipt_disposition != "scheduled"
           or receipt_map[m.inbound_event_id].mensagem_id != m.mensagem_id
           or receipt_map[m.inbound_event_id].sessao_id != sessao_id
           or receipt_map[m.inbound_event_id].agente_id != agente_id
           or not _old(receipt_map[m.inbound_event_id].received_at, audit_cutoff)
           for m in memberships):
        return retain("receipt_identity")
    if any(e.inbound_event_id in receipt_map for e in effects):
        return retain("receipt_reference")
    member_ids = {m.id for m in memberships}
    our_assignments = [a for a in assignments if a.turn_input_membership_id in member_ids]
    scheduled_map = {s.handoff_attempt_id: s for s in scheduled}
    for assignment in our_assignments:
        parent = scheduled_map.get(assignment.handoff_attempt_id)
        parent_turn = turn_map.get(parent.conversation_turn_id) if parent else None
        if (not _old(assignment.created_at, audit_cutoff)
            or any(e.handoff_attempt_id == assignment.handoff_attempt_id for e in effects)
            or (parent is not None and (parent_turn is None
                or parent_turn.session_slot != "terminal"
                or not _old(parent_turn.terminal_at, audit_cutoff)
                or any(not _known(a) for a in attempts
                       if a.turn_id == parent_turn.id)))):
            return retain("context_reference")
    # SQL DELETEs deliberately do not invoke ORM cascades. All RESTRICT edges
    # stay enforced. Clear the nullable back-links to break turn/revision cycles;
    # delete replacement revision before predecessor (max two revisions).
    for assignment in our_assignments:
        await db.execute(delete(HandoffContextAssignment).where(
            HandoffContextAssignment.turn_input_membership_id == assignment.turn_input_membership_id))
    for scheduled_handoff in our_scheduled:
        await db.execute(delete(ScheduledHandoff).where(
            ScheduledHandoff.handoff_attempt_id == scheduled_handoff.handoff_attempt_id))
    await db.execute(delete(TurnInputMembership).where(TurnInputMembership.turn_id == turn_id))
    await db.execute(delete(GenerationAttempt).where(GenerationAttempt.turn_id == turn_id))
    await db.execute(update(ConversationTurn).where(ConversationTurn.id == turn_id)
                     .values(current_revision_id=None, final_revision_id=None))
    for revision in sorted(ours, key=lambda r: r.revision_number, reverse=True):
        await db.execute(delete(TurnRevision).where(TurnRevision.id == revision.id))
    await db.execute(delete(ConversationTurn).where(ConversationTurn.id == turn_id))
    # Receipt is last: its immutable scheduled disposition is never rewritten.
    # The mensagem_id association disappears with the receipt, not the message.
    for receipt in receipts:
        await db.execute(delete(InboundEvent).where(InboundEvent.id == receipt.id))
    result.turns_purged = 1
    result.revisions_purged = len(ours)
    result.attempts_purged = len(our_attempts)
    result.memberships_purged = len(memberships)
    result.assignments_purged = len(our_assignments)
    result.scheduled_handoffs_purged = len(our_scheduled)
    result.receipts_purged = len(receipts)
    return result


async def _checkpoint(session_factory, *, version: int, cursor: int) -> bool:
    # S1: short standalone CAS, no graph locks; version prevents ABA on wrap.
    async with session_factory() as db:
        changed = await db.execute(update(SchedulerRetentionProgress).where(
            SchedulerRetentionProgress.id == 1,
            SchedulerRetentionProgress.version == version,
        ).values(last_turn_id=cursor, version=version + 1))
        await db.commit()
        return changed.rowcount == 1


def health_query(*, draft: bool, turn_ids: tuple[int, ...] = ()) -> Select:
    # FU-1: purge retains the O(1) ordered age index. Draft may ONLY inspect
    # exact IDs from a page already traversed; never search scrubbed history.
    if draft:
        if not turn_ids or len(turn_ids) > 1000:
            raise ValueError("draft health requires 1..1000 observed turn IDs")
        return (select(ConversationTurn.terminal_at).select_from(TurnRevision)
            .join(ConversationTurn, ConversationTurn.id == TurnRevision.turn_id)
            .with_hint(TurnRevision, "FORCE INDEX (uq_turn_revision_number)", dialect_name="mysql")
            .with_hint(ConversationTurn, "FORCE INDEX (PRIMARY)", dialect_name="mysql")
            .prefix_with("/*+ MAX_EXECUTION_TIME(2000) */", dialect="mysql")
            .where(TurnRevision.turn_id.in_(turn_ids), TurnRevision.draft_text.is_not(None),
                   ConversationTurn.session_slot == "terminal")
            .order_by(ConversationTurn.terminal_at).limit(1))
    return (select(ConversationTurn.terminal_at)
        .prefix_with("/*+ MAX_EXECUTION_TIME(2000) */", dialect="mysql")
        .with_hint(ConversationTurn, "FORCE INDEX (idx_turn_retention_terminal)", dialect_name="mysql")
        .where(ConversationTurn.session_slot == "terminal")
        .order_by(ConversationTurn.terminal_at).limit(1))


async def _draft_probe(session_factory: Callable[[], AsyncSession], turn_ids: tuple[int, ...]) -> datetime | None:
    if not turn_ids:
        return None
    async with session_factory() as db:
        return (await db.execute(health_query(draft=True, turn_ids=turn_ids))).scalar_one_or_none()


def _invalidate_draft_coverage(scan: DraftScan) -> None:
    # FU-1: losing a cursor race must not keep a previously complete draft
    # observation looking current. No query, failure counter or ERROR here.
    global _coverage
    _coverage = None
    _health.update(draft_available=False, draft_scan_complete=False,
                   reason="coverage_incomplete", oldest_draft_terminal_age_seconds=None,
                   oldest_observed_draft_age_seconds=None,
                   draft_sla_exceeded=True if "draft_sla_exceeded" in _sla_episodes else None,
                   scan_started_at=scan.started_at, scan_start_cursor=scan.start_cursor,
                   scan_start_version=scan.start_version, scan_cursor=scan.cursor,
                   scan_version=scan.version, scan_rows=scan.scanned,
                   scan_prefix_known=False, scan_continuity_lost=True)


async def _progress_version(session_factory) -> int:
    # Own short read view, closed before lifecycle authority. No S→X upgrade.
    async with session_factory() as db:
        return (await db.execute(select(SchedulerRetentionProgress.version).where(
            SchedulerRetentionProgress.id == 1))).scalar_one()


async def _observe_health(session_factory: Callable[[], AsyncSession], *, payload_days: int,
                          receipt_days: int, tolerance_seconds: int,
                          draft_scan: DraftScan | None = None) -> None:
    """FU-1: bounded per-page draft observations; no unbounded second scan.

    Incomplete coverage is informational, not a measurement failure. A partial
    observation can prove a violation, but cannot prove no global violation.
    """
    global _health
    try:
        async with asyncio.timeout(3):
            async with session_factory() as db:
                now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
                oldest_purge = (await db.execute(health_query(draft=False).where(
                    ConversationTurn.terminal_at < now - timedelta(days=receipt_days)))).scalar_one_or_none()
        complete = draft_scan is not None and draft_scan.complete
        _health = {"available": True, "observed_at": now.isoformat(),
                   "sla_tolerance_seconds": tolerance_seconds, "draft_available": complete,
                   "reason": None if complete else "coverage_incomplete",
                   "scan_started_at": draft_scan.started_at if draft_scan else None,
                   "scan_cursor": draft_scan.cursor if draft_scan else None,
                   "scan_start_cursor": draft_scan.start_cursor if draft_scan else None,
                   "scan_start_version": draft_scan.start_version if draft_scan else None,
                   "scan_prefix_known": draft_scan.prefix_known if draft_scan else False,
                   "scan_version": draft_scan.version if draft_scan else None,
                   "scan_rows": draft_scan.scanned if draft_scan else 0,
                   "draft_scan_complete": complete}
        observed_draft = draft_scan.oldest if draft_scan else None
        _health["oldest_observed_draft_age_seconds"] = ((now - observed_draft).total_seconds()
                                                         if observed_draft is not None else None)
        for key, value, days in (("draft", observed_draft, payload_days),
                                 ("purge", oldest_purge, receipt_days)):
            age = max(0.0, (now - value).total_seconds()) if value is not None else None
            exceeded = age is not None and age > days * 86400 + tolerance_seconds
            known = key != "draft" or complete
            _health[f"oldest_{key}_terminal_age_seconds"] = age if known else None
            _health[f"{key}_sla_exceeded"] = exceeded if known or exceeded else None
            _sla_episode(key, exceeded if known or exceeded else None)
    except Exception as exc:
        _health = {"available": False, "reason": "measurement_failed"}
        logger.error("Scheduler retention health unavailable | erro_tipo={}", type(exc).__name__)
        _signal("health_unavailable")


async def _cleanup_retention(session_factory: Callable[[], AsyncSession], *,
                             payload_days: int = 7, receipt_days: int = 90,
                             limit: int = 100, max_rows: int | None = None,
                             max_seconds: float | None = None,
                             candidate_pass: CandidatePass) -> RetentionResult:
    # P3-2: observations are process-local, including a losing replica's page.
    # One synchronous finalizer covers returns, breaks, exceptions and cancellation
    # without changing checkpoint/coverage authority or swallowing any exception.
    result = RetentionResult()
    try:
        return await _cleanup_retention_pages(session_factory, payload_days=payload_days,
            receipt_days=receipt_days, limit=limit, max_rows=max_rows,
            max_seconds=max_seconds, candidate_pass=candidate_pass, result=result)
    finally:
        if result.nonterminal_observed:
            _counts["nonterminal"] = _counts.get("nonterminal", 0) + result.nonterminal_observed
            logger.error("Scheduler retention nonterminal aggregate | observed={} count={} threshold={}",
                         result.nonterminal_observed, _counts["nonterminal"], SIGNAL_THRESHOLD)


async def _cleanup_retention_pages(session_factory: Callable[[], AsyncSession], *,
                            payload_days: int = 7, receipt_days: int = 90,
                            limit: int = 100, max_rows: int | None = None,
                            max_seconds: float | None = None,
                            candidate_pass: CandidatePass,
                            result: RetentionResult) -> RetentionResult:
    """S1: iterate pages; hard work deadline, then bounded checkpoint + health.

    A completed pass advances durable progress even if every candidate is held.
    Restarts after every pass therefore cost no extra prefix scan. A crash before
    checkpoint can replay at most one page. No progress guarantee is possible if
    every process dies before its first checkpoint or a DB is continuously down.
    """
    global _coverage, _last_complete_scan_at, _last_complete_scan_monotonic
    max_rows = settings.scheduler_retention_max_rows if max_rows is None else max_rows
    max_seconds = settings.scheduler_retention_max_seconds if max_seconds is None else max_seconds
    if (type(limit) is not int or not 1 <= limit <= 1000
        or type(max_rows) is not int or not 1 <= max_rows <= 1000000
        or not math.isfinite(max_seconds) or not 0.1 <= max_seconds <= 300
        or payload_days < 7 or receipt_days < 90):
        raise ValueError("retention requires limit 1..1000, rows 1..1000000, seconds 0.1..300, payload >=7d, audit >=90d")
    deadline = time.monotonic() + max_seconds
    # Discovery and cursor reads close before ANY lifecycle lock.
    try:
        async with asyncio.timeout_at(deadline):
            async with session_factory() as db:
                progress = (await db.execute(select(SchedulerRetentionProgress).where(
                    SchedulerRetentionProgress.id == 1))).scalar_one()
                cursor, version = progress.last_turn_id, progress.version
    except Exception as exc:
        # S1: no checkpoint authority → no candidate work; never crash the worker.
        logger.error("Scheduler retention checkpoint unavailable | erro_tipo={}", type(exc).__name__)
        _signal("checkpoint_unavailable")
        return result
    scan = _scan_for(cursor, version)
    while result.scanned < max_rows and time.monotonic() < deadline:
        page_size = min(limit, max_rows - result.scanned)
        next_cursor = cursor
        end = False
        timed_out = False
        probe_failed = False
        try:
            async with asyncio.timeout_at(deadline):
                async with session_factory() as discovery:
                    now = (await discovery.execute(select(func.utc_timestamp(6)))).scalar_one()
                    before = now - timedelta(days=payload_days)
                    rows = list((await discovery.execute(discovery_query(cursor, page_size))).all())
                    # Empty/young initial page after a saved cursor wraps once.
                    if cursor and not scan.prefix_known and result.scanned == 0 and (not rows or rows[0].created_at >= before):
                        cursor = 0
                        scan = _scan_for(0, version)
                        rows = list((await discovery.execute(discovery_query(0, page_size))).all())
                    await discovery.rollback()
                audit_cutoff = now - timedelta(days=receipt_days)
                end = len(rows) < page_size
                for row in rows:
                    result.scanned += 1
                    next_cursor = row.id
                    if row.created_at >= before:
                        end = True
                        continue
                    if row.session_slot != "terminal":
                        result.nonterminal_observed += 1
                        candidate_pass.observe("nonterminal", row.terminal_at)
                        scan.candidate_failed = True
                        result.retained += 1
                        continue
                    if not _actionable(row, before=before, audit_cutoff=audit_cutoff):
                        continue
                    # A winning replica already advanced this page: leave its
                    # cursor untouched and do not publish a stale SLA observation.
                    if await _progress_version(session_factory) != version:
                        _signal("checkpoint_contended")
                        _invalidate_draft_coverage(scan)
                        return result
                    turn_id, agente_id, sessao_id = row.id, row.agente_id, row.sessao_id

                    async def operation(db: AsyncSession) -> RetentionResult:
                        # S1: isolation must be set on EVERY whole-UoW retry.
                        await scheduler_repo.begin_scheduler_control(db)
                        return await _retain_one(db, turn_id=turn_id, agente_id=agente_id,
                            sessao_id=sessao_id, payload_days=payload_days, receipt_days=receipt_days)
                    try:
                        item = await lifecycle_repo.run_in_new_session(session_factory, operation)
                    except Exception as exc:
                        codes = set(db_errors.mysql_error_codes(exc))
                        if isinstance(exc, IntegrityError) and codes & {1451, 1217}:
                            reason = "foreign_key_reference"
                            _signal("foreign_key_reference")
                        elif db_errors.transaction_lost(exc):
                            reason = "transaction_lost"
                            _signal("transaction_lost")
                        else:
                            reason = "candidate_error"
                            logger.error("Scheduler retention candidate failed | erro_tipo={}", type(exc).__name__)
                            _signal("candidate_error")
                        item = RetentionResult(examined=1, retained=1, retained_reason=reason)
                    candidate_pass.observe(item.retained_reason, row.terminal_at)
                    scan.candidate_failed |= item.retained_reason is not None
                    result.add(item)
                # FU-1: one indexed probe per bounded page, AFTER its commits.
                try:
                    observed = await _draft_probe(session_factory, tuple(row.id for row in rows))
                    scan.scanned += len(rows)
                    if observed is not None and (scan.oldest is None or observed < scan.oldest):
                        scan.oldest = observed
                except Exception as exc:
                    logger.error("Scheduler retention draft probe failed | erro_tipo={}", type(exc).__name__)
                    _signal("draft_probe_failed")
                    probe_failed = True
        except TimeoutError:
            # Cancellation unwinds/rolls back the candidate before checkpoint.
            # Move past that candidate; revisit next sweep, do not pin the queue.
            timed_out = True
            end = False
        except Exception as exc:
            logger.error("Scheduler retention discovery failed | erro_tipo={}", type(exc).__name__)
            _signal("discovery_failed")
            return result  # discovery failed: never checkpoint an incomplete page
        next_cursor = 0 if end else next_cursor
        try:
            async with asyncio.timeout(3):
                saved = await _checkpoint(session_factory, version=version, cursor=next_cursor)
        except Exception as exc:
            # S1: progress not durable; next pass repeats at most this page.
            logger.error("Scheduler retention checkpoint failed | erro_tipo={}", type(exc).__name__)
            _signal("checkpoint_failed")
            break
        if not saved:
            _signal("checkpoint_contended")
            _invalidate_draft_coverage(scan)
            return result  # R2-S1: no stale SLA from a losing sweep
        cursor, version = next_cursor, version + 1
        if timed_out or probe_failed:
            # Cursor still advances, but no unobserved page becomes a proof.
            scan = _scan_for(cursor, -1)
            scan.prefix_known = False
        scan.cursor, scan.version = cursor, version
        _coverage = replace(scan)
        if not result.concurrent_progress and scan.oldest is not None:
            _sla_episode("draft", (now - scan.oldest).total_seconds() >
                         payload_days * 86400 + settings.scheduler_retention_sla_tolerance_seconds)
        if end:
            if scan.prefix_known and not result.concurrent_progress:
                scan.complete = True
                _last_complete_scan_at = now.isoformat()
                _last_complete_scan_monotonic = time.monotonic()
                candidate_pass.clean_sweep = not scan.candidate_failed
            _coverage = None
        if end:
            break
        if timed_out or result.scanned >= max_rows or time.monotonic() >= deadline:
            result.budget_exhausted = 1
            _signal("budget_exhausted")
            break
    if result.concurrent_progress:
        _invalidate_draft_coverage(scan)
    if not result.concurrent_progress:
        await _observe_health(session_factory, payload_days=payload_days, receipt_days=receipt_days,
                              tolerance_seconds=settings.scheduler_retention_sla_tolerance_seconds,
                              draft_scan=scan)
    return result


async def cleanup_retention(session_factory: Callable[[], AsyncSession], *,
                            payload_days: int = 7, receipt_days: int = 90,
                            limit: int = 100, max_rows: int | None = None,
                            max_seconds: float | None = None) -> RetentionResult:
    """P3-1: candidate diagnostics never masquerade as failed measurement.

    Publish even on a measurement failure; a partial empty pass cannot erase
    the last nonempty observation. No extra query, transaction or retained IDs.
    """
    global _candidate_last_pass, _candidate_last_failure
    summary = CandidatePass()
    before = measurement_failure_count()
    completed = False
    try:
        result = await _cleanup_retention(session_factory, payload_days=payload_days,
            receipt_days=receipt_days, limit=limit, max_rows=max_rows,
            max_seconds=max_seconds, candidate_pass=summary)
        completed = before == measurement_failure_count()
        return result
    finally:
        if before != measurement_failure_count():
            _health.update(available=False, reason="measurement_failed")
        _candidate_last_pass = {"observed_at": datetime.utcnow().isoformat(),
            "count": sum(summary.reasons.values()), "reasons": dict(summary.reasons),
            "oldest_terminal_at": summary.oldest.isoformat() if summary.oldest is not None else None,
            "measurement_available": completed}
        if summary.reasons:
            _candidate_last_failure = dict(_candidate_last_pass)
        elif summary.clean_sweep and completed:
            _candidate_last_failure = None
