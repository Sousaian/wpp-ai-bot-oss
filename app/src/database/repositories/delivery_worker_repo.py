from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import random
from typing import Literal
from uuid import uuid4

from loguru import logger

from sqlalchemy import and_, func, or_, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    ConversationTurn,
    HandoffContextAssignment,
    HandoffAttempt,
    OutboxDelivery,
    Sessao,
)
from src.database.repositories import handoff_attempt_repo, sessao_repo
from src.services.handoff_context import parse_coverage_id


TERMINAL_STATUSES = ("delivered", "dead", "cancelled")
ACTIVE_STATUSES = ("blocked", "pending", "processing", "retry")


@dataclass(frozen=True)
class ClaimedDelivery:
    id: int
    lease_token: str
    provider: str
    operation: str
    payload_version: int
    payload: dict[str, object]
    delivery_key: str
    agente_id: int | None
    sessao_id: int | None
    handoff_attempt_id: int | None
    attempt_count: int
    cutover_epoch: str
    delivery_class: str = "legacy_unknown"
    automated_effect: str | None = None
    conversation_turn_id: int | None = None
    turn_revision_id: int | None = None
    scheduler_epoch: str | None = None


@dataclass(frozen=True)
class MaintenanceResult:
    recovered: int
    ambiguity_counted: int
    cancelled: int
    expired: int
    dependency_terminal: int
    context_note_ambiguous: int = 0
    automated_skipped: int = 0


@dataclass(frozen=True)
class CleanupResult:
    payloads_scrubbed: int
    receipts_purged: int
    heartbeats_purged: int
    audits_purged: int
    deliveries_purged: int
    automated_nonterminal_skipped: int = 0
    abandon_intents_retained: int = 0


@dataclass(frozen=True)
class DeliveryMetadata:
    id: int
    delivery_key: str
    provider: str
    operation: str
    status: str
    attempt_count: int
    ambiguity_count: int
    agente_id: int | None
    sessao_id: int | None
    handoff_attempt_id: int | None
    next_attempt_at: datetime | None
    expires_at: datetime | None
    last_http_status: int | None
    last_error_code: str | None
    terminal_reason: str | None


@dataclass(frozen=True)
class HandoffDispatchOwnership:
    handoff_key: str
    inbox_id: int
    status: str


@dataclass(frozen=True)
class ContextNoteDispatchOwnership:
    handoff_key: str
    inbox_id: int
    contact_id: int
    conversation_id: int
    status: str


CLAIM_SQL = text(
    """
    SELECT d.id
    FROM outbox_deliveries AS d
    WHERE d.status IN ('pending', 'retry')
      AND d.cutover_epoch = :cutover_epoch
      AND d.next_attempt_at <= UTC_TIMESTAMP(6)
      AND d.expires_at > UTC_TIMESTAMP(6)
      AND d.cancel_requested_at IS NULL
      AND (
          d.delivery_class <> 'automated'
          OR EXISTS (
              SELECT 1
              FROM conversation_turns AS turn
              JOIN turn_revisions AS revision
                ON revision.id = d.turn_revision_id
               AND revision.turn_id = turn.id
               AND revision.scheduler_epoch = turn.scheduler_epoch
              JOIN agent_service_lanes AS lane
                ON lane.agente_id = turn.agente_id
               AND lane.scheduler_epoch = turn.scheduler_epoch
              WHERE turn.id = d.conversation_turn_id
                AND turn.status = 'awaiting_delivery'
                AND turn.session_slot = 'active'
                AND turn.final_revision_id = revision.id
                AND revision.status = 'admitted'
                AND revision.sealed_first_input_sequence IS NOT NULL
                AND revision.sealed_last_input_sequence IS NOT NULL
                AND lane.owner_token = turn.claim_token
                AND lane.active_turn_id = turn.id
                AND lane.active_revision_id = revision.id
                AND lane.lease_until > UTC_TIMESTAMP(6)
                AND turn.claim_until > UTC_TIMESTAMP(6)
          )
      )
      AND NOT EXISTS (
          SELECT 1
          FROM outbox_deliveries AS p FORCE INDEX (idx_outbox_predecessor)
          WHERE p.ordering_key = d.ordering_key
            AND p.stream_sequence < d.stream_sequence
            AND p.status IN ('blocked', 'pending', 'processing', 'retry')
      )
    ORDER BY d.priority DESC, d.next_attempt_at, d.id
    LIMIT :available_slots
    FOR UPDATE SKIP LOCKED
    """
)


def _claimed(row: OutboxDelivery) -> ClaimedDelivery:
    return ClaimedDelivery(
        id=row.id,
        lease_token=str(row.lease_token),
        provider=row.provider,
        operation=row.operation,
        payload_version=row.payload_version,
        payload=dict(row.payload),
        delivery_key=row.delivery_key,
        agente_id=row.agente_id,
        sessao_id=row.sessao_id,
        handoff_attempt_id=row.handoff_attempt_id,
        delivery_class=row.delivery_class,
        automated_effect=row.automated_effect,
        conversation_turn_id=row.conversation_turn_id,
        turn_revision_id=row.turn_revision_id,
        scheduler_epoch=row.scheduler_epoch,
        attempt_count=row.attempt_count,
        cutover_epoch=row.cutover_epoch,
    )


class ClaimRetryExhausted(RuntimeError):
    """A bounded series of whole-transaction claim retries was exhausted."""


def _mysql_error_code(exc: BaseException) -> int | None:
    current: BaseException | None = exc
    while current is not None:
        args = getattr(current, "args", ())
        if args and isinstance(args[0], int):
            return args[0]
        current = getattr(current, "orig", None)
    return None


def _is_retryable_claim_error(exc: BaseException) -> bool:
    return isinstance(exc, DBAPIError) and _mysql_error_code(exc) in {1205, 1213}


async def _lock_runtime_authority(db: AsyncSession, cutover_epoch: str) -> bool:
    """Lock the global gate before any lifecycle row.

    Runtime mutations use this order everywhere:
    delivery_runtime_state -> sessao -> handoff_attempt -> outbox_delivery.
    Maintenance and operator paths never acquire the runtime gate after a
    lifecycle lock, so a freeze cannot introduce the inverse edge.
    """

    from src.database.repositories import delivery_state_repo

    state = await delivery_state_repo.get_runtime_state(db, for_share=True)
    return state.mode == "outbox" and state.cutover_epoch == cutover_epoch


async def run_preclaim_maintenance(
    db: AsyncSession,
    *,
    context_note_discovery_limit: int = 1000,
) -> MaintenanceResult:
    """Recover leases and terminalize stale/dependent work using database UTC."""

    if not 1 <= context_note_discovery_limit <= 1000:
        raise ValueError("context-note discovery limit must be between 1 and 1000")

    cancelled_chatwoot_ids = list(
        (
            await db.execute(
                select(OutboxDelivery.id)
                .where(
                    OutboxDelivery.provider == "chatwoot",
                    OutboxDelivery.status == "processing",
                    OutboxDelivery.lock_until <= func.utc_timestamp(6),
                    OutboxDelivery.cancel_requested_at.is_not(None),
                )
                .order_by(OutboxDelivery.id)
                .limit(1000)
            )
        ).scalars()
    )
    expired_chatwoot_ids = list(
        (
            await db.execute(
                select(OutboxDelivery.id)
                .where(
                    OutboxDelivery.provider == "chatwoot",
                    OutboxDelivery.status.in_(("pending", "retry", "processing")),
                    OutboxDelivery.expires_at <= func.utc_timestamp(6),
                    (
                        (OutboxDelivery.status != "processing")
                        | (OutboxDelivery.lock_until <= func.utc_timestamp(6))
                    ),
                )
                .order_by(OutboxDelivery.id)
                .limit(1000)
            )
        ).scalars()
    )
    context_note_ambiguous = 0
    cancelled_chatwoot = 0
    for delivery_id in cancelled_chatwoot_ids:
        session, attempt, delivery = await _lock_context(db, delivery_id)
        if (
            delivery is None
            or delivery.status != "processing"
            or delivery.cancel_requested_at is None
            or delivery.lock_until is None
        ):
            continue
        now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        if delivery.lock_until > now:
            continue
        delivery.status = "cancelled"
        delivery.terminal_at = now
        delivery.terminal_reason = "cancel_requested"
        delivery.last_error_code = "lease_expired_after_cancel"
        if delivery.operation == CONTEXT_NOTE_OPERATION:
            # The lost lease may have posted: same rule as any lost note ack.
            delivery.ambiguity_count += 1
            delivery.terminal_reason = "context_note_ambiguous"
            context_note_ambiguous += 1
        _clear_lease(delivery)
        if (
            delivery.operation == "chatwoot.open_handoff.v1"
            and session is not None
            and attempt is not None
            and attempt.status == "pending"
        ):
            attempt.status = "cancelled"
            attempt.terminal_at = now
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status == "blocked",
                OutboxDelivery.delivery_class != "automated",
            )
            .values(
                status="cancelled",
                terminal_at=now,
                terminal_reason="dependency_cancelled",
                last_error_code="dependency_cancelled",
            )
        )
        cancelled_chatwoot += 1

    expired_chatwoot = 0
    for delivery_id in expired_chatwoot_ids:
        session, attempt, delivery = await _lock_context(db, delivery_id)
        if (
            delivery is None
            or delivery.status not in {"pending", "retry", "processing"}
            or delivery.expires_at is None
        ):
            continue
        now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        if (
            delivery.expires_at > now
            or (
                delivery.status == "processing"
                and (delivery.lock_until is None or delivery.lock_until > now)
            )
        ):
            continue
        was_processing = delivery.status == "processing"
        delivery.status = "dead"
        delivery.terminal_at = now
        delivery.terminal_reason = "expired"
        delivery.last_error_code = "expired"
        if delivery.operation == CONTEXT_NOTE_OPERATION and was_processing:
            delivery.ambiguity_count += 1
            delivery.terminal_reason = "context_note_ambiguous"
            context_note_ambiguous += 1
        _clear_lease(delivery)
        if (
            delivery.operation == "chatwoot.open_handoff.v1"
            and session is not None
            and attempt is not None
            and attempt.status == "pending"
        ):
            attempt.status = "failed"
            attempt.terminal_at = now
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status == "blocked",
                OutboxDelivery.delivery_class != "automated",
            )
            .values(
                status="dead",
                terminal_at=now,
                terminal_reason="dependency_failed",
                last_error_code="dependency_failed",
            )
        )
        expired_chatwoot += 1

    ambiguous_note_ids = list(
        (
            await db.execute(
                select(OutboxDelivery.id)
                .where(
                    OutboxDelivery.operation == "chatwoot.add_private_note.v1",
                    OutboxDelivery.status == "processing",
                    OutboxDelivery.lock_until <= func.utc_timestamp(6),
                    OutboxDelivery.cancel_requested_at.is_(None),
                )
                .order_by(OutboxDelivery.id)
                .limit(context_note_discovery_limit)
            )
        ).scalars()
    )
    for delivery_id in ambiguous_note_ids:
        _, _, delivery = await _lock_context(db, delivery_id)
        if (
            delivery is None
            or delivery.operation != "chatwoot.add_private_note.v1"
            or delivery.status != "processing"
            or delivery.lock_until is None
        ):
            continue
        now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
        if delivery.lock_until > now:
            continue
        delivery.status = "dead"
        delivery.ambiguity_count += 1
        delivery.terminal_at = now
        delivery.terminal_reason = "context_note_ambiguous"
        delivery.last_error_code = "lease_expired_ambiguous"
        _clear_lease(delivery)
        context_note_ambiguous += 1

    ambiguity_count = int(
        (
            await db.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM outbox_deliveries
                    WHERE status = 'processing'
                      AND lock_until <= UTC_TIMESTAMP(6)
                      AND cancel_requested_at IS NULL
                      AND provider = 'evolution'
                      AND delivery_class <> 'automated'
                    """
                )
            )
        ).scalar_one()
        or 0
    )
    cancelled_result = await db.execute(
        text(
            """
            UPDATE outbox_deliveries
            SET status = 'cancelled', lease_token = NULL, lock_owner = NULL,
                lock_until = NULL, terminal_at = UTC_TIMESTAMP(6),
                terminal_reason = 'cancel_requested',
                last_error_code = 'lease_expired_after_cancel'
            WHERE status = 'processing'
              AND lock_until <= UTC_TIMESTAMP(6)
              AND cancel_requested_at IS NOT NULL
              AND provider <> 'chatwoot'
              AND delivery_class <> 'automated'
            """
        )
    )
    recovery_result = await db.execute(
        text(
            """
            UPDATE outbox_deliveries
            SET status = 'retry', lease_token = NULL, lock_owner = NULL,
                lock_until = NULL, next_attempt_at = UTC_TIMESTAMP(6),
                ambiguity_count = ambiguity_count +
                    CASE WHEN provider = 'evolution' THEN 1 ELSE 0 END,
                last_error_code = 'lease_expired_ambiguous'
            WHERE status = 'processing'
              AND lock_until <= UTC_TIMESTAMP(6)
              AND cancel_requested_at IS NULL
              AND operation <> 'chatwoot.add_private_note.v1'
              AND delivery_class <> 'automated'
            """
        )
    )
    expired_result = await db.execute(
        text(
            """
            UPDATE outbox_deliveries
            SET status = 'dead', terminal_at = UTC_TIMESTAMP(6),
                terminal_reason = 'expired', last_error_code = 'expired'
            WHERE status IN ('pending', 'retry')
              AND expires_at <= UTC_TIMESTAMP(6)
              AND provider <> 'chatwoot'
              AND delivery_class <> 'automated'
            """
        )
    )
    dependency_result = await db.execute(
        text(
            """
            UPDATE outbox_deliveries AS child
            JOIN outbox_deliveries AS parent
              ON parent.id = child.depends_on_delivery_id
            SET child.status = CASE
                    WHEN parent.status = 'cancelled' THEN 'cancelled'
                    ELSE 'dead'
                END,
                child.terminal_at = UTC_TIMESTAMP(6),
                child.terminal_reason = CASE
                    WHEN parent.status = 'cancelled' THEN 'dependency_cancelled'
                    ELSE 'dependency_failed'
                END,
                child.last_error_code = CASE
                    WHEN parent.status = 'cancelled' THEN 'dependency_cancelled'
                    ELSE 'dependency_failed'
                END
            WHERE child.status = 'blocked'
              AND child.delivery_class <> 'automated'
              AND parent.status IN ('dead', 'cancelled')
            """
        )
    )
    automated_parent = OutboxDelivery.__table__.alias("automated_parent")
    automated_skipped = int(
        (
            await db.execute(
                select(func.count(OutboxDelivery.id)).where(
                    OutboxDelivery.delivery_class == "automated",
                    or_(
                        (
                            OutboxDelivery.status == "processing"
                        )
                        & (OutboxDelivery.lock_until <= func.utc_timestamp(6)),
                        (
                            OutboxDelivery.status.in_(("pending", "retry"))
                        )
                        & (OutboxDelivery.expires_at <= func.utc_timestamp(6)),
                        (
                            OutboxDelivery.status == "blocked"
                        )
                        & OutboxDelivery.depends_on_delivery_id.in_(
                            select(automated_parent.c.id).where(
                                automated_parent.c.status.in_(("dead", "cancelled"))
                            )
                        ),
                    ),
                )
            )
        ).scalar_one()
        or 0
    )
    await db.flush()
    return MaintenanceResult(
        recovered=int(recovery_result.rowcount or 0),
        ambiguity_counted=ambiguity_count,
        cancelled=cancelled_chatwoot + int(cancelled_result.rowcount or 0),
        expired=expired_chatwoot + int(expired_result.rowcount or 0),
        dependency_terminal=int(dependency_result.rowcount or 0),
        context_note_ambiguous=context_note_ambiguous,
        automated_skipped=automated_skipped,
    )


async def claim_due(
    db: AsyncSession,
    *,
    available_slots: int,
    worker_id: str,
    cutover_epoch: str,
    evolution_lease_seconds: int,
    chatwoot_lease_seconds: int,
) -> list[ClaimedDelivery]:
    """Claim a batch after locking the authoritative runtime gate.

    Caller owns the transaction and must commit before provider I/O. Production
    callers use ``claim_due_with_retry`` so any InnoDB deadlock rolls back this
    entire gate/select/fence unit before a fresh transaction starts.
    """

    if available_slots < 1:
        return []
    if available_slots > 100:
        raise ValueError("available_slots cannot exceed 100")
    if not await _lock_runtime_authority(db, cutover_epoch):
        return []
    selected = await db.execute(
        CLAIM_SQL,
        {
            "available_slots": available_slots,
            "cutover_epoch": cutover_epoch,
        },
    )
    ids = [int(value) for value in selected.scalars().all()]
    claimed: list[ClaimedDelivery] = []
    for delivery_id in ids:
        token = str(uuid4())
        result = await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.id == delivery_id,
                OutboxDelivery.status.in_(("pending", "retry")),
            )
            .values(
                status="processing",
                attempt_count=OutboxDelivery.attempt_count + 1,
                lease_token=token,
                lock_owner=worker_id,
                lock_until=func.timestampadd(
                    text("SECOND"),
                    func.if_(
                        OutboxDelivery.provider == "chatwoot",
                        chatwoot_lease_seconds,
                        evolution_lease_seconds,
                    ),
                    func.utc_timestamp(6),
                ),
            )
        )
        if result.rowcount != 1:
            raise RuntimeError("locked delivery could not be fenced")
        row = await db.get(OutboxDelivery, delivery_id)
        if row is None:
            raise RuntimeError("claimed delivery disappeared")
        claimed.append(_claimed(row))
    await db.flush()
    return claimed


async def claim_due_with_retry(
    session_factory,
    *,
    available_slots: int,
    worker_id: str,
    cutover_epoch: str,
    evolution_lease_seconds: int,
    chatwoot_lease_seconds: int,
    max_attempts: int,
    max_jitter_seconds: float,
    sleep=asyncio.sleep,
) -> list[ClaimedDelivery]:
    """Retry the complete claim transaction on expected MySQL lock failures."""

    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    if max_jitter_seconds < 0:
        raise ValueError("max_jitter_seconds cannot be negative")
    if available_slots < 1:
        return []

    last_error: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            async with session_factory() as db:
                async with db.begin():
                    return await claim_due(
                        db,
                        available_slots=available_slots,
                        worker_id=worker_id,
                        cutover_epoch=cutover_epoch,
                        evolution_lease_seconds=evolution_lease_seconds,
                        chatwoot_lease_seconds=chatwoot_lease_seconds,
                    )
        except BaseException as exc:
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            if not _is_retryable_claim_error(exc):
                raise
            last_error = exc
            if attempt == max_attempts:
                break
            await sleep(random.random() * max_jitter_seconds)

    raise ClaimRetryExhausted(
        f"claim transaction exhausted {max_attempts} attempts"
    ) from last_error


async def renew_lease(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    lease_seconds: int,
) -> bool:
    if not await _lock_runtime_authority(db, cutover_epoch):
        return False
    result = await db.execute(
        update(OutboxDelivery)
        .where(
            OutboxDelivery.id == delivery_id,
            OutboxDelivery.status == "processing",
            OutboxDelivery.lease_token == lease_token,
            OutboxDelivery.cutover_epoch == cutover_epoch,
        )
        .values(
            lock_until=func.timestampadd(
                text("SECOND"), lease_seconds, func.utc_timestamp(6)
            )
        )
    )
    await db.flush()
    return result.rowcount == 1


async def get_handoff_dispatch_ownership(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    authority: Literal["worker", "operator"] = "worker",
) -> HandoffDispatchOwnership | None:
    if authority == "worker" and not await _lock_runtime_authority(
        db, cutover_epoch
    ):
        return None
    result = await db.execute(
        select(HandoffAttempt, OutboxDelivery)
        .join(OutboxDelivery, OutboxDelivery.handoff_attempt_id == HandoffAttempt.id)
        .where(
            OutboxDelivery.id == delivery_id,
            OutboxDelivery.status == "processing",
            OutboxDelivery.lease_token == lease_token,
            OutboxDelivery.cutover_epoch == cutover_epoch,
            HandoffAttempt.sessao_id == OutboxDelivery.sessao_id,
            HandoffAttempt.agente_id == OutboxDelivery.agente_id,
        )
    )
    pair = result.one_or_none()
    if pair is None:
        return None
    attempt, _ = pair
    return HandoffDispatchOwnership(
        handoff_key=attempt.handoff_key,
        inbox_id=attempt.chatwoot_inbox_id,
        status=attempt.status,
    )


def _coverage_matches_assignments(
    parsed: list[tuple[int, int | None, int | None]],
    source_id: str,
    assignments: tuple[HandoffContextAssignment, ...],
) -> bool:
    """Exact coverage: memberships starting in this note are its assignments.

    A whole record or a first part starts in the note whose ``source_id`` the
    assignment pins; continuation parts must belong to an assignment of the
    same attempt pinned to an earlier note. Order follows coverage ordinals.
    """

    starting = [
        membership_id for membership_id, part, _ in parsed if part in (None, 1)
    ]
    here = [
        row.turn_input_membership_id
        for row in assignments
        if row.source_id == source_id
    ]
    by_membership = {row.turn_input_membership_id: row for row in assignments}
    ordinals = [
        by_membership[membership_id].coverage_ordinal
        for membership_id, _, _ in parsed
        if membership_id in by_membership
    ]
    return (
        starting == here
        and all(membership_id in by_membership for membership_id, _, _ in parsed)
        and ordinals == sorted(ordinals)
    )


async def get_context_note_dispatch_ownership(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    sessao_id: int,
    attempt_id: int,
    source_id: str,
    coverage: tuple[str, ...],
    authority: Literal["worker", "operator"] = "worker",
) -> ContextNoteDispatchOwnership | None:
    """Fresh canonical authority for a ready-mapping private note."""

    if authority == "worker" and not await _lock_runtime_authority(
        db, cutover_epoch
    ):
        return None
    session = await sessao_repo.lock_by_id(db, sessao_id, refresh_existing=True)
    attempt = (
        await db.execute(
            select(HandoffAttempt)
            .where(HandoffAttempt.id == attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    try:
        parsed = [parse_coverage_id(item) for item in coverage]
    except ValueError:
        return None
    covered_ids = sorted({membership_id for membership_id, _, _ in parsed})
    assignments = tuple(
        (
            await db.execute(
                select(HandoffContextAssignment)
                .where(
                    HandoffContextAssignment.handoff_attempt_id == attempt_id,
                    or_(
                        HandoffContextAssignment.source_id == source_id,
                        HandoffContextAssignment.turn_input_membership_id.in_(
                            covered_ids or [0]
                        ),
                    ),
                )
                .order_by(HandoffContextAssignment.coverage_ordinal)
                .with_for_update(read=True)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    delivery = (
        await db.execute(
            select(OutboxDelivery)
            .where(OutboxDelivery.id == delivery_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        session is None
        or attempt is None
        or delivery is None
        or attempt.sessao_id != session.id
        or attempt.agente_id != session.agente_id
        or session.handoff_generation != attempt.generation
        or session.handler != "encerrada"
        or session.motivo_encerramento != "handoff"
        or attempt.status != "ready"
        or attempt.chatwoot_contact_id is None
        or attempt.chatwoot_conversation_id is None
        or delivery.sessao_id != session.id
        or delivery.agente_id != attempt.agente_id
        or delivery.handoff_attempt_id != attempt.id
        or delivery.provider != "chatwoot"
        or delivery.operation != "chatwoot.add_private_note.v1"
        or delivery.delivery_class != "provider_setup_context"
        or delivery.status != "processing"
        or delivery.lease_token != lease_token
        or delivery.cutover_epoch != cutover_epoch
        or not _coverage_matches_assignments(parsed, source_id, assignments)
    ):
        return None
    payload = delivery.payload if isinstance(delivery.payload, dict) else {}
    if (
        payload.get("attempt_id") != attempt.id
        or payload.get("handoff_key") != attempt.handoff_key
        or payload.get("inbox_id") != attempt.chatwoot_inbox_id
    ):
        return None
    return ContextNoteDispatchOwnership(
        handoff_key=attempt.handoff_key,
        inbox_id=attempt.chatwoot_inbox_id,
        contact_id=attempt.chatwoot_contact_id,
        conversation_id=attempt.chatwoot_conversation_id,
        status=attempt.status,
    )


async def _lock_context(
    db: AsyncSession,
    delivery_id: int,
) -> tuple[Sessao | None, HandoffAttempt | None, OutboxDelivery | None]:
    """Lock fresh session -> attempt -> delivery lifecycle authority.

    ``populate_existing`` prevents an earlier non-locking lookup in the same
    transaction from overriding the state acquired under these row locks.
    """

    links = (
        await db.execute(
            select(
                OutboxDelivery.sessao_id,
                OutboxDelivery.handoff_attempt_id,
            ).where(OutboxDelivery.id == delivery_id)
        )
    ).one_or_none()
    if links is None:
        return None, None, None
    session = (
        await sessao_repo.lock_by_id(
            db,
            links.sessao_id,
            refresh_existing=True,
        )
        if links.sessao_id is not None
        else None
    )
    attempt = None
    if links.handoff_attempt_id is not None:
        attempt = (
            await db.execute(
                select(HandoffAttempt)
                .where(HandoffAttempt.id == links.handoff_attempt_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
    delivery = (
        await db.execute(
            select(OutboxDelivery)
            .where(OutboxDelivery.id == delivery_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return session, attempt, delivery


def _owns(delivery: OutboxDelivery | None, token: str, cutover_epoch: str) -> bool:
    return bool(
        delivery is not None
        and delivery.status == "processing"
        and delivery.lease_token == token
        and delivery.cutover_epoch == cutover_epoch
    )


def _clear_lease(delivery: OutboxDelivery) -> None:
    delivery.lease_token = None
    delivery.lock_owner = None
    delivery.lock_until = None


async def complete_evolution(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    external_id: str | None,
    http_status: int,
    authority: Literal["worker", "operator"] = "worker",
) -> bool:
    if authority == "worker" and not await _lock_runtime_authority(
        db, cutover_epoch
    ):
        return False
    _, _, delivery = await _lock_context(db, delivery_id)
    if not _owns(delivery, lease_token, cutover_epoch):
        return False
    assert delivery is not None
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    cancelled = delivery.cancel_requested_at is not None
    delivery.status = "delivered"
    delivery.external_id = external_id
    delivery.last_http_status = http_status
    delivery.last_error_code = None
    delivery.delivered_at = now
    delivery.terminal_at = now
    delivery.terminal_reason = "delivered_after_cancel" if cancelled else "delivered"
    _clear_lease(delivery)
    await db.flush()
    return True


async def complete_chatwoot_handoff(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    contact_id: int,
    conversation_id: int,
    http_status: int,
    transition_ttl_seconds: int,
    authority: Literal["worker", "operator"] = "worker",
) -> bool:
    if authority == "worker" and not await _lock_runtime_authority(
        db, cutover_epoch
    ):
        return False
    session, attempt, delivery = await _lock_context(db, delivery_id)
    if not _owns(delivery, lease_token, cutover_epoch):
        return False
    if session is None or attempt is None or delivery is None:
        raise RuntimeError("Chatwoot delivery lost lifecycle ownership")
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    cancelled = delivery.cancel_requested_at is not None
    delivery.status = "delivered"
    delivery.external_id = str(conversation_id)
    delivery.last_http_status = http_status
    delivery.last_error_code = None
    delivery.delivered_at = now
    delivery.terminal_at = now
    delivery.terminal_reason = "delivered_after_cancel" if cancelled else "delivered"
    _clear_lease(delivery)

    if cancelled:
        if attempt.status == "pending":
            attempt.status = "cancelled"
            attempt.terminal_at = now
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status == "blocked",
                OutboxDelivery.delivery_class != "automated",
            )
            .values(
                status="cancelled",
                terminal_at=now,
                terminal_reason="dependency_cancelled",
                last_error_code="dependency_cancelled",
            )
        )
    else:
        await handoff_attempt_repo.mark_ready(
            db,
            sessao=session,
            attempt_id=attempt.id,
            contact_id=contact_id,
            conversation_id=conversation_id,
            ready_at=now,
        )
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status == "blocked",
                OutboxDelivery.delivery_class != "automated",
            )
            .values(
                status="pending",
                next_attempt_at=now,
                expires_at=func.timestampadd(
                    text("SECOND"), transition_ttl_seconds, now
                ),
            )
        )
    await db.flush()
    return True


async def complete_chatwoot_context_note(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    sessao_id: int,
    attempt_id: int,
    source_id: str,
    coverage: tuple[str, ...],
    external_id: str,
    http_status: int,
    next_chunk_ttl_seconds: int,
    authority: Literal["worker", "operator"] = "worker",
) -> bool:
    """Finalize one exact note and release only its next ordered note chunk."""

    ownership = await get_context_note_dispatch_ownership(
        db,
        delivery_id=delivery_id,
        lease_token=lease_token,
        cutover_epoch=cutover_epoch,
        sessao_id=sessao_id,
        attempt_id=attempt_id,
        source_id=source_id,
        coverage=coverage,
        authority=authority,
    )
    if ownership is None:
        return False
    delivery = (
        await db.execute(
            select(OutboxDelivery)
            .where(OutboxDelivery.id == delivery_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    delivery.status = "delivered"
    delivery.external_id = external_id[:255]
    delivery.last_http_status = http_status
    delivery.last_error_code = None
    delivery.delivered_at = now
    delivery.terminal_at = now
    delivery.terminal_reason = "delivered"
    _clear_lease(delivery)
    await db.execute(
        update(OutboxDelivery)
        .where(
            OutboxDelivery.depends_on_delivery_id == delivery.id,
            OutboxDelivery.status == "blocked",
            OutboxDelivery.operation == "chatwoot.add_private_note.v1",
        )
        .values(
            status="pending",
            next_attempt_at=now,
            expires_at=func.timestampadd(
                text("SECOND"), next_chunk_ttl_seconds, now
            ),
        )
    )
    await db.flush()
    return True


async def complete_chatwoot_handoff_from_callback(
    db: AsyncSession,
    *,
    sessao_id: int,
    attempt_id: int,
    cutover_epoch: str,
    contact_id: int,
    conversation_id: int,
    delivery_id: int,
    delivery_key: str,
    inbox_id: int,
    transition_ttl_seconds: int,
) -> Literal["completed", "already_ready", "stale", "cancelled", "busy"]:
    """Reconcile provider success proved by an authenticated exact-marker callback.

    The callback is an independent confirmation path. It locks the global
    runtime authority first and then the lifecycle in canonical order. A live
    worker lease remains authoritative; the caller must roll back the receipt
    and return a retryable response while that fence is active. Cancellation
    and supersession never release the dependent transition.
    """

    if not await _lock_runtime_authority(db, cutover_epoch):
        return "stale"
    # Marker resolution preloads attempt/delivery without a row lock. Every
    # lifecycle entity below must replace that identity-map snapshot with the
    # state acquired in canonical lock order.
    session = await sessao_repo.lock_by_id(
        db,
        sessao_id,
        refresh_existing=True,
    )
    if session is None:
        return "stale"
    attempt = (
        await db.execute(
            select(HandoffAttempt)
            .where(HandoffAttempt.id == attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if attempt is None or attempt.sessao_id != session.id:
        return "stale"
    delivery = (
        await db.execute(
            select(OutboxDelivery)
            .where(
                OutboxDelivery.id == delivery_id,
                OutboxDelivery.delivery_key == delivery_key,
                OutboxDelivery.handoff_attempt_id == attempt.id,
                OutboxDelivery.provider == "chatwoot",
                OutboxDelivery.operation == "chatwoot.open_handoff.v1",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if delivery is None or delivery.cutover_epoch != cutover_epoch:
        return "stale"
    payload = delivery.payload if isinstance(delivery.payload, dict) else {}
    if (
        delivery.sessao_id != session.id
        or delivery.agente_id != attempt.agente_id
        or attempt.handoff_key != payload.get("handoff_key")
        or attempt.chatwoot_inbox_id != inbox_id
        or payload.get("inbox_id") != inbox_id
        or payload.get("phone") != session.telefone
        or session.handler != "encerrada"
        or session.motivo_encerramento != "handoff"
    ):
        return "stale"
    if (
        session.handoff_generation != attempt.generation
        or attempt.status in {"failed", "superseded", "cancelled"}
    ):
        return "stale"
    if delivery.cancel_requested_at is not None or delivery.status == "cancelled":
        return "cancelled"
    if attempt.status == "ready":
        if (
            attempt.chatwoot_conversation_id == conversation_id
            and attempt.chatwoot_contact_id == contact_id
            and delivery.status == "delivered"
        ):
            return "already_ready"
        return "stale"
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    if delivery.status == "processing":
        if delivery.lock_until is None or delivery.lock_until > now:
            return "busy"
    if (
        attempt.chatwoot_contact_id is not None
        and attempt.chatwoot_contact_id != contact_id
    ):
        return "stale"
    if attempt.status != "pending" or delivery.status not in {
        "pending",
        "retry",
        "processing",
    }:
        return "stale"

    delivery.status = "delivered"
    delivery.external_id = str(conversation_id)
    delivery.last_error_code = None
    delivery.delivered_at = now
    delivery.terminal_at = now
    delivery.terminal_reason = "confirmed_by_callback"
    _clear_lease(delivery)
    await handoff_attempt_repo.mark_ready(
        db,
        sessao=session,
        attempt_id=attempt.id,
        contact_id=contact_id,
        conversation_id=conversation_id,
        ready_at=now,
    )
    await db.execute(
        update(OutboxDelivery)
        .where(
            OutboxDelivery.depends_on_delivery_id == delivery.id,
            OutboxDelivery.status == "blocked",
            OutboxDelivery.cancel_requested_at.is_(None),
            OutboxDelivery.delivery_class != "automated",
        )
        .values(
            status="pending",
            next_attempt_at=now,
            expires_at=func.timestampadd(
                text("SECOND"), transition_ttl_seconds, now
            ),
        )
    )
    await db.flush()
    return "completed"


async def fail_processing(
    db: AsyncSession,
    *,
    delivery_id: int,
    lease_token: str,
    cutover_epoch: str,
    error_code: str,
    http_status: int | None,
    retry_delay_seconds: float | None,
    ambiguous: bool,
    authority: Literal["worker", "operator"] = "worker",
) -> Literal["stale", "retry", "dead", "cancelled"]:
    if authority == "worker" and not await _lock_runtime_authority(
        db, cutover_epoch
    ):
        return "stale"
    session, attempt, delivery = await _lock_context(db, delivery_id)
    if not _owns(delivery, lease_token, cutover_epoch):
        return "stale"
    assert delivery is not None
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    delivery.last_error_code = error_code
    delivery.last_http_status = http_status
    if ambiguous:
        delivery.ambiguity_count += 1
    _clear_lease(delivery)

    retry_at = None
    if delivery.cancel_requested_at is not None:
        delivery.status = "cancelled"
        delivery.terminal_at = now
        delivery.terminal_reason = "cancel_requested"
        outcome: Literal["retry", "dead", "cancelled"] = "cancelled"
    else:
        retry_at = (
            now + timedelta(seconds=retry_delay_seconds)
            if retry_delay_seconds is not None
            else None
        )
    if delivery.cancel_requested_at is None and retry_at is not None and delivery.expires_at is not None and retry_at < delivery.expires_at:
        delivery.status = "retry"
        delivery.next_attempt_at = retry_at
        outcome = "retry"
    elif delivery.cancel_requested_at is None:
        delivery.status = "dead"
        delivery.terminal_at = now
        delivery.terminal_reason = "expired" if retry_at is not None else error_code
        outcome = "dead"
    if (
        delivery.operation == CONTEXT_NOTE_OPERATION
        and ambiguous
        and outcome in {"dead", "cancelled"}
    ):
        # Every terminal ambiguous note (worker, CLI reconcile, cancel) carries
        # one reason, the only state resolve-context-note accepts.
        delivery.terminal_reason = "context_note_ambiguous"

    if (
        outcome in {"dead", "cancelled"}
        and delivery.provider == "chatwoot"
        and delivery.operation == "chatwoot.open_handoff.v1"
    ):
        if session is not None and attempt is not None and attempt.status == "pending":
            attempt.status = "cancelled" if outcome == "cancelled" else "failed"
            attempt.terminal_at = now
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status == "blocked",
                OutboxDelivery.delivery_class != "automated",
            )
            .values(
                status="cancelled" if outcome == "cancelled" else "dead",
                terminal_at=now,
                terminal_reason=(
                    "dependency_cancelled" if outcome == "cancelled" else "dependency_failed"
                ),
                last_error_code=(
                    "dependency_cancelled" if outcome == "cancelled" else "dependency_failed"
                ),
            )
        )
    await db.flush()
    return outcome


async def list_metadata(
    db: AsyncSession,
    *,
    statuses: tuple[str, ...] | None = None,
    limit: int = 100,
) -> list[DeliveryMetadata]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    query = select(OutboxDelivery).order_by(OutboxDelivery.id.desc()).limit(limit)
    if statuses:
        query = query.where(OutboxDelivery.status.in_(statuses))
    rows = (await db.execute(query)).scalars().all()
    return [
        DeliveryMetadata(
            id=row.id,
            delivery_key=row.delivery_key,
            provider=row.provider,
            operation=row.operation,
            status=row.status,
            attempt_count=row.attempt_count,
            ambiguity_count=row.ambiguity_count,
            agente_id=row.agente_id,
            sessao_id=row.sessao_id,
            handoff_attempt_id=row.handoff_attempt_id,
            next_attempt_at=row.next_attempt_at,
            expires_at=row.expires_at,
            last_http_status=row.last_http_status,
            last_error_code=row.last_error_code,
            terminal_reason=row.terminal_reason,
        )
        for row in rows
    ]


CONTEXT_NOTE_OPERATION = "chatwoot.add_private_note.v1"


async def _reblock_context_note_chain(
    db: AsyncSession,
    *,
    parent_ids: list[int],
    handoff_attempt_id: int,
) -> int:
    """Revive never-ambiguous ordered notes that a terminal parent cascaded.

    Only rows of the same handoff attempt whose terminal reason is the
    dependency cascade and whose ambiguity count is zero are re-blocked; an
    attempted-ambiguous chunk is never revived here.
    """

    revived = 0
    frontier = list(parent_ids)
    while frontier:
        children = list(
            (
                await db.execute(
                    select(OutboxDelivery)
                    .where(
                        OutboxDelivery.depends_on_delivery_id.in_(frontier),
                        OutboxDelivery.operation == CONTEXT_NOTE_OPERATION,
                        OutboxDelivery.handoff_attempt_id == handoff_attempt_id,
                        OutboxDelivery.status.in_(("dead", "cancelled")),
                        OutboxDelivery.terminal_reason.in_(
                            ("dependency_failed", "dependency_cancelled")
                        ),
                        OutboxDelivery.ambiguity_count == 0,
                    )
                    .order_by(OutboxDelivery.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
        for child in children:
            child.status = "blocked"
            child.next_attempt_at = None
            child.expires_at = None
            child.cancel_requested_at = None
            child.terminal_at = None
            child.terminal_reason = None
            child.last_error_code = None
        revived += len(children)
        frontier = [child.id for child in children]
    await db.flush()
    return revived


async def operator_retry(
    db: AsyncSession,
    *,
    delivery_id: int,
    actor: str,
    reason: str,
    ttl_seconds: int,
) -> OutboxDelivery:
    from src.database.repositories import delivery_state_repo

    actor, reason = delivery_state_repo.validate_audit_metadata(
        actor=actor, reason=reason
    )
    session, attempt, delivery = await _lock_context(db, delivery_id)
    if delivery is None:
        raise LookupError("delivery not found")
    if delivery.status not in {"dead", "cancelled"}:
        raise ValueError("only terminal deliveries can be manually retried")
    if delivery.delivery_class == "automated":
        raise ValueError(
            "automated deliveries require lane-authorized admission, not manual retry; "
            "use turn-resolve for a parked handoff turn"
        )
    if delivery.last_error_code == "duplicate_remote_effect":
        raise ValueError("duplicate remote effects require manual provider resolution")
    note = delivery.operation == CONTEXT_NOTE_OPERATION
    if note:
        if delivery.terminal_reason == "context_note_ambiguous":
            raise ValueError(
                "ambiguous context notes require exact reconciliation or audited resolution"
            )
        if (
            session is None
            or attempt is None
            or session.handoff_generation != attempt.generation
            or attempt.status != "ready"
        ):
            raise ValueError("context note requires a current ready Chatwoot mapping")
        parent = (
            await db.execute(
                select(OutboxDelivery.status).where(
                    OutboxDelivery.id == delivery.depends_on_delivery_id
                )
            )
        ).scalar_one_or_none()
        if parent != "delivered":
            raise ValueError("context note dependency has not been delivered")
    elif delivery.provider == "chatwoot":
        if session is None or attempt is None:
            raise ValueError("Chatwoot delivery lifecycle is incomplete")
        if session.handoff_generation != attempt.generation:
            raise ValueError("superseded handoff cannot be retried")
        if attempt.status not in {"failed", "cancelled", "pending"}:
            raise ValueError("Chatwoot handoff state cannot be retried")
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    delivery.status = "retry"
    delivery.next_attempt_at = now
    delivery.expires_at = now + timedelta(seconds=ttl_seconds)
    delivery.cancel_requested_at = None
    delivery.terminal_at = None
    delivery.terminal_reason = None
    delivery.last_error_code = None
    if note:
        await _reblock_context_note_chain(
            db, parent_ids=[delivery.id], handoff_attempt_id=delivery.handoff_attempt_id
        )
    elif delivery.provider == "chatwoot":
        assert attempt is not None
        attempt.status = "pending"
        attempt.terminal_at = None
        attempt.ready_at = None
        attempt.chatwoot_contact_id = None
        attempt.chatwoot_conversation_id = None
        # Ordered context notes first (their whole cascaded chain), then the
        # legacy direct-dependent revival; an automated transition only
        # returns to ``blocked`` and is never scheduled here.
        await _reblock_context_note_chain(
            db, parent_ids=[delivery.id], handoff_attempt_id=attempt.id
        )
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status.in_(("dead", "cancelled")),
                OutboxDelivery.delivery_class != "automated",
            )
            .values(
                status="blocked",
                next_attempt_at=None,
                expires_at=None,
                cancel_requested_at=None,
                terminal_at=None,
                terminal_reason=None,
                last_error_code=None,
            )
        )
    await delivery_state_repo.record_operator_action(
        db,
        delivery_id=delivery.id,
        action="retry",
        actor=actor,
        reason=reason,
    )
    await db.flush()
    return delivery


async def operator_cancel(
    db: AsyncSession,
    *,
    delivery_id: int,
    actor: str,
    reason: str,
) -> OutboxDelivery:
    from src.database.repositories import delivery_state_repo

    actor, reason = delivery_state_repo.validate_audit_metadata(
        actor=actor, reason=reason
    )
    session, attempt, delivery = await _lock_context(db, delivery_id)
    if delivery is None:
        raise LookupError("delivery not found")
    if delivery.delivery_class in {"automated"}:
        # Ticket07: an automated effect is cancelled only under the canonical
        # lane prefix so its turn settles in the same UoW
        # (lifecycle_repo.operator_cancel_automated); never session-first.
        raise ValueError(
            "automated deliveries are cancelled through the lane-fenced lifecycle path"
        )
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    if delivery.status in {"blocked", "pending", "retry"}:
        delivery.status = "cancelled"
        delivery.cancel_requested_at = now
        delivery.terminal_at = now
        delivery.terminal_reason = "operator_cancelled"
        if (
            delivery.provider == "chatwoot"
            and delivery.operation == "chatwoot.open_handoff.v1"
        ):
            if session is not None and attempt is not None and attempt.status == "pending":
                attempt.status = "cancelled"
                attempt.terminal_at = now
            await db.execute(
                update(OutboxDelivery)
                .where(
                    OutboxDelivery.depends_on_delivery_id == delivery.id,
                    OutboxDelivery.status == "blocked",
                    OutboxDelivery.delivery_class != "automated",
                )
                .values(
                    status="cancelled",
                    cancel_requested_at=now,
                    terminal_at=now,
                    terminal_reason="dependency_cancelled",
                    last_error_code="dependency_cancelled",
                )
            )
    elif delivery.status == "processing":
        delivery.cancel_requested_at = now
    elif delivery.status not in TERMINAL_STATUSES:
        raise ValueError("delivery cannot be cancelled")
    await delivery_state_repo.record_operator_action(
        db,
        delivery_id=delivery.id,
        action="cancel",
        actor=actor,
        reason=reason,
    )
    await db.flush()
    return delivery


async def claim_for_operator_reconciliation(
    db: AsyncSession,
    *,
    delivery_id: int,
    actor: str,
    reason: str,
    lease_seconds: int,
) -> ClaimedDelivery:
    from src.database.repositories import delivery_state_repo

    actor, reason = delivery_state_repo.validate_audit_metadata(
        actor=actor, reason=reason
    )
    session, attempt, delivery = await _lock_context(db, delivery_id)
    if delivery is None:
        raise LookupError("delivery not found")
    if delivery.provider != "chatwoot" or delivery.operation not in {
        "chatwoot.open_handoff.v1",
        "chatwoot.add_private_note.v1",
    }:
        raise ValueError("only Chatwoot handoff/context deliveries can be reconciled")
    if delivery.status not in {"pending", "retry", "dead"}:
        raise ValueError("delivery is not available for operator reconciliation")
    if delivery.cancel_requested_at is not None:
        raise ValueError("cancelled delivery cannot be reconciled")
    if session is None or attempt is None:
        raise ValueError("Chatwoot delivery lifecycle is incomplete")
    if session.handoff_generation != attempt.generation:
        raise ValueError("superseded handoff cannot be reconciled")
    if delivery.operation == "chatwoot.open_handoff.v1":
        if attempt.status not in {"pending", "failed"}:
            raise ValueError("Chatwoot handoff state cannot be reconciled")
    elif attempt.status != "ready":
        raise ValueError("context note requires a ready Chatwoot mapping")
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    token = str(uuid4())
    delivery.status = "processing"
    delivery.attempt_count += 1
    delivery.lease_token = token
    delivery.lock_owner = f"operator:{actor}"[:255]
    delivery.lock_until = now + timedelta(seconds=lease_seconds)
    if delivery.next_attempt_at is None:
        delivery.next_attempt_at = now
    if delivery.expires_at is None or delivery.expires_at <= now:
        delivery.expires_at = now + timedelta(seconds=lease_seconds)
    delivery.terminal_at = None
    delivery.terminal_reason = None
    if delivery.operation == CONTEXT_NOTE_OPERATION:
        # Exact recovery may prove delivery: restore the ordered chain that the
        # ambiguous terminal state cascaded, still blocked behind this note.
        await _reblock_context_note_chain(
            db, parent_ids=[delivery.id], handoff_attempt_id=attempt.id
        )
    if delivery.operation == "chatwoot.open_handoff.v1" and attempt.status == "failed":
        attempt.status = "pending"
        attempt.terminal_at = None
        await _reblock_context_note_chain(
            db, parent_ids=[delivery.id], handoff_attempt_id=attempt.id
        )
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status == "dead",
                OutboxDelivery.delivery_class != "automated",
            )
            .values(
                status="blocked",
                next_attempt_at=None,
                expires_at=None,
                terminal_at=None,
                terminal_reason=None,
                last_error_code=None,
            )
        )
    await delivery_state_repo.record_operator_action(
        db,
        delivery_id=delivery.id,
        action="reconcile",
        actor=actor,
        reason=reason,
    )
    await db.flush()
    return _claimed(delivery)


async def resolve_context_note_ambiguity(
    db: AsyncSession,
    *,
    delivery_id: int,
    resolution: Literal["delivered", "retry-safe", "cancelled"],
    actor: str,
    reason: str,
    ttl_seconds: int,
) -> OutboxDelivery:
    """Audited operator decision after exact recovery could not prove outcome."""

    from src.database.repositories import delivery_state_repo

    actor, reason = delivery_state_repo.validate_audit_metadata(
        actor=actor, reason=reason
    )
    identity = (
        await db.execute(
            select(
                OutboxDelivery.sessao_id,
                OutboxDelivery.handoff_attempt_id,
                OutboxDelivery.cutover_epoch,
            ).where(OutboxDelivery.id == delivery_id)
        )
    ).one_or_none()
    if identity is None or identity.sessao_id is None or identity.handoff_attempt_id is None:
        raise LookupError("context-note delivery not found")
    # End the discovery read view before any authority lock wait.
    await db.rollback()
    if not await _lock_runtime_authority(db, identity.cutover_epoch):
        raise ValueError("delivery runtime is not authoritative")
    session = await sessao_repo.lock_by_id(
        db, identity.sessao_id, refresh_existing=True
    )
    attempt = (
        await db.execute(
            select(HandoffAttempt)
            .where(HandoffAttempt.id == identity.handoff_attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    delivery = (
        await db.execute(
            select(OutboxDelivery)
            .where(OutboxDelivery.id == delivery_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if (
        session is None
        or attempt is None
        or delivery is None
        or delivery.operation != "chatwoot.add_private_note.v1"
        or delivery.status not in {"dead", "cancelled"}
        or delivery.terminal_reason != "context_note_ambiguous"
        or delivery.ambiguity_count < 1
        or attempt.status != "ready"
        or attempt.sessao_id != session.id
        or session.handoff_generation != attempt.generation
    ):
        raise ValueError("delivery is not an unresolved context-note ambiguity")
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    delivery.cancel_requested_at = None
    if resolution == "retry-safe":
        delivery.status = "retry"
        delivery.next_attempt_at = now
        delivery.expires_at = now + timedelta(seconds=ttl_seconds)
        delivery.terminal_at = None
        delivery.terminal_reason = None
        delivery.last_error_code = "operator_confirmed_not_delivered"
        await _reblock_context_note_chain(
            db, parent_ids=[delivery.id], handoff_attempt_id=attempt.id
        )
    else:
        # ``delivered``: the operator proved the exact note exists.
        # ``cancelled``: the operator abandons only this chunk's content; the
        # ordered chain continues so later late context still reaches the
        # human. Both release the next chunk after reviving the cascade.
        delivery.status = "delivered" if resolution == "delivered" else "cancelled"
        if resolution == "delivered":
            delivery.delivered_at = now
        delivery.terminal_at = now
        delivery.terminal_reason = f"operator_resolved_{resolution}"
        delivery.last_error_code = None
        await _reblock_context_note_chain(
            db, parent_ids=[delivery.id], handoff_attempt_id=attempt.id
        )
        await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.depends_on_delivery_id == delivery.id,
                OutboxDelivery.status == "blocked",
                OutboxDelivery.operation == CONTEXT_NOTE_OPERATION,
            )
            .values(
                status="pending",
                next_attempt_at=now,
                expires_at=func.timestampadd(text("SECOND"), ttl_seconds, now),
            )
        )
    await delivery_state_repo.record_operator_action(
        db,
        delivery_id=delivery.id,
        action=f"resolve_context_note_{resolution}",
        actor=actor,
        reason=reason,
    )
    await db.flush()
    return delivery


async def _scrub_superseded_context_notes(
    db: AsyncSession,
    *,
    payload_days: int,
    limit: int = 1000,
) -> int:
    """Scrub unresolved notes only once their handoff generation is gone.

    Plain scalar discovery of superseded candidates first, then canonical
    session -> attempt -> delivery locks per row (never a cross-table locking
    UPDATE from the delivery side). Discovery excludes still-actionable notes,
    so the bounded window always progresses.
    """

    candidate_ids = list(
        (
            await db.execute(
                select(OutboxDelivery.id)
                .outerjoin(
                    HandoffAttempt, HandoffAttempt.id == OutboxDelivery.handoff_attempt_id
                )
                .outerjoin(Sessao, Sessao.id == HandoffAttempt.sessao_id)
                .where(
                    OutboxDelivery.operation == CONTEXT_NOTE_OPERATION,
                    OutboxDelivery.status.in_(("dead", "cancelled")),
                    # Plain discovery read (no locks): only rows that can
                    # progress, so still-actionable notes never fill the window.
                    or_(
                        HandoffAttempt.id.is_(None),
                        Sessao.id.is_(None),
                        Sessao.handoff_generation != HandoffAttempt.generation,
                    ),
                    OutboxDelivery.terminal_at
                    < func.timestampadd(text("DAY"), -payload_days, func.utc_timestamp(6)),
                    func.json_length(OutboxDelivery.payload) > 0,
                    OutboxDelivery.terminal_reason.not_in(
                        ("operator_resolved_cancelled", "operator_cancelled", "cancel_requested")
                    ),
                )
                .order_by(OutboxDelivery.id)
                .limit(limit)
            )
        ).scalars()
    )
    scrubbed = 0
    for delivery_id in candidate_ids:
        session, attempt, delivery = await _lock_context(db, delivery_id)
        if (
            delivery is None
            or delivery.status not in {"dead", "cancelled"}
            or not delivery.payload
        ):
            continue
        if (
            session is not None
            and attempt is not None
            and session.handoff_generation == attempt.generation
        ):
            continue
        delivery.payload = {}
        scrubbed += 1
    await db.flush()
    return scrubbed


_retention_nonterminal_skips = 0
RETENTION_SKIP_THRESHOLD = 1


def retention_skip_count() -> int:
    return _retention_nonterminal_skips


async def _retain_automated_rows(
    db: AsyncSession,
    *,
    payload_days: int,
    receipt_days: int,
    limit: int,
) -> tuple[int, int, int]:
    """Scrub/delete automated rows only once their turn is terminal (ticket07).

    A terminal turn is immutable (``trg_conversation_turn_lifecycle_v1``), so
    the scalar discovery below is a stable truth; rows of a nonterminal turn
    (e.g. a parked handoff awaiting operator ``turn-resolve``) keep their
    payload and identity and never enter the bounded window. The id-based
    statements then lock only outbox rows and re-check terminal age.
    """

    global _retention_nonterminal_skips
    skipped = int((await db.execute(
        select(func.count(OutboxDelivery.id))
        .join(ConversationTurn, ConversationTurn.id == OutboxDelivery.conversation_turn_id)
        .where(
            OutboxDelivery.delivery_class == "automated",
            OutboxDelivery.status.in_(TERMINAL_STATUSES),
            ConversationTurn.session_slot != "terminal",
            or_(
                and_(
                    func.json_length(OutboxDelivery.payload) > 0,
                    OutboxDelivery.terminal_at < func.timestampadd(
                        text("DAY"), -payload_days, func.utc_timestamp(6)
                    ),
                ),
                and_(
                    func.json_length(OutboxDelivery.payload) == 0,
                    OutboxDelivery.terminal_at < func.timestampadd(
                        text("DAY"), -receipt_days, func.utc_timestamp(6)
                    ),
                ),
            ),
        )
    )).scalar_one())
    if skipped:
        _retention_nonterminal_skips += skipped
        logger.error(
            "Automated retention skipped nonterminal turns | rows={} count={} threshold={}",
            skipped, _retention_nonterminal_skips, RETENTION_SKIP_THRESHOLD,
        )
    turn = ConversationTurn.__table__
    dependent = OutboxDelivery.__table__.alias("dependent")
    base = (
        select(OutboxDelivery.id)
        .join(turn, turn.c.id == OutboxDelivery.conversation_turn_id)
        .where(
            OutboxDelivery.delivery_class == "automated",
            OutboxDelivery.status.in_(TERMINAL_STATUSES),
            turn.c.session_slot == "terminal",
        )
        .order_by(OutboxDelivery.id)
        .limit(limit)
    )
    scrub_ids = list(
        (
            await db.execute(
                base.where(
                    OutboxDelivery.terminal_at
                    < func.timestampadd(text("DAY"), -payload_days, func.utc_timestamp(6)),
                    func.json_length(OutboxDelivery.payload) > 0,
                )
            )
        ).scalars()
    )
    delete_ids = list(
        (
            await db.execute(
                base.where(
                    OutboxDelivery.terminal_at
                    < func.timestampadd(text("DAY"), -receipt_days, func.utc_timestamp(6)),
                    func.json_length(OutboxDelivery.payload) == 0,
                    ~select(dependent.c.id)
                    .where(dependent.c.depends_on_delivery_id == OutboxDelivery.id)
                    .exists(),
                )
            )
        ).scalars()
    )
    scrubbed = deleted = 0
    if scrub_ids:
        result = await db.execute(
            update(OutboxDelivery)
            .where(
                OutboxDelivery.id.in_(scrub_ids),
                OutboxDelivery.status.in_(TERMINAL_STATUSES),
                OutboxDelivery.terminal_at
                < func.timestampadd(text("DAY"), -payload_days, func.utc_timestamp(6)),
            )
            .values(payload=func.json_object())
            .execution_options(synchronize_session=False)
        )
        scrubbed = int(result.rowcount or 0)
    if delete_ids:
        result = await db.execute(
            OutboxDelivery.__table__.delete().where(
                OutboxDelivery.__table__.c.id.in_(delete_ids),
                OutboxDelivery.__table__.c.status.in_(TERMINAL_STATUSES),
                OutboxDelivery.__table__.c.terminal_at
                < func.timestampadd(text("DAY"), -receipt_days, func.utc_timestamp(6)),
            )
        )
        deleted = int(result.rowcount or 0)
    return scrubbed, deleted, skipped


async def cleanup_retention(
    db: AsyncSession,
    *,
    payload_days: int = 7,
    receipt_days: int = 90,
    automated_limit: int = 1000,
) -> CleanupResult:
    if not 1 <= automated_limit <= 1000:
        raise ValueError("automated retention limit must be between 1 and 1000")
    # Per-row canonical locks (session -> attempt -> delivery) run before any
    # bulk delivery lock in this transaction, never after it.
    superseded_notes = await _scrub_superseded_context_notes(
        db, payload_days=payload_days
    )
    automated_scrubbed, automated_deleted, automated_skipped = await _retain_automated_rows(
        db,
        payload_days=payload_days,
        receipt_days=receipt_days,
        limit=automated_limit,
    )
    # Context notes keep their exact payload until resolved: reconcile,
    # retry-safe and chain revival need it. Resolved = delivered or a final
    # operator/session decision; an unresolved note of a superseded handoff
    # generation can never be acted on and is scrubbed by the per-row pass.
    scrubbed = await db.execute(
        text(
            """
            UPDATE outbox_deliveries
            SET payload = JSON_OBJECT()
            WHERE status IN ('delivered', 'dead', 'cancelled')
              AND terminal_at < TIMESTAMPADD(DAY, -:payload_days, UTC_TIMESTAMP(6))
              AND JSON_LENGTH(payload) > 0
              AND delivery_class <> 'automated'
              AND (
                  operation <> 'chatwoot.add_private_note.v1'
                  OR status = 'delivered'
                  OR terminal_reason IN (
                      'operator_resolved_cancelled', 'operator_cancelled',
                      'cancel_requested'
                  )
              )
            """
        ),
        {"payload_days": payload_days},
    )

    # Scheduled receipt/message/membership identity is audit-retained work, not
    # an age-only cache. A future terminal retention flow must remove the
    # terminal membership first, then its message association/work, and only
    # then make the receipt eligible. This generic pass has no such authority.
    receipts = await db.execute(
        text(
            """
            DELETE receipts
            FROM inbound_events AS receipts
            WHERE receipts.received_at
                    < TIMESTAMPADD(DAY, -:receipt_days, UTC_TIMESTAMP(6))
              AND receipts.mensagem_id IS NULL
              AND receipts.receipt_disposition IN (
                  'ignored_nonbot', 'legacy_evolution', 'chatwoot'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_input_memberships AS membership
                  WHERE membership.inbound_event_id = receipts.id
              )
            """
        ),
        {"receipt_days": receipt_days},
    )
    heartbeats = await db.execute(
        text(
            """
            DELETE FROM delivery_worker_heartbeats
            WHERE last_seen_at < TIMESTAMPADD(DAY, -:receipt_days, UTC_TIMESTAMP(6))
            """
        ),
        {"receipt_days": receipt_days},
    )
    # Abandon is durable intent while its exact effect exists. ON DELETE SET
    # NULL makes it age-purgeable after the delivery's own safe retention.
    abandon_retained = int((await db.execute(text(
        "SELECT COUNT(*) FROM delivery_operator_actions "
        "WHERE action='turn_resolve:abandon' AND delivery_id IS NOT NULL "
        "AND created_at < TIMESTAMPADD(DAY, -:receipt_days, UTC_TIMESTAMP(6))"
    ), {"receipt_days": receipt_days})).scalar_one())
    if abandon_retained:
        logger.error("Retention kept abandon intents | count={} threshold=1", abandon_retained)
    audits = await db.execute(
        text(
            """
            DELETE FROM delivery_operator_actions
            WHERE created_at < TIMESTAMPADD(DAY, -:receipt_days, UTC_TIMESTAMP(6))
              AND (action <> 'turn_resolve:abandon' OR delivery_id IS NULL)
            """
        ),
        {"receipt_days": receipt_days},
    )
    child_deliveries = await db.execute(
        text(
            """
            DELETE FROM outbox_deliveries
            WHERE status IN ('delivered', 'dead', 'cancelled')
              AND terminal_at < TIMESTAMPADD(DAY, -:receipt_days, UTC_TIMESTAMP(6))
              AND (operation <> 'chatwoot.add_private_note.v1'
                   OR JSON_LENGTH(payload) = 0)
              AND delivery_class <> 'automated'
              AND depends_on_delivery_id IS NOT NULL
              AND id NOT IN (
                  SELECT parent_id FROM (
                      SELECT DISTINCT depends_on_delivery_id AS parent_id
                      FROM outbox_deliveries
                      WHERE depends_on_delivery_id IS NOT NULL
                  ) AS referenced
              )
            """
        ),
        {"receipt_days": receipt_days},
    )
    parent_deliveries = await db.execute(
        text(
            """
            DELETE FROM outbox_deliveries
            WHERE status IN ('delivered', 'dead', 'cancelled')
              AND terminal_at < TIMESTAMPADD(DAY, -:receipt_days, UTC_TIMESTAMP(6))
              AND (operation <> 'chatwoot.add_private_note.v1'
                   OR JSON_LENGTH(payload) = 0)
              AND delivery_class <> 'automated'
              AND id NOT IN (
                  SELECT parent_id FROM (
                      SELECT DISTINCT depends_on_delivery_id AS parent_id
                      FROM outbox_deliveries
                      WHERE depends_on_delivery_id IS NOT NULL
                  ) AS referenced
              )
            """
        ),
        {"receipt_days": receipt_days},
    )
    await db.flush()
    return CleanupResult(
        automated_nonterminal_skipped=automated_skipped,
        abandon_intents_retained=abandon_retained,
        payloads_scrubbed=int(scrubbed.rowcount or 0)
        + superseded_notes
        + automated_scrubbed,
        receipts_purged=int(receipts.rowcount or 0),
        heartbeats_purged=int(heartbeats.rowcount or 0),
        audits_purged=int(audits.rowcount or 0),
        deliveries_purged=int(child_deliveries.rowcount or 0)
        + int(parent_deliveries.rowcount or 0)
        + automated_deleted,
    )
