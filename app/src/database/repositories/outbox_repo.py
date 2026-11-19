from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    ConversationTurn,
    HandoffAttempt,
    OutboxDelivery,
    Sessao,
    TurnRevision,
)


Provider = Literal["evolution", "chatwoot"]
DeliveryClass = Literal[
    "legacy_unknown",
    "legacy_automated",
    "human",
    "automated",
    "provider_setup_context",
]
AutomatedEffect = Literal["reply", "handoff_transition"]
NONTERMINAL_STATUSES = ("blocked", "pending", "processing", "retry")


class OutboxConflictError(ValueError):
    pass


class ActiveDeliveriesError(ValueError):
    pass


class OutboxDeliveryClassRequiredError(ValueError):
    """A new delivery cannot enter the retained ``legacy_unknown`` class."""


def _validate_uuid(value: str, field: str) -> None:
    try:
        UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"{field} must be a UUID") from exc


def _classify_internal_legacy_path(
    *, provider: Provider, operation: str, dedupe_key: str
) -> DeliveryClass:
    """Classify only repository-owned intent, never webhook payload fields."""

    if provider == "chatwoot" and operation == "chatwoot.open_handoff.v1":
        return "provider_setup_context"
    if provider == "evolution" and operation == "evolution.send_text.v1":
        if dedupe_key.startswith("evolution:chatwoot:"):
            return "human"
        if dedupe_key.startswith("evolution:inbound:") or (
            dedupe_key.startswith("evolution:handoff:")
            and dedupe_key.endswith(":transition")
        ):
            return "legacy_automated"
    return "legacy_unknown"


async def enqueue(
    db: AsyncSession,
    *,
    cutover_epoch: str,
    dedupe_key: str,
    provider: Provider,
    operation: str,
    payload_version: int,
    payload: dict[str, Any],
    ordering_key: str,
    stream_sequence: int,
    next_attempt_at: datetime | None,
    expires_at: datetime | None,
    priority: int = 0,
    agente_id: int | None = None,
    sessao_id: int | None = None,
    handoff_attempt_id: int | None = None,
    inbound_event_id: int | None = None,
    depends_on_delivery_id: int | None = None,
    delivery_key: str | None = None,
    _delivery_class: DeliveryClass | None = None,
    _automated_effect: AutomatedEffect | None = None,
    _scheduler_epoch: str | None = None,
    _conversation_turn_id: int | None = None,
    _turn_revision_id: int | None = None,
) -> tuple[OutboxDelivery, bool]:
    """Idempotently stage a logical effect, returning ``(row, created)``."""

    if not dedupe_key or not dedupe_key.isascii():
        raise ValueError("dedupe_key must be non-empty ASCII")
    _validate_uuid(cutover_epoch, "cutover_epoch")
    _validate_uuid(ordering_key, "ordering_key")
    delivery_key = delivery_key or str(uuid4())
    _validate_uuid(delivery_key, "delivery_key")

    delivery_class = _delivery_class or _classify_internal_legacy_path(
        provider=provider,
        operation=operation,
        dedupe_key=dedupe_key,
    )
    # S5 security fix: retained rows may remain legacy_unknown, but every new
    # unclassified effect fails before INSERT instead of creating a permanent
    # enablement/drain blocker. Production callers always pass the intent.
    if delivery_class == "legacy_unknown":
        raise OutboxDeliveryClassRequiredError(
            "explicit delivery class is required for new outbox deliveries"
        )
    if delivery_class == "automated":
        if (
            _automated_effect is None
            or _automated_effect not in {"reply", "handoff_transition"}
            or _scheduler_epoch is None
            or _conversation_turn_id is None
            or _turn_revision_id is None
        ):
            raise ValueError("automated delivery requires exact scheduler authority")
        _validate_uuid(_scheduler_epoch, "scheduler_epoch")
    elif any(
        value is not None
        for value in (
            _automated_effect,
            _scheduler_epoch,
            _conversation_turn_id,
            _turn_revision_id,
        )
    ):
        raise ValueError("only automated delivery may carry scheduler authority")

    blocked = depends_on_delivery_id is not None
    if blocked and (next_attempt_at is not None or expires_at is not None):
        raise ValueError("blocked deliveries cannot be scheduled before dependency success")
    if not blocked and (next_attempt_at is None or expires_at is None):
        raise ValueError("ready deliveries require next_attempt_at and expires_at")

    delivery = OutboxDelivery(
        delivery_key=delivery_key,
        cutover_epoch=cutover_epoch,
        dedupe_key=dedupe_key,
        provider=provider,
        operation=operation,
        payload_version=payload_version,
        payload=payload,
        delivery_class=delivery_class,
        automated_effect=_automated_effect,
        scheduler_epoch=_scheduler_epoch,
        conversation_turn_id=_conversation_turn_id,
        turn_revision_id=_turn_revision_id,
        agente_id=agente_id,
        sessao_id=sessao_id,
        handoff_attempt_id=handoff_attempt_id,
        inbound_event_id=inbound_event_id,
        ordering_key=ordering_key,
        stream_sequence=stream_sequence,
        depends_on_delivery_id=depends_on_delivery_id,
        priority=priority,
        status="blocked" if blocked else "pending",
        next_attempt_at=next_attempt_at,
        expires_at=expires_at,
    )
    try:
        async with db.begin_nested():
            db.add(delivery)
            await db.flush()
        return delivery, True
    except IntegrityError:
        result = await db.execute(
            select(OutboxDelivery).where(OutboxDelivery.dedupe_key == dedupe_key)
        )
        existing = result.scalar_one_or_none()
        if existing is None:
            raise OutboxConflictError("delivery key or stream sequence already used")

        comparable = (
            "cutover_epoch",
            "provider",
            "operation",
            "payload_version",
            "payload",
            "delivery_class",
            "automated_effect",
            "scheduler_epoch",
            "conversation_turn_id",
            "turn_revision_id",
            "agente_id",
            "sessao_id",
            "handoff_attempt_id",
            "inbound_event_id",
            "ordering_key",
            "stream_sequence",
            "depends_on_delivery_id",
            "priority",
        )
        if any(getattr(existing, name) != getattr(delivery, name) for name in comparable):
            raise OutboxConflictError("dedupe key reused for a different logical effect")
        return existing, False


async def enqueue_automated(
    db: AsyncSession,
    *,
    scheduler_epoch: str,
    lane_owner_token: str,
    conversation_turn_id: int,
    turn_revision_id: int,
    automated_effect: AutomatedEffect,
    cutover_epoch: str,
    dedupe_key: str,
    payload_version: int,
    payload: dict[str, Any],
    ordering_key: str,
    stream_sequence: int,
    next_attempt_at: datetime,
    expires_at: datetime,
    agente_id: int,
    sessao_id: int,
    inbound_event_id: int | None = None,
    priority: int = 0,
) -> tuple[OutboxDelivery, bool]:
    """Stage a future paced effect with immutable exact turn/revision links."""

    # Local import avoids repository module initialization cycles. This method
    # owns the canonical prefix and stages the delivery only after the exact
    # live lane/turn/revision relationship has been locked and validated.
    from src.database.repositories import scheduler_repo

    _validate_uuid(scheduler_epoch, "scheduler_epoch")
    _validate_uuid(lane_owner_token, "lane_owner_token")
    runtime, scheduler, _, lane = await scheduler_repo.lock_authority_prefix(
        db,
        agente_id=agente_id,
        scheduler_epoch=scheduler_epoch,
        delivery_runtime_epoch=cutover_epoch,
        require_enabled=False,
    )
    if scheduler.state not in {"enabled", "draining"}:
        raise OutboxConflictError("scheduler is not authorized for automated enqueue")
    session = (
        await db.execute(
            select(Sessao)
            .where(Sessao.id == sessao_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    turn = (
        await db.execute(
            select(ConversationTurn)
            .where(ConversationTurn.id == conversation_turn_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    revision = (
        await db.execute(
            select(TurnRevision)
            .where(TurnRevision.id == turn_revision_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    if (
        runtime.cutover_epoch != cutover_epoch
        or session is None
        or session.agente_id != agente_id
        or turn is None
        or turn.agente_id != agente_id
        or turn.sessao_id != sessao_id
        or turn.scheduler_epoch != scheduler_epoch
        or turn.claim_token != lane_owner_token
        or turn.claim_until is None
        or turn.claim_until <= now
        or revision is None
        or revision.turn_id != conversation_turn_id
        or revision.scheduler_epoch != scheduler_epoch
        or revision.status != "admitted"
        or turn.final_revision_id != turn_revision_id
        or lane.owner_token != lane_owner_token
        or lane.active_turn_id != conversation_turn_id
        or lane.active_revision_id != turn_revision_id
        or lane.lease_until is None
        or lane.lease_until <= now
    ):
        raise OutboxConflictError("automated delivery authority links are stale or mismatched")

    return await enqueue(
        db,
        cutover_epoch=cutover_epoch,
        dedupe_key=dedupe_key,
        provider="evolution",
        operation="evolution.send_text.v1",
        payload_version=payload_version,
        payload=payload,
        ordering_key=ordering_key,
        stream_sequence=stream_sequence,
        next_attempt_at=next_attempt_at,
        expires_at=expires_at,
        priority=priority,
        agente_id=agente_id,
        sessao_id=sessao_id,
        inbound_event_id=inbound_event_id,
        _delivery_class="automated",
        _automated_effect=automated_effect,
        _scheduler_epoch=scheduler_epoch,
        _conversation_turn_id=conversation_turn_id,
        _turn_revision_id=turn_revision_id,
    )


async def get_by_delivery_key(
    db: AsyncSession,
    delivery_key: str,
) -> OutboxDelivery | None:
    """Resolve a provider-visible exact marker without taking lifecycle locks."""

    try:
        _validate_uuid(delivery_key, "delivery_key")
    except ValueError:
        return None
    return (
        await db.execute(
            select(OutboxDelivery).where(
                OutboxDelivery.delivery_key == delivery_key
            )
        )
    ).scalar_one_or_none()


async def reserve_transition(
    db: AsyncSession,
    *,
    dependency: OutboxDelivery,
    cutover_epoch: str,
    dedupe_key: str,
    payload_version: int,
    payload: dict[str, Any],
    ordering_key: str,
    stream_sequence: int,
    agente_id: int,
    sessao_id: int,
    handoff_attempt_id: int,
    inbound_event_id: int | None,
    priority: int = 0,
) -> tuple[OutboxDelivery, bool]:
    if dependency.id is None:
        raise ValueError("dependency must be flushed before reserving transition")
    return await enqueue(
        db,
        cutover_epoch=cutover_epoch,
        dedupe_key=dedupe_key,
        provider="evolution",
        operation="evolution.send_text.v1",
        payload_version=payload_version,
        payload=payload,
        ordering_key=ordering_key,
        stream_sequence=stream_sequence,
        next_attempt_at=None,
        expires_at=None,
        priority=priority,
        agente_id=agente_id,
        sessao_id=sessao_id,
        handoff_attempt_id=handoff_attempt_id,
        inbound_event_id=inbound_event_id,
        depends_on_delivery_id=dependency.id,
        _delivery_class="legacy_automated",
    )


async def get_by_id(
    db: AsyncSession,
    delivery_id: int,
    *,
    for_update: bool = False,
) -> OutboxDelivery | None:
    query = select(OutboxDelivery).where(OutboxDelivery.id == delivery_id)
    if for_update:
        query = query.with_for_update()
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def has_nonterminal_for_session(db: AsyncSession, sessao_id: int) -> bool:
    result = await db.execute(
        select(func.count(OutboxDelivery.id)).where(
            _belongs_to_session(sessao_id),
            OutboxDelivery.status.in_(NONTERMINAL_STATUSES),
        )
    )
    return bool(result.scalar_one())


async def has_nonterminal_for_agent(db: AsyncSession, agente_id: int) -> bool:
    result = await db.execute(
        select(func.count(OutboxDelivery.id)).where(
            _belongs_to_agent(agente_id),
            OutboxDelivery.status.in_(NONTERMINAL_STATUSES),
        )
    )
    return bool(result.scalar_one())


def _belongs_to_session(sessao_id: int):
    return or_(
        OutboxDelivery.sessao_id == sessao_id,
        OutboxDelivery.handoff_attempt_id.in_(
            select(HandoffAttempt.id).where(
                HandoffAttempt.sessao_id == sessao_id
            )
        ),
    )


def _belongs_to_agent(agente_id: int):
    return or_(
        OutboxDelivery.agente_id == agente_id,
        OutboxDelivery.sessao_id.in_(
            select(Sessao.id).where(Sessao.agente_id == agente_id)
        ),
        OutboxDelivery.handoff_attempt_id.in_(
            select(HandoffAttempt.id).where(
                HandoffAttempt.agente_id == agente_id
            )
        ),
    )


async def assert_session_deletable(db: AsyncSession, sessao_id: int) -> None:
    result = await db.execute(
        select(OutboxDelivery.id)
        .where(
            _belongs_to_session(sessao_id),
            OutboxDelivery.status.in_(NONTERMINAL_STATUSES),
        )
        .limit(1)
        .with_for_update()
    )
    if result.scalar_one_or_none() is not None:
        raise ActiveDeliveriesError("session has active deliveries")


async def assert_agent_deletable(db: AsyncSession, agente_id: int) -> None:
    result = await db.execute(
        select(OutboxDelivery.id)
        .where(
            _belongs_to_agent(agente_id),
            OutboxDelivery.status.in_(NONTERMINAL_STATUSES),
        )
        .limit(1)
        .with_for_update()
    )
    if result.scalar_one_or_none() is not None:
        raise ActiveDeliveriesError("agent has active deliveries")


async def request_cancellation_for_session(
    db: AsyncSession,
    *,
    sessao_id: int,
    requested_at: datetime | None = None,
) -> tuple[int, int]:
    """Cancel queued work and fence any work already owned by a worker."""

    now = requested_at or datetime.utcnow()
    queued_result = await db.execute(
        update(OutboxDelivery)
        .where(
            _belongs_to_session(sessao_id),
            OutboxDelivery.status.in_(("blocked", "pending", "retry")),
        )
        .values(
            status="cancelled",
            cancel_requested_at=now,
            terminal_at=now,
            terminal_reason="cancel_requested",
        )
    )
    processing_result = await db.execute(
        update(OutboxDelivery)
        .where(
            _belongs_to_session(sessao_id),
            OutboxDelivery.status == "processing",
            OutboxDelivery.cancel_requested_at.is_(None),
        )
        .values(cancel_requested_at=now)
    )
    await db.flush()
    return int(queued_result.rowcount or 0), int(processing_result.rowcount or 0)


async def get_open_handoff_delivery_for_update(
    db: AsyncSession,
    *,
    handoff_attempt_id: int,
) -> OutboxDelivery | None:
    """Lock the canonical Chatwoot-open job after session/attempt locks."""

    result = await db.execute(
        select(OutboxDelivery)
        .where(
            OutboxDelivery.handoff_attempt_id == handoff_attempt_id,
            OutboxDelivery.provider == "chatwoot",
            OutboxDelivery.operation == "chatwoot.open_handoff.v1",
        )
        .with_for_update()
    )
    return result.scalar_one_or_none()
