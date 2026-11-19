"""T08-5: Evolution webhook 503 technical codes and aggregate scheduler health.

Codes and health keys come from fixed allowlists: nothing derived from the
request (phone, content, instance, provider id, receipt hash) or from secrets
and epochs ever reaches a 503 body or a health payload. Exception messages
are matched exactly; the parity tests assert each mapped message still
exists in the raising module, so a rename fails a test instead of silently
degrading a cause to a generic code.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database import db_errors
from src.database.repositories import scheduler_metrics_repo, scheduler_repo

SIGNAL_THRESHOLD = 1
RETRY_AFTER_SECONDS = "5"
UNAVAILABLE_DETAIL = "temporarily unavailable"

STATE_REFUSAL_CODES = {"draining": "scheduler_draining"}
STATE_REFUSAL_DEFAULT = "scheduler_not_enabled"

REJECTION_CODES = frozenset({
    "scheduler_draining",
    "scheduler_not_enabled",
    "scheduler_not_ready",
    "cap_exceeded",
    "epoch_mismatch",
    "conflict",
    "runtime_mismatch",
    "runtime_frozen",
    "feature_disabled",
    "auth_not_enforced",
    "auth_unavailable",
    "gate_changed",
    "ownership_unavailable",
    "receipt_config_unavailable",
    "usage_invariant",
    "transaction_lost",
    "internal_error",
})

# scheduler_repo.assert_intake_caps messages → cap kind (process counter only).
CAP_KINDS = {
    "session pending input count cap exceeded": "session_inputs",
    "session pending input byte cap exceeded": "session_bytes",
    "agent queued turn cap exceeded": "agent_turns",
    "agent queued byte cap exceeded": "agent_bytes",
    "global queued turn cap exceeded": "global_turns",
    "global queued byte cap exceeded": "global_bytes",
}
CAP_KIND_UNKNOWN = "unknown"

# scheduler_repo authority-prefix messages with a cause more precise than
# the class default (SchedulerConflictError → conflict, SchedulerNotReadyError
# → scheduler_not_ready).
SCHEDULER_MESSAGE_CODES = {
    "delivery runtime is not outbox": "runtime_mismatch",
    "delivery runtime epoch changed": "epoch_mismatch",
    "scheduler epoch changed": "epoch_mismatch",
    "owned lane belongs to another scheduler epoch": "epoch_mismatch",
}

# evolution_orchestrator DeliveryOwnershipError / ScheduledIntakeUnavailable.
ORCHESTRATOR_MESSAGE_CODES = {
    "delivery runtime is frozen": "runtime_frozen",
    "configured delivery mode does not match database": "runtime_mismatch",
    "configured delivery epoch does not match database": "epoch_mismatch",
    "unsupported delivery runtime mode": "runtime_mismatch",
    "outbox ingress requires enforced authentication": "auth_not_enforced",
    "scheduled intake requires enforced auth": "auth_not_enforced",
    "scheduled intake feature gate is disabled": "feature_disabled",
    "scheduled intake delivery epoch is unavailable": "epoch_mismatch",
    "scheduled intake gate changed": "gate_changed",
    "agent scheduler opt-in changed": "gate_changed",
}

_rejections: dict[str, int] = {}
_cap_rejections: dict[str, int] = {}
_metrics_unavailable_count = 0
_metrics_available = True
# Scheduler states (read from the DB) in which unreadable metrics fail closed.
BLOCKING_STATES = frozenset({"enabling", "enabled", "draining"})
_attention_signal_count = 0
_attention_active = False


def state_refusal_code(state: str) -> str:
    return STATE_REFUSAL_CODES.get(state, STATE_REFUSAL_DEFAULT)


def _cause_chain(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__
    return chain


def classify_webhook_failure(exc: BaseException) -> tuple[str, str | None]:
    """Return ``(code, cap_kind)``; ``code`` is always in REJECTION_CODES."""

    from src.database.repositories import consumo_repo
    from src.services import evolution_orchestrator

    message = str(exc)
    if isinstance(exc, scheduler_repo.SchedulerDeadlockRetryExhausted) or db_errors.transaction_lost(exc):
        return "transaction_lost", None
    for item in _cause_chain(exc):
        if isinstance(item, scheduler_repo.SchedulerCapExceeded):
            return "cap_exceeded", CAP_KINDS.get(str(item), CAP_KIND_UNKNOWN)
        if isinstance(item, scheduler_repo.SchedulerConflictError):
            return SCHEDULER_MESSAGE_CODES.get(str(item), "conflict"), None
        if isinstance(item, scheduler_repo.SchedulerNotReadyError):
            state = getattr(item, "scheduler_state", None)
            if isinstance(state, str):
                return state_refusal_code(state), None
            return SCHEDULER_MESSAGE_CODES.get(str(item), "scheduler_not_ready"), None
    if isinstance(exc, evolution_orchestrator.DeliveryOwnershipError):
        return ORCHESTRATOR_MESSAGE_CODES.get(message, "ownership_unavailable"), None
    if isinstance(exc, evolution_orchestrator.ReceiptConfigurationError):
        return "receipt_config_unavailable", None
    if isinstance(exc, consumo_repo.UsagePersistenceInvariantError):
        return "usage_invariant", None
    return "internal_error", None


class TechnicalUnavailable(HTTPException):
    """HTTP 503 whose body adds a stable technical ``code`` to ``detail``."""

    def __init__(
        self,
        code: str,
        *,
        detail: str = UNAVAILABLE_DETAIL,
        retry_after: str | None = RETRY_AFTER_SECONDS,
    ) -> None:
        if code not in REJECTION_CODES:
            code = "internal_error"
        super().__init__(
            status_code=503,
            detail=detail,
            headers={"Retry-After": retry_after} if retry_after else None,
        )
        self.code = code


async def technical_unavailable_handler(
    request: Request, exc: TechnicalUnavailable
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "code": exc.code},
        headers=exc.headers,
    )


def record_rejection(code: str, cap_kind: str | None = None) -> str:
    if code not in REJECTION_CODES:
        code = "internal_error"
    _rejections[code] = _rejections.get(code, 0) + 1
    if cap_kind is not None:
        _cap_rejections[cap_kind] = _cap_rejections.get(cap_kind, 0) + 1
    logger.error(
        "Evolution webhook unavailable | code={} cap={} count={} threshold={}",
        code, cap_kind, _rejections[code], SIGNAL_THRESHOLD,
    )
    return code


def webhook_unavailable(exc: BaseException) -> TechnicalUnavailable:
    code, cap_kind = classify_webhook_failure(exc)
    return TechnicalUnavailable(record_rejection(code, cap_kind))


def rejection_counts() -> dict[str, Any]:
    return {
        "by_code": dict(sorted(_rejections.items())),
        "by_cap": dict(sorted(_cap_rejections.items())),
        "intake_state_refusals": scheduler_repo.intake_state_refusal_count(),
        "drain_refusals": scheduler_repo.drain_refusal_count(),
    }


def metrics_unavailable_count() -> int:
    return _metrics_unavailable_count


def attention_signal_count() -> int:
    return _attention_signal_count


def reset_health_transition_state() -> None:
    """Reset transition memory without erasing monotonic counters."""

    global _metrics_available, _attention_active
    _metrics_available = True
    _attention_active = False


def _attention(metrics: scheduler_metrics_repo.SchedulerMetrics) -> bool:
    return any((
        metrics.lease_expired_lanes,
        metrics.quarantined_lanes,
        metrics.outcome_unknown_attempts,
        metrics.unreconciled_attempts,
        metrics.usage_reconciliation_dead,
        metrics.saturated,
    ))


async def scheduler_health(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    configured_epoch: str | None,
    include_epoch: bool = False,
) -> dict[str, Any]:
    """Aggregate scheduler block; ``available`` False never reports numbers."""

    global _metrics_unavailable_count, _metrics_available
    global _attention_signal_count, _attention_active
    try:
        metrics = await scheduler_metrics_repo.collect(session_factory)
    except Exception as exc:
        state = getattr(exc, "scheduler_state", None)
        # Fail closed only for an active scheduler read from the DB. Disabled
        # (default off) keeps legacy health; an unreadable state row is already
        # the existing database failure, not a second reason.
        blocks_health = state in BLOCKING_STATES
        if _metrics_available:
            _metrics_unavailable_count += 1
            logger.error(
                "Scheduler metrics unavailable | erro_tipo={} state={} blocks_health={} count={} threshold={}",
                type(exc.__cause__ or exc).__name__, state, blocks_health,
                _metrics_unavailable_count, SIGNAL_THRESHOLD,
            )
        _metrics_available = False
        return {
            "available": False,
            "healthy": not blocks_health,
            "state": state,
            "reason": "metrics_unavailable",
            "metrics_unavailable_count": _metrics_unavailable_count,
            "signal_threshold": SIGNAL_THRESHOLD,
            "rejections": rejection_counts(),
        }
    _metrics_available = True
    attention = _attention(metrics)
    if attention and not _attention_active:
        _attention_signal_count += 1
        logger.error(
            "Scheduler attention required | count={} threshold={}",
            _attention_signal_count, SIGNAL_THRESHOLD,
        )
    _attention_active = attention
    block: dict[str, Any] = {
        "available": True,
        "healthy": True,
        "state": metrics.state,
        "version": metrics.version,
        "epoch_configured": bool(configured_epoch),
        "epoch_matches": bool(configured_epoch and configured_epoch == metrics.scheduler_epoch),
        "nonterminal_turns": {
            "count": metrics.nonterminal_turns,
            "bytes": metrics.nonterminal_bytes,
            "cap_turns": metrics.cap_global_turns,
            "cap_bytes": metrics.cap_global_bytes,
        },
        "ready_turns": metrics.ready_turns,
        "oldest_ready_age_seconds": metrics.oldest_ready_age_seconds,
        "lanes": {
            "owned": metrics.owned_lanes,
            "lease_expired": metrics.lease_expired_lanes,
            "quarantined": metrics.quarantined_lanes,
        },
        "unknown_usage": {
            "outcome_unknown_attempts": metrics.outcome_unknown_attempts,
            "started_attempts": metrics.started_attempts,
            "unreconciled_attempts": metrics.unreconciled_attempts,
            "quarantined_agents": metrics.quarantined_agents,
            "quarantined_lanes": metrics.quarantined_lanes,
            "usage_reconciliation_backlog": metrics.usage_reconciliation_backlog,
            "usage_reconciliation_dead": metrics.usage_reconciliation_dead,
        },
        "saturated": list(metrics.saturated),
        "row_bound": scheduler_metrics_repo.METRIC_ROW_BOUND,
        "attention_required": attention,
        "attention_signal_count": _attention_signal_count,
        "metrics_unavailable_count": _metrics_unavailable_count,
        "signal_threshold": SIGNAL_THRESHOLD,
        "rejections": rejection_counts(),
    }
    if include_epoch:
        block["epoch"] = metrics.scheduler_epoch
    return block
