"""T08-6: guarded, audited per-agent scheduler opt-in/opt-out.

Opt-in runs in its own scheduler control UoW (``begin_scheduler_control``,
READ COMMITTED, first statement) and takes only locking reads in canonical
order ``delivery_runtime_state (S) -> scheduler_runtime_state (X) ->
agentes (X)``. Every guard is evaluated on those fresh locked rows; local
process flags never authorize alone. The scheduler X lock serializes opt-in
with every audited scheduler transition, so no opt-in commits after a
committed ``enabled->draining``. Opt-out reuses the fenced
``lifecycle_repo.update_agent`` (refused while scheduled work is pending).
No public HTTP endpoint exposes either path.
"""

from __future__ import annotations

from dataclasses import dataclass
import json

from loguru import logger
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    Agente,
    DeliveryOperatorAction,
    DeliveryRuntimeState,
    SchedulerRuntimeState,
)
from src.database.repositories import (
    delivery_state_repo,
    lifecycle_repo,
    scheduler_repo,
)


SIGNAL_THRESHOLD = 1
AUDIT_REASON_MAX = 500
# Allowlist: opt-in is permitted only when every value below matches.
OPT_IN_SCHEDULER_STATES = frozenset({"enabled"})
OPT_IN_RUNTIME_MODES = frozenset({"outbox"})
OPT_IN_REFUSALS = (
    "agent_not_found",
    "scheduler_not_enabled",
    "scheduler_epoch_mismatch",
    "runtime_not_outbox",
    "delivery_epoch_mismatch",
    "provider_auth_not_enforced",
    "agent_inactive",
    "chatwoot_inbox_missing",
    "agent_already_opted_in",
)
OPT_OUT_REFUSALS = (
    "agent_not_found",
    "agent_not_opted_in",
    "scheduled_work_pending",
)

_refusal_counts: dict[str, int] = {"opt_in": 0, "opt_out": 0}


class AgentSchedulerRefused(lifecycle_repo.LifecycleRefused):
    """Named guard refusal; nothing was changed or audited."""

    def __init__(self, action: str, reasons: tuple[str, ...]) -> None:
        self.action = action
        self.reasons = reasons
        super().__init__(f"agent scheduler {action} refused: " + ",".join(reasons))


def refusal_counts() -> dict[str, int]:
    return dict(_refusal_counts)


def _refuse(action: str, agente_id: int, reasons: tuple[str, ...]) -> AgentSchedulerRefused:
    _refusal_counts[action] += 1
    logger.error(
        "Agent scheduler {} refused | agente={} reasons={} count={} threshold={}",
        action, agente_id, ",".join(reasons), _refusal_counts[action], SIGNAL_THRESHOLD,
    )
    return AgentSchedulerRefused(action, reasons)


def opt_in_refusals(
    *,
    runtime: DeliveryRuntimeState,
    scheduler: SchedulerRuntimeState,
    agent: Agente | None,
    scheduler_epoch: str,
    delivery_epoch: str,
    provider_auth_enforced: bool,
) -> tuple[str, ...]:
    """Every failed guard, in ``OPT_IN_REFUSALS`` order; empty means allowed."""

    if agent is None:
        return ("agent_not_found",)
    failed: list[str] = []
    if scheduler.state not in OPT_IN_SCHEDULER_STATES:
        failed.append("scheduler_not_enabled")
    if scheduler.scheduler_epoch != scheduler_epoch:
        failed.append("scheduler_epoch_mismatch")
    if runtime.mode not in OPT_IN_RUNTIME_MODES:
        failed.append("runtime_not_outbox")
    if runtime.cutover_epoch != delivery_epoch:
        failed.append("delivery_epoch_mismatch")
    if provider_auth_enforced is not True:
        failed.append("provider_auth_not_enforced")
    if agent.ativo is not True:
        failed.append("agent_inactive")
    if agent.chatwoot_inbox_id is None:
        failed.append("chatwoot_inbox_missing")
    if agent.inbox_scheduler_enabled is not False:
        failed.append("agent_already_opted_in")
    return tuple(failed)


async def _lock_agent(db: AsyncSession, agente_id: int, *, share: bool) -> Agente | None:
    query = (
        select(Agente)
        .where(Agente.id == agente_id)
        .with_for_update(read=share)
        .execution_options(populate_existing=True)
    )
    return (await db.execute(query)).scalar_one_or_none()


def _audit_reason(payload: dict[str, object]) -> str:
    value = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    if len(value) > AUDIT_REASON_MAX:
        raise ValueError("agent scheduler audit reason exceeds 500 characters")
    return value


def _metadata(actor: str, reason: str) -> tuple[str, str]:
    actor, reason = delivery_state_repo.validate_audit_metadata(actor=actor, reason=reason)
    if len(actor) > 255:
        raise ValueError("invalid agent scheduler audit metadata")
    return actor, reason


@dataclass(frozen=True)
class AgentSchedulerResult:
    agente_id: int
    inbox_scheduler_enabled: bool
    scheduler_state: str
    scheduler_epoch: str
    scheduler_version: int
    runtime_mode: str
    delivery_epoch: str


async def opt_in_agent(
    db: AsyncSession,
    *,
    agente_id: int,
    scheduler_epoch: str,
    delivery_epoch: str,
    provider_auth_enforced: bool,
    actor: str,
    reason: str,
) -> AgentSchedulerResult:
    """Guarded opt-in; must open its own transaction (scheduler control UoW)."""

    actor, reason = _metadata(actor, reason)
    await scheduler_repo.begin_scheduler_control(db)
    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)
    scheduler = await scheduler_repo.get_scheduler_state(db, for_update=True)
    agent = await _lock_agent(db, agente_id, share=False)
    failed = opt_in_refusals(
        runtime=runtime, scheduler=scheduler, agent=agent,
        scheduler_epoch=scheduler_epoch, delivery_epoch=delivery_epoch,
        provider_auth_enforced=provider_auth_enforced,
    )
    if failed:
        raise _refuse("opt_in", agente_id, failed)
    audit = _audit_reason({
        "v": 1, "agente_id": agente_id, "from": False, "to": True,
        "scheduler_state": scheduler.state, "scheduler_epoch": scheduler.scheduler_epoch,
        "scheduler_version": scheduler.version, "delivery_mode": runtime.mode,
        "delivery_epoch": runtime.cutover_epoch, "reason": reason,
    })
    # No FK parent: the audit insert takes no lock outside the canonical prefix.
    db.add(DeliveryOperatorAction(
        delivery_id=None, action="agent_scheduler:opt_in", actor=actor, reason=audit,
    ))
    agent.inbox_scheduler_enabled = True
    await db.flush()
    return AgentSchedulerResult(
        agente_id, True, scheduler.state, scheduler.scheduler_epoch,
        scheduler.version, runtime.mode, runtime.cutover_epoch,
    )


async def opt_out_agent(
    db: AsyncSession,
    *,
    agente_id: int,
    actor: str,
    reason: str,
) -> AgentSchedulerResult:
    """Opt-out through the fenced lifecycle update (409 with pending work).

    Runs under ``lifecycle_repo.run_with_deadlock_retry``: its discovery read
    view is closed before the canonical lifecycle prefix is locked.
    """

    actor, reason = _metadata(actor, reason)
    previous: list[bool] = []

    def prepare(agent: Agente, values: dict[str, object]) -> dict[str, object]:
        # Fresh locked row, evaluated after every lock wait.
        previous.append(bool(agent.inbox_scheduler_enabled))
        if agent.inbox_scheduler_enabled is not True:
            raise _refuse("opt_out", agente_id, ("agent_not_opted_in",))
        return values

    try:
        result = await lifecycle_repo.update_agent(
            db, agente_id=agente_id, payload={"inbox_scheduler_enabled": False},
            prepare=prepare,
        )
    except AgentSchedulerRefused:
        raise
    except lifecycle_repo.LifecycleRefused:
        raise _refuse("opt_out", agente_id, ("scheduled_work_pending",)) from None
    if result is None:
        raise _refuse("opt_out", agente_id, ("agent_not_found",))
    # Already held by the lifecycle prefix in the same modes: no S->X upgrade.
    runtime = await delivery_state_repo.get_runtime_state(db, for_share=True)
    scheduler = await scheduler_repo.get_scheduler_state(db, for_update=True)
    db.add(DeliveryOperatorAction(
        delivery_id=None, action="agent_scheduler:opt_out", actor=actor,
        reason=_audit_reason({
            "v": 1, "agente_id": agente_id, "from": previous[-1], "to": False,
            "scheduler_state": scheduler.state, "scheduler_epoch": scheduler.scheduler_epoch,
            "scheduler_version": scheduler.version, "delivery_mode": runtime.mode,
            "delivery_epoch": runtime.cutover_epoch, "reason": reason,
        }),
    ))
    await db.flush()
    return AgentSchedulerResult(
        agente_id, False, scheduler.state, scheduler.scheduler_epoch,
        scheduler.version, runtime.mode, runtime.cutover_epoch,
    )


async def show_agent(
    db: AsyncSession,
    *,
    agente_id: int,
    provider_auth_enforced: bool,
) -> dict[str, object]:
    """S2: non-authoritative READ ONLY snapshot; never blocks intake."""
    from src.database.repositories.scheduler_diagnostics_repo import begin_snapshot

    await begin_snapshot(db)
    runtime = await delivery_state_repo.get_runtime_state(db)
    scheduler = await scheduler_repo.get_scheduler_state(db)
    agent = (await db.execute(
        select(Agente).where(Agente.id == agente_id)
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    blockers = opt_in_refusals(
        runtime=runtime, scheduler=scheduler, agent=agent,
        scheduler_epoch=scheduler.scheduler_epoch, delivery_epoch=runtime.cutover_epoch,
        provider_auth_enforced=provider_auth_enforced,
    )
    return {
        "diagnostic_only": True,  # S2: blockers are not opt-in authorization.
        "consistency": "read_only_snapshot",
        "agente_id": agente_id,
        "found": agent is not None,
        "inbox_scheduler_enabled": bool(agent and agent.inbox_scheduler_enabled),
        "ativo": bool(agent and agent.ativo),
        "chatwoot_inbox_present": bool(agent and agent.chatwoot_inbox_id is not None),
        "scheduler_state": scheduler.state,
        "scheduler_epoch": scheduler.scheduler_epoch,
        "scheduler_version": scheduler.version,
        "runtime_mode": runtime.mode,
        "delivery_epoch": runtime.cutover_epoch,
        "provider_auth_enforced": provider_auth_enforced,
        "opt_in_blockers": list(blockers),
    }


async def list_opted_in(
    db: AsyncSession,
    *,
    after_id: int = 0,
    limit: int = 100,
) -> dict[str, object]:
    """S5 diagnostic: bounded opted-in snapshot, READ ONLY and lock-free."""

    if after_id < 0:
        raise ValueError("after_id cannot be negative")
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    # First SQL statement. The following plain SELECT uses MVCC and takes no
    # row lock, so listing cannot block lifecycle/intake writers.
    await db.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"))
    rows = (
        await db.execute(
            select(Agente.id, Agente.ativo, Agente.chatwoot_inbox_id)
            .where(
                Agente.inbox_scheduler_enabled.is_(True),
                Agente.id > after_id,
            )
            .order_by(Agente.id)
            .limit(limit + 1)
        )
    ).all()
    has_more = len(rows) > limit
    page = rows[:limit]
    return {
        "status": "ok",
        "agents": [
            {
                "agente_id": int(row.id),
                "ativo": bool(row.ativo),
                "chatwoot_inbox_present": row.chatwoot_inbox_id is not None,
            }
            for row in page
        ],
        "after_id": after_id,
        "limit": limit,
        "has_more": has_more,
        "next_after_id": int(page[-1].id) if has_more and page else None,
    }
