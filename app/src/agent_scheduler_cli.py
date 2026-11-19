"""T08-6/S5: ``delivery_cli agent-scheduler`` container-only operations.

Registered from ``delivery_cli`` in a block labelled T08-6. No HTTP endpoint.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import asdict

from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.database.repositories import agent_scheduler_repo, lifecycle_repo, scheduler_repo
from src.services.provider_auth_bootstrap import (
    provider_auth_readiness,
    reset_provider_auth_snapshot_cache,
)


def register(
    subparsers,
    *,
    uuid_text: Callable[[str], str],
    actor_reason: Callable[[argparse.ArgumentParser], None],
) -> None:
    command = subparsers.add_parser("agent-scheduler")
    actions = command.add_subparsers(dest="agent_scheduler_command", required=True)
    show = actions.add_parser("show")
    show.add_argument("agente_id", type=int)
    # S5 security fix: bounded, read-only discovery for drain opt-out.
    listing = actions.add_parser("list")
    listing.add_argument("--after-id", type=int, default=0)
    listing.add_argument("--limit", type=int, default=100)
    opt_in = actions.add_parser("opt-in")
    opt_in.add_argument("agente_id", type=int)
    opt_in.add_argument("--scheduler-epoch", required=True, type=uuid_text)
    opt_in.add_argument("--delivery-epoch", required=True, type=uuid_text)
    actor_reason(opt_in)
    opt_out = actions.add_parser("opt-out")
    opt_out.add_argument("agente_id", type=int)
    actor_reason(opt_out)


async def provider_auth_enforced() -> tuple[bool, list[str]]:
    """Enforced auth in this container AND a ready provider-auth snapshot.

    There is no durable auth row; the operator container shares the
    installation ``.env`` and the provider readiness snapshot.
    """

    reset_provider_auth_snapshot_cache()
    report = await provider_auth_readiness()
    enforced = bool(
        settings.provider_webhook_auth_mode == "enforced"
        and report.ready
        and report.auth_mode == "enforced"
    )
    return enforced, sorted(report.issues)


async def run(
    args: argparse.Namespace,
    session_factory: Callable[[], AsyncSession],
    emit: Callable[[object], None],
) -> int:
    if args.agent_scheduler_command == "list":
        result = await scheduler_repo.run_transaction_with_deadlock_retry(
            session_factory,
            lambda db: agent_scheduler_repo.list_opted_in(
                db, after_id=args.after_id, limit=args.limit
            ),
        )
        emit(result)
        return 0

    agente_id = args.agente_id
    try:
        if args.agent_scheduler_command == "show":
            enforced, issues = await provider_auth_enforced()
            result = await scheduler_repo.run_transaction_with_deadlock_retry(
                session_factory,
                lambda db: agent_scheduler_repo.show_agent(
                    db, agente_id=agente_id, provider_auth_enforced=enforced
                ),
            )
            result["provider_auth_issues"] = issues
            emit(result)
            return 0
        if args.agent_scheduler_command == "opt-in":
            enforced, issues = await provider_auth_enforced()
            row = await scheduler_repo.run_transaction_with_deadlock_retry(
                session_factory,
                lambda db: agent_scheduler_repo.opt_in_agent(
                    db, agente_id=agente_id, scheduler_epoch=args.scheduler_epoch,
                    delivery_epoch=args.delivery_epoch, provider_auth_enforced=enforced,
                    actor=args.actor, reason=args.reason,
                ),
            )
        else:
            issues = []
            row = await lifecycle_repo.run_in_new_session(
                session_factory,
                lambda db: agent_scheduler_repo.opt_out_agent(
                    db, agente_id=agente_id, actor=args.actor, reason=args.reason,
                ),
            )
    except agent_scheduler_repo.AgentSchedulerRefused as exc:
        emit({
            "status": "refused", "action": exc.action, "agente_id": agente_id,
            "reasons": list(exc.reasons), "provider_auth_issues": issues,
        })
        return 1
    emit({
        "status": "opted_in" if row.inbox_scheduler_enabled else "opted_out",
        **asdict(row),
    })
    return 0
