#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the ticket 07 matrix.

Each case removes exactly one protection from a copy of ``app/`` and names the
test that must fail against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T07 = "tests.test_lifecycle_mysql.LifecycleMySQLTests."
T04 = "tests.test_turn_processor_mysql.TurnProcessorMySQLTests."
LIFECYCLE = "app/src/database/repositories/lifecycle_repo.py"
SCHEDULER = "app/src/database/repositories/scheduler_repo.py"
DELIVERY = "app/src/database/repositories/turn_delivery_repo.py"
GENERATION = "app/src/database/repositories/turn_generation_repo.py"
WORKER = "app/src/database/repositories/delivery_worker_repo.py"
PROCESSOR = "app/src/services/turn_processor.py"
SESSIONS = "app/src/routers/sessoes.py"


def _skip_settle(reason: str, indent: str) -> tuple[str, str]:
    """Replace one cancel call by an inert dict with the same arguments."""

    old = (
        f"{indent}outcome = await cancel_scheduled_turns_locked(\n"
        f"{indent}    db,\n"
        f"{indent}    graph=graph,\n"
        f"{indent}    lane=prefix.lane,\n"
        f'{indent}    reason="{reason}",\n'
    )
    new = (
        f"{indent}outcome = SettleOutcome()\n"
        f"{indent}_unused = dict(\n"
        f"{indent}    db=db,\n"
        f"{indent}    graph=graph,\n"
        f"{indent}    lane=prefix.lane,\n"
        f'{indent}    reason="{reason}",\n'
    )
    return old, new


CASES = {
    # FU-2 / review r5: parked handoff with exact lane ownership.
    "abandon_owned_lane_not_released": (
        T07 + "test_abandon_parked_handoff_releases_owned_lane_and_claim", LIFECYCLE,
        "            if _owns_lane(prefix.lane, turn):\n",
        "            if _owns_lane(prefix.lane, turn) and False:\n",
    ),
    "abandon_intent_purged_early": (
        T07 + "test_live_abandon_finishes_after_owner_delivery", WORKER,
        "              AND (action <> 'turn_resolve:abandon' OR delivery_id IS NULL)",
        "              AND TRUE",
    ),
    "abandon_intent_lost_after_io": (
        T07 + "test_live_abandon_finishes_after_owner_delivery", DELIVERY,
        "    if abandoned is not None:", "    if False:",
    ),
    "operator_expired_lease_not_settled": (
        T07 + "test_processing_cancel_matrix", LIFECYCLE,
        "        cancel_open_effects_locked([delivery], now=now, turn_id=delivery.conversation_turn_id)",
        "        delivery.cancel_requested_at = delivery.cancel_requested_at or now",
    ),
    "abandon_expired_effect_refused": (
        T07 + "test_handoff_processing_cancel_matrix", LIFECYCLE,
        '    if resolution == "retry" and processing:',
        '    if processing:',
    ),
    "consumption_reuse_not_checked": (
        T07 + "test_one_ledger_row_cannot_reconcile_two_attempts", SCHEDULER,
        "            and _generation_link_duplicate(exc, values)\n",
        "            and False\n",
    ),
    "retention_skip_not_counted": (
        T07 + "test_retention_skips_limit_plus_one_parked_rows_and_purges_victim", WORKER,
        "    if skipped:\n        _retention_nonterminal_skips += skipped",
        "    skipped = 0\n    if skipped:\n        _retention_nonterminal_skips += skipped",
    ),
    "health_lifecycle_hidden": (
        T07 + "test_app_health_exposes_local_lifecycle_counts", "app/src/main.py",
        '        "lifecycle": lifecycle,\n        "delivery_ready": delivery_ready,',
        '        "delivery_ready": delivery_ready,',
    ),
    "deadlock_retry_not_logged": (
        T07 + "test_lifecycle_transaction_retries_after_real_deadlock", LIFECYCLE,
        '                _signal("deadlock_retried", "Lifecycle transaction lost; retrying")',
        '                _counts["deadlock_retried"] += 1',
    ),
    "seal_revision_range_drops_hint": (T07 + "test_dormant_primitives_use_session_hint", SCHEDULER, 'async def seal_revision_range(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    expected_input_version: int,\n) -> TurnRevision:\n    _, _, lane, _, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=sessao_id,', 'async def seal_revision_range(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    expected_input_version: int,\n) -> TurnRevision:\n    _, _, lane, _, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=None,'),
    "start_generation_attempt_drops_hint": (T07 + "test_dormant_primitives_use_session_hint", SCHEDULER, 'async def start_generation_attempt(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    attempt_type: Literal["response", "summary"],\n    modelo_llm: str,\n) -> GenerationAttempt:\n    _, scheduler, _, _, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=sessao_id,', 'async def start_generation_attempt(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    attempt_type: Literal["response", "summary"],\n    modelo_llm: str,\n) -> GenerationAttempt:\n    _, scheduler, _, _, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=None,'),
    "admit_revision_drops_hint": (T07 + "test_dormant_primitives_use_session_hint", SCHEDULER, 'async def admit_revision(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    expected_first_input_sequence: int,\n    expected_last_input_sequence: int,\n) -> bool:\n    _uuid(delivery_runtime_epoch, "delivery_runtime_epoch")\n    runtime, _, lane, _, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=sessao_id,', 'async def admit_revision(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    expected_first_input_sequence: int,\n    expected_last_input_sequence: int,\n) -> bool:\n    _uuid(delivery_runtime_epoch, "delivery_runtime_epoch")\n    runtime, _, lane, _, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=None,'),
    "yield_turn_for_retry_drops_hint": (T07 + "test_dormant_primitives_use_session_hint", SCHEDULER, 'async def yield_turn_for_retry(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    retry_at: datetime,\n) -> bool:\n    _, scheduler, lane, session, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=sessao_id,', 'async def yield_turn_for_retry(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    retry_at: datetime,\n) -> bool:\n    _, scheduler, lane, session, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=None,'),
    "complete_turn_and_release_lane_drops_hint": (T07 + "test_dormant_primitives_use_session_hint", SCHEDULER, 'async def complete_turn_and_release_lane(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    terminal_status: Literal["delivered", "handoff", "cancelled", "dead"],\n    terminal_reason: str,\n    promote_successor: bool,\n) -> ConversationTurn:\n    """Finalize only the exact owner/revision and optionally promote one successor."""\n\n    _, _, lane, session, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=sessao_id,', 'async def complete_turn_and_release_lane(\n    db: AsyncSession,\n    *,\n    turn_id: int,\n    sessao_id: int,\n    revision_id: int,\n    agente_id: int,\n    scheduler_epoch: str,\n    delivery_runtime_epoch: str,\n    owner_token: str,\n    terminal_status: Literal["delivered", "handoff", "cancelled", "dead"],\n    terminal_reason: str,\n    promote_successor: bool,\n) -> ConversationTurn:\n    """Finalize only the exact owner/revision and optionally promote one successor."""\n\n    _, _, lane, session, turn = await _lock_owned_turn(\n        db,\n        turn_id=turn_id,\n        sessao_id=None,'),
    # ── lifecycle cancellation ──────────────────────────────────────────
    "transfer_cancels_turns": (
        T07 + "test_transfer_cancels_queued_reply_and_successor_in_same_uow",
        LIFECYCLE,
        *_skip_settle("session_transferred", "    "),
    ),
    "resume_cancels_turns": (
        T07 + "test_resume_after_admitted_handoff_settles_in_same_uow",
        LIFECYCLE,
        *_skip_settle("session_resumed", "    "),
    ),
    "disable_cancels_turns": (
        T07 + "test_disable_cancels_every_session_and_reenable_never_replays",
        LIFECYCLE,
        *_skip_settle("agent_disabled", "        "),
    ),
    "opt_out_refused_with_pending_work": (
        T07 + "test_opt_out_with_pending_scheduled_work_is_refused",
        LIFECYCLE,
        "        if opting_out and not disabling and graph.turns:\n",
        "        if False:\n",
    ),
    "resume_supersedes_attempt": (
        T07 + "test_resume_after_admitted_handoff_settles_in_same_uow",
        LIFECYCLE,
        '        current.status = "superseded"\n',
        "        pass\n",
    ),
    "inflight_generation_keeps_lane": (
        T07 + "test_transfer_during_active_generation_keeps_lane_for_accounting",
        LIFECYCLE,
        "        in_flight = owns and any(\n",
        "        in_flight = False and any(\n",
    ),
    "live_effect_only_fenced": (
        T07 + "test_transfer_during_claimed_send_fences_and_records_delivery",
        LIFECYCLE,
        "            if row.lock_until is not None and row.lock_until > now:\n                live = True\n",
        "            if False:\n                live = True\n",
    ),
    "live_effect_cancel_requested": (
        T07 + "test_disable_during_claimed_send_fences_and_records_delivery",
        LIFECYCLE,
        "            if row.cancel_requested_at is None:\n"
        "                row.cancel_requested_at = now\n"
        "            if row.lock_until is not None",
        "            if row.lock_until is not None",
    ),
    "queued_effects_cancelled": (
        T07 + "test_transfer_cancels_queued_reply_and_successor_in_same_uow",
        LIFECYCLE,
        "        if row.status in QUEUED_EFFECT_STATUSES:\n            row.status = \"cancelled\"\n",
        "        if False:\n            row.status = \"cancelled\"\n",
    ),
    "owner_fence_cancels_formally": (
        T04 + "test_lifecycle_refusal_makes_no_provider_call",
        GENERATION,
        "        return await _cancel_for_lifecycle_locked(db, graph)\n",
        "        return await _release_locked(\n"
        "            db, graph, delay_seconds=policy.release_backoff_seconds,\n"
        '            reason="lifecycle_refused",\n'
        "        )\n",
    ),
    "owner_prelocks_successor": (
        T07 + "test_owner_lifecycle_cancel_prelocks_successor_before_revisions",
        GENERATION,
        "    graph = await _lock_owned_graph(db, lease, lock_successor=True)\n",
        "    graph = await _lock_owned_graph(db, lease)\n",
    ),
    "recover_cancelled_turn_lane": (
        T07 + "test_transfer_during_active_generation_keeps_lane_for_accounting",
        SCHEDULER,
        '    if turn.session_slot == "terminal":\n'
        "        return await _recover_lane_of_cancelled_turn(\n",
        "    if False:\n"
        "        return await _recover_lane_of_cancelled_turn(\n",
    ),
    "pending_write_guard": (
        T07 + "test_pending_write_is_refused_never_overwritten",
        LIFECYCLE,
        "    assert_no_pending_writes(db)\n    lifecycle_hint = (\n",
        "    lifecycle_hint = (\n",
    ),
    "fresh_locked_rows": (
        T07 + "test_update_derives_fields_from_fresh_row_not_preloaded_identity",
        LIFECYCLE,
        "        .with_for_update()\n"
        "        .execution_options(populate_existing=True)\n"
        "    )\n"
        "    if order is not None:\n",
        "        .with_for_update()\n"
        "    )\n"
        "    if order is not None:\n",
    ),
    "whole_transaction_retry": (
        T07 + "test_lifecycle_transaction_retries_after_real_deadlock",
        LIFECYCLE,
        "            lost = db_errors.transaction_lost_dbapi_error(exc)\n",
        "            lost = None\n",
    ),
    # ── deletes ─────────────────────────────────────────────────────────
    "session_delete_history_409": (
        T07 + "test_delete_with_scheduler_history_returns_409_and_keeps_rows",
        LIFECYCLE,
        "    if await _has_scheduler_history(db, ConversationTurn.sessao_id == session.id, index=\"idx_turn_session_status\"):\n",
        "    if False:\n",
    ),
    "agent_delete_history_409": (
        T07 + "test_delete_with_scheduler_history_returns_409_and_keeps_rows",
        LIFECYCLE,
        "    if await _has_scheduler_history(db, ConversationTurn.agente_id == agente_id, index=\"idx_turn_ready_claim\"):\n",
        "    if False:\n",
    ),
    "restrict_dependent_409": (
        T07 + "test_delete_restrict_dependent_is_409_not_500",
        SESSIONS,
        "    return bool({1451, 1217} & set(db_errors.mysql_error_codes(exc)))\n",
        "    return False\n",
    ),
    # ── operator ────────────────────────────────────────────────────────
    "operator_cancel_settles_turn": (
        T07 + "test_operator_cancel_automated_settles_turn_and_promotes",
        LIFECYCLE,
        '    if turn is not None and delivery.status == "cancelled":\n',
        "    if False:\n",
    ),
    "legacy_cancel_refuses_automated": (
        T07 + "test_legacy_operator_paths_refuse_automated_rows",
        WORKER,
        '    if delivery.delivery_class in {"automated"}:\n        # Ticket07',
        "    if False:\n        # Ticket07",
    ),
    "turn_resolve_current_generation": (
        T07 + "test_turn_resolve_retry_requires_current_handoff_generation",
        LIFECYCLE,
        "            and _handoff_current(session, attempt)\n",
        "",
    ),
    "turn_resolve_duplicate_risk": (
        T07 + "test_turn_resolve_ambiguous_effect_requires_explicit_risk",
        LIFECYCLE,
        "            if row.ambiguity_count > 0 and not accept_duplicate_risk:\n",
        "            if False:\n",
    ),
    "turn_resolve_resets_cap": (
        T07 + "test_turn_resolve_retry_rearms_parked_handoff_reply",
        LIFECYCLE,
        "        turn.delivery_attempt_count = 0\n    else:\n",
        "    else:\n",
    ),
    "turn_resolve_abandon_terminal": (
        T07 + "test_turn_resolve_abandon_terminalizes_parked_turn",
        LIFECYCLE,
        '            _terminalize(turn, reason="operator_abandoned", now=now)\n',
        "            pass  # mutation: omit terminal settlement\n",
    ),
    # ── generation quarantine ───────────────────────────────────────────
    "quarantine_failed_requires_risk": (
        T07 + "test_quarantine_failed_resolution_releases_lane_only_after_resolve",
        LIFECYCLE,
        "        if not accept_duplicate_spend_risk:\n",
        "        if False:\n",
    ),
    "quarantine_succeeded_requires_ledger": (
        T07 + "test_quarantine_succeeded_requires_exact_ledger_row",
        LIFECYCLE,
        '    if outcome == "succeeded" and consumo_llm_id is None:\n',
        "    if False:\n",
    ),
    "quarantine_lane_held_until_resolve": (
        T07 + "test_quarantine_failed_resolution_releases_lane_only_after_resolve",
        GENERATION,
        "            ~held_by_quarantine,\n"
        "        )\n"
        "        .order_by(AgentServiceLane.lease_until, AgentServiceLane.agente_id)\n"
        "        .limit(limit)\n"
        "    )\n"
        "    return [(int(",
        "        )\n"
        "        .order_by(AgentServiceLane.lease_until, AgentServiceLane.agente_id)\n"
        "        .limit(limit)\n"
        "    )\n"
        "    return [(int(",
    ),
    # ── runtime gate before I/O ─────────────────────────────────────────
    "runtime_gate_releases_on_prefix_error": (
        T07 + "test_preio_runtime_gate_frozen_releases_claim",
        DELIVERY,
        "        # canonical order. No I/O happened: release without ambiguity.\n",
        "        # canonical order. No I/O happened: release without ambiguity.\n        raise\n",
    ),
    "runtime_gate_epoch_releases": (
        T07 + "test_preio_runtime_gate_divergent_epoch_releases_claim",
        DELIVERY,
        "        # canonical order. No I/O happened: release without ambiguity.\n",
        "        # canonical order. No I/O happened: release without ambiguity.\n        raise\n",
    ),
    "scheduler_gate_releases": (
        T07 + "test_preio_scheduler_gate_releases_claim",
        DELIVERY,
        '    if refusal == "runtime":\n        return await _release_claim_at_runtime_gate(\n',
        '    if refusal == "runtime":\n        return False\n        return await _release_claim_at_runtime_gate(\n',
    ),
    "runtime_gate_release_not_ambiguous": (
        T07 + "test_preio_runtime_gate_frozen_releases_claim",
        DELIVERY,
        '    delivery.last_error_code = "preio_refused_runtime"\n',
        '    delivery.last_error_code = "preio_refused_runtime"\n    delivery.ambiguity_count += 1\n',
    ),
    # ── §16.1 A1 hints ──────────────────────────────────────────────────
    "claim_uses_hint": (
        T07 + "test_hinted_claim_recover_and_renew_take_only_locking_reads",
        SCHEDULER,
        "    if candidate is not None:\n        return await _claim_hinted_turn(\n",
        "    if False:\n        return await _claim_hinted_turn(\n",
    ),
    "renew_uses_claim_identity": (
        T07 + "test_hinted_claim_recover_and_renew_take_only_locking_reads",
        SCHEDULER,
        "    if sessao_id is None:\n        # Direct callers",
        "    if True:\n        # Direct callers",
    ),
    "recover_uses_hint": (
        T07 + "test_hinted_claim_recover_and_renew_take_only_locking_reads",
        SCHEDULER,
        "    if expected_sessao_id is not None:\n",
        "    if False:\n",
    ),
    "legacy_claim_owner_fence": (
        T07 + "test_owned_lane_refuses_hinted_and_legacy_claims",
        SCHEDULER,
        "    if lane.owner_token is not None:\n"
        "        # Expiry never transfers authority.",
        "    if False and lane.owner_token is not None:\n"
        "        # Expiry never transfers authority.",
    ),
    "hinted_claim_owner_fence": (
        T07 + "test_owned_lane_refuses_hinted_and_legacy_claims",
        SCHEDULER,
        "    if lane.owner_token is not None:\n"
        "        return None\n"
        "    turn_id, sessao_id = candidate\n",
        "    if False and lane.owner_token is not None:\n"
        "        return None\n"
        "    turn_id, sessao_id = candidate\n",
    ),
    "hinted_claim_head_of_line": (
        T07 + "test_stale_hints_are_refused_under_locks",
        SCHEDULER,
        "    if quarantined is not None or head != turn.id:\n",
        "    if quarantined is not None:\n",
    ),
    "hinted_claim_quarantine": (
        T07 + "test_quarantined_agent_hint_is_not_claimed",
        SCHEDULER,
        "    if quarantined is not None or head != turn.id:\n",
        "    if head != turn.id:\n",
    ),
    "recover_rejects_stale_hint": (
        T07 + "test_stale_hints_are_refused_under_locks",
        SCHEDULER,
        "        if expected_turn_id != lane.active_turn_id:\n",
        "        if False:\n",
    ),
    "owned_turn_checks_session": (
        T07 + "test_stale_hints_are_refused_under_locks",
        SCHEDULER,
        "        or turn.sessao_id != sessao_id\n        or lane.owner_token != owner_token\n",
        "        or lane.owner_token != owner_token\n",
    ),
    "processor_claim_hint": (
        T07 + "test_processor_passes_discovery_hints_to_every_lock_transaction",
        PROCESSOR,
        "                    candidate=hint,\n",
        "                    candidate=None,\n",
    ),
    "processor_renew_identity": (
        T07 + "test_processor_passes_discovery_hints_to_every_lock_transaction",
        PROCESSOR,
        "                        sessao_id=lease.sessao_id,\n",
        "",
    ),
    "processor_recover_hint": (
        T07 + "test_processor_recovers_expired_lanes_with_discovery_hints",
        PROCESSOR,
        "                expected_sessao_id=hint[1] if hint else None,\n",
        "                expected_sessao_id=None,\n",
    ),
    # ── retention ───────────────────────────────────────────────────────
    "retention_requires_terminal_turn": (
        T07 + "test_retention_skips_limit_plus_one_parked_rows_and_purges_victim",
        WORKER,
        '            turn.c.session_slot == "terminal",\n',
        "",
    ),
    "bulk_scrub_excludes_automated": (
        T07 + "test_retention_skips_limit_plus_one_parked_rows_and_purges_victim",
        WORKER,
        "              AND JSON_LENGTH(payload) > 0\n"
        "              AND delivery_class <> 'automated'\n",
        "              AND JSON_LENGTH(payload) > 0\n",
    ),
    "bulk_delete_excludes_automated": (
        T07 + "test_retention_skips_limit_plus_one_parked_rows_and_purges_victim",
        WORKER,
        "              AND delivery_class <> 'automated'\n"
        "              AND id NOT IN (\n",
        "              AND id NOT IN (\n",
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
    raise SystemExit("usage: inbox_ticket07_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
