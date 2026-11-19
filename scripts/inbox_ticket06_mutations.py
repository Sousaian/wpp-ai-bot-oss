#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the ticket 06 matrix.

Each case removes exactly one protection from a copy of ``app/`` and names the
test that must fail against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


T06 = "tests.test_turn_delivery_mysql.TurnDeliveryMySQLTests."
T05 = "tests.test_handoff_terminal_mysql.HandoffTerminalMySQLTests."
UNIT = "tests.test_turn_processor_unit."
WORKER_REPO = "app/src/database/repositories/delivery_worker_repo.py"
DELIVERY_REPO = "app/src/database/repositories/turn_delivery_repo.py"
SCHEDULER_REPO = "app/src/database/repositories/scheduler_repo.py"
HANDOFF_REPO = "app/src/database/repositories/turn_handoff_repo.py"
GENERATION_REPO = "app/src/database/repositories/turn_generation_repo.py"

_FALLBACK_INTENT_OLD = (
    "        and revision.draft_kind in {\n"
    "            None,\n"
    '            "soft_limit_fallback",\n'
    '            "technical_fallback",\n'
    "        }\n"
)
_FALLBACK_INTENT_NEW = "        and revision.draft_kind is None\n"


CASES = {
    # ── initial delivery (b0a80f2, updated for round 1 code) ────────────
    "claim_requires_lane_fence": (
        T06 + "test_claim_skips_automated_rows_without_live_lane_fence",
        WORKER_REPO,
        "          d.delivery_class <> 'automated'\n          OR EXISTS (\n",
        "          TRUE\n          OR EXISTS (\n",
    ),
    "preio_lane_owner_token": (
        T06 + "test_committed_claim_is_rejected_before_io_after_lane_loss",
        SCHEDULER_REPO,
        "        or turn.claim_token != lane.owner_token\n",
        "",
    ),
    "discovery_requires_open_delivered": (
        T06 + "test_discovery_requires_open_delivered_for_transition",
        DELIVERY_REPO,
        '            & (opened.c.status == "delivered")\n',
        "",
    ),
    "discovery_excludes_stuck_reply": (
        T06 + "test_handoff_discovery_is_bounded_and_skips_stuck_rows",
        DELIVERY_REPO,
        "            reply.c.id.is_not(None) & dispatchable(reply),\n",
        "            reply.c.id.is_not(None),\n",
    ),
    "admitted_handoff_continuation": (
        T06 + "test_handoff_reply_precedes_transition_released_only_after_open",
        GENERATION_REPO,
        "    if await _admitted_handoff_delivery(db, graph):\n",
        "    if False:\n",
    ),
    "reply_before_transition_selection": (
        T06 + "test_handoff_transition_waits_for_reply_delivery",
        DELIVERY_REPO,
        '        if reply is not None and reply.status not in {"delivered", "dead", "cancelled"}:\n'
        "            selected = reply\n",
        "        if False:\n            selected = reply\n",
    ),
    "empty_reply_has_no_row_or_gap": (
        T05 + "test_transfer_only_stages_transition_without_reply_or_sequence_gap",
        HANDOFF_REPO,
        "    sequence_count = 2 if reply_text else 1\n",
        "    sequence_count = 2\n",
    ),
    "reply_sequence_precedes_transition": (
        T06 + "test_handoff_reply_precedes_transition_released_only_after_open",
        HANDOFF_REPO,
        "            stream_sequence=effect_sequences[0],\n",
        "            stream_sequence=effect_sequences[-1] + 1,\n",
    ),
    "settle_admitted_on_lane_expiry": (
        T06 + "test_admitted_handoff_expired_lane_cap_parks_without_orphaning",
        SCHEDULER_REPO,
        '        cap_path="expired_lane",\n    )\n    if admitted is not None:\n',
        '        cap_path="expired_lane",\n    )\n    if False:\n',
    ),
    "settle_admitted_on_yield": (
        T05 + "test_admitted_handoff_yield_cap_parks_without_orphaning_effects",
        SCHEDULER_REPO,
        '            cap_path="yield_retry",\n        )\n        is not None\n',
        '            cap_path="yield_retry",\n        )\n        is None and False\n',
    ),
    "fallback_keeps_intent_recovery": (
        T06 + "test_recovery_cap_rev2_technical_fallback_keeps_handoff",
        SCHEDULER_REPO,
        _FALLBACK_INTENT_OLD,
        _FALLBACK_INTENT_NEW,
    ),
    "fallback_keeps_intent_yield": (
        T06 + "test_yield_cap_rev2_soft_limit_fallback_keeps_handoff",
        SCHEDULER_REPO,
        _FALLBACK_INTENT_OLD,
        _FALLBACK_INTENT_NEW,
    ),
    "fallback_keeps_intent_unit": (
        UNIT + "DraftClassificationTests.test_only_generated_rev2_overrides_carried_handoff_intent_at_cap",
        SCHEDULER_REPO,
        _FALLBACK_INTENT_OLD,
        _FALLBACK_INTENT_NEW,
    ),
    "admission_recovers_expired_claim": (
        T06 + "test_expired_processing_effect_is_recovered_by_lane_admission",
        DELIVERY_REPO,
        '    if selected.status == "processing":\n',
        '    if selected.status == "processing" and False:\n',
    ),
    "admission_terminalizes_expired_effect": (
        T06 + "test_expired_effect_at_admission_terminalizes_ordinary_turn",
        DELIVERY_REPO,
        '    if selected.status in {"pending", "retry"} and (\n'
        "        selected.expires_at is None or selected.expires_at <= now\n"
        "    ):\n",
        "    if False:\n",
    ),
    "failed_send_positive_backoff": (
        T06 + "test_failed_send_yields_to_tail_and_other_agents_progress",
        DELIVERY_REPO,
        "        retry_delay_seconds = max(MIN_RETRY_SECONDS, retry_delay_seconds)\n",
        "        pass\n",
    ),
    "failed_send_requeues_at_tail": (
        T06 + "test_failed_send_yields_to_tail_and_other_agents_progress",
        DELIVERY_REPO,
        "            turn.scheduling_sequence = graph.lane.next_scheduling_sequence\n"
        "            graph.lane.next_scheduling_sequence += 1\n",
        "",
    ),
    "dead_handoff_reply_parks": (
        T06 + "test_dead_handoff_reply_parks_and_never_releases_transition",
        DELIVERY_REPO,
        "    if handoff:\n"
        "        release_waiting()\n"
        "        _handoff_effect_terminal_count += 1\n",
        "    if False:\n"
        "        release_waiting()\n"
        "        _handoff_effect_terminal_count += 1\n",
    ),
    # ── round 1: P2-1 waiting for a claim never spends the cap ───────────
    "lane_expiry_never_counts": (
        T06 + "test_lane_expiries_waiting_for_claim_never_spend_the_cap",
        SCHEDULER_REPO,
        "    if turn.delivery_attempt_count < scheduler.max_delivery_attempts_per_turn:\n"
        '        turn.status = "awaiting_delivery"\n        await db.flush()\n        return "released"\n',
        "    if turn.delivery_attempt_count < scheduler.max_delivery_attempts_per_turn:\n"
        '        turn.status = "awaiting_delivery"\n        turn.delivery_attempt_count += 1\n'
        '        await db.flush()\n        return "released"\n',
    ),
    "settle_admitted_counts_waits": (
        T06 + "test_lane_expiries_waiting_for_claim_never_spend_the_cap",
        SCHEDULER_REPO,
        '        cap_path="expired_lane",\n    )\n    if admitted is not None:\n',
        '        cap_path="expired_lane",\n    )\n    if admitted is not None and False:\n',
    ),
    "ordinary_cap_dead_not_parked": (
        T06 + "test_ordinary_admitted_at_cap_on_lane_expiry_dies_and_promotes",
        SCHEDULER_REPO,
        "    if scheduled is not None:\n"
        '        turn.status = "awaiting_delivery"\n'
        "        _admitted_handoff_park_signal_count += 1\n",
        "    if True:\n"
        '        turn.status = "awaiting_delivery"\n'
        "        _admitted_handoff_park_signal_count += 1\n",
    ),
    "failure_cap_boundary": (  # review-B M5
        T06 + "test_real_send_failures_at_cap_kill_ordinary_turn_and_promote",
        DELIVERY_REPO,
        "    at_cap = turn.delivery_attempt_count >= graph.scheduler.max_delivery_attempts_per_turn\n",
        "    at_cap = turn.delivery_attempt_count > graph.scheduler.max_delivery_attempts_per_turn\n",
    ),
    "admission_defers_lane": (
        T06 + "test_admission_defers_instead_of_holding_the_lane",
        DELIVERY_REPO,
        "            # Never hold the agent lane waiting for another worker's lease.\n"
        "            return await _defer_admission(db, graph=graph, effect=selected)\n",
        '            raise scheduler_repo.SchedulerConflictError("held")\n',
    ),
    # ── round 1: P2-2 bounded discovery and requeue rotation ─────────────
    "discovery_skips_busy_lanes": (
        T06 + "test_discovery_skips_busy_lanes_and_finds_the_victim",
        DELIVERY_REPO,
        "            AgentServiceLane.owner_token.is_(None),\n",
        "",
    ),
    "requeue_refuses_busy_lane": (  # review-B M7
        T06 + "test_discovery_skips_busy_lanes_and_finds_the_victim",
        DELIVERY_REPO,
        '    if scheduler.state not in {"enabled", "draining"} or lane.owner_token is not None:\n'
        "        return False\n    from src.database.models import Sessao\n",
        '    if scheduler.state not in {"enabled", "draining"}:\n'
        "        return False\n    from src.database.models import Sessao\n",
    ),
    "requeue_rotates_ineligible": (
        T06 + "test_requeue_rotates_an_ineligible_turn_on_a_free_lane",
        DELIVERY_REPO,
        "    turn.scheduling_sequence = lane.next_scheduling_sequence\n"
        "    lane.next_scheduling_sequence += 1\n    if eligible:\n",
        "    if eligible:\n",
    ),
    # ── round 1: P2-3 cancellation and lifecycle at every fence ──────────
    "authority_refuses_cancel": (
        T06 + "test_resume_between_claim_and_preio_never_sends_the_reply",
        SCHEDULER_REPO,
        "    if delivery.cancel_requested_at is not None:\n"
        '        return "cancelled"\n',
        "",
    ),
    "authority_cancel_ordinary": (
        T06 + "test_preio_refuses_every_lifecycle_change",
        SCHEDULER_REPO,
        "    if delivery.cancel_requested_at is not None:\n",
        "    if False:\n",
    ),
    "authority_ordinary_session_bot": (
        T06 + "test_preio_refuses_every_lifecycle_change",
        SCHEDULER_REPO,
        '        if session.handler != "bot":\n            return "lifecycle"\n',
        '        if False:\n            return "lifecycle"\n',
    ),
    "authority_agent_active": (
        T06 + "test_preio_refuses_every_lifecycle_change",
        SCHEDULER_REPO,
        "        or not agent.ativo\n",
        "",
    ),
    "authority_attempt_status": (
        T06 + "test_preio_refuses_superseded_handoff_reply",
        SCHEDULER_REPO,
        '        or attempt.status not in {"pending", "ready"}\n'
        '        or session.handler != "encerrada"\n',
        '        or session.handler != "encerrada"\n',
    ),
    "authority_attempt_generation": (
        T06 + "test_preio_refuses_superseded_handoff_reply",
        SCHEDULER_REPO,
        "        or attempt.generation != session.handoff_generation\n",
        "",
    ),
    "authority_handoff_session_closed": (
        T06 + "test_preio_refuses_superseded_handoff_reply",
        SCHEDULER_REPO,
        '        or session.handler != "encerrada"\n'
        '        or session.motivo_encerramento != "handoff"\n',
        "",
    ),
    "preio_release_not_ambiguous": (  # review A-5
        T06 + "test_committed_claim_is_rejected_before_io_after_lane_loss",
        DELIVERY_REPO,
        "    _clear_delivery_lease(delivery)\n"
        '    delivery.last_error_code = f"preio_refused_{refusal}"\n',
        "    _clear_delivery_lease(delivery)\n"
        "    delivery.ambiguity_count += 1\n"
        '    delivery.last_error_code = f"preio_refused_{refusal}"\n',
    ),
    # ── round 1: P2-4 the lane outlives every authorized send ────────────
    "preio_extends_lane": (
        T06 + "test_preio_extends_lane_past_the_io_deadline",
        DELIVERY_REPO,
        "            graph.lane.lease_until = io_until\n",
        "            pass\n",
    ),
    "renew_extends_lane": (  # review-B M6
        T06 + "test_renewal_extends_lane_and_never_shortens_it",
        DELIVERY_REPO,
        "        authority.lane.lease_until = lane_until\n",
        "        pass\n",
    ),
    "renew_never_shortens_lane": (
        T06 + "test_renewal_extends_lane_and_never_shortens_it",
        DELIVERY_REPO,
        "    if authority.lane.lease_until is None or authority.lane.lease_until < lane_until:\n",
        "    if True:\n",
    ),
    # ── review-B surviving mutations M1–M4, M8–M12, M14 ─────────────────
    "authority_turn_awaiting_delivery": (  # M1
        T06 + "test_authority_requires_awaiting_delivery_and_matching_effect",
        SCHEDULER_REPO,
        '        or turn.status != "awaiting_delivery"\n        or revision.turn_id != turn.id\n',
        "        or revision.turn_id != turn.id\n",
    ),
    "authority_transition_open_delivered": (  # M2
        T06 + "test_transition_authority_requires_open_delivered",
        SCHEDULER_REPO,
        '        or graph.dependency.status != "delivered"\n',
        "",
    ),
    "authority_transition_attempt_ready": (  # M3
        T06 + "test_transition_authority_requires_ready_attempt",
        SCHEDULER_REPO,
        "        or attempt is None\n"
        '        or attempt.status != "ready"\n'
        "    ):\n"
        '        return "lane"\n',
        "        or attempt is None\n"
        "    ):\n"
        '        return "lane"\n',
    ),
    "claim_requires_turn_claim_until": (  # M4
        T06 + "test_claim_requires_live_turn_claim",
        WORKER_REPO,
        "                AND turn.claim_until > UTC_TIMESTAMP(6)\n",
        "",
    ),
    "bypass_requires_generation": (  # M8
        T06 + "test_bypass_requires_current_handoff_generation",
        GENERATION_REPO,
        "        and attempt.generation == graph.session.handoff_generation\n",
        "",
    ),
    "bypass_requires_live_attempt": (  # M9
        T06 + "test_bypass_requires_pending_or_ready_attempt",
        GENERATION_REPO,
        '        and attempt.status in {"pending", "ready"}\n',
        "",
    ),
    "maintenance_skips_automated_expiry": (  # M10
        T06 + "test_maintenance_never_expires_or_cascades_automated_rows",
        WORKER_REPO,
        "            WHERE status IN ('pending', 'retry')\n"
        "              AND expires_at <= UTC_TIMESTAMP(6)\n"
        "              AND provider <> 'chatwoot'\n"
        "              AND delivery_class <> 'automated'\n",
        "            WHERE status IN ('pending', 'retry')\n"
        "              AND expires_at <= UTC_TIMESTAMP(6)\n"
        "              AND provider <> 'chatwoot'\n",
    ),
    "maintenance_skips_automated_cascade": (  # M11
        T06 + "test_maintenance_never_expires_or_cascades_automated_rows",
        WORKER_REPO,
        "              AND child.delivery_class <> 'automated'\n",
        "",
    ),
    "authority_effect_matches_hint": (  # M12
        T06 + "test_transition_authority_refuses_a_reply_hint",
        SCHEDULER_REPO,
        "        or delivery.automated_effect != links.automated_effect\n",
        "",
    ),
    "handoff_terminal_at_admission_parks": (  # M14
        T06 + "test_terminal_effect_at_handoff_admission_parks_not_kills",
        DELIVERY_REPO,
        "    if handoff:\n        turn.status = \"awaiting_delivery\"\n"
        '        outcome: Literal["parked", "dead", "deferred"] = "parked"\n',
        "    if False:\n        turn.status = \"awaiting_delivery\"\n"
        '        outcome: Literal["parked", "dead", "deferred"] = "parked"\n',
    ),
    # ── round 2: X1 finalization records the remote truth ────────────────
    "finalize_ignores_lifecycle_handoff": (  # review-B N6
        T06 + "test_resume_during_handoff_reply_io_records_single_delivery",
        SCHEDULER_REPO,
        "    if for_completion:\n        return None\n",
        "",
    ),
    "finalize_ignores_lifecycle_ordinary": (
        T06 + "test_transfer_during_ordinary_io_records_single_delivery",
        SCHEDULER_REPO,
        "    if for_completion:\n        return None\n",
        "",
    ),
    "complete_settles_turn": (
        T06 + "test_agent_toggle_during_ordinary_io_records_single_delivery",
        DELIVERY_REPO,
        "    _clear_delivery_lease(delivery)\n"
        "    await db.flush()\n"
        "    await _settle_turn_after_effect(\n"
        '        db, graph=graph, effect=delivery, reason="delivered"\n'
        "    )\n",
        "    _clear_delivery_lease(delivery)\n    await db.flush()\n",
    ),
    "failure_retry_requires_lifecycle": (
        T06 + "test_failure_after_lifecycle_change_is_recorded_and_settles",
        DELIVERY_REPO,
        "        and not at_cap\n        and _lifecycle_allows(graph)\n",
        "        and not at_cap\n",
    ),
    "superseded_handoff_cancelled": (
        T06 + "test_resume_during_handoff_reply_io_records_single_delivery",
        DELIVERY_REPO,
        "    if handoff and not current:\n",
        "    if False:\n",
    ),
    # ── round 2: X2/X2b/R3b every terminal effect settles its turn ───────
    "preio_refusal_settles_turn": (
        T06 + "test_preio_lifecycle_refusal_settles_and_bot_answers_after_reactivation",
        DELIVERY_REPO,
        "        await _settle_turn_after_effect(\n"
        "            db, graph=graph, effect=delivery, reason=delivery.terminal_reason\n"
        "        )\n",
        "",
    ),
    "current_handoff_parks_on_refusal": (
        T06 + "test_preio_refusal_parks_a_current_handoff",
        DELIVERY_REPO,
        "    if handoff:\n"
        "        release_waiting()\n"
        "        _handoff_effect_terminal_count += 1\n",
        "    if False:\n"
        "        release_waiting()\n"
        "        _handoff_effect_terminal_count += 1\n",
    ),
    "requeue_settles_orphan": (
        T06 + "test_transfer_then_resume_with_pending_reply_settles_orphan",
        DELIVERY_REPO,
        "    if settle_effect is not None:\n",
        "    if False:\n",
    ),
    "discovery_lists_settle_rows": (
        T06 + "test_resume_with_pending_handoff_settles_and_bot_answers",
        DELIVERY_REPO,
        "                settle,\n",
        "",
    ),
    "admitted_lifecycle_refusal_releases": (
        T05 + "test_requeued_admitted_handoff_turn_never_generates_again",
        GENERATION_REPO,
        # Ticket07 replaced the release with formal cancellation; the
        # mutation reintroduces the release+backoff loop on the fence.
        "        return await _cancel_for_lifecycle_locked(db, graph)\n",
        "        return await _release_locked(\n"
        "            db, graph, delay_seconds=policy.release_backoff_seconds,\n"
        '            reason="lifecycle_refused",\n'
        "        )\n",
    ),
    # ── round 2: X4 lease rules ──────────────────────────────────────────
    "preio_extends_delivery_lease": (  # review-B N12
        T06 + "test_preio_extends_a_short_delivery_lease",
        DELIVERY_REPO,
        "            delivery.lock_until = io_until\n",
        "            pass\n",
    ),
    "preio_lane_refusal_backoff": (
        T06 + "test_committed_claim_is_rejected_before_io_after_lane_loss",
        DELIVERY_REPO,
        "        delivery.next_attempt_at = now + timedelta(seconds=PREIO_LANE_BACKOFF_SECONDS)\n",
        "        delivery.next_attempt_at = now\n",
    ),
    "lease_rule_covers_dispatch_only": (
        UNIT + "FeatureGateTests.test_dispatch_only_worker_requires_evolution_lease_beyond_io_deadline",
        "app/src/config.py",
        "        if (self.delivery_dispatch_enabled or self.inbox_scheduler_enabled) and (\n",
        "        if self.inbox_scheduler_enabled and (\n",
    ),
    # ── round 2: review-B suspicions ─────────────────────────────────────
    "first_admission_fifo_defer": (
        T06 + "test_admission_defers_behind_open_stream_predecessor",
        DELIVERY_REPO,
        "        if await _fifo_blocked(db, row):\n",
        "        if False:\n",
    ),
    "discovery_fifo_clear": (
        T06 + "test_admission_defers_behind_open_stream_predecessor",
        DELIVERY_REPO,
        "        return _due(effect, now) & (effect.c.cutover_epoch == runtime_epoch) & _fifo_clear(effect)\n",
        "        return _due(effect, now) & (effect.c.cutover_epoch == runtime_epoch)\n",
    ),
    "discovery_epoch_filter": (
        T06 + "test_stale_delivery_epoch_effect_is_deferred_not_held",
        DELIVERY_REPO,
        "        return _due(effect, now) & (effect.c.cutover_epoch == runtime_epoch) & _fifo_clear(effect)\n",
        "        return _due(effect, now) & _fifo_clear(effect)\n",
    ),
    "admission_epoch_defer": (
        T06 + "test_stale_delivery_epoch_effect_is_deferred_not_held",
        DELIVERY_REPO,
        "    if selected.cutover_epoch != candidate.lease.delivery_runtime_epoch:\n",
        "    if False:\n",
    ),
    "admission_resets_attempt_count": (
        T06 + "test_ordinary_admission_resets_generation_recovery_count",
        DELIVERY_REPO,
        "        graph.turn.delivery_attempt_count = 0\n",
        "",
    ),
    "handoff_admission_resets_attempt_count": (
        T06 + "test_handoff_admission_resets_generation_recovery_count",
        HANDOFF_REPO,
        "    owned.turn.delivery_attempt_count = 0\n",
        "",
    ),
    # ── round 3: Y1 every ending turn resolves all of its effects ───────
    "y1_live_lease_never_killed": (
        T06 + "test_y1_superseded_settle_waits_for_live_sibling_then_owner_settles",
        SCHEDULER_REPO,
        "    if any(\n"
        '        row.status == "processing"\n'
        "        and row.lock_until is not None\n"
        "        and row.lock_until > now\n"
        "        for row in rows\n"
        "    ):\n",
        "    if False:\n",
    ),
    "y1_cap_resolves_before_dead": (
        T06 + "test_y1_cap_never_kills_a_live_lease_and_resolves_an_expired_one",
        SCHEDULER_REPO,
        "    if not resolve_open_automated_effects_locked(\n        deliveries,\n",
        "    if False and not resolve_open_automated_effects_locked(\n        deliveries,\n",
    ),
    "y1_expired_processing_resolved": (
        T06 + "test_y1_orphan_effect_after_resume_resolves_and_bot_answers",
        SCHEDULER_REPO,
        '        elif row.status == "processing":\n            row.lease_token = None\n',
        '        elif False:\n            row.lease_token = None\n',
    ),
    "y1_cap_expired_processing_ambiguous": (
        T06 + "test_y1_cap_never_kills_a_live_lease_and_resolves_an_expired_one",
        SCHEDULER_REPO,
        "            row.ambiguity_count += 1\n"
        '            row.last_error_code = "lease_expired_ambiguous"\n',
        "",
    ),
    "y1_discovery_skips_live_lease": (
        T06 + "test_y1_orphan_effect_after_resume_resolves_and_bot_answers",
        DELIVERY_REPO,
        "    settle = no_live_effect & or_(\n",
        "    settle = or_(\n",
    ),
    "y1_discovery_lists_cancelled_orphan": (
        T06 + "test_y1_cancelled_orphan_of_inactive_agent_is_settled",
        DELIVERY_REPO,
        '            (reply.c.status == "processing")\n'
        "            & ordinary_refused\n",
        '            (reply.c.status == "never")\n'
        "            & ordinary_refused\n",
    ),
    "y1_requeue_settles_cancelled_orphan": (
        T06 + "test_y1_cancelled_orphan_of_inactive_agent_is_settled",
        DELIVERY_REPO,
        "        elif reply is not None and (\n",
        "        elif False and (\n",
    ),
    "y1_scheduler_completion_refuses_open_effect": (
        T06 + "test_completion_terminal_turn_refuses_open_automated_effect",
        SCHEDULER_REPO,
        "    if open_effect is not None:\n",
        "    if False:\n",
    ),

    # ── round 3: surviving review-B round-2 mutations ────────────────────

    "r2m3_superseded_resolves_remaining": (
        T06 + "test_y1_superseded_settle_waits_for_live_sibling_then_owner_settles",
        SCHEDULER_REPO,
        '        if row.status in {"blocked", "pending", "retry"}:\n'
        '            row.status = "cancelled"\n'
        "            row.terminal_at = now\n"
        "            row.terminal_reason = reason\n",
        "        if False:\n"
        "            pass\n",
    ),
    "r2m4_superseded_promotes_successor": (
        T06 + "test_y1_superseded_handoff_promotes_waiting_successor",
        DELIVERY_REPO,
        "        await scheduler_repo._promote_successor_after_terminal_active(\n"
        "            db, sessao_id=session.id\n"
        "        )\n"
        "        return True\n",
        '        if status != "cancelled":\n'
        "            await scheduler_repo._promote_successor_after_terminal_active(\n"
        "                db, sessao_id=session.id\n"
        "            )\n"
        "        return True\n",
    ),
    "y1_discovery_settles_ordinary_cap": (
        T06 + "test_y1_cap_never_kills_a_live_lease_and_resolves_an_expired_one",
        DELIVERY_REPO,
        "            ordinary_cap & reply.c.status.in_(_ACTIVE_STREAM_STATUSES),\n",
        "",
    ),
    "y1_requeue_settles_ordinary_cap": (
        T06 + "test_y1_cap_never_kills_a_live_lease_and_resolves_an_expired_one",
        DELIVERY_REPO,
        "            turn.delivery_attempt_count >= scheduler.max_delivery_attempts_per_turn\n",
        "            False\n",
    ),
    "y1_requeue_resolves_without_cancel_request": (
        T06 + "test_y1_ordinary_aborted_renewal_settles_without_cancel_request",
        DELIVERY_REPO,
        "                reply.cancel_requested_at is not None or not _lifecycle_allows(graph)\n",
        "                reply.cancel_requested_at is not None\n",
    ),
    "y1_preio_settles_remaining": (
        T06 + "test_y1_preio_refusal_leaves_no_open_effect",
        DELIVERY_REPO,
        "        await _settle_turn_after_effect(\n"
        "            db, graph=graph, effect=delivery, reason=delivery.terminal_reason\n"
        "        )\n",
        "",
    ),
    "y1_cap_recovery_and_yield_resolve_processing": (
        T06 + "test_y1_cap_recovery_and_yield_resolve_expired_processing",
        SCHEDULER_REPO,
        "    if not resolve_open_automated_effects_locked(\n        deliveries,\n",
        "    if False and not resolve_open_automated_effects_locked(\n        deliveries,\n",
    ),

    "y1_graph_prelocks_successor": (
        T06 + "test_y1_settle_prelocks_successor_before_revision_and_effects",
        SCHEDULER_REPO,
        '    if session is not None and turn is not None and turn.session_slot == "active":\n        await lock_pending_successor(db, sessao_id=session.id)\n',
        "",
    ),

    "y1_requeue_prelocks_successor": (
        T06 + "test_y1_settle_prelocks_successor_before_revision_and_effects",
        DELIVERY_REPO,
        '    await scheduler_repo.lock_pending_successor(db, sessao_id=session.id)\n',
        "",
    ),

}


# ── round 3: review-B surviving mutations R2M2/R2M18/R2M19/R2M20 ────────
# Self-contained block; each test lives in test_turn_delivery_round3_mysql.
R3 = "tests.test_turn_delivery_round3_mysql.TurnDeliveryRound3MySQLTests."
_R2M2_OLD = (
    "            not for_completion\n"
    "            and (delivery.lock_until is None or delivery.lock_until <= now)\n"
)
_R2M2_NEW = (
    "            True\n"
    "            and (delivery.lock_until is None or delivery.lock_until <= now)\n"
)
_R2M20_OLD = "    if _owns_lane(graph):\n        lane.owner_token = None\n"
_R2M20_NEW = "    if True:\n        lane.owner_token = None\n"
ROUND3_CASES = {
    "r2m2_completion_after_lapsed_lease": (
        R3 + "test_completion_after_delivery_lease_lapsed_records_delivered",
        SCHEDULER_REPO,
        _R2M2_OLD,
        _R2M2_NEW,
    ),
    "r2m2_failure_after_lapsed_lease": (
        R3 + "test_failure_after_delivery_lease_lapsed_is_recorded_once",
        SCHEDULER_REPO,
        _R2M2_OLD,
        _R2M2_NEW,
    ),
    "r2m18_requeue_rechecks_fifo": (
        R3 + "test_requeue_rechecks_fifo_after_discovery",
        DELIVERY_REPO,
        "        eligible = not await _fifo_blocked(db, candidate)\n",
        "        eligible = True\n",
    ),
    "r2m19_requeue_rechecks_epoch": (
        R3 + "test_requeue_rechecks_delivery_epoch_after_discovery",
        DELIVERY_REPO,
        "            and effect.cutover_epoch == runtime.cutover_epoch\n",
        "",
    ),
    "r2m20_completion_keeps_other_lane": (
        R3 + "test_late_completion_keeps_the_lane_of_another_turn",
        DELIVERY_REPO,
        _R2M20_OLD,
        _R2M20_NEW,
    ),
    "r2m20_failure_keeps_other_lane": (
        R3 + "test_late_failure_keeps_the_lane_of_another_turn",
        DELIVERY_REPO,
        _R2M20_OLD,
        _R2M20_NEW,
    ),
}
CASES.update(ROUND3_CASES)


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
        # Every target exists exactly once in the unmutated tree.
        root = Path(__file__).resolve().parent.parent
        for name, (_, relative, old, _new) in CASES.items():
            count = (root / relative).read_text().count(old)
            if count != 1:
                raise SystemExit(f"{name}: expected one mutation target, found {count}")
        print(f"targets ok: {len(CASES)}")
        return 0
    if len(argv) == 4 and argv[1] == "apply":
        apply(Path(argv[2]), argv[3])
        return 0
    raise SystemExit("usage: inbox_ticket06_mutations.py list | count | check | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
