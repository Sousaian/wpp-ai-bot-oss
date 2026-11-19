#!/usr/bin/env python3
"""Deterministic fix-removal mutations for the ticket 05 matrix.

Each case removes exactly one protection from a copy of ``app/`` and names the
test that must fail against the real disposable MySQL without it.
"""

from pathlib import Path
import sys


MYSQL = "tests.test_handoff_terminal_mysql.HandoffTerminalMySQLTests."
UNIT = "tests.test_handoff_terminal_unit."
REPO = "app/src/database/repositories/turn_handoff_repo.py"
WORKER_REPO = "app/src/database/repositories/delivery_worker_repo.py"

CASES = {
    "attempt_marker": (
        UNIT + "ContextNotePayloadTests.test_decoder_accepts_only_the_immutable_exact_payload",
        "app/src/services/delivery_adapters.py",
        'f"wppctx:v1:{attempt_id}:{chunk:04d}:{chunks:04d}:{digest}"',
        'f"wppctx:v1:{handoff_key}:{chunk:04d}:{chunks:04d}:{digest}"',
    ),
    "summary_final_turn_range": (
        MYSQL + "test_input_during_first_typing_refreshes_before_single_summary",
        REPO,
        "                        ConversationTurn.id == graph.turn.id,\n",
        "                        TurnInputMembership.assigned_revision_id\n"
        "                        == graph.current.id,\n",
    ),
    "summary_excludes_successor": (
        MYSQL + "test_input_during_refreshed_typing_is_late_context_exactly_once",
        REPO,
        "                        ConversationTurn.id == graph.turn.id,\n",
        "                        ConversationTurn.sessao_id == graph.session.id,\n",
    ),
    "successor_capture": (
        MYSQL + "test_input_during_summary_is_captured_once_not_summarized",
        REPO,
        "                    .where(TurnInputMembership.turn_id == successor.id)\n",
        "                    .where(TurnInputMembership.turn_id == -successor.id)\n",
    ),
    "candidate_status_fence": (
        MYSQL + "test_stale_superseded_candidate_is_rejected_under_locks",
        REPO,
        '        or revision.status != "final_candidate"\n',
        "",
    ),
    "park_admitted_turn": (
        MYSQL + "test_requeued_admitted_handoff_turn_never_generates_again",
        REPO,
        '        or revision.status != "admitted"\n        or owned.turn.final_revision_id != revision.id\n    ):\n        return None\n',
        "        or True\n    ):\n        return None\n",
    ),
    "summary_soft_limit": (
        MYSQL + "test_soft_limit_blocks_summary_call_and_uses_fallback",
        REPO,
        "    if await consumo_repo.atingiu_limite_mensal(owned.agent.id, db):\n",
        "    if False:\n",
    ),
    "setup_configured_before_summary": (
        MYSQL + "test_unconfigured_inbox_releases_lane_without_summary",
        REPO,
        "    if _lifecycle_refused(owned) or _setup_unconfigured(owned):\n        reason = (",
        "    if _lifecycle_refused(owned):\n        reason = (",
    ),
    "open_completion_keeps_transition_blocked": (
        MYSQL + "test_worker_claim_never_dispatches_ticket05_automated_rows",
        WORKER_REPO,
        '                OutboxDelivery.status == "blocked",\n'
        '                OutboxDelivery.delivery_class != "automated",\n'
        "            )\n"
        "            .values(\n"
        '                status="pending",\n'
        "                next_attempt_at=now,",
        '                OutboxDelivery.status == "blocked",\n'
        "            )\n"
        "            .values(\n"
        '                status="pending",\n'
        "                next_attempt_at=now,",
    ),
    "callback_keeps_transition_blocked": (
        MYSQL + "test_callback_confirmation_never_unblocks_automated_transition",
        WORKER_REPO,
        "            OutboxDelivery.cancel_requested_at.is_(None),\n"
        '            OutboxDelivery.delivery_class != "automated",\n',
        "            OutboxDelivery.cancel_requested_at.is_(None),\n",
    ),
    "operator_retry_refuses_automated": (
        MYSQL + "test_operator_paths_never_revive_automated_transition",
        WORKER_REPO,
        '    if delivery.delivery_class == "automated":\n',
        '    if delivery.delivery_class == "never":\n',
    ),
    "open_retry_revives_note_chain": (
        MYSQL + "test_operator_paths_never_revive_automated_transition",
        WORKER_REPO,
        "        # legacy direct-dependent revival; an automated transition only\n"
        "        # returns to ``blocked`` and is never scheduled here.\n"
        "        await _reblock_context_note_chain(\n",
        "        # legacy direct-dependent revival; an automated transition only\n"
        "        # returns to ``blocked`` and is never scheduled here.\n"
        "        if False:\n"
        "          await _reblock_context_note_chain(\n",
    ),
    "reconcile_revives_note_chain": (
        MYSQL + "test_remote_accept_with_local_failure_recovers_exactly_by_operator",
        WORKER_REPO,
        "    if delivery.operation == CONTEXT_NOTE_OPERATION:\n"
        "        # Exact recovery may prove delivery",
        "    if False:\n"
        "        # Exact recovery may prove delivery",
    ),
    "expired_note_becomes_ambiguous": (
        MYSQL + "test_remote_accept_with_local_failure_recovers_exactly_by_operator",
        WORKER_REPO,
        '                    OutboxDelivery.operation == "chatwoot.add_private_note.v1",\n'
        '                    OutboxDelivery.status == "processing",\n',
        '                    OutboxDelivery.operation == "chatwoot.add_private_note.v1",\n'
        '                    OutboxDelivery.status == "never",\n',
    ),
    "generic_recovery_excludes_notes": (
        MYSQL + "test_ambiguous_note_discovery_rotates_limit_plus_one",
        WORKER_REPO,
        "              AND operation <> 'chatwoot.add_private_note.v1'\n",
        "",
    ),
    "note_ownership_coverage": (
        MYSQL + "test_note_ownership_requires_exact_mapping_attempt_and_coverage",
        WORKER_REPO,
        "        or not _coverage_matches_assignments(parsed, source_id, assignments)\n",
        "        or False\n",
    ),
    "worker_never_retries_ambiguous_note": (
        UNIT + "ContextNoteWorkerTests.test_ambiguous_create_without_exact_match_is_never_retried",
        "app/src/delivery_worker.py",
        '            delivery.operation == "chatwoot.add_private_note.v1"\n'
        "            and failure is not None\n"
        '            and (failure.ambiguous or progress["post_sent"])\n',
        '            delivery.operation == "never"\n'
        "            and failure is not None\n"
        '            and (failure.ambiguous or progress["post_sent"])\n',
    ),
    "admission_whole_txn_retry": (
        MYSQL + "test_real_deadlock_retries_whole_terminal_admission_once",
        "app/src/services/turn_handoff.py",
        "        return await scheduler_repo.run_transaction_with_deadlock_retry(\n"
        "            self._sessions, operation\n"
        "        )\n",
        "        return await scheduler_repo.run_transaction_with_deadlock_retry(\n"
        "            self._sessions, operation, max_attempts=1\n"
        "        )\n",
    ),
    "split_oversized_record": (
        MYSQL + "test_escape_heavy_late_input_during_summary_is_split_and_admitted",
        "app/src/services/handoff_context.py",
        "    if _fits_alone(attempt_id, whole, maximum_characters):\n        return (whole,)\n",
        "    if True:\n        return (whole,)\n",
    ),
    "budget_on_rendered_note": (
        UNIT + "ContextChunkTests.test_escape_heavy_records_budget_on_the_rendered_note",
        "app/src/services/handoff_context.py",
        "    return len(rendered.content) <= maximum_characters\n",
        "    return len(piece.content) <= maximum_characters\n",
    ),
    "deterministic_allowlist_widened": (
        MYSQL + "test_unknown_exception_is_accounted_and_reclaimed_not_terminal",
        "app/src/services/turn_handoff.py",
        "        (\n"
        "            repo.HandoffLocalInvariantError,\n",
        "        (\n"
        "            Exception,\n"
        "            repo.HandoffLocalInvariantError,\n",
    ),
    "driver_assertion_not_terminal": (
        MYSQL + "test_driver_assertion_mid_admission_is_accounted_then_admitted",
        "app/src/services/turn_handoff.py",
        "        (\n"
        "            repo.HandoffLocalInvariantError,\n",
        "        (\n"
        "            AssertionError,\n"
        "            repo.HandoffLocalInvariantError,\n",
    ),
    "planner_assertion_promoted": (
        MYSQL + "test_deterministic_admission_failure_is_terminal_not_repaid",
        REPO,
        "    except (ValueError, AssertionError) as exc:\n",
        "    except ValueError as exc:\n",
    ),
    "finalize_terminal_branch": (
        MYSQL + "test_deterministic_admission_failure_is_terminal_not_repaid",
        "app/src/services/turn_handoff.py",
        "                if _deterministic_admission_failure(exc):\n"
        "                    try:\n"
        "                        return await self._fail_terminally(\n"
        "                            candidate, step.attempt_key, outcome, exc\n",
        "                if False:\n"
        "                    try:\n"
        "                        return await self._fail_terminally(\n"
        "                            candidate, step.attempt_key, outcome, exc\n",
    ),
    "terminal_fallback_accounting": (
        MYSQL + "test_terminalization_failure_falls_back_to_exact_accounting",
        "app/src/services/turn_handoff.py",
        "                        self._signal_terminal_failure_fallback(\n"
        "                            candidate, exc, terminal_exc\n"
        "                        )\n",
        "                        self._signal_terminal_failure_fallback(\n"
        "                            candidate, exc, terminal_exc\n"
        "                        )\n"
        "                        raise\n",
    ),
    "finalize_cancel_shielded_accounting": (
        MYSQL + "test_cancellation_during_admission_accounts_known_paid_summary",
        "app/src/services/turn_handoff.py",
        "            except asyncio.CancelledError:\n"
        "                # Cancelled mid-admission (shutdown)",
        "            except ZeroDivisionError:\n"
        "                # Cancelled mid-admission (shutdown)",
    ),
    "processor_cancel_shielded_accounting": (
        MYSQL + "test_cancellation_while_recording_response_accounts_attempt",
        "app/src/services/turn_processor.py",
        "            except asyncio.CancelledError:\n"
        "                # Cancelled while recording a known outcome",
        "            except ZeroDivisionError:\n"
        "                # Cancelled while recording a known outcome",
    ),
    "note_unknown_after_post_ambiguous": (
        UNIT + "ContextNoteWorkerTests.test_unknown_exception_after_post_is_ambiguous_never_retried",
        "app/src/delivery_worker.py",
        '            and (failure.ambiguous or progress["post_sent"])\n',
        "            and failure.ambiguous\n",
    ),
    "recovery_rehomes_successor": (
        MYSQL + "test_recovery_cap_promotes_ordinary_successor",
        "app/src/database/repositories/scheduler_repo.py",
        "        await _promote_successor_after_terminal_active(db, sessao_id=session.id)\n"
        '        outcome: Literal["retry", "dead"] = "dead"\n',
        '        outcome: Literal["retry", "dead"] = "dead"\n',
    ),
    "recovery_cap_preserves_handoff": (
        MYSQL + "test_recovery_cap_terminalizes_decided_handoff",
        "app/src/database/repositories/scheduler_repo.py",
        "        if cap_disposition == \"decided\":\n"
        "            await _terminalize_handoff_cap_locked(\n"
        "                db,\n"
        "                session=session,\n"
        "                lane=lane,\n"
        "                turn=turn,\n"
        "                graph=cap_graph,\n"
        "                now=now,\n"
        "                cap_path=\"expired_lane\",\n",
        "        if False:\n"
        "            await _terminalize_handoff_cap_locked(\n"
        "                db,\n"
        "                session=session,\n"
        "                lane=lane,\n"
        "                turn=turn,\n"
        "                graph=cap_graph,\n"
        "                now=now,\n"
        "                cap_path=\"expired_lane\",\n",
    ),
    "yield_cap_preserves_handoff": (
        MYSQL + "test_yield_cap_terminalizes_decided_handoff",
        "app/src/database/repositories/scheduler_repo.py",
        "        if cap_disposition == \"decided\":\n"
        "            await _terminalize_handoff_cap_locked(\n"
        "                db,\n"
        "                session=session,\n"
        "                lane=lane,\n"
        "                turn=turn,\n"
        "                graph=cap_graph,\n"
        "                now=now,\n"
        "                cap_path=\"yield_retry\",\n",
        "        if False:\n"
        "            await _terminalize_handoff_cap_locked(\n"
        "                db,\n"
        "                session=session,\n"
        "                lane=lane,\n"
        "                turn=turn,\n"
        "                graph=cap_graph,\n"
        "                now=now,\n"
        "                cap_path=\"yield_retry\",\n",
    ),
    "ordinary_cap_not_terminalized": (
        MYSQL + "test_recovery_cap_promotes_ordinary_successor",
        "app/src/database/repositories/scheduler_repo.py",
        '        or revision.needs_handoff is not True\n',
        '        or False\n',
    ),
    "handoff_cap_ignores_pending_refresh": (
        MYSQL + "test_recovery_cap_preserves_pending_refresh_then_processes_rev2",
        "app/src/database/repositories/scheduler_repo.py",
        "    if revision.revision_number == 1 and pending_refresh:\n",
        "    if False:\n",
    ),
    "handoff_cap_skips_durable_refresh": (
        MYSQL + "test_yield_cap_preserves_pending_refresh_then_processes_rev2",
        "app/src/database/repositories/scheduler_repo.py",
        "    await supersede_revision_for_refresh_locked(\n"
        "        db,\n"
        "        lane=lane,\n"
        "        turn=turn,\n"
        "        revision=graph.revision,\n"
        "        reason=\"refresh_input_at_scheduler_cap\",\n"
        "    )\n",
        "    # mutation: the cap forgot to persist the mandatory refresh\n",
    ),
    "handoff_cap_allows_second_refresh": (
        MYSQL + "test_recovery_cap_terminalizes_rev2_typing_handoff",
        "app/src/database/repositories/scheduler_repo.py",
        "    if revision.revision_number == 1 and pending_refresh:\n",
        "    if revision.revision_number in {1, 2}:\n",
    ),
    "handoff_cap_drops_carried_intent": (
        MYSQL + "test_recovery_cap_every_pass_fails_terminates_after_one_refresh",
        "app/src/database/repositories/scheduler_repo.py",
        "    if carried_handoff_intent:\n",
        "    if False:\n",
    ),
    "handoff_cap_ignores_supersession_reason": (
        MYSQL + "test_recovery_cap_does_not_carry_ordinary_supersession_reason",
        "app/src/database/repositories/scheduler_repo.py",
        "        and predecessor_revision.superseded_reason\n"
        "        == \"refresh_input_at_scheduler_cap\"\n",
        "        and True\n",
    ),
    "yield_rehomes_successor": (
        MYSQL + "test_yield_cap_promotes_ordinary_successor",
        "app/src/database/repositories/scheduler_repo.py",
        "        turn.terminal_reason = \"delivery_attempt_cap\"\n"
        "        turn.terminal_at = now\n"
        "        await db.flush()\n"
        "        await _promote_successor_after_terminal_active(db, sessao_id=turn.sessao_id)\n",
        "        turn.terminal_reason = \"delivery_attempt_cap\"\n"
        "        turn.terminal_at = now\n"
        "        await db.flush()\n",
    ),
    "completion_refuses_orphan": (
        MYSQL + "test_terminal_completion_without_promotion_refuses_to_orphan",
        "app/src/database/repositories/scheduler_repo.py",
        "        if orphan is not None:\n",
        "        if False:\n",
    ),
    "deterministic_allowlist_removed": (
        MYSQL + "test_deterministic_failure_on_fallback_path_is_terminal",
        "app/src/services/turn_handoff.py",
        "            if not _deterministic_admission_failure(exc):\n                raise\n",
        "            if True:\n                raise\n",
    ),
    "part_coverage_starts_pinned": (
        MYSQL + "test_part_coverage_ownership_is_exact",
        WORKER_REPO,
        "        starting == here\n",
        "        True\n",
    ),
    "expired_processing_note_ambiguous": (
        MYSQL + "test_processing_note_lost_by_expiry_or_cancel_is_ambiguous",
        WORKER_REPO,
        "        if delivery.operation == CONTEXT_NOTE_OPERATION and was_processing:\n",
        "        if False and was_processing:\n",
    ),
    "cancelled_processing_note_ambiguous": (
        MYSQL + "test_processing_note_lost_by_expiry_or_cancel_is_ambiguous",
        WORKER_REPO,
        "        if delivery.operation == CONTEXT_NOTE_OPERATION:\n"
        "            # The lost lease may have posted",
        "        if False:\n"
        "            # The lost lease may have posted",
    ),
    "ambiguous_note_terminal_reason": (
        MYSQL + "test_operator_reconcile_without_marker_stays_resolvable",
        WORKER_REPO,
        "        # one reason, the only state resolve-context-note accepts.\n"
        '        delivery.terminal_reason = "context_note_ambiguous"\n',
        "        # one reason, the only state resolve-context-note accepts.\n"
        "        pass\n",
    ),
    "pre_post_failure_retryable": (
        UNIT + "ContextNoteWorkerTests.test_pre_post_failure_is_retryable_without_ambiguity",
        "app/src/delivery_worker.py",
        '            if not progress["post_sent"]:\n',
        "            if False:\n",
    ),
    "scrub_keeps_unresolved_notes": (
        MYSQL + "test_retention_keeps_unresolved_note_payloads_until_resolution",
        WORKER_REPO,
        "                  operation <> 'chatwoot.add_private_note.v1'\n                  OR status = 'delivered'\n",
        "                  TRUE\n                  OR status = 'delivered'\n",
    ),
    "scrub_superseded_notes": (
        MYSQL + "test_retention_scrubs_unresolved_notes_of_superseded_generation",
        WORKER_REPO,
        "        delivery.payload = {}\n        scrubbed += 1\n",
        "        scrubbed += 1\n",
    ),
    "resolve_cancelled_continues_chain": (
        MYSQL + "test_audited_note_resolutions_restore_the_ordered_chain",
        WORKER_REPO,
        "    else:\n        # ``delivered``: the operator proved the exact note exists.\n",
        '    elif resolution == "delivered":\n        # ``delivered``: the operator proved the exact note exists.\n',
    ),
    "mapping_mismatch_is_not_ambiguous": (
        UNIT + "ContextNoteAdapterTests.test_wrong_account_inbox_or_contact_fails_before_post",
        "app/src/services/delivery_adapters.py",
        "            raise ContextNoteOwnershipMismatch(\n"
        '                "Chatwoot context-note mapping verification failed"\n',
        "            raise ProviderResultError(\n"
        '                "Chatwoot context-note mapping verification failed"\n',
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
    if len(argv) == 4 and argv[1] == "apply":
        apply(Path(argv[2]), argv[3])
        return 0
    raise SystemExit("usage: inbox_ticket05_mutations.py list | count | apply ROOT NAME")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
