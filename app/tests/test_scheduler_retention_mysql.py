"""T08-2 disposable MySQL tests; no provider and no schema/guard bypass in purge."""
import asyncio
import gc
import importlib
import json
import tempfile
import time
from datetime import datetime, timedelta
from decimal import Decimal
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import func, select, text
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import delivery_worker as worker_module
from src.database.models import (
    AgentServiceLane, Agente, Atendente, ConsumoLLM, ConversationTurn, GenerationAttempt, HandoffAttempt,
    HandoffContextAssignment, InboundEvent, LLMUsageReconciliation, Mensagem,
    OutboxDelivery, ScheduledHandoff, Sessao, TurnInputMembership, TurnRevision,
    SchedulerRetentionProgress,
)
from src.database.repositories import (delivery_worker_repo, lifecycle_repo,
                                       scheduler_repo, scheduler_retention_repo as retention)
from tests.disposable_db import assert_disposable_database


class SchedulerRetentionMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def _assert_disposable_database(self) -> None:
        await assert_disposable_database(self, self.engine)

    async def asyncSetUp(self):
        url = os.environ['OUTBOX_TEST_DATABASE_URL']
        self.engine = create_async_engine(url, poolclass=NullPool)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        # This fixture owns the ENTIRE disposable DB; production purge never
        # disables constraints. The runner must name the exact disposable schema;
        # coupling this guard to one suite's URL prefix made integrated matrices
        # fail before any case ran.
        await self._assert_disposable_database()
        async with self.engine.begin() as conn:
            await conn.execute(text('SET FOREIGN_KEY_CHECKS=0'))
            for name in ('handoff_context_assignments', 'scheduled_handoffs',
                         'delivery_operator_actions', 'outbox_deliveries',
                         'generation_attempts', 'turn_input_memberships',
                         'turn_revisions', 'conversation_turns', 'agent_service_lanes',
                         'handoff_attempts', 'inbound_events', 'llm_usage_reconciliation',
                         'consumo_llm', 'mensagens', 'sessoes', 'agentes', 'atendentes'):
                await conn.execute(text(f'DELETE FROM {name}'))
            await conn.execute(text('SET FOREIGN_KEY_CHECKS=1'))
            await conn.execute(text("UPDATE delivery_runtime_state SET mode='direct'"))
            await conn.execute(text("UPDATE scheduler_runtime_state SET state='disabled'"))
        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_retention_progress SET last_turn_id=0, version=0 WHERE id=1"))
            await db.commit()
        retention._health = {"available": False}
        retention._counts.clear()
        retention._coverage = None
        retention._last_complete_scan_at = None
        retention._last_complete_scan_monotonic = None
        retention._observing_since = time.monotonic()
        retention._sla_episodes = set()
        retention._candidate_last_pass = None
        retention._candidate_last_failure = None

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_disposable_database_marker_guard(self) -> None:
        with patch.dict(
            os.environ,
            {'WPP_DISPOSABLE_TEST_DATABASE_MARKER': 'definitely-not-this-database'},
        ):
            with self.assertRaises(AssertionError):
                await self._assert_disposable_database()

    async def graph(self, *, days=100, terminal=True, outcome='succeeded',
                    accounting='recorded', resolved=False, refresh=False,
                    receipt_days=None, handoff=False, session_generation=1, epoch=None,
                    with_ledger=True, terminal_days=None):
        old = datetime.utcnow() - timedelta(days=days)
        epoch = epoch or str(uuid4())
        async with self.sessions() as db:
            agent = Agente(nome='t082 fixture', instance_name=str(uuid4()),
                           system_prompt='synthetic', numero_notificacao='0')
            db.add(agent); await db.flush()
            session = Sessao(agente_id=agent.id, telefone='synthetic-t082', handler='bot',
                             handoff_generation=session_generation if handoff else 0)
            db.add(session); await db.flush()
            message = Mensagem(sessao_id=session.id, role='user', conteudo='preserved lead')
            assistant = Mensagem(sessao_id=session.id, role='assistant', conteudo='preserved answer')
            db.add_all([message, assistant]); await db.flush()
            turn = ConversationTurn(agente_id=agent.id, sessao_id=session.id,
                scheduler_epoch=epoch, arrival_sequence=1, scheduling_sequence=1,
                status='delivered' if terminal else 'pending',
                session_slot='terminal' if terminal else 'active', ready_at=old,
                created_at=old, terminal_at=(datetime.utcnow() - timedelta(days=terminal_days)
                    if terminal_days is not None else old) if terminal else None,
                terminal_reason='test' if terminal else None)
            db.add(turn); await db.flush()
            revision = TurnRevision(turn_id=turn.id, scheduler_epoch=epoch,
                revision_number=1, status='superseded' if refresh else 'admitted',
                superseded_reason='new_input' if refresh else None,
                draft_text='private draft', created_at=old)
            db.add(revision); await db.flush()
            current = revision
            if refresh:
                current = TurnRevision(turn_id=turn.id, scheduler_epoch=epoch,
                    revision_number=2, refresh_ordinal=1, replacement_of_revision_id=revision.id,
                    status='admitted', draft_text='new private draft', created_at=old)
                db.add(current); await db.flush()
            turn.current_revision_id = current.id
            turn.final_revision_id = current.id
            receipt = InboundEvent(provider='evolution', source_scope='t082',
                provider_event_id=str(uuid4()), event_type='messages.upsert',
                payload_hmac='a'*64, hmac_key_version=1, agente_id=agent.id,
                sessao_id=session.id, mensagem_id=message.id, receipt_disposition='scheduled',
                received_at=datetime.utcnow()-timedelta(days=receipt_days or days))
            db.add(receipt); await db.flush()
            member = TurnInputMembership(turn_id=turn.id, sessao_id=session.id,
                mensagem_id=message.id, inbound_event_id=receipt.id, input_sequence=1,
                content_bytes=14, membership_state='sealed', assigned_revision_id=current.id,
                created_at=old)
            db.add(member); await db.flush()
            ledger = None
            if outcome == 'succeeded' and with_ledger:
                ledger = ConsumoLLM(agente_id=agent.id, sessao_id=session.id,
                    modelo_llm='openai/gpt-4o-mini', tipo_chamada='resposta', tokens_input=1,
                    tokens_output=1, custo_creditos=Decimal('0.0001'))
                db.add(ledger); await db.flush()
            attempt = GenerationAttempt(agente_id=agent.id, sessao_id=session.id,
                turn_id=turn.id, revision_id=current.id, scheduler_epoch=epoch,
                lane_owner_token=str(uuid4()), attempt_type='response', attempt_ordinal=1,
                outcome=outcome, accounting_state=accounting,
                modelo_llm='openai/gpt-4o-mini', started_at=old,
                outcome_recorded_at=old if outcome != 'started' else None,
                tokens_input=1 if ledger else None, tokens_output=1 if ledger else None,
                custo_creditos=Decimal('0.0001') if ledger else None,
                consumo_llm_id=ledger.id if ledger else None,
                reconciled_at=old if resolved else None,
                reconciliation_actor='test' if resolved else None,
                reconciliation_reason='test evidence' if resolved else None)
            db.add(attempt); await db.flush()
            handoff_row = None
            if handoff:
                handoff_row = HandoffAttempt(agente_id=agent.id, sessao_id=session.id,
                    generation=1, status='ready', ready_at=old, created_at=old,
                    chatwoot_inbox_id=1, chatwoot_contact_id=1,
                    chatwoot_conversation_id=agent.id)
                db.add(handoff_row); await db.flush()
                db.add(ScheduledHandoff(handoff_attempt_id=handoff_row.id, agente_id=agent.id,
                    sessao_id=session.id, conversation_turn_id=turn.id, turn_revision_id=current.id,
                    summary_kind='technical_fallback', scheduler_epoch=epoch, created_at=old))
                db.add(HandoffContextAssignment(turn_input_membership_id=member.id,
                    handoff_attempt_id=handoff_row.id, agente_id=agent.id, sessao_id=session.id,
                    coverage_ordinal=1, source_id='context:1', created_at=old))
            await db.commit()
            return dict(agent=agent.id, session=session.id, turn=turn.id,
                        revision=current.id, receipt=receipt.id, attempt=attempt.id,
                        epoch=epoch, handoff=handoff_row.id if handoff_row else None)

    async def count(self, model):
        async with self.sessions() as db:
            return (await db.execute(select(func.count()).select_from(model))).scalar_one()

    async def effect(self, graph, *, status='delivered', note=False, kind=None,
                     depends_on=None, terminal_reason=None, ambiguity=0):
        """kind: reply|transition (automated) or open|note (provider setup)."""
        kind = kind or ('note' if note else 'reply')
        old = datetime.utcnow()-timedelta(days=100)
        chatwoot = kind in {'open', 'note'}
        blocked = status == 'blocked'
        live = status in {'pending', 'retry'}
        async with self.sessions() as db:
            effect = OutboxDelivery(cutover_epoch=str(uuid4()), dedupe_key=str(uuid4()),
                provider='chatwoot' if chatwoot else 'evolution',
                operation={'open': 'chatwoot.open_handoff.v1',
                           'note': 'chatwoot.add_private_note.v1'}.get(kind, 'evolution.send_text.v1'),
                payload_version=1, payload={'synthetic': True},
                delivery_class='provider_setup_context' if chatwoot else 'automated',
                automated_effect=None if chatwoot else
                    ('handoff_transition' if kind == 'transition' else 'reply'),
                scheduler_epoch=None if chatwoot else graph['epoch'],
                conversation_turn_id=None if chatwoot else graph['turn'],
                turn_revision_id=None if chatwoot else graph['revision'],
                agente_id=graph['agent'], sessao_id=graph['session'],
                handoff_attempt_id=graph['handoff'], ordering_key=str(uuid4()),
                stream_sequence=0 if chatwoot else 1, status=status, created_at=old,
                depends_on_delivery_id=depends_on, terminal_reason=terminal_reason,
                ambiguity_count=ambiguity,
                terminal_at=old if status in {'delivered', 'dead', 'cancelled'} else None,
                next_attempt_at=old if live else None,
                expires_at=datetime.utcnow()+timedelta(days=1) if live else None)
            assert not blocked or depends_on is not None
            db.add(effect); await db.commit()
            return effect.id

    async def worker_pass(self):
        """The real DeliveryWorker._retention: legacy commit, then scheduler."""
        with patch.object(worker_module, 'AsyncSessionLocal', self.sessions):
            await worker_module.DeliveryWorker()._retention()

    async def handoff_effects(self, graph):
        """Realistic handoff stream: open <- note chain(3); reply; transition <- open."""
        opened = await self.effect(graph, kind='open')
        parent = opened
        for _ in range(3):
            parent = await self.effect(graph, kind='note', depends_on=parent)
        await self.effect(graph, kind='reply')
        await self.effect(graph, kind='transition', depends_on=opened)

    async def test_ordinary_full_purge_preserves_ledger_and_messages(self):
        await self.graph()
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.revisions_purged,
                          result.attempts_purged, result.memberships_purged, result.receipts_purged),
                         (1,1,1,1,1,1))
        for model in (ConversationTurn, TurnRevision, GenerationAttempt, TurnInputMembership, InboundEvent):
            self.assertEqual(await self.count(model), 0)
        self.assertEqual(await self.count(ConsumoLLM), 1)
        self.assertEqual(await self.count(Mensagem), 2)

    async def test_refresh_cycle_deleted_replacement_first(self):
        await self.graph(refresh=True)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.revisions_purged, result.turns_purged), (2,2,1))

    async def test_seven_day_scrub_not_purge(self):
        await self.graph(days=8, refresh=True)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged), (2,0))
        self.assertEqual(await self.count(InboundEvent), 1)

    async def test_younger_than_seven_days_untouched(self):
        await self.graph(days=6)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual(result.examined, 0)
        self.assertEqual(result.drafts_scrubbed, 0)

    async def test_nonterminal_old_turn_keeps_everything(self):
        graph = await self.graph(terminal=False)
        # Locked defense-in-depth: refuses explicitly, never relies on NULL terminal_at.
        guarded = await self.retain_directly(graph)
        self.assertEqual((guarded.drafts_scrubbed, guarded.retained), (0, 1))
        self.assertEqual(retention.retention_counts()['nonterminal'], 1)
        retention._counts.clear()
        retention._coverage = None
        retention._last_complete_scan_at = None
        retention._last_complete_scan_monotonic = None
        retention._observing_since = time.monotonic()
        retention._sla_episodes = set()
        retention._candidate_last_pass = None
        retention._candidate_last_failure = None
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.retained), (0,0,1))
        self.assertEqual(retention.retention_counts()['nonterminal'], 1)

    async def test_started_and_unknown_and_unreconciled_retained(self):
        for outcome, accounting in [('started','pending'), ('outcome_unknown','reconciliation_required'),
                                    ('succeeded','reconciliation_required')]:
            await self.graph(outcome=outcome, accounting=accounting)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.retained), (0,0,3))
        self.assertEqual(await self.count(GenerationAttempt), 3)

    async def test_resolved_quarantine_can_purge_but_keeps_ledger(self):
        await self.graph(accounting='reconciliation_required', resolved=True)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual(result.turns_purged, 1)
        self.assertEqual(await self.count(ConsumoLLM), 1)

    async def test_pending_reconciliation_keeps_evidence(self):
        graph = await self.graph()
        async with self.sessions() as db:
            db.add(LLMUsageReconciliation(agente_id=graph['agent'], sessao_id=graph['session'],
                modelo_llm='openai/gpt-4o-mini', tipo_chamada='resposta',
                safe_error_code='synthetic', status='dead'))
            await db.commit()
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.retained), (0,0,1))
        self.assertEqual(await self.count(LLMUsageReconciliation), 1)

    async def test_recent_receipt_blocks_purge_not_scrub(self):
        await self.graph(receipt_days=89)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.retained), (1,0,1))
        self.assertEqual(await self.count(TurnInputMembership), 1)

    async def test_unsent_effect_keeps_text_and_graph(self):
        graph = await self.graph()
        await self.effect(graph, status='pending')
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.retained), (0,0,1))
        self.assertEqual(await self.count(OutboxDelivery), 1)

    async def test_handoff_notes_cleanup_then_graph_mapping_survives(self):
        graph = await self.graph(handoff=True, refresh=True)
        await self.effect(graph, note=True)
        await self.effect(graph)
        # Pass 1 (legacy then scheduler): note purged, automated reply only
        # scrubbed; the graph waits for its effect with a visible signal.
        await self.worker_pass()
        self.assertEqual(await self.count(OutboxDelivery), 1)
        self.assertEqual(await self.count(ConversationTurn), 1)
        self.assertEqual(retention.retention_counts()['delivery_reference'], 1)
        # Pass 2: reply deleted by legacy, then the graph purges.
        await self.worker_pass()
        self.assertEqual((await self.count(OutboxDelivery), await self.count(ConversationTurn),
                          await self.count(ScheduledHandoff),
                          await self.count(HandoffContextAssignment)), (0, 0, 0, 0))
        self.assertEqual(await self.count(HandoffAttempt), 1)
        self.assertEqual(await self.count(Mensagem), 2)
        async with self.sessions() as db:
            attempt = await db.get(HandoffAttempt, graph['handoff'])
            self.assertEqual((attempt.status, attempt.chatwoot_conversation_id), ('ready',graph['agent']))

    async def test_n_passes_converge_or_retain_with_signal(self):
        chain = await self.graph(handoff=True, refresh=True)
        await self.handoff_effects(chain)
        stuck = await self.graph(handoff=True)
        superseded = await self.graph(handoff=True, session_generation=2)
        for graph in (stuck, superseded):
            await self.effect(graph, kind='note', status='dead',
                              terminal_reason='context_note_ambiguous', ambiguity=1)
        converged: dict[str, int] = {}
        passes = 8
        for number in range(1, passes + 1):
            await self.worker_pass()
            async with self.sessions() as db:
                for name, graph in (('chain', chain), ('stuck', stuck), ('superseded', superseded)):
                    if name not in converged and await db.get(ConversationTurn, graph['turn']) is None:
                        converged[name] = number
        # Finite: chain needs exactly two legacy passes (scrub, then delete of
        # automated rows; dependency chain shrinks per pass), superseded note
        # one. An unresolved current-generation ambiguous note is audit
        # evidence: retained every pass, signalled every pass, never silent.
        self.assertEqual(converged, {'chain': 2, 'superseded': 1})
        self.assertEqual(retention.retention_counts()['delivery_reference'], 1 + passes)
        self.assertEqual(await self.count(ConversationTurn), 1)
        self.assertEqual(await self.count(OutboxDelivery), 1)
        self.assertEqual(await self.count(HandoffAttempt), 3)
        self.assertEqual(await self.count(Mensagem), 6)
        self.assertEqual(await self.count(ConsumoLLM), 3)

    async def test_limit_plus_one_blocked_rows_rotate_to_victim(self):
        for _ in range(3):
            await self.graph(outcome='outcome_unknown', accounting='reconciliation_required')
        victim = await self.graph()
        first = await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
        second = await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
        self.assertEqual((first.examined, first.turns_purged, second.examined, second.turns_purged), (2,0,2,1))
        async with self.sessions() as db:
            self.assertIsNone(await db.get(ConversationTurn, victim['turn']))
        self.assertEqual(await self.count(ConversationTurn), 3)

    async def hold_lane(self, graph, turn_id):
        async with self.sessions() as db:
            db.add(AgentServiceLane(agente_id=graph['agent'], scheduler_epoch=graph['epoch'],
                next_arrival_sequence=3, next_scheduling_sequence=3,
                active_turn_id=turn_id, owner_token=str(uuid4()),
                owner_worker_id='t082-worker', lease_until=datetime.utcnow()+timedelta(hours=1)))
            await db.commit()

    async def test_cursor_rotation_n_passes_reaches_every_victim(self):
        # limit=2; four stuck rows (> limit+1) around three victims; v0 sits
        # BELOW the cursor once its blocker clears and needs the wrap.
        v0 = await self.graph()
        await self.hold_lane(v0, v0['turn'])
        stuck = [await self.graph(outcome='outcome_unknown', accounting='reconciliation_required')
                 for _ in range(3)]
        v1 = await self.graph()
        stuck.append(await self.graph(outcome='outcome_unknown', accounting='reconciliation_required'))
        v2 = await self.graph()
        gone: dict[str, int] = {}
        for number in range(1, 7):
            await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
            if number == 1:
                self.assertEqual(retention.retention_counts()['lane_reference'], 1)
                async with self.sessions() as db:
                    await db.execute(text('UPDATE agent_service_lanes SET owner_token=NULL, '
                        'owner_worker_id=NULL, lease_until=NULL, active_turn_id=NULL '
                        'WHERE agente_id=:agent'), {'agent': v0['agent']})
                    await db.commit()
            async with self.sessions() as db:
                for name, graph in (('v0', v0), ('v1', v1), ('v2', v2)):
                    if name not in gone and await db.get(ConversationTurn, graph['turn']) is None:
                        gone[name] = number
        self.assertEqual(gone, {'v1': 3, 'v2': 4, 'v0': 5})
        self.assertEqual(await self.count(ConversationTurn), len(stuck))
        # Signalled on every examination: sweep 1 (passes 1-4) all four stuck
        # rows, sweep 2 (passes 5-6) s1, s2, s3.
        self.assertEqual(retention.retention_counts()['accounting_evidence'], 4 + 3)

    async def test_young_page_ends_sweep_and_wraps_to_victim(self):
        victim = await self.graph()
        for _ in range(3):
            await self.graph(days=1)
        async with self.sessions() as db:
            await db.execute(text("UPDATE scheduler_retention_progress SET last_turn_id=:turn WHERE id=1"),
                             {"turn": victim['turn']})
            await db.commit()
        first = await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
        # Page after the cursor starts young -> wrap inside the same pass.
        self.assertEqual((first.scanned, first.examined, first.turns_purged), (2, 1, 1))
        async with self.sessions() as db:
            self.assertEqual((await db.get(SchedulerRetentionProgress, 1)).last_turn_id, 0)
        self.assertEqual(await self.count(ConversationTurn), 3)

    async def test_scrubbed_terminal_waits_without_locks(self):
        graph = await self.graph(days=30)
        first = await retention.cleanup_retention(self.sessions)
        self.assertEqual((first.examined, first.drafts_scrubbed), (1, 1))
        locks = []
        original = lifecycle_repo.lock_lifecycle_prefix
        async def watched(db, **kw):
            locks.append(kw)
            return await original(db, **kw)
        with patch.object(lifecycle_repo, 'lock_lifecycle_prefix', watched):
            second = await retention.cleanup_retention(self.sessions)
        self.assertEqual((second.scanned, second.examined, locks), (1, 0, []))
        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(ConversationTurn, graph['turn']))

    async def test_succeeded_without_exact_ledger_retained(self):
        await self.graph(with_ledger=False)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.retained), (0, 0, 1))
        self.assertEqual(retention.retention_counts()['accounting_evidence'], 1)

    async def retain_directly(self, graph):
        async def operation(db):
            return await retention._retain_one(db, turn_id=graph['turn'], agente_id=graph['agent'],
                sessao_id=graph['session'], payload_days=7, receipt_days=90)
        return await lifecycle_repo.run_in_new_session(self.sessions, operation)

    async def test_locked_path_rechecks_ages_directly(self):
        # Bypass discovery: the locked transaction alone must enforce both
        # horizons from the turn's immutable terminal_at.
        fresh = await self.graph(terminal_days=2)
        mid = await self.graph(terminal_days=30)
        first = await self.retain_directly(fresh)
        second = await self.retain_directly(mid)
        self.assertEqual((first.drafts_scrubbed, first.turns_purged), (0, 0))
        self.assertEqual((second.drafts_scrubbed, second.turns_purged), (1, 0))
        self.assertEqual(await self.count(ConversationTurn), 2)

    async def later_turn(self, graph, *, terminal=True, assign_handoff=None, revision_id=None):
        """Second turn of the same session (e.g. a cancelled successor)."""
        old = datetime.utcnow() - timedelta(days=100)
        async with self.sessions() as db:
            message = Mensagem(sessao_id=graph['session'], role='user', conteudo='later lead')
            db.add(message); await db.flush()
            turn = ConversationTurn(agente_id=graph['agent'], sessao_id=graph['session'],
                scheduler_epoch=graph['epoch'], arrival_sequence=2, scheduling_sequence=2,
                status='cancelled' if terminal else 'pending',
                session_slot='terminal' if terminal else 'successor', ready_at=old,
                created_at=old, terminal_at=old if terminal else None,
                terminal_reason='handoff_superseded' if terminal else None)
            db.add(turn); await db.flush()
            revision = TurnRevision(turn_id=turn.id, scheduler_epoch=graph['epoch'],
                revision_number=1, status='cancelled', draft_text='successor draft',
                created_at=old)
            db.add(revision); await db.flush()
            receipt = InboundEvent(provider='evolution', source_scope='t082',
                provider_event_id=str(uuid4()), event_type='messages.upsert',
                payload_hmac='b'*64, hmac_key_version=1, agente_id=graph['agent'],
                sessao_id=graph['session'], mensagem_id=message.id,
                receipt_disposition='scheduled', received_at=old)
            db.add(receipt); await db.flush()
            member = TurnInputMembership(turn_id=turn.id, sessao_id=graph['session'],
                mensagem_id=message.id, inbound_event_id=receipt.id, input_sequence=2,
                content_bytes=10, membership_state='post_handoff_pending' if assign_handoff
                else 'sealed', assigned_revision_id=revision_id or revision.id, created_at=old)
            db.add(member); await db.flush()
            if assign_handoff:
                db.add(HandoffContextAssignment(turn_input_membership_id=member.id,
                    handoff_attempt_id=assign_handoff, agente_id=graph['agent'],
                    sessao_id=graph['session'], coverage_ordinal=2, source_id='context:2',
                    created_at=old))
            await db.commit()
            return turn.id

    async def test_successor_context_assignment_waits_for_parent_handoff(self):
        # Parent handoff turn still awaiting delivery: its context assignment
        # on the cancelled successor's membership is live authority.
        parent = await self.graph(handoff=True, terminal=False)
        successor = await self.later_turn(parent, assign_handoff=parent['handoff'])
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual(result.turns_purged, 0)
        self.assertEqual(retention.retention_counts()['context_reference'], 1)
        self.assertEqual(retention.retention_counts()['nonterminal'], 1)
        self.assertEqual(await self.count(HandoffContextAssignment), 2)
        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(ConversationTurn, successor))

    async def test_unexpected_restrict_and_poison_candidate_do_not_block_rotation(self):
        # A foreign membership pinned to this turn's revision is an unexpected
        # RESTRICT dependency: real 1451, whole candidate rolled back.
        pinned = await self.graph()
        await self.later_turn(pinned, revision_id=pinned['revision'])
        poison = await self.graph()
        victim = await self.graph()
        original = retention._retain_one
        async def poisoned(db, **kw):
            if kw['turn_id'] == poison['turn']:
                raise RuntimeError('synthetic poison candidate')
            return await original(db, **kw)
        with patch.object(retention, '_retain_one', poisoned):
            result = await retention.cleanup_retention(self.sessions)
        counts = retention.retention_counts()
        self.assertEqual((counts['foreign_key_reference'], counts['candidate_error']), (1, 1))
        async with self.sessions() as db:
            self.assertIsNone(await db.get(ConversationTurn, victim['turn']))
            self.assertIsNotNone(await db.get(ConversationTurn, pinned['turn']))
            self.assertIsNotNone(await db.get(ConversationTurn, poison['turn']))
            revision = await db.get(TurnRevision, pinned['revision'])
            # Rolled back as a unit: not even the scrub of the pinned graph stuck.
            self.assertEqual(revision.draft_text, 'private draft')
        self.assertGreaterEqual(result.turns_purged, 1)

    async def enable_scheduler(self):
        async with self.sessions() as db:
            await db.execute(text("UPDATE delivery_runtime_state SET mode='outbox'"))
            await db.execute(text("UPDATE scheduler_runtime_state SET state='enabled'"))
            runtime = (await db.execute(text('SELECT cutover_epoch FROM delivery_runtime_state'))).scalar_one()
            scheduler = (await db.execute(text('SELECT scheduler_epoch FROM scheduler_runtime_state'))).scalar_one()
            await db.commit()
        return runtime, scheduler

    async def successor_turn(self, graph):
        """Lane + a ready active turn in the same session as the old terminal one."""
        async with self.sessions() as db:
            db.add(AgentServiceLane(agente_id=graph['agent'], scheduler_epoch=graph['epoch'],
                                    next_arrival_sequence=3, next_scheduling_sequence=3))
            turn = ConversationTurn(agente_id=graph['agent'], sessao_id=graph['session'],
                scheduler_epoch=graph['epoch'], arrival_sequence=2, scheduling_sequence=2,
                status='pending', session_slot='active',
                ready_at=datetime.utcnow()-timedelta(seconds=5))
            db.add(turn); await db.commit()
            return turn.id

    async def claim(self, db, graph, turn_id, runtime, scheduler):
        return await scheduler_repo.claim_ready_turn(db, agente_id=graph['agent'],
            scheduler_epoch=scheduler, delivery_runtime_epoch=runtime,
            worker_id='t082-turn-worker', lease_seconds=60,
            candidate=(turn_id, graph['session']))

    async def test_turn_worker_claim_race_both_orders(self):
        runtime, scheduler = await self.enable_scheduler()
        # Order 1: the turn worker's claim holds the authority prefix first.
        first = await self.graph(epoch=scheduler)
        successor = await self.successor_turn(first)
        async with self.sessions() as worker:
            claimed = await self.claim(worker, first, successor, runtime, scheduler)
            self.assertEqual(claimed.turn_id, successor)
            task = asyncio.create_task(retention.cleanup_retention(self.sessions))
            await asyncio.sleep(0.2)
            self.assertFalse(task.done())
            await worker.commit()
        result = await asyncio.wait_for(task, 10)
        self.assertEqual(result.turns_purged, 1)
        async with self.sessions() as db:
            lane = await db.get(AgentServiceLane, first['agent'])
            turn = await db.get(ConversationTurn, successor)
            self.assertEqual((lane.active_turn_id, turn.status), (successor, 'claimed'))
            self.assertIsNone(await db.get(ConversationTurn, first['turn']))
        # Order 2: retention holds the prefix; the claim waits and then wins.
        second = await self.graph(epoch=scheduler)
        successor = await self.successor_turn(second)
        locked, release = asyncio.Event(), asyncio.Event()
        original = lifecycle_repo.lock_lifecycle_prefix
        async def held(db, **kw):
            prefix = await original(db, **kw)
            locked.set(); await release.wait()
            return prefix
        with patch.object(lifecycle_repo, 'lock_lifecycle_prefix', held):
            task = asyncio.create_task(retention.cleanup_retention(self.sessions))
            await asyncio.wait_for(locked.wait(), 5)
            async def claim():
                async with self.sessions() as db:
                    result = await self.claim(db, second, successor, runtime, scheduler)
                    await db.commit()
                    return result
            claiming = asyncio.create_task(claim())
            await asyncio.sleep(0.2)
            self.assertFalse(claiming.done())
            release.set()
            result = await asyncio.wait_for(task, 10)
            claimed = await asyncio.wait_for(claiming, 10)
        self.assertEqual((result.turns_purged, claimed.turn_id), (1, successor))
        self.assertEqual(await self.count(ConversationTurn), 2)
        self.assertEqual(await self.count(ConsumoLLM), 2)

    async def test_lane_referencing_terminal_turn_retains_graph(self):
        graph = await self.graph()
        await self.hold_lane(graph, graph['turn'])
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual((result.drafts_scrubbed, result.turns_purged, result.retained), (0, 0, 1))
        self.assertEqual(retention.retention_counts()['lane_reference'], 1)

    async def test_delete_after_purge_and_delete_race(self):
        graph = await self.graph()
        async with self.sessions() as db:
            with self.assertRaises(lifecycle_repo.LifecycleRefused):
                await lifecycle_repo.delete_session(db, agente_id=graph['agent'], telefone='synthetic-t082')
            await db.rollback()
        locked, release = asyncio.Event(), asyncio.Event()
        original = lifecycle_repo.lock_lifecycle_prefix
        async def held(db, **kw):
            prefix = await original(db, **kw)
            locked.set(); await release.wait()
            return prefix
        with patch.object(lifecycle_repo, 'lock_lifecycle_prefix', held):
            task = asyncio.create_task(retention.cleanup_retention(self.sessions))
            await asyncio.wait_for(locked.wait(), 5)
            async def delete_session():
                async with self.sessions() as db:
                    result = await lifecycle_repo.delete_session(db, agente_id=graph['agent'], telefone='synthetic-t082')
                    await db.commit()
                    return result
            deleting = asyncio.create_task(delete_session())
            await asyncio.sleep(0.05)
            self.assertFalse(deleting.done())
            release.set()
            self.assertEqual((await asyncio.wait_for(task, 10)).turns_purged, 1)
            self.assertEqual(await asyncio.wait_for(deleting, 10), 'deleted')
        self.assertEqual(await self.count(ConsumoLLM), 1)

    async def test_late_accounting_lock_wait_reads_fresh_state(self):
        graph = await self.graph(outcome='started', accounting='pending')
        async with self.sessions() as writer:
            # Accounting-only prefix matches the actual runtime: parent S then attempt X.
            await writer.execute(select(Agente).where(Agente.id==graph['agent']).with_for_update(read=True))
            await writer.execute(select(Sessao).where(Sessao.id==graph['session']).with_for_update(read=True))
            attempt = (await writer.execute(select(GenerationAttempt).where(
                GenerationAttempt.id==graph['attempt']).with_for_update())).scalar_one()
            task = asyncio.create_task(retention.cleanup_retention(self.sessions))
            await asyncio.sleep(0.1)
            self.assertFalse(task.done())
            attempt.outcome='failed'; attempt.outcome_recorded_at=datetime.utcnow()
            await writer.commit()
            result = await asyncio.wait_for(task, 10)
        # Fresh recent outcome: scrub allowed, purge prohibited for another 90d.
        self.assertEqual((result.drafts_scrubbed, result.turns_purged), (1,0))
        self.assertEqual(retention.retention_counts()['recent_accounting'], 1)

    async def test_discovery_explain_primary_range(self):
        # Enough rows and fresh statistics (ANALYZE needs only SELECT+INSERT)
        # that the plan is not a tiny-table artefact.
        for _ in range(20):
            await self.graph(refresh=True)
        query = retention.discovery_query(1, 2)
        sql = str(query.compile(self.engine.sync_engine, compile_kwargs={'literal_binds': True}))
        async with self.sessions() as db:
            await db.execute(text('ANALYZE TABLE conversation_turns, turn_revisions'))
            rows = (await db.execute(text('EXPLAIN '+sql))).mappings().all()
        self.assertEqual(rows[0]['key'], 'PRIMARY')
        self.assertEqual(rows[0]['type'], 'range')
        # Dependent draft probe uses the turn_id index of turn_revisions.
        probe = [row for row in rows if row['table'] == 'turn_revisions']
        self.assertEqual(len(probe), 1)
        self.assertIn(probe[0]['type'], {'ref', 'range'})
        self.assertIsNotNone(probe[0]['key'])

    async def test_contract_parity(self):
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        doc = (root / 'AGENTS.md').read_text()
        self.assertIn(lifecycle_repo.SCHEDULER_HISTORY_MESSAGE, doc)
        self.assertIn('scheduler_retention_error_threshold', (root / 'app/src/delivery_worker.py').read_text())
        self.assertIn('`mensagens` de lead/assistant são preservadas', doc)
        self.assertEqual(retention.SIGNAL_THRESHOLD, 1)

    async def test_real_deadlock_inside_savepoint_retries_whole_graph(self):
        await self._real_deadlock(nested=True)

    async def test_real_deadlock_production_path_retries_whole_graph(self):
        await self._real_deadlock(nested=False)

    async def _real_deadlock(self, *, nested: bool):
        graph = await self.graph()
        at_session = asyncio.Event()
        original_lock = retention._lock
        original_one = retention._retain_one
        calls = 0
        async def watched_lock(db, model, *args, **kw):
            if model is Sessao:
                at_session.set()
            return await original_lock(db, model, *args, **kw)
        async def counted(db, **kw):
            nonlocal calls
            calls += 1
            if not nested:
                return await original_one(db, **kw)
            # Fault injection only: production retention adds no savepoint.
            async with db.begin_nested():
                return await original_one(db, **kw)
        before = lifecycle_repo.lifecycle_counts()['deadlock_retried']
        async with self.sessions() as other:
            # Reverse order: session X first, then the scheduler mutex the
            # retention transaction already holds -> InnoDB 1213, real victim.
            await other.execute(select(Sessao).where(Sessao.id==graph['session']).with_for_update())
            other.add_all(Atendente(nome='t082-weight', numero_whatsapp=str(uuid4())) for _ in range(40))
            await other.flush()
            with patch.object(retention, '_lock', watched_lock), patch.object(retention, '_retain_one', counted):
                task = asyncio.create_task(retention.cleanup_retention(self.sessions))
                await asyncio.wait_for(at_session.wait(), 10)
                await asyncio.sleep(0.1)
                await asyncio.wait_for(other.execute(text(
                    'SELECT id FROM scheduler_runtime_state WHERE id=1 FOR UPDATE')), 10)
                await other.rollback()
                result = await asyncio.wait_for(task, 15)
        self.assertEqual(lifecycle_repo.lifecycle_counts()['deadlock_retried'], before+1)
        self.assertEqual((calls, result.turns_purged, result.attempts_purged), (2,1,1))
        self.assertEqual(await self.count(ConsumoLLM), 1)


    async def test_sec_s1_many_scrubbed_pages_reach_victim_in_one_interval(self):
        # k*limit old, already scrubbed rows precede a draft due at 7d.
        for _ in range(12):
            g = await self.graph(days=30)
            async with self.sessions() as db:
                await db.execute(text("UPDATE turn_revisions SET draft_text=NULL WHERE id=:id"),
                                 {"id": g['revision']})
                await db.commit()
        victim = await self.graph(days=7.5)
        result = await retention.cleanup_retention(self.sessions, limit=2)
        self.assertEqual((result.scanned, result.drafts_scrubbed), (13, 1))
        async with self.sessions() as db:
            self.assertIsNone((await db.get(TurnRevision, victim['revision'])).draft_text)

    async def test_sec_s1_restart_every_pass_and_cas_aba(self):
        for _ in range(5):
            await self.graph(outcome='outcome_unknown', accounting='reconciliation_required')
        victim = await self.graph(days=7.5)
        for number in range(3):
            importlib.reload(retention)  # new process has NO retained cursor/counters
            result = await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
            self.assertEqual(result.drafts_scrubbed, int(number == 2))
        async with self.sessions() as db:
            checkpoint = await db.get(SchedulerRetentionProgress, 1)
            self.assertEqual(checkpoint.last_turn_id, victim['turn'])
            version = checkpoint.version
        # Another worker wraps. Stale writer cannot regress even to same cursor (ABA).
        self.assertTrue(await retention._checkpoint(self.sessions, version=version, cursor=0))
        self.assertFalse(await retention._checkpoint(self.sessions, version=version, cursor=1))
        winners = await asyncio.gather(
            retention._checkpoint(self.sessions, version=version + 1, cursor=111),
            retention._checkpoint(self.sessions, version=version + 1, cursor=222))
        self.assertEqual(sorted(winners), [False, True])
        self.assertTrue(await retention._checkpoint(self.sessions, version=version + 2, cursor=0))
        async with self.sessions() as db:
            self.assertEqual((await db.get(SchedulerRetentionProgress, 1)).last_turn_id, 0)

    async def test_sec_s1_time_budget_rolls_back_and_advances(self):
        first = await self.graph()
        victim = await self.graph()
        original = retention._retain_one
        async def slow(db, **kw):
            if kw['turn_id'] == first['turn']:
                await original(db, **kw)
                await asyncio.sleep(2)
            return await original(db, **kw)
        start = time.monotonic()
        with patch.object(retention, '_retain_one', slow):
            result = await retention.cleanup_retention(self.sessions, max_seconds=0.1)
        self.assertLess(time.monotonic() - start, 1.5)
        self.assertEqual(result.budget_exhausted, 1)
        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(ConversationTurn, first['turn']))
            self.assertEqual((await db.get(SchedulerRetentionProgress, 1)).last_turn_id, first['turn'])
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual(result.turns_purged, 1)
        async with self.sessions() as db:
            self.assertIsNone(await db.get(ConversationTurn, victim['turn']))

    async def test_sec_s1_checkpoint_failure_degrades_with_signal(self):
        graph = await self.graph(outcome='outcome_unknown', accounting='reconciliation_required')
        records = []
        sink = logger.add(lambda message: records.append(message.record))
        try:
            with patch.object(retention, '_checkpoint', side_effect=TimeoutError()):
                result = await retention.cleanup_retention(self.sessions)
            self.assertEqual((result.examined, result.retained), (1, 1))
            self.assertEqual(retention.retention_counts()['checkpoint_failed'], 1)
            async with self.sessions() as db:
                self.assertEqual((await db.get(SchedulerRetentionProgress, 1)).version, 0)
                await db.execute(text("DELETE FROM scheduler_retention_progress"))
                await db.commit()
            try:
                result = await retention.cleanup_retention(self.sessions)
            finally:
                async with self.sessions() as db:
                    await db.execute(text("INSERT INTO scheduler_retention_progress VALUES (1, 0, 0)"))
                    await db.commit()
        finally:
            logger.remove(sink)
        self.assertEqual((result.scanned, result.examined), (0, 0))
        self.assertEqual(retention.retention_counts()['checkpoint_unavailable'], 1)
        self.assertTrue(all(r['exception'] is None for r in records if r['level'].name == 'ERROR'))
        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(ConversationTurn, graph['turn']))

    async def test_sec_s1_cross_tenant(self):
        a = await self.graph()
        b = await self.graph()
        locked, release = asyncio.Event(), asyncio.Event()
        original = retention._lock
        async def held(db, model, *args, **kw):
            rows = await original(db, model, *args, **kw)
            if model is LLMUsageReconciliation:
                locked.set()
                await release.wait()
            return rows
        with patch.object(retention, '_lock', held):
            task = asyncio.create_task(retention.cleanup_retention(self.sessions, max_rows=1))
            try:
                await asyncio.wait_for(locked.wait(), 5)
                async with self.sessions() as db:
                    await db.execute(text("SET SESSION innodb_lock_wait_timeout=1"))
                    for model, key in ((GenerationAttempt, 'attempt'), (TurnRevision, 'revision')):
                        await db.execute(select(model).where(model.id == b[key]).with_for_update(nowait=True))
                    await db.execute(text("UPDATE generation_attempts SET outcome_recorded_at=outcome_recorded_at WHERE id=:id"), {'id': b['attempt']})
                    await db.execute(text("UPDATE turn_revisions SET draft_text=NULL WHERE id=:id"), {'id': b['revision']})
                    row = LLMUsageReconciliation(agente_id=b['agent'], sessao_id=b['session'],
                        modelo_llm='openai/gpt-4o-mini', tipo_chamada='resposta',
                        safe_error_code='synthetic', status='pending')
                    db.add(row); await db.flush()
                    row.safe_error_code = 'synthetic_update'; await db.flush()
                    await db.commit()
                    # New revision + attempt under B while A remains held.
                    revision = TurnRevision(turn_id=b['turn'], scheduler_epoch=b['epoch'],
                        revision_number=2, refresh_ordinal=1, status='cancelled')
                    db.add(revision); await db.flush()
                    db.add(GenerationAttempt(agente_id=b['agent'], sessao_id=b['session'],
                        turn_id=b['turn'], revision_id=revision.id, scheduler_epoch=b['epoch'],
                        lane_owner_token=str(uuid4()), attempt_type='response', attempt_ordinal=1,
                        outcome='started', accounting_state='pending', modelo_llm='openai/gpt-4o-mini',
                        started_at=datetime.utcnow()))
                    await db.flush(); await db.commit()
            finally:
                release.set()
                await asyncio.wait_for(task, 10)
        self.assertEqual(await self.count(ConversationTurn), 1)
        print('SEC08_CROSS_TENANT B_revision_INSERT_UPDATE=free B_attempt_INSERT_UPDATE=free B_reconciliation_INSERT_UPDATE=free')

    async def test_sec_s1_nonterminal_aggregate_without_lifecycle(self):
        for _ in range(5):
            await self.graph(terminal=False)
        records = []
        sink = logger.add(lambda message: records.append(message.record))
        try:
            with patch.object(lifecycle_repo, 'lock_lifecycle_prefix', side_effect=AssertionError('no authority needed')) as locks:
                for _ in range(2):
                    result = await retention.cleanup_retention(self.sessions, limit=2)
                    locks.assert_not_called()
                    self.assertEqual(result.nonterminal_observed, 5)
        finally:
            logger.remove(sink)
        errors = [r for r in records if r['level'].name == 'ERROR']
        self.assertEqual(len(errors), 2)  # one aggregate per pass, never per turn
        self.assertTrue(all('nonterminal aggregate' in r['message'] for r in errors))
        self.assertEqual(retention.retention_counts()['nonterminal'], 10)

    async def test_sec_s1_sla_health_and_safe_error(self):
        await self.graph(outcome='outcome_unknown', accounting='reconciliation_required')
        records = []
        sink = logger.add(lambda message: records.append(message.record))
        try:
            with patch.object(retention, '_retain_one', side_effect=RuntimeError('PRIVATE_ERROR_CANARY')):
                await retention.cleanup_retention(self.sessions)
        finally:
            logger.remove(sink)
        errors = [r for r in records if 'candidate failed' in r['message']]
        self.assertEqual(len(errors), 1)
        self.assertIsNone(errors[0]['exception'])
        self.assertNotIn('PRIVATE_ERROR_CANARY', errors[0]['message'])
        health = retention.retention_health()
        self.assertTrue(health['available'])
        for key in ('draft', 'purge'):
            self.assertGreater(health[f'oldest_{key}_terminal_age_seconds'], 99*86400)
            self.assertTrue(health[f'{key}_sla_exceeded'])
            self.assertEqual(retention.retention_counts()[f'{key}_sla_exceeded'], 1)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(worker_module.settings, 'delivery_worker_health_path', directory + '/health.json'):
                worker_module.DeliveryWorker()._publish_health_snapshot()
            snapshot = json.loads(Path(directory + '/health.json').read_text())
        for key, value in health.items():
            if key not in {'last_complete_scan_age_seconds', 'candidate_failures'}:
                self.assertEqual(snapshot['scheduler_retention_health'][key], value)
        self.assertEqual(snapshot['scheduler_retention_health']['candidate_failures']['counts'],
                         health['candidate_failures']['counts'])
        self.assertEqual(snapshot['scheduler_retention_error_threshold'], 1)
        async with self.sessions() as db:
            await db.execute(text("UPDATE turn_revisions SET draft_text=NULL"))
            await db.commit()
        await retention._observe_health(self.sessions, payload_days=7, receipt_days=900,
                                         tolerance_seconds=86400,
                                         draft_scan=retention.DraftScan(datetime.utcnow().isoformat(), 0, 0,
                                                                        True, complete=True))
        health = retention.retention_health()
        self.assertIsNone(health['oldest_draft_terminal_age_seconds'])
        self.assertIsNone(health['oldest_purge_terminal_age_seconds'])
        self.assertFalse(health['draft_sla_exceeded'])
        self.assertFalse(health['purge_sla_exceeded'])
        with patch.object(retention, 'select', side_effect=RuntimeError('failed')):
            await retention._observe_health(self.sessions, payload_days=7, receipt_days=90,
                                             tolerance_seconds=86400)
        self.assertFalse(retention.retention_health()['available'])
        self.assertEqual(retention.retention_counts()['health_unavailable'], 1)

    async def test_sec_s1_validated_budgets_and_contract(self):
        from pydantic import ValidationError
        from src.config import Settings
        for values in ({'max_rows': 0}, {'max_rows': 1000001}, {'max_rows': 1.5},
                       {'max_seconds': float('inf')}, {'max_seconds': float('nan')},
                       {'max_seconds': 0}, {'max_seconds': 301}):
            with self.assertRaises(ValueError):
                await retention.cleanup_retention(self.sessions, **values)
        for values in ({'scheduler_retention_max_rows': 0},
                       {'scheduler_retention_max_seconds': float('inf')},
                       {'scheduler_retention_max_seconds': float('nan')},
                       {'scheduler_retention_sla_tolerance_seconds': 0},
                       {'delivery_heartbeat_interval_seconds': 59, 'delivery_poll_interval_seconds': 1}):
            with self.assertRaises(ValidationError):
                Settings(**values)
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        for name, value in (('MAX_ROWS', 10000), ('MAX_SECONDS', 30), ('SLA_TOLERANCE_SECONDS', 86400)):
            token = f'SCHEDULER_RETENTION_{name}={value}'
            for path in ('AGENTS.md', '.env.example'):
                self.assertIn(token, (root / path).read_text())
        self.assertEqual(Settings(scheduler_retention_max_seconds=300).scheduler_retention_max_seconds, 300)
        self.assertEqual(retention.settings.scheduler_retention_max_rows, 10000)
        self.assertEqual(retention.settings.scheduler_retention_max_seconds, 30)
        self.assertEqual(retention.settings.scheduler_retention_sla_tolerance_seconds, 86400)

    async def test_sec2_s1_worker_loop_never_waits_for_retention(self):
        graph = await self.graph(days=10)
        entered, release = asyncio.Event(), asyncio.Event()
        original = retention._retain_one
        starts = 0
        async def slow(db, **kwargs):
            nonlocal starts
            starts += 1
            result = await original(db, **kwargs)
            entered.set()
            await release.wait()  # long budget, real MySQL transaction still open
            return result
        worker = worker_module.DeliveryWorker()
        claims, beats, replicas = [], [], []
        heartbeat, replica = worker._heartbeat, worker._replica_beat
        async def claim(capacity):
            async with self.sessions() as db:
                await db.execute(select(func.count()).select_from(ConversationTurn))
            claims.append(time.monotonic())
            return []
        async def beat():
            await heartbeat()
            beats.append(time.monotonic())
        async def replica_beat():
            await replica()
            replicas.append(time.monotonic())
        async def noop():
            pass
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(worker_module, 'AsyncSessionLocal', self.sessions), \
             patch.object(worker_module.settings, 'delivery_worker_health_path', directory + '/health.json'), \
             patch.object(worker_module.settings, 'delivery_poll_interval_seconds', 0.3), \
             patch.object(worker_module.settings, 'delivery_retention_interval_seconds', 0), \
             patch.object(retention.settings, 'scheduler_retention_max_seconds', 300), \
             patch.object(retention, '_retain_one', slow), \
             patch.object(worker, '_maintenance', noop), \
             patch.object(worker, '_claim', claim), \
             patch.object(worker, '_heartbeat', beat), \
             patch.object(worker, '_replica_beat', replica_beat), \
             patch.object(worker_module, 'engine', self.engine):
            # The 0.3 s budget measures loop scheduling, not the interpreter: a
            # full-heap gen2 collection late in the suite can pause ~0.5 s.
            gc.collect()
            gc.disable()
            started = time.monotonic()
            task = asyncio.create_task(worker.run())
            try:
                await asyncio.wait_for(entered.wait(), 2)
                await asyncio.sleep(0.7)
                self.assertGreaterEqual(len(claims), 2)
                self.assertLess(claims[0] - started, 0.3)
                self.assertLess(beats[0] - started, 0.3)
                self.assertLess(replicas[0] - started, 0.3)
                self.assertEqual(starts, 1)  # no overlapping pass at zero cadence
            finally:
                gc.enable()
                worker.request_stop()
                await asyncio.wait_for(task, 5)
        # Cancellation rolled back the candidate and released all graph locks.
        self.assertTrue(worker._retention_task.done())
        self.assertEqual(await self.draft(graph), 'private draft')
        async with self.sessions() as db:
            await db.execute(text('SET innodb_lock_wait_timeout=1'))
            await db.execute(select(ConversationTurn).where(
                ConversationTurn.id == graph['turn']).with_for_update())
            progress = await db.get(SchedulerRetentionProgress, 1)
            self.assertEqual(progress.last_turn_id, 0)
        result = await retention.cleanup_retention(self.sessions)
        self.assertEqual(result.drafts_scrubbed, 1)
        print('SEC2_S1_LOOP', {'first_claim': claims[0]-started,
              'first_heartbeat': beats[0]-started, 'first_replica': replicas[0]-started,
              'claims': len(claims), 'single_flight': starts})

    async def test_sec2_s1_discovery_and_legacy_errors_do_not_kill_worker(self):
        from sqlalchemy.exc import OperationalError
        failure = OperationalError('synthetic', {}, Exception(2013, 'synthetic disconnect'))
        graph = await self.graph(days=10)
        with patch.object(retention, 'discovery_query', side_effect=failure):
            result = await retention.cleanup_retention(self.sessions)
        self.assertEqual(result.scanned, 0)
        self.assertEqual(retention.retention_counts()['discovery_failed'], 1)
        async with self.sessions() as db:
            progress = await db.get(SchedulerRetentionProgress, 1)
            self.assertEqual((progress.last_turn_id, progress.version), (0, 0))
        worker = worker_module.DeliveryWorker()
        with patch.object(worker_module, 'AsyncSessionLocal', self.sessions), \
             patch.object(delivery_worker_repo, 'cleanup_retention', side_effect=failure):
            await worker._retention()
        self.assertEqual(worker._retention_errors['legacy'], 1)
        self.assertIsNone(await self.draft(graph))  # scheduler continues independently
        with patch.object(worker_module, 'AsyncSessionLocal', self.sessions), \
             patch.object(retention, 'cleanup_retention', side_effect=failure):
            await worker._retention()
        self.assertEqual(worker._retention_errors['scheduler'], 1)
        with patch.object(worker_module, 'AsyncSessionLocal', self.sessions):
            await worker._retention()  # next cadence still works

    async def test_sec2_s1_two_replicas_recheck_checkpoint_without_error_or_sla(self):
        for _ in range(8):
            await self.graph()
        first_entered, release = asyncio.Event(), asyncio.Event()
        original = retention._retain_one
        calls = {}
        async def pause_loser(db, **kwargs):
            name = asyncio.current_task().get_name()
            calls[name] = calls.get(name, 0) + 1
            if name == 'sec2-s1-loser' and calls[name] == 1:
                first_entered.set()
                await release.wait()
            return await original(db, **kwargs)
        errors = []
        sink = logger.add(lambda msg: errors.append(str(msg)), level='ERROR')
        try:
            with patch.object(retention, '_retain_one', pause_loser):
                loser = asyncio.create_task(retention.cleanup_retention(self.sessions, limit=4),
                                            name='sec2-s1-loser')
                await asyncio.wait_for(first_entered.wait(), 2)
                winner = await retention.cleanup_retention(self.sessions, limit=4)
                release.set()
                result = await asyncio.wait_for(loser, 5)
        finally:
            release.set()
            logger.remove(sink)
        self.assertEqual(winner.turns_purged, 8)
        self.assertLessEqual(calls['sec2-s1-loser'], 1)
        self.assertEqual(result.turns_purged, 0)
        self.assertEqual(await self.count(ConversationTurn), 0)
        self.assertGreaterEqual(retention.retention_counts()['checkpoint_contended'], 1)
        self.assertFalse(errors, errors)
        self.assertNotIn('draft_sla_exceeded', retention.retention_counts())
        self.assertNotIn('purge_sla_exceeded', retention.retention_counts())
        # FU-1: expected contention is informational, not a fresh old minimum.
        self.assertFalse(retention.retention_health()['draft_available'])
        self.assertEqual(retention.retention_health()['reason'], 'coverage_incomplete')
        self.assertTrue(retention.retention_health()['scan_continuity_lost'])
        print('SEC2_S1_REPLICAS', {'winner': winner.turns_purged, 'loser': result.turns_purged,
                                  'calls': calls, 'signals': retention.retention_counts()})

    async def test_sec2_s1_health_explain_indexed_with_synthetic_volume(self):
        from sqlalchemy.dialects import mysql
        graph = await self.graph(days=100)
        async with self.sessions.begin() as db:
            await db.execute(text('SET SESSION cte_max_recursion_depth=10001'))
            await db.execute(text("INSERT INTO conversation_turns "
                "(turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,"
                "status,session_slot,ready_at,terminal_at,terminal_reason,created_at) "
                "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<10000) "
                "SELECT UUID(),:a,:s,:e,n+10,n+10,'delivered','terminal',UTC_TIMESTAMP(6),"
                "UTC_TIMESTAMP(6)-INTERVAL 5 DAY,'synthetic',UTC_TIMESTAMP(6)-INTERVAL 5 DAY FROM seq"),
                {'a': graph['agent'], 's': graph['session'], 'e': graph['epoch']})
        async with self.sessions() as db:
            for draft in (False, True):
                sql = str(retention.health_query(draft=draft, turn_ids=(graph['turn'],)).compile(dialect=mysql.dialect(),
                    compile_kwargs={'literal_binds': True}))
                rows = [dict(r) for r in (await db.execute(text('EXPLAIN '+sql))).mappings()]
                turns = [r for r in rows if r['table'] == 'conversation_turns']
                self.assertTrue(turns, rows)
                expected_key = 'PRIMARY' if draft else 'idx_turn_retention_terminal'
                self.assertTrue(all(r['key'] == expected_key and
                                    r['type'] in {'ref', 'range', 'const'} for r in turns), rows)
                self.assertFalse([r for r in rows if r['type'] == 'ALL'], rows)
                self.assertIn('LIMIT 1', sql)
                self.assertNotIn('min(', sql.lower())
                print('SEC2_S1_HEALTH_EXPLAIN', draft, json.dumps(rows, default=str))
        await retention._observe_health(self.sessions, payload_days=7, receipt_days=90,
                                       tolerance_seconds=86400,
                                       draft_scan=retention.DraftScan(datetime.utcnow().isoformat(), 0, 0,
                                           True, oldest=await retention._draft_probe(self.sessions,
                                               (graph['turn'],)), complete=True))
        self.assertTrue(retention.retention_health()['draft_sla_exceeded'])
        self.assertTrue(retention.retention_health()['purge_sla_exceeded'])

    async def draft(self, graph):
        async with self.sessions() as db:
            revision = await db.get(TurnRevision, graph['revision'])
            return revision.draft_text if revision else None

    async def test_fu1_stuck_task_signals_stale_and_recovers_without_blocking_dispatch(self):
        entered, release = asyncio.Event(), asyncio.Event()
        worker = worker_module.DeliveryWorker()
        worker._last_retention = -worker_module.settings.delivery_retention_interval_seconds
        original = worker._legacy_retention
        async def paused():
            entered.set()
            await release.wait()
            return await original()
        claims = []
        async def claim(capacity):
            async with self.sessions() as db:
                await db.execute(select(func.count()).select_from(ConversationTurn))
            claims.append(time.monotonic())
            return []
        async def noop():
            pass
        records = []
        sink = logger.add(lambda msg: records.append(msg.record))
        retention._health = {'available': True, 'observed_at': 'prior-observation'}
        try:
            with tempfile.TemporaryDirectory() as directory, \
                 patch.object(worker_module, 'AsyncSessionLocal', self.sessions), \
                 patch.object(worker_module, 'engine', self.engine), \
                 patch.object(worker_module.settings, 'delivery_worker_health_path', directory + '/health.json'), \
                 patch.object(worker_module.settings, 'delivery_poll_interval_seconds', 0.03), \
                 patch.object(worker, '_legacy_retention', paused), \
                 patch.object(worker, '_claim', claim), \
                 patch.object(worker, '_maintenance', noop):
                run = asyncio.create_task(worker.run())
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    age = worker_module.settings.scheduler_retention_max_seconds + worker_module.settings.delivery_retention_overrun_margin_seconds + 1
                    worker._retention_started_monotonic = time.monotonic() - age
                    worker._retention_observing_since = time.monotonic() - (
                        worker_module.settings.delivery_retention_interval_seconds *
                        worker_module.settings.delivery_retention_completion_max_intervals + 1)
                    for _ in range(2):
                        await worker._heartbeat()
                        snapshot = json.loads(Path(directory + '/health.json').read_text())
                        task = snapshot['retention_task']
                        self.assertEqual(task['state'], 'running')
                        self.assertGreater(task['running_seconds'], age - 1)
                        self.assertIsNotNone(task['started_at'])
                        self.assertIsNone(task['last_completed_at'])
                        self.assertEqual(task['counts'], {'running_over_budget': 1, 'completion_overdue': 1})
                        self.assertTrue(snapshot['scheduler_retention_health']['stale'])
                        self.assertFalse(snapshot['scheduler_retention_health']['available'])
                    before = len(claims)
                    await asyncio.sleep(0.12)
                    self.assertGreater(len(claims), before)
                    release.set()
                    await asyncio.wait_for(worker._retention_task, 3)
                    await worker._heartbeat()
                    recovered = json.loads(Path(directory + '/health.json').read_text())
                    self.assertEqual(recovered['retention_task']['state'], 'idle')
                    self.assertIsNotNone(recovered['retention_task']['last_completed_at'])
                    self.assertIsNone(recovered['retention_task']['running_seconds'])
                    self.assertFalse(recovered['scheduler_retention_health']['stale'])
                    self.assertTrue(recovered['scheduler_retention_health']['available'])
                finally:
                    release.set()
                    worker.request_stop()
                    await asyncio.wait_for(run, 5)
        finally:
            logger.remove(sink)
        signals = [r for r in records if r['level'].name == 'ERROR' and 'retention watchdog' in r['message']]
        self.assertEqual(len(signals), 2)

    async def test_fu1_failed_task_state_and_next_completion(self):
        from sqlalchemy.exc import OperationalError
        failure = OperationalError('synthetic', {}, Exception(2013, 'synthetic disconnect'))
        worker = worker_module.DeliveryWorker()
        with patch.object(worker_module, 'AsyncSessionLocal', self.sessions):
            with patch.object(worker, '_legacy_retention', side_effect=failure):
                worker._schedule_retention(1.0)  # first pass cannot depend on host uptime
                await worker._retention_task
            state = worker._retention_task_snapshot()
            self.assertEqual(state['state'], 'failed')
            self.assertIsNone(state['last_completed_at'])
            self.assertTrue(state['stale'])
            worker._last_retention = -worker_module.settings.delivery_retention_interval_seconds
            worker._schedule_retention(time.monotonic())
            await worker._retention_task
        state = worker._retention_task_snapshot()
        self.assertEqual(state['state'], 'idle')
        self.assertIsNotNone(state['last_completed_at'])
        self.assertFalse(state['stale'])
        # Cancellation before _run_retention executes must not leave state=running.
        with patch.object(worker_module, 'AsyncSessionLocal', self.sessions):
            worker._last_retention = -worker_module.settings.delivery_retention_interval_seconds
            worker._schedule_retention(time.monotonic())
            worker._retention_task.cancel()
            await asyncio.gather(worker._retention_task, return_exceptions=True)
        state = worker._retention_task_snapshot()
        self.assertEqual(state['state'], 'failed')
        self.assertTrue(state['stale'])

    async def test_fu1_partial_coverage_continuity_restart_and_sweep_watchdog(self):
        for _ in range(6):
            await self.graph(days=10)
        records = []
        sink = logger.add(lambda msg: records.append(msg.record))
        try:
            await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
            first = retention.retention_health()
            self.assertTrue(first['available'])
            self.assertFalse(first['draft_available'])
            self.assertEqual(first['reason'], 'coverage_incomplete')
            self.assertIsNotNone(first['scan_started_at'])
            self.assertGreater(first['scan_cursor'], 0)
            self.assertEqual(first['scan_rows'], 2)
            self.assertIsNone(first['oldest_draft_terminal_age_seconds'])
            await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
            self.assertEqual(retention.retention_health()['scan_rows'], 4)
            # A different replica's CAS invalidates this process's prefix.
            async with self.sessions() as db:
                progress = await db.get(SchedulerRetentionProgress, 1)
                version, cursor = progress.version, progress.last_turn_id
            self.assertTrue(await retention._checkpoint(self.sessions, version=version, cursor=cursor))
            await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
            self.assertEqual(retention.retention_health()['scan_rows'], 2)
            self.assertFalse(retention.retention_health()['draft_available'])
            retention._coverage = None  # restart: no minimum is invented from durable cursor
            await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
            self.assertFalse(retention.retention_health()['draft_available'])
            self.assertFalse([r for r in records if r['level'].name == 'ERROR'], records)
            retention._observing_since -= worker_module.settings.scheduler_retention_sweep_max_age_seconds + 1
            retention.observe_watchdog()
            retention.observe_watchdog()
            self.assertEqual(retention.retention_counts()['sweep_completion_overdue'], 1)
            self.assertTrue(retention.retention_health()['stale'])
            # A complete prefix->end sweep repairs coverage and rearms watchdog.
            for _ in range(5):
                await retention.cleanup_retention(self.sessions, limit=2, max_rows=2)
                if retention.retention_health()['draft_available']:
                    break
            health = retention.retention_health()
            self.assertTrue(health['draft_available'])
            self.assertTrue(health['draft_scan_complete'])
            self.assertFalse(health['stale'])
            self.assertIsNotNone(health['last_complete_scan_at'])
            self.assertIsNotNone(health['last_complete_scan_age_seconds'])
            self.assertIsNone(health['oldest_draft_terminal_age_seconds'])
            retention.observe_watchdog()
            self.assertNotIn('sweep_completion_overdue', retention._sla_episodes)
        finally:
            logger.remove(sink)

    async def test_fu1_partial_scan_signals_positive_sla_once_per_episode(self):
        await self.graph(outcome='outcome_unknown', accounting='reconciliation_required')
        for _ in range(3):
            await self.graph(days=10)
        entered, release = asyncio.Event(), asyncio.Event()
        original = retention._retain_one
        calls = 0
        async def pause_second(db, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                entered.set()
                await release.wait()
            return await original(db, **kwargs)
        with patch.object(retention, '_retain_one', pause_second):
            task = asyncio.create_task(retention.cleanup_retention(self.sessions, limit=1, max_rows=2))
            try:
                await asyncio.wait_for(entered.wait(), 3)
                # Positive evidence is signaled before the pass ends, not only
                # by the final health publication.
                self.assertEqual(retention.retention_counts().get('draft_sla_exceeded'), 1)
            finally:
                release.set()
                await asyncio.wait_for(task, 3)
        health = retention.retention_health()
        self.assertFalse(health['draft_available'])
        self.assertIsNone(health['oldest_draft_terminal_age_seconds'])
        self.assertGreater(health['oldest_observed_draft_age_seconds'], 99 * 86400)
        self.assertTrue(health['draft_sla_exceeded'])
        self.assertEqual(retention.retention_counts()['draft_sla_exceeded'], 1)
        for _ in range(4):
            await retention.cleanup_retention(self.sessions, limit=1, max_rows=1)
        self.assertEqual(retention.retention_counts()['draft_sla_exceeded'], 1)
        async with self.sessions.begin() as db:
            await db.execute(text('UPDATE turn_revisions SET draft_text=NULL'))
        for _ in range(3):
            await retention.cleanup_retention(self.sessions)
            if retention.retention_health()['draft_sla_exceeded'] is False:
                break
        self.assertFalse(retention.retention_health()['draft_sla_exceeded'])
        await self.graph(outcome='outcome_unknown', accounting='reconciliation_required')
        await retention.cleanup_retention(self.sessions)
        self.assertEqual(retention.retention_counts()['draft_sla_exceeded'], 2)

    async def test_fu1_probe_failure_is_unavailable_not_empty(self):
        from sqlalchemy.exc import OperationalError
        await self.graph(days=10)
        failure = OperationalError('synthetic', {}, Exception(2013, 'synthetic disconnect'))
        with patch.object(retention, '_draft_probe', side_effect=failure):
            await retention.cleanup_retention(self.sessions)
        health = retention.retention_health()
        self.assertFalse(health['available'])
        self.assertFalse(health['draft_available'])
        self.assertIsNone(health['oldest_draft_terminal_age_seconds'])
        self.assertEqual(retention.retention_counts()['draft_probe_failed'], 1)
        await retention.cleanup_retention(self.sessions)
        self.assertTrue(retention.retention_health()['available'])
        self.assertTrue(retention.retention_health()['draft_available'])

    async def test_fu1_observation_never_issues_global_draft_query(self):
        from sqlalchemy import event
        await self.graph(days=10)
        statements = []
        def capture(conn, cursor, statement, params, context, executemany):
            statements.append(statement)
        event.listen(self.engine.sync_engine, 'before_cursor_execute', capture)
        try:
            await retention._observe_health(self.sessions, payload_days=7, receipt_days=90,
                                             tolerance_seconds=86400)
        finally:
            event.remove(self.engine.sync_engine, 'before_cursor_execute', capture)
        self.assertFalse([sql for sql in statements if 'turn_revisions' in sql], statements)
        self.assertTrue(retention.retention_health()['available'])
        self.assertFalse(retention.retention_health()['draft_available'])
        self.assertEqual(retention.retention_health()['reason'], 'coverage_incomplete')
        with self.assertRaises(ValueError):
            retention.health_query(draft=True)
        with self.assertRaises(ValueError):
            retention.health_query(draft=True, turn_ids=tuple(range(1001)))

    async def test_fu1_watchdog_config_bounds_and_parity(self):
        from pydantic import ValidationError
        from src.config import Settings
        for values in (
            {'delivery_retention_overrun_margin_seconds': float('nan')},
            {'delivery_retention_overrun_margin_seconds': float('inf')},
            {'delivery_retention_overrun_margin_seconds': 5},
            {'delivery_retention_completion_max_intervals': 1},
            {'delivery_retention_completion_max_intervals': 25},
            {'scheduler_retention_sweep_max_age_seconds': 60},
            {'scheduler_retention_sweep_max_age_seconds': 604800},
            {'delivery_retention_interval_seconds': 60, 'scheduler_retention_max_seconds': 300},
            {'scheduler_retention_sweep_max_age_seconds': 518400},
        ):
            with self.assertRaises(ValidationError, msg=str(values)):
                Settings(**values)
        root = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
        for name, expected in (
            ('DELIVERY_RETENTION_OVERRUN_MARGIN_SECONDS', 30),
            ('DELIVERY_RETENTION_COMPLETION_MAX_INTERVALS', 2),
            ('SCHEDULER_RETENTION_SWEEP_MAX_AGE_SECONDS', 172800),
        ):
            self.assertEqual(getattr(Settings(), name.lower()), expected)
            for path in ('AGENTS.md', '.env.example', 'app/.env.example'):
                self.assertIn(f'{name}={expected}', (root / path).read_text())


    async def test_p31_persistent_poison_rotates_and_completes_real_worker_loop(self):
        # Actual 1451: a nonterminal foreign membership pins the old revision.
        pinned = await self.graph(days=110)
        await self.later_turn(pinned, terminal=False, revision_id=pinned['revision'])
        poison = await self.graph(days=100)
        victim = await self.graph(days=100)
        original = retention._retain_one
        async def poisoned(db, **kwargs):
            if kwargs['turn_id'] == poison['turn']:
                raise RuntimeError('synthetic poison candidate')
            return await original(db, **kwargs)
        async def noop(*args):
            return []
        worker = worker_module.DeliveryWorker()
        records = []
        sink = logger.add(lambda msg: records.append(msg.record))
        try:
            with tempfile.TemporaryDirectory() as directory, \
                 patch.object(worker_module, 'AsyncSessionLocal', self.sessions), \
                 patch.object(worker_module, 'engine', self.engine), \
                 patch.object(worker_module.settings, 'delivery_worker_health_path', directory+'/health.json'), \
                 patch.object(worker_module.settings, 'delivery_poll_interval_seconds', 0.02), \
                 patch.object(worker, '_claim', noop), \
                 patch.object(worker, '_maintenance', noop), \
                 patch.object(retention, '_retain_one', poisoned):
                run = asyncio.create_task(worker.run())
                previous = None
                try:
                    for iteration in range(2):
                        worker._last_retention = -worker_module.settings.delivery_retention_interval_seconds
                        async with asyncio.timeout(5):
                            while (worker._retention_task is None or not worker._retention_task.done()
                                   or worker._retention_started_at == previous):
                                await asyncio.sleep(0.01)
                        previous = worker._retention_started_at
                        await worker._heartbeat()
                        snapshot = json.loads(Path(directory+'/health.json').read_text())
                        task = snapshot['retention_task']
                        print('SEC09_POISON', iteration, task, snapshot['scheduler_retention_health'])
                        self.assertEqual(task['state'], 'idle')
                        self.assertFalse(task['stale'])
                        self.assertIsNotNone(task['last_completed_at'])
                        self.assertTrue(snapshot['scheduler_retention_health']['available'])
                        failures = snapshot['scheduler_retention_health']['candidate_failures']
                        self.assertGreaterEqual(failures['counts']['candidate_error'], iteration+1)
                        self.assertGreaterEqual(failures['counts']['foreign_key_reference'], iteration+1)
                        self.assertEqual(failures['last_failure_reasons']['foreign_key_reference'], 1)
                        self.assertGreater(failures['oldest_terminal_age_seconds'], 109*86400)
                        self.assertEqual(retention.measurement_failure_count(), 0)
                        async with self.sessions() as db:
                            self.assertIsNone(await db.get(ConversationTurn, victim['turn']))
                            self.assertIsNotNone(await db.get(ConversationTurn, pinned['turn']))
                            self.assertIsNotNone(await db.get(ConversationTurn, poison['turn']))
                finally:
                    worker.request_stop()
                    await asyncio.wait_for(run, 5)
        finally:
            logger.remove(sink)
        for reason in ('candidate_error', 'foreign_key_reference'):
            self.assertTrue(any(r['level'].name == 'ERROR' and f'reason={reason}' in r['message']
                                and 'threshold=1' in r['message'] for r in records), reason)

    async def test_p31_candidate_taxonomy_and_nonempty_signal_survives_partial_pass(self):
        from sqlalchemy.exc import OperationalError
        poison = await self.graph(days=100)
        victim = await self.graph(days=100)
        original = retention._retain_one
        # A4 remains a whole-UoW retry. Exhausted candidate retry is local,
        # unlike the same OperationalError raised by discovery/checkpoint/probe.
        for code in (1205, 1213, 1305):
            calls = []
            async def lost(db, **kwargs):
                if kwargs['turn_id'] == poison['turn']:
                    calls.append(kwargs['turn_id'])
                    raise OperationalError('synthetic', {}, Exception(code, 'synthetic'))
                return await original(db, **kwargs)
            worker = worker_module.DeliveryWorker()
            with patch.object(worker_module, 'AsyncSessionLocal', self.sessions), \
                 patch.object(retention, '_retain_one', lost):
                await worker._run_retention()
            self.assertGreater(len(calls), 1)
            self.assertEqual(worker._retention_state, 'idle')
            self.assertEqual(retention.measurement_failure_count(), 0)
            self.assertIn('transaction_lost', retention.retention_health()['candidate_failures']['counts'])
        # Expected retained evidence is another candidate reason, not a failed task.
        await self.graph(days=100, terminal_days=5)  # scanned, non-actionable prefix
        held = await self.graph(days=10, outcome='outcome_unknown',
                                accounting='reconciliation_required', with_ledger=False)
        await self.graph(days=100, terminal_days=5)  # healthy suffix
        await retention.cleanup_retention(self.sessions)
        health = retention.retention_health()['candidate_failures']
        self.assertEqual(health['last_failure_reasons'], {'accounting_evidence': 1})
        remembered = health['last_failure_observed_at']
        # Empty partial page after the last ID must not erase last failure.
        async with self.sessions() as db:
            await db.execute(text('UPDATE scheduler_retention_progress SET last_turn_id=0,version=version+1 WHERE id=1'))
            await db.commit()
        retention._coverage = None
        await retention.cleanup_retention(self.sessions, limit=1, max_rows=1)
        health = retention.retention_health()['candidate_failures']
        self.assertEqual(health['last_pass']['count'], 0)
        self.assertEqual(health['last_failure_observed_at'], remembered)
        # A failure on an earlier page still taints that sweep's later empty end.
        await retention.cleanup_retention(self.sessions, limit=1, max_rows=1)
        remembered = retention.retention_health()['candidate_failures']['last_failure_observed_at']
        await retention.cleanup_retention(self.sessions)
        health = retention.retention_health()['candidate_failures']
        self.assertEqual(health['last_pass']['count'], 0)
        self.assertEqual(health['last_failure_observed_at'], remembered)
        # Recovery: resolve the held attempt; full sweep proves no failed graph.
        async with self.sessions() as db:
            attempt = await db.get(GenerationAttempt, held['attempt'])
            attempt.outcome = 'failed'
            attempt.reconciled_at = datetime.utcnow()
            attempt.reconciliation_actor = 'p31-test'
            attempt.reconciliation_reason = 'synthetic resolution'
            await db.commit()
        await retention.cleanup_retention(self.sessions)
        # Resume from partial cursor cannot establish clean prefix until wrap.
        await retention.cleanup_retention(self.sessions)
        health = retention.retention_health()['candidate_failures']
        self.assertIsNone(health['last_failure_observed_at'])
        self.assertIsNone(health['oldest_terminal_age_seconds'])
        self.assertGreater(health['count'], 0)  # cumulative diagnostics survive recovery
        async with self.sessions() as db:
            self.assertIsNone(await db.get(ConversationTurn, victim['turn']))

    async def test_p31_measurement_failures_remain_failed_and_unavailable(self):
        from sqlalchemy.exc import OperationalError
        await self.graph(days=10)
        failure = OperationalError('synthetic', {}, Exception(2006, 'synthetic'))
        for target in ('_progress_version', '_draft_probe', 'health_query', '_checkpoint'):
            worker = worker_module.DeliveryWorker()
            before = retention.measurement_failure_count()
            with patch.object(worker_module, 'AsyncSessionLocal', self.sessions), \
                 patch.object(retention, target, side_effect=failure):
                await worker._run_retention()
            self.assertGreater(retention.measurement_failure_count(), before, target)
            self.assertEqual(worker._retention_state, 'failed', target)
            self.assertIsNone(worker._retention_last_completed_at)
            self.assertTrue(worker._retention_task_snapshot()['stale'])
            self.assertFalse(retention.retention_health()['available'])
        # All counters classified, including stage failures outside the repo.
        for reason in retention.CANDIDATE_REASONS:
            before = retention.measurement_failure_count()
            retention._signal(reason)
            self.assertEqual(retention.measurement_failure_count(), before, reason)
        for reason in ('health_unavailable', 'draft_probe_failed', 'checkpoint_unavailable',
                       'checkpoint_failed', 'discovery_failed'):
            before = retention.measurement_failure_count()
            retention._signal(reason)
            self.assertEqual(retention.measurement_failure_count(), before+1, reason)

    def assert_p32_aggregate(self, records: list[dict], *, observed: int,
                             cumulative: int | None = None) -> None:
        failures = retention.candidate_failure_health()
        self.assertEqual(failures['last_pass']['reasons'], {'nonterminal': observed})
        self.assertEqual(failures['counts'].get('nonterminal', 0),
                         observed if cumulative is None else cumulative)
        errors = [r for r in records if r['level'].name == 'ERROR'
                  and 'Scheduler retention nonterminal aggregate |' in r['message']]
        self.assertEqual(len(errors), 1)
        self.assertIn(f'observed={observed}', errors[0]['message'])
        self.assertIn('threshold=1', errors[0]['message'])

    async def p32_progress(self) -> tuple[int, int]:
        async with self.sessions() as db:
            progress = await db.get(SchedulerRetentionProgress, 1)
            return progress.last_turn_id, progress.version

    async def test_p32_nonterminal_version_loser_finalizes_once(self) -> None:
        nonterminal = await self.graph(terminal=False)
        victim = await self.graph()
        original_version = retention._progress_version
        original_checkpoint = retention._checkpoint
        records = []
        sink = logger.add(lambda message: records.append(message.record))

        async def winner_before_version(session_factory: async_sessionmaker[AsyncSession]) -> int:
            # Real winner commit on another connection, before the loser's read.
            self.assertTrue(await original_checkpoint(self.sessions, version=0, cursor=victim['turn']))
            return await original_version(session_factory)

        try:
            with patch.object(retention, '_progress_version', winner_before_version), \
                 patch.object(retention, '_checkpoint', wraps=original_checkpoint) as checkpoint:
                result = await retention.cleanup_retention(self.sessions)
                checkpoint.assert_not_called()
        finally:
            logger.remove(sink)
        self.assert_p32_aggregate(records, observed=1)
        self.assertEqual(result.nonterminal_observed, 1)
        self.assertEqual(retention.measurement_failure_count(), 0)
        self.assertEqual(await self.p32_progress(), (victim['turn'], 1))
        self.assertIsNone(retention._coverage)
        self.assertIsNone(retention._last_complete_scan_at)
        async with self.sessions() as db:
            self.assertIsNotNone(await db.get(ConversationTurn, victim['turn']))
            self.assertIsNotNone(await db.get(ConversationTurn, nonterminal['turn']))

    async def test_p32_nonterminal_checkpoint_cas_loser_finalizes_once(self) -> None:
        await self.graph(terminal=False)
        victim = await self.graph()
        original_checkpoint = retention._checkpoint
        records = []
        sink = logger.add(lambda message: records.append(message.record))

        async def winner_before_cas(session_factory: async_sessionmaker[AsyncSession], *, version: int, cursor: int) -> bool:
            self.assertTrue(await original_checkpoint(self.sessions, version=version, cursor=victim['turn']))
            return await original_checkpoint(session_factory, version=version, cursor=cursor)

        try:
            with patch.object(retention, '_checkpoint', side_effect=winner_before_cas) as checkpoint:
                result = await retention.cleanup_retention(self.sessions)
                self.assertEqual(checkpoint.await_count, 1)
        finally:
            logger.remove(sink)
        self.assert_p32_aggregate(records, observed=1)
        self.assertEqual(result.nonterminal_observed, 1)
        self.assertEqual(retention.measurement_failure_count(), 0)
        self.assertEqual(await self.p32_progress(), (victim['turn'], 1))
        self.assertIsNone(retention._coverage)
        self.assertIsNone(retention._last_complete_scan_at)

    async def test_p32_nonterminal_discovery_failure_finalizes_once(self) -> None:
        await self.graph(terminal=False)
        await self.graph()
        records = []
        sink = logger.add(lambda message: records.append(message.record))
        try:
            with patch.object(retention, '_progress_version', side_effect=RuntimeError('synthetic probe failure')), \
                 patch.object(retention, '_checkpoint') as checkpoint:
                result = await retention.cleanup_retention(self.sessions)
                checkpoint.assert_not_called()
        finally:
            logger.remove(sink)
        self.assert_p32_aggregate(records, observed=1)
        self.assertEqual(result.nonterminal_observed, 1)
        self.assertEqual(retention.measurement_failure_count(), 1)
        self.assertEqual(retention.retention_counts()['discovery_failed'], 1)
        self.assertFalse(retention.candidate_failure_health()['last_pass']['measurement_available'])
        self.assertEqual(await self.p32_progress(), (0, 0))

    async def test_p32_nonterminal_checkpoint_failure_finalizes_once(self) -> None:
        await self.graph(terminal=False)
        await self.graph()
        records = []
        sink = logger.add(lambda message: records.append(message.record))
        try:
            with patch.object(retention, '_checkpoint', side_effect=RuntimeError('synthetic checkpoint failure')):
                result = await retention.cleanup_retention(self.sessions)
        finally:
            logger.remove(sink)
        self.assert_p32_aggregate(records, observed=1)
        self.assertEqual(result.nonterminal_observed, 1)
        self.assertEqual(retention.measurement_failure_count(), 1)
        self.assertEqual(retention.retention_counts()['checkpoint_failed'], 1)
        self.assertEqual(await self.p32_progress(), (0, 0))

    async def test_p32_nonterminal_cancellation_finalizes_once(self) -> None:
        await self.graph(terminal=False)
        await self.graph()
        entered = asyncio.Event()
        never_release = asyncio.Event()
        records = []
        sink = logger.add(lambda message: records.append(message.record))

        async def wait_after_observation(session_factory: async_sessionmaker[AsyncSession]) -> None:
            entered.set()
            await never_release.wait()

        task = None
        try:
            with patch.object(retention, '_progress_version', wait_after_observation), \
                 patch.object(retention, '_checkpoint') as checkpoint:
                task = asyncio.create_task(retention.cleanup_retention(self.sessions))
                await asyncio.wait_for(entered.wait(), 5)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                checkpoint.assert_not_called()
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            logger.remove(sink)
        self.assert_p32_aggregate(records, observed=1)
        self.assertEqual(retention.measurement_failure_count(), 0)
        self.assertFalse(retention.candidate_failure_health()['last_pass']['measurement_available'])
        self.assertEqual(await self.p32_progress(), (0, 0))

    async def test_p32_nonterminal_propagated_exception_finalizes_once(self) -> None:
        await self.graph(terminal=False)
        records = []
        sink = logger.add(lambda message: records.append(message.record))
        try:
            # Unhandled exception after the scan; finalization must not suppress it.
            with patch.object(retention, '_observe_health', side_effect=RuntimeError('synthetic propagated failure')):
                with self.assertRaisesRegex(RuntimeError, 'synthetic propagated failure'):
                    await retention.cleanup_retention(self.sessions)
        finally:
            logger.remove(sink)
        self.assert_p32_aggregate(records, observed=1)
        self.assertEqual(retention.measurement_failure_count(), 0)
        self.assertFalse(retention.candidate_failure_health()['last_pass']['measurement_available'])
        self.assertEqual(await self.p32_progress(), (0, 1))

    async def test_p32_nonterminal_normal_and_budget_exact_aggregate(self) -> None:
        graphs = [await self.graph(terminal=False) for _ in range(3)]
        for limit, max_rows, observed, cumulative in ((2, 10, 3, 3), (1, 1, 1, 4)):
            with self.subTest(max_rows=max_rows):
                records = []
                sink = logger.add(lambda message: records.append(message.record))
                try:
                    result = await retention.cleanup_retention(self.sessions, limit=limit, max_rows=max_rows)
                finally:
                    logger.remove(sink)
                self.assert_p32_aggregate(records, observed=observed, cumulative=cumulative)
                self.assertEqual(result.nonterminal_observed, observed)
                self.assertEqual(result.budget_exhausted, int(max_rows == 1))
                self.assertEqual(retention.measurement_failure_count(), 0)
        self.assertEqual(await self.p32_progress(), (graphs[0]['turn'], 3))
