-- Ticket 05 upgrade fixture: populated scheduler/handoff/outbox rows written
-- with columns present at every supported predecessor (35f9c07, d52a495,
-- f59c32c). Synthetic identifiers only.
INSERT INTO agentes (nome, instance_name, system_prompt, numero_notificacao, chatwoot_inbox_id)
VALUES ('fixture-t05', 'fixture-t05', 'synthetic', '0', 7);
SET @agent = LAST_INSERT_ID();
INSERT INTO sessoes (agente_id, telefone, delivery_stream_key, handoff_generation)
VALUES (@agent, '551100000005', '55555555-5555-4555-8555-555555555555', 1);
SET @session = LAST_INSERT_ID();
INSERT INTO mensagens (sessao_id, role, conteudo) VALUES (@session, 'user', 'fixture input');
SET @message = LAST_INSERT_ID();
INSERT INTO inbound_events (provider, source_scope, provider_event_id, event_type,
    payload_hmac, hmac_key_version, agente_id, sessao_id, receipt_disposition,
    mensagem_id, processed_at)
VALUES ('evolution', 'fixture-t05', 'fixture-event-1', 'messages.upsert', REPEAT('a', 64),
    1, @agent, @session, 'scheduled', @message, UTC_TIMESTAMP(6));
SET @receipt = LAST_INSERT_ID();
INSERT INTO agent_service_lanes (agente_id, scheduler_epoch, next_arrival_sequence,
    next_scheduling_sequence)
VALUES (@agent, '22222222-2222-4222-8222-222222222222', 2, 2);
INSERT INTO conversation_turns (turn_key, agente_id, sessao_id, scheduler_epoch,
    arrival_sequence, scheduling_sequence, status, session_slot, ready_at)
VALUES (UUID(), @agent, @session, '22222222-2222-4222-8222-222222222222', 1, 1,
    'pending', 'active', UTC_TIMESTAMP(6));
SET @turn = LAST_INSERT_ID();
INSERT INTO turn_revisions (revision_key, turn_id, scheduler_epoch, revision_number)
VALUES (UUID(), @turn, '22222222-2222-4222-8222-222222222222', 1);
UPDATE conversation_turns SET current_revision_id = LAST_INSERT_ID() WHERE id = @turn;
INSERT INTO turn_input_memberships (turn_id, sessao_id, mensagem_id, inbound_event_id,
    input_sequence, content_bytes)
VALUES (@turn, @session, @message, @receipt, 1, 13);
INSERT INTO handoff_attempts (handoff_key, sessao_id, agente_id, generation, chatwoot_inbox_id)
VALUES ('66666666-6666-4666-8666-666666666666', @session, @agent, 1, 7);
SET @attempt = LAST_INSERT_ID();
INSERT INTO outbox_deliveries (delivery_key, cutover_epoch, dedupe_key, provider, operation,
    payload_version, payload, delivery_class, agente_id, sessao_id, handoff_attempt_id,
    ordering_key, stream_sequence, status, next_attempt_at, expires_at)
VALUES (UUID(), '33333333-3333-4333-8333-333333333333', 'fixture-open', 'chatwoot',
    'chatwoot.open_handoff.v1', 1, JSON_OBJECT('summary', 'fixture'),
    'provider_setup_context', @agent, @session, @attempt,
    '66666666-6666-4666-8666-666666666666', 0, 'pending',
    UTC_TIMESTAMP(6), UTC_TIMESTAMP(6) + INTERVAL 1 DAY);
