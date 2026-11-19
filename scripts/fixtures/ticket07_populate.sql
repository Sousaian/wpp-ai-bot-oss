-- Populated ticket06 shape: one immutable scheduled transition and no reply.
-- automated_effect is required at INSERT time by the ticket06 authority check.
INSERT INTO agentes (nome, instance_name, system_prompt, numero_notificacao, chatwoot_inbox_id,
    inbox_scheduler_enabled)
VALUES ('fixture-t06','fixture-t06','synthetic','0',7,TRUE);
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key,handoff_generation,next_delivery_sequence,
    handler,motivo_encerramento,encerrada_em)
VALUES (@agent,'551100000006','66666666-6666-4666-8666-666666666660',1,2,
    'encerrada','handoff',UTC_TIMESTAMP(6));
SET @session=LAST_INSERT_ID();
INSERT INTO agent_service_lanes (agente_id,scheduler_epoch,next_arrival_sequence,next_scheduling_sequence)
VALUES (@agent,'22222222-2222-4222-8222-222222222222',2,2);
INSERT INTO conversation_turns (turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,
    scheduling_sequence,status,session_slot,ready_at)
VALUES ('66666666-6666-4666-8666-666666666661',@agent,@session,
    '22222222-2222-4222-8222-222222222222',1,1,'awaiting_delivery','active',UTC_TIMESTAMP(6));
SET @turn=LAST_INSERT_ID();
INSERT INTO turn_revisions (revision_key,turn_id,scheduler_epoch,revision_number,status,draft_kind,
    draft_text,needs_handoff,sealed_first_input_sequence,sealed_last_input_sequence,
    sealed_input_count,sealed_input_bytes,sealed_at,
    typing_delay_ms,typing_jitter_ms,generation_completed_at,typing_deadline_at,admitted_at)
VALUES ('66666666-6666-4666-8666-666666666662',@turn,
    '22222222-2222-4222-8222-222222222222',1,'admitted','generated','fixture reply',TRUE,
    1,1,1,13,UTC_TIMESTAMP(6),2000,0,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6));
SET @revision=LAST_INSERT_ID();
UPDATE conversation_turns SET current_revision_id=@revision,final_revision_id=@revision WHERE id=@turn;
INSERT INTO handoff_attempts (handoff_key,sessao_id,agente_id,generation,status,chatwoot_inbox_id)
VALUES ('66666666-6666-4666-8666-666666666663',@session,@agent,1,'pending',7);
SET @attempt=LAST_INSERT_ID();
INSERT INTO outbox_deliveries (delivery_key,cutover_epoch,dedupe_key,provider,operation,payload_version,
    payload,delivery_class,agente_id,sessao_id,handoff_attempt_id,ordering_key,stream_sequence,status,
    next_attempt_at,expires_at)
VALUES ('66666666-6666-4666-8666-666666666664','33333333-3333-4333-8333-333333333333',
    'fixture-t06-open','chatwoot','chatwoot.open_handoff.v1',1,JSON_OBJECT('synthetic',TRUE),
    'provider_setup_context',@agent,@session,@attempt,'66666666-6666-4666-8666-666666666663',0,
    'pending',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)+INTERVAL 1 DAY);
SET @open=LAST_INSERT_ID();
INSERT INTO outbox_deliveries (delivery_key,cutover_epoch,dedupe_key,provider,operation,payload_version,
    payload,delivery_class,automated_effect,scheduler_epoch,conversation_turn_id,turn_revision_id,agente_id,sessao_id,
    handoff_attempt_id,ordering_key,stream_sequence,depends_on_delivery_id,status)
VALUES ('66666666-6666-4666-8666-666666666665','33333333-3333-4333-8333-333333333333',
    'fixture-t06-transition','evolution','evolution.send_text.v1',1,JSON_OBJECT('synthetic',TRUE),
    'automated','handoff_transition','22222222-2222-4222-8222-222222222222',@turn,@revision,@agent,@session,@attempt,
    '66666666-6666-4666-8666-666666666660',1,@open,'blocked');
INSERT INTO scheduled_handoffs (handoff_attempt_id,agente_id,sessao_id,conversation_turn_id,
    turn_revision_id,summary_kind,scheduler_epoch)
VALUES (@attempt,@agent,@session,@turn,@revision,'technical_fallback',
    '22222222-2222-4222-8222-222222222222');
