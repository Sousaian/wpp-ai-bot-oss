SET @scheduler_epoch = '22222222-2222-4222-8222-222222222222';
SET SESSION cte_max_recursion_depth = 10001;

INSERT INTO agentes (
    nome, instance_name, system_prompt, numero_notificacao, chatwoot_inbox_id
)
VALUES ('scheduler-plan', CONCAT('scheduler-plan-', UUID()), 'synthetic', '0', 7);
SET @plan_agent = LAST_INSERT_ID();

INSERT INTO agent_service_lanes (
    agente_id, scheduler_epoch, next_arrival_sequence, next_scheduling_sequence
)
VALUES (@plan_agent, @scheduler_epoch, 10001, 10001);

INSERT INTO sessoes (agente_id, telefone, delivery_stream_key)
WITH RECURSIVE streams(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM streams WHERE n < 10000
)
SELECT @plan_agent, CONCAT('scheduler-plan-', LPAD(n, 6, '0')), UUID()
FROM streams;

INSERT INTO conversation_turns (
    turn_key, agente_id, sessao_id, scheduler_epoch,
    arrival_sequence, scheduling_sequence, status, session_slot, ready_at
)
SELECT
    UUID(), @plan_agent, s.id, @scheduler_epoch,
    ROW_NUMBER() OVER (ORDER BY s.id),
    ROW_NUMBER() OVER (ORDER BY s.id),
    CASE WHEN MOD(s.id, 5) = 0 THEN 'retry' ELSE 'pending' END,
    'active',
    UTC_TIMESTAMP(6) - INTERVAL MOD(s.id, 300) SECOND
FROM sessoes AS s
WHERE s.agente_id = @plan_agent;

EXPLAIN ANALYZE
SELECT id
FROM conversation_turns FORCE INDEX (idx_turn_ready_claim)
WHERE agente_id = @plan_agent
  AND scheduler_epoch = @scheduler_epoch
  AND session_slot = 'active'
  AND status IN ('pending', 'retry')
  AND ready_at <= UTC_TIMESTAMP(6)
  AND (retry_at IS NULL OR retry_at <= UTC_TIMESTAMP(6))
ORDER BY scheduling_sequence, id
LIMIT 1;

DELETE FROM conversation_turns WHERE agente_id = @plan_agent;
DELETE FROM agent_service_lanes WHERE agente_id = @plan_agent;
DELETE FROM sessoes WHERE agente_id = @plan_agent;
DELETE FROM agentes WHERE id = @plan_agent;
