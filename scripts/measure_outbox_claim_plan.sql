SET @plan_epoch = '11111111-1111-4111-8111-111111111111';

INSERT INTO agentes (
    nome, instance_name, system_prompt, numero_notificacao, chatwoot_inbox_id
)
VALUES ('claim-plan', 'claim-plan', 'synthetic', '0', 7);
SET @plan_agent = LAST_INSERT_ID();

INSERT INTO sessoes (
    agente_id, telefone, delivery_stream_key
)
WITH RECURSIVE streams(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM streams WHERE n < 1000
)
SELECT @plan_agent, CONCAT('plan-', LPAD(n, 6, '0')), UUID()
FROM streams;

INSERT INTO outbox_deliveries (
    delivery_key, cutover_epoch, dedupe_key, provider, operation,
    payload_version, payload, agente_id, sessao_id, ordering_key,
    stream_sequence, priority, status, next_attempt_at, expires_at,
    delivered_at, terminal_at, terminal_reason
)
WITH RECURSIVE sequences(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM sequences WHERE n < 10
)
SELECT
    UUID(),
    @plan_epoch,
    CONCAT('claim-plan:', s.id, ':', sequences.n),
    'evolution',
    'evolution.send_text.v1',
    1,
    JSON_OBJECT('synthetic', TRUE),
    @plan_agent,
    s.id,
    s.delivery_stream_key,
    sequences.n,
    MOD(s.id, 4),
    CASE
        WHEN sequences.n = 1 THEN 'pending'
        WHEN sequences.n = 2 THEN 'retry'
        WHEN sequences.n <= 6 THEN 'delivered'
        ELSE 'dead'
    END,
    UTC_TIMESTAMP(6) - INTERVAL MOD(s.id, 300) SECOND,
    UTC_TIMESTAMP(6) + INTERVAL 1 DAY,
    CASE WHEN sequences.n BETWEEN 3 AND 6 THEN UTC_TIMESTAMP(6) ELSE NULL END,
    CASE WHEN sequences.n >= 3 THEN UTC_TIMESTAMP(6) ELSE NULL END,
    CASE WHEN sequences.n BETWEEN 3 AND 6 THEN 'delivered'
         WHEN sequences.n >= 7 THEN 'synthetic_dead'
         ELSE NULL END
FROM sessoes AS s
CROSS JOIN sequences
WHERE s.agente_id = @plan_agent;

EXPLAIN ANALYZE
SELECT d.id
FROM outbox_deliveries AS d
WHERE d.status IN ('pending', 'retry')
  AND d.cutover_epoch = @plan_epoch
  AND d.next_attempt_at <= UTC_TIMESTAMP(6)
  AND d.expires_at > UTC_TIMESTAMP(6)
  AND d.cancel_requested_at IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM outbox_deliveries AS p FORCE INDEX (idx_outbox_predecessor)
      WHERE p.ordering_key = d.ordering_key
        AND p.stream_sequence < d.stream_sequence
        AND p.status IN ('blocked', 'pending', 'processing', 'retry')
  )
ORDER BY d.priority DESC, d.next_attempt_at, d.id
LIMIT 8
FOR UPDATE SKIP LOCKED;

DELETE FROM outbox_deliveries WHERE agente_id = @plan_agent;
DELETE FROM sessoes WHERE agente_id = @plan_agent;
DELETE FROM agentes WHERE id = @plan_agent;
