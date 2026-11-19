-- T08-3 predecessor fixture: pre-existing legacy delivery heartbeat and audit
-- rows (base columns present in every released predecessor). The upgrade
-- must keep them byte-identical and must not create replica rows.
INSERT INTO delivery_worker_heartbeats (worker_id, code_version, started_at, last_seen_at)
VALUES ('83838383-8383-4383-8383-838383838383', 'outbox-v0',
        UTC_TIMESTAMP(6) - INTERVAL 1 DAY, UTC_TIMESTAMP(6) - INTERVAL 1 DAY);
INSERT INTO delivery_operator_actions (delivery_id, action, actor, reason)
VALUES (NULL, 'cutover', 't083-fixture', 'synthetic predecessor audit');
