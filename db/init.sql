-- Este script é executado no banco definido por MYSQL_DATABASE no container MySQL.

-- Ticket07 (H): CHECK literals carry explicit latin1 introducers, preserving
-- legacy byte-exact fingerprints independently of the connection charset.
-- Existing objects are never rewritten to converge.

-- ─── AGENTES ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agentes (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    nome                VARCHAR(255)    NOT NULL,
    instance_name       VARCHAR(255)    NOT NULL UNIQUE,
    system_prompt       TEXT            NOT NULL,
    modelo_openai       VARCHAR(100)    NOT NULL DEFAULT 'gpt-4o-mini',
    modelo_llm          VARCHAR(255)    NULL,
    numero_notificacao  VARCHAR(50)     NOT NULL,
    limite_mensal_creditos DECIMAL(12,6) NULL,
    chatwoot_inbox_id   INT             NULL,
    max_historico       INT             NOT NULL DEFAULT 20,
    ativo               BOOLEAN         NOT NULL DEFAULT TRUE,
    inbox_scheduler_enabled BOOLEAN      NOT NULL DEFAULT FALSE,
    criado_em           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ─── ATENDENTES ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atendentes (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    nome                VARCHAR(255)    NOT NULL,
    numero_whatsapp     VARCHAR(50)     NOT NULL UNIQUE,
    ativo               BOOLEAN         NOT NULL DEFAULT TRUE,
    indice_round_robin  INT             NOT NULL DEFAULT 0,
    criado_em           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ─── SESSOES ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessoes (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    agente_id           INT             NOT NULL,
    telefone            VARCHAR(50)     NOT NULL,
    handler             ENUM('bot','humano','encerrada') NOT NULL DEFAULT 'bot',
    total_mensagens     INT             NOT NULL DEFAULT 0,
    motivo_handoff      TEXT            NULL,
    encerrada_em        DATETIME        NULL,
    motivo_encerramento VARCHAR(50)     NULL,
    atendente_id        INT             NULL,
    chatwoot_conversation_id INT        NULL,
    delivery_stream_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    next_delivery_sequence BIGINT UNSIGNED NOT NULL DEFAULT 1,
    handoff_generation  INT UNSIGNED    NOT NULL DEFAULT 0,
    criado_em           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ultima_interacao    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT fk_sessao_agente FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT fk_sessao_atendente FOREIGN KEY (atendente_id) REFERENCES atendentes(id),
    CONSTRAINT uq_agente_telefone UNIQUE (agente_id, telefone),
    CONSTRAINT uq_sessoes_delivery_stream UNIQUE (delivery_stream_key),
    CONSTRAINT uq_sessoes_chatwoot_conversation UNIQUE (chatwoot_conversation_id),
    INDEX idx_sessoes_agente_id (agente_id),
    INDEX idx_sessoes_telefone (telefone),
    INDEX idx_sessoes_atendente_id (atendente_id)
);

-- ─── MENSAGENS ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mensagens (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    sessao_id   INT                         NOT NULL,
    role        ENUM('user','assistant')    NOT NULL,
    conteudo    TEXT                        NOT NULL,
    criado_em   DATETIME                    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_mensagem_sessao FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE CASCADE,
    INDEX idx_mensagens_sessao_id (sessao_id),
    INDEX idx_mensagens_criado_em (criado_em)
);

-- ─── CONSUMO LLM (v4.1) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS consumo_llm (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    agente_id INT NOT NULL,
    sessao_id INT NULL,
    modelo_llm VARCHAR(255) NOT NULL,
    modelo_llm_reportado VARCHAR(255) NULL,
    tipo_chamada ENUM('resposta','resumo') NOT NULL,
    tokens_input INT UNSIGNED NOT NULL DEFAULT 0,
    tokens_output INT UNSIGNED NOT NULL DEFAULT 0,
    tokens_reasoning INT UNSIGNED NULL,
    tokens_cache_read INT UNSIGNED NULL,
    tokens_cache_write INT UNSIGNED NULL,
    custo_creditos DECIMAL(12,8) NOT NULL DEFAULT 0,
    provider_generation_id VARCHAR(255) NULL,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_consumo_llm_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT fk_consumo_llm_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE SET NULL,
    INDEX idx_consumo_llm_agente_data (agente_id, criado_em),
    INDEX idx_consumo_llm_sessao (sessao_id),
    UNIQUE KEY uq_consumo_llm_generation (provider_generation_id)
);

-- ─── RECIBOS DE WEBHOOK ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inbound_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    provider ENUM('evolution','chatwoot') NOT NULL,
    source_scope VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    provider_event_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_type VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload_hmac CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    hmac_key_version SMALLINT UNSIGNED NOT NULL,
    agente_id INT NULL,
    sessao_id INT NULL,
    receipt_disposition ENUM(
        'scheduled','ignored_nonbot','legacy_evolution','chatwoot','legacy_unknown'
    ) NOT NULL DEFAULT 'legacy_unknown',
    mensagem_id INT NULL,
    received_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    processed_at DATETIME(6) NULL,
    CONSTRAINT fk_inbound_event_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE SET NULL,
    CONSTRAINT fk_inbound_event_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE SET NULL,
    CONSTRAINT fk_inbound_event_mensagem
        FOREIGN KEY (mensagem_id) REFERENCES mensagens(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT ck_inbound_hmac_key_version CHECK (hmac_key_version > 0),
    UNIQUE KEY uq_inbound_event_provider_scope_id
        (provider, source_scope, provider_event_id),
    UNIQUE KEY uq_inbound_event_mensagem (mensagem_id),
    INDEX idx_inbound_events_received_at (received_at)
);

-- ─── AUTORIDADE DO AGENDADOR DE TURNS ───────────────────────────
CREATE TABLE IF NOT EXISTS scheduler_runtime_state (
    id SMALLINT PRIMARY KEY,
    state ENUM('disabled','enabling','enabled','draining') NOT NULL DEFAULT 'disabled',
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    version BIGINT UNSIGNED NOT NULL DEFAULT 1,
    schema_version INT UNSIGNED NOT NULL DEFAULT 1,
    max_pending_inputs_per_session INT UNSIGNED NOT NULL DEFAULT 128,
    max_pending_input_bytes_per_session INT UNSIGNED NOT NULL DEFAULT 262144,
    max_queued_turns_per_agent INT UNSIGNED NOT NULL DEFAULT 1000,
    max_queued_bytes_per_agent BIGINT UNSIGNED NOT NULL DEFAULT 16777216,
    max_global_queued_turns INT UNSIGNED NOT NULL DEFAULT 10000,
    max_global_queued_bytes BIGINT UNSIGNED NOT NULL DEFAULT 268435456,
    max_generation_attempts_per_turn SMALLINT UNSIGNED NOT NULL DEFAULT 2,
    max_delivery_attempts_per_turn SMALLINT UNSIGNED NOT NULL DEFAULT 8,
    max_refreshes_per_turn SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    accounting_drain_seconds SMALLINT UNSIGNED NOT NULL DEFAULT 30,
    changed_by VARCHAR(255) NOT NULL,
    change_reason VARCHAR(500) NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT ck_scheduler_runtime_singleton CHECK (id = 1),
    CONSTRAINT ck_scheduler_runtime_version CHECK (version > 0),
    CONSTRAINT ck_scheduler_schema_version CHECK (schema_version > 0),
    CONSTRAINT ck_scheduler_session_input_cap CHECK (
        max_pending_inputs_per_session BETWEEN 1 AND 10000
    ),
    CONSTRAINT ck_scheduler_session_bytes_cap CHECK (
        max_pending_input_bytes_per_session BETWEEN 1 AND 16777215
    ),
    CONSTRAINT ck_scheduler_agent_turn_cap CHECK (
        max_queued_turns_per_agent BETWEEN 1 AND 100000
    ),
    CONSTRAINT ck_scheduler_agent_bytes_cap CHECK (
        max_queued_bytes_per_agent BETWEEN 1 AND 1073741824
    ),
    CONSTRAINT ck_scheduler_global_turn_cap CHECK (
        max_global_queued_turns BETWEEN 1 AND 1000000
    ),
    CONSTRAINT ck_scheduler_global_bytes_cap CHECK (
        max_global_queued_bytes BETWEEN 1 AND 17179869184
    ),
    CONSTRAINT ck_scheduler_generation_cap CHECK (
        max_generation_attempts_per_turn BETWEEN 1 AND 8
    ),
    CONSTRAINT ck_scheduler_delivery_cap CHECK (
        max_delivery_attempts_per_turn BETWEEN 1 AND 32
    ),
    CONSTRAINT ck_scheduler_refresh_cap CHECK (max_refreshes_per_turn = 1),
    CONSTRAINT ck_scheduler_accounting_drain CHECK (
        accounting_drain_seconds BETWEEN 1 AND 300
    )
);

INSERT INTO scheduler_runtime_state (
    id, state, scheduler_epoch, version, schema_version, changed_by, change_reason
)
VALUES (1, 'disabled', UUID(), 1, 1, 'migration', 'initial disabled scheduler state')
ON DUPLICATE KEY UPDATE id = id;

CREATE TABLE IF NOT EXISTS agent_service_lanes (
    agente_id INT PRIMARY KEY,
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    next_arrival_sequence BIGINT UNSIGNED NOT NULL DEFAULT 1,
    next_scheduling_sequence BIGINT UNSIGNED NOT NULL DEFAULT 1,
    active_turn_id BIGINT NULL,
    active_revision_id BIGINT NULL,
    owner_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    owner_worker_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    lease_until DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_lane_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT ck_lane_sequences_positive CHECK (
        next_arrival_sequence > 0 AND next_scheduling_sequence > 0
    ),
    CONSTRAINT ck_lane_lease_tuple CHECK (
        (owner_token IS NULL AND owner_worker_id IS NULL AND lease_until IS NULL
            AND active_turn_id IS NULL AND active_revision_id IS NULL)
        OR (owner_token IS NOT NULL AND owner_worker_id IS NOT NULL
            AND lease_until IS NOT NULL AND active_turn_id IS NOT NULL)
    ),
    INDEX idx_agent_lanes_lease (lease_until, agente_id)
);

CREATE TABLE IF NOT EXISTS conversation_turns (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    turn_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    agente_id INT NOT NULL,
    sessao_id INT NOT NULL,
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    arrival_sequence BIGINT UNSIGNED NOT NULL,
    scheduling_sequence BIGINT UNSIGNED NOT NULL,
    status ENUM(
        'pending','claimed','generating','typing','awaiting_delivery','retry',
        'delivered','handoff','cancelled','dead'
    ) NOT NULL DEFAULT 'pending',
    session_slot ENUM('active','successor','terminal') NOT NULL DEFAULT 'active',
    active_session_guard INT GENERATED ALWAYS AS (
        CASE WHEN session_slot = 'active' THEN sessao_id ELSE NULL END
    ) STORED,
    successor_session_guard INT GENERATED ALWAYS AS (
        CASE WHEN session_slot = 'successor' THEN sessao_id ELSE NULL END
    ) STORED,
    input_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
    refresh_count SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    current_revision_id BIGINT NULL,
    final_revision_id BIGINT NULL,
    claim_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    claim_owner VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    claim_until DATETIME(6) NULL,
    ready_at DATETIME(6) NOT NULL,
    retry_at DATETIME(6) NULL,
    delivery_attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
    terminal_reason VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    terminal_at DATETIME(6) NULL,
    CONSTRAINT fk_turn_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT fk_turn_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE RESTRICT,
    CONSTRAINT ck_turn_arrival_positive CHECK (arrival_sequence > 0),
    CONSTRAINT ck_turn_schedule_positive CHECK (scheduling_sequence > 0),
    CONSTRAINT ck_turn_input_version_positive CHECK (input_version > 0),
    CONSTRAINT ck_turn_refresh_count CHECK (refresh_count BETWEEN 0 AND 1),
    CONSTRAINT ck_turn_delivery_attempts CHECK (delivery_attempt_count BETWEEN 0 AND 32),
    CONSTRAINT ck_turn_claim_tuple CHECK (
        (claim_token IS NULL AND claim_owner IS NULL AND claim_until IS NULL)
        OR (claim_token IS NOT NULL AND claim_owner IS NOT NULL AND claim_until IS NOT NULL)
    ),
    CONSTRAINT ck_turn_slot_status CHECK (
        (session_slot = _latin1'successor' AND status = _latin1'pending')
        OR (session_slot = _latin1'active' AND status IN
            (_latin1'pending',_latin1'claimed',_latin1'generating',_latin1'typing',_latin1'awaiting_delivery',_latin1'retry'))
        OR (session_slot = _latin1'terminal' AND status IN
            (_latin1'delivered',_latin1'handoff',_latin1'cancelled',_latin1'dead'))
    ),
    CONSTRAINT ck_turn_terminal_state CHECK (
        (session_slot = _latin1'terminal' AND terminal_at IS NOT NULL
            AND terminal_reason IS NOT NULL)
        OR (session_slot <> _latin1'terminal' AND terminal_at IS NULL)
    ),
    UNIQUE KEY uq_conversation_turn_key (turn_key),
    UNIQUE KEY uq_turn_agent_arrival_sequence (agente_id, arrival_sequence),
    UNIQUE KEY uq_turn_agent_scheduling_sequence (agente_id, scheduling_sequence),
    UNIQUE KEY uq_turn_active_session (active_session_guard),
    UNIQUE KEY uq_turn_successor_session (successor_session_guard),
    UNIQUE KEY uq_turn_authority_identity
        (id, agente_id, sessao_id, scheduler_epoch),
    INDEX idx_turn_ready_claim
        (agente_id, status, ready_at, scheduling_sequence, id),
    INDEX idx_turn_session_status (sessao_id, status, id),
    INDEX idx_turn_claim_lease (status, claim_until, id)
);

CREATE TABLE IF NOT EXISTS turn_revisions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    revision_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    turn_id BIGINT NOT NULL,
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    revision_number SMALLINT UNSIGNED NOT NULL,
    refresh_ordinal SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    replacement_of_revision_id BIGINT NULL,
    status ENUM(
        'collecting','generating','draft','typing','final_candidate','admitted',
        'superseded','cancelled','dead'
    ) NOT NULL DEFAULT 'collecting',
    sealed_first_input_sequence BIGINT UNSIGNED NULL,
    sealed_last_input_sequence BIGINT UNSIGNED NULL,
    sealed_input_count INT UNSIGNED NULL,
    sealed_input_bytes BIGINT UNSIGNED NULL,
    draft_text TEXT NULL,
    draft_text_bytes INT UNSIGNED NULL,
    needs_handoff BOOLEAN NULL,
    draft_kind ENUM('generated','soft_limit_fallback','technical_fallback') NULL,
    typing_delay_ms INT UNSIGNED NULL,
    typing_jitter_ms SMALLINT NULL,
    typing_deadline_at DATETIME(6) NULL,
    generation_completed_at DATETIME(6) NULL,
    sealed_at DATETIME(6) NULL,
    admitted_at DATETIME(6) NULL,
    superseded_reason VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_revision_turn
        FOREIGN KEY (turn_id) REFERENCES conversation_turns(id) ON DELETE CASCADE,
    CONSTRAINT fk_revision_replacement
        FOREIGN KEY (replacement_of_revision_id) REFERENCES turn_revisions(id) ON DELETE RESTRICT,
    CONSTRAINT ck_revision_number CHECK (revision_number BETWEEN 1 AND 2),
    CONSTRAINT ck_revision_refresh_ordinal CHECK (refresh_ordinal BETWEEN 0 AND 1),
    CONSTRAINT ck_revision_sealed_range CHECK (
        (sealed_first_input_sequence IS NULL AND sealed_last_input_sequence IS NULL
            AND sealed_input_count IS NULL AND sealed_input_bytes IS NULL AND sealed_at IS NULL)
        OR (sealed_first_input_sequence IS NOT NULL AND sealed_last_input_sequence IS NOT NULL
            AND sealed_first_input_sequence <= sealed_last_input_sequence
            AND sealed_input_count > 0 AND sealed_input_bytes >= 0 AND sealed_at IS NOT NULL)
    ),
    CONSTRAINT ck_revision_typing_delay CHECK (
        typing_delay_ms IS NULL OR typing_delay_ms BETWEEN 2000 AND 12000
    ),
    CONSTRAINT ck_revision_typing_jitter CHECK (
        typing_jitter_ms IS NULL OR typing_jitter_ms BETWEEN -1000 AND 1000
    ),
    UNIQUE KEY uq_turn_revision_key (revision_key),
    UNIQUE KEY uq_turn_revision_number (turn_id, revision_number),
    UNIQUE KEY uq_turn_revision_replacement (replacement_of_revision_id),
    UNIQUE KEY uq_revision_authority_identity (id, turn_id, scheduler_epoch),
    INDEX idx_revision_turn_status (turn_id, status, id)
);

CREATE TABLE IF NOT EXISTS turn_input_memberships (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    turn_id BIGINT NOT NULL,
    sessao_id INT NOT NULL,
    mensagem_id INT NOT NULL,
    inbound_event_id BIGINT NOT NULL,
    input_sequence BIGINT UNSIGNED NOT NULL,
    content_bytes INT UNSIGNED NOT NULL,
    membership_state ENUM('active','sealed','post_handoff_pending') NOT NULL DEFAULT 'active',
    assigned_revision_id BIGINT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_turn_input_turn
        FOREIGN KEY (turn_id) REFERENCES conversation_turns(id) ON DELETE RESTRICT,
    CONSTRAINT fk_turn_input_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE RESTRICT,
    CONSTRAINT fk_turn_input_mensagem
        FOREIGN KEY (mensagem_id) REFERENCES mensagens(id) ON DELETE RESTRICT,
    CONSTRAINT fk_turn_input_receipt
        FOREIGN KEY (inbound_event_id) REFERENCES inbound_events(id) ON DELETE RESTRICT,
    CONSTRAINT fk_turn_input_revision
        FOREIGN KEY (assigned_revision_id) REFERENCES turn_revisions(id) ON DELETE RESTRICT,
    CONSTRAINT ck_turn_input_sequence_positive CHECK (input_sequence > 0),
    CONSTRAINT ck_turn_input_bytes CHECK (content_bytes >= 0),
    CONSTRAINT ck_turn_input_membership_state CHECK (
        (membership_state = _latin1'active' AND assigned_revision_id IS NULL)
        OR (membership_state = _latin1'sealed' AND assigned_revision_id IS NOT NULL)
        OR membership_state = _latin1'post_handoff_pending'
    ),
    UNIQUE KEY uq_turn_input_message (mensagem_id),
    UNIQUE KEY uq_turn_input_receipt (inbound_event_id),
    UNIQUE KEY uq_turn_input_session_sequence (sessao_id, input_sequence),
    INDEX idx_turn_input_range (turn_id, input_sequence, id),
    INDEX idx_turn_input_session_state (sessao_id, membership_state, id)
);

CREATE TABLE IF NOT EXISTS generation_attempts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    attempt_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    agente_id INT NOT NULL,
    sessao_id INT NOT NULL,
    turn_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    lane_owner_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    attempt_type ENUM('response','summary') NOT NULL,
    attempt_ordinal SMALLINT UNSIGNED NOT NULL,
    outcome ENUM('started','succeeded','failed','outcome_unknown') NOT NULL DEFAULT 'started',
    accounting_state ENUM('pending','recorded','reconciliation_required') NOT NULL DEFAULT 'pending',
    provider_generation_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    modelo_llm VARCHAR(255) NOT NULL,
    tokens_input INT UNSIGNED NULL,
    tokens_output INT UNSIGNED NULL,
    tokens_reasoning INT UNSIGNED NULL,
    tokens_cache_read INT UNSIGNED NULL,
    tokens_cache_write INT UNSIGNED NULL,
    custo_creditos DECIMAL(12,8) NULL,
    consumo_llm_id BIGINT NULL,
    safe_error_code VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NULL,
    started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    accounting_drain_until DATETIME(6) NULL,
    outcome_recorded_at DATETIME(6) NULL,
    reconciled_at DATETIME(6) NULL,
    reconciliation_actor VARCHAR(255) NULL,
    reconciliation_reason VARCHAR(500) NULL,
    CONSTRAINT fk_generation_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT fk_generation_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE CASCADE,
    CONSTRAINT fk_generation_turn
        FOREIGN KEY (turn_id) REFERENCES conversation_turns(id) ON DELETE CASCADE,
    CONSTRAINT fk_generation_revision
        FOREIGN KEY (revision_id) REFERENCES turn_revisions(id) ON DELETE CASCADE,
    CONSTRAINT fk_generation_consumo
        FOREIGN KEY (consumo_llm_id) REFERENCES consumo_llm(id) ON DELETE RESTRICT,
    CONSTRAINT ck_generation_attempt_ordinal CHECK (attempt_ordinal BETWEEN 1 AND 8),
    CONSTRAINT ck_generation_outcome_timestamp CHECK (
        (outcome = _latin1'started' AND outcome_recorded_at IS NULL)
        OR (outcome IN (_latin1'succeeded',_latin1'failed',_latin1'outcome_unknown')
            AND outcome_recorded_at IS NOT NULL)
    ),
    CONSTRAINT ck_generation_tokens_input CHECK (tokens_input IS NULL OR tokens_input >= 0),
    CONSTRAINT ck_generation_tokens_output CHECK (tokens_output IS NULL OR tokens_output >= 0),
    CONSTRAINT ck_generation_cost CHECK (custo_creditos IS NULL OR custo_creditos >= 0),
    CONSTRAINT ck_generation_reconciliation_audit CHECK (
        (reconciled_at IS NULL AND reconciliation_actor IS NULL
            AND reconciliation_reason IS NULL)
        OR (reconciled_at IS NOT NULL AND reconciliation_actor IS NOT NULL
            AND reconciliation_reason IS NOT NULL)
    ),
    UNIQUE KEY uq_generation_attempt_key (attempt_key),
    UNIQUE KEY uq_generation_attempt_ordinal (revision_id, attempt_type, attempt_ordinal),
    UNIQUE KEY uq_generation_provider_id (provider_generation_id),
    INDEX idx_generation_turn_outcome (turn_id, outcome, id),
    INDEX idx_generation_agent_unknown (agente_id, outcome, id),
    INDEX idx_generation_agent_reconciliation
        (agente_id, accounting_state, reconciled_at, id)
);

-- ─── CICLO DE VIDA DE HANDOFF ────────────────────────────────────
CREATE TABLE IF NOT EXISTS handoff_attempts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    handoff_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sessao_id INT NOT NULL,
    agente_id INT NOT NULL,
    generation INT UNSIGNED NOT NULL,
    status ENUM('pending','ready','failed','superseded','cancelled') NOT NULL DEFAULT 'pending',
    chatwoot_inbox_id INT NOT NULL,
    chatwoot_contact_id INT NULL,
    chatwoot_conversation_id INT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ready_at DATETIME(6) NULL,
    terminal_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_handoff_attempt_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE CASCADE,
    CONSTRAINT fk_handoff_attempt_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT ck_handoff_generation_positive CHECK (generation > 0),
    CONSTRAINT ck_handoff_status_timestamps CHECK (
        (status = _latin1'pending' AND ready_at IS NULL AND terminal_at IS NULL)
        OR (status = _latin1'ready' AND ready_at IS NOT NULL AND terminal_at IS NULL
            AND chatwoot_conversation_id IS NOT NULL)
        OR (status IN (_latin1'failed',_latin1'superseded',_latin1'cancelled') AND terminal_at IS NOT NULL)
    ),
    UNIQUE KEY uq_handoff_attempt_key (handoff_key),
    UNIQUE KEY uq_handoff_session_generation (sessao_id, generation),
    UNIQUE KEY uq_handoff_chatwoot_conversation (chatwoot_conversation_id),
    UNIQUE KEY uq_handoff_authority_identity (id, agente_id, sessao_id),
    INDEX idx_handoff_attempts_session_status (sessao_id, status)
);

-- ─── OUTBOX DURÁVEL ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outbox_deliveries (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    delivery_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    cutover_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    dedupe_key VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    provider ENUM('evolution','chatwoot') NOT NULL,
    operation VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload_version SMALLINT UNSIGNED NOT NULL,
    payload JSON NOT NULL,
    delivery_class ENUM(
        'legacy_unknown','legacy_automated','human','automated','provider_setup_context'
    ) NOT NULL DEFAULT 'legacy_unknown',
    automated_effect ENUM('reply','handoff_transition')
        CHARACTER SET ascii COLLATE ascii_bin NULL,
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    conversation_turn_id BIGINT NULL,
    turn_revision_id BIGINT NULL,
    agente_id INT NULL,
    sessao_id INT NULL,
    handoff_attempt_id BIGINT NULL,
    inbound_event_id BIGINT NULL,
    ordering_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    stream_sequence BIGINT UNSIGNED NOT NULL,
    depends_on_delivery_id BIGINT NULL,
    priority SMALLINT NOT NULL DEFAULT 0,
    status ENUM('blocked','pending','processing','retry','delivered','dead','cancelled') NOT NULL,
    attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
    ambiguity_count INT UNSIGNED NOT NULL DEFAULT 0,
    next_attempt_at DATETIME(6) NULL,
    expires_at DATETIME(6) NULL,
    lease_token CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    lock_owner VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    lock_until DATETIME(6) NULL,
    cancel_requested_at DATETIME(6) NULL,
    external_id VARCHAR(255) NULL,
    last_http_status SMALLINT UNSIGNED NULL,
    last_error_code VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NULL,
    terminal_reason VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    delivered_at DATETIME(6) NULL,
    terminal_at DATETIME(6) NULL,
    CONSTRAINT fk_outbox_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE SET NULL,
    CONSTRAINT fk_outbox_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE SET NULL,
    CONSTRAINT fk_outbox_handoff
        FOREIGN KEY (handoff_attempt_id) REFERENCES handoff_attempts(id) ON DELETE SET NULL,
    CONSTRAINT fk_outbox_inbound
        FOREIGN KEY (inbound_event_id) REFERENCES inbound_events(id) ON DELETE SET NULL,
    CONSTRAINT fk_outbox_conversation_turn
        FOREIGN KEY (conversation_turn_id) REFERENCES conversation_turns(id) ON DELETE RESTRICT,
    CONSTRAINT fk_outbox_turn_revision
        FOREIGN KEY (turn_revision_id) REFERENCES turn_revisions(id) ON DELETE RESTRICT,
    CONSTRAINT fk_outbox_turn_authority
        FOREIGN KEY (conversation_turn_id, agente_id, sessao_id, scheduler_epoch)
        REFERENCES conversation_turns(id, agente_id, sessao_id, scheduler_epoch)
        ON DELETE RESTRICT,
    CONSTRAINT fk_outbox_revision_authority
        FOREIGN KEY (turn_revision_id, conversation_turn_id, scheduler_epoch)
        REFERENCES turn_revisions(id, turn_id, scheduler_epoch)
        ON DELETE RESTRICT,
    CONSTRAINT fk_outbox_handoff_authority
        FOREIGN KEY (handoff_attempt_id, agente_id, sessao_id)
        REFERENCES handoff_attempts(id, agente_id, sessao_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_outbox_dependency
        FOREIGN KEY (depends_on_delivery_id) REFERENCES outbox_deliveries(id) ON DELETE RESTRICT,
    CONSTRAINT ck_outbox_sequence_nonnegative CHECK (stream_sequence >= 0),
    CONSTRAINT ck_outbox_provider_sequence CHECK (
        (provider = _latin1'evolution' AND stream_sequence > 0)
        OR (provider = _latin1'chatwoot' AND stream_sequence = 0)
    ),
    CONSTRAINT ck_outbox_attempt_count CHECK (attempt_count >= 0),
    CONSTRAINT ck_outbox_ambiguity_count CHECK (ambiguity_count >= 0),
    CONSTRAINT ck_outbox_lease_tuple CHECK (
        (lease_token IS NULL AND lock_owner IS NULL AND lock_until IS NULL
            AND status <> _latin1'processing')
        OR (lease_token IS NOT NULL AND lock_owner IS NOT NULL AND lock_until IS NOT NULL
            AND status = _latin1'processing')
    ),
    CONSTRAINT ck_outbox_schedule_state CHECK (
        (status = _latin1'blocked' AND depends_on_delivery_id IS NOT NULL
            AND next_attempt_at IS NULL AND expires_at IS NULL)
        OR (status IN (_latin1'pending',_latin1'retry',_latin1'processing')
            AND next_attempt_at IS NOT NULL AND expires_at IS NOT NULL)
        OR status IN (_latin1'delivered',_latin1'dead',_latin1'cancelled')
    ),
    CONSTRAINT ck_outbox_delivery_authority_v2 CHECK (
        (delivery_class = _latin1'automated' AND automated_effect IS NOT NULL
            AND conversation_turn_id IS NOT NULL AND turn_revision_id IS NOT NULL
            AND scheduler_epoch IS NOT NULL)
        OR (delivery_class <> _latin1'automated' AND automated_effect IS NULL
            AND conversation_turn_id IS NULL AND turn_revision_id IS NULL
            AND scheduler_epoch IS NULL)
    ),
    UNIQUE KEY uq_outbox_delivery_key (delivery_key),
    UNIQUE KEY uq_outbox_dedupe_key (dedupe_key),
    UNIQUE KEY uq_outbox_stream_sequence (ordering_key, stream_sequence),
    UNIQUE KEY uq_outbox_turn_revision_effect
        (conversation_turn_id, turn_revision_id, automated_effect),
    INDEX idx_outbox_claim (status, next_attempt_at, priority, id),
    INDEX idx_outbox_predecessor (ordering_key, stream_sequence, status),
    INDEX idx_outbox_lease (status, lock_until),
    INDEX idx_outbox_dependency (depends_on_delivery_id, status),
    INDEX idx_outbox_session_status (sessao_id, status),
    INDEX idx_outbox_delivery_class_status (delivery_class, status, id)
);

-- ─── RECONCILIAÇÃO DE USO LLM ────────────────────────────────────
CREATE TABLE IF NOT EXISTS llm_usage_reconciliation (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    reconciliation_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    agente_id INT NOT NULL,
    sessao_id INT NULL,
    modelo_llm VARCHAR(255) NOT NULL,
    modelo_llm_reportado VARCHAR(255) NULL,
    tipo_chamada ENUM('resposta','resumo') NOT NULL,
    tokens_input INT UNSIGNED NOT NULL DEFAULT 0,
    tokens_output INT UNSIGNED NOT NULL DEFAULT 0,
    tokens_reasoning INT UNSIGNED NULL,
    tokens_cache_read INT UNSIGNED NULL,
    tokens_cache_write INT UNSIGNED NULL,
    custo_creditos DECIMAL(12,8) NOT NULL DEFAULT 0,
    provider_generation_id VARCHAR(255) NULL,
    safe_error_code VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status ENUM('pending','processing','resolved','dead') NOT NULL DEFAULT 'pending',
    attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
    next_attempt_at DATETIME(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_error_code VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NULL,
    resolution_consumo_id BIGINT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    resolved_at DATETIME(6) NULL,
    CONSTRAINT fk_usage_reconciliation_agente
        FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT fk_usage_reconciliation_sessao
        FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE SET NULL,
    CONSTRAINT fk_usage_reconciliation_consumo
        FOREIGN KEY (resolution_consumo_id) REFERENCES consumo_llm(id) ON DELETE SET NULL,
    CONSTRAINT ck_usage_reconciliation_attempts CHECK (attempt_count >= 0),
    UNIQUE KEY uq_usage_reconciliation_key (reconciliation_key),
    UNIQUE KEY uq_usage_reconciliation_generation (provider_generation_id),
    INDEX idx_usage_reconciliation_due (status, next_attempt_at, id),
    INDEX idx_usage_reconciliation_session (sessao_id)
);

-- ─── ESTADO OPERACIONAL DA ENTREGA ───────────────────────────────
CREATE TABLE IF NOT EXISTS delivery_worker_heartbeats (
    worker_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    code_version VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    started_at DATETIME(6) NOT NULL,
    last_seen_at DATETIME(6) NOT NULL,
    current_lease_count INT UNSIGNED NOT NULL DEFAULT 0,
    dispatch_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    configured_mode ENUM('direct','outbox') NOT NULL DEFAULT 'direct',
    configured_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    provider_ready BOOLEAN NOT NULL DEFAULT FALSE,
    runtime_compatible BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT ck_worker_heartbeat_leases CHECK (current_lease_count >= 0),
    INDEX idx_worker_heartbeats_last_seen (last_seen_at)
);

CREATE TABLE IF NOT EXISTS delivery_operator_actions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    delivery_id BIGINT NULL,
    action VARCHAR(50) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor VARCHAR(255) NOT NULL,
    reason VARCHAR(500) NOT NULL,
    snapshot_cutover_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    snapshot_status VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NULL,
    snapshot_attempt_count INT UNSIGNED NULL,
    snapshot_ambiguity_count INT UNSIGNED NULL,
    snapshot_terminal_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_operator_action_delivery
        FOREIGN KEY (delivery_id) REFERENCES outbox_deliveries(id) ON DELETE SET NULL,
    INDEX idx_operator_actions_delivery (delivery_id, created_at)
);

CREATE TABLE IF NOT EXISTS delivery_runtime_state (
    id SMALLINT PRIMARY KEY,
    mode ENUM('direct','outbox','frozen') NOT NULL DEFAULT 'direct',
    cutover_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    version BIGINT UNSIGNED NOT NULL DEFAULT 1,
    changed_by VARCHAR(255) NOT NULL,
    change_reason VARCHAR(500) NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    CONSTRAINT ck_delivery_runtime_singleton CHECK (id = 1),
    CONSTRAINT ck_delivery_runtime_version CHECK (version > 0)
);

INSERT INTO delivery_runtime_state (
    id, mode, cutover_epoch, version, changed_by, change_reason
)
VALUES (1, 'direct', UUID(), 1, 'migration', 'initial durable delivery state')
ON DUPLICATE KEY UPDATE id = id;

-- ─── UPGRADE RETROCOMPATÍVEL (MySQL 8) ───────────────────────────
-- O MySQL 8 não aceita ADD COLUMN IF NOT EXISTS. As verificações pelo
-- information_schema tornam este bootstrap seguro para volumes legados.
DELIMITER $$

DROP PROCEDURE IF EXISTS migrate_wpp_ai_bot_v41$$

CREATE PROCEDURE migrate_wpp_ai_bot_v41()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'agentes'
          AND column_name = 'chatwoot_inbox_id'
    ) THEN
        ALTER TABLE agentes ADD COLUMN chatwoot_inbox_id INT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'agentes'
          AND column_name = 'modelo_llm'
    ) THEN
        ALTER TABLE agentes ADD COLUMN modelo_llm VARCHAR(255) NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'agentes'
          AND column_name = 'limite_mensal_creditos'
    ) THEN
        ALTER TABLE agentes ADD COLUMN limite_mensal_creditos DECIMAL(12,6) NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'agentes'
          AND column_name = 'inbox_scheduler_enabled'
    ) THEN
        ALTER TABLE agentes
            ADD COLUMN inbox_scheduler_enabled BOOLEAN NOT NULL DEFAULT FALSE
            AFTER ativo;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'inbound_events'
          AND column_name = 'mensagem_id'
    ) THEN
        ALTER TABLE inbound_events
            ADD COLUMN mensagem_id INT NULL AFTER sessao_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'inbound_events'
          AND column_name = 'mensagem_id'
          AND data_type = 'int'
          AND column_type = 'int'
          AND is_nullable = 'YES'
          AND column_default IS NULL
    ) THEN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'inbound_events'
              AND column_name = 'mensagem_id'
              AND data_type NOT IN ('tinyint','smallint','mediumint','int','bigint')
        ) AND EXISTS (
            SELECT 1 FROM inbound_events WHERE mensagem_id IS NOT NULL LIMIT 1
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'inbound message column type requires audited repair';
        END IF;
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'inbound_events'
              AND column_name = 'mensagem_id'
              AND data_type IN ('tinyint','smallint','mediumint','int','bigint')
        ) AND EXISTS (
            SELECT 1 FROM inbound_events
            WHERE mensagem_id < -2147483648 OR mensagem_id > 2147483647
            LIMIT 1
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'inbound message column range requires audited repair';
        END IF;
        IF EXISTS (
            SELECT 1 FROM inbound_events WHERE mensagem_id = 0 LIMIT 1
        ) AND NOT (
            EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'inbound_events'
                  AND column_name = 'mensagem_id'
                  AND data_type = 'int'
                  AND is_nullable = 'NO'
                  AND column_default = '0'
            )
            AND NOT EXISTS (SELECT 1 FROM mensagens WHERE id = 0)
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'ambiguous zero inbound message association requires audited repair';
        END IF;
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'inbound_events'
              AND column_name = 'mensagem_id'
              AND data_type = 'int'
              AND is_nullable = 'NO'
              AND column_default = '0'
        ) AND NOT EXISTS (SELECT 1 FROM mensagens WHERE id = 0) THEN
            ALTER TABLE inbound_events
                MODIFY COLUMN mensagem_id INT NULL DEFAULT 0;
            UPDATE inbound_events SET mensagem_id = NULL WHERE mensagem_id = 0;
        END IF;
        ALTER TABLE inbound_events
            MODIFY COLUMN mensagem_id INT NULL DEFAULT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'inbound_events'
          AND column_name = 'receipt_disposition'
    ) THEN
        ALTER TABLE inbound_events
            ADD COLUMN receipt_disposition ENUM(
                'scheduled','ignored_nonbot','legacy_evolution','chatwoot','legacy_unknown'
            ) NULL AFTER sessao_id;
    END IF;

    UPDATE inbound_events AS receipt
    SET receipt.receipt_disposition = CASE
        WHEN receipt.provider = 'chatwoot' THEN 'chatwoot'
        WHEN receipt.mensagem_id IS NOT NULL OR EXISTS (
            SELECT 1 FROM turn_input_memberships AS membership
            WHERE membership.inbound_event_id = receipt.id
        ) THEN 'scheduled'
        ELSE 'legacy_unknown'
    END
    WHERE receipt.receipt_disposition IS NULL;

    ALTER TABLE inbound_events
        MODIFY COLUMN receipt_disposition ENUM(
            'scheduled','ignored_nonbot','legacy_evolution','chatwoot','legacy_unknown'
        ) NOT NULL DEFAULT 'legacy_unknown';

    IF EXISTS (
        SELECT 1
        FROM inbound_events AS receipt
        WHERE (
            receipt.mensagem_id IS NOT NULL
            OR EXISTS (
                SELECT 1
                FROM turn_input_memberships AS membership
                WHERE membership.inbound_event_id = receipt.id
            )
        )
          AND receipt.receipt_disposition <> 'scheduled'
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'inbound receipt disposition evidence conflict requires audited repair';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM inbound_events AS receipt
        WHERE (receipt.provider = 'chatwoot' AND receipt.receipt_disposition <> 'chatwoot')
           OR (receipt.provider <> 'chatwoot' AND receipt.receipt_disposition = 'chatwoot')
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'inbound receipt provider disposition conflict requires audited repair';
    END IF;

    IF NOT (
        (SELECT COUNT(*)
         FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name = 'inbound_events'
           AND index_name = 'uq_inbound_event_mensagem') = 1
        AND EXISTS (
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'inbound_events'
              AND index_name = 'uq_inbound_event_mensagem'
              AND non_unique = 0
              AND seq_in_index = 1
              AND column_name = 'mensagem_id'
        )
    ) THEN
        IF EXISTS (
            SELECT mensagem_id
            FROM inbound_events
            WHERE mensagem_id IS NOT NULL
            GROUP BY mensagem_id
            HAVING COUNT(*) > 1
            LIMIT 1
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'duplicate inbound message association requires audited repair';
        END IF;
        IF EXISTS (
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'inbound_events'
              AND index_name = 'uq_inbound_event_mensagem'
        ) THEN
            IF NOT (
                (SELECT COUNT(*)
                 FROM information_schema.statistics
                 WHERE table_schema = DATABASE()
                   AND table_name = 'inbound_events'
                   AND index_name = 'uq_inbound_event_mensagem_repair') = 1
                AND EXISTS (
                    SELECT 1 FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND table_name = 'inbound_events'
                      AND index_name = 'uq_inbound_event_mensagem_repair'
                      AND non_unique = 0
                      AND seq_in_index = 1
                      AND column_name = 'mensagem_id'
                )
            ) THEN
                IF EXISTS (
                    SELECT 1 FROM information_schema.statistics
                    WHERE table_schema = DATABASE()
                      AND table_name = 'inbound_events'
                      AND index_name = 'uq_inbound_event_mensagem_repair'
                ) THEN
                    ALTER TABLE inbound_events
                        DROP INDEX uq_inbound_event_mensagem_repair;
                END IF;
                ALTER TABLE inbound_events
                    ADD UNIQUE INDEX uq_inbound_event_mensagem_repair (mensagem_id);
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'inbound_events'
                  AND index_name = 'idx_inbound_event_agente_repair'
            ) THEN
                ALTER TABLE inbound_events
                    ADD INDEX idx_inbound_event_agente_repair (agente_id);
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'inbound_events'
                  AND index_name = 'idx_inbound_event_sessao_repair'
            ) THEN
                ALTER TABLE inbound_events
                    ADD INDEX idx_inbound_event_sessao_repair (sessao_id);
            END IF;
            ALTER TABLE inbound_events
                DROP INDEX uq_inbound_event_mensagem;
            ALTER TABLE inbound_events
                RENAME INDEX uq_inbound_event_mensagem_repair
                TO uq_inbound_event_mensagem;
        ELSEIF (
            (SELECT COUNT(*)
             FROM information_schema.statistics
             WHERE table_schema = DATABASE()
               AND table_name = 'inbound_events'
               AND index_name = 'uq_inbound_event_mensagem_repair') = 1
            AND EXISTS (
                SELECT 1 FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'inbound_events'
                  AND index_name = 'uq_inbound_event_mensagem_repair'
                  AND non_unique = 0
                  AND seq_in_index = 1
                  AND column_name = 'mensagem_id'
            )
        ) THEN
            ALTER TABLE inbound_events
                RENAME INDEX uq_inbound_event_mensagem_repair
                TO uq_inbound_event_mensagem;
        ELSE
            IF EXISTS (
                SELECT 1 FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'inbound_events'
                  AND index_name = 'uq_inbound_event_mensagem_repair'
            ) THEN
                ALTER TABLE inbound_events
                    DROP INDEX uq_inbound_event_mensagem_repair;
            END IF;
            ALTER TABLE inbound_events
                ADD UNIQUE INDEX uq_inbound_event_mensagem (mensagem_id);
        END IF;
    END IF;

    IF NOT (
        (SELECT COUNT(*)
         FROM information_schema.key_column_usage
         WHERE constraint_schema = DATABASE()
           AND table_name = 'inbound_events'
           AND constraint_name = 'fk_inbound_event_mensagem') = 1
        AND EXISTS (
            SELECT 1
            FROM information_schema.key_column_usage AS kcu
            JOIN information_schema.referential_constraints AS rc
              ON rc.constraint_schema = kcu.constraint_schema
             AND rc.table_name = kcu.table_name
             AND rc.constraint_name = kcu.constraint_name
            WHERE kcu.constraint_schema = DATABASE()
              AND kcu.table_name = 'inbound_events'
              AND kcu.constraint_name = 'fk_inbound_event_mensagem'
              AND kcu.column_name = 'mensagem_id'
              AND kcu.referenced_table_schema = DATABASE()
              AND kcu.referenced_table_name = 'mensagens'
              AND kcu.referenced_column_name = 'id'
              AND rc.delete_rule = 'RESTRICT'
              AND rc.update_rule = 'RESTRICT'
        )
    ) THEN
        IF EXISTS (
            SELECT 1
            FROM inbound_events AS receipt
            LEFT JOIN mensagens AS message ON message.id = receipt.mensagem_id
            WHERE receipt.mensagem_id IS NOT NULL
              AND message.id IS NULL
            LIMIT 1
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'orphan inbound message association requires audited repair';
        END IF;
        IF EXISTS (
            SELECT 1 FROM information_schema.referential_constraints
            WHERE constraint_schema = DATABASE()
              AND table_name = 'inbound_events'
              AND constraint_name = 'fk_inbound_event_mensagem'
        ) THEN
            ALTER TABLE inbound_events
                DROP FOREIGN KEY fk_inbound_event_mensagem;
        END IF;
        ALTER TABLE inbound_events
            ADD CONSTRAINT fk_inbound_event_mensagem
                FOREIGN KEY (mensagem_id) REFERENCES mensagens(id)
                ON DELETE RESTRICT ON UPDATE RESTRICT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND column_name = 'chatwoot_conversation_id'
    ) THEN
        ALTER TABLE sessoes ADD COLUMN chatwoot_conversation_id INT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND column_name = 'delivery_stream_key'
    ) THEN
        ALTER TABLE sessoes
            ADD COLUMN delivery_stream_key CHAR(36)
                CHARACTER SET ascii COLLATE ascii_bin NULL;
    END IF;

    UPDATE sessoes
    SET delivery_stream_key = UUID()
    WHERE delivery_stream_key IS NULL OR delivery_stream_key = '';

    ALTER TABLE sessoes
        MODIFY COLUMN delivery_stream_key CHAR(36)
            CHARACTER SET ascii COLLATE ascii_bin NOT NULL;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND column_name = 'next_delivery_sequence'
    ) THEN
        ALTER TABLE sessoes
            ADD COLUMN next_delivery_sequence BIGINT UNSIGNED NOT NULL DEFAULT 1;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND column_name = 'handoff_generation'
    ) THEN
        ALTER TABLE sessoes
            ADD COLUMN handoff_generation INT UNSIGNED NOT NULL DEFAULT 0;
    END IF;

    ALTER TABLE sessoes
        MODIFY COLUMN handler ENUM('bot','humano','encerrada') NOT NULL DEFAULT 'bot';

    UPDATE agentes
    SET modelo_llm = CASE modelo_openai
        WHEN 'gpt-4o-mini' THEN 'openai/gpt-4o-mini'
        ELSE modelo_openai
    END
    WHERE modelo_llm IS NULL;

    IF EXISTS (
        SELECT 1
        FROM sessoes
        WHERE chatwoot_conversation_id IS NOT NULL
        GROUP BY chatwoot_conversation_id
        HAVING COUNT(*) > 1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'duplicate legacy chatwoot_conversation_id values';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM sessoes
        GROUP BY delivery_stream_key
        HAVING COUNT(*) > 1
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'duplicate delivery_stream_key values';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND index_name = 'uq_sessoes_delivery_stream'
    ) THEN
        CREATE UNIQUE INDEX uq_sessoes_delivery_stream
            ON sessoes (delivery_stream_key);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND index_name = 'uq_sessoes_chatwoot_conversation'
    ) THEN
        CREATE UNIQUE INDEX uq_sessoes_chatwoot_conversation
            ON sessoes (chatwoot_conversation_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND index_name = 'idx_sessoes_agente_id'
    ) THEN
        CREATE INDEX idx_sessoes_agente_id ON sessoes (agente_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND index_name = 'idx_sessoes_telefone'
    ) THEN
        CREATE INDEX idx_sessoes_telefone ON sessoes (telefone);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'sessoes'
          AND index_name = 'idx_sessoes_atendente_id'
    ) THEN
        CREATE INDEX idx_sessoes_atendente_id ON sessoes (atendente_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'mensagens'
          AND index_name = 'idx_mensagens_sessao_id'
    ) THEN
        CREATE INDEX idx_mensagens_sessao_id ON mensagens (sessao_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'mensagens'
          AND index_name = 'idx_mensagens_criado_em'
    ) THEN
        CREATE INDEX idx_mensagens_criado_em ON mensagens (criado_em);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'consumo_llm'
          AND index_name = 'idx_consumo_llm_agente_data'
    ) THEN
        CREATE INDEX idx_consumo_llm_agente_data
            ON consumo_llm (agente_id, criado_em);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'consumo_llm'
          AND index_name = 'idx_consumo_llm_sessao'
    ) THEN
        CREATE INDEX idx_consumo_llm_sessao ON consumo_llm (sessao_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'consumo_llm'
          AND index_name = 'uq_consumo_llm_generation'
    ) THEN
        CREATE UNIQUE INDEX uq_consumo_llm_generation
            ON consumo_llm (provider_generation_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'delivery_worker_heartbeats'
          AND column_name = 'dispatch_enabled'
    ) THEN
        ALTER TABLE delivery_worker_heartbeats
            ADD COLUMN dispatch_enabled BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'delivery_worker_heartbeats'
          AND column_name = 'configured_mode'
    ) THEN
        ALTER TABLE delivery_worker_heartbeats
            ADD COLUMN configured_mode ENUM('direct','outbox') NOT NULL DEFAULT 'direct';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'delivery_worker_heartbeats'
          AND column_name = 'configured_epoch'
    ) THEN
        ALTER TABLE delivery_worker_heartbeats
            ADD COLUMN configured_epoch CHAR(36)
                CHARACTER SET ascii COLLATE ascii_bin NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'delivery_worker_heartbeats'
          AND column_name = 'provider_ready'
    ) THEN
        ALTER TABLE delivery_worker_heartbeats
            ADD COLUMN provider_ready BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'delivery_worker_heartbeats'
          AND column_name = 'runtime_compatible'
    ) THEN
        ALTER TABLE delivery_worker_heartbeats
            ADD COLUMN runtime_compatible BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'delivery_operator_actions'
          AND column_name = 'snapshot_cutover_epoch'
    ) THEN
        ALTER TABLE delivery_operator_actions
            ADD COLUMN snapshot_cutover_epoch CHAR(36)
                CHARACTER SET ascii COLLATE ascii_bin NULL,
            ADD COLUMN snapshot_status VARCHAR(20)
                CHARACTER SET ascii COLLATE ascii_bin NULL,
            ADD COLUMN snapshot_attempt_count INT UNSIGNED NULL,
            ADD COLUMN snapshot_ambiguity_count INT UNSIGNED NULL,
            ADD COLUMN snapshot_terminal_at DATETIME(6) NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND column_name = 'delivery_class'
    ) THEN
        ALTER TABLE outbox_deliveries
            ADD COLUMN delivery_class ENUM(
                'legacy_unknown','legacy_automated','human','automated','provider_setup_context'
            ) NULL AFTER payload;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND column_name = 'scheduler_epoch'
    ) THEN
        ALTER TABLE outbox_deliveries
            ADD COLUMN scheduler_epoch CHAR(36)
                CHARACTER SET ascii COLLATE ascii_bin NULL AFTER delivery_class,
            ADD COLUMN conversation_turn_id BIGINT NULL AFTER scheduler_epoch,
            ADD COLUMN turn_revision_id BIGINT NULL AFTER conversation_turn_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'scheduler_runtime_state'
          AND column_name = 'max_global_queued_turns'
    ) THEN
        ALTER TABLE scheduler_runtime_state
            ADD COLUMN max_global_queued_turns INT UNSIGNED NOT NULL DEFAULT 10000
                AFTER max_queued_bytes_per_agent,
            ADD COLUMN max_global_queued_bytes BIGINT UNSIGNED NOT NULL DEFAULT 268435456
                AFTER max_global_queued_turns,
            ADD COLUMN accounting_drain_seconds SMALLINT UNSIGNED NOT NULL DEFAULT 30
                AFTER max_refreshes_per_turn,
            ADD CONSTRAINT ck_scheduler_global_turn_cap CHECK (
                max_global_queued_turns BETWEEN 1 AND 1000000
            ),
            ADD CONSTRAINT ck_scheduler_global_bytes_cap CHECK (
                max_global_queued_bytes BETWEEN 1 AND 17179869184
            ),
            ADD CONSTRAINT ck_scheduler_accounting_drain CHECK (
                accounting_drain_seconds BETWEEN 1 AND 300
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'generation_attempts'
          AND column_name = 'accounting_drain_until'
    ) THEN
        ALTER TABLE generation_attempts
            ADD COLUMN accounting_drain_until DATETIME(6) NULL AFTER started_at,
            ADD COLUMN reconciled_at DATETIME(6) NULL AFTER outcome_recorded_at,
            ADD COLUMN reconciliation_actor VARCHAR(255) NULL AFTER reconciled_at,
            ADD COLUMN reconciliation_reason VARCHAR(500) NULL AFTER reconciliation_actor,
            ADD CONSTRAINT ck_generation_reconciliation_audit CHECK (
                (reconciled_at IS NULL AND reconciliation_actor IS NULL
                    AND reconciliation_reason IS NULL)
                OR (reconciled_at IS NOT NULL AND reconciliation_actor IS NOT NULL
                    AND reconciliation_reason IS NOT NULL)
            );
    END IF;

    UPDATE outbox_deliveries
    SET delivery_class = CASE
        WHEN operation = 'chatwoot.open_handoff.v1'
            AND provider = 'chatwoot'
            THEN 'provider_setup_context'
        WHEN operation = 'evolution.send_text.v1'
            AND provider = 'evolution'
            AND dedupe_key LIKE 'evolution:chatwoot:%'
            THEN 'human'
        WHEN operation = 'evolution.send_text.v1'
            AND provider = 'evolution'
            AND (
                dedupe_key LIKE 'evolution:inbound:%'
                OR dedupe_key LIKE 'evolution:handoff:%:transition'
            )
            THEN 'legacy_automated'
        ELSE 'legacy_unknown'
    END
    WHERE delivery_class IS NULL OR delivery_class = 'legacy_unknown';

    ALTER TABLE outbox_deliveries
        MODIFY COLUMN delivery_class ENUM(
            'legacy_unknown','legacy_automated','human','automated','provider_setup_context'
        ) NOT NULL DEFAULT 'legacy_unknown';

    -- Ticket 06 era (automated_effect exists): the predecessor key is never
    -- re-created, so a lost successor fails closed instead of converging.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND index_name = 'uq_outbox_turn_revision'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND column_name = 'automated_effect'
    ) THEN
        CREATE UNIQUE INDEX uq_outbox_turn_revision
            ON outbox_deliveries (conversation_turn_id, turn_revision_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND index_name = 'idx_outbox_delivery_class_status'
    ) THEN
        CREATE INDEX idx_outbox_delivery_class_status
            ON outbox_deliveries (delivery_class, status, id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'conversation_turns'
          AND index_name = 'uq_turn_authority_identity'
    ) THEN
        CREATE UNIQUE INDEX uq_turn_authority_identity
            ON conversation_turns (id, agente_id, sessao_id, scheduler_epoch);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'turn_revisions'
          AND index_name = 'uq_revision_authority_identity'
    ) THEN
        CREATE UNIQUE INDEX uq_revision_authority_identity
            ON turn_revisions (id, turn_id, scheduler_epoch);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'handoff_attempts'
          AND index_name = 'uq_handoff_authority_identity'
    ) THEN
        CREATE UNIQUE INDEX uq_handoff_authority_identity
            ON handoff_attempts (id, agente_id, sessao_id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'generation_attempts'
          AND index_name = 'idx_generation_agent_reconciliation'
    ) THEN
        CREATE INDEX idx_generation_agent_reconciliation
            ON generation_attempts (agente_id, accounting_state, reconciled_at, id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'agent_service_lanes'
          AND constraint_name = 'fk_lane_active_turn'
    ) THEN
        ALTER TABLE agent_service_lanes
            ADD CONSTRAINT fk_lane_active_turn
                FOREIGN KEY (active_turn_id) REFERENCES conversation_turns(id)
                ON DELETE RESTRICT,
            ADD CONSTRAINT fk_lane_active_revision
                FOREIGN KEY (active_revision_id) REFERENCES turn_revisions(id)
                ON DELETE RESTRICT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'conversation_turns'
          AND constraint_name = 'fk_turn_current_revision'
    ) AND NOT EXISTS (
        -- Ticket06 era marker (created only after a complete run): from then
        -- on authority FKs are never re-created; drift fails closed below.
        SELECT 1 FROM information_schema.triggers
        WHERE trigger_schema = DATABASE()
          AND trigger_name = 'trg_outbox_authority_immutable_v3'
    ) THEN
        ALTER TABLE conversation_turns
            ADD CONSTRAINT fk_turn_current_revision
                FOREIGN KEY (current_revision_id) REFERENCES turn_revisions(id)
                ON DELETE RESTRICT,
            ADD CONSTRAINT fk_turn_final_revision
                FOREIGN KEY (final_revision_id) REFERENCES turn_revisions(id)
                ON DELETE RESTRICT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND constraint_name = 'fk_outbox_conversation_turn'
    ) AND NOT EXISTS (
        -- Ticket06 era marker (created only after a complete run): from then
        -- on authority FKs are never re-created; drift fails closed below.
        SELECT 1 FROM information_schema.triggers
        WHERE trigger_schema = DATABASE()
          AND trigger_name = 'trg_outbox_authority_immutable_v3'
    ) THEN
        ALTER TABLE outbox_deliveries
            ADD CONSTRAINT fk_outbox_conversation_turn
                FOREIGN KEY (conversation_turn_id) REFERENCES conversation_turns(id)
                ON DELETE RESTRICT,
            ADD CONSTRAINT fk_outbox_turn_revision
                FOREIGN KEY (turn_revision_id) REFERENCES turn_revisions(id)
                ON DELETE RESTRICT;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.referential_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'generation_attempts'
          AND constraint_name = 'fk_generation_consumo'
          AND delete_rule <> 'RESTRICT'
    ) THEN
        ALTER TABLE generation_attempts
            DROP FOREIGN KEY fk_generation_consumo;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.referential_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'generation_attempts'
          AND constraint_name = 'fk_generation_consumo'
    ) THEN
        ALTER TABLE generation_attempts
            ADD CONSTRAINT fk_generation_consumo
                FOREIGN KEY (consumo_llm_id) REFERENCES consumo_llm(id)
                ON DELETE RESTRICT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND constraint_name = 'fk_outbox_turn_authority'
    ) AND NOT EXISTS (
        -- Ticket06 era marker (created only after a complete run): from then
        -- on authority FKs are never re-created; drift fails closed below.
        SELECT 1 FROM information_schema.triggers
        WHERE trigger_schema = DATABASE()
          AND trigger_name = 'trg_outbox_authority_immutable_v3'
    ) THEN
        ALTER TABLE outbox_deliveries
            ADD CONSTRAINT fk_outbox_turn_authority
                FOREIGN KEY (conversation_turn_id, agente_id, sessao_id, scheduler_epoch)
                REFERENCES conversation_turns(id, agente_id, sessao_id, scheduler_epoch)
                ON DELETE RESTRICT,
            ADD CONSTRAINT fk_outbox_revision_authority
                FOREIGN KEY (turn_revision_id, conversation_turn_id, scheduler_epoch)
                REFERENCES turn_revisions(id, turn_id, scheduler_epoch)
                ON DELETE RESTRICT,
            ADD CONSTRAINT fk_outbox_handoff_authority
                FOREIGN KEY (handoff_attempt_id, agente_id, sessao_id)
                REFERENCES handoff_attempts(id, agente_id, sessao_id)
                ON DELETE RESTRICT;
    END IF;

    -- Ticket 06 supersedes this check with ck_outbox_delivery_authority_v2;
    -- in the ticket06 era (automated_effect exists) the predecessor is never
    -- re-created, so a lost successor fails closed instead of converging.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND column_name = 'automated_effect'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_schema = DATABASE()
              AND table_name = 'outbox_deliveries'
              AND constraint_name = 'ck_outbox_delivery_authority'
        ) THEN
            ALTER TABLE outbox_deliveries
                ADD CONSTRAINT ck_outbox_delivery_authority CHECK (
                    (delivery_class = _latin1'automated' AND conversation_turn_id IS NOT NULL
                        AND turn_revision_id IS NOT NULL AND scheduler_epoch IS NOT NULL)
                    OR (delivery_class <> _latin1'automated' AND conversation_turn_id IS NULL
                        AND turn_revision_id IS NULL AND scheduler_epoch IS NULL)
                );
        END IF;

        ALTER TABLE outbox_deliveries
            DROP CHECK ck_outbox_delivery_authority,
            ADD CONSTRAINT ck_outbox_delivery_authority CHECK (
                (delivery_class = _latin1'automated' AND conversation_turn_id IS NOT NULL
                    AND turn_revision_id IS NOT NULL AND scheduler_epoch IS NOT NULL)
                OR (delivery_class <> _latin1'automated' AND conversation_turn_id IS NULL
                    AND turn_revision_id IS NULL AND scheduler_epoch IS NULL)
            );
    END IF;

    -- Ticket 04: requested (allowlisted) model stays in modelo_llm and is the
    -- exact commercial link key; the provider-reported id is kept apart.
    -- Existing rows are never rewritten: their reported value stays NULL.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'consumo_llm'
          AND column_name = 'modelo_llm_reportado'
    ) THEN
        ALTER TABLE consumo_llm
            ADD COLUMN modelo_llm_reportado VARCHAR(255) NULL AFTER modelo_llm;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns AS reported
        JOIN information_schema.columns AS requested
          ON requested.table_schema = reported.table_schema
         AND requested.table_name = reported.table_name
         AND requested.column_name = 'modelo_llm'
        WHERE reported.table_schema = DATABASE()
          AND reported.table_name = 'consumo_llm'
          AND reported.column_name = 'modelo_llm_reportado'
          AND reported.column_type = 'varchar(255)'
          AND reported.is_nullable = 'YES'
          AND reported.column_default IS NULL
          AND reported.collation_name <=> requested.collation_name
          AND reported.ordinal_position = requested.ordinal_position + 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'consumption reported model column requires audited repair';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'llm_usage_reconciliation'
          AND column_name = 'modelo_llm_reportado'
    ) THEN
        ALTER TABLE llm_usage_reconciliation
            ADD COLUMN modelo_llm_reportado VARCHAR(255) NULL AFTER modelo_llm;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns AS reported
        JOIN information_schema.columns AS requested
          ON requested.table_schema = reported.table_schema
         AND requested.table_name = reported.table_name
         AND requested.column_name = 'modelo_llm'
        WHERE reported.table_schema = DATABASE()
          AND reported.table_name = 'llm_usage_reconciliation'
          AND reported.column_name = 'modelo_llm_reportado'
          AND reported.column_type = 'varchar(255)'
          AND reported.is_nullable = 'YES'
          AND reported.column_default IS NULL
          AND reported.collation_name <=> requested.collation_name
          AND reported.ordinal_position = requested.ordinal_position + 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'usage reconciliation reported model column requires audited repair';
    END IF;

    -- Ticket 04: persisted draft origin and bounded typing jitter. Additive
    -- columns are created with their exact shape; any divergent pre-existing
    -- shape fails closed for audited repair instead of being rewritten.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'turn_revisions'
          AND column_name = 'draft_kind'
    ) THEN
        ALTER TABLE turn_revisions
            ADD COLUMN draft_kind ENUM(
                'generated','soft_limit_fallback','technical_fallback'
            ) NULL AFTER needs_handoff;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS draft_kind
        JOIN information_schema.columns AS status_column
          ON status_column.table_schema = draft_kind.table_schema
         AND status_column.table_name = draft_kind.table_name
         AND status_column.column_name = 'status'
        JOIN information_schema.columns AS needs_handoff
          ON needs_handoff.table_schema = draft_kind.table_schema
         AND needs_handoff.table_name = draft_kind.table_name
         AND needs_handoff.column_name = 'needs_handoff'
        WHERE draft_kind.table_schema = DATABASE()
          AND draft_kind.table_name = 'turn_revisions'
          AND draft_kind.column_name = 'draft_kind'
          AND draft_kind.column_type = 'enum(''generated'',''soft_limit_fallback'',''technical_fallback'')'
          AND draft_kind.is_nullable = 'YES'
          AND draft_kind.column_default IS NULL
          AND draft_kind.collation_name <=> status_column.collation_name
          AND draft_kind.ordinal_position = needs_handoff.ordinal_position + 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'turn revision draft kind column requires audited repair';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'turn_revisions'
          AND column_name = 'typing_jitter_ms'
    ) THEN
        ALTER TABLE turn_revisions
            ADD COLUMN typing_jitter_ms SMALLINT NULL AFTER typing_delay_ms;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS typing_jitter
        JOIN information_schema.columns AS typing_delay
          ON typing_delay.table_schema = typing_jitter.table_schema
         AND typing_delay.table_name = typing_jitter.table_name
         AND typing_delay.column_name = 'typing_delay_ms'
        WHERE typing_jitter.table_schema = DATABASE()
          AND typing_jitter.table_name = 'turn_revisions'
          AND typing_jitter.column_name = 'typing_jitter_ms'
          AND typing_jitter.column_type = 'smallint'
          AND typing_jitter.is_nullable = 'YES'
          AND typing_jitter.column_default IS NULL
          AND typing_jitter.collation_name IS NULL
          AND typing_jitter.ordinal_position = typing_delay.ordinal_position + 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'turn revision typing jitter column requires audited repair';
    END IF;
    IF EXISTS (
        SELECT 1 FROM turn_revisions
        WHERE typing_jitter_ms IS NOT NULL
          AND typing_jitter_ms NOT BETWEEN -1000 AND 1000
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'turn revision typing jitter range requires audited repair';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'turn_revisions'
          AND constraint_name = 'ck_revision_typing_jitter'
    ) THEN
        ALTER TABLE turn_revisions
            DROP CHECK ck_revision_typing_jitter,
            ADD CONSTRAINT ck_revision_typing_jitter CHECK (
                typing_jitter_ms IS NULL OR typing_jitter_ms BETWEEN -1000 AND 1000
            );
    ELSE
        ALTER TABLE turn_revisions
            ADD CONSTRAINT ck_revision_typing_jitter CHECK (
                typing_jitter_ms IS NULL OR typing_jitter_ms BETWEEN -1000 AND 1000
            );
    END IF;
END$$

DELIMITER ;

CALL migrate_wpp_ai_bot_v41();
DROP PROCEDURE migrate_wpp_ai_bot_v41;

-- ─── HANDOFF AGENDADO + COBERTURA EXATA DE CONTEXTO TARDIO ─────
CREATE TABLE IF NOT EXISTS scheduled_handoffs (
    handoff_attempt_id BIGINT PRIMARY KEY,
    agente_id INT NOT NULL,
    sessao_id INT NOT NULL,
    conversation_turn_id BIGINT NOT NULL,
    turn_revision_id BIGINT NOT NULL,
    summary_generation_attempt_id BIGINT NULL,
    summary_kind ENUM('generated','soft_limit_fallback','technical_fallback')
        CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_scheduled_handoff_attempt
        FOREIGN KEY (handoff_attempt_id) REFERENCES handoff_attempts(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_scheduled_handoff_summary_attempt
        FOREIGN KEY (summary_generation_attempt_id) REFERENCES generation_attempts(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_scheduled_handoff_attempt_authority
        FOREIGN KEY (handoff_attempt_id, agente_id, sessao_id)
        REFERENCES handoff_attempts(id, agente_id, sessao_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_scheduled_handoff_turn_authority
        FOREIGN KEY (conversation_turn_id, agente_id, sessao_id, scheduler_epoch)
        REFERENCES conversation_turns(id, agente_id, sessao_id, scheduler_epoch)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_scheduled_handoff_revision_authority
        FOREIGN KEY (turn_revision_id, conversation_turn_id, scheduler_epoch)
        REFERENCES turn_revisions(id, turn_id, scheduler_epoch)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT ck_scheduled_handoff_summary_source CHECK (
        (summary_kind = _latin1'generated' AND summary_generation_attempt_id IS NOT NULL)
        OR summary_kind IN (_latin1'soft_limit_fallback',_latin1'technical_fallback')
    ),
    UNIQUE KEY uq_scheduled_handoff_turn (conversation_turn_id),
    UNIQUE KEY uq_scheduled_handoff_revision (turn_revision_id),
    UNIQUE KEY uq_scheduled_handoff_summary_attempt (summary_generation_attempt_id),
    INDEX idx_scheduled_handoff_session (sessao_id, handoff_attempt_id)
);

-- ticket05-schema-boundary: scheduled-handoffs-created

CREATE TABLE IF NOT EXISTS handoff_context_assignments (
    turn_input_membership_id BIGINT PRIMARY KEY,
    handoff_attempt_id BIGINT NOT NULL,
    agente_id INT NOT NULL,
    sessao_id INT NOT NULL,
    coverage_ordinal INT UNSIGNED NOT NULL,
    source_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_handoff_context_membership
        FOREIGN KEY (turn_input_membership_id) REFERENCES turn_input_memberships(id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_handoff_context_attempt_authority
        FOREIGN KEY (handoff_attempt_id, agente_id, sessao_id)
        REFERENCES handoff_attempts(id, agente_id, sessao_id)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT ck_handoff_context_coverage_ordinal CHECK (coverage_ordinal > 0),
    UNIQUE KEY uq_handoff_context_coverage_ordinal
        (handoff_attempt_id, coverage_ordinal),
    INDEX idx_handoff_context_source
        (handoff_attempt_id, source_id, coverage_ordinal)
);

-- ticket05-schema-boundary: handoff-context-assignments-created

DELIMITER $$

DROP PROCEDURE IF EXISTS validate_ticket05_schema$$
CREATE PROCEDURE validate_ticket05_schema()
BEGIN
    DECLARE actual_shape LONGTEXT;
    SET SESSION group_concat_max_len = 65535;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', column_name, column_type, is_nullable,
            COALESCE(column_default, '<NULL>'),
            COALESCE(character_set_name, '<NULL>'),
            COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'scheduled_handoffs';
    -- NULL (absent object / empty set) must fail closed, never compare as unknown.
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'handoff_attempt_id|bigint|NO|<NULL>|<NULL>|<NULL>|;',
        'agente_id|int|NO|<NULL>|<NULL>|<NULL>|;',
        'sessao_id|int|NO|<NULL>|<NULL>|<NULL>|;',
        'conversation_turn_id|bigint|NO|<NULL>|<NULL>|<NULL>|;',
        'turn_revision_id|bigint|NO|<NULL>|<NULL>|<NULL>|;',
        'summary_generation_attempt_id|bigint|YES|<NULL>|<NULL>|<NULL>|;',
        'summary_kind|enum(''generated'',''soft_limit_fallback'',''technical_fallback'')|NO|<NULL>|ascii|ascii_bin|;',
        'scheduler_epoch|char(36)|NO|<NULL>|ascii|ascii_bin|;',
        'created_at|datetime(6)|NO|CURRENT_TIMESTAMP(6)|<NULL>|<NULL>|DEFAULT_GENERATED'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket05 scheduled handoff column shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', column_name, column_type, is_nullable,
            COALESCE(column_default, '<NULL>'),
            COALESCE(character_set_name, '<NULL>'),
            COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'handoff_context_assignments';
    -- NULL (absent object / empty set) must fail closed, never compare as unknown.
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'turn_input_membership_id|bigint|NO|<NULL>|<NULL>|<NULL>|;',
        'handoff_attempt_id|bigint|NO|<NULL>|<NULL>|<NULL>|;',
        'agente_id|int|NO|<NULL>|<NULL>|<NULL>|;',
        'sessao_id|int|NO|<NULL>|<NULL>|<NULL>|;',
        'coverage_ordinal|int unsigned|NO|<NULL>|<NULL>|<NULL>|;',
        'source_id|varchar(255)|NO|<NULL>|ascii|ascii_bin|;',
        'created_at|datetime(6)|NO|CURRENT_TIMESTAMP(6)|<NULL>|<NULL>|DEFAULT_GENERATED'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket05 handoff context column shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', table_name, index_name, non_unique, seq_in_index, column_name,
            COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'),
            index_type, is_visible, COALESCE(expression, '<NULL>'))
        ORDER BY table_name, index_name, seq_in_index SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name IN ('scheduled_handoffs','handoff_context_assignments');
    -- NULL (absent object / empty set) must fail closed, never compare as unknown.
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'handoff_context_assignments|fk_handoff_context_attempt_authority|1|1|handoff_attempt_id|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|fk_handoff_context_attempt_authority|1|2|agente_id|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|fk_handoff_context_attempt_authority|1|3|sessao_id|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|idx_handoff_context_source|1|1|handoff_attempt_id|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|idx_handoff_context_source|1|2|source_id|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|idx_handoff_context_source|1|3|coverage_ordinal|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|PRIMARY|0|1|turn_input_membership_id|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|uq_handoff_context_coverage_ordinal|0|1|handoff_attempt_id|A|<NULL>|BTREE|YES|<NULL>;',
        'handoff_context_assignments|uq_handoff_context_coverage_ordinal|0|2|coverage_ordinal|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_attempt_authority|1|1|handoff_attempt_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_attempt_authority|1|2|agente_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_attempt_authority|1|3|sessao_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_revision_authority|1|1|turn_revision_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_revision_authority|1|2|conversation_turn_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_revision_authority|1|3|scheduler_epoch|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|1|1|conversation_turn_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|1|2|agente_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|1|3|sessao_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|1|4|scheduler_epoch|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|idx_scheduled_handoff_session|1|1|sessao_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|idx_scheduled_handoff_session|1|2|handoff_attempt_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|PRIMARY|0|1|handoff_attempt_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|uq_scheduled_handoff_revision|0|1|turn_revision_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|uq_scheduled_handoff_summary_attempt|0|1|summary_generation_attempt_id|A|<NULL>|BTREE|YES|<NULL>;',
        'scheduled_handoffs|uq_scheduled_handoff_turn|0|1|conversation_turn_id|A|<NULL>|BTREE|YES|<NULL>'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket05 handoff index shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', k.table_name, k.constraint_name, k.ordinal_position,
            k.column_name, k.referenced_table_name, k.referenced_column_name,
            r.update_rule, r.delete_rule)
        ORDER BY k.table_name, k.constraint_name, k.ordinal_position SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.key_column_usage AS k
    JOIN information_schema.referential_constraints AS r
      ON r.constraint_schema = k.constraint_schema
     AND r.constraint_name = k.constraint_name
     AND r.table_name = k.table_name
    WHERE k.constraint_schema = DATABASE()
      AND k.table_name IN ('scheduled_handoffs','handoff_context_assignments')
      AND k.referenced_table_schema = DATABASE();
    -- NULL (absent object / empty set) must fail closed, never compare as unknown.
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'handoff_context_assignments|fk_handoff_context_attempt_authority|1|handoff_attempt_id|handoff_attempts|id|RESTRICT|RESTRICT;',
        'handoff_context_assignments|fk_handoff_context_attempt_authority|2|agente_id|handoff_attempts|agente_id|RESTRICT|RESTRICT;',
        'handoff_context_assignments|fk_handoff_context_attempt_authority|3|sessao_id|handoff_attempts|sessao_id|RESTRICT|RESTRICT;',
        'handoff_context_assignments|fk_handoff_context_membership|1|turn_input_membership_id|turn_input_memberships|id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_attempt|1|handoff_attempt_id|handoff_attempts|id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_attempt_authority|1|handoff_attempt_id|handoff_attempts|id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_attempt_authority|2|agente_id|handoff_attempts|agente_id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_attempt_authority|3|sessao_id|handoff_attempts|sessao_id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_revision_authority|1|turn_revision_id|turn_revisions|id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_revision_authority|2|conversation_turn_id|turn_revisions|turn_id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_revision_authority|3|scheduler_epoch|turn_revisions|scheduler_epoch|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_summary_attempt|1|summary_generation_attempt_id|generation_attempts|id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|1|conversation_turn_id|conversation_turns|id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|2|agente_id|conversation_turns|agente_id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|3|sessao_id|conversation_turns|sessao_id|RESTRICT|RESTRICT;',
        'scheduled_handoffs|fk_scheduled_handoff_turn_authority|4|scheduler_epoch|conversation_turns|scheduler_epoch|RESTRICT|RESTRICT'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket05 handoff foreign key shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', tc.table_name, tc.constraint_name, tc.enforced,
            SHA2(cc.check_clause, 256))
        ORDER BY tc.table_name, tc.constraint_name SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema
     AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE()
      AND tc.table_name IN ('scheduled_handoffs','handoff_context_assignments');
    -- NULL (absent object / empty set) must fail closed, never compare as unknown.
    -- FU-3: closed, byte-exact allowlist. MySQL 8.0 table maintenance
    -- re-serializes this legacy ascii-column CHECK from _latin1 to _ascii.
    -- Both full shapes are audited; no SQL/literal/whitespace normalization,
    -- no CHECK replacement, and no schema-version bump or data rewrite.
    IF actual_shape IS NULL OR actual_shape NOT IN (
        CONCAT(
            'handoff_context_assignments|ck_handoff_context_coverage_ordinal|YES|',
            '3e4184d44f594a95d742a26eda7b2c50a657e9f2f73024c8cd6a6eff53ea8a5d;',
            'scheduled_handoffs|ck_scheduled_handoff_summary_source|YES|',
            '12213a6acf680656113841970e1ec05c2a3ab972725a469af6936615af3c0c9a'
        ),
        CONCAT(
            'handoff_context_assignments|ck_handoff_context_coverage_ordinal|YES|',
            '3e4184d44f594a95d742a26eda7b2c50a657e9f2f73024c8cd6a6eff53ea8a5d;',
            'scheduled_handoffs|ck_scheduled_handoff_summary_source|YES|',
            '0308f61fbc7eacfab2b60cc48797863b0be253ca9fc7afae7aea361fe4097e13'
        )
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket05 handoff check shape requires audited repair';
    END IF;
END$$

CALL validate_ticket05_schema()$$
DROP PROCEDURE validate_ticket05_schema$$

-- ticket05-schema-boundary: exact-schema-validated

-- ─── TICKET 06: EFFECT IDENTITY FOR AUTOMATED DELIVERY ───────
-- The new key is created before the predecessor is dropped. Interrupted runs
-- therefore retain at least one uniqueness guard at every DDL boundary.
DELIMITER $$

DROP PROCEDURE IF EXISTS migrate_ticket06_automated_effect$$
CREATE PROCEDURE migrate_ticket06_automated_effect()
BEGIN
    DECLARE actual_shape LONGTEXT;
    SET SESSION group_concat_max_len = 65535;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'outbox_deliveries'
          AND column_name = 'automated_effect'
    ) THEN
        ALTER TABLE outbox_deliveries
            ADD COLUMN automated_effect ENUM('reply','handoff_transition')
                CHARACTER SET ascii COLLATE ascii_bin NULL AFTER delivery_class;
    END IF;
    -- ticket06-schema-boundary: automated-effect-column-created

    SELECT CONCAT_WS('|', column_type, is_nullable,
                     COALESCE(column_default, '<NULL>'),
                     COALESCE(character_set_name, '<NULL>'),
                     COALESCE(collation_name, '<NULL>'), ordinal_position,
                     COALESCE(extra, ''))
      INTO actual_shape
      FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = 'outbox_deliveries'
       AND column_name = 'automated_effect';
    IF actual_shape IS NULL
       OR actual_shape <> CONCAT(
            'enum(''reply'',''handoff_transition'')|YES|<NULL>|ascii|ascii_bin|',
            (SELECT ordinal_position + 1 FROM information_schema.columns
              WHERE table_schema = DATABASE()
                AND table_name = 'outbox_deliveries'
                AND column_name = 'delivery_class'), '|'
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket06 automated effect column shape requires audited repair';
    END IF;

    -- Only ticket05 rows have automated authority before this migration. The
    -- exact scheduled-handoff relationship proves the immutable effect; no
    -- payload or mutable status is consulted.
    UPDATE outbox_deliveries AS delivery
    JOIN scheduled_handoffs AS scheduled
      ON scheduled.handoff_attempt_id = delivery.handoff_attempt_id
     AND scheduled.conversation_turn_id = delivery.conversation_turn_id
     AND scheduled.turn_revision_id = delivery.turn_revision_id
     AND scheduled.scheduler_epoch = delivery.scheduler_epoch
       SET delivery.automated_effect = 'handoff_transition'
     WHERE delivery.delivery_class = 'automated'
       AND delivery.automated_effect IS NULL;

    IF EXISTS (
        SELECT 1 FROM outbox_deliveries
         WHERE (delivery_class = 'automated' AND automated_effect IS NULL)
            OR (delivery_class <> 'automated' AND automated_effect IS NOT NULL)
         LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket06 automated effect classification requires audited repair';
    END IF;
    -- ticket06-schema-boundary: existing-transitions-classified

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name = 'outbox_deliveries'
           AND index_name = 'uq_outbox_turn_revision_effect'
    ) THEN
        -- Only an upgrade (predecessor still present) may create it; a
        -- migrated schema that lost its successor needs audited repair.
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.statistics
             WHERE table_schema = DATABASE()
               AND table_name = 'outbox_deliveries'
               AND index_name = 'uq_outbox_turn_revision'
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'ticket06 automated effect index shape requires audited repair';
        END IF;
        CREATE UNIQUE INDEX uq_outbox_turn_revision_effect
            ON outbox_deliveries
                (conversation_turn_id, turn_revision_id, automated_effect);
    END IF;
    -- ticket06-schema-boundary: effect-index-created

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', non_unique, seq_in_index, column_name, collation,
                  COALESCE(sub_part, '<NULL>'), index_type, is_visible)
        ORDER BY seq_in_index SEPARATOR ';'
    ) INTO actual_shape
      FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND table_name = 'outbox_deliveries'
       AND index_name = 'uq_outbox_turn_revision_effect';
    -- NULL (absent object / empty set) must fail closed, never compare as unknown.
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '0|1|conversation_turn_id|A|<NULL>|BTREE|YES;',
        '0|2|turn_revision_id|A|<NULL>|BTREE|YES;',
        '0|3|automated_effect|A|<NULL>|BTREE|YES'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket06 automated effect index shape requires audited repair';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name = 'outbox_deliveries'
           AND index_name = 'uq_outbox_turn_revision'
    ) THEN
        ALTER TABLE outbox_deliveries DROP INDEX uq_outbox_turn_revision;
    END IF;
    -- ticket06-schema-boundary: predecessor-index-dropped

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
         WHERE constraint_schema = DATABASE()
           AND table_name = 'outbox_deliveries'
           AND constraint_name = 'ck_outbox_delivery_authority_v2'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
             WHERE constraint_schema = DATABASE()
               AND table_name = 'outbox_deliveries'
               AND constraint_name = 'ck_outbox_delivery_authority'
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'ticket06 automated authority check shape requires audited repair';
        END IF;
        ALTER TABLE outbox_deliveries
            -- Explicit introducers: inside a routine the literal charset would
            -- otherwise follow the client that created it (latin1 under a
            -- bare docker exec), changing the exact clause digest.
            ADD CONSTRAINT ck_outbox_delivery_authority_v2 CHECK (
                (delivery_class = _utf8mb4'automated' AND automated_effect IS NOT NULL
                    AND conversation_turn_id IS NOT NULL
                    AND turn_revision_id IS NOT NULL
                    AND scheduler_epoch IS NOT NULL)
                OR (delivery_class <> _utf8mb4'automated' AND automated_effect IS NULL
                    AND conversation_turn_id IS NULL
                    AND turn_revision_id IS NULL
                    AND scheduler_epoch IS NULL)
            );
    END IF;
    -- ticket06-schema-boundary: authority-check-v2-created

    -- Validate the successor check before the predecessor guard is dropped.
    SELECT CONCAT_WS('|', tc.enforced, SHA2(cc.check_clause, 256))
      INTO actual_shape
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.check_constraints AS cc
        ON cc.constraint_schema = tc.constraint_schema
       AND cc.constraint_name = tc.constraint_name
     WHERE tc.constraint_schema = DATABASE()
       AND tc.table_name = 'outbox_deliveries'
       AND tc.constraint_name = 'ck_outbox_delivery_authority_v2';
    IF actual_shape IS NULL
       OR actual_shape <> CONCAT(
            'YES|',
            '0ed7c0fbe73cff6435080978b49dd5f511c41b32d5d1d7051c914b5b0983f96a'
       ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'ticket06 automated authority check shape requires audited repair';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.table_constraints
         WHERE constraint_schema = DATABASE()
           AND table_name = 'outbox_deliveries'
           AND constraint_name = 'ck_outbox_delivery_authority'
    ) THEN
        ALTER TABLE outbox_deliveries DROP CHECK ck_outbox_delivery_authority;
    END IF;
    -- ticket06-schema-boundary: authority-check-v1-dropped

END$$

CALL migrate_ticket06_automated_effect()$$
DROP PROCEDURE migrate_ticket06_automated_effect$$

-- Scheduler authority foreign keys (ticket02/03) are validated by exact shape
-- on every run: a dropped or altered FK fails closed with a named error
-- instead of being silently absent (existence was keyed on one name only).
DROP PROCEDURE IF EXISTS validate_scheduler_authority_foreign_keys$$
CREATE PROCEDURE validate_scheduler_authority_foreign_keys()
BEGIN
    DECLARE actual_shape LONGTEXT;
    SET SESSION group_concat_max_len = 65535;
    SELECT GROUP_CONCAT(
        CONCAT_WS('|', k.table_name, k.constraint_name, k.ordinal_position,
                  k.column_name, k.referenced_table_name, k.referenced_column_name,
                  r.update_rule, r.delete_rule)
        ORDER BY k.table_name, k.constraint_name, k.ordinal_position SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.key_column_usage AS k
    JOIN information_schema.referential_constraints AS r
      ON r.constraint_schema = k.constraint_schema
     AND r.constraint_name = k.constraint_name
     AND r.table_name = k.table_name
    WHERE k.constraint_schema = DATABASE()
      AND k.referenced_table_schema = DATABASE()
      AND k.constraint_name IN (
          'fk_outbox_turn_authority', 'fk_outbox_revision_authority',
          'fk_outbox_handoff_authority', 'fk_outbox_conversation_turn',
          'fk_outbox_turn_revision', 'fk_turn_current_revision',
          'fk_turn_final_revision');
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'conversation_turns|fk_turn_current_revision|1|current_revision_id|turn_revisions|id|NO ACTION|RESTRICT;',
        'conversation_turns|fk_turn_final_revision|1|final_revision_id|turn_revisions|id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_conversation_turn|1|conversation_turn_id|conversation_turns|id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_handoff_authority|1|handoff_attempt_id|handoff_attempts|id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_handoff_authority|2|agente_id|handoff_attempts|agente_id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_handoff_authority|3|sessao_id|handoff_attempts|sessao_id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_revision_authority|1|turn_revision_id|turn_revisions|id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_revision_authority|2|conversation_turn_id|turn_revisions|turn_id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_revision_authority|3|scheduler_epoch|turn_revisions|scheduler_epoch|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_turn_authority|1|conversation_turn_id|conversation_turns|id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_turn_authority|2|agente_id|conversation_turns|agente_id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_turn_authority|3|sessao_id|conversation_turns|sessao_id|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_turn_authority|4|scheduler_epoch|conversation_turns|scheduler_epoch|NO ACTION|RESTRICT;',
        'outbox_deliveries|fk_outbox_turn_revision|1|turn_revision_id|turn_revisions|id|NO ACTION|RESTRICT'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'scheduler authority foreign key shape requires audited repair';
    END IF;
END$$

CALL validate_scheduler_authority_foreign_keys()$$
DROP PROCEDURE validate_scheduler_authority_foreign_keys$$

-- Ticket07 (B): one commercial ledger row backs at most one generation attempt.
-- NULL stays allowed (unlinked attempts); provider_generation_id NULL is never a
-- wildcard. A missing key is created only when no ledger row is already reused
-- (reuse fails closed, nothing rewritten); a divergent key fails closed.
-- ticket07-schema-boundary: before-consumption-link
DROP PROCEDURE IF EXISTS migrate_ticket07_consumption_link$$
CREATE PROCEDURE migrate_ticket07_consumption_link()
BEGIN
    DECLARE actual_shape TEXT DEFAULT NULL;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name = 'generation_attempts'
           AND index_name = 'uq_generation_consumo_link'
    ) THEN
        IF EXISTS (
            SELECT 1 FROM generation_attempts
             WHERE consumo_llm_id IS NOT NULL
             GROUP BY consumo_llm_id
            HAVING COUNT(*) > 1
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'generation consumption link reuse requires audited repair';
        END IF;
        CREATE UNIQUE INDEX uq_generation_consumo_link
            ON generation_attempts (consumo_llm_id);
    END IF;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', non_unique, seq_in_index, column_name, collation,
                  COALESCE(sub_part, '<NULL>'), index_type, is_visible)
        ORDER BY seq_in_index SEPARATOR ';'
    ) INTO actual_shape
      FROM information_schema.statistics
     WHERE table_schema = DATABASE()
       AND table_name = 'generation_attempts'
       AND index_name = 'uq_generation_consumo_link';
    IF actual_shape IS NULL
       OR actual_shape <> '0|1|consumo_llm_id|A|<NULL>|BTREE|YES' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation consumption link index shape requires audited repair';
    END IF;
END$$

CALL migrate_ticket07_consumption_link()$$
-- ticket07-schema-boundary: consumption-link-index-created
DROP PROCEDURE migrate_ticket07_consumption_link$$

CREATE TRIGGER IF NOT EXISTS trg_inbound_event_disposition_immutable_v2
BEFORE UPDATE ON inbound_events
FOR EACH ROW
BEGIN
    IF NOT (OLD.receipt_disposition <=> NEW.receipt_disposition) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'inbound receipt disposition is immutable';
    END IF;
END$$

-- ticket03-trigger-boundary: inbound-disposition-v2-created
CREATE TRIGGER IF NOT EXISTS trg_outbox_authority_immutable_v3
BEFORE UPDATE ON outbox_deliveries
FOR EACH ROW
BEGIN
    IF NOT (OLD.delivery_class <=> NEW.delivery_class)
       OR NOT (OLD.automated_effect <=> NEW.automated_effect)
       OR NOT (OLD.scheduler_epoch <=> NEW.scheduler_epoch)
       OR NOT (OLD.conversation_turn_id <=> NEW.conversation_turn_id)
       OR NOT (OLD.turn_revision_id <=> NEW.turn_revision_id) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'outbox delivery authority is immutable';
    END IF;
    IF (OLD.delivery_class = 'automated' OR NEW.delivery_class = 'automated') AND (
        NOT (OLD.cutover_epoch <=> NEW.cutover_epoch)
        OR NOT (OLD.agente_id <=> NEW.agente_id)
        OR NOT (OLD.sessao_id <=> NEW.sessao_id)
        OR NOT (OLD.handoff_attempt_id <=> NEW.handoff_attempt_id)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'automated delivery ownership is immutable';
    END IF;
END$$

-- ticket06-trigger-boundary: outbox-authority-v3-created
CREATE TRIGGER IF NOT EXISTS trg_turn_revision_seal_immutable_v2
BEFORE UPDATE ON turn_revisions
FOR EACH ROW
BEGIN
    IF OLD.sealed_at IS NOT NULL AND (
        NOT (OLD.sealed_at <=> NEW.sealed_at)
        OR NOT (OLD.sealed_first_input_sequence <=> NEW.sealed_first_input_sequence)
        OR NOT (OLD.sealed_last_input_sequence <=> NEW.sealed_last_input_sequence)
        OR NOT (OLD.sealed_input_count <=> NEW.sealed_input_count)
        OR NOT (OLD.sealed_input_bytes <=> NEW.sealed_input_bytes)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'sealed revision input range is immutable';
    END IF;
END$$

-- ticket03-trigger-boundary: turn-revision-v2-created
CREATE TRIGGER IF NOT EXISTS trg_turn_input_identity_immutable_v2
BEFORE UPDATE ON turn_input_memberships
FOR EACH ROW
BEGIN
    IF NOT (OLD.turn_id <=> NEW.turn_id)
       OR NOT (OLD.sessao_id <=> NEW.sessao_id)
       OR NOT (OLD.mensagem_id <=> NEW.mensagem_id)
       OR NOT (OLD.inbound_event_id <=> NEW.inbound_event_id)
       OR NOT (OLD.input_sequence <=> NEW.input_sequence)
       OR NOT (OLD.content_bytes <=> NEW.content_bytes) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'turn input identity is immutable';
    END IF;
END$$

-- ticket03-trigger-boundary: turn-input-v2-created
CREATE TRIGGER IF NOT EXISTS trg_conversation_turn_identity_immutable_v2
BEFORE UPDATE ON conversation_turns
FOR EACH ROW
BEGIN
    IF NOT (OLD.turn_key <=> NEW.turn_key)
       OR NOT (OLD.agente_id <=> NEW.agente_id)
       OR NOT (OLD.sessao_id <=> NEW.sessao_id)
       OR NOT (OLD.scheduler_epoch <=> NEW.scheduler_epoch)
       OR NOT (OLD.arrival_sequence <=> NEW.arrival_sequence)
       OR NEW.input_version < OLD.input_version
       OR NEW.refresh_count < OLD.refresh_count THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'conversation turn identity is immutable';
    END IF;
END$$

-- ticket03-trigger-boundary: conversation-turn-v2-created
CREATE TRIGGER IF NOT EXISTS trg_generation_attempt_identity_immutable_v2
BEFORE UPDATE ON generation_attempts
FOR EACH ROW
BEGIN
    IF NOT (OLD.attempt_key <=> NEW.attempt_key)
       OR NOT (OLD.agente_id <=> NEW.agente_id)
       OR NOT (OLD.sessao_id <=> NEW.sessao_id)
       OR NOT (OLD.turn_id <=> NEW.turn_id)
       OR NOT (OLD.revision_id <=> NEW.revision_id)
       OR NOT (OLD.scheduler_epoch <=> NEW.scheduler_epoch)
       OR NOT (OLD.lane_owner_token <=> NEW.lane_owner_token)
       OR NOT (OLD.attempt_type <=> NEW.attempt_type)
       OR NOT (OLD.attempt_ordinal <=> NEW.attempt_ordinal)
       OR NOT (OLD.modelo_llm <=> NEW.modelo_llm) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation attempt identity is immutable';
    END IF;
    IF OLD.accounting_drain_until IS NOT NULL
       AND NOT (OLD.accounting_drain_until <=> NEW.accounting_drain_until) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation accounting drain deadline is immutable';
    END IF;
    IF OLD.accounting_drain_until IS NULL
       AND NEW.accounting_drain_until IS NOT NULL
       AND (OLD.outcome <> 'started' OR NEW.outcome <> 'started') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation accounting drain requires started outcome';
    END IF;
    IF OLD.reconciled_at IS NOT NULL AND (
        NOT (OLD.reconciled_at <=> NEW.reconciled_at)
        OR NOT (OLD.reconciliation_actor <=> NEW.reconciliation_actor)
        OR NOT (OLD.reconciliation_reason <=> NEW.reconciliation_reason)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation reconciliation audit is immutable';
    END IF;
    IF OLD.outcome <> 'started' AND NOT (
        OLD.accounting_state = 'reconciliation_required'
        AND OLD.reconciled_at IS NULL
        AND NEW.reconciled_at IS NOT NULL
        AND NEW.reconciliation_actor IS NOT NULL
        AND NEW.reconciliation_reason IS NOT NULL
        AND (
            (OLD.outcome = 'outcome_unknown'
                AND NEW.outcome IN ('succeeded','failed'))
            OR NEW.outcome = OLD.outcome
        )
    ) AND (
        NOT (OLD.outcome <=> NEW.outcome)
        OR NOT (OLD.accounting_state <=> NEW.accounting_state)
        OR NOT (OLD.provider_generation_id <=> NEW.provider_generation_id)
        OR NOT (OLD.tokens_input <=> NEW.tokens_input)
        OR NOT (OLD.tokens_output <=> NEW.tokens_output)
        OR NOT (OLD.tokens_reasoning <=> NEW.tokens_reasoning)
        OR NOT (OLD.tokens_cache_read <=> NEW.tokens_cache_read)
        OR NOT (OLD.tokens_cache_write <=> NEW.tokens_cache_write)
        OR NOT (OLD.custo_creditos <=> NEW.custo_creditos)
        OR NOT (OLD.consumo_llm_id <=> NEW.consumo_llm_id)
        OR NOT (OLD.safe_error_code <=> NEW.safe_error_code)
        OR NOT (OLD.outcome_recorded_at <=> NEW.outcome_recorded_at)
        OR NOT (OLD.reconciled_at <=> NEW.reconciled_at)
        OR NOT (OLD.reconciliation_actor <=> NEW.reconciliation_actor)
        OR NOT (OLD.reconciliation_reason <=> NEW.reconciliation_reason)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation attempt outcome is immutable';
    END IF;
    IF OLD.outcome <> 'started'
       AND OLD.accounting_state = 'reconciliation_required'
       AND OLD.reconciled_at IS NULL
       AND NEW.reconciled_at IS NOT NULL THEN
        IF NOT (OLD.accounting_state <=> NEW.accounting_state)
           OR NOT (OLD.outcome_recorded_at <=> NEW.outcome_recorded_at)
           OR NOT (OLD.safe_error_code <=> NEW.safe_error_code) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'generation reconciliation cannot rewrite audit history';
        END IF;
        IF OLD.outcome <> 'outcome_unknown' AND (
            NOT (OLD.outcome <=> NEW.outcome)
            OR NOT (OLD.provider_generation_id <=> NEW.provider_generation_id)
            OR NOT (OLD.tokens_input <=> NEW.tokens_input)
            OR NOT (OLD.tokens_output <=> NEW.tokens_output)
            OR NOT (OLD.tokens_reasoning <=> NEW.tokens_reasoning)
            OR NOT (OLD.tokens_cache_read <=> NEW.tokens_cache_read)
            OR NOT (OLD.tokens_cache_write <=> NEW.tokens_cache_write)
            OR NOT (OLD.custo_creditos <=> NEW.custo_creditos)
            OR (
                NOT (OLD.consumo_llm_id <=> NEW.consumo_llm_id)
                AND NOT (
                    OLD.outcome = 'succeeded'
                    AND OLD.consumo_llm_id IS NULL
                    AND NEW.consumo_llm_id IS NOT NULL
                )
            )
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'known generation outcome evidence is immutable';
        END IF;
    END IF;
    IF OLD.outcome = 'started' AND NEW.outcome = 'started' AND (
        NOT (OLD.accounting_state <=> NEW.accounting_state)
        OR NOT (OLD.provider_generation_id <=> NEW.provider_generation_id)
        OR NOT (OLD.tokens_input <=> NEW.tokens_input)
        OR NOT (OLD.tokens_output <=> NEW.tokens_output)
        OR NOT (OLD.tokens_reasoning <=> NEW.tokens_reasoning)
        OR NOT (OLD.tokens_cache_read <=> NEW.tokens_cache_read)
        OR NOT (OLD.tokens_cache_write <=> NEW.tokens_cache_write)
        OR NOT (OLD.custo_creditos <=> NEW.custo_creditos)
        OR NOT (OLD.consumo_llm_id <=> NEW.consumo_llm_id)
        OR NOT (OLD.safe_error_code <=> NEW.safe_error_code)
        OR NOT (OLD.outcome_recorded_at <=> NEW.outcome_recorded_at)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation attempt outcome fields require terminal transition';
    END IF;
    IF NEW.consumo_llm_id IS NOT NULL AND NOT EXISTS (
        SELECT 1
        FROM consumo_llm AS consumo
        WHERE consumo.id = NEW.consumo_llm_id
          AND consumo.agente_id = NEW.agente_id
          AND consumo.sessao_id = NEW.sessao_id
          AND consumo.modelo_llm = NEW.modelo_llm
          AND (
              (NEW.attempt_type = 'response' AND consumo.tipo_chamada = 'resposta')
              OR (NEW.attempt_type = 'summary' AND consumo.tipo_chamada = 'resumo')
          )
          AND (consumo.provider_generation_id <=> NEW.provider_generation_id)
          AND (consumo.tokens_input <=> NEW.tokens_input)
          AND (consumo.tokens_output <=> NEW.tokens_output)
          AND (consumo.tokens_reasoning <=> NEW.tokens_reasoning)
          AND (consumo.tokens_cache_read <=> NEW.tokens_cache_read)
          AND (consumo.tokens_cache_write <=> NEW.tokens_cache_write)
          AND (consumo.custo_creditos <=> NEW.custo_creditos)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation consumption link does not match evidence';
    END IF;
    IF NEW.outcome = 'succeeded'
       AND (NEW.accounting_state = 'recorded' OR NEW.reconciled_at IS NOT NULL)
       AND NEW.consumo_llm_id IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'accounted generation success requires consumption link';
    END IF;
    IF NEW.outcome = 'succeeded'
       AND NEW.accounting_state NOT IN ('recorded','reconciliation_required') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generation success requires durable accounting state';
    END IF;
    IF NEW.outcome = 'outcome_unknown'
       AND NEW.accounting_state <> 'reconciliation_required' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'unknown generation outcome requires quarantine';
    END IF;
END$$

-- ticket03-trigger-boundary: generation-attempt-v2-created
CREATE TRIGGER IF NOT EXISTS trg_turn_revision_draft_immutable_v2
BEFORE UPDATE ON turn_revisions
FOR EACH ROW
BEGIN
    IF OLD.generation_completed_at IS NOT NULL AND (
        NOT (OLD.generation_completed_at <=> NEW.generation_completed_at)
        OR NOT (OLD.draft_kind <=> NEW.draft_kind)
        OR NOT (OLD.draft_text_bytes <=> NEW.draft_text_bytes)
        OR NOT (OLD.needs_handoff <=> NEW.needs_handoff)
        OR (NEW.draft_text IS NOT NULL AND NOT (OLD.draft_text <=> NEW.draft_text))
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generated revision draft is immutable';
    END IF;
    IF NEW.generation_completed_at IS NOT NULL AND (
        NEW.draft_kind IS NULL
        OR NEW.draft_text_bytes IS NULL
        OR NEW.needs_handoff IS NULL
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'generated revision draft is incomplete';
    END IF;
    IF OLD.typing_deadline_at IS NOT NULL AND (
        NOT (OLD.typing_deadline_at <=> NEW.typing_deadline_at)
        OR NOT (OLD.typing_delay_ms <=> NEW.typing_delay_ms)
        OR NOT (OLD.typing_jitter_ms <=> NEW.typing_jitter_ms)
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'revision typing schedule is immutable';
    END IF;
    IF NEW.typing_deadline_at IS NOT NULL AND (
        NEW.generation_completed_at IS NULL
        OR NEW.typing_delay_ms IS NULL
        OR NEW.typing_jitter_ms IS NULL
        OR NEW.typing_deadline_at <> NEW.generation_completed_at
            + INTERVAL (NEW.typing_delay_ms * 1000) MICROSECOND
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'revision typing schedule is inconsistent';
    END IF;
    IF OLD.status IN ('superseded','cancelled','dead')
       AND NOT (OLD.status <=> NEW.status) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'terminal revision status is immutable';
    END IF;
    IF OLD.status IN ('final_candidate','admitted')
       AND NEW.status IN ('collecting','generating','draft','typing','superseded') THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'sealed final candidate cannot be reopened';
    END IF;
    IF OLD.status = 'admitted'
       AND NOT (OLD.status <=> NEW.status) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'admitted revision status is immutable';
    END IF;
    IF NEW.status IN ('typing','final_candidate')
       AND NOT (OLD.status <=> NEW.status)
       AND (NEW.typing_deadline_at IS NULL OR NEW.draft_kind IS NULL) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'typing revision requires a persisted draft schedule';
    END IF;
    IF NEW.status = 'superseded' AND NEW.superseded_reason IS NULL THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'superseded revision requires a reason';
    END IF;
END$$

-- ticket04-trigger-boundary: turn-revision-draft-v2-created
CREATE TRIGGER IF NOT EXISTS trg_scheduled_handoff_immutable_v1
BEFORE UPDATE ON scheduled_handoffs
FOR EACH ROW
BEGIN
    IF NOT (OLD.handoff_attempt_id <=> NEW.handoff_attempt_id)
       OR NOT (OLD.agente_id <=> NEW.agente_id)
       OR NOT (OLD.sessao_id <=> NEW.sessao_id)
       OR NOT (OLD.conversation_turn_id <=> NEW.conversation_turn_id)
       OR NOT (OLD.turn_revision_id <=> NEW.turn_revision_id)
       OR NOT (OLD.summary_generation_attempt_id <=> NEW.summary_generation_attempt_id)
       OR NOT (OLD.summary_kind <=> NEW.summary_kind)
       OR NOT (OLD.scheduler_epoch <=> NEW.scheduler_epoch)
       OR NOT (OLD.created_at <=> NEW.created_at) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'scheduled handoff authority is immutable';
    END IF;
END$$

-- ticket05-trigger-boundary: scheduled-handoff-v1-created
CREATE TRIGGER IF NOT EXISTS trg_handoff_context_assignment_immutable_v1
BEFORE UPDATE ON handoff_context_assignments
FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'handoff context assignment is immutable';
END$$

-- ticket05-trigger-boundary: handoff-context-assignment-v1-created
CREATE TRIGGER IF NOT EXISTS trg_turn_input_handoff_state_v1
BEFORE UPDATE ON turn_input_memberships
FOR EACH ROW
BEGIN
    IF NOT (
        (OLD.membership_state = 'active'
            AND NEW.membership_state IN ('active','sealed'))
        OR (OLD.membership_state = 'sealed'
            AND NEW.membership_state = 'sealed')
        OR (OLD.membership_state = 'post_handoff_pending'
            AND NEW.membership_state = 'post_handoff_pending'
            AND NEW.assigned_revision_id IS NULL)
        OR (
            OLD.membership_state = 'active'
            AND NEW.membership_state = 'post_handoff_pending'
            AND NEW.assigned_revision_id IS NULL
            AND EXISTS (
                SELECT 1
                FROM handoff_context_assignments AS assignment
                WHERE assignment.turn_input_membership_id = NEW.id
                  AND assignment.sessao_id = NEW.sessao_id
            )
        )
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'turn input membership transition is not allowed';
    END IF;
END$$

-- ticket05-trigger-boundary: turn-input-handoff-state-v1-created
CREATE TRIGGER IF NOT EXISTS trg_outbox_context_payload_immutable_v1
BEFORE UPDATE ON outbox_deliveries
FOR EACH ROW
BEGIN
    IF OLD.operation = 'chatwoot.add_private_note.v1'
       OR NEW.operation = 'chatwoot.add_private_note.v1' THEN
        IF NOT (OLD.operation <=> NEW.operation)
           OR NOT (OLD.payload_version <=> NEW.payload_version)
           OR NOT (OLD.delivery_class <=> NEW.delivery_class)
           OR NOT (OLD.handoff_attempt_id <=> NEW.handoff_attempt_id)
           OR NOT (OLD.depends_on_delivery_id <=> NEW.depends_on_delivery_id)
           OR NOT (OLD.dedupe_key <=> NEW.dedupe_key)
           OR NOT (OLD.ordering_key <=> NEW.ordering_key)
           OR NOT (OLD.stream_sequence <=> NEW.stream_sequence)
           OR (
               NOT (OLD.payload <=> NEW.payload)
               AND NOT (
                   OLD.status IN ('delivered','dead','cancelled')
                   AND JSON_LENGTH(NEW.payload) = 0
               )
           ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'handoff context delivery payload is immutable';
        END IF;
    END IF;
END$$

-- ticket05-trigger-boundary: outbox-context-payload-v1-created
-- Ticket07: allowlist of turn slot transitions. With ck_turn_slot_status this
-- is the full status allowlist: nonterminal -> nonterminal|terminal, terminal
-- -> identical (status, reason, time). A new guard: no predecessor to drop.
CREATE TRIGGER IF NOT EXISTS trg_conversation_turn_lifecycle_v1
BEFORE UPDATE ON conversation_turns
FOR EACH ROW
BEGIN
    IF NOT (
        (OLD.session_slot = 'terminal'
            AND NEW.session_slot = 'terminal'
            AND NEW.status = OLD.status
            AND NEW.terminal_reason <=> OLD.terminal_reason
            AND NEW.terminal_at <=> OLD.terminal_at)
        OR (OLD.session_slot = 'active'
            AND NEW.session_slot IN ('active','terminal'))
        OR (OLD.session_slot = 'successor'
            AND NEW.session_slot IN ('successor','active','terminal'))
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'conversation turn lifecycle transition is not allowed';
    END IF;
END$$

-- ticket07-trigger-boundary: conversation-turn-lifecycle-v1-created
DROP PROCEDURE IF EXISTS validate_wpp_ai_bot_immutability_guards$$
CREATE PROCEDURE validate_wpp_ai_bot_immutability_guards()
BEGIN
    IF EXISTS (
        SELECT 1
        FROM (
            SELECT 'trg_inbound_event_disposition_immutable_v2' AS trigger_name,
                   '56320587450386f07e0c99ad566db2d90964f33ce3c31a90c57efcde30fbeaa7' AS body_sha256
            UNION ALL SELECT 'trg_outbox_authority_immutable_v3',
                   '69c580f050889598f38a18167461f8d14156c70e5e811ad859f011d935edf1c7'
            UNION ALL SELECT 'trg_turn_revision_seal_immutable_v2',
                   '5d9dba4dae1efc97e76bca784c4eeb70d75db7816ea781d072c48c95d07d70fb'
            UNION ALL SELECT 'trg_turn_input_identity_immutable_v2',
                   'cf6af34982d510a016bb0cc81c4c5435142651d9902287d3e40479a86987f9b7'
            UNION ALL SELECT 'trg_conversation_turn_identity_immutable_v2',
                   'cd41c5d9a331db9fc12e7802a5344cfcec2c272add07f17adc500488d4945c42'
            UNION ALL SELECT 'trg_generation_attempt_identity_immutable_v2',
                   'c090f5382b1d2e6de9ac4599393e73f79b70900d66aa03e7d01800019e6f1dd1'
            UNION ALL SELECT 'trg_turn_revision_draft_immutable_v2',
                   '99bf8974c9cd291800ba12fd2d1eb46106a2d79073f7c023c7520e237cc837f8'
            UNION ALL SELECT 'trg_scheduled_handoff_immutable_v1',
                   '3715f24992cf671c9de663201a856508b1e6de9dde9936a8396bcabd7b267c6b'
            UNION ALL SELECT 'trg_handoff_context_assignment_immutable_v1',
                   '6f5b6b38ae7b4f86f2976a193864ad529c6c2b6c1b579abb0d2f344916b694fa'
            UNION ALL SELECT 'trg_turn_input_handoff_state_v1',
                   'd177ed721c78c42f9967d444d6fe65d50d1c381835cd08522558a74971a91841'
            UNION ALL SELECT 'trg_outbox_context_payload_immutable_v1',
                   '508783b2b83495d9a277871b427cde01fb12466128d4acee61a8c8c4c86151f2'
            UNION ALL SELECT 'trg_conversation_turn_lifecycle_v1',
                   '8e2db92704b6989ac7c523bbb5b6e44c05a7a6f381871cd09c56ddce9222c453'
        ) AS expected
        LEFT JOIN information_schema.triggers AS installed
          ON installed.trigger_schema = DATABASE()
         AND installed.trigger_name = expected.trigger_name
        WHERE installed.trigger_name IS NULL
           OR installed.action_timing <> 'BEFORE'
           OR installed.event_manipulation <> 'UPDATE'
           OR installed.action_orientation <> 'ROW'
           OR SHA2(installed.action_statement, 256) <> expected.body_sha256
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'immutability guard definition requires audited repair';
    END IF;
END$$

CALL validate_wpp_ai_bot_immutability_guards()$$
DROP PROCEDURE validate_wpp_ai_bot_immutability_guards$$

-- A valid versioned guard exists before any predecessor is removed. Each DROP
-- is therefore interruption-safe despite MySQL DDL autocommit.
DROP TRIGGER IF EXISTS trg_inbound_event_disposition_immutable$$
-- ticket03-trigger-boundary: inbound-disposition-predecessor-dropped
DROP TRIGGER IF EXISTS trg_outbox_authority_immutable$$
-- ticket03-trigger-boundary: outbox-authority-predecessor-dropped
DROP TRIGGER IF EXISTS trg_outbox_authority_immutable_v2$$
-- ticket06-trigger-boundary: outbox-authority-v2-dropped
DROP TRIGGER IF EXISTS trg_turn_revision_seal_immutable$$
-- ticket03-trigger-boundary: turn-revision-predecessor-dropped
DROP TRIGGER IF EXISTS trg_turn_input_identity_immutable$$
-- ticket03-trigger-boundary: turn-input-predecessor-dropped
DROP TRIGGER IF EXISTS trg_conversation_turn_identity_immutable$$
-- ticket03-trigger-boundary: conversation-turn-predecessor-dropped
DROP TRIGGER IF EXISTS trg_generation_attempt_identity_immutable$$
-- ticket03-trigger-boundary: generation-attempt-predecessor-dropped
DROP TRIGGER IF EXISTS trg_turn_revision_draft_immutable_v1$$
-- ticket04-trigger-boundary: turn-revision-draft-v1-dropped

DELIMITER ;

-- ─── T08-3: COMPATIBILIDADE DE RÉPLICAS ─────────────────────────
-- Additive. One heartbeat row per app / delivery-worker / turn-worker process
-- with the exact configuration it runs with. Configuration columns have no
-- default: a writer that omits one fails instead of reporting "compatible".
-- VARCHAR + CHECK instead of ENUM: a NOT NULL ENUM silently defaults to its
-- first member when omitted. CHECK literals carry the column's own _ascii
-- introducer: MySQL re-serializes CHECK clauses of ascii columns to _ascii on
-- any later ALTER TABLE, so _ascii is the only digest-stable form.
-- No FK (leaf table): no DELETE/purge path of any existing table is affected.
-- Exact shape is validated below; divergence fails closed with a named error
-- and nothing is rewritten or repositioned. Nothing is dropped, so every DDL
-- boundary leaves the previous guards intact.
CREATE TABLE IF NOT EXISTS replica_heartbeats (
    replica_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    replica_kind VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    code_version VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    schema_version INT UNSIGNED NOT NULL,
    delivery_mode VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    delivery_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    scheduler_enabled BOOLEAN NOT NULL,
    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    turn_processor_enabled BOOLEAN NOT NULL,
    dispatch_enabled BOOLEAN NOT NULL,
    webhook_auth_mode VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    started_at DATETIME(6) NOT NULL,
    last_seen_at DATETIME(6) NOT NULL,
    stopped_at DATETIME(6) NULL,
    PRIMARY KEY (replica_id),
    CONSTRAINT ck_replica_heartbeat_schema_version CHECK (schema_version > 0),
    CONSTRAINT ck_replica_heartbeat_kind CHECK (
        replica_kind IN (_ascii'app',_ascii'delivery_worker',_ascii'turn_worker')
    ),
    CONSTRAINT ck_replica_heartbeat_delivery_mode CHECK (
        delivery_mode IN (_ascii'direct',_ascii'outbox')
    ),
    CONSTRAINT ck_replica_heartbeat_auth_mode CHECK (
        webhook_auth_mode IN (_ascii'provisioning',_ascii'enforced')
    ),
    INDEX idx_replica_heartbeats_last_seen (last_seen_at)
);

-- t083-schema-boundary: replica-heartbeats-created

DELIMITER $$

DROP PROCEDURE IF EXISTS validate_t083_replica_heartbeats$$
CREATE PROCEDURE validate_t083_replica_heartbeats()
BEGIN
    DECLARE actual_shape LONGTEXT DEFAULT NULL;
    SET SESSION group_concat_max_len = 65535;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', column_name, column_type, is_nullable,
            COALESCE(column_default, '<NULL>'),
            COALESCE(character_set_name, '<NULL>'),
            COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'replica_heartbeats';
    -- NULL (absent object / empty set) must fail closed, never compare as unknown.
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'replica_id|char(36)|NO|<NULL>|ascii|ascii_bin|;',
        'replica_kind|varchar(20)|NO|<NULL>|ascii|ascii_bin|;',
        'code_version|varchar(100)|NO|<NULL>|ascii|ascii_bin|;',
        'schema_version|int unsigned|NO|<NULL>|<NULL>|<NULL>|;',
        'delivery_mode|varchar(10)|NO|<NULL>|ascii|ascii_bin|;',
        'delivery_epoch|char(36)|YES|<NULL>|ascii|ascii_bin|;',
        'scheduler_enabled|tinyint(1)|NO|<NULL>|<NULL>|<NULL>|;',
        'scheduler_epoch|char(36)|YES|<NULL>|ascii|ascii_bin|;',
        'turn_processor_enabled|tinyint(1)|NO|<NULL>|<NULL>|<NULL>|;',
        'dispatch_enabled|tinyint(1)|NO|<NULL>|<NULL>|<NULL>|;',
        'webhook_auth_mode|varchar(20)|NO|<NULL>|ascii|ascii_bin|;',
        'started_at|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        'last_seen_at|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        'stopped_at|datetime(6)|YES|<NULL>|<NULL>|<NULL>|'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 't083 replica heartbeat column shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
            COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'),
            index_type, is_visible, COALESCE(expression, '<NULL>'))
        ORDER BY index_name, seq_in_index SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'replica_heartbeats';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'idx_replica_heartbeats_last_seen|1|1|last_seen_at|A|<NULL>|BTREE|YES|<NULL>;',
        'PRIMARY|0|1|replica_id|A|<NULL>|BTREE|YES|<NULL>'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 't083 replica heartbeat index shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(
        CONCAT_WS('|', tc.constraint_name, tc.enforced, SHA2(cc.check_clause, 256))
        ORDER BY tc.constraint_name SEPARATOR ';'
    ) INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema
     AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE()
      AND tc.table_name = 'replica_heartbeats';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'ck_replica_heartbeat_auth_mode|YES|',
        '4615d1a4f5624e0073096ccc913a94897addf5c115d8f614a04f32188745ee93;',
        'ck_replica_heartbeat_delivery_mode|YES|',
        'e8f53a678ba8e3a32819307037653da78e5e196a04a8d6d7ded1c04d2d78a9c1;',
        'ck_replica_heartbeat_kind|YES|',
        '938f4b24865c829cf206a3f671044dbecd0585355696f50261443071d946d46b;',
        'ck_replica_heartbeat_schema_version|YES|',
        'd236410650e429e6854d677eabad2978f7b83da35ed13b1e3cc84d432b857ccf'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 't083 replica heartbeat check shape requires audited repair';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.referential_constraints
        WHERE constraint_schema = DATABASE()
          AND (table_name = 'replica_heartbeats'
               OR referenced_table_name = 'replica_heartbeats')
    ) OR EXISTS (
        SELECT 1 FROM information_schema.triggers
        WHERE trigger_schema = DATABASE()
          AND event_object_table = 'replica_heartbeats'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 't083 replica heartbeat dependency shape requires audited repair';
    END IF;
END$$

CALL validate_t083_replica_heartbeats()$$
DROP PROCEDURE validate_t083_replica_heartbeats$$

-- t083-schema-boundary: replica-heartbeats-validated

-- Monotonic schema marker, written only after the exact table is validated.
-- Binaries compiled for schema 2 refuse readiness / start while it is 1;
-- a newer marker (> 2) is never lowered.
UPDATE scheduler_runtime_state SET schema_version = 2
WHERE id = 1 AND schema_version = 1$$

-- t083-schema-boundary: schema-version-recorded

DROP PROCEDURE IF EXISTS validate_t083_schema_version$$
CREATE PROCEDURE validate_t083_schema_version()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM scheduler_runtime_state WHERE id = 1 AND schema_version >= 2
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 't083 scheduler schema version requires audited repair';
    END IF;
END$$

CALL validate_t083_schema_version()$$
DROP PROCEDURE validate_t083_schema_version$$

DELIMITER ;

-- S1: durable retention checkpoint. Leaf table, no FK, CHECK or trigger.
CREATE TABLE IF NOT EXISTS scheduler_retention_progress (
    id INT NOT NULL,
    last_turn_id BIGINT UNSIGNED NOT NULL,
    version BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (id)
);
-- sec-s1-schema-boundary: progress-created
DELIMITER $$
DROP PROCEDURE IF EXISTS validate_sec_s1_retention_progress$$
CREATE PROCEDURE validate_sec_s1_retention_progress()
BEGIN
    DECLARE actual_shape LONGTEXT DEFAULT NULL;
    SET SESSION group_concat_max_len = 65535;
    SELECT GROUP_CONCAT(CONCAT_WS('|', ordinal_position, column_name, column_type,
        is_nullable, COALESCE(column_default, '<NULL>'),
        COALESCE(character_set_name, '<NULL>'), COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'scheduler_retention_progress';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '1|id|int|NO|<NULL>|<NULL>|<NULL>|;',
        '2|last_turn_id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|;',
        '3|version|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sec-s1 retention column shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY index_name, seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'scheduler_retention_progress';
    IF actual_shape IS NULL OR actual_shape <> 'PRIMARY|0|1|id|A|<NULL>|BTREE|YES|<NULL>' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sec-s1 retention index shape requires audited repair';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.referential_constraints
        WHERE constraint_schema = DATABASE() AND (table_name = 'scheduler_retention_progress'
            OR referenced_table_name = 'scheduler_retention_progress'))
        OR EXISTS (SELECT 1 FROM information_schema.triggers WHERE trigger_schema = DATABASE()
            AND event_object_table = 'scheduler_retention_progress')
        OR EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema = DATABASE()
            AND table_name = 'scheduler_retention_progress' AND constraint_type <> 'PRIMARY KEY') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sec-s1 retention dependency shape requires audited repair';
    END IF;
END$$
CALL validate_sec_s1_retention_progress()$$
DROP PROCEDURE validate_sec_s1_retention_progress$$
-- sec-s1-schema-boundary: progress-validated
INSERT INTO scheduler_retention_progress (id, last_turn_id, version)
    SELECT 1, 0, 0 WHERE NOT EXISTS (SELECT 1 FROM scheduler_retention_progress WHERE id = 1)$$
-- sec-s1-schema-boundary: progress-seeded
UPDATE scheduler_runtime_state SET schema_version = 3 WHERE id = 1 AND schema_version = 2$$
-- sec-s1-schema-boundary: schema-version-recorded
DELIMITER ;

-- R2-S1: indexed retention SLA, schema 3 is not released yet. No FK/guard change.
DELIMITER $$
DROP PROCEDURE IF EXISTS install_sec2_s1_retention_index$$
CREATE PROCEDURE install_sec2_s1_retention_index()
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE() AND table_name = 'conversation_turns'
          AND index_name = 'idx_turn_retention_terminal') THEN
        ALTER TABLE conversation_turns
            ADD INDEX idx_turn_retention_terminal (session_slot, terminal_at),
            ALGORITHM=INPLACE, LOCK=NONE;
    END IF;
END$$
CALL install_sec2_s1_retention_index()$$
DROP PROCEDURE install_sec2_s1_retention_index$$
-- sec2-s1-schema-boundary: index-created
DROP PROCEDURE IF EXISTS validate_sec2_s1_retention_index$$
CREATE PROCEDURE validate_sec2_s1_retention_index()
BEGIN
    DECLARE actual_shape LONGTEXT DEFAULT NULL;
    SET SESSION group_concat_max_len = 65535;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'conversation_turns'
      AND index_name = 'idx_turn_retention_terminal';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'idx_turn_retention_terminal|1|1|session_slot|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_turn_retention_terminal|1|2|terminal_at|A|<NULL>|BTREE|YES|<NULL>') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sec2-s1 retention index shape requires audited repair';
    END IF;
END$$
CALL validate_sec2_s1_retention_index()$$
DROP PROCEDURE validate_sec2_s1_retention_index$$
-- sec2-s1-schema-boundary: index-validated
DELIMITER ;

-- AUTH-1: API operator authentication (single company, API only). Additive
-- leaf tables: no existing table gains a column, FK, CHECK or trigger, and
-- scheduler_runtime_state.schema_version is unchanged (workers never read
-- them). Users are never deleted (status desativado), so the RESTRICT FKs
-- below add no DELETE/purge path. A future company scope adds a column;
-- e-mail stays globally unique. VARCHAR + CHECK instead of ENUM (a NOT NULL
-- ENUM silently defaults to its first member); CHECK literals carry the
-- _ascii introducer of their ascii columns (digest-stable form).
CREATE TABLE IF NOT EXISTS auth_usuarios (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    email VARCHAR(254) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    papel VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    senha_hash VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
    senha_pepper_versao INT UNSIGNED NULL,
    totp_segredo_cifrado VARBINARY(255) NULL,
    totp_chave_versao INT UNSIGNED NULL,
    totp_confirmado_em DATETIME(6) NULL,
    totp_ultimo_passo BIGINT UNSIGNED NULL,
    falhas_consecutivas INT UNSIGNED NOT NULL DEFAULT 0,
    totp_falhas INT UNSIGNED NOT NULL DEFAULT 0,
    totp_bloqueios INT UNSIGNED NOT NULL DEFAULT 0,
    totp_bloqueado_ate DATETIME(6) NULL,
    criado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    atualizado_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uq_auth_usuario_email (email),
    INDEX idx_auth_usuario_papel (papel, status),
    CONSTRAINT ck_auth_usuario_papel CHECK (
        papel IN (_ascii'admin',_ascii'operador',_ascii'leitura')
    ),
    CONSTRAINT ck_auth_usuario_status CHECK (
        status IN (_ascii'pendente',_ascii'ativo',_ascii'desativado')
    ),
    CONSTRAINT ck_auth_usuario_senha CHECK (
        (senha_hash IS NULL) = (senha_pepper_versao IS NULL)
        AND (status <> _ascii'ativo' OR senha_hash IS NOT NULL)
    ),
    CONSTRAINT ck_auth_usuario_totp CHECK (
        (totp_segredo_cifrado IS NULL) = (totp_chave_versao IS NULL)
        AND (totp_confirmado_em IS NULL OR totp_segredo_cifrado IS NOT NULL)
    )
);

-- auth-1-schema-boundary: usuarios-created

CREATE TABLE IF NOT EXISTS auth_sessoes (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    token_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    usuario_id BIGINT UNSIGNED NOT NULL,
    estagio VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    csrf_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    tentativas_mfa INT UNSIGNED NOT NULL DEFAULT 0,
    criada_em DATETIME(6) NOT NULL,
    ultimo_uso_em DATETIME(6) NOT NULL,
    expira_em DATETIME(6) NOT NULL,
    revogada_em DATETIME(6) NULL,
    motivo_revogacao VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_auth_sessao_token (token_hash),
    INDEX idx_auth_sessao_usuario (usuario_id, revogada_em),
    INDEX idx_auth_sessao_expira (expira_em),
    CONSTRAINT fk_auth_sessao_usuario FOREIGN KEY (usuario_id)
        REFERENCES auth_usuarios(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT ck_auth_sessao_estagio CHECK (
        estagio IN (_ascii'mfa',_ascii'enrolamento',_ascii'completa')
    ),
    CONSTRAINT ck_auth_sessao_revogacao CHECK (
        (revogada_em IS NULL) = (motivo_revogacao IS NULL)
    )
);

-- auth-1-schema-boundary: sessoes-created

-- Progressive delay per (account, source). Nothing here locks an account for
-- other sources; rows expire after 24 h of silence.
CREATE TABLE IF NOT EXISTS auth_tentativas (
    usuario_id BIGINT UNSIGNED NOT NULL,
    origem VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    falhas INT UNSIGNED NOT NULL,
    ultima_falha_em DATETIME(6) NOT NULL,
    liberado_em DATETIME(6) NULL,
    PRIMARY KEY (usuario_id, origem),
    INDEX idx_auth_tentativa_expira (usuario_id, ultima_falha_em),
    CONSTRAINT fk_auth_tentativa_usuario FOREIGN KEY (usuario_id)
        REFERENCES auth_usuarios(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT ck_auth_tentativa_falhas CHECK (falhas > 0)
);

-- auth-1-schema-boundary: tentativas-created

CREATE TABLE IF NOT EXISTS auth_tokens_servico (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    nome VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    papel VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    criado_em DATETIME(6) NOT NULL,
    expira_em DATETIME(6) NOT NULL,
    revogado_em DATETIME(6) NULL,
    ultimo_uso_em DATETIME(6) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_auth_token_servico_hash (token_hash),
    CONSTRAINT ck_auth_token_servico_papel CHECK (
        papel IN (_ascii'operador',_ascii'leitura')
    ),
    CONSTRAINT ck_auth_token_servico_validade CHECK (expira_em > criado_em)
);

-- auth-1-schema-boundary: tokens-servico-created

CREATE TABLE IF NOT EXISTS auth_tokens_uso_unico (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    usuario_id BIGINT UNSIGNED NOT NULL,
    tipo VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    token_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    criado_em DATETIME(6) NOT NULL,
    expira_em DATETIME(6) NOT NULL,
    usado_em DATETIME(6) NULL,
    revogado_em DATETIME(6) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_auth_token_uso_unico_hash (token_hash),
    INDEX idx_auth_token_uso_unico_usuario (usuario_id, tipo),
    CONSTRAINT fk_auth_token_uso_unico_usuario FOREIGN KEY (usuario_id)
        REFERENCES auth_usuarios(id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT ck_auth_token_uso_unico_tipo CHECK (
        tipo IN (_ascii'ativacao',_ascii'reset_senha',_ascii'reset_mfa')
    ),
    CONSTRAINT ck_auth_token_uso_unico_validade CHECK (expira_em > criado_em)
);

-- auth-1-schema-boundary: tokens-uso-unico-created

-- Append-only audit: leaf without FK (actors may be anonymous or CLI).
-- detalhe holds allowlisted keys only; phones and e-mails are masked.
CREATE TABLE IF NOT EXISTS auth_auditoria (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ocorrido_em DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    evento VARCHAR(48) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    resultado VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ator_tipo VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ator_id BIGINT UNSIGNED NULL,
    ator_rotulo VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    alvo_tipo VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    alvo_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    origem_ip VARCHAR(45) CHARACTER SET ascii COLLATE ascii_bin NULL,
    detalhe JSON NULL,
    PRIMARY KEY (id),
    INDEX idx_auth_auditoria_data (ocorrido_em),
    INDEX idx_auth_auditoria_ator (ator_tipo, ator_id, ocorrido_em),
    CONSTRAINT ck_auth_auditoria_resultado CHECK (
        resultado IN (_ascii'sucesso',_ascii'negado',_ascii'falha')
    ),
    CONSTRAINT ck_auth_auditoria_ator CHECK (
        ator_tipo IN (_ascii'usuario',_ascii'token_servico',_ascii'cli',_ascii'anonimo')
    )
);

-- auth-1-schema-boundary: auditoria-created

DELIMITER $$

-- Exact shape: columns (ordinal, type, signedness, nullability, default,
-- charset/collation, extra), indexes, CHECK digests, FKs (target, columns,
-- rules) and no trigger. NULL (absent object) fails closed; nothing is
-- rewritten, repositioned or dropped to converge.
DROP PROCEDURE IF EXISTS validate_auth_1_schema$$
CREATE PROCEDURE validate_auth_1_schema()
BEGIN
    DECLARE actual_shape LONGTEXT DEFAULT NULL;
    SET SESSION group_concat_max_len = 65535;

    SELECT GROUP_CONCAT(CONCAT_WS('|', ordinal_position, column_name, column_type,
        is_nullable, COALESCE(column_default, '<NULL>'),
        COALESCE(character_set_name, '<NULL>'), COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'auth_usuarios';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '1|id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|auto_increment;',
        '2|email|varchar(254)|NO|<NULL>|utf8mb4|utf8mb4_bin|;',
        '3|papel|varchar(16)|NO|<NULL>|ascii|ascii_bin|;',
        '4|status|varchar(16)|NO|<NULL>|ascii|ascii_bin|;',
        '5|senha_hash|varchar(255)|YES|<NULL>|ascii|ascii_bin|;',
        '6|senha_pepper_versao|int unsigned|YES|<NULL>|<NULL>|<NULL>|;',
        '7|totp_segredo_cifrado|varbinary(255)|YES|<NULL>|<NULL>|<NULL>|;',
        '8|totp_chave_versao|int unsigned|YES|<NULL>|<NULL>|<NULL>|;',
        '9|totp_confirmado_em|datetime(6)|YES|<NULL>|<NULL>|<NULL>|;',
        '10|totp_ultimo_passo|bigint unsigned|YES|<NULL>|<NULL>|<NULL>|;',
        '11|falhas_consecutivas|int unsigned|NO|0|<NULL>|<NULL>|;',
        '12|totp_falhas|int unsigned|NO|0|<NULL>|<NULL>|;',
        '13|totp_bloqueios|int unsigned|NO|0|<NULL>|<NULL>|;',
        '14|totp_bloqueado_ate|datetime(6)|YES|<NULL>|<NULL>|<NULL>|;',
        '15|criado_em|datetime(6)|NO|CURRENT_TIMESTAMP(6)|<NULL>|<NULL>|DEFAULT_GENERATED;',
        '16|atualizado_em|datetime(6)|NO|CURRENT_TIMESTAMP(6)|<NULL>|<NULL>|DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_usuarios column shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY index_name, seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'auth_usuarios';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'idx_auth_usuario_papel|1|1|papel|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_usuario_papel|1|2|status|A|<NULL>|BTREE|YES|<NULL>;',
        'PRIMARY|0|1|id|A|<NULL>|BTREE|YES|<NULL>;',
        'uq_auth_usuario_email|0|1|email|A|<NULL>|BTREE|YES|<NULL>') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_usuarios index shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', tc.constraint_name, tc.enforced, SHA2(cc.check_clause, 256))
        ORDER BY tc.constraint_name SEPARATOR ';') INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE() AND tc.table_name = 'auth_usuarios';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'ck_auth_usuario_papel|YES|2361b00422ece0f21e5b3547bdc4f6084aa8783f34d3222b8dda485289502a96;',
        'ck_auth_usuario_senha|YES|ed4826c91ef61fac74ac2650f3f35e7b673856a0e57fcea3e5b7ba848215ce23;',
        'ck_auth_usuario_status|YES|7bd4d72c3cc5d9a390d6cc74c89995531bdb65f0ffc1f15964bdf931485b7ea5;',
        'ck_auth_usuario_totp|YES|dd66538ac9b4eca7321f02cb4d611022ed7eaec564e6c8fb6be8503a4f8cb409') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_usuarios check shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(CONCAT_WS('|', ordinal_position, column_name, column_type,
        is_nullable, COALESCE(column_default, '<NULL>'),
        COALESCE(character_set_name, '<NULL>'), COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'auth_sessoes';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '1|id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|auto_increment;',
        '2|token_hash|char(64)|NO|<NULL>|ascii|ascii_bin|;',
        '3|usuario_id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|;',
        '4|estagio|varchar(16)|NO|<NULL>|ascii|ascii_bin|;',
        '5|csrf_hash|char(64)|YES|<NULL>|ascii|ascii_bin|;',
        '6|tentativas_mfa|int unsigned|NO|0|<NULL>|<NULL>|;',
        '7|criada_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '8|ultimo_uso_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '9|expira_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '10|revogada_em|datetime(6)|YES|<NULL>|<NULL>|<NULL>|;',
        '11|motivo_revogacao|varchar(32)|YES|<NULL>|ascii|ascii_bin|') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_sessoes column shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY index_name, seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'auth_sessoes';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'idx_auth_sessao_expira|1|1|expira_em|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_sessao_usuario|1|1|usuario_id|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_sessao_usuario|1|2|revogada_em|A|<NULL>|BTREE|YES|<NULL>;',
        'PRIMARY|0|1|id|A|<NULL>|BTREE|YES|<NULL>;',
        'uq_auth_sessao_token|0|1|token_hash|A|<NULL>|BTREE|YES|<NULL>') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_sessoes index shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', tc.constraint_name, tc.enforced, SHA2(cc.check_clause, 256))
        ORDER BY tc.constraint_name SEPARATOR ';') INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE() AND tc.table_name = 'auth_sessoes';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'ck_auth_sessao_estagio|YES|a3739b7700594b596147d4ea2642170e7f7ebfb82df9b2362ff0b5d42e8251da;',
        'ck_auth_sessao_revogacao|YES|9e46fe24d90b49c15782de1137064da8e070a709738ec2b51f162e5ad4c816ee') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_sessoes check shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(CONCAT_WS('|', ordinal_position, column_name, column_type,
        is_nullable, COALESCE(column_default, '<NULL>'),
        COALESCE(character_set_name, '<NULL>'), COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'auth_tentativas';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '1|usuario_id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|;',
        '2|origem|varchar(64)|NO|<NULL>|ascii|ascii_bin|;',
        '3|falhas|int unsigned|NO|<NULL>|<NULL>|<NULL>|;',
        '4|ultima_falha_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '5|liberado_em|datetime(6)|YES|<NULL>|<NULL>|<NULL>|') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tentativas column shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY index_name, seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'auth_tentativas';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'idx_auth_tentativa_expira|1|1|usuario_id|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_tentativa_expira|1|2|ultima_falha_em|A|<NULL>|BTREE|YES|<NULL>;',
        'PRIMARY|0|1|usuario_id|A|<NULL>|BTREE|YES|<NULL>;',
        'PRIMARY|0|2|origem|A|<NULL>|BTREE|YES|<NULL>') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tentativas index shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', tc.constraint_name, tc.enforced, SHA2(cc.check_clause, 256))
        ORDER BY tc.constraint_name SEPARATOR ';') INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE() AND tc.table_name = 'auth_tentativas';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'ck_auth_tentativa_falhas|YES|498921396db34c943a2f385f8f1da9fea09e80425962c2ea4aba927b14dc666c') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tentativas check shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(CONCAT_WS('|', ordinal_position, column_name, column_type,
        is_nullable, COALESCE(column_default, '<NULL>'),
        COALESCE(character_set_name, '<NULL>'), COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'auth_tokens_servico';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '1|id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|auto_increment;',
        '2|nome|varchar(100)|NO|<NULL>|utf8mb4|utf8mb4_bin|;',
        '3|papel|varchar(16)|NO|<NULL>|ascii|ascii_bin|;',
        '4|token_hash|char(64)|NO|<NULL>|ascii|ascii_bin|;',
        '5|criado_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '6|expira_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '7|revogado_em|datetime(6)|YES|<NULL>|<NULL>|<NULL>|;',
        '8|ultimo_uso_em|datetime(6)|YES|<NULL>|<NULL>|<NULL>|') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tokens_servico column shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY index_name, seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'auth_tokens_servico';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'PRIMARY|0|1|id|A|<NULL>|BTREE|YES|<NULL>;',
        'uq_auth_token_servico_hash|0|1|token_hash|A|<NULL>|BTREE|YES|<NULL>') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tokens_servico index shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', tc.constraint_name, tc.enforced, SHA2(cc.check_clause, 256))
        ORDER BY tc.constraint_name SEPARATOR ';') INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE() AND tc.table_name = 'auth_tokens_servico';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'ck_auth_token_servico_papel|YES|67872f291e358f1c0fda1971f989e16c13e3f964314928c7b17a33daab170de5;',
        'ck_auth_token_servico_validade|YES|2e20510a12c9dc8946c877a635a30e3690f4549305fb9e8dfe1bcad785641922') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tokens_servico check shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(CONCAT_WS('|', ordinal_position, column_name, column_type,
        is_nullable, COALESCE(column_default, '<NULL>'),
        COALESCE(character_set_name, '<NULL>'), COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'auth_tokens_uso_unico';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '1|id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|auto_increment;',
        '2|usuario_id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|;',
        '3|tipo|varchar(16)|NO|<NULL>|ascii|ascii_bin|;',
        '4|token_hash|char(64)|NO|<NULL>|ascii|ascii_bin|;',
        '5|criado_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '6|expira_em|datetime(6)|NO|<NULL>|<NULL>|<NULL>|;',
        '7|usado_em|datetime(6)|YES|<NULL>|<NULL>|<NULL>|;',
        '8|revogado_em|datetime(6)|YES|<NULL>|<NULL>|<NULL>|') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tokens_uso_unico column shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY index_name, seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'auth_tokens_uso_unico';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'idx_auth_token_uso_unico_usuario|1|1|usuario_id|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_token_uso_unico_usuario|1|2|tipo|A|<NULL>|BTREE|YES|<NULL>;',
        'PRIMARY|0|1|id|A|<NULL>|BTREE|YES|<NULL>;',
        'uq_auth_token_uso_unico_hash|0|1|token_hash|A|<NULL>|BTREE|YES|<NULL>') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tokens_uso_unico index shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', tc.constraint_name, tc.enforced, SHA2(cc.check_clause, 256))
        ORDER BY tc.constraint_name SEPARATOR ';') INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE() AND tc.table_name = 'auth_tokens_uso_unico';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'ck_auth_token_uso_unico_tipo|YES|5c08342b9bbc03ba48abac05d72d8c73d00a986ac82da13304f9d90527b06b06;',
        'ck_auth_token_uso_unico_validade|YES|2e20510a12c9dc8946c877a635a30e3690f4549305fb9e8dfe1bcad785641922') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_tokens_uso_unico check shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(CONCAT_WS('|', ordinal_position, column_name, column_type,
        is_nullable, COALESCE(column_default, '<NULL>'),
        COALESCE(character_set_name, '<NULL>'), COALESCE(collation_name, '<NULL>'), extra)
        ORDER BY ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'auth_auditoria';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        '1|id|bigint unsigned|NO|<NULL>|<NULL>|<NULL>|auto_increment;',
        '2|ocorrido_em|datetime(6)|NO|CURRENT_TIMESTAMP(6)|<NULL>|<NULL>|DEFAULT_GENERATED;',
        '3|evento|varchar(48)|NO|<NULL>|ascii|ascii_bin|;',
        '4|resultado|varchar(16)|NO|<NULL>|ascii|ascii_bin|;',
        '5|ator_tipo|varchar(16)|NO|<NULL>|ascii|ascii_bin|;',
        '6|ator_id|bigint unsigned|YES|<NULL>|<NULL>|<NULL>|;',
        '7|ator_rotulo|varchar(100)|YES|<NULL>|utf8mb4|utf8mb4_bin|;',
        '8|alvo_tipo|varchar(32)|YES|<NULL>|ascii|ascii_bin|;',
        '9|alvo_id|varchar(64)|YES|<NULL>|ascii|ascii_bin|;',
        '10|origem_ip|varchar(45)|YES|<NULL>|ascii|ascii_bin|;',
        '11|detalhe|json|YES|<NULL>|<NULL>|<NULL>|') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_auditoria column shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', index_name, non_unique, seq_in_index, column_name,
        COALESCE(collation, '<NULL>'), COALESCE(sub_part, '<NULL>'), index_type,
        is_visible, COALESCE(expression, '<NULL>')) ORDER BY index_name, seq_in_index SEPARATOR ';')
        INTO actual_shape FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'auth_auditoria';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'idx_auth_auditoria_ator|1|1|ator_tipo|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_auditoria_ator|1|2|ator_id|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_auditoria_ator|1|3|ocorrido_em|A|<NULL>|BTREE|YES|<NULL>;',
        'idx_auth_auditoria_data|1|1|ocorrido_em|A|<NULL>|BTREE|YES|<NULL>;',
        'PRIMARY|0|1|id|A|<NULL>|BTREE|YES|<NULL>') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_auditoria index shape requires audited repair';
    END IF;
    SELECT GROUP_CONCAT(CONCAT_WS('|', tc.constraint_name, tc.enforced, SHA2(cc.check_clause, 256))
        ORDER BY tc.constraint_name SEPARATOR ';') INTO actual_shape
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.check_constraints AS cc
      ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name
    WHERE tc.constraint_schema = DATABASE() AND tc.table_name = 'auth_auditoria';
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'ck_auth_auditoria_ator|YES|6314ae239335f4f41353c7eb61b21e96b82f5e3c901912fcba649f5156686e44;',
        'ck_auth_auditoria_resultado|YES|2d2d39c3096742e0e2ff1a7a928b59cbd40cbf72b59c7fb101add8662b9928bf') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 auth_auditoria check shape requires audited repair';
    END IF;

    SELECT GROUP_CONCAT(CONCAT_WS('|', rc.constraint_name, rc.table_name, rc.referenced_table_name,
        kcu.referenced_table_schema = rc.constraint_schema, rc.update_rule, rc.delete_rule,
        kcu.column_name, kcu.referenced_column_name, kcu.ordinal_position)
        ORDER BY rc.constraint_name, kcu.ordinal_position SEPARATOR ';') INTO actual_shape
    FROM information_schema.referential_constraints AS rc
    JOIN information_schema.key_column_usage AS kcu
      ON kcu.constraint_schema = rc.constraint_schema AND kcu.constraint_name = rc.constraint_name
     AND kcu.table_name = rc.table_name
    WHERE rc.constraint_schema = DATABASE()
      AND (rc.table_name LIKE 'auth\\_%' OR rc.referenced_table_name LIKE 'auth\\_%');
    IF actual_shape IS NULL OR actual_shape <> CONCAT(
        'fk_auth_sessao_usuario|auth_sessoes|auth_usuarios|1|RESTRICT|RESTRICT|usuario_id|id|1;',
        'fk_auth_tentativa_usuario|auth_tentativas|auth_usuarios|1|RESTRICT|RESTRICT|usuario_id|id|1;',
        'fk_auth_token_uso_unico_usuario|auth_tokens_uso_unico|auth_usuarios|1|RESTRICT|RESTRICT|usuario_id|id|1') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 foreign key shape requires audited repair';
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.triggers
        WHERE trigger_schema = DATABASE() AND event_object_table LIKE 'auth\\_%'
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'auth-1 trigger shape requires audited repair';
    END IF;
END$$

CALL validate_auth_1_schema()$$
DROP PROCEDURE validate_auth_1_schema$$

-- auth-1-schema-boundary: exact-schema-validated

DELIMITER ;
