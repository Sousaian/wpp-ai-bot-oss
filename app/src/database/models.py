from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Computed,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    JSON,
    Numeric,
    SmallInteger,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.mysql import BIGINT, CHAR, DATETIME, ENUM as MYSQL_ENUM, INTEGER, SMALLINT
from sqlalchemy.orm import relationship, DeclarativeBase
from datetime import datetime
from uuid import uuid4


class Base(DeclarativeBase):
    pass


class Agente(Base):
    __tablename__ = "agentes"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    nome                = Column(String(255), nullable=False)
    instance_name       = Column(String(255), nullable=False, unique=True)
    system_prompt       = Column(Text, nullable=False)
    modelo_openai       = Column(String(100), nullable=False, default="gpt-4o-mini")
    modelo_llm          = Column(String(255), nullable=True)
    numero_notificacao  = Column(String(50), nullable=False)
    chatwoot_inbox_id   = Column(Integer, nullable=True)
    limite_mensal_creditos = Column(Numeric(12, 6), nullable=True)
    max_historico       = Column(Integer, nullable=False, default=20)
    ativo               = Column(Boolean, nullable=False, default=True)
    inbox_scheduler_enabled = Column(Boolean, nullable=False, default=False)
    criado_em           = Column(DateTime, nullable=False, default=datetime.utcnow)
    atualizado_em       = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    sessoes = relationship("Sessao", back_populates="agente", cascade="all, delete-orphan")
    consumos_llm = relationship("ConsumoLLM", back_populates="agente", cascade="all, delete-orphan")


class Atendente(Base):
    __tablename__ = "atendentes"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    nome                = Column(String(255), nullable=False)
    numero_whatsapp     = Column(String(50), nullable=False, unique=True)
    ativo               = Column(Boolean, nullable=False, default=True)
    indice_round_robin  = Column(Integer, nullable=False, default=0)
    criado_em           = Column(DateTime, nullable=False, default=datetime.utcnow)
    atualizado_em       = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    sessoes = relationship("Sessao", back_populates="atendente")


class Sessao(Base):
    __tablename__ = "sessoes"
    __table_args__ = (
        UniqueConstraint("agente_id", "telefone", name="uq_agente_telefone"),
        UniqueConstraint(
            "delivery_stream_key", name="uq_sessoes_delivery_stream"
        ),
        UniqueConstraint(
            "chatwoot_conversation_id",
            name="uq_sessoes_chatwoot_conversation",
        ),
        Index("idx_sessoes_agente_id", "agente_id"),
        Index("idx_sessoes_telefone", "telefone"),
        Index("idx_sessoes_atendente_id", "atendente_id"),
    )

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    agente_id           = Column(
        Integer,
        ForeignKey("agentes.id", name="fk_sessao_agente", ondelete="CASCADE"),
        nullable=False,
    )
    atendente_id        = Column(
        Integer,
        ForeignKey("atendentes.id", name="fk_sessao_atendente"),
        nullable=True,
    )
    telefone            = Column(String(50), nullable=False)
    handler             = Column(Enum("bot", "humano", "encerrada"), nullable=False, default="bot")
    total_mensagens     = Column(Integer, nullable=False, default=0)
    motivo_handoff      = Column(Text, nullable=True)
    encerrada_em        = Column(DateTime, nullable=True)
    motivo_encerramento = Column(String(50), nullable=True)
    chatwoot_conversation_id = Column(Integer, nullable=True)
    delivery_stream_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"),
        nullable=False,
        default=lambda: str(uuid4()),
    )
    next_delivery_sequence = Column(BIGINT(unsigned=True), nullable=False, default=1)
    handoff_generation = Column(INTEGER(unsigned=True), nullable=False, default=0)
    criado_em           = Column(DateTime, nullable=False, default=datetime.utcnow)
    ultima_interacao    = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    agente    = relationship("Agente", back_populates="sessoes")
    atendente = relationship("Atendente", back_populates="sessoes")
    mensagens = relationship("Mensagem", back_populates="sessao", cascade="all, delete-orphan")
    consumos_llm = relationship("ConsumoLLM", back_populates="sessao")


class Mensagem(Base):
    __tablename__ = "mensagens"
    __table_args__ = (
        Index("idx_mensagens_sessao_id", "sessao_id"),
        Index("idx_mensagens_criado_em", "criado_em"),
    )

    id        = Column(Integer, primary_key=True, autoincrement=True)
    sessao_id = Column(
        Integer,
        ForeignKey("sessoes.id", name="fk_mensagem_sessao", ondelete="CASCADE"),
        nullable=False,
    )
    role      = Column(Enum("user", "assistant"), nullable=False)
    conteudo  = Column(Text, nullable=False)
    criado_em = Column(DateTime, nullable=False, default=datetime.utcnow)

    sessao = relationship("Sessao", back_populates="mensagens")


class ConsumoLLM(Base):
    __tablename__ = "consumo_llm"
    __table_args__ = (
        UniqueConstraint(
            "provider_generation_id", name="uq_consumo_llm_generation"
        ),
        Index("idx_consumo_llm_agente_data", "agente_id", "criado_em"),
        Index("idx_consumo_llm_sessao", "sessao_id"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    agente_id = Column(
        Integer,
        ForeignKey(
            "agentes.id", name="fk_consumo_llm_agente", ondelete="CASCADE"
        ),
        nullable=False,
    )
    sessao_id = Column(
        Integer,
        ForeignKey(
            "sessoes.id", name="fk_consumo_llm_sessao", ondelete="SET NULL"
        ),
        nullable=True,
    )
    modelo_llm = Column(String(255), nullable=False)
    # Provider-reported model id, verbatim; modelo_llm is the requested
    # allowlisted id used for billing and exact attempt linkage.
    modelo_llm_reportado = Column(String(255), nullable=True)
    tipo_chamada = Column(Enum("resposta", "resumo"), nullable=False)
    tokens_input = Column(INTEGER(unsigned=True), nullable=False, default=0)
    tokens_output = Column(INTEGER(unsigned=True), nullable=False, default=0)
    tokens_reasoning = Column(INTEGER(unsigned=True), nullable=True)
    tokens_cache_read = Column(INTEGER(unsigned=True), nullable=True)
    tokens_cache_write = Column(INTEGER(unsigned=True), nullable=True)
    custo_creditos = Column(Numeric(12, 8), nullable=False, default=0)
    provider_generation_id = Column(String(255), nullable=True)
    criado_em = Column(DateTime, nullable=False, default=datetime.utcnow)

    agente = relationship("Agente", back_populates="consumos_llm")
    sessao = relationship("Sessao", back_populates="consumos_llm")


class InboundEvent(Base):
    __tablename__ = "inbound_events"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "source_scope",
            "provider_event_id",
            name="uq_inbound_event_provider_scope_id",
        ),
        UniqueConstraint("mensagem_id", name="uq_inbound_event_mensagem"),
        CheckConstraint("hmac_key_version > 0", name="ck_inbound_hmac_key_version"),
        Index("idx_inbound_events_received_at", "received_at"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    provider = Column(Enum("evolution", "chatwoot"), nullable=False)
    source_scope = Column(String(255, collation="ascii_bin"), nullable=False)
    provider_event_id = Column(String(255, collation="ascii_bin"), nullable=False)
    event_type = Column(String(100, collation="ascii_bin"), nullable=False)
    payload_hmac = Column(
        CHAR(64, charset="ascii", collation="ascii_bin"), nullable=False
    )
    hmac_key_version = Column(SMALLINT(unsigned=True), nullable=False)
    agente_id = Column(
        Integer,
        ForeignKey(
            "agentes.id", name="fk_inbound_event_agente", ondelete="SET NULL"
        ),
        nullable=True,
    )
    sessao_id = Column(
        Integer,
        ForeignKey(
            "sessoes.id", name="fk_inbound_event_sessao", ondelete="SET NULL"
        ),
        nullable=True,
    )
    receipt_disposition = Column(
        Enum(
            "scheduled",
            "ignored_nonbot",
            "legacy_evolution",
            "chatwoot",
            "legacy_unknown",
        ),
        nullable=False,
        default="legacy_unknown",
        server_default=text("'legacy_unknown'"),
    )
    mensagem_id = Column(
        Integer,
        ForeignKey(
            "mensagens.id",
            name="fk_inbound_event_mensagem",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        nullable=True,
    )
    received_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6)"),
    )
    processed_at = Column(DATETIME(fsp=6), nullable=True)


class SchedulerRuntimeState(Base):
    __tablename__ = "scheduler_runtime_state"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_scheduler_runtime_singleton"),
        CheckConstraint("version > 0", name="ck_scheduler_runtime_version"),
        CheckConstraint("schema_version > 0", name="ck_scheduler_schema_version"),
        CheckConstraint(
            "max_pending_inputs_per_session BETWEEN 1 AND 10000",
            name="ck_scheduler_session_input_cap",
        ),
        CheckConstraint(
            "max_pending_input_bytes_per_session BETWEEN 1 AND 16777215",
            name="ck_scheduler_session_bytes_cap",
        ),
        CheckConstraint(
            "max_queued_turns_per_agent BETWEEN 1 AND 100000",
            name="ck_scheduler_agent_turn_cap",
        ),
        CheckConstraint(
            "max_queued_bytes_per_agent BETWEEN 1 AND 1073741824",
            name="ck_scheduler_agent_bytes_cap",
        ),
        CheckConstraint(
            "max_global_queued_turns BETWEEN 1 AND 1000000",
            name="ck_scheduler_global_turn_cap",
        ),
        CheckConstraint(
            "max_global_queued_bytes BETWEEN 1 AND 17179869184",
            name="ck_scheduler_global_bytes_cap",
        ),
        CheckConstraint(
            "max_generation_attempts_per_turn BETWEEN 1 AND 8",
            name="ck_scheduler_generation_cap",
        ),
        CheckConstraint(
            "max_delivery_attempts_per_turn BETWEEN 1 AND 32",
            name="ck_scheduler_delivery_cap",
        ),
        CheckConstraint("max_refreshes_per_turn = 1", name="ck_scheduler_refresh_cap"),
        CheckConstraint(
            "accounting_drain_seconds BETWEEN 1 AND 300",
            name="ck_scheduler_accounting_drain",
        ),
    )

    id = Column(SmallInteger, primary_key=True, default=1)
    state = Column(
        Enum("disabled", "enabling", "enabled", "draining"),
        nullable=False,
        default="disabled",
    )
    scheduler_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    version = Column(BIGINT(unsigned=True), nullable=False, default=1)
    schema_version = Column(INTEGER(unsigned=True), nullable=False, default=1)
    max_pending_inputs_per_session = Column(
        INTEGER(unsigned=True), nullable=False, default=128
    )
    max_pending_input_bytes_per_session = Column(
        INTEGER(unsigned=True), nullable=False, default=262144
    )
    max_queued_turns_per_agent = Column(
        INTEGER(unsigned=True), nullable=False, default=1000
    )
    max_queued_bytes_per_agent = Column(
        BIGINT(unsigned=True), nullable=False, default=16777216
    )
    max_global_queued_turns = Column(
        INTEGER(unsigned=True), nullable=False, default=10000
    )
    max_global_queued_bytes = Column(
        BIGINT(unsigned=True), nullable=False, default=268435456
    )
    max_generation_attempts_per_turn = Column(
        SMALLINT(unsigned=True), nullable=False, default=2
    )
    max_delivery_attempts_per_turn = Column(
        SMALLINT(unsigned=True), nullable=False, default=8
    )
    max_refreshes_per_turn = Column(
        SMALLINT(unsigned=True), nullable=False, default=1
    )
    accounting_drain_seconds = Column(
        SMALLINT(unsigned=True), nullable=False, default=30
    )
    changed_by = Column(String(255), nullable=False)
    change_reason = Column(String(500), nullable=False)
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"),
    )


class AgentServiceLane(Base):
    __tablename__ = "agent_service_lanes"
    __table_args__ = (
        CheckConstraint(
            "next_arrival_sequence > 0 AND next_scheduling_sequence > 0",
            name="ck_lane_sequences_positive",
        ),
        CheckConstraint(
            "(owner_token IS NULL AND owner_worker_id IS NULL AND lease_until IS NULL "
            "AND active_turn_id IS NULL AND active_revision_id IS NULL) OR "
            "(owner_token IS NOT NULL AND owner_worker_id IS NOT NULL "
            "AND lease_until IS NOT NULL AND active_turn_id IS NOT NULL)",
            name="ck_lane_lease_tuple",
        ),
        Index("idx_agent_lanes_lease", "lease_until", "agente_id"),
    )

    agente_id = Column(
        Integer,
        ForeignKey("agentes.id", name="fk_lane_agente", ondelete="CASCADE"),
        primary_key=True,
    )
    scheduler_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    next_arrival_sequence = Column(BIGINT(unsigned=True), nullable=False, default=1)
    next_scheduling_sequence = Column(BIGINT(unsigned=True), nullable=False, default=1)
    active_turn_id = Column(
        BigInteger,
        ForeignKey(
            "conversation_turns.id",
            name="fk_lane_active_turn",
            ondelete="RESTRICT",
            use_alter=True,
        ),
        nullable=True,
    )
    active_revision_id = Column(
        BigInteger,
        ForeignKey(
            "turn_revisions.id",
            name="fk_lane_active_revision",
            ondelete="RESTRICT",
            use_alter=True,
        ),
        nullable=True,
    )
    owner_token = Column(CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True)
    owner_worker_id = Column(String(255, collation="ascii_bin"), nullable=True)
    lease_until = Column(DATETIME(fsp=6), nullable=True)
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"),
    )


class ConversationTurn(Base):
    __tablename__ = "conversation_turns"
    __table_args__ = (
        UniqueConstraint("turn_key", name="uq_conversation_turn_key"),
        UniqueConstraint(
            "agente_id", "arrival_sequence", name="uq_turn_agent_arrival_sequence"
        ),
        UniqueConstraint(
            "agente_id", "scheduling_sequence", name="uq_turn_agent_scheduling_sequence"
        ),
        UniqueConstraint("active_session_guard", name="uq_turn_active_session"),
        UniqueConstraint("successor_session_guard", name="uq_turn_successor_session"),
        UniqueConstraint(
            "id", "agente_id", "sessao_id", "scheduler_epoch",
            name="uq_turn_authority_identity",
        ),
        CheckConstraint("arrival_sequence > 0", name="ck_turn_arrival_positive"),
        CheckConstraint("scheduling_sequence > 0", name="ck_turn_schedule_positive"),
        CheckConstraint("input_version > 0", name="ck_turn_input_version_positive"),
        CheckConstraint("refresh_count BETWEEN 0 AND 1", name="ck_turn_refresh_count"),
        CheckConstraint("delivery_attempt_count BETWEEN 0 AND 32", name="ck_turn_delivery_attempts"),
        CheckConstraint(
            "(claim_token IS NULL AND claim_owner IS NULL AND claim_until IS NULL) OR "
            "(claim_token IS NOT NULL AND claim_owner IS NOT NULL AND claim_until IS NOT NULL)",
            name="ck_turn_claim_tuple",
        ),
        CheckConstraint(
            "(session_slot = 'successor' AND status = 'pending') OR "
            "(session_slot = 'active' AND status IN "
            "('pending','claimed','generating','typing','awaiting_delivery','retry')) OR "
            "(session_slot = 'terminal' AND status IN "
            "('delivered','handoff','cancelled','dead'))",
            name="ck_turn_slot_status",
        ),
        CheckConstraint(
            "(session_slot = 'terminal' AND terminal_at IS NOT NULL "
            "AND terminal_reason IS NOT NULL) OR "
            "(session_slot <> 'terminal' AND terminal_at IS NULL)",
            name="ck_turn_terminal_state",
        ),
        Index(
            "idx_turn_ready_claim",
            "agente_id",
            "status",
            "ready_at",
            "scheduling_sequence",
            "id",
        ),
        Index("idx_turn_session_status", "sessao_id", "status", "id"),
        Index("idx_turn_claim_lease", "status", "claim_until", "id"),
        Index("idx_turn_retention_terminal", "session_slot", "terminal_at"),  # R2-S1
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    turn_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"),
        nullable=False,
        default=lambda: str(uuid4()),
    )
    agente_id = Column(
        Integer,
        ForeignKey("agentes.id", name="fk_turn_agente", ondelete="CASCADE"),
        nullable=False,
    )
    sessao_id = Column(
        Integer,
        ForeignKey("sessoes.id", name="fk_turn_sessao", ondelete="RESTRICT"),
        nullable=False,
    )
    scheduler_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    arrival_sequence = Column(BIGINT(unsigned=True), nullable=False)
    scheduling_sequence = Column(BIGINT(unsigned=True), nullable=False)
    status = Column(
        Enum(
            "pending", "claimed", "generating", "typing", "awaiting_delivery",
            "retry", "delivered", "handoff", "cancelled", "dead",
        ),
        nullable=False,
        default="pending",
    )
    session_slot = Column(
        Enum("active", "successor", "terminal"), nullable=False, default="active"
    )
    active_session_guard = Column(
        Integer,
        Computed("CASE WHEN session_slot = 'active' THEN sessao_id ELSE NULL END", persisted=True),
        nullable=True,
    )
    successor_session_guard = Column(
        Integer,
        Computed(
            "CASE WHEN session_slot = 'successor' THEN sessao_id ELSE NULL END",
            persisted=True,
        ),
        nullable=True,
    )
    input_version = Column(BIGINT(unsigned=True), nullable=False, default=1)
    refresh_count = Column(SMALLINT(unsigned=True), nullable=False, default=0)
    current_revision_id = Column(
        BigInteger,
        ForeignKey(
            "turn_revisions.id",
            name="fk_turn_current_revision",
            ondelete="RESTRICT",
            use_alter=True,
        ),
        nullable=True,
    )
    final_revision_id = Column(
        BigInteger,
        ForeignKey(
            "turn_revisions.id",
            name="fk_turn_final_revision",
            ondelete="RESTRICT",
            use_alter=True,
        ),
        nullable=True,
    )
    claim_token = Column(CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True)
    claim_owner = Column(String(255, collation="ascii_bin"), nullable=True)
    claim_until = Column(DATETIME(fsp=6), nullable=True)
    ready_at = Column(DATETIME(fsp=6), nullable=False)
    retry_at = Column(DATETIME(fsp=6), nullable=True)
    delivery_attempt_count = Column(INTEGER(unsigned=True), nullable=False, default=0)
    terminal_reason = Column(String(100, collation="ascii_bin"), nullable=True)
    created_at = Column(
        DATETIME(fsp=6), nullable=False, server_default=text("CURRENT_TIMESTAMP(6)")
    )
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"),
    )
    terminal_at = Column(DATETIME(fsp=6), nullable=True)


class TurnRevision(Base):
    __tablename__ = "turn_revisions"
    __table_args__ = (
        UniqueConstraint("revision_key", name="uq_turn_revision_key"),
        UniqueConstraint("turn_id", "revision_number", name="uq_turn_revision_number"),
        UniqueConstraint("replacement_of_revision_id", name="uq_turn_revision_replacement"),
        UniqueConstraint(
            "id", "turn_id", "scheduler_epoch", name="uq_revision_authority_identity"
        ),
        CheckConstraint("revision_number BETWEEN 1 AND 2", name="ck_revision_number"),
        CheckConstraint("refresh_ordinal BETWEEN 0 AND 1", name="ck_revision_refresh_ordinal"),
        CheckConstraint(
            "(sealed_first_input_sequence IS NULL AND sealed_last_input_sequence IS NULL "
            "AND sealed_input_count IS NULL AND sealed_input_bytes IS NULL AND sealed_at IS NULL) OR "
            "(sealed_first_input_sequence IS NOT NULL AND sealed_last_input_sequence IS NOT NULL "
            "AND sealed_first_input_sequence <= sealed_last_input_sequence "
            "AND sealed_input_count > 0 AND sealed_input_bytes >= 0 AND sealed_at IS NOT NULL)",
            name="ck_revision_sealed_range",
        ),
        CheckConstraint(
            "typing_delay_ms IS NULL OR typing_delay_ms BETWEEN 2000 AND 12000",
            name="ck_revision_typing_delay",
        ),
        CheckConstraint(
            "typing_jitter_ms IS NULL OR typing_jitter_ms BETWEEN -1000 AND 1000",
            name="ck_revision_typing_jitter",
        ),
        Index("idx_revision_turn_status", "turn_id", "status", "id"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    revision_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"),
        nullable=False,
        default=lambda: str(uuid4()),
    )
    turn_id = Column(
        BigInteger,
        ForeignKey("conversation_turns.id", name="fk_revision_turn", ondelete="CASCADE"),
        nullable=False,
    )
    scheduler_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    revision_number = Column(SMALLINT(unsigned=True), nullable=False)
    refresh_ordinal = Column(SMALLINT(unsigned=True), nullable=False, default=0)
    replacement_of_revision_id = Column(
        BigInteger,
        ForeignKey("turn_revisions.id", name="fk_revision_replacement", ondelete="RESTRICT"),
        nullable=True,
    )
    status = Column(
        Enum(
            "collecting", "generating", "draft", "typing", "final_candidate",
            "admitted", "superseded", "cancelled", "dead",
        ),
        nullable=False,
        default="collecting",
    )
    sealed_first_input_sequence = Column(BIGINT(unsigned=True), nullable=True)
    sealed_last_input_sequence = Column(BIGINT(unsigned=True), nullable=True)
    sealed_input_count = Column(INTEGER(unsigned=True), nullable=True)
    sealed_input_bytes = Column(BIGINT(unsigned=True), nullable=True)
    draft_text = Column(Text, nullable=True)
    draft_text_bytes = Column(INTEGER(unsigned=True), nullable=True)
    needs_handoff = Column(Boolean, nullable=True)
    draft_kind = Column(
        Enum("generated", "soft_limit_fallback", "technical_fallback"),
        nullable=True,
    )
    typing_delay_ms = Column(INTEGER(unsigned=True), nullable=True)
    typing_jitter_ms = Column(SmallInteger, nullable=True)
    typing_deadline_at = Column(DATETIME(fsp=6), nullable=True)
    generation_completed_at = Column(DATETIME(fsp=6), nullable=True)
    sealed_at = Column(DATETIME(fsp=6), nullable=True)
    admitted_at = Column(DATETIME(fsp=6), nullable=True)
    superseded_reason = Column(String(100, collation="ascii_bin"), nullable=True)
    created_at = Column(
        DATETIME(fsp=6), nullable=False, server_default=text("CURRENT_TIMESTAMP(6)")
    )
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"),
    )


class TurnInputMembership(Base):
    __tablename__ = "turn_input_memberships"
    __table_args__ = (
        UniqueConstraint("mensagem_id", name="uq_turn_input_message"),
        UniqueConstraint("inbound_event_id", name="uq_turn_input_receipt"),
        UniqueConstraint(
            "sessao_id", "input_sequence", name="uq_turn_input_session_sequence"
        ),
        CheckConstraint("input_sequence > 0", name="ck_turn_input_sequence_positive"),
        CheckConstraint("content_bytes >= 0", name="ck_turn_input_bytes"),
        CheckConstraint(
            "(membership_state = 'active' AND assigned_revision_id IS NULL) OR "
            "(membership_state = 'sealed' AND assigned_revision_id IS NOT NULL) OR "
            "(membership_state = 'post_handoff_pending')",
            name="ck_turn_input_membership_state",
        ),
        Index("idx_turn_input_range", "turn_id", "input_sequence", "id"),
        Index("idx_turn_input_session_state", "sessao_id", "membership_state", "id"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    turn_id = Column(
        BigInteger,
        ForeignKey("conversation_turns.id", name="fk_turn_input_turn", ondelete="RESTRICT"),
        nullable=False,
    )
    sessao_id = Column(
        Integer,
        ForeignKey("sessoes.id", name="fk_turn_input_sessao", ondelete="RESTRICT"),
        nullable=False,
    )
    mensagem_id = Column(
        Integer,
        ForeignKey("mensagens.id", name="fk_turn_input_mensagem", ondelete="RESTRICT"),
        nullable=False,
    )
    inbound_event_id = Column(
        BigInteger,
        ForeignKey("inbound_events.id", name="fk_turn_input_receipt", ondelete="RESTRICT"),
        nullable=False,
    )
    input_sequence = Column(BIGINT(unsigned=True), nullable=False)
    content_bytes = Column(INTEGER(unsigned=True), nullable=False)
    membership_state = Column(
        Enum("active", "sealed", "post_handoff_pending"),
        nullable=False,
        default="active",
    )
    assigned_revision_id = Column(
        BigInteger,
        ForeignKey("turn_revisions.id", name="fk_turn_input_revision", ondelete="RESTRICT"),
        nullable=True,
    )
    created_at = Column(
        DATETIME(fsp=6), nullable=False, server_default=text("CURRENT_TIMESTAMP(6)")
    )


class GenerationAttempt(Base):
    __tablename__ = "generation_attempts"
    __table_args__ = (
        UniqueConstraint("attempt_key", name="uq_generation_attempt_key"),
        UniqueConstraint(
            "revision_id", "attempt_type", "attempt_ordinal", name="uq_generation_attempt_ordinal"
        ),
        UniqueConstraint("provider_generation_id", name="uq_generation_provider_id"),
        # Ticket07: one commercial ledger row backs at most one attempt.
        UniqueConstraint("consumo_llm_id", name="uq_generation_consumo_link"),
        CheckConstraint("attempt_ordinal BETWEEN 1 AND 8", name="ck_generation_attempt_ordinal"),
        CheckConstraint(
            "(outcome = 'started' AND outcome_recorded_at IS NULL) OR "
            "(outcome IN ('succeeded','failed','outcome_unknown') "
            "AND outcome_recorded_at IS NOT NULL)",
            name="ck_generation_outcome_timestamp",
        ),
        CheckConstraint("tokens_input IS NULL OR tokens_input >= 0", name="ck_generation_tokens_input"),
        CheckConstraint("tokens_output IS NULL OR tokens_output >= 0", name="ck_generation_tokens_output"),
        CheckConstraint("custo_creditos IS NULL OR custo_creditos >= 0", name="ck_generation_cost"),
        CheckConstraint(
            "(reconciled_at IS NULL AND reconciliation_actor IS NULL "
            "AND reconciliation_reason IS NULL) OR "
            "(reconciled_at IS NOT NULL AND reconciliation_actor IS NOT NULL "
            "AND reconciliation_reason IS NOT NULL)",
            name="ck_generation_reconciliation_audit",
        ),
        Index("idx_generation_turn_outcome", "turn_id", "outcome", "id"),
        Index("idx_generation_agent_unknown", "agente_id", "outcome", "id"),
        Index(
            "idx_generation_agent_reconciliation",
            "agente_id", "accounting_state", "reconciled_at", "id",
        ),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    attempt_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"),
        nullable=False,
        default=lambda: str(uuid4()),
    )
    agente_id = Column(
        Integer,
        ForeignKey("agentes.id", name="fk_generation_agente", ondelete="CASCADE"),
        nullable=False,
    )
    sessao_id = Column(
        Integer,
        ForeignKey("sessoes.id", name="fk_generation_sessao", ondelete="CASCADE"),
        nullable=False,
    )
    turn_id = Column(
        BigInteger,
        ForeignKey("conversation_turns.id", name="fk_generation_turn", ondelete="CASCADE"),
        nullable=False,
    )
    revision_id = Column(
        BigInteger,
        ForeignKey("turn_revisions.id", name="fk_generation_revision", ondelete="CASCADE"),
        nullable=False,
    )
    scheduler_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    lane_owner_token = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    attempt_type = Column(Enum("response", "summary"), nullable=False)
    attempt_ordinal = Column(SMALLINT(unsigned=True), nullable=False)
    outcome = Column(
        Enum("started", "succeeded", "failed", "outcome_unknown"),
        nullable=False,
        default="started",
    )
    accounting_state = Column(
        Enum("pending", "recorded", "reconciliation_required"),
        nullable=False,
        default="pending",
    )
    provider_generation_id = Column(String(255, collation="ascii_bin"), nullable=True)
    modelo_llm = Column(String(255), nullable=False)
    tokens_input = Column(INTEGER(unsigned=True), nullable=True)
    tokens_output = Column(INTEGER(unsigned=True), nullable=True)
    tokens_reasoning = Column(INTEGER(unsigned=True), nullable=True)
    tokens_cache_read = Column(INTEGER(unsigned=True), nullable=True)
    tokens_cache_write = Column(INTEGER(unsigned=True), nullable=True)
    custo_creditos = Column(Numeric(12, 8), nullable=True)
    consumo_llm_id = Column(
        BigInteger,
        ForeignKey("consumo_llm.id", name="fk_generation_consumo", ondelete="RESTRICT"),
        nullable=True,
    )
    safe_error_code = Column(String(100, collation="ascii_bin"), nullable=True)
    started_at = Column(
        DATETIME(fsp=6), nullable=False, server_default=text("CURRENT_TIMESTAMP(6)")
    )
    accounting_drain_until = Column(DATETIME(fsp=6), nullable=True)
    outcome_recorded_at = Column(DATETIME(fsp=6), nullable=True)
    reconciled_at = Column(DATETIME(fsp=6), nullable=True)
    reconciliation_actor = Column(String(255), nullable=True)
    reconciliation_reason = Column(String(500), nullable=True)


class HandoffAttempt(Base):
    __tablename__ = "handoff_attempts"
    __table_args__ = (
        UniqueConstraint("handoff_key", name="uq_handoff_attempt_key"),
        UniqueConstraint("sessao_id", "generation", name="uq_handoff_session_generation"),
        UniqueConstraint(
            "chatwoot_conversation_id",
            name="uq_handoff_chatwoot_conversation",
        ),
        UniqueConstraint(
            "id", "agente_id", "sessao_id", name="uq_handoff_authority_identity"
        ),
        CheckConstraint("generation > 0", name="ck_handoff_generation_positive"),
        CheckConstraint(
            "(status = 'pending' AND ready_at IS NULL AND terminal_at IS NULL) OR "
            "(status = 'ready' AND ready_at IS NOT NULL AND terminal_at IS NULL "
            " AND chatwoot_conversation_id IS NOT NULL) OR "
            "(status IN ('failed','superseded','cancelled') AND terminal_at IS NOT NULL)",
            name="ck_handoff_status_timestamps",
        ),
        Index("idx_handoff_attempts_session_status", "sessao_id", "status"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    handoff_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"),
        nullable=False,
        default=lambda: str(uuid4()),
    )
    sessao_id = Column(
        Integer,
        ForeignKey(
            "sessoes.id", name="fk_handoff_attempt_sessao", ondelete="CASCADE"
        ),
        nullable=False,
    )
    agente_id = Column(
        Integer,
        ForeignKey(
            "agentes.id", name="fk_handoff_attempt_agente", ondelete="CASCADE"
        ),
        nullable=False,
    )
    generation = Column(INTEGER(unsigned=True), nullable=False)
    status = Column(
        Enum("pending", "ready", "failed", "superseded", "cancelled"),
        nullable=False,
        default="pending",
    )
    chatwoot_inbox_id = Column(Integer, nullable=False)
    chatwoot_contact_id = Column(Integer, nullable=True)
    chatwoot_conversation_id = Column(Integer, nullable=True)
    created_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6)"),
    )
    ready_at = Column(DATETIME(fsp=6), nullable=True)
    terminal_at = Column(DATETIME(fsp=6), nullable=True)
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text(
            "CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
        ),
    )


class ScheduledHandoff(Base):
    """Exact scheduler authority captured by a terminal bot handoff."""

    __tablename__ = "scheduled_handoffs"
    __table_args__ = (
        UniqueConstraint("conversation_turn_id", name="uq_scheduled_handoff_turn"),
        UniqueConstraint("turn_revision_id", name="uq_scheduled_handoff_revision"),
        UniqueConstraint(
            "summary_generation_attempt_id", name="uq_scheduled_handoff_summary_attempt"
        ),
        CheckConstraint(
            "(summary_kind = 'generated' AND summary_generation_attempt_id IS NOT NULL) "
            "OR (summary_kind IN ('soft_limit_fallback','technical_fallback'))",
            name="ck_scheduled_handoff_summary_source",
        ),
        ForeignKeyConstraint(
            ["handoff_attempt_id", "agente_id", "sessao_id"],
            [
                "handoff_attempts.id",
                "handoff_attempts.agente_id",
                "handoff_attempts.sessao_id",
            ],
            name="fk_scheduled_handoff_attempt_authority",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["conversation_turn_id", "agente_id", "sessao_id", "scheduler_epoch"],
            [
                "conversation_turns.id",
                "conversation_turns.agente_id",
                "conversation_turns.sessao_id",
                "conversation_turns.scheduler_epoch",
            ],
            name="fk_scheduled_handoff_turn_authority",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["turn_revision_id", "conversation_turn_id", "scheduler_epoch"],
            [
                "turn_revisions.id",
                "turn_revisions.turn_id",
                "turn_revisions.scheduler_epoch",
            ],
            name="fk_scheduled_handoff_revision_authority",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        Index("idx_scheduled_handoff_session", "sessao_id", "handoff_attempt_id"),
    )

    handoff_attempt_id = Column(
        BigInteger,
        ForeignKey(
            "handoff_attempts.id",
            name="fk_scheduled_handoff_attempt",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        primary_key=True,
    )
    agente_id = Column(Integer, nullable=False)
    sessao_id = Column(Integer, nullable=False)
    conversation_turn_id = Column(BigInteger, nullable=False)
    turn_revision_id = Column(BigInteger, nullable=False)
    summary_generation_attempt_id = Column(
        BigInteger,
        ForeignKey(
            "generation_attempts.id",
            name="fk_scheduled_handoff_summary_attempt",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        nullable=True,
    )
    summary_kind = Column(
        MYSQL_ENUM(
            "generated",
            "soft_limit_fallback",
            "technical_fallback",
            charset="ascii",
            collation="ascii_bin",
        ),
        nullable=False,
    )
    scheduler_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    created_at = Column(
        DATETIME(fsp=6), nullable=False, server_default=text("CURRENT_TIMESTAMP(6)")
    )


class HandoffContextAssignment(Base):
    """Immutable exact late-input coverage for one private-note chunk."""

    __tablename__ = "handoff_context_assignments"
    __table_args__ = (
        UniqueConstraint(
            "handoff_attempt_id",
            "coverage_ordinal",
            name="uq_handoff_context_coverage_ordinal",
        ),
        ForeignKeyConstraint(
            ["handoff_attempt_id", "agente_id", "sessao_id"],
            [
                "handoff_attempts.id",
                "handoff_attempts.agente_id",
                "handoff_attempts.sessao_id",
            ],
            name="fk_handoff_context_attempt_authority",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        CheckConstraint(
            "coverage_ordinal > 0", name="ck_handoff_context_coverage_ordinal"
        ),
        Index(
            "idx_handoff_context_source",
            "handoff_attempt_id",
            "source_id",
            "coverage_ordinal",
        ),
    )

    turn_input_membership_id = Column(
        BigInteger,
        ForeignKey(
            "turn_input_memberships.id",
            name="fk_handoff_context_membership",
            ondelete="RESTRICT",
            onupdate="RESTRICT",
        ),
        primary_key=True,
    )
    handoff_attempt_id = Column(BigInteger, nullable=False)
    agente_id = Column(Integer, nullable=False)
    sessao_id = Column(Integer, nullable=False)
    coverage_ordinal = Column(INTEGER(unsigned=True), nullable=False)
    source_id = Column(String(255, collation="ascii_bin"), nullable=False)
    created_at = Column(
        DATETIME(fsp=6), nullable=False, server_default=text("CURRENT_TIMESTAMP(6)")
    )


class OutboxDelivery(Base):
    __tablename__ = "outbox_deliveries"
    __table_args__ = (
        UniqueConstraint("delivery_key", name="uq_outbox_delivery_key"),
        UniqueConstraint("dedupe_key", name="uq_outbox_dedupe_key"),
        UniqueConstraint("ordering_key", "stream_sequence", name="uq_outbox_stream_sequence"),
        CheckConstraint("stream_sequence >= 0", name="ck_outbox_sequence_nonnegative"),
        CheckConstraint(
            "(provider = 'evolution' AND stream_sequence > 0) OR "
            "(provider = 'chatwoot' AND stream_sequence = 0)",
            name="ck_outbox_provider_sequence",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_outbox_attempt_count"),
        CheckConstraint("ambiguity_count >= 0", name="ck_outbox_ambiguity_count"),
        CheckConstraint(
            "(lease_token IS NULL AND lock_owner IS NULL AND lock_until IS NULL "
            " AND status <> 'processing') OR "
            "(lease_token IS NOT NULL AND lock_owner IS NOT NULL AND lock_until IS NOT NULL "
            " AND status = 'processing')",
            name="ck_outbox_lease_tuple",
        ),
        CheckConstraint(
            "(status = 'blocked' AND depends_on_delivery_id IS NOT NULL "
            " AND next_attempt_at IS NULL AND expires_at IS NULL) OR "
            "(status IN ('pending','retry','processing') "
            " AND next_attempt_at IS NOT NULL AND expires_at IS NOT NULL) OR "
            "(status IN ('delivered','dead','cancelled'))",
            name="ck_outbox_schedule_state",
        ),
        CheckConstraint(
            "(delivery_class = 'automated' AND automated_effect IS NOT NULL "
            "AND conversation_turn_id IS NOT NULL AND turn_revision_id IS NOT NULL "
            "AND scheduler_epoch IS NOT NULL) OR "
            "(delivery_class <> 'automated' AND automated_effect IS NULL "
            "AND conversation_turn_id IS NULL AND turn_revision_id IS NULL "
            "AND scheduler_epoch IS NULL)",
            name="ck_outbox_delivery_authority_v2",
        ),
        Index("idx_outbox_claim", "status", "next_attempt_at", "priority", "id"),
        Index(
            "idx_outbox_predecessor",
            "ordering_key",
            "stream_sequence",
            "status",
        ),
        Index("idx_outbox_lease", "status", "lock_until"),
        Index("idx_outbox_dependency", "depends_on_delivery_id", "status"),
        Index("idx_outbox_session_status", "sessao_id", "status"),
        Index("idx_outbox_delivery_class_status", "delivery_class", "status", "id"),
        UniqueConstraint(
            "conversation_turn_id",
            "turn_revision_id",
            "automated_effect",
            name="uq_outbox_turn_revision_effect",
        ),
        ForeignKeyConstraint(
            ["conversation_turn_id", "agente_id", "sessao_id", "scheduler_epoch"],
            [
                "conversation_turns.id",
                "conversation_turns.agente_id",
                "conversation_turns.sessao_id",
                "conversation_turns.scheduler_epoch",
            ],
            name="fk_outbox_turn_authority",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["turn_revision_id", "conversation_turn_id", "scheduler_epoch"],
            ["turn_revisions.id", "turn_revisions.turn_id", "turn_revisions.scheduler_epoch"],
            name="fk_outbox_revision_authority",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["handoff_attempt_id", "agente_id", "sessao_id"],
            ["handoff_attempts.id", "handoff_attempts.agente_id", "handoff_attempts.sessao_id"],
            name="fk_outbox_handoff_authority",
            ondelete="RESTRICT",
        ),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    delivery_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"),
        nullable=False,
        default=lambda: str(uuid4()),
    )
    cutover_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    dedupe_key = Column(String(255, collation="ascii_bin"), nullable=False)
    provider = Column(Enum("evolution", "chatwoot"), nullable=False)
    operation = Column(String(100, collation="ascii_bin"), nullable=False)
    payload_version = Column(SMALLINT(unsigned=True), nullable=False)
    payload = Column(JSON, nullable=False)
    delivery_class = Column(
        Enum(
            "legacy_unknown",
            "legacy_automated",
            "human",
            "automated",
            "provider_setup_context",
        ),
        nullable=False,
        default="legacy_unknown",
        server_default="legacy_unknown",
    )
    automated_effect = Column(
        MYSQL_ENUM(
            "reply", "handoff_transition", charset="ascii", collation="ascii_bin"
        ),
        nullable=True,
    )
    scheduler_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True
    )
    conversation_turn_id = Column(
        BigInteger,
        ForeignKey(
            "conversation_turns.id",
            name="fk_outbox_conversation_turn",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    turn_revision_id = Column(
        BigInteger,
        ForeignKey(
            "turn_revisions.id",
            name="fk_outbox_turn_revision",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    agente_id = Column(
        Integer,
        ForeignKey("agentes.id", name="fk_outbox_agente", ondelete="SET NULL"),
        nullable=True,
    )
    sessao_id = Column(
        Integer,
        ForeignKey("sessoes.id", name="fk_outbox_sessao", ondelete="SET NULL"),
        nullable=True,
    )
    handoff_attempt_id = Column(
        BigInteger,
        ForeignKey(
            "handoff_attempts.id", name="fk_outbox_handoff", ondelete="SET NULL"
        ),
        nullable=True,
    )
    inbound_event_id = Column(
        BigInteger,
        ForeignKey(
            "inbound_events.id", name="fk_outbox_inbound", ondelete="SET NULL"
        ),
        nullable=True,
    )
    ordering_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    stream_sequence = Column(BIGINT(unsigned=True), nullable=False)
    depends_on_delivery_id = Column(
        BigInteger,
        ForeignKey(
            "outbox_deliveries.id", name="fk_outbox_dependency", ondelete="RESTRICT"
        ),
        nullable=True,
    )
    priority = Column(SMALLINT, nullable=False, default=0)
    status = Column(
        Enum(
            "blocked",
            "pending",
            "processing",
            "retry",
            "delivered",
            "dead",
            "cancelled",
        ),
        nullable=False,
    )
    attempt_count = Column(INTEGER(unsigned=True), nullable=False, default=0)
    ambiguity_count = Column(INTEGER(unsigned=True), nullable=False, default=0)
    next_attempt_at = Column(DATETIME(fsp=6), nullable=True)
    expires_at = Column(DATETIME(fsp=6), nullable=True)
    lease_token = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True
    )
    lock_owner = Column(String(255, collation="ascii_bin"), nullable=True)
    lock_until = Column(DATETIME(fsp=6), nullable=True)
    cancel_requested_at = Column(DATETIME(fsp=6), nullable=True)
    external_id = Column(String(255), nullable=True)
    last_http_status = Column(SMALLINT(unsigned=True), nullable=True)
    last_error_code = Column(String(100, collation="ascii_bin"), nullable=True)
    terminal_reason = Column(String(100, collation="ascii_bin"), nullable=True)
    created_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6)"),
    )
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text(
            "CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
        ),
    )
    delivered_at = Column(DATETIME(fsp=6), nullable=True)
    terminal_at = Column(DATETIME(fsp=6), nullable=True)


class LLMUsageReconciliation(Base):
    __tablename__ = "llm_usage_reconciliation"
    __table_args__ = (
        UniqueConstraint(
            "reconciliation_key", name="uq_usage_reconciliation_key"
        ),
        UniqueConstraint(
            "provider_generation_id",
            name="uq_usage_reconciliation_generation",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_usage_reconciliation_attempts"),
        Index("idx_usage_reconciliation_due", "status", "next_attempt_at", "id"),
        Index("idx_usage_reconciliation_session", "sessao_id"),
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    reconciliation_key = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"),
        nullable=False,
        default=lambda: str(uuid4()),
    )
    agente_id = Column(
        Integer,
        ForeignKey(
            "agentes.id",
            name="fk_usage_reconciliation_agente",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    sessao_id = Column(
        Integer,
        ForeignKey(
            "sessoes.id",
            name="fk_usage_reconciliation_sessao",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    modelo_llm = Column(String(255), nullable=False)
    # Provider-reported model id, verbatim; modelo_llm is the requested
    # allowlisted id used for billing and exact attempt linkage.
    modelo_llm_reportado = Column(String(255), nullable=True)
    tipo_chamada = Column(Enum("resposta", "resumo"), nullable=False)
    tokens_input = Column(INTEGER(unsigned=True), nullable=False, default=0)
    tokens_output = Column(INTEGER(unsigned=True), nullable=False, default=0)
    tokens_reasoning = Column(INTEGER(unsigned=True), nullable=True)
    tokens_cache_read = Column(INTEGER(unsigned=True), nullable=True)
    tokens_cache_write = Column(INTEGER(unsigned=True), nullable=True)
    custo_creditos = Column(Numeric(12, 8), nullable=False, default=0)
    provider_generation_id = Column(String(255), nullable=True)
    safe_error_code = Column(String(100, collation="ascii_bin"), nullable=False)
    status = Column(
        Enum("pending", "processing", "resolved", "dead"),
        nullable=False,
        default="pending",
    )
    attempt_count = Column(INTEGER(unsigned=True), nullable=False, default=0)
    next_attempt_at = Column(
        DATETIME(fsp=6),
        nullable=True,
        server_default=text("CURRENT_TIMESTAMP(6)"),
    )
    last_error_code = Column(String(100, collation="ascii_bin"), nullable=True)
    resolution_consumo_id = Column(
        BigInteger,
        ForeignKey(
            "consumo_llm.id",
            name="fk_usage_reconciliation_consumo",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    created_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6)"),
    )
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text(
            "CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
        ),
    )
    resolved_at = Column(DATETIME(fsp=6), nullable=True)


class DeliveryWorkerHeartbeat(Base):
    __tablename__ = "delivery_worker_heartbeats"
    __table_args__ = (
        CheckConstraint("current_lease_count >= 0", name="ck_worker_heartbeat_leases"),
        Index("idx_worker_heartbeats_last_seen", "last_seen_at"),
    )

    worker_id = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), primary_key=True
    )
    code_version = Column(String(100, collation="ascii_bin"), nullable=False)
    started_at = Column(DATETIME(fsp=6), nullable=False)
    last_seen_at = Column(DATETIME(fsp=6), nullable=False)
    current_lease_count = Column(INTEGER(unsigned=True), nullable=False, default=0)
    dispatch_enabled = Column(Boolean, nullable=False, default=False)
    configured_mode = Column(
        Enum("direct", "outbox"), nullable=False, default="direct"
    )
    configured_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True
    )
    provider_ready = Column(Boolean, nullable=False, default=False)
    runtime_compatible = Column(Boolean, nullable=False, default=False)


class ReplicaHeartbeat(Base):
    """T08-3: per-process compatibility heartbeat (app/delivery/turn worker)."""

    __tablename__ = "replica_heartbeats"
    __table_args__ = (
        CheckConstraint("schema_version > 0", name="ck_replica_heartbeat_schema_version"),
        CheckConstraint(
            "replica_kind IN ('app','delivery_worker','turn_worker')",
            name="ck_replica_heartbeat_kind",
        ),
        CheckConstraint(
            "delivery_mode IN ('direct','outbox')", name="ck_replica_heartbeat_delivery_mode"
        ),
        CheckConstraint(
            "webhook_auth_mode IN ('provisioning','enforced')",
            name="ck_replica_heartbeat_auth_mode",
        ),
        Index("idx_replica_heartbeats_last_seen", "last_seen_at"),
    )

    replica_id = Column(CHAR(36, charset="ascii", collation="ascii_bin"), primary_key=True)
    # VARCHAR + CHECK: a NOT NULL ENUM would default to its first member.
    replica_kind = Column(String(20, collation="ascii_bin"), nullable=False)
    code_version = Column(String(100, collation="ascii_bin"), nullable=False)
    schema_version = Column(INTEGER(unsigned=True), nullable=False)
    delivery_mode = Column(String(10, collation="ascii_bin"), nullable=False)
    delivery_epoch = Column(CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True)
    scheduler_enabled = Column(Boolean, nullable=False)
    scheduler_epoch = Column(CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True)
    turn_processor_enabled = Column(Boolean, nullable=False)
    dispatch_enabled = Column(Boolean, nullable=False)
    webhook_auth_mode = Column(String(20, collation="ascii_bin"), nullable=False)
    started_at = Column(DATETIME(fsp=6), nullable=False)
    last_seen_at = Column(DATETIME(fsp=6), nullable=False)
    stopped_at = Column(DATETIME(fsp=6), nullable=True)


class DeliveryOperatorAction(Base):
    __tablename__ = "delivery_operator_actions"
    __table_args__ = (Index("idx_operator_actions_delivery", "delivery_id", "created_at"),)

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    delivery_id = Column(
        BigInteger,
        ForeignKey(
            "outbox_deliveries.id",
            name="fk_operator_action_delivery",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    action = Column(String(50, collation="ascii_bin"), nullable=False)
    actor = Column(String(255), nullable=False)
    reason = Column(String(500), nullable=False)
    snapshot_cutover_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=True
    )
    snapshot_status = Column(String(20, collation="ascii_bin"), nullable=True)
    snapshot_attempt_count = Column(INTEGER(unsigned=True), nullable=True)
    snapshot_ambiguity_count = Column(INTEGER(unsigned=True), nullable=True)
    snapshot_terminal_at = Column(DATETIME(fsp=6), nullable=True)
    created_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP(6)"),
    )


class DeliveryRuntimeState(Base):
    __tablename__ = "delivery_runtime_state"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_delivery_runtime_singleton"),
        CheckConstraint("version > 0", name="ck_delivery_runtime_version"),
    )

    id = Column(SmallInteger, primary_key=True, default=1)
    mode = Column(Enum("direct", "outbox", "frozen"), nullable=False, default="direct")
    cutover_epoch = Column(
        CHAR(36, charset="ascii", collation="ascii_bin"), nullable=False
    )
    version = Column(BIGINT(unsigned=True), nullable=False, default=1)
    changed_by = Column(String(255), nullable=False)
    change_reason = Column(String(500), nullable=False)
    updated_at = Column(
        DATETIME(fsp=6),
        nullable=False,
        server_default=text(
            "CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)"
        ),
    )


class SchedulerRetentionProgress(Base):
    """S1: leaf checkpoint; no lifecycle authority and no FK/trigger."""

    __tablename__ = "scheduler_retention_progress"
    id = Column(Integer, primary_key=True, autoincrement=False)
    last_turn_id = Column(BIGINT(unsigned=True), nullable=False)
    version = Column(BIGINT(unsigned=True), nullable=False)
