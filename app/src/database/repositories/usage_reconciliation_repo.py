from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import db_errors
from src.database.models import ConsumoLLM, LLMUsageReconciliation


class UsageReconciliationConflictError(ValueError):
    pass


@dataclass(frozen=True)
class ReconciliationBatchResult:
    examined: int
    resolved: int
    retried: int
    dead: int


def _same_usage(row: LLMUsageReconciliation, values: dict[str, object]) -> bool:
    fields = (
        "agente_id",
        "sessao_id",
        "modelo_llm",
        "modelo_llm_reportado",
        "tipo_chamada",
        "tokens_input",
        "tokens_output",
        "tokens_reasoning",
        "tokens_cache_read",
        "tokens_cache_write",
    )
    if any(getattr(row, field) != values[field] for field in fields):
        return False
    return Decimal(row.custo_creditos) == Decimal(values["custo_creditos"])


async def record(
    db: AsyncSession,
    *,
    agente_id: int,
    sessao_id: int | None,
    modelo_llm: str,
    tipo_chamada: str,
    tokens_input: int,
    tokens_output: int,
    custo_creditos: Decimal,
    safe_error_code: str,
    tokens_reasoning: int | None = None,
    tokens_cache_read: int | None = None,
    tokens_cache_write: int | None = None,
    provider_generation_id: str | None = None,
    modelo_llm_reportado: str | None = None,
) -> LLMUsageReconciliation:
    values: dict[str, object] = {
        "agente_id": agente_id,
        "sessao_id": sessao_id,
        "modelo_llm": modelo_llm,
        "modelo_llm_reportado": modelo_llm_reportado,
        "tipo_chamada": tipo_chamada,
        "tokens_input": tokens_input,
        "tokens_output": tokens_output,
        "tokens_reasoning": tokens_reasoning,
        "tokens_cache_read": tokens_cache_read,
        "tokens_cache_write": tokens_cache_write,
        "custo_creditos": custo_creditos,
    }
    row = LLMUsageReconciliation(
        reconciliation_key=str(uuid4()),
        safe_error_code=safe_error_code,
        provider_generation_id=provider_generation_id,
        **values,
    )
    try:
        async with db.begin_nested():
            db.add(row)
            await db.flush()
        return row
    except IntegrityError:
        if provider_generation_id is None:
            raise
        result = await db.execute(
            select(LLMUsageReconciliation).where(
                LLMUsageReconciliation.provider_generation_id
                == provider_generation_id
            )
        )
        existing = result.scalar_one_or_none()
        if existing is None:
            raise
        if not _same_usage(existing, values):
            raise UsageReconciliationConflictError(
                "provider generation has conflicting reconciliation usage"
            )
        return existing


async def get_by_generation_id(
    db: AsyncSession,
    provider_generation_id: str,
) -> LLMUsageReconciliation | None:
    result = await db.execute(
        select(LLMUsageReconciliation).where(
            LLMUsageReconciliation.provider_generation_id == provider_generation_id
        )
    )
    return result.scalar_one_or_none()


def _same_consumption(
    consumo: ConsumoLLM,
    reconciliation: LLMUsageReconciliation,
) -> bool:
    return (
        consumo.agente_id == reconciliation.agente_id
        and consumo.sessao_id == reconciliation.sessao_id
        and consumo.modelo_llm == reconciliation.modelo_llm
        and consumo.modelo_llm_reportado == reconciliation.modelo_llm_reportado
        and consumo.tipo_chamada == reconciliation.tipo_chamada
        and consumo.tokens_input == reconciliation.tokens_input
        and consumo.tokens_output == reconciliation.tokens_output
        and consumo.tokens_reasoning == reconciliation.tokens_reasoning
        and consumo.tokens_cache_read == reconciliation.tokens_cache_read
        and consumo.tokens_cache_write == reconciliation.tokens_cache_write
        and Decimal(consumo.custo_creditos)
        == Decimal(reconciliation.custo_creditos)
    )


async def _persist_consumption(
    db: AsyncSession,
    reconciliation: LLMUsageReconciliation,
) -> ConsumoLLM:
    consumo = ConsumoLLM(
        agente_id=reconciliation.agente_id,
        sessao_id=reconciliation.sessao_id,
        modelo_llm=reconciliation.modelo_llm,
        modelo_llm_reportado=reconciliation.modelo_llm_reportado,
        tipo_chamada=reconciliation.tipo_chamada,
        tokens_input=reconciliation.tokens_input,
        tokens_output=reconciliation.tokens_output,
        tokens_reasoning=reconciliation.tokens_reasoning,
        tokens_cache_read=reconciliation.tokens_cache_read,
        tokens_cache_write=reconciliation.tokens_cache_write,
        custo_creditos=reconciliation.custo_creditos,
        provider_generation_id=reconciliation.provider_generation_id,
    )
    try:
        async with db.begin_nested():
            db.add(consumo)
            await db.flush()
        return consumo
    except IntegrityError:
        if reconciliation.provider_generation_id is None:
            raise
        result = await db.execute(
            select(ConsumoLLM).where(
                ConsumoLLM.provider_generation_id
                == reconciliation.provider_generation_id
            )
        )
        existing = result.scalar_one_or_none()
        if existing is None:
            raise
        if not _same_consumption(existing, reconciliation):
            raise UsageReconciliationConflictError(
                "provider generation has conflicting persisted usage"
            )
        return existing


def _retry_delay(attempt_count: int) -> timedelta:
    seconds = (5, 15, 45, 120, 300, 900, 1800, 3600)
    return timedelta(seconds=seconds[min(max(attempt_count - 1, 0), len(seconds) - 1)])


async def reconcile_due(
    db: AsyncSession,
    *,
    limit: int = 100,
    max_attempts: int = 8,
) -> ReconciliationBatchResult:
    """Convert a bounded due batch into verified consumption rows.

    Reconciliation performs no network I/O, so rows stay locked for the short
    database-only conversion transaction rather than acquiring a durable
    worker lease. Callers commit the returned mutations atomically.
    """

    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")

    now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
    result = await db.execute(
        select(LLMUsageReconciliation)
        .where(
            LLMUsageReconciliation.status == "pending",
            LLMUsageReconciliation.next_attempt_at.is_not(None),
            LLMUsageReconciliation.next_attempt_at <= now,
        )
        .order_by(
            LLMUsageReconciliation.next_attempt_at,
            LLMUsageReconciliation.id,
        )
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = list(result.scalars().all())
    resolved = 0
    retried = 0
    dead = 0

    for row in rows:
        row.attempt_count += 1
        try:
            consumo = await _persist_consumption(db, row)
        except UsageReconciliationConflictError:
            row.status = "dead"
            row.next_attempt_at = None
            row.last_error_code = "generation_usage_conflict"
            dead += 1
        except SQLAlchemyError as exc:
            # A lost transaction also released every row lock taken above;
            # never keep mutating rows in a new implicit transaction.
            if db_errors.transaction_lost(exc):
                raise
            row.last_error_code = "consumo_reconcile_failed"
            if row.attempt_count >= max_attempts:
                row.status = "dead"
                row.next_attempt_at = None
                dead += 1
            else:
                row.status = "pending"
                row.next_attempt_at = now + _retry_delay(row.attempt_count)
                retried += 1
        else:
            row.status = "resolved"
            row.next_attempt_at = None
            row.last_error_code = None
            row.resolution_consumo_id = consumo.id
            row.resolved_at = now
            resolved += 1

    await db.flush()
    return ReconciliationBatchResult(
        examined=len(rows),
        resolved=resolved,
        retried=retried,
        dead=dead,
    )
