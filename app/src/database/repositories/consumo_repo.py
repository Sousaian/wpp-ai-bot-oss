from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal

from loguru import logger
from sqlalchemy import case, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import db_errors
from src.database.models import Agente, ConsumoLLM, LLMUsageReconciliation
from src.database.repositories import usage_reconciliation_repo


TipoChamada = Literal["resposta", "resumo"]


class UsagePersistenceInvariantError(RuntimeError):
    """A successful generation could not be made durably accountable."""


class UsageConflictError(UsagePersistenceInvariantError):
    pass


@dataclass(frozen=True)
class UsagePersistenceResult:
    consumo: ConsumoLLM | None
    reconciliation: LLMUsageReconciliation | None

    @property
    def reconciliacao_pendente(self) -> bool:
        return self.reconciliation is not None


def _inicio_mes(referencia: datetime | None = None) -> datetime:
    base = referencia or datetime.utcnow()
    return datetime(base.year, base.month, 1)


async def registrar(
    db: AsyncSession,
    *,
    agente_id: int,
    sessao_id: int | None,
    modelo_llm: str,
    tipo_chamada: TipoChamada,
    tokens_input: int,
    tokens_output: int,
    custo_creditos: Decimal,
    tokens_reasoning: int | None = None,
    tokens_cache_read: int | None = None,
    tokens_cache_write: int | None = None,
    provider_generation_id: str | None = None,
    modelo_llm_reportado: str | None = None,
) -> ConsumoLLM:
    consumo = ConsumoLLM(
        agente_id=agente_id,
        sessao_id=sessao_id,
        modelo_llm=modelo_llm,
        modelo_llm_reportado=modelo_llm_reportado,
        tipo_chamada=tipo_chamada,
        tokens_input=tokens_input,
        tokens_output=tokens_output,
        tokens_reasoning=tokens_reasoning,
        tokens_cache_read=tokens_cache_read,
        tokens_cache_write=tokens_cache_write,
        custo_creditos=custo_creditos,
        provider_generation_id=provider_generation_id,
    )
    db.add(consumo)
    await db.flush()
    logger.debug(
        "Consumo LLM registrado | agente={} sessao={} tipo={} custo={}",
        agente_id,
        sessao_id,
        tipo_chamada,
        custo_creditos,
    )
    return consumo


def _consumo_confere(
    consumo: ConsumoLLM,
    *,
    agente_id: int,
    sessao_id: int | None,
    modelo_llm: str,
    tipo_chamada: TipoChamada,
    tokens_input: int,
    tokens_output: int,
    custo_creditos: Decimal,
    tokens_reasoning: int | None,
    tokens_cache_read: int | None,
    tokens_cache_write: int | None,
    modelo_llm_reportado: str | None,
) -> bool:
    return (
        consumo.agente_id == agente_id
        and consumo.sessao_id == sessao_id
        and consumo.modelo_llm == modelo_llm
        and consumo.modelo_llm_reportado == modelo_llm_reportado
        and consumo.tipo_chamada == tipo_chamada
        and consumo.tokens_input == tokens_input
        and consumo.tokens_output == tokens_output
        and consumo.tokens_reasoning == tokens_reasoning
        and consumo.tokens_cache_read == tokens_cache_read
        and consumo.tokens_cache_write == tokens_cache_write
        and Decimal(consumo.custo_creditos) == Decimal(custo_creditos)
    )


async def registrar_ou_reconciliar(
    db: AsyncSession,
    *,
    agente_id: int,
    sessao_id: int | None,
    modelo_llm: str,
    tipo_chamada: TipoChamada,
    tokens_input: int,
    tokens_output: int,
    custo_creditos: Decimal,
    tokens_reasoning: int | None = None,
    tokens_cache_read: int | None = None,
    tokens_cache_write: int | None = None,
    provider_generation_id: str | None = None,
    modelo_llm_reportado: str | None = None,
) -> UsagePersistenceResult:
    """Persist usage or stage the same normalized data for reconciliation.

    The consumption insert is isolated by a savepoint. A duplicate provider
    generation is accepted only when every normalized field matches. Other
    row-level database failures produce a durable reconciliation row in the
    caller's transaction; failure to persist that fallback propagates.
    A deadlock, lock wait timeout or lost savepoint means the transaction is
    gone: it is re-raised so the caller restarts the whole transaction.
    """

    try:
        async with db.begin_nested():
            consumo = await registrar(
                db,
                agente_id=agente_id,
                sessao_id=sessao_id,
                modelo_llm=modelo_llm,
                tipo_chamada=tipo_chamada,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                custo_creditos=custo_creditos,
                tokens_reasoning=tokens_reasoning,
                tokens_cache_read=tokens_cache_read,
                tokens_cache_write=tokens_cache_write,
                provider_generation_id=provider_generation_id,
                modelo_llm_reportado=modelo_llm_reportado,
            )
        return UsagePersistenceResult(consumo=consumo, reconciliation=None)
    except SQLAlchemyError as exc:
        if db_errors.transaction_lost(exc):
            raise
        if provider_generation_id is not None:
            result = await db.execute(
                select(ConsumoLLM).where(
                    ConsumoLLM.provider_generation_id == provider_generation_id
                )
            )
            existing = result.scalar_one_or_none()
            if existing is not None:
                if not _consumo_confere(
                    existing,
                    agente_id=agente_id,
                    sessao_id=sessao_id,
                    modelo_llm=modelo_llm,
                    tipo_chamada=tipo_chamada,
                    tokens_input=tokens_input,
                    tokens_output=tokens_output,
                    custo_creditos=custo_creditos,
                    tokens_reasoning=tokens_reasoning,
                    tokens_cache_read=tokens_cache_read,
                    tokens_cache_write=tokens_cache_write,
                    modelo_llm_reportado=modelo_llm_reportado,
                ):
                    raise UsageConflictError(
                        "provider generation has conflicting persisted usage"
                    )
                return UsagePersistenceResult(consumo=existing, reconciliation=None)

        try:
            reconciliation = await usage_reconciliation_repo.record(
                db,
                agente_id=agente_id,
                sessao_id=sessao_id,
                modelo_llm=modelo_llm,
                tipo_chamada=tipo_chamada,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                custo_creditos=custo_creditos,
                tokens_reasoning=tokens_reasoning,
                tokens_cache_read=tokens_cache_read,
                tokens_cache_write=tokens_cache_write,
                provider_generation_id=provider_generation_id,
                modelo_llm_reportado=modelo_llm_reportado,
                safe_error_code="consumo_insert_failed",
            )
        except Exception as exc:
            lost_transaction = db_errors.transaction_lost_dbapi_error(exc)
            if lost_transaction is not None:
                raise lost_transaction
            raise UsagePersistenceInvariantError(
                "successful generation has no durable usage record"
            ) from exc
        logger.error(
            "Consumo LLM aguardando reconciliação | agente={} sessao={} tipo={}",
            agente_id,
            sessao_id,
            tipo_chamada,
        )
        return UsagePersistenceResult(consumo=None, reconciliation=reconciliation)


async def total_do_mes(
    agente_id: int,
    db: AsyncSession,
    referencia: datetime | None = None,
) -> Decimal:
    inicio_mes = _inicio_mes(referencia)
    consumo_stmt = select(func.coalesce(func.sum(ConsumoLLM.custo_creditos), 0)).where(
        ConsumoLLM.agente_id == agente_id,
        ConsumoLLM.criado_em >= inicio_mes,
    )
    pending_stmt = select(
        func.coalesce(func.sum(LLMUsageReconciliation.custo_creditos), 0)
    ).where(
        LLMUsageReconciliation.agente_id == agente_id,
        LLMUsageReconciliation.created_at >= inicio_mes,
        LLMUsageReconciliation.status != "resolved",
    )
    consumo_total = (await db.execute(consumo_stmt)).scalar_one()
    pending_total = (await db.execute(pending_stmt)).scalar_one()
    return Decimal(str(consumo_total or 0)) + Decimal(str(pending_total or 0))


async def resumo_por_periodo(
    agente_id: int,
    db: AsyncSession,
    inicio: datetime,
    fim: datetime,
) -> dict[str, int | Decimal | datetime]:
    stmt = select(
        func.count(ConsumoLLM.id),
        func.coalesce(func.sum(ConsumoLLM.tokens_input), 0),
        func.coalesce(func.sum(ConsumoLLM.tokens_output), 0),
        func.coalesce(func.sum(ConsumoLLM.tokens_reasoning), 0),
        func.coalesce(func.sum(ConsumoLLM.tokens_cache_read), 0),
        func.coalesce(func.sum(ConsumoLLM.tokens_cache_write), 0),
        func.coalesce(func.sum(ConsumoLLM.custo_creditos), 0),
        func.coalesce(
            func.sum(case((ConsumoLLM.tipo_chamada == "resposta", 1), else_=0)),
            0,
        ),
        func.coalesce(
            func.sum(case((ConsumoLLM.tipo_chamada == "resumo", 1), else_=0)),
            0,
        ),
    ).where(
        ConsumoLLM.agente_id == agente_id,
        ConsumoLLM.criado_em >= inicio,
        ConsumoLLM.criado_em <= fim,
    )
    result = await db.execute(stmt)
    (
        chamadas_total,
        tokens_input,
        tokens_output,
        tokens_reasoning,
        tokens_cache_read,
        tokens_cache_write,
        custo_total,
        chamadas_resposta,
        chamadas_resumo,
    ) = result.one()

    pending_stmt = select(
        func.count(LLMUsageReconciliation.id),
        func.coalesce(func.sum(LLMUsageReconciliation.tokens_input), 0),
        func.coalesce(func.sum(LLMUsageReconciliation.tokens_output), 0),
        func.coalesce(func.sum(LLMUsageReconciliation.tokens_reasoning), 0),
        func.coalesce(func.sum(LLMUsageReconciliation.tokens_cache_read), 0),
        func.coalesce(func.sum(LLMUsageReconciliation.tokens_cache_write), 0),
        func.coalesce(func.sum(LLMUsageReconciliation.custo_creditos), 0),
        func.coalesce(
            func.sum(
                case((LLMUsageReconciliation.tipo_chamada == "resposta", 1), else_=0)
            ),
            0,
        ),
        func.coalesce(
            func.sum(
                case((LLMUsageReconciliation.tipo_chamada == "resumo", 1), else_=0)
            ),
            0,
        ),
    ).where(
        LLMUsageReconciliation.agente_id == agente_id,
        LLMUsageReconciliation.created_at >= inicio,
        LLMUsageReconciliation.created_at <= fim,
        LLMUsageReconciliation.status != "resolved",
    )
    pending_result = await db.execute(pending_stmt)
    (
        reconciliacoes_pendentes,
        pending_tokens_input,
        pending_tokens_output,
        pending_tokens_reasoning,
        pending_tokens_cache_read,
        pending_tokens_cache_write,
        pending_custo,
        pending_respostas,
        pending_resumos,
    ) = pending_result.one()

    confirmed_cost = Decimal(str(custo_total or 0))
    unresolved_cost = Decimal(str(pending_custo or 0))

    return {
        "agente_id": agente_id,
        "inicio": inicio,
        "fim": fim,
        "chamadas_total": int(chamadas_total or 0) + int(reconciliacoes_pendentes or 0),
        "chamadas_resposta": int(chamadas_resposta or 0) + int(pending_respostas or 0),
        "chamadas_resumo": int(chamadas_resumo or 0) + int(pending_resumos or 0),
        "tokens_input_total": int(tokens_input or 0) + int(pending_tokens_input or 0),
        "tokens_output_total": int(tokens_output or 0) + int(pending_tokens_output or 0),
        "tokens_reasoning_total": int(tokens_reasoning or 0) + int(pending_tokens_reasoning or 0),
        "tokens_cache_read_total": int(tokens_cache_read or 0) + int(pending_tokens_cache_read or 0),
        "tokens_cache_write_total": int(tokens_cache_write or 0) + int(pending_tokens_cache_write or 0),
        "custo_total_creditos": confirmed_cost + unresolved_cost,
        "reconciliacoes_pendentes": int(reconciliacoes_pendentes or 0),
        "custo_pendente_reconciliacao_creditos": unresolved_cost,
    }


async def atingiu_limite_mensal(
    agente_id: int,
    db: AsyncSession,
    referencia: datetime | None = None,
) -> bool:
    agente = await db.get(Agente, agente_id)
    if not agente or agente.limite_mensal_creditos is None:
        return False

    total_mes = await total_do_mes(agente_id=agente_id, db=db, referencia=referencia)
    limite = Decimal(str(agente.limite_mensal_creditos))
    return total_mes >= limite
