from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.services.auth import auditoria
from src.services.auth.contexto import Principal, get_auth_sessions, proibido
from src.services.auth.rbac import CAMPOS_OPERACIONAIS, Permissao as P, exigir, exigir_rota_declarada, pode
from src.database.connection import get_db
from src.database import db_errors
from src.database.repositories import (
    agente_repo,
    consumo_repo,
    lifecycle_repo,
    outbox_repo,
)


MODELOS_PERMITIDOS: dict[str, set[str]] = {
    "economico": {
        "openai/gpt-4o-mini",
        "google/gemini-2.5-flash",
    },
    "padrao": {
        "anthropic/claude-haiku-4.5",
    },
    "premium": {
        "anthropic/claude-sonnet-4",
    },
}
MODELOS_PERMITIDOS_FLAT = set().union(*MODELOS_PERMITIDOS.values())


class AgenteBaseSchema(BaseModel):
    nome: str = Field(min_length=1, max_length=255)
    instance_name: str = Field(min_length=1, max_length=255)
    system_prompt: str = Field(min_length=1)
    modelo_openai: str = Field(default="gpt-4o-mini", min_length=1, max_length=100)
    modelo_llm: str | None = Field(default=None, min_length=1, max_length=255)
    numero_notificacao: str = Field(min_length=1, max_length=50)
    chatwoot_inbox_id: int | None = None
    limite_mensal_creditos: Decimal | None = Field(default=None, ge=Decimal("0"))
    max_historico: int = Field(default=20, ge=1)
    ativo: bool = True


class AgenteCreateSchema(AgenteBaseSchema):
    pass


class AgenteUpdateSchema(BaseModel):
    nome: str | None = Field(default=None, min_length=1, max_length=255)
    instance_name: str | None = Field(default=None, min_length=1, max_length=255)
    system_prompt: str | None = Field(default=None, min_length=1)
    modelo_openai: str | None = Field(default=None, min_length=1, max_length=100)
    modelo_llm: str | None = Field(default=None, min_length=1, max_length=255)
    numero_notificacao: str | None = Field(default=None, min_length=1, max_length=50)
    chatwoot_inbox_id: int | None = None
    limite_mensal_creditos: Decimal | None = Field(default=None, ge=Decimal("0"))
    max_historico: int | None = Field(default=None, ge=1)
    ativo: bool | None = None


class AgenteResponseSchema(AgenteBaseSchema):
    id: int
    chatwoot_inbox_id: int | None = None
    criado_em: datetime | None = None
    atualizado_em: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class AgenteConsumoResponseSchema(BaseModel):
    agente_id: int
    inicio: datetime
    fim: datetime
    chamadas_total: int
    chamadas_resposta: int
    chamadas_resumo: int
    tokens_input_total: int
    tokens_output_total: int
    tokens_reasoning_total: int
    tokens_cache_read_total: int
    tokens_cache_write_total: int
    custo_total_creditos: Decimal
    reconciliacoes_pendentes: int
    custo_pendente_reconciliacao_creditos: Decimal
    custo_mes_atual_creditos: Decimal
    limite_mensal_creditos: Decimal | None
    percentual_franquia_mensal: Decimal | None


# AUTH-2: every route declares exactly one ``exigir(...)``; the router guard
# refuses (403) a route added without one (fail closed).
router = APIRouter(
    prefix="/agentes", tags=["agentes"], dependencies=[Depends(exigir_rota_declarada)]
)


def _inicio_mes_atual() -> datetime:
    now = datetime.utcnow()
    return datetime(now.year, now.month, 1)


def _percentual_franquia(custo_mes: Decimal, limite: Decimal | None) -> Decimal | None:
    if limite is None or limite <= Decimal("0"):
        return None
    return (custo_mes / limite) * Decimal("100")


def _normalizar_modelo_llm(modelo_llm: str | None, modelo_openai: str | None) -> str:
    if modelo_llm and modelo_llm.strip():
        return modelo_llm.strip()
    fallback = (modelo_openai or "gpt-4o-mini").strip()
    if "/" not in fallback:
        return f"openai/{fallback}"
    return fallback


def _validar_modelo_llm(modelo_llm: str) -> None:
    if modelo_llm not in MODELOS_PERMITIDOS_FLAT:
        raise HTTPException(status_code=422, detail="modelo_llm nao permitido para cadastro")


def _visivel(principal: Principal, agente) -> AgenteResponseSchema:
    """Without sessao.ler_telefone (leitura) the legacy phone is masked too."""

    resposta = AgenteResponseSchema.model_validate(agente)
    if not pode(principal, P.SESSAO_LER_TELEFONE):
        resposta = resposta.model_copy(
            update={"numero_notificacao": auditoria.mascarar_telefone(resposta.numero_notificacao)}
        )
    return resposta


@router.get("", response_model=list[AgenteResponseSchema])
async def listar_agentes(
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(exigir(P.AGENTE_LER)),
):
    return [_visivel(principal, agente) for agente in await agente_repo.get_all(db)]


@router.post("", response_model=AgenteResponseSchema, status_code=status.HTTP_201_CREATED)
async def criar_agente(
    dados: AgenteCreateSchema,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(exigir(P.AGENTE_CRIAR)),
):
    payload = dados.model_dump()
    payload["modelo_llm"] = _normalizar_modelo_llm(
        modelo_llm=payload.get("modelo_llm"),
        modelo_openai=payload.get("modelo_openai"),
    )
    _validar_modelo_llm(payload["modelo_llm"])

    try:
        agente = await agente_repo.create(db, payload)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="instance_name ja cadastrado")
    await auditoria.registrar(
        db, "agente_criado", **auditoria.ator(principal), alvo_tipo="agente", alvo_id=agente.id,
        detalhe={"campos": sorted(payload)},
    )
    return agente


@router.get("/{agente_id}", response_model=AgenteResponseSchema)
async def detalhar_agente(
    agente_id: int,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(exigir(P.AGENTE_LER)),
):
    agente = await agente_repo.get_by_id(db, agente_id)
    if not agente:
        raise HTTPException(status_code=404, detail="Agente nao encontrado")
    return _visivel(principal, agente)


@router.put("/{agente_id}", response_model=AgenteResponseSchema)
async def atualizar_agente(
    agente_id: int,
    dados: AgenteUpdateSchema,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(exigir(P.AGENTE_EDITAR, P.AGENTE_EDITAR_OPERACIONAL)),
    sessions=Depends(get_auth_sessions),
):
    payload = dados.model_dump(exclude_unset=True)
    if not payload:
        raise HTTPException(status_code=400, detail="Nenhum campo enviado para atualizacao")
    # AUTH-2: operador edits only system_prompt/ativo; any other field needs
    # agente.editar. Checked before any lock or write.
    if not pode(principal, P.AGENTE_EDITAR) and set(payload) - CAMPOS_OPERACIONAIS:
        await auditoria.registrar_isolado(
            sessions, "acesso_negado", resultado="negado", **auditoria.ator(principal),
            detalhe={"metodo": "PUT", "rota": "/agentes/{agente_id}", "permissoes": [P.AGENTE_EDITAR.value],
                     "campos": sorted(set(payload) - CAMPOS_OPERACIONAIS)},
        )
        raise proibido()

    def prepare(agente_atual, values: dict) -> dict:
        if "modelo_llm" in values or "modelo_openai" in values:
            values["modelo_llm"] = _normalizar_modelo_llm(
                modelo_llm=values.get("modelo_llm"),
                modelo_openai=values.get("modelo_openai") or agente_atual.modelo_openai,
            )
            _validar_modelo_llm(values["modelo_llm"])
        return values

    # Fenced update (ticket07): disabling cancels the agent's scheduled work
    # under runtime → scheduler → agent → lane → sessions → turns → … locks.
    try:
        result = await lifecycle_repo.run_with_deadlock_retry(
            db,
            lambda tx: lifecycle_repo.update_agent(
                tx, agente_id=agente_id, payload=payload, prepare=prepare
            ),
        )
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="instance_name ja cadastrado")
    except lifecycle_repo.LifecycleRefused as exc:
        await db.rollback()
        raise HTTPException(status_code=409, detail=f"Agente: {exc}") from None

    if result is None:
        raise HTTPException(status_code=404, detail="Agente nao encontrado")
    await auditoria.registrar(
        db, "agente_atualizado", **auditoria.ator(principal), alvo_tipo="agente", alvo_id=agente_id,
        detalhe={"campos": sorted(payload)},
    )
    return result.agent


@router.delete("/{agente_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deletar_agente(
    agente_id: int,
    db: AsyncSession = Depends(get_db),
    principal: Principal = Depends(exigir(P.AGENTE_EXCLUIR)),
):
    def runtime_guard(state) -> None:
        compatible = bool(
            (settings.delivery_mode == "direct" and state.mode == "direct")
            or (
                settings.delivery_mode == "outbox"
                and state.mode == "outbox"
                and settings.delivery_cutover_epoch
                and state.cutover_epoch == settings.delivery_cutover_epoch
            )
        )
        if not compatible:
            raise HTTPException(status_code=503, detail="Delivery runtime indisponivel")

    # S3: delete_agent opens its own READ COMMITTED transaction and checks the
    # locked runtime row (S) through the guard before anything else.
    async def operation(tx: AsyncSession):
        return await lifecycle_repo.delete_agent(
            tx, agente_id=agente_id, runtime_guard=runtime_guard
        )

    try:
        result = await lifecycle_repo.run_with_deadlock_retry(db, operation)
    except outbox_repo.ActiveDeliveriesError:
        await db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Agente possui entregas pendentes",
        ) from None
    except lifecycle_repo.LifecycleRefused as exc:
        await db.rollback()
        raise HTTPException(status_code=409, detail=f"Agente: {exc}") from None
    except IntegrityError as exc:
        await db.rollback()
        if not {1451, 1217} & set(db_errors.mysql_error_codes(exc)):
            raise
        raise HTTPException(status_code=409, detail="Agente possui historico retido") from None
    if result == "not_found":
        raise HTTPException(status_code=404, detail="Agente nao encontrado")
    await auditoria.registrar(
        db, "agente_excluido", **auditoria.ator(principal), alvo_tipo="agente", alvo_id=agente_id,
    )
    return None


@router.get(
    "/{agente_id}/consumo",
    response_model=AgenteConsumoResponseSchema,
    dependencies=[Depends(exigir(P.CONSUMO_LER))],
)
async def consumo_agente(
    agente_id: int,
    inicio: datetime | None = None,
    fim: datetime | None = None,
    db: AsyncSession = Depends(get_db),
):
    agente = await agente_repo.get_by_id(db, agente_id)
    if not agente:
        raise HTTPException(status_code=404, detail="Agente nao encontrado")

    periodo_inicio = inicio or _inicio_mes_atual()
    periodo_fim = fim or datetime.utcnow()
    if periodo_inicio > periodo_fim:
        raise HTTPException(status_code=400, detail="inicio deve ser menor ou igual a fim")

    resumo = await consumo_repo.resumo_por_periodo(
        agente_id=agente_id,
        db=db,
        inicio=periodo_inicio,
        fim=periodo_fim,
    )
    custo_mes_atual = await consumo_repo.total_do_mes(agente_id=agente_id, db=db)
    limite = (
        Decimal(str(agente.limite_mensal_creditos))
        if agente.limite_mensal_creditos is not None
        else None
    )

    return AgenteConsumoResponseSchema(
        **resumo,
        custo_mes_atual_creditos=custo_mes_atual,
        limite_mensal_creditos=limite,
        percentual_franquia_mensal=_percentual_franquia(custo_mes_atual, limite),
    )
