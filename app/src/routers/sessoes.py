from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.database import db_errors
from src.database.connection import get_db
from src.database.repositories import (
    delivery_state_repo,
    lifecycle_repo,
    mensagem_repo,
    outbox_repo,
    sessao_repo,
)


def _normalizar_telefone(telefone: str | None) -> str | None:
    if not isinstance(telefone, str) or not telefone.strip():
        return None

    telefone_limpo = telefone.strip()
    if telefone_limpo.endswith("@g.us"):
        return None

    return telefone_limpo.replace("@s.whatsapp.net", "")


class SessaoResponseSchema(BaseModel):
    id: int
    agente_id: int
    telefone: str
    handler: str
    total_mensagens: int
    motivo_handoff: str | None = None
    encerrada_em: datetime | None = None
    motivo_encerramento: str | None = None
    atendente_id: int | None = None
    criado_em: datetime | None = None
    ultima_interacao: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class MensagemResponseSchema(BaseModel):
    id: int
    sessao_id: int
    role: str
    conteudo: str
    criado_em: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class SessaoListResponseSchema(BaseModel):
    total: int
    sessoes: list[SessaoResponseSchema]


class HistoricoSessaoResponseSchema(BaseModel):
    total: int
    mensagens: list[MensagemResponseSchema]


class OperacaoSessaoResponseSchema(BaseModel):
    status: str
    agente_id: int
    telefone: str


router = APIRouter(prefix="/sessoes", tags=["sessoes"])


async def _lock_runtime_authority(db: AsyncSession) -> None:
    _require_runtime_compatible(await delivery_state_repo.get_runtime_state(db, for_share=True))


def _require_runtime_compatible(state) -> None:
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


async def _buscar_sessao_por_telefone(
    db: AsyncSession,
    agente_id: int,
    telefone: str,
):
    """Read-only lookup; every mutation locks through ``lifecycle_repo``."""

    telefone_normalizado = _normalizar_telefone(telefone)
    if not telefone_normalizado:
        raise HTTPException(status_code=400, detail="Telefone invalido")

    sessao = await sessao_repo.get_by_agente_telefone(
        db, agente_id, telefone_normalizado
    )
    if not sessao:
        raise HTTPException(status_code=404, detail="Sessao nao encontrada")

    return sessao, telefone_normalizado


@router.get("", response_model=SessaoListResponseSchema)
async def listar_sessoes(
    agente_id: int | None = Query(default=None, ge=1),
    handler: Literal["bot", "humano", "encerrada"] | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    sessoes = await sessao_repo.get_all(db, agente_id=agente_id, handler=handler)
    return {"total": len(sessoes), "sessoes": sessoes}


@router.get("/{telefone}", response_model=SessaoResponseSchema)
async def detalhar_sessao(
    telefone: str,
    agente_id: int = Query(..., ge=1),
    db: AsyncSession = Depends(get_db),
):
    sessao, _ = await _buscar_sessao_por_telefone(db, agente_id, telefone)
    return sessao


def _foreign_key_restrict(exc: IntegrityError) -> bool:
    # 1451: direct child; 1217: RESTRICT reached through an ON DELETE CASCADE.
    return bool({1451, 1217} & set(db_errors.mysql_error_codes(exc)))


@router.post("/{telefone}/transferir", response_model=OperacaoSessaoResponseSchema)
async def transferir_sessao(
    telefone: str,
    agente_id: int = Query(..., ge=1),
    motivo: str | None = Query(default=None, min_length=1),
    db: AsyncSession = Depends(get_db),
):
    telefone_normalizado = _normalizar_telefone(telefone)
    if not telefone_normalizado:
        raise HTTPException(status_code=400, detail="Telefone invalido")
    # Fenced lifecycle (ticket07): runtime → scheduler → agent → lane →
    # session → turns → … → deliveries; scheduled bot work is cancelled.
    result = await lifecycle_repo.run_with_deadlock_retry(
        db,
        lambda tx: lifecycle_repo.transfer_session(
            tx,
            agente_id=agente_id,
            telefone=telefone_normalizado,
            motivo=motivo or "transferencia_manual",
        ),
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Sessao nao encontrada")

    return {
        "status": "transferred",
        "agente_id": agente_id,
        "telefone": telefone_normalizado,
    }


@router.post("/{telefone}/retomar", response_model=OperacaoSessaoResponseSchema)
async def retomar_sessao(
    telefone: str,
    agente_id: int = Query(..., ge=1),
    db: AsyncSession = Depends(get_db),
):
    telefone_normalizado = _normalizar_telefone(telefone)
    if not telefone_normalizado:
        raise HTTPException(status_code=400, detail="Telefone invalido")

    async def operation(tx: AsyncSession):
        await _lock_runtime_authority(tx)
        return await lifecycle_repo.resume_session(
            tx, agente_id=agente_id, telefone=telefone_normalizado
        )

    result = await lifecycle_repo.run_with_deadlock_retry(db, operation)
    if result is None:
        raise HTTPException(status_code=404, detail="Sessao nao encontrada")

    return {
        "status": "resumed",
        "agente_id": agente_id,
        "telefone": telefone_normalizado,
    }


@router.delete("/{telefone}", status_code=status.HTTP_204_NO_CONTENT)
async def deletar_sessao(
    telefone: str,
    agente_id: int = Query(..., ge=1),
    db: AsyncSession = Depends(get_db),
):
    telefone_normalizado = _normalizar_telefone(telefone)
    if not telefone_normalizado:
        raise HTTPException(status_code=400, detail="Telefone invalido")

    # S3: delete_session opens its own READ COMMITTED transaction and checks
    # the locked runtime row (S) through the guard before anything else.
    async def operation(tx: AsyncSession):
        return await lifecycle_repo.delete_session(
            tx, agente_id=agente_id, telefone=telefone_normalizado,
            runtime_guard=_require_runtime_compatible,
        )

    try:
        result = await lifecycle_repo.run_with_deadlock_retry(db, operation)
    except outbox_repo.ActiveDeliveriesError:
        await db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Sessao possui entregas pendentes",
        ) from None
    except lifecycle_repo.LifecycleRefused as exc:
        await db.rollback()
        raise HTTPException(status_code=409, detail=f"Sessao: {exc}") from None
    except IntegrityError as exc:
        # Any retained RESTRICT dependent (C1): refuse, never a 500.
        await db.rollback()
        if not _foreign_key_restrict(exc):
            raise
        raise HTTPException(
            status_code=409,
            detail="Sessao possui historico retido",
        ) from None
    if result == "not_found":
        raise HTTPException(status_code=404, detail="Sessao nao encontrada")
    return None


@router.get("/{telefone}/historico", response_model=HistoricoSessaoResponseSchema)
async def historico_sessao(
    telefone: str,
    agente_id: int = Query(..., ge=1),
    limite: int | None = Query(default=None, ge=1),
    db: AsyncSession = Depends(get_db),
):
    sessao, _ = await _buscar_sessao_por_telefone(db, agente_id, telefone)
    mensagens = await mensagem_repo.get_historico(db, sessao.id, limite=limite)
    return {"total": len(mensagens), "mensagens": mensagens}
