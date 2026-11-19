from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.connection import get_db
from src.database.repositories import atendente_repo


class AtendenteBaseSchema(BaseModel):
    nome: str = Field(min_length=1, max_length=255)
    numero_whatsapp: str = Field(min_length=1, max_length=50)
    ativo: bool = True


class AtendenteCreateSchema(AtendenteBaseSchema):
    pass


class AtendenteUpdateSchema(BaseModel):
    nome: str | None = Field(default=None, min_length=1, max_length=255)
    numero_whatsapp: str | None = Field(default=None, min_length=1, max_length=50)
    ativo: bool | None = None


class AtendenteResponseSchema(AtendenteBaseSchema):
    id: int
    indice_round_robin: int
    criado_em: datetime | None = None
    atualizado_em: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


router = APIRouter(prefix="/atendentes", tags=["atendentes"])


@router.get("", response_model=list[AtendenteResponseSchema])
async def listar_atendentes(
    apenas_ativos: bool = False,
    db: AsyncSession = Depends(get_db),
):
    return await atendente_repo.list_atendentes(db, apenas_ativos=apenas_ativos)


@router.post("", response_model=AtendenteResponseSchema, status_code=status.HTTP_201_CREATED)
async def criar_atendente(
    dados: AtendenteCreateSchema,
    db: AsyncSession = Depends(get_db),
):
    try:
        return await atendente_repo.create_atendente(db, dados.model_dump())
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="numero_whatsapp ja cadastrado")


@router.get("/{atendente_id}", response_model=AtendenteResponseSchema)
async def detalhar_atendente(
    atendente_id: int,
    db: AsyncSession = Depends(get_db),
):
    atendente = await atendente_repo.get_atendente_by_id(db, atendente_id)
    if not atendente:
        raise HTTPException(status_code=404, detail="Atendente nao encontrado")
    return atendente


@router.put("/{atendente_id}", response_model=AtendenteResponseSchema)
async def atualizar_atendente(
    atendente_id: int,
    dados: AtendenteUpdateSchema,
    db: AsyncSession = Depends(get_db),
):
    payload = dados.model_dump(exclude_unset=True)
    if not payload:
        raise HTTPException(status_code=400, detail="Nenhum campo enviado para atualizacao")

    try:
        atendente = await atendente_repo.update_atendente(db, atendente_id, payload)
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=409, detail="numero_whatsapp ja cadastrado")

    if not atendente:
        raise HTTPException(status_code=404, detail="Atendente nao encontrado")
    return atendente


@router.delete("/{atendente_id}", status_code=status.HTTP_204_NO_CONTENT)
async def deletar_atendente(
    atendente_id: int,
    db: AsyncSession = Depends(get_db),
):
    deleted = await atendente_repo.delete_atendente(db, atendente_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Atendente nao encontrado")
    return None
