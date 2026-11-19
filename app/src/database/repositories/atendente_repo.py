from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Atendente


async def create_atendente(db: AsyncSession, dados: dict) -> Atendente:
    atendente = Atendente(**dados)
    db.add(atendente)
    await db.flush()
    await db.refresh(atendente)
    return atendente


async def get_atendente_by_id(db: AsyncSession, atendente_id: int) -> Atendente | None:
    result = await db.execute(select(Atendente).where(Atendente.id == atendente_id))
    return result.scalar_one_or_none()


async def list_atendentes(db: AsyncSession, apenas_ativos: bool = False) -> list[Atendente]:
    query = select(Atendente).order_by(Atendente.id.asc())
    if apenas_ativos:
        query = query.where(Atendente.ativo.is_(True))
    result = await db.execute(query)
    return result.scalars().all()


async def update_atendente(db: AsyncSession, atendente_id: int, dados: dict) -> Atendente | None:
    atendente = await get_atendente_by_id(db, atendente_id)
    if not atendente:
        return None

    for campo, valor in dados.items():
        setattr(atendente, campo, valor)

    await db.flush()
    await db.refresh(atendente)
    return atendente


async def delete_atendente(db: AsyncSession, atendente_id: int) -> bool:
    atendente = await get_atendente_by_id(db, atendente_id)
    if not atendente:
        return False

    await db.delete(atendente)
    await db.flush()
    return True


async def get_proximo_round_robin(db: AsyncSession) -> Atendente | None:
    result = await db.execute(
        select(Atendente)
        .where(Atendente.ativo.is_(True))
        .order_by(Atendente.indice_round_robin.asc(), Atendente.id.asc())
        .limit(1)
    )
    atendente = result.scalar_one_or_none()
    if not atendente:
        return None

    atendente.indice_round_robin += 1
    await db.flush()
    return atendente
