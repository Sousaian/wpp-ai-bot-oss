from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.database.models import Agente
from loguru import logger


async def get_by_instance_name(
    db: AsyncSession,
    instance_name: str,
    *,
    for_share: bool = False,
) -> Agente | None:
    """Busca agente pelo instance_name — usado no webhook handler"""
    query = select(Agente).where(
        Agente.instance_name == instance_name,
        Agente.ativo == True,
    )
    if for_share:
        query = query.with_for_update(read=True).execution_options(
            populate_existing=True
        )
    result = await db.execute(query)
    return result.scalar_one_or_none()


async def get_by_id(db: AsyncSession, agente_id: int) -> Agente | None:
    """Busca agente pelo id"""
    result = await db.execute(
        select(Agente).where(Agente.id == agente_id)
    )
    return result.scalar_one_or_none()


async def lock_by_id(db: AsyncSession, agente_id: int) -> Agente | None:
    """Load and lock an agent before delete guards run."""

    result = await db.execute(
        select(Agente)
        .where(Agente.id == agente_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def get_all(db: AsyncSession) -> list[Agente]:
    """Lista todos os agentes"""
    result = await db.execute(select(Agente).order_by(Agente.nome))
    return result.scalars().all()


async def get_active_chatwoot_inbox_ids(db: AsyncSession) -> tuple[int, ...]:
    """Return configured durable-delivery inboxes without exposing agent data."""

    result = await db.execute(
        select(Agente.chatwoot_inbox_id)
        .where(
            Agente.ativo == True,
            Agente.chatwoot_inbox_id.is_not(None),
        )
        .distinct()
        .order_by(Agente.chatwoot_inbox_id)
    )
    return tuple(int(value) for value in result.scalars().all())


async def create(db: AsyncSession, dados: dict) -> Agente:
    """Cria novo agente"""
    agente = Agente(**dados)
    db.add(agente)
    await db.flush()
    await db.refresh(agente)
    logger.info(f"Agente criado: {agente.nome} ({agente.instance_name})")
    return agente


async def update(db: AsyncSession, agente_id: int, dados: dict) -> Agente | None:
    """Atualiza configurações do agente"""
    agente = await get_by_id(db, agente_id)
    if not agente:
        return None
    for campo, valor in dados.items():
        setattr(agente, campo, valor)
    await db.flush()
    await db.refresh(agente)
    logger.info(f"Agente atualizado: {agente.nome}")
    return agente
