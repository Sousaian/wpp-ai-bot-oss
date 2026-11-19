from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.database.models import Mensagem
from loguru import logger


async def salvar(db: AsyncSession, sessao_id: int, role: str, conteudo: str) -> Mensagem:
    """Salva mensagem no histórico"""
    mensagem = Mensagem(sessao_id=sessao_id, role=role, conteudo=conteudo)
    db.add(mensagem)
    await db.flush()
    logger.debug(f"Mensagem salva: sessao={sessao_id} role={role}")
    return mensagem


async def get_historico(
    db: AsyncSession,
    sessao_id: int,
    limite: int | None = 20,
    *,
    current_read: bool = False,
) -> list[Mensagem]:
    """
    Busca últimas N mensagens da sessão em ordem cronológica.
    Usado pelo Agent Runner para montar o contexto da IA.
    """
    query = (
        select(Mensagem)
        .where(Mensagem.sessao_id == sessao_id)
        .order_by(Mensagem.criado_em.desc(), Mensagem.id.desc())
    )
    if limite is not None:
        query = query.limit(limite)
    if current_read:
        query = query.with_for_update()

    result = await db.execute(query)
    mensagens = result.scalars().all()
    return list(reversed(mensagens))  # retorna em ordem cronológica


async def get_ultima_mensagem(db: AsyncSession, sessao_id: int) -> Mensagem | None:
    """Busca última mensagem da sessão — usada na notificação de handoff"""
    result = await db.execute(
        select(Mensagem)
        .where(Mensagem.sessao_id == sessao_id)
        .order_by(Mensagem.criado_em.desc(), Mensagem.id.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()
