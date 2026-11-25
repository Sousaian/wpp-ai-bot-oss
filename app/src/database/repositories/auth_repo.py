"""AUTH-1: the only SQL of API operator authentication (§10.2).

Lock order inside an auth transaction: ``auth_usuarios`` (admin range by
index, then rows by id) → ``auth_tentativas`` → ``auth_sessoes`` → ``auth_tokens_uso_unico`` /
``auth_tokens_servico``. Decisions are taken only on locking reads with
``populate_existing``; a plain read is used solely to discover the row id
that the transaction locks and then revalidates. Request authentication
never locks: it reads, then touches ``ultimo_uso_em`` with a conditional
UPDATE on the session/token row only. Auth tables never join a domain UoW.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import db_errors
from src.database.models import (
    AuthSessao,
    AuthTentativa,
    AuthTokenServico,
    AuthTokenUsoUnico,
    AuthUsuario,
)


class EmailIndisponivel(Exception):
    """Unique e-mail conflict, without the conflicting value."""


async def agora(db: AsyncSession) -> datetime:
    # No table access: never opens the InnoDB read view.
    return (await db.execute(select(func.utc_timestamp(6)))).scalar_one()


# ── usuarios ───────────────────────────────────────────────────────────────
async def get_usuario_por_email(db: AsyncSession, email: str) -> AuthUsuario | None:
    return (
        await db.execute(select(AuthUsuario).where(AuthUsuario.email == email))
    ).scalar_one_or_none()


async def get_usuario(db: AsyncSession, usuario_id: int) -> AuthUsuario | None:
    return (
        await db.execute(select(AuthUsuario).where(AuthUsuario.id == usuario_id))
    ).scalar_one_or_none()


async def lock_usuario(db: AsyncSession, usuario_id: int) -> AuthUsuario | None:
    return (
        await db.execute(
            select(AuthUsuario)
            .where(AuthUsuario.id == usuario_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def lock_admins(db: AsyncSession) -> list[AuthUsuario]:
    """X-lock the whole ``papel='admin'`` index range (next-key included).

    Serializes "last admin" and bootstrap decisions: a concurrent insert or
    promotion of an admin waits on (or deadlocks with) this range, and the
    loser repeats the complete transaction.
    """

    return list(
        (
            await db.execute(
                select(AuthUsuario)
                .where(AuthUsuario.papel == "admin")
                .order_by(AuthUsuario.status, AuthUsuario.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )


async def listar_usuarios(db: AsyncSession) -> list[AuthUsuario]:
    return list((await db.execute(select(AuthUsuario).order_by(AuthUsuario.id))).scalars())


async def criar_usuario(
    db: AsyncSession, *, email: str, papel: str, status: str
) -> AuthUsuario:
    usuario = AuthUsuario(
        email=email, papel=papel, status=status, falhas_consecutivas=0, totp_falhas=0, totp_bloqueios=0
    )
    # No savepoint: a duplicate aborts the caller's whole short transaction.
    db.add(usuario)
    try:
        await db.flush()
    except IntegrityError as exc:
        if db_errors.transaction_lost(exc):
            raise
        if 1062 in db_errors.mysql_error_codes(exc):
            raise EmailIndisponivel() from None
        raise
    return usuario


async def substituir_senha_se_inalterada(
    db: AsyncSession, *, usuario_id: int, anterior: str, novo_hash: str, versao: int
) -> bool:
    """Rehash on login: only if no concurrent change replaced the hash."""

    result = await db.execute(
        update(AuthUsuario)
        .where(AuthUsuario.id == usuario_id, AuthUsuario.senha_hash == anterior)
        .values(senha_hash=novo_hash, senha_pepper_versao=versao)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


# ── tentativas (caller holds the user X lock) ──────────────────────────────
async def lock_tentativa(db: AsyncSession, *, usuario_id: int, origem: str) -> AuthTentativa | None:
    return (
        await db.execute(
            select(AuthTentativa)
            .where(AuthTentativa.usuario_id == usuario_id, AuthTentativa.origem == origem)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def registrar_tentativa(
    db: AsyncSession, *, registro: AuthTentativa | None, usuario_id: int, origem: str, falhas: int,
    agora: datetime, liberado_em: datetime | None,
) -> None:
    if registro is None:
        registro = AuthTentativa(usuario_id=usuario_id, origem=origem)
        db.add(registro)
    registro.falhas = falhas
    registro.ultima_falha_em = agora
    registro.liberado_em = liberado_em
    await db.flush()


async def limpar_tentativas(
    db: AsyncSession, *, usuario_id: int, origem: str | None = None, antes_de: datetime | None = None
) -> int:
    criteria = [AuthTentativa.usuario_id == usuario_id]
    if origem is not None:
        criteria.append(AuthTentativa.origem == origem)
    if antes_de is not None:
        criteria.append(AuthTentativa.ultima_falha_em < antes_de)
    result = await db.execute(
        delete(AuthTentativa).where(*criteria).execution_options(synchronize_session=False)
    )
    return int(result.rowcount or 0)


async def contar_atrasos(db: AsyncSession, *, agora: datetime) -> dict[int, int]:
    rows = await db.execute(
        select(AuthTentativa.usuario_id, func.count())
        .where(AuthTentativa.liberado_em > agora)
        .group_by(AuthTentativa.usuario_id)
    )
    return {int(usuario_id): int(total) for usuario_id, total in rows}


# ── sessoes ────────────────────────────────────────────────────────────────
async def criar_sessao(
    db: AsyncSession,
    *,
    usuario_id: int,
    estagio: str,
    token_hash: str,
    csrf_hash: str | None,
    agora: datetime,
    expira_em: datetime,
) -> AuthSessao:
    sessao = AuthSessao(
        usuario_id=usuario_id,
        estagio=estagio,
        token_hash=token_hash,
        csrf_hash=csrf_hash,
        tentativas_mfa=0,
        criada_em=agora,
        ultimo_uso_em=agora,
        expira_em=expira_em,
    )
    db.add(sessao)
    await db.flush()
    return sessao


async def get_sessao_com_usuario(
    db: AsyncSession, token_hash: str
) -> tuple[AuthSessao, AuthUsuario] | None:
    """Non-locking read used by per-request authentication."""

    row = (
        await db.execute(
            select(AuthSessao, AuthUsuario)
            .join(AuthUsuario, AuthUsuario.id == AuthSessao.usuario_id)
            .where(AuthSessao.token_hash == token_hash)
        )
    ).first()
    return (row[0], row[1]) if row else None


async def get_sessao(db: AsyncSession, sessao_id: int) -> AuthSessao | None:
    return (
        await db.execute(select(AuthSessao).where(AuthSessao.id == sessao_id))
    ).scalar_one_or_none()


async def lock_sessao(db: AsyncSession, sessao_id: int) -> AuthSessao | None:
    return (
        await db.execute(
            select(AuthSessao)
            .where(AuthSessao.id == sessao_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def tocar_sessao(
    db: AsyncSession, *, sessao_id: int, anterior: datetime, agora: datetime
) -> None:
    await db.execute(
        update(AuthSessao)
        .where(
            AuthSessao.id == sessao_id,
            AuthSessao.revogada_em.is_(None),
            AuthSessao.ultimo_uso_em == anterior,
        )
        .values(ultimo_uso_em=agora)
        .execution_options(synchronize_session=False)
    )


async def revogar_sessao(
    db: AsyncSession, *, sessao_id: int, motivo: str, agora: datetime
) -> bool:
    result = await db.execute(
        update(AuthSessao)
        .where(AuthSessao.id == sessao_id, AuthSessao.revogada_em.is_(None))
        .values(revogada_em=agora, motivo_revogacao=motivo)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


async def revogar_sessoes_usuario(
    db: AsyncSession, *, usuario_id: int, motivo: str, agora: datetime
) -> int:
    """Caller holds the user row X lock (canonical order)."""

    result = await db.execute(
        update(AuthSessao)
        .where(AuthSessao.usuario_id == usuario_id, AuthSessao.revogada_em.is_(None))
        .values(revogada_em=agora, motivo_revogacao=motivo)
        .execution_options(synchronize_session=False)
    )
    return int(result.rowcount or 0)


async def contar_sessoes_ativas(db: AsyncSession, *, usuario_id: int, agora: datetime) -> int:
    return int(
        (
            await db.execute(
                select(func.count(AuthSessao.id)).where(
                    AuthSessao.usuario_id == usuario_id,
                    AuthSessao.revogada_em.is_(None),
                    AuthSessao.expira_em > agora,
                )
            )
        ).scalar_one()
    )


# ── tokens de serviço ──────────────────────────────────────────────────────
async def criar_token_servico(
    db: AsyncSession,
    *,
    nome: str,
    papel: str,
    token_hash: str,
    agora: datetime,
    expira_em: datetime,
) -> AuthTokenServico:
    token = AuthTokenServico(
        nome=nome, papel=papel, token_hash=token_hash, criado_em=agora, expira_em=expira_em
    )
    db.add(token)
    await db.flush()
    return token


async def get_token_servico_por_hash(db: AsyncSession, token_hash: str) -> AuthTokenServico | None:
    return (
        await db.execute(select(AuthTokenServico).where(AuthTokenServico.token_hash == token_hash))
    ).scalar_one_or_none()


async def get_token_servico(db: AsyncSession, token_id: int) -> AuthTokenServico | None:
    return (
        await db.execute(select(AuthTokenServico).where(AuthTokenServico.id == token_id))
    ).scalar_one_or_none()


async def tocar_token_servico(
    db: AsyncSession, *, token_id: int, anterior: datetime | None, agora: datetime
) -> None:
    criteria = [AuthTokenServico.id == token_id, AuthTokenServico.revogado_em.is_(None)]
    criteria.append(
        AuthTokenServico.ultimo_uso_em.is_(None)
        if anterior is None
        else AuthTokenServico.ultimo_uso_em == anterior
    )
    await db.execute(
        update(AuthTokenServico)
        .where(*criteria)
        .values(ultimo_uso_em=agora)
        .execution_options(synchronize_session=False)
    )


async def listar_tokens_servico(db: AsyncSession) -> list[AuthTokenServico]:
    return list(
        (await db.execute(select(AuthTokenServico).order_by(AuthTokenServico.id))).scalars()
    )


async def revogar_token_servico(db: AsyncSession, *, token_id: int, agora: datetime) -> bool | None:
    token = (
        await db.execute(
            select(AuthTokenServico)
            .where(AuthTokenServico.id == token_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if token is None:
        return None
    if token.revogado_em is not None:
        return False
    token.revogado_em = agora
    await db.flush()
    return True


# ── tokens de uso único ────────────────────────────────────────────────────
async def emitir_token_uso_unico(
    db: AsyncSession,
    *,
    usuario_id: int,
    tipo: str,
    token_hash: str,
    agora: datetime,
    expira_em: datetime,
) -> AuthTokenUsoUnico:
    """Caller holds the user X lock; any unused token of the user is revoked."""

    await db.execute(
        update(AuthTokenUsoUnico)
        .where(
            AuthTokenUsoUnico.usuario_id == usuario_id,
            AuthTokenUsoUnico.usado_em.is_(None),
            AuthTokenUsoUnico.revogado_em.is_(None),
        )
        .values(revogado_em=agora)
        .execution_options(synchronize_session=False)
    )
    token = AuthTokenUsoUnico(
        usuario_id=usuario_id, tipo=tipo, token_hash=token_hash, criado_em=agora, expira_em=expira_em
    )
    db.add(token)
    await db.flush()
    return token


async def get_token_uso_unico_por_hash(db: AsyncSession, token_hash: str) -> AuthTokenUsoUnico | None:
    return (
        await db.execute(
            select(AuthTokenUsoUnico).where(AuthTokenUsoUnico.token_hash == token_hash)
        )
    ).scalar_one_or_none()


async def lock_token_uso_unico(db: AsyncSession, token_id: int) -> AuthTokenUsoUnico | None:
    return (
        await db.execute(
            select(AuthTokenUsoUnico)
            .where(AuthTokenUsoUnico.id == token_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def revogar_tokens_uso_unico(db: AsyncSession, *, usuario_id: int, agora: datetime) -> int:
    result = await db.execute(
        update(AuthTokenUsoUnico)
        .where(
            AuthTokenUsoUnico.usuario_id == usuario_id,
            AuthTokenUsoUnico.usado_em.is_(None),
            AuthTokenUsoUnico.revogado_em.is_(None),
        )
        .values(revogado_em=agora)
        .execution_options(synchronize_session=False)
    )
    return int(result.rowcount or 0)
