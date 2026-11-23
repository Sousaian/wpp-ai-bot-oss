"""FastAPI dependencies that authenticate every /agentes, /sessoes, /auth call.

Fail closed: no credential or an invalid one → 401; authenticated without the
required permission → 403 (``rbac.exigir``); auth not provisioned or its DB unavailable → 503.
Validation runs in its own short transaction on ``get_auth_sessions`` (never
the route's ``get_db`` session) and yields an immutable scalar ``Principal``;
no ORM row reaches a route, so the domain UoW starts clean (§16.1 A1).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from fastapi import Depends, HTTPException, Request
from loguru import logger
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import settings
from src.database.connection import AsyncSessionLocal
from src.database.repositories import auth_repo, scheduler_repo
from src.services.auth import segredos


SESSION_COOKIE = "__Host-wpp_sessao"
CSRF_HEADER = "X-CSRF-Token"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

SESSAO_OCIOSA = timedelta(seconds=1800)
SESSAO_ABSOLUTA = timedelta(seconds=43200)
# ultimo_uso_em is touched at most once a minute: idle expiry can only come
# earlier than 30 min after the last request, never later.
TOQUE_MINIMO = timedelta(seconds=60)

_UNAUTHORIZED_HEADERS = {"WWW-Authenticate": 'Bearer realm="wpp-ai-bot"'}


@dataclass(frozen=True)
class Principal:
    tipo: Literal["usuario", "token_servico"]
    id: int
    papel: str
    sessao_id: int | None = None
    via_cookie: bool = False


@dataclass(frozen=True)
class _SessaoValida:
    principal: Principal
    csrf_hash: str | None


def get_auth_sessions() -> async_sessionmaker[AsyncSession]:
    """Own session factory for auth transactions (overridable in tests)."""

    return AsyncSessionLocal


def nao_autenticado() -> HTTPException:
    return HTTPException(
        status_code=401, detail="autenticacao requerida", headers=_UNAUTHORIZED_HEADERS
    )


def proibido(detail: str = "permissao insuficiente") -> HTTPException:
    return HTTPException(status_code=403, detail=detail)


def indisponivel() -> HTTPException:
    return HTTPException(status_code=503, detail="autenticacao indisponivel")


def exigir_configuracao() -> None:
    """Pepper and TOTP keyrings both provisioned (admin login needs TOTP)."""

    if not settings.auth_configured():
        raise indisponivel()


def sessao_vigente(sessao, usuario, agora: datetime) -> bool:
    return bool(
        sessao.revogada_em is None
        and sessao.expira_em > agora
        and sessao.ultimo_uso_em > agora - SESSAO_OCIOSA
        and usuario.status == "ativo"
        # AUTH-2: an admin session is valid only while TOTP stays enrolled.
        and (usuario.papel != "admin" or usuario.totp_confirmado_em is not None)
    )


async def _validar_sessao(db: AsyncSession, token_hash: str) -> _SessaoValida | None:
    found = await auth_repo.get_sessao_com_usuario(db, token_hash)
    if found is None:
        return None
    sessao, usuario = found
    agora = await auth_repo.agora(db)
    if sessao.estagio != "completa" or not sessao_vigente(sessao, usuario, agora):
        return None
    if agora - sessao.ultimo_uso_em >= TOQUE_MINIMO:
        await auth_repo.tocar_sessao(
            db, sessao_id=sessao.id, anterior=sessao.ultimo_uso_em, agora=agora
        )
    return _SessaoValida(
        Principal(
            tipo="usuario",
            id=int(usuario.id),
            papel=usuario.papel,
            sessao_id=int(sessao.id),
            via_cookie=True,
        ),
        sessao.csrf_hash,
    )


async def _validar_token_servico(db: AsyncSession, token_hash: str) -> Principal | None:
    token = await auth_repo.get_token_servico_por_hash(db, token_hash)
    if token is None:
        return None
    agora = await auth_repo.agora(db)
    if token.revogado_em is not None or token.expira_em <= agora:
        return None
    if token.ultimo_uso_em is None or agora - token.ultimo_uso_em >= TOQUE_MINIMO:
        await auth_repo.tocar_token_servico(
            db, token_id=token.id, anterior=token.ultimo_uso_em, agora=agora
        )
    return Principal(tipo="token_servico", id=int(token.id), papel=token.papel)


async def run_auth_transaction(
    sessions: async_sessionmaker[AsyncSession],
    operation: Callable[[AsyncSession], Awaitable[object]],
):
    """Short auth transaction with whole-transaction retry.

    Database unavailability → 503 (fail closed); domain refusals propagate.
    """

    try:
        return await scheduler_repo.run_transaction_with_deadlock_retry(sessions, operation)
    except (SQLAlchemyError, OSError, scheduler_repo.SchedulerDeadlockRetryExhausted) as exc:
        logger.error("Auth transaction unavailable | erro_tipo={}", type(exc).__name__)
        raise indisponivel() from None


async def autenticar(
    request: Request,
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
) -> Principal:
    exigir_configuracao()
    authorization = request.headers.get("authorization")
    cookie = request.cookies.get(SESSION_COOKIE)
    if authorization is not None and cookie is not None:
        # One credential per request: no ambient cookie behind a bearer.
        raise nao_autenticado()
    if authorization is not None:
        scheme, _, value = authorization.partition(" ")
        token_hash = (
            segredos.digest_se_valido("servico", value.strip())
            if scheme.lower() == "bearer"
            else None
        )
        if token_hash is None:
            raise nao_autenticado()
        principal = await run_auth_transaction(
            sessions, lambda db: _validar_token_servico(db, token_hash)
        )
        if principal is None:
            raise nao_autenticado()
        return principal
    if cookie is None:
        raise nao_autenticado()
    token_hash = segredos.digest_se_valido("sessao", cookie)
    if token_hash is None:
        raise nao_autenticado()
    valida = await run_auth_transaction(sessions, lambda db: _validar_sessao(db, token_hash))
    if valida is None:
        raise nao_autenticado()
    if request.method.upper() not in SAFE_METHODS and not segredos.iguais(
        request.headers.get(CSRF_HEADER), valida.csrf_hash
    ):
        raise proibido("csrf invalido")
    return valida.principal
