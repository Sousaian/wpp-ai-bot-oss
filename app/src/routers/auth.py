"""AUTH-1: /auth — login, logout, me and one-time-token activation/reset.

API only. Credential endpoints share a per-source window (429) and every
login failure returns the same 401 body. The session token travels only in
the ``__Host-`` cookie (HttpOnly, Secure, SameSite=Strict, Path=/); the
CSRF token is returned once in the login body and must accompany every
cookie-authenticated mutation as ``X-CSRF-Token``.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.repositories import auth_repo
from src.services.auth import limites, origem, senha, sessao
from src.services.auth.contexto import (
    SESSION_COOKIE,
    Principal,
    autenticar,
    exigir_configuracao,
    get_auth_sessions,
    run_auth_transaction,
)


router = APIRouter(prefix="/auth", tags=["auth"])

CREDENCIAIS_INVALIDAS = "credenciais invalidas"


class LoginSchema(BaseModel):
    # Unconstrained on purpose: every malformed value takes the generic path.
    email: str
    senha: str


class LoginResponseSchema(BaseModel):
    csrf_token: str
    expira_em: datetime
    papel: str


class AtivarSchema(BaseModel):
    token: str
    senha: str


class StatusSchema(BaseModel):
    status: str


class MeResponseSchema(BaseModel):
    tipo: str
    id: int
    papel: str
    email: str | None = None
    nome: str | None = None
    mfa_ativo: bool | None = None
    expira_em: datetime | None = None


def _ocupado() -> HTTPException:
    return HTTPException(
        status_code=503, detail="autenticacao ocupada", headers={"Retry-After": "2"}
    )


async def _limitar(request: Request, escopo: str) -> str:
    """Count one attempt of the real client; return its IP."""

    ip = await origem.cliente(request)
    retry_after = limites.LIMITE.registrar(escopo, origem.chave(ip))
    if retry_after is not None:
        raise HTTPException(
            status_code=429,
            detail="muitas tentativas",
            headers={"Retry-After": str(int(retry_after + 0.999))},
        )
    return ip


def _definir_cookie(response: Response, emitida: sessao.SessaoEmitida) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        emitida.token,
        max_age=int(sessao.SESSAO_ABSOLUTA.total_seconds()),
        path="/",
        secure=True,
        httponly=True,
        samesite="strict",
    )


@router.post("/login", response_model=LoginResponseSchema)
async def login(
    dados: LoginSchema,
    request: Request,
    response: Response,
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    exigir_configuracao()
    ip = await _limitar(request, "login")
    try:
        emitida = await sessao.login(
            sessions, email=dados.email, senha_informada=dados.senha, origem_ip=ip
        )
    except senha.HashOcupado:
        raise _ocupado() from None
    except sessao.CredenciaisInvalidas:
        raise HTTPException(status_code=401, detail=CREDENCIAIS_INVALIDAS) from None
    _definir_cookie(response, emitida)
    response.headers["Cache-Control"] = "no-store"
    return LoginResponseSchema(csrf_token=emitida.csrf_token, expira_em=emitida.expira_em, papel=emitida.papel)


@router.post("/logout", response_model=StatusSchema)
async def logout(
    response: Response,
    principal: Principal = Depends(autenticar),
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    if principal.sessao_id is None:
        raise HTTPException(status_code=400, detail="logout requer sessao")
    await sessao.logout(sessions, sessao_id=principal.sessao_id)
    response.delete_cookie(SESSION_COOKIE, path="/", secure=True, httponly=True, samesite="strict")
    return StatusSchema(status="encerrada")


@router.get("/me", response_model=MeResponseSchema)
async def me(
    response: Response,
    principal: Principal = Depends(autenticar),
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    response.headers["Cache-Control"] = "no-store"

    async def load(db: AsyncSession) -> MeResponseSchema | None:
        if principal.tipo == "token_servico":
            token = await auth_repo.get_token_servico(db, principal.id)
            if token is None:
                return None
            return MeResponseSchema(tipo="token_servico", id=principal.id, papel=principal.papel,
                                    nome=token.nome, expira_em=token.expira_em)
        usuario = await auth_repo.get_usuario(db, principal.id)
        sessao_row = await auth_repo.get_sessao(db, principal.sessao_id) if principal.sessao_id else None
        if usuario is None or sessao_row is None:
            return None
        return MeResponseSchema(tipo="usuario", id=principal.id, papel=principal.papel, email=usuario.email,
                                mfa_ativo=usuario.totp_confirmado_em is not None,
                                expira_em=sessao_row.expira_em)

    result = await run_auth_transaction(sessions, load)
    if result is None:
        raise HTTPException(status_code=401, detail="autenticacao requerida")
    return result


@router.post("/ativar", response_model=StatusSchema)
async def ativar(
    dados: AtivarSchema,
    request: Request,
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    """Consume an out-of-band activation or password-reset token."""

    exigir_configuracao()
    await _limitar(request, "ativar")
    try:
        await sessao.usar_token(sessions, token=dados.token, nova_senha=dados.senha)
    except sessao.TokenInvalido:
        raise HTTPException(status_code=400, detail="token invalido") from None
    except senha.HashOcupado:
        raise _ocupado() from None
    except senha.SenhaRecusada as exc:
        raise HTTPException(status_code=422, detail=exc.code) from None
    return StatusSchema(status="senha_definida")
