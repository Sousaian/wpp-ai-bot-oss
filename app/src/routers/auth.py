"""AUTH-1/2: /auth — login, TOTP, logout, me and one-time-token flows.

API only. Credential endpoints share a per-source window (429) and every
login failure returns the same 401 body. The session token travels only in
the ``__Host-`` cookie (HttpOnly, Secure, SameSite=Strict, Path=/); the
CSRF token is returned once with the session and must accompany every
cookie-authenticated mutation as ``X-CSRF-Token``. Users with confirmed TOTP
(always every admin) get a body-only challenge from ``/login`` and the
session from ``/totp/verificar``.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.repositories import auth_repo
from src.services.auth import auditoria, limites, senha, sessao
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
    """Either a session (cookie + csrf_token) or a TOTP challenge."""

    mfa_requerido: bool
    csrf_token: str | None = None
    papel: str | None = None
    desafio: str | None = None
    expira_em: datetime


class AtivarSchema(BaseModel):
    token: str
    # Not used by reset_mfa tokens.
    senha: str | None = None


class CodigoSchema(BaseModel):
    desafio: str
    codigo: str


class EnrolamentoResponseSchema(BaseModel):
    status: str
    desafio: str | None = None
    expira_em: datetime | None = None
    # Shown once; the client renders it as a QR code. Never logged.
    totp_segredo: str | None = None
    totp_uri: str | None = None


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


def _limitar(request: Request, escopo: str) -> None:
    retry_after = limites.LIMITE.registrar(escopo, request.client.host if request.client else None)
    if retry_after is not None:
        raise HTTPException(
            status_code=429,
            detail="muitas tentativas",
            headers={"Retry-After": str(int(retry_after + 0.999))},
        )


def _origem(request: Request) -> str | None:
    return auditoria.origem(request)


def _enrolamento(iniciado: sessao.EnrolamentoIniciado | None, status: str) -> EnrolamentoResponseSchema:
    if iniciado is None:
        return EnrolamentoResponseSchema(status=status)
    return EnrolamentoResponseSchema(
        status="totp_enrolamento_requerido", desafio=iniciado.desafio, expira_em=iniciado.expira_em,
        totp_segredo=iniciado.segredo, totp_uri=iniciado.uri,
    )


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
    _limitar(request, "login")
    response.headers["Cache-Control"] = "no-store"
    try:
        resultado = await sessao.login(
            sessions, email=dados.email, senha_informada=dados.senha, origem_ip=_origem(request)
        )
    except sessao.CredenciaisInvalidas:
        raise HTTPException(status_code=401, detail=CREDENCIAIS_INVALIDAS) from None
    if isinstance(resultado, sessao.DesafioMfa):
        return LoginResponseSchema(mfa_requerido=True, desafio=resultado.desafio, expira_em=resultado.expira_em)
    _definir_cookie(response, resultado)
    return LoginResponseSchema(
        mfa_requerido=False, csrf_token=resultado.csrf_token, papel=resultado.papel,
        expira_em=resultado.expira_em,
    )


@router.post("/totp/verificar", response_model=LoginResponseSchema)
async def verificar_totp(
    dados: CodigoSchema,
    request: Request,
    response: Response,
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    """Second login step: challenge + TOTP code → session cookie."""

    exigir_configuracao()
    _limitar(request, "totp")
    response.headers["Cache-Control"] = "no-store"
    try:
        emitida = await sessao.verificar_totp(
            sessions, desafio=dados.desafio, codigo=dados.codigo, origem_ip=_origem(request)
        )
    except sessao.CodigoInvalido:
        raise HTTPException(status_code=401, detail="codigo invalido") from None
    _definir_cookie(response, emitida)
    return LoginResponseSchema(
        mfa_requerido=False, csrf_token=emitida.csrf_token, papel=emitida.papel, expira_em=emitida.expira_em,
    )


@router.post("/totp/enrolar", response_model=EnrolamentoResponseSchema)
async def enrolar_totp(
    request: Request,
    response: Response,
    principal: Principal = Depends(autenticar),
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    """Voluntary TOTP for a logged-in user that has none (admins always have it)."""

    if principal.tipo != "usuario":
        raise HTTPException(status_code=400, detail="enrolamento requer sessao")
    response.headers["Cache-Control"] = "no-store"
    try:
        iniciado = await sessao.iniciar_enrolamento(
            sessions, usuario_id=principal.id, origem_ip=_origem(request)
        )
    except sessao.EnrolamentoNaoPermitido:
        raise HTTPException(status_code=409, detail="totp ja configurado") from None
    return _enrolamento(iniciado, "totp_enrolamento_requerido")


@router.post("/totp/confirmar", response_model=StatusSchema)
async def confirmar_totp(
    dados: CodigoSchema,
    request: Request,
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    """Finish an enrolment (activation, reset_mfa or voluntary)."""

    exigir_configuracao()
    _limitar(request, "totp")
    try:
        status = await sessao.confirmar_totp(
            sessions, desafio=dados.desafio, codigo=dados.codigo, origem_ip=_origem(request)
        )
    except sessao.CodigoInvalido:
        raise HTTPException(status_code=401, detail="codigo invalido") from None
    return StatusSchema(status=f"totp_confirmado:{status}")


@router.post("/logout", response_model=StatusSchema)
async def logout(
    request: Request,
    response: Response,
    principal: Principal = Depends(autenticar),
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    if principal.sessao_id is None:
        raise HTTPException(status_code=400, detail="logout requer sessao")
    await sessao.logout(
        sessions, sessao_id=principal.sessao_id, usuario_id=principal.id, origem_ip=_origem(request)
    )
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


@router.post("/ativar", response_model=EnrolamentoResponseSchema)
async def ativar(
    dados: AtivarSchema,
    request: Request,
    response: Response,
    sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
):
    """Consume an out-of-band activation, password-reset or MFA-reset token."""

    exigir_configuracao()
    _limitar(request, "ativar")
    response.headers["Cache-Control"] = "no-store"
    try:
        iniciado = await sessao.usar_token(
            sessions, token=dados.token, nova_senha=dados.senha, origem_ip=_origem(request)
        )
    except sessao.TokenInvalido:
        raise HTTPException(status_code=400, detail="token invalido") from None
    except senha.SenhaRecusada as exc:
        raise HTTPException(status_code=422, detail=exc.code) from None
    return _enrolamento(iniciado, "senha_definida")
