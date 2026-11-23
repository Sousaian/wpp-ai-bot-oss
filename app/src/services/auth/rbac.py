"""Fixed roles and the permission catalog (code, not data).

Every /agentes and /sessoes route declares exactly one ``exigir(...)``; the
router-level guard refuses (403) any route of those routers that declares
none, so a route added without a permission fails closed at runtime, and
``test_auth_rbac_mysql`` fails statically.
"""

from __future__ import annotations

from enum import Enum

from fastapi import Depends, Request
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.services.auth import auditoria
from src.services.auth.contexto import Principal, autenticar, get_auth_sessions, proibido


class Permissao(str, Enum):
    AGENTE_LER = "agente.ler"
    AGENTE_CRIAR = "agente.criar"
    # Any field of PUT /agentes/{id} (model, limits, inbox, instance …).
    AGENTE_EDITAR = "agente.editar"
    # Only system_prompt and ativo.
    AGENTE_EDITAR_OPERACIONAL = "agente.editar_operacional"
    AGENTE_EXCLUIR = "agente.excluir"
    CONSUMO_LER = "consumo.ler"
    SESSAO_LER = "sessao.ler"
    # Without it every phone in a response is masked.
    SESSAO_LER_TELEFONE = "sessao.ler_telefone"
    SESSAO_TRANSFERIR = "sessao.transferir"
    SESSAO_RETOMAR = "sessao.retomar"
    SESSAO_EXCLUIR = "sessao.excluir"
    # Users and service tokens are managed by the container CLI only; no
    # HTTP route grants these yet.
    USUARIO_GERENCIAR = "usuario.gerenciar"
    TOKEN_GERENCIAR = "token.gerenciar"


CAMPOS_OPERACIONAIS = frozenset({"system_prompt", "ativo"})

PAPEIS: dict[str, frozenset[Permissao]] = {
    "admin": frozenset(Permissao),
    "operador": frozenset({
        Permissao.AGENTE_LER,
        Permissao.AGENTE_EDITAR_OPERACIONAL,
        Permissao.CONSUMO_LER,
        Permissao.SESSAO_LER,
        Permissao.SESSAO_LER_TELEFONE,
        Permissao.SESSAO_TRANSFERIR,
        Permissao.SESSAO_RETOMAR,
    }),
    "leitura": frozenset({
        Permissao.AGENTE_LER,
        Permissao.CONSUMO_LER,
        Permissao.SESSAO_LER,
    }),
}


def permissoes(papel: str) -> frozenset[Permissao]:
    """Unknown role → no permission (fail closed)."""

    return PAPEIS.get(papel, frozenset())


def pode(principal: Principal, permissao: Permissao) -> bool:
    return permissao in permissoes(principal.papel)


def exigir(*alternativas: Permissao):
    """Dependency: authenticated principal holding at least one permission."""

    if not alternativas:
        raise ValueError("exigir requires at least one permission")

    async def dependency(
        request: Request,
        principal: Principal = Depends(autenticar),
        sessions: async_sessionmaker[AsyncSession] = Depends(get_auth_sessions),
    ) -> Principal:
        if not any(pode(principal, p) for p in alternativas):
            await auditoria.acesso_negado(sessions, request, principal, alternativas)
            raise proibido()
        return principal

    dependency.__permissoes__ = tuple(alternativas)  # type: ignore[attr-defined]
    return dependency


def permissoes_da_rota(route) -> tuple[Permissao, ...] | None:
    declared = [
        getattr(dep.call, "__permissoes__")
        for dep in getattr(getattr(route, "dependant", None), "dependencies", ())
        if hasattr(dep.call, "__permissoes__")
    ]
    return declared[0] if len(declared) == 1 else None


async def exigir_rota_declarada(
    request: Request, principal: Principal = Depends(autenticar)
) -> Principal:
    """Router-level guard: a route without exactly one ``exigir`` is refused."""

    if permissoes_da_rota(request.scope.get("route")) is None:
        logger.error("Route without declared permission refused | rota={}",
                     getattr(request.scope.get("route"), "path", "?"))
        raise proibido()
    return principal
