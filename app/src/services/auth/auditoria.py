"""Allowlisted audit of logins, authorization denials and admin actions.

Phones and e-mails never enter ``auth_auditoria`` in clear: ``detalhe``
accepts only allowlisted keys and the masking helpers below. Domain actions
are recorded inside the route's own transaction, after every lifecycle lock
(leaf insert, no FK), so the action and its audit commit or roll back
together. Login events and denials use their own short transaction; if that
insert fails the request outcome stands and the gap is signalled
(counter + ERROR, threshold 1).
"""

from __future__ import annotations

from collections.abc import Iterable

from fastapi import Request
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.repositories import auth_repo, scheduler_repo
from src.services.auth import segredos


EVENTOS = frozenset({
    # /auth
    "login_sucesso", "login_falha", "mfa_falha", "logout", "senha_definida",
    "totp_enrolamento_iniciado", "totp_confirmado", "acesso_negado",
    # /agentes, /sessoes
    "agente_criado", "agente_atualizado", "agente_excluido",
    "sessao_transferida", "sessao_retomada", "sessao_excluida",
    # container CLI
    "bootstrap_admin", "usuario_criado", "usuario_papel_alterado", "usuario_desativado",
    "usuario_reativado", "usuario_desbloqueado", "reset_emitido", "sessoes_revogadas",
    "token_servico_criado", "token_servico_revogado",
})
CHAVES_DETALHE = frozenset({
    "agente_id", "campos", "codigo", "email_mascarado", "metodo", "motivo", "origens_liberadas", "papel",
    "papel_anterior", "permissoes", "rota", "sessoes_revogadas", "status", "telefone_mascarado",
    "tipo",
})
SIGNAL_THRESHOLD = 1
_falhas_registro = 0


def falhas_registro() -> int:
    return _falhas_registro


def mascarar_telefone(telefone: object) -> str:
    """Keep the first 4 and last 4 digits: ``5511*****0001``."""

    texto = str(telefone or "")
    if len(texto) <= 8:
        return "*" * max(len(texto) - 2, 0) + texto[-2:]
    return texto[:4] + "*" * (len(texto) - 8) + texto[-4:]


def mascarar_email(email: object) -> str:
    texto = str(email or "")
    if not segredos.texto_utf8_valido(texto):
        return "***"
    local, arroba, dominio = texto.partition("@")
    if not arroba:
        return "***"
    return (local[:1] or "*") + "***@" + dominio


def _detalhe(detalhe: dict[str, object] | None) -> dict[str, object] | None:
    if not detalhe:
        return None
    unknown = set(detalhe) - CHAVES_DETALHE
    if unknown:
        raise ValueError(f"audit detail keys not allowlisted: {sorted(unknown)}")
    return dict(detalhe)


def ator(principal) -> dict[str, object]:
    return {"ator_tipo": principal.tipo, "ator_id": principal.id, "origem_ip": principal.origem_ip}


async def registrar(
    db: AsyncSession,
    evento: str,
    *,
    resultado: str = "sucesso",
    ator_tipo: str,
    ator_id: int | None = None,
    ator_rotulo: str | None = None,
    alvo_tipo: str | None = None,
    alvo_id: object = None,
    origem_ip: str | None = None,
    detalhe: dict[str, object] | None = None,
) -> None:
    """Insert in the caller's transaction (no commit)."""

    if evento not in EVENTOS:
        raise ValueError(f"audit event not allowlisted: {evento}")
    await auth_repo.registrar_auditoria(
        db,
        evento=evento,
        resultado=resultado,
        ator_tipo=ator_tipo,
        ator_id=ator_id,
        ator_rotulo=(ator_rotulo or None) and ator_rotulo[:100],
        alvo_tipo=alvo_tipo,
        alvo_id=None if alvo_id is None else str(alvo_id)[:64],
        origem_ip=origem_ip,
        detalhe=_detalhe(detalhe),
    )


async def registrar_isolado(sessions: async_sessionmaker[AsyncSession], evento: str, **kwargs) -> None:
    """Own short transaction; failure is signalled, never raised."""

    global _falhas_registro
    try:
        await scheduler_repo.run_transaction_with_deadlock_retry(
            sessions, lambda db: registrar(db, evento, **kwargs)
        )
    except ValueError:
        raise
    except Exception as exc:
        _falhas_registro += 1
        logger.error(
            "Auth audit insert failed | evento={} erro_tipo={} count={} threshold={}",
            evento, type(exc).__name__, _falhas_registro, SIGNAL_THRESHOLD,
        )


async def acesso_negado(
    sessions: async_sessionmaker[AsyncSession], request: Request, principal, permissoes: Iterable[object]
) -> None:
    route = request.scope.get("route")
    await registrar_isolado(
        sessions,
        "acesso_negado",
        resultado="negado",
        **ator(principal),
        detalhe={
            "metodo": request.method,
            "rota": getattr(route, "path", None),
            "permissoes": sorted(str(getattr(p, "value", p)) for p in permissoes),
        },
    )
