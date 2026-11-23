"""Container-only administration used by ``python -m src.auth_cli``.

Plaintext tokens are returned exactly once to the caller (the CLI prints
them); only their SHA-256 is stored. Role change, deactivation and every
reset revoke the user's sessions in the same transaction. The last usable
admin can never be demoted or disabled (admin range locked first). Every
mutation writes its ``auth_auditoria`` row (``ator_tipo=cli``, operator
label from ``--actor``) in the same transaction; e-mails are masked.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.models import AUTH_PAPEIS, AUTH_PAPEIS_TOKEN_SERVICO
from src.database.repositories import auth_repo
from src.services.auth import auditoria, segredos
from src.services.auth.contexto import run_auth_transaction
from src.services.auth.sessao import (
    TOKEN_ATIVACAO_VALIDADE,
    TOKEN_RESET_VALIDADE,
    canonicalizar_email,
)


TOKEN_SERVICO_MAX_DIAS = 365
NOME_TOKEN_MAX = 100
TIPOS_RESET = {"senha": "reset_senha", "mfa": "reset_mfa", "ativacao": "ativacao"}


class Recusado(Exception):
    """Named refusal; ``code`` is stable and value-free."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class TokenEmitido:
    usuario_id: int
    tipo: str
    token: str
    expira_em: datetime


def _papel(papel: str, permitidos=AUTH_PAPEIS) -> str:
    if papel not in permitidos:
        raise Recusado("papel_invalido")
    return papel


async def _audit(db: AsyncSession, evento: str, ator: str, *, alvo_tipo: str, alvo_id: object,
                 detalhe: dict[str, object] | None = None) -> None:
    await auditoria.registrar(
        db, evento, ator_tipo="cli", ator_rotulo=ator, alvo_tipo=alvo_tipo, alvo_id=alvo_id, detalhe=detalhe
    )


def _admin_utilizavel(usuario) -> bool:
    return usuario.papel == "admin" and usuario.status == "ativo"


async def _emitir(db: AsyncSession, *, usuario_id: int, tipo: str, agora: datetime) -> TokenEmitido:
    token, token_hash = segredos.novo("uso_unico")
    validade = TOKEN_ATIVACAO_VALIDADE if tipo == "ativacao" else TOKEN_RESET_VALIDADE
    expira_em = agora + validade
    await auth_repo.emitir_token_uso_unico(
        db, usuario_id=usuario_id, tipo=tipo, token_hash=token_hash, agora=agora, expira_em=expira_em
    )
    return TokenEmitido(usuario_id, tipo, token, expira_em)


async def bootstrap_admin(sessions: async_sessionmaker[AsyncSession], *, ator: str, email: str) -> TokenEmitido:
    """First admin only: refused once any admin row (any status) exists."""

    canonico = canonicalizar_email(email)

    async def op(db: AsyncSession) -> TokenEmitido:
        if await auth_repo.lock_admins(db):
            raise Recusado("admin_ja_existe")
        agora = await auth_repo.agora(db)
        try:
            usuario = await auth_repo.criar_usuario(db, email=canonico, papel="admin", status="pendente")
        except auth_repo.EmailIndisponivel:
            raise Recusado("email_indisponivel") from None
        await _audit(db, "bootstrap_admin", ator, alvo_tipo="usuario", alvo_id=usuario.id,
                     detalhe={"email_mascarado": auditoria.mascarar_email(canonico), "papel": "admin"})
        return await _emitir(db, usuario_id=int(usuario.id), tipo="ativacao", agora=agora)

    return await run_auth_transaction(sessions, op)


async def criar_usuario(
    sessions: async_sessionmaker[AsyncSession], *, ator: str, email: str, papel: str
) -> TokenEmitido:
    canonico = canonicalizar_email(email)
    _papel(papel)

    async def op(db: AsyncSession) -> TokenEmitido:
        if papel == "admin":
            await auth_repo.lock_admins(db)
        agora = await auth_repo.agora(db)
        try:
            usuario = await auth_repo.criar_usuario(db, email=canonico, papel=papel, status="pendente")
        except auth_repo.EmailIndisponivel:
            raise Recusado("email_indisponivel") from None
        await _audit(db, "usuario_criado", ator, alvo_tipo="usuario", alvo_id=usuario.id,
                     detalhe={"email_mascarado": auditoria.mascarar_email(canonico), "papel": papel})
        return await _emitir(db, usuario_id=int(usuario.id), tipo="ativacao", agora=agora)

    return await run_auth_transaction(sessions, op)


async def _lock_alvo(db: AsyncSession, usuario_id: int):
    admins = await auth_repo.lock_admins(db)
    usuario = await auth_repo.lock_usuario(db, usuario_id)
    if usuario is None:
        raise Recusado("usuario_inexistente")
    return admins, usuario


def _ultimo_admin(admins, usuario) -> bool:
    restantes = [a for a in admins if a.id != usuario.id and _admin_utilizavel(a)]
    return _admin_utilizavel(usuario) and not restantes


async def alterar_papel(
    sessions: async_sessionmaker[AsyncSession], *, ator: str, usuario_id: int, papel: str
) -> dict[str, object]:
    _papel(papel)

    async def op(db: AsyncSession) -> dict[str, object]:
        admins, usuario = await _lock_alvo(db, usuario_id)
        if usuario.papel == papel:
            raise Recusado("papel_inalterado")
        if usuario.papel == "admin" and _ultimo_admin(admins, usuario):
            raise Recusado("ultimo_admin")
        agora = await auth_repo.agora(db)
        anterior = usuario.papel
        usuario.papel = papel
        await db.flush()
        revogadas = await auth_repo.revogar_sessoes_usuario(
            db, usuario_id=usuario_id, motivo="papel_alterado", agora=agora
        )
        await _audit(db, "usuario_papel_alterado", ator, alvo_tipo="usuario", alvo_id=usuario_id,
                     detalhe={"papel_anterior": anterior, "papel": papel, "sessoes_revogadas": revogadas})
        return {"usuario_id": usuario_id, "papel_anterior": anterior, "papel": papel,
                "sessoes_revogadas": revogadas,
                # An admin logs in only after TOTP: issue `reset --tipo mfa` if absent.
                "mfa_requerido": papel == "admin" and usuario.totp_confirmado_em is None}

    return await run_auth_transaction(sessions, op)


async def alterar_status(
    sessions: async_sessionmaker[AsyncSession], *, ator: str, usuario_id: int, ativo: bool
) -> dict[str, object]:
    async def op(db: AsyncSession) -> dict[str, object]:
        admins, usuario = await _lock_alvo(db, usuario_id)
        agora = await auth_repo.agora(db)
        if not ativo:
            if usuario.status == "desativado":
                raise Recusado("status_inalterado")
            if _ultimo_admin(admins, usuario):
                raise Recusado("ultimo_admin")
            usuario.status = "desativado"
            await db.flush()
            revogadas = await auth_repo.revogar_sessoes_usuario(
                db, usuario_id=usuario_id, motivo="usuario_desativado", agora=agora
            )
            await auth_repo.revogar_tokens_uso_unico(db, usuario_id=usuario_id, agora=agora)
            await _audit(db, "usuario_desativado", ator, alvo_tipo="usuario", alvo_id=usuario_id,
                         detalhe={"sessoes_revogadas": revogadas})
            return {"usuario_id": usuario_id, "status": "desativado", "sessoes_revogadas": revogadas}
        if usuario.status != "desativado":
            raise Recusado("status_inalterado")
        # Reactivation needs a usable password; otherwise use a reset token.
        usuario.status = "ativo" if usuario.senha_hash else "pendente"
        await db.flush()
        await _audit(db, "usuario_reativado", ator, alvo_tipo="usuario", alvo_id=usuario_id,
                     detalhe={"status": usuario.status})
        return {"usuario_id": usuario_id, "status": usuario.status, "sessoes_revogadas": 0}

    return await run_auth_transaction(sessions, op)


async def emitir_reset(
    sessions: async_sessionmaker[AsyncSession], *, ator: str, usuario_id: int, tipo: str
) -> TokenEmitido:
    tipo_token = TIPOS_RESET.get(tipo)
    if tipo_token is None:
        raise Recusado("tipo_invalido")

    async def op(db: AsyncSession) -> TokenEmitido:
        usuario = await auth_repo.lock_usuario(db, usuario_id)
        if usuario is None:
            raise Recusado("usuario_inexistente")
        if usuario.status == "desativado":
            raise Recusado("usuario_desativado")
        if (tipo_token == "ativacao") != (usuario.status == "pendente"):
            raise Recusado("tipo_incompativel_com_status")
        agora = await auth_repo.agora(db)
        revogadas = await auth_repo.revogar_sessoes_usuario(
            db, usuario_id=usuario_id, motivo="reset_emitido", agora=agora
        )
        await _audit(db, "reset_emitido", ator, alvo_tipo="usuario", alvo_id=usuario_id,
                     detalhe={"tipo": tipo_token, "sessoes_revogadas": revogadas})
        return await _emitir(db, usuario_id=usuario_id, tipo=tipo_token, agora=agora)

    return await run_auth_transaction(sessions, op)


async def desbloquear(sessions: async_sessionmaker[AsyncSession], *, ator: str, usuario_id: int) -> dict[str, object]:
    async def op(db: AsyncSession) -> dict[str, object]:
        usuario = await auth_repo.lock_usuario(db, usuario_id)
        if usuario is None:
            raise Recusado("usuario_inexistente")
        usuario.falhas_consecutivas = 0
        usuario.bloqueios_consecutivos = 0
        usuario.bloqueado_ate = None
        await db.flush()
        await _audit(db, "usuario_desbloqueado", ator, alvo_tipo="usuario", alvo_id=usuario_id)
        return {"usuario_id": usuario_id, "status": "desbloqueado"}

    return await run_auth_transaction(sessions, op)


async def revogar_sessoes(sessions: async_sessionmaker[AsyncSession], *, ator: str, usuario_id: int) -> dict[str, object]:
    async def op(db: AsyncSession) -> dict[str, object]:
        usuario = await auth_repo.lock_usuario(db, usuario_id)
        if usuario is None:
            raise Recusado("usuario_inexistente")
        agora = await auth_repo.agora(db)
        revogadas = await auth_repo.revogar_sessoes_usuario(
            db, usuario_id=usuario_id, motivo="operador", agora=agora
        )
        await _audit(db, "sessoes_revogadas", ator, alvo_tipo="usuario", alvo_id=usuario_id,
                     detalhe={"sessoes_revogadas": revogadas})
        return {"usuario_id": usuario_id, "sessoes_revogadas": revogadas}

    return await run_auth_transaction(sessions, op)


async def listar_usuarios(sessions: async_sessionmaker[AsyncSession]) -> list[dict[str, object]]:
    async def op(db: AsyncSession) -> list[dict[str, object]]:
        agora = await auth_repo.agora(db)
        return [
            {
                "id": int(u.id), "email": u.email, "papel": u.papel, "status": u.status,
                "mfa_ativo": u.totp_confirmado_em is not None,
                "bloqueado": bool(u.bloqueado_ate and u.bloqueado_ate > agora),
            }
            for u in await auth_repo.listar_usuarios(db)
        ]

    return await run_auth_transaction(sessions, op)


async def criar_token_servico(
    sessions: async_sessionmaker[AsyncSession], *, ator: str, nome: str, papel: str, dias: int
) -> dict[str, object]:
    """Service tokens never carry ``admin``: that role requires human MFA."""

    _papel(papel, AUTH_PAPEIS_TOKEN_SERVICO)
    nome = (nome or "").strip()
    if not nome or len(nome) > NOME_TOKEN_MAX:
        raise Recusado("nome_invalido")
    if not 1 <= dias <= TOKEN_SERVICO_MAX_DIAS:
        raise Recusado("validade_invalida")
    token, token_hash = segredos.novo("servico")

    async def op(db: AsyncSession) -> dict[str, object]:
        agora = await auth_repo.agora(db)
        row = await auth_repo.criar_token_servico(
            db, nome=nome, papel=papel, token_hash=token_hash, agora=agora,
            expira_em=agora + timedelta(days=dias),
        )
        await _audit(db, "token_servico_criado", ator, alvo_tipo="token_servico", alvo_id=row.id,
                     detalhe={"papel": papel})
        return {"id": int(row.id), "nome": nome, "papel": papel, "expira_em": row.expira_em.isoformat(),
                "token": token}

    return await run_auth_transaction(sessions, op)


async def listar_tokens_servico(sessions: async_sessionmaker[AsyncSession]) -> list[dict[str, object]]:
    async def op(db: AsyncSession) -> list[dict[str, object]]:
        agora = await auth_repo.agora(db)
        return [
            {
                "id": int(t.id), "nome": t.nome, "papel": t.papel,
                "expira_em": t.expira_em.isoformat(),
                "revogado": t.revogado_em is not None,
                "expirado": t.expira_em <= agora,
                "ultimo_uso_em": t.ultimo_uso_em.isoformat() if t.ultimo_uso_em else None,
            }
            for t in await auth_repo.listar_tokens_servico(db)
        ]

    return await run_auth_transaction(sessions, op)


async def revogar_token_servico(sessions: async_sessionmaker[AsyncSession], *, ator: str, token_id: int) -> dict[str, object]:
    async def op(db: AsyncSession) -> dict[str, object]:
        agora = await auth_repo.agora(db)
        result = await auth_repo.revogar_token_servico(db, token_id=token_id, agora=agora)
        if result is None:
            raise Recusado("token_inexistente")
        if result is False:
            raise Recusado("token_ja_revogado")
        await _audit(db, "token_servico_revogado", ator, alvo_tipo="token_servico", alvo_id=token_id)
        return {"id": token_id, "status": "revogado"}

    return await run_auth_transaction(sessions, op)
