"""Login, logout and one-time-token activation/reset.

Every login failure branch (unknown e-mail, pending/disabled user, delayed
source, wrong password, malformed input) runs exactly one argon2id
derivation and raises the same ``CredenciaisInvalidas``. Hashing always runs
outside any DB transaction; the outcome is applied under the user row lock
and revalidated against the hash that was actually verified.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.repositories import auth_repo
from src.services.auth import limites, origem, segredos, senha
from src.services.auth.contexto import SESSAO_ABSOLUTA, run_auth_transaction


EMAIL_MAX = 254
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
TOKEN_ATIVACAO_VALIDADE = timedelta(hours=72)
TOKEN_RESET_VALIDADE = timedelta(hours=24)


class CredenciaisInvalidas(Exception):
    """Single generic login failure (no enumeration)."""


class TokenInvalido(Exception):
    """Unknown, expired, revoked or already used one-time token."""


class EmailInvalido(ValueError):
    pass


@dataclass(frozen=True)
class SessaoEmitida:
    token: str
    csrf_token: str
    expira_em: datetime
    usuario_id: int
    papel: str


@dataclass(frozen=True)
class _Candidato:
    usuario_id: int
    senha_hash: str | None
    senha_pepper_versao: int | None


def canonicalizar_email(email: object) -> str:
    """strip + NFC + lowercase; the canonical form is the UNIQUE key."""

    if not isinstance(email, str):
        raise EmailInvalido("email_invalido")
    canonico = unicodedata.normalize("NFC", email.strip()).lower()
    if len(canonico) > EMAIL_MAX or not _EMAIL.fullmatch(canonico):
        raise EmailInvalido("email_invalido")
    return canonico


async def _emitir_sessao(db: AsyncSession, *, usuario_id: int, papel: str, agora: datetime) -> SessaoEmitida:
    token, token_hash = segredos.novo("sessao")
    csrf, csrf_hash = segredos.novo("csrf")
    expira_em = agora + SESSAO_ABSOLUTA
    await auth_repo.criar_sessao(
        db,
        usuario_id=usuario_id,
        estagio="completa",
        token_hash=token_hash,
        csrf_hash=csrf_hash,
        agora=agora,
        expira_em=expira_em,
    )
    return SessaoEmitida(token, csrf, expira_em, usuario_id, papel)


async def avaliar_origem(db: AsyncSession, usuario, *, chave: str, agora: datetime, ok: bool) -> str | None:
    """Delay decision for one (account, source); caller holds the user lock.

    Returns None when the correct password may proceed, else the refusal
    motive. A delayed source is refused even with the right password and its
    attempts neither count nor extend the delay.
    """

    usuario_id = int(usuario.id)
    registro = await auth_repo.lock_tentativa(db, usuario_id=usuario_id, origem=chave)
    if registro is not None and limites.atrasado(registro.liberado_em, agora):
        return "atrasado"
    if ok:
        if registro is not None:
            await auth_repo.limpar_tentativas(db, usuario_id=usuario_id, origem=chave)
        return None
    recente = registro is not None and registro.ultima_falha_em > agora - limites.FALHAS_VALIDADE
    falhas = (int(registro.falhas) if recente else 0) + 1
    espera = limites.atraso(falhas)
    if registro is None:
        # Bounded growth: expired sources leave when a new one appears.
        await auth_repo.limpar_tentativas(
            db, usuario_id=usuario_id, antes_de=agora - limites.FALHAS_VALIDADE
        )
    await auth_repo.registrar_tentativa(
        db, registro=registro, usuario_id=usuario_id, origem=chave, falhas=falhas, agora=agora,
        liberado_em=agora + espera if espera else None,
    )
    usuario.falhas_consecutivas = int(usuario.falhas_consecutivas) + 1
    if usuario.falhas_consecutivas % limites.SINAL_FALHAS_CONTA == 0:
        logger.error(
            "Auth account under repeated failures | usuario={} count={} threshold={}",
            usuario_id, usuario.falhas_consecutivas, limites.SINAL_FALHAS_CONTA,
        )
    await db.flush()
    return "atraso_aplicado" if espera else "credenciais"


async def login(
    sessions: async_sessionmaker[AsyncSession], *, email: object, senha_informada: object, origem_ip: str | None
) -> SessaoEmitida:
    senha.exigir_capacidade()
    try:
        canonico = canonicalizar_email(email)
    except EmailInvalido:
        canonico = None

    async def discover(db: AsyncSession) -> _Candidato | None:
        if canonico is None:
            return None
        usuario = await auth_repo.get_usuario_por_email(db, canonico)
        if usuario is None:
            return None
        return _Candidato(int(usuario.id), usuario.senha_hash, usuario.senha_pepper_versao)

    candidato = await run_auth_transaction(sessions, discover)
    texto = senha_informada if isinstance(senha_informada, str) and len(senha_informada) <= 4 * senha.SENHA_MAX else None
    verificacao = await senha.verificar(
        texto,
        candidato.senha_hash if candidato else None,
        candidato.senha_pepper_versao if candidato else None,
    )
    if candidato is None:
        raise CredenciaisInvalidas()

    async def apply(db: AsyncSession) -> SessaoEmitida | None:
        usuario = await auth_repo.lock_usuario(db, candidato.usuario_id)
        if usuario is None or usuario.senha_hash != candidato.senha_hash:
            return None  # concurrent password change: the verified hash is stale
        agora = await auth_repo.agora(db)
        if usuario.status != "ativo":
            return None
        if await avaliar_origem(
            db, usuario, chave=origem.chave(origem_ip), agora=agora, ok=verificacao.ok
        ) is not None:
            return None
        usuario.falhas_consecutivas = 0
        await db.flush()
        return await _emitir_sessao(db, usuario_id=int(usuario.id), papel=usuario.papel, agora=agora)

    emitida = await run_auth_transaction(sessions, apply)
    if emitida is None:
        raise CredenciaisInvalidas()
    if verificacao.precisa_rehash:
        await _rehash(sessions, candidato, texto)
    return emitida


async def _rehash(
    sessions: async_sessionmaker[AsyncSession], candidato: _Candidato, texto: str
) -> None:
    try:
        novo_hash, versao = await senha.gerar_hash(texto)
    except (senha.SenhaRecusada, senha.HashOcupado):
        return  # legacy policy or saturated pool: keep the old hash, login stands
    await run_auth_transaction(
        sessions,
        lambda db: auth_repo.substituir_senha_se_inalterada(
            db,
            usuario_id=candidato.usuario_id,
            anterior=candidato.senha_hash or "",
            novo_hash=novo_hash,
            versao=versao,
        ),
    )


async def logout(sessions: async_sessionmaker[AsyncSession], *, sessao_id: int) -> None:
    async def revoke(db: AsyncSession) -> None:
        agora = await auth_repo.agora(db)
        await auth_repo.revogar_sessao(db, sessao_id=sessao_id, motivo="logout", agora=agora)

    await run_auth_transaction(sessions, revoke)


async def usar_token(
    sessions: async_sessionmaker[AsyncSession], *, token: object, nova_senha: object
) -> None:
    """Consume an ``ativacao`` or ``reset_senha`` token and set the password.

    The password policy is checked before the token is touched; a token is
    consumed only together with the password change, in one transaction.
    """

    token_hash = segredos.digest_se_valido("uso_unico", token)
    if token_hash is None:
        raise TokenInvalido()

    async def discover(db: AsyncSession) -> tuple[int, int] | None:
        found = await auth_repo.get_token_uso_unico_por_hash(db, token_hash)
        return (int(found.id), int(found.usuario_id)) if found else None

    found = await run_auth_transaction(sessions, discover)
    if found is None:
        raise TokenInvalido()
    if not isinstance(nova_senha, str):
        raise senha.SenhaRecusada("senha_invalida")
    novo_hash, versao = await senha.gerar_hash(nova_senha)
    token_id, usuario_id = found

    async def apply(db: AsyncSession) -> bool:
        usuario = await auth_repo.lock_usuario(db, usuario_id)
        registro = await auth_repo.lock_token_uso_unico(db, token_id)
        agora = await auth_repo.agora(db)
        if (
            usuario is None
            or registro is None
            or registro.usuario_id != usuario.id
            or registro.tipo not in {"ativacao", "reset_senha"}
            or registro.usado_em is not None
            or registro.revogado_em is not None
            or registro.expira_em <= agora
            or usuario.status == "desativado"
            or (registro.tipo == "ativacao") != (usuario.status == "pendente")
        ):
            return False
        registro.usado_em = agora
        usuario.senha_hash = novo_hash
        usuario.senha_pepper_versao = versao
        usuario.status = "ativo"
        usuario.falhas_consecutivas = 0
        await db.flush()
        await auth_repo.limpar_tentativas(db, usuario_id=usuario_id)
        await auth_repo.revogar_sessoes_usuario(
            db, usuario_id=usuario_id, motivo="senha_redefinida", agora=agora
        )
        return True

    if not await run_auth_transaction(sessions, apply):
        raise TokenInvalido()
