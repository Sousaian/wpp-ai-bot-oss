"""Login (password + TOTP), logout and one-time-token activation/reset.

Every login failure branch (unknown e-mail, pending/disabled user, delayed
source, wrong password, admin without TOTP, malformed input) runs exactly
one argon2id derivation and raises the same ``CredenciaisInvalidas``.
Hashing always runs outside any DB transaction; the outcome is applied under
the user row lock and revalidated against the hash that was actually
verified.

AUTH-2: a correct password for a user with confirmed TOTP yields only a
short-lived challenge (``wpd_…``, body only, never a cookie); the session is
issued by ``verificar_totp``. An admin without confirmed TOTP is refused:
TOTP is enrolled through the activation or ``reset_mfa`` token flow, never
from a password-only login. TOTP codes are accepted only for a step newer
than the last accepted one, under the user row lock. Ten wrong codes lock the
second factor of the account for 15 min (doubling up to 1 h): only someone who
already proved the password reaches it, so it is no third-party lockout.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.repositories import auth_repo
from src.services.auth import auditoria, limites, origem, segredos, senha, totp
from src.services.auth.contexto import SESSAO_ABSOLUTA, run_auth_transaction


EMAIL_MAX = 254
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
TOKEN_ATIVACAO_VALIDADE = timedelta(hours=72)
TOKEN_RESET_VALIDADE = timedelta(hours=24)
DESAFIO_MFA_VALIDADE = timedelta(minutes=5)
DESAFIO_ENROLAMENTO_VALIDADE = timedelta(minutes=10)
TENTATIVAS_POR_DESAFIO = 5


class CredenciaisInvalidas(Exception):
    """Single generic login failure (no enumeration)."""


class CodigoInvalido(Exception):
    """Unknown/expired challenge or wrong/replayed TOTP code (generic)."""


class TokenInvalido(Exception):
    """Unknown, expired, revoked or already used one-time token."""


class EmailInvalido(ValueError):
    pass


class EnrolamentoNaoPermitido(Exception):
    """Voluntary enrolment while TOTP is already confirmed."""


@dataclass(frozen=True)
class SessaoEmitida:
    token: str
    csrf_token: str
    expira_em: datetime
    usuario_id: int
    papel: str


@dataclass(frozen=True)
class DesafioMfa:
    desafio: str
    expira_em: datetime


@dataclass(frozen=True)
class EnrolamentoIniciado:
    desafio: str
    expira_em: datetime
    segredo: str
    uri: str


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


def _registrar_falha_totp(usuario, agora: datetime) -> bool:
    """Count one wrong code; return True when it locked the second factor."""

    usuario.totp_falhas = int(usuario.totp_falhas) + 1
    if usuario.totp_falhas < limites.TOTP_FALHAS_ATE_BLOQUEIO:
        return False
    usuario.totp_bloqueado_ate = agora + limites.bloqueio_totp(int(usuario.totp_bloqueios))
    usuario.totp_bloqueios = int(usuario.totp_bloqueios) + 1
    usuario.totp_falhas = 0
    return True


def _zerar_falhas(usuario) -> None:
    usuario.falhas_consecutivas = 0
    usuario.totp_falhas = 0
    usuario.totp_bloqueios = 0
    usuario.totp_bloqueado_ate = None


async def _emitir_sessao(db: AsyncSession, *, usuario_id: int, papel: str, agora: datetime) -> SessaoEmitida:
    token, token_hash = segredos.novo("sessao")
    csrf, csrf_hash = segredos.novo("csrf")
    expira_em = agora + SESSAO_ABSOLUTA
    await auth_repo.criar_sessao(
        db, usuario_id=usuario_id, estagio="completa", token_hash=token_hash,
        csrf_hash=csrf_hash, agora=agora, expira_em=expira_em,
    )
    return SessaoEmitida(token, csrf, expira_em, usuario_id, papel)


async def _emitir_desafio(
    db: AsyncSession, *, usuario_id: int, estagio: str, agora: datetime, validade: timedelta
) -> tuple[str, datetime]:
    desafio, desafio_hash = segredos.novo("desafio")
    expira_em = agora + validade
    await auth_repo.criar_sessao(
        db, usuario_id=usuario_id, estagio=estagio, token_hash=desafio_hash,
        csrf_hash=None, agora=agora, expira_em=expira_em,
    )
    return desafio, expira_em


def _audit_login(usuario, email: str | None, origem_ip: str | None, motivo: str | None) -> dict[str, object]:
    detalhe: dict[str, object] = {"email_mascarado": auditoria.mascarar_email(email)}
    if motivo:
        detalhe["motivo"] = motivo
    return {
        "resultado": "falha" if motivo else "sucesso",
        "ator_tipo": "anonimo" if motivo or usuario is None else "usuario",
        "ator_id": None if motivo or usuario is None else int(usuario.id),
        "alvo_tipo": "usuario" if usuario is not None else None,
        "alvo_id": int(usuario.id) if usuario is not None else None,
        "origem_ip": origem_ip,
        "detalhe": detalhe,
    }


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
    sessions: async_sessionmaker[AsyncSession],
    *,
    email: object,
    senha_informada: object,
    origem_ip: str | None = None,
) -> SessaoEmitida | DesafioMfa:
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
    texto = (
        senha_informada
        if isinstance(senha_informada, str) and len(senha_informada) <= 4 * senha.SENHA_MAX
        else None
    )
    verificacao = await senha.verificar(
        texto,
        candidato.senha_hash if candidato else None,
        candidato.senha_pepper_versao if candidato else None,
    )
    if candidato is None:
        await auditoria.registrar_isolado(
            sessions, "login_falha", **_audit_login(None, canonico, origem_ip, "credenciais")
        )
        raise CredenciaisInvalidas()

    async def apply(db: AsyncSession) -> SessaoEmitida | DesafioMfa | None:
        usuario = await auth_repo.lock_usuario(db, candidato.usuario_id)
        if usuario is None:
            return None
        agora = await auth_repo.agora(db)
        motivo = None
        if usuario.senha_hash != candidato.senha_hash:
            motivo = "senha_alterada"  # the verified hash is stale
        elif usuario.status != "ativo":
            motivo = "status"
        else:
            motivo = await avaliar_origem(
                db, usuario, chave=origem.chave(origem_ip), agora=agora, ok=verificacao.ok
            )
        if motivo is None and usuario.totp_confirmado_em is None and usuario.papel == "admin":
            motivo = "admin_sem_mfa"
        if motivo:
            await db.flush()
            await auditoria.registrar(db, "login_falha", **_audit_login(usuario, canonico, origem_ip, motivo))
            return None
        if usuario.totp_confirmado_em is not None:
            # Password proven: the source delay is cleared; the second factor
            # keeps its own counters.
            usuario.falhas_consecutivas = 0
            await db.flush()
            desafio, expira_em = await _emitir_desafio(
                db, usuario_id=int(usuario.id), estagio="mfa", agora=agora,
                validade=DESAFIO_MFA_VALIDADE,
            )
            return DesafioMfa(desafio, expira_em)
        _zerar_falhas(usuario)
        await db.flush()
        await auditoria.registrar(db, "login_sucesso", **_audit_login(usuario, canonico, origem_ip, None))
        return await _emitir_sessao(db, usuario_id=int(usuario.id), papel=usuario.papel, agora=agora)

    resultado = await run_auth_transaction(sessions, apply)
    if resultado is None:
        raise CredenciaisInvalidas()
    if verificacao.precisa_rehash:
        await _rehash(sessions, candidato, texto)
    return resultado


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


async def _desafio_locked(db: AsyncSession, desafio_hash: str, estagio: str):
    """Discover the challenge row, then lock user → session and revalidate."""

    found = await auth_repo.get_sessao_com_usuario(db, desafio_hash)
    if found is None or found[0].estagio != estagio:
        return None
    usuario = await auth_repo.lock_usuario(db, int(found[1].id))
    registro = await auth_repo.lock_sessao(db, int(found[0].id))
    agora = await auth_repo.agora(db)
    if (
        usuario is None
        or registro is None
        or registro.usuario_id != usuario.id
        or registro.estagio != estagio
        or registro.revogada_em is not None
        or registro.expira_em <= agora
        or usuario.status == "desativado"
    ):
        return None
    return usuario, registro, agora


def _passo(usuario, codigo: object) -> int | None:
    if usuario.totp_segredo_cifrado is None:
        return None
    segredo = totp.decifrar(usuario.totp_segredo_cifrado, int(usuario.totp_chave_versao), int(usuario.id))
    if segredo is None:
        return None
    passo = totp.passo_aceito(segredo, codigo)
    if passo is None or (usuario.totp_ultimo_passo is not None and passo <= int(usuario.totp_ultimo_passo)):
        return None  # wrong code or replay of an already accepted step
    return passo


async def verificar_totp(
    sessions: async_sessionmaker[AsyncSession],
    *,
    desafio: object,
    codigo: object,
    origem_ip: str | None = None,
) -> SessaoEmitida:
    desafio_hash = segredos.digest_se_valido("desafio", desafio)
    if desafio_hash is None:
        raise CodigoInvalido()

    async def apply(db: AsyncSession) -> SessaoEmitida | None:
        locked = await _desafio_locked(db, desafio_hash, "mfa")
        if locked is None:
            return None
        usuario, registro, agora = locked
        registro.tentativas_mfa = int(registro.tentativas_mfa) + 1
        motivo = None
        if usuario.status != "ativo" or usuario.totp_confirmado_em is None:
            motivo = "status"
        elif limites.atrasado(usuario.totp_bloqueado_ate, agora):
            motivo = "bloqueado"
        elif registro.tentativas_mfa > TENTATIVAS_POR_DESAFIO:
            motivo = "tentativas"
        passo = None if motivo else _passo(usuario, codigo)
        if motivo is None and passo is None:
            motivo = "bloqueio_aplicado" if _registrar_falha_totp(usuario, agora) else "codigo"
        if motivo:
            if motivo in {"tentativas", "status", "bloqueado", "bloqueio_aplicado"}:
                registro.revogada_em, registro.motivo_revogacao = agora, "mfa_falhou"
            await db.flush()
            await auditoria.registrar(
                db, "mfa_falha", resultado="falha", ator_tipo="anonimo", alvo_tipo="usuario",
                alvo_id=int(usuario.id), origem_ip=origem_ip, detalhe={"motivo": motivo},
            )
            return None
        usuario.totp_ultimo_passo = passo
        _zerar_falhas(usuario)
        registro.revogada_em, registro.motivo_revogacao = agora, "mfa_concluida"
        await db.flush()
        await auditoria.registrar(
            db, "login_sucesso", ator_tipo="usuario", ator_id=int(usuario.id), alvo_tipo="usuario",
            alvo_id=int(usuario.id), origem_ip=origem_ip,
            detalhe={"email_mascarado": auditoria.mascarar_email(usuario.email), "tipo": "totp"},
        )
        return await _emitir_sessao(db, usuario_id=int(usuario.id), papel=usuario.papel, agora=agora)

    emitida = await run_auth_transaction(sessions, apply)
    if emitida is None:
        raise CodigoInvalido()
    return emitida


async def _iniciar_enrolamento_locked(db: AsyncSession, usuario, agora: datetime) -> EnrolamentoIniciado:
    """Caller holds the user X lock. Replaces any unconfirmed secret."""

    segredo = totp.novo_segredo()
    cifrado = totp.cifrar(segredo, int(usuario.id))
    usuario.totp_segredo_cifrado = cifrado.cifrado
    usuario.totp_chave_versao = cifrado.versao
    usuario.totp_confirmado_em = None
    usuario.totp_ultimo_passo = None
    await db.flush()
    desafio, expira_em = await _emitir_desafio(
        db, usuario_id=int(usuario.id), estagio="enrolamento", agora=agora,
        validade=DESAFIO_ENROLAMENTO_VALIDADE,
    )
    return EnrolamentoIniciado(desafio, expira_em, segredo, totp.uri(segredo, usuario.email))


async def iniciar_enrolamento(
    sessions: async_sessionmaker[AsyncSession], *, usuario_id: int, origem_ip: str | None = None
) -> EnrolamentoIniciado:
    """Voluntary enrolment from a full session (non-admin without TOTP)."""

    async def op(db: AsyncSession) -> EnrolamentoIniciado:
        usuario = await auth_repo.lock_usuario(db, usuario_id)
        if usuario is None or usuario.status != "ativo" or usuario.totp_confirmado_em is not None:
            raise EnrolamentoNaoPermitido()
        agora = await auth_repo.agora(db)
        iniciado = await _iniciar_enrolamento_locked(db, usuario, agora)
        await auditoria.registrar(
            db, "totp_enrolamento_iniciado", ator_tipo="usuario", ator_id=usuario_id,
            alvo_tipo="usuario", alvo_id=usuario_id, origem_ip=origem_ip, detalhe={"tipo": "voluntario"},
        )
        return iniciado

    return await run_auth_transaction(sessions, op)


async def confirmar_totp(
    sessions: async_sessionmaker[AsyncSession],
    *,
    desafio: object,
    codigo: object,
    origem_ip: str | None = None,
) -> str:
    """Confirm an enrolment; a pending admin becomes ``ativo`` only here."""

    desafio_hash = segredos.digest_se_valido("desafio", desafio)
    if desafio_hash is None:
        raise CodigoInvalido()

    async def apply(db: AsyncSession) -> str | None:
        locked = await _desafio_locked(db, desafio_hash, "enrolamento")
        if locked is None:
            return None
        usuario, registro, agora = locked
        registro.tentativas_mfa = int(registro.tentativas_mfa) + 1
        passo = (
            _passo(usuario, codigo)
            if registro.tentativas_mfa <= TENTATIVAS_POR_DESAFIO and usuario.totp_confirmado_em is None
            else None
        )
        if passo is None:
            if registro.tentativas_mfa >= TENTATIVAS_POR_DESAFIO:
                registro.revogada_em, registro.motivo_revogacao = agora, "mfa_falhou"
            await db.flush()
            await auditoria.registrar(
                db, "mfa_falha", resultado="falha", ator_tipo="anonimo", alvo_tipo="usuario",
                alvo_id=int(usuario.id), origem_ip=origem_ip, detalhe={"motivo": "enrolamento"},
            )
            return None
        usuario.totp_confirmado_em = agora
        usuario.totp_ultimo_passo = passo
        if usuario.status == "pendente" and usuario.senha_hash is not None:
            usuario.status = "ativo"
        registro.revogada_em, registro.motivo_revogacao = agora, "enrolamento_concluido"
        await db.flush()
        await auditoria.registrar(
            db, "totp_confirmado", ator_tipo="usuario", ator_id=int(usuario.id), alvo_tipo="usuario",
            alvo_id=int(usuario.id), origem_ip=origem_ip, detalhe={"status": usuario.status},
        )
        return usuario.status

    status = await run_auth_transaction(sessions, apply)
    if status is None:
        raise CodigoInvalido()
    return status


async def logout(
    sessions: async_sessionmaker[AsyncSession], *, sessao_id: int, usuario_id: int, origem_ip: str | None = None
) -> None:
    async def revoke(db: AsyncSession) -> None:
        agora = await auth_repo.agora(db)
        await auth_repo.revogar_sessao(db, sessao_id=sessao_id, motivo="logout", agora=agora)
        await auditoria.registrar(
            db, "logout", ator_tipo="usuario", ator_id=usuario_id, alvo_tipo="sessao", alvo_id=sessao_id,
            origem_ip=origem_ip,
        )

    await run_auth_transaction(sessions, revoke)


async def usar_token(
    sessions: async_sessionmaker[AsyncSession],
    *,
    token: object,
    nova_senha: object,
    origem_ip: str | None = None,
) -> EnrolamentoIniciado | None:
    """Consume an ``ativacao``, ``reset_senha`` or ``reset_mfa`` token.

    ``ativacao``/``reset_senha`` set the password; an admin activation then
    starts the mandatory TOTP enrolment (the user stays ``pendente`` until
    ``confirmar_totp``). ``reset_mfa`` needs no password: it clears TOTP,
    revokes every session and starts a new enrolment. The token is consumed
    together with its effect, in one transaction.
    """

    token_hash = segredos.digest_se_valido("uso_unico", token)
    if token_hash is None:
        raise TokenInvalido()

    async def discover(db: AsyncSession) -> tuple[int, int, str] | None:
        found = await auth_repo.get_token_uso_unico_por_hash(db, token_hash)
        return (int(found.id), int(found.usuario_id), found.tipo) if found else None

    found = await run_auth_transaction(sessions, discover)
    if found is None:
        raise TokenInvalido()
    token_id, usuario_id, tipo = found
    novo_hash = versao = None
    if tipo != "reset_mfa":
        if not isinstance(nova_senha, str):
            raise senha.SenhaRecusada("senha_invalida")
        novo_hash, versao = await senha.gerar_hash(nova_senha)

    async def apply(db: AsyncSession) -> tuple[bool, EnrolamentoIniciado | None]:
        usuario = await auth_repo.lock_usuario(db, usuario_id)
        registro = await auth_repo.lock_token_uso_unico(db, token_id)
        agora = await auth_repo.agora(db)
        if (
            usuario is None
            or registro is None
            or registro.usuario_id != usuario.id
            or registro.tipo != tipo
            or registro.usado_em is not None
            or registro.revogado_em is not None
            or registro.expira_em <= agora
            or usuario.status == "desativado"
            or (tipo == "ativacao") != (usuario.status == "pendente")
        ):
            return False, None
        registro.usado_em = agora
        if tipo == "reset_mfa":
            await auth_repo.limpar_totp(db, usuario)
            motivo = "mfa_redefinido"
        else:
            usuario.senha_hash = novo_hash
            usuario.senha_pepper_versao = versao
            _zerar_falhas(usuario)
            if not (tipo == "ativacao" and usuario.papel == "admin"):
                usuario.status = "ativo"
            motivo = "senha_redefinida"
            await auth_repo.limpar_tentativas(db, usuario_id=usuario_id)
        await db.flush()
        await auth_repo.revogar_sessoes_usuario(db, usuario_id=usuario_id, motivo=motivo, agora=agora)
        enrolamento = None
        if tipo == "reset_mfa" or (tipo == "ativacao" and usuario.papel == "admin"):
            enrolamento = await _iniciar_enrolamento_locked(db, usuario, agora)
        await auditoria.registrar(
            db, "senha_definida" if tipo != "reset_mfa" else "totp_enrolamento_iniciado",
            ator_tipo="anonimo", alvo_tipo="usuario", alvo_id=usuario_id, origem_ip=origem_ip,
            detalhe={"tipo": tipo},
        )
        return True, enrolamento

    ok, enrolamento = await run_auth_transaction(sessions, apply)
    if not ok:
        raise TokenInvalido()
    return enrolamento
