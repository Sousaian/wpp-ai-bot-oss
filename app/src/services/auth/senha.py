"""Password policy and hashing: HMAC-SHA-256 pepper, then argon2id.

The pepper keyring is versioned (``AUTH_PASSWORD_PEPPER_KEYS``); a hash made
with an older version or older argon2 parameters is rehashed on login. Every
verification runs exactly one argon2id derivation with the current cost,
including the branches without a usable hash (dummy hash), so login failure
branches are not distinguishable by work performed.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import unicodedata
from dataclasses import dataclass

from argon2 import PasswordHasher, Type
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

from src.config import settings


SENHA_MIN = 8
SENHA_MAX = 64

# argon2id m=64 MiB, t=3, p=4, 16-byte salt, 32-byte digest (PHC string).
HASHER = PasswordHasher(
    time_cost=3, memory_cost=65536, parallelism=4, hash_len=32, salt_len=16, type=Type.ID
)

# Small blocklist of common choices; long, unique passphrases are preferred.
BLOCKLIST = frozenset({
    "12345678", "123456789", "1234567890", "87654321", "11111111", "00000000",
    "password", "password1", "passw0rd", "qwertyui", "qwerty123", "abcdefgh",
    "senha123", "senha1234", "mudar123", "trocar123", "admin123", "administrador",
    "iloveyou", "letmein1", "welcome1", "changeme", "whatsapp", "atendimento",
})


class SenhaRecusada(ValueError):
    """Policy refusal with a fixed, value-free code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class PepperIndisponivel(RuntimeError):
    """The keyring cannot hash or verify (auth not provisioned)."""


@dataclass(frozen=True)
class Verificacao:
    ok: bool
    precisa_rehash: bool


def normalizar(senha: str) -> str:
    """NFC normalization; 8..64 code points, no composition rules."""

    if not isinstance(senha, str):
        raise SenhaRecusada("senha_invalida")
    normalizada = unicodedata.normalize("NFC", senha)
    if "\x00" in normalizada:
        raise SenhaRecusada("senha_invalida")
    if len(normalizada) < SENHA_MIN:
        raise SenhaRecusada("senha_curta")
    if len(normalizada) > SENHA_MAX:
        raise SenhaRecusada("senha_longa")
    if normalizada.casefold() in BLOCKLIST or len(set(normalizada)) < 3:
        raise SenhaRecusada("senha_comum")
    return normalizada


def _pepper(versao: int) -> bytes:
    secret = settings.auth_password_pepper_keys.get(versao)
    if secret is None or not settings.auth_password_pepper_configured():
        raise PepperIndisponivel("password pepper unavailable")
    return secret.get_secret_value().strip().encode()


def _peppered(senha: str, versao: int) -> bytes:
    # Bounded input for argon2 and no NUL bytes: hex of the HMAC digest.
    digest = hmac.new(_pepper(versao), senha.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest.encode("ascii")


_dummy_hash: str | None = None


def _dummy() -> str:
    global _dummy_hash
    if _dummy_hash is None or not _dummy_hash.startswith(_params_prefix()):
        _dummy_hash = HASHER.hash(b"wpp-auth-dummy-never-matches")
    return _dummy_hash


def _params_prefix() -> str:
    return f"$argon2id$v=19$m={HASHER.memory_cost},t={HASHER.time_cost},p={HASHER.parallelism}$"


def _hash_sync(senha: str) -> tuple[str, int]:
    versao = settings.auth_password_pepper_current_version
    return HASHER.hash(_peppered(senha, versao)), versao


def _verify_sync(senha: str | None, senha_hash: str | None, versao: int | None) -> Verificacao:
    usable = bool(
        isinstance(senha, str)
        and senha_hash
        and versao is not None
        and versao in settings.auth_password_pepper_keys
        and senha_hash.startswith("$argon2id$")
    )
    if not usable:
        # Same single derivation as a real mismatch.
        try:
            HASHER.verify(_dummy(), b"wpp-auth-dummy-input")
        except VerificationError:
            pass
        return Verificacao(ok=False, precisa_rehash=False)
    try:
        normalizada = unicodedata.normalize("NFC", senha)
        HASHER.verify(senha_hash, _peppered(normalizada, versao))
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return Verificacao(ok=False, precisa_rehash=False)
    return Verificacao(
        ok=True,
        precisa_rehash=bool(
            versao != settings.auth_password_pepper_current_version
            or HASHER.check_needs_rehash(senha_hash)
        ),
    )


async def gerar_hash(senha: str) -> tuple[str, int]:
    """Policy-checked password → (PHC hash, pepper version); runs off-loop."""

    normalizada = normalizar(senha)
    _pepper(settings.auth_password_pepper_current_version)
    return await asyncio.to_thread(_hash_sync, normalizada)


async def verificar(
    senha: str | None, senha_hash: str | None, versao: int | None
) -> Verificacao:
    """Exactly one argon2id derivation on every branch; runs off-loop."""

    _pepper(settings.auth_password_pepper_current_version)
    return await asyncio.to_thread(_verify_sync, senha, senha_hash, versao)
