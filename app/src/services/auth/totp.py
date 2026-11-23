"""RFC 6238 TOTP (SHA-1, 6 digits, 30 s, ±1 step) with replay protection.

The base32 secret is stored only as AES-256-GCM ciphertext, base64 of
``nonce || ciphertext || tag`` (aiomysql 0.2.0 cannot bind raw ``bytes``
parameters with the pinned PyMySQL), under a versioned key from
``AUTH_TOTP_ENCRYPTION_KEYS``; the AAD binds it to the user row, so a
ciphertext copied to another user does not decrypt. A code is accepted only
for a step strictly greater than ``totp_ultimo_passo`` (conditional UPDATE),
so the same code is never accepted twice, even concurrently.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import os
import time
from dataclasses import dataclass

import pyotp
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.config import settings


PERIODO = 30
DIGITOS = 6
JANELA = 1
EMISSOR = "WPP AI Bot"
_NONCE = 12


class TotpIndisponivel(RuntimeError):
    """Encryption keyring missing or unusable."""


@dataclass(frozen=True)
class SegredoCifrado:
    cifrado: str
    versao: int


def _aad(usuario_id: int) -> bytes:
    return f"auth_usuarios:{int(usuario_id)}:totp".encode("ascii")


def _chave(versao: int) -> bytes:
    secret = settings.auth_totp_encryption_keys.get(versao)
    if secret is None or not settings.auth_totp_configured():
        raise TotpIndisponivel("totp keyring unavailable")
    return base64.b64decode(secret.get_secret_value().strip(), validate=True)


def novo_segredo() -> str:
    return pyotp.random_base32(32)  # 160 bits


def cifrar(segredo: str, usuario_id: int) -> SegredoCifrado:
    versao = settings.auth_totp_encryption_current_version
    nonce = os.urandom(_NONCE)
    cifrado = AESGCM(_chave(versao)).encrypt(nonce, segredo.encode("ascii"), _aad(usuario_id))
    return SegredoCifrado(base64.b64encode(nonce + cifrado).decode("ascii"), versao)


def decifrar(armazenado: bytes | str, versao: int, usuario_id: int) -> str | None:
    try:
        bruto = base64.b64decode(armazenado, validate=True)
        claro = AESGCM(_chave(versao)).decrypt(bruto[:_NONCE], bruto[_NONCE:], _aad(usuario_id))
    except (InvalidTag, ValueError, binascii.Error):
        return None
    return claro.decode("ascii")


def uri(segredo: str, rotulo: str) -> str:
    return pyotp.TOTP(segredo, digits=DIGITOS, interval=PERIODO).provisioning_uri(
        name=rotulo, issuer_name=EMISSOR
    )


def passo_aceito(segredo: str, codigo: object, *, agora: float | None = None) -> int | None:
    """Return the matching step within ±1 of now, or None. Constant-time compare."""

    # ASCII only: str.isdigit() accepts other Unicode digits, which
    # hmac.compare_digest cannot compare (TypeError → 500).
    if not isinstance(codigo, str) or len(codigo) != DIGITOS or not (codigo.isascii() and codigo.isdigit()):
        return None
    instante = time.time() if agora is None else agora
    gerador = pyotp.TOTP(segredo, digits=DIGITOS, interval=PERIODO)
    atual = int(instante // PERIODO)
    encontrado = None
    for passo in range(atual - JANELA, atual + JANELA + 1):
        if hmac.compare_digest(gerador.generate_otp(passo), codigo) and encontrado is None:
            encontrado = passo
    return encontrado
