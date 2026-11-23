"""Opaque bearer secrets: 32 random bytes, only SHA-256 is persisted.

Each kind has its own prefix, so a secret of one kind is never accepted as
another (a session token is not a service token, a CSRF token is neither).
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from typing import Literal


Tipo = Literal["sessao", "csrf", "servico", "uso_unico"]

PREFIXOS: dict[str, str] = {
    "sessao": "wps_",
    "csrf": "wpc_",
    "servico": "wpt_",
    "uso_unico": "wpa_",
}
# token_urlsafe(32) → 43 characters of the URL-safe base64 alphabet.
_CORPO = re.compile(r"[A-Za-z0-9_-]{43}")


def novo(tipo: Tipo) -> tuple[str, str]:
    """Return (plaintext shown once, SHA-256 hex stored)."""

    token = PREFIXOS[tipo] + secrets.token_urlsafe(32)
    return token, digest(token)


def digest(token: str) -> str:
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def digest_se_valido(tipo: Tipo, token: object) -> str | None:
    """Bounded format check before hashing; malformed input never reaches SQL."""

    if not isinstance(token, str):
        return None
    prefixo = PREFIXOS[tipo]
    if not token.startswith(prefixo) or not _CORPO.fullmatch(token[len(prefixo):]):
        return None
    return digest(token)


def iguais(token: str | None, esperado_hash: str | None) -> bool:
    if not token or not esperado_hash:
        return False
    return hmac.compare_digest(digest(token), esperado_hash)
