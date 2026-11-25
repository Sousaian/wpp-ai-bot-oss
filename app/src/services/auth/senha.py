"""Password policy and hashing: HMAC-SHA-256 pepper, then argon2id.

The pepper keyring is versioned (``AUTH_PASSWORD_PEPPER_KEYS``); a hash made
with an older version or older argon2 parameters is rehashed on login. Every
verification runs exactly one argon2id derivation with the current cost,
including the branches without a usable hash (dummy hash), so login failure
branches are not distinguishable by work performed.

Derivations run on a dedicated pool of ``WORKERS`` threads with a bounded
queue, never on the event loop nor on the default executor: hashing memory is
capped at ``WORKERS`` × 32 MiB whatever the number of concurrent logins, and
/webhook keeps its own threads. Beyond the queue, or after ``ESPERA_MAXIMA``
seconds waiting, the caller gets ``HashOcupado`` (503), identically on every
login branch.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import threading
import unicodedata
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from argon2 import PasswordHasher, Type, extract_parameters
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from loguru import logger

from src.config import settings


SENHA_MIN = 8
SENHA_MAX = 64

# argon2id m=32 MiB, t=3, p=1, 16-byte salt, 32-byte digest (PHC string):
# ~45 ms on one vCPU and above OWASP's 19 MiB/t=2 and 12 MiB/t=3 minimums.
HASHER = PasswordHasher(
    time_cost=3, memory_cost=32768, parallelism=1, hash_len=32, salt_len=16, type=Type.ID
)
# A stored hash costlier than this would break the memory budget: it never
# verifies (ERROR signal) and its owner needs a reset_senha token.
CUSTO_MAXIMO = (32768, 3, 1)  # memory KiB, time, parallelism
WORKERS = 2
FILA_MAXIMA = 16
ESPERA_MAXIMA = 5.0

class SenhaRecusada(ValueError):
    """Policy refusal with a fixed, value-free code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class PepperIndisponivel(RuntimeError):
    """The keyring cannot hash or verify (auth not provisioned)."""


class HashOcupado(RuntimeError):
    """Hashing capacity exhausted; retry later (503)."""


class PiscinaHash:
    """Dedicated pool with bounded admission.

    A slot belongs to the queued job, not to the request, and is freed only
    when a worker consumes it: cancelling requests can neither free a slot that
    is still hashing nor grow the executor queue. Abandoned jobs do no work.
    """

    def __init__(self, workers: int = WORKERS, fila: int = FILA_MAXIMA) -> None:
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="auth-hash")
        self._capacidade = workers + fila
        self._ativos = 0
        self._lock = threading.Lock()
        self.recusas = 0

    def _liberar(self, _futuro: Future) -> None:
        with self._lock:
            self._ativos -= 1

    def _recusar(self) -> HashOcupado:
        self.recusas += 1
        logger.error("Auth hashing saturated | count={} threshold=1", self.recusas)
        return HashOcupado("auth hashing saturated")

    @property
    def saturada(self) -> bool:
        with self._lock:
            return self._ativos >= self._capacidade

    async def executar(self, funcao, *args):
        abandonado = threading.Event()

        def trabalho():
            return None if abandonado.is_set() else funcao(*args)

        with self._lock:
            if self._ativos >= self._capacidade:
                recusado = True
            else:
                recusado = False
                self._ativos += 1
        if recusado:
            raise self._recusar()
        try:
            futuro = self._executor.submit(trabalho)
        except BaseException:
            with self._lock:
                self._ativos -= 1
            raise
        futuro.add_done_callback(self._liberar)
        try:
            # shield: the executor future is never cancelled, so its slot stays.
            return await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(futuro)), ESPERA_MAXIMA)
        except asyncio.TimeoutError:
            raise self._recusar() from None
        finally:
            abandonado.set()


PISCINA = PiscinaHash()


@dataclass(frozen=True)
class Verificacao:
    ok: bool
    precisa_rehash: bool


def normalizar(senha: str) -> str:
    """Definition policy: NFC, 8..64 code points, Nd, Lu and punctuation/symbol."""

    if not isinstance(senha, str):
        raise SenhaRecusada("senha_invalida")
    normalizada = unicodedata.normalize("NFC", senha)
    if "\x00" in normalizada:
        raise SenhaRecusada("senha_invalida")
    if len(normalizada) < SENHA_MIN:
        raise SenhaRecusada("senha_curta")
    if len(normalizada) > SENHA_MAX:
        raise SenhaRecusada("senha_longa")
    categorias = {unicodedata.category(c) for c in normalizada}
    if "Nd" not in categorias:
        raise SenhaRecusada("senha_sem_numero")
    if "Lu" not in categorias:
        raise SenhaRecusada("senha_sem_maiuscula")
    if not any(c[0] in {"P", "S"} for c in categorias):
        raise SenhaRecusada("senha_sem_especial")
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


def _dentro_do_orcamento(senha_hash: str) -> bool:
    try:
        p = extract_parameters(senha_hash)
    except InvalidHashError:
        return False
    if p.memory_cost <= CUSTO_MAXIMO[0] and p.time_cost <= CUSTO_MAXIMO[1] and p.parallelism <= CUSTO_MAXIMO[2]:
        return True
    logger.error("Auth stored hash above the memory budget refused | threshold=1")
    return False


def exigir_capacidade() -> None:
    """Refuse before any database work when hashing is already saturated."""

    if PISCINA.saturada:
        raise PISCINA._recusar()


def _verify_sync(senha: str | None, senha_hash: str | None, versao: int | None) -> Verificacao:
    usable = bool(
        isinstance(senha, str)
        and senha_hash
        and versao is not None
        and versao in settings.auth_password_pepper_keys
        and senha_hash.startswith("$argon2id$")
        and _dentro_do_orcamento(senha_hash)
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
    return await PISCINA.executar(_hash_sync, normalizada)


async def rehash_verificada(senha: str) -> tuple[str, int]:
    """Rehash only after successful verification; never apply definition policy."""

    normalizada = unicodedata.normalize("NFC", senha)
    _pepper(settings.auth_password_pepper_current_version)
    return await PISCINA.executar(_hash_sync, normalizada)


async def verificar(
    senha: str | None, senha_hash: str | None, versao: int | None
) -> Verificacao:
    """Exactly one argon2id derivation on every branch; runs off-loop."""

    _pepper(settings.auth_password_pepper_current_version)
    return await PISCINA.executar(_verify_sync, senha, senha_hash, versao)
