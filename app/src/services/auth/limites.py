"""Login throttling: per-source sliding window plus per-account lockout.

Per source (client address) the window lives in process memory and bounds
attempts on the credential endpoints with 429. It is per process and resets
on restart; the durable control is the per-account lockout in the DB:
10 consecutive failures lock for 15 min, doubling per lock up to 24 h; a
successful login resets both counters. A locked account answers exactly like
a wrong password (no enumeration).
"""

from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timedelta


FALHAS_ATE_BLOQUEIO = 10
BLOQUEIO_BASE = timedelta(minutes=15)
BLOQUEIO_MAXIMO = timedelta(hours=24)

JANELA_SEGUNDOS = 300.0
TENTATIVAS_POR_JANELA = 30
_MAX_ORIGENS = 10000


def duracao_bloqueio(bloqueios_anteriores: int) -> timedelta:
    """15 min, 30 min, 1 h … capped at 24 h."""

    expoente = min(max(bloqueios_anteriores, 0), 16)
    return min(BLOQUEIO_BASE * (2 ** expoente), BLOQUEIO_MAXIMO)


def bloqueado(bloqueado_ate: datetime | None, agora: datetime) -> bool:
    return bloqueado_ate is not None and bloqueado_ate > agora


class LimitePorOrigem:
    """Sliding window of attempts per (endpoint, source)."""

    def __init__(
        self,
        tentativas: int = TENTATIVAS_POR_JANELA,
        janela_segundos: float = JANELA_SEGUNDOS,
        relogio=time.monotonic,
    ) -> None:
        self.tentativas = tentativas
        self.janela = janela_segundos
        self._relogio = relogio
        self._eventos: dict[tuple[str, str], deque[float]] = {}
        self.recusas = 0

    def reset(self) -> None:
        self._eventos.clear()

    def registrar(self, escopo: str, origem: str | None) -> float | None:
        """Count one attempt; return Retry-After seconds when over the limit."""

        agora = self._relogio()
        chave = (escopo, origem or "desconhecida")
        eventos = self._eventos.get(chave)
        if eventos is None:
            if len(self._eventos) >= _MAX_ORIGENS:
                self._podar(agora)
            eventos = self._eventos.setdefault(chave, deque())
        while eventos and eventos[0] <= agora - self.janela:
            eventos.popleft()
        if len(eventos) >= self.tentativas:
            self.recusas += 1
            return max(1.0, eventos[0] + self.janela - agora)
        eventos.append(agora)
        return None

    def _podar(self, agora: float) -> None:
        for chave in [k for k, v in self._eventos.items() if not v or v[-1] <= agora - self.janela]:
            del self._eventos[chave]
        if len(self._eventos) >= _MAX_ORIGENS:
            # Memory bound under a spray of sources: forget the oldest windows.
            for chave in sorted(self._eventos, key=lambda k: self._eventos[k][-1])[: _MAX_ORIGENS // 2]:
                del self._eventos[chave]


LIMITE = LimitePorOrigem()
