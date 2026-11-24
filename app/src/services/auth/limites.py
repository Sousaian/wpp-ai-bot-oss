"""Login throttling: per-source window and per-(account, source) delay.

Per source the window lives in process memory and bounds attempts on the
credential endpoints with 429. It is per process and resets on restart.

The durable control is a progressive delay per (account, source) in
``auth_tentativas``: 5 free failures, then 30 s doubling up to 15 min, reset
by a correct password, 24 h of silence or ``auth_cli usuario desbloquear``.
It never locks the account for other sources, so a third party who knows an
e-mail cannot keep its owner out. A delayed source gets the same answer as a
wrong password (no enumeration). Failures across sources only raise a signal.
"""

from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timedelta


FALHAS_LIVRES = 5
ATRASO_BASE = timedelta(seconds=30)
ATRASO_MAXIMO = timedelta(minutes=15)
FALHAS_VALIDADE = timedelta(hours=24)
# Consecutive failures of one account over all sources: signal, never a lock.
SINAL_FALHAS_CONTA = 50

JANELA_SEGUNDOS = 300.0
TENTATIVAS_POR_JANELA = 30
_MAX_ORIGENS = 10000


def atraso(falhas: int) -> timedelta:
    """0 up to 4 failures; 5 → 30 s, 6 → 60 s … capped at 15 min."""

    if falhas < FALHAS_LIVRES:
        return timedelta(0)
    return min(ATRASO_BASE * (2 ** min(falhas - FALHAS_LIVRES, 16)), ATRASO_MAXIMO)


def atrasado(liberado_em: datetime | None, agora: datetime) -> bool:
    return liberado_em is not None and liberado_em > agora


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
