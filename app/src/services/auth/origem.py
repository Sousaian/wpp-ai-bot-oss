"""Client address behind the edge proxy, used by rate limit, delay and audit.

``X-Forwarded-For`` counts only when the TCP peer is the configured proxy
(``AUTH_TRUSTED_PROXY_HOST``, the Compose service name by default, resolved
through Docker DNS) and carries exactly one address: Nginx overwrites the
header with ``$remote_addr``. Any other peer, a list, a malformed value or a
DNS failure falls back to the TCP peer, never to a client-chosen address.
Uvicorn's own proxy handling stays off (``app_server``), so ``request.client``
is always the real peer.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import time

from fastapi import Request
from loguru import logger

from src.config import settings


DESCONHECIDA = "desconhecida"
RESOLUCAO_TTL_SEGUNDOS = 10.0
RESOLUCAO_TIMEOUT_SEGUNDOS = 1.0


def canonico(valor: object) -> str | None:
    """Canonical IP text (IPv4-mapped IPv6 → IPv4), or None."""

    try:
        ip = ipaddress.ip_address(str(valor).strip())
    except ValueError:
        return None
    mapped = getattr(ip, "ipv4_mapped", None)
    return str(mapped or ip)


def chave(ip: str | None) -> str:
    """Throttling key: an IPv6 client owns a whole /64."""

    texto = canonico(ip) if ip else None
    if texto is None:
        return DESCONHECIDA
    if ":" in texto:
        return str(ipaddress.ip_network(f"{texto}/64", strict=False).network_address) + "/64"
    return texto


async def _resolver(host: str) -> frozenset[str]:
    infos = await asyncio.wait_for(
        asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM),
        RESOLUCAO_TIMEOUT_SEGUNDOS,
    )
    return frozenset(ip for ip in (canonico(info[4][0]) for info in infos) if ip)


class ProxyConfiavel:
    """Short-lived cache of the proxy addresses; failure trusts nobody."""

    def __init__(self, resolver=_resolver, relogio=time.monotonic) -> None:
        self._resolver = resolver
        self._relogio = relogio
        self._host: str | None = None
        self._ips: frozenset[str] = frozenset()
        self._expira = 0.0
        self.falhas = 0

    def reset(self) -> None:
        self._host, self._ips, self._expira = None, frozenset(), 0.0

    async def confia(self, peer: str) -> bool:
        host = settings.auth_trusted_proxy_host
        if not host:
            return False
        literal = canonico(host)
        if literal is not None:
            return peer == literal
        agora = self._relogio()
        if host != self._host or agora >= self._expira:
            try:
                ips = await self._resolver(host)
            except Exception as exc:
                if not self.falhas:
                    logger.error("Trusted proxy unresolved | erro_tipo={} threshold=1", type(exc).__name__)
                self.falhas += 1
                ips = frozenset()
            else:
                self.falhas = 0
            self._host, self._ips, self._expira = host, ips, self._relogio() + RESOLUCAO_TTL_SEGUNDOS
        return peer in self._ips


PROXY = ProxyConfiavel()


async def cliente(request: Request) -> str:
    """Canonical client IP (``DESCONHECIDA`` without a usable peer)."""

    peer = canonico(request.client.host) if request.client else None
    if peer is None:
        return DESCONHECIDA
    valores = request.headers.getlist("x-forwarded-for")
    if len(valores) != 1:
        return peer
    encaminhado = canonico(valores[0])
    if encaminhado is None or not await PROXY.confia(peer):
        return peer
    return encaminhado
