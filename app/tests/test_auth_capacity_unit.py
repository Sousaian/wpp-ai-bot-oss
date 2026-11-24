"""AUTH-1: bounded hashing memory/latency and trusted client address."""

import asyncio
import json
import os
import re
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError
from starlette.requests import Request

from src import app_server
from src.config import Settings, settings
from src.services.auth import origem, senha


ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
CHILD = r'''
import asyncio, json, resource, time
from src.services.auth import senha
async def main():
    stored, version = await senha.gerar_hash("frase longa sintetica 42")
    base = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    lag, stop = [0.0], False
    async def ticker():
        while not stop:
            t = time.perf_counter(); await asyncio.sleep(0.01)
            lag[0] = max(lag[0], time.perf_counter() - t - 0.01)
    tick = asyncio.create_task(ticker())
    results = await asyncio.gather(*[senha.verificar("frase errada 00", stored, version) for _ in range(64)],
                                   return_exceptions=True)
    stop = True; await tick
    print(json.dumps({"growth_mib": (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - base) / 1024,
                      "busy": sum(isinstance(r, senha.HashOcupado) for r in results),
                      "done": sum(isinstance(r, senha.Verificacao) for r in results), "lag": lag[0]}))
asyncio.run(main())
'''


def request(peer: str | None, *forwarded: str) -> Request:
    return Request({"type": "http", "method": "POST", "path": "/auth/login", "query_string": b"",
                    "headers": [(b"x-forwarded-for", v.encode()) for v in forwarded],
                    "client": (peer, 1) if peer else None})


class HashCapacityTests(unittest.IsolatedAsyncioTestCase):
    def test_64_simultaneous_logins_stay_within_the_memory_budget(self) -> None:
        env = {**os.environ, "AUTH_PASSWORD_PEPPER_KEYS": '{"1":"synthetic-pepper-0123456789abcdef-xyz"}'}
        env.setdefault("DATABASE_URL", "mysql+aiomysql://x:y@db.invalid/z")
        result = subprocess.run([sys.executable, "-c", CHILD], env=env, capture_output=True, text=True,
                                cwd=ROOT / "app", timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        report = json.loads(result.stdout.strip().splitlines()[-1])
        capacity = senha.WORKERS + senha.FILA_MAXIMA
        self.assertEqual((report["done"], report["busy"]), (capacity, 64 - capacity), report)
        # Absolute budget, not derived from the constants under test: two 32 MiB
        # blocks (one already resident) plus allocator slack.
        self.assertLess(report["growth_mib"], 64, report)
        self.assertLess(report["lag"], 0.5, report)  # event loop stays responsive

    def test_budget_constants(self) -> None:
        self.assertEqual((senha.HASHER.memory_cost, senha.HASHER.time_cost, senha.HASHER.parallelism),
                         (32 * 1024, 3, 1))
        self.assertLessEqual(senha.WORKERS * senha.HASHER.memory_cost, 64 * 1024)

    async def test_cancelled_request_keeps_its_slot_until_the_thread_ends(self) -> None:
        pool, release = senha.PiscinaHash(workers=1, fila=1), threading.Event()
        started = threading.Event()

        def job() -> None:
            started.set()
            release.wait(10)

        running = asyncio.create_task(pool.executar(job))
        await asyncio.to_thread(started.wait, 5)
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        queued = asyncio.create_task(pool.executar(int))
        await asyncio.sleep(0)
        with self.assertRaises(senha.HashOcupado):  # 1 still hashing + 1 queued
            async with asyncio.timeout(1):  # refused at once, not by the wait limit
                await pool.executar(int)
        queued.cancel()  # a queued job that never started frees its slot
        await asyncio.gather(queued, return_exceptions=True)
        waiting = asyncio.create_task(pool.executar(int))
        await asyncio.sleep(0.05)
        self.assertFalse(waiting.done())
        release.set()
        self.assertEqual(await waiting, 0)

    async def test_wait_is_bounded(self) -> None:
        pool, release = senha.PiscinaHash(workers=1, fila=2), threading.Event()
        blocked = asyncio.create_task(pool.executar(release.wait, 10))
        await asyncio.sleep(0.05)  # the only worker is taken
        try:
            with patch.object(senha, "ESPERA_MAXIMA", 0.05):
                with self.assertRaises(senha.HashOcupado):
                    await pool.executar(int)
        finally:
            release.set()
            await asyncio.gather(blocked, return_exceptions=True)


class ClientAddressTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        origem.PROXY.reset()
        self.addCleanup(origem.PROXY.reset)

    async def test_only_the_resolved_proxy_is_believed(self) -> None:
        calls = []

        async def resolver(host: str) -> frozenset[str]:
            calls.append(host)
            return frozenset({"172.30.0.2"})

        clock = [0.0]
        proxy = origem.ProxyConfiavel(resolver, lambda: clock[0])
        with patch.object(origem, "PROXY", proxy), patch.object(settings, "auth_trusted_proxy_host", "nginx"):
            cases = {
                ("172.30.0.2", ("203.0.113.5",)): "203.0.113.5",
                ("172.30.0.9", ("203.0.113.5",)): "172.30.0.9",  # another container
                ("172.30.0.2", ("203.0.113.5, 203.0.113.6",)): "172.30.0.2",
                ("172.30.0.2", ("203.0.113.5", "203.0.113.6")): "172.30.0.2",
                ("172.30.0.2", ("lixo",)): "172.30.0.2",
                ("172.30.0.2", ()): "172.30.0.2",
                ("172.30.0.2", ("::ffff:203.0.113.5",)): "203.0.113.5",
                ("::ffff:172.30.0.2", ("203.0.113.5",)): "203.0.113.5",
                (None, ("203.0.113.5",)): origem.DESCONHECIDA,
            }
            for (peer, forwarded), expected in cases.items():
                self.assertEqual(await origem.cliente(request(peer, *forwarded)), expected, (peer, forwarded))
            self.assertEqual(calls, ["nginx"])  # cached, not once per request
            clock[0] = origem.RESOLUCAO_TTL_SEGUNDOS + 1
            await origem.cliente(request("172.30.0.2", "203.0.113.5"))
            self.assertEqual(len(calls), 2)

    async def test_resolution_failure_trusts_nobody(self) -> None:
        async def resolver(_host: str) -> frozenset[str]:
            raise OSError("synthetic")

        proxy = origem.ProxyConfiavel(resolver)
        with patch.object(origem, "PROXY", proxy), patch.object(settings, "auth_trusted_proxy_host", "nginx"):
            self.assertEqual(await origem.cliente(request("172.30.0.2", "203.0.113.5")), "172.30.0.2")
        self.assertEqual(proxy.falhas, 1)
        with patch.object(settings, "auth_trusted_proxy_host", ""):
            self.assertEqual(await origem.cliente(request("172.30.0.2", "203.0.113.5")), "172.30.0.2")

    def test_wildcards_and_ranges_are_refused(self) -> None:
        base = dict(openrouter_api_key="s", evolution_api_url="http://e.invalid", evolution_api_key="s",
                    database_url="mysql+aiomysql://x:y@h/z", _env_file=None)
        for bad in ("*", "172.16.0.0/12", "nginx,app", "nginx app", "http://nginx"):
            with self.assertRaises(ValidationError, msg=bad):
                Settings(auth_trusted_proxy_host=bad, **base)
        for good in ("nginx", "172.30.0.2", "2001:db8::2", ""):
            self.assertEqual(Settings(auth_trusted_proxy_host=good, **base).auth_trusted_proxy_host, good)

    def test_throttle_key(self) -> None:
        self.assertEqual(origem.chave("203.0.113.5"), "203.0.113.5")
        self.assertEqual(origem.chave("2001:db8:1:2:3::9"), "2001:db8:1:2::/64")
        self.assertEqual(origem.chave("lixo"), origem.DESCONHECIDA)

    def test_uvicorn_never_rewrites_the_peer(self) -> None:
        class Stop(Exception):
            pass

        captured = {}

        def config(*args, **kwargs):
            captured.update(kwargs)
            raise Stop

        with patch.object(app_server.uvicorn, "Config", config), patch.object(app_server, "set_current"), \
             patch("src.utils.logger.setup_logger"), patch.object(app_server, "require_runtime_version"), \
             patch.dict(os.environ, {"FORWARDED_ALLOW_IPS": "*"}):
            with self.assertRaises(Stop):
                app_server.main(object())
        self.assertIs(captured["proxy_headers"], False)

    def test_every_nginx_block_towards_the_app_overwrites_the_header(self) -> None:
        for name in ("nginx/nginx.conf", "nginx/nginx.dev.conf"):
            blocks = [b for b in re.findall(r"location [^{]+\{(.*?)\n        \}", (ROOT / name).read_text(), re.S)
                      if "$app_upstream" in b and "proxy_pass" in b]
            self.assertEqual(len(blocks), 4, name)
            for block in blocks:
                self.assertRegex(block, r"X-Forwarded-For\s+\$remote_addr;")
                self.assertNotIn("proxy_add_x_forwarded_for", block)


if __name__ == "__main__":
    unittest.main()
