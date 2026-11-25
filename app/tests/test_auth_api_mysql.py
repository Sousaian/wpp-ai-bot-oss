"""AUTH-1/2: API operator authentication against a real disposable MySQL.

Routes are exercised through the ASGI app with a real cookie jar; users and
tokens are created through the same service the container CLI uses. Only
synthetic data (``*.invalid`` e-mails). argon2 runs with a cheap cost here;
``test_production_hash_parameters`` pins the production parameters.
"""

from __future__ import annotations

import asyncio
import json
import threading
import os
import re
import unittest
from contextlib import ExitStack
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import base64
import time

import httpx
import pyotp
from argon2 import PasswordHasher, Type
from fastapi.routing import APIRoute
from loguru import logger
from pydantic import SecretStr
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src import auth_cli, main
from src.config import settings
from src.database.connection import get_db
from src.database.models import (
    AuthSessao,
    AuthTentativa,
    AuthTokenServico,
    AuthTokenUsoUnico,
    AuthUsuario,
    Base,
)
from src.database.unit_of_work import UnitOfWork
from src.services.auth import administracao, contexto, limites, origem, rbac, segredos, senha, totp
from tests.disposable_db import assert_disposable_database


TEST_DATABASE_URL = os.getenv("OUTBOX_TEST_DATABASE_URL")
REPO_ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
PEPPER = {1: SecretStr("auth-test-pepper-synthetic-0123456789abcdef")}
TOTP_KEYS = {1: SecretStr(base64.b64encode(bytes(range(32))).decode())}
SENHA = "Frase longa sintetica 42!"
BASE_URL = "https://auth.invalid"
PROXY_IP = "172.30.0.2"
AUTH_TABLES = ("auth_auditoria", "auth_tentativas", "auth_sessoes", "auth_tokens_uso_unico", "auth_tokens_servico", "auth_usuarios")
PRODUCTION_HASHER = senha.HASHER
CHEAP = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1, hash_len=32, salt_len=16, type=Type.ID)

class FakeTime:
    """Stands in for the ``time`` module inside services/auth/totp.py."""

    def __init__(self, clock: list[float]) -> None:
        self._clock = clock

    def time(self) -> float:
        return self._clock[0]


class CountingHasher(PasswordHasher):
    """Counts argon2 derivations (the base class uses __slots__)."""

    hashes = 0
    verifies = 0

    def hash(self, *args, **kwargs):
        self.hashes += 1
        return super().hash(*args, **kwargs)

    def verify(self, *args, **kwargs):
        self.verifies += 1
        return super().verify(*args, **kwargs)


# Representative valid-looking calls; the matrix itself enumerates app.routes.
SAMPLE_QUERY = {"agente_id": "1"}


def protected_routes() -> list[tuple[str, str]]:
    routes = []
    for route in main.app.routes:
        if isinstance(route, APIRoute) and route.path.startswith(("/agentes", "/sessoes")):
            for method in sorted(route.methods - {"HEAD"}):
                path = route.path.replace("{agente_id}", "999999").replace("{telefone}", "5511999999001")
                routes.append((method, path))
    return routes


class AuthApiMySQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        if not TEST_DATABASE_URL:
            self.fail("OUTBOX_TEST_DATABASE_URL is required for the mandatory auth suite")
        self.engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
        await assert_disposable_database(self, self.engine)
        self.sessions = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        async with self.engine.begin() as connection:
            for table in AUTH_TABLES:
                await connection.execute(text(f"DELETE FROM {table}"))
            # Routes that mutate sessions need the runtime this process expects.
            await connection.execute(text("UPDATE delivery_runtime_state SET mode='direct'"))
        self.patches = ExitStack()
        self.patches.enter_context(patch.object(settings, "auth_password_pepper_keys", PEPPER))
        self.patches.enter_context(patch.object(settings, "auth_password_pepper_current_version", 1))
        self.patches.enter_context(patch.object(senha, "HASHER", CHEAP))
        self.patches.enter_context(patch.object(settings, "auth_totp_encryption_keys", TOTP_KEYS))
        self.patches.enter_context(patch.object(settings, "auth_totp_encryption_current_version", 1))
        # Virtual clock: every TOTP use moves 30 s ahead, so anti-replay holds
        # while a test logs the same admin in several times.
        self.clock = [time.time()]
        self.patches.enter_context(patch.object(totp, "time", FakeTime(self.clock)))
        self.totp_secrets: dict[str, str] = {}
        limites.LIMITE.reset()
        # Trusted proxy as a literal address; other suites cover resolution.
        self.patches.enter_context(patch.object(settings, "auth_trusted_proxy_host", PROXY_IP))
        origem.PROXY.reset()

        async def db_override():
            async with self.sessions() as db:
                yield db
                await db.commit()

        main.app.dependency_overrides[get_db] = db_override
        main.app.dependency_overrides[contexto.get_auth_sessions] = lambda: self.sessions
        self.clients: list[httpx.AsyncClient] = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.aclose()
        main.app.dependency_overrides.clear()
        limites.LIMITE.reset()
        self.patches.close()
        await self.engine.dispose()

    # ── helpers ─────────────────────────────────────────────────────────
    def client(self, source: str = "198.51.100.10", base_url: str = BASE_URL) -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app, client=(source, 40000)), base_url=base_url
        )
        self.clients.append(client)
        return client

    async def user(self, papel: str, email: str | None = None, *, ativar: bool = True) -> tuple[int, str]:
        email = email or f"{papel}-{len(self.clients)}-{id(self)}@example.invalid"
        if papel == "admin" and not await self.count(AuthUsuario, AuthUsuario.papel == "admin"):
            emitido = await administracao.bootstrap_admin(self.sessions, ator="teste", email=email)
        else:
            emitido = await administracao.criar_usuario(self.sessions, ator="teste", email=email, papel=papel)
        if ativar:
            response = await self.client("192.0.2.200").post(
                "/auth/ativar", json={"token": emitido.token, "senha": SENHA})
            self.assertEqual(response.status_code, 200, response.text)
            if papel == "admin":
                body = response.json()
                self.assertEqual(body["status"], "totp_enrolamento_requerido")
                await self.confirm(body, email)
        return emitido.usuario_id, email

    def code(self, email: str) -> str:
        self.clock[0] += totp.PERIODO
        return pyotp.TOTP(self.totp_secrets[email]).at(self.clock[0])

    async def confirm(self, body: dict, email: str) -> None:
        self.totp_secrets[email] = body["totp_segredo"]
        response = await self.client("192.0.2.201").post(
            "/auth/totp/confirmar", json={"desafio": body["desafio"], "codigo": self.code(email)})
        self.assertEqual(response.status_code, 200, response.text)

    async def full_login(self, email: str, password: str = SENHA, source: str = "198.51.100.20"):
        """Password and, when challenged, the TOTP step: client + final response."""

        client, response = await self.login(email, password, source)
        if response.status_code == 200 and response.json()["mfa_requerido"]:
            response = await client.post("/auth/totp/verificar", json={
                "desafio": response.json()["desafio"], "codigo": self.code(email)})
        return client, response

    async def login(self, email: str, password: str = SENHA, source: str = "198.51.100.20",
                    forwarded: str | None = None):
        client = self.client(source)
        headers = {"X-Forwarded-For": forwarded} if forwarded is not None else None
        response = await client.post("/auth/login", json={"email": email, "senha": password}, headers=headers)
        return client, response

    async def logged(self, papel: str) -> tuple[httpx.AsyncClient, str, int]:
        usuario_id, email = await self.user(papel)
        client, response = await self.full_login(email)
        self.assertEqual(response.status_code, 200, response.text)
        return client, response.json()["csrf_token"], usuario_id

    async def service_token(self, papel: str, dias: int = 30) -> tuple[str, int]:
        created = await administracao.criar_token_servico(self.sessions, ator="teste", nome=f"auto-{papel}", papel=papel, dias=dias)
        return created["token"], created["id"]

    async def count(self, model, *criteria) -> int:
        async with self.sessions() as db:
            return int((await db.execute(select(func.count()).select_from(model).where(*criteria))).scalar_one())

    async def call(self, client: httpx.AsyncClient, method: str, path: str, **kwargs) -> httpx.Response:
        params = kwargs.pop("params", SAMPLE_QUERY if path.startswith("/sessoes") else None)
        return await client.request(method, path, params=params, **kwargs)

    async def execute(self, statement) -> None:
        async with self.sessions() as db:
            await db.execute(statement)
            await db.commit()

    # ── 401 / 403 matrix ────────────────────────────────────────────────
    async def test_route_inventory_is_complete(self) -> None:
        routes = protected_routes()
        self.assertEqual(len(routes), 12, routes)
        for router in (main.agentes_router, main.sessoes_router):
            self.assertIn(rbac.exigir_rota_declarada, [d.dependency for d in router.dependencies])

    async def test_missing_or_invalid_credentials_are_401_on_every_route(self) -> None:
        await self.user("admin")
        headers_cases = {
            "none": {},
            "cookie_garbage": {"Cookie": f"{contexto.SESSION_COOKIE}=garbage"},
            "cookie_unknown": {"Cookie": f"{contexto.SESSION_COOKIE}={segredos.novo('sessao')[0]}"},
            "bearer_unknown": {"Authorization": f"Bearer {segredos.novo('servico')[0]}"},
            "bearer_session_kind": {"Authorization": f"Bearer {segredos.novo('sessao')[0]}"},
            "basic_scheme": {"Authorization": "Basic YTpi"},
            "empty_bearer": {"Authorization": "Bearer "},
        }
        executed = 0
        client = self.client()
        for name, headers in headers_cases.items():
            for method, path in protected_routes():
                response = await self.call(client, method, path, headers=headers, json={})
                self.assertEqual(response.status_code, 401, (name, method, path, response.text))
                self.assertEqual(response.json(), {"detail": "autenticacao requerida"})
                executed += 1
        self.assertEqual(executed, 7 * 12)

    async def test_unprovisioned_keyrings_fail_closed_503(self) -> None:
        for setting in ("auth_password_pepper_keys", "auth_totp_encryption_keys"):
            with patch.object(settings, setting, {}):
                client = self.client()
                for method, path in protected_routes():
                    self.assertEqual((await self.call(client, method, path, json={})).status_code, 503, setting)
                response = await client.post("/auth/login", json={"email": "a@example.invalid", "senha": SENHA})
                self.assertEqual(response.status_code, 503, setting)

    # ── cookie and CSRF ─────────────────────────────────────────────────
    async def test_session_cookie_attributes_and_real_jar(self) -> None:
        _, email = await self.user("admin")
        client, response = await self.full_login(email)
        self.assertEqual(response.status_code, 200)
        cookie = response.headers["set-cookie"]
        self.assertTrue(cookie.startswith(f"{contexto.SESSION_COOKIE}=wps_"))
        lowered = cookie.lower()
        for attribute in ("httponly", "secure", "samesite=strict", "path=/", "max-age=43200"):
            self.assertIn(attribute, lowered)
        self.assertNotIn("domain=", lowered)
        self.assertNotIn(response.json()["csrf_token"], cookie)
        self.assertEqual(response.headers["cache-control"], "no-store")
        # The jar sends the Secure cookie over https ...
        self.assertEqual((await client.get("/auth/me")).status_code, 200)
        # ... and never over plain http, even to the same host.
        plain = self.client(base_url="http://auth.invalid")
        plain.cookies = client.cookies
        self.assertEqual((await plain.get("/auth/me")).status_code, 401)

    async def test_csrf_required_on_cookie_mutations_only(self) -> None:
        admin, csrf, _ = await self.logged("admin")
        other_csrf = segredos.novo("csrf")[0]
        executed = 0
        for method, path in protected_routes():
            if method == "GET":
                response = await self.call(admin, method, path)
                self.assertNotIn(response.status_code, (401, 403), (method, path))
            else:
                for headers in ({}, {contexto.CSRF_HEADER: other_csrf}, {contexto.CSRF_HEADER: "x"}):
                    response = await self.call(admin, method, path, headers=headers, json={})
                    self.assertEqual(response.status_code, 403, (method, path, headers))
                    self.assertEqual(response.json(), {"detail": "csrf invalido"})
                    executed += 1
        self.assertEqual(executed, 3 * 6)
        # Logout is a cookie mutation too.
        self.assertEqual((await admin.post("/auth/logout")).status_code, 403)
        # A bearer request carries no ambient credential: CSRF does not apply.
        token, _ = await self.service_token("leitura")
        response = await self.client().post("/auth/logout", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual((response.status_code, response.json()["detail"]), (400, "logout requer sessao"))

    async def test_cookie_and_bearer_together_are_refused(self) -> None:
        admin, _, _ = await self.logged("admin")
        token, _ = await self.service_token("operador")
        response = await admin.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.status_code, 401)

    # ── session expiry and revocation ───────────────────────────────────
    async def _session_row(self, usuario_id: int) -> AuthSessao:
        async with self.sessions() as db:
            return (await db.execute(select(AuthSessao).where(AuthSessao.usuario_id == usuario_id)
                                     .order_by(AuthSessao.id.desc()))).scalars().first()

    async def test_idle_and_absolute_session_expiry(self) -> None:
        admin, _, usuario_id = await self.logged("admin")
        row = await self._session_row(usuario_id)
        self.assertEqual(row.expira_em - row.criada_em, timedelta(hours=12))
        # 29 min idle: still valid, and the touch moves ultimo_uso_em forward.
        await self.execute(update(AuthSessao).where(AuthSessao.id == row.id)
                           .values(ultimo_uso_em=func.utc_timestamp(6) - text("INTERVAL 29 MINUTE")))
        backdated = await self._session_row(usuario_id)
        self.assertEqual((await admin.get("/agentes")).status_code, 200)
        touched = await self._session_row(usuario_id)
        self.assertGreater(touched.ultimo_uso_em, backdated.ultimo_uso_em + timedelta(minutes=28))
        # Within 60 s the row is not rewritten on every request.
        self.assertEqual((await admin.get("/agentes")).status_code, 200)
        self.assertEqual((await self._session_row(usuario_id)).ultimo_uso_em, touched.ultimo_uso_em)
        # 30 min idle: refused.
        await self.execute(update(AuthSessao).where(AuthSessao.id == row.id)
                           .values(ultimo_uso_em=func.utc_timestamp(6) - text("INTERVAL 1801 SECOND")))
        self.assertEqual((await admin.get("/agentes")).status_code, 401)
        # Absolute 12 h: refused even when recently used.
        fresh, _ = await self.full_login(await self._email(usuario_id))
        row = await self._session_row(usuario_id)
        self.assertEqual((await fresh.get("/agentes")).status_code, 200)
        await self.execute(update(AuthSessao).where(AuthSessao.id == row.id)
                           .values(expira_em=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
        self.assertEqual((await fresh.get("/agentes")).status_code, 401)

    async def _email(self, usuario_id: int) -> str:
        async with self.sessions() as db:
            return (await db.execute(select(AuthUsuario.email).where(AuthUsuario.id == usuario_id))).scalar_one()

    async def test_logout_and_operator_revocations(self) -> None:
        admin, csrf, admin_id = await self.logged("admin")
        response = await admin.post("/auth/logout", headers={contexto.CSRF_HEADER: csrf})
        self.assertEqual(response.status_code, 200)
        self.assertIn(f"{contexto.SESSION_COOKIE}=", response.headers["set-cookie"])
        row = await self._session_row(admin_id)
        self.assertEqual(row.motivo_revogacao, "logout")
        # The jar dropped it; replaying the old token is refused too.
        self.assertEqual((await admin.get("/auth/me")).status_code, 401)
        replay = self.client()
        replay.cookies.set(contexto.SESSION_COOKIE, "x")
        self.assertEqual((await replay.get("/auth/me")).status_code, 401)

        email = await self._email(admin_id)
        first, _ = await self.full_login(email)
        second, _ = await self.full_login(email)
        await administracao.revogar_sessoes(self.sessions, ator="teste", usuario_id=admin_id)
        for client in (first, second):
            self.assertEqual((await client.get("/agentes")).status_code, 401)

        # Role change and deactivation revoke immediately.
        operador, _, operador_id = await self.logged("operador")
        await administracao.alterar_papel(self.sessions, ator="teste", usuario_id=operador_id, papel="leitura")
        self.assertEqual((await operador.get("/auth/me")).status_code, 401)
        leitura, _ = await self.login(await self._email(operador_id))
        self.assertEqual((await leitura.get("/auth/me")).json()["papel"], "leitura")
        await administracao.alterar_status(self.sessions, ator="teste", usuario_id=operador_id, ativo=False)
        self.assertEqual((await leitura.get("/auth/me")).status_code, 401)
        self.assertEqual((await self.login(await self._email(operador_id)))[1].status_code, 401)

    async def test_revocation_concurrent_with_requests(self) -> None:
        admin, _, admin_id = await self.logged("admin")

        async def requests() -> list[int]:
            return [(await admin.get("/auth/me")).status_code for _ in range(20)]

        statuses, _ = await asyncio.gather(
            requests(), administracao.revogar_sessoes(self.sessions, ator="teste", usuario_id=admin_id))
        # Once a 401 is seen, no later request succeeds (revocation is monotonic).
        if 401 in statuses:
            self.assertTrue(all(code == 401 for code in statuses[statuses.index(401):]), statuses)
        self.assertEqual((await admin.get("/auth/me")).status_code, 401)

    # ── service tokens ──────────────────────────────────────────────────
    async def test_service_token_lifecycle(self) -> None:
        token, token_id = await self.service_token("operador", dias=1)
        bearer = {"Authorization": f"Bearer {token}"}
        me = await self.client().get("/auth/me", headers=bearer)
        self.assertEqual((me.status_code, me.json()["tipo"], me.json()["papel"]), (200, "token_servico", "operador"))
        async with self.sessions() as db:
            stored = (await db.execute(select(AuthTokenServico).where(AuthTokenServico.id == token_id))).scalar_one()
        self.assertEqual(stored.token_hash, segredos.digest(token))
        self.assertNotIn(token, repr(stored.__dict__))
        self.assertIsNotNone(stored.ultimo_uso_em)
        # Expired.
        await self.execute(update(AuthTokenServico).where(AuthTokenServico.id == token_id)
                           .values(criado_em=func.utc_timestamp(6) - text("INTERVAL 2 DAY"),
                                   expira_em=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
        self.assertEqual((await self.client().get("/auth/me", headers=bearer)).status_code, 401)
        # Revoked.
        token, token_id = await self.service_token("leitura")
        bearer = {"Authorization": f"Bearer {token}"}
        self.assertEqual((await self.client().get("/auth/me", headers=bearer)).status_code, 200)
        self.assertEqual((await administracao.revogar_token_servico(self.sessions, ator="teste", token_id=token_id))["status"],
                         "revogado")
        self.assertEqual((await self.client().get("/auth/me", headers=bearer)).status_code, 401)
        with self.assertRaises(administracao.Recusado) as refused:
            await administracao.revogar_token_servico(self.sessions, ator="teste", token_id=token_id)
        self.assertEqual(refused.exception.code, "token_ja_revogado")

    async def test_service_token_never_admin_and_bounded(self) -> None:
        for kwargs, code in (({"papel": "admin", "dias": 30}, "papel_invalido"),
                             ({"papel": "operador", "dias": 0}, "validade_invalida"),
                             ({"papel": "operador", "dias": 366}, "validade_invalida")):
            with self.assertRaises(administracao.Recusado) as refused:
                await administracao.criar_token_servico(self.sessions, ator="teste", nome="x", **kwargs)
            self.assertEqual(refused.exception.code, code)
        with self.assertRaises(Exception):
            async with self.sessions() as db:
                await db.execute(text(
                    "INSERT INTO auth_tokens_servico (nome, papel, token_hash, criado_em, expira_em) "
                    "VALUES ('x', 'admin', REPEAT('a', 64), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6) + INTERVAL 1 DAY)"))
                await db.commit()

    # ── login throttling, lockout and enumeration ───────────────────────
    async def test_rate_limit_per_source(self) -> None:
        statuses = []
        for _ in range(limites.TENTATIVAS_POR_JANELA + 1):
            _, response = await self.login("nobody@example.invalid", source="203.0.113.7")
            statuses.append(response.status_code)
        self.assertEqual(statuses[:-1], [401] * limites.TENTATIVAS_POR_JANELA)
        self.assertEqual(statuses[-1], 429)
        _, response = await self.login("nobody@example.invalid", source="203.0.113.7")
        self.assertGreaterEqual(int(response.headers["retry-after"]), 1)
        _, other = await self.login("nobody@example.invalid", source="203.0.113.8")
        self.assertEqual(other.status_code, 401)
        # The activation endpoint has its own window.
        response = await self.client("203.0.113.7").post("/auth/ativar", json={"token": "x", "senha": SENHA})
        self.assertEqual(response.status_code, 400)

    async def test_window_slides(self) -> None:
        now = [0.0]
        limit = limites.LimitePorOrigem(tentativas=2, janela_segundos=10, relogio=lambda: now[0])
        self.assertIsNone(limit.registrar("login", "a"))
        self.assertIsNone(limit.registrar("login", "a"))
        self.assertEqual(limit.registrar("login", "a"), 10)
        now[0] = 10.5
        self.assertIsNone(limit.registrar("login", "a"))
        self.assertEqual(limit.recusas, 1)

    async def test_one_client_behind_the_proxy_never_blocks_another(self) -> None:
        """Nginx is the TCP peer of everybody: the window follows the real client."""

        _, email = await self.user("operador")
        limites.LIMITE.reset()
        statuses = [(await self.login("nobody@example.invalid", source=PROXY_IP, forwarded="203.0.113.50"))[1].status_code
                    for _ in range(limites.TENTATIVAS_POR_JANELA + 3)]
        self.assertEqual(statuses, [401] * limites.TENTATIVAS_POR_JANELA + [429] * 3)
        _, other = await self.login(email, source=PROXY_IP, forwarded="203.0.113.60")
        self.assertEqual(other.status_code, 200)  # was 429 when the proxy address was the key

        # A peer that is not the proxy cannot pick its own address.
        limites.LIMITE.reset()
        statuses = [(await self.login("nobody@example.invalid", source="198.51.100.77", forwarded=f"203.0.113.{n}"))[1].status_code
                    for n in range(1, limites.TENTATIVAS_POR_JANELA + 2)]
        self.assertEqual(statuses[-1], 429)
        # Through the proxy, a list or garbage (never sent by Nginx) falls back to the peer.
        limites.LIMITE.reset()
        statuses = [(await self.login("nobody@example.invalid", source=PROXY_IP, forwarded=value))[1].status_code
                    for n in range(limites.TENTATIVAS_POR_JANELA + 1)
                    for value in [f"203.0.113.{n}, 203.0.113.9" if n % 2 else f"lixo-{n}"]]
        self.assertEqual(statuses[-1], 429)

    async def _attempt(self, usuario_id: int, source: str) -> AuthTentativa | None:
        async with self.sessions() as db:
            return (await db.execute(select(AuthTentativa).where(
                AuthTentativa.usuario_id == usuario_id, AuthTentativa.origem == source))).scalar_one_or_none()

    async def test_progressive_delay_is_per_account_and_source(self) -> None:
        usuario_id, email = await self.user("operador")
        attacker, owner = "203.0.113.10", "203.0.113.11"
        for _ in range(limites.FALHAS_LIVRES):
            self.assertEqual((await self.login(email, "senha errada 000", attacker))[1].status_code, 401)
        row = await self._attempt(usuario_id, attacker)
        self.assertEqual(row.falhas, limites.FALHAS_LIVRES)
        self.assertEqual(row.liberado_em - row.ultima_falha_em, timedelta(seconds=30))
        # Delayed source: even the right password gets the generic answer and nothing is counted.
        refused = (await self.login(email, SENHA, attacker))[1]
        self.assertEqual((refused.status_code, refused.json()), (401, {"detail": "credenciais invalidas"}))
        again = await self._attempt(usuario_id, attacker)
        self.assertEqual((again.falhas, again.liberado_em), (row.falhas, row.liberado_em))
        # Accepted residual: another client on the same address shares the delay ...
        self.assertEqual((await self.login(email, SENHA, attacker))[1].status_code, 401)
        # ... but the owner, elsewhere, is never affected by the attacker.
        self.assertEqual((await self.login(email, SENHA, owner))[1].status_code, 200)
        # Delay elapsed: the next failure doubles it; the right password clears the row.
        for expected in (60, 120, 240, 480, 900, 900):
            await self.execute(update(AuthTentativa).values(liberado_em=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
            await self.login(email, "senha errada 000", attacker)
            row = await self._attempt(usuario_id, attacker)
            self.assertEqual((row.liberado_em - row.ultima_falha_em).total_seconds(), expected)
        await self.execute(update(AuthTentativa).values(liberado_em=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
        limites.LIMITE.reset()
        self.assertEqual((await self.login(email, SENHA, attacker))[1].status_code, 200)
        self.assertIsNone(await self._attempt(usuario_id, attacker))
        self.assertEqual([limites.atraso(n).total_seconds() for n in (0, 4, 5, 6, 10, 500)], [0, 0, 30, 60, 900, 900])
        # 24 h of silence restarts the count, and expired rows leave with the next new source.
        for _ in range(limites.FALHAS_LIVRES):
            await self.login(email, "senha errada 000", attacker)
        await self.execute(update(AuthTentativa).values(
            ultima_falha_em=func.utc_timestamp(6) - text("INTERVAL 25 HOUR"),
            liberado_em=func.utc_timestamp(6) - text("INTERVAL 24 HOUR")))
        await self.login(email, "senha errada 000", attacker)
        self.assertEqual((await self._attempt(usuario_id, attacker)).falhas, 1)
        await self.execute(update(AuthTentativa).values(ultima_falha_em=func.utc_timestamp(6) - text("INTERVAL 25 HOUR")))
        await self.login(email, "senha errada 000", "203.0.113.12")
        self.assertIsNone(await self._attempt(usuario_id, attacker))

    async def test_third_parties_cannot_keep_the_owner_out(self) -> None:
        usuario_id, email = await self.user("operador")
        for n in range(12):
            for _ in range(limites.FALHAS_LIVRES + 1):
                await self.login(email, "senha errada 000", f"203.0.113.{100 + n}")
        async with self.sessions() as db:
            row = (await db.execute(select(AuthUsuario).where(AuthUsuario.id == usuario_id))).scalar_one()
        self.assertEqual(row.falhas_consecutivas, 12 * limites.FALHAS_LIVRES)
        self.assertGreater(row.falhas_consecutivas, limites.SINAL_FALHAS_CONTA)
        self.assertEqual((await self.login(email, SENHA, "198.51.100.200"))[1].status_code, 200)

    async def test_concurrent_burst_cannot_outrun_the_delay(self) -> None:
        usuario_id, email = await self.user("operador")
        source = "203.0.113.20"
        burst = await asyncio.gather(*[self.login(email, "senha errada 000", source) for _ in range(12)])
        self.assertEqual({r.status_code for _, r in burst}, {401})
        self.assertEqual((await self._attempt(usuario_id, source)).falhas, limites.FALHAS_LIVRES)
        self.assertEqual((await self.login(email, SENHA, source))[1].status_code, 401)

    async def test_cli_unlock_clears_delays(self) -> None:
        usuario_id, email = await self.user("operador")
        sources = ("203.0.113.30", "2001:db8:1:2::aa")
        for source in sources:
            for _ in range(limites.FALHAS_LIVRES):
                await self.login(email, "senha errada 000", source)
        listed = {u["id"]: u for u in await administracao.listar_usuarios(self.sessions)}[usuario_id]
        self.assertEqual((listed["origens_atrasadas"], listed["totp_bloqueado"]), (2, False))
        self.assertIsNotNone(await self._attempt(usuario_id, "2001:db8:1:2::/64"))
        one = await administracao.desbloquear(self.sessions, ator="teste", usuario_id=usuario_id, origem="2001:db8:1:2::ffff")
        self.assertEqual(one["origens_liberadas"], 1)
        self.assertEqual((await self.login(email, SENHA, sources[1]))[1].status_code, 200)
        self.assertEqual((await self.login(email, SENHA, sources[0]))[1].status_code, 401)
        everything = await administracao.desbloquear(self.sessions, ator="teste", usuario_id=usuario_id)
        self.assertEqual(everything["origens_liberadas"], 1)
        self.assertEqual((await self.login(email, SENHA, sources[0]))[1].status_code, 200)
        with self.assertRaises(administracao.Recusado):
            await administracao.desbloquear(self.sessions, ator="teste", usuario_id=usuario_id, origem="nao-e-ip")
        with self.assertRaises(administracao.Recusado):
            await administracao.desbloquear(self.sessions, ator="teste", usuario_id=10**9)
        args = auth_cli._parser().parse_args(["usuario", "desbloquear", str(usuario_id), "--actor", "op",
                                              "--origem", "203.0.113.30"])
        emitted: list[object] = []
        self.assertEqual(await auth_cli.run(args, sessions=self.sessions, emit=emitted.append), 0)
        self.assertEqual(emitted[0]["status"], "desbloqueado")

    async def test_login_saturation_is_bounded_and_webhook_keeps_answering(self) -> None:
        release, pool = threading.Event(), senha.PiscinaHash()

        class Blocked(CountingHasher):
            def verify(self, *args, **kwargs):
                release.wait(30)
                return super().verify(*args, **kwargs)

        hasher = Blocked(time_cost=1, memory_cost=8, parallelism=1, hash_len=32, salt_len=16, type=Type.ID)
        capacity = senha.WORKERS + senha.FILA_MAXIMA

        async def uow_override():
            async with UnitOfWork(self.sessions) as uow:
                yield uow

        main.app.dependency_overrides[main.get_unit_of_work] = uow_override
        senha._dummy()
        with patch.object(senha, "HASHER", hasher), patch.object(senha, "_dummy_hash", hasher.hash("x")), \
             patch.object(senha, "PISCINA", pool), patch.object(senha, "ESPERA_MAXIMA", 30):
            logins = [asyncio.create_task(self.login(f"n{n}@example.invalid", source=f"198.51.100.{n + 1}"))
                      for n in range(capacity + 10)]
            try:
                async with asyncio.timeout(10):
                    while sum(t.done() for t in logins) < 10:
                        await asyncio.sleep(0.02)
                rejected = [t.result()[1] for t in logins if t.done()]
                self.assertEqual([(r.status_code, r.headers["retry-after"]) for r in rejected], [(503, "2")] * 10)
                # Every hashing thread is busy: the webhook still reaches the DB and answers.
                payload = {"event": "messages.upsert", "instance": "inexistente", "data": {
                    "key": {"id": "sat-1", "remoteJid": "5511999990001@s.whatsapp.net", "fromMe": False},
                    "message": {"conversation": "oi"}}}
                async with asyncio.timeout(3):
                    webhook = await self.client().post("/webhook", json=payload)
                    await asyncio.to_thread(int)  # default executor is not the hashing pool
                self.assertLess(webhook.status_code, 500, webhook.text)
                self.assertEqual(sum(not t.done() for t in logins), capacity)
                # Saturated logins are refused before touching the shared DB pool.
                opened = []
                main.app.dependency_overrides[contexto.get_auth_sessions] = lambda: (
                    lambda: opened.append(1) or self.sessions())
                late = (await self.login("tarde@example.invalid", source="198.51.100.240"))[1]
                main.app.dependency_overrides[contexto.get_auth_sessions] = lambda: self.sessions
                self.assertEqual((late.status_code, opened), (503, []))
            finally:
                release.set()
            accepted = [r for _, r in await asyncio.gather(*logins)]
        self.assertEqual(sorted(r.status_code for r in accepted), [401] * capacity + [503] * 10)
        self.assertEqual(pool.recusas, 11)

    async def test_login_failures_are_indistinguishable(self) -> None:
        _, ativo = await self.user("operador", "ativo@example.invalid")
        _, pendente = await self.user("leitura", "pendente@example.invalid", ativar=False)
        desativado_id, desativado = await self.user("leitura", "desativado@example.invalid")
        await administracao.alterar_status(self.sessions, ator="teste", usuario_id=desativado_id, ativo=False)
        admin_id, admin_sem_mfa = await self.user("admin", "admin-sem-mfa@example.invalid")
        await self.execute(update(AuthUsuario).where(AuthUsuario.id == admin_id).values(
            totp_segredo_cifrado=None, totp_chave_versao=None, totp_confirmado_em=None, totp_ultimo_passo=None))
        _, atrasado = await self.user("leitura", "atrasado@example.invalid")
        for _ in range(limites.FALHAS_LIVRES):
            await self.login(atrasado, "senha errada 000", "192.0.2.10")
        limites.LIMITE.reset()
        cases = {
            "admin_without_totp": (admin_sem_mfa, SENHA),
            "unknown": ("ninguem@example.invalid", SENHA),
            "wrong_password": (ativo, "senha errada 000"),
            "pending": (pendente, SENHA),
            "disabled": (desativado, SENHA),
            "delayed": (atrasado, SENHA),
            "malformed_email": ("not-an-email", SENHA),
            "oversized_password": (ativo, "x" * 5000),
            "case_variant_wrong": (ativo.upper(), "senha errada 000"),
        }
        counting = CountingHasher(time_cost=1, memory_cost=8, parallelism=1, hash_len=32, salt_len=16, type=Type.ID)
        bodies = set()
        with patch.object(senha, "HASHER", counting):
            senha._dummy()  # warm the dummy hash outside the measurement
            for name, (email, password) in cases.items():
                counting.hashes = counting.verifies = 0
                _, response = await self.login(email, password, source="192.0.2.10")
                bodies.add((response.status_code, response.text, response.headers.get("set-cookie")))
                self.assertEqual((counting.hashes, counting.verifies), (0, 1), name)
        self.assertEqual(bodies, {(401, '{"detail":"credenciais invalidas"}', None)})
        # Canonical e-mail: case and surrounding space do not create a second identity.
        self.assertEqual((await self.login("  " + ativo.upper() + " "))[1].status_code, 200)

    async def test_validation_error_never_echoes_secrets(self) -> None:
        response = await self.client().post("/auth/login", json={"email": "x@example.invalid",
                                                                 "senha": {"segredo": "sentinela-senha-77"}})
        self.assertEqual(response.status_code, 422)
        self.assertNotIn("sentinela-senha-77", response.text)
        self.assertEqual(response.json(), {"detail": [{"campo": "body.senha", "tipo": "string_type"}]})
        response = await self.client().post("/auth/ativar", json={"token": 123456789, "senha": SENHA})
        self.assertNotIn("123456789", response.text)

    async def test_password_rehash_on_login(self) -> None:
        usuario_id, email = await self.user("operador")
        async with self.sessions() as db:
            before = (await db.execute(select(AuthUsuario).where(AuthUsuario.id == usuario_id))).scalar_one()
        new_keys = {**PEPPER, 2: SecretStr("auth-test-pepper-v2-synthetic-abcdef0123456789")}
        with patch.object(settings, "auth_password_pepper_keys", new_keys), \
             patch.object(settings, "auth_password_pepper_current_version", 2):
            self.assertEqual((await self.login(email))[1].status_code, 200)
            async with self.sessions() as db:
                after = (await db.execute(select(AuthUsuario).where(AuthUsuario.id == usuario_id))).scalar_one()
            self.assertEqual((before.senha_pepper_versao, after.senha_pepper_versao), (1, 2))
            self.assertNotEqual(before.senha_hash, after.senha_hash)
            self.assertEqual((await self.login(email))[1].status_code, 200)
        # A version that left the keyring can no longer verify.
        with patch.object(settings, "auth_password_pepper_keys", {1: PEPPER[1]}), \
             patch.object(settings, "auth_password_pepper_current_version", 1):
            self.assertEqual((await self.login(email))[1].status_code, 401)

    async def test_legacy_password_login_and_rehash_after_pepper_removal(self) -> None:
        usuario_id, email = await self.user("operador")
        legacy = "e\u0301 antiga sem classes"
        # Synthetic historical credential, intentionally not a definition.
        old_hash, old_version = senha._hash_sync("é antiga sem classes")
        await self.execute(update(AuthUsuario).where(AuthUsuario.id == usuario_id).values(
            senha_hash=old_hash, senha_pepper_versao=old_version))
        self.assertEqual((await self.login(email, legacy))[1].status_code, 200)
        v2 = SecretStr("auth-test-pepper-v2-synthetic-abcdef0123456789")
        with patch.object(settings, "auth_password_pepper_keys", {**PEPPER, 2: v2}), \
             patch.object(settings, "auth_password_pepper_current_version", 2):
            self.assertEqual((await self.login(email, legacy))[1].status_code, 200)
        with patch.object(settings, "auth_password_pepper_keys", {2: v2}), \
             patch.object(settings, "auth_password_pepper_current_version", 2):
            self.assertEqual((await self.login(email, legacy))[1].status_code, 200)
            self.assertEqual((await self.login(email, "errada"))[1].status_code, 401)

    async def test_cli_tokens_use_definition_policy_without_consuming_on_refusal(self) -> None:
        cases = [
            ("Aa1!aaa", "senha_curta"), ("A1!" + "a" * 62, "senha_longa"),
            ("A!aaaaaa", "senha_sem_numero"), ("a1!aaaaa", "senha_sem_maiuscula"),
            ("A1 aaaaa", "senha_sem_especial"),
        ]
        async def cli(argv):
            output = []
            code = await auth_cli.run(auth_cli._parser().parse_args(argv), self.sessions, output.append)
            self.assertEqual(code, 0, output)
            return output[0]

        async def consume(token, role, email):
            logs = []
            sink = logger.add(lambda message: logs.append(str(message)))
            try:
                for value, code in cases:
                    response = await self.client().post("/auth/ativar", json={"token": token, "senha": value})
                    self.assertEqual((response.status_code, response.json()), (422, {"detail": code}))
                    self.assertNotIn(value, response.text)
                    self.assertNotIn(value, "".join(logs))
                    self.assertEqual(await self.count(
                        AuthTokenUsoUnico, AuthTokenUsoUnico.token_hash == segredos.digest(token),
                        AuthTokenUsoUnico.usado_em.is_(None)), 1)
                response = await self.client().post("/auth/ativar", json={"token": token, "senha": SENHA})
                self.assertEqual(response.status_code, 200, response.text)
                if role == "admin" and response.json().get("status") == "totp_enrolamento_requerido":
                    await self.confirm(response.json(), email)
            finally:
                logger.remove(sink)

        for role in ("admin", "operador"):
            email = f"cli-policy-{role}@example.invalid"
            argv = (["bootstrap-admin", "--email", email] if role == "admin" else
                    ["usuario", "criar", "--email", email, "--papel", role])
            emitted = await cli([*argv, "--actor", "synthetic-operator"])
            if role == "operador":
                emitted = await cli(["usuario", "reset", str(emitted["usuario_id"]),
                                     "--tipo", "ativacao", "--actor", "synthetic-operator"])
            await consume(emitted["token"], role, email)
            reset = await cli(["usuario", "reset", str(emitted["usuario_id"]),
                               "--tipo", "senha", "--actor", "synthetic-operator"])
            await consume(reset["token"], role, email)
            self.assertEqual((await self.login(email))[1].status_code, 200)

    def safe_client(self, source: str = "198.51.100.10") -> httpx.AsyncClient:
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app, client=(source, 40000), raise_app_exceptions=False),
            base_url=BASE_URL)
        self.clients.append(client)
        return client

    async def test_unencodable_login_inputs_are_generic_and_counted(self) -> None:
        active_id, active = await self.user("operador")
        _, pending = await self.user("leitura", "pending-utf@example.invalid", ativar=False)
        _, delayed = await self.user("leitura", "delayed-utf@example.invalid")
        source = "192.0.2.155"
        for _ in range(5):
            await self.login(delayed, "wrong", source)
        counting = CountingHasher(time_cost=1, memory_cost=8, parallelism=1, type=Type.ID)
        logs = []
        sink = logger.add(lambda message: logs.append(str(message)))
        bad_password = "A1!aaaa\ud800"
        try:
            with patch.object(senha, "HASHER", counting):
                senha._dummy()
                for email in (active, "unknown@example.invalid", pending, delayed):
                    for body in ({"email": email, "senha": bad_password},
                                 {"email": "\ud800@example.invalid", "senha": SENHA},
                                 {"email": "user@exa\udfffmple.invalid", "senha": SENHA}):
                        before = counting.verifies
                        response = await self.safe_client(source).post(
                            "/auth/login", content=json.dumps(body), headers={"Content-Type": "application/json"})
                        self.assertEqual(response.status_code, 401, response.text)
                        self.assertEqual(response.json(), {"detail": "credenciais invalidas"})
                        self.assertEqual(counting.verifies - before, 1)
            async with self.sessions() as db:
                attempt = (await db.execute(select(AuthTentativa).where(
                    AuthTentativa.usuario_id == active_id, AuthTentativa.origem == source))).scalar_one()
            self.assertEqual(attempt.falhas, 1)  # unusable text counts as a normal wrong password
            self.assertNotIn(bad_password, "".join(logs))
            self.assertNotIn("\ud800", "".join(logs))
            self.assertNotIn("\udfff", "".join(logs))
        finally:
            logger.remove(sink)

    async def test_unencodable_activation_tokens_and_non_ascii_credentials_are_inert(self) -> None:
        emitted = []
        await auth_cli.run(auth_cli._parser().parse_args([
            "usuario", "criar", "--email", "utf-new@example.invalid",
            "--papel", "operador", "--actor", "synthetic-operator"
        ]), self.sessions, emitted.append)
        token = emitted[0]["token"]
        client = self.safe_client()
        logs = []
        sink = logger.add(lambda message: logs.append(str(message)))
        try:
            for value in ("A1!aaaa\ud800", "A1!aaaa\udfff"):
                response = await client.post("/auth/ativar", content=json.dumps({"token": token, "senha": value}),
                                             headers={"Content-Type": "application/json"})
                self.assertEqual(response.status_code, 422, response.text)
                self.assertEqual(response.json(), {"detail": "senha_invalida"})
                self.assertEqual(await self.count(AuthTokenUsoUnico,
                    AuthTokenUsoUnico.token_hash == segredos.digest(token),
                    AuthTokenUsoUnico.usado_em.is_(None)), 1)
            response = await client.post("/auth/ativar", content=json.dumps({"token": "wpa_\ud800", "senha": SENHA}),
                                         headers={"Content-Type": "application/json"})
            self.assertEqual((response.status_code, response.json()), (400, {"detail": "token invalido"}))
            response = await client.post("/auth/ativar", json={"token": token, "senha": SENHA})
            self.assertEqual(response.status_code, 200)
            _, email = await self.user("operador")
            original, response = await self.login(email)
            logged = self.safe_client()
            logged.cookies.update(original.cookies)
            for raw in (b"caf\xe9", b"\xff", b"\xed\xa0\x80"):
                response = await logged.post("/auth/logout", headers=[(b"x-csrf-token", raw)])
                self.assertEqual(response.status_code, 403, response.text)
                self.assertEqual(response.json(), {"detail": "csrf invalido"})
                for header in ((b"cookie", b"__Host-wpp_sessao=" + raw),
                               (b"authorization", b"Bearer " + raw)):
                    response = await self.safe_client().get("/auth/me", headers=[header])
                    self.assertEqual((response.status_code, response.json()),
                                     (401, {"detail": "autenticacao requerida"}))
            self.assertNotIn("\ud800", "".join(logs))
            self.assertNotIn("\udfff", "".join(logs))
        finally:
            logger.remove(sink)

    async def test_production_hash_parameters(self) -> None:
        hasher = PRODUCTION_HASHER
        self.assertEqual((hasher.memory_cost, hasher.time_cost, hasher.parallelism, hasher.salt_len, hasher.type),
                         (32768, 3, 1, 16, Type.ID))
        with patch.object(senha, "HASHER", PRODUCTION_HASHER):
            stored, versao = await senha.gerar_hash(SENHA)
            self.assertTrue(stored.startswith("$argon2id$v=19$m=32768,t=3,p=1$"))
            self.assertEqual(versao, 1)
            self.assertTrue((await senha.verificar(SENHA, stored, 1)).ok)
            self.assertFalse((await senha.verificar(SENHA + "x", stored, 1)).ok)
        # The pepper is part of the derivation: same password, other pepper → mismatch.
        other = {1: SecretStr("auth-test-pepper-other-synthetic-0123456789")}
        with patch.object(senha, "HASHER", PRODUCTION_HASHER), patch.object(settings, "auth_password_pepper_keys", other):
            self.assertFalse((await senha.verificar(SENHA, stored, 1)).ok)

    # ── activation, reset and bootstrap ─────────────────────────────────
    async def test_activation_token_single_use_and_expiry(self) -> None:
        emitido = await administracao.criar_usuario(self.sessions, ator="teste", email="novo@example.invalid", papel="operador")
        async with self.sessions() as db:
            stored = (await db.execute(select(AuthTokenUsoUnico))).scalar_one()
        self.assertEqual(stored.token_hash, segredos.digest(emitido.token))
        client = self.client()
        weak = await client.post("/auth/ativar", json={"token": emitido.token, "senha": "12345678"})
        self.assertEqual((weak.status_code, weak.json()["detail"]), (422, "senha_sem_maiuscula"))
        self.assertEqual((await client.post("/auth/ativar", json={"token": emitido.token, "senha": SENHA})).status_code, 200)
        reused = await client.post("/auth/ativar", json={"token": emitido.token, "senha": SENHA + "x"})
        self.assertEqual((reused.status_code, reused.json()), (400, {"detail": "token invalido"}))
        self.assertEqual((await self.login("novo@example.invalid"))[1].status_code, 200)

        reset = await administracao.emitir_reset(self.sessions, ator="teste", usuario_id=emitido.usuario_id, tipo="senha")
        self.assertEqual((await self.login("novo@example.invalid"))[1].status_code, 200)
        await self.execute(update(AuthTokenUsoUnico).where(AuthTokenUsoUnico.token_hash == segredos.digest(reset.token))
                           .values(criado_em=func.utc_timestamp(6) - text("INTERVAL 2 DAY"),
                                   expira_em=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
        self.assertEqual((await client.post("/auth/ativar", json={"token": reset.token, "senha": SENHA})).status_code, 400)
        # A new reset revokes the previous unused token and every session.
        logged, _ = await self.login("novo@example.invalid")
        first = await administracao.emitir_reset(self.sessions, ator="teste", usuario_id=emitido.usuario_id, tipo="senha")
        second = await administracao.emitir_reset(self.sessions, ator="teste", usuario_id=emitido.usuario_id, tipo="senha")
        self.assertEqual((await logged.get("/auth/me")).status_code, 401)
        self.assertEqual((await client.post("/auth/ativar", json={"token": first.token, "senha": SENHA})).status_code, 400)
        new_password = "Outra frase sintetica 7!"
        self.assertEqual((await client.post("/auth/ativar", json={"token": second.token, "senha": new_password})).status_code, 200)
        self.assertEqual((await self.login("novo@example.invalid"))[1].status_code, 401)
        self.assertEqual((await self.login("novo@example.invalid", new_password))[1].status_code, 200)
        # A consumed reset token cannot be replayed while the user stays active.
        replay = await client.post("/auth/ativar", json={"token": second.token, "senha": "Terceira frase sintetica 8!"})
        self.assertEqual(replay.status_code, 400)
        self.assertEqual((await self.login("novo@example.invalid", new_password))[1].status_code, 200)
        with self.assertRaises(administracao.Recusado) as refused:
            await administracao.emitir_reset(self.sessions, ator="teste", usuario_id=emitido.usuario_id, tipo="ativacao")
        self.assertEqual(refused.exception.code, "tipo_incompativel_com_status")

    async def test_concurrent_activation_consumes_token_once(self) -> None:
        emitido = await administracao.criar_usuario(self.sessions, ator="teste", email="corrida@example.invalid", papel="leitura")
        responses = await asyncio.gather(*[
            self.client(f"192.0.2.{n}").post("/auth/ativar", json={"token": emitido.token, "senha": f"{SENHA} {n}"})
            for n in range(4)
        ])
        self.assertEqual(sorted(r.status_code for r in responses), [200, 400, 400, 400])

    async def test_bootstrap_only_once_even_concurrently(self) -> None:
        results = await asyncio.gather(
            *[administracao.bootstrap_admin(self.sessions, ator="teste", email=f"boot{n}@example.invalid") for n in range(3)],
            return_exceptions=True,
        )
        ok = [r for r in results if isinstance(r, administracao.TokenEmitido)]
        refused = [r for r in results if isinstance(r, administracao.Recusado)]
        self.assertEqual((len(ok), len(refused)), (1, 2), results)
        self.assertEqual({r.code for r in refused}, {"admin_ja_existe"})
        self.assertEqual(await self.count(AuthUsuario, AuthUsuario.papel == "admin"), 1)

    async def test_last_admin_is_protected(self) -> None:
        admin_id, _ = await self.user("admin")
        for operation in (
            administracao.alterar_papel(self.sessions, ator="teste", usuario_id=admin_id, papel="operador"),
            administracao.alterar_status(self.sessions, ator="teste", usuario_id=admin_id, ativo=False),
        ):
            with self.assertRaises(administracao.Recusado) as refused:
                await operation
            self.assertEqual(refused.exception.code, "ultimo_admin")
        second_id, _ = await self.user("admin")
        await administracao.alterar_status(self.sessions, ator="teste", usuario_id=admin_id, ativo=False)
        with self.assertRaises(administracao.Recusado):
            await administracao.alterar_papel(self.sessions, ator="teste", usuario_id=second_id, papel="leitura")
        with self.assertRaises(administracao.Recusado) as refused:
            await administracao.criar_usuario(self.sessions, ator="teste", email=(await self._email(second_id)).upper(),
                                              papel="leitura")
        self.assertEqual(refused.exception.code, "email_indisponivel")

    # ── schema parity (H) ───────────────────────────────────────────────
    async def test_orm_matches_init_sql_columns(self) -> None:
        async with self.sessions() as db:
            for table in AUTH_TABLES:
                rows = (await db.execute(text(
                    "SELECT column_name, is_nullable FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() AND table_name = :t ORDER BY ordinal_position"
                ), {"t": table})).all()
                orm = Base.metadata.tables[table]
                self.assertEqual([r[0] for r in rows], [c.name for c in orm.columns], table)
                self.assertEqual([r[1] == "YES" for r in rows], [bool(c.nullable) for c in orm.columns], table)
        init_sql = (REPO_ROOT / "db" / "init.sql").read_text()
        self.assertIn("CALL validate_auth_1_schema()$$", init_sql)
        self.assertIsNone(re.search(r"auth_(usuarios|sessoes|tokens|auditoria)", init_sql.split("-- AUTH-1:", 1)[0]))


if __name__ == "__main__":
    unittest.main()
