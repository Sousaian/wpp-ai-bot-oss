"""AUTH-2: roles, permission catalog, mandatory admin TOTP, masking, audit.

Reuses the fixtures of ``test_auth_api_mysql`` (real disposable MySQL, ASGI
app, real cookie jar, virtual TOTP clock); its own cases are not re-run.
Only synthetic data: ``*.invalid`` e-mails, ``55119999990xx`` phones.
"""

from __future__ import annotations

import asyncio
import json
import unittest
from uuid import uuid4
from unittest.mock import patch

import pyotp
from fastapi import Depends
from sqlalchemy import func, select, text, update

from src import main
from src.database.models import Agente, AuthAuditoria, AuthUsuario, Sessao
from src.services.auth import administracao, auditoria, limites, rbac, segredos, totp
from src.services.auth.rbac import Permissao as P
from tests import test_auth_api_mysql as base


PHONE = "5511999990042"


def base64_of(raw: bytes) -> str:
    import base64

    return base64.b64encode(raw).decode()
MASKED = "5511*****0042"
EMAIL_SENTINEL = "sentinela-email"


# Maintainer decision: admin everything; operador reads, transfers/resumes
# and edits system_prompt/ativo; leitura only GET with masked phones.
EXPECTED_ALLOWED = {
    ("GET", "/agentes"): {"admin", "operador", "leitura", "t_operador", "t_leitura"},
    ("POST", "/agentes"): {"admin"},
    ("GET", "/agentes/{agente_id}"): {"admin", "operador", "leitura", "t_operador", "t_leitura"},
    ("PUT", "/agentes/{agente_id}"): {"admin", "operador", "t_operador"},
    ("DELETE", "/agentes/{agente_id}"): {"admin"},
    ("GET", "/agentes/{agente_id}/consumo"): {"admin", "operador", "leitura", "t_operador", "t_leitura"},
    ("GET", "/sessoes"): {"admin", "operador", "leitura", "t_operador", "t_leitura"},
    ("GET", "/sessoes/{telefone}"): {"admin", "operador", "leitura", "t_operador", "t_leitura"},
    ("POST", "/sessoes/{telefone}/transferir"): {"admin", "operador", "t_operador"},
    ("POST", "/sessoes/{telefone}/retomar"): {"admin", "operador", "t_operador"},
    ("DELETE", "/sessoes/{telefone}"): {"admin"},
    ("GET", "/sessoes/{telefone}/historico"): {"admin", "operador", "leitura", "t_operador", "t_leitura"},
}


class AuthRbacMySQLTests(base.AuthApiMySQLTests):
    # ── fixtures ────────────────────────────────────────────────────────
    async def agent_with_session(self) -> int:
        # A fresh agent per test: (agente_id, telefone) is unique, so rows left
        # by other suites never collide and nothing is deleted globally.
        async with self.sessions() as db:
            agente = Agente(nome="rbac sintetico", instance_name=f"rbac-{uuid4()}", system_prompt="original",
                            numero_notificacao=PHONE, modelo_llm="openai/gpt-4o-mini")
            db.add(agente)
            await db.flush()
            db.add(Sessao(agente_id=agente.id, telefone=PHONE, handler="bot"))
            await db.commit()
            return int(agente.id)

    async def principals(self) -> dict[str, tuple]:
        """name → (client, extra headers) for every role and credential kind."""

        result = {}
        for papel in ("admin", "operador", "leitura"):
            client, csrf, _ = await self.logged(papel)
            result[papel] = (client, {"X-CSRF-Token": csrf})
        for papel in ("operador", "leitura"):
            token, _ = await self.service_token(papel)
            result[f"t_{papel}"] = (self.client(), {"Authorization": f"Bearer {token}"})
        return result

    async def audit_rows(self) -> list[AuthAuditoria]:
        async with self.sessions() as db:
            return list((await db.execute(select(AuthAuditoria).order_by(AuthAuditoria.id))).scalars())

    # ── catalog ─────────────────────────────────────────────────────────
    async def test_permission_catalog_matches_decision(self) -> None:
        self.assertEqual(set(rbac.PAPEIS), {"admin", "operador", "leitura"})
        self.assertEqual(rbac.PAPEIS["admin"], frozenset(P))
        self.assertEqual(rbac.PAPEIS["operador"], {
            P.AGENTE_LER, P.AGENTE_EDITAR_OPERACIONAL, P.CONSUMO_LER, P.SESSAO_LER,
            P.SESSAO_LER_TELEFONE, P.SESSAO_TRANSFERIR, P.SESSAO_RETOMAR})
        self.assertEqual(rbac.PAPEIS["leitura"], {P.AGENTE_LER, P.CONSUMO_LER, P.SESSAO_LER})
        self.assertEqual(rbac.CAMPOS_OPERACIONAIS, {"system_prompt", "ativo"})
        self.assertEqual(rbac.permissoes("root"), frozenset())
        for papel in ("operador", "leitura"):
            self.assertFalse({P.USUARIO_GERENCIAR, P.TOKEN_GERENCIAR, P.AGENTE_CRIAR, P.AGENTE_EXCLUIR}
                             & rbac.PAPEIS[papel])

    async def test_every_route_declares_exactly_one_permission(self) -> None:
        declared = {}
        for route in main.app.routes:
            if getattr(route, "path", "").startswith(("/agentes", "/sessoes")):
                for method in route.methods - {"HEAD"}:
                    declared[(method, route.path)] = rbac.permissoes_da_rota(route)
        self.assertEqual(set(declared), set(EXPECTED_ALLOWED))
        self.assertTrue(all(declared.values()), declared)

    async def test_route_without_permission_fails_closed(self) -> None:
        async def unguarded() -> dict:
            return {"leak": True}

        main.app.add_api_route("/rota-sem-permissao/teste", unguarded, methods=["GET"],
                               dependencies=[Depends(rbac.exigir_rota_declarada)])
        try:
            admin, _, _ = await self.logged("admin")
            response = await admin.get("/rota-sem-permissao/teste")
            self.assertEqual(response.status_code, 403)
            self.assertNotIn("leak", response.text)
            self.assertEqual((await self.client().get("/rota-sem-permissao/teste")).status_code, 401)
        finally:
            main.app.router.routes[:] = [r for r in main.app.router.routes
                                         if getattr(r, "path", None) != "/rota-sem-permissao/teste"]

    # ── 401/403 matrix by route and role ────────────────────────────────
    async def test_rbac_matrix_by_route_and_role(self) -> None:
        agente_id = await self.agent_with_session()
        principals = await self.principals()
        bodies = {("PUT", "/agentes/{agente_id}"): {"system_prompt": "matriz"}}
        executed = 0
        for (method, template), allowed in EXPECTED_ALLOWED.items():
            path = template.replace("{agente_id}", str(agente_id)).replace("{telefone}", PHONE)
            for name, (client, headers) in principals.items():
                params = {"agente_id": str(agente_id)} if path.startswith("/sessoes") else None
                if method in {"POST", "DELETE"} and name in allowed and template != "/sessoes/{telefone}/retomar":
                    # Allowed destructive calls run against a missing target.
                    path_run = path.replace(str(agente_id), "999999").replace(PHONE, "5511999990099")
                    params = {"agente_id": "999999"} if path.startswith("/sessoes") else None
                else:
                    path_run = path
                response = await client.request(method, path_run, params=params, headers=headers,
                                                json=bodies.get((method, template), {}))
                if name in allowed:
                    self.assertNotIn(response.status_code, (401, 403), (method, template, name, response.text))
                else:
                    self.assertEqual(response.status_code, 403, (method, template, name, response.text))
                    self.assertEqual(response.json(), {"detail": "permissao insuficiente"})
                executed += 1
        self.assertEqual(executed, 12 * 5)
        denied = [r for r in await self.audit_rows() if r.evento == "acesso_negado"]
        expected_denials = sum(5 - len(allowed) for allowed in EXPECTED_ALLOWED.values())
        self.assertEqual(len(denied), expected_denials)

    async def test_operador_edits_only_prompt_and_active(self) -> None:
        agente_id = await self.agent_with_session()
        principals = await self.principals()
        for name in ("operador", "t_operador"):
            client, headers = principals[name]
            for body in ({"modelo_llm": "anthropic/claude-sonnet-4"}, {"system_prompt": "x", "max_historico": 3},
                         {"chatwoot_inbox_id": 7}, {"limite_mensal_creditos": "1"}, {"nome": "outro"}):
                response = await client.put(f"/agentes/{agente_id}", headers=headers, json=body)
                self.assertEqual(response.status_code, 403, (name, body))
            response = await client.put(f"/agentes/{agente_id}", headers=headers,
                                        json={"system_prompt": f"novo {name}", "ativo": True})
            self.assertEqual(response.status_code, 200, response.text)
        async with self.sessions() as db:
            agente = (await db.execute(select(Agente).where(Agente.id == agente_id))).scalar_one()
        self.assertEqual((agente.system_prompt, agente.modelo_llm, agente.max_historico),
                         ("novo t_operador", "openai/gpt-4o-mini", 20))
        admin, headers = principals["admin"]
        self.assertEqual((await admin.put(f"/agentes/{agente_id}", headers=headers,
                                          json={"max_historico": 5})).status_code, 200)
        updates = [r for r in await self.audit_rows() if r.evento == "agente_atualizado"]
        self.assertEqual([r.detalhe["campos"] for r in updates],
                         [["ativo", "system_prompt"], ["ativo", "system_prompt"], ["max_historico"]])

    # ── masking ─────────────────────────────────────────────────────────
    async def test_leitura_sees_masked_phones(self) -> None:
        agente_id = await self.agent_with_session()
        principals = await self.principals()
        seen = {}
        for name, (client, headers) in principals.items():
            listing = await client.get("/sessoes", params={"agente_id": agente_id}, headers=headers)
            detail = await client.get(f"/sessoes/{PHONE}", params={"agente_id": agente_id}, headers=headers)
            self.assertEqual((listing.status_code, detail.status_code), (200, 200), name)
            seen[name] = (listing.json()["sessoes"][0]["telefone"], detail.json()["telefone"])
            if name in {"leitura", "t_leitura"}:
                self.assertNotIn(PHONE, listing.text + detail.text, name)
        self.assertEqual(seen, {
            "admin": (PHONE, PHONE), "operador": (PHONE, PHONE), "t_operador": (PHONE, PHONE),
            "leitura": (MASKED, MASKED), "t_leitura": (MASKED, MASKED),
        })
        # The agent's legacy numero_notificacao is a phone too (review finding).
        for name, (client, headers) in principals.items():
            listing = await client.get("/agentes", headers=headers)
            detail = await client.get(f"/agentes/{agente_id}", headers=headers)
            numbers = ([a["numero_notificacao"] for a in listing.json() if a["id"] == agente_id][0],
                       detail.json()["numero_notificacao"])
            self.assertEqual(numbers, (MASKED, MASKED) if name.endswith("leitura") else (PHONE, PHONE), name)
            if name.endswith("leitura"):
                self.assertNotIn(PHONE, listing.text + detail.text, name)

    def test_keyring_validation(self) -> None:
        from src import config

        self.assertTrue(config._aes256_key_is_usable(base.TOTP_KEYS[1]))
        for bad in ("", "nao-e-base64!", "QUFB", base64_of(b"\x00" * 32), base64_of(b"k" * 31)):
            self.assertFalse(config._aes256_key_is_usable(base.SecretStr(bad)), bad)
        self.assertTrue(config._auth_secret_is_usable(base.PEPPER[1]))
        for bad in ("curto", "replace-with-a-32-character-secret-value", "CHANGE-ME" + "x" * 40):
            self.assertFalse(config._auth_secret_is_usable(base.SecretStr(bad)), bad)

    def test_masking_helpers(self) -> None:
        self.assertEqual(auditoria.mascarar_telefone("5511999990042"), "5511*****0042")
        self.assertEqual(auditoria.mascarar_telefone("551199990042"), "5511****0042")
        self.assertEqual(auditoria.mascarar_telefone("12345678"), "******78")
        self.assertEqual(auditoria.mascarar_telefone(None), "")
        self.assertEqual(auditoria.mascarar_email("pessoa@example.invalid"), "p***@example.invalid")
        self.assertEqual(auditoria.mascarar_email("sem-arroba"), "***")

    # ── TOTP ────────────────────────────────────────────────────────────
    async def test_admin_login_requires_totp_and_without_it_is_refused(self) -> None:
        admin_id, email = await self.user("admin")
        client, response = await self.login(email)
        body = response.json()
        self.assertEqual((response.status_code, body["mfa_requerido"]), (200, True))
        self.assertTrue(body["desafio"].startswith("wpd_"))
        self.assertNotIn("set-cookie", response.headers)  # password alone yields no session
        verified = await client.post("/auth/totp/verificar", json={"desafio": body["desafio"],
                                                                   "codigo": self.code(email)})
        self.assertEqual(verified.status_code, 200)
        self.assertEqual((await client.get("/agentes")).status_code, 200)
        # TOTP removed behind the app's back: the live session dies and login is refused.
        await self.execute(update(AuthUsuario).where(AuthUsuario.id == admin_id).values(
            totp_segredo_cifrado=None, totp_chave_versao=None, totp_confirmado_em=None, totp_ultimo_passo=None))
        self.assertEqual((await client.get("/agentes")).status_code, 401)
        refused = (await self.login(email))[1]
        self.assertEqual((refused.status_code, refused.json()), (401, {"detail": "credenciais invalidas"}))
        # reset_mfa: out-of-band token → enrolment → confirmation → login again.
        from src import auth_cli
        emitted = []
        rc = await auth_cli.run(auth_cli._parser().parse_args([
            "usuario", "reset", str(admin_id), "--tipo", "mfa", "--actor", "synthetic-operator"
        ]), self.sessions, emitted.append)
        self.assertEqual(rc, 0)
        reset_token = emitted[0]["token"]
        enrol = await self.client().post("/auth/ativar", json={"token": reset_token})
        self.assertEqual(enrol.json()["status"], "totp_enrolamento_requerido")
        self.assertEqual((await self.login(email))[1].status_code, 401)  # still refused before confirm
        await self.confirm(enrol.json(), email)
        _, final = await self.full_login(email)
        self.assertEqual(final.status_code, 200)

    async def test_admin_activation_stays_pending_until_totp_confirmed(self) -> None:
        emitido = await administracao.bootstrap_admin(self.sessions, ator="teste", email="boot@example.invalid")
        response = await self.client().post("/auth/ativar", json={"token": emitido.token, "senha": base.SENHA})
        body = response.json()
        self.assertEqual(body["status"], "totp_enrolamento_requerido")
        self.assertTrue(body["totp_uri"].startswith("otpauth://totp/"))
        self.assertEqual(response.headers["cache-control"], "no-store")
        async with self.sessions() as db:
            row = (await db.execute(select(AuthUsuario).where(AuthUsuario.id == emitido.usuario_id))).scalar_one()
        self.assertEqual((row.status, row.totp_confirmado_em), ("pendente", None))
        self.assertNotIn(body["totp_segredo"].encode(), bytes(row.totp_segredo_cifrado))
        self.assertEqual((await self.login("boot@example.invalid"))[1].status_code, 401)
        await self.confirm(body, "boot@example.invalid")
        async with self.sessions() as db:
            self.assertEqual((await db.execute(select(AuthUsuario.status).where(
                AuthUsuario.id == emitido.usuario_id))).scalar_one(), "ativo")

    async def test_totp_replay_window_and_concurrency(self) -> None:
        _, email = await self.user("admin")
        secret = self.totp_secrets[email]
        challenge = lambda r: r.json()["desafio"]  # noqa: E731
        client, first = await self.login(email)
        self.clock[0] += totp.PERIODO
        code = pyotp.TOTP(secret).at(self.clock[0])
        self.assertEqual((await client.post("/auth/totp/verificar",
                                            json={"desafio": challenge(first), "codigo": code})).status_code, 200)
        # Same code (same step) on a new challenge: replay refused.
        client, second = await self.login(email)
        replay = await client.post("/auth/totp/verificar", json={"desafio": challenge(second), "codigo": code})
        self.assertEqual((replay.status_code, replay.json()), (401, {"detail": "codigo invalido"}))
        # Two steps back is outside the ±1 window.
        old = pyotp.TOTP(secret).at(self.clock[0] - 2 * totp.PERIODO)
        self.assertEqual((await client.post("/auth/totp/verificar",
                                            json={"desafio": challenge(second), "codigo": old})).status_code, 401)
        # Concurrent use of one fresh code on two challenges: exactly one session.
        self.clock[0] += totp.PERIODO
        fresh = pyotp.TOTP(secret).at(self.clock[0])
        c1, r1 = await self.login(email)
        c2, r2 = await self.login(email)
        results = await asyncio.gather(
            c1.post("/auth/totp/verificar", json={"desafio": challenge(r1), "codigo": fresh}),
            c2.post("/auth/totp/verificar", json={"desafio": challenge(r2), "codigo": fresh}))
        self.assertEqual(sorted(r.status_code for r in results), [200, 401])

    async def test_totp_window_is_one_step(self) -> None:
        _, email = await self.user("admin")
        secret = self.totp_secrets[email]
        self.clock[0] += 10 * totp.PERIODO  # every candidate step is newer than the last accepted one
        for offset, accepted in ((-2, False), (2, False), (-1, True), (1, True)):
            client, response = await self.login(email)
            code = pyotp.TOTP(secret).at(self.clock[0] + offset * totp.PERIODO)
            result = await client.post("/auth/totp/verificar",
                                       json={"desafio": response.json()["desafio"], "codigo": code})
            self.assertEqual(result.status_code, 200 if accepted else 401, offset)
            if accepted:
                self.clock[0] += 3 * totp.PERIODO

    async def test_challenge_attempts_and_kinds(self) -> None:
        _, email = await self.user("admin")
        client, response = await self.login(email)
        desafio = response.json()["desafio"]
        for _ in range(5):
            wrong = await client.post("/auth/totp/verificar", json={"desafio": desafio, "codigo": "000000"})
            self.assertEqual(wrong.status_code, 401)
        right = await client.post("/auth/totp/verificar", json={"desafio": desafio, "codigo": self.code(email)})
        self.assertEqual(right.status_code, 401)  # challenge burnt after 5 attempts
        # A challenge is never a session cookie, and a session is never a challenge.
        client, response = await self.login(email)
        jar = self.client()
        jar.cookies.set("__Host-wpp_sessao", response.json()["desafio"], domain="auth.invalid")
        self.assertEqual((await jar.get("/auth/me")).status_code, 401)
        session, logged = await self.full_login(email)
        cookie = session.cookies.get("__Host-wpp_sessao")
        self.assertEqual((await self.client().post("/auth/totp/verificar", json={
            "desafio": cookie, "codigo": self.code(email)})).status_code, 401)
        for bad in ("12345", "abcdef", "1234567", "", "１２３４５６", "١٢٣٤٥٦"):
            self.assertIsNone(totp.passo_aceito(self.totp_secrets[email], bad))
        # Non-ASCII digits are an ordinary wrong code on both endpoints (review finding).
        client, response = await self.login(email)
        bad = await client.post("/auth/totp/verificar", json={"desafio": response.json()["desafio"],
                                                              "codigo": "１２３４５６"})
        self.assertEqual((bad.status_code, bad.json()), (401, {"detail": "codigo invalido"}))
        async with self.sessions() as db:
            attempts = (await db.execute(text("SELECT tentativas_mfa FROM auth_sessoes WHERE token_hash = :h"),
                                         {"h": segredos.digest(response.json()["desafio"])})).scalar_one()
        self.assertEqual(attempts, 1)
        self.assertIn("mfa_falha", [r.evento for r in await self.audit_rows()])
        confirm = await self.client().post("/auth/totp/confirmar", json={"desafio": response.json()["desafio"],
                                                                         "codigo": "١٢٣٤٥٦"})
        self.assertEqual(confirm.status_code, 401)

    async def test_enrolment_confirmation_rejects_non_ascii_digits(self) -> None:
        _, email = await self.user("operador")
        client, response = await self.login(email)
        enrol = await client.post("/auth/totp/enrolar", headers={"X-CSRF-Token": response.json()["csrf_token"]})
        desafio = enrol.json()["desafio"]
        for codigo in ("１２３４５６", "١٢٣٤٥٦"):
            bad = await self.client().post("/auth/totp/confirmar", json={"desafio": desafio, "codigo": codigo})
            self.assertEqual((bad.status_code, bad.json()), (401, {"detail": "codigo invalido"}))
        async with self.sessions() as db:
            attempts = (await db.execute(text("SELECT tentativas_mfa FROM auth_sessoes WHERE token_hash = :h"),
                                         {"h": segredos.digest(desafio)})).scalar_one()
        self.assertEqual(attempts, 2)
        self.totp_secrets[email] = enrol.json()["totp_segredo"]
        ok = await self.client().post("/auth/totp/confirmar", json={"desafio": desafio, "codigo": self.code(email)})
        self.assertEqual(ok.status_code, 200)

    async def test_totp_lock_is_short_account_wide_and_needs_the_password(self) -> None:
        admin_id, email = await self.user("admin")

        async def wrong_codes(total: int) -> None:
            while total:
                client, response = await self.login(email, source=f"198.51.100.{total}")
                for _ in range(min(5, total)):
                    bad = await client.post("/auth/totp/verificar", json={
                        "desafio": response.json()["desafio"], "codigo": "000000"})
                    self.assertEqual(bad.status_code, 401)
                total -= min(5, total)
                limites.LIMITE.reset()

        async def row() -> AuthUsuario:
            async with self.sessions() as db:
                return (await db.execute(select(AuthUsuario).where(AuthUsuario.id == admin_id))).scalar_one()

        # A stranger without the password never reaches the second factor.
        for n in range(20):
            await self.login(email, "senha errada 000", source=f"203.0.113.{n + 1}")
        self.assertEqual(((await row()).totp_falhas, (await row()).totp_bloqueado_ate), (0, None))
        for expected in (900, 1800, 3600, 3600):
            await wrong_codes(limites.TOTP_FALHAS_ATE_BLOQUEIO)
            locked = await row()
            async with self.sessions() as db:
                now = (await db.execute(select(func.utc_timestamp(6)))).scalar_one()
            self.assertAlmostEqual((locked.totp_bloqueado_ate - now).total_seconds(), expected, delta=60)
            # Locked: the right code is refused from any source.
            self.assertEqual((await self.full_login(email, source="192.0.2.77"))[1].status_code, 401)
            limites.LIMITE.reset()
            await self.execute(update(AuthUsuario).where(AuthUsuario.id == admin_id).values(
                totp_bloqueado_ate=func.utc_timestamp(6) - text("INTERVAL 1 SECOND")))
        self.assertEqual((await self.full_login(email))[1].status_code, 200)  # success resets
        self.assertEqual(((await row()).totp_bloqueios, (await row()).totp_bloqueado_ate), (0, None))
        await wrong_codes(limites.TOTP_FALHAS_ATE_BLOQUEIO)
        self.assertEqual((await self.full_login(email))[1].status_code, 401)
        await administracao.desbloquear(self.sessions, ator="teste", usuario_id=admin_id)
        self.assertEqual((await self.full_login(email))[1].status_code, 200)
        self.assertIn("usuario_desbloqueado", [r.evento for r in await self.audit_rows()])

    async def test_audit_records_the_real_client_behind_the_proxy(self) -> None:
        agente_id = await self.agent_with_session()
        _, email = await self.user("admin")
        _, leitura_email = await self.user("leitura")
        via = {"X-Forwarded-For": "203.0.113.200"}
        client = self.client(base.PROXY_IP)
        challenge = await client.post("/auth/login", headers=via, json={"email": email, "senha": base.SENHA})
        session = await client.post("/auth/totp/verificar", headers=via, json={
            "desafio": challenge.json()["desafio"], "codigo": self.code(email)})
        headers = {**via, "X-CSRF-Token": session.json()["csrf_token"]}
        self.assertEqual((await client.post(f"/sessoes/{PHONE}/transferir", params={"agente_id": agente_id},
                                            headers=headers)).status_code, 200)
        reader = self.client(base.PROXY_IP)
        csrf = (await reader.post("/auth/login", headers=via, json={
            "email": leitura_email, "senha": base.SENHA})).json()["csrf_token"]
        await reader.delete("/agentes/999999", headers={**via, "X-CSRF-Token": csrf})
        # A direct peer cannot write somebody else's address into the trail.
        await self.login(email, "senha errada 000", source="198.51.100.9", forwarded="203.0.113.201")
        by_event = {r.evento: r.origem_ip for r in await self.audit_rows()}
        for event in ("login_sucesso", "sessao_transferida", "acesso_negado"):
            self.assertEqual(by_event[event], "203.0.113.200", event)
        self.assertEqual(by_event["login_falha"], "198.51.100.9")

    async def test_secret_is_encrypted_and_bound_to_user(self) -> None:
        admin_id, email = await self.user("admin")
        secret = self.totp_secrets[email]
        async with self.sessions() as db:
            row = (await db.execute(select(AuthUsuario).where(AuthUsuario.id == admin_id))).scalar_one()
        stored = bytes(row.totp_segredo_cifrado)
        self.assertNotIn(secret.encode(), stored)
        self.assertEqual(totp.decifrar(stored, row.totp_chave_versao, admin_id), secret)
        self.assertIsNone(totp.decifrar(stored, row.totp_chave_versao, admin_id + 1))  # AAD = user row
        other = {1: base.SecretStr("AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=")}
        with patch.object(base.settings, "auth_totp_encryption_keys", other):
            self.assertIsNone(totp.decifrar(stored, 1, admin_id))

    async def test_voluntary_enrolment_and_promotion(self) -> None:
        operador_id, email = await self.user("operador")
        client, csrf, _ = await self.logged("leitura")
        self.assertEqual((await client.post("/auth/totp/enrolar")).status_code, 403)  # CSRF still applies
        client, response = await self.login(email)
        csrf = response.json()["csrf_token"]
        enrol = await client.post("/auth/totp/enrolar", headers={"X-CSRF-Token": csrf})
        self.assertEqual(enrol.status_code, 200)
        await self.confirm(enrol.json(), email)
        again = await client.post("/auth/totp/enrolar", headers={"X-CSRF-Token": csrf})
        self.assertEqual(again.status_code, 409)
        self.assertTrue((await self.login(email))[1].json()["mfa_requerido"])
        # Promotion of a user without TOTP: refused at login until reset_mfa.
        leitura_id, leitura_email = await self.user("leitura")
        result = await administracao.alterar_papel(self.sessions, ator="teste", usuario_id=leitura_id, papel="admin")
        self.assertTrue(result["mfa_requerido"])
        self.assertEqual((await self.login(leitura_email))[1].status_code, 401)

    # ── audit ───────────────────────────────────────────────────────────
    async def test_audit_trail_without_clear_phone_or_email(self) -> None:
        agente_id = await self.agent_with_session()
        email = f"{EMAIL_SENTINEL}@example.invalid"
        await self.user("admin", email)
        _, operador_email = await self.user("operador", f"{EMAIL_SENTINEL}-op@example.invalid")
        await self.login(operador_email, "senha errada 000")
        await self.login(f"{EMAIL_SENTINEL}-x@example.invalid", base.SENHA)
        admin, response = await self.full_login(email)
        csrf = {"X-CSRF-Token": response.json()["csrf_token"]}
        self.assertEqual((await admin.post(f"/sessoes/{PHONE}/transferir", params={"agente_id": agente_id},
                                           headers=csrf)).status_code, 200)
        self.assertEqual((await admin.post(f"/sessoes/{PHONE}/retomar", params={"agente_id": agente_id},
                                           headers=csrf)).status_code, 200)
        leitura, leitura_csrf, _ = await self.logged("leitura")
        await leitura.post(f"/sessoes/{PHONE}/transferir", params={"agente_id": agente_id},
                           headers={"X-CSRF-Token": leitura_csrf})
        await admin.post("/auth/logout", headers=csrf)
        token = await administracao.criar_token_servico(self.sessions, ator="operador-cli", nome="n8n",
                                                        papel="leitura", dias=7)
        await administracao.revogar_token_servico(self.sessions, ator="operador-cli", token_id=token["id"])

        rows = await self.audit_rows()
        events = [r.evento for r in rows]
        for expected in ("bootstrap_admin", "usuario_criado", "senha_definida", "totp_confirmado",
                         "login_falha", "login_sucesso", "sessao_transferida", "sessao_retomada",
                         "acesso_negado", "logout", "token_servico_criado", "token_servico_revogado"):
            self.assertIn(expected, events)
        self.assertEqual(len([e for e in events if e == "login_falha"]), 2)
        transfer = next(r for r in rows if r.evento == "sessao_transferida")
        self.assertEqual((transfer.ator_tipo, transfer.detalhe["telefone_mascarado"], transfer.detalhe["agente_id"]),
                         ("usuario", MASKED, agente_id))
        cli = next(r for r in rows if r.evento == "token_servico_criado")
        self.assertEqual((cli.ator_tipo, cli.ator_rotulo), ("cli", "operador-cli"))
        dump = json.dumps([[getattr(r, c.name) for c in AuthAuditoria.__table__.columns] for r in rows], default=str)
        self.assertNotIn(PHONE, dump)
        self.assertNotIn(EMAIL_SENTINEL, dump)
        self.assertNotIn(token["token"], dump)
        for secret in self.totp_secrets.values():
            self.assertNotIn(secret, dump)

    async def test_audit_rejects_unknown_event_and_detail_keys(self) -> None:
        async with self.sessions() as db:
            with self.assertRaises(ValueError):
                await auditoria.registrar(db, "evento_livre", ator_tipo="cli")
            with self.assertRaises(ValueError):
                await auditoria.registrar(db, "logout", ator_tipo="cli", detalhe={"telefone": PHONE})

    async def test_domain_action_and_audit_commit_together(self) -> None:
        agente_id = await self.agent_with_session()
        admin, csrf, _ = await self.logged("admin")

        async def failing(*args, **kwargs):
            raise RuntimeError("audit store down")

        with patch.object(auditoria.auth_repo, "registrar_auditoria", failing):
            with self.assertRaises(RuntimeError):
                await admin.post(f"/sessoes/{PHONE}/transferir", params={"agente_id": agente_id},
                                 headers={"X-CSRF-Token": csrf})
        async with self.sessions() as db:
            handler = (await db.execute(select(Sessao.handler).where(
                Sessao.telefone == PHONE, Sessao.agente_id == agente_id))).scalar_one()
        self.assertEqual(handler, "bot")  # no audit, no transfer


# Inherited fixture cases already run in test_auth_api_mysql.
for _name in dir(base.AuthApiMySQLTests):
    if _name.startswith("test_"):
        setattr(AuthRbacMySQLTests, _name, None)


if __name__ == "__main__":
    unittest.main()
