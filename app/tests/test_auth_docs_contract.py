"""AUTH docs contract (H): AGENTS.md, README, templates and code say the same."""

import os
import subprocess
import sys
from datetime import datetime
from unittest.mock import patch, AsyncMock

import httpx
from fastapi import Response
from sqlalchemy.exc import SQLAlchemyError
import re
import unittest
from pathlib import Path

from fastapi.routing import APIRoute

from src import main
from src.config import Settings
from src.services.auth import auditoria, contexto, limites, origem, rbac, senha, sessao, totp
from src.services.auth.rbac import Permissao as P
from src.routers import auth
from tests import test_auth_api_mysql as base


ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
AUTH_ENV = (
    "AUTH_PASSWORD_PEPPER_CURRENT_VERSION=1",
    "AUTH_PASSWORD_PEPPER_KEYS={}",
    "AUTH_TOTP_ENCRYPTION_CURRENT_VERSION=1",
    "AUTH_TOTP_ENCRYPTION_KEYS={}",
    "AUTH_TRUSTED_PROXY_HOST=nginx",
)


def section(text: str, start: str, end: str) -> str:
    return text.split(start, 1)[1].split(end, 1)[0]


class AuthDocsContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.agents = (ROOT / "AGENTS.md").read_text()
        self.readme = (ROOT / "README.md").read_text()

    def test_auth_routes_documented(self) -> None:
        section_7 = section(self.agents, "## 7. API Endpoints", "## 8. Stack Oficial")
        routes = sorted(
            (method, route.path)
            for route in main.app.routes
            if isinstance(route, APIRoute) and route.path.startswith("/auth")
            for method in route.methods - {"HEAD"}
        )
        self.assertEqual(len(routes), 7)
        for method, path in routes:
            self.assertIn(f"| `{method}` | `{path}` |", section_7, path)
            self.assertIn(path, self.readme, path)

    def test_permission_table_matches_catalog(self) -> None:
        section_7 = section(self.agents, "Permissões por rota", "### Agentes — interno")
        rows = {
            line.split("|")[1].strip(): [cell.strip() for cell in line.split("|")[2:5]]
            for line in section_7.splitlines()
            if line.startswith("| `")
        }
        expected = {
            "`GET /agentes`, `GET /agentes/{id}`, `GET /agentes/{id}/consumo`": (P.AGENTE_LER, P.CONSUMO_LER),
            "`POST /agentes`, `DELETE /agentes/{id}`": (P.AGENTE_CRIAR, P.AGENTE_EXCLUIR),
            "`PUT /agentes/{id}`": (P.AGENTE_EDITAR_OPERACIONAL,),
            "`GET /sessoes`, `GET /sessoes/{telefone}`, `GET …/historico`": (P.SESSAO_LER,),
            "`POST …/transferir`, `POST …/retomar`": (P.SESSAO_TRANSFERIR, P.SESSAO_RETOMAR),
            "`DELETE /sessoes/{telefone}`": (P.SESSAO_EXCLUIR,),
        }
        self.assertEqual(set(rows), set(expected))
        for row, permissions in expected.items():
            documented = [cell != "—" for cell in rows[row]]
            actual = [all(p in rbac.PAPEIS[papel] for p in permissions)
                      for papel in ("admin", "operador", "leitura")]
            self.assertEqual(documented, actual, row)
        self.assertIn("só `system_prompt`/`ativo`", rows["`PUT /agentes/{id}`"][1])
        self.assertEqual(rbac.CAMPOS_OPERACIONAIS, {"system_prompt", "ativo"})
        self.assertIn("telefone mascarado", rows["`GET /sessoes`, `GET /sessoes/{telefone}`, `GET …/historico`"][2])
        self.assertIn("`numero_notificacao` mascarado",
                      rows["`GET /agentes`, `GET /agentes/{id}`, `GET /agentes/{id}/consumo`"][2])

    def test_effective_route_permissions_match_table(self) -> None:
        table = section(self.agents, "Permissões por rota", "### Agentes — interno")
        rows = {}
        for line in table.splitlines():
            if not line.startswith("| `"):
                continue
            cells = [c.strip() for c in line.split("|")[1:-1]]
            for method, path in re.findall(r"`(GET|POST|PUT|DELETE) ([^`]+)`", cells[0]):
                path = path.replace("{id}", "{agente_id}").replace("…", "/sessoes/{telefone}")
                rows[(method, path)] = cells[1:]
        actual = {}
        for route in main.app.routes:
            if not isinstance(route, APIRoute) or not route.path.startswith(("/agentes", "/sessoes")):
                continue
            declared = rbac.permissoes_da_rota(route)
            self.assertIsNotNone(declared, route.path)
            for method in route.methods - {"HEAD"}:
                key = (method, route.path)
                actual[key] = [
                    any(p in rbac.permissoes(role) for p in declared)
                    for role in ("admin", "operador", "leitura")
                ]
        self.assertEqual(set(rows), set(actual))
        for key in rows:
            self.assertEqual([c != "—" for c in rows[key]], actual[key], key)

    def test_cookie_attributes_match_document(self) -> None:
        doc = section(self.agents, "### 10.16 Autenticação", "## 11.")
        response = Response()
        auth._definir_cookie(response, sessao.SessaoEmitida(
            token="synthetic", csrf_token="synthetic", expira_em=datetime.now(),
            usuario_id=1, papel="leitura"))
        cookie = response.headers["set-cookie"]
        for attribute in ("HttpOnly", "Secure", "SameSite=Strict", "Path=/"):
            self.assertIn(attribute.lower(), cookie.lower())
            self.assertIn(attribute, doc)
        self.assertNotIn("domain=", cookie.lower())
        self.assertIn("sem Domain", doc)

    def test_internal_examples_pass_bearer_without_shell_expansion(self) -> None:
        for filename in ("AGENTS.md", "docs/durable-delivery-runbook.md", "docs/inbox-scheduler-runbook.md"):
            text = (ROOT / filename).read_text()
            examples = [block for block in re.findall(r"```bash\n(.*?)```", text, re.S)
                        if "httpx.get(" in block and re.search(r"/(?:agentes|sessoes)", block)]
            self.assertTrue(examples, filename)
            for block in examples:
                self.assertIn("set +x", block)
                self.assertIn("read -rs", block)
                self.assertIn("exec -T -e WPP_SERVICE_TOKEN app", block)
                self.assertIn('headers={"Authorization": "Bearer " + os.environ["WPP_SERVICE_TOKEN"]}', block)
                self.assertNotIn("$WPP_SERVICE_TOKEN", block)
                self.assertNotIn("${WPP_SERVICE_TOKEN}", block)
                self.assertIn("unset WPP_SERVICE_TOKEN", block)
                self.assertIn("http://127.0.0.1:5000/", block)

    def test_env_keys_in_templates_agents_and_settings(self) -> None:
        section_9_3 = section(self.agents, "### 9.3 Chaves de autenticação da API", "### FU-1")
        for source in (ROOT / ".env.example", ROOT / "app" / ".env.example"):
            text = source.read_text()
            for line in AUTH_ENV:
                self.assertEqual(text.count(line), 1, (source, line))
        for line in AUTH_ENV:
            self.assertIn(line, section_9_3)
            self.assertIn(line.split("=")[0].lower(), Settings.model_fields)

    def test_numbers_match_code(self) -> None:
        section_10_16 = section(self.agents, "### 10.16 Autenticação", "## 11.")
        self.assertEqual((contexto.SESSAO_OCIOSA.total_seconds(), contexto.SESSAO_ABSOLUTA.total_seconds()),
                         (1800, 43200))
        self.assertIn("30 min ocioso, 12 h absoluto", section_10_16)
        self.assertIn("30 min ocioso, 12 h absoluto",
                      section(self.agents, "### 9.3 Chaves", "### FU-1"))
        hasher = senha.HASHER
        self.assertIn(f"argon2id `m={hasher.memory_cost // 1024} MiB, t={hasher.time_cost}, p={hasher.parallelism}`", section_10_16)
        self.assertEqual(hasher.memory_cost, 32 * 1024)
        self.assertIn(f"pool dedicado de {senha.WORKERS} threads com fila de {senha.FILA_MAXIMA} "
                      f"e espera máxima de {senha.ESPERA_MAXIMA:.0f} s", section_10_16)
        self.assertIn(f"Teto de memória de hash: {senha.WORKERS * hasher.memory_cost // 1024} MiB", section_10_16)
        self.assertIn(f"TTL {origem.RESOLUCAO_TTL_SEGUNDOS:.0f} s", section_10_16)
        self.assertIn(f"{senha.SENHA_MIN}–{senha.SENHA_MAX} caracteres", section_10_16)
        self.assertIn(f"{limites.FALHAS_LIVRES} falhas livres, depois {limites.ATRASO_BASE.seconds} s dobrando "
                      f"até {limites.ATRASO_MAXIMO.seconds // 60} min", section_10_16)
        self.assertIn(f"a cada {limites.SINAL_FALHAS_CONTA} falhas seguidas", section_10_16)
        self.assertIn(f"{limites.TOTP_FALHAS_ATE_BLOQUEIO} códigos errados seguidos bloqueiam só o segundo fator "
                      f"da conta por 15 min, dobrando até 1 h", section_10_16)
        self.assertEqual([limites.bloqueio_totp(n).total_seconds() for n in (0, 1, 2, 9)], [900, 1800, 3600, 3600])
        self.assertIn(f"{limites.TENTATIVAS_POR_JANELA} tentativas/5 min", section_10_16)
        self.assertEqual(limites.JANELA_SEGUNDOS, 300)
        self.assertIn(f"janela ±{totp.JANELA}", section_10_16)
        self.assertIn(f"{sessao.TENTATIVAS_POR_DESAFIO} tentativas por desafio", section_10_16)
        self.assertIn(f"SHA-1/{totp.DIGITOS} dígitos/30 s", section_10_16)
        self.assertIn(f"`{contexto.CSRF_HEADER}`", section_10_16)
        self.assertIn(auditoria.mascarar_telefone("5511999990042"), section_10_16)
        self.assertIn(f"cookie `{contexto.SESSION_COOKIE}`", section_10_16)

    def test_dependency_count_and_baseline(self) -> None:
        direct = re.findall(r"^([\w-]+)(?:\[[^]]+\])?==", (ROOT / "app/requirements.in").read_text(), re.M)
        self.assertIn(f"fixa as {len(direct)} dependências diretas", self.agents)
        for name in ("argon2-cffi", "pyotp"):
            self.assertIn(name, direct)
            self.assertIn(f"`{name}==", self.agents)
        # Discovery only: no test execution, no recursive subprocess call.
        result = subprocess.run(
            [sys.executable, "-c",
             "import unittest; print(unittest.defaultTestLoader.discover("
             + repr(str(ROOT / "app/tests")) + ", top_level_dir="
             + repr(str(ROOT / "app")) + ").countTestCases())"],
            check=True, capture_output=True, text=True,
        )
        count = int(result.stdout.strip().splitlines()[-1])
        self.assertIn(f"Linha de base atual: **{count} testes OK**", self.agents)

    def test_scope_statements_current(self) -> None:
        self.assertNotIn("CRUD sem autenticação de usuário no app", self.agents)
        self.assertNotIn("Auth de operador não existe no app", self.agents)
        self.assertNotIn("sem autenticação de operador no app", self.readme)
        section_18 = section(self.agents, "## 18. Fora do Escopo Atual", "Testes automatizados")
        self.assertIn("`empresa_id`", section_18)
        self.assertIn("Expor `/auth`", section_18)
        for doc in ("arquitetura.md", "codigo.md", "dados.md", "operacao-e-validacao.md", "estado-e-roadmap.md"):
            text = (ROOT / "docs/mapa-do-projeto" / doc).read_text()
            self.assertNotIn("sem auth de operador", text, doc)
            self.assertNotIn("sem login próprio", text, doc)
        self.assertIn("auth_auditoria", (ROOT / "docs/mapa-do-projeto/dados.md").read_text())


class AuthDocsBehaviorTests(base.AuthApiMySQLTests):
    """Documented status/atomicity compared with actual ASGI + MySQL behavior."""

    def doc(self):
        return (ROOT / "AGENTS.md").read_text()

    async def test_login_audit_failure_aborts_result(self) -> None:
        doc = section(self.doc(), "- Auditoria:", "- CLI container-only")
        self.assertIn("falha de auditoria aborta a transação → 503, sem contador isolado", doc)
        self.assertIn("sem alterar a resposta", doc)
        self.assertEqual(auditoria.SIGNAL_THRESHOLD, 1)
        _, email = await self.user("operador")
        before = auditoria.falhas_registro()
        for password in (base.SENHA, "incorreta"):
            with patch.object(auditoria.auth_repo, "registrar_auditoria", AsyncMock(side_effect=SQLAlchemyError("synthetic"))):
                response = await self.client().post("/auth/login", json={"email": email, "senha": password})
            self.assertEqual(response.status_code, 503)
            self.assertEqual(auditoria.falhas_registro(), before)
        self.assertEqual(await self.count(base.AuthSessao, base.AuthSessao.estagio == "completa"), 0)
        self.assertEqual(await self.count(base.AuthTentativa), 0)
        with patch.object(auditoria.auth_repo, "registrar_auditoria", AsyncMock(side_effect=SQLAlchemyError("synthetic"))):
            response = await self.client().post("/auth/login", json={"email": "ausente@example.invalid", "senha": base.SENHA})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(auditoria.falhas_registro(), before + 1)

    async def test_login_status_contract(self) -> None:
        table = section(self.doc(), "### Autenticação — interno", "Permissões por rota")
        login_rows = [line for line in table.splitlines() if "| `/auth/login` |" in line]
        self.assertEqual(len(login_rows), 1)
        login_row = login_rows[0]
        for phrase in ("401 genérico", "corpo inválido → 422", "limite por origem → 429", "indisponibilidade → 503"):
            self.assertIn(phrase, login_row)
        self.assertIn("sem derivação", section(self.doc(), "- Login sem enumeração:", "- Origem:"))
        for document in (self.doc(), (ROOT / "README.md").read_text()):
            self.assertNotIn("sempre 401", document)
            for status in ("422", "429", "503"):
                self.assertIn(status, document)
        client = self.client()
        with patch.object(senha, "verificar", wraps=senha.verificar) as derive:
            for body in ({"email": "synthetic"}, {"email": None, "senha": "sentinel"},
                         {"email": "synthetic", "senha": 12}):
                response = await client.post("/auth/login", json=body)
                self.assertEqual(response.status_code, 422)
                for detail in response.json()["detail"]:
                    self.assertEqual(set(detail), {"campo", "tipo"})
                self.assertNotIn("sentinel", response.text)
            self.assertEqual(derive.call_count, 0)
            response = await client.post("/auth/login", json={"email": "absent@example.invalid", "senha": base.SENHA})
            self.assertEqual(response.status_code, 401)
            self.assertEqual(derive.call_count, 1)
        with patch.object(limites.LIMITE, "registrar", return_value=1):
            response = await client.post("/auth/login", json={"email": "synthetic", "senha": base.SENHA})
            self.assertEqual(response.status_code, 429)
        with patch.object(senha, "exigir_capacidade", side_effect=senha.HashOcupado):
            response = await client.post("/auth/login", json={"email": "synthetic", "senha": base.SENHA})
            self.assertEqual(response.status_code, 503)
        with patch.object(base.settings, "auth_password_pepper_keys", {}):
            response = await client.post("/auth/login", json={"email": "synthetic", "senha": base.SENHA})
            self.assertEqual(response.status_code, 503)
        with patch.object(sessao.auth_repo, "get_usuario_por_email", AsyncMock(side_effect=SQLAlchemyError("synthetic"))):
            response = await client.post("/auth/login", json={"email": "absent@example.invalid", "senha": base.SENHA})
            self.assertEqual(response.status_code, 503)


# Reuse fixtures, not inherited cases (also keeps discovery baseline unique).
for _name in dir(base.AuthApiMySQLTests):
    if _name.startswith("test_"):
        setattr(AuthDocsBehaviorTests, _name, None)


if __name__ == "__main__":
    unittest.main()
