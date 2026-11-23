"""AUTH docs contract (H): AGENTS.md, README, templates and code say the same."""

import os
import re
import unittest
from pathlib import Path

from fastapi.routing import APIRoute

from src import main
from src.config import Settings
from src.services.auth import auditoria, contexto, limites, rbac, senha, totp
from src.services.auth.rbac import Permissao as P


ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
AUTH_ENV = (
    "AUTH_PASSWORD_PEPPER_CURRENT_VERSION=1",
    "AUTH_PASSWORD_PEPPER_KEYS={}",
    "AUTH_TOTP_ENCRYPTION_CURRENT_VERSION=1",
    "AUTH_TOTP_ENCRYPTION_KEYS={}",
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
        hasher = senha.HASHER
        self.assertIn(f"argon2id `m=64 MiB, t={hasher.time_cost}, p={hasher.parallelism}`", section_10_16)
        self.assertEqual(hasher.memory_cost, 64 * 1024)
        self.assertIn(f"{senha.SENHA_MIN}–{senha.SENHA_MAX} caracteres", section_10_16)
        self.assertIn(f"{limites.FALHAS_ATE_BLOQUEIO} falhas (senha ou TOTP) → 15 min, dobrando até 24 h",
                      section_10_16)
        self.assertIn(f"{limites.TENTATIVAS_POR_JANELA} tentativas/5 min", section_10_16)
        self.assertEqual(limites.JANELA_SEGUNDOS, 300)
        self.assertIn(f"janela ±{totp.JANELA}", section_10_16)
        self.assertIn("5 tentativas por desafio", section_10_16)
        self.assertIn(auditoria.mascarar_telefone("5511999990042"), section_10_16)
        self.assertIn(f"cookie `{contexto.SESSION_COOKIE}`", section_10_16)

    def test_dependency_count_and_baseline(self) -> None:
        direct = re.findall(r"^([\w-]+)(?:\[[^]]+\])?==", (ROOT / "app/requirements.in").read_text(), re.M)
        self.assertIn(f"fixa as {len(direct)} dependências diretas", self.agents)
        for name in ("argon2-cffi", "pyotp"):
            self.assertIn(name, direct)
            self.assertIn(f"`{name}==", self.agents)
        self.assertRegex(self.agents, r"Linha de base atual: \*\*\d+ testes OK\*\*")

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


if __name__ == "__main__":
    unittest.main()
