"""Offline contracts for removal of the unused v3/MVP Python surface."""
import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1] / "src"
REMOVED = (
    "routers/atendentes.py",
    "database/repositories/atendente_repo.py",
    "services/session_manager.py",
    "services/notificador.py",
    "models/schemas.py",
)


class LegacyRemovalTests(unittest.TestCase):
    def test_removed_modules_absent(self):
        for relative in REMOVED:
            with self.subTest(module=relative):
                self.assertFalse((ROOT / relative).exists())

    def test_no_imports_of_removed_modules(self):
        modules = {"src." + name[:-3].replace("/", ".") for name in REMOVED}
        for path in ROOT.rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertNotIn(alias.name, modules, str(path))
                elif isinstance(node, ast.ImportFrom):
                    self.assertNotIn(node.module, modules, str(path))
                    for alias in node.names:
                        self.assertNotIn(f"{node.module}.{alias.name}", modules, str(path))

    def test_attendant_router_not_registered(self):
        source = (ROOT / "main.py").read_text()
        self.assertNotIn("atendentes_router", source)
        self.assertNotIn("atendentes_router", (ROOT / "routers/__init__.py").read_text())

    def test_session_closure_has_no_attendant_assignment(self):
        tree = ast.parse((ROOT / "database/repositories/sessao_repo.py").read_text())
        functions = {node.name: node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)}
        self.assertNotIn("set_handler", functions)
        for name in ("encerrar_locked", "encerrar_sessao"):
            args = functions[name].args
            self.assertNotIn("atendente_id", [arg.arg for arg in args.args + args.kwonlyargs])
