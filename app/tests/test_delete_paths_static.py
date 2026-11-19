"""S3 class guard: only ``lifecycle_repo`` may delete ``agentes``/``sessoes``.

Those deletes CASCADE into usage/handoff evidence (``consumo_llm``,
``llm_usage_reconciliation``, ``handoff_attempts``); the lifecycle guards are
the only thing preserving it, so any other delete path is a regression.
"""
import ast
from pathlib import Path
import re
import unittest


SRC = Path(__file__).resolve().parents[1] / "src"
ALLOWED = SRC / "database" / "repositories" / "lifecycle_repo.py"
MODELS = {"Agente", "Sessao"}
INSTANCE_NAME = re.compile(r"agent|agente|sessao|session", re.IGNORECASE)
SQL_DELETE = re.compile(r"\bDELETE\s+(?:\w+\s+)*FROM\s+`?(?:agentes|sessoes)`?\b", re.IGNORECASE)
SQL_TRUNCATE = re.compile(r"\bTRUNCATE\s+(?:TABLE\s+)?`?(?:agentes|sessoes)`?\b", re.IGNORECASE)


def _name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def violations(source: str, label: str) -> list[str]:
    found: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _name(node.func) == "delete":
            func = node.func
            # sqlalchemy delete(Agente) / delete(Sessao)
            if any(_name(arg) in MODELS for arg in node.args):
                found.append(f"{label}:{node.lineno} delete(model)")
            # Agente.__table__.delete()
            elif (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Attribute)
                    and func.value.attr == "__table__" and _name(func.value.value) in MODELS):
                found.append(f"{label}:{node.lineno} __table__.delete()")
            # db.delete(agente) / session.delete(sessao)
            elif (isinstance(func, ast.Attribute) and len(node.args) == 1
                    and (_name(node.args[0]) or "") and INSTANCE_NAME.search(_name(node.args[0]))):
                found.append(f"{label}:{node.lineno} orm delete({_name(node.args[0])})")
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if SQL_DELETE.search(node.value) or SQL_TRUNCATE.search(node.value):
                found.append(f"{label}:{node.lineno} sql delete")
    return found


class DeletePathsStaticTests(unittest.TestCase):
    def test_no_agent_or_session_delete_outside_lifecycle_repo(self) -> None:
        files = sorted(SRC.rglob("*.py"))
        self.assertGreater(len(files), 30)
        offending: list[str] = []
        for path in files:
            if path == ALLOWED:
                continue
            offending += violations(path.read_text(), str(path.relative_to(SRC)))
        self.assertEqual(offending, [], "delete agentes/sessoes only via lifecycle_repo (S3)")

    def test_scanner_detects_every_form(self) -> None:
        # Positive control: the allowed module itself is detected (two paths).
        allowed = violations(ALLOWED.read_text(), "lifecycle_repo")
        self.assertEqual(len([v for v in allowed if "orm delete" in v]), 2, allowed)
        cases = {
            "await db.delete(agente)": 1,
            "await session.delete(sessao)": 1,
            "await db.delete(row_session)": 1,
            "await db.execute(delete(Agente).where(Agente.id == 1))": 1,
            "await db.execute(sa.delete(Sessao))": 1,
            "await db.execute(Sessao.__table__.delete())": 1,
            "await db.execute(text('DELETE FROM agentes WHERE id=1'))": 1,
            "q = '''delete  from `sessoes` where id=1'''": 1,
            "q = 'DELETE LOW_PRIORITY FROM sessoes'": 1,
            "q = 'TRUNCATE TABLE agentes'": 1,
            # Not agent/session deletes:
            "await db.delete(atendente)": 0,
            "await db.execute(delete(OutboxDelivery))": 0,
            "q = 'DELETE FROM sessoes_archive'": 0,
            "q = 'DELETE FROM outbox_deliveries'": 0,
        }
        for snippet, expected in cases.items():
            with self.subTest(snippet=snippet):
                self.assertEqual(len(violations(snippet, "case")), expected)
        self.assertEqual(len(cases), 14)


if __name__ == "__main__":
    unittest.main()
