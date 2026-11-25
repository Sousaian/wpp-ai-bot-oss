"""Password definition policy and the exclusive password-writing boundaries."""

import ast
import os
import unittest
import unicodedata
from pathlib import Path

from src.services.auth import senha


ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))


class PasswordPolicyTests(unittest.TestCase):
    def test_boundaries_and_unicode_classes_after_nfc(self):
        accepted = [
            "Aa1!aaaa", "A1!" + "a" * 61, "É١!aaaaa",
            "A1😀aaaaa", "Password1!", "A1!A1!A1!", "A1!AAAAA",
            "E\u03011!aaaaa", "E\u03011!" + "a" * 61, "A1!a\u0301aaaa", "A1!aaa\u0001a",
        ]
        for value in accepted:
            with self.subTest(value=value):
                try:
                    normalized = senha.normalizar(value)
                except senha.SenhaRecusada as exc:
                    self.fail(f"valid synthetic definition refused: {exc.code}")
                self.assertEqual(normalized, unicodedata.normalize("NFC", value))
        refused = [
            ("Aa1!aaa", "senha_curta"),
            ("A1!" + "a" * 62, "senha_longa"),
            ("A!aaaaaa", "senha_sem_numero"),
            ("a1!aaaaa", "senha_sem_maiuscula"),
            ("A1aaaaaa", "senha_sem_especial"),
            ("A1 aaaaa", "senha_com_espaco"),
            ("中1!中中中中中", "senha_sem_maiuscula"),
            ("A²!aaaaa", "senha_sem_numero"),
            ("A1aaaaa\u0301a", "senha_sem_especial"),
            ("A1aaaa\u0001a", "senha_sem_especial"),
            ("E\u03011!aaaa", "senha_curta"),  # raw 8 → NFC 7
            ("A1!\u0000aaaa", "senha_invalida"),
            (None, "senha_invalida"),
            ("aaaaaaa", "senha_curta"),  # deterministic precedence
            ("a" * 65, "senha_longa"),
            ("aaaaaaaa", "senha_sem_numero"),
        ]
        for value, code in refused:
            with self.subTest(value=value):
                with self.assertRaises(senha.SenhaRecusada) as caught:
                    senha.normalizar(value)
                self.assertEqual(str(caught.exception), code)
                self.assertEqual(caught.exception.code, code)

    def test_all_unicode_whitespace_is_rejected_without_trimming(self):
        whitespace = ("\t\n\v\f\r " + "\x1c\x1d\x1e\x1f\x85\xa0\u1680"
                      + "\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
                      + "\u2028\u2029\u202f\u205f\u3000")
        for char in whitespace:
            for value in (char + "Aa1!aaaa", "Aa1!" + char + "aaaa", "Aa1!aaaa" + char):
                with self.subTest(codepoint=ord(char), value=value):
                    with self.assertRaises(senha.SenhaRecusada) as caught:
                        senha.normalizar(value)
                    self.assertEqual(caught.exception.code, "senha_com_espaco")


class PasswordBoundaryTests(unittest.TestCase):
    def test_only_definition_and_verified_rehash_write_password_hashes(self):
        writes, hash_calls = set(), set()
        for path in (ROOT / "app/src").rglob("*.py"):
            relative = path.relative_to(ROOT).as_posix()
            tree = ast.parse(path.read_text())
            # Walk with function ownership so moving a call elsewhere is detected.
            def walk(node, owner="<module>"):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    owner = node.name
                if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)) and not (
                    isinstance(node, ast.AnnAssign) and node.value is None
                ):
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    for target in targets:
                        for item in ast.walk(target):
                            if isinstance(item, (ast.Attribute, ast.Name)) and (
                                getattr(item, "attr", None) == "senha_hash"
                                or getattr(item, "id", None) == "senha_hash"
                            ):
                                writes.add((relative, owner))
                if isinstance(node, ast.Call):
                    if any(k.arg == "senha_hash" for k in node.keywords):
                        writes.add((relative, owner))
                    func = node.func
                    name = getattr(func, "attr", getattr(func, "id", ""))
                    if name in {"gerar_hash", "rehash_verificada", "_hash_sync", "hash"}:
                        hash_calls.add((relative, owner, ast.unparse(func)))
                for child in ast.iter_child_nodes(node):
                    walk(child, owner)
            walk(tree)
        self.assertEqual(writes, {
            ("app/src/database/models.py", "<module>"),
            ("app/src/services/auth/sessao.py", "apply"),
            ("app/src/database/repositories/auth_repo.py", "substituir_senha_se_inalterada"),
        })
        # Other non-password hash methods are not password hashers.
        password_calls = {c for c in hash_calls if c[2] != "hashlib.hash"}
        self.assertEqual(password_calls, {
            ("app/src/services/auth/sessao.py", "usar_token", "senha.gerar_hash"),
            ("app/src/services/auth/sessao.py", "_rehash", "senha.rehash_verificada"),
            ("app/src/services/auth/senha.py", "_dummy", "HASHER.hash"),
            ("app/src/services/auth/senha.py", "_hash_sync", "HASHER.hash"),
        })
        tree = ast.parse((ROOT / "app/src/services/auth/senha.py").read_text())
        definition = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "gerar_hash")
        self.assertTrue(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                            and n.func.id == "normalizar" for n in ast.walk(definition)))
