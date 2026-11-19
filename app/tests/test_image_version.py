"""Image identity matrix: no settings-only version can impersonate a baked build."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.utils import image_version as iv
from src.services import replica_compat


class ImageVersionTests(unittest.TestCase):
    def test_file_and_required_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "version"
            cases = [(None, "true", "v1", "missing"), ("", "true", "v1", "invalid"),
                     ("v1", "true", "v2", "mismatch"), ("v1", "true", "v1", None),
                     (None, "false", "v1", None), ("v1", "false", "v1", None)]
            for content, flag, configured, error in cases:
                with self.subTest(content=content, flag=flag, error=error), patch.object(iv, "VERSION_FILE", path), patch.dict(os.environ, WPP_REQUIRE_IMAGE_VERSION=flag):
                    iv._signals.clear()
                    if content is None:
                        path.unlink(missing_ok=True)
                    else:
                        path.write_text(content)
                    if error:
                        with self.assertRaises(SystemExit) as raised:
                            iv.require_runtime_version(configured)
                        self.assertEqual(raised.exception.code, 2)
                        self.assertEqual(iv.signal_counts()["image_version_" + ("file_" if error != "mismatch" else "") + error], 1)
                    else:
                        iv.require_runtime_version(configured)
                    if content is None and flag == "false":
                        self.assertEqual(iv.signal_counts()["image_version_source_fallback"], 1)
                    print("R2_IMAGE_CASE PASS")
            self.assertEqual(len(cases), 6)
            path.mkdir() if not path.exists() else None
            with patch.object(iv, "VERSION_FILE", Path(directory)), patch.dict(os.environ, WPP_REQUIRE_IMAGE_VERSION="true"):
                with self.assertRaises(SystemExit):
                    iv.require_runtime_version("v1")

    def test_fingerprint_uses_baked_not_environment_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "version"
            path.write_text("baked-v1\n")
            with patch.object(iv, "VERSION_FILE", path), patch.object(replica_compat.settings, "delivery_worker_code_version", "spoofed-v2"):
                self.assertEqual(replica_compat.local_fingerprint().code_version, "baked-v1")


    def test_dev_bind_warns_once_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            version = Path(directory) / "version"
            version.write_text("dev-v1")
            with patch.object(iv, "VERSION_FILE", version), patch.dict(os.environ, WPP_DEV_BIND_CODE="true"), patch.object(iv.logger, "warning") as warning:
                iv._signals.clear()
                iv.require_runtime_version("dev-v1")
                iv.require_runtime_version("dev-v1")
                self.assertEqual(iv.signal_counts()["dev_bind_code_unattested"], 1)
                self.assertTrue(any(call.args[1] == "dev_bind_code_unattested" for call in warning.call_args_list))

    def test_sha_version_lifespan_finishes_unittest(self) -> None:
        import subprocess
        import sys
        import src
        from src.config import settings
        version = iv.image_code_version(settings.delivery_worker_code_version)
        # Child imports the same src as this process, independent of inherited cwd/PYTHONPATH.
        import_root = str(Path(src.__file__).resolve().parents[1])
        # Explicit env, matching the baked file even for a SHA-tagged image.
        env = {"PATH": os.defpath, "PYTHONPATH": import_root, "DELIVERY_WORKER_CODE_VERSION": version,
               "OPENROUTER_API_KEY": "synthetic", "EVOLUTION_API_KEY": "synthetic",
               "EVOLUTION_API_URL": "http://evolution.invalid",
               "DATABASE_URL": "mysql+aiomysql://u:p@db.invalid/wpp", "WPP_REQUIRE_IMAGE_VERSION": "true"}
        # Source-only tests have no baked file; retain their documented fallback.
        if not iv.VERSION_FILE.exists():
            env["WPP_REQUIRE_IMAGE_VERSION"] = "false"
        code = """
import unittest
from unittest.mock import AsyncMock, patch
from src import main
class SHAStartup(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan(self):
        with patch.object(main, 'check_db_connection', AsyncMock(return_value=False)):
            async with main.lifespan(main.app):
                pass
unittest.main(verbosity=2)
"""
        result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertRegex(result.stderr, r"Ran 1 test in")
        self.assertIn("OK", result.stderr)
