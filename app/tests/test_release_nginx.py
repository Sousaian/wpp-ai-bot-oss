"""Exact path/method/canonical-upstream parity across edge and operator docs."""
import os
from pathlib import Path
import re
import unittest

ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
PUBLIC = {"/": "GET", "/health": "GET", "/webhook": "POST", "/chatwoot/webhook": "POST"}


class ReleaseNginxTests(unittest.TestCase):
    def test_exact_public_contract(self) -> None:
        doc = (ROOT / "AGENTS.md").read_text()
        for name, count in (("nginx/nginx.conf", 2), ("nginx/nginx.dev.conf", 1)):
            source = (ROOT / name).read_text()
            locations = re.findall(r"location = (\S+) \{(.*?)(?=\n        \})", source, re.S)
            self.assertEqual(len(locations), 4 * count, name)
            self.assertEqual({path for path, _ in locations}, set(PUBLIC))
            for path, body in locations:
                if PUBLIC[path] == "GET":
                    self.assertIn('if ($request_method !~ "^(GET|HEAD)$") { return 405; }', body)
                else:
                    self.assertIn('if ($request_method != POST) { return 405; }', body)
                if "proxy_pass" in body:
                    self.assertIn(f"proxy_pass         http://$app_upstream{path}$is_args$args;", body)
            self.assertEqual(source.count("location / { return 404; }"), count, name)
            self.assertIn("$privacy_reject_encoded_digit", source)
        for path, method in PUBLIC.items():
            self.assertIn(f"`{method} {path}`", doc)
        for name in ("docs/durable-delivery-runbook.md", "docs/inbox-scheduler-runbook.md"):
            source = (ROOT / name).read_text()
            for path, method in PUBLIC.items():
                self.assertIn(f"`{method} {path}`", source)
            self.assertIn("Do not publish port 5000", source)
            self.assertIn("release_build.sh", source)
