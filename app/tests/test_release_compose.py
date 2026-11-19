"""Parsed release/override invariants; image consumers cannot override code."""
import os
from pathlib import Path
import re
import unittest

import yaml

ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))
CODE_SERVICES = ("app", "delivery-worker", "turn-worker", "delivery-ops",
                 "provider-auth-bootstrap", "provider-auth-monitor")


def validate_base(source: str) -> None:
    config = yaml.safe_load(source)
    assert config["name"] == "${COMPOSE_PROJECT_NAME:?COMPOSE_PROJECT_NAME_required}", "project required"
    image = ("${COMPOSE_PROJECT_NAME:?COMPOSE_PROJECT_NAME_required}-app:"
             "${DELIVERY_WORKER_CODE_VERSION:?DELIVERY_WORKER_CODE_VERSION_required}")
    builders = []
    for name, service in config["services"].items():
        assert "container_name" not in service, "container_name forbidden"
        for mount in service.get("volumes", []):
            target = mount.get("target") if isinstance(mount, dict) else mount.split(":")[1] if ":" in mount else mount
            assert target != "/app", "runtime code mount forbidden"
        ref = service.get("image", "")
        if name in CODE_SERVICES:
            assert ref == image, "single versioned Python image required"
            assert service["environment"]["WPP_REQUIRE_IMAGE_VERSION"] == "true", "image version guard required"
            if "build" in service:
                builders.append(name)
        else:
            assert re.fullmatch(r"[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}", ref), "external image digest required"
    assert builders == ["app"], "one build context required"
    assert config["services"]["app"]["build"]["context"] == "./app"
    for network in config["networks"].values():
        assert "name" not in network, "global network name forbidden"
    for volume in config["volumes"].values():
        assert not volume or "name" not in volume, "global volume name forbidden"


class ReleaseComposeTests(unittest.TestCase):
    def test_release_base_contract(self) -> None:
        source = (ROOT / "docker-compose.yml").read_text()
        validate_base(source)
        for variable in ("NGINX_BIND_ADDRESS:-127.0.0.1", "NGINX_HTTP_PORT:-80", "NGINX_HTTPS_PORT:-443"):
            self.assertIn("${" + variable + "}", source)

    def test_dev_override_restores_only_code_binds(self) -> None:
        override = yaml.safe_load((ROOT / "docker-compose.dev.yml").read_text())
        self.assertEqual(set(override["services"]), set(CODE_SERVICES))
        for service in override["services"].values():
            self.assertEqual(service["volumes"], ["./app:/app"])
            self.assertEqual(service["environment"]["WPP_DEV_BIND_CODE"], "true")

    def test_removed_names_have_no_operational_dependents(self) -> None:
        script = (ROOT / "nginx/test/privacy_log_check.sh").read_text()
        self.assertIn("docker compose ps -q", script)
        self.assertNotRegex(script, r"\bwpp_(app|nginx|mysql|evolution|redis|postgres)\b")

    def test_mount_mutation_matrix(self) -> None:
        source = (ROOT / "docker-compose.yml").read_text()
        mounts = ["./app:/app", "./app:/app:ro", '"./app:/app"',
                  "${PWD}/app:/app", "$PWD/app:/app", "{type: bind, source: ./app, target: /app, read_only: true}"]
        for mount in mounts:
            with self.subTest(mount=mount):
                mutated = source.replace("      - app_logs:/app/logs", "      - " + mount, 1)
                with self.assertRaisesRegex(AssertionError, "runtime code mount forbidden"):
                    validate_base(mutated)
                print("R2_BIND_MUTATION PASS")
        self.assertEqual(len(mounts), 6)

    def test_image_scope_mutation_matrix(self) -> None:
        # R3 P2-B: a host-global tag lets another stack overwrite this one.
        source = (ROOT / "docker-compose.yml").read_text()
        scoped = "${COMPOSE_PROJECT_NAME:?COMPOSE_PROJECT_NAME_required}-app:"
        self.assertEqual(source.count(scoped), 1)
        refs = ["wpp-app:", "${COMPOSE_PROJECT_NAME}-app:", "${COMPOSE_PROJECT_NAME:-wpp}-app:",
                "app:", "${COMPOSE_PROJECT_NAME:?COMPOSE_PROJECT_NAME_required}-api:"]
        for ref in refs:
            with self.subTest(ref=ref):
                with self.assertRaisesRegex(AssertionError, "single versioned Python image required"):
                    validate_base(source.replace(scoped, ref, 1))
                print("R3_IMAGE_SCOPE_MUTATION PASS")
        self.assertEqual(len(refs), 5)
