import json
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path


class OpenRouterOnlyStartupTests(unittest.TestCase):
    FLOAT_SETTINGS = (
        "DELIVERY_CLAIM_RETRY_JITTER_SECONDS",
        "DELIVERY_POLL_INTERVAL_SECONDS",
        "INBOX_TURN_POLL_INTERVAL_SECONDS",
        "INBOX_GENERATION_TIMEOUT_SECONDS",
        "HEALTH_OBSERVABILITY_CACHE_SECONDS",
    )

    @staticmethod
    def _minimal_env() -> dict[str, str]:
        return {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            "OPENROUTER_API_KEY": "test-openrouter-key",
            "OPENROUTER_BASE_URL": "https://openrouter.example/v1",
            "EVOLUTION_API_URL": "http://evolution:8080",
            "EVOLUTION_API_KEY": "test-evolution-key",
            "DATABASE_URL": "mysql+aiomysql://user:password@db:3306/test",
            "CHATWOOT_WEBHOOK_ID": "",
        }

    def test_settings_load_without_legacy_openai_variables(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        minimal_env = self._minimal_env()

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from src.config import settings; "
                    "assert settings.openrouter_base_url == "
                    "'https://openrouter.example/v1'; "
                    "assert not hasattr(settings, 'openai_api_key'); "
                    "assert not hasattr(settings, 'openai_model'); "
                    "assert settings.evolution_webhook_bearer_token is None; "
                    "assert settings.chatwoot_webhook_secret is None; "
                    "assert settings.chatwoot_webhook_id is None; "
                    "assert settings.provider_webhook_auth_mode == 'provisioning'; "
                    "assert settings.delivery_mode == 'direct'; "
                    "assert settings.delivery_dispatch_enabled is False; "
                    "assert settings.delivery_cutover_epoch is None; "
                    "assert settings.inbox_scheduler_enabled is False; "
                    "assert settings.inbox_scheduler_epoch is None; "
                    "assert settings.delivery_claim_max_attempts == 4; "
                    "assert settings.delivery_shutdown_grace_seconds == 30; "
                    "assert settings.delivery_shutdown_abort_seconds == 5; "
                    "assert settings.delivery_maintenance_skip_error_threshold == 3; "
                    "assert settings.delivery_worker_health_path == "
                    "'/tmp/delivery-worker-health.json'; "
                    "rendered = repr(settings); "
                    "assert 'test-openrouter-key' not in rendered; "
                    "assert 'evolution_webhook_bearer_token' not in rendered; "
                    "assert 'chatwoot_webhook_secret' not in rendered"
                ),
            ],
            cwd=app_root / "tests",
            env=minimal_env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0)

    def test_every_float_setting_rejects_non_finite_values(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        for setting in self.FLOAT_SETTINGS:
            for value in ("nan", "inf", "-inf"):
                with self.subTest(setting=setting, value=value):
                    result = subprocess.run(
                        [sys.executable, "-c", "from src.config import settings"],
                        cwd=app_root / "tests",
                        env=self._minimal_env() | {setting: value},
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(setting.casefold(), result.stderr.casefold())
                    self.assertNotIn("test-openrouter-key", result.stderr)
                    self.assertNotIn("test-evolution-key", result.stderr)

        metadata = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from src.config import Settings; "
                    f"names={self.FLOAT_SETTINGS!r}; "
                    "assert all('allow_inf_nan=False' in "
                    "repr(Settings.model_fields[name.lower()].metadata) "
                    "for name in names)"
                ),
            ],
            cwd=app_root / "tests",
            env=self._minimal_env(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(metadata.returncode, 0, metadata.stderr)

    def test_turn_poll_interval_must_fit_before_lease_renewal(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        invalid = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=self._minimal_env()
            | {
                "INBOX_TURN_LEASE_SECONDS": "30",
                "INBOX_TURN_POLL_INTERVAL_SECONDS": "10",
            },
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(invalid.returncode, 0)
        self.assertIn(
            "inbox turn poll interval must be shorter than one third",
            invalid.stderr,
        )
        self.assertNotIn("test-openrouter-key", invalid.stderr)
        self.assertNotIn("test-evolution-key", invalid.stderr)

        valid = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=self._minimal_env()
            | {
                "INBOX_TURN_LEASE_SECONDS": "30",
                "INBOX_TURN_POLL_INTERVAL_SECONDS": "9.999",
            },
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stderr)

    def test_health_observability_cache_ttl_is_bounded_and_documented(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        repo_root = Path(os.environ.get("WPP_REPO_ROOT", app_root.parent))
        for value in ("4.999", "10.001", "nan", "inf", "-inf"):
            with self.subTest(value=value):
                result = subprocess.run(
                    [sys.executable, "-c", "from src.config import settings"],
                    cwd=app_root / "tests",
                    env=self._minimal_env()
                    | {"HEALTH_OBSERVABILITY_CACHE_SECONDS": value},
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("health_observability_cache_seconds", result.stderr)

        for source in (
            repo_root / ".env.example",
            repo_root / "app" / ".env.example",
            repo_root / "AGENTS.md",
        ):
            self.assertIn(
                "HEALTH_OBSERVABILITY_CACHE_SECONDS=5", source.read_text(), str(source)
            )

    def test_renewed_leases_need_not_exceed_total_operation_deadlines(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        epoch = "12345678-1234-5678-1234-567812345678"
        result = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=self._minimal_env()
            | {
                "PROVIDER_WEBHOOK_AUTH_MODE": "enforced",
                "DELIVERY_MODE": "outbox",
                "DELIVERY_DISPATCH_ENABLED": "true",
                "DELIVERY_CUTOVER_EPOCH": epoch,
                "INBOX_SCHEDULER_ENABLED": "true",
                "INBOX_SCHEDULER_EPOCH": epoch,
                "INBOX_TURN_PROCESSOR_ENABLED": "true",
                "INBOUND_RECEIPT_HMAC_CURRENT_VERSION": "1",
                "INBOUND_RECEIPT_HMAC_KEYS": '{"1":"test-secret"}',
                # Renewed leases deliberately remain below total deadlines.
                "INBOX_TURN_LEASE_SECONDS": "30",
                "INBOX_GENERATION_TIMEOUT_SECONDS": "45",
                "DELIVERY_CHATWOOT_LEASE_SECONDS": "120",
                "DELIVERY_CHATWOOT_DEADLINE_SECONDS": "300",
            },
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_worker_code_version_is_nonblank_bounded_safe_ascii(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        for value in ("", "   ", "versão-1", "version/1", "v" * 101):
            with self.subTest(value=value):
                result = subprocess.run(
                    [sys.executable, "-c", "from src.config import settings"],
                    cwd=app_root / "tests",
                    env=self._minimal_env()
                    | {"DELIVERY_WORKER_CODE_VERSION": value},
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("delivery_worker_code_version", result.stderr)
                self.assertNotIn("test-openrouter-key", result.stderr)

        valid = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from src.config import settings; "
                    "assert settings.delivery_worker_code_version == 'outbox.v1-rc_2'"
                ),
            ],
            cwd=app_root / "tests",
            env=self._minimal_env()
            | {"DELIVERY_WORKER_CODE_VERSION": "outbox.v1-rc_2"},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stderr)

    def test_local_snapshot_paths_must_be_absolute(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        for setting in (
            "DELIVERY_WORKER_HEALTH_PATH",
            "INBOX_TURN_WORKER_HEALTH_PATH",
            "PROVIDER_AUTH_READINESS_PATH",
        ):
            with self.subTest(setting=setting):
                result = subprocess.run(
                    [sys.executable, "-c", "from src.config import settings"],
                    cwd=app_root / "tests",
                    env=self._minimal_env() | {setting: "relative/status.json"},
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("local snapshot paths must be absolute", result.stderr)
                self.assertNotIn("test-openrouter-key", result.stderr)

    def test_inbox_scheduler_requires_complete_compatible_gate(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        invalid = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=self._minimal_env() | {"INBOX_SCHEDULER_ENABLED": "true"},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(invalid.returncode, 0)
        self.assertIn(
            "inbox scheduler requires enforced auth, outbox mode and both epochs",
            invalid.stderr,
        )

        epoch = "12345678-1234-5678-1234-567812345678"
        valid = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from src.config import settings; "
                    "assert settings.inbox_scheduler_enabled is True; "
                    f"assert settings.inbox_scheduler_epoch == '{epoch}'"
                ),
            ],
            cwd=app_root / "tests",
            env=self._minimal_env()
            | {
                "PROVIDER_WEBHOOK_AUTH_MODE": "enforced",
                "DELIVERY_MODE": "outbox",
                "DELIVERY_CUTOVER_EPOCH": epoch,
                "INBOX_SCHEDULER_ENABLED": "true",
                "INBOX_SCHEDULER_EPOCH": epoch,
                "INBOUND_RECEIPT_HMAC_CURRENT_VERSION": "1",
                "INBOUND_RECEIPT_HMAC_KEYS": '{"1":"test-secret"}',
            },
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stderr)

    def test_delivery_and_inbox_defaults_match_templates_and_agents_section_9(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        repo_root = Path(os.environ.get("WPP_REPO_ROOT", app_root.parent))
        defaults_result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import json; from src.config import Settings; "
                    "print(json.dumps({name.upper(): field.default for name, field "
                    "in Settings.model_fields.items() if name.startswith(('delivery_', "
                    "'inbox_')) or name == 'provider_auth_readiness_path'}, "
                    "sort_keys=True))"
                ),
            ],
            cwd=app_root / "tests",
            env=self._minimal_env(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(defaults_result.returncode, 0, defaults_result.stderr)
        config_defaults = json.loads(defaults_result.stdout)

        def canonical(value: object) -> str:
            if value is None:
                return ""
            if isinstance(value, bool):
                return str(value).lower()
            if isinstance(value, float) and value.is_integer():
                return str(int(value))
            return str(value)

        expected = {key: canonical(value) for key, value in config_defaults.items()}

        def parse_env(text: str) -> dict[str, str]:
            values: dict[str, str] = {}
            for line in text.splitlines():
                if not line or line.lstrip().startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.startswith(("DELIVERY_", "INBOX_")) or key == (
                    "PROVIDER_AUTH_READINESS_PATH"
                ):
                    values[key] = value
            return values

        sources = {
            str(repo_root / ".env.example"): parse_env(
                (repo_root / ".env.example").read_text()
            ),
            str(repo_root / "app" / ".env.example"): parse_env(
                (repo_root / "app" / ".env.example").read_text()
            ),
        }
        agents = (repo_root / "AGENTS.md").read_text()
        section_9 = agents.split("## 9. Variáveis de Ambiente", 1)[1].split(
            "### 9.1", 1
        )[0]
        sources["AGENTS.md §9"] = parse_env(section_9)

        self.assertEqual(len(expected), 39)
        for source, values in sources.items():
            self.assertEqual(values, expected, source)

    def test_scheduler_db_cap_defaults_and_bounds_match_agents_section_9(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        repo_root = Path(os.environ.get("WPP_REPO_ROOT", app_root.parent))
        sql = (repo_root / "db" / "init.sql").read_text()
        table = sql.split("CREATE TABLE IF NOT EXISTS scheduler_runtime_state (", 1)[
            1
        ].split("\n);", 1)[0]
        cap_names = (
            "max_pending_inputs_per_session",
            "max_pending_input_bytes_per_session",
            "max_queued_turns_per_agent",
            "max_queued_bytes_per_agent",
            "max_global_queued_turns",
            "max_global_queued_bytes",
            "max_generation_attempts_per_turn",
            "max_delivery_attempts_per_turn",
            "max_refreshes_per_turn",
            "accounting_drain_seconds",
        )
        db_contract: dict[str, tuple[int, str]] = {}
        for name in cap_names:
            default_match = re.search(
                rf"^\s*{name}\s+[^\n]*\bDEFAULT\s+(\d+)", table, re.MULTILINE
            )
            self.assertIsNotNone(default_match, name)
            range_match = re.search(
                rf"\b{name}\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)", table
            )
            exact_match = re.search(rf"\b{name}\s*=\s*(\d+)", table)
            self.assertTrue(range_match or exact_match, name)
            bound = (
                f"{range_match.group(1)}..{range_match.group(2)}"
                if range_match
                else f"={exact_match.group(1)}"
            )
            db_contract[name] = (int(default_match.group(1)), bound)

        # Existing-volume upgrades add the three late caps in a separate ALTER.
        # Its defaults and bounds are part of the same contract as fresh CREATE.
        additive = sql.split(
            "ADD COLUMN max_global_queued_turns INT UNSIGNED NOT NULL DEFAULT 10000",
            1,
        )[1].split("    END IF;", 1)[0]
        for name in (
            "max_global_queued_turns",
            "max_global_queued_bytes",
            "accounting_drain_seconds",
        ):
            default_match = re.search(
                rf"(?:ADD COLUMN\s+)?{name}\s+[^\n]*\bDEFAULT\s+(\d+)",
                ("max_global_queued_turns INT UNSIGNED NOT NULL DEFAULT 10000" + additive),
            )
            range_match = re.search(
                rf"\b{name}\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)", additive
            )
            self.assertIsNotNone(default_match, f"additive default {name}")
            self.assertIsNotNone(range_match, f"additive bound {name}")
            self.assertEqual(
                (int(default_match.group(1)), f"{range_match.group(1)}..{range_match.group(2)}"),
                db_contract[name],
                f"additive cap parity {name}",
            )

        agents = (repo_root / "AGENTS.md").read_text()
        documented = {
            name: (int(default), bounds)
            for name, default, bounds in re.findall(
                r"^\| `([a-z_]+)` \| `(\d+)` \| `([^`]+)` \|$",
                agents,
                re.MULTILINE,
            )
            if name in cap_names
        }
        self.assertEqual(len(db_contract), 10)
        self.assertEqual(documented, db_contract)

    def test_delivery_shutdown_budget_must_fit_compose_grace(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        invalid_env = self._minimal_env() | {
            "DELIVERY_SHUTDOWN_GRACE_SECONDS": "35",
            "DELIVERY_SHUTDOWN_ABORT_SECONDS": "5",
        }
        result = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=invalid_env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "delivery drain plus abort cleanup must remain below",
            result.stderr,
        )

    def test_delivery_maintenance_skip_threshold_is_bounded(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        for value in ("0", "101"):
            result = subprocess.run(
                [sys.executable, "-c", "from src.config import settings"],
                cwd=app_root / "tests",
                env=self._minimal_env()
                | {"DELIVERY_MAINTENANCE_SKIP_ERROR_THRESHOLD": value},
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("delivery_maintenance_skip_error_threshold", result.stderr)

    def test_versioned_receipt_keys_are_secret_safe_and_require_current_version(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        valid_env = self._minimal_env() | {
            "INBOUND_RECEIPT_HMAC_CURRENT_VERSION": "2",
            "INBOUND_RECEIPT_HMAC_KEYS": '{"1":"old-secret","2":"new-secret"}',
            "PROVIDER_WEBHOOK_AUTH_MODE": "enforced",
        }
        valid = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from src.config import settings; "
                    "assert sorted(settings.inbound_receipt_hmac_keys) == [1, 2]; "
                    "rendered = repr(settings); "
                    "assert 'old-secret' not in rendered; "
                    "assert 'new-secret' not in rendered"
                ),
            ],
            cwd=app_root / "tests",
            env=valid_env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stderr)

        invalid_env = valid_env | {"INBOUND_RECEIPT_HMAC_CURRENT_VERSION": "3"}
        invalid = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=invalid_env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(invalid.returncode, 0)
        self.assertIn(
            "current inbound receipt HMAC key version is unavailable",
            invalid.stderr,
        )
        self.assertNotIn("old-secret", invalid.stderr)
        self.assertNotIn("new-secret", invalid.stderr)

    def test_receipt_keyring_mode_matrix_and_blank_rejection(self) -> None:
        app_root = Path(__file__).resolve().parents[1]

        for mode_env in (
            {"PROVIDER_WEBHOOK_AUTH_MODE": "enforced"},
            {"DELIVERY_MODE": "outbox"},
        ):
            missing = subprocess.run(
                [sys.executable, "-c", "from src.config import settings"],
                cwd=app_root / "tests",
                env=self._minimal_env() | mode_env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("receipt HMAC keyring is required", missing.stderr)

        provisioning_direct = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=self._minimal_env(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(provisioning_direct.returncode, 0)

        blank = subprocess.run(
            [sys.executable, "-c", "from src.config import settings"],
            cwd=app_root / "tests",
            env=self._minimal_env()
            | {"INBOUND_RECEIPT_HMAC_KEYS": '{"1":"   "}'},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(blank.returncode, 0)
        self.assertIn("receipt HMAC keyring is invalid", blank.stderr)

        for placeholder in (
            "gere-um-segredo-independente",
            "replace-with-an-independent-secret",
        ):
            public_key = subprocess.run(
                [sys.executable, "-c", "from src.config import settings"],
                cwd=app_root / "tests",
                env=self._minimal_env()
                | {"INBOUND_RECEIPT_HMAC_KEYS": f'{{"1":"{placeholder}"}}'},
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(public_key.returncode, 0)
            self.assertIn("receipt HMAC keyring is invalid", public_key.stderr)
            self.assertNotIn(placeholder, public_key.stderr)


if __name__ == "__main__":
    unittest.main()
