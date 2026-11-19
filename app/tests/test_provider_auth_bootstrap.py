import contextlib
import asyncio
import io
import json
import os
from pathlib import Path
import signal
import shutil
import sys
import textwrap
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from pydantic import ValidationError
from pydantic import SecretStr
from fastapi import Response

from src import main, provider_auth_cli
from src.config import Settings
from src.routers import chatwoot as chatwoot_router
from src.services import chatwoot_client, provider_auth_bootstrap
from src.services.provider_auth_bootstrap import (
    CHATWOOT_SECRET_KEY,
    EVOLUTION_SECRET_KEY,
    ProvisionReport,
    RECEIPT_CURRENT_VERSION_KEY,
    RECEIPT_KEYS_KEY,
    ReadinessReport,
    active_provider_auth_readiness,
    provider_auth_readiness,
    provision_provider_auth,
    read_env_value,
    reset_provider_auth_snapshot_cache,
    update_env_file,
    write_provider_auth_snapshot,
)


class EnvironmentBootstrapTests(unittest.TestCase):
    def test_secret_file_update_is_idempotent_private_and_silent(self) -> None:
        evolution_secret = "evolution-secret-marker"
        chatwoot_secret = "chatwoot-secret-marker"
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text("OTHER=value\nEVOLUTION_WEBHOOK_BEARER_TOKEN=\n")
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                for _ in range(2):
                    update_env_file(
                        env_file,
                        {
                            EVOLUTION_SECRET_KEY: evolution_secret,
                            CHATWOOT_SECRET_KEY: chatwoot_secret,
                        },
                    )

            self.assertEqual(output.getvalue(), "")
            self.assertEqual(read_env_value(env_file, EVOLUTION_SECRET_KEY), evolution_secret)
            self.assertEqual(read_env_value(env_file, CHATWOOT_SECRET_KEY), chatwoot_secret)
            self.assertEqual(env_file.read_text().count(f"{EVOLUTION_SECRET_KEY}="), 1)
            self.assertEqual(env_file.read_text().count(f"{CHATWOOT_SECRET_KEY}="), 1)
            self.assertEqual(env_file.stat().st_mode & 0o777, 0o600)

    def test_prepare_material_keeps_retained_receipt_keys_secret(self) -> None:
        with (
            patch.object(
                provider_auth_bootstrap.settings,
                "inbound_receipt_hmac_current_version",
                2,
            ),
            patch.object(
                provider_auth_bootstrap.settings,
                "inbound_receipt_hmac_keys",
                {1: SecretStr("old-key"), 2: SecretStr("new-key")},
            ),
        ):
            current, serialized = provider_auth_bootstrap.serialized_receipt_keyring()

        self.assertEqual(current, "2")
        self.assertEqual(json.loads(serialized), {"1": "old-key", "2": "new-key"})
        self.assertNotIn("old-key", repr(provider_auth_bootstrap.settings))
        self.assertNotIn("new-key", repr(provider_auth_bootstrap.settings))

    def test_initial_receipt_key_is_independent_from_provider_bearer(self) -> None:
        with (
            patch.object(
                provider_auth_bootstrap.settings,
                "inbound_receipt_hmac_current_version",
                1,
            ),
            patch.object(
                provider_auth_bootstrap.settings,
                "inbound_receipt_hmac_keys",
                {},
            ),
            patch.object(
                provider_auth_bootstrap,
                "generate_receipt_secret",
                return_value="independent-receipt-key",
            ),
        ):
            current, serialized = provider_auth_bootstrap.serialized_receipt_keyring()

        self.assertEqual(current, "1")
        self.assertEqual(json.loads(serialized), {"1": "independent-receipt-key"})
        self.assertNotEqual(
            json.loads(serialized)["1"],
            "evolution-secret-marker",
        )

    def test_target_env_retains_all_receipt_key_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                f"{RECEIPT_CURRENT_VERSION_KEY}=2\n"
                f'{RECEIPT_KEYS_KEY}={{"1":"old","2":"current"}}\n'
            )
            with (
                patch.object(
                    provider_auth_bootstrap.settings,
                    "inbound_receipt_hmac_keys",
                    {},
                ),
                patch.object(
                    provider_auth_bootstrap,
                    "generate_receipt_secret",
                    side_effect=AssertionError("genuine keyring was rotated"),
                ),
            ):
                current, serialized = (
                    provider_auth_bootstrap.serialized_receipt_keyring(env_file)
                )
                repeated = provider_auth_bootstrap.serialized_receipt_keyring(env_file)

        self.assertEqual(current, "2")
        self.assertEqual(json.loads(serialized), {"1": "old", "2": "current"})
        self.assertEqual(repeated, (current, serialized))

    def test_target_env_public_placeholder_fails_without_provider_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                f"{RECEIPT_CURRENT_VERSION_KEY}=1\n"
                f'{RECEIPT_KEYS_KEY}={{"1":"replace-with-an-independent-secret"}}\n'
            )
            with self.assertRaisesRegex(
                provider_auth_bootstrap.ProviderAuthBootstrapError,
                "Receipt HMAC keyring is invalid",
            ):
                provider_auth_bootstrap.serialized_receipt_keyring(env_file)


class ProviderPrepareTemplateTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_real_templates_generate_private_stable_receipt_key(self) -> None:
        repo_root = Path(
            os.getenv("WPP_REPO_ROOT", Path(__file__).resolve().parents[2])
        )
        templates = (repo_root / ".env.example", repo_root / "app/.env.example")
        self.assertTrue(all(path.is_file() for path in templates))

        class FakeSession:
            async def __aenter__(self):
                return SimpleNamespace()

            async def __aexit__(self, *_args):
                return None

        report = ProvisionReport(
            evolution_instances=1,
            chatwoot_webhook_created=True,
            chatwoot_webhook_updated=False,
            chatwoot_webhook_id=9,
            chatwoot_obsolete_removed=0,
            chatwoot_secret_replaced=False,
            delivery_attribute_created=True,
        )
        bearer = "template-test-evolution-bearer"

        for template in templates:
            with self.subTest(template=str(template)), tempfile.TemporaryDirectory() as directory:
                env_file = Path(directory) / ".env"
                shutil.copyfile(template, env_file)
                output = io.StringIO()
                with (
                    patch.object(
                        provider_auth_cli.settings,
                        "evolution_webhook_bearer_token",
                        bearer,
                    ),
                    patch.object(
                        provider_auth_cli,
                        "AsyncSessionLocal",
                        return_value=FakeSession(),
                    ),
                    patch.object(
                        provider_auth_cli,
                        "provision_provider_auth",
                        AsyncMock(return_value=(report, "private-chatwoot-secret")),
                    ),
                    patch.object(
                        provider_auth_bootstrap,
                        "generate_receipt_secret",
                        return_value="generated-private-receipt-key",
                    ) as generate,
                    contextlib.redirect_stdout(output),
                    contextlib.redirect_stderr(output),
                ):
                    await provider_auth_cli._prepare(env_file)

                first = read_env_value(env_file, RECEIPT_KEYS_KEY)
                self.assertIsNotNone(first)
                first_keys = json.loads(first)
                self.assertEqual(first_keys, {"1": "generated-private-receipt-key"})
                self.assertNotEqual(first_keys["1"], bearer)
                self.assertNotIn(
                    first_keys["1"],
                    provider_auth_bootstrap.INBOUND_RECEIPT_HMAC_PUBLIC_PLACEHOLDERS,
                )
                generate.assert_called_once_with()

                with (
                    patch.object(
                        provider_auth_cli.settings,
                        "evolution_webhook_bearer_token",
                        bearer,
                    ),
                    patch.object(
                        provider_auth_cli,
                        "AsyncSessionLocal",
                        return_value=FakeSession(),
                    ),
                    patch.object(
                        provider_auth_cli,
                        "provision_provider_auth",
                        AsyncMock(return_value=(report, "private-chatwoot-secret")),
                    ),
                    patch.object(
                        provider_auth_bootstrap,
                        "generate_receipt_secret",
                        side_effect=AssertionError("prepare rotated generated key"),
                    ),
                    contextlib.redirect_stdout(output),
                    contextlib.redirect_stderr(output),
                ):
                    await provider_auth_cli._prepare(env_file)

                self.assertEqual(read_env_value(env_file, RECEIPT_KEYS_KEY), first)
                rendered = output.getvalue()
                self.assertNotIn("generated-private-receipt-key", rendered)
                self.assertNotIn(bearer, rendered)
                self.assertNotIn("private-chatwoot-secret", rendered)

    async def test_prepare_rejects_legacy_public_placeholders_before_provider_io(self) -> None:
        for placeholder in (
            "gere-um-segredo-independente",
            "replace-with-an-independent-secret",
        ):
            with self.subTest(placeholder=placeholder), tempfile.TemporaryDirectory() as directory:
                env_file = Path(directory) / ".env"
                env_file.write_text(
                    f"{RECEIPT_CURRENT_VERSION_KEY}=1\n"
                    f'{RECEIPT_KEYS_KEY}={{"1":"{placeholder}"}}\n'
                )
                provision = AsyncMock()
                with (
                    patch.object(
                        provider_auth_cli,
                        "provision_provider_auth",
                        provision,
                    ),
                    self.assertRaisesRegex(
                        provider_auth_bootstrap.ProviderAuthBootstrapError,
                        "Receipt HMAC keyring is invalid",
                    ) as raised,
                ):
                    await provider_auth_cli._prepare(env_file)

                provision.assert_not_awaited()
                self.assertNotIn(placeholder, str(raised.exception))


class ProviderProvisioningTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.receipt_keys_patch = patch.object(
            main.settings,
            "inbound_receipt_hmac_keys",
            {1: SecretStr("receipt-key")},
        )
        self.receipt_keys_patch.start()

    def tearDown(self) -> None:
        self.receipt_keys_patch.stop()

    async def test_provisioning_is_repeatable_for_every_active_agent(self) -> None:
        agents = [
            SimpleNamespace(
                ativo=True,
                instance_name="instance-a",
                chatwoot_inbox_id=7,
            ),
            SimpleNamespace(
                ativo=True,
                instance_name="instance-b",
                chatwoot_inbox_id=8,
            ),
            SimpleNamespace(
                ativo=False,
                instance_name="inactive",
                chatwoot_inbox_id=9,
            ),
        ]
        webhook_report = chatwoot_client.ChatwootWebhookReport(
            webhook_id=11,
            created=False,
            updated=False,
            removed_obsolete=0,
            secret="chatwoot-secret",
        )
        delivery_report = chatwoot_client.ChatwootContractReport(
            account_id=1,
            inbox_ids=(7, 8),
            attribute_created=False,
        )
        with (
            patch.object(main.settings, "provider_webhook_auth_mode", "provisioning"),
            patch.object(
                main.settings,
                "provider_callback_base_url",
                "http://app:5000",
            ),
            patch(
                "src.services.provider_auth_bootstrap.agente_repo.get_all",
                AsyncMock(return_value=agents),
            ),
            patch(
                "src.services.provider_auth_bootstrap.evolution_client.set_webhook",
                AsyncMock(),
            ) as set_webhook,
            patch(
                "src.services.provider_auth_bootstrap.evolution_client.verify_webhook",
                AsyncMock(),
            ) as verify_webhook,
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.provision_account_webhook",
                AsyncMock(return_value=webhook_report),
            ) as provision_chatwoot,
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.check_delivery_contract",
                AsyncMock(return_value=delivery_report),
            ) as delivery_contract,
        ):
            for _ in range(2):
                report, secret = await provision_provider_auth(
                    SimpleNamespace(),
                    evolution_secret="evolution-secret",
                    configured_chatwoot_secret="chatwoot-secret",
                    managed_chatwoot_webhook_id=None,
                )

        self.assertEqual(report.evolution_instances, 2)
        self.assertEqual(secret, "chatwoot-secret")
        self.assertEqual(set_webhook.await_count, 4)
        self.assertEqual(verify_webhook.await_count, 4)
        self.assertEqual(provision_chatwoot.await_count, 2)
        self.assertEqual(delivery_contract.await_count, 2)
        self.assertEqual(
            delivery_contract.await_args.kwargs,
            {"bootstrap_attribute": True},
        )
        self.assertEqual(delivery_contract.await_args.args[0], {7, 8})

    async def test_readiness_is_sanitized_and_reports_mismatch(self) -> None:
        agents = [
            SimpleNamespace(
                ativo=True,
                instance_name="private-instance-name",
                chatwoot_inbox_id=7,
            )
        ]
        with (
            patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
            patch.object(main.settings, "evolution_webhook_bearer_token", "secret-a"),
            patch.object(main.settings, "chatwoot_webhook_secret", "secret-b"),
            patch.object(
                main.settings,
                "provider_callback_base_url",
                "http://app:5000",
            ),
            patch(
                "src.services.provider_auth_bootstrap.agente_repo.get_all",
                AsyncMock(return_value=agents),
            ),
            patch(
                "src.services.provider_auth_bootstrap.evolution_client.verify_webhook",
                AsyncMock(side_effect=RuntimeError("provider body secret marker")),
            ),
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.verify_account_webhook",
                AsyncMock(),
            ),
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.check_delivery_contract",
                AsyncMock(
                    return_value=chatwoot_client.ChatwootContractReport(1, (7,), False)
                ),
            ),
        ):
            report = await active_provider_auth_readiness(SimpleNamespace())

        rendered = str(report.as_dict())
        self.assertFalse(report.ready)
        self.assertEqual(report.issues, ("evolution_webhook_mismatch",))
        self.assertNotIn("private-instance-name", rendered)
        self.assertNotIn("provider body secret marker", rendered)
        self.assertNotIn("secret-a", rendered)
        self.assertNotIn("secret-b", rendered)

    async def test_readiness_succeeds_after_exact_provider_verification(self) -> None:
        agents = [SimpleNamespace(ativo=True, instance_name="a", chatwoot_inbox_id=7)]
        with (
            patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
            patch.object(main.settings, "evolution_webhook_bearer_token", "secret-a"),
            patch.object(main.settings, "chatwoot_webhook_secret", "secret-b"),
            patch.object(
                main.settings,
                "provider_callback_base_url",
                "http://app:5000",
            ),
            patch(
                "src.services.provider_auth_bootstrap.agente_repo.get_all",
                AsyncMock(return_value=agents),
            ),
            patch(
                "src.services.provider_auth_bootstrap.evolution_client.verify_webhook",
                AsyncMock(),
            ),
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.verify_account_webhook",
                AsyncMock(return_value=11),
            ),
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.check_delivery_contract",
                AsyncMock(
                    return_value=chatwoot_client.ChatwootContractReport(1, (7,), False)
                ),
            ),
        ):
            report = await active_provider_auth_readiness(SimpleNamespace())

        self.assertTrue(report.ready)
        self.assertEqual(report.issues, ())

    async def test_readiness_fails_closed_without_receipt_keys(self) -> None:
        with (
            patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
            patch.object(main.settings, "inbound_receipt_hmac_keys", {}),
            patch.object(main.settings, "evolution_webhook_bearer_token", "secret-a"),
            patch.object(main.settings, "chatwoot_webhook_secret", "secret-b"),
            patch.object(main.settings, "provider_callback_base_url", "http://app:5000"),
            patch(
                "src.services.provider_auth_bootstrap.agente_repo.get_all",
                AsyncMock(return_value=[]),
            ),
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.verify_account_webhook",
                AsyncMock(),
            ),
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.check_delivery_contract",
                AsyncMock(),
            ),
        ):
            report = await active_provider_auth_readiness(SimpleNamespace())

        self.assertFalse(report.ready)
        self.assertEqual(report.issues, ("receipt_hmac_keyring_invalid",))


class ProviderReadinessSnapshotTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        reset_provider_auth_snapshot_cache()
        self.receipt_keys_patch = patch.object(
            main.settings,
            "inbound_receipt_hmac_keys",
            {1: SecretStr("receipt-key")},
        )
        self.receipt_keys_patch.start()

    def tearDown(self) -> None:
        self.receipt_keys_patch.stop()
        reset_provider_auth_snapshot_cache()

    async def test_concurrent_health_reads_one_snapshot_without_provider_fanout(self) -> None:
        ready = ReadinessReport(True, "enforced", 2, 2, ())
        with tempfile.TemporaryDirectory() as directory:
            snapshot_path = str(Path(directory) / "provider-auth.json")
            with (
                patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
                patch.object(main.settings, "evolution_webhook_bearer_token", "secret-a"),
                patch.object(main.settings, "chatwoot_webhook_secret", "secret-b"),
                patch.object(main.settings, "chatwoot_account_id", 1),
                patch.object(main.settings, "chatwoot_webhook_id", 9),
                patch.object(
                    main.settings,
                    "provider_callback_base_url",
                    "http://app:5000",
                ),
                patch.object(
                    main.settings,
                    "provider_auth_readiness_path",
                    snapshot_path,
                ),
                patch.object(
                    main.settings,
                    "provider_auth_readiness_cache_seconds",
                    5,
                ),
            ):
                write_provider_auth_snapshot(ready, checked_at=1000)
                with (
                    patch(
                        "src.services.provider_auth_bootstrap._load_provider_auth_snapshot",
                        wraps=provider_auth_bootstrap._load_provider_auth_snapshot,
                    ) as load_snapshot,
                    patch(
                        "src.services.provider_auth_bootstrap.agente_repo.get_all",
                        AsyncMock(),
                    ) as agent_lookup,
                    patch(
                        "src.services.provider_auth_bootstrap.evolution_client.verify_webhook",
                        AsyncMock(),
                    ) as evolution_probe,
                    patch(
                        "src.services.provider_auth_bootstrap.chatwoot_client.verify_account_webhook",
                        AsyncMock(),
                    ) as chatwoot_probe,
                ):
                    reports = await asyncio.gather(
                        *(
                            provider_auth_readiness(
                                now_epoch=1001,
                                now_monotonic=1,
                            )
                            for _ in range(25)
                        )
                    )

                self.assertTrue(all(report.ready for report in reports))
                self.assertEqual(load_snapshot.call_count, 1)
                agent_lookup.assert_not_awaited()
                evolution_probe.assert_not_awaited()
                chatwoot_probe.assert_not_awaited()

    async def test_cache_is_bounded_and_refresh_failure_is_safe(self) -> None:
        ready = ReadinessReport(True, "enforced", 1, 1, ())
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "provider-auth.json"
            with (
                patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
                patch.object(main.settings, "evolution_webhook_bearer_token", "secret-a"),
                patch.object(main.settings, "chatwoot_webhook_secret", "secret-b"),
                patch.object(main.settings, "chatwoot_account_id", 1),
                patch.object(main.settings, "chatwoot_webhook_id", 9),
                patch.object(
                    main.settings,
                    "provider_callback_base_url",
                    "http://app:5000",
                ),
                patch.object(main.settings, "provider_auth_readiness_path", str(snapshot)),
                patch.object(main.settings, "provider_auth_readiness_cache_seconds", 5),
                patch.object(
                    main.settings,
                    "provider_auth_readiness_max_age_seconds",
                    300,
                ),
            ):
                write_provider_auth_snapshot(ready, checked_at=1000)
                first = await provider_auth_readiness(
                    now_epoch=1001, now_monotonic=10
                )
                snapshot.write_text("not-json", encoding="utf-8")
                cached = await provider_auth_readiness(
                    now_epoch=1002, now_monotonic=14
                )
                refreshed = await provider_auth_readiness(
                    now_epoch=1007, now_monotonic=16
                )

                self.assertTrue(first.ready)
                self.assertTrue(cached.ready)
                self.assertFalse(refreshed.ready)
                self.assertEqual(refreshed.issues, ("readiness_snapshot_invalid",))

                write_provider_auth_snapshot(ready, checked_at=1000)
                reset_provider_auth_snapshot_cache()
                expired = await provider_auth_readiness(
                    now_epoch=1301, now_monotonic=20
                )
                self.assertFalse(expired.ready)
                self.assertEqual(expired.issues, ("readiness_snapshot_expired",))

    async def test_snapshot_is_bound_to_secret_safe_receipt_keyring_fingerprint(self) -> None:
        ready = ReadinessReport(True, "enforced", 1, 1, ())
        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "provider-auth.json"
            with (
                patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
                patch.object(main.settings, "provider_auth_readiness_path", str(snapshot)),
                patch.object(main.settings, "provider_auth_readiness_cache_seconds", 1),
                patch.object(
                    main.settings,
                    "inbound_receipt_hmac_keys",
                    {1: SecretStr("receipt-key-before-rotation")},
                ),
            ):
                write_provider_auth_snapshot(ready, checked_at=1000)
                serialized = snapshot.read_text()
                self.assertNotIn("receipt-key-before-rotation", serialized)
                with patch.object(
                    main.settings,
                    "inbound_receipt_hmac_keys",
                    {1: SecretStr("receipt-key-after-rotation")},
                ):
                    reset_provider_auth_snapshot_cache()
                    report = await provider_auth_readiness(
                        now_epoch=1001,
                        now_monotonic=1001,
                    )

        self.assertFalse(report.ready)
        self.assertEqual(
            report.issues,
            ("readiness_snapshot_configuration_changed",),
        )

    async def test_public_health_route_only_reads_snapshot(self) -> None:
        ready = ReadinessReport(True, "enforced", 3, 2, ())
        response = main.Response()
        with (
            patch.object(
                main,
                "provider_auth_readiness",
                AsyncMock(return_value=ready),
            ) as snapshot_read,
            patch(
                "src.services.provider_auth_bootstrap.agente_repo.get_all",
                AsyncMock(),
            ) as agent_lookup,
            patch(
                "src.services.provider_auth_bootstrap.evolution_client.verify_webhook",
                AsyncMock(),
            ) as evolution_probe,
            patch(
                "src.services.provider_auth_bootstrap.chatwoot_client.verify_account_webhook",
                AsyncMock(),
            ) as chatwoot_probe,
        ):
            results = await asyncio.gather(
                *(main.provider_auth_health(response) for _ in range(20))
            )

        self.assertTrue(all(result["ready"] for result in results))
        self.assertEqual(snapshot_read.await_count, 20)
        agent_lookup.assert_not_awaited()
        evolution_probe.assert_not_awaited()
        chatwoot_probe.assert_not_awaited()


class ProviderAuthMonitorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        reset_provider_auth_snapshot_cache()
        self.receipt_keys_patch = patch.object(
            main.settings,
            "inbound_receipt_hmac_keys",
            {1: SecretStr("receipt-key")},
        )
        self.receipt_keys_patch.start()

    def tearDown(self) -> None:
        self.receipt_keys_patch.stop()
        reset_provider_auth_snapshot_cache()

    async def test_monitor_slow_success_schedule_accounts_for_duration(
        self,
    ) -> None:
        ready = ReadinessReport(True, "enforced", 1, 1, ())
        clock = [1000.0]
        starts: list[float] = []
        writes: list[float] = []
        delays: list[float] = []
        with tempfile.TemporaryDirectory() as directory:
            snapshot = str(Path(directory) / "provider-auth.json")
            with (
                patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
                patch.object(main.settings, "evolution_webhook_bearer_token", "secret-a"),
                patch.object(main.settings, "chatwoot_webhook_secret", "secret-b"),
                patch.object(main.settings, "chatwoot_account_id", 1),
                patch.object(main.settings, "chatwoot_webhook_id", 9),
                patch.object(
                    main.settings,
                    "provider_callback_base_url",
                    "http://app:5000",
                ),
                patch.object(main.settings, "provider_auth_readiness_path", snapshot),
                patch.object(
                    main.settings,
                    "provider_auth_readiness_max_age_seconds",
                    13,
                ),
                patch.object(
                    main.settings,
                    "provider_auth_monitor_interval_seconds",
                    4,
                ),
                patch.object(
                    main.settings,
                    "provider_auth_monitor_audit_timeout_seconds",
                    7,
                ),
                patch.object(
                    main.settings,
                    "provider_auth_monitor_retry_initial_seconds",
                    1,
                ),
                patch.object(
                    main.settings,
                    "provider_auth_monitor_retry_max_seconds",
                    2,
                ),
                patch.object(
                    main.settings,
                    "provider_auth_monitor_shutdown_timeout_seconds",
                    1,
                ),
            ):
                async def check() -> bool:
                    starts.append(clock[0])
                    clock[0] += 7
                    writes.append(clock[0])
                    write_provider_auth_snapshot(ready, checked_at=clock[0])
                    return True

                async def wait(delay: float) -> bool:
                    delays.append(delay)
                    clock[0] += delay
                    return len(delays) == 4

                await provider_auth_cli._monitor_loop(
                    check,
                    wait,
                    lambda _issue: None,
                    monotonic=lambda: clock[0],
                )
                reset_provider_auth_snapshot_cache()
                report = await provider_auth_readiness(
                    now_epoch=clock[0],
                    now_monotonic=clock[0],
                )

        self.assertEqual(
            starts,
            [1000.0, 1007.0, 1014.0, 1021.0],
        )
        self.assertEqual(delays, [0.0, 0.0, 0.0, 0.0])
        self.assertTrue(
            all(later - earlier < 13 for earlier, later in zip(writes, writes[1:]))
        )
        self.assertGreater(clock[0] - starts[0], 26)
        self.assertTrue(report.ready)

    def test_settings_reject_review_4_7_10_timing(self) -> None:
        with self.assertRaisesRegex(
            ValidationError,
            "cadence plus audit and shutdown timeouts must be shorter",
        ):
            Settings(
                openrouter_api_key="test-key",
                evolution_api_url="http://evolution.test",
                evolution_api_key="test-key",
                database_url="mysql+aiomysql://user:pass@db/test",
                provider_auth_readiness_max_age_seconds=10,
                provider_auth_monitor_interval_seconds=4,
                provider_auth_monitor_audit_timeout_seconds=7,
                provider_auth_monitor_retry_initial_seconds=1,
                provider_auth_monitor_retry_max_seconds=2,
                provider_auth_monitor_shutdown_timeout_seconds=1,
            )

    async def test_monitor_failure_backoff_is_bounded_and_recovers(self) -> None:
        attempts = 0
        delays: list[float] = []
        failure_issues: list[str] = []
        with (
            patch.object(
                provider_auth_cli.settings,
                "provider_auth_monitor_interval_seconds",
                4,
            ),
            patch.object(
                provider_auth_cli.settings,
                "provider_auth_monitor_retry_initial_seconds",
                2,
            ),
            patch.object(
                provider_auth_cli.settings,
                "provider_auth_monitor_retry_max_seconds",
                5,
            ),
        ):
            async def check() -> bool:
                nonlocal attempts
                attempts += 1
                if attempts <= 3:
                    raise RuntimeError("private provider failure")
                return True

            def on_failure(issue: str) -> None:
                failure_issues.append(issue)

            async def wait(delay: float) -> bool:
                delays.append(delay)
                return len(delays) == 4

            await provider_auth_cli._monitor_loop(
                check,
                wait,
                on_failure,
                monotonic=lambda: 0.0,
            )

        self.assertEqual(attempts, 4)
        self.assertEqual(
            failure_issues,
            ["provider_auth_monitor_failed"] * 3,
        )
        self.assertEqual(delays, [2.0, 4.0, 5.0, 4.0])

    async def test_audit_deadline_publishes_sanitized_failure(self) -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()
        stop_event = asyncio.Event()
        secret_marker = "provider-private-deadline-detail"

        async def blocked_audit() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        async def check() -> bool:
            return await provider_auth_cli._run_monitor_audit(
                blocked_audit,
                stop_event,
                audit_timeout=0.02,
                shutdown_timeout=0.5,
            )

        async def stop_after_failure(_delay: float) -> bool:
            return True

        with tempfile.TemporaryDirectory() as directory:
            snapshot = Path(directory) / "provider-auth.json"
            with (
                patch.object(
                    provider_auth_cli.settings,
                    "provider_auth_readiness_path",
                    str(snapshot),
                ),
                patch.object(
                    provider_auth_cli.settings,
                    "provider_webhook_auth_mode",
                    "enforced",
                ),
                patch.object(
                    provider_auth_cli.settings,
                    "evolution_webhook_bearer_token",
                    secret_marker,
                ),
                patch.object(
                    provider_auth_cli.settings,
                    "chatwoot_webhook_secret",
                    "chatwoot-secret",
                ),
            ):
                await provider_auth_cli._monitor_loop(
                    check,
                    stop_after_failure,
                    provider_auth_cli._write_monitor_failure_snapshot,
                )
                reset_provider_auth_snapshot_cache()
                report = await provider_auth_readiness()
                serialized = snapshot.read_text()

        self.assertTrue(started.is_set())
        self.assertTrue(cancelled.is_set())
        self.assertFalse(report.ready)
        self.assertEqual(
            report.issues,
            ("provider_auth_audit_deadline_exceeded",),
        )
        self.assertNotIn(secret_marker, serialized)
        self.assertNotIn("TimeoutError", serialized)

    async def test_shutdown_cancels_and_awaits_blocked_audit(self) -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()
        blocker = asyncio.Event()
        stop_event = asyncio.Event()

        async def blocked_audit() -> None:
            started.set()
            try:
                await blocker.wait()
            finally:
                cancelled.set()

        task = asyncio.create_task(
            provider_auth_cli._run_monitor_audit(
                blocked_audit,
                stop_event,
                audit_timeout=30,
                shutdown_timeout=1,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        stop_event.set()

        result = await asyncio.wait_for(task, timeout=1)

        self.assertFalse(result)
        self.assertTrue(cancelled.is_set())
        self.assertTrue(task.done())
        self.assertFalse(
            any(
                candidate.get_name() == "provider-auth-audit"
                for candidate in asyncio.all_tasks()
                if not candidate.done()
            )
        )

    async def test_real_sigterm_interrupts_blocked_monitor(self) -> None:
        app_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            started = Path(directory) / "started"
            cancelled = Path(directory) / "cancelled"
            program = textwrap.dedent(
                f"""
                import asyncio
                from pathlib import Path
                from src import provider_auth_cli

                started = Path({str(started)!r})
                cancelled = Path({str(cancelled)!r})

                async def blocked_verify(**_kwargs):
                    started.write_text("started")
                    try:
                        await asyncio.Event().wait()
                    finally:
                        cancelled.write_text("cancelled")

                provider_auth_cli._verify = blocked_verify
                asyncio.run(provider_auth_cli._monitor())
                """
            )
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                program,
                cwd=app_root,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                for _ in range(100):
                    if started.exists() or process.returncode is not None:
                        break
                    await asyncio.sleep(0.02)
                self.assertTrue(started.exists())
                process.send_signal(signal.SIGTERM)
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(), timeout=3
                )
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            cancelled_observed = cancelled.exists()

        self.assertEqual(process.returncode, 0, stderr.decode())
        self.assertTrue(cancelled_observed)
        self.assertIn('"status": "monitor_stopped"', stdout.decode())


class StagedRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_provisioning_mode_preserves_evolution_direct_callback(self) -> None:
        request = SimpleNamespace(
            headers={},
            json=AsyncMock(return_value={"event": "provider.auth.probe"}),
        )
        with patch.object(main.settings, "provider_webhook_auth_mode", "provisioning"):
            result = await main.webhook_handler(request, SimpleNamespace())
        self.assertEqual(result["reason"], "unsupported_event")

    async def test_provisioning_mode_preserves_chatwoot_direct_callback(self) -> None:
        request = SimpleNamespace(
            headers={},
            body=AsyncMock(
                return_value=(
                    b'{"event":"provider.auth.probe","account":{"id":1}}'
                )
            ),
        )
        with (
            patch.object(
                chatwoot_router.settings,
                "provider_webhook_auth_mode",
                "provisioning",
            ),
            patch.object(chatwoot_router.settings, "chatwoot_account_id", 1),
        ):
            result = await chatwoot_router.chatwoot_webhook(
                request, Response(), SimpleNamespace()
            )
        self.assertEqual(result["reason"], "unsupported_event")


if __name__ == "__main__":
    unittest.main()
