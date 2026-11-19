import io
import os
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from loguru import logger

from src.services import chatwoot_client, evolution_client


class _Response:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.request = httpx.Request("GET", "http://chatwoot.test/contract")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = httpx.Response(
                self.status_code,
                request=self.request,
                text="sensitive provider response",
            )
            raise httpx.HTTPStatusError(
                "sensitive provider response",
                request=self.request,
                response=response,
            )

    def json(self) -> object:
        return self.payload


class _AsyncClientContext:
    def __init__(self, client: object) -> None:
        self.client = client

    async def __aenter__(self) -> object:
        return self.client

    async def __aexit__(self, *args: object) -> None:
        return None


def _definition() -> dict[str, object]:
    return {
        "id": 1,
        "attribute_key": chatwoot_client.DELIVERY_ATTRIBUTE_KEY,
        "attribute_model": chatwoot_client.DELIVERY_ATTRIBUTE_MODEL,
        "attribute_display_type": chatwoot_client.DELIVERY_ATTRIBUTE_DISPLAY_TYPE,
    }


def _conversation(conversation_id: int, delivery_key: str) -> dict[str, object]:
    return {
        "id": conversation_id,
        "account_id": chatwoot_client.settings.chatwoot_account_id,
        "status": "resolved" if conversation_id % 2 else "open",
        "custom_attributes": {
            chatwoot_client.DELIVERY_ATTRIBUTE_KEY: delivery_key,
        },
    }


class ProviderPinTests(unittest.TestCase):
    def test_provider_images_are_versioned_and_digest_pinned(self) -> None:
        configured_root = os.getenv("WPP_REPO_ROOT")
        candidates = []
        if configured_root:
            candidates.append(Path(configured_root))
        candidates.extend(Path(__file__).resolve().parents)
        candidates.extend(Path.cwd().resolve().parents)
        repository_root = next(
            (
                candidate
                for candidate in candidates
                if (candidate / "docker-compose.yml").is_file()
                and (candidate / "app" / "src").is_dir()
            ),
            None,
        )
        self.assertIsNotNone(
            repository_root,
            "repository root must be mounted or WPP_REPO_ROOT must be set",
        )
        compose = repository_root / "docker-compose.yml"
        compose_text = compose.read_text()
        self.assertIn(
            "evoapicloud/evolution-api:v2.3.7@sha256:"
            "966625532d9076a2381e973a271307d107e6f070450de3abeeea8bd18be07252",
            compose_text,
        )
        self.assertEqual(
            compose_text.count(
                "chatwoot/chatwoot:v4.17.1@sha256:"
                "18f280a63070073c7588164eed31303cc788123d899733e54aeec4c39d1d8dd9"
            ),
            2,
        )
        self.assertNotIn("evoapicloud/evolution-api:latest", compose_text)
        self.assertNotIn("chatwoot/chatwoot:latest", compose_text)


class EvolutionContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_webhook_configuration_uses_installed_nested_header_contract(self) -> None:
        response = _Response({"id": "configured"}, status_code=201)
        client = SimpleNamespace(post=AsyncMock(return_value=response))
        stream = io.StringIO()
        sink_id = logger.add(stream, format="{message}\n")
        try:
            with patch.object(
                evolution_client.httpx,
                "AsyncClient",
                return_value=_AsyncClientContext(client),
            ):
                result = await evolution_client.evolution_client.set_webhook(
                    "https://app.example/webhook?private=marker",
                    "instance-a",
                )
        finally:
            logger.remove(sink_id)

        self.assertEqual(result, {"id": "configured"})
        request_payload = client.post.await_args.kwargs["json"]
        self.assertEqual(
            request_payload,
            {
                "webhook": {
                    "enabled": True,
                    "url": "https://app.example/webhook?private=marker",
                    "headers": {
                        "Authorization": (
                            "Bearer "
                            + evolution_client.settings.evolution_webhook_bearer_token
                        )
                    },
                    "byEvents": False,
                    "base64": False,
                    "events": [
                        "MESSAGES_UPSERT",
                        "MESSAGES_UPDATE",
                        "CONNECTION_UPDATE",
                    ],
                }
            },
        )
        logs = stream.getvalue()
        self.assertNotIn(
            evolution_client.settings.evolution_webhook_bearer_token,
            logs,
        )
        self.assertNotIn("private=marker", logs)

    async def test_webhook_verification_checks_exact_installed_header(self) -> None:
        response = _Response(
            {
                "enabled": True,
                "url": "http://app:5000/webhook",
                "headers": {"Authorization": "Bearer expected-secret"},
                "events": [
                    "MESSAGES_UPSERT",
                    "MESSAGES_UPDATE",
                    "CONNECTION_UPDATE",
                ],
            }
        )
        client = SimpleNamespace(get=AsyncMock(return_value=response))
        with patch.object(
            evolution_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            await evolution_client.evolution_client.verify_webhook(
                "http://app:5000/webhook",
                "instance-a",
                bearer_token="expected-secret",
            )


class ChatwootContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_webhook_rerun_is_idempotent_and_secret_scoped(self) -> None:
        webhook = {
            "id": 9,
            "name": chatwoot_client.CHATWOOT_WEBHOOK_NAME,
            "url": "http://app:5000/chatwoot/webhook",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": [chatwoot_client.CHATWOOT_WEBHOOK_SUBSCRIPTION],
            "secret": "provider-generated-secret",
        }
        client = SimpleNamespace(
            get=AsyncMock(
                return_value=_Response({"payload": {"webhooks": [webhook]}})
            ),
            post=AsyncMock(),
            patch=AsyncMock(),
            delete=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            report = await chatwoot_client.provision_account_webhook(
                webhook["url"],
                configured_secret=webhook["secret"],
            )

        self.assertEqual(report.webhook_id, 9)
        self.assertFalse(report.created)
        self.assertFalse(report.updated)
        self.assertEqual(report.removed_obsolete, 0)
        client.post.assert_not_awaited()
        client.patch.assert_not_awaited()
        client.delete.assert_not_awaited()

    async def test_account_webhook_secret_mismatch_is_rejected(self) -> None:
        webhook = {
            "id": 9,
            "name": chatwoot_client.CHATWOOT_WEBHOOK_NAME,
            "url": "http://app:5000/chatwoot/webhook",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": [chatwoot_client.CHATWOOT_WEBHOOK_SUBSCRIPTION],
            "secret": "provider-generated-secret",
        }
        client = SimpleNamespace(
            get=AsyncMock(
                return_value=_Response({"payload": {"webhooks": [webhook]}})
            ),
            post=AsyncMock(),
            patch=AsyncMock(),
            delete=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            with self.assertRaises(chatwoot_client.ChatwootContractError):
                await chatwoot_client.provision_account_webhook(
                    webhook["url"],
                    configured_secret="wrong-secret",
                )

    async def test_account_webhook_url_rotation_updates_stable_id(self) -> None:
        old_webhook = {
            "id": 9,
            "name": chatwoot_client.CHATWOOT_WEBHOOK_NAME,
            "url": "https://old.example/chatwoot/webhook",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": [chatwoot_client.CHATWOOT_WEBHOOK_SUBSCRIPTION],
            "secret": "provider-generated-secret",
        }
        unrelated_webhook = {
            "id": 10,
            "name": "Operator audit webhook",
            "url": "https://operator.example/events",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": ["conversation_created"],
            "secret": "operator-secret",
        }
        new_url = "https://new.example/chatwoot/webhook"
        updated_webhook = {**old_webhook, "url": new_url}
        client = SimpleNamespace(
            get=AsyncMock(
                return_value=_Response(
                    {"payload": {"webhooks": [old_webhook, unrelated_webhook]}}
                )
            ),
            post=AsyncMock(),
            patch=AsyncMock(return_value=_Response(updated_webhook)),
            delete=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            report = await chatwoot_client.provision_account_webhook(
                new_url,
                configured_secret=old_webhook["secret"],
                managed_webhook_id=9,
            )

        self.assertEqual(report.webhook_id, 9)
        self.assertTrue(report.updated)
        self.assertFalse(report.created)
        self.assertEqual(report.removed_obsolete, 0)
        client.post.assert_not_awaited()
        client.delete.assert_not_awaited()
        self.assertEqual(
            client.patch.await_args.args[0],
            f"{chatwoot_client._base_url()}/webhooks/9",
        )
        self.assertEqual(
            client.patch.await_args.kwargs["json"]["webhook"]["url"],
            new_url,
        )

    async def test_stale_id_cannot_mutate_unrelated_operator_webhook(self) -> None:
        unrelated = {
            "id": 44,
            "name": "Operator payroll webhook",
            "url": "https://operator.example/payroll",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": ["conversation_created"],
            "secret": "operator-secret",
        }
        client = SimpleNamespace(
            get=AsyncMock(
                return_value=_Response({"payload": {"webhooks": [unrelated]}})
            ),
            post=AsyncMock(),
            patch=AsyncMock(),
            delete=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            with self.assertRaises(chatwoot_client.ChatwootContractError):
                await chatwoot_client.provision_account_webhook(
                    "https://new.example/chatwoot/webhook",
                    configured_secret="managed-secret",
                    managed_webhook_id=44,
                )

        client.post.assert_not_awaited()
        client.patch.assert_not_awaited()
        client.delete.assert_not_awaited()

    async def test_first_adoption_rejects_unrelated_same_name_contract(self) -> None:
        unrelated = {
            "id": 44,
            "name": chatwoot_client.CHATWOOT_WEBHOOK_NAME,
            "url": "https://operator.example/events",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": ["conversation_created"],
            "secret": "operator-secret",
        }
        client = SimpleNamespace(
            get=AsyncMock(
                return_value=_Response({"payload": {"webhooks": [unrelated]}})
            ),
            post=AsyncMock(),
            patch=AsyncMock(),
            delete=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            with self.assertRaises(chatwoot_client.ChatwootContractError):
                await chatwoot_client.provision_account_webhook(
                    "https://new.example/chatwoot/webhook",
                    configured_secret=None,
                )

        client.post.assert_not_awaited()
        client.patch.assert_not_awaited()
        client.delete.assert_not_awaited()

    async def test_first_adoption_rejects_ambiguous_managed_candidates(self) -> None:
        canonical = {
            "id": 9,
            "name": chatwoot_client.CHATWOOT_WEBHOOK_NAME,
            "url": "https://new.example/chatwoot/webhook",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": [chatwoot_client.CHATWOOT_WEBHOOK_SUBSCRIPTION],
            "secret": "provider-generated-secret",
        }
        obsolete = {
            **canonical,
            "id": 8,
            "url": "https://old.example/chatwoot/webhook",
        }
        unrelated = {
            "id": 10,
            "name": "Operator audit webhook",
            "url": "https://operator.example/events",
            "account_id": chatwoot_client.settings.chatwoot_account_id,
            "subscriptions": ["conversation_created"],
            "secret": "operator-secret",
        }
        client = SimpleNamespace(
            get=AsyncMock(
                return_value=_Response(
                    {"payload": {"webhooks": [obsolete, canonical, unrelated]}}
                )
            ),
            post=AsyncMock(),
            patch=AsyncMock(),
            delete=AsyncMock(return_value=_Response({}, status_code=200)),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            with self.assertRaises(chatwoot_client.ChatwootContractError):
                await chatwoot_client.provision_account_webhook(
                    canonical["url"],
                    configured_secret=canonical["secret"],
                )

        client.post.assert_not_awaited()
        client.patch.assert_not_awaited()
        client.delete.assert_not_awaited()

    async def test_exact_filter_paginates_beyond_first_page_and_all_statuses(self) -> None:
        delivery_key = "a2eb78de-1d35-43fb-b06b-2fdd2ffd3ec3"
        first_page = [_conversation(index, delivery_key) for index in range(1, 21)]
        second_page = [_conversation(21, delivery_key)]
        client = SimpleNamespace(
            post=AsyncMock(
                side_effect=(
                    _Response({"meta": {"all_count": 21}, "payload": first_page}),
                    _Response({"meta": {"all_count": 21}, "payload": second_page}),
                )
            )
        )

        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            matches = await chatwoot_client.filter_conversations_by_delivery_key(
                delivery_key
            )

        self.assertEqual([row["id"] for row in matches], list(range(1, 22)))
        self.assertEqual(
            [call.kwargs["params"]["page"] for call in client.post.await_args_list],
            [1, 2],
        )
        for call in client.post.await_args_list:
            condition = call.kwargs["json"]["payload"][0]
            self.assertEqual(condition["filter_operator"], "equal_to")
            self.assertEqual(condition["values"], [delivery_key])
            self.assertEqual(
                condition["custom_attribute_type"],
                "conversation_attribute",
            )
            self.assertNotIn("status", str(call.kwargs["json"]))

    async def test_contract_bootstraps_definition_and_accepts_unlocked_inbox(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=(
                    _Response([]),
                    _Response(
                        {"id": 7, "lock_to_single_conversation": False}
                    ),
                )
            ),
            post=AsyncMock(
                side_effect=(
                    _Response(_definition()),
                    _Response({"meta": {"all_count": 0}, "payload": []}),
                )
            ),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            report = await chatwoot_client.check_delivery_contract([7, 7])

        self.assertEqual(report.inbox_ids, (7,))
        self.assertTrue(report.attribute_created)
        create_payload = client.post.await_args_list[0].kwargs["json"]
        definition = create_payload["custom_attribute_definition"]
        self.assertEqual(definition["attribute_key"], "wpp_delivery_key")
        self.assertEqual(definition["attribute_display_type"], "text")

    async def test_locked_inbox_fails_before_filter_probe(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=(
                    _Response([_definition()]),
                    _Response({"id": 7, "lock_to_single_conversation": True}),
                )
            ),
            post=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            with self.assertRaises(chatwoot_client.ChatwootContractError) as raised:
                await chatwoot_client.check_delivery_contract([7])

        self.assertIn("locked", str(raised.exception))
        client.post.assert_not_awaited()

    async def test_worker_readiness_never_bootstraps_missing_definition(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(return_value=_Response([])),
            post=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            with self.assertRaises(chatwoot_client.ChatwootContractError):
                await chatwoot_client.check_delivery_contract(
                    [7],
                    bootstrap_attribute=False,
                )
        client.post.assert_not_awaited()

    async def test_permission_failure_is_sanitized(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(return_value=_Response({}, status_code=403)),
            post=AsyncMock(),
        )
        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            with self.assertRaises(chatwoot_client.ChatwootContractError) as raised:
                await chatwoot_client.check_delivery_contract([])

        error = str(raised.exception)
        self.assertIn("status=403", error)
        self.assertNotIn("sensitive provider response", error)
        self.assertNotIn("api_access_token", error)


if __name__ == "__main__":
    unittest.main()
