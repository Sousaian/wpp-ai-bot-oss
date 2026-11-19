import hashlib
import hmac
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException, Response

from src import main
from src.routers import chatwoot as chatwoot_router
from src.services.webhook_auth import (
    WebhookAuthError,
    verify_chatwoot_signature,
    verify_evolution_bearer,
)


def _chatwoot_headers(raw_body: bytes, *, timestamp: int) -> dict[str, str]:
    timestamp_text = str(timestamp)
    digest = hmac.new(
        chatwoot_router.settings.chatwoot_webhook_secret.encode(),
        timestamp_text.encode() + b"." + raw_body,
        hashlib.sha256,
    ).hexdigest()
    return {
        "X-Chatwoot-Timestamp": timestamp_text,
        "X-Chatwoot-Signature": f"sha256={digest}",
    }


class WebhookAuthPrimitiveTests(unittest.TestCase):
    def test_evolution_bearer_is_exact_and_constant_time_compatible(self) -> None:
        verify_evolution_bearer(
            {"Authorization": "Bearer expected-token"},
            "expected-token",
        )
        for supplied in ("", "expected-token", "Bearer wrong-token"):
            with self.subTest(supplied=supplied):
                with self.assertRaises(WebhookAuthError):
                    verify_evolution_bearer(
                        {"Authorization": supplied},
                        "expected-token",
                    )

    def test_chatwoot_signature_covers_timestamp_and_exact_raw_body(self) -> None:
        now = 1_800_000_000
        raw_body = b'{"event":"message_created","content":"exact bytes"}'
        headers = _chatwoot_headers(raw_body, timestamp=now)

        verify_chatwoot_signature(
            headers=headers,
            raw_body=raw_body,
            secret=chatwoot_router.settings.chatwoot_webhook_secret,
            max_age_seconds=300,
            now=now,
        )
        with self.assertRaises(WebhookAuthError):
            verify_chatwoot_signature(
                headers=headers,
                raw_body=raw_body + b" ",
                secret=chatwoot_router.settings.chatwoot_webhook_secret,
                max_age_seconds=300,
                now=now,
            )

    def test_chatwoot_rejects_stale_and_future_timestamps(self) -> None:
        now = 1_800_000_000
        raw_body = b"{}"
        for timestamp in (now - 301, now + 301):
            with self.subTest(timestamp=timestamp):
                with self.assertRaises(WebhookAuthError):
                    verify_chatwoot_signature(
                        headers=_chatwoot_headers(raw_body, timestamp=timestamp),
                        raw_body=raw_body,
                        secret=chatwoot_router.settings.chatwoot_webhook_secret,
                        max_age_seconds=300,
                        now=now,
                    )


class WebhookAuthRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_evolution_rejects_before_json_parse_or_database_lookup(self) -> None:
        request = SimpleNamespace(
            headers={"Authorization": "Bearer wrong-token"},
            json=AsyncMock(),
        )
        with (
            patch.object(main.settings, "provider_webhook_auth_mode", "enforced"),
            patch.object(
                main.agente_repo,
                "get_by_instance_name",
                AsyncMock(),
            ) as lookup,
        ):
            with self.assertRaises(HTTPException) as raised:
                await main.webhook_handler(request, SimpleNamespace())

        self.assertEqual(raised.exception.status_code, 401)
        request.json.assert_not_awaited()
        lookup.assert_not_awaited()

    async def test_chatwoot_rejects_before_json_parse_or_database_lookup(self) -> None:
        raw_body = b'{"event":"message_created"}'
        request = SimpleNamespace(body=AsyncMock(return_value=raw_body), headers={})
        with (
            patch.object(
                chatwoot_router.settings,
                "provider_webhook_auth_mode",
                "enforced",
            ),
            patch.object(chatwoot_router.chatwoot_orchestrator, "orchestrate", AsyncMock()) as lookup,
        ):
            with self.assertRaises(HTTPException) as raised:
                await chatwoot_router.chatwoot_webhook(request, Response(), SimpleNamespace())

        self.assertEqual(raised.exception.status_code, 401)
        lookup.assert_not_awaited()

    async def test_body_account_cannot_replace_configured_scope(self) -> None:
        configured_account_id = chatwoot_router.settings.chatwoot_account_id
        self.assertIsNotNone(configured_account_id)
        payload = {
            "event": "message_created",
            "account": {"id": configured_account_id + 1},
        }
        raw_body = json.dumps(payload, separators=(",", ":")).encode()
        now = int(time.time())
        request = SimpleNamespace(
            body=AsyncMock(return_value=raw_body),
            headers=_chatwoot_headers(raw_body, timestamp=now),
        )
        with (
            patch.object(
                chatwoot_router.settings,
                "provider_webhook_auth_mode",
                "enforced",
            ),
            patch.object(chatwoot_router.chatwoot_orchestrator, "orchestrate", AsyncMock()) as lookup,
        ):
            with self.assertRaises(HTTPException) as raised:
                await chatwoot_router.chatwoot_webhook(request, Response(), SimpleNamespace())

        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(
            chatwoot_router.trusted_account_scope(),
            str(configured_account_id),
        )
        lookup.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
