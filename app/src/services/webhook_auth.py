import hashlib
import hmac
import secrets
import time
from collections.abc import Mapping


EVOLUTION_AUTH_HEADER = "authorization"
CHATWOOT_TIMESTAMP_HEADER = "x-chatwoot-timestamp"
CHATWOOT_SIGNATURE_HEADER = "x-chatwoot-signature"


class WebhookAuthError(ValueError):
    """Webhook authentication failed without retaining credential material."""

    def __init__(self, code: str) -> None:
        super().__init__("invalid webhook authentication")
        self.code = code


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    direct = headers.get(name)
    if isinstance(direct, str):
        return direct

    normalized_name = name.casefold()
    for key, value in headers.items():
        if key.casefold() == normalized_name and isinstance(value, str):
            return value
    return None


def verify_evolution_bearer(
    headers: Mapping[str, str],
    expected_token: str,
) -> None:
    provided = _header_value(headers, EVOLUTION_AUTH_HEADER) or ""
    expected = f"Bearer {expected_token}"
    if not secrets.compare_digest(provided.encode(), expected.encode()):
        raise WebhookAuthError("invalid_evolution_bearer")


def verify_chatwoot_signature(
    *,
    headers: Mapping[str, str],
    raw_body: bytes,
    secret: str,
    max_age_seconds: int,
    now: int | None = None,
) -> None:
    timestamp = _header_value(headers, CHATWOOT_TIMESTAMP_HEADER)
    signature = _header_value(headers, CHATWOOT_SIGNATURE_HEADER)
    if timestamp is None or signature is None:
        raise WebhookAuthError("missing_chatwoot_signature")

    if not timestamp.isascii() or not timestamp.isdigit() or len(timestamp) > 12:
        raise WebhookAuthError("invalid_chatwoot_timestamp")

    timestamp_seconds = int(timestamp)
    current_seconds = int(time.time()) if now is None else now
    if abs(current_seconds - timestamp_seconds) > max_age_seconds:
        raise WebhookAuthError("stale_chatwoot_timestamp")

    signed_payload = timestamp.encode() + b"." + raw_body
    digest = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
    expected_signature = f"sha256={digest}"
    if not secrets.compare_digest(signature.encode(), expected_signature.encode()):
        raise WebhookAuthError("invalid_chatwoot_signature")
