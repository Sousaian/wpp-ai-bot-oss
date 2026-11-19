import asyncio
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from pathlib import Path
import secrets
import tempfile
import threading
import time
from collections.abc import Mapping
from urllib.parse import urlsplit

import httpx
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import INBOUND_RECEIPT_HMAC_PUBLIC_PLACEHOLDERS, settings
from src.database.repositories import agente_repo
from src.services import chatwoot_client
from src.services.evolution_client import evolution_client


EVOLUTION_SECRET_KEY = "EVOLUTION_WEBHOOK_BEARER_TOKEN"
CHATWOOT_SECRET_KEY = "CHATWOOT_WEBHOOK_SECRET"
AUTH_MODE_KEY = "PROVIDER_WEBHOOK_AUTH_MODE"
CHATWOOT_WEBHOOK_ID_KEY = "CHATWOOT_WEBHOOK_ID"
RECEIPT_CURRENT_VERSION_KEY = "INBOUND_RECEIPT_HMAC_CURRENT_VERSION"
RECEIPT_KEYS_KEY = "INBOUND_RECEIPT_HMAC_KEYS"
SNAPSHOT_SCHEMA_VERSION = 1
MAX_SNAPSHOT_BYTES = 16 * 1024


class ProviderAuthBootstrapError(RuntimeError):
    """Sanitized operator-facing provider authentication failure."""


@dataclass(frozen=True)
class ProvisionReport:
    evolution_instances: int
    chatwoot_webhook_created: bool
    chatwoot_webhook_updated: bool
    chatwoot_webhook_id: int
    chatwoot_obsolete_removed: int
    chatwoot_secret_replaced: bool
    delivery_attribute_created: bool


@dataclass(frozen=True)
class ReadinessReport:
    ready: bool
    auth_mode: str
    evolution_instances: int
    chatwoot_inboxes: int
    issues: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "auth_mode": self.auth_mode,
            "evolution_instances": self.evolution_instances,
            "chatwoot_inboxes": self.chatwoot_inboxes,
            "issues": list(self.issues),
        }


_snapshot_cache_guard = threading.Lock()
_snapshot_cache_deadline = 0.0
_snapshot_cache_key: tuple[str, str] | None = None
_snapshot_cache_report: ReadinessReport | None = None


def _not_ready(issue: str) -> ReadinessReport:
    return ReadinessReport(
        ready=False,
        auth_mode=settings.provider_webhook_auth_mode,
        evolution_instances=0,
        chatwoot_inboxes=0,
        issues=(issue,),
    )


def _configuration_fingerprint() -> str:
    material = {
        "auth_mode": settings.provider_webhook_auth_mode,
        "callback_base_url": settings.provider_callback_base_url,
        "chatwoot_account_id": settings.chatwoot_account_id,
        "chatwoot_webhook_id": settings.chatwoot_webhook_id,
        "evolution_secret": hashlib.sha256(
            (settings.evolution_webhook_bearer_token or "").encode()
        ).hexdigest(),
        "chatwoot_secret": hashlib.sha256(
            (settings.chatwoot_webhook_secret or "").encode()
        ).hexdigest(),
        "receipt_keyring": settings.receipt_hmac_keyring_fingerprint(),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _atomic_write_private(path: Path, contents: str) -> None:
    original_owner = (
        (path.stat().st_uid, path.stat().st_gid) if path.exists() else None
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent, text=True
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
        if original_owner is not None:
            os.chown(temporary_path, *original_owner)
        os.chmod(temporary_path, 0o600)
        os.replace(temporary_path, path)
        os.chmod(path, 0o600)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _report_from_snapshot(value: object) -> ReadinessReport:
    if not isinstance(value, Mapping):
        raise ValueError("snapshot report must be an object")
    ready = value.get("ready")
    auth_mode = value.get("auth_mode")
    evolution_instances = value.get("evolution_instances")
    chatwoot_inboxes = value.get("chatwoot_inboxes")
    issues = value.get("issues")
    if (
        not isinstance(ready, bool)
        or auth_mode not in {"provisioning", "enforced"}
        or not isinstance(evolution_instances, int)
        or isinstance(evolution_instances, bool)
        or evolution_instances < 0
        or not isinstance(chatwoot_inboxes, int)
        or isinstance(chatwoot_inboxes, bool)
        or chatwoot_inboxes < 0
        or not isinstance(issues, list)
        or len(issues) > 20
        or not all(
            isinstance(issue, str) and 0 < len(issue) <= 100 for issue in issues
        )
        or ready != (not issues)
    ):
        raise ValueError("snapshot report is incompatible")
    return ReadinessReport(
        ready=ready,
        auth_mode=auth_mode,
        evolution_instances=evolution_instances,
        chatwoot_inboxes=chatwoot_inboxes,
        issues=tuple(issues),
    )


def write_provider_auth_snapshot(
    report: ReadinessReport,
    *,
    checked_at: float | None = None,
) -> None:
    timestamp = time.time() if checked_at is None else checked_at
    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "checked_at": timestamp,
        "configuration_fingerprint": _configuration_fingerprint(),
        "report": report.as_dict(),
    }
    contents = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    _atomic_write_private(Path(settings.provider_auth_readiness_path), contents)
    reset_provider_auth_snapshot_cache()


def _load_provider_auth_snapshot(*, now_epoch: float) -> ReadinessReport:
    if not settings.receipt_hmac_keyring_is_valid():
        return _not_ready("receipt_hmac_keyring_invalid")
    path = Path(settings.provider_auth_readiness_path)
    try:
        raw = path.read_bytes()
        if len(raw) > MAX_SNAPSHOT_BYTES:
            return _not_ready("readiness_snapshot_invalid")
        payload = json.loads(raw)
    except FileNotFoundError:
        return _not_ready("readiness_snapshot_missing")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return _not_ready("readiness_snapshot_invalid")

    if not isinstance(payload, Mapping):
        return _not_ready("readiness_snapshot_invalid")
    checked_at = payload.get("checked_at")
    if (
        payload.get("schema_version") != SNAPSHOT_SCHEMA_VERSION
        or not isinstance(checked_at, (int, float))
        or isinstance(checked_at, bool)
    ):
        return _not_ready("readiness_snapshot_invalid")
    if payload.get("configuration_fingerprint") != _configuration_fingerprint():
        return _not_ready("readiness_snapshot_configuration_changed")
    age = now_epoch - float(checked_at)
    if age < -60 or age > settings.provider_auth_readiness_max_age_seconds:
        return _not_ready("readiness_snapshot_expired")
    try:
        return _report_from_snapshot(payload.get("report"))
    except ValueError:
        return _not_ready("readiness_snapshot_invalid")


def reset_provider_auth_snapshot_cache() -> None:
    global _snapshot_cache_deadline, _snapshot_cache_key, _snapshot_cache_report
    with _snapshot_cache_guard:
        _snapshot_cache_deadline = 0.0
        _snapshot_cache_key = None
        _snapshot_cache_report = None


async def provider_auth_readiness(
    *,
    now_epoch: float | None = None,
    now_monotonic: float | None = None,
) -> ReadinessReport:
    """Serve a bounded durable snapshot without database or provider fan-out."""
    global _snapshot_cache_deadline, _snapshot_cache_key, _snapshot_cache_report
    epoch = time.time() if now_epoch is None else now_epoch
    monotonic = time.monotonic() if now_monotonic is None else now_monotonic
    cache_key = (settings.provider_auth_readiness_path, _configuration_fingerprint())
    with _snapshot_cache_guard:
        if (
            _snapshot_cache_report is not None
            and _snapshot_cache_key == cache_key
            and monotonic < _snapshot_cache_deadline
        ):
            return _snapshot_cache_report
        report = _load_provider_auth_snapshot(now_epoch=epoch)
        _snapshot_cache_key = cache_key
        _snapshot_cache_report = report
        _snapshot_cache_deadline = (
            monotonic + settings.provider_auth_readiness_cache_seconds
        )
        return report


def _callback_base_url() -> str:
    value = (settings.provider_callback_base_url or "").strip().rstrip("/")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ProviderAuthBootstrapError(
            "Provider callback base URL is not configured safely"
        )
    return value


def provider_callback_urls() -> tuple[str, str]:
    base_url = _callback_base_url()
    return f"{base_url}/webhook", f"{base_url}/chatwoot/webhook"


def read_env_value(path: Path, key: str) -> str | None:
    if not path.exists():
        return None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        candidate, value = stripped.split("=", 1)
        if candidate.strip() == key:
            normalized = value.strip().strip('"').strip("'")
            return normalized or None
    return None


def update_env_file(path: Path, values: dict[str, str]) -> None:
    """Atomically update selected keys without ever rendering their values."""
    if any("\n" in value or "\r" in value for value in values.values()):
        raise ValueError("environment values must be single-line")

    original_lines = (
        path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    )
    remaining = dict(values)
    rendered: list[str] = []
    seen: set[str] = set()
    for line in original_lines:
        stripped = line.strip()
        key = stripped.split("=", 1)[0].strip() if "=" in stripped else None
        if key in values:
            if key not in seen:
                rendered.append(f"{key}={values[key]}")
                seen.add(key)
                remaining.pop(key, None)
            continue
        rendered.append(line)
    if rendered and remaining:
        rendered.append("")
    rendered.extend(f"{key}={value}" for key, value in remaining.items())
    contents = "\n".join(rendered).rstrip("\n") + "\n"

    _atomic_write_private(path, contents)


def generate_evolution_secret() -> str:
    return secrets.token_urlsafe(48)


def generate_receipt_secret() -> str:
    """Generate receipt identity independently from provider bearer secrets."""
    return secrets.token_urlsafe(48)


def serialized_receipt_keyring(env_file: Path | None = None) -> tuple[str, str]:
    """Return an env-safe retained keyring, creating only the initial key."""
    stored_keys = read_env_value(env_file, RECEIPT_KEYS_KEY) if env_file else None
    stored_current = (
        read_env_value(env_file, RECEIPT_CURRENT_VERSION_KEY) if env_file else None
    )
    if stored_keys is not None:
        try:
            parsed = json.loads(stored_keys)
            current = int(stored_current or settings.inbound_receipt_hmac_current_version)
            if (
                not isinstance(parsed, dict)
                or isinstance(current, bool)
                or not 1 <= current <= 65535
                or any(
                    not isinstance(version, str)
                    or not version.isdigit()
                    or not 1 <= int(version) <= 65535
                    or not isinstance(value, str)
                    or not value.strip()
                    or value.strip().casefold()
                    in INBOUND_RECEIPT_HMAC_PUBLIC_PLACEHOLDERS
                    for version, value in parsed.items()
                )
                or (parsed and str(current) not in parsed)
            ):
                raise ValueError
        except (TypeError, ValueError, json.JSONDecodeError):
            raise ProviderAuthBootstrapError("Receipt HMAC keyring is invalid") from None
        current_version = current
        keys = (
            {
                str(int(version)): parsed[version]
                for version in sorted(parsed, key=int)
            }
            if parsed
            else {str(current_version): generate_receipt_secret()}
        )
    elif settings.inbound_receipt_hmac_keys:
        if not settings.receipt_hmac_keyring_is_valid():
            raise ProviderAuthBootstrapError("Receipt HMAC keyring is invalid")
        keys = {
            str(version): settings.inbound_receipt_hmac_keys[version].get_secret_value()
            for version in sorted(settings.inbound_receipt_hmac_keys)
        }
        current_version = settings.inbound_receipt_hmac_current_version
    else:
        current_version = settings.inbound_receipt_hmac_current_version
        keys = {
            str(current_version): generate_receipt_secret()
        }
    return (
        str(current_version),
        json.dumps(keys, sort_keys=True, separators=(",", ":")),
    )


async def provision_provider_auth(
    db: AsyncSession,
    *,
    evolution_secret: str,
    configured_chatwoot_secret: str | None,
    managed_chatwoot_webhook_id: int | None,
) -> tuple[ProvisionReport, str]:
    """Provision provider-side state while route enforcement remains staged."""
    if settings.provider_webhook_auth_mode != "provisioning":
        raise ProviderAuthBootstrapError(
            "Provisioning requires PROVIDER_WEBHOOK_AUTH_MODE=provisioning"
        )
    evolution_url, chatwoot_url = provider_callback_urls()
    agents = [agent for agent in await agente_repo.get_all(db) if agent.ativo]

    for agent in agents:
        await evolution_client.set_webhook(
            evolution_url,
            agent.instance_name,
            bearer_token=evolution_secret,
        )
        await evolution_client.verify_webhook(
            evolution_url,
            agent.instance_name,
            bearer_token=evolution_secret,
        )

    webhook_report = await chatwoot_client.provision_account_webhook(
        chatwoot_url,
        # Chatwoot owns secret generation. Once a managed ID is stored, its
        # matching provider secret is mandatory independent identity proof
        # before any URL/subscription mutation is allowed.
        configured_secret=configured_chatwoot_secret,
        managed_webhook_id=managed_chatwoot_webhook_id,
    )
    inbox_ids = {
        agent.chatwoot_inbox_id
        for agent in agents
        if agent.chatwoot_inbox_id is not None
    }
    delivery_report = await chatwoot_client.check_delivery_contract(
        inbox_ids,
        bootstrap_attribute=True,
    )
    return (
        ProvisionReport(
            evolution_instances=len(agents),
            chatwoot_webhook_created=webhook_report.created,
            chatwoot_webhook_updated=webhook_report.updated,
            chatwoot_webhook_id=webhook_report.webhook_id,
            chatwoot_obsolete_removed=webhook_report.removed_obsolete,
            chatwoot_secret_replaced=(
                configured_chatwoot_secret is not None
                and not secrets.compare_digest(
                    configured_chatwoot_secret.encode(),
                    webhook_report.secret.encode(),
                )
            ),
            delivery_attribute_created=delivery_report.attribute_created,
        ),
        webhook_report.secret,
    )


async def active_provider_auth_readiness(db: AsyncSession) -> ReadinessReport:
    """Actively probe providers; callable only from the operator command."""
    issues: set[str] = set()
    if settings.provider_webhook_auth_mode != "enforced":
        issues.add("authentication_not_enforced")
    if not settings.evolution_webhook_bearer_token:
        issues.add("evolution_secret_missing")
    if not settings.chatwoot_webhook_secret:
        issues.add("chatwoot_secret_missing")
    if not settings.receipt_hmac_keyring_is_valid():
        issues.add("receipt_hmac_keyring_invalid")

    try:
        evolution_url, chatwoot_url = provider_callback_urls()
    except ProviderAuthBootstrapError:
        evolution_url = ""
        chatwoot_url = ""
        issues.add("callback_url_invalid")

    agents = [agent for agent in await agente_repo.get_all(db) if agent.ativo]
    inbox_ids = {
        agent.chatwoot_inbox_id
        for agent in agents
        if agent.chatwoot_inbox_id is not None
    }

    if evolution_url and settings.evolution_webhook_bearer_token:
        for agent in agents:
            try:
                await evolution_client.verify_webhook(
                    evolution_url,
                    agent.instance_name,
                    bearer_token=settings.evolution_webhook_bearer_token,
                )
            except Exception as exc:
                logger.warning(
                    "Provider readiness failed | provider=evolution error_type={}",
                    type(exc).__name__,
                )
                issues.add("evolution_webhook_mismatch")

    if chatwoot_url and settings.chatwoot_webhook_secret:
        try:
            await chatwoot_client.verify_account_webhook(
                chatwoot_url,
                configured_secret=settings.chatwoot_webhook_secret,
                managed_webhook_id=settings.chatwoot_webhook_id,
            )
        except Exception as exc:
            logger.warning(
                "Provider readiness failed | provider=chatwoot_webhook error_type={}",
                type(exc).__name__,
            )
            issues.add("chatwoot_webhook_mismatch")

    try:
        await chatwoot_client.check_delivery_contract(
            inbox_ids,
            bootstrap_attribute=False,
        )
    except Exception as exc:
        logger.warning(
            "Provider readiness failed | provider=chatwoot_delivery error_type={}",
            type(exc).__name__,
        )
        issues.add("chatwoot_delivery_contract_mismatch")

    ordered_issues = tuple(sorted(issues))
    return ReadinessReport(
        ready=not ordered_issues,
        auth_mode=settings.provider_webhook_auth_mode,
        evolution_instances=len(agents),
        chatwoot_inboxes=len(inbox_ids),
        issues=ordered_issues,
    )


async def probe_callback_authentication(*, require_rejection: bool) -> None:
    """Exercise the live app routes without producing provider/domain effects."""
    evolution_secret = settings.evolution_webhook_bearer_token
    chatwoot_secret = settings.chatwoot_webhook_secret
    if not evolution_secret or not chatwoot_secret:
        raise ProviderAuthBootstrapError("Provider callback secrets are missing")

    base_url = settings.provider_auth_probe_base_url.rstrip("/")
    evolution_body = b'{"event":"provider.auth.probe"}'
    chatwoot_body = json.dumps(
        {
            "event": "provider.auth.probe",
            "account": {"id": settings.chatwoot_account_id},
        },
        separators=(",", ":"),
    ).encode()
    timestamp = str(int(time.time()))
    digest = hmac.new(
        chatwoot_secret.encode(),
        timestamp.encode() + b"." + chatwoot_body,
        hashlib.sha256,
    ).hexdigest()

    async def post_with_startup_retry(
        client: httpx.AsyncClient,
        url: str,
        **kwargs: object,
    ) -> httpx.Response:
        for attempt in range(20):
            try:
                return await client.post(url, **kwargs)
            except httpx.TransportError:
                if attempt == 19:
                    raise
                await asyncio.sleep(0.5)
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(timeout=15.0) as client:
        evolution_valid = await post_with_startup_retry(
            client,
            f"{base_url}/webhook",
            content=evolution_body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {evolution_secret}",
            },
        )
        chatwoot_valid = await client.post(
            f"{base_url}/chatwoot/webhook",
            content=chatwoot_body,
            headers={
                "Content-Type": "application/json",
                "X-Chatwoot-Timestamp": timestamp,
                "X-Chatwoot-Signature": f"sha256={digest}",
            },
        )
        if evolution_valid.status_code != 200 or chatwoot_valid.status_code != 200:
            raise ProviderAuthBootstrapError("Authenticated callback probe failed")

        if require_rejection:
            evolution_invalid = await client.post(
                f"{base_url}/webhook",
                content=b"not-json",
                headers={"Authorization": "Bearer invalid"},
            )
            chatwoot_invalid = await client.post(
                f"{base_url}/chatwoot/webhook",
                content=b"not-json",
                headers={
                    "X-Chatwoot-Timestamp": timestamp,
                    "X-Chatwoot-Signature": "sha256=invalid",
                },
            )
            if (
                evolution_invalid.status_code != 401
                or chatwoot_invalid.status_code != 401
            ):
                raise ProviderAuthBootstrapError(
                    "Invalid callback credentials were not rejected"
                )
