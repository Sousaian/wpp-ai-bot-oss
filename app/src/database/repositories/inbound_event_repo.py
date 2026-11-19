import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import InboundEvent


Provider = Literal["evolution", "chatwoot"]
ReceiptDisposition = Literal[
    "scheduled",
    "ignored_nonbot",
    "legacy_evolution",
    "chatwoot",
    "legacy_unknown",
]


class ReceiptCollisionError(ValueError):
    """A provider reused an event identity with different actionable data."""


class ReceiptKeyVersionError(ValueError):
    pass


class RetiredReceiptKeyError(ReceiptKeyVersionError):
    pass


class MalformedReceiptDigestError(ValueError):
    pass


_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def canonicalize_actionable_fields(fields: Mapping[str, object]) -> bytes:
    """Serialize only normalized actionable fields with stable JSON rules."""

    if not fields or any(not isinstance(key, str) for key in fields):
        raise ValueError("actionable fields must be a non-empty string-keyed mapping")
    try:
        canonical = json.dumps(
            fields,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("actionable fields are not canonically serializable") from exc
    return canonical.encode("utf-8")


@dataclass(frozen=True)
class ReceiptHMACKeyRing:
    current_version: int
    keys: Mapping[int, bytes | str]

    def __post_init__(self) -> None:
        if (
            isinstance(self.current_version, bool)
            or not isinstance(self.current_version, int)
            or self.current_version < 1
        ):
            raise ReceiptKeyVersionError("current receipt key version must be positive")
        normalized: dict[int, bytes] = {}
        for version, secret in self.keys.items():
            if isinstance(version, bool) or not isinstance(version, int) or version < 1:
                raise ReceiptKeyVersionError("receipt key versions must be positive integers")
            secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
            if not isinstance(secret_bytes, bytes) or not secret_bytes:
                raise ValueError("receipt HMAC secrets must be non-empty")
            normalized[version] = secret_bytes
        if self.current_version not in normalized:
            raise ReceiptKeyVersionError("current receipt key is unavailable")
        object.__setattr__(self, "keys", normalized)

    def digest(
        self,
        actionable_fields: Mapping[str, object],
        *,
        version: int | None = None,
    ) -> str:
        selected_version = self.current_version if version is None else version
        if (
            isinstance(selected_version, bool)
            or not isinstance(selected_version, int)
            or selected_version < 1
        ):
            raise ReceiptKeyVersionError("receipt key version must be positive")
        secret = self.keys.get(selected_version)
        if secret is None:
            raise RetiredReceiptKeyError("receipt key version is unavailable")
        return hmac.new(
            secret,
            canonicalize_actionable_fields(actionable_fields),
            hashlib.sha256,
        ).hexdigest()

    def verify(
        self,
        actionable_fields: Mapping[str, object],
        *,
        version: int,
        digest: str,
    ) -> None:
        if not isinstance(digest, str) or _SHA256_HEX.fullmatch(digest) is None:
            raise MalformedReceiptDigestError("stored receipt digest is malformed")
        expected = self.digest(actionable_fields, version=version)
        if not hmac.compare_digest(expected, digest):
            raise ReceiptCollisionError("provider event actionable fields changed")


@dataclass(frozen=True)
class ReceiptResult:
    event: InboundEvent
    created: bool

    @property
    def replay(self) -> bool:
        return not self.created


async def record(
    db: AsyncSession,
    *,
    provider: Provider,
    source_scope: str,
    provider_event_id: str,
    event_type: str,
    actionable_fields: Mapping[str, object],
    keyring: ReceiptHMACKeyRing,
    receipt_disposition: ReceiptDisposition = "legacy_unknown",
    agente_id: int | None = None,
    sessao_id: int | None = None,
) -> ReceiptResult:
    """Insert one receipt without poisoning the outer transaction on replay."""

    payload_hmac = keyring.digest(actionable_fields)
    event = InboundEvent(
        provider=provider,
        source_scope=source_scope,
        provider_event_id=provider_event_id,
        event_type=event_type,
        payload_hmac=payload_hmac,
        hmac_key_version=keyring.current_version,
        receipt_disposition=receipt_disposition,
        agente_id=agente_id,
        sessao_id=sessao_id,
    )
    try:
        async with db.begin_nested():
            db.add(event)
            await db.flush()
        return ReceiptResult(event=event, created=True)
    except IntegrityError:
        result = await db.execute(
            select(InboundEvent)
            .where(
                InboundEvent.provider == provider,
                InboundEvent.source_scope == source_scope,
                InboundEvent.provider_event_id == provider_event_id,
            )
            # FOR SHARE: the failed duplicate INSERT already holds a shared lock
            # on this unique record (savepoint rollback keeps InnoDB locks).
            # Upgrading to FOR UPDATE deadlocks parallel duplicates (1213).
            # No caller writes a replayed receipt; S still blocks mutation.
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        existing = result.scalar_one_or_none()
        if existing is None:
            raise
        keyring.verify(
            actionable_fields,
            version=existing.hmac_key_version,
            digest=existing.payload_hmac,
        )
        return ReceiptResult(event=existing, created=False)


async def mark_processed(
    db: AsyncSession,
    event_id: int,
    *,
    agente_id: int | None,
    sessao_id: int | None,
    processed_at: datetime | None = None,
) -> InboundEvent | None:
    event = await db.get(InboundEvent, event_id)
    if event is None:
        return None
    event.agente_id = agente_id
    event.sessao_id = sessao_id
    event.processed_at = processed_at or datetime.utcnow()
    await db.flush()
    return event


async def get_by_identity(
    db: AsyncSession,
    *,
    provider: Provider,
    source_scope: str,
    provider_event_id: str,
) -> InboundEvent | None:
    result = await db.execute(
        select(InboundEvent).where(
            InboundEvent.provider == provider,
            InboundEvent.source_scope == source_scope,
            InboundEvent.provider_event_id == provider_event_id,
        )
    )
    return result.scalar_one_or_none()
