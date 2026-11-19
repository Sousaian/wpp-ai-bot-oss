from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import time
from typing import Any, Literal

import httpx

from src.config import settings
from src.services import chatwoot_client
from src.services.handoff_context import (
    CHATWOOT_PRIVATE_NOTE_MAX_CHARACTERS,
    parse_coverage_id,
)


@dataclass(frozen=True)
class EvolutionTextPayloadV1:
    phone: str
    message: str
    instance_name: str


@dataclass(frozen=True)
class ChatwootHandoffPayloadV1:
    phone: str
    summary: str
    inbox_id: int
    handoff_key: str
    contact_name: str | None = None


@dataclass(frozen=True)
class ChatwootContextNotePayloadV1:
    attempt_id: int
    handoff_key: str
    inbox_id: int
    source_id: str
    content: str
    body_sha256: str
    chunk: int
    chunks: int
    coverage: tuple[str, ...]


@dataclass(frozen=True)
class DeliveryResult:
    external_id: str | None
    http_status: int
    contact_id: int | None = None
    conversation_id: int | None = None
    reconciled: bool = False


class InvalidDeliveryPayload(ValueError):
    pass


class DuplicateRemoteEffect(RuntimeError):
    pass


class ContextNoteOwnershipMismatch(InvalidDeliveryPayload):
    """The remote conversation is not the exact local mapping; never post."""


class ProviderResultError(RuntimeError):
    pass


class ChatwootReconciliationRequired(RuntimeError):
    """The create outcome is ambiguous and must be filtered before retry."""


class _DeadlineClient:
    """Inject and enforce one monotonic budget across every HTTP page/request."""

    def __init__(self, client: httpx.AsyncClient, deadline_at: float) -> None:
        self._client = client
        self._deadline_at = deadline_at

    def remaining(self) -> float:
        remaining = self._deadline_at - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError("delivery operation deadline exhausted")
        return remaining

    async def _call(self, method: str, *args, **kwargs):
        remaining = self.remaining()
        kwargs["timeout"] = remaining
        async with asyncio.timeout(remaining):
            return await getattr(self._client, method)(*args, **kwargs)

    async def get(self, *args, **kwargs):
        return await self._call("get", *args, **kwargs)

    async def post(self, *args, **kwargs):
        return await self._call("post", *args, **kwargs)


def _required_text(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise InvalidDeliveryPayload(f"{key} must be a non-empty string")
    return value


def decode_evolution_text(
    operation: str,
    payload_version: int,
    payload: object,
) -> EvolutionTextPayloadV1:
    if operation != "evolution.send_text.v1" or payload_version != 1:
        raise InvalidDeliveryPayload("unsupported Evolution operation or payload version")
    if not isinstance(payload, Mapping):
        raise InvalidDeliveryPayload("Evolution payload must be an object")
    phone = _required_text(payload, "phone")
    if not phone.replace("+", "", 1).isdigit():
        raise InvalidDeliveryPayload("phone must contain only digits and optional leading plus")
    return EvolutionTextPayloadV1(
        phone=phone,
        message=_required_text(payload, "message"),
        instance_name=_required_text(payload, "instance_name"),
    )


def decode_chatwoot_handoff(
    operation: str,
    payload_version: int,
    payload: object,
) -> ChatwootHandoffPayloadV1:
    if operation != "chatwoot.open_handoff.v1" or payload_version != 1:
        raise InvalidDeliveryPayload("unsupported Chatwoot operation or payload version")
    if not isinstance(payload, Mapping):
        raise InvalidDeliveryPayload("Chatwoot payload must be an object")
    inbox_id = payload.get("inbox_id")
    if not isinstance(inbox_id, int) or isinstance(inbox_id, bool) or inbox_id < 1:
        raise InvalidDeliveryPayload("inbox_id must be a positive integer")
    name = payload.get("contact_name")
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise InvalidDeliveryPayload("contact_name must be a non-empty string")
    return ChatwootHandoffPayloadV1(
        phone=_required_text(payload, "phone"),
        summary=_required_text(payload, "summary"),
        inbox_id=inbox_id,
        handoff_key=_required_text(payload, "handoff_key"),
        contact_name=name,
    )


def decode_chatwoot_context_note(
    operation: str,
    payload_version: int,
    payload: object,
) -> ChatwootContextNotePayloadV1:
    if operation != "chatwoot.add_private_note.v1" or payload_version != 1:
        raise InvalidDeliveryPayload("unsupported Chatwoot context-note operation")
    if not isinstance(payload, Mapping):
        raise InvalidDeliveryPayload("Chatwoot context-note payload must be an object")
    attempt_id = payload.get("attempt_id")
    inbox_id = payload.get("inbox_id")
    chunk = payload.get("chunk")
    chunks = payload.get("chunks")
    coverage = payload.get("coverage")
    for name, value in (("attempt_id", attempt_id), ("inbox_id", inbox_id)):
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise InvalidDeliveryPayload(f"{name} must be a positive integer")
    if (
        not isinstance(chunk, int)
        or isinstance(chunk, bool)
        or not isinstance(chunks, int)
        or isinstance(chunks, bool)
        or chunk < 1
        or chunks < chunk
    ):
        raise InvalidDeliveryPayload("context-note chunk ordinal is invalid")
    if (
        not isinstance(coverage, list)
        or not coverage
        or not all(isinstance(item, str) and item for item in coverage)
        or len(set(coverage)) != len(coverage)
    ):
        raise InvalidDeliveryPayload("context-note coverage must be unique and non-empty")
    try:
        for item in coverage:
            parse_coverage_id(item)
    except ValueError as exc:
        raise InvalidDeliveryPayload("context-note coverage id is malformed") from exc
    handoff_key = _required_text(payload, "handoff_key")
    source_id = _required_text(payload, "source_id")
    content = _required_text(payload, "content")
    body_sha256 = _required_text(payload, "body_sha256")
    if not handoff_key.isascii() or not source_id.isascii() or len(source_id) > 255:
        raise InvalidDeliveryPayload("context-note identities must be bounded ASCII")
    if len(content) > CHATWOOT_PRIVATE_NOTE_MAX_CHARACTERS:
        raise InvalidDeliveryPayload("context-note content exceeds verified provider limit")
    try:
        header_line, body = content.split("\n", 1)
        header = json.loads(header_line)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidDeliveryPayload("context-note content is not parseable") from exc
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    expected_source = (
        f"wppctx:v1:{attempt_id}:{chunk:04d}:{chunks:04d}:{digest}"
    )
    if (
        len(body_sha256) != 64
        or digest != body_sha256
        or source_id != expected_source
        or not isinstance(header, Mapping)
        or header.get("marker") != source_id
        or header.get("body_sha256") != digest
        or header.get("chunk") != chunk
        or header.get("chunks") != chunks
        or header.get("coverage") != coverage
    ):
        raise InvalidDeliveryPayload("context-note marker, hash or coverage is inconsistent")
    return ChatwootContextNotePayloadV1(
        attempt_id=int(attempt_id),
        handoff_key=handoff_key,
        inbox_id=int(inbox_id),
        source_id=source_id,
        content=content,
        body_sha256=body_sha256,
        chunk=chunk,
        chunks=chunks,
        coverage=tuple(coverage),
    )


def _safe_json(response: httpx.Response) -> object:
    try:
        return response.json()
    except ValueError as exc:
        raise ProviderResultError("provider returned invalid JSON") from exc


def _optional_evolution_message_id(data: object) -> str | None:
    if not isinstance(data, Mapping):
        return None
    candidates: list[object] = [data.get("id")]
    key = data.get("key")
    if isinstance(key, Mapping):
        candidates.append(key.get("id"))
    message = data.get("message")
    if isinstance(message, Mapping):
        candidates.append(message.get("id"))
        message_key = message.get("key")
        if isinstance(message_key, Mapping):
            candidates.append(message_key.get("id"))
    for candidate in candidates:
        if isinstance(candidate, (str, int)) and not isinstance(candidate, bool):
            normalized = str(candidate).strip()
            if normalized:
                return normalized[:255]
    return None


async def send_evolution_text(
    payload: EvolutionTextPayloadV1,
    *,
    timeout_seconds: float,
) -> DeliveryResult:
    remote_jid = (
        payload.phone
        if payload.phone.endswith("@s.whatsapp.net")
        else f"{payload.phone}@s.whatsapp.net"
    )
    url = (
        f"{settings.evolution_api_url.rstrip('/')}/message/sendText/"
        f"{payload.instance_name}"
    )
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(
            url,
            headers={
                "apikey": settings.evolution_api_key,
                "Content-Type": "application/json",
            },
            json={"number": remote_jid, "text": payload.message},
        )
    response.raise_for_status()
    external_id = None
    if response.content:
        external_id = _optional_evolution_message_id(_safe_json(response))
    return DeliveryResult(
        external_id=external_id,
        http_status=response.status_code,
    )


async def send_evolution_presence(
    *,
    phone: str,
    instance_name: str,
    presence: Literal["composing", "paused"],
    delay_ms: int,
    timeout_seconds: float,
) -> int:
    """Pinned Evolution 2.3.7 root-level presence-only contract."""

    if delay_ms < 0 or delay_ms > 12000:
        raise ValueError("presence delay must be between 0 and 12000 ms")
    remote_jid = phone if phone.endswith("@s.whatsapp.net") else f"{phone}@s.whatsapp.net"
    url = (
        f"{settings.evolution_api_url.rstrip('/')}/chat/sendPresence/"
        f"{instance_name}"
    )
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(
            url,
            headers={
                "apikey": settings.evolution_api_key,
                "Content-Type": "application/json",
            },
            json={"number": remote_jid, "presence": presence, "delay": delay_ms},
        )
    response.raise_for_status()
    return response.status_code


def _conversation_contact_id(row: Mapping[str, Any]) -> int | None:
    direct = row.get("contact_id")
    if isinstance(direct, int) and not isinstance(direct, bool):
        return direct
    meta = row.get("meta")
    sender = meta.get("sender") if isinstance(meta, Mapping) else None
    sender_id = sender.get("id") if isinstance(sender, Mapping) else None
    if isinstance(sender_id, int) and not isinstance(sender_id, bool):
        return sender_id
    contact_inbox = row.get("contact_inbox")
    nested = (
        contact_inbox.get("contact_id")
        if isinstance(contact_inbox, Mapping)
        else None
    )
    return nested if isinstance(nested, int) and not isinstance(nested, bool) else None


def _conversation_inbox_id(row: Mapping[str, Any]) -> int | None:
    direct = row.get("inbox_id")
    if isinstance(direct, int) and not isinstance(direct, bool):
        return direct
    inbox = row.get("inbox")
    nested = inbox.get("id") if isinstance(inbox, Mapping) else None
    return nested if isinstance(nested, int) and not isinstance(nested, bool) else None


def _conversation_resource(data: object) -> Mapping[str, Any]:
    if not isinstance(data, Mapping):
        raise ProviderResultError("Chatwoot conversation response is incompatible")
    candidates: list[Mapping[str, Any]] = [data]
    for wrapper_name in ("payload", "data", "conversation"):
        wrapper = data.get(wrapper_name)
        if isinstance(wrapper, Mapping):
            candidates.append(wrapper)
            nested = wrapper.get("conversation")
            if isinstance(nested, Mapping):
                candidates.append(nested)
    for candidate in candidates:
        conversation_id = candidate.get("id")
        if (
            isinstance(conversation_id, int)
            and not isinstance(conversation_id, bool)
            and conversation_id > 0
        ):
            return candidate
    raise ProviderResultError("Chatwoot conversation response has no valid ID")


def _verify_conversation(
    row: Mapping[str, Any],
    *,
    marker: str,
    inbox_id: int,
    contact_id: int,
) -> int:
    conversation_id = row.get("id")
    custom_attributes = row.get("custom_attributes")
    if (
        not isinstance(conversation_id, int)
        or isinstance(conversation_id, bool)
        or conversation_id < 1
        or row.get("account_id") != settings.chatwoot_account_id
        or _conversation_inbox_id(row) != inbox_id
        or _conversation_contact_id(row) != contact_id
        or not isinstance(custom_attributes, Mapping)
        or custom_attributes.get(chatwoot_client.DELIVERY_ATTRIBUTE_KEY) != marker
    ):
        raise ProviderResultError("Chatwoot conversation ownership verification failed")
    return conversation_id


async def _verify_inbox_unlocked(
    client: httpx.AsyncClient,
    inbox_id: int,
) -> None:
    response = await client.get(
        f"{chatwoot_client._base_url()}/inboxes/{inbox_id}",
        headers=chatwoot_client._headers(),
    )
    response.raise_for_status()
    inbox = _safe_json(response)
    if (
        not isinstance(inbox, Mapping)
        or inbox.get("id") != inbox_id
        or inbox.get("lock_to_single_conversation") is not False
    ):
        raise chatwoot_client.ChatwootContractError(
            "Chatwoot inbox is unavailable or locked"
        )


async def _exact_marker_result(
    client: httpx.AsyncClient,
    *,
    marker: str,
    inbox_id: int,
    contact_id: int,
) -> tuple[int, bool] | None:
    rows = await chatwoot_client._filter_conversations_by_delivery_key(
        client,
        marker,
    )
    verified = [
        _verify_conversation(
            row,
            marker=marker,
            inbox_id=inbox_id,
            contact_id=contact_id,
        )
        for row in rows
    ]
    if len(verified) > 1:
        raise DuplicateRemoteEffect("multiple exact Chatwoot delivery markers")
    return (verified[0], True) if verified else None


async def open_or_reconcile_chatwoot_handoff(
    payload: ChatwootHandoffPayloadV1,
    *,
    delivery_key: str,
    timeout_seconds: float | None = None,
    deadline_at: float | None = None,
    reconcile_only: bool = False,
) -> DeliveryResult | None:
    """Reconcile first, then create once with marker and private summary note."""

    if deadline_at is None:
        if timeout_seconds is None or timeout_seconds <= 0:
            raise ValueError("a positive timeout_seconds or deadline_at is required")
        deadline_at = time.monotonic() + timeout_seconds
    elif timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    initial_timeout = max(deadline_at - time.monotonic(), 0.001)
    async with httpx.AsyncClient(timeout=initial_timeout) as raw_client:
        client = _DeadlineClient(raw_client, deadline_at)
        await _verify_inbox_unlocked(client, payload.inbox_id)
        contact_id = await chatwoot_client._search_exact_contact_id(
            client,
            payload.phone,
        )
        if contact_id is None and reconcile_only:
            return None
        if contact_id is None:
            contact_response = await client.post(
                f"{chatwoot_client._base_url()}/contacts",
                headers=chatwoot_client._headers(),
                json={
                    "name": payload.contact_name or payload.phone,
                    "phone_number": (
                        payload.phone
                        if payload.phone.startswith("+")
                        else f"+{payload.phone}"
                    ),
                },
            )
            if contact_response.status_code in {409, 422}:
                contact_id = await chatwoot_client._search_exact_contact_id(
                    client,
                    payload.phone,
                )
                if contact_id is None:
                    contact_response.raise_for_status()
            else:
                contact_response.raise_for_status()
                contact_id = chatwoot_client._extract_resource_id(
                    _safe_json(contact_response),
                    "contact",
                )
        assert contact_id is not None

        exact = await _exact_marker_result(
            client,
            marker=delivery_key,
            inbox_id=payload.inbox_id,
            contact_id=contact_id,
        )
        if exact is not None:
            conversation_id, _ = exact
            return DeliveryResult(
                external_id=str(conversation_id),
                http_status=200,
                contact_id=contact_id,
                conversation_id=conversation_id,
                reconciled=True,
            )
        if reconcile_only:
            return None

        create_response = await client.post(
            f"{chatwoot_client._base_url()}/conversations",
            headers=chatwoot_client._headers(),
            json={
                "source_id": None,
                "inbox_id": payload.inbox_id,
                "contact_id": contact_id,
                "status": "open",
                "custom_attributes": {
                    chatwoot_client.DELIVERY_ATTRIBUTE_KEY: delivery_key,
                },
                "message": {
                    "content": payload.summary,
                    "message_type": "outgoing",
                    "private": True,
                },
            },
        )
        create_response.raise_for_status()
        conversation_id = chatwoot_client._extract_resource_id(
            _safe_json(create_response),
            "conversation",
        )
        verify_response = await client.get(
            f"{chatwoot_client._base_url()}/conversations/{conversation_id}",
            headers=chatwoot_client._headers(),
        )
        verify_response.raise_for_status()
        verified_id = _verify_conversation(
            _conversation_resource(_safe_json(verify_response)),
            marker=delivery_key,
            inbox_id=payload.inbox_id,
            contact_id=contact_id,
        )
        if verified_id != conversation_id:
            raise ProviderResultError("Chatwoot returned a different conversation")
        return DeliveryResult(
            external_id=str(conversation_id),
            http_status=create_response.status_code,
            contact_id=contact_id,
            conversation_id=conversation_id,
            reconciled=False,
        )


def _verify_context_note(
    row: Mapping[str, Any],
    *,
    payload: ChatwootContextNotePayloadV1,
    conversation_id: int,
) -> int:
    message_id = row.get("id")
    if (
        not isinstance(message_id, int)
        or isinstance(message_id, bool)
        or message_id < 1
        or row.get("conversation_id") != conversation_id
        or row.get("inbox_id") != payload.inbox_id
        or row.get("message_type") != 1
        or row.get("private") is not True
        or row.get("source_id") != payload.source_id
        or row.get("content") != payload.content
    ):
        raise ProviderResultError("Chatwoot private-note ownership verification failed")
    return message_id


async def _find_exact_context_note(
    client: _DeadlineClient,
    *,
    payload: ChatwootContextNotePayloadV1,
    conversation_id: int,
    contact_id: int,
) -> DeliveryResult | None:
    after = 0
    seen: set[int] = set()
    matches: list[int] = []
    for _ in range(10_000):
        response = await client.get(
            f"{chatwoot_client._base_url()}/conversations/{conversation_id}/messages",
            headers=chatwoot_client._headers(),
            params={"after": after},
        )
        response.raise_for_status()
        data = _safe_json(response)
        if not isinstance(data, Mapping):
            raise ProviderResultError("Chatwoot message listing is incompatible")
        rows = data.get("payload")
        meta = data.get("meta")
        meta_contact = meta.get("contact") if isinstance(meta, Mapping) else None
        if (
            not isinstance(rows, list)
            or not all(isinstance(row, Mapping) for row in rows)
            or not isinstance(meta_contact, Mapping)
            or meta_contact.get("id") != contact_id
        ):
            raise ProviderResultError("Chatwoot message listing ownership is incompatible")
        for row in rows:
            message_id = row.get("id")
            if (
                not isinstance(message_id, int)
                or isinstance(message_id, bool)
                or message_id < 1
                or message_id in seen
                or message_id <= after
            ):
                raise ProviderResultError("Chatwoot message pagination did not advance")
            seen.add(message_id)
            if row.get("source_id") == payload.source_id:
                matches.append(
                    _verify_context_note(
                        row, payload=payload, conversation_id=conversation_id
                    )
                )
        if len(matches) > 1:
            raise DuplicateRemoteEffect("multiple exact Chatwoot context-note markers")
        if len(rows) < 100:
            break
        after = int(rows[-1]["id"])
    else:
        raise ProviderResultError("Chatwoot message recovery exceeded its page bound")
    if not matches:
        return None
    return DeliveryResult(
        external_id=str(matches[0]),
        http_status=200,
        contact_id=contact_id,
        conversation_id=conversation_id,
        reconciled=True,
    )


async def create_or_reconcile_chatwoot_context_note(
    payload: ChatwootContextNotePayloadV1,
    *,
    contact_id: int,
    conversation_id: int,
    timeout_seconds: float | None = None,
    deadline_at: float | None = None,
    reconcile_only: bool = False,
    progress: dict[str, bool] | None = None,
) -> DeliveryResult | None:
    """Create once; after ambiguity only exhaustive exact-marker recovery is safe.

    ``progress["post_sent"]`` becomes true immediately before the create POST.
    A failure while it is false happened during the read-only mapping check or
    exhaustive scan, so nothing of this attempt can have been created.
    """

    if contact_id < 1 or conversation_id < 1:
        raise ValueError("positive Chatwoot mapping is required")
    if deadline_at is None:
        if timeout_seconds is None or timeout_seconds <= 0:
            raise ValueError("a positive timeout_seconds or deadline_at is required")
        deadline_at = time.monotonic() + timeout_seconds
    initial_timeout = max(deadline_at - time.monotonic(), 0.001)
    async with httpx.AsyncClient(timeout=initial_timeout) as raw_client:
        client = _DeadlineClient(raw_client, deadline_at)
        conversation_response = await client.get(
            f"{chatwoot_client._base_url()}/conversations/{conversation_id}",
            headers=chatwoot_client._headers(),
        )
        conversation_response.raise_for_status()
        conversation = _conversation_resource(_safe_json(conversation_response))
        if (
            conversation.get("id") != conversation_id
            or conversation.get("account_id") != settings.chatwoot_account_id
            or _conversation_inbox_id(conversation) != payload.inbox_id
            or _conversation_contact_id(conversation) != contact_id
        ):
            raise ContextNoteOwnershipMismatch(
                "Chatwoot context-note mapping verification failed"
            )

        exact = await _find_exact_context_note(
            client,
            payload=payload,
            conversation_id=conversation_id,
            contact_id=contact_id,
        )
        if exact is not None or reconcile_only:
            return exact

        if progress is not None:
            progress["post_sent"] = True
        response = await client.post(
            f"{chatwoot_client._base_url()}/conversations/{conversation_id}/messages",
            headers=chatwoot_client._headers(),
            json={
                "content": payload.content,
                "private": True,
                "message_type": "outgoing",
                "source_id": payload.source_id,
            },
        )
        response.raise_for_status()
        created = _safe_json(response)
        if not isinstance(created, Mapping):
            raise ProviderResultError("Chatwoot private-note response is incompatible")
        message_id = _verify_context_note(
            created, payload=payload, conversation_id=conversation_id
        )
        return DeliveryResult(
            external_id=str(message_id),
            http_status=response.status_code,
            contact_id=contact_id,
            conversation_id=conversation_id,
            reconciled=False,
        )


def classify_http_failure(
    provider: Literal["evolution", "chatwoot"],
    status_code: int,
) -> tuple[bool, bool, str]:
    if status_code in {408, 425, 429} or status_code >= 500:
        return True, provider == "chatwoot" or status_code >= 500, f"http_{status_code}"
    return False, False, f"http_{status_code}"


def retry_after_seconds(response: httpx.Response, *, maximum: int = 3600) -> int | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = int(value)
    except ValueError:
        try:
            deadline = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        seconds = int((deadline - datetime.now(timezone.utc)).total_seconds())
    return max(0, min(seconds, maximum))
