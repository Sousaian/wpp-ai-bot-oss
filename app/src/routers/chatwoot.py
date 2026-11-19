import json
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from loguru import logger

from src.config import settings
from src.database.repositories import inbound_event_repo
from src.database.unit_of_work import UnitOfWork, get_unit_of_work
from src.services import chatwoot_orchestrator
from src.services.chatwoot_client import DELIVERY_ATTRIBUTE_KEY
from src.services.webhook_auth import WebhookAuthError, verify_chatwoot_signature


router = APIRouter(prefix="/chatwoot", tags=["chatwoot"])
_HUMAN_SENDER_TYPES = frozenset({"user", "agent", "operator"})


def _positive_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _message_data(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    envelope: Mapping[str, Any] = payload
    for wrapper_name in ("data", "payload"):
        wrapper = payload.get(wrapper_name)
        if isinstance(wrapper, Mapping):
            envelope = wrapper
            break
    nested_message = envelope.get("message")
    if isinstance(nested_message, Mapping):
        return nested_message, envelope
    return payload, payload


def trusted_account_scope() -> str:
    if settings.chatwoot_account_id is None or settings.chatwoot_account_id <= 0:
        raise RuntimeError("Chatwoot account scope is not configured")
    return str(settings.chatwoot_account_id)


def _payload_account_id(payload: Mapping[str, Any]) -> int | None:
    account = payload.get("account")
    if isinstance(account, Mapping):
        return _positive_int(account.get("id"))
    return None


def _conversation_resource(
    message: Mapping[str, Any], envelope: Mapping[str, Any]
) -> Mapping[str, Any] | None:
    for candidate in (message.get("conversation"), envelope.get("conversation")):
        if isinstance(candidate, Mapping):
            return candidate
    return None


def _conversation_inbox_id(conversation: Mapping[str, Any] | None) -> int | None:
    if conversation is None:
        return None
    direct = _positive_int(conversation.get("inbox_id"))
    if direct is not None:
        return direct
    inbox = conversation.get("inbox")
    return _positive_int(inbox.get("id")) if isinstance(inbox, Mapping) else None


def _conversation_contact_id(conversation: Mapping[str, Any] | None) -> int | None:
    if conversation is None:
        return None
    direct = _positive_int(conversation.get("contact_id"))
    if direct is not None:
        return direct
    meta = conversation.get("meta")
    sender = meta.get("sender") if isinstance(meta, Mapping) else None
    nested = _positive_int(sender.get("id")) if isinstance(sender, Mapping) else None
    if nested is not None:
        return nested
    contact_inbox = conversation.get("contact_inbox")
    return (
        _positive_int(contact_inbox.get("contact_id"))
        if isinstance(contact_inbox, Mapping)
        else None
    )


def _conversation_handoff_key(conversation: Mapping[str, Any] | None) -> str | None:
    if conversation is None:
        return None
    attributes = conversation.get("custom_attributes")
    value = attributes.get(DELIVERY_ATTRIBUTE_KEY) if isinstance(attributes, Mapping) else None
    return value.strip() if isinstance(value, str) and value.strip() else None


def _parse_actionable_event(
    payload: Mapping[str, Any],
    *,
    account_id: int,
    remote_account_id: int | None,
) -> chatwoot_orchestrator.ChatwootReplyEvent | dict[str, str]:
    message, envelope = _message_data(payload)
    event = payload.get("event") or envelope.get("event")
    if event != "message_created":
        return {"status": "ignored", "reason": "unsupported_event"}
    message_id = _positive_int(message.get("id"))
    if message_id is None:
        return {"status": "ignored", "reason": "invalid_message_id"}
    conversation = _conversation_resource(message, envelope)
    conversation_id = _positive_int(
        conversation.get("id") if conversation is not None else None
    )
    if conversation_id is None:
        conversation_id = _positive_int(
            message.get("conversation_id") or envelope.get("conversation_id")
        )
    if conversation_id is None:
        return {"status": "ignored", "reason": "invalid_conversation_id"}
    content = message.get("content") or message.get("body")
    if not isinstance(content, str) or not content.strip():
        return {"status": "ignored", "reason": "missing_content"}
    if message.get("private") not in (None, False):
        return {"status": "ignored", "reason": "private_message"}
    if str(message.get("message_type", "")).lower() != "outgoing":
        return {"status": "ignored", "reason": "not_outgoing"}
    sender = message.get("sender")
    sender_type = sender.get("type") if isinstance(sender, Mapping) else None
    if str(sender_type).lower() not in _HUMAN_SENDER_TYPES:
        return {"status": "ignored", "reason": "not_human_sender"}
    return chatwoot_orchestrator.ChatwootReplyEvent(
        account_id=account_id,
        remote_account_id=remote_account_id,
        message_id=message_id,
        conversation_id=conversation_id,
        content=content.strip(),
        inbox_id=_conversation_inbox_id(conversation),
        contact_id=_conversation_contact_id(conversation),
        handoff_key=_conversation_handoff_key(conversation),
    )


@router.post("/webhook")
async def chatwoot_webhook(
    request: Request,
    response: Response,
    uow: UnitOfWork = Depends(get_unit_of_work),
) -> dict[str, Any]:
    raw_body = await request.body()
    authenticated = settings.provider_webhook_auth_mode == "enforced"
    if authenticated:
        if not settings.chatwoot_webhook_secret:
            logger.error("Chatwoot webhook authentication is not provisioned")
            raise HTTPException(status_code=503, detail="webhook authentication unavailable")
        try:
            verify_chatwoot_signature(
                headers=request.headers,
                raw_body=raw_body,
                secret=settings.chatwoot_webhook_secret,
                max_age_seconds=settings.chatwoot_webhook_max_age_seconds,
            )
        except WebhookAuthError:
            logger.warning("Chatwoot webhook rejected: authentication failed")
            raise HTTPException(status_code=401, detail="invalid webhook authentication") from None

    try:
        payload = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail="invalid webhook payload") from None
    if not isinstance(payload, Mapping):
        return {"status": "ignored", "reason": "invalid_payload"}

    try:
        account_id = int(trusted_account_scope())
    except RuntimeError:
        raise HTTPException(status_code=503, detail="webhook account scope unavailable") from None
    body_account_id = _payload_account_id(payload)
    if body_account_id is not None and body_account_id != account_id:
        logger.warning("Chatwoot webhook rejected: account scope mismatch")
        raise HTTPException(status_code=403, detail="invalid webhook account")

    parsed = _parse_actionable_event(
        payload,
        account_id=account_id,
        remote_account_id=body_account_id,
    )
    if isinstance(parsed, dict):
        return parsed
    try:
        result = await chatwoot_orchestrator.orchestrate(
            event=parsed,
            uow=uow,
            authenticated=authenticated,
        )
    except inbound_event_repo.ReceiptCollisionError:
        logger.error("Chatwoot receipt collision rejected")
        raise HTTPException(status_code=409, detail="event identity collision") from None
    except (
        chatwoot_orchestrator.ChatwootOwnershipError,
        chatwoot_orchestrator.ChatwootReceiptConfigurationError,
    ) as exc:
        logger.error("Chatwoot webhook acceptance unavailable | erro_tipo={}", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="temporarily unavailable",
            headers={"Retry-After": "5"},
        ) from None
    except Exception as exc:
        logger.error("Chatwoot webhook transaction failed | erro_tipo={}", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="temporarily unavailable",
            headers={"Retry-After": "5"},
        ) from None
    response.status_code = result.http_status
    return result.body
