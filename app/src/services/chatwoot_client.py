from collections.abc import Mapping
from dataclasses import dataclass, field
import secrets
from typing import Any, Optional
from uuid import uuid4

import httpx
from loguru import logger

from src.config import settings


TIMEOUT = 15.0
DELIVERY_ATTRIBUTE_KEY = "wpp_delivery_key"
DELIVERY_ATTRIBUTE_MODEL = "conversation_attribute"
DELIVERY_ATTRIBUTE_DISPLAY_TYPE = "text"
MAX_FILTER_PAGES = 1000


class ChatwootResponseError(RuntimeError):
    """Resposta do Chatwoot incompatível com o contrato esperado."""


class ChatwootContractError(RuntimeError):
    """Installed Chatwoot contract is unsafe for durable delivery."""


@dataclass(frozen=True)
class ChatwootContractReport:
    account_id: int
    inbox_ids: tuple[int, ...]
    attribute_created: bool


@dataclass(frozen=True)
class ChatwootWebhookReport:
    webhook_id: int
    created: bool
    updated: bool
    secret: str = field(repr=False)
    removed_obsolete: int = 0


CHATWOOT_WEBHOOK_NAME = "WPP AI Bot callback"
CHATWOOT_WEBHOOK_SUBSCRIPTION = "message_created"


def _base_url() -> str:
    if not settings.chatwoot_base_url or not settings.chatwoot_account_id:
        raise RuntimeError("Chatwoot settings not configured")
    return f"{settings.chatwoot_base_url.rstrip('/')}/api/v1/accounts/{settings.chatwoot_account_id}"


def _headers() -> dict[str, str]:
    return {
        "api_access_token": settings.chatwoot_api_token or "",
        "Content-Type": "application/json",
    }


def _account_webhook(data: object, operation: str) -> Mapping[str, Any]:
    if not isinstance(data, Mapping):
        raise ChatwootContractError(
            f"Chatwoot account webhook response is incompatible | operation={operation}"
        )
    payload = data.get("payload")
    if isinstance(payload, Mapping):
        nested = payload.get("webhook")
        if isinstance(nested, Mapping):
            return nested
    nested = data.get("webhook")
    if isinstance(nested, Mapping):
        return nested
    return data


def _account_webhooks(data: object) -> list[Mapping[str, Any]]:
    if not isinstance(data, Mapping):
        raise ChatwootContractError(
            "Chatwoot account webhook list is incompatible"
        )
    payload = data.get("payload")
    webhooks = payload.get("webhooks") if isinstance(payload, Mapping) else None
    if not isinstance(webhooks, list) or not all(
        isinstance(webhook, Mapping) for webhook in webhooks
    ):
        raise ChatwootContractError(
            "Chatwoot account webhook list is incompatible"
        )
    return webhooks


def _validate_account_webhook(
    webhook: Mapping[str, Any],
    *,
    webhook_url: str,
    configured_secret: str | None,
) -> str:
    webhook_id = webhook.get("id")
    account_id = webhook.get("account_id")
    subscriptions = webhook.get("subscriptions")
    secret = webhook.get("secret")
    if (
        not isinstance(webhook_id, int)
        or isinstance(webhook_id, bool)
        or webhook_id <= 0
        or account_id != settings.chatwoot_account_id
        or webhook.get("name") != CHATWOOT_WEBHOOK_NAME
        or webhook.get("url") != webhook_url
        or not isinstance(subscriptions, list)
        or CHATWOOT_WEBHOOK_SUBSCRIPTION not in subscriptions
        or not isinstance(secret, str)
        or not secret
    ):
        raise ChatwootContractError(
            "Chatwoot account webhook contract does not match"
        )
    if configured_secret and not secrets.compare_digest(
        configured_secret.encode(), secret.encode()
    ):
        raise ChatwootContractError(
            "Chatwoot account webhook signing secret does not match"
        )
    return secret


def _prove_managed_webhook_identity(
    webhook: Mapping[str, Any],
    *,
    configured_secret: str | None,
) -> int:
    """Prove stored-ID ownership before any provider mutation.

    Numeric IDs can be stale or copied between installations. The account,
    stable managed name, and provider-generated signing secret are independent
    identity material; all must match before PATCH is permitted.
    """
    webhook_id = webhook.get("id")
    remote_secret = webhook.get("secret")
    if (
        not isinstance(webhook_id, int)
        or isinstance(webhook_id, bool)
        or webhook_id <= 0
        or webhook.get("account_id") != settings.chatwoot_account_id
        or webhook.get("name") != CHATWOOT_WEBHOOK_NAME
        or not isinstance(remote_secret, str)
        or not remote_secret
        or not isinstance(configured_secret, str)
        or not configured_secret
        or not secrets.compare_digest(
            configured_secret.encode(), remote_secret.encode()
        )
    ):
        raise ChatwootContractError(
            "Configured Chatwoot webhook ownership could not be proven"
        )
    return webhook_id


async def _list_account_webhooks(
    client: httpx.AsyncClient,
) -> list[Mapping[str, Any]]:
    response = await client.get(f"{_base_url()}/webhooks", headers=_headers())
    return _account_webhooks(_contract_json(response, "list_account_webhooks"))


async def provision_account_webhook(
    webhook_url: str,
    *,
    configured_secret: str | None,
    managed_webhook_id: int | None = None,
) -> ChatwootWebhookReport:
    """Create/update the one trusted account webhook and return its provider secret.

    Chatwoot 4.17 generates the signing secret and exposes it to account admins;
    its API intentionally does not accept caller-selected secrets.  A clean
    bootstrap therefore adopts that generated value once. The stable provider
    ID identifies the webhook independently of its URL so callback rotation
    updates it in place. The ID never authorizes mutation by itself: the stable
    name, trusted account, and provider signing secret must also match.
    """
    if not webhook_url:
        raise ValueError("webhook_url must not be empty")
    if not settings.chatwoot_api_token:
        raise ChatwootContractError("Chatwoot API settings are not configured")

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        webhooks = await _list_account_webhooks(client)
        managed_by_name = [
            webhook
            for webhook in webhooks
            if webhook.get("name") == CHATWOOT_WEBHOOK_NAME
        ]

        created = False
        updated = False
        webhook: Mapping[str, Any]
        if managed_webhook_id is not None:
            matches = [
                candidate
                for candidate in webhooks
                if candidate.get("id") == managed_webhook_id
            ]
            if len(matches) != 1:
                raise ChatwootContractError(
                    "Configured Chatwoot managed webhook ID was not found"
                )
            webhook = matches[0]
            _prove_managed_webhook_identity(
                webhook,
                configured_secret=configured_secret,
            )
            if any(
                candidate.get("id") != managed_webhook_id
                for candidate in managed_by_name
            ):
                raise ChatwootContractError(
                    "Chatwoot managed webhook identity is ambiguous"
                )
        elif len(managed_by_name) > 1:
            raise ChatwootContractError(
                "Chatwoot managed webhook identity is ambiguous"
            )
        elif managed_by_name:
            # First adoption has no stored ID or independent secret yet. Only
            # an exact desired contract is a sufficient structural proof; it
            # is never renamed, rotated, or deleted during adoption.
            webhook = managed_by_name[0]
            _validate_account_webhook(
                webhook,
                webhook_url=webhook_url,
                configured_secret=configured_secret,
            )
        else:
            if configured_secret:
                raise ChatwootContractError(
                    "Chatwoot webhook secret has no managed identity"
                )
            response = await client.post(
                f"{_base_url()}/webhooks",
                headers=_headers(),
                json={
                    "webhook": {
                        "name": CHATWOOT_WEBHOOK_NAME,
                        "url": webhook_url,
                        "subscriptions": [CHATWOOT_WEBHOOK_SUBSCRIPTION],
                    }
                },
            )
            webhook = _account_webhook(
                _contract_json(response, "create_account_webhook"),
                "create_account_webhook",
            )
            created = True

        if not created:
            subscriptions = webhook.get("subscriptions")
            normalized_subscriptions = (
                [str(item) for item in subscriptions]
                if isinstance(subscriptions, list)
                else []
            )
            if CHATWOOT_WEBHOOK_SUBSCRIPTION not in normalized_subscriptions:
                normalized_subscriptions.append(CHATWOOT_WEBHOOK_SUBSCRIPTION)
            if (
                webhook.get("name") != CHATWOOT_WEBHOOK_NAME
                or webhook.get("url") != webhook_url
                or subscriptions != normalized_subscriptions
            ):
                webhook_id = webhook.get("id")
                if not isinstance(webhook_id, int) or isinstance(webhook_id, bool):
                    raise ChatwootContractError(
                        "Chatwoot account webhook ID is incompatible"
                    )
                response = await client.patch(
                    f"{_base_url()}/webhooks/{webhook_id}",
                    headers=_headers(),
                    json={
                        "webhook": {
                            "name": CHATWOOT_WEBHOOK_NAME,
                            "url": webhook_url,
                            "subscriptions": normalized_subscriptions,
                        }
                    },
                )
                webhook = _account_webhook(
                    _contract_json(response, "update_account_webhook"),
                    "update_account_webhook",
                )
                updated = True

        secret = _validate_account_webhook(
            webhook,
            webhook_url=webhook_url,
            configured_secret=configured_secret,
        )
        webhook_id = webhook["id"]
        return ChatwootWebhookReport(
            webhook_id=webhook_id,
            created=created,
            updated=updated,
            removed_obsolete=0,
            secret=secret,
        )


async def verify_account_webhook(
    webhook_url: str,
    *,
    configured_secret: str,
    managed_webhook_id: int | None = None,
) -> int:
    """Verify a unique account-scoped webhook without returning its secret."""
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        webhooks = await _list_account_webhooks(client)
    if managed_webhook_id is not None:
        matches = [
            webhook
            for webhook in webhooks
            if webhook.get("id") == managed_webhook_id
        ]
    else:
        matches = [
            webhook
            for webhook in webhooks
            if webhook.get("name") == CHATWOOT_WEBHOOK_NAME
        ]
    if len(matches) != 1:
        raise ChatwootContractError(
            "Chatwoot account webhook is missing or not unique"
        )
    if managed_webhook_id is not None:
        _prove_managed_webhook_identity(
            matches[0],
            configured_secret=configured_secret,
        )
    obsolete_managed = [
        webhook
        for webhook in webhooks
        if webhook.get("name") == CHATWOOT_WEBHOOK_NAME
        and webhook.get("id") != matches[0].get("id")
    ]
    if obsolete_managed:
        raise ChatwootContractError(
            "Obsolete Chatwoot managed webhooks are still active"
        )
    _validate_account_webhook(
        matches[0],
        webhook_url=webhook_url,
        configured_secret=configured_secret,
    )
    return int(matches[0]["id"])


def _raise_for_contract_status(response: httpx.Response, operation: str) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise ChatwootContractError(
            f"Chatwoot contract check failed | operation={operation} "
            f"status={exc.response.status_code}"
        ) from None


def _contract_json(response: httpx.Response, operation: str) -> object:
    _raise_for_contract_status(response, operation)
    try:
        return response.json()
    except ValueError:
        raise ChatwootContractError(
            f"Chatwoot contract check returned invalid JSON | operation={operation}"
        ) from None


def _definition_list(data: object) -> list[Mapping[str, Any]]:
    if not isinstance(data, list) or not all(
        isinstance(item, Mapping) for item in data
    ):
        raise ChatwootContractError(
            "Chatwoot custom-attribute definition response is incompatible"
        )
    return data


def _matching_delivery_definition(
    definitions: list[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    keyed = [
        definition
        for definition in definitions
        if definition.get("attribute_key") == DELIVERY_ATTRIBUTE_KEY
    ]
    if not keyed:
        return None
    if len(keyed) != 1:
        raise ChatwootContractError(
            "Chatwoot delivery custom attribute is not unique"
        )

    definition = keyed[0]
    if (
        definition.get("attribute_model") != DELIVERY_ATTRIBUTE_MODEL
        or definition.get("attribute_display_type")
        != DELIVERY_ATTRIBUTE_DISPLAY_TYPE
    ):
        raise ChatwootContractError(
            "Chatwoot delivery custom attribute has an incompatible definition"
        )
    return definition


async def _list_delivery_definitions(
    client: httpx.AsyncClient,
) -> list[Mapping[str, Any]]:
    response = await client.get(
        f"{_base_url()}/custom_attribute_definitions",
        headers=_headers(),
        params={"attribute_model": DELIVERY_ATTRIBUTE_MODEL},
    )
    return _definition_list(
        _contract_json(response, "list_custom_attribute_definitions")
    )


async def _ensure_delivery_definition(
    client: httpx.AsyncClient,
    *,
    bootstrap: bool,
) -> bool:
    definitions = await _list_delivery_definitions(client)
    if _matching_delivery_definition(definitions) is not None:
        return False
    if not bootstrap:
        raise ChatwootContractError(
            "Chatwoot delivery custom attribute is missing"
        )

    response = await client.post(
        f"{_base_url()}/custom_attribute_definitions",
        headers=_headers(),
        json={
            "custom_attribute_definition": {
                "attribute_display_name": "WPP delivery key",
                "attribute_description": (
                    "Durable delivery reconciliation key managed by WPP AI Bot"
                ),
                "attribute_display_type": DELIVERY_ATTRIBUTE_DISPLAY_TYPE,
                "attribute_key": DELIVERY_ATTRIBUTE_KEY,
                "attribute_model": DELIVERY_ATTRIBUTE_MODEL,
            }
        },
    )
    if response.status_code in {409, 422}:
        definitions = await _list_delivery_definitions(client)
        if _matching_delivery_definition(definitions) is None:
            raise ChatwootContractError(
                "Chatwoot delivery custom attribute bootstrap conflicted"
            )
        return False

    created = _contract_json(response, "create_custom_attribute_definition")
    if not isinstance(created, Mapping):
        raise ChatwootContractError(
            "Chatwoot custom-attribute creation response is incompatible"
        )
    if _matching_delivery_definition([created]) is None:
        raise ChatwootContractError(
            "Chatwoot did not create the expected delivery custom attribute"
        )
    return True


def _conversation_filter_page(
    data: object,
) -> tuple[list[Mapping[str, Any]], int]:
    if not isinstance(data, Mapping):
        raise ChatwootContractError(
            "Chatwoot conversation filter response is incompatible"
        )
    payload = data.get("payload")
    meta = data.get("meta")
    if (
        not isinstance(payload, list)
        or not all(isinstance(row, Mapping) for row in payload)
        or not isinstance(meta, Mapping)
    ):
        raise ChatwootContractError(
            "Chatwoot conversation filter response is incompatible"
        )
    all_count = meta.get("all_count")
    if (
        not isinstance(all_count, int)
        or isinstance(all_count, bool)
        or all_count < 0
    ):
        raise ChatwootContractError(
            "Chatwoot conversation filter count is incompatible"
        )
    return payload, all_count


async def _filter_conversations_by_delivery_key(
    client: httpx.AsyncClient,
    delivery_key: str,
) -> list[Mapping[str, Any]]:
    page = 1
    expected_count: int | None = None
    seen_ids: set[int] = set()
    matches: list[Mapping[str, Any]] = []
    filter_payload = {
        "payload": [
            {
                "attribute_key": DELIVERY_ATTRIBUTE_KEY,
                "filter_operator": "equal_to",
                "values": [delivery_key],
                "query_operator": None,
                "custom_attribute_type": DELIVERY_ATTRIBUTE_MODEL,
            }
        ]
    }

    while page <= MAX_FILTER_PAGES:
        response = await client.post(
            f"{_base_url()}/conversations/filter",
            headers=_headers(),
            params={"page": page},
            json=filter_payload,
        )
        rows, all_count = _conversation_filter_page(
            _contract_json(response, "filter_conversations")
        )
        if expected_count is None:
            expected_count = all_count
        elif all_count != expected_count:
            raise ChatwootContractError(
                "Chatwoot conversation filter count changed during pagination"
            )

        if not rows:
            if len(matches) != expected_count:
                raise ChatwootContractError(
                    "Chatwoot conversation filter pagination ended early"
                )
            return matches

        for row in rows:
            conversation_id = row.get("id")
            if (
                not isinstance(conversation_id, int)
                or isinstance(conversation_id, bool)
                or conversation_id <= 0
                or conversation_id in seen_ids
            ):
                raise ChatwootContractError(
                    "Chatwoot conversation filter pagination is incompatible"
                )
            if row.get("account_id") != settings.chatwoot_account_id:
                raise ChatwootContractError(
                    "Chatwoot conversation filter crossed account scope"
                )
            custom_attributes = row.get("custom_attributes")
            if (
                not isinstance(custom_attributes, Mapping)
                or custom_attributes.get(DELIVERY_ATTRIBUTE_KEY) != delivery_key
            ):
                raise ChatwootContractError(
                    "Chatwoot conversation filter returned a non-exact match"
                )
            seen_ids.add(conversation_id)
            matches.append(row)

        if len(matches) == expected_count:
            return matches
        if len(matches) > expected_count:
            raise ChatwootContractError(
                "Chatwoot conversation filter returned too many rows"
            )
        page += 1

    raise ChatwootContractError(
        "Chatwoot conversation filter exceeded the pagination safety limit"
    )


async def filter_conversations_by_delivery_key(
    delivery_key: str,
) -> list[Mapping[str, Any]]:
    """Return every exact marker match across all Chatwoot result pages."""
    if not delivery_key:
        raise ValueError("delivery_key must not be empty")
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        return await _filter_conversations_by_delivery_key(client, delivery_key)


async def check_delivery_contract(
    inbox_ids: list[int] | tuple[int, ...] | set[int],
    *,
    bootstrap_attribute: bool = True,
) -> ChatwootContractReport:
    """Validate the installed Chatwoot contract required by durable delivery."""
    if not settings.chatwoot_base_url or not settings.chatwoot_api_token:
        raise ChatwootContractError("Chatwoot API settings are not configured")
    if settings.chatwoot_account_id is None or settings.chatwoot_account_id <= 0:
        raise ChatwootContractError("Chatwoot account scope is not configured")

    normalized_inboxes = tuple(sorted(set(inbox_ids)))
    if any(
        not isinstance(inbox_id, int)
        or isinstance(inbox_id, bool)
        or inbox_id <= 0
        for inbox_id in normalized_inboxes
    ):
        raise ValueError("inbox_ids must contain only positive integers")

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            attribute_created = await _ensure_delivery_definition(
                client,
                bootstrap=bootstrap_attribute,
            )
            for inbox_id in normalized_inboxes:
                response = await client.get(
                    f"{_base_url()}/inboxes/{inbox_id}",
                    headers=_headers(),
                )
                inbox = _contract_json(response, "get_inbox")
                if not isinstance(inbox, Mapping) or inbox.get("id") != inbox_id:
                    raise ChatwootContractError(
                        "Chatwoot inbox visibility response is incompatible"
                    )
                if inbox.get("lock_to_single_conversation") is not False:
                    raise ChatwootContractError(
                        f"Chatwoot inbox is locked to one conversation | inbox={inbox_id}"
                    )

            probe_key = str(uuid4())
            probe_matches = await _filter_conversations_by_delivery_key(
                client,
                probe_key,
            )
            if probe_matches:
                raise ChatwootContractError(
                    "Chatwoot contract probe marker unexpectedly exists"
                )
    except ChatwootContractError:
        raise
    except Exception as exc:
        raise ChatwootContractError(
            "Chatwoot contract check failed | "
            f"error_type={type(exc).__name__}"
        ) from None

    return ChatwootContractReport(
        account_id=settings.chatwoot_account_id,
        inbox_ids=normalized_inboxes,
        attribute_created=attribute_created,
    )


def _json_response(response: httpx.Response, operation: str) -> object:
    try:
        return response.json()
    except ValueError:
        raise ChatwootResponseError(
            f"Chatwoot retornou JSON inválido em {operation}"
        ) from None


def _extract_contacts(data: object) -> list[Mapping[str, Any]]:
    """Normaliza as variantes conhecidas do resultado de busca de contatos."""
    contacts: object

    if isinstance(data, list):
        contacts = data
    elif isinstance(data, Mapping):
        if "payload" in data:
            payload = data["payload"]
            if isinstance(payload, list):
                contacts = payload
            elif isinstance(payload, Mapping) and "contacts" in payload:
                contacts = payload["contacts"]
            else:
                raise ChatwootResponseError(
                    "Chatwoot retornou payload inválido na busca de contatos"
                )
        elif "contacts" in data:
            contacts = data["contacts"]
        else:
            raise ChatwootResponseError(
                "Chatwoot retornou resposta sem contatos na busca"
            )
    else:
        raise ChatwootResponseError(
            "Chatwoot retornou resposta inválida na busca de contatos"
        )

    if not isinstance(contacts, list):
        raise ChatwootResponseError(
            "Chatwoot retornou lista de contatos inválida"
        )
    if not all(isinstance(contact, Mapping) for contact in contacts):
        raise ChatwootResponseError(
            "Chatwoot retornou contato inválido na busca"
        )

    return contacts


def _extract_search_page(data: object) -> tuple[list[Mapping[str, Any]], bool]:
    """Normalize the installed search page and its pagination marker."""
    contacts = _extract_contacts(data)
    if not isinstance(data, Mapping) or "meta" not in data:
        return contacts, False

    meta = data["meta"]
    if not isinstance(meta, Mapping):
        raise ChatwootResponseError(
            "Chatwoot retornou metadados inválidos na busca de contatos"
        )

    has_more = meta.get("has_more", False)
    if not isinstance(has_more, bool):
        raise ChatwootResponseError(
            "Chatwoot retornou paginação inválida na busca de contatos"
        )
    return contacts, has_more


def _extract_resource_id(data: object, resource: str) -> int:
    """Extrai um ID positivo de envelopes compatíveis sem incluir o corpo no erro."""
    if not isinstance(data, Mapping):
        raise ChatwootResponseError(
            f"Chatwoot retornou resposta inválida ao criar {resource}"
        )

    candidates: list[Mapping[str, Any]] = [data]
    nested_resource = data.get(resource)
    if isinstance(nested_resource, Mapping):
        candidates.append(nested_resource)

    for wrapper_name in ("payload", "data"):
        wrapper = data.get(wrapper_name)
        if not isinstance(wrapper, Mapping):
            continue
        candidates.append(wrapper)
        nested_resource = wrapper.get(resource)
        if isinstance(nested_resource, Mapping):
            candidates.append(nested_resource)

    for candidate in candidates:
        resource_id = candidate.get("id")
        if isinstance(resource_id, int) and not isinstance(resource_id, bool) and resource_id > 0:
            return resource_id

    raise ChatwootResponseError(
        f"Chatwoot retornou resposta sem ID válido ao criar {resource}"
    )


def _normalize_phone_number(phone_number: object) -> str | None:
    if not isinstance(phone_number, str):
        return None

    normalized = "".join(character for character in phone_number if character in "0123456789")
    return normalized or None


def _find_exact_contact_id(
    contacts: list[Mapping[str, Any]],
    telefone: str,
) -> int | None:
    normalized_phone = _normalize_phone_number(telefone)
    if normalized_phone is None:
        raise ValueError("Telefone inválido para busca no Chatwoot")

    for contact in contacts:
        if _normalize_phone_number(contact.get("phone_number")) == normalized_phone:
            return _extract_resource_id(contact, "contact")

    return None


async def _search_exact_contact_id(
    client: httpx.AsyncClient,
    telefone: str,
) -> int | None:
    """Traverse the installed Chatwoot search contract until match/exhaustion."""
    page = 1
    while True:
        response = await client.get(
            f"{_base_url()}/contacts/search",
            headers=_headers(),
            params={
                "q": telefone,
                "include_contacts": "true",
                "page": page,
            },
        )
        response.raise_for_status()
        contacts, has_more = _extract_search_page(
            _json_response(response, "buscar contato")
        )
        contact_id = _find_exact_contact_id(contacts, telefone)
        if contact_id is not None:
            return contact_id
        if not has_more:
            return None
        page += 1


def _is_contact_create_conflict(exc: httpx.HTTPStatusError) -> bool:
    """Installed uniqueness validation is HTTP 422; keep 409 compatibility."""
    return exc.response.status_code in {409, 422}


async def get_or_create_contact(telefone: str, nome: Optional[str] = None) -> int:
    """
    Busca contato no Chatwoot pelo número de telefone.
    Se não encontrar, cria um novo.
    Retorna o contact_id (int).
    """
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            contact_id = await _search_exact_contact_id(client, telefone)
            if contact_id is not None:
                logger.debug("Chatwoot: contato encontrado | id={}", contact_id)
                return contact_id

            payload = {
                "name": nome or telefone,
                "phone_number": f"+{telefone}" if not telefone.startswith("+") else telefone,
            }
            resp = await client.post(
                f"{_base_url()}/contacts",
                headers=_headers(),
                json=payload,
            )
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if not _is_contact_create_conflict(exc):
                    raise

                contact_id = await _search_exact_contact_id(client, telefone)
                if contact_id is None:
                    raise
                logger.info(
                    "Chatwoot: contato localizado após conflito de criação | id={}",
                    contact_id,
                )
                return contact_id

            contact_id = _extract_resource_id(
                _json_response(resp, "criar contato"),
                "contact",
            )
            logger.info("Chatwoot: contato criado | id={}", contact_id)
            return contact_id
    except Exception as exc:
        logger.error(
            "Falha no cliente Chatwoot | operacao=get_or_create_contact | erro_tipo={}",
            type(exc).__name__,
        )
        raise


async def create_conversation(contact_id: int, inbox_id: int, resumo: str) -> int:
    """
    Cria uma conversa aberta no Chatwoot.
    Retorna o conversation_id (int).
    """
    payload = {
        "source_id": None,
        "inbox_id": inbox_id,
        "contact_id": contact_id,
        "status": "open",
        "message": {"content": resumo},
    }
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.post(f"{_base_url()}/conversations", headers=_headers(), json=payload)
            resp.raise_for_status()
            conversation_id = _extract_resource_id(
                _json_response(resp, "criar conversa"),
                "conversation",
            )
            logger.info(
                "Chatwoot: conversa criada | id={} | contact={} | inbox={}",
                conversation_id,
                contact_id,
                inbox_id,
            )
            return conversation_id
    except Exception as exc:
        logger.error(
            "Falha no cliente Chatwoot | operacao=create_conversation | "
            "contact={} | inbox={} | erro_tipo={}",
            contact_id,
            inbox_id,
            type(exc).__name__,
        )
        raise


async def add_message_to_conversation(conversation_id: int, content: str) -> None:
    """
    Adiciona uma mensagem a uma conversa existente.
    """
    payload = {"content": content, "message_type": "outgoing"}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.post(f"{_base_url()}/conversations/{conversation_id}/messages", headers=_headers(), json=payload)
            resp.raise_for_status()
            logger.debug("Chatwoot: mensagem adicionada | conversa={}", conversation_id)
    except Exception as exc:
        logger.error(
            "Falha no cliente Chatwoot | operacao=add_message_to_conversation | "
            "conversa={} | erro_tipo={}",
            conversation_id,
            type(exc).__name__,
        )
        raise


async def assign_conversation(conversation_id: int, assignee_id: int) -> None:
    """
    Atribui uma conversa a um atendente específico.
    """
    payload = {"assignee_id": assignee_id}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.post(f"{_base_url()}/conversations/{conversation_id}/assignments", headers=_headers(), json=payload)
            resp.raise_for_status()
            logger.info(
                "Chatwoot: conversa {} atribuída ao agente {}",
                conversation_id,
                assignee_id,
            )
    except Exception as exc:
        logger.error(
            "Falha no cliente Chatwoot | operacao=assign_conversation | "
            "conversa={} | assignee={} | erro_tipo={}",
            conversation_id,
            assignee_id,
            type(exc).__name__,
        )
        raise
