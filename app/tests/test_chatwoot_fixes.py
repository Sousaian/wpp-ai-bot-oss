import hashlib
import hmac
import io
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
from fastapi import Response
from loguru import logger

from src.database.repositories import sessao_repo
from src.routers import chatwoot as chatwoot_router
from src.services import chatwoot_client, handoff_manager


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self.payload


class _AsyncClientContext:
    def __init__(self, client: object) -> None:
        self.client = client

    async def __aenter__(self) -> object:
        return self.client

    async def __aexit__(self, *args: object) -> None:
        return None


def _signed_chatwoot_request(payload: object) -> SimpleNamespace:
    raw_body = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    timestamp = str(int(time.time()))
    digest = hmac.new(
        chatwoot_router.settings.chatwoot_webhook_secret.encode(),
        timestamp.encode() + b"." + raw_body,
        hashlib.sha256,
    ).hexdigest()
    return SimpleNamespace(
        body=AsyncMock(return_value=raw_body),
        headers={
            "X-Chatwoot-Timestamp": timestamp,
            "X-Chatwoot-Signature": f"sha256={digest}",
        },
    )


class ChatwootResponseParsingTests(unittest.TestCase):
    def test_contact_search_accepts_installed_and_compatible_wrappers(self) -> None:
        variants = (
            {"payload": [{"id": 11}]},
            {"payload": {"contacts": [{"id": 12}]}},
            {"contacts": [{"id": 13}]},
            [{"id": 14}],
        )

        extracted_ids = [
            chatwoot_client._extract_resource_id(
                chatwoot_client._extract_contacts(response)[0],
                "contact",
            )
            for response in variants
        ]

        self.assertEqual(extracted_ids, [11, 12, 13, 14])

    def test_contact_search_rejects_malformed_payload(self) -> None:
        with self.assertRaises(chatwoot_client.ChatwootResponseError):
            chatwoot_client._extract_contacts({"payload": {"unexpected": []}})

    def test_contact_search_rejects_malformed_pagination_marker(self) -> None:
        with self.assertRaises(chatwoot_client.ChatwootResponseError):
            chatwoot_client._extract_search_page(
                {"meta": {"has_more": "true"}, "payload": []}
            )

    def test_creation_ids_accept_installed_and_compatible_wrappers(self) -> None:
        contact_variants = (
            {"payload": {"contact": {"id": 21}}},
            {"payload": {"id": 22}},
            {"contact": {"id": 23}},
            {"id": 24},
        )
        conversation_variants = (
            {"payload": {"conversation": {"id": 31}}},
            {"data": {"id": 32}},
            {"conversation": {"id": 33}},
            {"id": 34},
        )

        self.assertEqual(
            [
                chatwoot_client._extract_resource_id(response, "contact")
                for response in contact_variants
            ],
            [21, 22, 23, 24],
        )
        self.assertEqual(
            [
                chatwoot_client._extract_resource_id(response, "conversation")
                for response in conversation_variants
            ],
            [31, 32, 33, 34],
        )

    def test_creation_id_rejects_missing_or_invalid_id(self) -> None:
        for response in (
            {"payload": {"contact": {}}},
            {"payload": {"contact": {"id": None}}},
            {"payload": {"contact": {"id": 0}}},
            {"payload": []},
        ):
            with self.subTest(response=response):
                with self.assertRaises(chatwoot_client.ChatwootResponseError):
                    chatwoot_client._extract_resource_id(response, "contact")


class ChatwootAsyncFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_chatwoot_client_failure_log_omits_response_and_phone(self) -> None:
        phone = "15550100999"
        raw_body = "raw response with api_access_token=secret"
        response = SimpleNamespace(
            raise_for_status=Mock(side_effect=RuntimeError(f"{raw_body} {phone}"))
        )
        client = SimpleNamespace(get=AsyncMock(return_value=response))
        stream = io.StringIO()
        sink_id = logger.add(stream, format="{message}\n")

        try:
            with patch.object(
                chatwoot_client.httpx,
                "AsyncClient",
                return_value=_AsyncClientContext(client),
            ):
                with self.assertRaises(RuntimeError):
                    await chatwoot_client.get_or_create_contact(phone)
        finally:
            logger.remove(sink_id)

        logs = stream.getvalue()
        self.assertIn("operacao=get_or_create_contact", logs)
        self.assertIn("erro_tipo=RuntimeError", logs)
        self.assertNotIn(phone, logs)
        self.assertNotIn(raw_body, logs)
        self.assertNotIn("api_access_token", logs)

    async def test_get_or_create_contact_uses_installed_creation_contract(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(return_value=_Response({"payload": []})),
            post=AsyncMock(
                return_value=_Response(
                    {
                        "payload": {
                            "contact": {"id": 21},
                            "contact_inbox": {"source_id": "source"},
                        }
                    }
                )
            ),
        )

        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            contact_id = await chatwoot_client.get_or_create_contact("15550100999")

        self.assertEqual(contact_id, 21)
        client.get.assert_awaited_once()
        client.post.assert_awaited_once()

    async def test_contact_search_selects_exact_normalized_phone_from_fuzzy_results(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                return_value=_Response(
                    {
                        "payload": [
                            {
                                "id": 20,
                                "name": "15550100999 appears only in the name",
                                "phone_number": "+1 555 999 0000",
                            },
                            {
                                "id": 21,
                                "phone_number": "+1 (555) 010-0999",
                            },
                        ]
                    }
                )
            ),
            post=AsyncMock(),
        )

        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            contact_id = await chatwoot_client.get_or_create_contact("15550100999")

        self.assertEqual(contact_id, 21)
        client.post.assert_not_awaited()

    async def test_contact_search_follows_pagination_until_exact_match(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=(
                    _Response(
                        {
                            "meta": {"current_page": 1, "has_more": True},
                            "payload": [
                                {"id": 20, "phone_number": "+1 555 999 0000"}
                            ],
                        }
                    ),
                    _Response(
                        {
                            "meta": {"current_page": 2, "has_more": False},
                            "payload": [
                                {"id": 21, "phone_number": "+1 (555) 010-0999"}
                            ],
                        }
                    ),
                )
            ),
            post=AsyncMock(),
        )

        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            contact_id = await chatwoot_client.get_or_create_contact("15550100999")

        self.assertEqual(contact_id, 21)
        self.assertEqual(
            [awaited.kwargs["params"]["page"] for awaited in client.get.await_args_list],
            [1, 2],
        )
        client.post.assert_not_awaited()

    async def test_contact_create_uniqueness_conflict_requeries_all_pages(self) -> None:
        request = httpx.Request("POST", "http://chatwoot.test/contacts")
        conflict = httpx.Response(422, request=request)
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=(
                    _Response(
                        {
                            "meta": {"current_page": 1, "has_more": False},
                            "payload": [],
                        }
                    ),
                    _Response(
                        {
                            "meta": {"current_page": 1, "has_more": True},
                            "payload": [
                                {"id": 30, "phone_number": "+1 555 999 0000"}
                            ],
                        }
                    ),
                    _Response(
                        {
                            "meta": {"current_page": 2, "has_more": False},
                            "payload": [
                                {"id": 31, "phone_number": "+1 (555) 010-0999"}
                            ],
                        }
                    ),
                )
            ),
            post=AsyncMock(return_value=conflict),
        )

        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            contact_id = await chatwoot_client.get_or_create_contact("15550100999")

        self.assertEqual(contact_id, 31)
        self.assertEqual(
            [awaited.kwargs["params"]["page"] for awaited in client.get.await_args_list],
            [1, 1, 2],
        )
        client.post.assert_awaited_once()

    async def test_create_conversation_validates_installed_wrapper(self) -> None:
        client = SimpleNamespace(
            post=AsyncMock(
                return_value=_Response(
                    {"payload": {"conversation": {"id": 31}}}
                )
            )
        )

        with patch.object(
            chatwoot_client.httpx,
            "AsyncClient",
            return_value=_AsyncClientContext(client),
        ):
            conversation_id = await chatwoot_client.create_conversation(
                contact_id=21,
                inbox_id=7,
                resumo="conteúdo privado",
            )

        self.assertEqual(conversation_id, 31)
        client.post.assert_awaited_once()

    async def test_session_lookup_eager_loads_agent(self) -> None:
        expected = SimpleNamespace(id=9)
        result = SimpleNamespace(scalar_one_or_none=lambda: expected)
        db = SimpleNamespace(execute=AsyncMock(return_value=result))

        actual = await sessao_repo.get_by_chatwoot_conversation_id(db, 88)

        self.assertIs(actual, expected)
        statement = db.execute.await_args.args[0]
        self.assertEqual(len(statement._with_options), 1)
        self.assertIn("Sessao.agente", str(statement._with_options[0].path))

    async def test_webhook_forwards_with_loaded_agent(self) -> None:
        request = _signed_chatwoot_request(
            {
                "event": "message_created",
                "data": {
                    "conversation": {"id": 88},
                    "message": {
                        "id": 77,
                        "content": "mensagem privada",
                        "sender": {"type": "agent"},
                        "message_type": "outgoing",
                        "private": False,
                    },
                },
            }
        )
        with patch.object(
            chatwoot_router.chatwoot_orchestrator,
            "orchestrate",
            AsyncMock(
                return_value=chatwoot_router.chatwoot_orchestrator.ChatwootOrchestrationResult(
                    {"status": "queued", "conversation": 88}, http_status=202
                )
            ),
        ) as orchestrate:
            http_response = Response()
            response = await chatwoot_router.chatwoot_webhook(
                request, http_response, SimpleNamespace()
            )

        self.assertEqual(response, {"status": "queued", "conversation": 88})
        self.assertEqual(http_response.status_code, 202)
        event = orchestrate.await_args.kwargs["event"]
        self.assertEqual((event.message_id, event.conversation_id, event.content), (77, 88, "mensagem privada"))

    async def test_native_chatwoot_message_created_payload_is_forwarded(self) -> None:
        request = _signed_chatwoot_request(
            {
                "event": "message_created",
                "id": 77,
                "content": "resposta do atendente",
                "conversation": {"id": 88},
                "message_type": "outgoing",
                "private": False,
                "sender": {"id": 3, "type": "user"},
            }
        )
        with patch.object(
            chatwoot_router.chatwoot_orchestrator,
            "orchestrate",
            AsyncMock(
                return_value=chatwoot_router.chatwoot_orchestrator.ChatwootOrchestrationResult(
                    {"status": "queued", "conversation": 88}, http_status=202
                )
            ),
        ) as orchestrate:
            response = await chatwoot_router.chatwoot_webhook(
                request, Response(), SimpleNamespace()
            )

        self.assertEqual(response, {"status": "queued", "conversation": 88})
        self.assertEqual(orchestrate.await_args.kwargs["event"].content, "resposta do atendente")

    async def test_webhook_rejects_unsafe_native_payload_variants_before_lookup(self) -> None:
        valid_payload = {
            "event": "message_created",
            "id": 77,
            "content": "não encaminhar",
            "conversation": {"id": 88},
            "message_type": "outgoing",
            "private": False,
            "sender": {"id": 3, "type": "user"},
        }
        variants = (
            ({**valid_payload, "event": "message_updated"}, "unsupported_event"),
            ({**valid_payload, "private": True}, "private_message"),
            ({**valid_payload, "message_type": "incoming"}, "not_outgoing"),
            ({**valid_payload, "sender": {"type": "contact"}}, "not_human_sender"),
            ({**valid_payload, "id": 0}, "invalid_message_id"),
            ({**valid_payload, "id": True}, "invalid_message_id"),
            ({**valid_payload, "conversation": {"id": "88"}}, "invalid_conversation_id"),
        )

        for payload, reason in variants:
            with self.subTest(reason=reason):
                request = _signed_chatwoot_request(payload)
                with patch.object(
                    chatwoot_router.chatwoot_orchestrator,
                    "orchestrate",
                    AsyncMock(),
                ) as orchestrate:
                    response = await chatwoot_router.chatwoot_webhook(
                        request,
                        Response(),
                        SimpleNamespace(),
                    )

                self.assertEqual(response, {"status": "ignored", "reason": reason})
                orchestrate.assert_not_awaited()

    async def test_handoff_uses_fallback_summary_when_monthly_limit_is_reached(self) -> None:
        phone = "15550100999"
        db = SimpleNamespace(refresh=AsyncMock())
        agente = SimpleNamespace(id=3, chatwoot_inbox_id=7, instance_name="instancia-a")
        sessao = SimpleNamespace(id=9)

        with (
            patch.object(
                handoff_manager.consumo_repo,
                "atingiu_limite_mensal",
                AsyncMock(return_value=True),
            ) as limit_check,
            patch.object(
                handoff_manager,
                "gerar_resumo",
                AsyncMock(),
            ) as gerar_resumo,
            patch.object(
                handoff_manager.chatwoot_client,
                "get_or_create_contact",
                AsyncMock(return_value=21),
            ),
            patch.object(
                handoff_manager.chatwoot_client,
                "create_conversation",
                AsyncMock(return_value=31),
            ) as create_conversation,
            patch.object(
                handoff_manager.evolution_client,
                "send_text_message",
                AsyncMock(),
            ),
            patch.object(
                handoff_manager.sessao_repo,
                "encerrar_sessao",
                AsyncMock(),
            ),
        ):
            await handoff_manager.executar_handoff_direto(agente, sessao, phone, db)

        limit_check.assert_awaited_once_with(3, db)
        gerar_resumo.assert_not_awaited()
        self.assertEqual(
            create_conversation.await_args.kwargs["resumo"],
            f"Handoff solicitado. Sessão: 9 | Lead: {phone}",
        )

    async def test_handoff_logs_only_safe_error_context_and_still_closes(self) -> None:
        phone = "15550100999"
        sensitive_exception_text = (
            "raw provider body with key fingerprint and https://provider.example/key/secret"
        )
        stream = io.StringIO()
        sink_id = logger.add(stream, format="{message}\n")
        db = SimpleNamespace(refresh=AsyncMock())
        agente = SimpleNamespace(id=3, chatwoot_inbox_id=7, instance_name="instancia-a")
        sessao = SimpleNamespace(id=9)

        try:
            with (
                patch.object(
                    handoff_manager.consumo_repo,
                    "atingiu_limite_mensal",
                    AsyncMock(return_value=False),
                ),
                patch.object(
                    handoff_manager,
                    "gerar_resumo",
                    AsyncMock(side_effect=RuntimeError(sensitive_exception_text)),
                ),
                patch.object(
                    handoff_manager.chatwoot_client,
                    "get_or_create_contact",
                    AsyncMock(return_value=21),
                ),
                patch.object(
                    handoff_manager.chatwoot_client,
                    "create_conversation",
                    AsyncMock(return_value=31),
                ),
                patch.object(
                    handoff_manager.evolution_client,
                    "send_text_message",
                    AsyncMock(side_effect=RuntimeError(f"failed for {phone}")),
                ),
                patch.object(
                    handoff_manager.sessao_repo,
                    "encerrar_sessao",
                    AsyncMock(),
                ) as close_session,
            ):
                await handoff_manager.executar_handoff_direto(agente, sessao, phone, db)
        finally:
            logger.remove(sink_id)

        logs = stream.getvalue()
        self.assertIn("erro_tipo=RuntimeError", logs)
        self.assertNotIn(phone, logs)
        self.assertNotIn(sensitive_exception_text, logs)
        self.assertNotIn("provider.example", logs)
        close_session.assert_awaited_once_with(
            db=db,
            sessao_id=9,
            motivo="handoff",
            chatwoot_conversation_id=31,
        )


if __name__ == "__main__":
    unittest.main()
