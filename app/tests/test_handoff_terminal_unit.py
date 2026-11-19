import asyncio
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from src import delivery_worker
from src.services import delivery_adapters
from src.services.handoff_context import (
    LateInputRecord,
    parse_coverage_id,
    plan_context_chunks,
)


class _Response:
    def __init__(self, payload: object, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status
        self.request = httpx.Request("GET", "http://chatwoot.invalid")
        self.headers: dict[str, str] = {}
        self.content = b"{}"

    def json(self) -> object:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "provider failure",
                request=self.request,
                response=httpx.Response(self.status_code, request=self.request),
            )


class _Context:
    def __init__(self, client: object) -> None:
        self.client = client

    async def __aenter__(self) -> object:
        return self.client

    async def __aexit__(self, *args: object) -> None:
        return None


def _chunks(limit: int = 150_000):
    return plan_context_chunks(
        73,
        (
            LateInputRecord(11, 2, "primeiro 👋"),
            LateInputRecord(12, 3, "segundo\nsem truncar"),
        ),
        maximum_characters=limit,
    )


def _body_contents(chunks):
    return [
        json.loads(line)["content"]
        for chunk in chunks
        for line in chunk.content.split("\n")[1:]
    ]


def _payload(chunk):
    return {
        "attempt_id": 73,
        "handoff_key": "handoff-73",
        "inbox_id": 7,
        "source_id": chunk.source_id,
        "content": chunk.content,
        "body_sha256": chunk.body_sha256,
        "chunk": chunk.ordinal,
        "chunks": chunk.total,
        "coverage": list(chunk.coverage_ids),
    }


class ContextChunkTests(unittest.TestCase):
    def test_marker_uses_attempt_and_exact_membership_coverage(self) -> None:
        chunks = _chunks()
        self.assertEqual(len(chunks), 1)
        chunk = chunks[0]
        self.assertTrue(chunk.source_id.startswith("wppctx:v1:73:0001:0001:"))
        self.assertEqual(chunk.coverage_ids, ("membership:11", "membership:12"))
        self.assertIn("primeiro 👋", chunk.content)
        self.assertIn("segundo\\nsem truncar", chunk.content)
        self.assertEqual(_chunks(), chunks)

    def test_ordered_chunking_is_complete_and_within_full_rendered_limit(self) -> None:
        records = tuple(
            LateInputRecord(index, index, f"{index}:" + "á" * 90)
            for index in range(1, 8)
        )
        chunks = plan_context_chunks(91, records, maximum_characters=430)
        self.assertGreater(len(chunks), 1)
        self.assertEqual(
            [membership for chunk in chunks for membership in chunk.membership_ids],
            list(range(1, 8)),
        )
        self.assertTrue(all(len(chunk.content) <= 430 for chunk in chunks))
        self.assertEqual(
            [(chunk.ordinal, chunk.total) for chunk in chunks],
            [(index, len(chunks)) for index in range(1, len(chunks) + 1)],
        )

    def test_oversized_record_splits_into_ordered_parts_without_truncation(self) -> None:
        chunks = plan_context_chunks(
            1, (LateInputRecord(1, 1, "x" * 1000),), maximum_characters=450
        )
        self.assertGreater(len(chunks), 1)
        coverage = [item for chunk in chunks for item in chunk.coverage_ids]
        total = len(coverage)
        self.assertEqual(
            coverage, [f"membership:1#part{n:04d}/{total:04d}" for n in range(1, total + 1)]
        )
        self.assertTrue(all(len(chunk.content) <= 450 for chunk in chunks))
        self.assertEqual("".join(_body_contents(chunks)), "x" * 1000)

    def test_escape_heavy_records_budget_on_the_rendered_note(self) -> None:
        # Each class expands under JSON escaping or is multi-unit Unicode.
        samples = {
            "control": "\x01" * 30000,
            "quote_backslash": '"\\' * 20000,
            "newline_tab": "\n\t\r\b\f" * 12000,
            "astral": "😀" * 60000,
            "combining": "e\u0301" * 30000,
            "mixed": ("\x02a\"é😀\\" * 9000),
        }
        records = tuple(
            LateInputRecord(index, index, text)
            for index, text in enumerate(samples.values(), start=1)
        )
        chunks = plan_context_chunks(77, records)
        self.assertTrue(all(len(chunk.content) <= 150_000 for chunk in chunks))
        rebuilt: dict[int, str] = {}
        for chunk in chunks:
            for line in chunk.content.split("\n")[1:]:
                row = json.loads(line)
                membership_id, _, _ = parse_coverage_id(row["coverage_id"])
                rebuilt[membership_id] = rebuilt.get(membership_id, "") + row["content"]
        self.assertEqual(rebuilt, {record.membership_id: record.content for record in records})
        for chunk in chunks:
            delivery_adapters.decode_chatwoot_context_note(
                "chatwoot.add_private_note.v1",
                1,
                {
                    "attempt_id": 77,
                    "handoff_key": "h",
                    "inbox_id": 1,
                    "source_id": chunk.source_id,
                    "content": chunk.content,
                    "body_sha256": chunk.body_sha256,
                    "chunk": chunk.ordinal,
                    "chunks": chunk.total,
                    "coverage": list(chunk.coverage_ids),
                },
            )

    def test_planner_only_raises_below_fixed_header_overhead(self) -> None:
        with self.assertRaisesRegex(ValueError, "header overhead"):
            plan_context_chunks(1, (LateInputRecord(1, 1, "x" * 10),), maximum_characters=200)
        largest_message = "\x01" * 65535  # mensagens.conteudo TEXT bound
        chunks = plan_context_chunks(10**9, (LateInputRecord(10**12, 1, largest_message),))
        self.assertTrue(all(len(chunk.content) <= 150_000 for chunk in chunks))

    def test_part_coverage_ids_are_strict(self) -> None:
        self.assertEqual(parse_coverage_id("membership:5"), (5, None, None))
        self.assertEqual(parse_coverage_id("membership:5#part0002/0003"), (5, 2, 3))
        for bad in (
            "membership:0",
            "membership:5#part0004/0003",
            "membership:5#part0001/0001",
            "membership:5#part1/3",
            "membership:5 ",
            "membership:05",
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                parse_coverage_id(bad)

    def test_invalid_identity_and_order_fail_closed(self) -> None:
        with self.assertRaises(ValueError):
            plan_context_chunks(0, (LateInputRecord(1, 1, "x"),))
        with self.assertRaisesRegex(ValueError, "ordered"):
            plan_context_chunks(
                1,
                (LateInputRecord(1, 2, "x"), LateInputRecord(2, 1, "y")),
            )
        with self.assertRaisesRegex(ValueError, "unique"):
            plan_context_chunks(
                1,
                (LateInputRecord(1, 1, "x"), LateInputRecord(1, 2, "y")),
            )


class ContextNotePayloadTests(unittest.TestCase):
    def test_decoder_accepts_only_the_immutable_exact_payload(self) -> None:
        original = _payload(_chunks()[0])
        decoded = delivery_adapters.decode_chatwoot_context_note(
            "chatwoot.add_private_note.v1", 1, original
        )
        self.assertEqual(decoded.coverage, ("membership:11", "membership:12"))
        for field, replacement in (
            ("attempt_id", 74),
            ("source_id", "wppctx:v1:73:0001:0001:" + "0" * 64),
            ("body_sha256", "0" * 64),
            ("coverage", ["membership:11"]),
            ("content", original["content"] + "x"),
        ):
            mutated = dict(original)
            mutated[field] = replacement
            with self.subTest(field=field), self.assertRaises(
                delivery_adapters.InvalidDeliveryPayload
            ):
                delivery_adapters.decode_chatwoot_context_note(
                    "chatwoot.add_private_note.v1", 1, mutated
                )

    def test_decoder_counts_unicode_characters_not_bytes(self) -> None:
        record = LateInputRecord(1, 1, "á" * 200)
        chunk = plan_context_chunks(9, (record,), maximum_characters=1000)[0]
        payload = {
            "attempt_id": 9,
            "handoff_key": "h",
            "inbox_id": 1,
            "source_id": chunk.source_id,
            "content": chunk.content,
            "body_sha256": chunk.body_sha256,
            "chunk": 1,
            "chunks": 1,
            "coverage": list(chunk.coverage_ids),
        }
        self.assertGreater(len(chunk.content.encode("utf-8")), len(chunk.content))
        delivery_adapters.decode_chatwoot_context_note(
            "chatwoot.add_private_note.v1", 1, payload
        )


class ContextNoteAdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.payload = delivery_adapters.decode_chatwoot_context_note(
            "chatwoot.add_private_note.v1", 1, _payload(_chunks()[0])
        )
        self.conversation = {
            "id": 33,
            "account_id": delivery_adapters.settings.chatwoot_account_id,
            "inbox_id": 7,
            "contact_id": 9,
        }

    def _note(self, note_id: int = 44) -> dict[str, object]:
        return {
            "id": note_id,
            "conversation_id": 33,
            "inbox_id": 7,
            "message_type": 1,
            "private": True,
            "source_id": self.payload.source_id,
            "content": self.payload.content,
        }

    async def test_create_validates_mapping_and_posts_exact_private_payload(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=[
                    _Response(self.conversation),
                    _Response({"payload": [], "meta": {"contact": {"id": 9}}}),
                ]
            ),
            post=AsyncMock(return_value=_Response(self._note(), 201)),
        )
        with patch.object(
            delivery_adapters.httpx, "AsyncClient", return_value=_Context(client)
        ):
            result = await delivery_adapters.create_or_reconcile_chatwoot_context_note(
                self.payload,
                contact_id=9,
                conversation_id=33,
                timeout_seconds=10,
            )
        self.assertEqual(result.external_id, "44")
        self.assertEqual(
            client.post.await_args.kwargs["json"],
            {
                "content": self.payload.content,
                "private": True,
                "message_type": "outgoing",
                "source_id": self.payload.source_id,
            },
        )

    async def test_exact_recovery_after_remote_accept_avoids_second_post(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=[
                    _Response(self.conversation),
                    _Response(
                        {
                            "payload": [self._note()],
                            "meta": {"contact": {"id": 9}},
                        }
                    ),
                ]
            ),
            post=AsyncMock(),
        )
        with patch.object(
            delivery_adapters.httpx, "AsyncClient", return_value=_Context(client)
        ):
            result = await delivery_adapters.create_or_reconcile_chatwoot_context_note(
                self.payload,
                contact_id=9,
                conversation_id=33,
                timeout_seconds=10,
                reconcile_only=True,
            )
        self.assertTrue(result.reconciled)
        self.assertEqual(result.external_id, "44")
        client.post.assert_not_awaited()

    async def test_zero_exact_recovery_is_unknown_not_safe_to_retry(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=[
                    _Response(self.conversation),
                    _Response({"payload": [], "meta": {"contact": {"id": 9}}}),
                ]
            ),
            post=AsyncMock(),
        )
        with patch.object(
            delivery_adapters.httpx, "AsyncClient", return_value=_Context(client)
        ):
            result = await delivery_adapters.create_or_reconcile_chatwoot_context_note(
                self.payload,
                contact_id=9,
                conversation_id=33,
                timeout_seconds=10,
                reconcile_only=True,
            )
        self.assertIsNone(result)
        client.post.assert_not_awaited()

    async def test_wrong_account_inbox_or_contact_fails_before_post(self) -> None:
        for field, value in (("account_id", 999), ("inbox_id", 8), ("contact_id", 10)):
            conversation = dict(self.conversation)
            conversation[field] = value
            client = SimpleNamespace(
                get=AsyncMock(return_value=_Response(conversation)), post=AsyncMock()
            )
            with (
                self.subTest(field=field),
                patch.object(
                    delivery_adapters.httpx,
                    "AsyncClient",
                    return_value=_Context(client),
                ),
                self.assertRaises(delivery_adapters.ContextNoteOwnershipMismatch),
            ):
                await delivery_adapters.create_or_reconcile_chatwoot_context_note(
                    self.payload,
                    contact_id=9,
                    conversation_id=33,
                    timeout_seconds=10,
                )
            client.post.assert_not_awaited()

    async def test_duplicate_exact_markers_are_terminal(self) -> None:
        client = SimpleNamespace(
            get=AsyncMock(
                side_effect=[
                    _Response(self.conversation),
                    _Response(
                        {
                            "payload": [self._note(44), self._note(45)],
                            "meta": {"contact": {"id": 9}},
                        }
                    ),
                ]
            ),
            post=AsyncMock(),
        )
        with (
            patch.object(
                delivery_adapters.httpx, "AsyncClient", return_value=_Context(client)
            ),
            self.assertRaises(delivery_adapters.DuplicateRemoteEffect),
        ):
            await delivery_adapters.create_or_reconcile_chatwoot_context_note(
                self.payload,
                contact_id=9,
                conversation_id=33,
                timeout_seconds=10,
                reconcile_only=True,
            )
        client.post.assert_not_awaited()


class _Session:
    def __init__(self) -> None:
        self.commit = AsyncMock()

    async def __aenter__(self) -> "_Session":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class ContextNoteWorkerTests(unittest.IsolatedAsyncioTestCase):
    """Worker rules: an ambiguous note is reconciled exactly or parked dead."""

    def _claimed(self) -> object:
        return delivery_worker.delivery_worker_repo.ClaimedDelivery(
            id=41,
            lease_token="11111111-1111-4111-8111-111111111111",
            provider="chatwoot",
            operation="chatwoot.add_private_note.v1",
            payload_version=1,
            payload=_payload(_chunks()[0]),
            delivery_key="22222222-2222-4222-8222-222222222222",
            agente_id=1,
            sessao_id=2,
            handoff_attempt_id=73,
            attempt_count=1,
            cutover_epoch="33333333-3333-4333-8333-333333333333",
        )

    async def _dispatch(self, *, initial, reconcile, post_sent=True):
        ownership = delivery_worker.delivery_worker_repo.ContextNoteDispatchOwnership(
            handoff_key="handoff-73",
            inbox_id=7,
            contact_id=9,
            conversation_id=33,
            status="ready",
        )
        complete = AsyncMock(return_value=True)
        fail = AsyncMock(return_value="dead")
        create = AsyncMock(side_effect=reconcile)

        async def dispatch(_self, delivery, *, deadline_at, progress=None):
            if progress is not None and post_sent:
                progress["post_sent"] = True
            raise initial
        with (
            patch.object(
                delivery_worker.DeliveryWorker,
                "_dispatch_chatwoot",
                side_effect=dispatch,
                autospec=True,
            ),
            patch.object(delivery_worker, "create_or_reconcile_chatwoot_context_note", create),
            patch.object(
                delivery_worker.delivery_worker_repo,
                "get_context_note_dispatch_ownership",
                AsyncMock(return_value=ownership),
            ),
            patch.object(
                delivery_worker.delivery_worker_repo,
                "complete_chatwoot_context_note",
                complete,
            ),
            patch.object(delivery_worker.delivery_worker_repo, "fail_processing", fail),
            patch.object(
                delivery_worker.DeliveryWorker, "_renew_loop", AsyncMock(return_value=None)
            ),
            patch.object(delivery_worker, "AsyncSessionLocal", side_effect=lambda: _Session()),
        ):
            await delivery_worker.DeliveryWorker()._dispatch_one(self._claimed())
        return complete, fail, create

    async def test_remote_accept_with_lost_ack_completes_by_exact_marker(self) -> None:
        request = httpx.Request("POST", "http://chatwoot.invalid")
        complete, fail, create = await self._dispatch(
            initial=httpx.ReadTimeout("lost ack", request=request),
            reconcile=[
                delivery_adapters.DeliveryResult(
                    external_id="44",
                    http_status=200,
                    contact_id=9,
                    conversation_id=33,
                    reconciled=True,
                )
            ],
        )
        self.assertTrue(create.await_args.kwargs["reconcile_only"])
        complete.assert_awaited_once()
        fail.assert_not_awaited()

    async def test_ambiguous_create_without_exact_match_is_never_retried(self) -> None:
        request = httpx.Request("POST", "http://chatwoot.invalid")
        cases = {
            "zero_matches": (httpx.ReadTimeout("lost", request=request), [None]),
            "scan_transport_error": (
                httpx.ReadTimeout("lost", request=request),
                httpx.ConnectError("scan failed", request=request),
            ),
            "gateway_5xx": (
                httpx.HTTPStatusError(
                    "bad gateway",
                    request=request,
                    response=httpx.Response(502, request=request),
                ),
                [None],
            ),
            "unverifiable_response": (
                delivery_adapters.ProviderResultError("bad body"),
                delivery_adapters.ProviderResultError("bad scan"),
            ),
        }
        for name, (initial, reconcile) in cases.items():
            with self.subTest(case=name):
                complete, fail, _ = await self._dispatch(
                    initial=initial, reconcile=reconcile
                )
                complete.assert_not_awaited()
                kwargs = fail.await_args.kwargs
                self.assertEqual(kwargs["error_code"], "context_note_ambiguous")
                self.assertIsNone(kwargs["retry_delay_seconds"])
                self.assertTrue(kwargs["ambiguous"])

    async def test_ownership_mismatch_is_terminal_without_ambiguity(self) -> None:
        complete, fail, create = await self._dispatch(
            initial=delivery_adapters.ContextNoteOwnershipMismatch("wrong inbox"),
            reconcile=[None],
            post_sent=False,  # mapping verification runs before any POST
        )
        create.assert_not_awaited()
        complete.assert_not_awaited()
        kwargs = fail.await_args.kwargs
        self.assertIsNone(kwargs["retry_delay_seconds"])
        self.assertFalse(kwargs["ambiguous"])

    async def test_pre_post_failure_is_retryable_without_ambiguity(self) -> None:
        request = httpx.Request("GET", "http://chatwoot.invalid")
        cases = {
            "scan_timeout": httpx.ReadTimeout("scan", request=request),
            "mapping_5xx": httpx.HTTPStatusError(
                "bad gateway", request=request, response=httpx.Response(503, request=request)
            ),
            "scan_unverifiable": delivery_adapters.ProviderResultError("listing"),
        }
        for name, initial in cases.items():
            with self.subTest(case=name):
                complete, fail, create = await self._dispatch(
                    initial=initial, reconcile=[None], post_sent=False
                )
                create.assert_not_awaited()
                complete.assert_not_awaited()
                kwargs = fail.await_args.kwargs
                self.assertEqual(kwargs["error_code"], "context_note_pre_post_failure")
                self.assertIsNotNone(kwargs["retry_delay_seconds"])
                self.assertFalse(kwargs["ambiguous"])

    async def test_adapter_marks_post_sent_only_immediately_before_create(self) -> None:
        payload = delivery_adapters.decode_chatwoot_context_note(
            "chatwoot.add_private_note.v1", 1, _payload(_chunks()[0])
        )
        conversation = {
            "id": 33,
            "account_id": delivery_adapters.settings.chatwoot_account_id,
            "inbox_id": 7,
            "contact_id": 9,
        }
        request = httpx.Request("GET", "http://chatwoot.invalid")
        for name, get_effect, expected in (
            ("scan_fails", [_Response(conversation), httpx.ReadTimeout("x", request=request)], False),
            ("post_fails", [_Response(conversation), _Response({"payload": [], "meta": {"contact": {"id": 9}}})], True),
        ):
            progress = {"post_sent": False}
            client = SimpleNamespace(
                get=AsyncMock(side_effect=get_effect),
                post=AsyncMock(side_effect=httpx.ReadTimeout("post", request=request)),
            )
            with (
                self.subTest(case=name),
                patch.object(delivery_adapters.httpx, "AsyncClient", return_value=_Context(client)),
                self.assertRaises(httpx.ReadTimeout),
            ):
                await delivery_adapters.create_or_reconcile_chatwoot_context_note(
                    payload, contact_id=9, conversation_id=33, timeout_seconds=10,
                    progress=progress,
                )
            self.assertEqual(progress["post_sent"], expected)

    async def test_unknown_exception_after_post_is_ambiguous_never_retried(self) -> None:
        complete, fail, create = await self._dispatch(
            initial=RuntimeError("unexpected after create"), reconcile=[None], post_sent=True
        )
        complete.assert_not_awaited()
        kwargs = fail.await_args.kwargs
        self.assertEqual(kwargs["error_code"], "context_note_ambiguous")
        self.assertIsNone(kwargs["retry_delay_seconds"])
        self.assertTrue(kwargs["ambiguous"])
        # The same unknown failure before the POST stays a plain retry.
        complete, fail, create = await self._dispatch(
            initial=RuntimeError("unexpected before create"), reconcile=[None], post_sent=False
        )
        kwargs = fail.await_args.kwargs
        self.assertEqual(kwargs["error_code"], "unexpected_dispatch_failure")
        self.assertIsNotNone(kwargs["retry_delay_seconds"])
        self.assertFalse(kwargs["ambiguous"])
