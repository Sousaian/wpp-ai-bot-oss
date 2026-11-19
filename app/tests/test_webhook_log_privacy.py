import io
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from loguru import logger

from src import main


class EvolutionWebhookLogPrivacyTests(unittest.IsolatedAsyncioTestCase):
    async def test_malformed_and_non_text_messages_never_log_complete_phone(self) -> None:
        phone = "15550100999"
        agente = SimpleNamespace(id=3)
        payloads = (
            (
                {
                    "event": "messages.upsert",
                    "instance": "instancia-a",
                    "data": {
                        "key": {"fromMe": False, "remoteJid": f"{phone}@s.whatsapp.net"},
                    },
                },
                "missing_message_content",
            ),
            (
                {
                    "event": "messages.upsert",
                    "instance": "instancia-a",
                    "data": {
                        "key": {"fromMe": False, "remoteJid": f"{phone}@s.whatsapp.net"},
                        "message": {"imageMessage": {"caption": ""}},
                    },
                },
                "no_text",
            ),
        )

        for payload, reason in payloads:
            with self.subTest(reason=reason):
                stream = io.StringIO()
                sink_id = logger.add(stream, format="{message}\n")
                request = SimpleNamespace(
                    headers={
                        "Authorization": (
                            f"Bearer {main.settings.evolution_webhook_bearer_token}"
                        )
                    },
                    json=AsyncMock(return_value=payload),
                )
                try:
                    with patch.object(
                        main.agente_repo,
                        "get_by_instance_name",
                        AsyncMock(return_value=agente),
                    ):
                        response = await main.webhook_handler(request, SimpleNamespace())
                finally:
                    logger.remove(sink_id)

                self.assertEqual(response, {"status": "ignored", "reason": reason})
                self.assertNotIn(phone, stream.getvalue())

    def test_limit_message_does_not_promise_uninitiated_human_handoff(self) -> None:
        normalized_message = main.MENSAGEM_LIMITE_ATINGIDO.casefold()

        self.assertNotIn("especialista", normalized_message)
        self.assertNotIn("atendente", normalized_message)
        self.assertIn("tente novamente", normalized_message)


if __name__ == "__main__":
    unittest.main()
