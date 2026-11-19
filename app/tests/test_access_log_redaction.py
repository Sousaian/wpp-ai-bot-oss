import logging
import unittest

from uvicorn.logging import AccessFormatter

from src.utils.access_log import (
    REDACTED_PHONE,
    PhoneRedactingAccessFilter,
    configure_uvicorn_access_log_redaction,
    redact_phone_like_values,
)


class RequestTargetRedactionTests(unittest.TestCase):
    def test_redacts_plain_and_formatted_phone_values_without_losing_shape(self) -> None:
        request_target = (
            "/sessoes/5511999999999/historico"
            "?q=%2B55%20(11)%2099999-9999&handler=bot"
        )

        redacted = redact_phone_like_values(request_target)

        self.assertEqual(
            redacted,
            f"/sessoes/{REDACTED_PHONE}/historico"
            f"?q={REDACTED_PHONE}&handler=bot",
        )
        self.assertNotIn("5511999999999", redacted)
        self.assertNotIn("99999-9999", redacted)

    def test_redacts_percent_encoded_digits(self) -> None:
        request_target = "/contacts/search?q=%35%35%31%31%39%39%39%39%39%39%39%39%39"

        redacted = redact_phone_like_values(request_target)

        self.assertEqual(redacted, f"/contacts/search?q={REDACTED_PHONE}")

    def test_preserves_short_ids_and_safe_query_metadata(self) -> None:
        request_target = "/agentes/1729?status=active&request_id=abc-123"

        self.assertEqual(redact_phone_like_values(request_target), request_target)


class UvicornAccessFilterTests(unittest.TestCase):
    def test_formatted_access_log_keeps_safe_fields_and_redacts_only_target(self) -> None:
        phone = "5511999999999"
        record = logging.LogRecord(
            name="uvicorn.access",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='%s - "%s %s HTTP/%s" %d',
            args=("127.0.0.1:43210", "GET", f"/sessoes/{phone}", "1.1", 200),
            exc_info=None,
        )

        accepted = PhoneRedactingAccessFilter().filter(record)
        rendered = AccessFormatter().format(record)

        self.assertTrue(accepted)
        self.assertNotIn(phone, rendered)
        self.assertIn("127.0.0.1:43210", rendered)
        self.assertIn(f'GET /sessoes/{REDACTED_PHONE} HTTP/1.1', rendered)
        self.assertIn('HTTP/1.1" 200', rendered)

    def test_configuration_is_idempotent_and_access_logger_remains_enabled(self) -> None:
        access_logger = logging.getLogger("uvicorn.access")
        original_filters = list(access_logger.filters)
        original_disabled = access_logger.disabled
        try:
            access_logger.filters = [
                item
                for item in access_logger.filters
                if not getattr(item, "_wpp_phone_redaction_filter", False)
            ]

            configure_uvicorn_access_log_redaction()
            configure_uvicorn_access_log_redaction()

            installed = [
                item
                for item in access_logger.filters
                if getattr(item, "_wpp_phone_redaction_filter", False)
            ]
            self.assertEqual(len(installed), 1)
            self.assertEqual(access_logger.disabled, original_disabled)
        finally:
            access_logger.filters = original_filters


if __name__ == "__main__":
    unittest.main()
