"""Privacy filters for HTTP access logs."""

from __future__ import annotations

import logging
import re
from typing import Any


REDACTED_PHONE = "[REDACTED_PHONE]"

# Match 8-15 digit phone-like values, including common formatting and URL-
# encoded digits/separators. The request target is only copied for logging; the
# ASGI scope used by routing is never changed.
_DIGIT = r"(?:[0-9]|%3[0-9])"
_SEPARATOR = r"(?:[ +.()\-]|%(?:20|28|29|2[dDeE]))*"
_PHONE_LIKE = re.compile(
    rf"(?<![A-Za-z0-9])(?:\+|%2[bB])?{_DIGIT}(?:{_SEPARATOR}{_DIGIT}){{7,14}}"
    r"(?![A-Za-z0-9])"
)


def redact_phone_like_values(request_target: str) -> str:
    """Redact complete phone-like values from a logged path/query string."""
    return _PHONE_LIKE.sub(REDACTED_PHONE, request_target)


class PhoneRedactingAccessFilter(logging.Filter):
    """Redact the request target in Uvicorn's structured access-log arguments."""

    _wpp_phone_redaction_filter = True

    def filter(self, record: logging.LogRecord) -> bool:
        args: Any = record.args
        if isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str):
            redacted_args = list(args)
            redacted_args[2] = redact_phone_like_values(args[2])
            record.args = tuple(redacted_args)

        return True


def configure_uvicorn_access_log_redaction() -> None:
    """Install the access-log filter once while leaving access logging enabled."""
    access_logger = logging.getLogger("uvicorn.access")
    if any(
        getattr(item, "_wpp_phone_redaction_filter", False)
        for item in access_logger.filters
    ):
        return

    access_logger.addFilter(PhoneRedactingAccessFilter())
