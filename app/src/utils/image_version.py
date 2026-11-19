"""Immutable image identity, outside /app so development mounts cannot hide it."""
from collections import Counter
import os
from pathlib import Path
import re

from loguru import logger

VERSION_FILE = Path("/etc/wpp-app/IMAGE_CODE_VERSION")
_signals: Counter[str] = Counter()


class ImageVersionError(RuntimeError):
    """Named errors never carry environment values or credentials."""


def _signal(code: str, *, warning: bool = False) -> None:
    _signals[code] += 1
    log = logger.warning if warning else logger.error
    log("Image identity | reason={} count={} threshold=1", code, _signals[code])


def signal_counts() -> dict[str, int]:
    return dict(_signals)


def image_code_version(configured: str) -> str:
    if os.environ.get("WPP_DEV_BIND_CODE", "false").lower() == "true" and not _signals["dev_bind_code_unattested"]:
        _signal("dev_bind_code_unattested", warning=True)
    required = os.environ.get("WPP_REQUIRE_IMAGE_VERSION", "false").lower()
    if required not in ("true", "false"):
        raise ImageVersionError("image_version_required_flag_invalid")
    try:
        version = VERSION_FILE.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        if required == "true":
            raise ImageVersionError("image_version_file_missing") from None
        if not _signals["image_version_source_fallback"]:
            _signal("image_version_source_fallback", warning=True)
        return configured
    except (OSError, UnicodeError):
        raise ImageVersionError("image_version_file_unreadable") from None
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", version):
        raise ImageVersionError("image_version_file_invalid")
    return version


def require_runtime_version(configured: str) -> None:
    try:
        if image_code_version(configured) != configured:
            raise ImageVersionError("image_version_mismatch")
    except ImageVersionError as exc:
        _signal(str(exc))
        raise SystemExit(2) from None
