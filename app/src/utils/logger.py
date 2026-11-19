from loguru import logger
import sys


# S4/P3-8: exception logs never render frame locals (diagnose) nor extended
# frames (backtrace). Every process entrypoint configures handlers here.
SAFE_HANDLER_OPTIONS = {"backtrace": False, "diagnose": False}


def setup_logger(log_file: str | None = None, level: str = "INFO"):
    """
    Replace every Loguru handler with privacy-safe ones.

    Args:
        log_file: Optional rotating log file (app only); CLIs/workers use stderr.
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # Remove default handler (diagnose=True) and any previous configuration
    logger.remove()

    # stderr keeps stdout free for CLI JSON output.
    logger.add(sys.stderr, level=level, **SAFE_HANDLER_OPTIONS)

    if log_file is not None:
        logger.add(
            log_file,
            rotation="1 day",
            retention="7 days",
            level=level,
            **SAFE_HANDLER_OPTIONS,
        )

    return logger
