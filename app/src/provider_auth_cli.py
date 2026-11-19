import argparse
import asyncio
from collections.abc import Awaitable, Callable
import json
from pathlib import Path
import signal
import sys
import time

from loguru import logger

from src.utils.image_version import require_runtime_version
from src.config import settings
from src.database.connection import AsyncSessionLocal, engine
from src.utils.logger import setup_logger
from src.services.provider_auth_bootstrap import (
    AUTH_MODE_KEY,
    CHATWOOT_SECRET_KEY,
    CHATWOOT_WEBHOOK_ID_KEY,
    EVOLUTION_SECRET_KEY,
    RECEIPT_CURRENT_VERSION_KEY,
    RECEIPT_KEYS_KEY,
    ProviderAuthBootstrapError,
    ReadinessReport,
    active_provider_auth_readiness,
    generate_evolution_secret,
    probe_callback_authentication,
    provider_auth_readiness,
    provision_provider_auth,
    read_env_value,
    serialized_receipt_keyring,
    update_env_file,
    write_provider_auth_snapshot,
)


def _safe_print(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True))


class ProviderAuthAuditDeadlineError(ProviderAuthBootstrapError):
    """Sanitized whole-audit deadline exhaustion."""


async def _prepare(env_file: Path) -> None:
    evolution_secret = (
        settings.evolution_webhook_bearer_token
        or read_env_value(env_file, EVOLUTION_SECRET_KEY)
        or generate_evolution_secret()
    )
    chatwoot_secret = (
        settings.chatwoot_webhook_secret
        or read_env_value(env_file, CHATWOOT_SECRET_KEY)
    )
    receipt_current_version, receipt_keys = serialized_receipt_keyring(env_file)
    configured_webhook_id = settings.chatwoot_webhook_id
    if configured_webhook_id is None:
        stored_webhook_id = read_env_value(env_file, CHATWOOT_WEBHOOK_ID_KEY)
        if stored_webhook_id:
            try:
                configured_webhook_id = int(stored_webhook_id)
            except ValueError:
                raise ProviderAuthBootstrapError(
                    "Configured Chatwoot managed webhook ID is invalid"
                ) from None
            if configured_webhook_id <= 0:
                raise ProviderAuthBootstrapError(
                    "Configured Chatwoot managed webhook ID is invalid"
                )

    async with AsyncSessionLocal() as db:
        report, provider_chatwoot_secret = await provision_provider_auth(
            db,
            evolution_secret=evolution_secret,
            configured_chatwoot_secret=chatwoot_secret,
            managed_chatwoot_webhook_id=configured_webhook_id,
        )

    update_env_file(
        env_file,
        {
            EVOLUTION_SECRET_KEY: evolution_secret,
            CHATWOOT_SECRET_KEY: provider_chatwoot_secret,
            CHATWOOT_WEBHOOK_ID_KEY: str(report.chatwoot_webhook_id),
            RECEIPT_CURRENT_VERSION_KEY: receipt_current_version,
            RECEIPT_KEYS_KEY: receipt_keys,
            AUTH_MODE_KEY: "provisioning",
        },
    )
    _safe_print(
        {
            "status": "provisioned",
            "evolution_instances": report.evolution_instances,
            "chatwoot_webhook_created": report.chatwoot_webhook_created,
            "chatwoot_webhook_updated": report.chatwoot_webhook_updated,
            "chatwoot_obsolete_removed": report.chatwoot_obsolete_removed,
            "chatwoot_secret_replaced": report.chatwoot_secret_replaced,
            "delivery_attribute_created": report.delivery_attribute_created,
            "next": "restart_app_then_verify",
        }
    )


async def _verify(
    *,
    allow_staged: bool,
    probe: bool,
    emit: bool = True,
) -> None:
    async with AsyncSessionLocal() as db:
        report = await active_provider_auth_readiness(db)

    acceptable = {"authentication_not_enforced"} if allow_staged else set()
    unexpected = set(report.issues) - acceptable
    if unexpected or (not allow_staged and not report.ready):
        write_provider_auth_snapshot(report)
        if emit:
            _safe_print({"status": "not_ready", **report.as_dict()})
        raise ProviderAuthBootstrapError("Provider authentication is not ready")
    if probe:
        try:
            await probe_callback_authentication(require_rejection=not allow_staged)
        except Exception:
            failed_report = ReadinessReport(
                ready=False,
                auth_mode=report.auth_mode,
                evolution_instances=report.evolution_instances,
                chatwoot_inboxes=report.chatwoot_inboxes,
                issues=tuple(sorted(set(report.issues) | {"callback_probe_failed"})),
            )
            write_provider_auth_snapshot(failed_report)
            raise
    write_provider_auth_snapshot(report)
    if emit:
        _safe_print(
            {"status": "ready" if report.ready else "staged", **report.as_dict()}
        )


async def _monitor_loop(
    check: Callable[[], Awaitable[bool]],
    wait: Callable[[float], Awaitable[bool]],
    on_failure: Callable[[str], None],
    *,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    """Refresh on a start-to-start cadence until an audit or wait stops."""
    consecutive_failures = 0
    while True:
        attempt_started = monotonic()
        try:
            if not await check():
                return
        except Exception as exc:
            consecutive_failures += 1
            issue = (
                "provider_auth_audit_deadline_exceeded"
                if isinstance(exc, ProviderAuthAuditDeadlineError)
                else "provider_auth_monitor_failed"
            )
            try:
                on_failure(issue)
            except Exception as snapshot_exc:
                logger.error(
                    "Provider auth monitor could not persist failure | error_type={}",
                    type(snapshot_exc).__name__,
                )
            logger.warning(
                "Provider auth monitor check failed | error_type={} failures={}",
                type(exc).__name__,
                consecutive_failures,
            )
            exponent = min(consecutive_failures - 1, 30)
            delay = min(
                settings.provider_auth_monitor_retry_initial_seconds
                * (2**exponent),
                settings.provider_auth_monitor_retry_max_seconds,
            )
        else:
            consecutive_failures = 0
            delay = settings.provider_auth_monitor_interval_seconds
        elapsed = max(0.0, monotonic() - attempt_started)
        if await wait(max(0.0, float(delay) - elapsed)):
            return


async def _cancel_and_await_owned_task(
    task: asyncio.Task[object],
    *,
    timeout: float,
) -> None:
    """Cancel one monitor-owned task and consume its terminal state."""
    if not task.done():
        task.cancel()
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if task not in done:
        task.cancel()
        raise ProviderAuthBootstrapError(
            "Provider authentication audit did not stop within its shutdown deadline"
        )
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def _run_monitor_audit(
    check: Callable[[], Awaitable[None]],
    stop_event: asyncio.Event,
    *,
    audit_timeout: float,
    shutdown_timeout: float,
) -> bool:
    """Race one whole audit against its deadline and monitor shutdown."""
    if stop_event.is_set():
        return False
    audit_task = asyncio.create_task(check(), name="provider-auth-audit")
    stop_task = asyncio.create_task(
        stop_event.wait(), name="provider-auth-monitor-stop"
    )
    try:
        done, _ = await asyncio.wait(
            {audit_task, stop_task},
            timeout=audit_timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            await _cancel_and_await_owned_task(
                audit_task,
                timeout=shutdown_timeout,
            )
            return False
        if audit_task in done:
            await audit_task
            return True

        await _cancel_and_await_owned_task(
            audit_task,
            timeout=shutdown_timeout,
        )
        raise ProviderAuthAuditDeadlineError(
            "Provider authentication audit exceeded its operation deadline"
        )
    finally:
        if not stop_task.done():
            stop_task.cancel()
        try:
            await stop_task
        except asyncio.CancelledError:
            pass


def _write_monitor_failure_snapshot(
    issue: str = "provider_auth_monitor_failed",
) -> None:
    write_provider_auth_snapshot(
        ReadinessReport(
            ready=False,
            auth_mode=settings.provider_webhook_auth_mode,
            evolution_instances=0,
            chatwoot_inboxes=0,
            issues=(issue,),
        )
    )


async def _monitor() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, stop_event.set)
            installed_signals.append(signum)
        except (NotImplementedError, RuntimeError):
            pass

    async def audit() -> None:
        await _verify(allow_staged=False, probe=True, emit=False)

    async def check() -> bool:
        return await _run_monitor_audit(
            audit,
            stop_event,
            audit_timeout=settings.provider_auth_monitor_audit_timeout_seconds,
            shutdown_timeout=settings.provider_auth_monitor_shutdown_timeout_seconds,
        )

    async def wait(delay: float) -> bool:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except TimeoutError:
            return False
        return True

    _safe_print(
        {
            "status": "monitor_started",
            "interval_seconds": settings.provider_auth_monitor_interval_seconds,
            "audit_timeout_seconds": (
                settings.provider_auth_monitor_audit_timeout_seconds
            ),
            "retry_max_seconds": settings.provider_auth_monitor_retry_max_seconds,
        }
    )
    try:
        await _monitor_loop(check, wait, _write_monitor_failure_snapshot)
    finally:
        for signum in installed_signals:
            loop.remove_signal_handler(signum)
    _safe_print({"status": "monitor_stopped"})


async def _snapshot_health() -> None:
    report = await provider_auth_readiness()
    if not report.ready:
        raise ProviderAuthBootstrapError("Provider authentication snapshot is not ready")


async def _enable(env_file: Path) -> None:
    if settings.provider_webhook_auth_mode != "provisioning":
        raise ProviderAuthBootstrapError(
            "Enable must run while authentication is still provisioning"
        )
    await _verify(allow_staged=True, probe=True)
    update_env_file(env_file, {AUTH_MODE_KEY: "enforced"})
    _safe_print({"status": "enforcement_configured", "next": "restart_app_then_check"})


async def _run(args: argparse.Namespace) -> None:
    env_file = Path(args.env_file).resolve()
    try:
        if args.command == "prepare":
            await _prepare(env_file)
        elif args.command == "verify":
            await _verify(allow_staged=True, probe=True)
        elif args.command == "enable":
            await _enable(env_file)
        elif args.command == "check":
            await _verify(allow_staged=False, probe=True)
        elif args.command == "monitor":
            await _monitor()
        elif args.command == "snapshot-health":
            await _snapshot_health()
        else:
            raise AssertionError(f"unknown command: {args.command}")
    finally:
        await engine.dispose()


def main() -> int:
    require_runtime_version(settings.delivery_worker_code_version)
    setup_logger()
    parser = argparse.ArgumentParser(
        description="Provision and verify provider webhook authentication"
    )
    parser.add_argument(
        "command",
        choices=("prepare", "verify", "enable", "check", "monitor", "snapshot-health"),
    )
    parser.add_argument("--env-file", default="/workspace/.env")
    args = parser.parse_args()
    try:
        asyncio.run(_run(args))
    except Exception as exc:
        _safe_print(
            {
                "status": "error",
                "error_type": type(exc).__name__,
            }
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
