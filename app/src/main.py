import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import settings
from src.database.connection import AsyncSessionLocal, check_db_connection, get_db, engine
from src.database.repositories import agente_repo, consumo_repo, delivery_state_repo, inbound_event_repo
from src.database.unit_of_work import UnitOfWork, get_unit_of_work
from src.routers import agentes_router, auth_router, chatwoot_router, sessoes_router
from src.services import evolution_orchestrator, replica_compat, scheduler_observability
from src.services.evolution_client import evolution_client
from src.services.webhook_auth import WebhookAuthError, verify_evolution_bearer
from src.services.provider_auth_bootstrap import provider_auth_readiness
from src.utils.access_log import configure_uvicorn_access_log_redaction
from src.utils.async_ttl_cache import AsyncSingleFlightTTL
from src.utils.logger import setup_logger
from src.utils.shutdown import APP_CLEANUP_SECONDS, wait_until
from src.utils.process_shutdown import (
    current, hard_deadline, writers_quiescent, marker_finished,
    MARKER_RESERVE_SECONDS, DISPOSE_RESERVE_SECONDS,
)


# Configurar logger
setup_logger("logs/app.log")
configure_uvicorn_access_log_redaction()


MENSAGEM_LIMITE_ATINGIDO = evolution_orchestrator.MENSAGEM_LIMITE_ATINGIDO
_scheduler_health_cache: AsyncSingleFlightTTL[dict[str, object]] = AsyncSingleFlightTTL()
_replica_health_cache: AsyncSingleFlightTTL[replica_compat.FleetReport] = AsyncSingleFlightTTL()


# S4: public /health logs only on healthy→unhealthy, never once per request.
HEALTH_SIGNAL_THRESHOLD = 1
_health_unhealthy_transitions = 0
_health_was_healthy = True


def reset_health_observability_caches() -> None:
    """Reset process-local `/health` caches without changing signal counters."""

    _scheduler_health_cache.reset()
    _replica_health_cache.reset()


def reset_health_transition_state() -> None:
    """Reset transition memory without erasing monotonic counters."""

    global _health_was_healthy
    _health_was_healthy = True


def health_unhealthy_transitions() -> int:
    return _health_unhealthy_transitions


def _record_health(healthy: bool, reason: str) -> None:
    global _health_unhealthy_transitions, _health_was_healthy
    if not healthy and _health_was_healthy:
        _health_unhealthy_transitions += 1
        logger.error(
            "Public health unhealthy | reason={} count={} threshold={}",
            reason, _health_unhealthy_transitions, HEALTH_SIGNAL_THRESHOLD,
        )
    _health_was_healthy = healthy


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle events"""
    from src.utils.image_version import require_runtime_version
    require_runtime_version(settings.delivery_worker_code_version)
    logger.info("🚀 Starting application...")
    controllers = set(asyncio.all_tasks())
    guard = current()
    # T08-3: ingress replica heartbeat for compatibility readiness.
    replica_heartbeat = replica_compat.HeartbeatLoop(AsyncSessionLocal, kind="app")
    replica_heartbeat.start()

    try:
        yield
    finally:
        # R2-S4: shield isolates HTTP waiters, not process shutdown.
        deadline = hard_deadline(guard, APP_CLEANUP_SECONDS)
        reserve = min(DISPOSE_RESERVE_SECONDS, APP_CLEANUP_SECONDS / 4) if guard is None else DISPOSE_RESERVE_SECONDS
        marker_reserve = min(MARKER_RESERVE_SECONDS, APP_CLEANUP_SECONDS / 4) if guard is None else MARKER_RESERVE_SECONDS
        writers_end = deadline - marker_reserve - reserve
        marker_end = deadline - reserve
        cache_tasks = [
            asyncio.create_task(_scheduler_health_cache.aclose()),
            asyncio.create_task(_replica_health_cache.aclose()),
        ]

        async def before_marker() -> bool:
            ignored = controllers | {stop_job, replica_heartbeat._shutdown_task}
            safe = await writers_quiescent(writers_end, ignored)
            if not safe:
                replica_compat.signal_cleanup_failure("app")
                if guard is not None:
                    guard.mark_unsafe()
            return safe

        # Stop/cancel heartbeat concurrently with the two cache loads; only
        # its FINAL marker waits for all possible writers (including shields).
        stop_job = asyncio.create_task(replica_heartbeat.stop(
            max(0.0, marker_end - asyncio.get_running_loop().time()), before_marker=before_marker,
        ))
        try:
            for task in cache_tasks:
                finished = await wait_until(task, writers_end)
                if not finished or task.cancelled() or task.exception() is not None:
                    replica_compat.signal_cleanup_failure("app")
            await stop_job
            if guard is not None:
                await marker_finished(
                    guard, recorded=replica_heartbeat.stopped_recorded,
                    deadline=marker_end, controllers=controllers | {stop_job},
                )
        finally:
            for task in cache_tasks:
                if not task.done():
                    task.cancel()
            dispose = asyncio.create_task(engine.dispose())
            finished = await wait_until(dispose, deadline)
            if not finished or dispose.cancelled() or dispose.exception() is not None:
                replica_compat.signal_cleanup_failure("app")
                if guard is not None and (
                    not finished
                    or not await writers_quiescent(deadline, controllers)
                    or asyncio.get_running_loop().time() >= deadline
                ):
                    guard.mark_unsafe()
    logger.info("👋 Shutting down application...")


app = FastAPI(
    title="MVP Atendimento Pós-Venda IA",
    description="Sistema de atendimento automatizado via WhatsApp",
    version="0.1.0",
    lifespan=lifespan
)
app.include_router(auth_router)
app.include_router(agentes_router)
app.include_router(sessoes_router)
app.include_router(chatwoot_router)
# T08-5: 503 bodies carry a stable technical code next to the legacy detail.
app.add_exception_handler(
    scheduler_observability.TechnicalUnavailable,
    scheduler_observability.technical_unavailable_handler,
)



# AUTH-1: /auth validation errors name only field and type, never the input
# (a password or token must not be echoed back). Other routes unchanged.
async def _validation_error_handler(request: Request, exc: RequestValidationError):
    if request.url.path == "/auth" or request.url.path.startswith("/auth/"):
        return JSONResponse(
            status_code=422,
            content={"detail": [
                {"campo": ".".join(str(part) for part in error.get("loc", ())), "tipo": error.get("type")}
                for error in exc.errors()
            ]},
        )
    return await request_validation_exception_handler(request, exc)


app.add_exception_handler(RequestValidationError, _validation_error_handler)


# FU-4: liveness only; service name/version stay out of every public body.
@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "online"}


@app.post("/webhook")
async def webhook_handler(
    request: Request,
    uow: UnitOfWork = Depends(get_unit_of_work),
    response: Response = None,
):
    """Authenticate, validate and atomically accept an Evolution event."""

    authenticated = settings.provider_webhook_auth_mode == "enforced"
    if authenticated:
        if not settings.evolution_webhook_bearer_token:
            logger.error("Evolution webhook authentication is not provisioned")
            # T08-5: same status/detail/no Retry-After, plus technical code.
            raise scheduler_observability.TechnicalUnavailable(
                scheduler_observability.record_rejection("auth_unavailable"),
                detail="webhook authentication unavailable",
                retry_after=None,
            )
        try:
            verify_evolution_bearer(
                request.headers,
                settings.evolution_webhook_bearer_token,
            )
        except WebhookAuthError:
            logger.warning("Evolution webhook rejected: authentication failed")
            raise HTTPException(
                status_code=401,
                detail="invalid webhook authentication",
            ) from None

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid webhook payload") from None

    try:
        parsed = evolution_orchestrator.parse_event(payload)
        if isinstance(parsed, evolution_orchestrator.OrchestrationResult):
            return parsed.body
        result = await evolution_orchestrator.orchestrate(
            event=parsed,
            uow=uow,
            authenticated=authenticated,
        )
    except evolution_orchestrator.InvalidEvolutionEvent as exc:
        raise HTTPException(
            status_code=503 if exc.retryable else 400,
            detail=exc.reason,
        ) from None
    except inbound_event_repo.ReceiptCollisionError:
        logger.error("Evolution receipt collision rejected")
        raise HTTPException(status_code=409, detail="event identity collision") from None
    except (
        evolution_orchestrator.DeliveryOwnershipError,
        evolution_orchestrator.ReceiptConfigurationError,
        consumo_repo.UsagePersistenceInvariantError,
    ) as exc:
        logger.error("Evolution webhook acceptance unavailable | erro_tipo={}", type(exc).__name__)
        # T08-5: technical cause code; Retry-After and detail unchanged.
        raise scheduler_observability.webhook_unavailable(exc) from None
    except Exception as exc:
        logger.error("Evolution webhook transaction failed | erro_tipo={}", type(exc).__name__)
        raise scheduler_observability.webhook_unavailable(exc) from None

    if response is not None:
        response.status_code = result.http_status
    return result.body


# FU-4: internal diagnostics; same cached readiness collector as public health.
@app.get("/internal/health", include_in_schema=False)
async def health_check(response: Response, db: AsyncSession = Depends(get_db)):
    """DB/local-snapshot readiness; neither surface fans out to providers."""
    from src.database.repositories import lifecycle_repo

    lifecycle = {"counts": lifecycle_repo.lifecycle_counts(), "signal_threshold": lifecycle_repo.SIGNAL_THRESHOLD}
    db_ok = await check_db_connection()
    # T08-5: bounded non-locking aggregates; unreadable metrics fail closed
    # only while the DB scheduler is active (enabling|enabled|draining).
    # Own short read-only session on the request engine, apart from ``db``.
    health_sessions = async_sessionmaker(
        bind=db.bind, class_=AsyncSession, expire_on_commit=False
    )
    scheduler = await _scheduler_health_cache.get(
        lambda: scheduler_observability.scheduler_health(
            health_sessions,
            configured_epoch=settings.inbox_scheduler_epoch,
        ),
        ttl_seconds=settings.health_observability_cache_seconds,
    )
    replicas = await _replica_health_cache.get(
        lambda: replica_compat.fleet_report(
            health_sessions, reader=replica_compat.local_fingerprint()
        ),
        ttl_seconds=settings.health_observability_cache_seconds,
    )
    try:
        state = await delivery_state_repo.get_runtime_state(db)
        fleet = await delivery_state_repo.get_worker_fleet_readiness(db)
        provider = await provider_auth_readiness()
    except Exception as exc:
        if _health_was_healthy:
            logger.warning("Delivery readiness unavailable | erro_tipo={}", type(exc).__name__)
        _record_health(False, "readiness_unavailable")
        response.status_code = 503
        return {
            "status": "degraded",
            "database": db_ok,
            "lifecycle": lifecycle,
            "delivery_ready": False,
            "reason": "readiness_unavailable",
            "scheduler": scheduler,
        }

    epoch_matches = bool(
        settings.delivery_cutover_epoch
        and state.cutover_epoch == settings.delivery_cutover_epoch
    )
    runtime_matches = bool(
        (settings.delivery_mode == "direct" and state.mode == "direct")
        or (
            settings.delivery_mode == "outbox"
            and state.mode == "outbox"
            and epoch_matches
        )
    )
    dispatch_matches = bool(
        (settings.delivery_mode == "direct" and not settings.delivery_dispatch_enabled)
        or (
            settings.delivery_mode == "outbox"
            and settings.delivery_dispatch_enabled
            and fleet.workers
            and all(
                worker.dispatch_enabled
                and worker.configured_mode == "outbox"
                and worker.configured_epoch == settings.delivery_cutover_epoch
                and worker.provider_ready
                and worker.runtime_compatible
                for worker in fleet.workers
            )
        )
    )
    backlog_ok = bool(
        fleet.oldest_due is None
        or fleet.db_now - fleet.oldest_due <= timedelta(minutes=5)
    )
    provider_ready = bool(provider.ready)
    delivery_ready = bool(
        db_ok
        and runtime_matches
        and dispatch_matches
        and backlog_ok
        and (settings.delivery_mode != "outbox" or provider_ready)
        and replicas.healthy
    )
    healthy = bool(delivery_ready and scheduler["healthy"])  # T08-5
    _record_health(healthy, "delivery_not_ready" if not delivery_ready else "scheduler_unhealthy")
    if not healthy:
        response.status_code = 503
    return {
        "status": "healthy" if healthy else "degraded",
        "database": db_ok,
        "lifecycle": lifecycle,
        "delivery_ready": delivery_ready,
        "scheduler": scheduler,
        "runtime_mode": state.mode,
        "runtime_matches": runtime_matches,
        "cutover_epoch_matches": epoch_matches if settings.delivery_mode == "outbox" else True,
        "dispatch_matches": dispatch_matches,
        "worker_heartbeat_count": len(fleet.workers),
        "provider_ready": provider_ready,
        "backlog_within_slo": backlog_ok,
        "replica_compatibility": replicas.as_dict(),
    }


# FU-4: public readiness has an exact one-key allowlist, no diagnostic fields.
@app.get("/health")
async def public_health_check(response: Response, db: AsyncSession = Depends(get_db)):
    details = await health_check(response, db)
    return {"status": "healthy" if details["status"] == "healthy" else "unhealthy"}


@app.get("/health/provider-auth")
async def provider_auth_health(
    response: Response,
) -> dict[str, object]:
    """Serve the bounded operator-produced snapshot without provider fan-out."""
    report = await provider_auth_readiness()
    if not report.ready:
        response.status_code = 503
    return report.as_dict()


if __name__ == "__main__":
    from src.app_server import main
    main(app, host=settings.app_host, port=settings.app_port)
