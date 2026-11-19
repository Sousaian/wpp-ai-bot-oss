"""T08-3: shared replica-heartbeat fixtures for real-MySQL suites."""

from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

from sqlalchemy import text

from src.config import settings
from src.utils.image_version import image_code_version
from src.database.repositories import replica_repo
from src.services import replica_compat


CODE_VERSION = image_code_version(settings.delivery_worker_code_version)


def ready_fingerprint(*, delivery_epoch: str, scheduler_epoch: str, **overrides) -> replica_repo.Fingerprint:
    base = replica_repo.Fingerprint(
        code_version=CODE_VERSION,
        schema_version=replica_compat.EXPECTED_SCHEMA_VERSION,
        delivery_mode="outbox",
        delivery_epoch=delivery_epoch,
        scheduler_enabled=True,
        scheduler_epoch=scheduler_epoch,
        turn_processor_enabled=True,
        dispatch_enabled=True,
        webhook_auth_mode="enforced",
    )
    return replace(base, **overrides)


async def put_replica(
    sessions,
    fingerprint: replica_repo.Fingerprint,
    *,
    kind: str = "app",
    replica_id: str | None = None,
    age_seconds: int = 0,
    stopped: bool = False,
) -> str:
    replica_id = replica_id or str(uuid4())
    async with sessions() as db:
        await replica_repo.record(
            db, replica_id=replica_id, replica_kind=kind, fingerprint=fingerprint, stopped=stopped
        )
        if age_seconds:
            await db.execute(
                text(
                    "UPDATE replica_heartbeats SET last_seen_at = "
                    "TIMESTAMPADD(SECOND, -:age, UTC_TIMESTAMP(6)) WHERE replica_id = :id"
                ),
                {"age": age_seconds, "id": replica_id},
            )
        await db.commit()
    return replica_id


async def set_schema_version(sessions, version: int) -> None:
    async with sessions() as db:
        await db.execute(
            text("UPDATE scheduler_runtime_state SET schema_version = :v WHERE id = 1"),
            {"v": version},
        )
        await db.commit()


async def ready_fleet(
    sessions, *, delivery_epoch: str, scheduler_epoch: str, kinds=replica_repo.REPLICA_KINDS
) -> replica_repo.EnableAttestation:
    """Exactly one compatible fresh replica per kind + matching attestation."""

    async with sessions() as db:
        await db.execute(text("DELETE FROM replica_heartbeats"))
        await db.commit()
    await set_schema_version(sessions, replica_compat.EXPECTED_SCHEMA_VERSION)
    fingerprint = ready_fingerprint(delivery_epoch=delivery_epoch, scheduler_epoch=scheduler_epoch)
    for kind in kinds:
        await put_replica(sessions, fingerprint, kind=kind)
    return replica_repo.EnableAttestation(old_replicas_removed=True, operator=fingerprint)
