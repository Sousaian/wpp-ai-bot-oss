import base64
import binascii
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    Field,
    SecretStr,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


INBOUND_RECEIPT_HMAC_PUBLIC_PLACEHOLDERS = frozenset(
    {
        "gere-um-segredo-independente",
        "replace-with-an-independent-secret",
    }
)

# AUTH-1: a pepper must be an independent high-entropy secret.
AUTH_SECRET_MIN_LENGTH = 32



# Seconds an authorized automated send keeps its lane past the provider deadline.
AUTOMATED_IO_MARGIN_SECONDS = 5

SafeCodeVersion = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=100,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    ),
]


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # OpenRouter is the only LLM provider used by the v4.1 runtime.
    openrouter_api_key: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1)
    ] = Field(repr=False)
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    
    # Evolution API
    evolution_api_url: str
    evolution_api_key: str
    evolution_webhook_bearer_token: str | None = Field(default=None, repr=False)
    inbound_receipt_hmac_current_version: int = Field(default=1, ge=1, le=65535)
    inbound_receipt_hmac_keys: dict[int, SecretStr] = Field(
        default_factory=dict,
        repr=False,
    )
    database_url: str
    
    # Application
    app_host: str = "0.0.0.0"
    app_port: int = 5000
    debug: bool = False
    health_observability_cache_seconds: float = Field(
        default=5.0, ge=5.0, le=10.0, allow_inf_nan=False
    )

    # Durable delivery worker. Safe rollout defaults keep the process idle;
    # DB runtime state remains authoritative when dispatch is enabled.
    delivery_mode: Literal["direct", "outbox"] = "direct"
    delivery_dispatch_enabled: bool = False
    delivery_cutover_epoch: str | None = None
    delivery_worker_concurrency: int = Field(default=8, ge=1, le=100)
    delivery_claim_max_attempts: int = Field(default=4, ge=1, le=20)
    delivery_claim_retry_jitter_seconds: float = Field(
        default=0.05, ge=0, le=5, allow_inf_nan=False
    )
    delivery_poll_interval_seconds: float = Field(
        default=1.0, ge=0.1, le=60, allow_inf_nan=False
    )
    delivery_lease_renew_interval_seconds: int = Field(default=30, ge=1, le=300)
    delivery_evolution_lease_seconds: int = Field(default=90, ge=5, le=3600)
    delivery_chatwoot_lease_seconds: int = Field(default=120, ge=5, le=3600)
    delivery_evolution_deadline_seconds: int = Field(default=60, ge=1, le=600)
    delivery_chatwoot_deadline_seconds: int = Field(default=300, ge=1, le=1800)
    delivery_transition_ttl_seconds: int = Field(default=1800, ge=60, le=86400)
    delivery_automated_ttl_seconds: int = Field(default=1800, ge=60, le=86400)
    delivery_human_reply_ttl_seconds: int = Field(
        default=86400,
        ge=60,
        le=604800,
    )
    delivery_handoff_setup_ttl_seconds: int = Field(
        default=86400,
        ge=60,
        le=604800,
    )
    delivery_shutdown_grace_seconds: int = Field(default=30, ge=1, le=300)
    delivery_shutdown_abort_seconds: int = Field(default=5, ge=1, le=30)
    delivery_heartbeat_interval_seconds: int = Field(default=15, ge=1, le=300)
    delivery_readiness_interval_seconds: int = Field(default=60, ge=1, le=3600)
    delivery_maintenance_interval_seconds: int = Field(default=15, ge=1, le=3600)
    delivery_maintenance_skip_error_threshold: int = Field(default=3, ge=1, le=100)
    delivery_retention_interval_seconds: int = Field(default=3600, ge=60, le=86400)
    # S1: bounded scheduler retention sweep; separate from legacy batch size.
    scheduler_retention_max_rows: int = Field(default=10000, ge=1, le=1000000)
    scheduler_retention_max_seconds: float = Field(default=30, ge=0.1, le=300, allow_inf_nan=False)
    scheduler_retention_sla_tolerance_seconds: int = Field(default=86400, ge=60, le=604800)
    # FU-1: expected partial coverage is informational, stalled work is not.
    delivery_retention_overrun_margin_seconds: float = Field(default=30, ge=6, le=3600, allow_inf_nan=False)
    delivery_retention_completion_max_intervals: int = Field(default=2, ge=2, le=24)
    scheduler_retention_sweep_max_age_seconds: int = Field(default=172800, ge=60, le=604799)
    delivery_usage_reconciliation_batch: int = Field(default=100, ge=1, le=1000)
    # Container-local path: replicas must never share a readiness snapshot.
    delivery_worker_health_path: str = "/tmp/delivery-worker-health.json"
    delivery_worker_code_version: SafeCodeVersion = "outbox-v1"

    # Durable per-number turn intake. Both switches default off. An opted-in
    # agent is accepted by the scheduler only when this process, delivery
    # runtime and scheduler singleton all name the same live epochs.
    inbox_scheduler_enabled: bool = False
    inbox_scheduler_epoch: str | None = None

    # Separate fenced turn processor (generation, one refresh, local typing
    # schedule + ticket06 delivery admission). Off by default. Its entrypoint
    # refuses start unless the scheduler gate and both epochs are configured.
    inbox_turn_processor_enabled: bool = False
    inbox_turn_processor_concurrency: int = Field(default=4, ge=1, le=64)
    inbox_turn_lease_seconds: int = Field(default=30, ge=6, le=600)
    inbox_turn_poll_interval_seconds: float = Field(
        default=0.5, ge=0.05, le=10, allow_inf_nan=False
    )
    inbox_generation_timeout_seconds: float = Field(
        default=45, ge=1, le=300, allow_inf_nan=False
    )
    inbox_typing_jitter_ms: int = Field(default=400, ge=0, le=1000)
    inbox_turn_release_backoff_seconds: int = Field(default=5, ge=1, le=600)
    inbox_turn_worker_health_path: str = "/tmp/turn-worker-health.json"

    # Provider webhook authentication is deliberately staged.  A new or
    # upgraded installation starts in provisioning mode so existing direct
    # callbacks keep flowing while the operator configures both providers.
    # The bootstrap command changes this to ``enforced`` only after it has
    # verified the installed provider contracts.
    provider_webhook_auth_mode: Literal["provisioning", "enforced"] = "provisioning"
    provider_callback_base_url: str | None = None
    provider_auth_probe_base_url: str = "http://app:5000"
    provider_auth_readiness_path: str = "/app/logs/provider-auth-readiness.json"
    provider_auth_readiness_max_age_seconds: int = Field(
        default=300, ge=10, le=86400
    )
    provider_auth_readiness_cache_seconds: int = Field(default=5, ge=1, le=60)
    provider_auth_monitor_interval_seconds: int = Field(
        default=120, ge=1, le=43200
    )
    provider_auth_monitor_audit_timeout_seconds: int = Field(
        default=120, ge=1, le=43200
    )
    provider_auth_monitor_retry_initial_seconds: int = Field(
        default=5, ge=1, le=3600
    )
    provider_auth_monitor_retry_max_seconds: int = Field(
        default=60, ge=1, le=3600
    )
    provider_auth_monitor_shutdown_timeout_seconds: int = Field(
        default=5, ge=1, le=15
    )

    # Chatwoot
    chatwoot_base_url: str | None = None
    chatwoot_api_token: str | None = None
    chatwoot_account_id: int | None = None
    chatwoot_webhook_id: int | None = Field(default=None, ge=1)
    chatwoot_webhook_secret: str | None = Field(default=None, repr=False)
    chatwoot_webhook_max_age_seconds: int = Field(default=300, ge=30, le=3600)

    # AUTH-1: API operator login (/auth, /agentes, /sessoes). An empty pepper
    # keyring keeps every protected route closed (503); keys never render.
    # Rotation adds a version and keeps the old ones until every hash using
    # them was rehashed on login.
    auth_password_pepper_current_version: int = Field(default=1, ge=1, le=65535)
    auth_password_pepper_keys: dict[int, SecretStr] = Field(
        default_factory=dict,
        repr=False,
    )
    # AES-256-GCM keys for TOTP secrets, JSON {"version": base64(32 bytes)}.
    # Without them no admin can log in (TOTP is mandatory for admin).
    auth_totp_encryption_current_version: int = Field(default=1, ge=1, le=65535)
    auth_totp_encryption_keys: dict[int, SecretStr] = Field(
        default_factory=dict,
        repr=False,
    )

    @field_validator("chatwoot_webhook_id", mode="before")
    @classmethod
    def blank_managed_webhook_id_is_unset(cls, value: object) -> object:
        """Allow the bootstrap template to leave the provider-owned ID blank."""
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("delivery_cutover_epoch", mode="before")
    @classmethod
    def blank_delivery_epoch_is_unset(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("inbox_scheduler_epoch", mode="before")
    @classmethod
    def blank_scheduler_epoch_is_unset(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator(
        "delivery_worker_health_path",
        "inbox_turn_worker_health_path",
        "provider_auth_readiness_path",
    )
    @classmethod
    def local_snapshot_path_is_absolute(cls, value: str) -> str:
        if not value.strip() or "\x00" in value or not Path(value).is_absolute():
            raise ValueError("local snapshot paths must be absolute")
        return value

    @model_validator(mode="after")
    def auth_keyrings_are_valid(self) -> "Settings":
        if self.auth_password_pepper_keys and not self.auth_password_pepper_configured():
            raise ValueError("auth password pepper keyring is invalid")
        if self.auth_totp_encryption_keys and not self.auth_totp_configured():
            raise ValueError("auth totp encryption keyring is invalid")
        return self

    @model_validator(mode="after")
    def monitor_refreshes_before_snapshot_expiry(self) -> "Settings":
        if self.inbound_receipt_hmac_keys:
            if not self.receipt_hmac_keyring_is_valid():
                if (
                    self.inbound_receipt_hmac_current_version
                    not in self.inbound_receipt_hmac_keys
                ):
                    raise ValueError(
                        "current inbound receipt HMAC key version is unavailable"
                    )
                raise ValueError("inbound receipt HMAC keyring is invalid")
        if (
            self.provider_webhook_auth_mode == "enforced"
            or self.delivery_mode == "outbox"
        ) and not self.receipt_hmac_keyring_is_valid():
            raise ValueError(
                "inbound receipt HMAC keyring is required for enforced auth or outbox mode"
            )
        if self.delivery_dispatch_enabled and (
            self.delivery_mode != "outbox" or not self.delivery_cutover_epoch
        ):
            raise ValueError(
                "delivery dispatch requires outbox mode and a cutover epoch"
            )
        if self.inbox_scheduler_enabled:
            if (
                self.provider_webhook_auth_mode != "enforced"
                or self.delivery_mode != "outbox"
                or not self.delivery_cutover_epoch
                or not self.inbox_scheduler_epoch
            ):
                raise ValueError(
                    "inbox scheduler requires enforced auth, outbox mode and both epochs"
                )
            try:
                UUID(self.inbox_scheduler_epoch)
            except ValueError:
                raise ValueError("inbox scheduler epoch must be a UUID") from None
        if self.inbox_turn_processor_enabled and not self.inbox_scheduler_enabled:
            raise ValueError("inbox turn processor requires the inbox scheduler gate")
        # TurnProcessor renews the lane every lease_seconds / 3
        # (services/turn_processor.py:452 renew_due, :559 _renew_loop); the
        # lease need not exceed the generation timeout. Keep the poll below
        # that interval so waits return in time for the owner to renew.
        if (
            self.inbox_turn_poll_interval_seconds
            >= self.inbox_turn_lease_seconds / 3
        ):
            raise ValueError(
                "inbox turn poll interval must be shorter than one third of "
                "the turn lease"
            )
        if self.delivery_cutover_epoch:
            try:
                UUID(self.delivery_cutover_epoch)
            except ValueError:
                raise ValueError("delivery cutover epoch must be a UUID") from None
        if (self.delivery_dispatch_enabled or self.inbox_scheduler_enabled) and (
            self.delivery_evolution_lease_seconds
            <= self.delivery_evolution_deadline_seconds + AUTOMATED_IO_MARGIN_SECONDS
        ):
            # Any process that can dispatch (delivery worker) or stage automated
            # rows (scheduler) must keep the claimed lease longer than the
            # provider deadline plus margin, so an authorized send never
            # outlives its own lease. Checked per service, independently.
            raise ValueError(
                "evolution delivery lease must exceed its deadline plus the "
                "automated I/O margin"
            )
        # FU-1: watchdogs cannot expire during a normally budgeted pass.
        if (self.delivery_retention_interval_seconds * self.delivery_retention_completion_max_intervals
            <= self.scheduler_retention_max_seconds + self.delivery_retention_overrun_margin_seconds):
            raise ValueError("retention completion window must exceed budget plus margin")
        if (self.scheduler_retention_sweep_max_age_seconds <= self.delivery_retention_interval_seconds
            or self.scheduler_retention_sweep_max_age_seconds + self.scheduler_retention_sla_tolerance_seconds >= 7 * 86400):
            raise ValueError("retention sweep age must exceed interval and remain below seven days minus SLA tolerance")
        # R2-S1: retention has an independent task (budget up to 300s).
        # Only dispatch-loop cadence competes with the 60s fleet freshness.
        if self.delivery_heartbeat_interval_seconds + self.delivery_poll_interval_seconds >= 60:
            raise ValueError("delivery heartbeat plus poll must remain below 60-second freshness; retention must run independently")
        if self.delivery_lease_renew_interval_seconds >= min(
            self.delivery_evolution_lease_seconds,
            self.delivery_chatwoot_lease_seconds,
        ):
            raise ValueError("delivery lease renewal must be shorter than every lease")
        if (
            self.delivery_shutdown_grace_seconds
            + self.delivery_shutdown_abort_seconds
            >= 40
        ):
            raise ValueError(
                "delivery drain plus abort cleanup must remain below the "
                "40-second Compose stop grace"
            )
        attempt_period = max(
            self.provider_auth_monitor_interval_seconds,
            self.provider_auth_monitor_retry_max_seconds,
        )
        if (
            attempt_period
            + self.provider_auth_monitor_audit_timeout_seconds
            + self.provider_auth_monitor_shutdown_timeout_seconds
            >= self.provider_auth_readiness_max_age_seconds
        ):
            raise ValueError(
                "provider auth monitor cadence plus audit and shutdown timeouts "
                "must be shorter than snapshot max age"
            )
        if (
            self.provider_auth_monitor_retry_initial_seconds
            > self.provider_auth_monitor_retry_max_seconds
        ):
            raise ValueError(
                "provider auth monitor initial retry must not exceed maximum retry"
            )
        return self

    def receipt_hmac_keyring_is_valid(self) -> bool:
        """Validate key availability without rendering any secret material."""
        keys = self.inbound_receipt_hmac_keys
        return bool(keys) and self.inbound_receipt_hmac_current_version in keys and all(
            isinstance(secret, SecretStr)
            and bool(secret.get_secret_value().strip())
            and secret.get_secret_value().strip().casefold()
            not in INBOUND_RECEIPT_HMAC_PUBLIC_PLACEHOLDERS
            for secret in keys.values()
        )

    def auth_password_pepper_configured(self) -> bool:
        """Current version present; every retained key long and not a placeholder."""
        keys = self.auth_password_pepper_keys
        return bool(keys) and self.auth_password_pepper_current_version in keys and all(
            _auth_secret_is_usable(secret) for secret in keys.values()
        )

    def auth_totp_configured(self) -> bool:
        """Current version present; every retained key decodes to 32 bytes."""
        keys = self.auth_totp_encryption_keys
        return bool(keys) and self.auth_totp_encryption_current_version in keys and all(
            _aes256_key_is_usable(secret) for secret in keys.values()
        )

    def auth_configured(self) -> bool:
        return self.auth_password_pepper_configured() and self.auth_totp_configured()

    def receipt_hmac_keyring_fingerprint(self) -> str:
        """Bind readiness snapshots to every retained key, without exposing keys."""
        if not self.receipt_hmac_keyring_is_valid():
            return "invalid"
        material = {
            "current": self.inbound_receipt_hmac_current_version,
            "keys": [
                [
                    version,
                    hashlib.sha256(
                        self.inbound_receipt_hmac_keys[version]
                        .get_secret_value()
                        .encode()
                    ).hexdigest(),
                ]
                for version in sorted(self.inbound_receipt_hmac_keys)
            ],
        }
        encoded = json.dumps(material, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
        hide_input_in_errors=True,
    )


def _auth_secret_is_usable(secret: object) -> bool:
    if not isinstance(secret, SecretStr):
        return False
    value = secret.get_secret_value().strip()
    folded = value.casefold()
    return bool(
        len(value) >= AUTH_SECRET_MIN_LENGTH
        and not folded.startswith(("replace-", "change-me", "sua-", "gere-"))
    )


def _aes256_key_is_usable(secret: object) -> bool:
    if not isinstance(secret, SecretStr):
        return False
    try:
        raw = base64.b64decode(secret.get_secret_value().strip(), validate=True)
    except (binascii.Error, ValueError):
        return False
    return len(raw) == 32 and len(set(raw)) > 1


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Convenience
settings = get_settings()
