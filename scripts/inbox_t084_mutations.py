#!/usr/bin/env python3
"""Fix-removal/config-parity mutations for the T08-4 matrix."""
from pathlib import Path
import sys

T = "tests.test_settings.OpenRouterOnlyStartupTests."
CASES = {
    "finite_claim_jitter": (T+"test_every_float_setting_rejects_non_finite_values", "app/src/config.py", "default=0.05, ge=0, le=5, allow_inf_nan=False", "default=0.05, ge=0, le=5"),
    "finite_delivery_poll": (T+"test_every_float_setting_rejects_non_finite_values", "app/src/config.py", "default=1.0, ge=0.1, le=60, allow_inf_nan=False", "default=1.0, ge=0.1, le=60"),
    "finite_turn_poll": (T+"test_every_float_setting_rejects_non_finite_values", "app/src/config.py", "default=0.5, ge=0.05, le=10, allow_inf_nan=False", "default=0.5, ge=0.05, le=10"),
    "finite_generation_timeout": (T+"test_every_float_setting_rejects_non_finite_values", "app/src/config.py", "default=45, ge=1, le=300, allow_inf_nan=False", "default=45, ge=1, le=300"),
    "turn_poll_lease_guard_weakened": (T+"test_turn_poll_interval_must_fit_before_lease_renewal", "app/src/config.py", "self.inbox_turn_lease_seconds / 3", "self.inbox_turn_lease_seconds"),
    "code_version_type_removed": (T+"test_worker_code_version_is_nonblank_bounded_safe_ascii", "app/src/config.py", "delivery_worker_code_version: SafeCodeVersion", "delivery_worker_code_version: str"),
    "code_version_pattern_widened": (T+"test_worker_code_version_is_nonblank_bounded_safe_ascii", "app/src/config.py", 'pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$"', 'pattern=r"^.*$"'),
    "absolute_path_guard_removed": (T+"test_local_snapshot_paths_must_be_absolute", "app/src/config.py", ' or not Path(value).is_absolute()', ''),
    "root_env_lease_drift": (T+"test_delivery_and_inbox_defaults_match_templates_and_agents_section_9", ".env.example", "INBOX_TURN_LEASE_SECONDS=30", "INBOX_TURN_LEASE_SECONDS=31"),
    "app_env_version_drift": (T+"test_delivery_and_inbox_defaults_match_templates_and_agents_section_9", "app/.env.example", "DELIVERY_WORKER_CODE_VERSION=outbox-v1", "DELIVERY_WORKER_CODE_VERSION=outbox-v2"),
    "agents_deadline_drift": (T+"test_delivery_and_inbox_defaults_match_templates_and_agents_section_9", "AGENTS.md", "DELIVERY_CHATWOOT_DEADLINE_SECONDS=300", "DELIVERY_CHATWOOT_DEADLINE_SECONDS=301"),
    "config_concurrency_drift": (T+"test_delivery_and_inbox_defaults_match_templates_and_agents_section_9", "app/src/config.py", "delivery_worker_concurrency: int = Field(default=8", "delivery_worker_concurrency: int = Field(default=9"),
    "agents_cap_bound_drift": (T+"test_scheduler_db_cap_defaults_and_bounds_match_agents_section_9", "AGENTS.md", "| `max_pending_inputs_per_session` | `128` | `1..10000` |", "| `max_pending_inputs_per_session` | `128` | `1..10001` |"),
    "fresh_cap_default_drift": (T+"test_scheduler_db_cap_defaults_and_bounds_match_agents_section_9", "db/init.sql", "max_pending_inputs_per_session INT UNSIGNED NOT NULL DEFAULT 128", "max_pending_inputs_per_session INT UNSIGNED NOT NULL DEFAULT 129"),
    "fresh_cap_bound_drift": (T+"test_scheduler_db_cap_defaults_and_bounds_match_agents_section_9", "db/init.sql", "max_pending_inputs_per_session BETWEEN 1 AND 10000", "max_pending_inputs_per_session BETWEEN 1 AND 10001"),
    "additive_cap_default_drift": (T+"test_scheduler_db_cap_defaults_and_bounds_match_agents_section_9", "db/init.sql", "ADD COLUMN accounting_drain_seconds SMALLINT UNSIGNED NOT NULL DEFAULT 30", "ADD COLUMN accounting_drain_seconds SMALLINT UNSIGNED NOT NULL DEFAULT 31"),
    "additive_cap_bound_drift": (T+"test_scheduler_db_cap_defaults_and_bounds_match_agents_section_9", "db/init.sql", "ADD CONSTRAINT ck_scheduler_accounting_drain CHECK (\n                accounting_drain_seconds BETWEEN 1 AND 300", "ADD CONSTRAINT ck_scheduler_accounting_drain CHECK (\n                accounting_drain_seconds BETWEEN 1 AND 301"),
}

def apply(root: Path, name: str) -> None:
    test, rel, old, new = CASES[name]
    path = root / rel
    text = path.read_text()
    if text.count(old) != 1:
        raise SystemExit(f"{name}: expected one target, found {text.count(old)}")
    path.write_text(text.replace(old, new, 1))

def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[1] == "list":
        for name, (test, *_rest) in CASES.items(): print(name, test)
        return 0
    if len(argv) == 2 and argv[1] == "count": print(len(CASES)); return 0
    if len(argv) == 2 and argv[1] == "check":
        root = Path(__file__).resolve().parent.parent
        for name, (_, rel, old, _new) in CASES.items():
            count = (root / rel).read_text().count(old)
            if count != 1: raise SystemExit(f"{name}: expected one target, found {count}")
        return 0
    if len(argv) == 4 and argv[1] == "apply": apply(Path(argv[2]), argv[3]); return 0
    raise SystemExit("usage: inbox_t084_mutations.py list|count|check|apply <root> <name>")

if __name__ == "__main__": raise SystemExit(main(sys.argv))
