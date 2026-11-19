"""Read-only release configuration gate. Output is key names + fixed reasons only."""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import posixpath
from pathlib import Path
import re
import shlex
from typing import Any
import stat
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from release_process import Cancelled, cancellation, private_copy, private_directory, run
from release_identity import IdentityError, clean_head, valid_image_reference, verify_receipt
from urllib.parse import unquote, urlsplit
from uuid import UUID

ROOT = Path(__file__).resolve().parent.parent
REQUIRED = (
    "COMPOSE_PROJECT_NAME", "MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD",
    "MYSQL_ROOT_PASSWORD", "DATABASE_URL", "POSTGRES_DATABASE", "POSTGRES_USERNAME",
    "POSTGRES_PASSWORD", "AUTHENTICATION_API_KEY", "DATABASE_CONNECTION_URI", "OPENROUTER_API_KEY", "EVOLUTION_API_URL", "EVOLUTION_API_KEY",
    "CHATWOOT_DB_PASSWORD", "CHATWOOT_SECRET_KEY", "CHATWOOT_FRONTEND_URL",
    "CHATWOOT_MAILER_EMAIL", "CHATWOOT_BASE_URL", "CHATWOOT_API_TOKEN", "CHATWOOT_ACCOUNT_ID",
    "NGINX_CONF_PATH", "NGINX_BIND_ADDRESS", "NGINX_HTTP_PORT", "NGINX_HTTPS_PORT",
    "DELIVERY_MODE", "DELIVERY_DISPATCH_ENABLED", "DELIVERY_WORKER_CODE_VERSION",
    "INBOX_SCHEDULER_ENABLED", "INBOX_TURN_PROCESSOR_ENABLED", "PROVIDER_WEBHOOK_AUTH_MODE",
    "PROVIDER_CALLBACK_BASE_URL", "CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK",
)
SECRET_KEYS = (
    "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "POSTGRES_PASSWORD", "OPENROUTER_API_KEY",
    "EVOLUTION_API_KEY", "AUTHENTICATION_API_KEY", "CHATWOOT_DB_PASSWORD", "CHATWOOT_SECRET_KEY", "CHATWOOT_API_TOKEN",
    "EVOLUTION_WEBHOOK_BEARER_TOKEN", "CHATWOOT_WEBHOOK_SECRET", "SMTP_PASSWORD",
)
SMTP_KEYS = ("SMTP_ADDRESS", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD")
PYTHON_SERVICES = ("app", "delivery-worker", "turn-worker", "delivery-ops",
                   "provider-auth-bootstrap", "provider-auth-monitor")
VOLUMES = ("mysql_data", "app_logs", "evolution_instances", "evolution_redis",
           "postgres_data", "chatwoot_pg_data")


# Explicit allowlist: all future/unknown Compose controls fail closed.
SAFE_COMPOSE_CONTROLS = frozenset(("COMPOSE_PROJECT_NAME", "COMPOSE_ANSI", "COMPOSE_STATUS_STDOUT",
    "COMPOSE_PROGRESS", "COMPOSE_MENU", "COMPOSE_HTTP_TIMEOUT", "COMPOSE_PARALLEL_LIMIT"))
DEFAULT_COMPOSE_FILES = ("compose.yaml", "compose.yml", "docker-compose.yml", "docker-compose.yaml")
AUTO_OVERRIDE_FILES = ("compose.override.yaml", "compose.override.yml",
                       "docker-compose.override.yml", "docker-compose.override.yaml")


def forbidden_compose_controls(candidate: dict[str, str], environment: dict[str, str]) -> set[str]:
    return {key for key in candidate.keys() | environment.keys()
            if key.startswith("COMPOSE_") and key not in SAFE_COMPOSE_CONTROLS}


def deploy_config(path: Path) -> dict:
    # No operational .env is read/copied. Resolve the documented no--f CLI in
    # a model-only snapshot whose .env is the candidate, with the SAME shell.
    with private_directory("wpp-release-model-") as snapshot:
        for name in (*DEFAULT_COMPOSE_FILES, *AUTO_OVERRIDE_FILES):
            source = ROOT / name
            if source.is_file():
                (snapshot / name).write_text(source.read_text().replace(str(ROOT), str(snapshot)))
        private_copy(path, snapshot / ".env")
        result = run(["docker", "compose", "--project-directory", str(snapshot),
                                 "--env-file", str(snapshot / ".env"), "config", "--format", "json"],
                                cwd=snapshot, capture_output=True, text=True, timeout=30, check=True)
        config = json.loads(result.stdout)
        # Compare resolved models, discounting only relocation of this snapshot.
        def restore(value: Any) -> Any:
            if isinstance(value, str):
                return value.replace(str(snapshot), str(ROOT))
            if isinstance(value, dict):
                return {key: restore(item) for key, item in value.items()}
            if isinstance(value, list):
                return [restore(item) for item in value]
            return value
        return restore(config)



def python_file_mounts_safe(config: dict, service: dict, kind: str, root: Path) -> bool:
    """Validate Compose-resolved configs/secrets metadata, never payload bytes.

    Linux targets follow Compose create.go: config relative/default at /;
    secret relative/default at /run/secrets/. Compose parses YAML, not this guard.
    """
    definitions = config.get(kind, {})
    references = service.get(kind, [])
    if not isinstance(definitions, dict) or not isinstance(references, list):
        return False
    for reference in references:
        # `compose config --format json` expands short references to objects.
        if not isinstance(reference, dict):
            return False
        if any(key not in {"source", "target", "uid", "gid", "mode"}
               and not key.startswith("x-") for key in reference):
            return False
        source = reference.get("source")
        if not isinstance(source, str) or not re.fullmatch(r"[a-zA-Z0-9._-]+", source):
            return False
        definition = definitions.get(source)
        if not isinstance(definition, dict):
            return False
        fields = {"name", "file", "environment", "external", "labels", "driver_opts", "template_driver"}
        fields |= {"content"} if kind == "configs" else {"driver"}
        if any(key not in fields and not key.startswith("x-") for key in definition):
            return False
        for key in ("name", "file", "environment", "template_driver", "driver"):
            if key in definition and (not isinstance(definition[key], str)
                    or not definition[key] or "\0" in definition[key]):
                return False
        if "content" in definition and not isinstance(definition["content"], str):
            return False
        if "external" in definition and not isinstance(definition["external"], bool):
            return False
        external = definition.get("external", False)
        inputs = [key for key in ("file", "environment", "content") if key in definition]
        if (external and (inputs or any(key not in {"name", "external"}
                and not key.startswith("x-") for key in definition))) or (not external and len(inputs) != 1):
            return False
        if "labels" in definition and (not isinstance(definition["labels"], dict)
                or not all(isinstance(k, str) and isinstance(v, str) for k,v in definition["labels"].items())):
            return False
        if "driver_opts" in definition and (not isinstance(definition["driver_opts"], dict)
                or not all(isinstance(k, str) and type(v) in (str, int, float)
                           for k,v in definition["driver_opts"].items())):
            return False
        for key in ("uid", "gid"):
            if key in reference and (not isinstance(reference[key], str) or not re.fullmatch(r"[0-9]+", reference[key])):
                return False
        if "mode" in reference:
            mode = reference["mode"]
            # Compose JSON emits octal strings (e.g. "0444"); retain integer
            # model support too. bool is not an integer permission mode.
            if not ((type(mode) is int and 0 <= mode <= 0o7777) or
                    (isinstance(mode, str) and re.fullmatch(r"[0-7]{1,5}", mode)
                     and int(mode, 8) <= 0o7777)):
                return False
        target = reference.get("target", "")
        if not isinstance(target, str) or "\0" in target:
            return False
        base = "/" if kind == "configs" else "/run/secrets/"
        target = target or source
        absolute = target if target.startswith("/") else base + target
        normalized = posixpath.normpath("/" + absolute.lstrip("/"))
        if normalized == "/app" or normalized.startswith("/app/"):
            return False
        # File-backed configs/secrets are bind mounts too: preserve source policy.
        if "file" in definition:
            app = (root / "app").resolve()
            path = (root / definition["file"]).resolve()
            if path == app or app in path.parents:
                return False
    return True


def python_mounts_safe(config: dict, root: Path) -> bool:
    """Resolved Compose model only; never try to parse short mount syntax here."""
    app = (root / "app").resolve()
    def under_app(target: str) -> bool:
        normalized = posixpath.normpath("/" + target.lstrip("/"))
        return normalized == "/app" or normalized.startswith("/app/")
    try:
        definition = config.get("volumes", {}).get("app_logs", {}) or {}
        legitimate_logs = (definition.get("name") == str(config.get("name")) + "_app_logs"
                           and not definition.get("external") and not definition.get("driver_opts")
                           and definition.get("driver") in (None, "local"))
        for name in PYTHON_SERVICES:
            service = config.get("services", {}).get(name, {})
            for kind in ("configs", "secrets"):
                if not python_file_mounts_safe(config, service, kind, root):
                    return False
            if service.get("volumes_from"):
                return False
            for target in service.get("tmpfs", []):
                if under_app(target.split(":", 1)[0]):
                    return False
            for mount in service.get("volumes", []):
                if not isinstance(mount, dict):
                    return False
                target = mount.get("target")
                if not isinstance(target, str) or not target.startswith("/"):
                    return False
                source = mount.get("source", "")
                if mount.get("type") == "bind":
                    source_path = (root / source).resolve()
                    if source_path == app or app in source_path.parents:
                        return False
                if under_app(target) and not (target == "/app/logs" and mount.get("type") == "volume"
                    and source == "app_logs" and legitimate_logs):
                    return False
        return True
    except (OSError, ValueError, TypeError, AttributeError):
        return False


def parse_env(source: str) -> dict[str, str]:
    """Parse dotenv literals, without shell expansion, execution or ambient env."""
    values: dict[str, str] = {}
    for raw in source.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=(.*)", line)
        if not match:
            raise ValueError("invalid_assignment")
        key, value = match.groups()
        if key in values:
            raise ValueError("duplicate_key")
        value = value.strip()
        if value.startswith(("'", '"')):
            parts = shlex.split(value, comments=True, posix=True)
            if len(parts) != 1:
                raise ValueError("invalid_quoted_value")
            value = parts[0]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].rstrip()
        values[key] = value
    return values


def compose_effective(path: Path, extra_files: list[str], *, all_profiles: bool = True) -> tuple[dict, dict[str, str]]:
    command = ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(path.resolve()),
               "-f", str(ROOT / "docker-compose.yml")]
    for file in extra_files:
        command += ["-f", file]
    # Validate a candidate .env.new as the runtime env_file too, without ever
    # opening the live .env. This read-only override is passed over stdin.
    env_services = (*PYTHON_SERVICES, "db", "evolution", "evolution-postgres")
    overlay = "services:\n" + "".join(
        f"  {name}:\n    env_file: !override\n      - {json.dumps(str(path.resolve()))}\n"
        for name in env_services)
    command += ["-f", "-"]
    if all_profiles:
        command += ["--profile", "*"]
    command += ["config", "--format", "json"]
    result = run(command, input=overlay, capture_output=True, text=True,
                            timeout=30, check=True)
    config = json.loads(result.stdout)
    return config, config["services"]["app"]["environment"]


def shell_override_keys(candidate: dict[str, str], extra_files: list[str]) -> set[str]:
    # Shell precedence applies to interpolated variables, NOT env_file-only
    # keys. Candidate references are included for nested interpolation.
    sources = [ROOT / "docker-compose.yml", *(Path(file) for file in extra_files)]
    text = "\n".join(path.read_text() for path in sources) + "\n" + "\n".join(candidate.values())
    references = re.findall(r"(?<!\$)\$(?:\{([A-Za-z_][A-Za-z0-9_]*)|([A-Za-z_][A-Za-z0-9_]*))", text)
    return {braced or bare for braced, bare in references}


def mysql_components(uri: str) -> tuple[str, str, str, str, str]:
    # Same component grammar as pinned SQLAlchemy 2.0.52 engine.url._parse_url.
    # Keep preflight stdlib-only on the operator host; tests use make_url as oracle.
    pattern = r"""(?P<name>[\w\+]+)://
        (?:(?P<username>[^:/]*)(?::(?P<password>[^@]*))?@)?
        (?:(?:\[(?P<ipv6host>[^/\?]+)\]|(?P<ipv4host>[^/:\?]+))?
        (?::(?P<port>[^/\?]*))?)?
        (?:/(?P<database>[^\?]*))?(?:\?(?P<query>.*))?"""
    match = re.match(pattern, uri, re.X)
    if not match:
        raise ValueError("invalid_mysql_uri")
    values = match.groupdict()
    if values["port"] is not None:
        int(values["port"])
    return (values["name"], values["ipv4host"] or values["ipv6host"] or "",
            unquote(values["username"] or ""), unquote(values["password"] or ""),
            values["database"] or "")


def connection_matches(uri: str, schemes: tuple[str, ...], user: str, password: str, database: str) -> bool:
    try:
        if schemes == ("mysql+aiomysql",):
            scheme, host, uri_user, uri_password, uri_database = mysql_components(uri)
        else:
            parsed = urlsplit(uri)
            _ = parsed.port  # Invalid libpq/urllib ports must not pass as valid.
            scheme, host = parsed.scheme, parsed.hostname
            uri_user, uri_password = unquote(parsed.username or ""), unquote(parsed.password or "")
            uri_database = unquote(parsed.path.removeprefix("/"))
        return (scheme in schemes and bool(host) and bool(user) and bool(password) and bool(database)
                and (uri_user, uri_password, uri_database) == (user, password, database))
    except ValueError:
        return False


def placeholder(value: str, key: str) -> bool:
    known = {"postgres", "password", "changeme", "change-me", "secret", "x",
             "gere-um-segredo-independente", "replace-with-an-independent-secret"}
    for template in (ROOT / ".env.example", ROOT / "app/.env.example"):
        candidate = parse_env(template.read_text()).get(key, "")
        if candidate:
            known.add(candidate.lower())
    lower = value.lower()
    return lower in known or lower.startswith(("replace-", "sua-", "your-", "sk-or-your"))


def has_tls_certificates(certs: Path) -> bool:
    return all((certs / name).is_file() for name in ("fullchain.pem", "privkey.pem"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("env_file", nargs="?", default="./.env")
    parser.add_argument("--expected-project", required=True)
    parser.add_argument("--phase", choices=("legacy", "outbox"), default="legacy")
    parser.add_argument("--check-volumes", action="store_true")
    parser.add_argument("--require-smtp", action="store_true")
    parser.add_argument("--compose-file", action="append", default=[])
    args = parser.parse_args()
    checks = 0
    failures = 0

    def check(key: str, valid: bool, reason: str) -> None:
        nonlocal checks, failures
        checks += 1
        if not valid:
            failures += 1
            print(f"PREFLIGHT_FAIL {key} {reason}")

    path = Path(args.env_file)
    try:
        check("ENV_FILE", stat.S_IMODE(path.stat().st_mode) == 0o600, "mode_must_be_600")
        values = parse_env(path.read_text())
    except (OSError, UnicodeError, ValueError):
        check("ENV_FILE", False, "unreadable_or_invalid_syntax")
        print(f"PREFLIGHT_RESULT checks={checks} failures={failures}")
        return 1
    raw_values = values
    forbidden = forbidden_compose_controls(raw_values, dict(os.environ))
    for key in sorted(forbidden):
        check(key, False, "compose_control_forbidden")
    if forbidden:
        print(f"PREFLIGHT_RESULT checks={checks} failures={failures}")
        return 1
    try:
        clean_head(ROOT)
        check("RELEASE_CHECKOUT", True, "release_checkout_verified")
    except (IdentityError, OSError, ValueError, subprocess.SubprocessError) as error:
        reason = str(error) if isinstance(error, IdentityError) else "release_git_unavailable"
        check("RELEASE_IDENTITY", False, reason)
        print(f"PREFLIGHT_RESULT checks={checks} failures={failures}")
        return 1
    check("IMAGE_REFERENCE", valid_image_reference(args.expected_project, "probe"),
          "invalid_docker_image_reference")
    for name in AUTO_OVERRIDE_FILES:
        check("COMPOSE_CONFIG", not (ROOT / name).exists(), "automatic_override_forbidden")
    try:
        override_keys = shell_override_keys(raw_values, args.compose_file)
    except (OSError, UnicodeError):
        check("COMPOSE_CONFIG", False, "configuration_unreadable")
        print(f"PREFLIGHT_RESULT checks={checks} failures={failures}")
        return 1
    for key in sorted(override_keys):
        check(key, key not in os.environ or os.environ[key] == raw_values.get(key, ""), "shell_override_mismatch")
    try:
        config, values = compose_effective(path, args.compose_file)
        services = config["services"]
        expected_default, _ = compose_effective(path, args.compose_file, all_profiles=False)
        actual_default = deploy_config(path)
        check("COMPOSE_CONFIG", actual_default == expected_default, "deployment_configuration_mismatch")
        for model in (config, expected_default, actual_default):
            check("PYTHON_MOUNTS", python_mounts_safe(model, ROOT), "python_code_mount_forbidden")
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        check("COMPOSE_CONFIG", False, "effective_configuration_unavailable")
        print(f"PREFLIGHT_RESULT checks={checks} failures={failures}")
        return 1
    for key in REQUIRED:
        check(key, key in raw_values and bool(values.get(key, "").strip()), "required_nonempty_after_interpolation")
    check("COMPOSE_PROJECT_NAME", bool(re.fullmatch(r"[a-z0-9][a-z0-9_-]*", args.expected_project))
          and config.get("name") == args.expected_project == values.get("COMPOSE_PROJECT_NAME"),
          "effective_project_mismatch")
    ports = services.get("nginx", {}).get("ports", [])
    for target, key in ((80, "NGINX_HTTP_PORT"), (443, "NGINX_HTTPS_PORT")):
        matches = [p for p in ports if p.get("target") == target]
        check(key, len(matches) == 1 and matches[0].get("host_ip") == values.get("NGINX_BIND_ADDRESS")
              and str(matches[0].get("published")) == values.get(key), "effective_binding_mismatch")
    chatwoot = services.get("chatwoot", {}).get("environment", {})
    for key, field in (("CHATWOOT_SECRET_KEY", "SECRET_KEY_BASE"),
                       *((key, key) for key in SMTP_KEYS)):
        check(key, chatwoot.get(field, "") == values.get(key, ""), "effective_service_value_mismatch")
    smtp = {key: values.get(key, "").strip() for key in SMTP_KEYS}
    if args.require_smtp or any(smtp.values()):
        for key, value in smtp.items():
            check(key, bool(value), "smtp_group_required_nonempty")
    else:
        print("PREFLIGHT_WARN SMTP_CONFIG smtp_disabled")
    for key in SECRET_KEYS:
        if values.get(key):
            check(key, not placeholder(values[key], key), "known_placeholder_forbidden")
    evolution = services.get("evolution", {}).get("environment", {})
    check("AUTHENTICATION_API_KEY", bool(values.get("AUTHENTICATION_API_KEY"))
          and values.get("AUTHENTICATION_API_KEY") == values.get("EVOLUTION_API_KEY")
          == evolution.get("AUTHENTICATION_API_KEY"), "evolution_api_key_mismatch")
    check("DATABASE_CONNECTION_URI", connection_matches(
        evolution.get("DATABASE_CONNECTION_URI", ""), ("postgresql", "postgres"),
        values.get("POSTGRES_USERNAME", ""), values.get("POSTGRES_PASSWORD", ""),
        values.get("POSTGRES_DATABASE", "")) and evolution.get("DATABASE_CONNECTION_URI")
        == values.get("DATABASE_CONNECTION_URI"), "postgres_connection_mismatch")
    check("DATABASE_URL", connection_matches(values.get("DATABASE_URL", ""), ("mysql+aiomysql",),
        values.get("MYSQL_USER", ""), values.get("MYSQL_PASSWORD", ""),
        values.get("MYSQL_DATABASE", "")), "mysql_connection_mismatch")
    version = values.get("DELIVERY_WORKER_CODE_VERSION", "")
    images = {services.get(name, {}).get("image") for name in PYTHON_SERVICES}
    check("IMAGE_REFERENCE", valid_image_reference(config.get("name", ""), version),
          "invalid_docker_image_reference")
    # Tag is scoped to the effective project so another stack cannot replace it.
    check("DELIVERY_WORKER_CODE_VERSION", bool(config.get("name")) and bool(version)
          and images == {f"{config.get('name')}-app:{version}"}, "project_scoped_image_required")
    check("WPP_REQUIRE_IMAGE_VERSION", all(services.get(name, {}).get("environment", {}).get(
          "WPP_REQUIRE_IMAGE_VERSION") == "true" for name in PYTHON_SERVICES), "image_guard_required")
    try:
        image = services["app"]["image"]
        inspected = run(["docker", "image", "inspect", image, "--format", "{{json .Config.Labels}}"],
                                   capture_output=True, text=True, timeout=15, check=True)
        label = json.loads(inspected.stdout).get("org.opencontainers.image.revision")
        check("DELIVERY_WORKER_CODE_VERSION", label == version, "image_revision_mismatch")
    except (OSError, ValueError, KeyError, AttributeError, subprocess.SubprocessError):
        check("DELIVERY_WORKER_CODE_VERSION", False, "image_missing_or_unreadable")
    check("DELIVERY_MODE", values.get("DELIVERY_MODE") == ("direct" if args.phase == "legacy" else "outbox"),
          "phase_mismatch")
    dispatch = values.get("DELIVERY_DISPATCH_ENABLED", "").lower()
    check("DELIVERY_DISPATCH_ENABLED", dispatch == "false" if args.phase == "legacy"
          else dispatch in ("false", "true"), "phase_boolean_required")
    epoch = values.get("DELIVERY_CUTOVER_EPOCH")
    valid_epoch = epoch == ""  # Absence is not an explicit legacy configuration.
    if args.phase == "outbox":
        try:
            valid_epoch = str(UUID(epoch or "")) == epoch.lower()
        except ValueError:
            valid_epoch = False
    check("DELIVERY_CUTOVER_EPOCH", valid_epoch, "empty_in_legacy_uuid_in_outbox")
    for key in ("INBOX_SCHEDULER_ENABLED", "INBOX_TURN_PROCESSOR_ENABLED"):
        check(key, values.get(key, "").lower() == "false", "must_remain_false")
    auth = values.get("PROVIDER_WEBHOOK_AUTH_MODE")
    check("PROVIDER_WEBHOOK_AUTH_MODE", auth == "enforced" if args.phase == "outbox"
          else auth in ("provisioning", "enforced"), "phase_auth_mismatch")
    if auth == "enforced":
        for key in ("EVOLUTION_WEBHOOK_BEARER_TOKEN", "CHATWOOT_WEBHOOK_SECRET"):
            check(key, bool(values.get(key, "").strip()), "required_when_enforced")
        try:
            keys = json.loads(values.get("INBOUND_RECEIPT_HMAC_KEYS", ""))
            active = values.get("INBOUND_RECEIPT_HMAC_CURRENT_VERSION")
            valid_keys = isinstance(keys, dict) and isinstance(keys.get(active), str) and bool(keys[active].strip())
        except (ValueError, TypeError):
            valid_keys = False
        check("INBOUND_RECEIPT_HMAC_KEYS", valid_keys, "active_key_required_when_enforced")
    try:
        callback = urlsplit(values.get("PROVIDER_CALLBACK_BASE_URL", ""))
        host = (callback.hostname or "").rstrip(".")
        port = callback.port
        valid_callback = callback.scheme in ("http", "https") and bool(host) and (port is None or 1 <= port <= 65535)
        try:
            private = not ipaddress.ip_address(host).is_global
        except ValueError:
            private = "." not in host or host.endswith((".localhost", ".local", ".internal"))
    except ValueError:
        valid_callback, private = False, True
    check("PROVIDER_CALLBACK_BASE_URL", valid_callback, "invalid_callback_url")
    safe_fetch = values.get("CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK", "").lower()
    check("CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK", safe_fetch in ("true", "false")
          and (not private or safe_fetch == "true"), "private_callback_requires_true")
    try:
        sha = run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True, check=True, timeout=10).stdout.strip()
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        sha = None
    version = values.get("DELIVERY_WORKER_CODE_VERSION", "")
    try:
        verify_receipt(ROOT, args.expected_project, version)
        check("RELEASE_IDENTITY", True, "release_receipt_verified")
    except (IdentityError, OSError, ValueError, subprocess.SubprocessError) as error:
        reason = str(error) if isinstance(error, IdentityError) else "release_receipt_unavailable"
        check("RELEASE_IDENTITY", False, reason)
    check("DELIVERY_WORKER_CODE_VERSION", bool(sha) and version == sha
          and bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", version)), "must_match_head_short_sha")
    for key in ("OPENAI_ENABLED", "DEBUG"):
        check(key, key not in values or values[key].lower() == "false", "must_be_absent_or_false")
    for key in ("OPENAI_API_KEY", "OPENAI_MODEL"):
        if key in values:
            print(f"PREFLIGHT_WARN {key} ignored_by_app")
    check("NGINX_BIND_ADDRESS", auth == "enforced" or values.get("NGINX_BIND_ADDRESS") == "127.0.0.1",
          "provisioning_requires_loopback")
    certs = ROOT / "nginx/certs"
    has_certs = has_tls_certificates(certs)
    try:
        dev_conf = (ROOT / values.get("NGINX_CONF_PATH", "")).resolve() == ROOT / "nginx/nginx.dev.conf"
    except (OSError, ValueError):
        dev_conf = False
    check("NGINX_CONF_PATH", has_certs or dev_conf, "no_certs_requires_dev_conf")
    if args.check_volumes:
        for suffix in VOLUMES:
            try:
                result = run(["docker", "volume", "inspect", f"{args.expected_project}_{suffix}"],
                                        capture_output=True, timeout=15, check=False)
                exists = result.returncode == 0
            except (OSError, subprocess.TimeoutExpired):
                exists = False
            check("VOLUME_" + suffix.upper(), exists, "required_project_volume_missing")
    print(f"PREFLIGHT_RESULT checks={checks} failures={failures}")
    return int(bool(failures))


if __name__ == "__main__":
    try:
        with cancellation():
            code = main()
    except Cancelled as error:
        print("PREFLIGHT_FAIL PROCESS cancelled")
        code = error.code
    except RuntimeError:
        print("PREFLIGHT_FAIL PROCESS private_directory_cleanup_failed")
        code = 1
    raise SystemExit(code)
