"""Counted read-only Compose proof; synthetic .env sentinel, no containers/up.

Pass a synthetic candidate naming an already-built project-scoped image. Logs
must live inside the checkout so the copied project's git identity stays HEAD.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import ModuleType

ROOT = Path(__file__).resolve().parent.parent


def load_preflight(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("release_preflight_real", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module



def assert_discovery_equivalent(project: Path, environment: dict[str, str],
                                explicit_env_file: Path) -> None:
    """Oracle: same snapshot/cwd/process env, only --env-file differs."""
    command = ["docker", "compose", "--project-directory", str(project)]
    models = []
    for options in (["--env-file", str(explicit_env_file)], []):
        result = subprocess.run(command + options + ["config", "--format", "json"],
                                cwd=project, env=environment, capture_output=True,
                                text=True, timeout=60, check=True)
        models.append(json.loads(result.stdout))
    assert models[0] == models[1], "implicit_env_discovery_mismatch"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate")
    parser.add_argument("--log-root", required=True)
    parser.add_argument("--resource-prefix", default="p50-")
    args = parser.parse_args()
    logs = Path(args.log_root).resolve()
    if not logs.is_relative_to(ROOT / ".test-runs"):
        raise SystemExit("log_root_must_be_checkout_test_runs")
    logs.mkdir(parents=True, exist_ok=True)
    module = load_preflight(ROOT / "scripts/release_preflight.py")
    base = module.parse_env(Path(args.candidate).read_text())
    project_name = base["COMPOSE_PROJECT_NAME"]
    if not project_name.startswith(args.resource_prefix):
        raise SystemExit("synthetic_project_prefix_required")
    project = logs / "project"
    (project / "scripts").mkdir(parents=True, exist_ok=True)
    (project / "app").mkdir(exist_ok=True)
    (project / "nginx").mkdir(exist_ok=True)
    for name in ("docker-compose.yml", ".env.example", "app/.env.example",
                 "nginx/nginx.dev.conf", "scripts/release_preflight.py", "scripts/release_preflight.sh",
                 "scripts/release_process.py", "scripts/release_identity.py"):
        shutil.copyfile(ROOT / name, project / name)
    # This matrix proves config resolution, NOT release origin. Its copied
    # project is a synthetic model, not a committed clean Git checkout.
    adapter = project / "scripts/config-fixture.py"
    adapter.write_text("import release_preflight as p\np.verify_receipt=lambda *a:None\np.clean_head=lambda *a:None\nraise SystemExit(p.main())\n")
    sentinel = "P48_LOCAL_DOTENV_MUST_NOT_BE_READ"
    local_env = project / ".env"
    local_env.write_text("COMPOSE_PROJECT_NAME=p48-wrong-local\nMYSQL_PASSWORD=" + sentinel
                         + "\nAUTHENTICATION_API_KEY=" + sentinel + "\nP48_ONLY_IN_LOCAL_ENV=" + sentinel + "\n")
    local_env.chmod(0o600)
    # Put probes in the synthetic base itself: release overrides must now
    # equal the no--f deployment model, not silently alter its runtime graph.
    base_model = project / "docker-compose.yml"
    original_model = base_model.read_text()
    base_model.write_text(original_model.replace('      WPP_REQUIRE_IMAGE_VERSION: "true"',
        '      WPP_REQUIRE_IMAGE_VERSION: "true"\n      LOCAL_DOTENV_PROBE: ${P48_ONLY_IN_LOCAL_ENV:-}\n      SHELL_ONLY_PROBE: ${P48_ONLY_IN_MODEL:-}', 1))
    shutil.copyfile(ROOT / "docker-compose.dev.yml", project / "docker-compose.dev.yml")
    env = {"PATH": str(Path(sys.executable).parent) + os.pathsep + os.defpath,
           "HOME": str(logs), "LANG": "C.UTF-8"}
    cases = [("valid", {}, {}, None), ("shell_equal", {}, {"MYSQL_PASSWORD":base["MYSQL_PASSWORD"]}, None)]
    for key in ("MYSQL_PASSWORD", "NGINX_CONF_PATH", "CHATWOOT_SECRET_KEY", "AUTHENTICATION_API_KEY",
                "DELIVERY_WORKER_CODE_VERSION", "P48_ONLY_IN_MODEL"):
        failure = key if key in module.shell_override_keys(base, []) or key == "P48_ONLY_IN_MODEL" else None
        cases.append(("shell_" + key, {}, {key:"shell-different-synthetic"}, failure))
    cases += [
        ("api_placeholder", {"AUTHENTICATION_API_KEY":"sua-evolution-api-key-aqui"}, {}, "AUTHENTICATION_API_KEY"),
        ("api_missing", {"AUTHENTICATION_API_KEY":None}, {}, "AUTHENTICATION_API_KEY"),
        ("api_empty", {"AUTHENTICATION_API_KEY":""}, {}, "AUTHENTICATION_API_KEY"),
        ("api_unset", {"AUTHENTICATION_API_KEY":"${UNSET_P48_API_KEY}"}, {}, "AUTHENTICATION_API_KEY"),
        ("api_mismatch", {"AUTHENTICATION_API_KEY":"synthetic-other-key"}, {}, "AUTHENTICATION_API_KEY"),
        ("mysql_uri", {"DATABASE_URL":"mysql+aiomysql://other:other@db/other"}, {}, "DATABASE_URL"),
        ("postgres_uri", {"DATABASE_CONNECTION_URI":"postgresql://postgres:postgres@evolution-postgres/evolution"}, {}, "DATABASE_CONNECTION_URI"),
    ]
    cases += [("shell_debug_ignored", {}, {"DEBUG":"1"}, None),
              ("shell_log_ignored", {}, {"LOG_LEVEL":"debug"}, None),
              ("mysql_slash", {"MYSQL_PASSWORD":"synthetic/pass", "DATABASE_URL":"mysql+aiomysql://"+base["MYSQL_USER"]+":synthetic/pass@db/"+base["MYSQL_DATABASE"]}, {}, None),
              ("mysql_hash", {"MYSQL_PASSWORD":"synthetic#pass", "DATABASE_URL":"mysql+aiomysql://"+base["MYSQL_USER"]+":synthetic#pass@db/"+base["MYSQL_DATABASE"]}, {}, None),
              ("mysql_raw_at", {"MYSQL_PASSWORD":"synthetic@pass", "DATABASE_URL":"mysql+aiomysql://"+base["MYSQL_USER"]+":synthetic@pass@db/"+base["MYSQL_DATABASE"]}, {}, "DATABASE_URL")]
    safe = {"COMPOSE_ANSI":"never", "COMPOSE_STATUS_STDOUT":"true", "COMPOSE_PROGRESS":"plain",
            "COMPOSE_MENU":"false", "COMPOSE_HTTP_TIMEOUT":"60", "COMPOSE_PARALLEL_LIMIT":"2"}
    controls = {"COMPOSE_FILE":"docker-compose.yml:docker-compose.dev.yml", "COMPOSE_PROFILES":"inbox",
                "COMPOSE_PATH_SEPARATOR":":", "COMPOSE_ENV_FILES":"other.env", "COMPOSE_UNKNOWN_CONTROL":"on"}
    for key, value in controls.items():
        cases += [("control_"+key, {key:value}, {}, key), ("shell_"+key, {}, {key:value}, key)]
    for key, value in safe.items():
        cases += [("safe_"+key, {key:value}, {}, None), ("shell_safe_"+key, {}, {key:value}, None)]
    cases += [("invalid_reference_tail", {"COMPOSE_PROJECT_NAME":"p48-a_"}, {}, "IMAGE_REFERENCE"),
              ("invalid_reference_separator", {"COMPOSE_PROJECT_NAME":"p48-a___b"}, {}, "IMAGE_REFERENCE")]
    assert len(cases) == 44
    for label, edits, shell, failure in cases:
        values = base | edits
        candidate = logs / (label + ".env")
        candidate.write_text("\n".join(k + "=" + value for k, value in values.items() if value is not None) + "\n")
        candidate.chmod(0o600)
        before = (local_env.read_bytes(), local_env.stat().st_mtime_ns, candidate.read_bytes(), candidate.stat().st_mtime_ns)
        result = subprocess.run([sys.executable, str(adapter), str(candidate),
                                 "--expected-project", values["COMPOSE_PROJECT_NAME"]],
                                env=env | shell, capture_output=True, text=True, timeout=60)
        output = result.stdout + result.stderr
        (logs / (label + ".log")).write_text(output)
        assert sentinel not in output, label
        assert result.returncode == int(failure is not None), (label, output)
        if failure:
            assert "PREFLIGHT_FAIL " + failure + " " in output, (label, output)
        assert before == (local_env.read_bytes(), local_env.stat().st_mtime_ns, candidate.read_bytes(), candidate.stat().st_mtime_ns)
        print("R3_REAL_PREFLIGHT PASS case=" + label, flush=True)
    print("R5_CONFIG_FIXTURE origin=explicit_double compose=real", flush=True)
    # Inspect the actual resolved runtime config, not just sanitized diagnostics.
    copied = load_preflight(project / "scripts/release_preflight.py")
    previous = os.environ.copy()
    try:
        os.environ.clear()
        os.environ.update(env)
        config, values = copied.compose_effective(logs / "valid.env", [])
    finally:
        os.environ.clear()
        os.environ.update(previous)
    assert sentinel not in json.dumps(config)
    assert values.get("LOCAL_DOTENV_PROBE") == ""
    assert config["name"] == project_name
    print("R3_LOCAL_DOTENV_SENTINEL PASS used=0 leaks=0 writes=0", flush=True)
    # Preflight's explicit snapshot/.env is equivalent to deployment's implicit
    # discovery only for the SAME candidate snapshot and blocked Compose selectors.
    discovery = logs / "discovery-snapshot"
    discovery.mkdir(exist_ok=True)
    for name in (*module.DEFAULT_COMPOSE_FILES, *module.AUTO_OVERRIDE_FILES):
        source = project / name
        if source.is_file():
            shutil.copyfile(source, discovery / name)
    shutil.copyfile(logs / "valid.env", discovery / ".env")
    (discovery / ".env").chmod(0o600)
    assert_discovery_equivalent(discovery, env, discovery / ".env")
    print("R4_ENV_DISCOVERY_EQUIVALENCE PASS explicit=1 implicit=1", flush=True)
    other = logs / "wrong-discovery.env"
    other.write_text((discovery / ".env").read_text().replace(
        "COMPOSE_PROJECT_NAME=" + project_name,
        "COMPOSE_PROJECT_NAME=" + args.resource_prefix + "wrong-discovery", 1))
    other.chmod(0o600)
    try:
        assert_discovery_equivalent(discovery, env, other)
    except AssertionError as error:
        assert str(error) == "implicit_env_discovery_mismatch"
    else:
        raise AssertionError("wrong_env_file_mutation_survived")
    print("R4_ENV_DISCOVERY_MUTATION PASS variants=1", flush=True)
    # Automatic overrides and alternate default files must be caught even when
    # no COMPOSE_* variable is present and explicit-base validation is healthy.
    model_cases = [*module.AUTO_OVERRIDE_FILES, "compose.yaml", "explicit_dev"]
    for name in model_cases:
        if name in module.AUTO_OVERRIDE_FILES:
            file = project / name
            file.write_text('services:\n  app:\n    environment:\n      WPP_DEV_BIND_CODE: "true"\n')
        elif name == "compose.yaml":
            file = project / name
            file.write_text(base_model.read_text().replace('WPP_REQUIRE_IMAGE_VERSION: "true"', 'WPP_REQUIRE_IMAGE_VERSION: "false"', 1))
        else:
            file = None
        command = [sys.executable, str(adapter), str(logs / "valid.env"),
                   "--expected-project", project_name]
        if name == "explicit_dev": command += ["--compose-file", str(project / "docker-compose.dev.yml")]
        try:
            result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=60)
            output = result.stdout + result.stderr
            (logs / (name + ".log")).write_text(output)
            assert result.returncode == 1 and "deployment_configuration_mismatch" in output, (name, output)
            if name in module.AUTO_OVERRIDE_FILES: assert "automatic_override_forbidden" in output, name
            assert sentinel not in output, name
        finally:
            if file is not None: file.unlink()
        print("R4_REAL_MODEL_DRIFT PASS case="+name, flush=True)
    assert len(model_cases) == 6
    print("R4_REAL_PREFLIGHT_MATRIX PASS cases=44 model_drift=6 sentinel=1 discovery_equivalence=1 discovery_mutations=1", flush=True)


if __name__ == "__main__":
    main()
