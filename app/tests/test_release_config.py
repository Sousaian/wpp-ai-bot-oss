"""Compose/template parity and a counted synthetic preflight matrix."""
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(os.environ.get("WPP_REPO_ROOT", Path(__file__).resolve().parents[2]))


def validate_templates(compose: str, templates: dict[str, str]) -> None:
    variables = set(re.findall(r"(?<!\$)\$\{([A-Z][A-Z0-9_]*)", compose))
    for name, text in templates.items():
        keys = set(re.findall(r"^([A-Z][A-Z0-9_]*)=", text, re.M))
        assert not variables - keys, f"{name}: missing {','.join(sorted(variables - keys))}"


class ReleaseConfigTests(unittest.TestCase):
    def test_compose_template_parity(self) -> None:
        validate_templates("\n".join((ROOT / p).read_text() for p in
                                     ("docker-compose.yml", "docker-compose.dev.yml")),
                           {p: (ROOT / p).read_text() for p in (".env.example", "app/.env.example")})

    def test_preflight_counted_matrix(self) -> None:
        sentinel = "FIXTURE_SECRET_DO_NOT_PRINT"
        base = dict(re.findall(r"^([A-Z][A-Z0-9_]*)=(.*)$", (ROOT / ".env.example").read_text(), re.M))
        for key in ("MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "POSTGRES_PASSWORD", "OPENROUTER_API_KEY",
                    "EVOLUTION_API_KEY", "CHATWOOT_DB_PASSWORD", "CHATWOOT_SECRET_KEY", "CHATWOOT_API_TOKEN"):
            base[key] = sentinel + "_" + key
        for key in ("SMTP_ADDRESS", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD"):
            base[key] = ""
        base.update(COMPOSE_PROJECT_NAME="p48-fixture", DELIVERY_WORKER_CODE_VERSION="abcdef0",
                    CHATWOOT_API_TOKEN=sentinel, OPENROUTER_API_KEY=sentinel,
                    CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK="true", DEBUG="false")
        base.update(AUTHENTICATION_API_KEY=base["EVOLUTION_API_KEY"],
                    DATABASE_URL="mysql+aiomysql://${MYSQL_USER}:${MYSQL_PASSWORD}@db:3306/${MYSQL_DATABASE}",
                    DATABASE_CONNECTION_URI="postgresql://${POSTGRES_USERNAME}:${POSTGRES_PASSWORD}@evolution-postgres:5432/${POSTGRES_DATABASE}?schema=evolution_api")
        enforced = {"PROVIDER_WEBHOOK_AUTH_MODE": "enforced", "EVOLUTION_WEBHOOK_BEARER_TOKEN": sentinel,
                    "CHATWOOT_WEBHOOK_SECRET": sentinel, "INBOUND_RECEIPT_HMAC_KEYS": '{"1":"'+sentinel+'"}'}
        outbox = enforced | {"DELIVERY_MODE": "outbox", "DELIVERY_CUTOVER_EPOCH": "12345678-1234-4234-8234-123456789abc"}
        # label, phase, edits (None removes a key), failed key, mode, extra flags
        cases = [
            ("legacy", "legacy", {}, None, 0o600, []),
            ("outbox_idle", "outbox", outbox, None, 0o600, []),
            ("outbox_dispatch", "outbox", outbox | {"DELIVERY_DISPATCH_ENABLED": "true"}, None, 0o600, []),
            ("permissions", "legacy", {}, "ENV_FILE", 0o644, []),
            ("required_absent", "legacy", {"MYSQL_PASSWORD": None}, "MYSQL_PASSWORD", 0o600, []),
            ("required_empty", "legacy", {"MYSQL_PASSWORD": ""}, "MYSQL_PASSWORD", 0o600, []),
            ("project", "legacy", {"COMPOSE_PROJECT_NAME": sentinel}, "COMPOSE_PROJECT_NAME", 0o600, []),
            ("delivery", "legacy", {"DELIVERY_MODE": "outbox"}, "DELIVERY_MODE", 0o600, []),
            ("dispatch", "legacy", {"DELIVERY_DISPATCH_ENABLED": "true"}, "DELIVERY_DISPATCH_ENABLED", 0o600, []),
            ("dispatch_outbox", "outbox", outbox | {"DELIVERY_DISPATCH_ENABLED": "maybe"}, "DELIVERY_DISPATCH_ENABLED", 0o600, []),
            ("epoch_legacy", "legacy", {"DELIVERY_CUTOVER_EPOCH": sentinel}, "DELIVERY_CUTOVER_EPOCH", 0o600, []),
            ("epoch_outbox", "outbox", outbox | {"DELIVERY_CUTOVER_EPOCH": sentinel}, "DELIVERY_CUTOVER_EPOCH", 0o600, []),
            ("scheduler", "legacy", {"INBOX_SCHEDULER_ENABLED": "true"}, "INBOX_SCHEDULER_ENABLED", 0o600, []),
            ("processor", "legacy", {"INBOX_TURN_PROCESSOR_ENABLED": "true"}, "INBOX_TURN_PROCESSOR_ENABLED", 0o600, []),
            ("auth_legacy", "legacy", {"PROVIDER_WEBHOOK_AUTH_MODE": sentinel}, "PROVIDER_WEBHOOK_AUTH_MODE", 0o600, []),
            ("auth_outbox", "outbox", outbox | {"PROVIDER_WEBHOOK_AUTH_MODE": "provisioning"}, "PROVIDER_WEBHOOK_AUTH_MODE", 0o600, []),
            ("callback", "legacy", {"CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK": "false"}, "CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK", 0o600, []),
            ("callback_invalid", "legacy", {"PROVIDER_CALLBACK_BASE_URL": sentinel}, "PROVIDER_CALLBACK_BASE_URL", 0o600, []),
            ("version", "legacy", {"DELIVERY_WORKER_CODE_VERSION": sentinel}, "DELIVERY_WORKER_CODE_VERSION", 0o600, []),
            ("openai_enabled", "legacy", {"OPENAI_ENABLED": "true"}, "OPENAI_ENABLED", 0o600, []),
            ("debug", "legacy", {"DEBUG": "true"}, "DEBUG", 0o600, []),
            ("legacy_warning", "legacy", {"OPENAI_API_KEY": sentinel, "OPENAI_MODEL": sentinel}, None, 0o600, []),
            ("bind", "legacy", {"NGINX_BIND_ADDRESS": "0.0.0.0"}, "NGINX_BIND_ADDRESS", 0o600, []),
            ("certificates", "legacy", {"NGINX_CONF_PATH": "./nginx/nginx.conf"}, "NGINX_CONF_PATH", 0o600, []),
            ("bearer", "outbox", outbox | {"EVOLUTION_WEBHOOK_BEARER_TOKEN": ""}, "EVOLUTION_WEBHOOK_BEARER_TOKEN", 0o600, []),
            ("chatwoot_secret", "outbox", outbox | {"CHATWOOT_WEBHOOK_SECRET": ""}, "CHATWOOT_WEBHOOK_SECRET", 0o600, []),
            ("hmac", "outbox", outbox | {"INBOUND_RECEIPT_HMAC_KEYS": "{}"}, "INBOUND_RECEIPT_HMAC_KEYS", 0o600, []),
            ("volumes", "legacy", {}, None, 0o600, ["--check-volumes"]),
            ("missing_volumes", "legacy", {}, "VOLUME_MYSQL_DATA", 0o600, ["--check-volumes"]),
        ]
        base["OPERATOR_FUTURE_KEY"] = "fixture-operator"
        import importlib.util
        spec = importlib.util.spec_from_file_location("preflight_matrix", ROOT / "scripts/release_preflight.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        interpolation = module.shell_override_keys(base, [])
        for key in sorted(base):
            cases.append(("shell_" + key, "legacy", {}, key if key in interpolation else None, 0o600, []))
        for key in ("MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "POSTGRES_PASSWORD", "OPENROUTER_API_KEY",
                    "EVOLUTION_API_KEY", "AUTHENTICATION_API_KEY", "CHATWOOT_DB_PASSWORD", "CHATWOOT_SECRET_KEY", "CHATWOOT_API_TOKEN"):
            cases.append(("placeholder_" + key, "legacy", {key: "replace-known-public-placeholder"}, key, 0o600, []))
            cases.append(("unset_" + key, "legacy", {key: "${UNSET_FIXTURE_SECRET}"}, key, 0o600, []))
        cases += [
            ("smtp_disabled", "legacy", {}, None, 0o600, []),
            ("smtp_required", "legacy", {}, "SMTP_ADDRESS", 0o600, ["--require-smtp"]),
            ("smtp_partial", "legacy", {"SMTP_ADDRESS": "mail.example.invalid"}, "SMTP_PORT", 0o600, []),
            ("smtp_enabled", "legacy", {"SMTP_ADDRESS": "mail.example.invalid", "SMTP_PORT": "587", "SMTP_USERNAME": "fixture-user", "SMTP_PASSWORD": sentinel}, None, 0o600, ["--require-smtp"]),
            ("image_missing", "legacy", {}, "DELIVERY_WORKER_CODE_VERSION", 0o600, []),
            # R3 P2-B: host-global or foreign-project tags are refused.
            ("image_unscoped", "legacy", {}, "DELIVERY_WORKER_CODE_VERSION", 0o600, []),
            ("image_foreign_project", "legacy", {}, "DELIVERY_WORKER_CODE_VERSION", 0o600, []),
        ]
        complete_smtp = {"SMTP_ADDRESS":"mail.example.invalid", "SMTP_PORT":"587", "SMTP_USERNAME":"fixture-user", "SMTP_PASSWORD":sentinel}
        for key in complete_smtp:
            cases.append(("smtp_unset_"+key, "legacy", complete_smtp | {key:"${UNSET_FIXTURE_SECRET}"}, key, 0o600, ["--require-smtp"]))
        cases += [
            ("evolution_key_absent", "legacy", {"AUTHENTICATION_API_KEY":None}, "AUTHENTICATION_API_KEY", 0o600, []),
            ("evolution_key_empty", "legacy", {"AUTHENTICATION_API_KEY":""}, "AUTHENTICATION_API_KEY", 0o600, []),
            ("evolution_key_mismatch", "legacy", {"AUTHENTICATION_API_KEY":sentinel+"-other"}, "AUTHENTICATION_API_KEY", 0o600, []),
        ]
        for label, uri, key in (
            ("mysql_user", "mysql+aiomysql://other:${MYSQL_PASSWORD}@db/${MYSQL_DATABASE}", "DATABASE_URL"),
            ("mysql_password", "mysql+aiomysql://${MYSQL_USER}:other@db/${MYSQL_DATABASE}", "DATABASE_URL"),
            ("mysql_database", "mysql+aiomysql://${MYSQL_USER}:${MYSQL_PASSWORD}@db/other", "DATABASE_URL"),
            ("mysql_scheme", "postgresql://${MYSQL_USER}:${MYSQL_PASSWORD}@db/${MYSQL_DATABASE}", "DATABASE_URL"),
            ("mysql_invalid", "mysql+aiomysql://[bad", "DATABASE_URL"),
            ("postgres_user", "postgresql://other:${POSTGRES_PASSWORD}@db/${POSTGRES_DATABASE}", "DATABASE_CONNECTION_URI"),
            ("postgres_password", "postgresql://${POSTGRES_USERNAME}:other@db/${POSTGRES_DATABASE}", "DATABASE_CONNECTION_URI"),
            ("postgres_public", "postgresql://${POSTGRES_USERNAME}:postgres@db/${POSTGRES_DATABASE}", "DATABASE_CONNECTION_URI"),
            ("postgres_database", "postgresql://${POSTGRES_USERNAME}:${POSTGRES_PASSWORD}@db/other", "DATABASE_CONNECTION_URI"),
            ("postgres_invalid", "not-a-uri", "DATABASE_CONNECTION_URI"),
        ):
            cases.append((label, "legacy", {key:uri}, key, 0o600, []))
        cases += [
            ("mysql_encoded", "legacy", {"MYSQL_PASSWORD":"synthetic@pass", "DATABASE_URL":"mysql+aiomysql://${MYSQL_USER}:synthetic%40pass@db/${MYSQL_DATABASE}"}, None, 0o600, []),
            ("postgres_encoded", "legacy", {"POSTGRES_PASSWORD":"synthetic@pass", "DATABASE_CONNECTION_URI":"postgresql://${POSTGRES_USERNAME}:synthetic%40pass@db/${POSTGRES_DATABASE}"}, None, 0o600, []),
        ]
        safe_controls = {"COMPOSE_ANSI":"never", "COMPOSE_STATUS_STDOUT":"true", "COMPOSE_PROGRESS":"plain",
                         "COMPOSE_MENU":"false", "COMPOSE_HTTP_TIMEOUT":"60", "COMPOSE_PARALLEL_LIMIT":"2"}
        for key, value in safe_controls.items():
            cases.append(("control_safe_"+key, "legacy", {key:value}, None, 0o600, []))
            cases.append(("shell_safe_"+key, "legacy", {}, None, 0o600, []))
        for key, value in {"COMPOSE_FILE":"docker-compose.yml:docker-compose.dev.yml", "COMPOSE_PROFILES":"inbox",
                           "COMPOSE_PATH_SEPARATOR":":", "COMPOSE_ENV_FILES":"other.env", "COMPOSE_FUTURE_CONTROL":"on"}.items():
            cases.append(("control_forbidden_"+key, "legacy", {key:value}, key, 0o600, []))
            cases.append(("shell_forbidden_"+key, "legacy", {}, key, 0o600, []))
        cases += [
            ("mysql_slash", "legacy", {"MYSQL_PASSWORD":"synthetic/pass", "DATABASE_URL":"mysql+aiomysql://${MYSQL_USER}:synthetic/pass@db/${MYSQL_DATABASE}"}, None, 0o600, []),
            ("mysql_hash", "legacy", {"MYSQL_PASSWORD":"synthetic#pass", "DATABASE_URL":"mysql+aiomysql://${MYSQL_USER}:synthetic#pass@db/${MYSQL_DATABASE}"}, None, 0o600, []),
            ("mysql_at_raw", "legacy", {"MYSQL_PASSWORD":"synthetic@pass", "DATABASE_URL":"mysql+aiomysql://${MYSQL_USER}:synthetic@pass@db/${MYSQL_DATABASE}"}, "DATABASE_URL", 0o600, []),
        ]
        cases += [
            ("postgres_slash_encoded", "legacy", {"POSTGRES_PASSWORD":"synthetic/pass", "DATABASE_CONNECTION_URI":"postgresql://${POSTGRES_USERNAME}:synthetic%2Fpass@db/${POSTGRES_DATABASE}"}, None, 0o600, []),
            ("postgres_hash_encoded", "legacy", {"POSTGRES_PASSWORD":"synthetic#pass", "DATABASE_CONNECTION_URI":"postgresql://${POSTGRES_USERNAME}:synthetic%23pass@db/${POSTGRES_DATABASE}"}, None, 0o600, []),
            ("postgres_slash_raw", "legacy", {"POSTGRES_PASSWORD":"synthetic/pass", "DATABASE_CONNECTION_URI":"postgresql://${POSTGRES_USERNAME}:synthetic/pass@db/${POSTGRES_DATABASE}"}, "DATABASE_CONNECTION_URI", 0o600, []),
            ("postgres_hash_raw", "legacy", {"POSTGRES_PASSWORD":"synthetic#pass", "DATABASE_CONNECTION_URI":"postgresql://${POSTGRES_USERNAME}:synthetic#pass@db/${POSTGRES_DATABASE}"}, "DATABASE_CONNECTION_URI", 0o600, []),
        ]
        cases += [("mount_"+model, "legacy", {}, "PYTHON_MOUNTS", 0o600, [])
                  for model in ("base", "default", "deploy")]
        cases += [("file_"+kind+"_"+model+"_"+service, "legacy", {}, "PYTHON_MOUNTS", 0o600, [])
                  for kind in ("configs", "secrets") for model in ("all", "base", "default", "deploy")
                  for service in ("app", "delivery-worker", "turn-worker", "delivery-ops",
                                  "provider-auth-bootstrap", "provider-auth-monitor")]
        expected_cases = 153 + len(base)
        self.assertEqual(len(cases), expected_cases)
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            git = temp / "git"
            git.write_text("#!/bin/sh\nprintf 'abcdef0\\n'\n")
            git.chmod(0o700)
            docker = temp / "docker"
            spy = temp / "calls"
            docker.write_text("#!" + sys.executable + "\n" + r'''
import json,os,re,sys
from pathlib import Path
a=sys.argv[1:]
if a[:2]==["volume","inspect"]:
    with open(os.environ["SPY"],"a") as f: f.write(a[2]+"\n")
    raise SystemExit(int(os.environ.get("DOCKER_EXIT","0")))
if a[:2]==["image","inspect"]:
    if os.environ.get("IMAGE_MISSING") or a[2]!=os.environ["EXPECT_IMAGE"]: raise SystemExit(1)
    print(json.dumps({"org.opencontainers.image.revision":"abcdef0"}));raise SystemExit(0)
assert a[0]=="compose",a
v=dict(line.split("=",1) for line in Path(a[a.index("--env-file")+1]).read_text().splitlines())
interpolation=v | dict(os.environ)
for key in list(interpolation): interpolation[key]=re.sub(r"\$\{([A-Z_]+)\}",lambda m:interpolation.get(m[1],""),interpolation[key])
for k in list(v): v[k]=re.sub(r"\$\{([A-Z_]+)\}",lambda m:interpolation.get(m[1],""),v[k])
if "--environment" in a:
    print("\n".join(k+"="+value for k,value in v.items()));raise SystemExit(0)
image=os.environ.get("IMAGE_REF") or interpolation.get("COMPOSE_PROJECT_NAME","")+"-app:"+interpolation.get("DELIVERY_WORKER_CODE_VERSION","")
services={n:{"image":image,"environment":v | {"WPP_REQUIRE_IMAGE_VERSION":"true"}} for n in ("app","delivery-worker","turn-worker","delivery-ops","provider-auth-bootstrap","provider-auth-monitor")}
model=os.environ.get("MOUNT_MODEL")
if ((model=="base" and "--profile" in a) or (model=="default" and "-f" in a and "--profile" not in a)
    or (model=="deploy" and "-f" not in a)):
    services["app"]["volumes"]=[{"type":"bind","source":"/unrelated","target":"/app"}]
definitions={}
file_kind=os.environ.get("FILE_KIND")
file_model=os.environ.get("FILE_MODEL")
if file_kind and (file_model=="all" or (file_model=="base" and "--profile" in a)
    or (file_model=="default" and "-f" in a and "--profile" not in a)
    or (file_model=="deploy" and "-f" not in a)):
    services[os.environ["FILE_SERVICE"]][file_kind]=[{"source":"synthetic","target":"/app/src/config.py"}]
    definitions[file_kind]={"synthetic":{"file":"/unrelated/synthetic"}}
services["evolution"]={"environment":v}
services["chatwoot"]={"environment":{k:(interpolation.get("CHATWOOT_SECRET_KEY","") if k=="SECRET_KEY_BASE" else interpolation.get(k,"")) for k in ("SECRET_KEY_BASE","SMTP_ADDRESS","SMTP_PORT","SMTP_USERNAME","SMTP_PASSWORD")}}
services["nginx"]={"ports":[{"target":port,"published":interpolation.get(key,""),"host_ip":interpolation.get("NGINX_BIND_ADDRESS")} for port,key in ((80,"NGINX_HTTP_PORT"),(443,"NGINX_HTTPS_PORT"))]}
print(json.dumps({"name":interpolation.get("COMPOSE_PROJECT_NAME"),"services":services}|definitions))
''')
            docker.chmod(0o700)
            count = 0
            for label, phase, edits, failure, mode, flags in cases:
                with self.subTest(label=label):
                    values = base.copy()
                    for key, value in edits.items():
                        if value is None:
                            values.pop(key, None)
                        else:
                            values[key] = value
                    fixture = temp / ".env"
                    fixture.write_text("\n".join(f"{k}={v}" for k, v in values.items()) + "\n")
                    fixture.chmod(mode)
                    before = fixture.read_bytes(), fixture.stat().st_mtime_ns, fixture.stat().st_mode
                    # Deliberately no os.environ inheritance: operator-loaded
                    # secrets/configuration must never affect synthetic fixtures.
                    env = {"PATH": str(temp) + os.pathsep + str(Path(sys.executable).parent) + os.pathsep + os.defpath,
                           "HOME": str(temp), "LANG": "C.UTF-8", "SPY": str(spy),
                           "DOCKER_EXIT": "1" if label == "missing_volumes" else "0"}
                    env["EXPECT_IMAGE"] = "p48-fixture-app:abcdef0"
                    if label.startswith("mount_"): env["MOUNT_MODEL"] = label.removeprefix("mount_")
                    if label.startswith("file_"):
                        _,env["FILE_KIND"],env["FILE_MODEL"],env["FILE_SERVICE"] = label.split("_",3)
                    if label == "image_unscoped": env["IMAGE_REF"] = "wpp-app:abcdef0"
                    if label == "image_foreign_project": env["IMAGE_REF"] = "p48-other-app:abcdef0"
                    if label.startswith("shell_") and not label.startswith(("shell_safe_", "shell_forbidden_")):
                        key = label.removeprefix("shell_")
                        env[key] = "shell-different-" + key
                    if label.startswith("shell_forbidden_"):
                        env[label.removeprefix("shell_forbidden_")] = "forbidden-control"
                    if label.startswith("shell_safe_"):
                        key = label.removeprefix("shell_safe_")
                        env[key] = safe_controls[key]
                    if label == "image_missing": env["IMAGE_MISSING"] = "1"
                    env.pop("UNSET_FIXTURE_SECRET", None)
                    # Config-only matrix: explicit origin double. The independent
                    # identity suite exercises the real clean/receipt mechanism.
                    adapter = "import sys;sys.path.insert(0,sys.argv.pop(1));import release_preflight as p;p.verify_receipt=lambda *a:None;p.clean_head=lambda *a:None;raise SystemExit(p.main())"
                    result = subprocess.run([sys.executable, "-c", adapter, str(ROOT / "scripts"), str(fixture),
                                             "--expected-project", "p48-fixture", "--phase", phase, *flags],
                                            capture_output=True, text=True, env=env)
                    combined = result.stdout + result.stderr
                    self.assertNotIn(sentinel, combined)
                    self.assertEqual(result.returncode, int(failure is not None), combined)
                    self.assertRegex(result.stdout, r"PREFLIGHT_RESULT checks=\d+ failures=\d+")
                    if failure:
                        self.assertIn("PREFLIGHT_FAIL " + failure + " ", combined)
                    if label.startswith("image_") and label != "image_missing":
                        self.assertIn("PREFLIGHT_FAIL DELIVERY_WORKER_CODE_VERSION project_scoped_image_required", combined)
                    if failure is None:
                        self.assertNotIn("project_scoped_image_required", combined)
                    if label == "legacy_warning":
                        self.assertIn("PREFLIGHT_WARN OPENAI_API_KEY", combined)
                        self.assertIn("PREFLIGHT_WARN OPENAI_MODEL", combined)
                    self.assertEqual(before, (fixture.read_bytes(), fixture.stat().st_mtime_ns, fixture.stat().st_mode))
                    count += 1
                    print(f"T3_PREFLIGHT PASS case={label}")
            self.assertEqual(count, expected_cases)
            calls = spy.read_text().splitlines()
            self.assertEqual(len(calls), 12)
            self.assertEqual(set(calls), {"p48-fixture_" + s for s in
                             ("mysql_data", "app_logs", "evolution_instances", "evolution_redis", "postgres_data", "chatwoot_pg_data")})
            print(f"T3_MATRIX PASS cases={expected_cases} volume_inspects=12 secret_leaks=0 writes=0")

    def test_certificate_pair_matrix(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("preflight", ROOT / "scripts/release_preflight.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for names in ((), ("fullchain.pem",), ("privkey.pem",), ("fullchain.pem", "privkey.pem")):
                for name in ("fullchain.pem", "privkey.pem"):
                    (root / name).unlink(missing_ok=True)
                for name in names:
                    (root / name).write_text("synthetic")
                self.assertEqual(module.has_tls_certificates(root), len(names) == 2)
                print("R2_CERT_CASE PASS")


    def test_shell_interpolation_key_discovery(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("preflight", ROOT / "scripts/release_preflight.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            extra = Path(directory) / "override.yml"
            extra.write_text('services:\n  app:\n    environment:\n      EXTRA: ${P48_ONLY_IN_OVERRIDE:-default}\n      BARE: $P48_BARE\n')
            keys = module.shell_override_keys({"ARBITRARY":"${NESTED_VALUE}"}, [str(extra)])
            self.assertTrue({"NESTED_VALUE", "P48_ONLY_IN_OVERRIDE", "P48_BARE"} <= keys)
            self.assertNotIn("ARBITRARY", keys)


    def test_mysql_parser_matches_sqlalchemy_oracle(self) -> None:
        import importlib.util
        from sqlalchemy.engine import make_url
        spec = importlib.util.spec_from_file_location("preflight_parser", ROOT / "scripts/release_preflight.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cases = ["plain", "p/ss", "p#ss", "p@ss", "p%40ss", "p%2Fss", "p%23ss", "p:ss", "p%25ss"]
        for password in cases:
            uri = "mysql+aiomysql://user:"+password+"@db:3306/database"
            expected = make_url(uri)
            self.assertEqual(module.mysql_components(uri), (expected.drivername, expected.host,
                             expected.username, expected.password, expected.database))
            print("R4_SQLALCHEMY_ORACLE PASS")
        self.assertEqual(len(cases), 9)

    def test_python_mount_model_matrix(self) -> None:
        import importlib.util
        from copy import deepcopy
        spec=importlib.util.spec_from_file_location("mount_model",ROOT/"scripts/release_preflight.py")
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/"app/src").mkdir(parents=True)
            base={"name":"p51-unit","volumes":{"app_logs":{"name":"p51-unit_app_logs"}},"services":{n:{"volumes":[{"type":"volume","source":"app_logs","target":"/app/logs"}]} for n in module.PYTHON_SERVICES}}
            self.assertTrue(module.python_mounts_safe(base,root))
            bad=[{"type":"bind","source":"./app","target":"/app"},
                {"type":"bind","source":"./app","target":"/outside","read_only":True},
                {"type":"bind","source":"./app/src","target":"/outside"},
                {"type":"volume","source":"other","target":"/app/logs"},
                {"type":"volume","source":"app_logs","target":"/app/logs/sub"},
                *({"type":"volume","source":"other","target":t} for t in ("/app/src","/app//src","//app/src","/app/a/../src","/app")),
                {"type":"bind","source":"/unrelated","target":"/app/logs","read_only":True},
                {"type":"volume","source":"app_logs","target":"/app/logs/."}]
            count=0
            for service in module.PYTHON_SERVICES:
                for mount in bad:
                    model=deepcopy(base);model["services"][service]["volumes"]=[mount]
                    self.assertFalse(module.python_mounts_safe(model,root))
                    count+=1;print("P2R5_MOUNT_CASE PASS")
                for edits in ({"tmpfs":["/app/src:rw"]},{"volumes_from":["other"]}):
                    model=deepcopy(base);model["services"][service].update(edits)
                    self.assertFalse(module.python_mounts_safe(model,root));count+=1;print("P2R5_MOUNT_CASE PASS")
                model=deepcopy(base);model["services"][service]["volumes"].append({"type":"bind","source":"./app-sibling","target":"/outside"})
                self.assertTrue(module.python_mounts_safe(model,root));count+=1;print("P2R5_MOUNT_CASE PASS")
            for edits in ({"external":True},{"name":"other_app_logs"},{"driver_opts":{"type":"none","o":"bind","device":str(root/"app")}}):
                model=deepcopy(base);model["volumes"]["app_logs"].update(edits)
                self.assertFalse(module.python_mounts_safe(model,root));count+=1;print("P2R5_MOUNT_CASE PASS")
            self.assertEqual(count,93)

    def test_mysql_port_matches_sqlalchemy_oracle(self) -> None:
        import importlib.util
        from sqlalchemy.engine import make_url
        spec = importlib.util.spec_from_file_location("port_oracle", ROOT / "scripts/release_preflight.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for port in (None, "", "0", "3306", "bad"):
            uri = "mysql+aiomysql://u:p@db" + ("" if port is None else ":"+port) + "/w"
            try:
                expected = make_url(uri)
            except ValueError:
                with self.assertRaises(ValueError): module.mysql_components(uri)
            else:
                self.assertEqual(module.mysql_components(uri), (expected.drivername, expected.host,
                    expected.username, expected.password, expected.database))
            print("R5_SQLALCHEMY_PORT PASS")

    def test_docker_reference_matrix(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("preflight_reference", ROOT / "scripts/release_preflight.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cases = [("p48-a", True), ("p48-a__b", True), ("p48-a_b", True), ("p48-a--b", True),
                 ("p48-a_", False), ("p48-a___b", False), ("p48-A", False), ("p48-"+"a"*251, False)]
        for project, valid in cases:
            self.assertEqual(module.valid_image_reference(project, "abc123"), valid)
            print("R4_IMAGE_REFERENCE PASS")
        self.assertEqual(len(cases), 8)
        boundaries = [(232, True), (233, True), (234, True), (242, True), (243, True), (244, False), (250, False), (251, False)]
        for size, valid in boundaries:
            self.assertEqual(module.valid_image_reference("p50-"+"a"*(size-4), "v1"), valid)
            print("R5_REFERENCE_BOUNDARY PASS")
        for size, valid in ((127, True), (128, True), (129, False)):
            self.assertEqual(module.valid_image_reference("p50-tags", "v"*size), valid)
            print("R5_REFERENCE_BOUNDARY PASS")
