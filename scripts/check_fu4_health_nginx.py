"""FU-4 network boundary proof. All Docker resources belong to the run prefix."""
from __future__ import annotations

import os
from pathlib import Path
import re
import secrets
import subprocess
import time

ROOT = Path(__file__).resolve().parent.parent
PREFIX = os.environ.get("WPP_TEST_RESOURCE_PREFIX", "fu4-")
if not re.fullmatch(r"[a-z][a-z0-9-]*-", PREFIX):
    raise SystemExit("invalid resource prefix")
SUFFIX = f"{int(time.time())}-{os.getpid()}"
LOG = Path(os.environ.get("WPP_TEST_LOG_ROOT", ROOT / ".test-runs" / f"fu4-{SUFFIX}"))
LOG.mkdir(parents=True, exist_ok=True)
NETWORK = f"{PREFIX}nginx-net-{SUFFIX}"
MYSQL = f"{PREFIX}nginx-mysql-{SUFFIX}"
APP = f"{PREFIX}nginx-app-{SUFFIX}"
IMAGE = os.environ.get("WPP_TEST_IMAGE", f"{PREFIX}nginx-app-{SUFFIX}:local")
VOLUME = f"{PREFIX}nginx-data-{SUFFIX}"
containers: list[str] = []
built = False
password = secrets.token_hex(24)


def run(args, *, data=None, check=True):
    result = subprocess.run(args, input=data, text=True, capture_output=True)
    if check and result.returncode:
        # Never print Docker argv/environment (contains synthetic credentials).
        raise RuntimeError(f"command failed ({args[0]}): {result.stderr[-2000:]}")
    return result


def shared_count():
    return len(run(["docker", "ps", "-aq", "--filter",
                    "label=com.docker.compose.project=traycer-wpp-ai-bot-soft-ocelot"]).stdout.splitlines())


def sql(statement):
    return run(["docker", "exec", "-i", "-e", f"MYSQL_PWD={password}", MYSQL,
                "mysql", "-uroot", "fu4_health"], data=statement)


# F0-T4: route x method, exact normalization and no-upstream proof.
PUBLIC = {"/": "GET", "/health": "GET", "/webhook": "POST", "/chatwoot/webhook": "POST"}
METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS")
DENIED = (
    "/agentes", "/agentes/1", "/agentes/1/consumo", "/sessoes",
    "/sessoes/5511987654321/historico", "/atendentes", "/atendentes/1",
    "/agentes/", "/agentes/1/", "/agentes/1/consumo/", "/sessoes/",
    "/sessoes/5511987654321/historico/", "/atendentes/", "/atendentes/1/",
    "/agentes%2F1", "/sessoes%2F5511987654321%2Fhistorico", "/atendentes%2F1",
    "/%61gentes", "/%73essoes", "/%61tendentes", "/%2561gentes",
    "/agentes%252F1", "/AGENTES", "/SESSOES/5511987654321", "/ATENDENTES",
    "/internal/health", "/internal/health/", "/%69nternal/health", "/internal//health",
    "/health/provider-auth", "/health/%70rovider-auth", "/health/", "/HEALTH",
    "/docs", "/docs/", "/docs/oauth2-redirect", "/%64ocs", "/redoc", "/redoc/",
    "/openapi.json", "/openapi%2Ejson", "/qualquer", "/qualquer/5511987654321",
    "/webhook/", "/chatwoot/webhook/", "/CHATWOOT/webhook",
    "/sessoes/%2535%2535%2531%2531", "/sessoes/%2535511987654321",
    "/sessoes/551198%2537654321", "/sessoes/551198765432%2531",
)
NORMALIZED_PUBLIC = (("/%68ealth", "GET"), ("/chatwoot%2Fwebhook", "POST"),
                     ("/webhook?phone=5511987654321", "POST"))
FAKE = f"{PREFIX}nginx-fake-{SUFFIX}"
NGINX_IMAGE = re.search(r"(?m)^  nginx:.*?\n    image: (\S+)", (ROOT / "docker-compose.yml").read_text(), re.S).group(1)
MYSQL_IMAGE = re.search(r"(?m)^  db:.*?\n    image: (\S+)", (ROOT / "docker-compose.yml").read_text(), re.S).group(1)

FAKE_CODE = r'''
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
hits=[]
class Handler(BaseHTTPRequestHandler):
    def handle_request(self):
        if self.path=="/__hits":
            body=json.dumps(hits).encode()
        else:
            self.rfile.read(int(self.headers.get("Content-Length",0)))
            hits.append([self.command,self.path])
            body=b'{"fake":true}'
        self.send_response(200)
        self.send_header("Content-Length",str(len(body)))
        self.end_headers()
        if self.command!="HEAD": self.wfile.write(body)
    do_GET=do_POST=do_PUT=do_DELETE=do_PATCH=do_HEAD=do_OPTIONS=handle_request
    def log_message(self,*args): pass
HTTPServer(("0.0.0.0",5000),Handler).serve_forever()
'''


RAW_CASES = (
    ("GET", "/sessoes/..", "/"),
    ("GET", "/agentes/%2E%2E", "/"),
    ("GET", "/atendentes/%2e%2e?phone=5511987654321", "/?phone=5511987654321"),
    ("POST", "/health/../webhook", "/webhook"),
    ("GET", "/internal/health/%2e%2e/%2e%2e/health", "/health"),
    ("GET", "/docs/%2E%2E", "/"),
    ("GET", "//health", "/health"),
    ("POST", "//webhook", "/webhook"),
    ("GET", "/health//", 404),
    ("GET", "/health;test", 404),
    ("POST", "/webhook;test", 404),
    ("GET", "/%00", 400),
    ("GET", "/sessoes/%00", 400),
    ("GET", "/health/../webhook", 405),
    ("HEAD", "/agentes/..", "/"),
    ("POST", "/a/%2e%2e/chatwoot/webhook?x=1", "/chatwoot/webhook?x=1"),
)


def raw_probe_code(config, protocol, name, host, *, mutate=False, outage=False):
    return f'''
import socket,ssl,httpx
name={name!r};host={host!r};tls={protocol == "https"!r}
redirect={config == "prod" and protocol == "http"!r}
mutate={mutate!r};outage={outage!r}
with httpx.Client(timeout=5) as c:
    def hits(): return c.get("http://app:5000/__hits").json()
    def request(method,path,hostname=host):
        connection=socket.create_connection((name,443 if tls else 80),timeout=5)
        if tls:
            context=ssl._create_unverified_context()
            connection=context.wrap_socket(connection,server_hostname=hostname)
        connection.sendall((method+" "+path+" HTTP/1.1\\r\\nHost: "+hostname+"\\r\\nConnection: close\\r\\nContent-Length: 0\\r\\n\\r\\n").encode("ascii"))
        chunks=[]
        while True:
            part=connection.recv(65536)
            if not part: break
            chunks.append(part)
        connection.close()
        return int(b"".join(chunks).split(b" ",2)[1])
    cases={RAW_CASES!r}
    if mutate: cases=cases[:1]
    if outage: cases=(("GET","/sessoes/../health?phone=5511987654321",502),)
    count=0
    for method,path,target in cases:
        before=hits()
        status=request(method,path)
        after=hits()
        forwarded=isinstance(target,str) and not redirect
        expected=301 if redirect and isinstance(target,str) else 200 if isinstance(target,str) else target
        assert status==expected,(method,path,status,expected)
        assert len(after)-len(before)==int(forwarded),(path,status,after[-1:])
        if forwarded:
            if mutate and after[-1]!=[method,target]:
                print("R2_URI_MUTATION PASS config={config} protocol={protocol} raw_uri_reached_upstream=1",flush=True)
            assert after[-1]==[method,target],("UPSTREAM_PATH",after[-1],[method,target])
        count+=1
        print(("R2_OUTAGE_CASE" if outage else "R2_RAW_CASE")+" PASS config={config} protocol={protocol} case="+str(count),flush=True)
    assert count==(1 if outage else 16),count
    if {config == "prod" and protocol == "http"!r} and not outage:
        for domain in ("evolution.seudominio.com.br","painel.seudominio.com.br","chatwoot.seudominio.com.br"):
            before=hits()
            assert request("GET","/manager",domain)==301,domain
            assert hits()==before
            print("R2_HOST_REDIRECT PASS",flush=True)
'''


def validate_nginx(config, variant, mutate=False):
    name = f"{PREFIX}nginx-{config}-{variant}-{SUFFIX}"
    source = (ROOT / "nginx" / f"nginx{'.dev' if config == 'dev' else ''}.conf").read_text()
    if mutate is True:
        old = "        location / { return 404; }"
        # Turn the closed fallback into the old catch-all proxy.
        assert source.count(old) == (1 if config == "dev" else 2)
        source = source.replace(old, "        location / { set $app_upstream app:5000; proxy_pass http://$app_upstream; }")
    if mutate == "uri":
        source, changed = re.subn(r"proxy_pass\s+http://\$app_upstream[^;]*;", "proxy_pass http://$app_upstream;", source)
        assert changed == 4
    if mutate == "outage":
        source = source.replace("$app_upstream app:5000", "$app_upstream app:59999")
    path = LOG / f"{config}-{variant}.conf"
    path.write_text(source)
    containers.append(name)
    run(["docker", "run", "-d", "--name", name, "--network", NETWORK,
         "--tmpfs", "/var/cache/nginx", "--tmpfs", "/var/run", "--tmpfs", "/var/log/nginx",
         "-v", f"{path}:/etc/nginx/nginx.conf:ro",
         "-v", f"{LOG / 'certs'}:/etc/nginx/certs:ro",
         "-v", f"{LOG / 'acme'}:/var/www/certbot:ro", NGINX_IMAGE])
    syntax = run(["docker", "exec", name, "nginx", "-t"])
    (LOG / f"{config}-{variant}-syntax.log").write_text(syntax.stdout + syntax.stderr)
    host = "localhost" if config == "dev" else "seudominio.com.br"
    protocols = ["http"] if config == "dev" else ["https"] if mutate in ("outage", "uri") else ["http", "https"]
    for protocol in protocols:
        code = f'''
import httpx
public={PUBLIC!r}
methods={METHODS!r}
denied={DENIED!r}
normalized={NORMALIZED_PUBLIC!r}
mutate={mutate!r}
redirect={config == "prod" and protocol == "http"!r}
base={f"{protocol}://{name}"!r}
headers={{"Host":{host!r}}}
count=0
with httpx.Client(verify=False, follow_redirects=False, timeout=5) as c:
    def hits(): return len(c.get("http://app:5000/__hits").json())
    cases=[(path, method, 301 if redirect else 200, not redirect)
           if method==allowed or (allowed=="GET" and method=="HEAD") else (path, method, 405, False)
           for path,allowed in public.items() for method in methods]
    cases += [(path,method,400 if "%253" in path else 404,False) for path in denied for method in methods]
    cases += [(path,method,301 if redirect else 200,not redirect) for path,method in normalized]
    assert len(cases)==381,len(cases)
    if {protocol == "http"!r}: cases.append(("/.well-known/acme-challenge/f0-check","GET",200,False))
    expected=382 if {protocol == "http"!r} else 381
    if mutate: cases=[("/qualquer","GET",404,False)]
    for path,method,status,forward in cases:
        before=hits()
        response=c.request(method,base+path,headers=headers)
        delta=hits()-before
        if mutate and delta:
            print("F0T4_MUTATION_HIT PASS protocol={protocol} config={config} upstream_delta="+str(delta),flush=True)
        assert (response.status_code,delta)==(status,int(forward)),(path,method,response.status_code,status,delta)
        count+=1
        print("F0T4_CASE PASS config={config} protocol={protocol} case="+str(count),flush=True)
    assert count==expected,count
    print("F0T4_EDGE PASS config={config} protocol={protocol} cases="+str(count),flush=True)
'''
        if mutate in ("uri", "outage"):
            code = raw_probe_code(config, protocol, name, host, mutate=mutate == "uri", outage=mutate == "outage")
        result = run(["docker", "exec", "-i", APP, "python", "-"], data=code, check=False)
        (LOG / f"{config}-{variant}-{protocol}.log").write_text(result.stdout + result.stderr)
        if mutate == "uri":
            assert result.returncode != 0 and "AssertionError" in result.stderr and "UPSTREAM_PATH" in result.stderr, result.stderr
            assert "raw_uri_reached_upstream=1" in result.stdout
        elif mutate is True:
            assert result.returncode != 0 and "AssertionError" in result.stderr, result.stderr
            assert "upstream_delta=1" in result.stdout
        else:
            assert result.returncode == 0, result.stderr
            if not mutate:
                assert result.stdout.count("F0T4_CASE PASS ") == (382 if protocol == "http" else 381)
                raw = run(["docker", "exec", "-i", APP, "python", "-"], data=raw_probe_code(config, protocol, name, host), check=False)
                (LOG / f"{config}-{variant}-{protocol}-raw.log").write_text(raw.stdout + raw.stderr)
                assert raw.returncode == 0, raw.stderr
                assert raw.stdout.count("R2_RAW_CASE PASS ") == 16
                print(raw.stdout.strip(), flush=True)
        print(result.stdout.strip(), flush=True)
    if not mutate or mutate == "outage":
        access = run(["docker", "exec", name, "cat", "/var/log/nginx/access.log"]).stdout
        error = run(["docker", "exec", name, "cat", "/var/log/nginx/error.log"]).stdout
        (LOG / f"{config}-{variant}-access.log").write_text(access)
        (LOG / f"{config}-{variant}-error.log").write_text(error)
        for forbidden in ("5511987654321", "%2535", "%2531", "%2537"):
            assert forbidden.lower() not in (access+error).lower(), "unsafe log"
        for shape in (() if mutate == "outage" else ("/agentes", "/sessoes/[REDACTED_PHONE]/historico", "/atendentes", "[REDACTED_PATH]")):
            assert f'path="{shape}"' in access, shape
        if mutate == "outage":
            assert 'path="/health"' in access and 'status=502' in access
            print(f"R2_OUTAGE_PRIVACY PASS config={config} status=502 raw_target_phone=1 leaks=0", flush=True)
        print(f"F0T4_PRIVACY PASS config={config} variant={variant} phone_leaks=0", flush=True)
    run(["docker", "rm", "-f", name])
    print(f"F0T4_REMOVED container={name}", flush=True)
    containers.remove(name)


assert shared_count() == 0, "shared Compose must stay stopped"
try:
    if "WPP_TEST_IMAGE" not in os.environ:
        run(["docker", "build", "-q", "--build-arg", "DELIVERY_WORKER_CODE_VERSION=outbox-v1", "-t", IMAGE, str(ROOT / "app")])
        built = True
    image_version = run(["docker", "image", "inspect", IMAGE, "--format",
                         '{{index .Config.Labels "org.opencontainers.image.revision"}}']).stdout.strip()
    assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", image_version), "image_version_label_required"
    run(["docker", "network", "create", "--internal", NETWORK])
    run(["docker", "volume", "create", VOLUME])
    containers.append(MYSQL)
    run(["docker", "run", "-d", "--name", MYSQL, "--network", NETWORK,
         "-v", f"{VOLUME}:/var/lib/mysql", "-e", f"MYSQL_ROOT_PASSWORD={password}",
         "-e", "MYSQL_DATABASE=fu4_health", MYSQL_IMAGE])
    ready = False
    for _ in range(150):
        result = run(["docker", "exec", "-e", f"MYSQL_PWD={password}", MYSQL,
                      "mysql", "-uroot", "-h127.0.0.1", "-e", "SELECT 1"], check=False)
        if result.returncode == 0:
            ready = True
            break
        time.sleep(1)
    assert ready, "disposable MySQL did not start"
    sql((ROOT / "db/init.sql").read_text())
    sql(f"CREATE USER 'fu4_app'@'%' IDENTIFIED BY '{password}'; "
        "GRANT SELECT,INSERT,UPDATE,DELETE ON fu4_health.* TO 'fu4_app'@'%';")
    containers.append(APP)
    args = ["docker", "run", "-d", "--name", APP, "--network", NETWORK,
            "--tmpfs", "/app/logs", "--tmpfs", "/tmp", "-w", "/app"]
    env = {"DELIVERY_WORKER_CODE_VERSION": image_version, "PYTHONPATH": "/app", "PYTHONDONTWRITEBYTECODE": "1",
           "DATABASE_URL": f"mysql+aiomysql://fu4_app:{password}@{MYSQL}:3306/fu4_health",
           "OPENROUTER_API_KEY": "fu4-synthetic", "EVOLUTION_API_URL": "http://evolution.invalid",
           "EVOLUTION_API_KEY": "fu4-synthetic", "CHATWOOT_BASE_URL": "http://chatwoot.invalid",
           "CHATWOOT_API_TOKEN": "fu4-synthetic", "CHATWOOT_ACCOUNT_ID": "1",
           "CHATWOOT_WEBHOOK_SECRET": "fu4-synthetic", "DELIVERY_MODE": "direct",
           "DELIVERY_DISPATCH_ENABLED": "false", "INBOX_SCHEDULER_ENABLED": "false"}
    for key, value in env.items():
        args += ["-e", f"{key}={value}"]
    run(args + [IMAGE, "python", "-m", "src.app_server"])
    probe = 'import httpx; r=httpx.get("http://127.0.0.1:5000/health"); assert r.status_code==200, r.text'
    ready = False
    for _ in range(60):
        if run(["docker", "exec", APP, "python", "-c", probe], check=False).returncode == 0:
            ready = True
            break
        time.sleep(1)
    assert ready, "real app did not become healthy"
    internal = run(["docker", "exec", "-i", APP, "python", "-"], data='''
import httpx
with httpx.Client(timeout=5) as c:
    public=c.get("http://127.0.0.1:5000/health")
    detail=c.get("http://127.0.0.1:5000/internal/health")
    provider=c.get("http://127.0.0.1:5000/health/provider-auth")
    assert public.status_code==detail.status_code==200
    assert public.json()=={"status":"healthy"}
    assert {"scheduler","replica_compatibility","lifecycle","runtime_mode"} <= set(detail.json())
    assert set(provider.json())=={"ready","auth_mode","evolution_instances","chatwoot_inboxes","issues"}
    assert provider.status_code==(200 if provider.json()["ready"] else 503)
    root=c.get("http://127.0.0.1:5000/")
    assert root.status_code==200 and root.json()=={"status":"online"}, root.text
    docs=c.get("http://127.0.0.1:5000/docs")
    assert docs.status_code==200 and "swagger" in docs.text.lower()
    redoc=c.get("http://127.0.0.1:5000/redoc")
    assert redoc.status_code==200 and "redoc" in redoc.text.lower()
    schema=c.get("http://127.0.0.1:5000/openapi.json")
    assert schema.status_code==200 and "/health" in schema.json()["paths"]
    oauth=c.get("http://127.0.0.1:5000/docs/oauth2-redirect")
    assert oauth.status_code==200
print("FU4_INTERNAL PASS direct=app:5000 details=preserved provider=preserved docs=preserved public=minimal root=minimal mysql=real")
''')
    print(internal.stdout.strip(), flush=True)
    (LOG / "certs").mkdir(exist_ok=True)
    run(["openssl", "req", "-x509", "-nodes", "-newkey", "rsa:2048", "-days", "1",
         "-subj", "/CN=seudominio.com.br", "-keyout", str(LOG / "certs/privkey.pem"),
         "-out", str(LOG / "certs/fullchain.pem")])
    acme = LOG / "acme/.well-known/acme-challenge"
    acme.mkdir(parents=True, exist_ok=True)
    (acme / "f0-check").write_text("synthetic-acme-check")
    containers.append(FAKE)
    run(["docker", "run", "-d", "--name", FAKE, "--network", NETWORK,
         "--network-alias", "app", IMAGE, "python", "-c", FAKE_CODE])
    for _ in range(30):
        if run(["docker", "exec", APP, "python", "-c",
                'import httpx; assert httpx.get("http://app:5000/__hits").status_code==200'], check=False).returncode == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("fake upstream did not become ready")
    for config in ["dev", "prod"]:
        validate_nginx(config, "control")
        validate_nginx(config, "without-fallback", mutate=True)
        validate_nginx(config, "raw-uri", mutate="uri")
        validate_nginx(config, "outage", mutate="outage")
        validate_nginx(config, "restored")
    print("F0T4_MATRIX PASS configs=2 controls=4 protocols=6 cases=2290 raw_cases=96 uri_mutations=2 fallback_mutations=3 host_redirects=6 outage_privacy=2 mysql=real fake_upstream=counted", flush=True)

finally:
    if APP in containers:
        app_log = run(["docker", "logs", APP], check=False)
        (LOG / "app.log").write_text(app_log.stdout + app_log.stderr)
    for name in reversed(containers):
        assert name.startswith(PREFIX)
        run(["docker", "rm", "-f", name], check=False)
        print(f"F0T4_REMOVED container={name}", flush=True)
    run(["docker", "network", "rm", NETWORK], check=False)
    print(f"F0T4_REMOVED network={NETWORK}", flush=True)
    run(["docker", "volume", "rm", VOLUME], check=False)
    print(f"F0T4_REMOVED volume={VOLUME}", flush=True)
    if built:
        run(["docker", "image", "rm", IMAGE], check=False)
    for kind, name in [("container", n) for n in containers] + [("network", NETWORK), ("volume", VOLUME)]:
        assert run(["docker", kind, "inspect", name], check=False).returncode != 0, (kind, name)
    assert shared_count() == 0
    print(f"FU4_CLEANUP PASS owned=0 shared=0 logs={LOG}", flush=True)
