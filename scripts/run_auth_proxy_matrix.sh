#!/usr/bin/env bash
# AUTH-1 edge proof with the real Nginx config, app and MySQL 8.0 (throwaway,
# internal network, synthetic credentials). /auth is NOT public: the run injects
# one test-only location into a copy of nginx.conf that reuses the headers of
# the published app blocks. Proves that clients behind Nginx are told apart,
# that X-Forwarded-For cannot be forged, and that a login flood stays within
# memory while /webhook keeps answering.
set -Eeuo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo}/scripts/test_image_version.sh"
version="$(image_test_build_version "${repo}")"
id="authpx-$$"; net="$id-net"; db="$id-db"; app="$id-app"; edge="$id-nginx"; img="$id:app"
work="$(mktemp -d)"
cleanup() {
    docker rm -f "$edge" "$app" "$db" >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
    docker image rm "$img" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT
passed=0
ok() { passed=$((passed + 1)); echo "PASS $1"; }
fail() { echo "FAIL $1"; docker logs --tail 20 "$app" 2>&1 | tail -20; exit 1; }

nginx_image="$(sed -n '/^  nginx:/,/image:/s/.*image: //p' "$repo/docker-compose.yml")"
python3 - "$repo/nginx/nginx.conf" "$work/nginx.conf" <<'PY'
import re, sys
source = open(sys.argv[1]).read()
block = re.search(r"        location = /webhook \{\n(?:(?!\n        \}).)*?proxy_pass.*?\n        \}\n",
                  source[source.index("listen 443 ssl;"):], re.S).group(0)
test_only = block.replace("= /webhook", "= /auth/login").replace("app_upstream/webhook", "app_upstream/auth/login")
marker = source.index(block, source.index("listen 443 ssl;"))
open(sys.argv[2], "w").write(source[:marker] + test_only + "\n" + source[marker:])
PY
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$work/privkey.pem" -out "$work/fullchain.pem" -days 1 \
    -subj /CN=seudominio.com.br >/dev/null 2>&1
chmod 755 "$work"; chmod 644 "$work"/*
docker build -q --build-arg "DELIVERY_WORKER_CODE_VERSION=$version" -t "$img" "$repo/app" >/dev/null
subnet="10.$((RANDOM % 100 + 100)).$((RANDOM % 250)).0/24"; prefix="${subnet%.0/24}"
docker network create --internal --subnet "$subnet" "$net" >/dev/null
docker run -d --name "$db" --network "$net" --network-alias db -e MYSQL_ROOT_PASSWORD=synthetic-root mysql:8.0 >/dev/null
for _ in $(seq 1 180); do
    docker exec -e MYSQL_PWD=synthetic-root "$db" mysql -uroot -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1 && break; sleep 1
done
docker exec -e MYSQL_PWD=synthetic-root "$db" mysql -uroot -e 'CREATE DATABASE wpp_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'
docker exec -i -e MYSQL_PWD=synthetic-root "$db" mysql -uroot --default-character-set=latin1 wpp_test < "$repo/db/init.sql" >/dev/null 2>&1
key="$(python3 -c 'import base64; print(base64.b64encode(bytes(range(32))).decode())')"
start_app() {
    docker rm -f "$app" >/dev/null 2>&1 || true
    docker run -d --name "$app" --network "$net" --network-alias app --memory 512m --memory-swap 512m \
        -e DATABASE_URL='mysql+aiomysql://root:synthetic-root@db:3306/wpp_test' -e OPENROUTER_API_KEY=synthetic \
        -e EVOLUTION_API_URL=http://evolution.invalid -e EVOLUTION_API_KEY=synthetic -e FORWARDED_ALLOW_IPS='*' \
        -e "DELIVERY_WORKER_CODE_VERSION=$version" \
        -e 'AUTH_PASSWORD_PEPPER_KEYS={"1":"synthetic-pepper-0123456789abcdef-xyz"}' \
        -e "AUTH_TOTP_ENCRYPTION_KEYS={\"1\":\"$key\"}" "$img" >/dev/null
    for _ in $(seq 1 60); do
        docker exec "$app" python -c 'import httpx; httpx.get("http://127.0.0.1:5000/").raise_for_status()' >/dev/null 2>&1 && return
        sleep 1
    done
    fail "app start"
}
start_app
docker run -d --name "$edge" --network "$net" --network-alias nginx -v "$work/nginx.conf:/etc/nginx/nginx.conf:ro" \
    -v "$work:/etc/nginx/certs:ro" "$nginx_image" >/dev/null
sleep 3
# client <ip> <python using c(method, path, body, headers, direct) -> status>
client() {
    docker run --rm -i --network "$net" --ip "$prefix.$1" --entrypoint python "$img" - <<PY
import asyncio, collections, httpx, socket, sys
edge = socket.gethostbyname("nginx")
async def main():
    async with httpx.AsyncClient(verify=False, timeout=20, limits=httpx.Limits(max_connections=100)) as http:
        async def login(forwarded=None, direct=False):
            headers = {"Host": "seudominio.com.br", **({"X-Forwarded-For": forwarded} if forwarded else {})}
            url = "http://app:5000/auth/login" if direct else f"https://{edge}/auth/login"
            r = await http.post(url, headers=headers, json={"email": "ninguem@example.invalid", "senha": "frase sintetica"})
            return r.status_code
        async def webhook():
            t = asyncio.get_running_loop().time()
            r = await http.post(f"https://{edge}/webhook", headers={"Host": "seudominio.com.br"}, json={
                "event": "messages.upsert", "instance": "inexistente", "data": {"key": {"id": "x",
                "remoteJid": "5511999990001@s.whatsapp.net", "fromMe": False}, "message": {"conversation": "oi"}}})
            return r.status_code, asyncio.get_running_loop().time() - t
$2
asyncio.run(main())
PY
}
first="$(client 50 '        print([await login() for _ in range(31)])')"
[[ "$first" == "[$(printf '401, %.0s' $(seq 30))429]" ]] || fail "window A: $first"; ok "client-A-limited-after-30"
[[ "$(client 60 '        print(await login())')" == 401 ]] || fail "client B blocked by A"; ok "client-B-not-blocked-by-A"
[[ "$(client 50 '        print({await login(f"203.0.113.{n}") for n in range(5)})')" == "{429}" ]] || fail "forged header through nginx"; ok "forged-header-through-nginx-ignored"
[[ "$(client 70 '        print([await login(f"203.0.113.{n}", direct=True) for n in range(31)][-1])')" == 429 ]] || fail "forged header by another container"; ok "forged-header-by-other-container-ignored"
docker exec "$edge" nginx -s reload; docker network disconnect "$net" "$edge"; docker network connect --ip "$prefix.200" --alias nginx "$net" "$edge"; sleep 11
[[ "$(client 80 '        print(await login())')" == 401 ]] || fail "proxy address change"; ok "proxy-address-change-followed"

start_app; sleep 2
peak_before="$(docker exec "$app" cat /sys/fs/cgroup/memory.current)"
# 64 simultaneous logins from distinct sources cannot come from one container:
# use two clients (30 each, under the window) at once, plus the webhook probe.
flood='        s = await asyncio.gather(*[login() for _ in range(30)]); print(dict(collections.Counter(s)))'
client 91 "$flood" > "$work/f1" & a=$!; client 92 "$flood" > "$work/f2" & b=$!
probe="$(client 93 '        out = [await webhook() for _ in range(200)]; print(max(t for _, t in out), {s for s, _ in out})')"
wait $a $b
peak="$(docker exec "$app" cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)"
growth=$(( (peak - peak_before) / 1048576 ))
echo "flood: $(cat "$work/f1") $(cat "$work/f2") | webhook max_s/status: $probe | growth_mib: $growth"
python3 - "$probe" "$growth" <<'PY' || fail "flood"
import sys
worst, statuses = sys.argv[1].split(" ", 1)
assert float(worst) < 2 and statuses.strip() == "{200}", sys.argv
assert 0 <= int(sys.argv[2]) < 160, sys.argv
PY
[[ "$(docker inspect -f '{{.State.OOMKilled}}' "$app")" == false ]] || fail "OOM"
ok "flood-within-memory-and-webhook-answers"
echo "auth proxy matrix: $passed/6 passed"
[[ "$passed" == 6 ]]
