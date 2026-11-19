#!/usr/bin/env bash
# Full test suite with a throwaway MySQL 8.0 on an internal Docker network.
# Used by CI (.github/workflows/ci.yml) and runnable locally:  bash scripts/ci_full_suite.sh
# Credentials are synthetic and the database lives only for the duration of the run.
set -Eeuo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo}")"
label="${CI_SUITE_LABEL:-local}-$$"
logs="${CI_SUITE_LOG_DIR:-$repo/.test-runs/ci}"
mkdir -p "$logs"
net="wppci-$label-net"; db="wppci-$label-db"; img="wppci-app:$label"

cleanup() {
    docker rm -f "$db" >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
    docker image rm "$img" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "::group::Build app image"
docker build --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" -t "$img" "$repo/app"
echo "::endgroup::"

echo "::group::Start disposable MySQL"
docker network create --internal "$net" >/dev/null
docker run -d --name "$db" --network "$net" -e MYSQL_ROOT_PASSWORD=synthetic-root \
    mysql:8.0 --performance-schema=ON >/dev/null
ready=0
for _ in $(seq 1 180); do
    if docker exec -e MYSQL_PWD=synthetic-root "$db" mysql -uroot -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1; then
        ready=1; break
    fi
    sleep 1
done
[[ "$ready" == 1 ]] || { echo "MySQL did not become ready"; docker logs "$db" | tail -50; exit 1; }
q() { docker exec -e MYSQL_PWD=synthetic-root "$db" mysql -uroot "$@" </dev/null; }
q -e 'CREATE DATABASE wpp_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'
docker exec -i -e MYSQL_PWD=synthetic-root "$db" mysql -uroot --default-character-set=latin1 wpp_test \
    < "$repo/db/init.sql" > "$logs/migration.log" 2>&1 || { echo "Migration failed"; tail -20 "$logs/migration.log"; exit 1; }
q -e "CREATE USER 'wpp'@'%' IDENTIFIED BY 'synthetic';
GRANT SELECT,INSERT,UPDATE,DELETE,LOCK TABLES ON wpp_test.* TO 'wpp'@'%';
GRANT SELECT ON performance_schema.* TO 'wpp'@'%'; GRANT PROCESS ON *.* TO 'wpp'@'%';"
echo "::endgroup::"

echo "::group::Run unittest suite"
set +e
docker run --rm --network "container:$db" -v "$repo:/workspace:ro" --tmpfs /runtime --tmpfs /tmp/pycache \
    --workdir /runtime \
    -e PYTHONPYCACHEPREFIX=/tmp/pycache -e PYTHONPATH=/workspace/app -e WPP_REPO_ROOT=/workspace \
    -e OUTBOX_TEST_DATABASE_URL='mysql+aiomysql://wpp:synthetic@127.0.0.1:3306/wpp_test' \
    -e DATABASE_URL='mysql+aiomysql://wpp:synthetic@127.0.0.1:3306/wpp_test' \
    -e P40_ADMIN_DATABASE_URL='mysql+aiomysql://root:synthetic-root@127.0.0.1:3306/wpp_test' \
    -e WPP_DISPOSABLE_TEST_DATABASE_MARKER=wpp_test -e OPENROUTER_API_KEY=synthetic \
    -e EVOLUTION_API_URL=http://evolution.invalid -e EVOLUTION_API_KEY=synthetic \
    -e EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
    -e CHATWOOT_BASE_URL=http://chatwoot.invalid -e CHATWOOT_API_TOKEN=synthetic \
    -e CHATWOOT_ACCOUNT_ID=1 -e CHATWOOT_WEBHOOK_SECRET=synthetic \
    -e DELIVERY_WORKER_CODE_VERSION=outbox-v1 \
    "$img" timeout 3600 python -m unittest discover -s /workspace/app/tests -t /workspace/app \
    > "$logs/unittest.log" 2>&1
rc=$?
set -e
echo "::endgroup::"

grep -E "^(FAIL|ERROR): " "$logs/unittest.log" | sed -E 's/ \(.*//' | sort -u > "$logs/failures.txt" || true
grep -E "^(Ran |OK|FAILED)" "$logs/unittest.log" | tail -3
if [[ "$rc" != 0 ]] || ! grep -qE '^OK' "$logs/unittest.log"; then
    echo "Failing tests:"; cat "$logs/failures.txt"
    tail -80 "$logs/unittest.log"
    exit 1
fi
