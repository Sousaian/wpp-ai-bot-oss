#!/usr/bin/env bash
set -uo pipefail

# T08-2 matrix: scheduler retention on a disposable MySQL 8.0 as a DML-only
# user. No DDL in T08-2 (db/init.sql unchanged from the ticket07 merge), so the
# schema gate is a fresh + repeated install. Then the targeted suite with an
# exact case count, the lifecycle/delivery-worker regressions and fix-removal
# mutations that must each fail against the real DB. Synthetic credentials
# only; no provider, LLM or WhatsApp effect. Loops read their list from fd 3
# so docker never drains it.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${T082_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-t082-}"
container="${resource_prefix}mysql-${suffix}"
network="${resource_prefix}net-${suffix}"
volume="${resource_prefix}data-${suffix}"
image="${WPP_TEST_IMAGE:-${resource_prefix}matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
# S1: keep raw logs for recounting when requested.
if [[ -n "${WPP_TEST_LOG_ROOT:-}" ]]; then
    workdir="${WPP_TEST_LOG_ROOT%/}/t082-${suffix}"
    mkdir -p "${workdir}"
    retain_workdir=1
else
    workdir="$(mktemp -d)"
    retain_workdir=0
fi
mutations_py="${repo_root}/scripts/inbox_t082_mutations.py"
suite="${repo_root}/app/tests/test_scheduler_retention_mysql.py"
schema_base="148dd57"
expected_targeted=58
expected_mutations=65
declare -A expected_regressions=(
    [tests.test_lifecycle_mysql]=56  # FU-2
    [tests.test_delivery_worker_mysql]=18
    [tests.test_delivery_worker_unit]=37
)
root_password="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(24))
PY
)"
app_password="$(python - <<'PY'
import secrets
print(secrets.token_urlsafe(24))
PY
)"

shared_count() {
    docker ps -aq --filter "label=com.docker.compose.project=${shared_project}" \
        | wc -l | tr -d ' '
}

resource_count() {
    local count=0
    docker container inspect "${container}" >/dev/null 2>&1 && count=$((count + 1))
    docker network inspect "${network}" >/dev/null 2>&1 && count=$((count + 1))
    docker volume inspect "${volume}" >/dev/null 2>&1 && count=$((count + 1))
    printf '%s' "${count}"
}

cleanup() {
    local original=$? cleanup_status=0
    (( retain_workdir == 1 )) || rm -rf "${workdir}"
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then docker image rm -f "${image}" >/dev/null 2>&1 || true; fi
    if [[ "$(resource_count)" != 0 ]]; then
        echo "T082_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "T082_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

declared="$(grep -cE '^    (async )?def test_' "${suite}")"
[[ "${declared}" == "${expected_targeted}" ]] \
    || { echo "T082_CASE_COUNT → FAIL: declared=${declared} expected=${expected_targeted}" >&2; exit 1; }
[[ "$(python "${mutations_py}" count)" == "${expected_mutations}" ]] \
    || { echo "T082_CASE_COUNT → FAIL: mutation list size" >&2; exit 1; }
python "${mutations_py}" check || exit $?
git -C "${repo_root}" diff --quiet "${schema_base}" 7ad8578 -- db/init.sql \
    || { echo "T082_SCHEMA → FAIL: T08-2 commit range changed db/init.sql" >&2; exit 1; }
echo "T082_SCHEMA → PASS: T08-2 commit range leaves db/init.sql unchanged"

if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?

docker network create "${network}" >/dev/null || exit $?
docker volume create "${volume}" >/dev/null || exit $?
docker run --detach --name "${container}" --network "${network}" \
    --volume "${volume}:/var/lib/mysql" --env "MYSQL_ROOT_PASSWORD=${root_password}" \
    mysql:8.0 --character-set-server=utf8mb4 \
    --collation-server=utf8mb4_unicode_ci >/dev/null || exit $?
ready=0
streak=0
for _ in $(seq 1 150); do
    if docker exec --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --host=127.0.0.1 --protocol=tcp \
        --execute='SELECT VERSION()' </dev/null >/dev/null 2>&1; then
        streak=$((streak + 1))
        if (( streak >= 3 )); then ready=1; break; fi
    else
        streak=0
    fi
    sleep 1
done
(( ready == 1 )) || { docker logs --tail 100 "${container}" >&2; exit 1; }
echo "MYSQL_READY → PASS: disposable server (TCP streak 3)"

mysql_root() {
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "$@"
}
mysql_root </dev/null --execute="CREATE DATABASE t082_fresh CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" || exit $?
for _ in 1 2; do
    mysql_root --default-character-set=latin1 t082_fresh < "${repo_root}/db/init.sql" || exit $?
done
echo "T082_FRESH → PASS: clean + repeated install"
# S1: predecessor upgrades, DDL interruptions, exact-shape tamper matrix.
source "${repo_root}/scripts/inbox_sec_s1_schema_matrix.sh" || exit $?
source "${repo_root}/scripts/inbox_sec2_s1_schema_matrix.sh" || exit $?
mysql_root </dev/null --execute="CREATE USER 't082_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT,INSERT,UPDATE,DELETE ON t082_fresh.* TO 't082_app'@'%';" || exit $?

run_tests() {
    local root=$1; shift
    docker run --rm --name="${resource_prefix}test-${suffix}" --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://t082_app:${app_password}@127.0.0.1:3306/t082_fresh" \
        --env "DATABASE_URL=mysql+aiomysql://t082_app:${app_password}@127.0.0.1:3306/t082_fresh" \
        --env WPP_DISPOSABLE_TEST_DATABASE_MARKER=t082_fresh \
        --env OPENROUTER_API_KEY=synthetic-t082-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 1500 python -m unittest "$@" </dev/null
}

target_log="${workdir}/target.log"
run_tests "${repo_root}" tests.test_scheduler_retention_mysql >"${target_log}" 2>&1 \
    || { tail -n 60 "${target_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${target_log}" && grep -qE '^OK$' "${target_log}" \
    || { tail -n 20 "${target_log}" >&2; echo "T082_CASE_COUNT → FAIL: targeted" >&2; exit 1; }
echo "T082_TARGETED → PASS: ${expected_targeted}/${expected_targeted} (zero skip, DML-only user)"

regressions=0
for module in tests.test_lifecycle_mysql tests.test_delivery_worker_mysql tests.test_delivery_worker_unit; do
    log="${workdir}/${module}.log"
    expected="${expected_regressions[${module}]}"
    run_tests "${repo_root}" "${module}" >"${log}" 2>&1 \
        || { tail -n 60 "${log}" >&2; echo "T082_REGRESSION → FAIL: ${module}" >&2; exit 1; }
    grep -qE "^Ran ${expected} tests?" "${log}" && grep -qE '^OK$' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "T082_REGRESSION → FAIL: ${module} count" >&2; exit 1; }
    echo "T082_REGRESSION → PASS: ${module} ${expected}/${expected}"
    regressions=$((regressions + 1))
done
[[ "${regressions}" == 3 ]] || { echo "T082_REGRESSION → FAIL: ran ${regressions}/3" >&2; exit 1; }

mutations=0
while read -r -u 3 name test; do
    [[ -n "${name}" ]] || continue
    copy="${workdir}/mut-${name}"
    mkdir -p "${copy}"
    cp -R "${repo_root}/app" "${copy}/app" || exit $?
    cp "${repo_root}/AGENTS.md" "${copy}/AGENTS.md" || exit $?
    cp "${repo_root}/.env.example" "${copy}/.env.example" || exit $?
    python "${mutations_py}" apply "${copy}" "${name}" || exit $?
    log="${workdir}/mut-${name}.log"
    if run_tests "${copy}" "${test}" >"${log}" 2>&1; then
        tail -n 20 "${log}" >&2
        echo "T082_MUTATION → FAIL (${name}): ${test} still passes" >&2
        exit 1
    fi
    grep -qE '^Ran 1 tests?' "${log}" && grep -qE '^FAILED \((failures|errors)=1\)' "${log}" \
        || { tail -n 30 "${log}" >&2; echo "T082_MUTATION → FAIL (${name}): no single failing case" >&2; exit 1; }
    echo "T082_MUTATION → PASS (${name}): ${test##*.} fails"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${mutations_py}" list)
[[ "${mutations}" == "${expected_mutations}" ]] \
    || { echo "T082_MUTATION → FAIL: ran ${mutations}/${expected_mutations}" >&2; exit 1; }
echo "T082_MUTATIONS → PASS: ${mutations}/${expected_mutations} each failed against MySQL"
echo "T082_MATRIX → PASS: targeted=${expected_targeted} regressions=3 mutations=${mutations}"
