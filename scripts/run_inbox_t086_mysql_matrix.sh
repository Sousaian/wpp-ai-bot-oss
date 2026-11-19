#!/usr/bin/env bash
set -uo pipefail

# T08-6 matrix: guarded, audited per-agent scheduler opt-in/opt-out. Runs the
# targeted real-MySQL suite as a DML-only user (plus SELECT on
# performance_schema for lock-wait observation), repeats the opt-in × drain
# barrier test, runs every fix-removal mutation (each must FAIL), an unmutated
# control and, unless T086_REGRESSION=0, every app test module as regression
# (regression DB name contains `t082` because the T08-2 retention suite
# refuses URLs without it). No schema change in T08-6. Synthetic credentials
# only; no provider, LLM or WhatsApp effect. Loops read their lists from fd 3
# so docker exec never drains them.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${T086_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-t086-}"
container="${resource_prefix}mysql-${suffix}"
network="${resource_prefix}net-${suffix}"
volume="${resource_prefix}data-${suffix}"
image="${WPP_TEST_IMAGE:-${resource_prefix}matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
workdir="$(mktemp -d)"
mutations_py="${repo_root}/scripts/inbox_t086_mutations.py"
suite="${repo_root}/app/tests/test_agent_scheduler_opt_in_mysql.py"
module="tests.test_agent_scheduler_opt_in_mysql"
expected_targeted=12
expected_mutations=29
barrier_repeats=3
barriers=(
    test_opt_in_vs_drain_race_both_orders_repeated
    test_put_inbox_clear_vs_opt_in_race_both_orders_repeated
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
    rm -rf "${workdir}"
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then docker image rm -f "${image}" >/dev/null 2>&1 || true; fi
    if [[ "$(resource_count)" != 0 ]]; then
        echo "T086_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "T086_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

declared="$(grep -cE '^    (async )?def test_' "${suite}")"
[[ "${declared}" == "${expected_targeted}" ]] \
    || { echo "T086_CASE_COUNT → FAIL: declared=${declared} expected=${expected_targeted}" >&2; exit 1; }
[[ "$(python "${mutations_py}" count)" == "${expected_mutations}" ]] \
    || { echo "T086_CASE_COUNT → FAIL: mutation list size" >&2; exit 1; }
python "${mutations_py}" check || exit $?
for name in "${barriers[@]}"; do
    grep -qE "^    async def ${name}\(" "${suite}" \
        || { echo "T086_CASE_COUNT → FAIL: barrier ${name} missing" >&2; exit 1; }
done

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
prepare_db() {
    mysql_root </dev/null --execute="CREATE DATABASE $1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT SELECT,INSERT,UPDATE,DELETE ON $1.* TO 't086_app'@'%';" || return $?
    mysql_root --default-character-set=latin1 "$1" < "${repo_root}/db/init.sql"
}
mysql_root </dev/null --execute="CREATE USER 't086_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT ON performance_schema.* TO 't086_app'@'%';" || exit $?
prepare_db t086_fresh || exit $?
[[ "$(mysql_root --batch --skip-column-names t086_fresh </dev/null \
    --execute="SELECT state FROM scheduler_runtime_state WHERE id=1")" == disabled ]] || exit 1
echo "T086_DEFAULT → PASS: fresh install scheduler state=disabled"

run_tests() {
    local root=$1 db=$2; shift 2
    docker run --rm --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://t086_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "DATABASE_URL=mysql+aiomysql://t086_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "WPP_DISPOSABLE_TEST_DATABASE_MARKER=${db}" \
        --env OPENROUTER_API_KEY=synthetic-t086-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 2400 python -m unittest "$@" </dev/null
}

# ── 1. Targeted suite ───────────────────────────────────────────────────
target_log="${workdir}/target.log"
run_tests "${repo_root}" t086_fresh "${module}" >"${target_log}" 2>&1 \
    || { tail -n 60 "${target_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests in " "${target_log}" && grep -qE '^OK$' "${target_log}" \
    || { tail -n 20 "${target_log}" >&2; echo "T086_CASE_COUNT → FAIL: targeted" >&2; exit 1; }
if grep -qE 'Deadlock found|\(1213,' "${target_log}"; then
    echo "T086_DEADLOCK → FAIL: 1213 in targeted log" >&2; exit 1
fi
echo "T086_TARGETED → PASS: ${expected_targeted}/${expected_targeted} (zero skip, DML-only user, zero 1213)"

# ── 2. Barrier repetitions (each test also loops internally) ────────────
barrier_args=()
for name in "${barriers[@]}"; do barrier_args+=("${module}.AgentSchedulerOptInMySQLTests.${name}"); done
for round in $(seq 1 "${barrier_repeats}"); do
    log="${workdir}/barrier-${round}.log"
    run_tests "${repo_root}" t086_fresh "${barrier_args[@]}" >"${log}" 2>&1 \
        || { tail -n 40 "${log}" >&2; exit 1; }
    # unittest prints "Ran 1 test" (singular) for a single barrier test.
    grep -qE "^Ran ${#barriers[@]} tests? in " "${log}" && grep -qE '^OK$' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "T086_BARRIERS → FAIL: round ${round}" >&2; exit 1; }
    grep -qE 'Deadlock found|\(1213,' "${log}" && { echo "T086_DEADLOCK → FAIL (round ${round})" >&2; exit 1; }
    echo "T086_BARRIERS → PASS: round ${round}/${barrier_repeats} ${#barriers[@]}/${#barriers[@]} zero 1213"
done

# ── 3. Fix-removal mutations (each must fail against the DB) ────────────
mutations=0
while read -r -u 3 name test_name; do
    [[ -n "${name}" ]] || continue
    copy="${workdir}/mutation-${name}"
    mkdir -p "${copy}"
    tar --exclude=__pycache__ --exclude=logs -C "${repo_root}" -cf - app \
        | tar -C "${copy}" -xf - || exit $?
    mkdir -p "${copy}/app/logs"
    python "${mutations_py}" apply "${copy}" "${name}" || exit $?
    log="${workdir}/${name}.log"
    if run_tests "${copy}" t086_fresh "${test_name}" >"${log}" 2>&1; then
        echo "T086_MUTATION → FAIL (${name}): ${test_name##*.} passed without the fix" >&2
        exit 1
    fi
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "T086_MUTATION → FAIL (${name}): harness did not run the test" >&2; exit 1; }
    echo "T086_MUTATION → PASS (${name}): ${test_name##*.} fails without the fix"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${mutations_py}" list)
[[ "${mutations}" == "${expected_mutations}" ]] \
    || { echo "T086_MUTATION → FAIL: ${mutations}/${expected_mutations}" >&2; exit 1; }

# ── 4. Unmutated control ────────────────────────────────────────────────
run_tests "${repo_root}" t086_fresh "${module}" >"${workdir}/control.log" 2>&1 \
    || { tail -n 40 "${workdir}/control.log" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests in " "${workdir}/control.log" || exit 1
echo "T086_CONTROL → PASS: unmutated suite green after mutations"

# ── 5. Regression: every app test module (unit + real MySQL) ────────────
if [[ "${T086_REGRESSION:-1}" == 1 ]]; then
    prepare_db t086_t082_regression || exit $?
    reg_log="${workdir}/regression.log"
    run_tests "${repo_root}" t086_t082_regression discover -s /workspace/app/tests -t /workspace/app -p 'test_*.py' >"${reg_log}" 2>&1 \
        || { grep -E '^(FAIL|ERROR):' "${reg_log}" >&2; tail -n 20 "${reg_log}" >&2; exit 1; }
    ran="$(grep -oE '^Ran [0-9]+ tests?' "${reg_log}")"
    grep -qE '^OK( \(skipped=[0-9]+\))?$' "${reg_log}" || exit 1
    grep -E '^OK' "${reg_log}"
    echo "T086_REGRESSION → PASS: ${ran}"
fi

PYTHONPYCACHEPREFIX="${workdir}/pycache" python -m py_compile \
    "${repo_root}/app/src/database/repositories/agent_scheduler_repo.py" \
    "${repo_root}/app/src/agent_scheduler_cli.py" \
    "${repo_root}/app/src/delivery_cli.py" \
    "${suite}" "${mutations_py}" || exit $?
bash -n "${repo_root}/scripts/run_inbox_t086_mysql_matrix.sh" || exit $?
echo "T086_MATRIX → PASS: targeted=${expected_targeted} barrier_rounds=${barrier_repeats} mutations=${expected_mutations}"
