#!/usr/bin/env bash
set -uo pipefail

# FU-2 gate, using the T08-7 disposable-container driver conventions.
# Prefix override + reusable image; zero shared stack, DML-only tests,
# performance_schema observation, raw logs, exact counts, isolated mutations.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${FU2_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-fu2-}"
container="${resource_prefix}mysql-${suffix}"
network="${resource_prefix}net-${suffix}"
volume="${resource_prefix}data-${suffix}"
image="${WPP_TEST_IMAGE:-${resource_prefix}matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
workdir="$(mktemp -d)"
mutations_py="${repo_root}/scripts/inbox_fu2_mutations.py"
expected_targeted=5
expected_mutations=10
barrier_repeats=3
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
    # FU-2: preserve raw proof, including failed mutations and baseline.
    if [[ -n "${FU2_LOG_DIR:-}" ]]; then
        mkdir -p "${FU2_LOG_DIR}"
        find "${workdir}" -maxdepth 1 -name '*.log' -exec cp {} "${FU2_LOG_DIR}/" \;
    fi
    rm -rf "${workdir}"
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then docker image rm -f "${image}" >/dev/null 2>&1 || true; fi
    if [[ "$(resource_count)" != 0 ]]; then
        echo "FU2_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "FU2_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

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
GRANT SELECT,INSERT,UPDATE,DELETE ON $1.* TO 'fu2_app'@'%';" || return $?
    mysql_root --default-character-set=latin1 "$1" < "${repo_root}/db/init.sql"
}
mysql_root </dev/null --execute="CREATE USER 'fu2_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT ON performance_schema.* TO 'fu2_app'@'%';" || exit $?
prepare_db fu2_t082 || exit $?
[[ "$(mysql_root --batch --skip-column-names fu2_t082 </dev/null \
    --execute="SELECT state FROM scheduler_runtime_state WHERE id=1")" == disabled ]] || exit 1
echo "FU2_DEFAULT → PASS: fresh install scheduler state=disabled"

run_tests() {
    local root=$1 db=$2; shift 2
    docker run --rm --name="${resource_prefix}test-${suffix}-${RANDOM}" --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://fu2_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "DATABASE_URL=mysql+aiomysql://fu2_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "WPP_DISPOSABLE_TEST_DATABASE_MARKER=${db}" \
        --env OPENROUTER_API_KEY=synthetic-fu2-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 2400 python -m unittest "$@" </dev/null
}

tests=(
  tests.test_lock_footprint_mysql
  tests.test_lifecycle_mysql.LifecycleMySQLTests.test_two_agents_late_accounting_no_unique_gap_deadlock
  tests.test_lifecycle_mysql.LifecycleMySQLTests.test_accounting_savepoint_real_deadlock_escapes
  tests.test_lifecycle_mysql.LifecycleMySQLTests.test_abandon_parked_handoff_releases_owned_lane_and_claim
)
check_log() {
    grep -qE "^Ran $2 tests?" "$1" && grep -q '^OK$' "$1"
}
[[ "$(python "${mutations_py}" count)" == "${expected_mutations}" ]] || exit 1
python "${mutations_py}" check || exit $?
# Exact unfixed baseline: new tests copied, production code unchanged.
baseline="${workdir}/baseline"
mkdir -p "${baseline}"
git -C "${repo_root}" archive "${FU2_BASELINE_REF:-a3fe23f}" app AGENTS.md | tar -C "${baseline}" -xf - || exit $?
cp "${repo_root}/app/tests/test_lifecycle_mysql.py" "${repo_root}/app/tests/test_lock_footprint_mysql.py" "${baseline}/app/tests/"
if run_tests "${baseline}" fu2_t082 tests.test_lock_footprint_mysql \
    tests.test_lifecycle_mysql.LifecycleMySQLTests.test_two_agents_late_accounting_no_unique_gap_deadlock \
    >"${workdir}/baseline.log" 2>&1; then
    echo "FU2_BASELINE → FAIL: unfixed baseline passed"; exit 1
fi
grep -q '^Ran 3 tests' "${workdir}/baseline.log" && grep -q '^FAILED (failures=' "${workdir}/baseline.log" \
    && ! grep -q '^ERROR:' "${workdir}/baseline.log" || { tail -80 "${workdir}/baseline.log"; exit 1; }
for name in test_session_locking_paths_under_skewed_history test_settlement_absent_operator_intent_takes_no_gap_lock test_two_agents_late_accounting_no_unique_gap_deadlock; do
    grep -q "^FAIL: ${name}" "${workdir}/baseline.log" || exit 1
done
echo "FU2_BASELINE → PASS: 3/3 fail on ${FU2_BASELINE_REF:-a3fe23f}; measured foreign locks, audit gaps, accounting 1213"
rm -rf "${baseline}"
run_tests "${repo_root}" fu2_t082 "${tests[@]}" >"${workdir}/target.log" 2>&1 \
    && check_log "${workdir}/target.log" "${expected_targeted}" || { tail -80 "${workdir}/target.log"; exit 1; }
echo "FU2_TARGETED → PASS: ${expected_targeted}/${expected_targeted}"
for round in $(seq 1 "${barrier_repeats}"); do
    log="${workdir}/barrier-${round}.log"
    run_tests "${repo_root}" fu2_t082 "${tests[@]}" >"${log}" 2>&1 \
        && check_log "${log}" "${expected_targeted}" || { tail -80 "${log}"; exit 1; }
    echo "FU2_BARRIERS → PASS: round ${round}/${barrier_repeats} ${expected_targeted}/${expected_targeted}"
done
mutations=0
while read -r -u 3 name test_name; do
    copy="${workdir}/mutation-${name}"
    mkdir -p "${copy}"
    tar --exclude=__pycache__ --exclude=logs -C "${repo_root}" -cf - app | tar -C "${copy}" -xf - || exit $?
    python "${mutations_py}" apply "${copy}" "${name}" || exit $?
    log="${workdir}/${name}.log"
    if run_tests "${copy}" fu2_t082 "${test_name}" >"${log}" 2>&1; then
        echo "FU2_MUTATION → FAIL: ${name} survived"; exit 1
    fi
    grep -q '^Ran 1 test' "${log}" && grep -q '^FAILED (' "${log}" \
        || { tail -80 "${log}"; exit 1; }
    echo "FU2_MUTATION → PASS: ${name}"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${mutations_py}" list)
[[ "${mutations}" == "${expected_mutations}" ]] || exit 1
run_tests "${repo_root}" fu2_t082 "${tests[@]}" >"${workdir}/control.log" 2>&1 \
    && check_log "${workdir}/control.log" "${expected_targeted}" || { tail -80 "${workdir}/control.log"; exit 1; }
echo "FU2_CONTROL → PASS: ${expected_targeted}/${expected_targeted}"
echo "FU2_MATRIX → PASS: targeted=${expected_targeted} barrier_rounds=${barrier_repeats} mutations=${expected_mutations} baseline=3"
