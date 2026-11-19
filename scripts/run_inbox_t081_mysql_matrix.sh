#!/usr/bin/env bash
set -uo pipefail

# T08-1 matrix: scheduler state machine, audited CLI, complete clearance guard,
# drain of accepted work and replay outside `enabled`. Runs the targeted
# real-MySQL suite as a DML-only user (plus SELECT on performance_schema for
# lock-footprint and lock-wait observation), repeats the barrier tests, runs
# every fix-removal mutation (each must FAIL), an unmutated control and,
# unless T081_REGRESSION=0, every app test module as regression. No schema
# change in T08-1, so migration matrices of tickets 02–07 are unaffected.
# Synthetic credentials only; no provider, LLM or WhatsApp effect. Loops read
# their lists from fd 3 so docker exec never drains them.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${T081_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-t081-}"
container="${resource_prefix}mysql-${suffix}"
network="${resource_prefix}net-${suffix}"
volume="${resource_prefix}data-${suffix}"
image="${WPP_TEST_IMAGE:-${resource_prefix}matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
workdir="$(mktemp -d)"
mutations_py="${repo_root}/scripts/inbox_t081_mutations.py"
suite="${repo_root}/app/tests/test_scheduler_drain_mysql.py"
module="tests.test_scheduler_drain_mysql"
expected_targeted=29
expected_mutations=31
barrier_repeats=3
barriers=(
    test_scheduler_show_snapshot_does_not_block_intake
    test_agent_show_snapshot_does_not_block_intake
    test_guard_vs_delivery_claim_no_deadlock_repeated
    test_guard_vs_late_accounting_no_deadlock_repeated
    test_guard_vs_intake_no_deadlock_repeated
    test_guard_vs_lifecycle_no_deadlock_repeated
    test_drain_intake_and_replay_lock_barriers_repeated
    test_drain_claim_barrier_repeated
    test_disable_owned_lane_barrier_repeated
    test_clearance_lock_footprint_no_gap_or_secondary_locks
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
    # S2: preserve raw proof, including failed mutations and optional baseline.
    if [[ -n "${T081_LOG_DIR:-}" ]]; then
        mkdir -p "${T081_LOG_DIR}"
        find "${workdir}" -maxdepth 1 -name '*.log' -exec cp {} "${T081_LOG_DIR}/" \;
    fi
    rm -rf "${workdir}"
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then docker image rm -f "${image}" >/dev/null 2>&1 || true; fi
    if [[ "$(resource_count)" != 0 ]]; then
        echo "T081_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "T081_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

declared="$(grep -cE '^    (async )?def test_' "${suite}")"
[[ "${declared}" == "${expected_targeted}" ]] \
    || { echo "T081_CASE_COUNT → FAIL: declared=${declared} expected=${expected_targeted}" >&2; exit 1; }
[[ "$(python "${mutations_py}" count)" == "${expected_mutations}" ]] \
    || { echo "T081_CASE_COUNT → FAIL: mutation list size" >&2; exit 1; }
python "${mutations_py}" check || exit $?
for name in "${barriers[@]}"; do
    grep -qE "^    async def ${name}\(" "${suite}" \
        || { echo "T081_CASE_COUNT → FAIL: barrier ${name} missing" >&2; exit 1; }
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
GRANT SELECT,INSERT,UPDATE,DELETE ON $1.* TO 't081_app'@'%';" || return $?
    mysql_root --default-character-set=latin1 "$1" < "${repo_root}/db/init.sql"
}
mysql_root </dev/null --execute="CREATE USER 't081_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT ON performance_schema.* TO 't081_app'@'%';" || exit $?
prepare_db t081_fresh || exit $?
[[ "$(mysql_root --batch --skip-column-names t081_fresh </dev/null \
    --execute="SELECT state FROM scheduler_runtime_state WHERE id=1")" == disabled ]] || exit 1
echo "T081_DEFAULT → PASS: fresh install scheduler state=disabled"

run_tests() {
    local root=$1 db=$2; shift 2
    docker run --rm --name="${resource_prefix}test-${suffix}-${RANDOM}" --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://t081_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "DATABASE_URL=mysql+aiomysql://t081_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "WPP_DISPOSABLE_TEST_DATABASE_MARKER=${db}" \
        --env OPENROUTER_API_KEY=synthetic-t081-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 2400 python -m unittest "$@" </dev/null
}

# S2 baseline proof: only copy new tests onto the exact unfixed commit.
if [[ -n "${T081_SECURITY_BASELINE_REF:-}" ]]; then
    baseline="${workdir}/baseline"
    mkdir -p "${baseline}"
    git -C "${repo_root}" archive "${T081_SECURITY_BASELINE_REF}" app AGENTS.md | tar -C "${baseline}" -xf - || exit $?
    cp "${suite}" "${baseline}/app/tests/"
    baseline_log="${workdir}/baseline.log"
    if run_tests "${baseline}" t081_fresh \
        "${module}.SchedulerDrainMySQLTests.test_scheduler_show_snapshot_does_not_block_intake" \
        "${module}.SchedulerDrainMySQLTests.test_agent_show_snapshot_does_not_block_intake" >"${baseline_log}" 2>&1; then
        echo "S2_BASELINE → FAIL: unfixed diagnostics passed" >&2; exit 1
    fi
    grep -q '^Ran 2 tests' "${baseline_log}" && grep -q '^FAILED (failures=2)' "${baseline_log}" \
        && [[ "$(grep -c '^AssertionError:.*True, 0, 0' "${baseline_log}")" == 2 ]] \
        || { tail -80 "${baseline_log}" >&2; exit 1; }
    echo "S2_BASELINE → PASS: 2/2 fail on ${T081_SECURITY_BASELINE_REF} (intake blocked, InnoDB locks/waits)"
    rm -rf "${baseline}"
fi

# ── 1. Targeted suite ───────────────────────────────────────────────────
target_log="${workdir}/target.log"
run_tests "${repo_root}" t081_fresh "${module}" >"${target_log}" 2>&1 \
    || { tail -n 60 "${target_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${target_log}" && grep -qE '^OK$' "${target_log}" \
    || { tail -n 20 "${target_log}" >&2; echo "T081_CASE_COUNT → FAIL: targeted" >&2; exit 1; }
if grep -qE 'Deadlock found|\(1213,' "${target_log}"; then
    echo "T081_DEADLOCK → FAIL: 1213 in targeted log" >&2; exit 1
fi
echo "T081_TARGETED → PASS: ${expected_targeted}/${expected_targeted} (zero skip, DML-only user, zero 1213)"

# ── 2. Barrier repetitions (each test also loops internally) ────────────
barrier_args=()
for name in "${barriers[@]}"; do barrier_args+=("${module}.SchedulerDrainMySQLTests.${name}"); done
for round in $(seq 1 "${barrier_repeats}"); do
    log="${workdir}/barrier-${round}.log"
    run_tests "${repo_root}" t081_fresh "${barrier_args[@]}" >"${log}" 2>&1 \
        || { tail -n 40 "${log}" >&2; exit 1; }
    grep -qE "^Ran ${#barriers[@]} tests?" "${log}" && grep -qE '^OK$' "${log}" || exit 1
    grep -qE 'Deadlock found|\(1213,' "${log}" && { echo "T081_DEADLOCK → FAIL (round ${round})" >&2; exit 1; }
    echo "T081_BARRIERS → PASS: round ${round}/${barrier_repeats} ${#barriers[@]}/${#barriers[@]} zero 1213"
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
    if run_tests "${copy}" t081_fresh "${test_name}" >"${log}" 2>&1; then
        echo "T081_MUTATION → FAIL (${name}): ${test_name##*.} passed without the fix" >&2
        exit 1
    fi
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "T081_MUTATION → FAIL (${name}): harness did not run the test" >&2; exit 1; }
    echo "T081_MUTATION → PASS (${name}): ${test_name##*.} fails without the fix"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${mutations_py}" list)
[[ "${mutations}" == "${expected_mutations}" ]] \
    || { echo "T081_MUTATION → FAIL: ${mutations}/${expected_mutations}" >&2; exit 1; }

# ── 4. Unmutated control ────────────────────────────────────────────────
run_tests "${repo_root}" t081_fresh "${module}" >"${workdir}/control.log" 2>&1 \
    || { tail -n 40 "${workdir}/control.log" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${workdir}/control.log" || exit 1
echo "T081_CONTROL → PASS: unmutated suite green after mutations"

# ── 5. Regression: every app test module (unit + real MySQL) ────────────
if [[ "${T081_REGRESSION:-1}" == 1 ]]; then
    prepare_db t081_regression || exit $?
    reg_log="${workdir}/regression.log"
    run_tests "${repo_root}" t081_regression discover -s /workspace/app/tests -t /workspace/app -p 'test_*.py' >"${reg_log}" 2>&1 \
        || { grep -E '^(FAIL|ERROR):' "${reg_log}" >&2; tail -n 20 "${reg_log}" >&2; exit 1; }
    ran="$(grep -oE '^Ran [0-9]+ tests?' "${reg_log}")"
    grep -qE '^OK( \(skipped=[0-9]+\))?$' "${reg_log}" || exit 1
    grep -E '^OK' "${reg_log}"
    echo "T081_REGRESSION → PASS: ${ran}"
fi

PYTHONPYCACHEPREFIX="${workdir}/pycache" python -m py_compile \
    "${repo_root}/app/src/database/repositories/scheduler_repo.py" \
    "${repo_root}/app/src/services/evolution_orchestrator.py" \
    "${repo_root}/app/src/delivery_cli.py" \
    "${suite}" "${mutations_py}" || exit $?
bash -n "${repo_root}/scripts/run_inbox_t081_mysql_matrix.sh" || exit $?
echo "T081_MATRIX → PASS: targeted=${expected_targeted} barrier_rounds=${barrier_repeats} mutations=${expected_mutations}"
