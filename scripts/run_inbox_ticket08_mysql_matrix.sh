#!/usr/bin/env bash
set -uo pipefail

# Integrated ticket08 gate. All disposable Docker resources share t087-, every
# component log is retained under .test-runs, and the complete discovery runs
# exactly once after ticket07 + T08-1..6 gates and the isolated rehearsal.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${T087_RUN_SUFFIX:-$(date +%Y%m%d-%H%M%S)-$$-$RANDOM}"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-t087-}"
[[ "${resource_prefix}" =~ ^[a-z][a-z0-9-]*-$ ]] || exit 2
run_dir="${T087_RUN_DIR:-${repo_root}/.test-runs/t087-${suffix}}"
image="${WPP_TEST_IMAGE:-${resource_prefix}app-${suffix}:local}"
built_image=0
container="${resource_prefix}mysql-${suffix}"
network="${resource_prefix}net-${suffix}"
volume="${resource_prefix}data-${suffix}"
shared_project="traycer-wpp-ai-bot-soft-ocelot"
# Recounted with TestLoader.discover on the integrated FU-1..FU-4 tree.
expected_regression=747
expected_guard_cases=3
expected_t084=14
expected_t084_mutations=17
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
mkdir -p "${run_dir}/components" "${run_dir}/rehearsal" "${run_dir}/mutations"
manifest="${run_dir}/matrix.log"
: >"${manifest}"

shared_count() {
    docker ps -aq --filter "label=com.docker.compose.project=${shared_project}" | wc -l | tr -d ' '
}
owned_count() {
    local count=0
    docker container inspect "${container}" >/dev/null 2>&1 && count=$((count+1))
    docker network inspect "${network}" >/dev/null 2>&1 && count=$((count+1))
    docker volume inspect "${volume}" >/dev/null 2>&1 && count=$((count+1))
    printf '%s' "${count}"
}
cleanup() {
    local original=$? cleanup_status=0
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then docker image rm -f "${image}" >/dev/null 2>&1 || true; fi
    if [[ "$(owned_count)" != 0 ]]; then
        echo "T087_CLEANUP → FAIL: owned disposable resource remains" | tee -a "${manifest}" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: ${shared_project} is not zero" | tee -a "${manifest}" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "T087_CLEANUP → PASS: disposable resources absent" | tee -a "${manifest}"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0" | tee -a "${manifest}"
        echo "T087_LOGS → RETAINED: ${run_dir}" | tee -a "${manifest}"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }
export WPP_TEST_RESOURCE_PREFIX="${resource_prefix}"
export WPP_TEST_IMAGE="${image}"

if ! docker image inspect "${image}" >/dev/null 2>&1; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?

# Import/discover before running any component; never infer the baseline from
# source-method grep, skipped suites, or a previous branch's arithmetic.
discovery_log="${run_dir}/discovery.log"
docker run --rm --name="${resource_prefix}discover-${suffix}" --network=none \
    --volume="${repo_root}:/workspace:ro" --tmpfs=/app/logs --tmpfs=/runtime \
    --tmpfs=/tmp/pycache --env PYTHONPYCACHEPREFIX=/tmp/pycache \
    --workdir=/runtime --env PYTHONPATH=/workspace/app --env WPP_REPO_ROOT=/workspace \
    --env OPENROUTER_API_KEY=synthetic-t087 --env EVOLUTION_API_URL=http://evolution.invalid \
    --env EVOLUTION_API_KEY=synthetic --env DATABASE_URL=mysql+aiomysql://synthetic:synthetic@db.invalid/wpp \
    --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" python -c '
import unittest
loader = unittest.TestLoader()
suite = loader.discover("/workspace/app/tests", pattern="test_*.py", top_level_dir="/workspace/app")
print("DISCOVER_COUNT", suite.countTestCases())
print("IMPORT_ERRORS", len(loader.errors))
for error in loader.errors:
    print(error)
raise SystemExit(bool(loader.errors))
' >"${discovery_log}" 2>&1 || { cat "${discovery_log}" >&2; exit 1; }
grep -qx "DISCOVER_COUNT ${expected_regression}" "${discovery_log}" \
    && grep -qx 'IMPORT_ERRORS 0' "${discovery_log}" || { cat "${discovery_log}" >&2; exit 1; }
grep -E '^(DISCOVER_COUNT|IMPORT_ERRORS) ' "${discovery_log}" | tee -a "${manifest}"

run_component() {
    local label=$1; shift
    local log="${run_dir}/components/${label}.log"
    echo "T087_COMPONENT_BEGIN ${label}" | tee -a "${manifest}"
    "$@" >"${log}" 2>&1
    local status=$?
    grep -E '→ (PASS|FAIL|DEFERRED)|^FU[234]_.* (PASS|FAIL)|^Ran [0-9]+ tests?|^OK$|^FAILED' "${log}" | tee -a "${manifest}" || true
    (( status == 0 )) || { tail -n 100 "${log}" >&2; return "${status}"; }
    echo "T087_COMPONENT_END ${label} status=0" | tee -a "${manifest}"
}

# FU-3: closed CHECK digest allowlist + maintenance on every table + security
# schema matrices. Isolated server, same resource prefix and retained logs.
WPP_TEST_LOG_ROOT="${run_dir}/components" \
    run_component fu3 "${repo_root}/scripts/run_inbox_fu3_mysql_matrix.sh" || exit $?
grep -q '^FU3_MATRIX PASS charsets=3 sec_s1=24/12/39 sec2_s1=3/6/18/3$' \
    "${run_dir}/components/fu3.log" || exit 1
while read -r -u 3 kind expected; do
    [[ "$(grep -c "^FU3_${kind} PASS " "${run_dir}/components/fu3.log")" == "${expected}" ]] || exit 1
done 3<<'FU3_COUNTS'
BASE_RED 3
FIX_GREEN 3
MUTATION 3
MAINTENANCE 132
REBUILD 3
UPGRADE 30
INTERRUPTION 6
TAMPER 81
NEIGHBOR_CONTROL 3
LITERAL_TAMPER 9
TRIGGER_TAMPER 3
FU3_COUNTS

# Ticket07 chains ticket02..06. Defer ticket02's full discovery: this runner
# owns the single complete regression after every T08 component is exercised.
TICKET02_REGRESSION=0 WPP_TEST_LOG_ROOT="${run_dir}/components" run_component ticket07 "${repo_root}/scripts/run_inbox_ticket07_mysql_matrix.sh" || exit $?
grep -q '^TICKET07_MATRIX → PASS: targeted=56 upgrades=5 interruptions=4 tampers=2 mutations=60 guards=1' \
    "${run_dir}/components/ticket07.log" || { echo "T087_COUNT → FAIL: ticket07" >&2; exit 1; }

# FU-2: skewed locking footprint, accounting gaps/savepoint and owned abandon.
FU2_LOG_DIR="${run_dir}/components/fu2" run_component fu2 "${repo_root}/scripts/run_inbox_fu2_mysql_matrix.sh" || exit $?
grep -q '^FU2_MATRIX → PASS: targeted=5 barrier_rounds=3 mutations=10 baseline=3' "${run_dir}/components/fu2.log" || exit 1
[[ "$(grep -c '^FU2_MUTATION → PASS:' "${run_dir}/components/fu2.log")" == 10 ]] || exit 1
[[ "$(grep -c '^FU2_BARRIERS → PASS:' "${run_dir}/components/fu2.log")" == 3 ]] || exit 1
grep -q '^FU2_BASELINE → PASS: 3/3 fail on ' "${run_dir}/components/fu2.log" || exit 1

T081_REGRESSION=0 run_component t081 "${repo_root}/scripts/run_inbox_t081_mysql_matrix.sh" || exit $?
T083_REGRESSION=0 WPP_TEST_LOG_ROOT="${run_dir}/components" \
    run_component t083 "${repo_root}/scripts/run_inbox_t083_mysql_matrix.sh" || exit $?
WPP_TEST_LOG_ROOT="${run_dir}/components" \
    run_component t082 "${repo_root}/scripts/run_inbox_t082_mysql_matrix.sh" || exit $?
T085_REGRESSION=0 T085_INTEGRATED=1 WPP_TEST_LOG_ROOT="${run_dir}/components" run_component t085 "${repo_root}/scripts/run_inbox_t085_mysql_matrix.sh" || exit $?
T086_REGRESSION=0 run_component t086 "${repo_root}/scripts/run_inbox_t086_mysql_matrix.sh" || exit $?
# Security rounds: SEC-S1 and SEC2-S1 schema matrices are sourced by T082;
# SEC-S3 delete guards run as their own component under the t087- prefix.
SECS3_REGRESSION=0 run_component secs3 "${repo_root}/scripts/run_inbox_sec_s3_mysql_matrix.sh" || exit $?

grep -q '^T081_MATRIX → PASS: targeted=29 barrier_rounds=3 mutations=31' "${run_dir}/components/t081.log" || exit 1
grep -q '^T082_MATRIX → PASS: targeted=58 regressions=3 mutations=65' "${run_dir}/components/t082.log" || exit 1
grep -q '^T083_MATRIX → PASS: charsets=3 upgrades=7 interruptions=4 tampers=17 migration_mutations=5 targeted=29 barrier_rounds=3 mutations=63 db_mutations=4 regression=0' "${run_dir}/components/t083.log" || exit 1
grep -q '^T083_LOGS → RETAINED:' "${run_dir}/components/t083.log" || exit 1
[[ "$(find "${run_dir}/components" -path '*/t083-*/target.log' -type f | wc -l | tr -d ' ')" == 1 ]] || exit 1
grep -q '^T085_MATRIX → PASS: targeted=30 mutations=88 regression=deferred' "${run_dir}/components/t085.log" || exit 1
grep -q '^T086_MATRIX → PASS: targeted=12 barrier_rounds=3 mutations=29' "${run_dir}/components/t086.log" || exit 1
grep -q '^SECS3_MATRIX → PASS: targeted=17 barrier_rounds=3 mutations=17' "${run_dir}/components/secs3.log" || exit 1
grep -q '^SECS3_BASE → PASS' "${run_dir}/components/secs3.log" || exit 1
[[ "$(grep -c '^SECS3_MUTATION → PASS' "${run_dir}/components/secs3.log")" == 17 ]] || exit 1
[[ "$(grep -c '^SECS3_AFFECTED → PASS' "${run_dir}/components/secs3.log")" == 4 ]] || exit 1
grep -q '^SEC_S1_SCHEMA PASS upgrades=24 interruptions=12 tampers=39$' "${run_dir}/components/t082.log" || exit 1
[[ "$(grep -c '^SEC_S1_UPGRADE PASS' "${run_dir}/components/t082.log")" == 24 ]] || exit 1
[[ "$(grep -c '^SEC_S1_INTERRUPTION PASS' "${run_dir}/components/t082.log")" == 12 ]] || exit 1
[[ "$(grep -c '^SEC_S1_TAMPER PASS' "${run_dir}/components/t082.log")" == 39 ]] || exit 1
# R2-S1 prints one checked aggregate (not per-fixture PASS lines). Preserve
# raw SQL logs as well as its exact 3/6/18/3 aggregate for independent recount.
[[ "$(grep -cx 'SEC2_S1_SCHEMA PASS upgrades=3 interruptions=6 tampers=18 validator_mutations=3' "${run_dir}/components/t082.log")" == 1 ]] || exit 1
[[ "$(find "${run_dir}/components" -path '*/t082-*/sec2-s1-schema/sec2_s1_*.log' -type f | wc -l | tr -d ' ')" == 27 ]] || exit 1
grep '^SEC2_S1_SCHEMA PASS' "${run_dir}/components/t082.log" | tee -a "${manifest}"
grep '^SEC_S1_SCHEMA PASS' "${run_dir}/components/t082.log" | tee -a "${manifest}"
[[ "$(grep -c '^T081_MUTATION → PASS' "${run_dir}/components/t081.log")" == 31 ]] || exit 1
[[ "$(grep -c '^T082_MUTATION → PASS' "${run_dir}/components/t082.log")" == 65 ]] || exit 1
[[ "$(grep -c '^T083_MUTATION → PASS' "${run_dir}/components/t083.log")" == 63 ]] || exit 1
[[ "$(grep -c '^T083_DB_MUTATION → PASS' "${run_dir}/components/t083.log")" == 4 ]] || exit 1
[[ "$(grep -c '^T083_MIGRATION_MUTATION → PASS' "${run_dir}/components/t083.log")" == 15 ]] || exit 1
[[ "$(grep -c '^T085_MUTATION → PASS' "${run_dir}/components/t085.log")" == 88 ]] || exit 1
[[ "$(grep -c '^T086_MUTATION → PASS' "${run_dir}/components/t086.log")" == 29 ]] || exit 1
[[ "$(grep -c '^T083_UPGRADE → PASS' "${run_dir}/components/t083.log")" == 21 ]] || exit 1
[[ "$(grep -c '^T083_INTERRUPTION → PASS' "${run_dir}/components/t083.log")" == 12 ]] || exit 1
[[ "$(grep -c '^T083_TAMPER → PASS' "${run_dir}/components/t083.log")" == 51 ]] || exit 1
echo "T087_COMPONENT_COUNTS → PASS: ticket07 + T08-1/2/3/5/6 + SEC-S1/SEC2-S1 schema + SEC-S3 exact, zero skipped gates" | tee -a "${manifest}"

# FU-4: disposable real app + MySQL + production/development Nginx gate.
WPP_TEST_LOG_ROOT="${run_dir}/components/fu4" \
    run_component fu4 "${repo_root}/scripts/run_fu4_health_nginx.sh" || exit $?
grep -qx 'F0T4_MATRIX PASS configs=2 controls=4 protocols=6 cases=2290 raw_cases=96 uri_mutations=2 fallback_mutations=3 host_redirects=6 outage_privacy=2 mysql=real fake_upstream=counted' "${run_dir}/components/fu4.log" || exit 1
[[ "$(grep -c '^F0T4_PRIVACY PASS ' "${run_dir}/components/fu4.log")" == 6 ]] || exit 1
[[ "$(grep -c '^F0T4_MUTATION_HIT PASS ' "${run_dir}/components/fu4.log")" == 3 ]] || exit 1
[[ "$(grep -c '^F0T4_CASE PASS ' "${run_dir}/components/fu4.log")" == 2290 ]] || exit 1
grep -qx 'FU4_INTERNAL PASS direct=app:5000 details=preserved provider=preserved docs=preserved public=minimal root=minimal mysql=real' "${run_dir}/components/fu4.log" || exit 1

# T08-4 config suite + every original and additive-block parity mutation.
python "${repo_root}/scripts/inbox_t084_mutations.py" check || exit $?
[[ "$(python "${repo_root}/scripts/inbox_t084_mutations.py" count)" == "${expected_t084_mutations}" ]] || exit 1
run_unit() {
    local root=$1; shift
    docker run --rm --network=none --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace \
        --workdir=/runtime --env PYTHONPATH=/workspace/app --env OPENROUTER_API_KEY=synthetic-t087 \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env DATABASE_URL=mysql+aiomysql://synthetic:synthetic@db.invalid/wpp \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" python -m unittest "$@" </dev/null
}
t084_log="${run_dir}/components/t084.log"
run_unit "${repo_root}" tests.test_settings >"${t084_log}" 2>&1 || { tail -80 "${t084_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_t084} tests?" "${t084_log}" && grep -q '^OK$' "${t084_log}" || exit 1
mutations=0
while read -r -u 3 name test_name; do
    copy="${run_dir}/mutations/t084-${name}"
    mkdir -p "${copy}"
    tar --exclude=__pycache__ --exclude=logs -C "${repo_root}" -cf - app AGENTS.md db .env.example \
        | tar -C "${copy}" -xf - || exit $?
    python "${repo_root}/scripts/inbox_t084_mutations.py" apply "${copy}" "${name}" || exit $?
    log="${run_dir}/mutations/t084-${name}.log"
    if run_unit "${copy}" "${test_name}" >"${log}" 2>&1; then
        echo "T084_MUTATION → FAIL (${name}): passed without fix" >&2; exit 1
    fi
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" || exit 1
    echo "T084_MUTATION → PASS (${name})" | tee -a "${t084_log}" "${manifest}"
    rm -rf "${copy}"
    mutations=$((mutations+1))
done 3< <(python "${repo_root}/scripts/inbox_t084_mutations.py" list)
[[ "${mutations}" == "${expected_t084_mutations}" ]] || exit 1
echo "T084_MATRIX → PASS: targeted=${expected_t084} mutations=${expected_t084_mutations}" | tee -a "${t084_log}" "${manifest}"

# Dedicated DB for isolated rehearsal, indexed-plan capture and one regression.
docker network create "${network}" >/dev/null || exit $?
docker volume create "${volume}" >/dev/null || exit $?
docker run --detach --name "${container}" --network "${network}" \
    --volume "${volume}:/var/lib/mysql" --env "MYSQL_ROOT_PASSWORD=${root_password}" \
    mysql:8.0 --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci >/dev/null || exit $?
ready=0; streak=0
for _ in $(seq 1 150); do
    if docker exec --env "MYSQL_PWD=${root_password}" "${container}" mysql --user=root \
        --host=127.0.0.1 --protocol=tcp --execute='SELECT VERSION()' </dev/null >/dev/null 2>&1; then
        streak=$((streak+1)); if (( streak >= 3 )); then ready=1; break; fi
    else streak=0; fi
    sleep 1
done
(( ready == 1 )) || { docker logs --tail 100 "${container}" >&2; exit 1; }
mysql_root() { docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" mysql --user=root --protocol=socket "$@"; }
db=t087_integrated
mysql_root </dev/null --execute="CREATE DATABASE ${db} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" || exit $?
mysql_root --default-character-set=latin1 "${db}" <"${repo_root}/db/init.sql" || exit $?
mysql_root </dev/null --execute="CREATE USER 't087_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT,INSERT,UPDATE,DELETE ON ${db}.* TO 't087_app'@'%';
GRANT SELECT ON performance_schema.* TO 't087_app'@'%';" || exit $?

run_mysql_tests() {
    docker run --rm --network="container:${container}" --volume="${repo_root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache --env PYTHONPYCACHEPREFIX=/tmp/pycache \
        --workdir=/runtime --env PYTHONPATH=/workspace/app --env WPP_REPO_ROOT=/workspace \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://t087_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "DATABASE_URL=mysql+aiomysql://t087_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "WPP_DISPOSABLE_TEST_DATABASE_MARKER=${test_marker:-${db}}" \
        --env OPENROUTER_API_KEY=synthetic-t087-openrouter \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic-t087-evolution \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic-t087-bearer \
        --env INBOUND_RECEIPT_HMAC_CURRENT_VERSION=1 \
        --env 'INBOUND_RECEIPT_HMAC_KEYS={"1":"synthetic-t087-receipt"}' \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic-t087-chatwoot \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic-t087-chatwoot-secret \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 3600 python -m unittest "$@" </dev/null
}

# Shared disposable-DB guard: every whole-DB-owning T08 suite (T08-2 retention,
# T08-3 replicas, T08-7 rehearsal) must refuse a schema other than the runner's
# exact marker BEFORE touching data. A sentinel row proves nothing was wiped.
guard_log="${run_dir}/rehearsal/disposable-guard.log"
mysql_root </dev/null "${db}" --execute="INSERT INTO delivery_operator_actions (delivery_id, action, actor, reason) VALUES (NULL, 't087_guard_sentinel', 't087', 'guard proof');" || exit $?
if test_marker="not-${db}" run_mysql_tests \
    tests.test_scheduler_retention_mysql.SchedulerRetentionMySQLTests.test_ordinary_full_purge_preserves_ledger_and_messages \
    tests.test_replica_compat_mysql.ReplicaCompatMySQLTests.test_heartbeat_roundtrip_stop_marker_and_revival \
    tests.test_ticket08_rehearsal_mysql.Ticket08RehearsalMySQLTests.test_mixed_version_fails_readiness_and_enable_gate \
    >"${guard_log}" 2>&1; then
    echo "T087_GUARD → FAIL: suites ran against a non-marker database" >&2; exit 1
fi
grep -qE "^Ran ${expected_guard_cases} tests" "${guard_log}" \
    && grep -qE "^FAILED \(failures=${expected_guard_cases}\)" "${guard_log}" \
    && [[ "$(grep -c "AssertionError: 't087_integrated' != 'not-t087_integrated'" "${guard_log}")" == "${expected_guard_cases}" ]] \
    || { tail -60 "${guard_log}" >&2; echo "T087_GUARD → FAIL: unexpected refusal shape" >&2; exit 1; }
sentinel="$(mysql_root </dev/null --batch --skip-column-names "${db}" --execute="SELECT COUNT(*) FROM delivery_operator_actions WHERE action='t087_guard_sentinel'")"
[[ "${sentinel}" == 1 ]] || { echo "T087_GUARD → FAIL: sentinel wiped (${sentinel})" >&2; exit 1; }
echo "T087_GUARD → PASS: ${expected_guard_cases} whole-DB suites refused non-marker schema, sentinel intact" | tee -a "${manifest}"

life1="${run_dir}/rehearsal/worker-lifetime-1.log"
run_mysql_tests tests.test_ticket08_rehearsal_mysql.Ticket08RehearsalMySQLTests.test_two_stage_canary_drain_replay_and_fake_delivery >"${life1}" 2>&1 \
    || { tail -100 "${life1}" >&2; exit 1; }
sha256sum "${life1}" >"${run_dir}/rehearsal/worker-lifetime-1.sha256"
life2="${run_dir}/rehearsal/worker-lifetime-2.log"
run_mysql_tests \
    tests.test_ticket08_rehearsal_mysql.Ticket08RehearsalMySQLTests.test_unknown_generation_blocks_clearance_until_audited_resolution \
    tests.test_ticket08_rehearsal_mysql.Ticket08RehearsalMySQLTests.test_mixed_version_fails_readiness_and_enable_gate \
    tests.test_ticket08_rehearsal_mysql.Ticket08RehearsalMySQLTests.test_security_paths_health_cache_list_delete_and_checkpoint \
    >"${life2}" 2>&1 || { tail -100 "${life2}" >&2; exit 1; }
sha256sum "${life2}" >"${run_dir}/rehearsal/worker-lifetime-2.sha256"
grep -qE '^Ran 1 tests?' "${life1}" && grep -q '^OK$' "${life1}" || exit 1
grep -qE '^Ran 3 tests?' "${life2}" && grep -q '^OK$' "${life2}" || exit 1
for log in "${life1}" "${life2}"; do
    if grep -Fq -e '5511987654321' -e 't087-private-content-never-log' \
        -e 'new-private-content-never-log' -e 't087-secret-bearer-never-log' \
        -e 't087-secret-receipt-never-log' "${log}"; then
        echo "T087_PRIVACY → FAIL: sentinel leaked in ${log}" >&2; exit 1
    fi
    if grep -Eq '[[:xdigit:]]{64}' "${log}"; then
        echo "T087_PRIVACY → FAIL: receipt-like hash leaked in ${log}" >&2; exit 1
    fi
done
echo "T087_REHEARSAL → PASS: signed ingress, double delivery, provider fault, drain, quarantine, mixed version, cached /health, agent-scheduler list, delete 409, retention checkpoint" | tee -a "${manifest}"
echo "T087_PRIVACY → PASS: 2 preserved worker lifetimes, no phone/content/secret/receipt hash" | tee -a "${manifest}"

# Consolidated indexed claim/load evidence.
explain="${run_dir}/rehearsal/explain.log"
{
    echo 'OUTBOX_CLAIM_PLAN'
    mysql_root --batch --skip-column-names "${db}" <"${repo_root}/scripts/measure_outbox_claim_plan.sql"
    echo 'SCHEDULER_CLAIM_PLAN'
    mysql_root --batch --skip-column-names "${db}" <"${repo_root}/scripts/measure_scheduler_claim_plan.sql"
    echo 'AGGREGATE_LOAD_PLANS'
    grep '^T085_EXPLAIN ' "${run_dir}/components/t085.log"
} >"${explain}" || exit $?
grep -Eq 'idx_outbox|idx_turn_ready_claim|idx_turn_claim_lease' "${explain}" || { cat "${explain}" >&2; exit 1; }
echo "T087_EXPLAIN → PASS: outbox claim + scheduler claim + aggregate loads indexed" | tee -a "${manifest}"

reg_log="${run_dir}/regression.log"
run_mysql_tests discover -s /workspace/app/tests -t /workspace/app -p 'test_*.py' >"${reg_log}" 2>&1 \
    || { grep -E '^(FAIL|ERROR):' "${reg_log}" >&2; tail -100 "${reg_log}" >&2; exit 1; }
ran="$(grep -oE '^Ran [0-9]+ tests?' "${reg_log}")"
[[ "${ran}" == "Ran ${expected_regression} tests" ]] \
    || { echo "T087_REGRESSION_COUNT → FAIL: ${ran} expected=${expected_regression}" >&2; exit 1; }
grep -q '^OK$' "${reg_log}" || { tail -30 "${reg_log}" >&2; exit 1; }
if grep -qE '^OK \(skipped=|^skipped ' "${reg_log}"; then echo "T087_ZERO_SKIP → FAIL" >&2; exit 1; fi
echo "T087_REGRESSION → PASS: ${ran}, zero skip, complete discovery exactly once" | tee -a "${manifest}"

grep -R -qE '^(T08[1-6]|SECS3)_.*→ FAIL|^SEC(2)?_S1_[A-Z]+ FAIL|^FU[234]_.*(→ )?FAIL' "${run_dir}/components" && exit 1
bash -n "${repo_root}/scripts/run_inbox_ticket08_mysql_matrix.sh" || exit $?
python -m py_compile "${repo_root}/scripts/inbox_t084_mutations.py" \
    "${repo_root}/app/tests/test_ticket08_rehearsal_mysql.py" \
    "${repo_root}/app/tests/test_inbox_scheduler_runbook.py" || exit $?
echo "T087_MATRIX → PASS: baseline=${expected_regression} guard=${expected_guard_cases} ticket07=1 t08_matrices=6 sec_matrices=3 followups=3 rehearsal=4 privacy=2 explain=3" | tee -a "${manifest}"
