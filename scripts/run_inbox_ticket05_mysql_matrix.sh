#!/usr/bin/env bash
set -uo pipefail

# Ticket 05 matrix: every ticket 02/03/04 gate (through the ticket 04 matrix,
# including the full zero-skip discovery suite with an asserted case count),
# then ticket05 schema convergence from populated predecessors, interruption at
# every ticket05 DDL boundary, exact-shape fail-closed adversaries, targeted
# real-MySQL tests as a DML-only user, fix-removal code mutations and guard
# drops. Synthetic credentials only; no provider, LLM or WhatsApp effect.
# Every loop reads its case list from fd 3 so docker exec never drains it.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${TICKET05_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
container="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket05-mysql-${suffix}"
network="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket05-net-${suffix}"
volume="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket05-data-${suffix}"
image="${WPP_TEST_IMAGE:-wpp-ticket05-matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
workdir="$(mktemp -d)"
fixture="${repo_root}/scripts/fixtures/ticket05_populate.sql"
expected_targeted=86
# 264 = ticket02–04 + ticket06 unit cases; 66 = ticket06 MySQL suite;
# 6 = ticket06 review round 3 MySQL suite.
expected_full=$((264 + 66 + 6 + 53 + expected_targeted))
expected_mutations=49
# Fresh + repeat, then each populated predecessor → current (counted below).
expected_migrations=4
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
        echo "TICKET05_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "TICKET05_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

# Declared counts must match the source: a test added without updating the
# matrix (or a lost test) fails here, not silently.
declared="$(grep -cE '^    (async )?def test_' \
    "${repo_root}/app/tests/test_handoff_terminal_unit.py" \
    "${repo_root}/app/tests/test_handoff_terminal_mysql.py" \
    | awk -F: '{s+=$2} END {print s}')"
[[ "${declared}" == "${expected_targeted}" ]] \
    || { echo "TICKET05_CASE_COUNT → FAIL: declared=${declared} expected=${expected_targeted}" >&2; exit 1; }
[[ "$(python "${repo_root}/scripts/inbox_ticket05_mutations.py" count)" == "${expected_mutations}" ]] \
    || { echo "TICKET05_CASE_COUNT → FAIL: mutation list size" >&2; exit 1; }

# ── 1. Every predecessor gate (ticket 02/03/04) incl. full discovery ────
prior_log="${workdir}/ticket04.log"
"${repo_root}/scripts/run_inbox_ticket04_mysql_matrix.sh" >"${prior_log}" 2>&1
prior_status=$?
grep -E '→ (PASS|FAIL)|^Ran [0-9]+ tests?|^OK|^FAILED' "${prior_log}"
(( prior_status == 0 )) || { tail -n 80 "${prior_log}" >&2; exit "${prior_status}"; }
if [[ "${TICKET02_REGRESSION:-1}" == 1 ]]; then
    grep -qE "^Ran ${expected_full} tests?" "${prior_log}" \
        || { echo "TICKET05_CASE_COUNT → FAIL: full discovery is not ${expected_full}" >&2; exit 1; }
    grep -qE '^OK$' "${prior_log}" && ! grep -qE '^OK \(.*skipped' "${prior_log}" \
        || { echo "TICKET05_CASE_COUNT → FAIL: full discovery not a zero-skip OK" >&2; exit 1; }
    discovery="${expected_full} zero-skip"
else
    grep -q '^TICKET02_REGRESSION → DEFERRED: integrated matrix owns the single full regression' "${prior_log}" \
        || { echo "TICKET05_CASE_COUNT → FAIL: deferred discovery marker missing" >&2; exit 1; }
    discovery="deferred to integrated owner"
fi
grep -q '^TICKET04_MATRIX → PASS' "${prior_log}" || exit 1
echo "TICKET05_PREDECESSORS → PASS: ticket02/03/04 gates; full discovery=${discovery}"

if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?

for ref in 35f9c07 d52a495 f59c32c; do
    git -C "${repo_root}" show "${ref}:db/init.sql" > "${workdir}/${ref}.sql" || exit $?
done
sed 's/^CALL validate_ticket05_schema()\$\$$//' "${repo_root}/db/init.sql" \
    > "${workdir}/no-ticket05-validator.sql"
grep -q 'CALL validate_ticket05_schema' "${workdir}/no-ticket05-validator.sql" && exit 1

docker network create "${network}" >/dev/null || exit $?
docker volume create "${volume}" >/dev/null || exit $?
docker run --detach --name "${container}" --network "${network}" \
    --volume "${volume}:/var/lib/mysql" --env "MYSQL_ROOT_PASSWORD=${root_password}" \
    mysql:8.0 --character-set-server=utf8mb4 \
    --collation-server=utf8mb4_unicode_ci >/dev/null || exit $?
ready=0
# Ready means the final server: the mysql:8 entrypoint's temporary init
# server answers on the socket (with --skip-networking) and then restarts, so
# a socket probe can race into 2002. Require 3 consecutive TCP successes.
streak=0
for _ in $(seq 1 150); do
    if docker exec --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --host=127.0.0.1 --protocol=tcp \
        --execute='SELECT VERSION()' >/dev/null 2>&1; then
        streak=$((streak + 1))
        if (( streak >= 3 )); then
            ready=1
            break
        fi
    else
        streak=0
    fi
    sleep 1
done
(( ready == 1 )) || { docker logs --tail 100 "${container}" >&2; exit 1; }
# Assert the socket path the matrix uses reaches the same (final) server.
socket_version="$(docker exec --env "MYSQL_PWD=${root_password}" "${container}" \
    mysql --user=root --protocol=socket --batch --skip-column-names \
    --execute='SELECT VERSION()' </dev/null)" \
    || { echo "MYSQL_READY → FAIL: socket unreachable after TCP readiness" >&2; exit 1; }
echo "MYSQL_READY → PASS: final server ${socket_version} (TCP streak 3, socket ok)"

mysql_root() {
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "$@"
}
apply_file() {
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "$1" < "$2"
}
apply_current() { apply_file "$1" "${repo_root}/db/init.sql"; }
create_db() {
    mysql_root --execute="CREATE DATABASE $1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" </dev/null
}

# Exact ticket05 shape: columns (all attributes, ordinal), indexes (all
# attributes), FKs (schema/table/columns/rules), checks (enforced + clause
# digest) and every versioned guard digest.
ticket05_shape() {
    local tables="${2:-'scheduled_handoffs','handoff_context_assignments'}"
    mysql_root --batch --skip-column-names "$1" </dev/null --execute="
SET SESSION group_concat_max_len=4194304;
SELECT CONCAT_WS('#',
 (SELECT GROUP_CONCAT(CONCAT_WS(':',table_name,ordinal_position,column_name,column_type,
          is_nullable,IFNULL(column_default,'NULL'),IFNULL(character_set_name,'-'),
          IFNULL(collation_name,'-'),extra) ORDER BY table_name,ordinal_position)
  FROM information_schema.columns WHERE table_schema=DATABASE()
    AND table_name IN (${tables})),
 (SELECT GROUP_CONCAT(CONCAT_WS(':',table_name,index_name,non_unique,seq_in_index,
          column_name,IFNULL(collation,'-'),IFNULL(sub_part,'-'),index_type,is_visible)
          ORDER BY table_name,index_name,seq_in_index)
  FROM information_schema.statistics WHERE table_schema=DATABASE()
    AND table_name IN (${tables})),
 (SELECT GROUP_CONCAT(CONCAT_WS(':',k.table_name,k.constraint_name,k.ordinal_position,
          k.column_name,k.referenced_table_schema=DATABASE(),k.referenced_table_name,
          k.referenced_column_name,r.update_rule,r.delete_rule)
          ORDER BY k.table_name,k.constraint_name,k.ordinal_position)
  FROM information_schema.key_column_usage k
  JOIN information_schema.referential_constraints r
    ON r.constraint_schema=k.constraint_schema AND r.constraint_name=k.constraint_name
   AND r.table_name=k.table_name
  WHERE k.constraint_schema=DATABASE()
    AND k.table_name IN (${tables})),
 (SELECT GROUP_CONCAT(CONCAT_WS(':',tc.table_name,tc.constraint_name,tc.enforced,
          SHA2(cc.check_clause,256)) ORDER BY tc.table_name,tc.constraint_name)
  FROM information_schema.table_constraints tc
  JOIN information_schema.check_constraints cc
    ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name
  WHERE tc.constraint_schema=DATABASE()
    AND tc.table_name IN (${tables})),
 (SELECT GROUP_CONCAT(CONCAT_WS(':',event_object_table,trigger_name,
          SHA2(action_statement,256)) ORDER BY trigger_name)
  FROM information_schema.triggers WHERE trigger_schema=DATABASE()
    AND trigger_name LIKE 'trg\\_%\\_v_'));"
}

fixture_state() {
    mysql_root --batch --skip-column-names "$1" </dev/null --execute="
SELECT CONCAT_WS('|',
 (SELECT COUNT(*) FROM agentes WHERE instance_name='fixture-t05'),
 (SELECT COUNT(*) FROM sessoes WHERE telefone='551100000005'),
 (SELECT COUNT(*) FROM inbound_events WHERE provider_event_id='fixture-event-1'
     AND receipt_disposition='scheduled'),
 (SELECT COUNT(*) FROM conversation_turns WHERE status='pending'),
 (SELECT COUNT(*) FROM turn_input_memberships WHERE membership_state='active'),
 (SELECT COUNT(*) FROM handoff_attempts WHERE generation=1),
 (SELECT COUNT(*) FROM outbox_deliveries WHERE dedupe_key='fixture-open'
     AND delivery_class='provider_setup_context'
     AND JSON_EXTRACT(payload,'$.summary')='fixture'));"
}
expected_fixture="1|1|1|1|1|1|1"

migration_cases=0
create_db t05_fresh || exit $?
apply_current t05_fresh || exit $?
apply_current t05_fresh || exit $?
fresh_shape="$(ticket05_shape t05_fresh)" || exit $?
guards="$(mysql_root --batch --skip-column-names t05_fresh </dev/null --execute="
SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE()
AND trigger_name LIKE 'trg\\_%\\_v_'
AND trigger_name <> 'trg_conversation_turn_lifecycle_v1';")" || exit $?
# The ticket07 lifecycle guard is asserted by the ticket07 matrix.
[[ "${guards}" == 11 ]] || { echo "TICKET05_TRIGGERS → FAIL: ${guards}/11" >&2; exit 1; }
python "${repo_root}/scripts/check_trigger_body_digests.py" "${repo_root}/db/init.sql" \
    | tail -n 1 || exit $?
echo "TICKET05_MIGRATION → PASS (fresh + repeat; versioned guards=11/11)"
migration_cases=$((migration_cases + 1))

# ── 2. Populated predecessors → current, repeated ───────────────────────
while read -r -u 3 ref; do
    [[ -n "${ref}" ]] || continue
    db="t05_upgrade_${ref}"
    create_db "${db}" || exit $?
    apply_file "${db}" "${workdir}/${ref}.sql" || exit $?
    apply_file "${db}" "${fixture}" || exit $?
    [[ "$(fixture_state "${db}")" == "${expected_fixture}" ]] || exit 1
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(ticket05_shape "${db}")" == "${fresh_shape}" ]] \
        || { echo "TICKET05_MIGRATION → FAIL (${ref}): shape diverged" >&2; exit 1; }
    [[ "$(fixture_state "${db}")" == "${expected_fixture}" ]] \
        || { echo "TICKET05_MIGRATION → FAIL (${ref}): populated rows changed" >&2; exit 1; }
    echo "TICKET05_MIGRATION → PASS (${ref} populated → current, repeated): exact shape, rows kept"
    migration_cases=$((migration_cases + 1))
done 3<<'REFS'
35f9c07
d52a495
f59c32c
REFS
[[ "${migration_cases}" == "${expected_migrations}" ]] \
    || { echo "TICKET05_MIGRATION → FAIL: ${migration_cases}/${expected_migrations}" >&2; exit 1; }

# ── 3. Interruption at every ticket05 DDL boundary (populated base) ─────
interruption_cases=0
while read -r -u 3 marker; do
    [[ -n "${marker}" ]] || continue
    db="t05_interrupt_${interruption_cases}"
    prefix="${workdir}/interrupt-${interruption_cases}.sql"
    awk -v marker="${marker}" '{print; if (index($0, marker)) exit}' \
        "${repo_root}/db/init.sql" > "${prefix}"
    grep -qF -- "${marker}" "${prefix}" || { echo "missing marker ${marker}" >&2; exit 1; }
    create_db "${db}" || exit $?
    apply_file "${db}" "${workdir}/f59c32c.sql" || exit $?
    apply_file "${db}" "${fixture}" || exit $?
    apply_file "${db}" "${prefix}" || exit $?
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(ticket05_shape "${db}")" == "${fresh_shape}" ]] \
        || { echo "TICKET05_INTERRUPTION → FAIL (${marker})" >&2; exit 1; }
    [[ "$(fixture_state "${db}")" == "${expected_fixture}" ]] || exit 1
    echo "TICKET05_INTERRUPTION → PASS (${marker}): converged, rows kept"
    interruption_cases=$((interruption_cases + 1))
done 3<<'MARKERS'
ticket05-schema-boundary: scheduled-handoffs-created
ticket05-schema-boundary: handoff-context-assignments-created
ticket05-schema-boundary: exact-schema-validated
ticket05-trigger-boundary: scheduled-handoff-v1-created
ticket05-trigger-boundary: handoff-context-assignment-v1-created
ticket05-trigger-boundary: turn-input-handoff-state-v1-created
ticket05-trigger-boundary: outbox-context-payload-v1-created
MARKERS
[[ "${interruption_cases}" == 7 ]] \
    || { echo "TICKET05_INTERRUPTION → FAIL: ${interruption_cases}/7" >&2; exit 1; }

# ── 4. Exact-shape adversaries fail closed; nothing is rewritten ────────
shape_cases=0
while IFS='|' read -r -u 3 label table expected statement; do
    [[ -n "${label}" ]] || continue
    db="t05_bad_${shape_cases}"
    create_db "${db}" || exit $?
    if [[ "${label}" == "predecessor-column" ]]; then
        apply_file "${db}" "${workdir}/f59c32c.sql" || exit $?
    else
        apply_current "${db}" || exit $?
    fi
    mysql_root "${db}" </dev/null --execute="${statement}" || exit $?
    # Only the divergent table is compared: the other ticket05 table may be
    # created additively before the validator stops the migration.
    before="$(ticket05_shape "${db}" "'${table}'")" || exit $?
    output="$(apply_current "${db}" 2>&1)"; rc=$?
    if (( rc == 0 )) || ! grep -qF "${expected}" <<<"${output}"; then
        echo "TICKET05_SCHEMA → FAIL (${label}): expected '${expected}'" >&2
        printf '%s\n' "${output}" | tail -n 5 >&2
        exit 1
    fi
    [[ "$(ticket05_shape "${db}" "'${table}'")" == "${before}" ]] \
        || { echo "TICKET05_SCHEMA → FAIL (${label}): divergent shape was rewritten" >&2; exit 1; }
    echo "TICKET05_SCHEMA → PASS (${label}): named fail-closed, nothing rewritten"
    shape_cases=$((shape_cases + 1))
done 3<<'CASES'
predecessor-column|scheduled_handoffs|ticket05 scheduled handoff column shape requires audited repair|CREATE TABLE scheduled_handoffs (handoff_attempt_id BIGINT PRIMARY KEY);
column-collation|handoff_context_assignments|ticket05 handoff context column shape requires audited repair|ALTER TABLE handoff_context_assignments MODIFY source_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL;
column-ordinal|handoff_context_assignments|ticket05 handoff context column shape requires audited repair|ALTER TABLE handoff_context_assignments MODIFY created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) AFTER handoff_attempt_id;
column-signedness|handoff_context_assignments|ticket05 handoff context column shape requires audited repair|ALTER TABLE handoff_context_assignments DROP CHECK ck_handoff_context_coverage_ordinal, MODIFY coverage_ordinal INT NOT NULL, ADD CONSTRAINT ck_handoff_context_coverage_ordinal CHECK (coverage_ordinal > 0);
column-nullability|scheduled_handoffs|ticket05 scheduled handoff column shape requires audited repair|ALTER TABLE scheduled_handoffs MODIFY scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL;
index-missing|scheduled_handoffs|ticket05 handoff index shape requires audited repair|ALTER TABLE scheduled_handoffs DROP INDEX idx_scheduled_handoff_session;
index-invisible|scheduled_handoffs|ticket05 handoff index shape requires audited repair|ALTER TABLE scheduled_handoffs ALTER INDEX idx_scheduled_handoff_session INVISIBLE;
index-order|handoff_context_assignments|ticket05 handoff index shape requires audited repair|ALTER TABLE handoff_context_assignments DROP INDEX idx_handoff_context_source, ADD INDEX idx_handoff_context_source (handoff_attempt_id, coverage_ordinal, source_id);
fk-rule|scheduled_handoffs|ticket05 handoff foreign key shape requires audited repair|ALTER TABLE scheduled_handoffs DROP FOREIGN KEY fk_scheduled_handoff_summary_attempt; ALTER TABLE scheduled_handoffs ADD CONSTRAINT fk_scheduled_handoff_summary_attempt FOREIGN KEY (summary_generation_attempt_id) REFERENCES generation_attempts(id) ON DELETE CASCADE;
check-clause|handoff_context_assignments|ticket05 handoff check shape requires audited repair|ALTER TABLE handoff_context_assignments DROP CHECK ck_handoff_context_coverage_ordinal, ADD CONSTRAINT ck_handoff_context_coverage_ordinal CHECK (coverage_ordinal >= 0);
check-not-enforced|handoff_context_assignments|ticket05 handoff check shape requires audited repair|ALTER TABLE handoff_context_assignments ALTER CHECK ck_handoff_context_coverage_ordinal NOT ENFORCED;
weakened-guard|handoff_context_assignments|immutability guard definition requires audited repair|DROP TRIGGER trg_handoff_context_assignment_immutable_v1; CREATE TRIGGER trg_handoff_context_assignment_immutable_v1 BEFORE UPDATE ON handoff_context_assignments FOR EACH ROW SET @weakened = 1;
CASES
[[ "${shape_cases}" == 12 ]] || { echo "TICKET05_SCHEMA → FAIL: ${shape_cases}/12" >&2; exit 1; }

# The exact-shape validator is load-bearing: without its CALL the same
# divergent shape is silently accepted.
create_db t05_validator_mutation || exit $?
apply_current t05_validator_mutation || exit $?
mysql_root t05_validator_mutation </dev/null --execute="ALTER TABLE handoff_context_assignments MODIFY source_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL;" || exit $?
apply_file t05_validator_mutation "${workdir}/no-ticket05-validator.sql" \
    || { echo "TICKET05_SCHEMA_MUTATION → FAIL: mutant still rejected" >&2; exit 1; }
echo "TICKET05_SCHEMA_MUTATION → PASS: divergent shape accepted only without the validator"

# ── 5. Targeted tests as a DML-only user ────────────────────────────────
mysql_root </dev/null --execute="CREATE USER 'ticket05_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT,INSERT,UPDATE,DELETE ON t05_fresh.* TO 'ticket05_app'@'%';" || exit $?

run_tests() {
    local root=$1; shift
    docker run --rm --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://ticket05_app:${app_password}@127.0.0.1:3306/t05_fresh" \
        --env "DATABASE_URL=mysql+aiomysql://ticket05_app:${app_password}@127.0.0.1:3306/t05_fresh" \
        --env OPENROUTER_API_KEY=synthetic-ticket05-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 600 python -m unittest "$@" </dev/null
}

target_log="${workdir}/target.log"
run_tests "${repo_root}" tests.test_handoff_terminal_unit tests.test_handoff_terminal_mysql \
    >"${target_log}" 2>&1 || { tail -n 80 "${target_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${target_log}" && grep -qE '^OK$' "${target_log}" \
    || { tail -n 20 "${target_log}" >&2; echo "TICKET05_CASE_COUNT → FAIL: targeted" >&2; exit 1; }
echo "TICKET05_TARGETED → PASS: ${expected_targeted}/${expected_targeted} (zero skip, DML-only user)"

# ── 6. Fix-removal code mutations (each must fail against the DB) ───────
mutations=0
while read -r -u 3 name test_name; do
    [[ -n "${name}" ]] || continue
    copy="${workdir}/mutation-${name}"
    mkdir -p "${copy}"
    tar --exclude=__pycache__ --exclude=logs -C "${repo_root}" -cf - app \
        | tar -C "${copy}" -xf - || exit $?
    mkdir -p "${copy}/app/logs"
    python "${repo_root}/scripts/inbox_ticket05_mutations.py" apply "${copy}" "${name}" || exit $?
    log="${workdir}/${name}.log"
    if run_tests "${copy}" "${test_name}" >"${log}" 2>&1; then
        echo "TICKET05_MUTATION → FAIL (${name}): ${test_name##*.} passed without the fix" >&2
        exit 1
    fi
    # The failure must be the test's, not the harness's (class I).
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "TICKET05_MUTATION → FAIL (${name}): harness did not run the test" >&2; exit 1; }
    echo "TICKET05_MUTATION → PASS (${name}): ${test_name##*.} fails without the fix"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${repo_root}/scripts/inbox_ticket05_mutations.py" list)
[[ "${mutations}" == "${expected_mutations}" ]] \
    || { echo "TICKET05_MUTATION → FAIL: ${mutations}/${expected_mutations}" >&2; exit 1; }

# ── 7. Guard drops (each guard is load-bearing on the real DB) ──────────
guard_mutations=0
while IFS='|' read -r -u 3 trigger test_name; do
    [[ -n "${trigger}" ]] || continue
    mysql_root t05_fresh </dev/null --execute="DROP TRIGGER ${trigger};" || exit $?
    log="${workdir}/${trigger}.log"
    if run_tests "${repo_root}" "${test_name}" >"${log}" 2>&1; then
        echo "TICKET05_GUARD_MUTATION → FAIL (${trigger}): test passed without guard" >&2
        exit 1
    fi
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" \
        || { tail -n 20 "${log}" >&2; exit 1; }
    apply_current t05_fresh || exit $?
    echo "TICKET05_GUARD_MUTATION → PASS (${trigger}): ${test_name##*.} fails without it"
    guard_mutations=$((guard_mutations + 1))
done 3<<'GUARDS'
trg_scheduled_handoff_immutable_v1|tests.test_handoff_terminal_mysql.HandoffTerminalMySQLTests.test_scheduled_handoff_guard_is_load_bearing
trg_handoff_context_assignment_immutable_v1|tests.test_handoff_terminal_mysql.HandoffTerminalMySQLTests.test_context_assignment_guard_is_load_bearing
trg_turn_input_handoff_state_v1|tests.test_handoff_terminal_mysql.HandoffTerminalMySQLTests.test_post_handoff_membership_is_terminal
trg_turn_input_handoff_state_v1|tests.test_handoff_terminal_mysql.HandoffTerminalMySQLTests.test_membership_guard_full_status_matrix
trg_outbox_context_payload_immutable_v1|tests.test_handoff_terminal_mysql.HandoffTerminalMySQLTests.test_context_note_payload_guard_is_load_bearing
GUARDS
[[ "${guard_mutations}" == 5 ]] || { echo "TICKET05_GUARD_MUTATION → FAIL: ${guard_mutations}/5" >&2; exit 1; }
[[ "$(ticket05_shape t05_fresh)" == "${fresh_shape}" ]] || exit 1

python "${repo_root}/scripts/check_trigger_body_digests.py" --self-test \
    "${repo_root}/db/init.sql" | tail -n 1 || exit $?

# Unmutated control after all guard restorations.
run_tests "${repo_root}" tests.test_handoff_terminal_unit tests.test_handoff_terminal_mysql \
    >"${workdir}/control.log" 2>&1 || { tail -n 40 "${workdir}/control.log" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${workdir}/control.log" || exit 1
echo "TICKET05_CONTROL → PASS: unmutated targeted suite green after restorations"

PYTHONPYCACHEPREFIX="${workdir}/pycache" python -m py_compile \
    "${repo_root}/app/src/agents/agent_runner.py" \
    "${repo_root}/app/src/database/models.py" \
    "${repo_root}/app/src/database/repositories/turn_handoff_repo.py" \
    "${repo_root}/app/src/database/repositories/handoff_terminal_repo.py" \
    "${repo_root}/app/src/database/repositories/delivery_worker_repo.py" \
    "${repo_root}/app/src/database/repositories/scheduler_repo.py" \
    "${repo_root}/app/src/services/handoff_context.py" \
    "${repo_root}/app/src/services/turn_handoff.py" \
    "${repo_root}/app/src/services/delivery_adapters.py" \
    "${repo_root}/app/src/delivery_worker.py" \
    "${repo_root}/app/src/delivery_cli.py" \
    "${repo_root}/app/tests/test_handoff_terminal_unit.py" \
    "${repo_root}/app/tests/test_handoff_terminal_mysql.py" \
    "${repo_root}/scripts/inbox_ticket05_mutations.py" || exit $?
bash -n "${repo_root}/scripts/run_inbox_ticket05_mysql_matrix.sh" || exit $?
echo "TICKET05_MATRIX → PASS: full=${expected_full} targeted=${expected_targeted} migrations=${migration_cases} interruptions=${interruption_cases} schema=${shape_cases}+1 mutations=${mutations} guards=${guard_mutations}"
