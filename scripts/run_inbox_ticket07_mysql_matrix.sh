#!/usr/bin/env bash
set -uo pipefail

# Ticket 07 matrix: every ticket 02–06 gate (through the ticket 06 matrix,
# which chains 05 → 04 → 03 → 02 with full zero-skip discovery), then the
# ticket07 lifecycle guard: fresh/repeat install, populated upgrades from
# a961a1e (ticket06 merge), 1a7e1ed, f59c32c, d52a495 and 35f9c07, both
# interruption sides of the guard boundary, a tampered guard that must fail
# closed, the real-MySQL lifecycle suite as a DML-only user, fix-removal code
# mutations and a guard drop. Synthetic credentials only; no provider, LLM or
# WhatsApp effect. Every loop reads its case list from fd 3 so docker exec
# never drains it.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${TICKET07_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-t07-}"
container="${resource_prefix}mysql-${suffix}"
network="${resource_prefix}net-${suffix}"
volume="${resource_prefix}data-${suffix}"
image="${WPP_TEST_IMAGE:-${resource_prefix}matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
workdir="$(mktemp -d)"
mutations_py="${repo_root}/scripts/inbox_ticket07_mutations.py"
suite="${repo_root}/app/tests/test_lifecycle_mysql.py"
guard="trg_conversation_turn_lifecycle_v1"
marker="ticket07-trigger-boundary: conversation-turn-lifecycle-v1-created"
previous_marker="ticket05-trigger-boundary: outbox-context-payload-v1-created"
expected_targeted=56
expected_mutations=60
expected_upgrades=5
expected_interruptions=4
expected_tampers=2
expected_guards=12
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
    # FU-2: retain raw case/mutation logs when the integration harness asks.
    if [[ -n "${WPP_TEST_LOG_ROOT:-}" ]]; then
        mkdir -p "${WPP_TEST_LOG_ROOT}/ticket07-${suffix}"
        find "${workdir}" -maxdepth 1 -name '*.log' -exec cp {} "${WPP_TEST_LOG_ROOT}/ticket07-${suffix}/" \;
    fi
    rm -rf "${workdir}"
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then docker image rm -f "${image}" >/dev/null 2>&1 || true; fi
    if [[ "$(resource_count)" != 0 ]]; then
        echo "TICKET07_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "TICKET07_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

# Declared counts must match the source (a lost or unregistered case fails).
declared="$(grep -cE '^    (async )?def test_' "${suite}")"
[[ "${declared}" == "${expected_targeted}" ]] \
    || { echo "TICKET07_CASE_COUNT → FAIL: declared=${declared} expected=${expected_targeted}" >&2; exit 1; }
[[ "$(python "${mutations_py}" count)" == "${expected_mutations}" ]] \
    || { echo "TICKET07_CASE_COUNT → FAIL: mutation list size" >&2; exit 1; }
python "${mutations_py}" check || exit $?
[[ "$(grep -cF -- "${marker}" "${repo_root}/db/init.sql")" == 1 ]] \
    || { echo "TICKET07_CASE_COUNT → FAIL: guard boundary marker" >&2; exit 1; }

export WPP_TEST_RESOURCE_PREFIX="${resource_prefix}"

# ── 1. Every predecessor gate (ticket 02–06) incl. full discovery ───────
prior_log="${workdir}/ticket06.log"
"${repo_root}/scripts/run_inbox_ticket06_mysql_matrix.sh" >"${prior_log}" 2>&1
prior_status=$?
grep -E '→ (PASS|FAIL)|^Ran [0-9]+ tests?|^OK|^FAILED' "${prior_log}"
(( prior_status == 0 )) || { tail -n 80 "${prior_log}" >&2; exit "${prior_status}"; }
grep -q '^TICKET06_MATRIX → PASS' "${prior_log}" || exit 1
echo "TICKET07_PREDECESSORS → PASS: ticket02/03/04/05/06 gates (full discovery asserted by ticket05)"

if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?

for ref in a961a1e 1a7e1ed f59c32c d52a495 35f9c07; do
    git -C "${repo_root}" show "${ref}:db/init.sql" > "${workdir}/${ref}.sql" || exit $?
done

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
apply_file() {
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket --default-character-set="${sql_charset:-latin1}" "$1" < "$2"
}
apply_current() { apply_file "$1" "${repo_root}/db/init.sql"; }
create_db() {
    mysql_root --execute="CREATE DATABASE $1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" </dev/null
}
query() {
    mysql_root --batch --skip-column-names "$1" </dev/null --execute="SET SESSION group_concat_max_len=4194304; $2"
}

# Exact guard set: every versioned guard with its body digest and timing.
guard_shape() {
    query "$1" "
SELECT CONCAT_WS('#',
 (SELECT GROUP_CONCAT(CONCAT_WS(':',event_object_table,trigger_name,action_timing,
          event_manipulation,action_orientation,SHA2(action_statement,256))
          ORDER BY trigger_name)
  FROM information_schema.triggers WHERE trigger_schema=DATABASE()),
 (SELECT SHA2(GROUP_CONCAT(CONCAT_WS(':',constraint_name,check_clause) ORDER BY constraint_name),256)
  FROM information_schema.check_constraints WHERE constraint_schema=DATABASE()),
 (SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE()
    AND trigger_name LIKE 'trg\\_%\\_v_'));"
}
expected_digest="$(python - "${repo_root}/db/init.sql" <<'PY'
import re, sys
sql = open(sys.argv[1], "rb").read()
match = re.search(rb"'trg_conversation_turn_lifecycle_v1',\s*'([0-9a-f]{64})'", sql)
print(match.group(1).decode())
PY
)"
row_state() {
    query "$1" "
SELECT CONCAT_WS('|',
 (SELECT COUNT(*) FROM conversation_turns),
 (SELECT COUNT(*) FROM turn_revisions),
 (SELECT COUNT(*) FROM outbox_deliveries),
 (SELECT IFNULL(GROUP_CONCAT(CONCAT(status,':',session_slot) ORDER BY id),'-')
    FROM conversation_turns));"
}
# The guard denies leaving a terminal state on a real (fixture) turn.
guard_probe() {
    local db=$1 turn
    turn="$(query "${db}" "SELECT MIN(id) FROM conversation_turns")"
    [[ -n "${turn}" && "${turn}" != NULL ]] || { echo "no fixture turn in ${db}" >&2; return 1; }
    mysql_root "${db}" </dev/null --execute="UPDATE conversation_turns SET status='cancelled',
      session_slot='terminal', terminal_at=UTC_TIMESTAMP(6), terminal_reason='probe'
      WHERE id=${turn}" || return 1
    if mysql_root "${db}" </dev/null --execute="UPDATE conversation_turns SET status='pending',
      session_slot='active', terminal_at=NULL, terminal_reason=NULL WHERE id=${turn}" \
        >"${workdir}/probe.log" 2>&1; then
        echo "guard accepted a terminal revival in ${db}" >&2; return 1
    fi
    grep -q 'conversation turn lifecycle transition is not allowed' "${workdir}/probe.log"
}

charset_cases=0
for sql_charset in latin1 utf8mb4 utf8mb3; do
echo "TICKET07_CHARSET_BEGIN ${sql_charset}"
# ── 2. Fresh + repeated install ─────────────────────────────────────────
create_db t07_fresh || exit $?
apply_current t07_fresh || exit $?
apply_current t07_fresh || exit $?
fresh_shape="$(guard_shape t07_fresh)" || exit $?
grep -q "conversation_turns:${guard}:BEFORE:UPDATE:ROW:${expected_digest}" <<<"${fresh_shape}" \
    || { echo "TICKET07_FRESH → FAIL: guard shape" >&2; exit 1; }
[[ "${fresh_shape##*#}" == "${expected_guards}" ]] \
    || { echo "TICKET07_FRESH → FAIL: versioned guards ${fresh_shape##*#}/${expected_guards}" >&2; exit 1; }
echo "TICKET07_FRESH → PASS: clean + repeated install, ${expected_guards} versioned guards, exact digest"

# ── 3. Populated upgrades (repeated) ────────────────────────────────────
upgrades=0
while read -r -u 3 ref fixture; do
    [[ -n "${ref}" ]] || continue
    db="t07_up_${ref}"
    create_db "${db}" || exit $?
    sql_charset=latin1 apply_file "${db}" "${workdir}/${ref}.sql" || exit $?
    apply_file "${db}" "${repo_root}/scripts/fixtures/${fixture}" || exit $?
    before="$(row_state "${db}")" || exit $?
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(guard_shape "${db}")" == "${fresh_shape}" ]] \
        || { echo "TICKET07_UPGRADE → FAIL (${ref}): guard shape" >&2; exit 1; }
    [[ "$(row_state "${db}")" == "${before}" ]] \
        || { echo "TICKET07_UPGRADE → FAIL (${ref}): rows changed" >&2; exit 1; }
    guard_probe "${db}" || { echo "TICKET07_UPGRADE → FAIL (${ref}): guard probe" >&2; exit 1; }
    echo "TICKET07_UPGRADE → PASS (${ref} + ${fixture} → current ×2): guard exact, rows kept (${before}), terminal revival denied"
    upgrades=$((upgrades + 1))
done 3<<'REFS'
a961a1e ticket07_populate.sql
1a7e1ed ticket06_populate.sql
f59c32c ticket05_populate.sql
d52a495 ticket05_populate.sql
35f9c07 ticket05_populate.sql
REFS
[[ "${upgrades}" == "${expected_upgrades}" ]] \
    || { echo "TICKET07_UPGRADE → FAIL: ${upgrades}/${expected_upgrades}" >&2; exit 1; }

# ── 4. Interruption on both sides of the guard boundary ─────────────────
interruptions=0
while IFS='|' read -r -u 3 label cut expected_count; do
    [[ -n "${label}" ]] || continue
    db="t07_int_${interruptions}"
    variant="${workdir}/interrupt-${interruptions}.sql"
    awk -v marker="${cut}" '{print} index($0, marker) {found=1; exit} END {if (!found) exit 65}' \
        "${repo_root}/db/init.sql" > "${variant}" || { echo "missing marker ${cut}" >&2; exit 1; }
    create_db "${db}" || exit $?
    sql_charset=latin1 apply_file "${db}" "${workdir}/a961a1e.sql" || exit $?
    apply_file "${db}" "${repo_root}/scripts/fixtures/ticket07_populate.sql" || exit $?
    before="$(row_state "${db}")" || exit $?
    apply_file "${db}" "${variant}" || exit $?
    [[ "$(query "${db}" "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE() AND trigger_name='${guard}'")" == "${expected_count}" ]] \
        || { echo "TICKET07_INTERRUPTION → FAIL (${label}): guard count" >&2; exit 1; }
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(guard_shape "${db}")" == "${fresh_shape}" ]] \
        || { echo "TICKET07_INTERRUPTION → FAIL (${label}): shape" >&2; exit 1; }
    [[ "$(row_state "${db}")" == "${before}" ]] \
        || { echo "TICKET07_INTERRUPTION → FAIL (${label}): rows" >&2; exit 1; }
    echo "TICKET07_INTERRUPTION → PASS (${label}): converged, rows kept"
    interruptions=$((interruptions + 1))
done 3<<MARKERS
before-guard|${previous_marker}|0
after-guard|${marker}|1
before-ledger-index|ticket07-schema-boundary: before-consumption-link|0
after-ledger-index|ticket07-schema-boundary: consumption-link-index-created|0
MARKERS
[[ "${interruptions}" == "${expected_interruptions}" ]] \
    || { echo "TICKET07_INTERRUPTION → FAIL: ${interruptions}/${expected_interruptions}" >&2; exit 1; }

# ── 5. Tampered guard fails closed, never silently replaced ─────────────
tampers=0
while IFS='|' read -r -u 3 label body; do
    [[ -n "${label}" ]] || continue
    db="t07_tamper_${tampers}"
    create_db "${db}" || exit $?
    apply_current "${db}" || exit $?
    # mysql's statement parser needs a delimiter around compound trigger bodies.
    printf 'DROP TRIGGER %s;\nDELIMITER $$\nCREATE TRIGGER %s BEFORE UPDATE ON conversation_turns FOR EACH ROW %s$$\nDELIMITER ;\n' \
        "${guard}" "${guard}" "${body}" > "${workdir}/tampered-guard.sql"
    mysql_root "${db}" < "${workdir}/tampered-guard.sql" || exit $?
    tampered="$(query "${db}" "SELECT SHA2(action_statement,256) FROM information_schema.triggers WHERE trigger_schema=DATABASE() AND trigger_name='${guard}'")"
    if apply_current "${db}" >"${workdir}/tamper.log" 2>&1; then
        echo "TICKET07_TAMPER → FAIL (${label}): tampered guard accepted" >&2; exit 1
    fi
    grep -q 'immutability guard definition requires audited repair' "${workdir}/tamper.log" \
        || { cat "${workdir}/tamper.log" >&2; exit 1; }
    [[ "$(query "${db}" "SELECT SHA2(action_statement,256) FROM information_schema.triggers WHERE trigger_schema=DATABASE() AND trigger_name='${guard}'")" == "${tampered}" ]] \
        || { echo "TICKET07_TAMPER → FAIL (${label}): guard rewritten" >&2; exit 1; }
    echo "TICKET07_TAMPER → PASS (${label}): named failure, nothing rewritten"
    tampers=$((tampers + 1))
done 3<<'TAMPERS'
weakened-terminal|BEGIN IF OLD.session_slot = 'successor' AND NEW.session_slot = 'terminal' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'conversation turn lifecycle transition is not allowed'; END IF; END
whitespace-only|BEGIN  IF NOT ((OLD.session_slot = 'terminal' AND NEW.session_slot = 'terminal' AND NEW.status = OLD.status AND NEW.terminal_reason <=> OLD.terminal_reason AND NEW.terminal_at <=> OLD.terminal_at) OR (OLD.session_slot = 'active' AND NEW.session_slot IN ('active','terminal')) OR (OLD.session_slot = 'successor' AND NEW.session_slot IN ('successor','active','terminal'))) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'conversation turn lifecycle transition is not allowed'; END IF; END
TAMPERS
[[ "${tampers}" == "${expected_tampers}" ]] \
    || { echo "TICKET07_TAMPER → FAIL: ${tampers}/${expected_tampers}" >&2; exit 1; }

create_db t07_duplicate_consumption || exit $?
sql_charset=latin1 apply_file t07_duplicate_consumption "${workdir}/a961a1e.sql" || exit $?
apply_file t07_duplicate_consumption "${repo_root}/scripts/fixtures/ticket07_populate.sql" || exit $?
apply_file t07_duplicate_consumption "${repo_root}/scripts/fixtures/ticket07_duplicate_consumption.sql" || exit $?
if apply_current t07_duplicate_consumption >"${workdir}/duplicate-link.log" 2>&1; then
    echo "TICKET07_DUPLICATE_LINK → FAIL" >&2; exit 1
fi
grep -q 'generation consumption link reuse requires audited repair' "${workdir}/duplicate-link.log" || exit 1
[[ "$(query t07_duplicate_consumption 'SELECT COUNT(*) FROM generation_attempts WHERE consumo_llm_id IS NOT NULL')" == 2 ]] || exit 1
echo "TICKET07_DUPLICATE_LINK → PASS (${sql_charset}): refused; both rows preserved"

index_tampers=0
while IFS='|' read -r -u 3 label definition; do
    db="t07_idx_${index_tampers}"
    create_db "${db}" || exit $?
    apply_current "${db}" || exit $?
    mysql_root "${db}" --execute="CREATE INDEX t07_fk_support ON generation_attempts(consumo_llm_id); DROP INDEX uq_generation_consumo_link ON generation_attempts; ${definition};" </dev/null || exit $?
    if apply_current "${db}" >"${workdir}/index-tamper.log" 2>&1; then
        echo "TICKET07_INDEX_SHAPE → FAIL (${label})" >&2; exit 1
    fi
    grep -q 'generation consumption link index shape requires audited repair' "${workdir}/index-tamper.log" || exit 1
    echo "TICKET07_INDEX_SHAPE → PASS (${sql_charset}/${label}): named refusal"
    index_tampers=$((index_tampers + 1))
done 3<<'INDEX_SHAPES'
nonunique|CREATE INDEX uq_generation_consumo_link ON generation_attempts(consumo_llm_id)
wrong-column|CREATE UNIQUE INDEX uq_generation_consumo_link ON generation_attempts(attempt_key)
descending|CREATE UNIQUE INDEX uq_generation_consumo_link ON generation_attempts(consumo_llm_id DESC)
invisible|CREATE UNIQUE INDEX uq_generation_consumo_link ON generation_attempts(consumo_llm_id) INVISIBLE
INDEX_SHAPES
[[ "${index_tampers}" == 4 ]] || exit 1

charset_cases=$((charset_cases + 1))
echo "TICKET07_CHARSET → PASS: ${sql_charset} fresh=1 upgrades=${upgrades} interruptions=${interruptions} tampers=${tampers}"
if [[ "${sql_charset}" != utf8mb3 ]]; then
    while read -r -u 3 old_db; do
        mysql_root --execute="DROP DATABASE ${old_db}" </dev/null || exit $?
    done 3< <(mysql_root --batch --skip-column-names --execute="SHOW DATABASES LIKE 't07\_%'" </dev/null)
fi
done
[[ "${charset_cases}" == 3 ]] || exit 1

# Charset regression mutation: implicit CHECK literals must fail under utf8mb4.
create_db t07_charset_mutation || exit $?
sed "s/_latin1'/'/g" "${repo_root}/db/init.sql" > "${workdir}/charset-mutated.sql"
if sql_charset=utf8mb4 apply_file t07_charset_mutation "${workdir}/charset-mutated.sql" >"${workdir}/charset-mutation.log" 2>&1; then
    echo "TICKET07_CHARSET_MUTATION → FAIL: implicit charset accepted" >&2; exit 1
fi
grep -q 'ticket05 handoff check shape requires audited repair' "${workdir}/charset-mutation.log" || exit 1
echo "TICKET07_CHARSET_MUTATION → PASS: implicit literals rejected under utf8mb4"

# ── 6. Targeted lifecycle suite as a DML-only user ──────────────────────
mysql_root </dev/null --execute="CREATE USER 'ticket07_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT,INSERT,UPDATE,DELETE ON t07_fresh.* TO 'ticket07_app'@'%';" || exit $?

run_tests() {
    local root=$1; shift
    docker run --rm --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "T07_ALLOW_ABSENT_LEASE=${T07_ALLOW_ABSENT_LEASE:-0}" \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://ticket07_app:${app_password}@127.0.0.1:3306/t07_fresh" \
        --env "DATABASE_URL=mysql+aiomysql://ticket07_app:${app_password}@127.0.0.1:3306/t07_fresh" \
        --env OPENROUTER_API_KEY=synthetic-ticket07-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 1500 python -m unittest "$@" </dev/null
}

target_log="${workdir}/target.log"
run_tests "${repo_root}" tests.test_lifecycle_mysql >"${target_log}" 2>&1 \
    || { tail -n 60 "${target_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${target_log}" && grep -qE '^OK$' "${target_log}" \
    || { tail -n 20 "${target_log}" >&2; echo "TICKET07_CASE_COUNT → FAIL: targeted" >&2; exit 1; }
echo "TICKET07_TARGETED → PASS: ${expected_targeted}/${expected_targeted} (zero skip, DML-only user)"

# Robustness for an absent lease: impossible under the enforced production
# tuple CHECK. Relax only that CHECK in this disposable fixture, then restore.
mysql_root t07_fresh --execute="ALTER TABLE outbox_deliveries ALTER CHECK ck_outbox_lease_tuple NOT ENFORCED" </dev/null || exit $?
T07_ALLOW_ABSENT_LEASE=1 run_tests "${repo_root}" tests.test_lifecycle_mysql.LifecycleMySQLTests.test_processing_cancel_matrix >"${workdir}/absent-lease.log" 2>&1     || { tail -40 "${workdir}/absent-lease.log" >&2; exit 1; }
grep -qE '^Ran 1 tests?' "${workdir}/absent-lease.log" && grep -q '^OK$' "${workdir}/absent-lease.log" || exit 1
mysql_root t07_fresh --execute="ALTER TABLE outbox_deliveries ALTER CHECK ck_outbox_lease_tuple ENFORCED" </dev/null || exit $?
echo "TICKET07_ABSENT_LEASE → PASS: 24 cases; production CHECK restored"

# ── 7. Fix-removal code mutations (each must fail against the DB) ───────
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
    if run_tests "${copy}" "${test_name}" >"${log}" 2>&1; then
        echo "TICKET07_MUTATION → FAIL (${name}): ${test_name##*.} passed without the fix" >&2
        exit 1
    fi
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "TICKET07_MUTATION → FAIL (${name}): harness did not run the test" >&2; exit 1; }
    echo "TICKET07_MUTATION → PASS (${name}): ${test_name##*.} fails without the fix"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${mutations_py}" list)
[[ "${mutations}" == "${expected_mutations}" ]] \
    || { echo "TICKET07_MUTATION → FAIL: ${mutations}/${expected_mutations}" >&2; exit 1; }

# Unique ledger fence, independently of repository-level refusal.
mysql_root t07_fresh </dev/null --execute="CREATE INDEX t07_consumo_fk_support ON generation_attempts(consumo_llm_id); ALTER TABLE generation_attempts DROP INDEX uq_generation_consumo_link;" || exit $?
if run_tests "${repo_root}" tests.test_lifecycle_mysql.LifecycleMySQLTests.test_one_ledger_row_cannot_reconcile_two_attempts >"${workdir}/ledger-index-mutation.log" 2>&1; then
    echo "TICKET07_LEDGER_INDEX_MUTATION → FAIL" >&2; exit 1
fi
grep -q '^FAILED (' "${workdir}/ledger-index-mutation.log" || exit 1
# FU-2: uniqueness is now decided by the index, not a speculative pre-read.
# Its removal deliberately commits both links in this disposable fixture.
# Prove that exact failure, then reset the owned fixture before restoring
# DDL; production migration must still refuse duplicate commercial links.
grep -q '^R7B_LEDGER_REUSE True True True' "${workdir}/ledger-index-mutation.log" || exit 1
run_tests "${repo_root}" tests.test_lifecycle_mysql.LifecycleMySQLTests.test_turn_lifecycle_guard_full_status_matrix \
    >"${workdir}/ledger-fixture-reset.log" 2>&1 || { tail -40 "${workdir}/ledger-fixture-reset.log" >&2; exit 1; }
grep -q '^Ran 1 test' "${workdir}/ledger-fixture-reset.log" && grep -q '^OK$' "${workdir}/ledger-fixture-reset.log" || exit 1
apply_current t07_fresh || exit $?
mysql_root t07_fresh </dev/null --execute="DROP INDEX t07_consumo_fk_support ON generation_attempts;" || exit $?
echo "TICKET07_LEDGER_INDEX_MUTATION → PASS: unique link restored"

# ── 8. Guard drop: the lifecycle guard is load-bearing ──────────────────
guard_test="tests.test_lifecycle_mysql.LifecycleMySQLTests.test_turn_lifecycle_guard_full_status_matrix"
mysql_root t07_fresh </dev/null --execute="DROP TRIGGER ${guard};" || exit $?
if run_tests "${repo_root}" "${guard_test}" >"${workdir}/guard.log" 2>&1; then
    echo "TICKET07_GUARD_MUTATION → FAIL: matrix passed without ${guard}" >&2
    exit 1
fi
grep -qE '^Ran 1 tests?' "${workdir}/guard.log" && grep -qE '^FAILED \(' "${workdir}/guard.log" \
    || { tail -n 20 "${workdir}/guard.log" >&2; exit 1; }
apply_current t07_fresh || exit $?
[[ "$(guard_shape t07_fresh)" == "${fresh_shape}" ]] || exit 1
echo "TICKET07_GUARD_MUTATION → PASS (${guard}): restored to exact shape"

# Unmutated control after the guard restoration.
run_tests "${repo_root}" tests.test_lifecycle_mysql >"${workdir}/control.log" 2>&1 \
    || { tail -n 40 "${workdir}/control.log" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${workdir}/control.log" || exit 1
echo "TICKET07_CONTROL → PASS: unmutated lifecycle suite green after restoration"

python "${repo_root}/scripts/check_trigger_body_digests.py" --self-test \
    "${repo_root}/db/init.sql" | tail -n 1 || exit $?
PYTHONPYCACHEPREFIX="${workdir}/pycache" python -m py_compile \
    "${repo_root}/app/src/database/repositories/lifecycle_repo.py" \
    "${repo_root}/app/src/database/repositories/scheduler_repo.py" \
    "${repo_root}/app/src/database/repositories/turn_delivery_repo.py" \
    "${repo_root}/app/src/database/repositories/turn_generation_repo.py" \
    "${repo_root}/app/src/database/repositories/delivery_worker_repo.py" \
    "${repo_root}/app/src/routers/sessoes.py" \
    "${repo_root}/app/src/routers/agentes.py" \
    "${repo_root}/app/src/delivery_cli.py" \
    "${repo_root}/app/src/services/turn_processor.py" \
    "${suite}" "${mutations_py}" || exit $?
bash -n "${repo_root}/scripts/run_inbox_ticket07_mysql_matrix.sh" || exit $?
echo "TICKET07_MATRIX → PASS: targeted=${expected_targeted} upgrades=${expected_upgrades} interruptions=${expected_interruptions} tampers=${expected_tampers} mutations=${expected_mutations} guards=1"
