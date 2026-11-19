#!/usr/bin/env bash
set -uo pipefail

# T08-3 matrix: replica heartbeats, fleet compatibility readiness and the
# scheduler enabling->enabled gate. Migration (per client charset latin1,
# utf8mb4, utf8mb3): fresh + repeat, populated upgrades from every released
# predecessor (ticket07 refs a961a1e 1a7e1ed f59c32c d52a495 35f9c07 plus
# 148dd57 and 52c6aac), interruption at every DDL boundary, exact-shape
# tampers that must fail closed without rewrite, a newer schema marker that is
# never lowered and migration-validator removals that must let the tamper
# through. Then the targeted real-MySQL suite as a DML-only user, repeated
# barrier tests, every code mutation (each must FAIL), DB-level mutations,
# an unmutated control and, unless T083_REGRESSION=0, every app test module.
# Synthetic credentials only; no provider, LLM or WhatsApp effect. Every loop
# reads its list from fd 3 so docker exec never drains it.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${T083_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-t083-}"
container="${resource_prefix}mysql-${suffix}"
network="${resource_prefix}net-${suffix}"
volume="${resource_prefix}data-${suffix}"
image="${WPP_TEST_IMAGE:-${resource_prefix}matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
if [[ -n "${WPP_TEST_LOG_ROOT:-}" ]]; then
    workdir="${WPP_TEST_LOG_ROOT%/}/t083-${suffix}"
    mkdir -p "${workdir}"
    retain_workdir=1
else
    workdir="$(mktemp -d)"
    retain_workdir=0
fi
mutations_py="${repo_root}/scripts/inbox_t083_mutations.py"
suite="${repo_root}/app/tests/test_replica_compat_mysql.py"
module="tests.test_replica_compat_mysql"
klass="${module}.ReplicaCompatMySQLTests"
expected_targeted=29
expected_mutations=63
expected_upgrades=7
expected_interruptions=4
expected_tampers=17
expected_migration_mutations=5
expected_db_mutations=4
expected_charsets=3
barrier_repeats=3
barriers=(
    test_gate_reads_committed_heartbeats_after_waiting_for_scheduler_lock
    test_gate_and_heartbeat_writer_never_wait_on_each_other
    test_legacy_retention_lock_does_not_block_gate
)
markers=(
    "t083-schema-boundary: replica-heartbeats-created"
    "t083-schema-boundary: replica-heartbeats-validated"
    "t083-schema-boundary: schema-version-recorded"
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
        echo "T083_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "T083_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
        (( retain_workdir == 0 )) || echo "T083_LOGS → RETAINED: ${workdir}"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

# ── 0. Declared counts must match the source ────────────────────────────
declared="$(grep -cE '^    (async )?def test_' "${suite}")"
[[ "${declared}" == "${expected_targeted}" ]] \
    || { echo "T083_CASE_COUNT → FAIL: declared=${declared} expected=${expected_targeted}" >&2; exit 1; }
[[ "$(python "${mutations_py}" count)" == "${expected_mutations}" ]] \
    || { echo "T083_CASE_COUNT → FAIL: mutation list size" >&2; exit 1; }
python "${mutations_py}" check || exit $?
for marker in "${markers[@]}"; do
    [[ "$(grep -cF -- "${marker}" "${repo_root}/db/init.sql")" == 1 ]] \
        || { echo "T083_CASE_COUNT → FAIL: marker ${marker}" >&2; exit 1; }
done
for name in "${barriers[@]}"; do
    grep -qE "^    async def ${name}\(" "${suite}" \
        || { echo "T083_CASE_COUNT → FAIL: barrier ${name} missing" >&2; exit 1; }
done

if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?
refs=(a961a1e 1a7e1ed f59c32c d52a495 35f9c07 148dd57 52c6aac)
for ref in "${refs[@]}"; do
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
replica_shape() {
    query "$1" "
SELECT CONCAT_WS('#',
 (SELECT GROUP_CONCAT(CONCAT_WS('|',ordinal_position,column_name,column_type,is_nullable,
     COALESCE(column_default,'<NULL>'),COALESCE(character_set_name,'<NULL>'),
     COALESCE(collation_name,'<NULL>'),extra) ORDER BY ordinal_position SEPARATOR ';')
  FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='replica_heartbeats'),
 (SELECT GROUP_CONCAT(CONCAT_WS('|',index_name,non_unique,seq_in_index,column_name,is_visible)
     ORDER BY index_name,seq_in_index SEPARATOR ';')
  FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='replica_heartbeats'),
 (SELECT GROUP_CONCAT(CONCAT_WS('|',tc.constraint_name,tc.enforced,SHA2(cc.check_clause,256))
     ORDER BY tc.constraint_name SEPARATOR ';')
  FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc
    ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name
  WHERE tc.constraint_schema=DATABASE() AND tc.table_name='replica_heartbeats'));"
}
# Every trigger body + every pre-T08-3 CHECK digest (ticket07 shape); the
# replica table has its own exact shape above.
guard_shape() {
    query "$1" "
SELECT CONCAT_WS('#',
 (SELECT GROUP_CONCAT(CONCAT_WS(':',event_object_table,trigger_name,action_timing,
          event_manipulation,action_orientation,SHA2(action_statement,256))
          ORDER BY trigger_name)
  FROM information_schema.triggers WHERE trigger_schema=DATABASE()),
 (SELECT SHA2(GROUP_CONCAT(CONCAT_WS(':',constraint_name,check_clause) ORDER BY constraint_name),256)
  FROM information_schema.check_constraints WHERE constraint_schema=DATABASE()
   AND constraint_name NOT LIKE 'ck\\_replica\\_heartbeat\\_%'));"
}
row_state() {
    query "$1" "
SELECT CONCAT_WS('|',
 (SELECT COUNT(*) FROM agentes), (SELECT COUNT(*) FROM sessoes),
 (SELECT COUNT(*) FROM conversation_turns), (SELECT COUNT(*) FROM turn_revisions),
 (SELECT COUNT(*) FROM outbox_deliveries),
 (SELECT IFNULL(SHA2(GROUP_CONCAT(CONCAT_WS(',',worker_id,code_version,started_at,last_seen_at)
     ORDER BY worker_id),256),'-') FROM delivery_worker_heartbeats),
 (SELECT IFNULL(SHA2(GROUP_CONCAT(CONCAT_WS(',',id,action,actor,reason) ORDER BY id),256),'-')
    FROM delivery_operator_actions),
 (SELECT CONCAT_WS(',',state,scheduler_epoch,version) FROM scheduler_runtime_state),
 (SELECT CONCAT_WS(',',mode,cutover_epoch,version) FROM delivery_runtime_state));"
}
schema_version() { query "$1" "SELECT schema_version FROM scheduler_runtime_state WHERE id=1"; }
table_count() {
    query "$1" "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='replica_heartbeats'"
}

# Tamper DDL: the exact CREATE TABLE from db/init.sql with one divergence.
python - "${repo_root}/db/init.sql" "${workdir}" <<'PY' || exit $?
import pathlib, sys
sql = pathlib.Path(sys.argv[1]).read_text()
start = sql.index("CREATE TABLE IF NOT EXISTS replica_heartbeats (")
ddl = sql[start:sql.index(");\n", start) + 3]
cases = {
    "col-type": ("code_version VARCHAR(100)", "code_version VARCHAR(99)"),
    "col-null": ("schema_version INT UNSIGNED NOT NULL", "schema_version INT UNSIGNED NULL"),
    "col-signed": ("schema_version INT UNSIGNED NOT NULL", "schema_version INT NOT NULL"),
    "col-default": ("webhook_auth_mode VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
                    "webhook_auth_mode VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'enforced'"),
    "col-collation": ("replica_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL",
                      "replica_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL"),
    "col-order": ("    scheduler_enabled BOOLEAN NOT NULL,\n    scheduler_epoch",
                  "    scheduler_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,\n    scheduler_enabled BOOLEAN NOT NULL,\n    t083_dropme"),
    "col-extra": ("    stopped_at DATETIME(6) NULL,", "    stopped_at DATETIME(6) NULL,\n    note VARCHAR(10) NULL,"),
    "col-missing": ("    stopped_at DATETIME(6) NULL,\n", ""),
    "idx-missing": ("    INDEX idx_replica_heartbeats_last_seen (last_seen_at)", "    INDEX t083_other (started_at)"),
    "idx-unique": ("    INDEX idx_replica_heartbeats_last_seen (last_seen_at)",
                   "    UNIQUE INDEX idx_replica_heartbeats_last_seen (last_seen_at)"),
    "idx-invisible": ("    INDEX idx_replica_heartbeats_last_seen (last_seen_at)",
                      "    INDEX idx_replica_heartbeats_last_seen (last_seen_at) INVISIBLE"),
    "idx-wrong-column": ("    INDEX idx_replica_heartbeats_last_seen (last_seen_at)",
                         "    INDEX idx_replica_heartbeats_last_seen (started_at)"),
    "ck-missing": ("    CONSTRAINT ck_replica_heartbeat_kind CHECK (\n        replica_kind IN (_ascii'app',_ascii'delivery_worker',_ascii'turn_worker')\n    ),\n", ""),
    "ck-widened": ("delivery_mode IN (_ascii'direct',_ascii'outbox')",
                   "delivery_mode IN (_ascii'direct',_ascii'outbox',_ascii'frozen')"),
    "ck-not-enforced": ("        replica_kind IN (_ascii'app',_ascii'delivery_worker',_ascii'turn_worker')\n    ),",
                        "        replica_kind IN (_ascii'app',_ascii'delivery_worker',_ascii'turn_worker')\n    ) NOT ENFORCED,"),
    "dep-fk": ("", ""),
    "dep-trigger": ("", ""),
}
for name, (old, new) in cases.items():
    if old:
        assert ddl.count(old) == 1, name
        text = ddl.replace(old, new, 1)
    else:
        text = ddl
    if name == "col-order":
        # the moved column replaces the original scheduler_epoch line
        text = text.replace("    t083_dropme CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,\n", "", 1)
        assert "t083_dropme" not in text, name
    if name == "dep-fk":
        text += ("\nCREATE TABLE t083_ref (replica_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin,"
                 " CONSTRAINT fk_t083_ref FOREIGN KEY (replica_id) REFERENCES replica_heartbeats(replica_id));\n")
    if name == "dep-trigger":
        text += ("\nCREATE TRIGGER t083_trg BEFORE UPDATE ON replica_heartbeats FOR EACH ROW SET NEW.code_version = NEW.code_version;\n")
    assert text != ddl or name.startswith("dep-"), name
    (pathlib.Path(sys.argv[2]) / f"tamper-{name}.sql").write_text(text)
PY

charset_cases=0
for sql_charset in latin1 utf8mb4 utf8mb3; do
echo "T083_CHARSET_BEGIN ${sql_charset}"
# ── 1. Fresh + repeated install ─────────────────────────────────────────
create_db t083_fresh || exit $?
apply_current t083_fresh || exit $?
apply_current t083_fresh || exit $?
fresh_replica="$(replica_shape t083_fresh)" || exit $?
fresh_guards="$(guard_shape t083_fresh)" || exit $?
[[ -n "${fresh_replica}" && "$(schema_version t083_fresh)" == 3 \
   && "$(query t083_fresh 'SELECT COUNT(*) FROM replica_heartbeats')" == 0 \
   && "$(query t083_fresh "SELECT state FROM scheduler_runtime_state")" == disabled ]] \
    || { echo "T083_FRESH → FAIL (${sql_charset})" >&2; exit 1; }
if [[ -n "${latin1_replica:-}" ]]; then
    [[ "${fresh_replica}" == "${latin1_replica}" && "${fresh_guards}" == "${latin1_guards}" ]] \
        || { echo "T083_FRESH → FAIL (${sql_charset}): shape differs from latin1" >&2; exit 1; }
else
    latin1_replica="${fresh_replica}"; latin1_guards="${fresh_guards}"
fi
# A later ALTER TABLE re-serializes CHECK clauses; the _ascii form is a fixed
# point, so the next migration still accepts the exact shape.
mysql_root t083_fresh --execute="ALTER TABLE replica_heartbeats ADD COLUMN t083_tmp INT NULL; ALTER TABLE replica_heartbeats DROP COLUMN t083_tmp" </dev/null || exit $?
apply_current t083_fresh || { echo "T083_FRESH → FAIL (${sql_charset}): digest unstable after ALTER" >&2; exit 1; }
[[ "$(replica_shape t083_fresh)" == "${fresh_replica}" ]] || { echo "T083_FRESH → FAIL (${sql_charset}): shape after ALTER" >&2; exit 1; }
echo "T083_FRESH → PASS (${sql_charset}): clean + repeated install, schema_version=3, scheduler disabled, client-charset independent shape, CHECK digest stable after ALTER"

# ── 2. Populated upgrades from every predecessor (repeated) ─────────────
upgrades=0
while read -r -u 3 ref fixture; do
    [[ -n "${ref}" ]] || continue
    db="t083_up_${ref}"
    create_db "${db}" || exit $?
    sql_charset=latin1 apply_file "${db}" "${workdir}/${ref}.sql" || exit $?
    apply_file "${db}" "${repo_root}/scripts/fixtures/${fixture}" || exit $?
    apply_file "${db}" "${repo_root}/scripts/fixtures/t083_legacy_heartbeats.sql" || exit $?
    [[ "$(table_count "${db}")" == 0 && "$(schema_version "${db}")" == 1 ]] \
        || { echo "T083_UPGRADE → FAIL (${ref}): predecessor already has T08-3 state" >&2; exit 1; }
    before="$(row_state "${db}")" || exit $?
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(replica_shape "${db}")" == "${fresh_replica}" ]] \
        || { echo "T083_UPGRADE → FAIL (${ref}): replica shape" >&2; exit 1; }
    [[ "$(guard_shape "${db}")" == "${fresh_guards}" ]] \
        || { echo "T083_UPGRADE → FAIL (${ref}): guard shape" >&2; exit 1; }
    [[ "$(row_state "${db}")" == "${before}" ]] \
        || { echo "T083_UPGRADE → FAIL (${ref}): rows changed" >&2; exit 1; }
    [[ "$(schema_version "${db}")" == 3 && "$(query "${db}" 'SELECT COUNT(*) FROM replica_heartbeats')" == 0 ]] \
        || { echo "T083_UPGRADE → FAIL (${ref}): schema marker / replica rows" >&2; exit 1; }
    echo "T083_UPGRADE → PASS (${sql_charset}: ${ref} + ${fixture} → current ×2): exact shape, rows kept, schema_version 1→3"
    upgrades=$((upgrades + 1))
done 3<<'REFS'
a961a1e ticket07_populate.sql
1a7e1ed ticket06_populate.sql
f59c32c ticket05_populate.sql
d52a495 ticket05_populate.sql
35f9c07 ticket05_populate.sql
148dd57 ticket07_populate.sql
52c6aac ticket07_populate.sql
REFS
[[ "${upgrades}" == "${expected_upgrades}" ]] \
    || { echo "T083_UPGRADE → FAIL: ${upgrades}/${expected_upgrades}" >&2; exit 1; }

# ── 3. Interruption at every DDL boundary ───────────────────────────────
interruptions=0
while IFS='|' read -r -u 3 label cut expected_table expected_version; do
    [[ -n "${label}" ]] || continue
    db="t083_int_${interruptions}"
    variant="${workdir}/interrupt-${interruptions}.sql"
    awk -v marker="${cut}" '{print} index($0, marker) {found=1; exit} END {if (!found) exit 65}' \
        "${repo_root}/db/init.sql" > "${variant}" || { echo "missing marker ${cut}" >&2; exit 1; }
    create_db "${db}" || exit $?
    sql_charset=latin1 apply_file "${db}" "${workdir}/52c6aac.sql" || exit $?
    apply_file "${db}" "${repo_root}/scripts/fixtures/ticket07_populate.sql" || exit $?
    apply_file "${db}" "${repo_root}/scripts/fixtures/t083_legacy_heartbeats.sql" || exit $?
    before="$(row_state "${db}")" || exit $?
    apply_file "${db}" "${variant}" || exit $?
    [[ "$(table_count "${db}")" == "${expected_table}" && "$(schema_version "${db}")" == "${expected_version}" ]] \
        || { echo "T083_INTERRUPTION → FAIL (${label}): intermediate state" >&2; exit 1; }
    [[ "$(guard_shape "${db}")" == "${fresh_guards}" ]] \
        || { echo "T083_INTERRUPTION → FAIL (${label}): a guard is missing at the boundary" >&2; exit 1; }
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(replica_shape "${db}")" == "${fresh_replica}" && "$(schema_version "${db}")" == 3 ]] \
        || { echo "T083_INTERRUPTION → FAIL (${label}): did not converge" >&2; exit 1; }
    [[ "$(row_state "${db}")" == "${before}" ]] \
        || { echo "T083_INTERRUPTION → FAIL (${label}): rows" >&2; exit 1; }
    echo "T083_INTERRUPTION → PASS (${sql_charset}/${label}): table=${expected_table} schema_version=${expected_version} at cut, guards intact, converged, rows kept"
    interruptions=$((interruptions + 1))
done 3<<MARKERS
before-table|ticket04-trigger-boundary: turn-revision-draft-v1-dropped|0|1
after-table|${markers[0]}|1|1
after-validation|${markers[1]}|1|1
after-marker|${markers[2]}|1|2
MARKERS
[[ "${interruptions}" == "${expected_interruptions}" ]] \
    || { echo "T083_INTERRUPTION → FAIL: ${interruptions}/${expected_interruptions}" >&2; exit 1; }

# ── 4. Tampered shape fails closed; nothing rewritten; marker stays 1 ───
tampers=0
while IFS='|' read -r -u 3 label message; do
    [[ -n "${label}" ]] || continue
    db="t083_tamper_${tampers}"
    create_db "${db}" || exit $?
    sql_charset=latin1 apply_file "${db}" "${workdir}/52c6aac.sql" || exit $?
    apply_file "${db}" "${workdir}/tamper-${label}.sql" || exit $?
    shown="$(query "${db}" 'SHOW CREATE TABLE replica_heartbeats' | sha256sum)"
    if apply_current "${db}" >"${workdir}/tamper.log" 2>&1; then
        echo "T083_TAMPER → FAIL (${label}): divergent shape accepted" >&2; exit 1
    fi
    grep -q "t083 replica heartbeat ${message} shape requires audited repair" "${workdir}/tamper.log" \
        || { cat "${workdir}/tamper.log" >&2; exit 1; }
    [[ "$(query "${db}" 'SHOW CREATE TABLE replica_heartbeats' | sha256sum)" == "${shown}" \
       && "$(schema_version "${db}")" == 1 ]] \
        || { echo "T083_TAMPER → FAIL (${label}): rewritten or marker advanced" >&2; exit 1; }
    echo "T083_TAMPER → PASS (${sql_charset}/${label}): named ${message} refusal, table and schema_version unchanged"
    tampers=$((tampers + 1))
done 3<<'TAMPERS'
col-type|column
col-null|column
col-signed|column
col-default|column
col-collation|column
col-order|column
col-extra|column
col-missing|column
idx-missing|index
idx-unique|index
idx-invisible|index
idx-wrong-column|index
ck-missing|check
ck-widened|check
ck-not-enforced|check
dep-fk|dependency
dep-trigger|dependency
TAMPERS
[[ "${tampers}" == "${expected_tampers}" ]] \
    || { echo "T083_TAMPER → FAIL: ${tampers}/${expected_tampers}" >&2; exit 1; }

# A newer schema marker is never lowered.
create_db t083_future || exit $?
apply_current t083_future || exit $?
mysql_root t083_future --execute="UPDATE scheduler_runtime_state SET schema_version=4" </dev/null || exit $?
apply_current t083_future || exit $?
[[ "$(schema_version t083_future)" == 4 ]] || { echo "T083_FUTURE → FAIL" >&2; exit 1; }
echo "T083_FUTURE → PASS (${sql_charset}): schema_version 4 kept"

# ── 5. Migration validator removal lets the matching tamper through ─────
migration_mutations=0
while IFS='|' read -r -u 3 label tamper expect; do
    [[ -n "${label}" ]] || continue
    mutated="${workdir}/init-${label}.sql"
    python - "${repo_root}/db/init.sql" "${mutated}" "${label}" <<'PY' || exit $?
import pathlib, sys
sql = pathlib.Path(sys.argv[1]).read_text()
edits = {
    "column": ("IF actual_shape IS NULL OR actual_shape <> CONCAT(\n        'replica_id|",
               "IF FALSE AND actual_shape <> CONCAT(\n        'replica_id|"),
    "index": ("IF actual_shape IS NULL OR actual_shape <> CONCAT(\n        'idx_replica_heartbeats_last_seen|",
              "IF FALSE AND actual_shape <> CONCAT(\n        'idx_replica_heartbeats_last_seen|"),
    "check": ("IF actual_shape IS NULL OR actual_shape <> CONCAT(\n        'ck_replica_heartbeat_auth_mode|",
              "IF FALSE AND actual_shape <> CONCAT(\n        'ck_replica_heartbeat_auth_mode|"),
    "dependency": ("    IF EXISTS (\n        SELECT 1 FROM information_schema.referential_constraints\n        WHERE constraint_schema = DATABASE()\n          AND (table_name = 'replica_heartbeats'",
                   "    IF FALSE AND EXISTS (\n        SELECT 1 FROM information_schema.referential_constraints\n        WHERE constraint_schema = DATABASE()\n          AND (table_name = 'replica_heartbeats'"),
    "marker": ("UPDATE scheduler_runtime_state SET schema_version = 2\nWHERE id = 1 AND schema_version = 1$$\n", ""),
}
old, new = edits[sys.argv[3]]
assert sql.count(old) == 1, sys.argv[3]
pathlib.Path(sys.argv[2]).write_text(sql.replace(old, new, 1))
PY
    db="t083_mm_${migration_mutations}"
    create_db "${db}" || exit $?
    sql_charset=latin1 apply_file "${db}" "${workdir}/52c6aac.sql" || exit $?
    [[ "${tamper}" == none ]] || apply_file "${db}" "${workdir}/tamper-${tamper}.sql" || exit $?
    if [[ "${expect}" == accepted ]]; then
        apply_file "${db}" "${mutated}" >"${workdir}/mm.log" 2>&1 \
            || { cat "${workdir}/mm.log" >&2; echo "T083_MIGRATION_MUTATION → FAIL (${label}): tamper still refused" >&2; exit 1; }
    else
        if apply_file "${db}" "${mutated}" >"${workdir}/mm.log" 2>&1; then
            echo "T083_MIGRATION_MUTATION → FAIL (${label}): missing marker accepted" >&2; exit 1
        fi
        grep -q 't083 scheduler schema version requires audited repair' "${workdir}/mm.log" || exit 1
    fi
    echo "T083_MIGRATION_MUTATION → PASS (${sql_charset}/${label}): validator is load-bearing (${tamper} ${expect})"
    migration_mutations=$((migration_mutations + 1))
done 3<<'MIGRATION'
column|col-type|accepted
index|idx-unique|accepted
check|ck-widened|accepted
dependency|dep-fk|accepted
marker|none|refused
MIGRATION
[[ "${migration_mutations}" == "${expected_migration_mutations}" ]] \
    || { echo "T083_MIGRATION_MUTATION → FAIL: ${migration_mutations}" >&2; exit 1; }

charset_cases=$((charset_cases + 1))
echo "T083_CHARSET → PASS: ${sql_charset} fresh=1 upgrades=${upgrades} interruptions=${interruptions} tampers=${tampers} migration_mutations=${migration_mutations}"
while read -r -u 3 old_db; do
    mysql_root --execute="DROP DATABASE ${old_db}" </dev/null || exit $?
done 3< <(mysql_root --batch --skip-column-names --execute="SHOW DATABASES LIKE 't083\_%'" </dev/null)
done
[[ "${charset_cases}" == "${expected_charsets}" ]] || exit 1

# ── 6. Targeted suite as a DML-only user ────────────────────────────────
mysql_root </dev/null --execute="CREATE USER 't083_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT ON performance_schema.* TO 't083_app'@'%';" || exit $?
prepare_db() {
    mysql_root </dev/null --execute="CREATE DATABASE $1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT SELECT,INSERT,UPDATE,DELETE ON $1.* TO 't083_app'@'%';" || return $?
    mysql_root --default-character-set=latin1 "$1" < "${repo_root}/db/init.sql"
}
prepare_db t083_suite || exit $?

run_tests() {
    local root=$1 db=$2; shift 2
    # FU-3: transient test containers obey the effective resource prefix too.
    docker run --rm --name="${resource_prefix}test-${suffix}" --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://t083_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "DATABASE_URL=mysql+aiomysql://t083_app:${app_password}@127.0.0.1:3306/${db}" \
        --env "WPP_DISPOSABLE_TEST_DATABASE_MARKER=${db}" \
        --env OPENROUTER_API_KEY=synthetic-t083-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 2400 python -m unittest "$@" </dev/null
}

target_log="${workdir}/target.log"
run_tests "${repo_root}" t083_suite "${module}" >"${target_log}" 2>&1 \
    || { tail -n 60 "${target_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${target_log}" && grep -qE '^OK$' "${target_log}" \
    || { tail -n 20 "${target_log}" >&2; echo "T083_CASE_COUNT → FAIL: targeted" >&2; exit 1; }
if grep -qE 'Deadlock found|\(1213,' "${target_log}"; then
    echo "T083_DEADLOCK → FAIL: 1213 in targeted log" >&2; exit 1
fi
echo "T083_TARGETED → PASS: ${expected_targeted}/${expected_targeted} (zero skip, DML-only user, zero 1213)"

# ── 7. Barrier repetitions ──────────────────────────────────────────────
barrier_args=()
for name in "${barriers[@]}"; do barrier_args+=("${klass}.${name}"); done
for round in $(seq 1 "${barrier_repeats}"); do
    log="${workdir}/barrier-${round}.log"
    run_tests "${repo_root}" t083_suite "${barrier_args[@]}" >"${log}" 2>&1 \
        || { tail -n 40 "${log}" >&2; exit 1; }
    grep -qE "^Ran ${#barriers[@]} tests?" "${log}" && grep -qE '^OK$' "${log}" || exit 1
    grep -qE 'Deadlock found|\(1213,' "${log}" && { echo "T083_DEADLOCK → FAIL (round ${round})" >&2; exit 1; }
    echo "T083_BARRIERS → PASS: round ${round}/${barrier_repeats} ${#barriers[@]}/${#barriers[@]} zero 1213"
done

# ── 8. Fix-removal code mutations (each must fail against the DB) ───────
mutations=0
while read -r -u 3 name test_name; do
    [[ -n "${name}" ]] || continue
    copy="${workdir}/mutation-${name}"
    mkdir -p "${copy}"
    tar --exclude=__pycache__ --exclude=logs -C "${repo_root}" -cf - app AGENTS.md db docker-compose.yml .env.example \
        | tar -C "${copy}" -xf - || exit $?
    mkdir -p "${copy}/app/logs"
    python "${mutations_py}" apply "${copy}" "${name}" || exit $?
    log="${workdir}/${name}.log"
    if run_tests "${copy}" t083_suite "${test_name}" >"${log}" 2>&1; then
        echo "T083_MUTATION → FAIL (${name}): ${test_name##*.} passed without the fix" >&2
        exit 1
    fi
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "T083_MUTATION → FAIL (${name}): harness did not run the test" >&2; exit 1; }
    echo "T083_MUTATION → PASS (${name}): ${test_name##*.} fails without the fix"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${mutations_py}" list)
[[ "${mutations}" == "${expected_mutations}" ]] \
    || { echo "T083_MUTATION → FAIL: ${mutations}/${expected_mutations}" >&2; exit 1; }

# ── 9. DB-level mutations: closed value sets and no-default are load-bearing
db_test="${klass}.test_missing_configuration_value_never_defaults_compatible"
db_mutations=0
while IFS='|' read -r -u 3 label mutate restore; do
    [[ -n "${label}" ]] || continue
    mysql_root t083_suite --execute="${mutate}" </dev/null || exit $?
    if run_tests "${repo_root}" t083_suite "${db_test}" >"${workdir}/db-${label}.log" 2>&1; then
        echo "T083_DB_MUTATION → FAIL (${label}): passed without the constraint" >&2; exit 1
    fi
    grep -qE '^Ran 1 tests?' "${workdir}/db-${label}.log" && grep -qE '^FAILED \(' "${workdir}/db-${label}.log" \
        || { tail -n 20 "${workdir}/db-${label}.log" >&2; exit 1; }
    mysql_root t083_suite --execute="${restore}" </dev/null || exit $?
    mysql_root --default-character-set=latin1 t083_suite < "${repo_root}/db/init.sql" || exit $?
    [[ "$(replica_shape t083_suite)" == "${latin1_replica}" ]] || { echo "T083_DB_MUTATION → FAIL (${label}): restore" >&2; exit 1; }
    echo "T083_DB_MUTATION → PASS (${label}): test fails without it; exact shape restored"
    db_mutations=$((db_mutations + 1))
done 3<<'DBMUT'
kind-check|ALTER TABLE replica_heartbeats DROP CHECK ck_replica_heartbeat_kind|ALTER TABLE replica_heartbeats ADD CONSTRAINT ck_replica_heartbeat_kind CHECK (replica_kind IN (_ascii'app',_ascii'delivery_worker',_ascii'turn_worker'))
mode-check|ALTER TABLE replica_heartbeats DROP CHECK ck_replica_heartbeat_delivery_mode|ALTER TABLE replica_heartbeats ADD CONSTRAINT ck_replica_heartbeat_delivery_mode CHECK (delivery_mode IN (_ascii'direct',_ascii'outbox'))
auth-check|ALTER TABLE replica_heartbeats DROP CHECK ck_replica_heartbeat_auth_mode|ALTER TABLE replica_heartbeats ADD CONSTRAINT ck_replica_heartbeat_auth_mode CHECK (webhook_auth_mode IN (_ascii'provisioning',_ascii'enforced'))
auth-default|ALTER TABLE replica_heartbeats ALTER COLUMN webhook_auth_mode SET DEFAULT 'enforced'|ALTER TABLE replica_heartbeats ALTER COLUMN webhook_auth_mode DROP DEFAULT
DBMUT
[[ "${db_mutations}" == "${expected_db_mutations}" ]] \
    || { echo "T083_DB_MUTATION → FAIL: ${db_mutations}/${expected_db_mutations}" >&2; exit 1; }

# ── 10. Unmutated control ───────────────────────────────────────────────
run_tests "${repo_root}" t083_suite "${module}" >"${workdir}/control.log" 2>&1 \
    || { tail -n 40 "${workdir}/control.log" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${workdir}/control.log" && grep -qE '^OK$' "${workdir}/control.log" || exit 1
echo "T083_CONTROL → PASS: unmutated suite green after mutations"

# ── 11. Regression: every app test module (unit + real MySQL) ───────────
# The DB name carries both prefixes: the T08-2 suite guard requires 't082'.
regressed=0
if [[ "${T083_REGRESSION:-1}" == 1 ]]; then
    prepare_db t083_t082_regression || exit $?
    reg_log="${workdir}/regression.log"
    run_tests "${repo_root}" t083_t082_regression discover -s /workspace/app/tests -t /workspace/app -p 'test_*.py' >"${reg_log}" 2>&1 \
        || { grep -E '^(FAIL|ERROR):' "${reg_log}" >&2; tail -n 20 "${reg_log}" >&2; exit 1; }
    ran="$(grep -oE '^Ran [0-9]+ tests?' "${reg_log}")"
    grep -qE '^OK( \(skipped=[0-9]+\))?$' "${reg_log}" || exit 1
    grep -E '^OK' "${reg_log}"
    echo "T083_REGRESSION → PASS: ${ran}"
    regressed=1
fi

PYTHONPYCACHEPREFIX="${workdir}/pycache" python -m py_compile \
    "${repo_root}/app/src/database/repositories/replica_repo.py" \
    "${repo_root}/app/src/services/replica_compat.py" \
    "${repo_root}/app/src/database/repositories/scheduler_repo.py" \
    "${repo_root}/app/src/delivery_cli.py" "${repo_root}/app/src/delivery_worker.py" \
    "${repo_root}/app/src/main.py" "${repo_root}/app/src/turn_worker.py" \
    "${suite}" "${mutations_py}" || exit $?
bash -n "${repo_root}/scripts/run_inbox_t083_mysql_matrix.sh" || exit $?
echo "T083_MATRIX → PASS: charsets=${expected_charsets} upgrades=${expected_upgrades} interruptions=${expected_interruptions} tampers=${expected_tampers} migration_mutations=${expected_migration_mutations} targeted=${expected_targeted} barrier_rounds=${barrier_repeats} mutations=${expected_mutations} db_mutations=${expected_db_mutations} regression=${regressed}"
