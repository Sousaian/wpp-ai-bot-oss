#!/usr/bin/env bash
set -uo pipefail

# Ticket 06 matrix: every ticket 02–05 gate (through the ticket 05 matrix,
# including full zero-skip discovery with an asserted case count), then
# ticket06 effect-identity convergence from populated predecessors (1a7e1ed,
# a3674fb and the earlier released heads), a real interruption at every
# ticket06 DDL boundary, exact-shape fail-closed adversaries, targeted
# real-MySQL tests as a DML-only user, fix-removal code mutations and a guard
# drop. Synthetic credentials only; no provider, LLM or WhatsApp effect.
# Every loop reads its case list from fd 3 so docker exec never drains it.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${TICKET06_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
container="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket06-mysql-${suffix}"
network="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket06-net-${suffix}"
volume="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket06-data-${suffix}"
image="${WPP_TEST_IMAGE:-wpp-ticket06-matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
workdir="$(mktemp -d)"
fixture06="${repo_root}/scripts/fixtures/ticket06_populate.sql"
fixture05="${repo_root}/scripts/fixtures/ticket05_populate.sql"
mutations_py="${repo_root}/scripts/inbox_ticket06_mutations.py"
round3_tests="${repo_root}/app/tests/test_turn_delivery_round3_mysql.py"
expected_targeted=66
expected_mutations=83
# Review round 3 (R2M2/R2M18/R2M19/R2M20): own test module and mutation block.
expected_round3=6
expected_round3_mutations=6
expected_interruptions=8
expected_shapes=20
expected_sql_mutations=2
expected_fk_mutations=2
expected_upgrades=5
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
        echo "TICKET06_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != 0 ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "TICKET06_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    (( original == 0 )) || exit "${original}"
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

[[ "$(shared_count)" == 0 ]] || { echo "Shared Compose project must stay stopped" >&2; exit 2; }

# Declared counts must match the source (a lost or unregistered case fails).
declared="$(grep -cE '^    (async )?def test_' "${repo_root}/app/tests/test_turn_delivery_mysql.py")"
[[ "${declared}" == "${expected_targeted}" ]] \
    || { echo "TICKET06_CASE_COUNT → FAIL: declared=${declared} expected=${expected_targeted}" >&2; exit 1; }
round3_declared="$(grep -cE '^    (async )?def test_' "${round3_tests}")"
[[ "${round3_declared}" == "${expected_round3}" ]] \
    || { echo "TICKET06_CASE_COUNT → FAIL: round3 declared=${round3_declared} expected=${expected_round3}" >&2; exit 1; }
total_mutations=$((expected_mutations + expected_round3_mutations))
[[ "$(python "${mutations_py}" count)" == "${total_mutations}" ]] \
    || { echo "TICKET06_CASE_COUNT → FAIL: mutation list size" >&2; exit 1; }
python "${mutations_py}" check || exit $?
boundaries="$(grep -cE 'ticket06-(schema|trigger)-boundary:' "${repo_root}/db/init.sql")"
[[ "${boundaries}" == "${expected_interruptions}" ]] \
    || { echo "TICKET06_CASE_COUNT → FAIL: boundaries=${boundaries}" >&2; exit 1; }

# ── 1. Every predecessor gate (ticket 02–05) incl. full discovery ───────
prior_log="${workdir}/ticket05.log"
"${repo_root}/scripts/run_inbox_ticket05_mysql_matrix.sh" >"${prior_log}" 2>&1
prior_status=$?
grep -E '→ (PASS|FAIL)|^Ran [0-9]+ tests?|^OK|^FAILED' "${prior_log}"
(( prior_status == 0 )) || { tail -n 80 "${prior_log}" >&2; exit "${prior_status}"; }
grep -q '^TICKET05_MATRIX → PASS' "${prior_log}" || exit 1
echo "TICKET06_PREDECESSORS → PASS: ticket02/03/04/05 gates (full discovery asserted by ticket05)"

if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?

for ref in 1a7e1ed a3674fb f59c32c d52a495 35f9c07; do
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
query() {
    mysql_root --batch --skip-column-names "$1" </dev/null --execute="SET SESSION group_concat_max_len=4194304; $2"
}

# Exact ticket06 shape of outbox_deliveries: every column attribute and
# ordinal, every index attribute, every check (enforced + clause digest) and
# the authority guard digests (v3 present, predecessors absent).
ticket06_shape() {
    query "$1" "
SELECT CONCAT_WS('#',
 (SELECT GROUP_CONCAT(CONCAT_WS(':',ordinal_position,column_name,column_type,is_nullable,
          IFNULL(column_default,'NULL'),IFNULL(character_set_name,'-'),
          IFNULL(collation_name,'-'),extra) ORDER BY ordinal_position)
  FROM information_schema.columns WHERE table_schema=DATABASE()
    AND table_name='outbox_deliveries'),
 (SELECT GROUP_CONCAT(CONCAT_WS(':',index_name,non_unique,seq_in_index,column_name,
          IFNULL(collation,'-'),IFNULL(sub_part,'-'),index_type,is_visible)
          ORDER BY index_name,seq_in_index)
  FROM information_schema.statistics WHERE table_schema=DATABASE()
    AND table_name='outbox_deliveries'),
 (SELECT GROUP_CONCAT(CONCAT_WS(':',tc.constraint_name,tc.enforced,SHA2(cc.check_clause,256))
          ORDER BY tc.constraint_name)
  FROM information_schema.table_constraints tc
  JOIN information_schema.check_constraints cc
    ON cc.constraint_schema=tc.constraint_schema AND cc.constraint_name=tc.constraint_name
  WHERE tc.constraint_schema=DATABASE() AND tc.table_name='outbox_deliveries'),
 (SELECT GROUP_CONCAT(CONCAT_WS(':',trigger_name,SHA2(action_statement,256))
          ORDER BY trigger_name)
  FROM information_schema.triggers WHERE trigger_schema=DATABASE()
    AND event_object_table='outbox_deliveries'));"
}

# Populated ticket05 transition: classified, no retroactive reply, no
# sequence/status/payload rewrite.
fixture06_state() {
    query "$1" "
SELECT CONCAT_WS('|',
 (SELECT GROUP_CONCAT(CONCAT_WS(':',automated_effect,status,stream_sequence,
          depends_on_delivery_id IS NOT NULL,JSON_EXTRACT(payload,'\$.synthetic')) ORDER BY id)
  FROM outbox_deliveries WHERE delivery_class='automated'),
 (SELECT COUNT(*) FROM outbox_deliveries WHERE automated_effect='reply'),
 (SELECT next_delivery_sequence FROM sessoes WHERE telefone='551100000006'),
 (SELECT COUNT(*) FROM outbox_deliveries WHERE delivery_class<>'automated'
     AND automated_effect IS NULL));"
}
expected_fixture06="handoff_transition:blocked:1:1:true|0|2|1"

create_db t06_fresh || exit $?
apply_current t06_fresh || exit $?
apply_current t06_fresh || exit $?
fresh_shape="$(ticket06_shape t06_fresh)" || exit $?
[[ -n "${fresh_shape}" ]] || exit 1
grep -q 'automated_effect:enum('"'"'reply'"'"','"'"'handoff_transition'"'"'):YES:NULL:ascii:ascii_bin' <<<"${fresh_shape}" \
    || { echo "TICKET06_FRESH → FAIL: column shape" >&2; exit 1; }
grep -q 'uq_outbox_turn_revision_effect:0:3:automated_effect' <<<"${fresh_shape}" || exit 1
grep -q 'uq_outbox_turn_revision:' <<<"${fresh_shape}" && exit 1
grep -q 'ck_outbox_delivery_authority_v2:YES:0ed7c0fbe73cff6435080978b49dd5f511c41b32d5d1d7051c914b5b0983f96a' <<<"${fresh_shape}" || exit 1
grep -qE 'ck_outbox_delivery_authority:' <<<"${fresh_shape}" && exit 1
grep -q 'trg_outbox_authority_immutable_v3:' <<<"${fresh_shape}" || exit 1
grep -qE 'trg_outbox_authority_immutable(_v2)?:' <<<"${fresh_shape}" && exit 1
echo "TICKET06_FRESH → PASS: clean + repeated install, exact shape"

# ── 2. Populated upgrades (repeated) ────────────────────────────────────
upgrades=0
while read -r -u 3 ref fixture; do
    [[ -n "${ref}" ]] || continue
    db="t06_up_${ref}"
    create_db "${db}" || exit $?
    apply_file "${db}" "${workdir}/${ref}.sql" || exit $?
    apply_file "${db}" "${repo_root}/scripts/fixtures/${fixture}" || exit $?
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(ticket06_shape "${db}")" == "${fresh_shape}" ]] \
        || { echo "TICKET06_UPGRADE → FAIL (${ref}): shape" >&2; exit 1; }
    if [[ "${fixture}" == ticket06_populate.sql ]]; then
        state="$(fixture06_state "${db}")"
        [[ "${state}" == "${expected_fixture06}" ]] \
            || { echo "TICKET06_UPGRADE → FAIL (${ref}): ${state}" >&2; exit 1; }
    else
        [[ "$(query "${db}" "SELECT COUNT(*) FROM outbox_deliveries WHERE automated_effect IS NOT NULL")" == 0 ]] \
            || exit 1
    fi
    echo "TICKET06_UPGRADE → PASS (${ref} + ${fixture} → current ×2): exact shape, rows kept"
    upgrades=$((upgrades + 1))
done 3<<'REFS'
1a7e1ed ticket06_populate.sql
a3674fb ticket06_populate.sql
f59c32c ticket05_populate.sql
d52a495 ticket05_populate.sql
35f9c07 ticket05_populate.sql
REFS
[[ "${upgrades}" == "${expected_upgrades}" ]] \
    || { echo "TICKET06_UPGRADE → FAIL: ${upgrades}/${expected_upgrades}" >&2; exit 1; }

# ── 3. Interruption at every ticket06 DDL boundary (populated base) ─────
# Schema boundaries live inside the migration procedure: a SIGNAL injected
# after the marker aborts it exactly there (prior DDL already committed).
# Trigger boundaries are top-level: the file is cut after the marker.
interruptions=0
while IFS='|' read -r -u 3 kind marker; do
    [[ -n "${marker}" ]] || continue
    db="t06_int_${interruptions}"
    variant="${workdir}/interrupt-${interruptions}.sql"
    if [[ "${kind}" == schema ]]; then
        awk -v marker="${marker}" '{print} index($0, marker) {print "    SIGNAL SQLSTATE '"'"'45000'"'"' SET MESSAGE_TEXT = '"'"'ticket06 interruption fixture'"'"';"}' \
            "${repo_root}/db/init.sql" > "${variant}"
    else
        awk -v marker="${marker}" '{print; if (index($0, marker)) exit}' \
            "${repo_root}/db/init.sql" > "${variant}"
    fi
    grep -qF -- "${marker}" "${variant}" || { echo "missing marker ${marker}" >&2; exit 1; }
    create_db "${db}" || exit $?
    apply_file "${db}" "${workdir}/1a7e1ed.sql" || exit $?
    apply_file "${db}" "${fixture06}" || exit $?
    if [[ "${kind}" == schema ]]; then
        if apply_file "${db}" "${variant}" >"${workdir}/int.log" 2>&1; then
            echo "TICKET06_INTERRUPTION → FAIL (${marker}): not interrupted" >&2; exit 1
        fi
        grep -q 'ticket06 interruption fixture' "${workdir}/int.log" \
            || { cat "${workdir}/int.log" >&2; exit 1; }
    else
        apply_file "${db}" "${variant}" || exit $?
    fi
    apply_current "${db}" || exit $?
    apply_current "${db}" || exit $?
    [[ "$(ticket06_shape "${db}")" == "${fresh_shape}" ]] \
        || { echo "TICKET06_INTERRUPTION → FAIL (${marker}): shape" >&2; exit 1; }
    [[ "$(fixture06_state "${db}")" == "${expected_fixture06}" ]] \
        || { echo "TICKET06_INTERRUPTION → FAIL (${marker}): rows" >&2; exit 1; }
    echo "TICKET06_INTERRUPTION → PASS (${marker}): converged, rows kept"
    interruptions=$((interruptions + 1))
done 3<<'MARKERS'
schema|ticket06-schema-boundary: automated-effect-column-created
schema|ticket06-schema-boundary: existing-transitions-classified
schema|ticket06-schema-boundary: effect-index-created
schema|ticket06-schema-boundary: predecessor-index-dropped
schema|ticket06-schema-boundary: authority-check-v2-created
schema|ticket06-schema-boundary: authority-check-v1-dropped
trigger|ticket06-trigger-boundary: outbox-authority-v3-created
trigger|ticket06-trigger-boundary: outbox-authority-v2-dropped
MARKERS
[[ "${interruptions}" == "${expected_interruptions}" ]] \
    || { echo "TICKET06_INTERRUPTION → FAIL: ${interruptions}/${expected_interruptions}" >&2; exit 1; }

# ── 4. Exact-shape adversaries fail closed with a named error ───────────
# "old": populated 1a7e1ed base, then the current migration; the predecessor
# guard must survive. "fresh": fully migrated schema loses or alters an object,
# then a reapply must fail closed (P2-5) without silently re-creating it.
shapes=0
while IFS='|' read -r -u 3 base label expected survivor survivor_value statement; do
    [[ -n "${label}" ]] || continue
    db="t06_bad_${shapes}"
    create_db "${db}" || exit $?
    if [[ "${base}" == old ]]; then
        apply_file "${db}" "${workdir}/1a7e1ed.sql" || exit $?
        apply_file "${db}" "${fixture06}" || exit $?
    else
        apply_current "${db}" || exit $?
    fi
    mysql_root "${db}" </dev/null --execute="${statement}" || exit $?
    if apply_current "${db}" >"${workdir}/bad.log" 2>&1; then
        echo "TICKET06_SHAPE → FAIL (${label}): divergent shape accepted" >&2; exit 1
    fi
    grep -qF -- "${expected}" "${workdir}/bad.log" \
        || { cat "${workdir}/bad.log" >&2; echo "TICKET06_SHAPE → FAIL (${label}): wrong error" >&2; exit 1; }
    [[ "$(query "${db}" "${survivor}")" == "${survivor_value}" ]] \
        || { echo "TICKET06_SHAPE → FAIL (${label}): object state changed" >&2; exit 1; }
    echo "TICKET06_SHAPE → PASS (${base}/${label}): '${expected}', nothing re-created or dropped"
    shapes=$((shapes + 1))
done 3<<'SHAPES'
old|column-collation|ticket06 automated effect column shape requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name='uq_outbox_turn_revision' AND seq_in_index=1|1|ALTER TABLE outbox_deliveries ADD COLUMN automated_effect ENUM('reply','handoff_transition') CHARACTER SET ascii COLLATE ascii_general_ci NULL AFTER delivery_class
old|column-position|ticket06 automated effect column shape requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name='uq_outbox_turn_revision' AND seq_in_index=1|1|ALTER TABLE outbox_deliveries ADD COLUMN automated_effect ENUM('reply','handoff_transition') CHARACTER SET ascii COLLATE ascii_bin NULL AFTER payload
old|unclassifiable-row|ticket06 automated effect classification requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name='uq_outbox_turn_revision' AND seq_in_index=1|1|DELETE FROM scheduled_handoffs
old|index-order|ticket06 automated effect index shape requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name='uq_outbox_turn_revision' AND seq_in_index=1|1|CREATE UNIQUE INDEX uq_outbox_turn_revision_effect ON outbox_deliveries (turn_revision_id, conversation_turn_id, delivery_class)
old|index-nonunique|ticket06 automated effect index shape requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name='uq_outbox_turn_revision' AND seq_in_index=1|1|ALTER TABLE outbox_deliveries ADD COLUMN automated_effect ENUM('reply','handoff_transition') CHARACTER SET ascii COLLATE ascii_bin NULL AFTER delivery_class, ADD INDEX uq_outbox_turn_revision_effect (conversation_turn_id, turn_revision_id, automated_effect)
old|check-clause|ticket06 automated authority check shape requires audited repair|SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND constraint_name='ck_outbox_delivery_authority'|1|ALTER TABLE outbox_deliveries ADD CONSTRAINT ck_outbox_delivery_authority_v2 CHECK (delivery_class IS NOT NULL)
fresh|effect-index-missing|ticket06 automated effect index shape requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name IN ('uq_outbox_turn_revision','uq_outbox_turn_revision_effect')|0|ALTER TABLE outbox_deliveries DROP INDEX uq_outbox_turn_revision_effect
fresh|effect-index-nonunique|ticket06 automated effect index shape requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name='uq_outbox_turn_revision_effect' AND non_unique=1|3|ALTER TABLE outbox_deliveries DROP INDEX uq_outbox_turn_revision_effect, ADD INDEX uq_outbox_turn_revision_effect (conversation_turn_id, turn_revision_id, automated_effect)
fresh|authority-check-missing|ticket06 automated authority check shape requires audited repair|SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND constraint_name IN ('ck_outbox_delivery_authority','ck_outbox_delivery_authority_v2')|0|ALTER TABLE outbox_deliveries DROP CHECK ck_outbox_delivery_authority_v2
fresh|t05-check-dropped|ticket05 handoff check shape requires audited repair|SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND constraint_name='ck_scheduled_handoff_summary_source'|0|ALTER TABLE scheduled_handoffs DROP CHECK ck_scheduled_handoff_summary_source
fresh|t05-all-checks-dropped|ticket05 handoff check shape requires audited repair|SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND constraint_name IN ('ck_scheduled_handoff_summary_source','ck_handoff_context_coverage_ordinal')|0|ALTER TABLE scheduled_handoffs DROP CHECK ck_scheduled_handoff_summary_source; ALTER TABLE handoff_context_assignments DROP CHECK ck_handoff_context_coverage_ordinal
fresh|t05-index-missing|ticket05 handoff index shape requires audited repair|SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name='idx_scheduled_handoff_session'|0|ALTER TABLE scheduled_handoffs DROP INDEX idx_scheduled_handoff_session
fresh|authority-fk-fk_turn_current_revision|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name='fk_turn_current_revision'|0|ALTER TABLE conversation_turns DROP FOREIGN KEY fk_turn_current_revision
fresh|authority-fk-fk_turn_final_revision|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name='fk_turn_final_revision'|0|ALTER TABLE conversation_turns DROP FOREIGN KEY fk_turn_final_revision
fresh|authority-fk-fk_outbox_conversation_turn|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name='fk_outbox_conversation_turn'|0|ALTER TABLE outbox_deliveries DROP FOREIGN KEY fk_outbox_conversation_turn
fresh|authority-fk-fk_outbox_turn_revision|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name='fk_outbox_turn_revision'|0|ALTER TABLE outbox_deliveries DROP FOREIGN KEY fk_outbox_turn_revision
fresh|authority-fk-fk_outbox_turn_authority|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name='fk_outbox_turn_authority'|0|ALTER TABLE outbox_deliveries DROP FOREIGN KEY fk_outbox_turn_authority
fresh|authority-fk-fk_outbox_revision_authority|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name='fk_outbox_revision_authority'|0|ALTER TABLE outbox_deliveries DROP FOREIGN KEY fk_outbox_revision_authority
fresh|authority-fk-fk_outbox_handoff_authority|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name='fk_outbox_handoff_authority'|0|ALTER TABLE outbox_deliveries DROP FOREIGN KEY fk_outbox_handoff_authority
fresh|authority-fk-all|scheduler authority foreign key shape requires audited repair|SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=DATABASE() AND constraint_name IN ('fk_outbox_turn_authority','fk_outbox_revision_authority','fk_outbox_handoff_authority','fk_outbox_conversation_turn','fk_outbox_turn_revision','fk_turn_current_revision','fk_turn_final_revision')|0|ALTER TABLE conversation_turns DROP FOREIGN KEY fk_turn_current_revision, DROP FOREIGN KEY fk_turn_final_revision; ALTER TABLE outbox_deliveries DROP FOREIGN KEY fk_outbox_conversation_turn, DROP FOREIGN KEY fk_outbox_turn_revision, DROP FOREIGN KEY fk_outbox_turn_authority, DROP FOREIGN KEY fk_outbox_revision_authority, DROP FOREIGN KEY fk_outbox_handoff_authority
SHAPES
[[ "${shapes}" == "${expected_shapes}" ]] \
    || { echo "TICKET06_SHAPE → FAIL: ${shapes}/${expected_shapes}" >&2; exit 1; }

# ── 4b. SQL mutations: each FK protection is load-bearing (S5, era gate) ─
# Removing the NULL fail-closed check (S5) or the ticket06 era gate must let
# the all-FKs-dropped adversary pass silently; the real init.sql rejects it.
python - "${repo_root}/db/init.sql" "${workdir}" <<'PYSQL' || exit $?
import sys
from pathlib import Path
source = Path(sys.argv[1]).read_text()
out = Path(sys.argv[2])
start = source.index("CREATE PROCEDURE validate_scheduler_authority_foreign_keys()")
end = source.index("END$$", start)
body = source[start:end]
assert body.count("actual_shape IS NULL OR ") == 1
(out / "sql-s5.sql").write_text(
    source[:start] + body.replace("actual_shape IS NULL OR ", "", 1) + source[end:]
)
era = source
marker = (
    "    ) AND NOT EXISTS (\n"
    "        -- Ticket06 era marker (created only after a complete run): from then\n"
    "        -- on authority FKs are never re-created; drift fails closed below.\n"
    "        SELECT 1 FROM information_schema.triggers\n"
    "        WHERE trigger_schema = DATABASE()\n"
    "          AND trigger_name = 'trg_outbox_authority_immutable_v3'\n"
    "    ) THEN\n"
)
assert era.count(marker) == 3
(out / "sql-era-gate.sql").write_text(era.replace(marker, "    ) THEN\n"))
PYSQL
sql_mutations=0
drop_all_fks="ALTER TABLE conversation_turns DROP FOREIGN KEY fk_turn_current_revision, DROP FOREIGN KEY fk_turn_final_revision; ALTER TABLE outbox_deliveries DROP FOREIGN KEY fk_outbox_conversation_turn, DROP FOREIGN KEY fk_outbox_turn_revision, DROP FOREIGN KEY fk_outbox_turn_authority, DROP FOREIGN KEY fk_outbox_revision_authority, DROP FOREIGN KEY fk_outbox_handoff_authority"
while IFS='|' read -r -u 3 label variant; do
    [[ -n "${label}" ]] || continue
    db="t06_sqlmut_${sql_mutations}"
    create_db "${db}" || exit $?
    apply_current "${db}" || exit $?
    mysql_root "${db}" </dev/null --execute="${drop_all_fks}" || exit $?
    if ! apply_file "${db}" "${workdir}/${variant}" >"${workdir}/sqlmut.log" 2>&1; then
        cat "${workdir}/sqlmut.log" >&2
        echo "TICKET06_SQL_MUTATION → FAIL (${label}): mutated migration still rejected the adversary" >&2
        exit 1
    fi
    echo "TICKET06_SQL_MUTATION → PASS (${label}): all-FKs-dropped accepted without the protection"
    sql_mutations=$((sql_mutations + 1))
done 3<<'SQLMUT'
s5-null-fail-closed|sql-s5.sql
era-gate-no-recreate|sql-era-gate.sql
SQLMUT
[[ "${sql_mutations}" == "${expected_sql_mutations}" ]] \
    || { echo "TICKET06_SQL_MUTATION → FAIL: ${sql_mutations}/${expected_sql_mutations}" >&2; exit 1; }

# ── 5. Targeted tests as a DML-only user ────────────────────────────────
mysql_root </dev/null --execute="CREATE USER 'ticket06_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT,INSERT,UPDATE,DELETE ON t06_fresh.* TO 'ticket06_app'@'%';" || exit $?

run_tests() {
    local root=$1; shift
    docker run --rm --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://ticket06_app:${app_password}@127.0.0.1:3306/t06_fresh" \
        --env "DATABASE_URL=mysql+aiomysql://ticket06_app:${app_password}@127.0.0.1:3306/t06_fresh" \
        --env OPENROUTER_API_KEY=synthetic-ticket06-key \
        --env EVOLUTION_API_URL=http://evolution.invalid --env EVOLUTION_API_KEY=synthetic \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid --env CHATWOOT_API_TOKEN=synthetic \
        --env CHATWOOT_ACCOUNT_ID=1 --env CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 900 python -m unittest "$@" </dev/null
}

target_log="${workdir}/target.log"
run_tests "${repo_root}" tests.test_turn_delivery_mysql >"${target_log}" 2>&1 \
    || { tail -n 60 "${target_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${target_log}" && grep -qE '^OK$' "${target_log}" \
    || { tail -n 20 "${target_log}" >&2; echo "TICKET06_CASE_COUNT → FAIL: targeted" >&2; exit 1; }
echo "TICKET06_TARGETED → PASS: ${expected_targeted}/${expected_targeted} (zero skip, DML-only user)"
round3_log="${workdir}/round3.log"
run_tests "${repo_root}" tests.test_turn_delivery_round3_mysql >"${round3_log}" 2>&1 \
    || { tail -n 60 "${round3_log}" >&2; exit 1; }
grep -qE "^Ran ${expected_round3} tests?" "${round3_log}" && grep -qE '^OK$' "${round3_log}" \
    || { tail -n 20 "${round3_log}" >&2; echo "TICKET06_CASE_COUNT → FAIL: round3" >&2; exit 1; }
echo "TICKET06_ROUND3 → PASS: ${expected_round3}/${expected_round3} (zero skip, DML-only user)"

# ── 6. Fix-removal code mutations (each must fail against the DB) ───────
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
        echo "TICKET06_MUTATION → FAIL (${name}): ${test_name##*.} passed without the fix" >&2
        exit 1
    fi
    grep -qE '^Ran [1-9][0-9]* tests?' "${log}" && grep -qE '^FAILED \(' "${log}" \
        || { tail -n 20 "${log}" >&2; echo "TICKET06_MUTATION → FAIL (${name}): harness did not run the test" >&2; exit 1; }
    echo "TICKET06_MUTATION → PASS (${name}): ${test_name##*.} fails without the fix"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${mutations_py}" list)
[[ "${mutations}" == "${total_mutations}" ]] \
    || { echo "TICKET06_MUTATION → FAIL: ${mutations}/${total_mutations}" >&2; exit 1; }

# ── 7. Guard drop: the v3 authority guard is load-bearing ───────────────
guard_test="tests.test_turn_delivery_mysql.TurnDeliveryMySQLTests.test_effect_identity_is_immutable_and_unique_per_revision"
mysql_root t06_fresh </dev/null --execute="DROP TRIGGER trg_outbox_authority_immutable_v3;" || exit $?
if run_tests "${repo_root}" "${guard_test}" >"${workdir}/guard.log" 2>&1; then
    echo "TICKET06_GUARD_MUTATION → FAIL: test passed without trg_outbox_authority_immutable_v3" >&2
    exit 1
fi
grep -qE '^Ran 1 tests?' "${workdir}/guard.log" && grep -qE '^FAILED \(' "${workdir}/guard.log" \
    || { tail -n 20 "${workdir}/guard.log" >&2; exit 1; }
apply_current t06_fresh || exit $?
[[ "$(ticket06_shape t06_fresh)" == "${fresh_shape}" ]] || exit 1
echo "TICKET06_GUARD_MUTATION → PASS (trg_outbox_authority_immutable_v3): restored to exact shape"

# ── 8. Schema mutation: the authority FKs are load-bearing (P2-6) ───────
fk_test="tests.test_scheduler_state_mysql.SchedulerStateMySQLTests.test_automated_delivery_exact_authority_and_immutability"
fk_mutations=0
while IFS='|' read -r -u 3 fk_name restore; do
    [[ -n "${fk_name}" ]] || continue
    mysql_root t06_fresh </dev/null \
        --execute="ALTER TABLE outbox_deliveries DROP FOREIGN KEY ${fk_name};" || exit $?
    if run_tests "${repo_root}" "${fk_test}" >"${workdir}/fk.log" 2>&1; then
        echo "TICKET06_FK_MUTATION → FAIL (${fk_name}): test passed without the FK" >&2
        exit 1
    fi
    grep -qE '^Ran 1 tests?' "${workdir}/fk.log" && grep -qE '^FAILED \(' "${workdir}/fk.log" \
        || { tail -n 20 "${workdir}/fk.log" >&2; exit 1; }
    # The mutated run may leave the poisoned row the FK would reject; the
    # disposable test rows are cleared, then the FK is restored exactly (the
    # migration itself fails closed on a missing FK).
    wipe="$(query t06_fresh "SELECT GROUP_CONCAT(CONCAT('DELETE FROM \`',table_name,'\`') SEPARATOR '; ') FROM information_schema.tables WHERE table_schema=DATABASE() AND table_type='BASE TABLE' AND table_name NOT IN ('delivery_runtime_state','scheduler_runtime_state')")" || exit $?
    mysql_root t06_fresh </dev/null --execute="SET FOREIGN_KEY_CHECKS=0; ${wipe}; SET FOREIGN_KEY_CHECKS=1; ALTER TABLE outbox_deliveries ADD CONSTRAINT ${restore};" || exit $?
    apply_current t06_fresh || exit $?
    [[ "$(ticket06_shape t06_fresh)" == "${fresh_shape}" ]] || exit 1
    echo "TICKET06_FK_MUTATION → PASS (${fk_name}): authority test fails without it; restored"
    fk_mutations=$((fk_mutations + 1))
done 3<<'FKS'
fk_outbox_revision_authority|fk_outbox_revision_authority FOREIGN KEY (turn_revision_id, conversation_turn_id, scheduler_epoch) REFERENCES turn_revisions(id, turn_id, scheduler_epoch) ON DELETE RESTRICT
fk_outbox_handoff_authority|fk_outbox_handoff_authority FOREIGN KEY (handoff_attempt_id, agente_id, sessao_id) REFERENCES handoff_attempts(id, agente_id, sessao_id) ON DELETE RESTRICT
FKS
[[ "${fk_mutations}" == "${expected_fk_mutations}" ]] \
    || { echo "TICKET06_FK_MUTATION → FAIL: ${fk_mutations}/${expected_fk_mutations}" >&2; exit 1; }

# Unmutated control after the guard restoration.
run_tests "${repo_root}" tests.test_turn_delivery_mysql >"${workdir}/control.log" 2>&1 \
    || { tail -n 40 "${workdir}/control.log" >&2; exit 1; }
grep -qE "^Ran ${expected_targeted} tests?" "${workdir}/control.log" || exit 1
run_tests "${repo_root}" tests.test_turn_delivery_round3_mysql >"${workdir}/control3.log" 2>&1 \
    || { tail -n 40 "${workdir}/control3.log" >&2; exit 1; }
grep -qE "^Ran ${expected_round3} tests?" "${workdir}/control3.log" || exit 1
echo "TICKET06_CONTROL → PASS: unmutated targeted suite green after restoration"

python "${repo_root}/scripts/check_trigger_body_digests.py" --self-test \
    "${repo_root}/db/init.sql" | tail -n 1 || exit $?
PYTHONPYCACHEPREFIX="${workdir}/pycache" python -m py_compile \
    "${repo_root}/app/src/database/models.py" \
    "${repo_root}/app/src/database/repositories/scheduler_repo.py" \
    "${repo_root}/app/src/database/repositories/turn_delivery_repo.py" \
    "${repo_root}/app/src/database/repositories/turn_generation_repo.py" \
    "${repo_root}/app/src/database/repositories/turn_handoff_repo.py" \
    "${repo_root}/app/src/database/repositories/delivery_worker_repo.py" \
    "${repo_root}/app/src/services/turn_processor.py" \
    "${repo_root}/app/src/services/turn_delivery.py" \
    "${repo_root}/app/src/services/delivery_adapters.py" \
    "${repo_root}/app/src/delivery_worker.py" \
    "${repo_root}/app/src/turn_worker.py" \
    "${repo_root}/app/tests/test_turn_delivery_mysql.py" \
    "${round3_tests}" \
    "${mutations_py}" || exit $?
bash -n "${repo_root}/scripts/run_inbox_ticket06_mysql_matrix.sh" || exit $?
echo "TICKET06_MATRIX → PASS: targeted=${expected_targeted} round3=${expected_round3} upgrades=${expected_upgrades} interruptions=${expected_interruptions} shapes=${expected_shapes} mutations=${total_mutations} guards=1 fk_mutations=${expected_fk_mutations} sql_mutations=${expected_sql_mutations}"
