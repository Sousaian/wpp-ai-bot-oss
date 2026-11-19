#!/usr/bin/env bash
set -uo pipefail

# Ticket 04 matrix: every ticket 02/03 gate (fresh/repeat/intermediate/legacy
# schema, trigger digests, migration adversaries and the full zero-skip suite,
# which now includes the ticket 04 processor tests), then ticket 04 schema
# convergence/fail-closed checks and fix-removal mutation proofs on a separate
# disposable MySQL. Synthetic credentials only; no provider, LLM or WhatsApp.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${TICKET04_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
container="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket04-mysql-${suffix}"
network="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket04-net-${suffix}"
volume="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket04-data-${suffix}"
image="${WPP_TEST_IMAGE:-wpp-ticket04-matrix-${suffix}:local}"
built_image=0
shared_project="traycer-wpp-ai-bot-soft-ocelot"
predecessor_ref="${TICKET04_PREDECESSOR_REF:-35f9c07}"
ticket04_base_ref="${TICKET04_BASE_REF:-f40d3a6}"
workdir="$(mktemp -d)"
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
    local original_status=$?
    local cleanup_status=0
    rm -rf "${workdir}"
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then
        docker image rm -f "${image}" >/dev/null 2>&1 || true
    fi
    if [[ "$(resource_count)" != "0" ]]; then
        echo "TICKET04_CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != "0" ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "TICKET04_CLEANUP → PASS: disposable resources absent"
        echo "SHARED_STACK → PASS: ${shared_project} containers=0"
    fi
    if (( original_status != 0 )); then
        exit "${original_status}"
    fi
    exit "${cleanup_status}"
}
trap cleanup EXIT INT TERM

if [[ "$(shared_count)" != "0" ]]; then
    echo "Shared Compose project must stay stopped" >&2
    exit 2
fi

# ── 1. Every prior gate (ticket 02 + 03 matrices, full suite) ──────────
"${repo_root}/scripts/run_inbox_ticket03_mysql_matrix.sh" || exit $?

# ── 2. Ticket 04 disposable MySQL ───────────────────────────────────────
git -C "${repo_root}" show "${predecessor_ref}:db/init.sql" > "${workdir}/predecessor.sql" || exit $?
# First ticket 04 commit (before the requested/reported model split).
git -C "${repo_root}" show "${ticket04_base_ref}:db/init.sql" > "${workdir}/ticket04_base.sql" || exit $?
docker network create "${network}" >/dev/null || exit $?
docker volume create "${volume}" >/dev/null || exit $?
docker run --detach \
    --name "${container}" \
    --network "${network}" \
    --volume "${volume}:/var/lib/mysql" \
    --env "MYSQL_ROOT_PASSWORD=${root_password}" \
    mysql:8.0 \
    --character-set-server=utf8mb4 \
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
if (( ready != 1 )); then
    docker logs --tail 100 "${container}" >&2 || true
    exit 1
fi

mysql_root() {
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "$@"
}

apply_file() {
    local database=$1 file=$2
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "${database}" < "${file}"
}

apply_current() { apply_file "$1" "${repo_root}/db/init.sql"; }

create_db() {
    mysql_root --execute="CREATE DATABASE $1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
}

revision_shape() {
    mysql_root --batch --skip-column-names "$1" --execute="
SET SESSION group_concat_max_len = 1048576;
SELECT CONCAT_WS('|',
 (SELECT GROUP_CONCAT(CONCAT(column_name,':',column_type,':',is_nullable,':',
         IFNULL(column_default,'NULL'),':',IFNULL(collation_name,'-'))
         ORDER BY ordinal_position SEPARATOR ',')
  FROM information_schema.columns
  WHERE table_schema=DATABASE() AND table_name='turn_revisions'),
 (SELECT GROUP_CONCAT(CONCAT(cc.constraint_name,'=',cc.check_clause)
         ORDER BY cc.constraint_name SEPARATOR ',')
  FROM information_schema.check_constraints cc
  JOIN information_schema.table_constraints tc
    ON tc.constraint_schema=cc.constraint_schema
   AND tc.constraint_name=cc.constraint_name
  WHERE cc.constraint_schema=DATABASE() AND tc.table_name='turn_revisions'),
 (SELECT GROUP_CONCAT(CONCAT(trigger_name,'=',SHA2(action_statement,256))
         ORDER BY trigger_name SEPARATOR ',')
  FROM information_schema.triggers
  WHERE trigger_schema=DATABASE() AND event_object_table='turn_revisions'));
"
}

usage_shape() {
    mysql_root --batch --skip-column-names "$1" --execute="
SET SESSION group_concat_max_len = 1048576;
SELECT GROUP_CONCAT(CONCAT(table_name,'.',column_name,':',column_type,':',is_nullable,':',
       IFNULL(column_default,'NULL'),':',IFNULL(collation_name,'-'))
       ORDER BY table_name, ordinal_position SEPARATOR ',')
FROM information_schema.columns
WHERE table_schema=DATABASE() AND table_name IN ('consumo_llm','llm_usage_reconciliation');
"
}

seed_usage() {
    mysql_root "$1" <<'SQL'
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('usage','usage-ticket04','synthetic','0');
SET @usage_agent=LAST_INSERT_ID();
INSERT INTO consumo_llm (agente_id,modelo_llm,tipo_chamada,tokens_input,tokens_output,
 custo_creditos,provider_generation_id)
VALUES (@usage_agent,'openai/gpt-4o-mini-2024-07-18','resposta',7,3,0.00000210,'seed-generation');
INSERT INTO llm_usage_reconciliation (reconciliation_key,agente_id,modelo_llm,tipo_chamada,
 tokens_input,tokens_output,custo_creditos,provider_generation_id,safe_error_code)
VALUES (UUID(),@usage_agent,'openai/gpt-4o-mini','resumo',1,1,0.00000100,
 'seed-reconciliation','seed');
SQL
}

usage_kept() {
    mysql_root --batch --skip-column-names "$1" --execute="
SELECT CONCAT(
 (SELECT CONCAT(modelo_llm,'/',IFNULL(modelo_llm_reportado,'NULL')) FROM consumo_llm
  WHERE provider_generation_id='seed-generation'),'|',
 (SELECT CONCAT(modelo_llm,'/',IFNULL(modelo_llm_reportado,'NULL')) FROM llm_usage_reconciliation
  WHERE provider_generation_id='seed-reconciliation'));"
}

assert_usage_converged() {
    local database=$1 phase=$2 shape kept
    shape="$(usage_shape "${database}")" || exit $?
    if [[ "${shape}" != "${fresh_usage_shape}" ]]; then
        echo "TICKET04_MIGRATION → FAIL (${phase}): usage tables diverge from fresh" >&2
        printf 'fresh:   %s\nupgrade: %s\n' "${fresh_usage_shape}" "${shape}" >&2
        exit 1
    fi
    kept="$(usage_kept "${database}")" || exit $?
    [[ "${kept}" == "openai/gpt-4o-mini-2024-07-18/NULL|openai/gpt-4o-mini/NULL" ]] \
        || { echo "TICKET04_MIGRATION → FAIL (${phase}): usage rows rewritten: ${kept}" >&2; exit 1; }
    echo "TICKET04_MIGRATION → PASS (${phase}): usage tables converge, rows kept, reported NULL"
}

expect_apply_failure() {
    local database=$1 expected=$2 phase=$3 output rc
    output="$(apply_current "${database}" 2>&1)"
    rc=$?
    if (( rc == 0 )) || ! grep -q "${expected}" <<<"${output}"; then
        echo "TICKET04_MIGRATION → FAIL (${phase}): expected '${expected}'" >&2
        printf '%s\n' "${output}" >&2
        exit 1
    fi
    echo "TICKET04_MIGRATION → PASS (${phase}): failed closed with '${expected}'"
}

create_db t04_fresh || exit $?
apply_current t04_fresh || exit $?
apply_current t04_fresh || exit $?
fresh_shape="$(revision_shape t04_fresh)" || exit $?
grep -q "draft_kind:enum('generated','soft_limit_fallback','technical_fallback'):YES:NULL" \
    <<<"${fresh_shape}" || { echo "TICKET04_MIGRATION → FAIL: fresh draft_kind" >&2; exit 1; }
grep -q 'typing_jitter_ms:smallint:YES:NULL' <<<"${fresh_shape}" \
    || { echo "TICKET04_MIGRATION → FAIL: fresh typing_jitter_ms" >&2; exit 1; }
grep -q 'ck_revision_typing_jitter=' <<<"${fresh_shape}" \
    || { echo "TICKET04_MIGRATION → FAIL: fresh jitter check" >&2; exit 1; }
grep -q 'trg_turn_revision_draft_immutable_v2=99bf8974c9cd291800ba12fd2d1eb46106a2d79073f7c023c7520e237cc837f8' \
    <<<"${fresh_shape}" || { echo "TICKET04_MIGRATION → FAIL: fresh draft guard" >&2; exit 1; }
guard_count="$(mysql_root --batch --skip-column-names t04_fresh --execute="
SELECT COUNT(*) FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name IN (
 'trg_inbound_event_disposition_immutable_v2','trg_outbox_authority_immutable_v3',
 'trg_turn_revision_seal_immutable_v2','trg_turn_input_identity_immutable_v2',
 'trg_conversation_turn_identity_immutable_v2',
 'trg_generation_attempt_identity_immutable_v2',
 'trg_turn_revision_draft_immutable_v2');
")" || exit $?
[[ "${guard_count}" == "7" ]] || { echo "IMMUTABILITY_TRIGGERS → FAIL: ${guard_count}/7" >&2; exit 1; }
echo "IMMUTABILITY_TRIGGERS → PASS: 7/7"
fresh_usage_shape="$(usage_shape t04_fresh)" || exit $?
for table in consumo_llm llm_usage_reconciliation; do
    grep -q "${table}.modelo_llm:varchar(255):NO:NULL:[^,]*,${table}.modelo_llm_reportado:varchar(255):YES:NULL:" \
        <<<"${fresh_usage_shape}" \
        || { echo "TICKET04_MIGRATION → FAIL: fresh ${table}.modelo_llm_reportado" >&2; exit 1; }
done
echo "TICKET04_MIGRATION → PASS (fresh + repeat)"

# Merged ticket 03 predecessor with live revision data → current, repeated.
create_db t04_upgrade || exit $?
apply_file t04_upgrade "${workdir}/predecessor.sql" || exit $?
mysql_root t04_upgrade <<'SQL' || exit $?
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('upgrade','upgrade-ticket04','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key) VALUES (@agent,'u-1',UUID());
SET @session=LAST_INSERT_ID();
INSERT INTO conversation_turns (
 turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,
 status,session_slot,ready_at
) VALUES (UUID(),@agent,@session,'99999999-9999-4999-8999-999999999999',1,1,
 'pending','active',UTC_TIMESTAMP(6));
INSERT INTO turn_revisions (revision_key,turn_id,scheduler_epoch,revision_number,
 refresh_ordinal,status,draft_text,typing_delay_ms)
VALUES ('aaaaaaaa-0000-4000-8000-000000000004',LAST_INSERT_ID(),
 '99999999-9999-4999-8999-999999999999',1,0,'collecting','kept',3000);
SQL
seed_usage t04_upgrade || exit $?
apply_current t04_upgrade || exit $?
apply_current t04_upgrade || exit $?
assert_usage_converged t04_upgrade "${predecessor_ref} → current, repeated"
upgrade_shape="$(revision_shape t04_upgrade)" || exit $?
if [[ "${upgrade_shape}" != "${fresh_shape}" ]]; then
    echo "TICKET04_MIGRATION → FAIL: upgraded turn_revisions diverges from fresh" >&2
    printf 'fresh:   %s\nupgrade: %s\n' "${fresh_shape}" "${upgrade_shape}" >&2
    exit 1
fi
kept="$(mysql_root --batch --skip-column-names t04_upgrade --execute="
SELECT CONCAT(draft_text,':',typing_delay_ms,':',IFNULL(draft_kind,'NULL'),':',
 IFNULL(typing_jitter_ms,'NULL')) FROM turn_revisions;")" || exit $?
[[ "${kept}" == "kept:3000:NULL:NULL" ]] || { echo "TICKET04_MIGRATION → FAIL: data=${kept}" >&2; exit 1; }
echo "TICKET04_MIGRATION → PASS (${predecessor_ref} → current, repeated): exact convergence, data kept"

# Historical path: merged ticket 03 → first ticket 04 schema (v1 guard) →
# current v2 guard, with existing usage rows, repeated.
create_db t04_from_base || exit $?
apply_file t04_from_base "${workdir}/predecessor.sql" || exit $?
apply_file t04_from_base "${workdir}/ticket04_base.sql" || exit $?
seed_usage t04_from_base || exit $?
apply_current t04_from_base || exit $?
apply_current t04_from_base || exit $?
assert_usage_converged t04_from_base \
    "${predecessor_ref} → ${ticket04_base_ref} (v1) → current, repeated"
[[ "$(revision_shape t04_from_base)" == "${fresh_shape}" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: ${ticket04_base_ref} turn_revisions diverges" >&2; exit 1; }

# Interrupted between the two usage columns: only consumo_llm got it.
create_db t04_usage_partial || exit $?
apply_file t04_usage_partial "${workdir}/predecessor.sql" || exit $?
seed_usage t04_usage_partial || exit $?
mysql_root t04_usage_partial --execute="
ALTER TABLE consumo_llm ADD COLUMN modelo_llm_reportado VARCHAR(255) NULL AFTER modelo_llm;" || exit $?
apply_current t04_usage_partial || exit $?
assert_usage_converged t04_usage_partial "interrupted usage column add"

# Divergent reported-model shape fails closed and is not rewritten.
create_db t04_usage_divergent || exit $?
apply_file t04_usage_divergent "${workdir}/predecessor.sql" || exit $?
mysql_root t04_usage_divergent --execute="
ALTER TABLE consumo_llm ADD COLUMN modelo_llm_reportado VARCHAR(64) NULL AFTER modelo_llm;" || exit $?
expect_apply_failure t04_usage_divergent 'consumption reported model column requires audited repair' 'divergent reported model'
[[ "$(mysql_root --batch --skip-column-names t04_usage_divergent --execute="
SELECT column_type FROM information_schema.columns WHERE table_schema=DATABASE()
AND table_name='consumo_llm' AND column_name='modelo_llm_reportado';")" == "varchar(64)" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: divergent reported column rewritten" >&2; exit 1; }

# Interrupted after v2 is created while the historical v1 guard is still
# installed: both protect the table, then a re-run validates v2 before dropping v1.
create_db t04_interrupt || exit $?
apply_file t04_interrupt "${workdir}/predecessor.sql" || exit $?
apply_file t04_interrupt "${workdir}/ticket04_base.sql" || exit $?
awk '{ print } index($0, "ticket04-trigger-boundary: turn-revision-draft-v2-created") { found=1; exit }
     END { if (!found) exit 65 }' "${repo_root}/db/init.sql" > "${workdir}/prefix.sql" || exit $?
apply_file t04_interrupt "${workdir}/prefix.sql" || exit $?
[[ "$(mysql_root --batch --skip-column-names t04_interrupt --execute="
SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE()
AND trigger_name IN ('trg_turn_revision_draft_immutable_v1','trg_turn_revision_draft_immutable_v2');")" == "2" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: v1/v2 create boundary is unguarded" >&2; exit 1; }
apply_current t04_interrupt || exit $?
[[ "$(revision_shape t04_interrupt)" == "${fresh_shape}" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: interrupted run did not converge" >&2; exit 1; }
echo "TICKET04_MIGRATION → PASS (v1 + v2 create boundary): guarded and converged"

# Interrupted immediately after v1 is dropped: v2 remains present and the
# next complete run is idempotent.
create_db t04_interrupt_after_drop || exit $?
apply_file t04_interrupt_after_drop "${workdir}/predecessor.sql" || exit $?
apply_file t04_interrupt_after_drop "${workdir}/ticket04_base.sql" || exit $?
awk '{ print } index($0, "ticket04-trigger-boundary: turn-revision-draft-v1-dropped") { found=1; exit }
     END { if (!found) exit 65 }' "${repo_root}/db/init.sql" > "${workdir}/prefix-after-drop.sql" || exit $?
apply_file t04_interrupt_after_drop "${workdir}/prefix-after-drop.sql" || exit $?
[[ "$(mysql_root --batch --skip-column-names t04_interrupt_after_drop --execute="
SELECT CONCAT(
  SUM(trigger_name='trg_turn_revision_draft_immutable_v1'),':',
  SUM(trigger_name='trg_turn_revision_draft_immutable_v2'))
FROM information_schema.triggers WHERE trigger_schema=DATABASE();")" == "0:1" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: post-drop boundary lost v2 guard" >&2; exit 1; }
apply_current t04_interrupt_after_drop || exit $?
[[ "$(revision_shape t04_interrupt_after_drop)" == "${fresh_shape}" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: post-drop interrupted run did not converge" >&2; exit 1; }
echo "TICKET04_MIGRATION → PASS (v1 drop boundary): v2 present and converged"

# Divergent pre-existing column shape fails closed and is not rewritten.
create_db t04_divergent || exit $?
apply_file t04_divergent "${workdir}/predecessor.sql" || exit $?
mysql_root t04_divergent --execute="
ALTER TABLE turn_revisions ADD COLUMN draft_kind VARCHAR(32) NULL AFTER needs_handoff;" || exit $?
expect_apply_failure t04_divergent 'turn revision draft kind column requires audited repair' 'divergent draft_kind'
divergent_type="$(mysql_root --batch --skip-column-names t04_divergent --execute="
SELECT column_type FROM information_schema.columns WHERE table_schema=DATABASE()
AND table_name='turn_revisions' AND column_name='draft_kind';")" || exit $?
[[ "${divergent_type}" == "varchar(32)" ]] || { echo "TICKET04_MIGRATION → FAIL: divergent column rewritten" >&2; exit 1; }

# Correct enum values with divergent collation still fails exact shape.
create_db t04_divergent_collation || exit $?
apply_file t04_divergent_collation "${workdir}/predecessor.sql" || exit $?
mysql_root t04_divergent_collation --execute="
ALTER TABLE turn_revisions ADD COLUMN draft_kind
ENUM('generated','soft_limit_fallback','technical_fallback')
CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL AFTER needs_handoff;" || exit $?
expect_apply_failure t04_divergent_collation \
    'turn revision draft kind column requires audited repair' 'divergent draft_kind collation'
[[ "$(mysql_root --batch --skip-column-names t04_divergent_collation --execute="
SELECT collation_name FROM information_schema.columns WHERE table_schema=DATABASE()
AND table_name='turn_revisions' AND column_name='draft_kind';")" == "latin1_swedish_ci" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: divergent draft_kind collation rewritten" >&2; exit 1; }

# Correct enum/collation in the wrong ordinal position also fails closed.
create_db t04_divergent_ordinal || exit $?
apply_file t04_divergent_ordinal "${workdir}/predecessor.sql" || exit $?
mysql_root t04_divergent_ordinal --execute="
ALTER TABLE turn_revisions ADD COLUMN draft_kind
ENUM('generated','soft_limit_fallback','technical_fallback') NULL AFTER typing_delay_ms;" || exit $?
expect_apply_failure t04_divergent_ordinal \
    'turn revision draft kind column requires audited repair' 'divergent draft_kind ordinal'
ordinal_delta="$(mysql_root --batch --skip-column-names t04_divergent_ordinal --execute="
SELECT draft_kind.ordinal_position-needs_handoff.ordinal_position
FROM information_schema.columns draft_kind
JOIN information_schema.columns needs_handoff
 ON needs_handoff.table_schema=draft_kind.table_schema
AND needs_handoff.table_name=draft_kind.table_name
AND needs_handoff.column_name='needs_handoff'
WHERE draft_kind.table_schema=DATABASE() AND draft_kind.table_name='turn_revisions'
AND draft_kind.column_name='draft_kind';")" || exit $?
[[ "${ordinal_delta}" != "1" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: divergent draft_kind ordinal rewritten" >&2; exit 1; }

# Interrupted after the correctly positioned jitter column DDL autocommitted:
# current migration preserves data, adds the check/guard and remains idempotent.
create_db t04_jitter_interrupted || exit $?
apply_file t04_jitter_interrupted "${workdir}/predecessor.sql" || exit $?
mysql_root t04_jitter_interrupted <<'SQL' || exit $?
ALTER TABLE turn_revisions ADD COLUMN typing_jitter_ms SMALLINT NULL AFTER typing_delay_ms;
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('jitter-interrupted','jitter-interrupted-ticket04','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key) VALUES (@agent,'ji-1',UUID());
INSERT INTO conversation_turns (
 turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,
 status,session_slot,ready_at
) VALUES (UUID(),@agent,LAST_INSERT_ID(),'99999999-9999-4999-8999-999999999999',1,1,
 'pending','active',UTC_TIMESTAMP(6));
INSERT INTO turn_revisions (revision_key,turn_id,scheduler_epoch,revision_number,
 refresh_ordinal,status,typing_jitter_ms)
VALUES (UUID(),LAST_INSERT_ID(),'99999999-9999-4999-8999-999999999999',1,0,'collecting',321);
SQL
apply_current t04_jitter_interrupted || exit $?
apply_current t04_jitter_interrupted || exit $?
[[ "$(revision_shape t04_jitter_interrupted)" == "${fresh_shape}" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: interrupted jitter add did not converge" >&2; exit 1; }
[[ "$(mysql_root --batch --skip-column-names t04_jitter_interrupted \
    --execute='SELECT typing_jitter_ms FROM turn_revisions;')" == "321" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: interrupted jitter data rewritten" >&2; exit 1; }
echo "TICKET04_MIGRATION → PASS (interrupted correct jitter position + repeat): exact convergence, data kept"

# A correctly typed jitter column in the wrong ordinal position fails closed
# and is neither moved nor rewritten.
create_db t04_jitter_position || exit $?
apply_file t04_jitter_position "${workdir}/predecessor.sql" || exit $?
mysql_root t04_jitter_position <<'SQL' || exit $?
ALTER TABLE turn_revisions ADD COLUMN typing_jitter_ms SMALLINT NULL AFTER draft_text;
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('jitter-position','jitter-position-ticket04','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key) VALUES (@agent,'jp-1',UUID());
INSERT INTO conversation_turns (
 turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,
 status,session_slot,ready_at
) VALUES (UUID(),@agent,LAST_INSERT_ID(),'99999999-9999-4999-8999-999999999999',1,1,
 'pending','active',UTC_TIMESTAMP(6));
INSERT INTO turn_revisions (revision_key,turn_id,scheduler_epoch,revision_number,
 refresh_ordinal,status,typing_jitter_ms)
VALUES (UUID(),LAST_INSERT_ID(),'99999999-9999-4999-8999-999999999999',1,0,'collecting',321);
SQL
expect_apply_failure t04_jitter_position \
    'turn revision typing jitter column requires audited repair' 'divergent typing_jitter_ms ordinal'
jitter_delta="$(mysql_root --batch --skip-column-names t04_jitter_position --execute="
SELECT CAST(jitter.ordinal_position AS SIGNED)-CAST(delay_column.ordinal_position AS SIGNED)
FROM information_schema.columns jitter
JOIN information_schema.columns delay_column
 ON delay_column.table_schema=jitter.table_schema
AND delay_column.table_name=jitter.table_name
AND delay_column.column_name='typing_delay_ms'
WHERE jitter.table_schema=DATABASE() AND jitter.table_name='turn_revisions'
AND jitter.column_name='typing_jitter_ms';")" || exit $?
[[ "${jitter_delta}" != "1" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: divergent jitter position rewritten" >&2; exit 1; }
[[ "$(mysql_root --batch --skip-column-names t04_jitter_position \
    --execute='SELECT typing_jitter_ms FROM turn_revisions;')" == "321" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: divergent jitter data rewritten" >&2; exit 1; }

# Out-of-range persisted jitter fails closed and is not repaired.
create_db t04_jitter || exit $?
apply_file t04_jitter "${workdir}/predecessor.sql" || exit $?
mysql_root t04_jitter <<'SQL' || exit $?
ALTER TABLE turn_revisions ADD COLUMN typing_jitter_ms SMALLINT NULL AFTER typing_delay_ms;
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('jitter','jitter-ticket04','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key) VALUES (@agent,'j-1',UUID());
INSERT INTO conversation_turns (
 turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,
 status,session_slot,ready_at
) VALUES (UUID(),@agent,LAST_INSERT_ID(),'99999999-9999-4999-8999-999999999999',1,1,
 'pending','active',UTC_TIMESTAMP(6));
INSERT INTO turn_revisions (revision_key,turn_id,scheduler_epoch,revision_number,
 refresh_ordinal,status,typing_jitter_ms)
VALUES (UUID(),LAST_INSERT_ID(),'99999999-9999-4999-8999-999999999999',1,0,'collecting',5000);
SQL
expect_apply_failure t04_jitter 'turn revision typing jitter range requires audited repair' 'out-of-range jitter'
[[ "$(mysql_root --batch --skip-column-names t04_jitter --execute='SELECT typing_jitter_ms FROM turn_revisions;')" == "5000" ]] \
    || { echo "TICKET04_MIGRATION → FAIL: jitter silently repaired" >&2; exit 1; }

# A same-named but weakened guard is never accepted.
create_db t04_tamper || exit $?
apply_current t04_tamper || exit $?
mysql_root t04_tamper <<'SQL' || exit $?
DROP TRIGGER trg_turn_revision_draft_immutable_v2;
CREATE TRIGGER trg_turn_revision_draft_immutable_v2 BEFORE UPDATE ON turn_revisions
FOR EACH ROW SET @ticket04_weakened = 1;
SQL
expect_apply_failure t04_tamper 'immutability guard definition requires audited repair' 'weakened draft guard'

# Exact shape of an additive column plus the table's column order with the
# other ticket 04 additive columns filtered out: a failed run may add earlier
# additive columns, but the audited column must keep its place among the
# pre-existing ones and its type/nullability/default/collation.
column_shape() {
    mysql_root --batch --skip-column-names "$1" --execute="
SET SESSION group_concat_max_len = 1048576;
SELECT CONCAT_WS('|',
 (SELECT CONCAT_WS(':',column_type,is_nullable,IFNULL(column_default,'NULL'),
         IFNULL(collation_name,'-'))
  FROM information_schema.columns
  WHERE table_schema=DATABASE() AND table_name='$2' AND column_name='$3'),
 (SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position)
  FROM information_schema.columns
  WHERE table_schema=DATABASE() AND table_name='$2'
    AND (column_name='$3' OR column_name NOT IN
         ('draft_kind','typing_jitter_ms','modelo_llm_reportado'))));" </dev/null
}

# Full-shape audit (type, nullability, default, collation) of every ticket 04
# additive column: each divergent pre-existing shape fails closed with its
# named error and the column is left exactly as found.
shape_case=0
while IFS='|' read -r -u 3 table column ddl expected; do
    [[ -n "${table}" ]] || continue
    shape_case=$((shape_case + 1))
    database="t04_shape_${shape_case}"
    create_db "${database}" || exit $?
    apply_file "${database}" "${workdir}/predecessor.sql" || exit $?
    mysql_root "${database}" --execute="ALTER TABLE ${table} ADD COLUMN ${column} ${ddl};" || exit $?
    before="$(column_shape "${database}" "${table}" "${column}")" || exit $?
    expect_apply_failure "${database}" "${expected}" "divergent ${table}.${column} ${ddl}"
    [[ "$(column_shape "${database}" "${table}" "${column}")" == "${before}" ]] \
        || { echo "TICKET04_MIGRATION → FAIL: divergent ${table}.${column} rewritten" >&2; exit 1; }
done 3<<'CASES'
turn_revisions|typing_jitter_ms|INT NULL AFTER typing_delay_ms|turn revision typing jitter column requires audited repair
turn_revisions|typing_jitter_ms|SMALLINT NOT NULL DEFAULT 0 AFTER typing_delay_ms|turn revision typing jitter column requires audited repair
turn_revisions|typing_jitter_ms|SMALLINT NULL DEFAULT 0 AFTER typing_delay_ms|turn revision typing jitter column requires audited repair
turn_revisions|typing_jitter_ms|SMALLINT NULL AFTER needs_handoff|turn revision typing jitter column requires audited repair
turn_revisions|typing_jitter_ms|SMALLINT NULL AFTER typing_deadline_at|turn revision typing jitter column requires audited repair
turn_revisions|draft_kind|ENUM('generated','soft_limit_fallback','technical_fallback') NOT NULL DEFAULT 'generated' AFTER needs_handoff|turn revision draft kind column requires audited repair
turn_revisions|draft_kind|ENUM('generated','soft_limit_fallback','technical_fallback') NULL DEFAULT 'generated' AFTER needs_handoff|turn revision draft kind column requires audited repair
consumo_llm|modelo_llm_reportado|VARCHAR(255) NOT NULL DEFAULT '' AFTER modelo_llm|consumption reported model column requires audited repair
consumo_llm|modelo_llm_reportado|VARCHAR(255) NULL DEFAULT 'x' AFTER modelo_llm|consumption reported model column requires audited repair
consumo_llm|modelo_llm_reportado|VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL AFTER modelo_llm|consumption reported model column requires audited repair
llm_usage_reconciliation|modelo_llm_reportado|VARCHAR(64) NULL AFTER modelo_llm|usage reconciliation reported model column requires audited repair
llm_usage_reconciliation|modelo_llm_reportado|VARCHAR(255) NOT NULL DEFAULT '' AFTER modelo_llm|usage reconciliation reported model column requires audited repair
llm_usage_reconciliation|modelo_llm_reportado|VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL AFTER modelo_llm|usage reconciliation reported model column requires audited repair
CASES
echo "TICKET04_MIGRATION → PASS (full-shape audit): ${shape_case} divergent shapes failed closed, none rewritten"

# Migration fix-removal proofs: each exact-position predicate is load-bearing.
# Control: the real migration fails closed on a correctly typed column in the
# wrong ordinal position and leaves it in place. Mutant: the same migration
# without that one predicate accepts the fixture.
migration_mutations=0
while read -r -u 3 mutation table column expected; do
    [[ -n "${mutation}" ]] || continue
    fixture="$(python "${repo_root}/scripts/inbox_ticket04_mutations.py" migration-fixture "${mutation}")" \
        || exit $?
    for role in control mutant; do
        create_db "t04_mm_${migration_mutations}_${role}" || exit $?
        apply_file "t04_mm_${migration_mutations}_${role}" "${workdir}/predecessor.sql" || exit $?
        mysql_root "t04_mm_${migration_mutations}_${role}" --execute="${fixture}" || exit $?
    done
    control="t04_mm_${migration_mutations}_control"
    mutant="t04_mm_${migration_mutations}_mutant"
    before="$(column_shape "${control}" "${table}" "${column}")" || exit $?
    expect_apply_failure "${control}" "${expected}" "misplaced ${table}.${column}"
    [[ "$(column_shape "${control}" "${table}" "${column}")" == "${before}" ]] \
        || { echo "TICKET04_MIGRATION → FAIL: misplaced ${table}.${column} moved" >&2; exit 1; }
    cp "${repo_root}/db/init.sql" "${workdir}/${mutation}.sql" || exit $?
    python "${repo_root}/scripts/inbox_ticket04_mutations.py" apply-migration \
        "${workdir}/${mutation}.sql" "${mutation}" || exit $?
    if ! apply_file "${mutant}" "${workdir}/${mutation}.sql" >"${workdir}/${mutation}.log" 2>&1; then
        echo "MIGRATION_MUTATION → FAIL (${mutation}): fixture still rejected without the predicate" >&2
        tail -n 20 "${workdir}/${mutation}.log" >&2
        exit 1
    fi
    echo "MIGRATION_MUTATION → PASS (${mutation}): misplaced ${table}.${column} accepted only without the predicate"
    migration_mutations=$((migration_mutations + 1))
done 3< <(python "${repo_root}/scripts/inbox_ticket04_mutations.py" list-migration)
(( migration_mutations == 4 )) || { echo "MIGRATION_MUTATIONS → FAIL: ${migration_mutations}/4" >&2; exit 1; }
echo "TICKET04_MIGRATION_MUTATIONS → PASS: ${migration_mutations}/${migration_mutations} position predicates load-bearing"
echo "TICKET04_MIGRATIONS → PASS"

# ── 3. Fix-removal mutation proofs ─────────────────────────────────────
if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?
create_db t04_mutation || exit $?
apply_current t04_mutation || exit $?
mysql_root --execute="
CREATE USER 'ticket04_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT,INSERT,UPDATE,DELETE ON t04_mutation.* TO 'ticket04_app'@'%';
" || exit $?

run_tests() {
    local root=$1
    shift
    docker run --rm \
        --network="container:${container}" \
        --volume="${root}:/workspace:ro" \
        --tmpfs=/app/logs --tmpfs=/runtime --tmpfs=/tmp/pycache \
        --env PYTHONPYCACHEPREFIX=/tmp/pycache --env WPP_REPO_ROOT=/workspace \
        --workdir=/runtime --env PYTHONPATH=/workspace/app \
        --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://ticket04_app:${app_password}@127.0.0.1:3306/t04_mutation" \
        --env "DATABASE_URL=mysql+aiomysql://ticket04_app:${app_password}@127.0.0.1:3306/t04_mutation" \
        --env OPENROUTER_API_KEY=synthetic-ticket04-key \
        --env EVOLUTION_API_URL=http://evolution.invalid \
        --env EVOLUTION_API_KEY=synthetic-ticket04-key \
        --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic-ticket04-bearer \
        --env CHATWOOT_BASE_URL=http://chatwoot.invalid \
        --env CHATWOOT_API_TOKEN=synthetic-ticket04-token \
        --env CHATWOOT_ACCOUNT_ID=1 \
        --env CHATWOOT_WEBHOOK_SECRET=synthetic-ticket04-secret \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" timeout 180 python -m unittest "$@"
}

mutations=0
while read -r -u 3 mutation test_name; do
    [[ -n "${mutation}" ]] || continue
    copy="${workdir}/${mutation}"
    mkdir -p "${copy}"
    tar --exclude=__pycache__ --exclude=logs -C "${repo_root}" -cf - app \
        | tar -C "${copy}" -xf - || exit $?
    mkdir -p "${copy}/app/logs"
    python "${repo_root}/scripts/inbox_ticket04_mutations.py" apply "${copy}" "${mutation}" || exit $?
    if [[ "${mutation}" == "admitted_revision_terminal" ]]; then
        mysql_root t04_mutation < "${copy}/ticket04_weaken_admitted_guard.sql" || exit $?
    fi
    mutation_unexpected_pass=0
    if run_tests "${copy}" "${test_name}" >"${workdir}/${mutation}.log" 2>&1; then
        mutation_unexpected_pass=1
    fi
    if [[ "${mutation}" == "admitted_revision_terminal" ]]; then
        mysql_root t04_mutation \
            --execute="DROP TRIGGER trg_turn_revision_draft_immutable_v2;" || exit $?
        apply_current t04_mutation || exit $?
    fi
    if (( mutation_unexpected_pass == 1 )); then
        echo "MUTATION → FAIL (${mutation}): ${test_name##*.} still passed without the fix" >&2
        tail -n 40 "${workdir}/${mutation}.log" >&2
        exit 1
    fi
    echo "MUTATION → PASS (${mutation}): ${test_name##*.} fails without the fix"
    mutations=$((mutations + 1))
    rm -rf "${copy}"
done 3< <(python "${repo_root}/scripts/inbox_ticket04_mutations.py" list)
(( mutations > 0 )) || exit 1
echo "TICKET04_MUTATIONS → PASS: ${mutations}/${mutations} protections proven load-bearing"

# Unmutated control on the same database: the targeted tests pass.
run_tests "${repo_root}" tests.test_turn_processor_mysql tests.test_turn_processor_unit \
    >"${workdir}/control.log" 2>&1 || { tail -n 60 "${workdir}/control.log" >&2; exit 1; }
grep -E '^Ran [0-9]+ tests?' "${workdir}/control.log"
echo "TICKET04_CONTROL → PASS: unmutated processor tests pass on the mutation database"

pycache_dir="${workdir}/pycache"
PYTHONPYCACHEPREFIX="${pycache_dir}" python -m py_compile \
    "${repo_root}/app/src/agents/agent_runner.py" \
    "${repo_root}/app/src/config.py" \
    "${repo_root}/app/src/database/models.py" \
    "${repo_root}/app/src/database/db_errors.py" \
    "${repo_root}/app/src/database/repositories/consumo_repo.py" \
    "${repo_root}/app/src/database/repositories/usage_reconciliation_repo.py" \
    "${repo_root}/app/src/database/repositories/scheduler_repo.py" \
    "${repo_root}/app/src/database/repositories/turn_generation_repo.py" \
    "${repo_root}/app/src/services/llm_client.py" \
    "${repo_root}/app/src/services/turn_processor.py" \
    "${repo_root}/app/src/turn_worker.py" \
    "${repo_root}/app/src/delivery_worker.py" \
    "${repo_root}/app/tests/test_turn_processor_mysql.py" \
    "${repo_root}/app/tests/test_turn_processor_unit.py" \
    "${repo_root}/scripts/inbox_ticket04_mutations.py" \
    "${repo_root}/scripts/check_trigger_body_digests.py" || exit $?
bash -n "${repo_root}/scripts/run_inbox_ticket04_mysql_matrix.sh" || exit $?
echo "TICKET04_MATRIX → PASS"
