#!/usr/bin/env bash
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
suffix="${TICKET03_MIGRATION_SUFFIX:-$(date +%s)-$$-$RANDOM}"
container="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket03-migration-mysql-${suffix}"
network="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket03-migration-net-${suffix}"
volume="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket03-migration-data-${suffix}"
shared_project="traycer-wpp-ai-bot-soft-ocelot"
legacy_schema="$(mktemp)"
trigger_prefix="$(mktemp)"
root_password="$(python - <<'PY'
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
    rm -f "${legacy_schema}" "${trigger_prefix}"
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if [[ "$(resource_count)" != "0" ]]; then
        echo "TICKET03_MIGRATION_CLEANUP → FAIL: disposable resource remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != "0" ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        echo "TICKET03_MIGRATION_CLEANUP → PASS: disposable resources absent"
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

# Static, MySQL-free: every _v2 digest constant equals SHA-256 of its exact
# BEGIN...END body; self-test proves one-byte body and stale-constant drift fail.
python "${repo_root}/scripts/check_trigger_body_digests.py" "${repo_root}/db/init.sql" || exit $?
python "${repo_root}/scripts/check_trigger_body_digests.py" --self-test "${repo_root}/db/init.sql" || exit $?

git -C "${repo_root}" show 16626cd:db/init.sql > "${legacy_schema}" || exit $?
docker network create --label wpp.ticket03.migration=true "${network}" >/dev/null || exit $?
docker volume create --label wpp.ticket03.migration=true "${volume}" >/dev/null || exit $?
docker run --detach \
    --name "${container}" \
    --label wpp.ticket03.migration=true \
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

apply_current() {
    local database=$1
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "${database}" < "${repo_root}/db/init.sql"
}

# Current versioned successor of each ticket03 guard. Ticket 06 replaced the
# outbox authority guard (v2 → v3, create-before-drop); the others stay v2.
full_marker() {
    case "$1" in
        *trigger-boundary:*) echo "$1" ;;
        *) echo "ticket03-trigger-boundary: $1" ;;
    esac
}

successor_of() {
    case "$1" in
        trg_outbox_authority_immutable) echo "${1}_v3" ;;
        *) echo "${1}_v2" ;;
    esac
}

apply_legacy() {
    local database=$1
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "${database}" < "${legacy_schema}"
}

apply_through_trigger_boundary() {
    local database=$1 marker=$2
    awk -v marker="${marker}" '
        { print }
        index($0, marker) { found=1; exit }
        END { if (!found) exit 65 }
    ' "${repo_root}/db/init.sql" > "${trigger_prefix}" || return $?
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "${database}" < "${trigger_prefix}"
}

assert_trigger_probe_rejected() {
    local database=$1 trigger_base=$2 phase=$3 match_mode=${4:-exact}
    local sql expected output rc guard_count
    case "${trigger_base}" in
        trg_inbound_event_disposition_immutable)
            sql="UPDATE inbound_events SET receipt_disposition='ignored_nonbot' WHERE provider_event_id='trigger-probe';"
            expected='inbound receipt disposition is immutable'
            ;;
        trg_outbox_authority_immutable)
            sql="UPDATE outbox_deliveries SET delivery_class='legacy_automated' WHERE dedupe_key='trigger-probe';"
            expected='outbox delivery authority is immutable'
            ;;
        trg_turn_revision_seal_immutable)
            sql="UPDATE turn_revisions SET sealed_at=sealed_at + INTERVAL 1 SECOND WHERE revision_key='22222222-2222-4222-8222-222222222222';"
            expected='sealed revision input range is immutable'
            ;;
        trg_turn_input_identity_immutable)
            sql="UPDATE turn_input_memberships SET content_bytes=content_bytes + 1 WHERE inbound_event_id=(SELECT id FROM inbound_events WHERE provider_event_id='trigger-probe');"
            expected='turn input identity is immutable'
            ;;
        trg_conversation_turn_identity_immutable)
            sql="UPDATE conversation_turns SET turn_key='99999999-9999-4999-8999-999999999999' WHERE turn_key='11111111-1111-4111-8111-111111111111';"
            expected='conversation turn identity is immutable'
            ;;
        trg_generation_attempt_identity_immutable)
            sql="UPDATE generation_attempts SET attempt_key='99999999-9999-4999-8999-999999999998' WHERE attempt_key='33333333-3333-4333-8333-333333333333';"
            expected='generation attempt identity is immutable'
            ;;
        *)
            echo "Unknown trigger probe: ${trigger_base}" >&2
            exit 1
            ;;
    esac
    guard_count="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT COUNT(*) FROM information_schema.triggers
WHERE trigger_schema=DATABASE()
 AND trigger_name IN ('${trigger_base}','$(successor_of "${trigger_base}")');
")" || exit $?
    if (( guard_count < 1 )); then
        echo "TRIGGER_BOUNDARY → FAIL (${phase}): no guard" >&2
        exit 1
    fi
    output="$(mysql_root "${database}" --execute="${sql}" 2>&1)"
    rc=$?
    if (( rc == 0 )) || {
        [[ "${match_mode}" == "exact" ]] && ! grep -q "${expected}" <<<"${output}"
    }; then
        echo "TRIGGER_BOUNDARY → FAIL (${phase}): mutation was not rejected by guard" >&2
        printf '%s\n' "${output}" >&2
        exit 1
    fi
    echo "TRIGGER_BOUNDARY → PASS (${phase}): guards=${guard_count}, mutation rejected"
    assert_branch_probe_rejected "${database}" "${trigger_base}" "${phase}" "${match_mode}"
}

# Each guard's semantically sensitive predicate: the 'automated' and 'started'
# literals, the sealed_at IS NOT NULL gate, the monotonic input_version
# comparison, and NULL-safe identity comparisons not covered by the identity
# probe above. branch_tamper_body weakens exactly the predicate probed here.
branch_probe_sql() {
    case "$1" in
        trg_inbound_event_disposition_immutable)
            echo "UPDATE inbound_events SET receipt_disposition='legacy_unknown' WHERE provider_event_id='trigger-probe';" ;;
        trg_outbox_authority_immutable)
            echo "UPDATE outbox_deliveries SET cutover_epoch='88888888-8888-4888-8888-888888888888' WHERE dedupe_key='trigger-probe-automated';" ;;
        trg_turn_revision_seal_immutable)
            echo "UPDATE turn_revisions SET sealed_input_bytes=sealed_input_bytes + 1 WHERE revision_key='22222222-2222-4222-8222-222222222222';" ;;
        trg_turn_input_identity_immutable)
            echo "UPDATE turn_input_memberships SET input_sequence=input_sequence + 1 WHERE inbound_event_id=(SELECT id FROM inbound_events WHERE provider_event_id='trigger-probe');" ;;
        trg_conversation_turn_identity_immutable)
            echo "UPDATE conversation_turns SET input_version=input_version - 1 WHERE turn_key='11111111-1111-4111-8111-111111111111';" ;;
        trg_generation_attempt_identity_immutable)
            echo "UPDATE generation_attempts SET provider_generation_id='branch-probe-generation' WHERE attempt_key='33333333-3333-4333-8333-333333333333' AND outcome='started';" ;;
        *) echo "Unknown branch probe: $1" >&2; exit 1 ;;
    esac
}

branch_probe_expected() {
    case "$1" in
        trg_inbound_event_disposition_immutable) echo 'inbound receipt disposition is immutable' ;;
        trg_outbox_authority_immutable) echo 'automated delivery ownership is immutable' ;;
        trg_turn_revision_seal_immutable) echo 'sealed revision input range is immutable' ;;
        trg_turn_input_identity_immutable) echo 'turn input identity is immutable' ;;
        trg_conversation_turn_identity_immutable) echo 'conversation turn identity is immutable' ;;
        trg_generation_attempt_identity_immutable) echo 'generation attempt outcome fields require terminal transition' ;;
        *) echo "Unknown branch probe: $1" >&2; exit 1 ;;
    esac
}

assert_branch_probe_rejected() {
    local database=$1 trigger_base=$2 phase=$3 match_mode=${4:-exact}
    local sql expected output rc
    sql="$(branch_probe_sql "${trigger_base}")"
    expected="$(branch_probe_expected "${trigger_base}")"
    output="$(mysql_root "${database}" --execute="${sql}" 2>&1)"
    rc=$?
    if (( rc == 0 )) || {
        [[ "${match_mode}" == "exact" ]] && ! grep -qF "${expected}" <<<"${output}"
    }; then
        echo "TRIGGER_BRANCH → FAIL (${phase}): ${trigger_base} branch mutation was not rejected" >&2
        printf '%s\n' "${output}" >&2
        exit 1
    fi
    echo "TRIGGER_BRANCH → PASS (${phase}): ${trigger_base} branch mutation rejected"
}

# Proves the branch probe is sensitive: with only the weakened guard installed,
# the same mutation is admitted (then rolled back, leaving fixture data intact).
assert_branch_probe_admitted_by_weakened_guard() {
    local database=$1 trigger_base=$2 sql output
    sql="$(branch_probe_sql "${trigger_base}")"
    output="$(mysql_root --batch --skip-column-names "${database}" --execute="
START TRANSACTION;
${sql}
SELECT ROW_COUNT();
ROLLBACK;
" 2>&1)" || {
        echo "TRIGGER_BRANCH_ATTACK → FAIL (${trigger_base}): weakened guard still rejected probe; probe is not branch-sensitive" >&2
        printf '%s\n' "${output}" >&2
        exit 1
    }
    [[ "${output}" == "1" ]] || {
        echo "TRIGGER_BRANCH_ATTACK → FAIL (${trigger_base}): probe matched ${output} rows, expected 1" >&2
        exit 1
    }
}

install_predecessor_only() {
    local database=$1 trigger_base=$2 table_name action_statement
    table_name="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT event_object_table FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='$(successor_of "${trigger_base}")';
")" || exit $?
    action_statement="$(mysql_root --batch --raw --skip-column-names "${database}" --execute="
SELECT action_statement FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='$(successor_of "${trigger_base}")';
")" || exit $?
    [[ -n "${table_name}" && -n "${action_statement}" ]] || {
        echo "TRIGGER_BOUNDARY → FAIL: missing v2 source for ${trigger_base}" >&2
        exit 1
    }
    mysql_root "${database}" <<SQL || exit $?
DELIMITER \$\$
CREATE TRIGGER ${trigger_base}
BEFORE UPDATE ON ${table_name}
FOR EACH ROW
${action_statement}\$\$
DROP TRIGGER $(successor_of "${trigger_base}")\$\$
DELIMITER ;
SQL
}

install_tampered_v2_with_predecessor() {
    local database=$1 trigger_base=$2 variant=$3 table_name canonical_body tampered_body
    table_name="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT event_object_table FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='$(successor_of "${trigger_base}")';
")" || exit $?
    canonical_body="$(mysql_root --batch --raw --skip-column-names "${database}" --execute="
SELECT action_statement FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='$(successor_of "${trigger_base}")';
")" || exit $?
    [[ -n "${table_name}" && -n "${canonical_body}" ]] || {
        echo "TRIGGER_DEFINITION → FAIL: missing canonical body for ${trigger_base}" >&2
        exit 1
    }
    install_predecessor_only "${database}" "${trigger_base}"
    tampered_body="$(TRIGGER_BODY="${canonical_body}" python - "${trigger_base}" "${variant}" <<'PY'
import os
import re
import sys

trigger, variant = sys.argv[1:]
body = os.environ["TRIGGER_BODY"]
if variant == "literal":
    semantic = {
        "trg_outbox_authority_immutable": ("'automated'", "'auto mated'"),
        "trg_generation_attempt_identity_immutable": ("'started'", "'sta rted'"),
    }
    if trigger in semantic:
        before, after = semantic[trigger]
        changed = body.replace(before, after, 1)
    else:
        changed = re.sub(
            r"(SET MESSAGE_TEXT = ')([^'])([^']*)'",
            lambda match: match.group(1) + match.group(2).swapcase() + match.group(3) + "'",
            body,
            count=1,
        )
elif variant == "identifier":
    changed = re.sub(
        r"OLD\.([a-z_]+)",
        lambda match: "OLD." + match.group(1).upper(),
        body,
        count=1,
    )
elif variant == "operator":
    changed = body.replace("<=>", "=", 1)
elif variant == "branch":
    # Weaken only the predicate that the matching branch probe exercises.
    weakened = {
        "trg_inbound_event_disposition_immutable": (
            "OLD.receipt_disposition <=> NEW.receipt_disposition",
            "OLD.receipt_disposition <=> OLD.receipt_disposition",
        ),
        "trg_outbox_authority_immutable": (
            "IF (OLD.delivery_class = 'automated' OR NEW.delivery_class = 'automated') AND (",
            "IF (OLD.delivery_class = 'auto mated' OR NEW.delivery_class = 'auto mated') AND (",
        ),
        "trg_turn_revision_seal_immutable": (
            "IF OLD.sealed_at IS NOT NULL AND (",
            "IF OLD.sealed_at IS NULL AND (",
        ),
        "trg_turn_input_identity_immutable": (
            "OLD.input_sequence <=> NEW.input_sequence",
            "OLD.input_sequence <=> OLD.input_sequence",
        ),
        "trg_conversation_turn_identity_immutable": (
            "NEW.input_version < OLD.input_version",
            "NEW.input_version < NEW.input_version",
        ),
        "trg_generation_attempt_identity_immutable": (
            "IF OLD.outcome = 'started' AND NEW.outcome = 'started' AND (",
            "IF OLD.outcome = 'sta rted' AND NEW.outcome = 'started' AND (",
        ),
    }
    before, after = weakened[trigger]
    if body.count(before) != 1:
        raise SystemExit(f"branch predicate for {trigger} is not unique")
    changed = body.replace(before, after, 1)
else:
    raise SystemExit(f"unknown tamper variant: {variant}")
if changed == body:
    raise SystemExit(f"tamper variant {variant} made no change to {trigger}")
sys.stdout.write(changed)
PY
)" || exit $?
    mysql_root "${database}" <<SQL || exit $?
DELIMITER \$\$
CREATE TRIGGER $(successor_of "${trigger_base}")
BEFORE UPDATE ON ${table_name}
FOR EACH ROW
${tampered_body}\$\$
DELIMITER ;
SQL
}

install_metadata_v2_with_predecessor() {
    local database=$1 trigger_base=$2 table_name canonical_body
    table_name="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT event_object_table FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='$(successor_of "${trigger_base}")';
")" || exit $?
    canonical_body="$(mysql_root --batch --raw --skip-column-names "${database}" --execute="
SELECT action_statement FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='$(successor_of "${trigger_base}")';
")" || exit $?
    [[ -n "${table_name}" && -n "${canonical_body}" ]] || {
        echo "TRIGGER_METADATA → FAIL: missing canonical body for ${trigger_base}" >&2
        exit 1
    }
    install_predecessor_only "${database}" "${trigger_base}"
    mysql_root "${database}" <<SQL || exit $?
SET SESSION sql_mode='ANSI_QUOTES,NO_BACKSLASH_ESCAPES';
SET NAMES latin1 COLLATE latin1_swedish_ci;
DELIMITER \$\$
CREATE DEFINER='ticket03_guard'@'%'
TRIGGER $(successor_of "${trigger_base}")
BEFORE UPDATE ON ${table_name}
FOR EACH ROW
${canonical_body}\$\$
DELIMITER ;
SQL
}

mysql_root --execute="
CREATE DATABASE ticket03_column CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_unique_only CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_wrong_index CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_missing_fk CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_wrong_fk CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_duplicate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_orphan CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_wrong_column CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_wrong_type CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_wrong_range CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_cross_schema CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_disposition CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE ticket03_trigger_boundaries CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE other_ref CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
" || exit $?

for database in \
    ticket03_column ticket03_unique_only ticket03_wrong_index ticket03_missing_fk \
    ticket03_wrong_fk ticket03_duplicate ticket03_orphan; do
    apply_legacy "${database}" || exit $?
done
for database in \
    ticket03_wrong_column ticket03_wrong_type ticket03_wrong_range ticket03_cross_schema \
    ticket03_disposition ticket03_trigger_boundaries; do
    apply_legacy "${database}" || exit $?
done

mysql_root ticket03_column --execute="
ALTER TABLE inbound_events ADD COLUMN mensagem_id INT NULL AFTER sessao_id;
" || exit $?
mysql_root ticket03_unique_only --execute="
ALTER TABLE inbound_events
 ADD COLUMN mensagem_id INT NULL AFTER sessao_id,
 ADD UNIQUE INDEX uq_inbound_event_mensagem (mensagem_id);
" || exit $?
mysql_root ticket03_wrong_index --execute="
ALTER TABLE inbound_events
 ADD COLUMN mensagem_id INT NULL AFTER sessao_id,
 ADD UNIQUE INDEX uq_inbound_event_mensagem (sessao_id, mensagem_id);
" || exit $?

apply_current ticket03_missing_fk || exit $?
mysql_root ticket03_missing_fk --execute="
ALTER TABLE inbound_events DROP FOREIGN KEY fk_inbound_event_mensagem;
" || exit $?
apply_current ticket03_wrong_fk || exit $?
mysql_root ticket03_wrong_fk --execute="
ALTER TABLE inbound_events DROP FOREIGN KEY fk_inbound_event_mensagem;
ALTER TABLE inbound_events ADD CONSTRAINT fk_inbound_event_mensagem
 FOREIGN KEY (mensagem_id) REFERENCES mensagens(id) ON DELETE SET NULL;
" || exit $?

mysql_root ticket03_wrong_column --execute="
ALTER TABLE inbound_events
 ADD COLUMN mensagem_id INT NOT NULL DEFAULT 0 AFTER sessao_id;
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version
) VALUES (
 'evolution','wrong-column','sentinel-zero','messages.upsert',REPEAT('1',64),1
);
" || exit $?
mysql_root ticket03_wrong_type --execute="
ALTER TABLE inbound_events
 ADD COLUMN mensagem_id BIGINT UNSIGNED NULL DEFAULT NULL AFTER sessao_id;
" || exit $?
mysql_root ticket03_wrong_range --execute="
ALTER TABLE inbound_events
 ADD COLUMN mensagem_id BIGINT UNSIGNED NULL DEFAULT NULL AFTER sessao_id;
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,mensagem_id
) VALUES (
 'evolution','wrong-range','out-of-range','messages.upsert',REPEAT('5',64),1,2147483648
);
" || exit $?
mysql_root other_ref --execute="
CREATE TABLE mensagens (id INT PRIMARY KEY);
" || exit $?
mysql_root ticket03_cross_schema --execute="
ALTER TABLE inbound_events
 ADD COLUMN mensagem_id INT NULL DEFAULT NULL AFTER sessao_id,
 ADD UNIQUE INDEX uq_inbound_event_mensagem (mensagem_id),
 ADD CONSTRAINT fk_inbound_event_mensagem
 FOREIGN KEY (mensagem_id) REFERENCES other_ref.mensagens(id)
 ON DELETE RESTRICT ON UPDATE RESTRICT;
" || exit $?
mysql_root ticket03_disposition --execute="
ALTER TABLE inbound_events ADD COLUMN mensagem_id INT NULL AFTER sessao_id;
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('disposition','ticket03-disposition','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key)
VALUES (@agent,'ticket03-disposition',UUID());
SET @session=LAST_INSERT_ID();
INSERT INTO mensagens (sessao_id,role,conteudo)
VALUES (@session,'user','scheduled proof');
SET @message=LAST_INSERT_ID();
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,
 agente_id,sessao_id,mensagem_id,processed_at
) VALUES
('evolution','disposition','proven-scheduled','messages.upsert',REPEAT('2',64),1,@agent,@session,@message,UTC_TIMESTAMP(6)),
('evolution','disposition','ambiguous-legacy','messages.upsert',REPEAT('3',64),1,@agent,@session,NULL,UTC_TIMESTAMP(6)),
('chatwoot','1','proven-chatwoot','message_created',REPEAT('4',64),1,@agent,@session,NULL,UTC_TIMESTAMP(6));
" || exit $?

for database in ticket03_duplicate ticket03_orphan; do
    mysql_root "${database}" --execute="
ALTER TABLE inbound_events ADD COLUMN mensagem_id INT NULL AFTER sessao_id;
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('migration-negative','${database}','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key)
VALUES (@agent,'${database}',UUID());
SET @session=LAST_INSERT_ID();
INSERT INTO mensagens (sessao_id,role,conteudo) VALUES (@session,'user','one');
SET @message=LAST_INSERT_ID();
" || exit $?
done
mysql_root ticket03_duplicate --execute="
SET @agent=(SELECT id FROM agentes WHERE instance_name='ticket03_duplicate');
SET @session=(SELECT id FROM sessoes WHERE telefone='ticket03_duplicate');
SET @message=(SELECT id FROM mensagens WHERE sessao_id=@session LIMIT 1);
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,
 agente_id,sessao_id,mensagem_id,processed_at
) VALUES
('evolution','duplicate','duplicate-1','messages.upsert',REPEAT('a',64),1,@agent,@session,@message,UTC_TIMESTAMP(6)),
('evolution','duplicate','duplicate-2','messages.upsert',REPEAT('b',64),1,@agent,@session,@message,UTC_TIMESTAMP(6));
" || exit $?
mysql_root ticket03_orphan --execute="
SET @agent=(SELECT id FROM agentes WHERE instance_name='ticket03_orphan');
SET @session=(SELECT id FROM sessoes WHERE telefone='ticket03_orphan');
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,
 agente_id,sessao_id,mensagem_id,processed_at
) VALUES
('evolution','orphan','orphan-1','messages.upsert',REPEAT('c',64),1,@agent,@session,999999999,UTC_TIMESTAMP(6));
" || exit $?

# First-install interruptions stop immediately after each versioned CREATE.
# Later, predecessor replacement interruptions stop after CREATE and after DROP.
apply_through_trigger_boundary \
    ticket03_trigger_boundaries \
    'ticket03-trigger-boundary: inbound-disposition-v2-created' || exit $?
mysql_root ticket03_trigger_boundaries <<'SQL' || exit $?
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('trigger-boundary','ticket03-trigger-boundary','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key)
VALUES (@agent,'ticket03-trigger-boundary',UUID());
SET @session=LAST_INSERT_ID();
INSERT INTO mensagens (sessao_id,role,conteudo)
VALUES (@session,'user','trigger boundary');
SET @message=LAST_INSERT_ID();
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,
 agente_id,sessao_id,mensagem_id,receipt_disposition,processed_at
) VALUES (
 'evolution','trigger-boundary','trigger-probe','messages.upsert',REPEAT('7',64),1,
 @agent,@session,@message,'scheduled',UTC_TIMESTAMP(6)
);
SET @receipt=LAST_INSERT_ID();
INSERT INTO conversation_turns (
 turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,
 status,session_slot,ready_at,input_version
) VALUES (
 '11111111-1111-4111-8111-111111111111',@agent,@session,
 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',1,1,'pending','active',UTC_TIMESTAMP(6),2
);
SET @turn=LAST_INSERT_ID();
INSERT INTO turn_revisions (
 revision_key,turn_id,scheduler_epoch,revision_number,refresh_ordinal,status,
 sealed_first_input_sequence,sealed_last_input_sequence,sealed_input_count,
 sealed_input_bytes,sealed_at
) VALUES (
 '22222222-2222-4222-8222-222222222222',@turn,
 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',1,0,'final_candidate',1,1,1,16,UTC_TIMESTAMP(6)
);
SET @revision=LAST_INSERT_ID();
UPDATE conversation_turns SET current_revision_id=@revision WHERE id=@turn;
INSERT INTO turn_input_memberships (
 turn_id,sessao_id,mensagem_id,inbound_event_id,input_sequence,content_bytes,
 membership_state,assigned_revision_id
) VALUES (@turn,@session,@message,@receipt,1,16,'sealed',@revision);
INSERT INTO generation_attempts (
 attempt_key,agente_id,sessao_id,turn_id,revision_id,scheduler_epoch,
 lane_owner_token,attempt_type,attempt_ordinal,outcome,accounting_state,modelo_llm
) VALUES (
 '33333333-3333-4333-8333-333333333333',@agent,@session,@turn,@revision,
 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
 '44444444-4444-4444-8444-444444444444','response',1,'started','pending','synthetic/model'
);
INSERT INTO outbox_deliveries (
 delivery_key,cutover_epoch,dedupe_key,provider,operation,payload_version,payload,
 delivery_class,ordering_key,stream_sequence,status,next_attempt_at,expires_at
) VALUES (
 '55555555-5555-4555-8555-555555555555',
 '66666666-6666-4666-8666-666666666666','trigger-probe','evolution',
 'evolution.send_text.v1',1,JSON_OBJECT('synthetic',TRUE),'legacy_unknown',
 '77777777-7777-4777-8777-777777777777',1,'pending',UTC_TIMESTAMP(6),
 UTC_TIMESTAMP(6)+INTERVAL 1 DAY
);
INSERT INTO outbox_deliveries (
 delivery_key,cutover_epoch,dedupe_key,provider,operation,payload_version,payload,
 delivery_class,automated_effect,agente_id,sessao_id,conversation_turn_id,turn_revision_id,
 scheduler_epoch,ordering_key,stream_sequence,status,next_attempt_at,expires_at
) VALUES (
 '55555555-5555-4555-8555-555555555556',
 '66666666-6666-4666-8666-666666666666','trigger-probe-automated','evolution',
 'evolution.send_text.v1',1,JSON_OBJECT('synthetic',TRUE),'automated','reply',
 @agent,@session,@turn,@revision,'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
 '77777777-7777-4777-8777-777777777778',1,'pending',UTC_TIMESTAMP(6),
 UTC_TIMESTAMP(6)+INTERVAL 1 DAY
);
SQL

assert_trigger_probe_rejected \
    ticket03_trigger_boundaries \
    trg_inbound_event_disposition_immutable \
    'first install: inbound disposition CREATE'

trigger_specs=(
    'trg_outbox_authority_immutable|ticket06-trigger-boundary: outbox-authority-v3-created'
    'trg_turn_revision_seal_immutable|turn-revision-v2-created'
    'trg_turn_input_identity_immutable|turn-input-v2-created'
    'trg_conversation_turn_identity_immutable|conversation-turn-v2-created'
    'trg_generation_attempt_identity_immutable|generation-attempt-v2-created'
)
for spec in "${trigger_specs[@]}"; do
    IFS='|' read -r trigger_base marker <<<"${spec}"
    apply_through_trigger_boundary \
        ticket03_trigger_boundaries \
        "$(full_marker "${marker}")" || exit $?
    assert_trigger_probe_rejected \
        ticket03_trigger_boundaries "${trigger_base}" \
        "first install: ${marker}"
done

apply_current ticket03_trigger_boundaries || exit $?

replacement_specs=(
    'trg_inbound_event_disposition_immutable|inbound-disposition-v2-created|inbound-disposition-predecessor-dropped'
    'trg_outbox_authority_immutable|ticket06-trigger-boundary: outbox-authority-v3-created|outbox-authority-predecessor-dropped'
    'trg_turn_revision_seal_immutable|turn-revision-v2-created|turn-revision-predecessor-dropped'
    'trg_turn_input_identity_immutable|turn-input-v2-created|turn-input-predecessor-dropped'
    'trg_conversation_turn_identity_immutable|conversation-turn-v2-created|conversation-turn-predecessor-dropped'
    'trg_generation_attempt_identity_immutable|generation-attempt-v2-created|generation-attempt-predecessor-dropped'
)
for spec in "${replacement_specs[@]}"; do
    IFS='|' read -r trigger_base create_marker drop_marker <<<"${spec}"
    install_predecessor_only ticket03_trigger_boundaries "${trigger_base}"
    assert_trigger_probe_rejected \
        ticket03_trigger_boundaries "${trigger_base}" \
        "replacement: predecessor only ${trigger_base}"
    apply_through_trigger_boundary \
        ticket03_trigger_boundaries \
        "$(full_marker "${create_marker}")" || exit $?
    assert_trigger_probe_rejected \
        ticket03_trigger_boundaries "${trigger_base}" \
        "replacement: versioned CREATE ${trigger_base}"
    apply_through_trigger_boundary \
        ticket03_trigger_boundaries \
        "$(full_marker "${drop_marker}")" || exit $?
    assert_trigger_probe_rejected \
        ticket03_trigger_boundaries "${trigger_base}" \
        "replacement: predecessor DROP ${trigger_base}"
    apply_current ticket03_trigger_boundaries || exit $?
done
echo "TICKET03_MIGRATION → PASS: interruption-safe immutable trigger replacement"

for spec in "${replacement_specs[@]}"; do
    IFS='|' read -r trigger_base _ _ <<<"${spec}"
    for variant in literal identifier operator; do
        install_tampered_v2_with_predecessor \
            ticket03_trigger_boundaries "${trigger_base}" "${variant}"
        tamper_output="$(apply_current ticket03_trigger_boundaries 2>&1)"
        tamper_rc=$?
        if (( tamper_rc == 0 )) || ! grep -q \
            'immutability guard definition requires audited repair' \
            <<<"${tamper_output}"; then
            echo "TRIGGER_DEFINITION → FAIL (${trigger_base}/${variant}): malformed v2 accepted" >&2
            printf '%s\n' "${tamper_output}" >&2
            exit 1
        fi
        predecessor_count="$(mysql_root --batch --skip-column-names \
            ticket03_trigger_boundaries --execute="
SELECT COUNT(*) FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='${trigger_base}';
")" || exit $?
        [[ "${predecessor_count}" == "1" ]] || {
            echo "TRIGGER_DEFINITION → FAIL (${trigger_base}/${variant}): predecessor removed" >&2
            exit 1
        }
        assert_trigger_probe_rejected \
            ticket03_trigger_boundaries "${trigger_base}" \
            "adversarial ${variant}: predecessor retained" any
        mysql_root ticket03_trigger_boundaries --execute="
DROP TRIGGER $(successor_of "${trigger_base}");
" || exit $?
        apply_current ticket03_trigger_boundaries || exit $?
        apply_current ticket03_trigger_boundaries || exit $?
        assert_trigger_probe_rejected \
            ticket03_trigger_boundaries "${trigger_base}" \
            "adversarial ${variant}: exact recovery repeat"
        echo "TRIGGER_DEFINITION → PASS (${trigger_base}/${variant}): exact validator rejected tamper"
    done
done
echo "TICKET03_MIGRATION → PASS: adversarial exact trigger definitions"

# Branch-targeted attack: a _v2 weakened in exactly the probed predicate must
# fail validation before predecessor DROP, the predecessor must still reject
# that branch mutation, and the weakened guard alone must admit it (proving the
# probe exercises the weakened branch, not some other predicate).
for spec in "${replacement_specs[@]}"; do
    IFS='|' read -r trigger_base _ _ <<<"${spec}"
    install_tampered_v2_with_predecessor \
        ticket03_trigger_boundaries "${trigger_base}" branch
    tamper_output="$(apply_current ticket03_trigger_boundaries 2>&1)"
    tamper_rc=$?
    if (( tamper_rc == 0 )) || ! grep -q \
        'immutability guard definition requires audited repair' \
        <<<"${tamper_output}"; then
        echo "TRIGGER_BRANCH_ATTACK → FAIL (${trigger_base}): weakened v2 accepted" >&2
        printf '%s\n' "${tamper_output}" >&2
        exit 1
    fi
    predecessor_count="$(mysql_root --batch --skip-column-names \
        ticket03_trigger_boundaries --execute="
SELECT COUNT(*) FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name='${trigger_base}';
")" || exit $?
    [[ "${predecessor_count}" == "1" ]] || {
        echo "TRIGGER_BRANCH_ATTACK → FAIL (${trigger_base}): predecessor removed" >&2
        exit 1
    }
    assert_branch_probe_rejected \
        ticket03_trigger_boundaries "${trigger_base}" \
        "branch attack: predecessor retained"
    mysql_root ticket03_trigger_boundaries --execute="
DROP TRIGGER ${trigger_base};
" || exit $?
    assert_branch_probe_admitted_by_weakened_guard \
        ticket03_trigger_boundaries "${trigger_base}"
    mysql_root ticket03_trigger_boundaries --execute="
DROP TRIGGER $(successor_of "${trigger_base}");
" || exit $?
    apply_current ticket03_trigger_boundaries || exit $?
    apply_current ticket03_trigger_boundaries || exit $?
    assert_trigger_probe_rejected \
        ticket03_trigger_boundaries "${trigger_base}" \
        "branch attack: exact recovery repeat"
    echo "TRIGGER_BRANCH_ATTACK → PASS (${trigger_base}): weakened branch failed validation, predecessor rejected it, weakened-only admitted it"
done
echo "TICKET03_MIGRATION → PASS: branch-sensitive guard probes"

mysql_root --execute="
CREATE USER IF NOT EXISTS 'ticket03_guard'@'%' IDENTIFIED BY 'synthetic-disposable-only';
GRANT ALL PRIVILEGES ON ticket03_trigger_boundaries.* TO 'ticket03_guard'@'%';
" || exit $?
for spec in "${replacement_specs[@]}"; do
    IFS='|' read -r trigger_base _ _ <<<"${spec}"
    install_metadata_v2_with_predecessor \
        ticket03_trigger_boundaries "${trigger_base}"
done
apply_current ticket03_trigger_boundaries || exit $?
metadata_count="$(mysql_root --batch --skip-column-names \
    ticket03_trigger_boundaries --execute="
SELECT COUNT(*) FROM information_schema.triggers
WHERE trigger_schema=DATABASE()
  AND trigger_name REGEXP '_immutable_v[0-9]+$'
  AND definer='ticket03_guard@%'
  AND FIND_IN_SET('ANSI_QUOTES',sql_mode) > 0
  AND FIND_IN_SET('NO_BACKSLASH_ESCAPES',sql_mode) > 0
  AND character_set_client='latin1'
  AND collation_connection='latin1_swedish_ci';
")" || exit $?
[[ "${metadata_count}" == "6" ]] || {
    echo "TRIGGER_METADATA → FAIL: expected 6 preserved metadata variants, got ${metadata_count}" >&2
    exit 1
}
apply_current ticket03_trigger_boundaries || exit $?
for spec in "${replacement_specs[@]}"; do
    IFS='|' read -r trigger_base _ _ <<<"${spec}"
    assert_trigger_probe_rejected \
        ticket03_trigger_boundaries "${trigger_base}" \
        "metadata variant repeat ${trigger_base}"
done
echo "TICKET03_MIGRATION → PASS: exact bodies tolerate DEFINER/sql_mode/charset metadata"

assert_authority() {
    local database=$1 phase=$2 column_shape index_shape fk_shape
    column_shape="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT CONCAT(data_type,':',column_type,':',is_nullable,':',COALESCE(column_default,'<NULL>'))
FROM information_schema.columns
WHERE table_schema=DATABASE() AND table_name='inbound_events'
 AND column_name='mensagem_id';
")" || exit $?
    index_shape="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT GROUP_CONCAT(CONCAT(non_unique,':',seq_in_index,':',column_name)
 ORDER BY seq_in_index SEPARATOR '|')
FROM information_schema.statistics
WHERE table_schema=DATABASE() AND table_name='inbound_events'
 AND index_name='uq_inbound_event_mensagem';
")" || exit $?
    fk_shape="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT CONCAT(k.column_name,':',k.referenced_table_schema,':',k.referenced_table_name,':',k.referenced_column_name,':',r.delete_rule,':',r.update_rule)
FROM information_schema.key_column_usage AS k
JOIN information_schema.referential_constraints AS r
 ON r.constraint_schema=k.constraint_schema AND r.table_name=k.table_name
 AND r.constraint_name=k.constraint_name
WHERE k.constraint_schema=DATABASE() AND k.table_name='inbound_events'
 AND k.constraint_name='fk_inbound_event_mensagem';
")" || exit $?
    [[ "${column_shape}" == "int:int:YES:<NULL>" ]] || {
        echo "TICKET03_MIGRATION → FAIL (${phase}): column=${column_shape}" >&2
        exit 1
    }
    [[ "${index_shape}" == "0:1:mensagem_id" ]] || {
        echo "TICKET03_MIGRATION → FAIL (${phase}): index=${index_shape}" >&2
        exit 1
    }
    [[ "${fk_shape}" == "mensagem_id:${database}:mensagens:id:RESTRICT:RESTRICT" ]] || {
        echo "TICKET03_MIGRATION → FAIL (${phase}): fk=${fk_shape}" >&2
        exit 1
    }
    echo "TICKET03_MIGRATION → PASS (${phase}): exact unique + RESTRICT FK"
}

assert_enforcement() {
    local database=$1 phase=$2
    mysql_root "${database}" --execute="
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('enforcement','enforcement-${database}','synthetic','0');
SET @enforcement_agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key)
VALUES (@enforcement_agent,'enforcement-${database}',UUID());
SET @enforcement_session=LAST_INSERT_ID();
INSERT INTO mensagens (sessao_id,role,conteudo)
VALUES (@enforcement_session,'user','enforced');
SET @enforcement_message=LAST_INSERT_ID();
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,
 agente_id,sessao_id,mensagem_id,processed_at
) VALUES (
 'evolution','enforcement','enforcement-1','messages.upsert',REPEAT('d',64),1,
 @enforcement_agent,@enforcement_session,@enforcement_message,UTC_TIMESTAMP(6)
);
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,
 agente_id,sessao_id,processed_at
) VALUES (
 'evolution','enforcement','enforcement-null','messages.upsert',REPEAT('0',64),1,
 @enforcement_agent,@enforcement_session,UTC_TIMESTAMP(6)
);
" || exit $?
    if mysql_root "${database}" --execute="
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,
 agente_id,sessao_id,mensagem_id,processed_at
) SELECT
 'evolution','enforcement','enforcement-duplicate','messages.upsert',REPEAT('e',64),1,
 agente_id,sessao_id,mensagem_id,UTC_TIMESTAMP(6)
 FROM inbound_events WHERE provider_event_id='enforcement-1';
" >/dev/null 2>&1; then
        echo "TICKET03_MIGRATION → FAIL (${phase}): duplicate association accepted" >&2
        exit 1
    fi
    if mysql_root "${database}" --execute="
INSERT INTO inbound_events (
 provider,source_scope,provider_event_id,event_type,payload_hmac,hmac_key_version,mensagem_id
) VALUES ('evolution','enforcement','enforcement-orphan','messages.upsert',REPEAT('f',64),1,999999999);
" >/dev/null 2>&1; then
        echo "TICKET03_MIGRATION → FAIL (${phase}): orphan association accepted" >&2
        exit 1
    fi
    if mysql_root "${database}" --execute="
DELETE message FROM mensagens AS message
JOIN inbound_events AS receipt ON receipt.mensagem_id=message.id
WHERE receipt.provider_event_id='enforcement-1';
" >/dev/null 2>&1; then
        echo "TICKET03_MIGRATION → FAIL (${phase}): referenced message deleted" >&2
        exit 1
    fi
    echo "TICKET03_MIGRATION → PASS (${phase}): uniqueness/FK/RESTRICT enforced"
}

for fixture in column unique_only wrong_index missing_fk wrong_fk; do
    database="ticket03_${fixture}"
    apply_current "${database}" || exit $?
    assert_authority "${database}" "${fixture}"
    apply_current "${database}" || exit $?
    assert_authority "${database}" "${fixture} repeat"
    assert_enforcement "${database}" "${fixture}"
done

for fixture in wrong_column wrong_type cross_schema; do
    database="ticket03_${fixture}"
    apply_current "${database}" || exit $?
    assert_authority "${database}" "${fixture}"
    if [[ "${fixture}" == "wrong_column" ]]; then
        sentinel_state="$(mysql_root --batch --skip-column-names "${database}" --execute="
SELECT CONCAT(mensagem_id IS NULL,':',receipt_disposition)
FROM inbound_events WHERE provider_event_id='sentinel-zero';
")" || exit $?
        [[ "${sentinel_state}" == "1:legacy_unknown" ]] || {
            echo "TICKET03_MIGRATION → FAIL: sentinel/backfill=${sentinel_state}" >&2
            exit 1
        }
    fi
    apply_current "${database}" || exit $?
    assert_authority "${database}" "${fixture} repeat"
    assert_enforcement "${database}" "${fixture}"
done

range_output="$(apply_current ticket03_wrong_range 2>&1)"
range_rc=$?
if (( range_rc == 0 )) || ! grep -q \
    'inbound message column range requires audited repair' \
    <<<"${range_output}"; then
    echo "TICKET03_MIGRATION → FAIL: out-of-range predecessor did not fail closed" >&2
    printf '%s\n' "${range_output}" >&2
    exit 1
fi
range_state="$(mysql_root --batch --skip-column-names ticket03_wrong_range --execute="
SELECT mensagem_id FROM inbound_events WHERE provider_event_id='out-of-range';
")" || exit $?
[[ "${range_state}" == "2147483648" ]] || {
    echo "TICKET03_MIGRATION → FAIL: out-of-range data was silently changed" >&2
    exit 1
}
echo "TICKET03_MIGRATION → PASS: out-of-range predecessor retained for audited repair"
mysql_root ticket03_wrong_range --execute="
UPDATE inbound_events SET mensagem_id=NULL WHERE provider_event_id='out-of-range';
" || exit $?
apply_current ticket03_wrong_range || exit $?
apply_current ticket03_wrong_range || exit $?
assert_authority ticket03_wrong_range 'range repaired externally + repeat'
assert_enforcement ticket03_wrong_range 'range repaired externally'

apply_current ticket03_disposition || exit $?
disposition_state="$(mysql_root --batch --skip-column-names ticket03_disposition --execute="
SELECT GROUP_CONCAT(CONCAT(provider_event_id,':',receipt_disposition)
 ORDER BY provider_event_id SEPARATOR '|')
FROM inbound_events;
")" || exit $?
[[ "${disposition_state}" == "ambiguous-legacy:legacy_unknown|proven-chatwoot:chatwoot|proven-scheduled:scheduled" ]] || {
    echo "TICKET03_MIGRATION → FAIL: disposition backfill=${disposition_state}" >&2
    exit 1
}
if mysql_root ticket03_disposition --execute="
UPDATE inbound_events SET receipt_disposition='ignored_nonbot'
WHERE provider_event_id='proven-scheduled';
" >/dev/null 2>&1; then
    echo "TICKET03_MIGRATION → FAIL: disposition mutation accepted" >&2
    exit 1
fi
apply_current ticket03_disposition || exit $?
assert_authority ticket03_disposition 'disposition backfill + repeat'
echo "TICKET03_MIGRATION → PASS: conservative immutable disposition backfill"

# Reproduce a predecessor migration gap, then prove a repeat refuses to bless
# the contradictory non-null disposition and preserves the corrupted value.
mysql_root ticket03_disposition --execute="
DROP TRIGGER trg_inbound_event_disposition_immutable_v2;
UPDATE inbound_events SET receipt_disposition='ignored_nonbot'
WHERE provider_event_id='proven-scheduled';
" || exit $?
corruption_output="$(apply_current ticket03_disposition 2>&1)"
corruption_rc=$?
if (( corruption_rc == 0 )) || ! grep -q \
    'inbound receipt disposition evidence conflict requires audited repair' \
    <<<"${corruption_output}"; then
    echo "TICKET03_MIGRATION → FAIL: contradictory disposition did not fail closed" >&2
    printf '%s\n' "${corruption_output}" >&2
    exit 1
fi
corruption_state="$(mysql_root --batch --skip-column-names ticket03_disposition --execute="
SELECT receipt_disposition FROM inbound_events
WHERE provider_event_id='proven-scheduled';
")" || exit $?
[[ "${corruption_state}" == "ignored_nonbot" ]] || {
    echo "TICKET03_MIGRATION → FAIL: contradictory disposition was silently repaired" >&2
    exit 1
}
mysql_root ticket03_disposition --execute="
UPDATE inbound_events SET receipt_disposition='scheduled'
WHERE provider_event_id='proven-scheduled';
" || exit $?
apply_current ticket03_disposition || exit $?
apply_current ticket03_disposition || exit $?
if mysql_root ticket03_disposition --execute="
UPDATE inbound_events SET receipt_disposition='ignored_nonbot'
WHERE provider_event_id='proven-scheduled';
" >/dev/null 2>&1; then
    echo "TICKET03_MIGRATION → FAIL: recovered disposition guard is ineffective" >&2
    exit 1
fi
echo "TICKET03_MIGRATION → PASS: predecessor-gap corruption fails closed and repeat converges"

duplicate_output="$(apply_current ticket03_duplicate 2>&1)"
duplicate_rc=$?
if (( duplicate_rc == 0 )) || ! grep -q \
    'duplicate inbound message association requires audited repair' \
    <<<"${duplicate_output}"; then
    echo "TICKET03_MIGRATION → FAIL: duplicate predecessor did not fail closed" >&2
    printf '%s\n' "${duplicate_output}" >&2
    exit 1
fi
duplicate_state="$(mysql_root --batch --skip-column-names ticket03_duplicate --execute="
SELECT CONCAT(COUNT(*),':',COUNT(DISTINCT mensagem_id))
FROM inbound_events WHERE mensagem_id IS NOT NULL;
")" || exit $?
[[ "${duplicate_state}" == "2:1" ]] || {
    echo "TICKET03_MIGRATION → FAIL: duplicate data was silently changed" >&2
    exit 1
}
echo "TICKET03_MIGRATION → PASS: duplicate predecessor retained for audited repair"
mysql_root ticket03_duplicate --execute="
DELETE FROM inbound_events WHERE provider_event_id='duplicate-2';
" || exit $?
apply_current ticket03_duplicate || exit $?
apply_current ticket03_duplicate || exit $?
assert_authority ticket03_duplicate 'duplicate repaired externally + repeat'
assert_enforcement ticket03_duplicate 'duplicate repaired externally'

orphan_output="$(apply_current ticket03_orphan 2>&1)"
orphan_rc=$?
if (( orphan_rc == 0 )) || ! grep -q \
    'orphan inbound message association requires audited repair' \
    <<<"${orphan_output}"; then
    echo "TICKET03_MIGRATION → FAIL: orphan predecessor did not fail closed" >&2
    printf '%s\n' "${orphan_output}" >&2
    exit 1
fi
orphan_state="$(mysql_root --batch --skip-column-names ticket03_orphan --execute="
SELECT COUNT(*) FROM inbound_events WHERE provider_event_id='orphan-1';
")" || exit $?
[[ "${orphan_state}" == "1" ]] || {
    echo "TICKET03_MIGRATION → FAIL: orphan data was silently changed" >&2
    exit 1
}
echo "TICKET03_MIGRATION → PASS: orphan predecessor retained for audited repair"
mysql_root ticket03_orphan --execute="
DELETE FROM inbound_events WHERE provider_event_id='orphan-1';
" || exit $?
apply_current ticket03_orphan || exit $?
apply_current ticket03_orphan || exit $?
assert_authority ticket03_orphan 'orphan repaired externally + repeat'
assert_enforcement ticket03_orphan 'orphan repaired externally'

bash -n "${repo_root}/scripts/check_inbox_ticket03_migrations.sh" || exit $?
echo "TICKET03_MIGRATIONS → PASS"
