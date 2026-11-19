#!/usr/bin/env bash
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
suffix="${TICKET02_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
container="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket02-mysql-${suffix}"
network="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket02-net-${suffix}"
volume="${WPP_TEST_RESOURCE_PREFIX:-}wpp-ticket02-data-${suffix}"
image="${WPP_TEST_IMAGE:-wpp-ticket02-matrix-${suffix}:local}"
built_image=0
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
shared_project="traycer-wpp-ai-bot-soft-ocelot"
cleanup_verified=0

shared_count() {
    docker ps -aq --filter "label=com.docker.compose.project=${shared_project}" | wc -l | tr -d ' '
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
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker network rm "${network}" >/dev/null 2>&1 || true
    docker volume rm -f "${volume}" >/dev/null 2>&1 || true
    if (( built_image == 1 )); then
        docker image rm -f "${image}" >/dev/null 2>&1 || true
    fi
    if [[ "$(resource_count)" != "0" ]]; then
        echo "CLEANUP → FAIL: disposable container/network/volume remains" >&2
        cleanup_status=70
    fi
    if [[ "$(shared_count)" != "0" ]]; then
        echo "SHARED_STACK → FAIL: shared Compose project is not zero" >&2
        cleanup_status=71
    fi
    if (( cleanup_status == 0 )); then
        cleanup_verified=1
        echo "CLEANUP → PASS: disposable resources absent"
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

apply_schema() {
    local database=$1
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "${database}" < "${repo_root}/db/init.sql"
}

mysql_root --execute="
    CREATE DATABASE ticket02_fresh CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE DATABASE ticket02_legacy CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE DATABASE ticket02_duplicate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE DATABASE ticket02_unknown CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE DATABASE ticket02_intermediate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
" || exit $?

apply_schema ticket02_fresh || exit $?
apply_schema ticket02_fresh || exit $?

apply_schema ticket02_intermediate || exit $?
mysql_root ticket02_intermediate <<'SQL' || exit $?
ALTER TABLE generation_attempts DROP FOREIGN KEY fk_generation_consumo;
ALTER TABLE generation_attempts
    ADD CONSTRAINT fk_generation_consumo
    FOREIGN KEY (consumo_llm_id) REFERENCES consumo_llm(id) ON DELETE SET NULL;

DELIMITER $$
DROP TRIGGER IF EXISTS trg_generation_attempt_identity_immutable$$
CREATE TRIGGER trg_generation_attempt_identity_immutable
BEFORE UPDATE ON generation_attempts
FOR EACH ROW
BEGIN
    SET @ticket02_prior_generation_trigger = 1;
END$$
DELIMITER ;

INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('intermediate','intermediate-ticket02','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key)
VALUES (@agent,'intermediate-1',UUID());
SET @session=LAST_INSERT_ID();
INSERT INTO conversation_turns (
 turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,
 status,session_slot,ready_at
) VALUES (
 UUID(),@agent,@session,'33333333-3333-4333-8333-333333333333',1,1,
 'pending','active',UTC_TIMESTAMP(6)
);
SET @turn=LAST_INSERT_ID();
INSERT INTO turn_revisions (
 revision_key,turn_id,scheduler_epoch,revision_number,refresh_ordinal,status
) VALUES (
 UUID(),@turn,'33333333-3333-4333-8333-333333333333',1,0,'draft'
);
SET @revision=LAST_INSERT_ID();
INSERT INTO generation_attempts (
 attempt_key,agente_id,sessao_id,turn_id,revision_id,scheduler_epoch,
 lane_owner_token,attempt_type,attempt_ordinal,outcome,accounting_state,modelo_llm
) VALUES (
 '44444444-4444-4444-8444-444444444444',@agent,@session,@turn,@revision,
 '33333333-3333-4333-8333-333333333333',
 '55555555-5555-4555-8555-555555555555','response',1,'started','pending',
 'synthetic/model'
);
SQL

assert_intermediate_upgrade() {
    local phase=$1
    local delete_rule trigger_body probe_output probe_rc outcome
    delete_rule="$(mysql_root --batch --skip-column-names ticket02_intermediate --execute="
SELECT delete_rule FROM information_schema.referential_constraints
WHERE constraint_schema=DATABASE() AND table_name='generation_attempts'
  AND constraint_name='fk_generation_consumo';
")" || exit $?
    [[ "${delete_rule}" == "RESTRICT" ]] || {
        echo "INTERMEDIATE_UPGRADE → FAIL (${phase}): delete_rule=${delete_rule}" >&2
        exit 1
    }
    trigger_body="$(mysql_root --batch --skip-column-names ticket02_intermediate --execute="
SELECT action_statement FROM information_schema.triggers
WHERE trigger_schema=DATABASE()
  AND trigger_name='trg_generation_attempt_identity_immutable_v2';
")" || exit $?
    grep -q 'generation success requires durable accounting state' \
        <<<"${trigger_body}" || {
        echo "INTERMEDIATE_UPGRADE → FAIL (${phase}): current trigger missing" >&2
        exit 1
    }
    probe_output="$(mysql_root ticket02_intermediate --execute="
UPDATE generation_attempts
SET outcome='succeeded',accounting_state='pending',
    provider_generation_id='unsafe-intermediate-success',
    tokens_input=1,tokens_output=1,custo_creditos=0.00000100,
    outcome_recorded_at=UTC_TIMESTAMP(6)
WHERE attempt_key='44444444-4444-4444-8444-444444444444';
" 2>&1)"
    probe_rc=$?
    if (( probe_rc == 0 )); then
        echo "INTERMEDIATE_UPGRADE → FAIL (${phase}): unsafe update accepted" >&2
        exit 1
    fi
    grep -q 'generation success requires durable accounting state' \
        <<<"${probe_output}" || {
        echo "INTERMEDIATE_UPGRADE → FAIL (${phase}): unexpected trigger error" >&2
        printf '%s\n' "${probe_output}" >&2
        exit 1
    }
    outcome="$(mysql_root --batch --skip-column-names ticket02_intermediate --execute="
SELECT CONCAT(outcome,':',accounting_state,':',provider_generation_id IS NULL)
FROM generation_attempts
WHERE attempt_key='44444444-4444-4444-8444-444444444444';
")" || exit $?
    [[ "${outcome}" == "started:pending:1" ]] || {
        echo "INTERMEDIATE_UPGRADE → FAIL (${phase}): failed probe mutated row" >&2
        exit 1
    }
    echo "INTERMEDIATE_UPGRADE → PASS (${phase}): RESTRICT + current trigger"
}

apply_schema ticket02_intermediate || exit $?
assert_intermediate_upgrade 'SET NULL predecessor'
apply_schema ticket02_intermediate || exit $?
assert_intermediate_upgrade 'repeat after predecessor'

mysql_root ticket02_intermediate --execute="
ALTER TABLE generation_attempts DROP FOREIGN KEY fk_generation_consumo;
" || exit $?
missing_fk_count="$(mysql_root --batch --skip-column-names ticket02_intermediate --execute="
SELECT COUNT(*) FROM information_schema.referential_constraints
WHERE constraint_schema=DATABASE() AND table_name='generation_attempts'
  AND constraint_name='fk_generation_consumo';
")" || exit $?
[[ "${missing_fk_count}" == "0" ]] || exit 1
apply_schema ticket02_intermediate || exit $?
assert_intermediate_upgrade 'missing-FK interruption recovery'
apply_schema ticket02_intermediate || exit $?
assert_intermediate_upgrade 'repeat after missing-FK recovery'

legacy_schema="$(mktemp)"
if ! git -C "${repo_root}" show 16626cd:db/init.sql > "${legacy_schema}"; then
    rm -f "${legacy_schema}"
    exit 1
fi
for database in ticket02_legacy ticket02_unknown; do
    docker exec --interactive --env "MYSQL_PWD=${root_password}" "${container}" \
        mysql --user=root --protocol=socket "${database}" < "${legacy_schema}" || {
            rm -f "${legacy_schema}"
            exit 1
        }
done
rm -f "${legacy_schema}"

mysql_root ticket02_legacy <<'SQL' || exit $?
INSERT INTO agentes (nome, instance_name, system_prompt, numero_notificacao)
VALUES ('legacy', 'legacy-ticket02', 'synthetic', '0');
SET @agent = LAST_INSERT_ID();
INSERT INTO sessoes (agente_id, telefone, delivery_stream_key)
VALUES (@agent, 'legacy-1', UUID());
SET @session = LAST_INSERT_ID();
SET @stream = (SELECT delivery_stream_key FROM sessoes WHERE id=@session);
SET @epoch = '11111111-1111-4111-8111-111111111111';
INSERT INTO outbox_deliveries (
 delivery_key,cutover_epoch,dedupe_key,provider,operation,payload_version,payload,
 agente_id,sessao_id,ordering_key,stream_sequence,status,next_attempt_at,expires_at
) VALUES
(UUID(),@epoch,'evolution:inbound:1:reply','evolution','evolution.send_text.v1',1,JSON_OBJECT('x',1),@agent,@session,@stream,1,'pending',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)+INTERVAL 1 DAY),
(UUID(),@epoch,'evolution:handoff:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa:transition','evolution','evolution.send_text.v1',1,JSON_OBJECT('x',1),@agent,@session,@stream,2,'pending',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)+INTERVAL 1 DAY),
(UUID(),@epoch,'evolution:chatwoot:1:99','evolution','evolution.send_text.v1',1,JSON_OBJECT('x',1),@agent,@session,@stream,3,'pending',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)+INTERVAL 1 DAY),
(UUID(),@epoch,'terminal-unclassified','evolution','evolution.send_text.v1',1,JSON_OBJECT('x',1),@agent,@session,@stream,4,'dead',NULL,NULL);
INSERT INTO outbox_deliveries (
 delivery_key,cutover_epoch,dedupe_key,provider,operation,payload_version,payload,
 agente_id,sessao_id,ordering_key,stream_sequence,status,next_attempt_at,expires_at
) VALUES
(UUID(),@epoch,'chatwoot:handoff:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa:open','chatwoot','chatwoot.open_handoff.v1',1,JSON_OBJECT('x',1),@agent,@session,UUID(),0,'pending',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)+INTERVAL 1 DAY);
SQL
apply_schema ticket02_legacy || exit $?
apply_schema ticket02_legacy || exit $?

classification="$({ mysql_root --batch --skip-column-names ticket02_legacy --execute="
SELECT GROUP_CONCAT(CONCAT(dedupe_key,'=',delivery_class) ORDER BY id SEPARATOR '|')
FROM outbox_deliveries;
"; } 2>/dev/null)" || exit $?
grep -q 'evolution:inbound:1:reply=legacy_automated' <<<"${classification}" || exit 1
grep -q 'evolution:chatwoot:1:99=human' <<<"${classification}" || exit 1
grep -q 'chatwoot:handoff:.*:open=provider_setup_context' <<<"${classification}" || exit 1
grep -q 'terminal-unclassified=legacy_unknown' <<<"${classification}" || exit 1
echo "LEGACY_CLASSIFICATION → PASS: ${classification}"

mysql_root ticket02_unknown <<'SQL' || exit $?
INSERT INTO agentes (nome, instance_name, system_prompt, numero_notificacao)
VALUES ('unknown', 'unknown-ticket02', 'synthetic', '0');
SET @agent = LAST_INSERT_ID();
INSERT INTO sessoes (agente_id, telefone, delivery_stream_key)
VALUES (@agent, 'unknown-1', UUID());
SET @session = LAST_INSERT_ID();
SET @stream = (SELECT delivery_stream_key FROM sessoes WHERE id=@session);
INSERT INTO outbox_deliveries (
 delivery_key,cutover_epoch,dedupe_key,provider,operation,payload_version,payload,
 agente_id,sessao_id,ordering_key,stream_sequence,status,next_attempt_at,expires_at
) VALUES (
 UUID(),'11111111-1111-4111-8111-111111111111','unprovable-intent','evolution',
 'evolution.send_text.v1',1,JSON_OBJECT('x',1),@agent,@session,@stream,1,
 'pending',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)+INTERVAL 1 DAY
);
SQL
apply_schema ticket02_unknown || exit $?
unknown_count="$(mysql_root --batch --skip-column-names ticket02_unknown --execute="
SELECT COUNT(*) FROM outbox_deliveries
WHERE delivery_class='legacy_unknown' AND status IN ('blocked','pending','processing','retry');
")" || exit $?
[[ "${unknown_count}" == "1" ]] || exit 1
echo "UNKNOWN_NONTERMINAL → PASS: enablement blocker retained"

apply_schema ticket02_duplicate || exit $?
if mysql_root ticket02_duplicate <<'SQL' >/dev/null 2>&1
INSERT INTO agentes (nome,instance_name,system_prompt,numero_notificacao)
VALUES ('duplicate','duplicate-ticket02','synthetic','0');
SET @agent=LAST_INSERT_ID();
INSERT INTO sessoes (agente_id,telefone,delivery_stream_key)
VALUES (@agent,'duplicate-1',UUID());
SET @session=LAST_INSERT_ID();
INSERT INTO conversation_turns (
 turn_key,agente_id,sessao_id,scheduler_epoch,arrival_sequence,scheduling_sequence,
 status,session_slot,ready_at
) VALUES
(UUID(),@agent,@session,'22222222-2222-4222-8222-222222222222',1,1,'pending','active',UTC_TIMESTAMP(6)),
(UUID(),@agent,@session,'22222222-2222-4222-8222-222222222222',2,2,'pending','active',UTC_TIMESTAMP(6));
SQL
then
    echo "DUPLICATE_BARRIER → FAIL: second active turn was accepted" >&2
    exit 1
fi
echo "DUPLICATE_BARRIER → PASS: unique active slot enforced"

trigger_count="$(mysql_root --batch --skip-column-names ticket02_fresh --execute="
SELECT COUNT(*) FROM information_schema.triggers
WHERE trigger_schema=DATABASE() AND trigger_name IN (
 'trg_outbox_authority_immutable_v3','trg_turn_revision_seal_immutable_v2',
 'trg_turn_input_identity_immutable_v2','trg_conversation_turn_identity_immutable_v2',
 'trg_generation_attempt_identity_immutable_v2',
 'trg_inbound_event_disposition_immutable_v2'
);
")" || exit $?
[[ "${trigger_count}" == "6" ]] || exit 1
echo "IMMUTABILITY_TRIGGERS → PASS: 6/6"

plan_output="$(mysql_root --batch --skip-column-names ticket02_fresh \
    < "${repo_root}/scripts/measure_scheduler_claim_plan.sql")" || exit $?
printf '%s\n' "SCHEDULER_EXPLAIN → ${plan_output}"
grep -q 'idx_turn_ready_claim' <<<"${plan_output}" || exit 1

if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${image}" "${repo_root}/app" >/dev/null || exit $?
    built_image=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?

mysql_root --execute="
CREATE USER 'ticket02_app'@'%' IDENTIFIED BY '${app_password}';
GRANT SELECT,INSERT,UPDATE,DELETE ON ticket02_fresh.* TO 'ticket02_app'@'%';
" || exit $?

if [[ "${TICKET02_REGRESSION:-1}" == 1 ]]; then
docker run --rm \
    --network="container:${container}" \
    --volume="${repo_root}:/workspace:ro" \
    --tmpfs=/app/logs --tmpfs=/runtime \
    --workdir=/runtime --env PYTHONPATH=/workspace/app \
    --env WPP_REPO_ROOT=/workspace \
    --env "OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://ticket02_app:${app_password}@127.0.0.1:3306/ticket02_fresh" \
    --env "DATABASE_URL=mysql+aiomysql://ticket02_app:${app_password}@127.0.0.1:3306/ticket02_fresh" \
    --env WPP_DISPOSABLE_TEST_DATABASE_MARKER=ticket02_fresh \
    --env OPENROUTER_API_KEY=synthetic-ticket02-key \
    --env EVOLUTION_API_URL=http://evolution.invalid \
    --env EVOLUTION_API_KEY=synthetic-ticket02-key \
    --env EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic-ticket02-bearer \
    --env CHATWOOT_BASE_URL=http://chatwoot.invalid \
    --env CHATWOOT_API_TOKEN=synthetic-ticket02-token \
    --env CHATWOOT_ACCOUNT_ID=1 \
    --env CHATWOOT_WEBHOOK_SECRET=synthetic-ticket02-secret \
    --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" python -m unittest discover -s /workspace/app/tests -t /workspace/app -v || exit $?
else
    echo "TICKET02_REGRESSION → DEFERRED: integrated matrix owns the single full regression"
fi

docker run --rm --network=none --volume="${repo_root}:/workspace:ro" \
    --tmpfs=/app/logs --tmpfs=/tmp/pycache --env PYTHONPYCACHEPREFIX=/tmp/pycache \
    --workdir=/workspace/app --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${image}" \
    python -m py_compile \
        src/database/models.py \
        src/database/repositories/outbox_repo.py \
        src/database/repositories/scheduler_repo.py \
        tests/test_scheduler_state_mysql.py || exit $?

bash -n "${repo_root}/scripts/run_inbox_ticket02_mysql_matrix.sh" || exit $?
echo "TICKET02_MATRIX → PASS"
