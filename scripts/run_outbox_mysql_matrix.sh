#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo_root}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo_root}")"
mysql_container=""
mysql_root_password="outbox-matrix-root"
mysql_app_password="outbox-matrix-app"
app_image="${WPP_TEST_IMAGE:-wpp-outbox-matrix-app:local}"
mysql_ready_timeout_seconds="${OUTBOX_MYSQL_READY_TIMEOUT_SECONDS:-90}"
mysql_retry_timeout_seconds="${OUTBOX_MYSQL_RETRY_TIMEOUT_SECONDS:-30}"
mysql_cold_start_repetitions="${OUTBOX_MYSQL_COLD_START_REPETITIONS:-3}"

for numeric_setting in \
    mysql_ready_timeout_seconds \
    mysql_retry_timeout_seconds \
    mysql_cold_start_repetitions; do
    if [[ ! "${!numeric_setting}" =~ ^[0-9]+$ ]]; then
        echo "${numeric_setting} must be a non-negative integer" >&2
        exit 2
    fi
done
if (( mysql_ready_timeout_seconds < 1 || mysql_retry_timeout_seconds < 1 )); then
    echo "MySQL readiness and retry timeouts must be positive" >&2
    exit 2
fi
if (( mysql_cold_start_repetitions < 2 )); then
    echo "OUTBOX_MYSQL_COLD_START_REPETITIONS must be at least 2" >&2
    exit 2
fi

cleanup() {
    if [[ -n "${mysql_container}" ]]; then
        docker rm -f "${mysql_container}" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

start_mysql_container() {
    mysql_container="$1"
    docker run --detach --rm \
        --name "${mysql_container}" \
        --env MYSQL_ROOT_PASSWORD="${mysql_root_password}" \
        mysql:8.0 \
        --character-set-server=utf8mb4 \
        --collation-server=utf8mb4_unicode_ci >/dev/null
}

stop_mysql_container() {
    cleanup
    mysql_container=""
}

mysql_root() {
    docker exec \
        --env MYSQL_PWD="${mysql_root_password}" \
        "${mysql_container}" \
        mysql --user=root --protocol=socket "$@"
}

wait_for_final_mysql() {
    local deadline=$((SECONDS + mysql_ready_timeout_seconds))
    local process_name

    while (( SECONDS < deadline )); do
        process_name="$(docker exec "${mysql_container}" sh -c 'cat /proc/1/comm' 2>/dev/null || true)"
        if [[ "${process_name}" == "mysqld" ]] && \
            mysql_root --execute="SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "Timed out waiting for final MySQL server in ${mysql_container}" >&2
    docker logs --tail 80 "${mysql_container}" >&2 || true
    return 1
}

retry_mysql_admin_sql() {
    local description="$1"
    local sql="$2"
    local deadline=$((SECONDS + mysql_retry_timeout_seconds))

    while (( SECONDS < deadline )); do
        if mysql_root --execute="${sql}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "Timed out during ${description}" >&2
    mysql_root --execute="${sql}"
}

retry_mysql_schema_file() {
    local description="$1"
    local database="$2"
    local schema_file="$3"
    local deadline=$((SECONDS + mysql_retry_timeout_seconds))

    while (( SECONDS < deadline )); do
        if docker exec --interactive \
            --env MYSQL_PWD="${mysql_root_password}" \
            "${mysql_container}" \
            mysql --user=root --protocol=socket "${database}" \
            < "${schema_file}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    echo "Timed out during ${description}" >&2
    docker exec --interactive \
        --env MYSQL_PWD="${mysql_root_password}" \
        "${mysql_container}" \
        mysql --user=root --protocol=socket "${database}" \
        < "${schema_file}"
}

if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --quiet --tag "${app_image}" "${repo_root}/app" >/dev/null
fi
image_code_version="$(image_test_version "${app_image}")" || exit $?

for repetition in $(seq 1 "${mysql_cold_start_repetitions}"); do
    echo "Cold MySQL readiness check ${repetition}/${mysql_cold_start_repetitions}"
    start_mysql_container "wpp-outbox-cold-$$-${repetition}"
    wait_for_final_mysql
    retry_mysql_admin_sql \
        "cold-start administrative check ${repetition}" \
        "CREATE DATABASE IF NOT EXISTS outbox_cold_probe; DROP DATABASE outbox_cold_probe;"
    stop_mysql_container
done

start_mysql_container "wpp-outbox-matrix-$$"
wait_for_final_mysql
retry_mysql_admin_sql \
    "initial matrix database creation" \
    "CREATE DATABASE IF NOT EXISTS outbox_fresh CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; CREATE DATABASE IF NOT EXISTS outbox_legacy CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci; CREATE DATABASE IF NOT EXISTS outbox_duplicate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

# Fresh install and idempotent repeat.
retry_mysql_schema_file \
    "initial fresh schema application" \
    outbox_fresh \
    "${repo_root}/db/init.sql"
docker exec --interactive "${mysql_container}" mysql \
    --user=root --password="${mysql_root_password}" outbox_fresh \
    < "${repo_root}/db/init.sql"

# Representative pre-outbox schema and data.
docker exec --interactive "${mysql_container}" mysql \
    --user=root --password="${mysql_root_password}" outbox_legacy <<'SQL'
CREATE TABLE agentes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    instance_name VARCHAR(255) NOT NULL UNIQUE,
    system_prompt TEXT NOT NULL,
    modelo_openai VARCHAR(100) NOT NULL DEFAULT 'gpt-4o-mini',
    numero_notificacao VARCHAR(50) NOT NULL,
    max_historico INT NOT NULL DEFAULT 20,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
CREATE TABLE atendentes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    numero_whatsapp VARCHAR(50) NOT NULL UNIQUE,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    indice_round_robin INT NOT NULL DEFAULT 0,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
CREATE TABLE sessoes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    agente_id INT NOT NULL,
    telefone VARCHAR(50) NOT NULL,
    handler ENUM('bot','humano') NOT NULL DEFAULT 'bot',
    total_mensagens INT NOT NULL DEFAULT 0,
    motivo_handoff TEXT NULL,
    encerrada_em DATETIME NULL,
    motivo_encerramento VARCHAR(50) NULL,
    atendente_id INT NULL,
    chatwoot_conversation_id INT NULL,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ultima_interacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT uq_agente_telefone UNIQUE (agente_id, telefone),
    CONSTRAINT fk_sessao_agente FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT fk_sessao_atendente FOREIGN KEY (atendente_id) REFERENCES atendentes(id)
);
CREATE TABLE mensagens (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sessao_id INT NOT NULL,
    role ENUM('user','assistant') NOT NULL,
    conteudo TEXT NOT NULL,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_mensagem_sessao FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE CASCADE
);
INSERT INTO agentes (nome, instance_name, system_prompt, numero_notificacao)
VALUES ('Legacy', 'legacy-instance', 'legacy prompt', '0');
INSERT INTO sessoes (agente_id, telefone, chatwoot_conversation_id)
VALUES (1, '551100000001', 101), (1, '551100000002', 102);
SQL
docker exec --interactive "${mysql_container}" mysql \
    --user=root --password="${mysql_root_password}" outbox_legacy \
    < "${repo_root}/db/init.sql"
docker exec --interactive "${mysql_container}" mysql \
    --user=root --password="${mysql_root_password}" outbox_legacy \
    < "${repo_root}/db/init.sql"
docker exec "${mysql_container}" mysql \
    --batch --skip-column-names \
    --user=root --password="${mysql_root_password}" outbox_legacy \
    --execute="SELECT IF(COUNT(*) = 2 AND COUNT(DISTINCT delivery_stream_key) = 2, 1, 0) FROM sessoes; SELECT IF(COUNT(*) = 7, 1, 0) FROM information_schema.tables WHERE table_schema = 'outbox_legacy' AND table_name IN ('inbound_events','handoff_attempts','outbox_deliveries','llm_usage_reconciliation','delivery_worker_heartbeats','delivery_operator_actions','delivery_runtime_state');" | while read -r result; do
        [[ "${result}" == "1" ]]
    done

# Measure the exact fenced-claim query against 10,000 synthetic rows. Keep the
# output in CI logs; fail if MySQL does not use both purpose-built indexes.
claim_plan_output="$(
    docker exec --interactive \
        --env MYSQL_PWD="${mysql_root_password}" \
        "${mysql_container}" \
        mysql --user=root --protocol=socket --batch --skip-column-names outbox_fresh \
        < "${repo_root}/scripts/measure_outbox_claim_plan.sql"
)"
printf '%s\n' "Measured outbox claim plan (10,000 rows):" "${claim_plan_output}"
grep -q 'idx_outbox_claim' <<<"${claim_plan_output}"
grep -q 'idx_outbox_predecessor' <<<"${claim_plan_output}"

# A legacy duplicate must stop the migration instead of silently choosing a row.
docker exec --interactive "${mysql_container}" mysql \
    --user=root --password="${mysql_root_password}" outbox_duplicate <<'SQL'
CREATE TABLE agentes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    instance_name VARCHAR(255) NOT NULL UNIQUE,
    system_prompt TEXT NOT NULL,
    modelo_openai VARCHAR(100) NOT NULL DEFAULT 'gpt-4o-mini',
    numero_notificacao VARCHAR(50) NOT NULL,
    max_historico INT NOT NULL DEFAULT 20,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
CREATE TABLE atendentes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    numero_whatsapp VARCHAR(50) NOT NULL UNIQUE,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    indice_round_robin INT NOT NULL DEFAULT 0,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
CREATE TABLE sessoes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    agente_id INT NOT NULL,
    telefone VARCHAR(50) NOT NULL,
    handler ENUM('bot','humano') NOT NULL DEFAULT 'bot',
    total_mensagens INT NOT NULL DEFAULT 0,
    motivo_handoff TEXT NULL,
    encerrada_em DATETIME NULL,
    motivo_encerramento VARCHAR(50) NULL,
    atendente_id INT NULL,
    chatwoot_conversation_id INT NULL,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ultima_interacao DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT uq_agente_telefone UNIQUE (agente_id, telefone),
    CONSTRAINT fk_sessao_agente FOREIGN KEY (agente_id) REFERENCES agentes(id) ON DELETE CASCADE,
    CONSTRAINT fk_sessao_atendente FOREIGN KEY (atendente_id) REFERENCES atendentes(id)
);
CREATE TABLE mensagens (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sessao_id INT NOT NULL,
    role ENUM('user','assistant') NOT NULL,
    conteudo TEXT NOT NULL,
    criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_mensagem_sessao FOREIGN KEY (sessao_id) REFERENCES sessoes(id) ON DELETE CASCADE
);
INSERT INTO agentes (nome, instance_name, system_prompt, numero_notificacao)
VALUES ('Duplicate', 'duplicate-instance', 'legacy prompt', '0');
INSERT INTO sessoes (agente_id, telefone, chatwoot_conversation_id)
VALUES (1, '551100000011', 999), (1, '551100000012', 999);
SQL
if docker exec --interactive "${mysql_container}" mysql \
    --user=root --password="${mysql_root_password}" outbox_duplicate \
    < "${repo_root}/db/init.sql" >/dev/null 2>&1; then
    echo "duplicate legacy migration unexpectedly succeeded" >&2
    exit 1
fi

# Tests run as an ordinary schema-scoped user: no SUPER, trigger, or admin grant.
docker exec "${mysql_container}" mysql \
    --user=root \
    --password="${mysql_root_password}" \
    --execute="CREATE USER 'outbox_app'@'%' IDENTIFIED BY '${mysql_app_password}'; GRANT SELECT, INSERT, UPDATE, DELETE ON outbox_fresh.* TO 'outbox_app'@'%';"

docker run --rm \
    --network="container:${mysql_container}" \
    --volume="${repo_root}:/workspace:ro" \
    --volume="${repo_root}/app/src:/app/src:ro" \
    --volume="${repo_root}/app/tests:/app/tests:ro" \
    --volume="${repo_root}/app/.env.example:/app/.env.example:ro" \
    --tmpfs=/app/logs \
    --workdir=/app \
    --env WPP_REPO_ROOT=/workspace \
    --env OUTBOX_TEST_DATABASE_URL="mysql+aiomysql://outbox_app:${mysql_app_password}@127.0.0.1:3306/outbox_fresh" \
    --env DATABASE_URL="mysql+aiomysql://outbox_app:${mysql_app_password}@127.0.0.1:3306/outbox_fresh" \
    --env WPP_DISPOSABLE_TEST_DATABASE_MARKER=outbox_fresh \
    --env OPENROUTER_API_KEY=test-openrouter-key \
    --env EVOLUTION_API_URL=http://evolution.test \
    --env EVOLUTION_API_KEY=test-evolution-key \
    --env EVOLUTION_WEBHOOK_BEARER_TOKEN=test-evolution-webhook-secret \
    --env CHATWOOT_BASE_URL=http://chatwoot.test \
    --env CHATWOOT_API_TOKEN=test-chatwoot-token \
    --env CHATWOOT_ACCOUNT_ID=1 \
    --env CHATWOOT_WEBHOOK_SECRET=test-chatwoot-webhook-secret \
    --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "${app_image}" \
    python -m unittest discover -s tests -v
