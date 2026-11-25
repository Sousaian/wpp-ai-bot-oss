#!/usr/bin/env bash
# AUTH-1 migration matrix on a throwaway MySQL 8.0 (internal network, synthetic
# credentials): fresh install, re-application under latin1/utf8mb4 clients,
# upgrade of an existing database created by the base init.sql (legacy rows
# checksummed before/after, schema_version unchanged) and divergent shapes that
# must fail closed with the named error and stay unrewritten.
#   bash scripts/run_auth_schema_mysql_matrix.sh [base-ref]   (default: origin/main)
set -Eeuo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_ref="${1:-origin/main}"
label="authmx-$$"
net="$label-net"; db="$label-db"
work="$(mktemp -d)"
cleanup() {
    docker rm -f "$db" >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

git -C "$repo" show "$base_ref:db/init.sql" > "$work/base.sql"
cp "$repo/db/init.sql" "$work/new.sql"
grep -q "AUTH-1" "$work/base.sql" && { echo "base ref already contains AUTH-1"; exit 1; }

docker network create --internal "$net" >/dev/null
docker run -d --name "$db" --network "$net" -e MYSQL_ROOT_PASSWORD=synthetic-root mysql:8.0 >/dev/null
for _ in $(seq 1 180); do
    docker exec -e MYSQL_PWD=synthetic-root "$db" mysql -uroot -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1 && break
    sleep 1
done
q() { docker exec -e MYSQL_PWD=synthetic-root "$db" mysql -uroot -N -B "$@" </dev/null; }
apply() { # apply <database> <file> <charset>
    docker exec -i -e MYSQL_PWD=synthetic-root "$db" mysql -uroot --default-character-set="$3" "$1" < "$2" 2>&1
}
fresh() { q -e "DROP DATABASE IF EXISTS $1; CREATE DATABASE $1 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"; }

passed=0; expected=0
case_ok() { expected=$((expected + 1)); passed=$((passed + 1)); echo "PASS $1"; }
fail() { echo "FAIL $1"; exit 1; }

# 1. fresh install (both client charsets) + idempotent re-application
for cs in latin1 utf8mb4; do
    fresh "f_$cs"
    apply "f_$cs" "$work/new.sql" "$cs" >/dev/null || fail "fresh-$cs"
    [[ "$(q f_$cs -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name LIKE 'auth\\_%'")" == 6 ]] || fail "fresh-$cs tables"
    case_ok "fresh-$cs"
    for again in latin1 utf8mb4; do
        apply "f_$cs" "$work/new.sql" "$again" >/dev/null || fail "reapply-$cs-$again"
        case_ok "reapply-$cs-then-$again"
    done
done

# 2. existing database: base schema + synthetic legacy rows, then upgrade
fresh existing
apply existing "$work/base.sql" latin1 >/dev/null || fail "base apply"
q existing -e "INSERT INTO agentes (nome, instance_name, system_prompt, numero_notificacao) VALUES ('sintetico', 'authmx-inst', 'prompt', '0');
INSERT INTO sessoes (agente_id, telefone, handler, delivery_stream_key) SELECT id, '5511999999001', 'bot', UUID() FROM agentes;
INSERT INTO mensagens (sessao_id, role, conteudo) SELECT id, 'user', 'oi' FROM sessoes;"
# Settle the base's own idempotent backfills (e.g. modelo_llm) before measuring.
apply existing "$work/base.sql" latin1 >/dev/null || fail "base reapply"
legacy="agentes sessoes mensagens consumo_llm outbox_deliveries scheduler_runtime_state delivery_runtime_state"
before="$(q existing -e "CHECKSUM TABLE $(echo $legacy | sed 's/ /, /g') EXTENDED")"
version_before="$(q existing -e 'SELECT schema_version FROM scheduler_runtime_state WHERE id=1')"
apply existing "$work/new.sql" latin1 >/dev/null || fail "upgrade"
after="$(q existing -e "CHECKSUM TABLE $(echo $legacy | sed 's/ /, /g') EXTENDED")"
[[ "$before" == "$after" ]] || { diff <(echo "$before") <(echo "$after"); fail "upgrade legacy rows changed"; }
[[ "$(q existing -e 'SELECT schema_version FROM scheduler_runtime_state WHERE id=1')" == "$version_before" ]] || fail "schema_version changed"
case_ok "upgrade-existing-rows-and-schema-version-unchanged"
apply existing "$work/new.sql" utf8mb4 >/dev/null || fail "upgrade reapply"
case_ok "upgrade-reapply"

# 3. divergent shapes: named failure, no rewrite
divergent() { # divergent <name> <ddl> <expected-error-fragment>
    fresh d_x
    apply d_x "$work/new.sql" latin1 >/dev/null || fail "$1 setup"
    q d_x -e "$2"
    local shape_before shape_after output
    shape_before="$(q d_x -e "SHOW CREATE TABLE auth_usuarios; SHOW CREATE TABLE auth_sessoes; SHOW CREATE TABLE auth_tentativas; SHOW CREATE TABLE auth_auditoria")"
    if output="$(apply d_x "$work/new.sql" latin1)"; then fail "$1 accepted divergent shape"; fi
    grep -q "$3" <<<"$output" || { echo "$output" | tail -3; fail "$1 wrong error"; }
    shape_after="$(q d_x -e "SHOW CREATE TABLE auth_usuarios; SHOW CREATE TABLE auth_sessoes; SHOW CREATE TABLE auth_tentativas; SHOW CREATE TABLE auth_auditoria")"
    [[ "$shape_before" == "$shape_after" ]] || fail "$1 rewrote shape"
    case_ok "divergent-$1"
}
divergent column-type "ALTER TABLE auth_usuarios MODIFY email VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL" \
    "auth-1 auth_usuarios column shape requires audited repair"
divergent column-collation "ALTER TABLE auth_sessoes MODIFY token_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL" \
    "auth-1 auth_sessoes column shape requires audited repair"
divergent column-extra "ALTER TABLE auth_auditoria ADD COLUMN telefone VARCHAR(20) NULL" \
    "auth-1 auth_auditoria column shape requires audited repair"
divergent index-dropped "ALTER TABLE auth_sessoes DROP INDEX idx_auth_sessao_expira" \
    "auth-1 auth_sessoes index shape requires audited repair"
divergent unique-dropped "ALTER TABLE auth_usuarios DROP INDEX uq_auth_usuario_email, ADD INDEX uq_auth_usuario_email (email)" \
    "auth-1 auth_usuarios index shape requires audited repair"
divergent check-changed "ALTER TABLE auth_usuarios DROP CHECK ck_auth_usuario_papel, ADD CONSTRAINT ck_auth_usuario_papel CHECK (papel IN (_ascii'admin',_ascii'operador',_ascii'leitura',_ascii'root'))" \
    "auth-1 auth_usuarios check shape requires audited repair"
divergent check-not-enforced "ALTER TABLE auth_sessoes ALTER CHECK ck_auth_sessao_estagio NOT ENFORCED" \
    "auth-1 auth_sessoes check shape requires audited repair"
divergent fk-cascade "ALTER TABLE auth_sessoes DROP FOREIGN KEY fk_auth_sessao_usuario; ALTER TABLE auth_sessoes ADD CONSTRAINT fk_auth_sessao_usuario FOREIGN KEY (usuario_id) REFERENCES auth_usuarios(id) ON DELETE CASCADE" \
    "auth-1 foreign key shape requires audited repair"
divergent fk-foreign-reference "CREATE TABLE x_ref (id BIGINT UNSIGNED PRIMARY KEY, CONSTRAINT fk_x_ref FOREIGN KEY (id) REFERENCES auth_usuarios(id))" \
    "auth-1 foreign key shape requires audited repair"
divergent attempts-origin-collation "ALTER TABLE auth_tentativas MODIFY origem VARCHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL" \
    "auth-1 auth_tentativas column shape requires audited repair"
divergent attempts-fk-dropped "ALTER TABLE auth_tentativas DROP FOREIGN KEY fk_auth_tentativa_usuario" \
    "auth-1 foreign key shape requires audited repair"
divergent trigger-added "CREATE TRIGGER trg_x BEFORE UPDATE ON auth_auditoria FOR EACH ROW SET NEW.evento = NEW.evento" \
    "auth-1 trigger shape requires audited repair"

echo "auth schema matrix: $passed/$expected passed"
[[ "$passed" == 20 && "$expected" == 20 ]] || { echo "expected 20 executed cases"; exit 1; }
