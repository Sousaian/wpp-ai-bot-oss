#!/usr/bin/env bash
# F0-T5 R2: real process main()/SIGTERM, resistant MDL/socket, counted mutations.
# All named Docker resources are disposable p40-*; never starts shared Compose.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${repo}/scripts/test_image_version.sh"
build_code_version="$(image_test_build_version "${repo}")"
suffix="$(date +%s)-$$"
prefix="p40-heartbeat-${suffix}"
mysql="${prefix}-mysql"
network="${prefix}-net"
volume="${prefix}-data"
image="${WPP_TEST_IMAGE:-${prefix}-app:local}"
base_image="${prefix}-mysql:local"
[[ "$image" == p40-* ]] || { echo 'image must have p40- prefix' >&2; exit 2; }
logs="${WPP_TEST_LOG_ROOT:-/tmp/p40-heartbeat-logs}/${suffix}"
mkdir -p "$logs"
mutants="$(mktemp -d /tmp/p40-heartbeat-mutants-XXXXXX)"
built=0
shared=traycer-wpp-ai-bot-soft-ocelot
shared_count() { docker ps -aq --filter "label=com.docker.compose.project=$shared" | wc -l | tr -d ' '; }
cleanup() {
    result=$?
    trap - EXIT INT TERM
    for name in "${prefix}-tests" "$mysql"; do
        if docker container inspect "$name" >/dev/null 2>&1; then
            echo "F0T5_REMOVE container=$name"
            docker rm -f "$name" || result=70
        fi
    done
    if docker network inspect "$network" >/dev/null 2>&1; then
        echo "F0T5_REMOVE network=$network"
        docker network rm "$network" || result=70
    fi
    if docker volume inspect "$volume" >/dev/null 2>&1; then
        echo "F0T5_REMOVE volume=$volume"
        docker volume rm "$volume" || result=70
    fi
    if docker image inspect "$base_image" >/dev/null 2>&1; then
        echo "F0T5_REMOVE image=$base_image"
        docker image rm "$base_image" || result=70
    fi
    if [[ "$built" == 1 ]]; then
        echo "F0T5_REMOVE image=$image"
        docker image rm "$image" || result=70
    fi
    rm -rf "$mutants"
    for pair in "container:$mysql" "container:${prefix}-tests" "network:$network" "volume:$volume" "image:$base_image"; do
        kind=${pair%%:*}; name=${pair#*:}
        if docker "$kind" inspect "$name" >/dev/null 2>&1; then result=70; fi
    done
    if [[ "$(shared_count)" != 0 ]]; then result=71; fi
    echo "F0T5_CLEANUP → exit=$result resources_absent=$([[ $result != 70 ]] && echo 1 || echo 0) shared_containers=$(shared_count)"
    echo "F0T5_LOGS → $logs"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
[[ "$(shared_count)" == 0 ]] || { echo 'shared Compose stack must stay at zero'; exit 2; }
[[ "$(python "$repo/scripts/heartbeat_shutdown_mutations.py" count)" == 20 ]]
if [[ -z "${WPP_TEST_IMAGE:-}" ]]; then
    docker build --force-rm --build-arg "DELIVERY_WORKER_CODE_VERSION=${build_code_version}" --no-cache --tag "$image" "$repo/app" >"$logs/build.log" 2>&1
    built=1
fi
image_code_version="$(image_test_version "${image}")" || exit $?
docker tag mysql:8.0 "$base_image"
docker network create --internal "$network" >/dev/null
docker volume create "$volume" >/dev/null
docker run --detach --name "$mysql" --network "$network" -v "$volume:/var/lib/mysql" \
    -e MYSQL_ROOT_PASSWORD=p40-synthetic "$base_image" --performance-schema=ON >/dev/null
ready=0
streak=0
for _ in $(seq 1 150); do
    if docker exec -e MYSQL_PWD=p40-synthetic "$mysql" mysql -uroot -h127.0.0.1 -e 'SELECT 1' </dev/null >/dev/null 2>&1; then
        streak=$((streak + 1)); if (( streak >= 3 )); then ready=1; break; fi
    else streak=0; fi
    sleep 1
done
[[ "$ready" == 1 ]]
query() { docker exec -e MYSQL_PWD=p40-synthetic "$mysql" mysql -uroot "$@" </dev/null; }
query --batch --skip-column-names -e 'SELECT VERSION()' >"$logs/mysql-version.log"
query -e 'CREATE DATABASE p40_t083_t082 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'
docker exec -i -e MYSQL_PWD=p40-synthetic "$mysql" mysql -uroot --default-character-set=latin1 p40_t083_t082 \
    < "$repo/db/init.sql" >"$logs/migration.log" 2>&1
# DML-only runtime user. Extra PROCESS/diagnostic SELECT and LOCK TABLES are
# test-only capabilities, used exclusively for observed MDL/transaction barriers.
query -e "CREATE USER 'p40'@'%' IDENTIFIED BY 'synthetic';
GRANT SELECT,INSERT,UPDATE,DELETE,LOCK TABLES ON p40_t083_t082.* TO 'p40'@'%';
GRANT SELECT ON performance_schema.* TO 'p40'@'%'; GRANT PROCESS ON *.* TO 'p40'@'%';"
run_python() {
    local root=$1; shift
    docker run --rm --name "${prefix}-tests" --network "container:$mysql" \
        -v "$root:/workspace:ro" --tmpfs /runtime --tmpfs /tmp/pycache --workdir /runtime \
        -e PYTHONPYCACHEPREFIX=/tmp/pycache -e PYTHONPATH=/workspace/app -e WPP_REPO_ROOT=/workspace \
        -e OUTBOX_TEST_DATABASE_URL=mysql+aiomysql://p40:synthetic@127.0.0.1:3306/p40_t083_t082 \
        -e DATABASE_URL=mysql+aiomysql://p40:synthetic@127.0.0.1:3306/p40_t083_t082 \
        -e P40_ADMIN_DATABASE_URL=mysql+aiomysql://root:p40-synthetic@127.0.0.1:3306/p40_t083_t082 \
        -e WPP_DISPOSABLE_TEST_DATABASE_MARKER=p40_t083_t082 -e OPENROUTER_API_KEY=synthetic \
        -e EVOLUTION_API_URL=http://evolution.invalid -e EVOLUTION_API_KEY=synthetic \
        -e EVOLUTION_WEBHOOK_BEARER_TOKEN=synthetic -e CHATWOOT_BASE_URL=http://chatwoot.invalid \
        -e CHATWOOT_API_TOKEN=synthetic -e CHATWOOT_ACCOUNT_ID=1 -e CHATWOOT_WEBHOOK_SECRET=synthetic \
        --env "DELIVERY_WORKER_CODE_VERSION=${image_code_version}" "$image" timeout 2400 python "$@" </dev/null
}
run_tests() { local root=$1; shift; run_python "$root" -m unittest -v "$@"; }
check_pass() {
    local log=$1 expected=$2
    grep -qE "^Ran ${expected} tests? in " "$log"
    grep -qE '^OK$' "$log"
    [[ "$(grep -cE '^test_.* \(' "$log")" == "$expected" ]]
}
modules=(tests.test_heartbeat_shutdown_unit tests.test_process_shutdown_unit tests.test_heartbeat_shutdown_mysql tests.test_heartbeat_turn_shutdown_mysql tests.test_process_shutdown_mysql)
run_tests "$repo" "${modules[@]}" >"$logs/positive.log" 2>&1
check_pass "$logs/positive.log" 42
echo 'F0T5_POSITIVE → 42/42 (unit=23 mysql=8 subprocess_mysql=11)'
# Legacy mutations execute 22 fast unit/DB cases; process mutations execute
# focused REAL subprocess/MDL cases. Every list entry has an exact count.
killed=0
mutation_cases=0
while IFS='|' read -r -u 3 name expected test_names; do
    python "$repo/scripts/heartbeat_shutdown_mutations.py" apply "$name" "$repo" "$mutants/$name"
    read -r -a selected <<< "$test_names"
    if run_tests "$mutants/$name" "${selected[@]}" >"$logs/mutation-$name.log" 2>&1; then
        echo "F0T5_MUTATION → SURVIVED: $name"; exit 1
    fi
    grep -qE "^Ran ${expected} tests? in " "$logs/mutation-$name.log"
    grep -qE '^FAILED \(failures=[1-9]' "$logs/mutation-$name.log"
    [[ "$(grep -cE '^test_.* \(' "$logs/mutation-$name.log")" == "$expected" ]]
    ! grep -qE '^ERROR:|Failed to import test module' "$logs/mutation-$name.log"
    echo "F0T5_MUTATION → killed: $name ($expected cases executed)"
    killed=$((killed + 1))
    mutation_cases=$((mutation_cases + expected))
done 3< <(python "$repo/scripts/heartbeat_shutdown_mutations.py" matrix)
[[ "$killed" == 20 && "$mutation_cases" == 215 ]]
run_tests "$repo" "${modules[@]}" >"$logs/control.log" 2>&1
check_pass "$logs/control.log" 42
echo 'F0T5_CONTROL → 42/42'
run_tests "$repo" tests.test_replica_compat_mysql >"$logs/replica-regression.log" 2>&1
check_pass "$logs/replica-regression.log" 29
echo 'F0T5_REPLICA_REGRESSION → 29/29'
if [[ "${P40_REGRESSION:-1}" == 1 ]]; then
    run_python "$repo" -c 'import unittest; s=unittest.defaultTestLoader.discover("/workspace/app/tests", top_level_dir="/workspace/app"); print("F0T5_DISCOVERY_COUNT", s.countTestCases()); assert not unittest.defaultTestLoader.errors, unittest.defaultTestLoader.errors' \
        >"$logs/discovery-count.log" 2>&1
    expected="$(sed -n 's/^F0T5_DISCOVERY_COUNT //p' "$logs/discovery-count.log")"
    run_python "$repo" -m unittest discover -v -s /workspace/app/tests -t /workspace/app -p 'test_*.py' >"$logs/regression.log" 2>&1
    check_pass "$logs/regression.log" "$expected"
    echo "F0T5_GLOBAL_REGRESSION → $expected/$expected import_errors=0"
fi
bash -n "$repo/scripts/run_heartbeat_shutdown_matrix.sh"
git -C "$repo" diff --check
echo "F0T5_MATRIX → positives=42 control=42 mutations_killed=$killed mutation_cases=$mutation_cases replica_regression=29 process_cases=11"
