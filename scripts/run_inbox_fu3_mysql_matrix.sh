#!/usr/bin/env bash
set -euo pipefail

# FU-3: uses T08-7's resource/log contract; standalone schema-only gate.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
resource_prefix="${WPP_TEST_RESOURCE_PREFIX:-fu3-}"
[[ "${resource_prefix}" =~ ^[a-z][a-z0-9-]*-$ ]] || exit 2
suffix="${FU3_RUN_SUFFIX:-$(date +%s)-$$-$RANDOM}"
container="${resource_prefix}digest-${suffix}"
volume="${resource_prefix}data-${suffix}"
workdir="${WPP_TEST_LOG_ROOT:-${repo_root}/.test-runs}/fu3-${suffix}"
mkdir -p "${workdir}"
shared_project=traycer-wpp-ai-bot-soft-ocelot
shared_count() { docker ps -aq --filter "label=com.docker.compose.project=${shared_project}" | wc -l | tr -d ' '; }
[[ "$(shared_count)" == 0 ]] || exit 2
cleanup() {
    local status=$?
    trap - EXIT
    docker rm -f "${container}" >/dev/null 2>&1 || true
    docker volume rm "${volume}" >/dev/null 2>&1 || true
    if docker container inspect "${container}" >/dev/null 2>&1 || docker volume inspect "${volume}" >/dev/null 2>&1; then
        echo 'FU3_CLEANUP FAIL'; exit 70
    fi
    [[ "$(shared_count)" == 0 ]] || { echo 'SHARED_STACK FAIL'; exit 71; }
    echo 'FU3_CLEANUP PASS owned=0 shared=0'
    echo "FU3_LOGS ${workdir}"
    exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export MYSQL_PWD="$(python -c 'import secrets; print(secrets.token_urlsafe(24))')"
docker volume create "${volume}" >/dev/null
docker run --detach --name "${container}" --network=none --volume="${volume}:/var/lib/mysql" \
    --env "MYSQL_ROOT_PASSWORD=${MYSQL_PWD}" mysql:8.0 \
    --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci >/dev/null
ready=0
for _ in $(seq 1 150); do
    if docker exec --env MYSQL_PWD "${container}" mysql -uroot --protocol=socket -N -B -e 'SELECT VERSION()' </dev/null >"${workdir}/version.log" 2>&1; then
        ready=$((ready+1)); (( ready < 3 )) || break
    else ready=0; fi
    sleep 1
done
(( ready >= 3 )) || { docker logs "${container}"; exit 1; }
python "${repo_root}/scripts/inbox_fu3_schema_matrix.py" --container "${container}" --log-dir "${workdir}"
# Unchanged security matrices: exact original counts + raw SQL logs.
mysql_root() { docker exec --interactive --env MYSQL_PWD "${container}" mysql -uroot --protocol=socket "$@"; }
source "${repo_root}/scripts/inbox_sec_s1_schema_matrix.sh"
source "${repo_root}/scripts/inbox_sec2_s1_schema_matrix.sh"
echo 'FU3_MATRIX PASS charsets=3 sec_s1=24/12/39 sec2_s1=3/6/18/3'
