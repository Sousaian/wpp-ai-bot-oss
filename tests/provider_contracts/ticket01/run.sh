#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SHARED_PROJECT=traycer-wpp-ai-bot-soft-ocelot
EVOLUTION_IMAGE='evoapicloud/evolution-api@sha256:966625532d9076a2381e973a271307d107e6f070450de3abeeea8bd18be07252'
CHATWOOT_IMAGE='chatwoot/chatwoot@sha256:18f280a63070073c7588164eed31303cc788123d899733e54aeec4c39d1d8dd9'
RUN_ID_RAW=${TICKET01_RUN_ID:-"${$}-$(python -c 'import secrets; print(secrets.token_hex(4))')"}
RUN_ID=$(printf '%s' "$RUN_ID_RAW" | tr -cd '[:alnum:]-')
[[ -n "$RUN_ID" ]]
PROJECT="wpp_ticket01_contract_${RUN_ID//-/_}"
EXTRACT_ROOT=$(mktemp -d "/tmp/wpp-ticket01-${RUN_ID}.XXXXXX")
EVOLUTION_SOURCE_CONTAINER="wpp-ticket01-evolution-source-${RUN_ID}"
CHATWOOT_SOURCE_CONTAINER="wpp-ticket01-chatwoot-source-${RUN_ID}"
CLIENT_CONTAINER="wpp-ticket01-chatwoot-client-${RUN_ID}"

export TICKET01_POSTGRES_PASSWORD TICKET01_SECRET_KEY_BASE TICKET01_USER_PASSWORD
TICKET01_POSTGRES_PASSWORD=$(python -c 'import secrets; print(secrets.token_urlsafe(32))')
TICKET01_SECRET_KEY_BASE=$(python -c 'import secrets; print(secrets.token_hex(64))')
TICKET01_USER_PASSWORD=$(python -c 'import secrets; print("T1!" + secrets.token_urlsafe(32))')
COMPOSE=(docker compose --env-file /dev/null -p "$PROJECT" -f "$SCRIPT_DIR/docker-compose.chatwoot.yml")

cleanup() {
  local original_status=$?
  local cleanup_error=0
  local down_status=0
  local project_containers=''
  local project_networks=''
  local named_containers=''
  local shared_containers=''
  local query_status=0
  trap - EXIT INT TERM
  set +e

  docker rm -f \
    "$EVOLUTION_SOURCE_CONTAINER" \
    "$CHATWOOT_SOURCE_CONTAINER" \
    "$CLIENT_CONTAINER" >/dev/null 2>&1
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1
  down_status=$?
  (( down_status == 0 )) || cleanup_error=1
  rm -rf "$EXTRACT_ROOT"
  [[ ! -e "$EXTRACT_ROOT" ]] || cleanup_error=1

  project_containers=$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null)
  query_status=$?
  (( query_status == 0 )) || cleanup_error=1
  project_networks=$(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null)
  query_status=$?
  (( query_status == 0 )) || cleanup_error=1
  local container_name=''
  local found=''
  for container_name in \
    "$EVOLUTION_SOURCE_CONTAINER" \
    "$CHATWOOT_SOURCE_CONTAINER" \
    "$CLIENT_CONTAINER"; do
    found=$(docker ps -aq --filter "name=^/${container_name}$" 2>/dev/null)
    query_status=$?
    (( query_status == 0 )) || cleanup_error=1
    named_containers+=$found
  done
  shared_containers=$(docker ps -aq --filter "label=com.docker.compose.project=$SHARED_PROJECT" 2>/dev/null)
  query_status=$?
  (( query_status == 0 )) || cleanup_error=1

  [[ -z "$project_containers" ]] || cleanup_error=1
  [[ -z "$project_networks" ]] || cleanup_error=1
  [[ -z "$named_containers" ]] || cleanup_error=1
  [[ -z "$shared_containers" ]] || cleanup_error=1
  if [[ ${TICKET01_SIMULATE_CLEANUP_ERROR:-0} == 1 ]]; then
    cleanup_error=1
  fi

  if (( original_status == 0 && cleanup_error == 0 )); then
    echo '{"evidence":"CLEANUP","result":"pass","disposable_resources_removed":true,"shared_project_label_count":0}'
    exit 0
  fi

  printf '{"evidence":"CLEANUP","result":"fail","original_status":%d,"cleanup_error":%d,"disposable_resources_removed":%s,"shared_project_label_count":%s}\n' \
    "$original_status" \
    "$cleanup_error" \
    "$([[ -z "$project_containers$project_networks$named_containers" ]] && echo true || echo false)" \
    "$([[ -z "$shared_containers" ]] && echo 0 || echo unknown)" >&2
  if (( original_status != 0 )); then
    exit "$original_status"
  fi
  exit 70
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$SHARED_PROJECT")" ]]; then
  echo "shared Compose project must have zero containers: $SHARED_PROJECT" >&2
  exit 1
fi

for image in "$EVOLUTION_IMAGE" "$CHATWOOT_IMAGE" python:3.12-slim pgvector/pgvector:pg15 redis:7-alpine; do
  docker image inspect "$image" >/dev/null
done
[[ "$(docker image inspect "$EVOLUTION_IMAGE" --format '{{index .RepoDigests 0}}')" == "$EVOLUTION_IMAGE" ]]
[[ "$(docker image inspect "$CHATWOOT_IMAGE" --format '{{index .RepoDigests 0}}')" == "$CHATWOOT_IMAGE" ]]

mkdir -p "$EXTRACT_ROOT/evolution" "$EXTRACT_ROOT/chatwoot"
docker create --name "$EVOLUTION_SOURCE_CONTAINER" "$EVOLUTION_IMAGE" >/dev/null
docker create --name "$CHATWOOT_SOURCE_CONTAINER" "$CHATWOOT_IMAGE" >/dev/null

docker cp "$EVOLUTION_SOURCE_CONTAINER":/evolution/package.json "$EXTRACT_ROOT/evolution/package.json"
docker cp "$EVOLUTION_SOURCE_CONTAINER":/evolution/dist/api/routes/sendMessage.router.js.map "$EXTRACT_ROOT/evolution/sendMessage.router.js.map"
docker cp "$EVOLUTION_SOURCE_CONTAINER":/evolution/dist/api/controllers/sendMessage.controller.js.map "$EXTRACT_ROOT/evolution/sendMessage.controller.js.map"

docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/package.json "$EXTRACT_ROOT/chatwoot/package.json"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/app/builders/messages/message_builder.rb "$EXTRACT_ROOT/chatwoot/message_builder.rb"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/app/models/message.rb "$EXTRACT_ROOT/chatwoot/message.rb"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/app/finders/message_finder.rb "$EXTRACT_ROOT/chatwoot/message_finder.rb"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/app/controllers/api/v1/accounts/conversations/messages_controller.rb "$EXTRACT_ROOT/chatwoot/messages_controller.rb"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/app/views/api/v1/models/_message.json.jbuilder "$EXTRACT_ROOT/chatwoot/_message.json.jbuilder"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/app/jobs/send_reply_job.rb "$EXTRACT_ROOT/chatwoot/send_reply_job.rb"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/app/services/messages/send_email_notification_service.rb "$EXTRACT_ROOT/chatwoot/send_email_notification_service.rb"
docker cp "$CHATWOOT_SOURCE_CONTAINER":/app/db/schema.rb "$EXTRACT_ROOT/chatwoot/schema.rb"

docker rm "$EVOLUTION_SOURCE_CONTAINER" "$CHATWOOT_SOURCE_CONTAINER" >/dev/null
python "$SCRIPT_DIR/verify_pinned_sources.py" "$EXTRACT_ROOT"
python "$SCRIPT_DIR/synthetic_presence_contract.py"
python "$SCRIPT_DIR/chunking_contract.py"

if [[ ${TICKET01_STOP_AFTER_SOURCE_PROOF:-0} == 1 ]]; then
  exit 0
fi

"${COMPOSE[@]}" up -d postgres redis
if [[ ${TICKET01_INJECT_EARLY_FAILURE:-0} == 1 ]]; then
  echo "synthetic early failure after disposable Compose startup" >&2
  exit 42
fi

SEED_LOG="$EXTRACT_ROOT/chatwoot-seed.log"
"${COMPOSE[@]}" run --rm chatwoot bundle exec rails db:chatwoot_prepare >"$EXTRACT_ROOT/chatwoot-migrate.log" 2>&1
"${COMPOSE[@]}" run --rm chatwoot bundle exec rails runner /contract/chatwoot_seed.rb >"$SEED_LOG" 2>&1
SEED_B64=$(sed -n 's/^TICKET01_SEED=//p' "$SEED_LOG" | tail -n 1)
if [[ -z "$SEED_B64" ]]; then
  echo "Chatwoot synthetic seed did not return fixture metadata" >&2
  tail -n 40 "$SEED_LOG" >&2
  exit 1
fi
seed_value() {
  python - "$SEED_B64" "$1" <<'PY'
import base64, json, sys
print(json.loads(base64.b64decode(sys.argv[1]))[sys.argv[2]])
PY
}

export TICKET01_ACCOUNT_ID TICKET01_API_TOKEN TICKET01_INBOX_ID TICKET01_CONTACT_ID TICKET01_CONVERSATION_ID
TICKET01_ACCOUNT_ID=$(seed_value account_id)
TICKET01_API_TOKEN=$(seed_value api_token)
TICKET01_INBOX_ID=$(seed_value inbox_id)
TICKET01_CONTACT_ID=$(seed_value contact_id)
TICKET01_CONVERSATION_ID=$(seed_value conversation_display_id)
export TICKET01_CHATWOOT_URL=http://chatwoot:3000

"${COMPOSE[@]}" up -d chatwoot
docker run --rm \
  --name "$CLIENT_CONTAINER" \
  --network "${PROJECT}_default" \
  -e TICKET01_CHATWOOT_URL \
  -e TICKET01_ACCOUNT_ID \
  -e TICKET01_API_TOKEN \
  -e TICKET01_INBOX_ID \
  -e TICKET01_CONTACT_ID \
  -e TICKET01_CONVERSATION_ID \
  -v "$SCRIPT_DIR/chatwoot_http_contract.py:/contract/chatwoot_http_contract.py:ro" \
  python:3.12-slim python /contract/chatwoot_http_contract.py
