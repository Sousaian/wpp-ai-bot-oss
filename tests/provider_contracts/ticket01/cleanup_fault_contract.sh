#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SHARED_PROJECT=traycer-wpp-ai-bot-soft-ocelot
LOG_ROOT=$(mktemp -d /tmp/wpp-ticket01-cleanup-fault.XXXXXX)
trap 'rm -rf "$LOG_ROOT"' EXIT

assert_absent() {
  local run_id=$1
  local project="wpp_ticket01_contract_${run_id//-/_}"
  [[ -z "$(docker ps -aq --filter "label=com.docker.compose.project=$project")" ]]
  [[ -z "$(docker network ls -q --filter "label=com.docker.compose.project=$project")" ]]
  [[ -z "$(docker ps -aq --filter "name=wpp-ticket01-.*-${run_id}")" ]]
  [[ -z "$(docker ps -aq --filter "label=com.docker.compose.project=$SHARED_PROJECT")" ]]
}

early_id="fault-early-${$}"
set +e
TICKET01_RUN_ID=$early_id \
TICKET01_INJECT_EARLY_FAILURE=1 \
  "$SCRIPT_DIR/run.sh" >"$LOG_ROOT/early.out" 2>"$LOG_ROOT/early.err"
early_status=$?
set -e
[[ $early_status -eq 42 ]]
grep -q '"original_status":42' "$LOG_ROOT/early.err"
grep -q '"cleanup_error":0' "$LOG_ROOT/early.err"
grep -q '"disposable_resources_removed":true' "$LOG_ROOT/early.err"
grep -q '"shared_project_label_count":0' "$LOG_ROOT/early.err"
assert_absent "$early_id"

cleanup_id="fault-cleanup-${$}"
set +e
TICKET01_RUN_ID=$cleanup_id \
TICKET01_STOP_AFTER_SOURCE_PROOF=1 \
TICKET01_SIMULATE_CLEANUP_ERROR=1 \
  "$SCRIPT_DIR/run.sh" >"$LOG_ROOT/cleanup.out" 2>"$LOG_ROOT/cleanup.err"
cleanup_status=$?
set -e
[[ $cleanup_status -eq 70 ]]
grep -q '"original_status":0' "$LOG_ROOT/cleanup.err"
grep -q '"cleanup_error":1' "$LOG_ROOT/cleanup.err"
grep -q '"disposable_resources_removed":true' "$LOG_ROOT/cleanup.err"
grep -q '"shared_project_label_count":0' "$LOG_ROOT/cleanup.err"
assert_absent "$cleanup_id"

echo '{"evidence":"CLEANUP_FAULT_INJECTION","early_failure_status_preserved":42,"cleanup_error_status":70,"resources_removed":true,"shared_project_label_count":0}'
