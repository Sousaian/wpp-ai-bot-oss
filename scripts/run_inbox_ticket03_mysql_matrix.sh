#!/usr/bin/env bash
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Ticket 02's disposable matrix applies fresh/repeat/intermediate/legacy schema,
# runs the complete zero-skip suite (including ticket 03 intake), and owns
# unconditional container/network/volume/image cleanup plus shared-stack checks.
"${repo_root}/scripts/run_inbox_ticket02_mysql_matrix.sh"
status=$?
if (( status != 0 )); then
    exit "${status}"
fi

"${repo_root}/scripts/check_inbox_ticket03_migrations.sh"
status=$?
if (( status != 0 )); then
    exit "${status}"
fi

pycache_dir="$(mktemp -d)"
trap 'rm -rf "${pycache_dir}"' EXIT INT TERM
PYTHONPYCACHEPREFIX="${pycache_dir}" python -m py_compile \
    "${repo_root}/app/src/config.py" \
    "${repo_root}/app/src/database/models.py" \
    "${repo_root}/app/src/database/repositories/inbound_event_repo.py" \
    "${repo_root}/app/src/database/repositories/delivery_worker_repo.py" \
    "${repo_root}/app/src/database/repositories/scheduler_repo.py" \
    "${repo_root}/app/src/services/evolution_orchestrator.py" \
    "${repo_root}/app/src/main.py" \
    "${repo_root}/app/tests/test_scheduler_intake_mysql.py" \
    "${repo_root}/scripts/check_trigger_body_digests.py" || exit $?

bash -n "${repo_root}/scripts/run_inbox_ticket03_mysql_matrix.sh" || exit $?
bash -n "${repo_root}/scripts/check_inbox_ticket03_migrations.sh" || exit $?
echo "TICKET03_MATRIX → PASS"
