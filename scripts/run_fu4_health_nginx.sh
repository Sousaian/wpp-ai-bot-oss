#!/usr/bin/env bash
set -euo pipefail
# FU-4: real MySQL + app + both Nginx configurations, no shared stack/provider.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python "${repo_root}/scripts/check_fu4_health_nginx.py"
