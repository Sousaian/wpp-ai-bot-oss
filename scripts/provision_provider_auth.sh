#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_DIR"

if [ ! -f .env ]; then
  echo "provider-auth: .env is missing; create it from .env.example first" >&2
  exit 2
fi

chmod 600 .env

# Uses the release base: image code, not docker-compose.dev.yml.
# Build the one shared, explicitly versioned Python image before any run/up.
"$SCRIPT_DIR/release_build.sh" --project "${COMPOSE_PROJECT_NAME:?COMPOSE_PROJECT_NAME_required}" \
  --expected-commit "$(git rev-parse HEAD)"
# Safe rollout ordering:
# 1. Keep routes in provisioning mode while installing provider-side config.
# 2. Restart the app so it loads the newly stored secrets, then probe callbacks.
# 3. Only then write enforced mode, restart, and prove invalid credentials fail.
docker compose --profile ops run --pull never --rm provider-auth-bootstrap \
  python -m src.provider_auth_cli prepare
docker compose up -d --no-build --force-recreate app
docker compose --profile ops run --pull never --rm provider-auth-bootstrap \
  python -m src.provider_auth_cli verify
docker compose --profile ops run --pull never --rm provider-auth-bootstrap \
  python -m src.provider_auth_cli enable
docker compose up -d --no-build --force-recreate app
docker compose --profile ops run --pull never --rm provider-auth-bootstrap \
  python -m src.provider_auth_cli check
docker compose up -d --no-build --force-recreate provider-auth-monitor

echo "provider-auth: provisioning, enforced readiness, and monitor startup succeeded"
