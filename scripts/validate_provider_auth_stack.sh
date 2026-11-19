#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
VALIDATION_DIR=$(mktemp -d)
VALIDATION_ENV="$VALIDATION_DIR/.env"
PROJECT_NAME="wpp-provider-auth-validation"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.provider-auth-validation.yml"
. "$SCRIPT_DIR/test_image_version.sh"
BUILD_CODE_VERSION=$(image_test_build_version "$PROJECT_DIR")

cleanup() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" \
    --env-file "$VALIDATION_ENV" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$VALIDATION_DIR"
}
trap cleanup EXIT INT TERM
umask 077

MYSQL_PASSWORD=$(openssl rand -hex 24)
MYSQL_ROOT_PASSWORD=$(openssl rand -hex 24)
POSTGRES_PASSWORD=$(openssl rand -hex 24)
CHATWOOT_DB_PASSWORD=$(openssl rand -hex 24)
CHATWOOT_SECRET_KEY=$(openssl rand -hex 64)
EVOLUTION_API_KEY=$(openssl rand -hex 32)

export PROVIDER_AUTH_VALIDATION_DIR="$VALIDATION_DIR"
export PROVIDER_AUTH_VALIDATION_ENV="$VALIDATION_ENV"
export PROVIDER_AUTH_VALIDATION_UID
export PROVIDER_AUTH_VALIDATION_GID
PROVIDER_AUTH_VALIDATION_UID=$(id -u)
PROVIDER_AUTH_VALIDATION_GID=$(id -g)

{
  echo "COMPOSE_PROJECT_NAME=$PROJECT_NAME"
  echo "DELIVERY_WORKER_CODE_VERSION=$BUILD_CODE_VERSION"
  echo "OPENROUTER_API_KEY=validation-only"
  echo "DATABASE_URL=mysql+aiomysql://wpp_user:$MYSQL_PASSWORD@db:3306/wpp_bot"
  echo "MYSQL_DATABASE=wpp_bot"
  echo "MYSQL_USER=wpp_user"
  echo "MYSQL_PASSWORD=$MYSQL_PASSWORD"
  echo "MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD"
  echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD"
  echo "CHATWOOT_DB_PASSWORD=$CHATWOOT_DB_PASSWORD"
  echo "CHATWOOT_SECRET_KEY=$CHATWOOT_SECRET_KEY"
  echo "EVOLUTION_API_URL=http://evolution:8080"
  echo "EVOLUTION_API_KEY=$EVOLUTION_API_KEY"
  echo "EVOLUTION_WEBHOOK_BEARER_TOKEN="
  echo "CHATWOOT_BASE_URL=http://chatwoot:3000"
  echo "CHATWOOT_API_TOKEN="
  echo "CHATWOOT_ACCOUNT_ID=1"
  echo "CHATWOOT_WEBHOOK_ID="
  echo "CHATWOOT_WEBHOOK_SECRET="
  echo "PROVIDER_WEBHOOK_AUTH_MODE=provisioning"
  echo "PROVIDER_CALLBACK_BASE_URL=http://app:5000"
  echo "PROVIDER_AUTH_PROBE_BASE_URL=http://app:5000"
  echo "PROVIDER_AUTH_READINESS_MAX_AGE_SECONDS=10"
  echo "PROVIDER_AUTH_READINESS_CACHE_SECONDS=1"
  echo "PROVIDER_AUTH_MONITOR_INTERVAL_SECONDS=2"
  echo "PROVIDER_AUTH_MONITOR_AUDIT_TIMEOUT_SECONDS=4"
  echo "PROVIDER_AUTH_MONITOR_RETRY_INITIAL_SECONDS=1"
  echo "PROVIDER_AUTH_MONITOR_RETRY_MAX_SECONDS=4"
  echo "PROVIDER_AUTH_MONITOR_SHUTDOWN_TIMEOUT_SECONDS=1"
  echo "DELIVERY_POLL_INTERVAL_SECONDS=0.2"
  echo "DELIVERY_LEASE_RENEW_INTERVAL_SECONDS=2"
  echo "DELIVERY_EVOLUTION_LEASE_SECONDS=10"
  echo "DELIVERY_CHATWOOT_LEASE_SECONDS=12"
  echo "DELIVERY_EVOLUTION_DEADLINE_SECONDS=5"
  echo "DELIVERY_CHATWOOT_DEADLINE_SECONDS=20"
  echo "DELIVERY_HEARTBEAT_INTERVAL_SECONDS=2"
  echo "DELIVERY_READINESS_INTERVAL_SECONDS=2"
  echo "DELIVERY_MAINTENANCE_INTERVAL_SECONDS=2"
  echo "DELIVERY_SHUTDOWN_GRACE_SECONDS=5"
  echo "DELIVERY_SHUTDOWN_ABORT_SECONDS=2"
  echo "PROVIDER_AUTH_VALIDATION_DIR=$VALIDATION_DIR"
  echo "PROVIDER_AUTH_VALIDATION_ENV=$VALIDATION_ENV"
  echo "PROVIDER_AUTH_VALIDATION_UID=$PROVIDER_AUTH_VALIDATION_UID"
  echo "PROVIDER_AUTH_VALIDATION_GID=$PROVIDER_AUTH_VALIDATION_GID"
} >"$VALIDATION_ENV"

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" \
    --env-file "$VALIDATION_ENV" "$@"
}

compose up -d db evolution chatwoot

# Create the trusted Chatwoot account, administrator token, and an unlocked API
# inbox inside the pinned container. Credentials go only to the private temp file.
compose exec -T chatwoot bundle exec rails runner '
  user = User.find_or_create_by!(email: "provider-auth-validation@example.invalid") do |record|
    record.name = "Provider auth validation"
    record.password = "Validation!#{SecureRandom.hex(24)}A"
    record.confirmed_at = Time.current
  end
  account = Account.find_or_create_by!(name: "Provider auth validation")
  AccountUser.find_or_create_by!(account: account, user: user) do |record|
    record.role = :administrator
  end
  access_token = user.access_token || user.create_access_token!
  channel = Channel::Api.find_or_create_by!(account: account, webhook_url: "")
  inbox = Inbox.find_or_create_by!(account: account, channel: channel) do |record|
    record.name = "Provider auth validation"
  end
  File.write(
    "/validation/chatwoot.env",
    "CHATWOOT_API_TOKEN=#{access_token.token}\n" \
      "CHATWOOT_ACCOUNT_ID=#{account.id}\n" \
      "CHATWOOT_INBOX_ID=#{inbox.id}\n"
  )
  File.chown(
    Integer(ENV.fetch("PROVIDER_AUTH_VALIDATION_UID")),
    Integer(ENV.fetch("PROVIDER_AUTH_VALIDATION_GID")),
    "/validation/chatwoot.env"
  )
  File.chmod(0600, "/validation/chatwoot.env")
'

# Replace the initially blank account values without displaying them.
while IFS= read -r assignment; do
  key=${assignment%%=*}
  sed -i "/^${key}=/d" "$VALIDATION_ENV"
  printf '%s\n' "$assignment" >>"$VALIDATION_ENV"
done <"$VALIDATION_DIR/chatwoot.env"

compose run --rm --no-deps provider-auth-bootstrap python -c '
import httpx
from src.config import settings
response = httpx.post(
    f"{settings.evolution_api_url}/instance/create",
    headers={"apikey": settings.evolution_api_key},
    json={
        "instanceName": "provider-auth-validation",
        "integration": "WHATSAPP-BAILEYS",
        "qrcode": False,
    },
    timeout=30.0,
)
response.raise_for_status()
'

CHATWOOT_INBOX_ID=$(sed -n 's/^CHATWOOT_INBOX_ID=//p' "$VALIDATION_ENV")
CHATWOOT_ACCOUNT_ID=$(sed -n 's/^CHATWOOT_ACCOUNT_ID=//p' "$VALIDATION_ENV")
compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -u root wpp_bot -e "
    INSERT INTO agentes
      (nome, instance_name, system_prompt, modelo_openai, modelo_llm,
       numero_notificacao, chatwoot_inbox_id, ativo)
    VALUES
      ('validation', 'provider-auth-validation', 'validation', 'gpt-4o-mini',
       'openai/gpt-4o-mini', '0', ${CHATWOOT_INBOX_ID}, TRUE);
  "

compose up -d app
if compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli check; then
  echo "provider-auth validation: readiness unexpectedly passed before provisioning" >&2
  exit 1
fi
compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli prepare
# The bootstrap must create receipt identity independently from the Evolution
# bearer before enforcement, without printing either secret.
compose run --rm provider-auth-bootstrap python -c '
from src.config import settings
assert settings.receipt_hmac_keyring_is_valid()
receipt = settings.inbound_receipt_hmac_keys[
    settings.inbound_receipt_hmac_current_version
].get_secret_value()
assert receipt != settings.evolution_webhook_bearer_token
'
# The first adoption must persist the stable provider identity before an
# idempotent rerun or callback URL change can update the same webhook.
MANAGED_CHATWOOT_WEBHOOK_ID=$(sed -n 's/^CHATWOOT_WEBHOOK_ID=//p' "$VALIDATION_ENV")
case "$MANAGED_CHATWOOT_WEBHOOK_ID" in
  ''|*[!0-9]*)
    echo "provider-auth validation: managed Chatwoot webhook ID was not stored" >&2
    exit 1
    ;;
esac

# Create unrelated and ambiguous provider objects, then prove stale-ID and
# first-adoption failures do not mutate or delete either object.
compose exec -T chatwoot bundle exec rails runner '
  account = Account.find_by!(name: "Provider auth validation")
  operator = account.webhooks.create!(
    name: "Operator payroll webhook",
    url: "https://operator.example/payroll",
    subscriptions: ["conversation_created"]
  )
  ambiguous = account.webhooks.create!(
    name: "WPP AI Bot callback",
    url: "https://operator.example/ambiguous",
    subscriptions: ["message_created"]
  )
  File.write(
    "/validation/ownership.env",
    "OPERATOR_WEBHOOK_ID=#{operator.id}\n" \
      "AMBIGUOUS_WEBHOOK_ID=#{ambiguous.id}\n"
  )
'
OPERATOR_WEBHOOK_ID=$(sed -n 's/^OPERATOR_WEBHOOK_ID=//p' "$VALIDATION_DIR/ownership.env")
AMBIGUOUS_WEBHOOK_ID=$(sed -n 's/^AMBIGUOUS_WEBHOOK_ID=//p' "$VALIDATION_DIR/ownership.env")

sed -i "s/^CHATWOOT_WEBHOOK_ID=.*$/CHATWOOT_WEBHOOK_ID=${OPERATOR_WEBHOOK_ID}/" "$VALIDATION_ENV"
if compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli prepare; then
  echo "provider-auth validation: stale unrelated webhook ID was accepted" >&2
  exit 1
fi
compose exec -T \
  -e OPERATOR_WEBHOOK_ID="$OPERATOR_WEBHOOK_ID" \
  chatwoot bundle exec rails runner '
    webhook = Webhook.find(Integer(ENV.fetch("OPERATOR_WEBHOOK_ID")))
    raise "operator webhook mutated" unless \
      webhook.name == "Operator payroll webhook" && \
      webhook.url == "https://operator.example/payroll" && \
      webhook.subscriptions == ["conversation_created"]
  '

sed -i 's/^CHATWOOT_WEBHOOK_ID=.*$/CHATWOOT_WEBHOOK_ID=/' "$VALIDATION_ENV"
if compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli prepare; then
  echo "provider-auth validation: ambiguous first adoption was accepted" >&2
  exit 1
fi
compose exec -T \
  -e MANAGED_CHATWOOT_WEBHOOK_ID="$MANAGED_CHATWOOT_WEBHOOK_ID" \
  -e AMBIGUOUS_WEBHOOK_ID="$AMBIGUOUS_WEBHOOK_ID" \
  chatwoot bundle exec rails runner '
    managed = Webhook.find(Integer(ENV.fetch("MANAGED_CHATWOOT_WEBHOOK_ID")))
    ambiguous = Webhook.find(Integer(ENV.fetch("AMBIGUOUS_WEBHOOK_ID")))
    raise "managed webhook mutated" unless \
      managed.name == "WPP AI Bot callback" && \
      managed.url == "http://app:5000/chatwoot/webhook"
    raise "ambiguous webhook mutated" unless \
      ambiguous.name == "WPP AI Bot callback" && \
      ambiguous.url == "https://operator.example/ambiguous"
    ambiguous.destroy!
  '
sed -i "s/^CHATWOOT_WEBHOOK_ID=.*$/CHATWOOT_WEBHOOK_ID=${MANAGED_CHATWOOT_WEBHOOK_ID}/" "$VALIDATION_ENV"

compose up -d --force-recreate app
compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli prepare

# Rotate to a second reachable service alias. Provisioning must PATCH the same
# managed Chatwoot webhook, rather than create a URL-identified duplicate.
sed -i \
  's|^PROVIDER_CALLBACK_BASE_URL=.*$|PROVIDER_CALLBACK_BASE_URL=http://app-rotated:5000|' \
  "$VALIDATION_ENV"
compose up -d --force-recreate app
compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli prepare
ROTATED_CHATWOOT_WEBHOOK_ID=$(sed -n 's/^CHATWOOT_WEBHOOK_ID=//p' "$VALIDATION_ENV")
test "$ROTATED_CHATWOOT_WEBHOOK_ID" = "$MANAGED_CHATWOOT_WEBHOOK_ID"
compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli verify
compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli enable
compose up -d --force-recreate app
compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli check
compose run --rm provider-auth-bootstrap python -m src.provider_auth_cli check

# The public path must consume the sanitized snapshot shared by the operator
# container, not repeat active provider/DB probes on each request.
compose run --rm provider-auth-bootstrap python -c '
import httpx

response = httpx.get("http://app:5000/health/provider-auth", timeout=10.0)
response.raise_for_status()
payload = response.json()
assert payload["ready"] is True
assert payload["issues"] == []
'

# Keep the snapshot fresh beyond the original ten-second expiry, then verify a
# provider outage fails closed and the bounded monitor recovers automatically.
compose up -d provider-auth-monitor
sleep 13
compose exec -T app python -c '
import httpx
response = httpx.get("http://127.0.0.1:5000/health/provider-auth", timeout=5.0)
assert response.status_code == 200 and response.json()["ready"] is True
'
MONITOR_CONTAINER=$(compose ps -q provider-auth-monitor)
test "$(docker inspect "$MONITOR_CONTAINER" --format '{{.State.Health.Status}}')" = "healthy"

compose stop chatwoot
MONITOR_FAILED_CLOSED=false
for _ in $(seq 1 20); do
  if compose exec -T app python -c '
import httpx
response = httpx.get("http://127.0.0.1:5000/health/provider-auth", timeout=5.0)
raise SystemExit(0 if response.status_code == 503 else 1)
  '; then
    MONITOR_FAILED_CLOSED=true
    break
  fi
  sleep 1
done
test "$MONITOR_FAILED_CLOSED" = "true"

compose start chatwoot
MONITOR_RECOVERED=false
for _ in $(seq 1 30); do
  if compose exec -T app python -c '
import httpx
response = httpx.get("http://127.0.0.1:5000/health/provider-auth", timeout=5.0)
raise SystemExit(0 if response.status_code == 200 and response.json()["ready"] is True else 1)
  '; then
    MONITOR_RECOVERED=true
    break
  fi
  sleep 1
done
test "$MONITOR_RECOVERED" = "true"

compose exec -T \
  -e OPERATOR_WEBHOOK_ID="$OPERATOR_WEBHOOK_ID" \
  chatwoot bundle exec rails runner '
    webhook = Webhook.find(Integer(ENV.fetch("OPERATOR_WEBHOOK_ID")))
    raise "operator webhook mutated" unless \
      webhook.name == "Operator payroll webhook" && \
      webhook.url == "https://operator.example/payroll" && \
      webhook.subscriptions == ["conversation_created"]
  '
compose stop -t 10 provider-auth-monitor
test "$(docker inspect "$MONITOR_CONTAINER" --format '{{.State.ExitCode}}')" = "0"

# Exercise one real Chatwoot -> Sidekiq -> signed callback using only disposable
# provider state and synthetic content. Dispatch remains disabled, so this
# proves ingress durability without any WhatsApp send or external LLM spend.
SYNTHETIC_PHONE=5511987654321
SYNTHETIC_PREFIX=wpp-private-probe-
SYNTHETIC_CONTENT="${SYNTHETIC_PREFIX}human-reply"
compose exec -T \
  -e SYNTHETIC_PHONE="$SYNTHETIC_PHONE" \
  -e PROVIDER_AUTH_VALIDATION_UID="$PROVIDER_AUTH_VALIDATION_UID" \
  -e PROVIDER_AUTH_VALIDATION_GID="$PROVIDER_AUTH_VALIDATION_GID" \
  chatwoot bundle exec rails runner '
    account = Account.find_by!(name: "Provider auth validation")
    inbox = account.inboxes.find_by!(name: "Provider auth validation")
    phone = ENV.fetch("SYNTHETIC_PHONE")
    contact = account.contacts.create!(
      name: "Synthetic callback validation",
      phone_number: "+#{phone}"
    )
    contact_inbox = ContactInbox.create!(
      contact: contact,
      inbox: inbox,
      source_id: SecureRandom.uuid
    )
    conversation = account.conversations.create!(
      inbox: inbox,
      contact: contact,
      contact_inbox: contact_inbox,
      status: :open
    )
    File.write(
      "/validation/callback.env",
      "CHATWOOT_CONTACT_ID=#{contact.id}\n" \
        "CHATWOOT_CONVERSATION_ID=#{conversation.id}\n"
    )
    File.chown(
      Integer(ENV.fetch("PROVIDER_AUTH_VALIDATION_UID")),
      Integer(ENV.fetch("PROVIDER_AUTH_VALIDATION_GID")),
      "/validation/callback.env"
    )
    File.chmod(0600, "/validation/callback.env")
  '

CHATWOOT_CONTACT_ID=$(sed -n 's/^CHATWOOT_CONTACT_ID=//p' "$VALIDATION_DIR/callback.env")
CHATWOOT_CONVERSATION_ID=$(sed -n 's/^CHATWOOT_CONVERSATION_ID=//p' "$VALIDATION_DIR/callback.env")
case "$CHATWOOT_CONTACT_ID:$CHATWOOT_CONVERSATION_ID" in
  *[!0-9:]*|:*|*:)
    echo "provider-auth validation: synthetic Chatwoot IDs are invalid" >&2
    exit 1
    ;;
esac

DELIVERY_CUTOVER_EPOCH=$(python -c 'from uuid import uuid4; print(uuid4())')
sed -i '/^DELIVERY_MODE=/d;/^DELIVERY_DISPATCH_ENABLED=/d;/^DELIVERY_CUTOVER_EPOCH=/d' "$VALIDATION_ENV"
{
  echo "DELIVERY_MODE=outbox"
  echo "DELIVERY_DISPATCH_ENABLED=false"
  echo "DELIVERY_CUTOVER_EPOCH=$DELIVERY_CUTOVER_EPOCH"
} >>"$VALIDATION_ENV"

compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -u root wpp_bot -e "
    UPDATE delivery_runtime_state
    SET mode = 'outbox',
        cutover_epoch = '${DELIVERY_CUTOVER_EPOCH}',
        version = version + 1,
        changed_by = 'isolated-validation',
        change_reason = 'synthetic signed callback rehearsal'
    WHERE id = 1;

    SET @agent_id = (
      SELECT id FROM agentes WHERE instance_name = 'provider-auth-validation'
    );
    INSERT INTO sessoes
      (agente_id, telefone, handler, encerrada_em, motivo_encerramento,
       chatwoot_conversation_id, delivery_stream_key,
       next_delivery_sequence, handoff_generation)
    VALUES
      (@agent_id, '${SYNTHETIC_PHONE}', 'encerrada', NOW(6), 'handoff',
       ${CHATWOOT_CONVERSATION_ID}, UUID(), 1, 1);
    SET @session_id = LAST_INSERT_ID();
    INSERT INTO handoff_attempts
      (handoff_key, sessao_id, agente_id, generation, status,
       chatwoot_inbox_id, chatwoot_contact_id,
       chatwoot_conversation_id, ready_at)
    VALUES
      (UUID(), @session_id, @agent_id, 1, 'ready',
       ${CHATWOOT_INBOX_ID}, ${CHATWOOT_CONTACT_ID},
       ${CHATWOOT_CONVERSATION_ID}, NOW(6));
  "

compose up -d --force-recreate app
APP_READY=false
for _ in $(seq 1 30); do
  if compose exec -T app python -c '
import httpx
try:
    response = httpx.get("http://127.0.0.1:5000/", timeout=5.0)
except httpx.HTTPError:
    raise SystemExit(1)
raise SystemExit(0 if response.status_code == 200 else 1)
  '; then
    APP_READY=true
    break
  fi
  sleep 1
done
test "$APP_READY" = "true"
compose up -d chatwoot-sidekiq
compose run --rm provider-auth-bootstrap python -c "
import asyncio
from src.services.chatwoot_client import add_message_to_conversation
asyncio.run(add_message_to_conversation(
    ${CHATWOOT_CONVERSATION_ID},
    '${SYNTHETIC_CONTENT}',
))
"

CALLBACK_ACCEPTED=false
for _ in $(seq 1 30); do
  CALLBACK_COUNT=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
    mysql -N -u root wpp_bot -e "
      SELECT COUNT(*)
      FROM inbound_events AS event
      JOIN outbox_deliveries AS delivery
        ON delivery.inbound_event_id = event.id
      WHERE event.provider = 'chatwoot'
        AND event.source_scope = '${CHATWOOT_ACCOUNT_ID}'
        AND event.processed_at IS NOT NULL
        AND delivery.provider = 'evolution'
        AND delivery.operation = 'evolution.send_text.v1'
        AND delivery.status = 'pending'
        AND delivery.stream_sequence = 1
        AND JSON_UNQUOTE(JSON_EXTRACT(delivery.payload, '$.phone')) = '${SYNTHETIC_PHONE}'
        AND JSON_UNQUOTE(JSON_EXTRACT(delivery.payload, '$.message')) = '${SYNTHETIC_CONTENT}'
        AND JSON_UNQUOTE(JSON_EXTRACT(delivery.payload, '$.instance_name')) = 'provider-auth-validation';
    ")
  if [ "$CALLBACK_COUNT" = "1" ]; then
    CALLBACK_ACCEPTED=true
    break
  fi
  sleep 1
done
if [ "$CALLBACK_ACCEPTED" != "true" ]; then
  CALLBACK_RECEIPTS=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
    mysql -N -u root wpp_bot -e "
      SELECT COUNT(*) FROM inbound_events WHERE provider = 'chatwoot';
    ")
  CALLBACK_EFFECTS=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
    mysql -N -u root wpp_bot -e "
      SELECT COUNT(*) FROM outbox_deliveries WHERE provider = 'evolution';
    ")
  echo "provider-auth validation: signed callback not accepted | receipts=${CALLBACK_RECEIPTS} effects=${CALLBACK_EFFECTS}" >&2
  exit 1
fi

# No delivery worker exists in this rehearsal. The provider callback may only
# produce one pending durable effect; it must not perform direct delivery.
NON_PENDING_COUNT=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -N -u root wpp_bot -e "
    SELECT COUNT(*)
    FROM outbox_deliveries
    WHERE provider = 'evolution' AND status <> 'pending';
  ")
test "$NON_PENDING_COUNT" = "0"

# Stage a second, unmapped handoff and run the real dispatcher. Chatwoot is the
# pinned disposable provider; Evolution is deliberately replaced only inside
# the worker by a synthetic 503 transport, so no WhatsApp instance can send.
DISPATCH_PHONE=5511987654333
DISPATCH_SUMMARY=wpp-private-dispatch-summary
compose run --rm provider-auth-bootstrap python -c "
import asyncio
from datetime import datetime
from uuid import uuid4
from sqlalchemy import select
from src.config import settings
from src.database.connection import AsyncSessionLocal
from src.database.models import Agente, InboundEvent
from src.database.repositories import sessao_repo
from src.services.handoff_manager import planejar_handoff

async def main():
    async with AsyncSessionLocal() as db:
        async with db.begin():
            agente = (await db.execute(select(Agente).where(
                Agente.instance_name == 'provider-auth-validation'
            ))).scalar_one()
            sessao = await sessao_repo.get_or_create_for_update(
                db, agente.id, '${DISPATCH_PHONE}'
            )
            inbound = InboundEvent(
                provider='evolution',
                source_scope=agente.instance_name,
                provider_event_id=str(uuid4()),
                event_type='messages.upsert',
                payload_hmac='b' * 64,
                hmac_key_version=1,
                agente_id=agente.id,
                sessao_id=sessao.id,
                processed_at=datetime.utcnow(),
            )
            db.add(inbound)
            await db.flush()
            await planejar_handoff(
                agente=agente,
                sessao=sessao,
                telefone_lead='${DISPATCH_PHONE}',
                resumo='${DISPATCH_SUMMARY}',
                inbound_event=inbound,
                cutover_epoch=settings.delivery_cutover_epoch,
                db=db,
                now=datetime.utcnow(),
                setup_ttl_seconds=3600,
            )

asyncio.run(main())
"

OPEN_DELIVERY_ID=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -N -u root wpp_bot -e "
    SELECT id
    FROM outbox_deliveries
    WHERE provider = 'chatwoot'
      AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.phone')) = '${DISPATCH_PHONE}';
  ")
OPEN_MARKER=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -N -u root wpp_bot -e "
    SELECT delivery_key
    FROM outbox_deliveries
    WHERE id = ${OPEN_DELIVERY_ID};
  ")

# Fail exactly the first local terminalization after real Chatwoot acceptance.
# Claim fencing and the remote provider call remain real; only this disposable
# MySQL trigger injects the local commit fault.
compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -u root wpp_bot <<SQL
DELIMITER $$
CREATE TRIGGER validation_fail_chatwoot_finalize
BEFORE UPDATE ON outbox_deliveries
FOR EACH ROW
BEGIN
  IF OLD.id = ${OPEN_DELIVERY_ID} AND NEW.status = 'delivered' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'synthetic local finalize failure';
  END IF;
END$$
DELIMITER ;
SQL

sed -i 's/^DELIVERY_DISPATCH_ENABLED=false$/DELIVERY_DISPATCH_ENABLED=true/' "$VALIDATION_ENV"
compose up -d synthetic-evolution delivery-worker nginx

# Wait for one actual remote conversation while the trigger proves the local
# row is still processing. Then restart the worker after canonical lease expiry;
# exact marker reconciliation must reuse that conversation.
FINALIZE_FAULT_OBSERVED=false
for _ in $(seq 1 60); do
  LOCAL_STATE=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
    mysql -N -u root wpp_bot -e "
      SELECT CONCAT(status, ':', attempt_count)
      FROM outbox_deliveries WHERE id = ${OPEN_DELIVERY_ID};
    ")
  REMOTE_COUNT=$(compose exec -T -e PGPASSWORD="$CHATWOOT_DB_PASSWORD" \
    chatwoot-postgres psql -U chatwoot -d chatwoot -tAc \
    "SELECT COUNT(*) FROM conversations WHERE custom_attributes @> '{\"wpp_delivery_key\":\"${OPEN_MARKER}\"}'::jsonb;")
  if [ "$LOCAL_STATE" = "processing:1" ] && [ "$REMOTE_COUNT" = "1" ]; then
    FINALIZE_FAULT_OBSERVED=true
    break
  fi
  sleep 1
done
if [ "$FINALIZE_FAULT_OBSERVED" != "true" ]; then
  echo "provider-auth validation: accepted/commit-failure fault was not observed | local=${LOCAL_STATE} remote=${REMOTE_COUNT}" >&2
  exit 1
fi

# Preserve the removed worker incarnation privately before Compose recreation.
# Assertions expose only labels, never captured output.
ORIGINAL_WORKER_LOG="$VALIDATION_DIR/delivery-worker-original.log"
ORIGINAL_WORKER_CAPTURED=false
for _ in $(seq 1 20); do
  compose logs --no-color delivery-worker >"$ORIGINAL_WORKER_LOG" 2>&1
  if grep -q 'Delivery worker started' "$ORIGINAL_WORKER_LOG" && \
     grep -q 'synthetic local finalize failure' "$ORIGINAL_WORKER_LOG"; then
    ORIGINAL_WORKER_CAPTURED=true
    break
  fi
  sleep 1
done
if [ "$ORIGINAL_WORKER_CAPTURED" != "true" ]; then
  echo "provider-auth validation: original worker create/finalize-failure interval was not captured" >&2
  exit 1
fi

compose stop -t 10 delivery-worker
compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -u root wpp_bot -e "
    DROP TRIGGER validation_fail_chatwoot_finalize;
    UPDATE outbox_deliveries
    SET lock_until = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND
    WHERE id = ${OPEN_DELIVERY_ID} AND status = 'processing';
  "
compose up -d --force-recreate delivery-worker

DISPATCH_COMPLETED=false
for _ in $(seq 1 60); do
  DISPATCH_STATE=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
    mysql -N -u root wpp_bot -e "
      SELECT CONCAT(parent.status, ':', child.status, ':', child.attempt_count)
      FROM outbox_deliveries AS parent
      JOIN outbox_deliveries AS child
        ON child.depends_on_delivery_id = parent.id
      WHERE parent.provider = 'chatwoot'
        AND parent.operation = 'chatwoot.open_handoff.v1'
        AND JSON_UNQUOTE(JSON_EXTRACT(parent.payload, '$.phone')) = '${DISPATCH_PHONE}';
    ")
  case "$DISPATCH_STATE" in
    delivered:retry:*|delivered:processing:*|delivered:dead:*)
      DISPATCH_COMPLETED=true
      break
      ;;
  esac
  sleep 1
done
if [ "$DISPATCH_COMPLETED" != "true" ]; then
  echo "provider-auth validation: real worker did not complete pinned Chatwoot dispatch | state=${DISPATCH_STATE}" >&2
  exit 1
fi

# Exact marker reconciliation must leave one remote conversation. Synthetic
# Evolution effects must have attempts but never reach delivered.
compose exec -T -e OPEN_MARKER="$OPEN_MARKER" chatwoot bundle exec rails runner '
  count = Conversation.where(
    "custom_attributes @> ?", { "wpp_delivery_key" => ENV.fetch("OPEN_MARKER") }.to_json
  ).count
  raise "exact marker conversation count=#{count}" unless count == 1
'
EVOLUTION_DELIVERED=$(compose exec -T -e MYSQL_PWD="$MYSQL_ROOT_PASSWORD" db \
  mysql -N -u root wpp_bot -e "
    SELECT COUNT(*) FROM outbox_deliveries
    WHERE provider = 'evolution' AND status = 'delivered';
  ")
test "$EVOLUTION_DELIVERED" = "0"

# Exercise Nginx runtime redaction with sensitive path/query values. Upstream
# status is irrelevant; only emitted logs are inspected.
compose exec -T app python -c "
import httpx
httpx.get(
    'http://nginx/sessoes/${SYNTHETIC_PHONE}?telefone=${SYNTHETIC_PHONE}',
    headers={'Host': 'localhost'},
    timeout=5.0,
)
"

compose exec -T chatwoot bundle exec rails runner - \
  <"$PROJECT_DIR/chatwoot/test/phone_log_redaction_check.rb"

cp "$ORIGINAL_WORKER_LOG" "$VALIDATION_DIR/stack.log"
compose logs --no-color app delivery-worker nginx chatwoot chatwoot-sidekiq \
  evolution synthetic-evolution \
  >>"$VALIDATION_DIR/stack.log" 2>&1
python - "$VALIDATION_ENV" "$VALIDATION_DIR/stack.log" \
  "$SYNTHETIC_PHONE" "$DISPATCH_PHONE" "$SYNTHETIC_PREFIX" \
  "$SYNTHETIC_CONTENT" "$DISPATCH_SUMMARY" <<'PY'
from pathlib import Path
import sys

env_path, log_path, phone, dispatch_phone, prefix, content, summary = sys.argv[1:]
log = Path(log_path).read_text(errors="replace")
secret_keys = (
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "API_KEY",
    "BEARER",
    "HMAC_KEYS",
)
needles = [
    ("phone", phone),
    ("dispatch_phone", dispatch_phone),
    ("prefix", prefix),
    ("content", content),
    ("summary", summary),
]
for line in Path(env_path).read_text().splitlines():
    key, separator, value = line.partition("=")
    if separator and value and any(marker in key for marker in secret_keys):
        needles.append(("secret", value))
leaked = sorted({label for label, value in needles if value and value in log})
if leaked:
    raise SystemExit("cross-container log privacy failed: " + ",".join(leaked))
print("cross-container log privacy checks passed")
PY

# Assert that Docker actually resolved the two provider services to the approved
# immutable image IDs, rather than merely parsing the Compose file.
EVOLUTION_IMAGE=$(compose images -q evolution)
CHATWOOT_IMAGE=$(compose images -q chatwoot)
CHATWOOT_SIDEKIQ_IMAGE=$(compose images -q chatwoot-sidekiq)
test "$(docker image inspect "$EVOLUTION_IMAGE" --format '{{index .RepoDigests 0}}')" = \
  "evoapicloud/evolution-api@sha256:966625532d9076a2381e973a271307d107e6f070450de3abeeea8bd18be07252"
test "$(docker image inspect "$CHATWOOT_IMAGE" --format '{{index .RepoDigests 0}}')" = \
  "chatwoot/chatwoot@sha256:18f280a63070073c7588164eed31303cc788123d899733e54aeec4c39d1d8dd9"
test "$(docker image inspect "$CHATWOOT_SIDEKIQ_IMAGE" --format '{{index .RepoDigests 0}}')" = \
  "chatwoot/chatwoot@sha256:18f280a63070073c7588164eed31303cc788123d899733e54aeec4c39d1d8dd9"

echo "provider-auth validation: pinned providers, signed callback, real worker dispatch, synthetic Evolution outage, and privacy checks passed"
