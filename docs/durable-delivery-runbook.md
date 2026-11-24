# Durable delivery cutover and rollback

## Shared image build gate

Set COMPOSE_PROJECT_NAME explicitly; templates use `wpp-ai-bot-CHANGE-ME`, not D2.
Default Nginx bind is 127.0.0.1. Set DELIVERY_WORKER_CODE_VERSION to the release
HEAD before building. All six Python services consume that one immutable build
identity (file outside /app + OCI revision label); missing/divergent identity
refuses startup, and replica fingerprints report the baked version.
Before the following up/run flow, explicitly build and run preflight:

```bash
scripts/release_build.sh --project "$COMPOSE_PROJECT_NAME" --expected-commit "$(git rev-parse HEAD)"
scripts/release_preflight.sh .env --expected-project "$COMPOSE_PROJECT_NAME" --phase legacy
```

Shared image → `${COMPOSE_PROJECT_NAME}-app:${DELIVERY_WORKER_CODE_VERSION}`.
Only app builds; all six Python services use that project-scoped tag.
Profiles are enabled only through explicit `--profile` CLI flags, never COMPOSE_PROFILES.
Release rejects COMPOSE_FILE/COMPOSE_ENV_FILES/COMPOSE_PATH_SEPARATOR and all
unknown COMPOSE_* controls. Only COMPOSE_PROJECT_NAME plus presentation/performance
knobs COMPOSE_ANSI, COMPOSE_STATUS_STDOUT, COMPOSE_PROGRESS, COMPOSE_MENU,
COMPOSE_HTTP_TIMEOUT, COMPOSE_PARALLEL_LIMIT are allowed (see AGENTS §9/§17).
Automatic override files are forbidden; preflight compares the default no-`-f`
deploy model to the validated base, without profiles or dev overrides.
Only interpolated variables can be overridden by the shell: env_file-only
DEBUG/LOG_LEVEL exports do not replace candidate values.
Test builders use HEAD-dirty for tracked/staged/untracked changes; release
builds require a clean tree and the HEAD version.
`docker compose -p NAME` bypasses `name:`: always run preflight with the expected
effective project, never use a divergent `-p`. Dev code binds emit
`dev_bind_code_unattested` (WARN + counter): baked identity does not attest host source.

Use `--phase outbox` after the approved cutover; use `up -d --no-build` / `run --pull never`
to fail rather than silently use another tag. Force-recreate never replaces build.
SMTP is optional for local dev: all four SMTP_* empty → smtp_disabled warning;
a partial group fails. VPS preflight adds `--require-smtp` (all four required).
Preflight rejects known placeholder secrets and checks effective Compose values,
including divergent shell overrides of project/bind/ports, without printing values.
TLS preflight requires both fullchain.pem and privkey.pem.

## Public ingress allowlist (F0-T4)

Only `GET /`, `GET /health`, `POST /webhook`, `POST /chatwoot/webhook`
reach the app through either Nginx config. HEAD is also allowed for `/` and `/health`. Other methods → 405; other paths
(including future routes, CRUD, diagnostics and docs) → 404 without upstream.
Normalized single encodings still follow the exact allowlist; double-encoded
digits → 400. Production HTTP redirects only allowed route/method pairs.
ACME location exists on port 80, but Compose does not mount its webroot;
ACME provisioning requires a separate operational webroot mount. The test harness
mounts a synthetic webroot only. Provider callbacks/probes keep using
`http://app:5000` internally, not the public edge.

Operator CRUD (`/agentes*`, `/sessoes*`) is Docker-network only:

```bash
# Bash: token de serviço leitura/operador; não usar set -x nem imprimir ambiente.
set +x
read -rs -p 'Token de serviço: ' WPP_SERVICE_TOKEN; printf '\n'
export WPP_SERVICE_TOKEN
docker compose exec -T -e WPP_SERVICE_TOKEN app python -c 'import os, httpx; r=httpx.get("http://127.0.0.1:5000/agentes", headers={"Authorization": "Bearer " + os.environ["WPP_SERVICE_TOKEN"]}); r.raise_for_status(); print(r.status_code)'
unset WPP_SERVICE_TOKEN
```

Do not publish port 5000. `delivery-ops` remains a container-only CLI.

## Execution boundary

Run drills only on disposable infrastructure with synthetic provider effects.
Do not change shared `.env`, provider callbacks, dispatch ownership, WhatsApp
delivery, or LLM settings without separate deployment approval.

## Readiness contract

FU-4: public `GET /health` returns only `{"status":"healthy"}` (200) or
`{"status":"unhealthy"}` (503). No scheduler, counters, epochs or reasons.
The previous full diagnostic body is available at `GET /internal/health`,
directly through `app:5000` on the Docker network. Nginx denies both this
path and `/health/provider-auth` externally, including in development.
Existing container-only CLI healthchecks and provider-auth probes are unchanged.
FU-4: Nginx also denies `/docs*`, `/redoc*` and `/openapi.json*` externally;
FastAPI docs/schema stay reachable through `app:5000` only. `GET /` returns
only `{"status":"online"}` (no service name or version).

Both surfaces use exactly the same cached collector. They are ready only when:

- app `DELIVERY_MODE` matches `delivery_runtime_state.mode`;
- outbox app epoch matches DB epoch;
- `DELIVERY_DISPATCH_ENABLED=true` in outbox mode;
- every fresh worker heartbeat reports dispatch enabled, matching outbox mode
  and epoch, provider ready, and runtime compatible;
- at least one worker heartbeat is newer than 60 seconds;
- oldest due delivery is no more than five minutes old;
- provider-auth snapshot is current and ready.

Health reads DB and local snapshots. It never calls Evolution or Chatwoot.
Operator diagnostics (no exposed host port):

```bash
docker compose exec app python -c 'import httpx; r = httpx.get("http://127.0.0.1:5000/internal/health"); print(r.status_code, r.json())'
```


Chatwoot Rails and Sidekiq must load
`chatwoot/config/initializers/wpp_phone_log_redaction.rb`. The initializer keeps
upstream filters, redacts phone/content/credential parameters, and disables
Active Job argument logging so signed webhook payloads and secrets do not enter
cross-container logs.

Internal callback `http://app:5000` requires
`CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK=true`. This widens Chatwoot
`SafeFetch` for all administrator-created webhooks; restrict Chatwoot
administrators to trusted operators. Use a public HTTPS callback and set it
false when that trust boundary is not acceptable.

## Isolated cutover drill

1. Start isolated stack in `direct`, auth `enforced`, dispatch disabled.
2. Verify provider-auth snapshot, zero due/nonterminal work, fresh idle worker.
3. Generate a new UUID and record DB ownership:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli cutover \
  --epoch "$NEW_EPOCH" --expected-version "$VERSION" \
  --actor "$ACTOR" --reason "$REASON"
```

4. Update only isolated environment to the same epoch, `DELIVERY_MODE=outbox`,
   keep dispatch disabled, restart app/worker, verify mixed ownership fails
   closed.
5. Set dispatch true in isolated environment, restart worker, then verify
   `/health`, synthetic enqueue, claim, delivery, restart, outage and recovery.

DB change and process restarts are deliberately separate. During mismatch,
ingress/readiness fail closed; no route falls back to direct delivery.

## Normal rollback

Deploy the previous outbox-capable release with the same epoch. Do not change
delivery ownership.

## Guarded return to direct ownership

1. Freeze authenticated ingress externally.
2. Set dispatch false on every worker and wait for fresh heartbeats with zero
   leases.
3. Freeze DB authority:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli freeze \
  --expected-version "$VERSION" --actor "$ACTOR" --reason "$REASON"
```

4. Inspect every queued, processing, ambiguous, dead and cancelled delivery.
   Reconcile/deliver/cancel through existing commands. Record explicit terminal
   resolution where required. `resolve-rollback` is accepted only while the
   runtime remains locked in `frozen`; its evidence is bound to the delivery's
   current epoch, status, attempt count, ambiguity count and terminal timestamp.
   Any retry or later terminal incarnation requires a new resolution:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli resolve-rollback "$DELIVERY_ID" \
  --resolution duplicate-risk-accepted \
  --actor "$ACTOR" --reason "$REASON"

docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli rollback-check
```

5. `rollback-check` must report zero nonterminal, processing,
   ambiguous-unresolved, terminal-unresolved and usage-unresolved rows.
6. Create another new UUID and restore DB ownership:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli restore-direct \
  --epoch "$DIRECT_EPOCH" --expected-version "$VERSION" \
  --actor "$ACTOR" --reason "$REASON"
```

7. Configure every replica for direct/dispatch-disabled, restart, verify
   readiness, then resume ingress.

Blind direct rollback is prohibited. Physical removal of direct compatibility
requires a later post-observation deployment approval.

## Retention and duplicate limitation

- terminal payload scrub: 7 days;
- receipts, delivery metadata, heartbeat and operator audit: 90 days;
- Evolution: bounded at-least-once; provider acceptance followed by lost
  acknowledgement can duplicate a WhatsApp message;
- Chatwoot open: exact marker reconciliation; multiple matches are terminal for
  operator resolution. The remote `wpp_delivery_key` is the open outbox row's
  `delivery_key`, not the handoff attempt key. Callback proof cannot clear a live
  worker lease; Chatwoot receives a retryable response until worker finalization
  or canonical expired-lease recovery.

### Equivalência do snapshot de preflight

Preflight executa config sem -f/--profile em snapshot com .env=candidato,
cwd=--project-directory e --env-file <snapshot>/.env. Compose real deve produzir
JSON idêntico sem --env-file (descoberta implícita usada pelo deploy).
COMPOSE_FILE/COMPOSE_ENV_FILES/COMPOSE_PROFILES e controles desconhecidos são
bloqueados; .env operacional nunca é copiado/lido. Mutação apontando --env-file
para outro candidato deve falhar no oracle de equivalência.

### R5 certified origin

Release builder exports raw committed Git blobs, not the live app directory.
Tracked/staged/nonignored-untracked edits fail; ignored local files never enter
that context. Docker alone applies committed .dockerignore. Unsupported links,
submodules and LFS fail closed. A private atomic local receipt binds full HEAD,
project, version, git-blobs-v1 context hash and actual immutable Image ID.
Preflight reads it, never adopts an image by label. Missing/stale receipt or
replaced tag requires rebuild; direct compose build is dev/test only.
Trust includes Git/builder/daemon, not malicious host/forged receipt or changes
after the gate. SIGTERM143/SIGINT130 unwind owned children/context; SIGKILL can
leave private0700/0600 data. External recovery only by exact job-owned path,
never scan/glob/prune. Uncommitted diff tests are not a clean release certificate.
