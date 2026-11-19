# Inbox scheduler: ativação, drain e rollback

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
docker compose exec app python -c 'import httpx; r=httpx.get("http://127.0.0.1:5000/agentes"); r.raise_for_status(); print(r.status_code)'
```

Do not publish port 5000. `delivery-ops` remains a container-only CLI.

## Limite de execução

Somente ambiente isolado, credenciais sintéticas, `DELIVERY_MODE=outbox` e autenticação `enforced`.
Não executar ativação em produção por este runbook sem aprovação operacional separada.
Não usar LLM, Evolution, Chatwoot ou WhatsApp reais no ensaio.

Variáveis usadas nos exemplos:

```bash
export SCHEDULER_EPOCH="$(python -c 'import uuid; print(uuid.uuid4())')"
export NEXT_SCHEDULER_EPOCH="$(python -c 'import uuid; print(uuid.uuid4())')"
export DELIVERY_EPOCH="<delivery-runtime-epoch-atual>"
export VERSION="<scheduler-version-atual>"
export AGENT_ID="<agente-canario>"
export ATTEMPT_KEY="<generation-attempt-uuid>"
export CONSUMO_ID="<consumo-llm-id>"
export ACTOR="operador-identificado"
export REASON="mudanca-aprovada"
```

## 1. Pré-condições e ativação em dois estágios

1. Implantar código compatível e ainda desabilitado em todos os processos `app`, `delivery-worker` e `turn-worker`.
2. Manter `INBOX_SCHEDULER_ENABLED=false` e `INBOX_TURN_PROCESSOR_ENABLED=false`.
3. Remover réplicas antigas de ingress e dispatch. Esperar heartbeat antigo expirar ou `stopped_at` limpo. Heartbeat ausente não prova remoção.
4. Confirmar runtime `outbox`, epoch de delivery, auth `enforced`, dispatch e schema/code version iguais em toda frota.
5. Consultar estado e health:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli scheduler-state show

docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli health
```

Com scheduler `disabled`, incompatibilidade de réplica aparece no diagnóstico,
mas não derruba o health legado. Em `enabling`, `enabled` ou `draining`, qualquer
réplica incompatível torna readiness/health indisponível. O gate de enablement
recusa frota incompatível em qualquer caso; health saudável em `disabled` não é
autorização para ativar.

Regra de health por estado do scheduler (T08-3b + T08-5). O estado vem sempre de
`scheduler_runtime_state.state` lido do DB, nunca de `INBOX_SCHEDULER_ENABLED`.
FU-4: `/health` público mostra somente `status` (`healthy`/`unhealthy`) e HTTP
200/503. Diagnóstico completo: `/internal/health` direto em `app:5000` pela
rede Docker (Nginx nega), ou CLIs internas abaixo. `/health/provider-auth`
também fica restrito ao acesso direto. Não há monitor externo documentado que
parseie esses detalhes.
FU-4: Nginx também nega `/docs*`, `/redoc*` e `/openapi.json*`; consultar
docs/schema via `app:5000` na rede Docker. `GET /` retorna só
`{"status":"online"}` (sem service/version).
A decisão de readiness vale igual para `/health`, `/internal/health`,
`delivery_cli health` e `python -m src.turn_worker health`:

| Estado no DB | Réplica incompatível / frota ilegível (readiness compartilhada; campos abaixo só no detalhe interno/CLI) | Métricas agregadas ilegíveis (`/internal/health`, `delivery_cli health`) |
|--------------|----------------------------------------|------------------------------|
| `disabled` | health inalterado (legado default-off); payload `replica_compatibility.compatible=false`, `blocks_health=false`, `reasons`, contador + `ERROR` limiar 1 | health inalterado |
| `enabling`, `enabled`, `draining` | health falha fechado (`/health` 503, CLI unhealthy), `blocks_health=true` | health falha fechado, `metrics_unavailable` |
| estado ilegível | nenhum segundo motivo; a checagem de DB existente já reprova | nenhum segundo motivo |

Sem relaxamento fora do health: gate `enabling→enabled` exige frota compatível
em qualquer estado, e o turn worker recusa start com schema do DB ≠ binário.
Antes de `enabling`, ler `replica_compatibility.reasons` na CLI interna ou em `/internal/health` mesmo com health verde.

6. Estágio 1 → registrar epoch durável em `enabling`:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli scheduler-state enabling \
  --epoch "$SCHEDULER_EPOCH" --expected-version "$VERSION" \
  --actor "$ACTOR" --reason "$REASON"
```

7. Reiniciar todas as réplicas com `INBOX_SCHEDULER_ENABLED=true`, epoch correspondente, processor/dispatch conforme papel. Readiness deve permanecer fail-closed enquanto houver divergência.
8. Revalidar frota. Atestar remoção das réplicas antigas somente após verificação externa. Estágio 2:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli scheduler-state enabled \
  --epoch "$SCHEDULER_EPOCH" --expected-version "$VERSION" \
  --actor "$ACTOR" --reason "$REASON" \
  --attest-old-replicas-removed
```

Sem `--attest-old-replicas-removed` → gate recusa. Atestação não substitui inventário de processos.

## 2. Canary de um número e opt-in/opt-out

Selecionar agente dedicado a um número sintético. Exigir agente ativo, inbox Chatwoot, runtime/epochs correspondentes e auth `enforced`.

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli agent-scheduler show "$AGENT_ID"

docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli agent-scheduler opt-in "$AGENT_ID" \
  --scheduler-epoch "$SCHEDULER_EPOCH" --delivery-epoch "$DELIVERY_EPOCH" \
  --actor "$ACTOR" --reason "$REASON"
```

Validar somente no canary: ingress assinado, dedupe, geração/typing, outbox, accounting, handoff e métricas agregadas. Não ampliar opt-in com readiness degradada.

Opt-out é permitido depois que trabalho do agente assentar. Trabalho pendente → recusa auditada:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli agent-scheduler opt-out "$AGENT_ID" \
  --actor "$ACTOR" --reason "$REASON"
```

## 3. Drain e desativação

1. Entrar em `draining`. Novo intake agendado recebe HTTP 503, `code=scheduler_draining`, `Retry-After: 5`. Replay exato já aceito continua `already_accepted`.

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli scheduler-state draining \
  --epoch "$SCHEDULER_EPOCH" --expected-version "$VERSION" \
  --actor "$ACTOR" --reason "$REASON"
```

2. Manter turn processor e delivery worker operando. `draining` assenta trabalho aceito; não desligar consumers antes da clearance.
3. Inspecionar health até zero: lanes owned, turns não terminais, attempts não resolvidos/não reconciliados, scheduled handoffs, deliveries automated e `legacy_unknown`.
4. Resolver explicitamente quarentenas e efeitos ambíguos. Ausência de worker ou timeout não é resolução.
5. Depois da clearance, ainda em `draining`, listar todos os agentes opted-in e executar opt-out auditado para cada `agente_id`. Repetir a listagem com `--after-id` igual a `next_after_id` enquanto `has_more=true`:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli agent-scheduler list --after-id 0 --limit 100

docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli agent-scheduler opt-out "$AGENT_ID" \
  --actor "$ACTOR" --reason "$REASON"
```

Opt-out recusado por `scheduled_work_pending` invalida a clearance operacional → manter consumers, resolver trabalho e repetir. Não prosseguir enquanto a listagem retornar agente.

6. Confirmar lista vazia. Agente ainda opted-in depois de `disabled` recebe HTTP 503 `scheduler_not_enabled` em todo inbound novo; deixar o opt-in ativo causa indisponibilidade permanente do tenant e pode exceder a janela de retry da Evolution.

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli agent-scheduler list --after-id 0 --limit 100
```

7. Só então desabilitar:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli scheduler-state disabled \
  --epoch "$SCHEDULER_EPOCH" --expected-version "$VERSION" \
  --actor "$ACTOR" --reason "$REASON"
```

Recusa `SchedulerDrainBlocked` → usar contagens de `clearance`; não contornar via SQL.

## 4. Quarentena de geração

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli quarantine-list --agente-id "$AGENT_ID" --limit 100

docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli quarantine-inspect "$ATTEMPT_KEY"
```

Provider confirmou sucesso e existe ledger exato:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli quarantine-resolve "$ATTEMPT_KEY" \
  --outcome succeeded --evidence provider_generation_lookup \
  --consumo-id "$CONSUMO_ID" --actor "$ACTOR" --reason "$REASON"
```

Provider confirmou falha/ausência sem ledger. Risco de gasto duplicado deve ser aceito explicitamente:

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli quarantine-resolve "$ATTEMPT_KEY" \
  --outcome failed --evidence provider_dashboard \
  --accept-duplicate-spend-risk --actor "$ACTOR" --reason "$REASON"
```

Resolução é idempotente só para a mesma tupla de evidência. Ela registra decisão; recovery posterior libera lane/turn sob fences.

## 5. Rotação de epoch

Rotação exige ciclo completo `enabled → draining → disabled`, clearance zero e nova UUID. Nunca migrar delivery `automated` de epoch.

```bash
docker compose --profile ops run --rm delivery-ops \
  python -m src.delivery_cli scheduler-state enabling \
  --epoch "$NEXT_SCHEDULER_EPOCH" --expected-version "$VERSION" \
  --actor "$ACTOR" --reason "$REASON"
```

Reconfigurar toda frota, revalidar readiness, executar `scheduler-state enabled` com atestação e refazer opt-in guardado conforme plano de rollout.

## 6. Retenção

- ≥7 dias → scrub de `draft_text` somente para turn terminal e sem dependência bloqueante.
- ≥90 dias → purge FK-safe do grafo terminal; mensagens, ledger LLM e reconciliação comercial permanecem.
- Heartbeats e audit → retenção de 90 dias, discovery limitada.
- Attempts `started`, `outcome_unknown`, `reconciliation_required` não resolvidos, efeitos não terminais e notas ambíguas impedem purge.
- `scheduler_retention` no health do worker deve ficar saudável. Sinal repetido exige investigação; nunca usar `FOREIGN_KEY_CHECKS=0` operacionalmente.

## 7. Rollback

Rollback normal → release anterior compatível com outbox, mesmo delivery epoch; scheduler passa por drain e fica `disabled`.

Retorno a delivery direta → seguir também [`durable-delivery-runbook.md`](durable-delivery-runbook.md): congelar ingress, zerar leases/efeitos, `freeze`, resolver ambiguidades, `rollback-check`, `restore-direct`, reconfigurar toda frota, só então reabrir ingress. Nunca alternar `DELIVERY_MODE` ou epochs por SQL manual.

## 8. Falhas e riscos conhecidos

- Mixed version/code/schema/epoch/flags → diagnóstico em `disabled`; readiness 503 em `enabling`, `enabled` e `draining`; gate `enabling→enabled` sempre recusado.
- `outcome_unknown` → bloqueia nova geração do agente e clearance até resolução auditada.
- `legacy_unknown` não terminal → bloqueia enablement/drain; reconciliar individualmente.
<!-- FU-3: manutenção não altera a autoridade do CHECK. -->
- `ck_scheduled_handoff_summary_source` → allowlist fechada de duas formas byte-exatas: `_latin1` original e `_ascii` re-serializada pelo MySQL 8.0. Nome, ENFORCED e conjunto de CHECKs permanecem exatos; nenhum texto é normalizado.
- `ALTER TABLE` sem mudança de shape (ex.: COMMENT), `OPTIMIZE TABLE`, `mysqlcheck -o` e `ALTER TABLE … FORCE` → não são mais proibidos pelo risco de reserialização desse CHECK. Auditoria automatizada cobre OPTIMIZE e ALTER COMMENT em todas as tabelas, nos clientes latin1/utf8mb4/utf8mb3, seguida de `db-migrate`; triggers mantêm digest byte-exato.
- Ressalva → manutenção exige janela/aprovação operacional e backup; MDL/rebuild podem bloquear tráfego. Mudança real de shape continua proibida sem migration auditada. Ferramentas `pt-online-schema-change`/`pt-osc` e `gh-ost`, upgrade MySQL e outras versões do servidor não foram validados por esta matriz; exigem rehearsal próprio preservando FKs/triggers/CHECKs.
- Conjunto auditado de handoff: digest desconhecido, CHECK ausente/alterado/NOT ENFORCED ou CHECK extra → `db-migrate` continua falhando fechado com `ERROR 1644`; app/workers não sobem. Reparo somente por migration auditada, sem edição ad hoc em produção.
- Heartbeat ausente não prova que binário antigo saiu. Atestação depende de inventário externo.
- Nenhum alerta de throughput é inventado; definir limiares após carga representativa.


### FU-1 — sinais de retenção

- `retention_task`: running/idle/failed, started_at, last_completed_at,
  running_seconds e contadores no snapshot local do delivery worker.
- Passada >budget + `DELIVERY_RETENTION_OVERRUN_MARGIN_SECONDS=30`, ou sem
  conclusão >`DELIVERY_RETENTION_COMPLETION_MAX_INTERVALS=2` intervalos → ERROR
  limiar 1 por episódio. Snapshot SLA fica stale/available=false; dispatch segue.
- Draft é medido somente nos IDs das páginas já percorridas (até1000/probe).
  Durante cobertura parcial → draft_available=false, reason=coverage_incomplete,
  scan_started_at, scan_start_cursor, scan_cursor, scan_version e scan_rows.
  Isso é informativo, não falha/readiness. Draft acima do SLA visto numa página
  sinaliza imediatamente; não aguarda sweep completo.
- Mínimo O(1) local não sobrevive a restart. Cursor+version quebrados reiniciam
  cobertura. `last_complete_scan_at`/age permitem acompanhar convergência.
  Sem sweep completo >`SCHEDULER_RETENTION_SWEEP_MAX_AGE_SECONDS=172800` → ERROR
  por episódio. Limite+tolerância <7d; aumentar margem/N/idade não substitui
  dimensionamento da vazão. Restart contínuo antes do limite reinicia a graça
  local; monitorar também frequência de restart.

### Dimensionamento da retenção — S1

- Intervalo `I = DELIVERY_RETENTION_INTERVAL_SECONDS` (default 3600s).
- Orçamento por passada: `R = SCHEDULER_RETENTION_MAX_ROWS` (10000),
  `T = SCHEDULER_RETENTION_MAX_SECONDS` (30s), páginas de 100 linhas.
- `D` = turns/dia; estimativa conservadora `N = 90 × D + B`, onde `B` é
  backlog retido por accounting/handoff. Contar também não terminais antigos.
- Medir `Q` = linhas efetivamente examinadas/segundo incluindo discovery,
  transações e checkpoint. Capacidade efetiva `E = min(R, floor(Q × T))`;
  não confundir limite configurado com vazão garantida. Réplicas podem duplicar
  trabalho idempotente: dimensionar para uma réplica, sem multiplicar capacidade.
- Ciclo máximo estimado `C = ceil(N / E) × I`; para tolerância SLA de 1d,
  exigir `C ≤ 86400s`, com margem para retries, downtime e crescimento.
  Capacidade de purge também exige `E × 86400 / I > D`.
- Exemplo: `D=500`, `B=0` → `N≤45000`; com `E=10000`, `I=3600`,
  ciclo ≤5h. Exige throughput medido ≥334 linhas/s para caber nos 30s.
  Se `E=2000` → ciclo ≤23h, praticamente sem margem: reduzir intervalo
  ou aumentar orçamento após medir contenção/latência. Nunca aumentar sem medir.
  R2-S1: retenção roda em task própria single-flight; não pausa claim/heartbeat.
  Budget 0.1..300s não compete com frescor de 60s. Config exige heartbeat +
  poll <60s; não aumentar heartbeat para acomodar retenção. Shutdown cancela
  passada e usa drain/abort 30s/5s (stop grace 40s).
- Cursor durável/CAS mantém progresso mesmo com restart após cada passada.
  Crash antes do checkpoint repete até uma página. Tempo de trabalho limitado
  por `T`, mais até 3s de checkpoint e 3s de health; efeitos legados têm seu
  próprio custo. Processo encerrado antes de qualquer checkpoint não progride.
- Worker health `scheduler_retention_health`: idade do terminal mais antigo
  com draft e mais antigo elegível por idade ao purge (blockers incluídos).
  SLA = 7d/90d + `SCHEDULER_RETENTION_SLA_TOLERANCE_SECONDS` (86400s).
  `*_sla_exceeded`, `health_unavailable`,
  `checkpoint_failed`, `checkpoint_unavailable`, `discovery_failed` → contador +
  ERROR limiar 1. `available=false` não prova ausência de backlog.
  `budget_exhausted` → contador informativo de cobertura parcial (FU-1).
  `checkpoint_contended`/`discovery_changed` → somente contador, concorrência
  esperada; perdedora não publica SLA stale. Purge usa índice
  `idx_turn_retention_terminal`, ordenação + `LIMIT 1`; draft usa probe
  limitado aos IDs das páginas (FU-1), sem varrer histórico scrubbed.
- Blocker sem resolução pode tornar SLA impossível; investigar accounting/
  handoff. Nunca apagar evidência comercial para cumprir um indicador.


## F0-T5 — shutdown de processo e heartbeat

| Processo | Grace Compose | Drain | Writers/accounting | Marcador | Dispose | Saída forçada |
| --- | --- | --- | --- | --- | --- | --- |
| app | 10s | HTTP até 8s | caches/beat concorrentes até t=9s | até t=9.25s | até t=9.5s | t=9.5s, código 75 |
| turn-worker | 40s | jobs até 30s (setting) | abort 5s + excedente 4s até t=39s | até t=39.25s | até t=39.5s | t=39.5s, código 75 |

- Tempos absolutos a partir do primeiro SIGTERM/SIGINT; repetição não renova.
- Produção app → `python -m src.app_server`; `app/Dockerfile` usa esse entrypoint.
  Uvicorn recebe `timeout_graceful_shutdown=8`; `python -m src.main` também
  delega ao entrypoint. APP_HOST/APP_PORT são respeitados (defaults Docker
  0.0.0.0:5000). DEBUG=true é recusado com
  `app_server_debug_reload_not_supported` (exit 2), pois reload cria supervisor/
  filho fora do watchdog single-process. CLI externa `uvicorn src.main:app` não oferece watchdog.
- Handlers Python síncronos armam timer daemon antes de enfileirar o stop no loop,
  inclusive bootstrap do worker. Handler somente no event loop não basta se
  request síncrona ocupa o loop. Timer continua armado durante teardown de
  `asyncio.run`, inclusive seu gather ilimitado.
- Timer emite ERROR técnico estático via `os.write` não bloqueante e chama
  `os._exit(75)`. Não espera locks/sinks do Loguru nem flush arbitrário bloqueante.
  Margem de 0.5s para SIGKILL é explícita; perder a corrida equivale ao mesmo crash.
- Discovery/bootstrap cancelados no sinal; jobs ativos mantêm drain. Accounting
  blindado continua usando implementação existente e recebe todo excedente do
  grace, não apenas metade do abort. Sem nova regra de accounting/resolução.
- Caches e beat são cancelados concorrentemente; marcador só depois de fechar
  produtores e aguardar possíveis writers (qualquer task fora dos controllers
  conhecidos, inclusive shields). Controllers só fazem dispose/encerramento depois
  do marcador. Pendência → nenhum stopped_at; ERROR/counters `unsafe_stops`,
  `heartbeat_failures`/`cleanup_failures`; processo sai como crash. Readiness
  depende da expiração normal de 60s.
- Marcador falhou por DB indisponível, mas todos os writers terminaram e ainda
  há prazo → `marker_failed` + ERROR limiar 1, saída 0, sem unsafe_stops ou
  forced_exits. Gate final revalida tasks após cleanup da tentativa. unsafe/75
  são reservados a writer pendente ou prazo esgotado, não à mera ausência do
  marcador best-effort. Expiração de 60s continua igual.
- Attempts abandonados ficam started/drain → recuperação/quarentena/
  outcome_unknown existentes. Nada é resolvido artificialmente no shutdown.
- Direct pode perder ledger se request é cancelada depois de LLM pago e antes do
  commit: UoW faz rollback, CancelledError não gera reconciliação. Sinal ASGI
  `direct_shutdown_cancellations` + ERROR limiar 1 registra risco sem inventar
  custo. Drain HTTP de 8s maximiza janela útil mantendo 1s para quiescência,
  .25s marcador, .25s dispose e .5s margem; não garante accounting em crash.
- `HeartbeatLoop.stop()` mantém budget total standalone 5s; cancelamento de um
  caller não cancela shutdown compartilhado dos outros. Beat cancelado invalida
  conexão antes do marcador.
- Prova → `scripts/run_heartbeat_shutdown_matrix.sh`: processos reais via main,
  SIGTERM/os.kill, Uvicorn real, MDL e TCP proxy congelado, mutations contadas.
- Limite: C nativo que retém GIL indefinidamente não é preemptível por thread
  Python. SIGKILL continua limite externo. Timeouts globais de socket/engine
  permanecem fora do escopo; schema e flags/defaults não mudam.

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

### Remoção de superfície legada — 2026-11-19

CRUD de cadastro v3b, armazenamento JSON, notificação WhatsApp v3b e schemas MVP sem consumidores foram removidos. Handoff permanece no Chatwoot. Schema/ORM histórico e compatibilidade operacional permanecem; nenhuma migration destrutiva foi adicionada. Inventário, limites e procedimento de backup → [relatório de remoção](../LEGACY_REMOVAL_REPORT.md).
