# Operação, configuração e validação

Base `main` = `dev` = `5908a94`, snapshot de 2026-11-09. F0 integrado via PR #14, main promovida por fast-forward, certificação p55 aceita. Inventário versionado, não de containers inspecionados; nenhum start real foi inferido.

## Serviços e imagens do Compose principal

Fonte: [docker-compose.yml](../../docker-compose.yml). **17 serviços declarados**; profiles/one-shot não equivalem a 17 containers permanentes.

| Serviço | Imagem/build nesta base | Profile/condição |
| --- | --- | --- |
| `app` | `<projeto>-app:<versão>`, único build `./app` | Padrão, `src.app_server`, grace 10 s |
| `delivery-worker` | Mesma imagem Python, sem build próprio | Padrão; idle sem dispatch/autoridade, grace 40 s |
| `turn-worker` | Mesma imagem Python | `inbox`; processor off por default, grace 40 s |
| `delivery-ops` | Mesma imagem Python | `ops`; CLI |
| `provider-auth-bootstrap` | Mesma imagem Python | `ops`; CLI mutável |
| `provider-auth-monitor` | Mesma imagem Python | Padrão; auditoria ativa fora do health HTTP |
| `db` | `mysql:8.0` + digest | Dados do bot |
| `db-migrate` | Mesmo digest MySQL | One-shot; DDL antes do app |
| `evolution` | `evoapicloud/evolution-api:v2.3.7` + digest | Gateway |
| `frontend` | `evoapicloud/evolution-manager:latest` + digest | Painel externo |
| `redis` | `redis:latest` + digest | Evolution, AOF e volume `/data` |
| `evolution-postgres` | `postgres:15` + digest | Banco Evolution |
| `chatwoot-redis` | `redis:7-alpine` + digest | Filas/cache Chatwoot; persistência gerenciada ainda incompleta |
| `chatwoot-postgres` | `pgvector/pgvector:pg15` + digest | Banco Chatwoot |
| `chatwoot` | `chatwoot/chatwoot:v4.17.1` + digest | Rails |
| `chatwoot-sidekiq` | Mesmo digest Chatwoot | Worker |
| `nginx` | `nginx:alpine` + digest | Portas host parametrizadas, bind default loopback |

Imagens externas do Compose principal usam tag+digest linux/amd64. `latest@sha256:…` continua pinado por conteúdo; não é tag flutuante. Digests completos são fonte do Compose, não cópia concorrente neste mapa. Stack provider-auth-validation é distinta; não inferir que todo Compose auxiliar compartilha todos os pins do release principal.

## Imagem Python e identidade

- [requirements.in](../../app/requirements.in) fixa 12 diretas; [requirements.lock](../../app/requirements.lock) fixa transitivas/hashes; [requirements.txt](../../app/requirements.txt) é alias.
- [Dockerfile](../../app/Dockerfile) fixa Python por digest; instala wheels com `--only-binary=:all: --require-hashes`, sem apt/sdist fallback. [.dockerignore](../../app/.dockerignore) exclui testes, `.env*`, caches e logs.
- Seis serviços compartilham `${COMPOSE_PROJECT_NAME:?}-app:${DELIVERY_WORKER_CODE_VERSION:?}`; só `app` tem build no Compose principal. Não usar tag Python global entre projetos.
- `--build-arg DELIVERY_WORKER_CODE_VERSION=<versão>` é obrigatório. `/etc/wpp-app/IMAGE_CODE_VERSION` e label `org.opencontainers.image.revision` guardam a mesma versão.
- `WPP_REQUIRE_IMAGE_VERSION=true` nos seis serviços exige arquivo/env compatíveis antes de I/O de negócio; erro recusa startup. `utils/image_version.py` publica versão baked no fingerprint.
- Sem arquivo/flag, execução fonte usa fallback env com WARN/contador, não atestação de imagem. Dev bind sinaliza `dev_bind_code_unattested`; fingerprint não valida fonte montada.
- Release certificado usa [release_build.sh](../../scripts/release_build.sh) sobre blobs Git brutos app/ do commit limpo, não o contexto vivo; ignorados locais não entram. [release_identity.py](../../scripts/release_identity.py) valida índice sem flags minúsculas/S e sparse-checkout desativado, gera contexto git-blobs-v1 e recibo0600 atomicamente vinculado a commit/projeto/versão/contexto/Image ID. Preflight exige recibo e ID local correspondentes; tag/label isoladas não bastam. Confiança local Git/builder/daemon, sem atestação de host malicioso.
- Release só em árvore limpa com SHA correto. [Helpers](../../scripts/test_image_version.sh) usam SHA-dirty para tracked/staged/untracked e label para imagem reutilizada.

## Rede, binds e stores

- Projeto obrigatório identifica containers/rede/volumes; sem `container_name` fixo ou rede global. Rede física `<COMPOSE_PROJECT_NAME>_wpp-net`, aliases internos inalterados.
- Templates usam `wpp-ai-bot-CHANGE-ME`, não volumes de instalação existente. D2 operacional é escolha explícita; nunca usar projeto operacional em rehearsal.
- Release não monta `./app:/app`. [docker-compose.dev.yml](../../docker-compose.dev.yml) restaura bind só explicitamente e injeta `WPP_DEV_BIND_CODE=true` nos seis serviços.
- Bootstrap ainda monta raiz em `/workspace` para rename atômico de `.env`; isso não é bind de código `/app`. Não expor esse diretório operacional a testes.
- App escuta internamente em `5000`, sem porta host. `APP_HOST`/`APP_PORT` não publicam porta automaticamente.
- Nginx usa `NGINX_BIND_ADDRESS` default `127.0.0.1`, portas HTTP/HTTPS parametrizadas. Prod exige certificados; location ACME não cria webroot/mount/certificado automaticamente.
- App público permite rotas exatas e métodos de AGENTS §7. CRUD permanece interno sem login próprio; não criar proxy alternativo para contornar a borda.

| Volume lógico | Destino |
| --- | --- |
| `mysql_data` | MySQL bot |
| `app_logs` | Logs/snapshots Python |
| `evolution_instances` | Sessões Evolution |
| `evolution_redis` | `/data` do serviço `redis` |
| `postgres_data` | Postgres Evolution |
| `chatwoot_pg_data` | Postgres Chatwoot |

Baseline de seis volumes ainda não cobre filas/uploads Chatwoot: `chatwoot-redis` sem volume nomeado/AOF explícitos, Rails/Sidekiq sem `/app/storage` compartilhado. Alguns restarts/volumes anônimos podem manter estado, mas não há contrato completo de backup/recriação. Inventariar/resgatar antes de recriar instalação real; D9 aprovada prevê /app/storage compartilhado e Redis com volume/AOF everysec (~1s de fila em crash), mas implementação T7 e ampliação do backup F2 seguem pendentes.

## Configuração e defaults

Fontes: [Settings](../../app/src/config.py), [template raiz](../../.env.example), [template app](../../app/.env.example), [AGENTS §9](../../AGENTS.md#9-variáveis-de-ambiente).

| Grupo | Contrato |
| --- | --- |
| LLM | `OPENROUTER_API_KEY` obrigatória, base OpenRouter, modelo na allowlist |
| Banco bot | MySQL/aiomysql; `DATABASE_URL` coerente com MYSQL_* |
| Evolution | `AUTHENTICATION_API_KEY` igual a `EVOLUTION_API_KEY`; URI coerente com POSTGRES_* |
| Auth | Provisioning só bootstrap privado; enforced após instalação/verificação |
| Receipts | Keyring privado/versionado, distinto do bearer; rotação preserva horizonte |
| Delivery | `direct`, dispatch false, epoch vazio por default |
| Scheduler | Flags false e opt-in false; autoridade/epochs/frota separados |
| Versão | Env, imagem baked, label e fingerprint compatíveis; fallback tem sinal |
| Chatwoot | Conta/token/inbox/segredo, SMTP completo ou desabilitado em dev |
| Callback privado | `http://app:5000` exige SafeFetch privado; só admins confiáveis |

`.env` fornece interpolação e `env_file` dos serviços que declaram; Chatwoot recebe anchor selecionado. Não copiar templates por cima de configuração existente nem tratar placeholders como segredos.

## Preflight, snapshot e controle do ambiente

[release_preflight.sh](../../scripts/release_preflight.sh) e [release_preflight.py](../../scripts/release_preflight.py) recebem candidato, projeto esperado e fase legacy/outbox. Não fazem sourcing, start ou escrita de configuração; consultam config/imagem locais e, opcionalmente, seis volumes nominais. `--require-smtp` exige grupo SMTP. Erros emitem nomes/reasons, nunca valores.

- `COMPOSE_*` só admite `COMPOSE_PROJECT_NAME`, `COMPOSE_ANSI`, `COMPOSE_STATUS_STDOUT`, `COMPOSE_PROGRESS`, `COMPOSE_MENU`, `COMPOSE_HTTP_TIMEOUT`, `COMPOSE_PARALLEL_LIMIT`.
- Qualquer outra, inclusive desconhecida/vazia, falha no candidato/shell. Sem `COMPOSE_FILE`, `COMPOSE_ENV_FILES`, `COMPOSE_PROFILES` ou override automático.
- Shell divergente só é override relevante para chaves interpoladas em Compose/override/candidato; env_file-only `DEBUG`/`LOG_LEVEL` não é substituído pelo shell.
- Config default sem `-f` é comparado ao base em snapshot com Compose e `.env` candidato; `.env` real não é copiado/aberto para esse fim. Config com `--profile '*'` é separada para guards dos serviços.
- No snapshot, cwd=--project-directory. [Prova real](../../scripts/check_release_config_real.py) exige JSON com/sem `--env-file <snapshot>/.env` idêntico; candidato diferente deve falhar.
- `--compose-file` não pode alterar modelo release; override dev não é release. `-p` não substitui gate de projeto; profiles só por `--profile` explícito.
- Preflight valida checkout/índice/sparse, recibo/Image ID/label/referência Docker e mounts dos seis runtimes nos três modelos (allprofiles/base/deploy), além de segredos/URLs/TLS, fase/flags e loopback em provisioning. Mounts /app e descendentes são negados, exceto app_logs:/app/logs exato; configs/secrets não herdam exceção. Fonte bind/file repo/app ou descendente também é recusada. Guard não lê payload de segredo.
- Não há allowlist completa de startup/mounts fora de /app: P3-R6/R7 continua planejado; preflight não testa provider remoto, backup ou migração operacional.

Preflight passa apenas em candidato coerente e imagem exigida disponível. Em árvore documental suja, não declarar preflight/release verde a partir de SHA antigo ou build não realizado.

## Inspeção e ordem operacional

Inspeção sem abrir `.env` ou iniciar serviços:

```bash
git status --short
git branch --show-current
git rev-parse HEAD
git diff --check
docker compose version
```

`docker compose config` direto pode imprimir segredos; não publicar saída. `docker compose ps -q` resolve nome real em operação autorizada, sem nomes de container fixos.

Pré-condições de deploy, não autorização de execução:

1. Review/integração e imagem do SHA alvo limpo; builder certificado `scripts/release_build.sh --project "$COMPOSE_PROJECT_NAME" --expected-commit "$(git rev-parse HEAD)"` → preflight do candidato → `up --no-build`/`run --pull never`, só após todos os gates. `docker compose build app` isolado é dev/teste, não publica recibo; `force-recreate` não é build.
2. Backup, checksum, restore na cópia e migration idempotente antes de primeiro start real, porque `db-migrate` aplica DDL.
3. Candidato reconciliado + preflight, projeto/stores corretos, TLS/domínios e ingress fechado.
4. Preparar Chatwoot antes Rails/Sidekiq; barreira principal ainda pendente, já existente no Compose de validação.
5. Bootstrap privado → verify → enforced → check/monitor → gate de abertura. Wrapper constrói imagem, modifica `.env`/providers e reinicia processos; não executar como consulta.
6. Cutover/drill de rollback delivery por CLI/epochs, sem SQL manual.
7. Scheduler é trilha própria, com enabling→enabled, frota/attestation e canary opt-in; requer aprovação.
8. E2E dedicado/observação com número, credenciais e gasto autorizados.

Plano pós-F0 aprovado (não executado): D10 adota scheduler antes de tráfego comercial, outbox obrigatório para lead real, legado só bootstrap/rollback sem lead e ingress congelado no rollback a direct. F6b exige rehearsal isolado e enable/opt-in autorizado antes do canary. Borda protegida ou preflight verde não resolve esses bloqueadores.

## Provas e limites

- Casos em [app/tests](../../app/tests/); matrizes/mutações em [scripts](../../scripts/), recursos descartáveis/providers sintéticos.
- Código vem da imagem; testes e contratos montados read-only, ambiente sintético explícito, versão env=baked. MySQL usa rede própria e `OUTBOX_TEST_DATABASE_URL`; sem DB não é regressão completa.
- Conferir discovery, import errors, executados/skips, exit code, mutações, imagem/SHA e hashes. Warnings/falhas não devem ser ocultados.
- Gate integrado F0: `run_inbox_ticket08_mysql_matrix.sh`; revisar expected counts no SHA após novas mudanças. Número hardcoded não é evidência.
- Provas focais: shutdown, borda, provider-auth-validation, preflight fake/Compose real e digests/migrations. Stack de validação pode montar fonte e não representa release imutável.
- Certificação integrada p55 aceita em5908a94: preflight105/0, seis consumidores, integral746/OK; recontador v1 falhou por intercalação de streams e v2 recontou o mesmo log. Evidência histórica, não teste desta edição documental nem de seu futuro merge. Novas provas devem separar stdout/stderr e contar marcadores por token.
- CI não configurado no último estado conferido de main/dev. `.test-runs/` é local/ignorado, não retenção automática de evidência.
- Checagem estática de documentação verifica links, inventários, defaults e preservação de contratos; não substitui testes de negócio.

Sem `.env` operacional, provider real, `up` D2, prune ou remoção por filtro/curinga em teste. Prefixo descartável aprovado e cleanup nominal; `down -v` não é rotina.

Runbooks: [delivery](../durable-delivery-runbook.md), [inbox](../inbox-scheduler-runbook.md). Budgets/estados/rollback continuam nesses contratos e AGENTS.

### Remoção de superfície legada — 2026-11-19

CRUD de cadastro v3b, armazenamento JSON, notificação WhatsApp v3b e schemas MVP sem consumidores foram removidos. Handoff permanece no Chatwoot. Schema/ORM histórico e compatibilidade operacional permanecem; nenhuma migration destrutiva foi adicionada. Inventário, limites e procedimento de backup → [relatório de remoção](../../LEGACY_REMOVAL_REPORT.md).
