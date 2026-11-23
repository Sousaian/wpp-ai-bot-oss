# Mapa do código

Base `main` = `dev` = `5908a94`, snapshot documental de 2026-11-09: F0 integrado via PR #14, main promovida por fast-forward, certificação p55 aceita; não equivale a deploy/ativação. Caminhos relativos ao repo; responsabilidades não autorizam alterar módulos fora do ticket.

## Entrypoints e HTTP

| Arquivo em `app/src/` | Responsabilidade |
| --- | --- |
| [app_server.py](../../app/src/app_server.py) | Uvicorn, bind por Settings, deadline de processo |
| [main.py](../../app/src/main.py) | FastAPI/lifespan, Evolution webhook, health e routers |
| [config.py](../../app/src/config.py) | Settings, bounds e invariantes flags/budgets/epochs |
| [delivery_worker.py](../../app/src/delivery_worker.py) | Dispatcher, manutenção, retenção e health local |
| [turn_worker.py](../../app/src/turn_worker.py) | Processor, profile inbox, gate e drain |
| [delivery_cli.py](../../app/src/delivery_cli.py) | Delivery, cutover/rollback, scheduler-state, resolução e quarentena |
| [agent_scheduler_cli.py](../../app/src/agent_scheduler_cli.py) | Subcomandos `agent-scheduler` na CLI delivery, não serviço público |
| [provider_auth_cli.py](../../app/src/provider_auth_cli.py) | `prepare`, `verify`, `enable`, `check`, `monitor`, `snapshot-health` |
| [auth_cli.py](../../app/src/auth_cli.py) | Usuários, bootstrap do primeiro admin, resets e tokens de serviço da API (container-only) |

| Router | Superfície |
| --- | --- |
| [agentes.py](../../app/src/routers/agentes.py) | `/agentes`, CRUD/allowlist e consumo por agente |
| [sessoes.py](../../app/src/routers/sessoes.py) | Leitura/histórico, transferir/retomar, delete fenced |
| [chatwoot.py](../../app/src/routers/chatwoot.py) | Assinatura/parse e delegação de `/chatwoot/webhook` |
| [auth.py](../../app/src/routers/auth.py) | `/auth`: login, TOTP, ativação/reset por token, logout, me |

`Depends(get_db)` não é autenticação. Autenticação/autorização vem de `services/auth/contexto.py` (`autenticar`, transação própria) e `services/auth/rbac.py` (`exigir`, guard por router); cada rota de `/agentes`/`/sessoes` declara uma permissão. Nginx F0 continua bloqueando o CRUD; acesso ao container permanece privilegiado. Schemas ativos estão nos próprios routers.

## Negócio e providers

| Fonte em `app/src/` | Responsabilidade |
| --- | --- |
| [agents/agent_runner.py](../../app/src/agents/agent_runner.py) | Contexto, normalização, resposta/resumo e accounting legado |
| [services/llm_client.py](../../app/src/services/llm_client.py) | Único OpenRouter; wrapper legado vs tentativa única fenced |
| [services/evolution_orchestrator.py](../../app/src/services/evolution_orchestrator.py) | Evento/receipt/UoW e seleção direct/outbox/scheduler |
| [services/chatwoot_orchestrator.py](../../app/src/services/chatwoot_orchestrator.py) | Retorno humano, mapping/attempt, dedupe e enqueue |
| [services/evolution_client.py](../../app/src/services/evolution_client.py) | HTTP multi-instância, mensagens/presença |
| [services/chatwoot_client.py](../../app/src/services/chatwoot_client.py) | Contatos, conversas, mensagens e contrato provider |
| [services/webhook_auth.py](../../app/src/services/webhook_auth.py) | Bearer Evolution e criptografia dos callbacks Chatwoot |
| [services/auth/](../../app/src/services/auth/) | `contexto` (credencial/CSRF), `rbac` (papéis/permissões), `senha` (argon2id+pepper), `sessao` (login/TOTP/tokens), `totp`, `limites`, `auditoria`, `administracao` (CLI) |
| [services/provider_auth_bootstrap.py](../../app/src/services/provider_auth_bootstrap.py) | Instalação/auditoria/probes e snapshot provider |
| [services/handoff_manager.py](../../app/src/services/handoff_manager.py) | Handoff compatível direct/outbox não agendado |
| [services/delivery_adapters.py](../../app/src/services/delivery_adapters.py) | Decoders/payloads e operações remotas versionadas |

## Turns e observabilidade

| Fonte em `app/src/services/` | Responsabilidade |
| --- | --- |
| [turn_processor.py](../../app/src/services/turn_processor.py) | Claim/revisão, geração, refresh e typing |
| [turn_delivery.py](../../app/src/services/turn_delivery.py) | Preparação/admissão de efeitos outbox |
| [turn_handoff.py](../../app/src/services/turn_handoff.py) | Handoff final e resumo do final-candidate |
| [handoff_context.py](../../app/src/services/handoff_context.py) | Chunking/markers de contexto tardio |
| [replica_compat.py](../../app/src/services/replica_compat.py) | Fingerprints, heartbeats e stop/compatibilidade |
| [scheduler_observability.py](../../app/src/services/scheduler_observability.py) | Agregados, códigos técnicos, indisponibilidade |

## Dados e repositórios

[connection.py](../../app/src/database/connection.py) define engine/sessões; [unit_of_work.py](../../app/src/database/unit_of_work.py) exige commit explícito; [db_errors.py](../../app/src/database/db_errors.py) identifica transação perdida.

Todos os nomes abaixo estão em [database/repositories/](../../app/src/database/repositories/):

| Repositório | Área |
| --- | --- |
| `agente_repo.py` | Configuração/leitura do agente |
| `sessao_repo.py` | Sessão, handler, mapping e sequência |
| `mensagem_repo.py` | Persistência/histórico |
| `consumo_repo.py` | Ledger, totais, limite e destino seguro do uso |
| `usage_reconciliation_repo.py` | Pendências de reconciliação |
| `inbound_event_repo.py` | Receipts, HMAC, identidade/replay |
| `outbox_repo.py` | Enqueue, identidade/ordenação |
| `delivery_worker_repo.py` | Claim/lease/retry/finalização/ops |
| `delivery_state_repo.py` | Autoridade, epoch, frota e cutover |
| `handoff_attempt_repo.py` | Attempts Chatwoot |
| `handoff_terminal_repo.py` | Terminalização comum de handoff falho |
| `scheduler_repo.py` | Autoridade, lanes/turns/revisões/memberships |
| `turn_generation_repo.py` | Geração/refresh/draft/typing/accounting |
| `turn_delivery_repo.py` | Admissão/finalização dos efeitos |
| `turn_handoff_repo.py` | Handoff terminal e contexto tardio |
| `lifecycle_repo.py` | Operador, retomada/delete/cancel/quarentena |
| `agent_scheduler_repo.py` | Opt-in/opt-out guardado |
| `replica_repo.py` | Heartbeats/compatibilidade/atestação |
| `scheduler_diagnostics_repo.py` | Diagnóstico consistente/clearance |
| `scheduler_metrics_repo.py` | Queries agregadas |
| `scheduler_retention_repo.py` | Discovery/scrub/purge terminal |

Não deslocar SQL de negócio para router/service novo. [Mapa de dados](dados.md).

## Utilitários e infraestrutura

| Fonte | Responsabilidade |
| --- | --- |
| [image_version.py](../../app/src/utils/image_version.py) | Identidade baked, startup fail-closed e sinal de bind/fallback |
| [process_shutdown.py](../../app/src/utils/process_shutdown.py), [shutdown.py](../../app/src/utils/shutdown.py) | Deadline/quiescência/esperas limitadas |
| [async_ttl_cache.py](../../app/src/utils/async_ttl_cache.py) | Coalescing de subcoletores, não deadline global |
| [access_log.py](../../app/src/utils/access_log.py), [logger.py](../../app/src/utils/logger.py) | Redação e logging sem dumps sensíveis |
| [nginx.conf](../../nginx/nginx.conf), [nginx.dev.conf](../../nginx/nginx.dev.conf) | Allowlist pública exata, métodos/URI fixa e privacidade |
| [wpp_phone_log_redaction.rb](../../chatwoot/config/initializers/wpp_phone_log_redaction.rb) | Rails/Active Job: telefone, conteúdo e credenciais |
| [phone_log_redaction_check.rb](../../chatwoot/test/phone_log_redaction_check.rb) | Check no Rails instalado |
| [frontend-nginx.conf](../../frontend-nginx.conf) | Painel Evolution externo |

## Contrato de release e tooling F0

- [Dockerfile](../../app/Dockerfile), [requirements.in](../../app/requirements.in), [requirements.lock](../../app/requirements.lock), [requirements.txt](../../app/requirements.txt), [.dockerignore](../../app/.dockerignore) → imagem com base/diretas/transitivas/hashes e identidade baked.
- [Compose base](../../docker-compose.yml) e [override dev](../../docker-compose.dev.yml) → isolamento por projeto e separação entre imagem release e fonte montada.
- [release_build.sh](../../scripts/release_build.sh), [release_build.py](../../scripts/release_build.py), [release_identity.py](../../scripts/release_identity.py), [release_process.py](../../scripts/release_process.py) → exportação Git canônica, recibo local Image ID, índice/sparse fail-closed e cancelamento/cleanup.
- [release_preflight.sh](../../scripts/release_preflight.sh), [release_preflight.py](../../scripts/release_preflight.py) → candidato env/config/imagem, sem start nem sourcing.
- [check_release_config_real.py](../../scripts/check_release_config_real.py) → prova Compose real, snapshot/descoberta/sentinela e mutações.
- [test_image_version.sh](../../scripts/test_image_version.sh) → label de imagem reutilizada e SHA-dirty para builds de teste.

## Provas

- [app/tests/](../../app/tests/) → unitários, contratos, MySQL, concorrência/shutdown e fixtures.
- [scripts/](../../scripts/) → wrappers de matrizes/mutações, shape/digests, medição e provisionamento; nem todos read-only.
- [tests/provider_contracts/](../../tests/provider_contracts/) e [Compose de validação](../../docker-compose.provider-auth-validation.yml) → contratos isolados, não deployment principal.

## Investigação por sintoma

| Sintoma | Começar por |
| --- | --- |
| Callback rejeitado/replay diferente | HTTP/auth → orchestrator → inbound repo |
| Uso conhecido sem linha consolidada | LLMResult → consumo/reconciliação → attempt exato |
| Queued sem envio | Outbox → runtime/epoch → worker/lease → adapter |
| Transição handoff bloqueada | Attempt → open Chatwoot → mapping/fence/dependência |
| Agente não usa scheduler | Opt-in → flags/estado/epochs → frota |
| Health lento | main → pool/DB → caches/frota → snapshot |
| Processo não encerra | app_server/watchdog → quiescência → deadlines/heartbeat |
