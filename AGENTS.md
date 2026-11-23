# WPP AI Bot — AGENTS.md

> **Revisão documental:** 2026-11-09 — runtime v4.1, outbox, scheduler e F0 implementados; ativação operacional separada.
> **Base desta conciliação:** `main` = `dev` = `5908a94` (PR #14; promoção de main por fast-forward; certificação integrada p55). Esta edição documental não é deploy nem certifica seu futuro SHA de merge.
> **Repositório:** `Sousaian/wpp-ai-bot-oss` (público, Apache-2.0). Regras de fluxo, higiene pública e testes em [§0](#0-repositório-open-source--regras-de-trabalho) e [CONTRIBUTING.md](CONTRIBUTING.md). Histórico anterior a 2026-11-19 (PRs #1–#17, branch `dev`) existe só no repositório privado de origem e não é referência deste repo.
> **Regra:** este arquivo concentra instruções e contratos para agentes. Código/SQL descrevem implementação; estado operacional exige verificação própria. Merge não equivale a deploy.

## 0. Repositório open source — regras de trabalho

Vigência: 2026-11-19. Valem para pessoas e agentes (Traycer, Codex, Claude e outros).

1. **Workspace e remoto únicos:** checkout `wpp-ai-bot-oss` (local do mantenedor: `~/folder.dev/wpp-ai-bot-oss`), remoto `origin` = `github.com/Sousaian/wpp-ai-bot-oss`. O repositório privado de origem (`wpp-ai-bot`) é arquivo somente leitura: não editar, não fazer push e não sincronizar automaticamente com ele.
2. **Branches:** `main` é a única branch de longa duração (não há `dev`). Toda mudança entra em branch própria (`feat/…`, `fix/…`, `docs/…`, `chore/…`) por pull request. Sem push direto em `main`, sem force push e sem reescrever histórico publicado. Merge só com aprovação do mantenedor.
3. **Higiene pública (bloqueante):** nunca commitar segredos, `.env` reais, tokens, números/JIDs reais de WhatsApp, conversas reais, nomes de clientes ou empresas atendidas, caminhos absolutos de máquina (`/home/...`), IDs de recursos privados ou links para ferramentas internas. Dados de teste só sintéticos (`*.invalid`, `example.invalid`, `55119999990xx`). Antes de abrir PR: varredura com gitleaks (ou equivalente) na árvore e nos commits novos, sem achados reais.
4. **Licença:** Apache-2.0. Mantenha `LICENSE` e `NOTICE`. Nova dependência exige licença compatível com redistribuição Apache-2.0 (permissivas preferidas); dependência copyleft forte embutida no processo exige aprovação explícita. Serviços externos (Evolution, Chatwoot, MySQL) continuam sob as próprias licenças.
5. **Posicionamento:** plataforma open source e self-hosted. Não reintroduzir linguagem de produto B2B/SaaS comercial nem roadmap comercial privado na documentação pública.
6. **Arquitetura e compatibilidade:** persistir sessões em MySQL; atribuição e disponibilidade de atendentes ficam no Chatwoot. Preservar contratos dos modos direct, outbox e scheduler. Não retirar tabelas, campos ou valores de enum de compatibilidade sem migração explícita e auditada, com backup, restauração testada e validação em MySQL novo e existente. Coordenar SQL, ORM, API e testes conforme §5.6.
7. **Testes obrigatórios antes de PR:** suíte completa `python -m unittest discover` com MySQL 8.0 descartável em rede interna (credenciais sintéticas), sem falhas. Linha de base atual: **751 testes OK**. Mudança de contrato de documentação atualiza o teste correspondente no mesmo PR. Testes que pulam por falta de MySQL não contam como aprovação.
8. **Planejamento interno** (tickets, specs, revisões e evidências) fica fora do repositório. A documentação pública resume decisões e estado sem depender desses artefatos.

## 1. Contexto do Projeto

WPP AI Bot é uma plataforma open source (Apache-2.0) e self-hosted de atendimento automatizado por texto no WhatsApp, com IA generativa, transferência para atendimento humano no Chatwoot e medição de consumo LLM por agente.

Objetivo → responder leads no contexto da empresa, preservar histórico, transferir quando necessário e tornar operação, entrega e consumo auditáveis. Não é um chatbot genérico sem contexto nem um SaaS self-service já entregue.

### 1.1 Modelo atual e direção de produto

- **Implementado:** uma instalação por cliente; cada número/instância Evolution corresponde a um agente com prompt, modelo, histórico e limite próprios.
- **Operação atual:** configuração por operador via API/CLI; CRUD sem autenticação de usuário no app. Isolamento por agente não constitui isolamento de empresas autenticadas numa instalação compartilhada.
- **Direção planejada:** múltiplas empresas por instalação, portal/API autenticada, usuários individuais, MFA, papéis e auditoria. Ainda não há `empresa_id`, usuários ou RBAC no schema atual; não implementá-los por inferência.
- **Atendimento humano:** Chatwoot; atribuição a atendentes e disponibilidade são responsabilidades dele, não do MySQL do bot.
- **Comercial:** modelo permitido e soft limit por agente; medição de custo não é módulo de cobrança automática.

Definição e roadmap → [README.md](README.md), [mapa e estado](docs/mapa-do-projeto/estado-e-roadmap.md).

### 1.2 Estado das entregas

| Entrega | Código | Operação |
| --- | --- | --- |
| OpenRouter, allowlist, `consumo_llm`, soft limit | Implementados | E2E pareado do release atual pendente |
| Handoff Chatwoot | Implementado | Setup e validação operacional pendentes |
| Outbox fenced + CLIs de cutover/rollback | Implementados | `DELIVERY_MODE=direct`, dispatch desligado por default |
| Scheduler/turns, geração, typing, handoff e opt-in | Implementados, tickets inbox 01–09 | Flags desligadas; ativação exige epochs, frota compatível e opt-in |
| Deadline de shutdown do processo | PR #13 em `main` | Não corrige accounting pré-commit do legado |
| Imagens imutáveis, pins, preflight e allowlist Nginx | F0 integrado em dev via PR #14 e em main por fast-forward, base `5908a94` | Certificação integrada p55 aceita; não presumir deploy/ativação |
| Portal multi-empresa/MFA/RBAC | Planejamento | Não implementado |

Não usar números antigos de testes ou status de versões como prova de execução no checkout atual.

### 1.3 Handoff atual

`[TRANSFERIR]` é removido do texto ao lead. Sessão termina com motivo de handoff; bot cria/associa conversa Chatwoot e envia transição. Resposta humana retorna por `POST /chatwoot/webhook`. No scheduler, final-candidate e contexto tardio seguem §10.13/§10.14.

`atendentes`, `numero_notificacao`, `motivo_handoff` e handler `humano` preservam compatibilidade histórica. Não implementar atribuição própria nem enviar resumos de handoff a atendentes por WhatsApp.

## 2. Ferramentas e Providers

### 2.1 Desenvolvimento

| Ferramenta | Papel e limite |
| --- | --- |
| Traycer | Planejamento, artifacts, coordenação e gates |
| Codex | Implementação e validação conforme roteamento aprovado |
| Claude | Planejamento/review; fallback de implementação somente quando autorizado |
| Git/GitHub | Código e PRs em `Sousaian/wpp-ai-bot-oss` (§0); CI no GitHub Actions (`.github/workflows/ci.yml`): gitleaks, checagens estáticas e suíte completa com MySQL descartável via `scripts/ci_full_suite.sh` |
| Docker/Compose | Runtime e provas descartáveis, sem usar volumes operacionais para teste |

Projeto público (Apache-2.0): o código pode ser lido por qualquer ferramenta, mas segredos, `.env` reais, dados de conversas e credenciais de providers nunca saem da máquina do operador. OpenRouter runtime é uma integração diferente, explicitamente existente no produto. Preferências de modelo de desenvolvimento ficam no guia de seleção vigente, não em uma lista inventada neste arquivo.

### 2.2 Provider LLM do runtime

OpenRouter já é o provider do app. Cliente central → `app/src/services/llm_client.py`, SDK Python `openai`, `AsyncOpenAI`, `chat.completions.create()` e `OPENROUTER_BASE_URL=https://openrouter.ai/api/v1`.

- `OPENROUTER_API_KEY` é obrigatória; não é `OPENAI_API_KEY`.
- `OPENAI_API_KEY`/`OPENAI_MODEL` são extras ignorados por Settings do app; integração OpenAI nativa da Evolution não deve responder leads em paralelo.
- IDs autorizados vêm de `MODELOS_PERMITIDOS` em `app/src/routers/agentes.py`.
- `modelo_llm` é preferido; `modelo_openai` permanece como compatibilidade, não como indicação de provider direto OpenAI.
- Não adicionar SDK próprio OpenRouter, chaves hardcoded ou model IDs arbitrários pela API.

### 2.3 Segredos e consumo

Não logar chave, bearer, signing secret, keyring, payload de conversa ou dump de Settings. Provider de runtime não é autorização para testar com LLM pago. Uso real requer autorização e limite de gasto; provas normais usam providers sintéticos.

## 3. Arquitetura

### 3.1 Containers Docker

Compose principal declara **17 serviços**. Nome Redis da Evolution é `redis`; `evolution_redis` é volume. Painel Evolution é imagem externa, não frontend React mantido neste repo.

| Serviço | Tecnologia/referência nesta base F0 | Responsabilidade |
| --- | --- | --- |
| `app` | Python 3.12 / FastAPI | Webhooks, API e readiness; único build Python no Compose base |
| `delivery-worker` | Mesma imagem Python | Outbox fenced; idle sem autoridade/dispatch |
| `delivery-ops` | Mesma imagem, profile `ops` | CLI auditada, não serviço público |
| `turn-worker` | Mesma imagem, profile `inbox` | Processor de turns; default off |
| `provider-auth-bootstrap` | Mesma imagem, profile `ops` | Preparar/verificar/habilitar auth |
| `provider-auth-monitor` | Mesma imagem Python | Auditoria e snapshot local de readiness |
| `db` / `db-migrate` | `mysql:8.0` + mesmo digest | Dados do bot / migration one-shot |
| `evolution` | Evolution API `v2.3.7` + digest | Gateway WhatsApp |
| `evolution-postgres` | `postgres:15` + digest | Dados internos Evolution |
| `redis` | `redis:latest` + digest | Cache Evolution; volume e AOF |
| `frontend` | `evoapicloud/evolution-manager:latest` + digest | Painel Evolution |
| `chatwoot` / `chatwoot-sidekiq` | Chatwoot `v4.17.1` + digest | Rails / filas e callbacks |
| `chatwoot-postgres` | `pgvector/pgvector:pg15` + digest | Banco Chatwoot |
| `chatwoot-redis` | `redis:7-alpine` + digest | Filas/cache Chatwoot |
| `nginx` | `nginx:alpine` + digest | Borda HTTP/HTTPS |

Sem `container_name` ou nome global de rede. `COMPOSE_PROJECT_NAME` é obrigatório; containers, rede e volumes derivam do projeto. D2 operacional usa `traycer-wpp-ai-bot-soft-ocelot` somente por escolha explícita no `.env` real, preservando os seis volumes existentes; templates usam `wpp-ai-bot-CHANGE-ME`, não instalação existente.

Os seis serviços Python usam `${COMPOSE_PROJECT_NAME:?}-app:${DELIVERY_WORKER_CODE_VERSION:?}`; somente `app` declara build. Build de outro projeto não substitui essa tag. Código release vem da imagem, sem `./app:/app`; bind de código só com `docker-compose.dev.yml` explicitamente selecionado e sinal `WPP_DEV_BIND_CODE=true`. Fingerprint baked não atesta fonte montada em dev.

Rede → `<COMPOSE_PROJECT_NAME>_wpp-net`. App `5000`, Evolution `8080`, Chatwoot `3000` são internos. Nginx publica `80/443` com bind default `127.0.0.1`, portas parametrizadas; domínio de exemplo não é deployment. Borda app aplica allowlist §7; isso não adiciona auth/RBAC no FastAPI.

Mapas → [arquitetura](docs/mapa-do-projeto/arquitetura.md), [operação e versões](docs/mapa-do-projeto/operacao-e-validacao.md).

### 3.2 Camadas da aplicação

| Camada | Fontes | Responsabilidade |
| --- | --- | --- |
| Processo | `app_server.py`, `utils/process_shutdown.py`, `utils/shutdown.py`, `utils/image_version.py` | Uvicorn, sinais, budgets, marcador final e identidade baked |
| HTTP | `main.py`, `routers/` | Auth/parse, dependências, status; SQL fica nos repos |
| Orquestração | `services/evolution_orchestrator.py`, `services/chatwoot_orchestrator.py` | UoW, receipts, autoridade, branches exclusivos |
| Conversa/LLM | `agents/agent_runner.py`, `services/llm_client.py` | Contexto, tag de transferência, provider e resultado de uso |
| Handoff | `services/handoff_manager.py`, `turn_handoff.py`, `handoff_context.py` | Compatibilidade, handoff terminal e contexto tardio |
| Entrega | `delivery_worker.py`, `services/delivery_adapters.py` | I/O fora das transações outbox, lease/fence/retry |
| Scheduler | `turn_worker.py`, `services/turn_processor.py`, `turn_delivery.py` | Geração e admissão duráveis por agente/número |
| Dados | `database/models.py`, `database/repositories/`, `database/unit_of_work.py` | ORM, acesso MySQL e commit explícito |
| Controle/health | `provider_auth_cli.py`, `services/provider_auth_bootstrap.py`, `replica_compat.py`, `scheduler_observability.py` | Auth, compatibilidade, snapshots e métricas |
| Operador | `delivery_cli.py`, `agent_scheduler_cli.py` | Cutover, resolução, enablement e opt-in auditados |

Inventário detalhado → [mapa do código](docs/mapa-do-projeto/codigo.md).

### 3.3 Mensagem recebida — três caminhos distintos

```text
Evolution → POST /webhook → auth conforme modo → parse → agente ativo
    ├─ sem opt-in scheduler + direct
    │    lock runtime/agente/sessão → LLM/resumo → envio direto → commit
    ├─ sem opt-in scheduler + outbox
    │    lock runtime/agente/sessão → LLM/resumo → domínio + outbox → commit
    │    delivery-worker → HTTP de entrega → finalização fenced
    └─ opt-in scheduler + gates válidos
         intake/receipt/membership em transação curta → commit → HTTP 202
         turn-worker → attempt durável → LLM fora dos locks → finalização
         admissão outbox → delivery-worker
```

**Outbox não elimina a chamada LLM no webhook não agendado.** Elimina HTTP de entrega desse caminho; locks e accounting pré-commit do legado continuam. Scheduler exige auth enforced, epochs compatíveis, estado habilitado e opt-in do agente; não há fallback silencioso quando o gate fecha.

### 3.4 Handoff

No outbox não agendado, resumo ainda ocorre na UoW do webhook. Domínio, attempt, abertura Chatwoot e transição bloqueada são commitados juntos; worker abre a conversa e desbloqueia transição sob fence. No direct, integração é imediata e não tem a mesma garantia de retry. No scheduler, decisão final e memberships/contexto tardio seguem §10.13/§10.14.

Resposta e resumo são chamadas cobradas separadas, `tipo_chamada='resposta'` e `tipo_chamada='resumo'`. Falha/soft limit no resumo usa fallback controlado, não custo fictício.

### 3.5 Retorno humano

Chatwoot → `routers/chatwoot.py` → auth conforme modo → `services/chatwoot_orchestrator.py`. Em outbox: receipt, mapping/geração válidos e enqueue após transição → commit → worker. Compatibilidade direct tem envio próprio; não generalizar garantia durável a todo modo.

## 4. Estrutura e navegação

| Caminho | Conteúdo |
| --- | --- |
| `README.md` | Definição, objetivo, estado e entrada para colaboradores |
| `AGENTS.md` | Instruções, invariantes e contratos de implementação |
| `docs/mapa-do-projeto/` | Arquitetura, código, dados, operação e roadmap |
| `docs/durable-delivery-runbook.md` | Cutover outbox, rollback e limites de entrega |
| `docs/inbox-scheduler-runbook.md` | Enablement, canary, opt-in, drain, retenção e operação scheduler |
| `app/src/` | Runtime Python |
| `app/tests/` | Testes unitários/contratos/MySQL e fixtures |
| `db/init.sql` | Schema e migrations aditivas com shape/digest fail-closed |
| `docker-compose.yml` | Release: código da imagem, isolamento por projeto; executar pode alterar dados |
| `docker-compose.dev.yml` | Bind de fonte dev explícito; não usar como prova de release |
| `app/requirements.in`, `app/requirements.lock`, `app/requirements.txt` | Diretas, contrato com hashes e alias de compatibilidade |
| `scripts/release_build.sh`, `scripts/release_build.py`, `scripts/release_identity.py`, `scripts/release_process.py` | Builder Git canônico, recibo Image ID, guards índice/sparse e cancelamento/cleanup |
| `scripts/release_preflight.sh`, `scripts/release_preflight.py` | Preflight somente leitura de candidato/config/imagem/recibo e mounts/configs/secrets |
| `scripts/check_release_config_real.py`, `scripts/test_image_version.sh` | Prova Compose real e helpers de identidade dos runners |
| `docker-compose.provider-auth-validation.yml` | Stack de prova dos contratos provider; não configuração de produção |
| `.env.example`, `app/.env.example` | Templates públicos; nunca usar placeholders como segredos reais |
| `scripts/` | Matrizes, mutações, probes e provisionamento |
| `nginx/`, `chatwoot/` | Borda, privacidade de logs e checks Rails |
| `tests/provider_contracts/` | Provas auxiliares dos contratos Evolution/Chatwoot |

Documentação de mapeamento explica o código, não substitui §10, §16.1 ou os runbooks. Toda mudança de contrato deve atualizar documentação/testes pertinentes no mesmo changeset.

---

## 5. Banco de Dados

### 5.1 Tabela `agentes`

| Coluna | Tipo | Obrigatório | Descrição |
|--------|------|-------------|-----------|
| `id` | INT AUTO_INCREMENT | Sim | PK |
| `nome` | VARCHAR(255) | Sim | Nome da empresa cliente |
| `instance_name` | VARCHAR(255) UNIQUE | Sim | Nome da instância na Evolution API |
| `system_prompt` | TEXT | Sim | Instruções e personalidade do agente |
| `modelo_openai` | VARCHAR(100) | Sim | Compatibilidade legada; não remover sem migration e rollback |
| `modelo_llm` | VARCHAR(255) NULL | Não | Modelo OpenRouter solicitado, preferido pelo runtime e validado pela API |
| `numero_notificacao` | VARCHAR(50) | Sim | **LEGADO** — não usado no fluxo v4 |
| `chatwoot_inbox_id` | INT NULL | Não | ID do inbox no Chatwoot para este agente |
| `max_historico` | INT DEFAULT 20 | Sim | Mensagens enviadas como contexto à IA |
| `ativo` | BOOLEAN DEFAULT TRUE | Sim | Se o agente está ativo |
| `inbox_scheduler_enabled` | BOOLEAN DEFAULT FALSE | Sim | Opt-in por agente para intake durável; só vale com auth enforced, runtime outbox e epochs correspondentes |
| `limite_mensal_creditos` | DECIMAL(12,6) NULL | Não | Franquia mensal de custo LLM em créditos USD; `NULL` significa sem soft limit no app |
| `criado_em` / `atualizado_em` | DATETIME | Auto | Timestamps |

### 5.2 Tabela `sessoes`

| Coluna | Tipo | Obrigatório | Descrição |
|--------|------|-------------|-----------|
| `id` | INT AUTO_INCREMENT | Sim | PK |
| `agente_id` | INT FK → agentes | Sim | Referência ao agente |
| `telefone` | VARCHAR(50) | Sim | Número do contato (sem `@s.whatsapp.net`) |
| `handler` | ENUM(`bot`,`humano`,`encerrada`) | Sim | `humano` é legado v3; novos fluxos usam `encerrada` |
| `total_mensagens` | INT DEFAULT 0 | Sim | Contador de mensagens trocadas |
| `motivo_handoff` | TEXT NULL | Não | **LEGADO** — manter por compatibilidade |
| `encerrada_em` | DATETIME NULL | Não | Quando a sessão foi encerrada |
| `motivo_encerramento` | VARCHAR(50) NULL | Não | Ex.: `handoff`, `falha_tecnica`, `transferencia_manual` |
| `atendente_id` | INT NULL FK → atendentes | Não | **LEGADO v3b** — não é populado na v4 |
| `chatwoot_conversation_id` | INT NULL | Não | ID da conversa criada no Chatwoot |
| `criado_em` / `ultima_interacao` | DATETIME | Auto | Timestamps |

> Constraint única: `(agente_id, telefone)`. Sessão também possui `delivery_stream_key`, `next_delivery_sequence` e `handoff_generation`; mapping Chatwoot e ordenação não podem ser reconstruídos de payload mutável. Ver [mapa de dados](docs/mapa-do-projeto/dados.md).

### 5.3 Tabela `mensagens`

| Coluna | Tipo | Obrigatório | Descrição |
|--------|------|-------------|-----------|
| `id` | INT AUTO_INCREMENT | Sim | PK |
| `sessao_id` | INT FK → sessoes | Sim | Referência à sessão |
| `role` | ENUM(`user`,`assistant`) | Sim | Quem enviou |
| `conteudo` | TEXT | Sim | Texto da mensagem |
| `criado_em` | DATETIME | Auto | Timestamp |

### 5.4 Tabela `consumo_llm` — implementada

Esta tabela é a fonte de verdade de consumo do cliente para relatórios, faturamento, auditoria e soft limit. **Nunca calcular consumo comercial somente por `total_mensagens`**, pois custo varia por modelo, tamanho de contexto, resposta e cache.

`consumo_llm`, `llm_usage_reconciliation` e `handoff_attempts` nunca são apagados por delete via API: `DELETE /agentes/{id}` de agente com qualquer linha nessas tabelas é recusado (409; S3, §10.15). O `ON DELETE CASCADE` do schema permanece; desativar é `PUT ativo=false`.

| Coluna | Tipo | Obrigatório | Descrição |
|--------|------|-------------|-----------|
| `id` | BIGINT AUTO_INCREMENT | Sim | PK |
| `agente_id` | INT FK → agentes | Sim | Agente/cliente responsável pelo custo |
| `sessao_id` | INT FK → sessoes | Não | Sessão associada; pode ser nula para operações administrativas futuras |
| `modelo_llm` | VARCHAR(255) | Sim | ID **solicitado** (allowlisted) do modelo no OpenRouter; base de faturamento e chave exata do vínculo com `generation_attempts` |
| `modelo_llm_reportado` | VARCHAR(255) NULL | Não | ID do modelo exatamente como o provider reportou (ex.: variante datada); `NULL` quando ausente ou em linhas anteriores à coluna |
| `tipo_chamada` | ENUM(`resposta`,`resumo`) | Sim | Distingue resposta ao lead de resumo de handoff |
| `tokens_input` | INT UNSIGNED | Sim | Tokens de prompt/input reportados pelo provider |
| `tokens_output` | INT UNSIGNED | Sim | Tokens de completion/output reportados pelo provider |
| `tokens_reasoning` | INT UNSIGNED NULL | Não | Tokens de reasoning, quando o provider reportar |
| `tokens_cache_read` | INT UNSIGNED NULL | Não | Tokens lidos de cache, quando reportados |
| `tokens_cache_write` | INT UNSIGNED NULL | Não | Tokens escritos em cache, quando reportados |
| `custo_creditos` | DECIMAL(12,8) | Sim | Custo exato em USD/créditos retornado pelo OpenRouter |
| `provider_generation_id` | VARCHAR(255) NULL UNIQUE | Não | Identificador da geração para auditoria/consulta posterior |
| `criado_em` | DATETIME | Sim | Timestamp do consumo |

Índices obrigatórios:

```sql
INDEX idx_consumo_llm_agente_data (agente_id, criado_em),
INDEX idx_consumo_llm_sessao (sessao_id),
UNIQUE KEY uq_consumo_llm_generation (provider_generation_id)
```

### 5.5 Tabela `atendentes` — Compatibilidade do schema

Tabela e ORM mantêm dados históricos e a referência de `sessoes.atendente_id`. O Chatwoot gerencia a atribuição do handoff sem usar essa tabela. Não executar DROP no schema atual; qualquer mudança destrutiva exige migração explícita e auditada (§0).

### 5.6 Migrations e compatibilidade existentes

`modelo_llm`, `limite_mensal_creditos`, `consumo_llm` e reconciliação já existem. Não recriar a migração v4.1 como feature pendente nem executar exemplos históricos de ALTER fora de `db/init.sql`.

- Migração é aditiva e retrocompatível; `modelo_openai` permanece até decisão explícita de remoção.
- Valor novo de modelo é preferido; fallback legado existe durante compatibilidade. Cadastro novo valida allowlist e normaliza IDs no router.
- Banco: fonte de DDL, índices, checks e guards é `db/init.sql`; ORM é `database/models.py`, acesso é pelos repositórios.
- Rodar MySQL limpo, volume existente copiado, reaplicação idempotente e casos de shape divergente. DDL faz commit implícito; não confiar em rollback de transação para desfazer migration.
- Se contrato/schema mudar: SQL, ORM, repositórios, schemas Pydantic, templates e este arquivo no mesmo changeset, quando aplicáveis.
- Tabelas de autoridade/auditoria não são cadastro descartável; lifecycle, FKs e retenção seguem §10 e o mapa de dados.

### 5.7 Fundação de turns por inbox — adicionada, desabilitada

O schema aditivo contém `scheduler_runtime_state`, `agent_service_lanes`,
`conversation_turns`, `turn_revisions`, `turn_input_memberships` e
`generation_attempts`. `scheduler_runtime_state.state` nasce `disabled`; a
presença das tabelas não autoriza intake assíncrono, geração, typing ou dispatch.

`outbox_deliveries.delivery_class` é intenção interna imutável. Entregas novas
da classe `automated` exigem links exatos de turn/revision e epoch do scheduler.
Linhas legadas são classificadas somente por operações/chaves internas
prováveis; qualquer `legacy_unknown` não terminal bloqueia enablement.

Ticket 04 adiciona em `turn_revisions` `draft_kind`
(`generated`/`soft_limit_fallback`/`technical_fallback`) e `typing_jitter_ms`
(`-1000..1000`, `ck_revision_typing_jitter`). Status `typing` é o estado
`typing_wait`: duração 2–12s (`typing_delay_ms`), jitter sorteado e
`typing_deadline_at = generation_completed_at + typing_delay_ms` são gravados uma
vez. O guard `trg_turn_revision_draft_immutable_v2` torna draft e schedule
imutáveis (texto só pode ir a `NULL` no scrub), proíbe reabrir
`superseded`/`cancelled`/`dead` e `final_candidate`; `admitted` é terminal e não
pode mudar para nenhum outro status. O guard exige reason para `superseded`.
Migration aditiva falha fechada com erro nomeado para shape exato divergente
(tipo, nulabilidade, default, collation e posição) ou jitter fora da faixa;
`typing_jitter_ms` deve ficar imediatamente depois de `typing_delay_ms`. A regra
também cobre `draft_kind` e `modelo_llm_reportado` nas duas tabelas que recebem a
coluna. A migration nunca reposiciona coluna nem reescreve dados.

Ticket 05 adiciona `scheduled_handoffs` e `handoff_context_assignments`.
`scheduled_handoffs` fixa attempt, turn, revisão admitida, epoch e attempt de
resumo; `handoff_context_assignments` transfere cada membership sucessor aceito
uma única vez para o handoff, com ordinal e `source_id` exatos. Ambos usam FKs
`RESTRICT`. A migration valida shape exato (colunas com ordinal, tipo,
signedness, nulabilidade, default, charset/collation e extra; índices com
colunas, ordem, unicidade, collation, sub_part, tipo e visibilidade; FKs com
schema/tabela/colunas e regras; checks com `ENFORCED` e digest da cláusula) e
falha fechada com erro nomeado, sem reescrever. FU-3: somente o CHECK
`ck_scheduled_handoff_summary_source` aceita dois digests SHA-256 byte-exatos:
`12213a6acf680656113841970e1ec05c2a3ab972725a469af6936615af3c0c9a`
(forma legada `_latin1`) e
`0308f61fbc7eacfab2b60cc48797863b0be253ca9fc7afae7aea361fe4097e13`
(forma `_ascii` re-serializada pelo MySQL 8.0 após manutenção).
Nome, ENFORCED, conjunto de CHECKs e demais digests continuam exatos.
Sem normalização, DDL substitutivo ou mudança de schema_version.
Guards v1 (digest exato):
`trg_scheduled_handoff_immutable_v1`,
`trg_handoff_context_assignment_immutable_v1`,
`trg_turn_input_handoff_state_v1` (allowlist `active→active|sealed`,
`sealed→sealed`, `active→post_handoff_pending` só com assignment,
`post_handoff_pending→post_handoff_pending`) e
`trg_outbox_context_payload_immutable_v1` (payload/dependência/identidade da
nota imutáveis; só scrub terminal para `{}`). A presença dessas tabelas não
ativa scheduler, processor, provider, delivery automatizado ou presença.

Ticket 06 adiciona `outbox_deliveries.automated_effect`
(`reply`/`handoff_transition`) como identidade imutável e substitui
`uq_outbox_turn_revision` por `uq_outbox_turn_revision_effect`
(`conversation_turn_id`, `turn_revision_id`, `automated_effect`). Turn comum
mantém somente `reply`; handoff não vazio fixa reply antes da transition na
mesma UoW; `[TRANSFERIR]` sem texto fixa somente transition, sem gap. Linhas
ticket05 existentes são classificadas como `handoff_transition` pelo vínculo
imutável com `scheduled_handoffs`, nunca por payload/status, sem criar reply
retroativo nem alterar `stream_sequence`. Na era ticket06 (coluna presente)
os predecessores (`uq_outbox_turn_revision`, `ck_outbox_delivery_authority`)
nunca são recriados; sucessor ausente/divergente falha fechado com erro
nomeado. Todo validador de shape trata resultado `NULL` (objeto ausente) como
divergência. FKs de autoridade do scheduler (`fk_outbox_*_authority`,
`fk_outbox_conversation_turn`, `fk_outbox_turn_revision`,
`fk_turn_current_revision`, `fk_turn_final_revision`) têm shape exato
validado em toda aplicação (`scheduler authority foreign key shape requires
audited repair`). Depois da primeira execução completa (marcador:
`trg_outbox_authority_immutable_v3` existe), os blocos legados nunca recriam
essas FKs: drop de uma, de um líder de grupo ou de todas falha com o erro
nomeado.

Review round 1 (ticket07): `uq_generation_consumo_link` garante no máximo um
attempt por `consumo_llm_id` não nulo, com shape exato validado. Dados legados
com vínculo duplicado falham fechados; nenhuma linha é reescrita. Vínculo passa
pelo helper compartilhado de accounting/reconciliação, nunca por wildcard de
provider ID nulo. CHECKs novos usam introducers explícitos, preservando digests
legados independentemente do charset do cliente (latin1/utf8mb4/utf8mb3).

Ticket 07 adiciona o guard `trg_conversation_turn_lifecycle_v1` (BEFORE
UPDATE em `conversation_turns`, sem predecessor; digest validado com os
demais guards versionados, boundary
`ticket07-trigger-boundary: conversation-turn-lifecycle-v1-created`). Nenhuma
coluna ou FK nova; round 1 acrescenta o índice único descrito acima. Turn terminal é imutável no banco; ver §10.15.

S1 adiciona `scheduler_retention_progress` (folha sem FK/CHECK/trigger),
shape exato validado, divergência → `sec-s1 retention … requires audited repair`.
Migration não reescreve checkpoint existente. Após criar/validar/seed,
`schema_version=3` (2→3; futuro não rebaixado). Binário 2 contra DB 3 recusa
compatibilidade; binário 3 requer tabela nova. Boundaries
`sec-s1-schema-boundary: progress-created`, `progress-validated`,
`progress-seeded`, `schema-version-recorded`.

R2-S1 adiciona `idx_turn_retention_terminal (session_slot, terminal_at)` em
`conversation_turns`, dentro do schema 3 ainda não lançado. Migration online
`ALGORITHM=INPLACE, LOCK=NONE`; shape exato (ordem, colunas, unicidade,
collation, prefixo, tipo, visibilidade, expressão). Divergência →
`sec2-s1 retention index shape requires audited repair`. Sem nova FK/guard.
Boundaries `sec2-s1-schema-boundary: index-created`, `index-validated`.

T08-3 adiciona `replica_heartbeats` (folha, sem FK nem trigger): uma linha por
processo `app`/`delivery_worker`/`turn_worker` com `code_version`,
`schema_version`, mode/epoch delivery, flag/epoch scheduler, flags processor/
dispatch, modo de auth, `started_at`, `last_seen_at` e `stopped_at`. Colunas de
configuração não têm default (writer que omite valor falha, nunca vira
compatível). Shape exato validado (colunas com ordinal, tipo, nulabilidade,
default, charset/collation e extra; índices; CHECK com digest, literais com
introducer `_ascii` — forma estável quando um ALTER re-serializa o CHECK; zero FK/trigger)
com erros nomeados `t083 replica heartbeat … requires audited repair`, sem
reescrita. Depois da validação, marcador monotônico
`UPDATE scheduler_runtime_state SET schema_version = 2 … AND schema_version = 1`
(schema_version=2; valor maior nunca é rebaixado), validado em seguida.
Boundaries: `t083-schema-boundary: replica-heartbeats-created`,
`…: replica-heartbeats-validated`, `…: schema-version-recorded`. Nada é
dropado. Retenção: 90 dias por PK, discovery limitada (§10.10).

---

## 6. OpenRouter e Medição de Uso

### 6.1 Cliente LLM centralizado

`app/src/services/llm_client.py` já é o único cliente OpenRouter. `agent_runner.py` monta contexto/normaliza resposta, sem instanciar provider.

- `_client()` cria `AsyncOpenAI`, timeout `30.0` e `max_retries=0` no SDK.
- `completar()` é compatibilidade legado: até duas tentativas no wrapper para timeout/conexão/rate limit; isso é diferente do retry do SDK.
- `completar_tentativa_unica()` faz uma chamada para attempts fenced. Resultado ambíguo vira `LLMProviderError`/`outcome_unknown`, nunca retry automático do attempt.
- `LLMResult` contém texto, modelo solicitado/reportado, tokens, custo `Decimal` e ID de geração. Fonte exata dos campos e parsing é o código, não pseudocódigo duplicado aqui.
- Não logar conteúdo integral, segredos ou dados pessoais. Não adicionar retry ao caminho fenced sem revisão do risco de gasto duplicado.

### 6.2 Persistência de consumo

`processar()` e `gerar_resumo()` usam `_registrar_consumo_seguro()` e `consumo_repo.registrar_ou_reconciliar()`.

1. Uso conhecido deve resultar em ledger verificado ou reconciliação durável, conforme os contratos do repositório.
2. Falha de persistência não pode ser absorvida como simples observabilidade: `UsagePersistenceInvariantError` impede sucesso sem destino verificável do uso.
3. `modelo_llm` é o ID **solicitado**; `modelo_llm_reportado` preserva o eco exato. Nada de match aproximado ou reescrita do histórico.
4. Em fluxo não agendado, tudo permanece na UoW do webhook: rollback/crash pós-LLM pode perder ledger/reconciliação. Outbox sozinho não fecha essa janela.
5. Scheduler grava attempt antes do provider e finaliza uso/unknown sob fence; accounting-only preserva fatos pagos mesmo quando a resposta perde autoridade.

Nunca calcular consumo comercial por `total_mensagens`. Resumo de handoff também consome LLM. Lacunas legado e decisões de correção → [estado e roadmap](docs/mapa-do-projeto/estado-e-roadmap.md).

### 6.3 Limite mensal: duas camadas

| Camada | Fonte | Semântica |
| --- | --- | --- |
| Hard limit | Conta/chave OpenRouter, definido pelo operador | Limite externo; precisa de configuração/validação própria |
| Soft limit | `agente.limite_mensal_creditos`, uso e pendências conhecidos | Bloqueia novas chamadas após atingir o valor observado; não é reserva estrita de orçamento concorrente |

Antes de resposta/resumo, consultar limite pelo caminho do respectivo fluxo. `NULL` significa sem soft limit no app; não significa custo zero. Mensagem ao lead é neutra, sem informar crédito/plano/falha de cobrança e sem prometer um handoff que não foi iniciado. Handoff já decidido usa resumo fallback quando necessário.

### 6.4 Catálogo comercial de modelos

Fonte atual → `MODELOS_PERMITIDOS` em `app/src/routers/agentes.py`. POST/PUT não aceitam catálogo arbitrário. Modelo por agente e franquia não autorizam o lead a escolher provider ou custo.

Tiers e planos comerciais não são tabelas de billing implementadas. Não hardcodar preço de tokens para cobrança: `custo_creditos` reportado e a política comercial são fatos diferentes. Mudança da allowlist exige testes de cadastro/atualização e validação do fluxo LLM.

---

## 7. API Endpoints

Auth de operador não existe no app. O bloqueio Nginx é a fronteira atual; acesso direto pela rede Docker continua privilegiado e não deve ser publicado por outro proxy. Readiness de corpo mínimo não implica deadline total, que ainda é pendência.

F0-T4: allowlist pública exata do app nos dois Nginx: `GET /`,
`GET /health`, `POST /webhook`, `POST /chatwoot/webhook`.
HEAD também permitido em `/` e `/health`; demais métodos diferentes → 405; qualquer outra rota → 404 sem upstream.
HTTP de produção redireciona somente essas rotas (método validado antes);
Localização ACME em `/.well-known/acme-challenge/` existe na porta 80,
mas Compose não monta webroot: provisionamento ACME exige mount/configuração
operacional separada. Somente o harness de teste monta webroot sintético.
CRUD de agentes/sessões é **interno**, somente `app:5000` na rede
Docker. Nunca publicar porta do app; auth/RBAC permanece fora deste release.


### Webhook (Evolution API)

| Método | Rota | Descrição |
|--------|------|-----------|
| `POST` | `/webhook` | Recebe eventos `messages.upsert` da Evolution API |
| `GET` | `/` | **FU-4:** liveness básico, somente `{"status":"online"}`; sem service/version, externo e interno iguais |
| `GET` | `/health` | **FU-4:** público, somente `{"status":"healthy"}`/`{"status":"unhealthy"}` e HTTP 200/503 |
| `GET` | `/internal/health` | **FU-4:** diagnóstico completo anterior, acesso direto `app:5000` na rede Docker; negado pelo Nginx |
| `GET` | `/health/provider-auth` | **FU-4:** snapshot diagnóstico interno, URL direta preservada; negado pelo Nginx |
| `GET` | `/docs`, `/docs/oauth2-redirect`, `/redoc`, `/openapi.json` | **FU-4:** docs/schema FastAPI só via `app:5000` na rede Docker; Nginx nega prefixos `/docs`, `/redoc`, `/openapi.json` (slash final e encoding normalizado inclusos) |

### Agentes — interno (`app:5000`; Nginx 404)

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/agentes` | Lista todos os agentes |
| `POST` | `/agentes` | Cadastra agente; valida `modelo_llm` conforme allowlist após v4.1 |
| `GET` | `/agentes/{id}` | Detalhes de um agente |
| `PUT` | `/agentes/{id}` | Atualiza prompt, modelo, `chatwoot_inbox_id`, limite LLM etc. |
| `DELETE` | `/agentes/{id}` | Remove agente |
| `GET` | `/agentes/{id}/consumo` | Implementado — consumo LLM por período, tokens, custo e percentual da franquia |

### Sessões — interno (`app:5000`; Nginx 404)

| Método | Rota | Descrição |
|--------|------|-----------|
| `GET` | `/sessoes` | Lista sessões (filtros: `agente_id`, `handler`) |
| `GET` | `/sessoes/{telefone}` | Detalhes de uma sessão |
| `POST` | `/sessoes/{telefone}/transferir` | Encerra a sessão manualmente (`motivo='transferencia_manual'`) |
| `POST` | `/sessoes/{telefone}/retomar` | Retoma atendimento pelo bot |
| `DELETE` | `/sessoes/{telefone}` | Deleta sessão |
| `GET` | `/sessoes/{telefone}/historico` | Histórico de mensagens |

### Chatwoot

| Método | Rota | Descrição |
|--------|------|-----------|
| `POST` | `/chatwoot/webhook` | Recebe webhooks do Chatwoot e encaminha respostas de atendentes ao lead via Evolution |

---

## 8. Stack Oficial (runtime)

| Componente | Versão declarada nesta base F0 |
| --- | --- |
| Python | `3.12-slim`, digest linux/amd64 pinado no Dockerfile |
| FastAPI / Uvicorn | `0.109.0` / `0.27.0` |
| Pydantic / pydantic-settings | `2.5.3` / `2.1.0` |
| SQLAlchemy async / aiomysql | `2.0.52` / `0.2.0` |
| SDK `openai` | `1.54.0`, usado para OpenRouter |
| HTTP / logging | `httpx==0.26.0`, `requests==2.31.0`, `loguru==0.7.2` |
| Dependências auxiliares | `cryptography==42.0.8`, `python-dotenv==1.0.0` |
| Banco do bot | MySQL 8.0, não PostgreSQL |
| Providers | Evolution API/Chatwoot self-hosted, Postgres/Redis próprios |
| Orquestração / borda | Docker Engine + Compose / Nginx |

Números descrevem `requirements.in`/lock/Compose, não containers inspecionados ou recomendação de upgrade.

### Dependências reproduzíveis

`app/requirements.in` fixa as 12 dependências diretas. `app/requirements.lock`
é o contrato de instalação (transitivas e hashes); `app/requirements.txt` é
somente alias com `--require-hashes -r requirements.lock`.
Gerar com Python 3.12 linux/amd64 e `pip-tools==7.5.1`:

```bash
pip-compile --generate-hashes --output-file=requirements.lock requirements.in
```

Base `python:3.12-slim` fixada por digest linux/amd64 no Dockerfile.
Todas as dependências têm wheels para essa plataforma; sem camada apt.
Install usa `--only-binary=:all: --require-hashes`: sem fallback para sdist
nem execução de setup/build de terceiros. Hashes de sdist gerados pelo
pip-compile permanecem no lock, mas não são elegíveis na imagem.
Compose constrói `${COMPOSE_PROJECT_NAME}-app:${DELIVERY_WORKER_CODE_VERSION}`
(nunca tag global do host). Build exige `--build-arg DELIVERY_WORKER_CODE_VERSION=<versão>`; arquivo
`/etc/wpp-app/IMAGE_CODE_VERSION` e label OCI `org.opencontainers.image.revision`
gravados juntos, fora do bind dev `/app`. Seis serviços Compose forçam
`WPP_REQUIRE_IMAGE_VERSION=true`: arquivo ausente/ilegível ou versão divergente
do env recusa startup com erro nomeado/exit 2, antes de I/O de negócio.
Fingerprints publicam versão baked. Execução fonte sem arquivo/flag usa env com
WARN e contador `image_version_source_fallback` (threshold=1), nunca silencioso.
Override dev injeta `WPP_DEV_BIND_CODE=true` nos seis serviços: WARN + contador
`dev_bind_code_unattested` (threshold=1, uma vez por processo), sem bloquear.
Código montado pode divergir da versão baked: fingerprint não atesta fonte dev.
Atualização do lock/base exige repetir clean builds e verificar os hashes.
`app/.dockerignore` exclui testes, `.env*`, bytecode e logs da imagem.

---

## 9. Variáveis de Ambiente

O `.env` da raiz fornece interpolação Compose e `env_file` dos serviços que o declaram; Chatwoot recebe o anchor selecionado. Não presumir que toda variável chega a todos os containers. Nunca versionar chaves, tokens ou senhas. Exemplos abaixo são públicos, não configuração pronta.
Toda interpolação `${VAR}` do Compose base/override existe nos dois templates;
`tests.test_release_config` verifica paridade (inclusive variáveis opcionais).
Configuração operacional adicional dos dois templates:

```env
COMPOSE_PROJECT_NAME=wpp-ai-bot-CHANGE-ME
NGINX_BIND_ADDRESS=127.0.0.1
NGINX_HTTP_PORT=80
NGINX_HTTPS_PORT=443
NGINX_CONF_PATH=./nginx/nginx.dev.conf
CHATWOOT_DB_PASSWORD=replace-chatwoot-db-password
CHATWOOT_SECRET_KEY=replace-chatwoot-secret-key-base
CHATWOOT_FRONTEND_URL=http://chatwoot.localhost
CHATWOOT_MAILER_EMAIL=notifications@example.invalid
SMTP_ADDRESS=smtp.example.invalid
SMTP_PORT=587
SMTP_USERNAME=replace-smtp-username
SMTP_PASSWORD=replace-smtp-password
```

Preflight somente leitura: `scripts/release_preflight.sh <arquivo.env>
--expected-project <projeto> --phase legacy|outbox [--check-volumes]`.
Exige Python 3 + Git; não executa/sourcing do arquivo, não faz rede nem inicia
containers. Consulta Docker local para `compose config` e `image inspect`;
`--check-volumes` acrescenta `volume inspect` dos seis nomes exatos. Emite nomes de chaves/motivos fixos, nunca valores.
Lista obrigatória explícita em `scripts/release_preflight.py`; auth pode estar
vazia apenas em provisioning. SMTP é tudo-ou-nada: quatro SMTP_* vazios → aviso `smtp_disabled`;
parcialmente preenchidos → falha; `--require-smtp` exige os quatro (VPS).
Segredos conhecidos dos templates, `postgres`, `sua-*` e `replace-*` são recusados.
Preflight valida configuração Compose efetiva com ambiente do shell aplicado,
interpolação e env_file do candidato via overlay somente leitura (nunca lê .env
real para validar um candidato .env.new). Override de shell divergente é recusado
somente nas chaves interpoladas pelo Compose/override/candidato; chaves usadas
apenas por env_file não são sobrescritas pelo shell (DEBUG=1/LOG_LEVEL=debug
exportados não substituem o candidato). Diagnóstico só contém nomes/reasons.
Qualquer COMPOSE_* no candidato ou shell é proibida, inclusive futura/desconhecida,
exceto COMPOSE_PROJECT_NAME e allowlist só de apresentação/desempenho:
COMPOSE_ANSI, COMPOSE_STATUS_STDOUT, COMPOSE_PROGRESS, COMPOSE_MENU,
COMPOSE_HTTP_TIMEOUT, COMPOSE_PARALLEL_LIMIT. COMPOSE_FILE, COMPOSE_PROFILES,
COMPOSE_PATH_SEPARATOR e COMPOSE_ENV_FILES nunca são aceitas, nem vazias.
Preflight compara config base sem profiles ao deploy sem -f, com o mesmo ambiente,
em snapshot que contém somente arquivos Compose e .env=candidato; nunca copia
.env operacional. No snapshot, cwd e --project-directory apontam ao mesmo local;
--env-file <snapshot>/.env equivale à descoberta implícita sem --env-file.
Prova com Compose real compara JSON completo das duas execuções e rejeita
mutação que aponta --env-file a outro candidato. Equivalência depende de .env
idêntico ao candidato e de COMPOSE_FILE/COMPOSE_ENV_FILES/COMPOSE_PROFILES bloqueados.
Todos os nomes default/override descobertos pelo Compose são
considerados. Override automático presente → automatic_override_forbidden;
modelo diferente → deployment_configuration_mismatch. Guards dos seis serviços
usam separadamente config --profile '*'. --compose-file não pode alterar o
modelo release em relação ao deploy default, inclusive override dev.
Preflight também exige índice sem flags h/S/minúsculas e sparse-checkout desativado,
com mounts Python somente app_logs:/app/logs conforme contrato R5 no §17.
Preflight exige os seis serviços Python em `<projeto efetivo>-app:<versão>`,
imagem local existente e label OCI correspondente à versão.
`AUTHENTICATION_API_KEY` é obrigatória, não placeholder e igual a `EVOLUTION_API_KEY`.
Credenciais de `DATABASE_CONNECTION_URI` devem coincidir com POSTGRES_*;
usuário/senha/database de `DATABASE_URL` devem coincidir com MYSQL_*.
MySQL usa semântica de SQLAlchemy make_url (oracle da versão pinada): senha
com / ou # crus é aceita; @ cru só passa se o app realmente o interpretar igual.
Username/senha percent-encoded são decodificados; database segue make_url.
Postgres usa parsing urllib de URI, com credenciais/database percent-decoded.
Referência Docker completa é validada (separadores de nome, comprimento e tag);
projetos aceitos pelo Compose mas inválidos para a imagem são recusados.
Certificados TLS exigem ambos `fullchain.pem` e `privkey.pem`, não arquivo qualquer.
Legacy exige direct/dispatch false/epoch vazio; outbox exige enforced/epoch UUID
(e aceita dispatch false ou true). Scheduler/processor permanecem false.
`DELIVERY_WORKER_CODE_VERSION` deve ser SHA curto do HEAD para release, sem
alterar default de compatibilidade `outbox-v1`. DEBUG e OPENAI_ENABLED ausentes
ou false. Callback privado requer SAFE_FETCH true; provisioning requer bind
127.0.0.1; sem certificados requer nginx.dev.conf. Valores dos templates não
constituem configuração de release pronta (SafeFetch false permanece default).


```env
# Banco da aplicação
MYSQL_DATABASE=wpp_bot
DATABASE_URL=mysql+aiomysql://wpp_user:senha@db:3306/wpp_bot

# OpenRouter — runtime LLM v4.1
OPENROUTER_API_KEY=sk-or-...
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1

# Evolution API
EVOLUTION_API_URL=http://evolution:8080
EVOLUTION_API_KEY=...
EVOLUTION_WEBHOOK_BEARER_TOKEN=...
INBOUND_RECEIPT_HMAC_CURRENT_VERSION=2
INBOUND_RECEIPT_HMAC_KEYS={"1":"segredo-anterior","2":"segredo-atual"}

# Entrega durável — dispatch desligado até cutover auditado
DELIVERY_MODE=direct
DELIVERY_DISPATCH_ENABLED=false
DELIVERY_CUTOVER_EPOCH=
DELIVERY_WORKER_CONCURRENCY=8
DELIVERY_CLAIM_MAX_ATTEMPTS=4
DELIVERY_CLAIM_RETRY_JITTER_SECONDS=0.05
DELIVERY_POLL_INTERVAL_SECONDS=1
DELIVERY_LEASE_RENEW_INTERVAL_SECONDS=30
DELIVERY_EVOLUTION_LEASE_SECONDS=90
DELIVERY_CHATWOOT_LEASE_SECONDS=120
DELIVERY_EVOLUTION_DEADLINE_SECONDS=60
DELIVERY_CHATWOOT_DEADLINE_SECONDS=300
DELIVERY_TRANSITION_TTL_SECONDS=1800
DELIVERY_AUTOMATED_TTL_SECONDS=1800
DELIVERY_HUMAN_REPLY_TTL_SECONDS=86400
DELIVERY_HANDOFF_SETUP_TTL_SECONDS=86400
DELIVERY_SHUTDOWN_GRACE_SECONDS=30
DELIVERY_SHUTDOWN_ABORT_SECONDS=5
DELIVERY_HEARTBEAT_INTERVAL_SECONDS=15
DELIVERY_READINESS_INTERVAL_SECONDS=60
DELIVERY_MAINTENANCE_INTERVAL_SECONDS=15
DELIVERY_MAINTENANCE_SKIP_ERROR_THRESHOLD=3
DELIVERY_RETENTION_INTERVAL_SECONDS=3600
# S1: budgets por passada; SLA = horizonte + tolerância (segundos)
SCHEDULER_RETENTION_MAX_ROWS=10000
SCHEDULER_RETENTION_MAX_SECONDS=30
SCHEDULER_RETENTION_SLA_TOLERANCE_SECONDS=86400
# FU-1: retention watchdog (30s margin, 2 intervals without completion, 2d sweep)
DELIVERY_RETENTION_OVERRUN_MARGIN_SECONDS=30
DELIVERY_RETENTION_COMPLETION_MAX_INTERVALS=2
SCHEDULER_RETENTION_SWEEP_MAX_AGE_SECONDS=172800
DELIVERY_USAGE_RECONCILIATION_BATCH=100
DELIVERY_WORKER_HEALTH_PATH=/tmp/delivery-worker-health.json
DELIVERY_WORKER_CODE_VERSION=outbox-v1
HEALTH_OBSERVABILITY_CACHE_SECONDS=5

# Turns por inbox — scheduler e processor desligados até rollout explícito
INBOX_SCHEDULER_ENABLED=false
INBOX_SCHEDULER_EPOCH=
INBOX_TURN_PROCESSOR_ENABLED=false
INBOX_TURN_PROCESSOR_CONCURRENCY=4
INBOX_TURN_LEASE_SECONDS=30
INBOX_TURN_POLL_INTERVAL_SECONDS=0.5
INBOX_GENERATION_TIMEOUT_SECONDS=45
INBOX_TYPING_JITTER_MS=400
INBOX_TURN_RELEASE_BACKOFF_SECONDS=5
INBOX_TURN_WORKER_HEALTH_PATH=/tmp/turn-worker-health.json

# Snapshot local de readiness dos providers
PROVIDER_AUTH_READINESS_PATH=/app/logs/provider-auth-readiness.json

# Chatwoot
CHATWOOT_BASE_URL=http://chatwoot:3000
CHATWOOT_API_TOKEN=...
CHATWOOT_ACCOUNT_ID=1
CHATWOOT_DB_PASSWORD=...
CHATWOOT_SECRET_KEY=...
CHATWOOT_FRONTEND_URL=https://chatwoot.seudominio.com.br
CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK=true
CHATWOOT_MAILER_EMAIL=...
SMTP_ADDRESS=...
SMTP_PORT=587
SMTP_USERNAME=...
SMTP_PASSWORD=...
```

Contrato de configuração do delivery e scheduler:

- Todos os `float` de intervalo/timeout são finitos; `NaN`, `+inf` e `-inf`
  falham no startup.
- `INBOX_TURN_POLL_INTERVAL_SECONDS < INBOX_TURN_LEASE_SECONDS / 3`, pois
  `TurnProcessor._renew_loop` renova a lane a cada terço da lease.
- `DELIVERY_LEASE_RENEW_INTERVAL_SECONDS` é menor que as leases Evolution e
  Chatwoot. A relação deadline + 5s vale somente para
  `DELIVERY_EVOLUTION_LEASE_SECONDS` quando dispatch ou scheduler está ligado
  (§10.14/X4). Jobs Chatwoot são `provider_setup_context`, usam renovação e não
  exigem lease maior que o deadline total.
- `DELIVERY_WORKER_CODE_VERSION` identifica o build de todas as réplicas
  (app, delivery-worker, turn-worker; T08-3) e deve mudar a cada deploy de
  código; divergência entre réplicas frescas falha readiness.
- `DELIVERY_WORKER_CODE_VERSION` aceita 1–100 caracteres ASCII seguros:
  letra/dígito inicial e depois somente letras, dígitos, `.`, `_` ou `-`.
- `HEALTH_OBSERVABILITY_CACHE_SECONDS` é finito e fica entre 5–10 segundos.
  No `/health` público, os blocos scheduler e réplicas usam cache single-flight:
  requests concorrentes compartilham uma coleta e nenhum resultado healthy é
  servido depois do TTL sem nova coleta.
  R2-S4: cancelamento de waiter HTTP não cancela nem remove a carga
  compartilhada. A própria task publica resultado e libera o flight, inclusive
  sem waiters. Falhas normalizadas ficam indisponíveis pelo TTL; exceção
  inesperada também é cacheada e relançada, nunca substituída por healthy
  vencido. Shutdown do app cancela concorrentemente ambas as cargas e o beat;
  marcador stopped só depois da quiescência de possíveis writers (inclusive
  shield/accounting). Cache fechado não admite nova carga no mesmo loop.
- `DELIVERY_WORKER_HEALTH_PATH`, `INBOX_TURN_WORKER_HEALTH_PATH` e
  `PROVIDER_AUTH_READINESS_PATH` são paths absolutos locais ao container.

Caps duráveis pertencem ao singleton `scheduler_runtime_state` no MySQL, não ao
`.env`. `accounting_drain_seconds` também vive somente no DB; não participa da
validação de startup de `Settings`. Defaults e bounds abaixo devem permanecer em
paridade com os `CHECK`s de `db/init.sql`:

| Campo | Default | Bound DB |
| --- | --- | --- |
| `max_pending_inputs_per_session` | `128` | `1..10000` |
| `max_pending_input_bytes_per_session` | `262144` | `1..16777215` |
| `max_queued_turns_per_agent` | `1000` | `1..100000` |
| `max_queued_bytes_per_agent` | `16777216` | `1..1073741824` |
| `max_global_queued_turns` | `10000` | `1..1000000` |
| `max_global_queued_bytes` | `268435456` | `1..17179869184` |
| `max_generation_attempts_per_turn` | `2` | `1..8` |
| `max_delivery_attempts_per_turn` | `8` | `1..32` |
| `max_refreshes_per_turn` | `1` | `=1` |
| `accounting_drain_seconds` | `30` | `1..300` |

**Atenção crítica:** dentro da rede Docker, os containers se comunicam pelo nome do serviço. Use `http://evolution:8080`, `http://chatwoot:3000` e `@db:3306`, nunca `localhost`.

`PROVIDER_CALLBACK_BASE_URL=http://app:5000` exige
`CHATWOOT_SAFE_FETCH_ALLOW_PRIVATE_NETWORK=true`, pois o Chatwoot entrega o
webhook via `SafeFetch`. Essa opção libera destinos privados globalmente no
Chatwoot; somente administradores operacionais confiáveis podem criar webhooks
na instalação. Use callback HTTPS público e mantenha a opção falsa quando
esse trust boundary não for aceitável.

### 9.1 Chaves por cliente

O modelo operacional preferido é **uma chave OpenRouter por cliente/instalação**, armazenada apenas no `.env` da VPS correspondente. Isso permite hard limit por chave e reforça o isolamento financeiro entre clientes.

Não armazenar chaves OpenRouter no banco. Não retornar chaves em endpoints, logs, exceções ou schemas de resposta.

### 9.2 Chaves HMAC dos receipts

- `INBOUND_RECEIPT_HMAC_KEYS` é um objeto JSON versionado; valores são segredos
  independentes do bearer da Evolution.
- `INBOUND_RECEIPT_HMAC_CURRENT_VERSION` identifica a chave usada por novos
  receipts.
- Templates distribuem `INBOUND_RECEIPT_HMAC_KEYS={}`; `provider-auth prepare`
  gera a primeira chave privada. Placeholders públicos historicamente
  distribuídos são inválidos e nunca são promovidos a chave real.
- Rotação adiciona a nova versão e mantém todas as versões anteriores durante
  pelo menos os 90 dias de retenção de `inbound_events`.
- Remover uma chave antes do fim do horizonte impede validar replay de receipt
  antigo e deve ser tratado como falha operacional, nunca como evento novo.
- Nunca imprimir o mapa, seus valores, o bearer ou `payload_hmac` em log.

---

### FU-1 — observabilidade da retenção

- `DELIVERY_RETENTION_OVERRUN_MARGIN_SECONDS=30`: finito, 6..3600s, inclui
  etapas legadas/checkpoint/health além do budget scheduler.
- `DELIVERY_RETENTION_COMPLETION_MAX_INTERVALS=2`: inteiro, 2..24.
  `intervalo × N > budget + margem`; ausência de conclusão além de N
  intervalos → ERROR contador/limiar 1 por episódio.
- `SCHEDULER_RETENTION_SWEEP_MAX_AGE_SECONDS=172800`: inteiro, 60..604799;
  maior que intervalo de retenção; limite + tolerância SLA <7 dias.
  Nenhum sweep completo além desse limite → ERROR por episódio.
- Snapshot local: `retention_task` com state `running|idle|failed`, started_at,
  last_completed_at, running_seconds, contadores/limiar 1. Passada com idade
  >budget+margem → ERROR por episódio. Dispatch não aguarda watchdog/task.
- Task atrasada/falha → `scheduler_retention_health.stale=true`,
  `available=false`; valores históricos não são observação atual. Recuperação
  após conclusão boa remove stale e rearma episódios.
- Draft SLA deriva de probes limitados aos turn_ids das páginas já percorridas
  (até 1000 por query), índices de revisão por turn_id e turn PK. Sem DDL.
  Mínimo acumulado O(1) por sweep, continuidade cursor+version. Não sobrevive
  a restart; quebra de continuidade reinicia cobertura.
- Cobertura parcial esperada → draft_available=false,
  reason=coverage_incomplete, scan_started_at e posição cursor/version,
  sem ERROR/contador de falha nem impacto em readiness. Evidência positiva de
  draft acima do SLA sinaliza imediatamente, mesmo com cobertura parcial.
- Último sweep completo: timestamp/idade expostos; inexistente usa idade desde
  início da observação local para watchdog. Falha real de medição →
  available=false + ERROR, nunca zero. Nenhum draft medido é null, não idade 0.

### T08-2/FU-1 — P3-1 taxonomia de falhas da retenção

- Falha de medição/etapa: `health_unavailable`, `draft_probe_failed`,
  `checkpoint_unavailable`, `checkpoint_failed`, `discovery_failed`, exceção
  da etapa legada/scheduler/réplicas ou falha do purge de réplicas. Impede
  conclusão boa: task `failed`, SLA `available=false`/`stale=true` no snapshot.
- Falha/retenção por candidato: `candidate_error`, `foreign_key_reference`,
  `transaction_lost` após retry da UoW inteira e motivos de retenção sinalizada
  (`accounting_identity`, `accounting_evidence`, `unsent_effect`,
  `lane_reference`, `delivery_reference`, `recent_accounting`, `recent_identity`,
  `receipt_identity`, `receipt_reference`, `context_reference`, `nonterminal`).
  Contador próprio + ERROR limiar1 preservados; rotação continua. Não incrementa
  `measurement_failure_count`, não impede task `idle`/`last_completed_at` nem
  torna medição atual stale. SLA positivo continua independente desses erros.
- `candidate_failures` no health local: contadores cumulativos por motivo,
  resumo da última passada e último resumo não vazio (`last_failure_observed_at`,
  count, motivos, `oldest_terminal_at`/idade). Idade usa terminal_at descoberto,
  somente dos candidatos observados, não mínimo global. Uma passada parcial
  vazia não apaga sinal; só sweep completo sem falha de candidato e sem falha
  de medição limpa último resumo não vazio. Continuidade cursor+version exigida.
  Estado O(1) por processo, sem coleção de IDs; perde-se em restart.
- `nonterminal` observado é contabilizado localmente mesmo quando outra réplica
  vence version/CAS ou a passada falha/é cancelada. Cumulativo e um único ERROR
  agregado por passada são finalizados exatamente uma vez em todas as saídas;
  nunca um ERROR por linha. Não altera checkpoint, coverage nem autoridade SLA.
- Watchdogs usam conclusão/idade da task ou sweep, nunca contadores de candidato.
  `delivery_cli health` mantém readiness independente da retenção; imprime os
  blocos locais `retention_task`/`scheduler_retention_health` para diagnóstico,
  sem reclassificar falha de candidato como indisponibilidade.

### R2-S1 — retenção e frescor

`SCHEDULER_RETENTION_MAX_SECONDS` limita task independente; não compete com
cadência de heartbeat. Config exige
`DELIVERY_HEARTBEAT_INTERVAL_SECONDS + DELIVERY_POLL_INTERVAL_SECONDS < 60`.
Retenção, cancelamento e contadores → §10.15/T08-2.

## 10. Princípios Arquiteturais Obrigatórios

### 10.1 Isolamento por agente

- `instance_name` único por agente — nunca tratado como variável global
- `system_prompt` e `modelo_llm` próprios por agente
- Histórico isolado por `sessao_id`
- Sessão identificada por `(agente_id, telefone)` — constraint única no banco
- `chatwoot_inbox_id` próprio por agente — handoffs vão para o inbox correto
- Consumo LLM atribuído obrigatoriamente a um `agente_id`
- Nenhum contexto, custo ou configuração pode vazar entre agentes/clientes

### 10.2 Acesso ao banco só por repositórios

Toda interação com MySQL passa pela camada `app/src/database/repositories/`. Proibido escrever SQL ou queries ORM diretamente em rotas, handlers ou services.

### 10.3 Handoff encerra a sessão

- Sessão passa para `handler='encerrada'` após handoff — **não** `handler='humano'`
- `chatwoot_conversation_id` é persistido na sessão quando disponível
- O bot não responde em sessões encerradas

### 10.4 Roteamento humano é responsabilidade do Chatwoot

Não implementar round-robin, disponibilidade ou equipes no Python. Toda distribuição de conversas entre atendentes é configurada no Chatwoot.

### 10.5 Falha externa não deve travar o webhook

- Em `DELIVERY_MODE=outbox`, falhas da API do Chatwoot ou Evolution não fazem
  rollback do trabalho local já aceito: o webhook confirma somente depois do
  commit do outbox e o worker aplica retry/expiry/fencing.
- Compatibilidade `direct` existe apenas durante a janela de observação e para
  restauração auditada; nunca é fallback automático de `outbox`/`frozen`.
- Falhas do OpenRouter: registrar erro, não salvar uso fictício e retornar resposta controlada sem expor detalhes técnicos ao lead

### 10.6 A fonte comercial é o uso real

Para controle de consumo e eventual faturamento:

- `consumo_llm.custo_creditos` é a referência de custo técnico
- Delete de agente com ledger ou handoff é recusado; `consumo_llm` e `handoff_attempts` nunca são apagados por delete via API (§10.15 S3)
- `tokens_input` e `tokens_output` são dados auxiliares de auditoria e análise
- Nunca faturar pelo número de mensagens sem considerar custo real por modelo
- Nunca depender de preço de modelo hardcoded para fechar faturamento

### 10.7 Privacidade dos logs HTTP

- **FU-4:** `/health` público tem allowlist exata de corpo: somente `status`
  (`healthy`/`unhealthy`), HTTP 200/503. `/internal/health` conserva o corpo
  detalhado anterior (inclusive `degraded`), mesma lógica e cache compartilhado.
  F0-T4: ambos os Nginx permitem somente as quatro rotas exatas de §7;
  todo o resto é negado sem upstream, inclusive `/internal/` e `/health/`,
  `/internal/health` e `/health/provider-auth`; diagnóstico só via `app:5000`
  pela rede Docker, sem publicar a porta do app. Os dois paths têm entradas
  estáticas no mapa `privacy_safe`. Docs/OpenAPI (`/docs*`, `/redoc*`,
  `/openapi.json*`) também negados externamente; formas fixas `/docs`, `/docs/`,
  `/docs/oauth2-redirect`, `/redoc`, `/redoc/`, `/openapi.json`, `/openapi.json/`
  no `privacy_safe`. `GET /` responde só `{"status":"online"}`.

- Manter o access logging ativo, mas nunca registrar telefones completos.
- O Uvicorn deve usar `utils/access_log.py` para mascarar telefones no request
  target sem alterar o ASGI scope ou o roteamento.
- O Chatwoot deve carregar o initializer versionado
  `chatwoot/config/initializers/wpp_phone_log_redaction.rb`, montado read-only
  pelo Compose, e estender os filtros Rails existentes em vez de substituí-los.
  O mesmo initializer filtra conteúdo/credenciais e desabilita log de argumentos
  Active Job, porque `WebhookJob` carrega payload integral e signing secret.
- Ambos os arquivos Nginx devem manter access logging ativo com formato
  `privacy_safe`: o path logado vem exclusivamente de um mapa de formatos
  estáticos allowlisted. Segmentos de telefone e IDs numéricos usam marcadores
  fixos; qualquer path desconhecido vira `[REDACTED_PATH]`. Valores sensíveis de
  query também são substituídos por marcador e somente metadados allowlisted são
  preservados. O formato não registra request target bruto, Referer, User-Agent,
  Host ou forwarded headers.
- Qualquer token de dígito percent-encoded (`%3x`, case-insensitive) que ainda
  exista no path após a normalização é rejeitado pelo Nginx com `400`, antes do
  upstream. A regra é intencionalmente independente de tamanho e cobre encoding
  duplo total ou parcial no primeiro, meio ou último dígito. Paths crus ou
  codificados uma vez são avaliados pela allowlist depois da normalização;
  barras finais e maiúsculas não criam permissão adicional.
- O Nginx mantém o error log em nível `crit`, pois erros comuns de upstream
  incluem a request line bruta. O tradeoff é não reter detalhes de erro por
  requisição no error log; status HTTP, tempo total, status e tempos do upstream
  permanecem no access log `privacy_safe` para diagnóstico operacional. O mapa
  fechado também reduz o detalhe de paths desconhecidos em troca de não copiar
  conteúdo arbitrário para logs.
- Preservar método, formato da rota, status, tempo e contexto técnico seguro.
- Loguru (S4/P3-8): todo entrypoint (`main.py`, `delivery_worker.py`,
  `turn_worker.py`, `delivery_cli.py`, `provider_auth_cli.py`) chama
  `utils/logger.setup_logger`, que faz `logger.remove()` e só adiciona handlers
  com `diagnose=False` e `backtrace=False` (stderr; arquivo rotativo só no app).
  Traceback nunca renderiza valores de variáveis locais; stdout fica livre para
  o JSON dos CLIs.

### 10.8 Concorrência e autoridade do delivery worker

- Ordem global de locks para claims, renovação e finalização (gate compartilhado
  nos workers, exclusivo na mudança de modo/epoch):
  `delivery_runtime_state` → `sessoes` → `handoff_attempts` →
  `outbox_deliveries`. Manutenção e comandos operacionais que não bloqueiam o
  runtime começam em `sessoes` e nunca adquirem o gate global depois.
- Claim MySQL repete a transação inteira — gate, `SKIP LOCKED` e fences — após
  `1205`/`1213`, com rollback, sessão nova, tentativas limitadas e jitter.
- Renovação e finalização do worker exigem token, epoch da linha e autoridade
  DB `mode='outbox'` no mesmo epoch. Mudança para `frozen` ou outro epoch
  impede mapeamento Chatwoot e desbloqueio de dependências por dono antigo.
- Perda de autoridade cancela e aguarda I/O local. Efeito remoto já aceito não
  pode ser desfeito: permanece ambíguo para recovery/reconcile. Fencing não
  promete exactly-once remoto nem cancelamento instantâneo antes da próxima
  verificação de lease.
- `SIGTERM`/`SIGINT` fixa um único deadline e interrompe novos claims, mas
  mantém renovação e finalização fenced dos jobs ativos durante o drain.
  Claim já aguardado é cancelado e qualquer resultado posterior é descartado
  sem iniciar provider; linha possivelmente claimada fica para recovery fenced.
  Sinais repetidos não reiniciam a janela. Só perda real de autoridade ou o
  deadline cancela I/O local; efeito remoto sem confirmação permanece ambíguo.
  O default reserva 30s para drain e 5s para cancelamento/await, incluindo
  cleanup de loop, heartbeat final best-effort e dispose, abaixo do
  `stop_grace_period: 40s` do Compose.
- F0-T5: app explicita `stop_grace_period: 10s`; entrypoint
  `python -m src.app_server` configura Uvicorn `timeout_graceful_shutdown=8`.
  Entrypoints `src.app_server`/`src.main` respeitam APP_HOST/APP_PORT
  (`settings.app_host`/`settings.app_port`; default Docker 0.0.0.0:5000).
  DEBUG=true é recusado com `app_server_debug_reload_not_supported` e exit 2:
  reload cria supervisor/filho fora do watchdog/handler single-process.
  Drain HTTP 8s; `APP_CLEANUP_SECONDS=1.5`: quiescência concorrente de caches/beat
  até 9s, marcador até 9.25s, dispose até 9.5s; margem explícita 0.5s.
  Turn-worker usa grace Compose 40s: drain configurado (default 30s), abort
  (default 5s) + excedente disponível (default 4s) para accounting até 39s,
  marcador até 39.25s, dispose até 39.5s. Sem cortar accounting em 32.5s.
  Primeiro SIGTERM/SIGINT arma timer daemon independente do event loop ANTES de
  I/O de bootstrap. Timer permanece armado durante teardown de asyncio.run;
  deadline 39.5s/9.5s → contador `forced_exits` + ERROR threshold=1 e os._exit(75).
  Último log técnico estático usa os.write não bloqueante, sem locks/sinks do
  Loguru; log anterior síncrono já foi emitido. Flush arbitrário bloqueante não
  é aguardado. Perder corrida para SIGKILL equivale ao mesmo crash.
  Cache/beat/discovery param de produzir; tarefas fora dos controllers capturados
  antes de iniciar trabalho são possíveis writers, inclusive shields. Marcador
  só é permitido após quiescência; pendência → sem marcador, `unsafe_stops` +
  ERROR limiar 1 e saída crash. Falha isolada do marcador, sem writer pendente
  e ainda dentro do prazo, gera `marker_failed` + ERROR limiar 1 e saída 0;
  não gera unsafe_stops/forced_exits. Gate final revalida quiescência após
  cleanup da tentativa; unsafe/75 só para pendência ou prazo esgotado.
  Depois do marcador, controllers só fazem dispose/
  encerramento, sem escrita de domínio. Tarefas pendentes no prazo são abandonadas;
  recuperação existente trata attempts started/drain/quarentena/outcome_unknown,
  sem nova resolução. Ausência do marcador usa expiração normal de 60s.
  Direct: cancelar POST /webhook depois de LLM pago e antes de commit pode fazer
  rollback sem consumo_llm/llm_usage_reconciliation. Sinal ASGI
  `direct_shutdown_cancellations` + ERROR limiar 1 indica risco, sem inventar
  accounting. Janela preserva 8s de drain HTTP; crash ainda pode perder ledger.
- Health usa snapshot local por réplica, heartbeat da mesma `worker_id`, UTC do
  MySQL, compatibilidade mode/epoch e readiness do contrato provider. Modo
  direct idle não faz fan-out ao provider. Cada worker conta passes consecutivos
  de manutenção pulados por perda da transação: avisa abaixo do limiar
  `DELIVERY_MAINTENANCE_SKIP_ERROR_THRESHOLD`, escala para `ERROR` e marca seu
  snapshot local como não ready ao atingir o limiar, sem parar claims. Um pass
  concluído reseta o contador. O sinal usa somente estado local/DB, sem novo lock
  global nem fan-out ao provider.

### 10.9 Ingress Evolution e staging do outbox

- Evento acionável exige bearer validado, `instance_name` de agente ativo e
  `data.key.id` não vazio antes de criar efeito durável no modo outbox.
- `main.py` usa `UnitOfWork`; `services/evolution_orchestrator.py` adquire
  autoridade na ordem `delivery_runtime_state` → agente → receipt → sessão.
- Receipt usa HMAC-SHA-256 canônico versionado dos campos acionáveis. Replay
  exato retorna `already_accepted`; mesmo ID com conteúdo alterado retorna
  conflito e nunca repete LLM ou enqueue.
- Auth `enforced` e modo `outbox` exigem keyring de receipt não vazio com a
  versão corrente. Readiness inclui fingerprint secreto-seguro de todas as
  versões retidas; rotação mantém as versões anteriores por pelo menos 90 dias.
- Após `get_or_create_for_update`, handler, histórico, limite mensal, uso LLM e
  sequências são decididos sob o lock da sessão. Histórico usa leitura corrente
  para não reutilizar snapshot anterior à espera do lock no MySQL.
- `delivery_runtime_state` é a autoridade. `frozen`, mode divergente ou epoch
  divergente retorna falha retryable; nunca cai silenciosamente para direct.
- Modo outbox não executa HTTP de entrega. Resposta, soft-limit e erro técnico
  expiram em 30 minutos; setup Chatwoot em 24 horas; transição fica bloqueada
  sem expiry até confirmação do setup.
- Handoff cria attempt, job `chatwoot.open_handoff.v1`, reserva a sequência da
  transição `evolution.send_text.v1` bloqueada e encerra a sessão na mesma
  transação.
- Conteúdo LLM é normalizado antes de persistência/entrega. Resumo vazio usa o
  fallback local e preserva o uso faturável. Resposta apenas `[TRANSFERIR]`
  inicia o handoff sem criar envio vazio; a transição bloqueada ocupa a próxima
  sequência real, sem lacuna. Todo payload staged passa pelo decoder v1 do
  worker antes do enqueue.
- Resposta `delivery_status=queued` só ocorre depois de `commit()` explícito.
  Falha no commit reverte receipt, domínio, uso/reconciliação e outbox e retorna
  status não-2xx.

### 10.10 Chatwoot, cutover e rollback

- **FU-4:** monitor externo usa apenas HTTP/status de `/health`; diagnóstico
  operacional usa `/internal/health` direto no app ou CLIs internas existentes.
  Readiness, fail-closed por estado e TTL single-flight são idênticos nas duas
  superfícies. Nginx nega detalhes; healthchecks Compose por CLI não mudam.

- `/chatwoot/webhook` valida timestamp/HMAC antes do parse, usa o account ID
  configurado como scope, deduplica `message.id`, trava a sessão e enfileira
  `evolution.send_text.v1` com TTL de 24 horas. Nunca entrega direto quando
  `DELIVERY_MODE=outbox`.
- Reply anterior ao commit local do open resolve `wpp_delivery_key` pelo
  `outbox_deliveries.delivery_key` exato e segue o vínculo canônico até o
  attempt. Exige account/inbox/contact/session/generation/operação/payload/epoch
  atuais. Callback nunca revoga lease `processing` ainda válida: retorna falha
  retryable sem commitar receipt. Somente job sem owner ou lease expirada pode
  ser confirmado pelo callback, gravar mapping, desbloquear a transição com TTL
  de 30 minutos e então alocar a sequência maior do reply humano.
- `/retomar` supersede o attempt atual, limpa apenas o mapping espelho e cancela
  ou fenceia trabalho não terminal. Replies de conversas antigas recebem receipt
  e são ignorados como `stale_handoff`; novo handoff usa nova generation/UUID.
- Delivery Evolution permanece bounded at-least-once. Timeout/crash depois da
  aceitação remota pode duplicar mensagem; não declarar exactly-once.
- Payload terminal é scrubbed após 7 dias. Receipts comuns, metadados de
  delivery, heartbeat e auditoria ficam 90 dias. Receipt com disposition
  `scheduled` ou `legacy_unknown`, message link ou membership é audit-retained
  e não participa do purge etário genérico; cleanup terminal integrado deve
  remover membership → associação/work → receipt somente sob autoridade de
  lifecycle comprovada. T08-2 implementa esse purge terminal (§10.15).
  `mensagens` de lead/assistant são preservadas. Dedupe de receipt `scheduled`
  termina após ≥90 dias, como receipts legados e horizonte HMAC; replay posterior
  pode ser aceito como evento novo. Backups não são apagados pelo scrub.
- Readiness pública usa somente DB e snapshots locais: mode/epoch do app e DB,
  dispatch/config/epoch/provider readiness de todos os workers frescos,
  heartbeat e backlog. Não faz fan-out a Evolution/Chatwoot. T08-3: `/health`,
  `delivery_cli health` e `python -m src.turn_worker health` também falham
  fechado (FU-4: motivos `replica_compatibility.reasons` somente no detalhe interno/CLI) quando réplicas frescas (e o
  próprio processo) divergem e o state do scheduler no DB está em
  `enabling|enabled|draining` (§10.11 T08-3).
- Cutover e retorno a direct são comandos container-only auditados. Retorno
  exige `frozen`, dispatch desabilitado, leases assentados, zero não terminal,
  ambiguidades/dead/cancelled explicitamente resolvidos e reconciliação de uso
  zerada. `resolve-rollback` só grava prova sob lock do runtime `frozen` e a
  vincula ao epoch/status/attempt_count/ambiguity_count/terminal_at atuais;
  retry ou nova ambiguidade invalida a prova histórica. Rollback cego ou
  fallback automático é proibido.
- Remoção física da compatibilidade `direct` ocorre somente após janela de
  observação e aprovação de deployment; não confundir implementação/rehearsal
  isolado com ativação operacional.

### 10.11 Autoridade do scheduler de turns

- Ordem global scheduler-aware:
  `delivery_runtime_state` → `scheduler_runtime_state` → `agentes` →
  `agent_service_lanes` → `sessoes` em ordem de ID →
  `conversation_turns`/`turn_revisions` em ordem de ID →
  `handoff_attempts` → `outbox_deliveries`. O singleton
  `scheduler_runtime_state` usa lock exclusivo nas transações curtas do
  scheduler como mutex dos caps globais entre agentes.
- No intake scheduler-aware, locks de lifecycle acima precedem inserção de
  identidade na ordem `inbound_events` → `mensagens` →
  `turn_input_memberships`. O savepoint de unicidade do receipt não libera
  locks InnoDB antes do commit externo. Receipt, mensagem, membership e
  readiness pertencem à mesma UoW; depois de inserir/consultar identity rows,
  a transação não pode voltar e adquirir lane/session/turn. O ingress legado
  ainda usa agente→receipt→sessão enquanto scheduler está desabilitado; ticket
  de cutover deve substituir o branch agendado, não misturar ordens.
- `inbound_events.receipt_disposition` é outcome durável e imutável:
  `scheduled`, `ignored_nonbot`, `legacy_evolution`, `chatwoot` ou
  `legacy_unknown`. Replay nunca infere outcome pelo handler atual nem pela
  presença do link mutável. `scheduled` exige receipt→message→membership exatos;
  `legacy_unknown` fecha por padrão para reparo auditado. Backfill só classifica
  como `scheduled` quando message link/membership prova intake, e `chatwoot`
  pelo provider; todo Evolution ambíguo anterior permanece `legacy_unknown`.
- `generation_attempts` é inserido depois de session/turn/revision locks e antes
  do commit pré-provider. Completion/accounting tardio (owner perdido) toma, em
  ordem canônica, `agentes` FOR SHARE → `sessoes` FOR SHARE → o attempt exato
  FOR UPDATE → `consumo_llm` (insert/lock). Os share locks antecipam os locks de
  FK pai que o insert de consumo tomaria depois do attempt; sem eles o caminho
  inverte a ordem contra o recovery (agente X … attempt) e gera deadlock. Nunca
  toma runtime/scheduler/lane/turn/revision nem faz upgrade S→X. O vínculo só é aceito quando
  generation ID, agente, sessão, modelo **solicitado** (`consumo_llm.modelo_llm`
  = `generation_attempts.modelo_llm`), tipo de chamada, todos os tokens e custo
  normalizado conferem exatamente. Eco datado/variante do provider fica em
  `modelo_llm_reportado` e não afeta o vínculo nem quarentena.
- Claim do outbox existente continua em transação própria e deve commitar antes
  de uma futura reautorização de lane. Nunca adquirir lane mantendo delivery
  bloqueada.
- Toda leitura locked de epoch, lease, turn, revision ou attempt usa estado
  fresco (`populate_existing`); identidade previamente carregada não concede
  autoridade.
- Retry de deadlock repete a transação inteira com sessão nova e limite finito.
  `1205`/`1213`/`1305` (savepoint perdido após rollback total do InnoDB), em
  qualquer ponto da cadeia de causa/contexto, nunca são absorvidos por fallback
  de savepoint (`consumo_repo`, `usage_reconciliation_repo`): são re-lançados
  para o retry da transação inteira; a manutenção do delivery worker descarta a
  passada e repete no próximo intervalo.
- Ranges selados, identidade de membership e classe/links de delivery são
  imutáveis no banco. Replay terminal/resolvido exige igualdade da tupla
  completa: outcome, generation ID, todos os tokens, custo, link de consumo e
  erro seguro; campos omitidos não são wildcards. Outcome terminal de generation
  attempt é imutável salvo resolução auditada `outcome_unknown →
  succeeded|failed` ou anexação única do link comercial exato a um sucesso já
  conhecido. Sucesso não sai da quarentena sem `consumo_llm` correspondente;
  o FK usa `ON DELETE RESTRICT`. A resolução não restaura autoridade de domínio.
  Lease expirado exige recovery separado e nunca é sobrescrito por claim.
- Caps finitos de inputs/bytes por sessão, turns/bytes por agente e turns/bytes
  globais são verificados sob o mutex do scheduler. Replay exato já aceito não
  consome capacidade outra vez.
- Estado permitido: `disabled → enabling → enabled → draining → disabled`.
  Epoch incompatível, outcome de geração desconhecido ou classificação legada
  não terminal desconhecida falha fechado.
- **T08-1 — state/drain:** arestas permitidas exatamente
  `disabled→enabling`, `enabling→enabled|disabled`, `enabled→draining`,
  `draining→disabled`; qualquer outra (inclusive self-transition) é recusada.
  `enabling` permite somente replay, diagnóstico e resolução auditada, nunca
  intake/claim/geração/dispatch novos. `draining` permite claim, geração,
  typing e entrega de trabalho aceito, sem permitir intake novo.
- Replay exato em `draining|disabled|enabling` mantém bearer validado, auth
  enforced, flag local ligada, epochs correspondentes e agente ativo/opt-in.
  Relaxa somente o estado do scheduler, não HMAC nem integridade do grafo.
  Locks lane/session/turn precedem receipt; replay não consulta caps, não gera
  LLM nem enqueue. Receipt novo fora de `enabled` → rollback retryable (503).
- `draining→disabled` e `enabling→disabled` exigem clearance completo:
  `nonterminal_turns=0`, `owned_lanes=0`, `automated_deliveries=0`,
  `legacy_unknown_deliveries=0`, `unresolved_attempts=0`,
  `unreconciled_attempts=0`, `nonterminal_scheduled_handoffs=0`.
  Não terminal = allowlists existentes; attempts unresolved =
  `started|outcome_unknown`; unreconciled = `reconciliation_required` com
  `reconciled_at IS NULL`. Recusa expõe categorias e contagens agregadas,
  contador local e log ERROR com limiar 1. Nunca inferir resolução por timeout.
  Contagens são locking reads correntes sob runtime→scheduler, inclusive
  diante de accounting tardio (que não toma o mutex do scheduler).
- UoW de controle do scheduler (transição) abre a própria transação
  em `READ COMMITTED` (`SET TRANSACTION` como primeiro statement; read view ou
  escrita pendente anterior → recusa `SchedulerConflictError`). Scans de
  clearance usam `FORCE INDEX (PRIMARY)` e predicado estreito: só locks S de
  registro (`REC_NOT_GAP`) nas linhas clustered que bloqueiam, sem gap/next-key
  nem lock em índice secundário (A3). Audit é inserido antes das contagens.
  Inserção/revival de linha bloqueante só ocorre sob o mutex do scheduler;
  accounting tardio/quarentena só altera attempt já contado e travado.
  Writers sem mutex (claim bruto `SKIP LOCKED`, finalização/maintenance legado,
  Chatwoot) nunca esperam o guard por linhas não bloqueantes. **S5:** todo caller
  de produção passa `_delivery_class`; `outbox_repo.enqueue` recusa com
  `OutboxDeliveryClassRequiredError` qualquer enqueue novo que resultaria em
  `legacy_unknown`. Linhas legadas existentes permanecem classificadas e
  continuam bloqueando enablement/drain até reparo auditado.
- **T08-1 — S2:** `scheduler-state show` usa snapshot consistente próprio
  `REPEATABLE READ, READ ONLY`, sem locks de registro/gap/tabela em InnoDB.
  `clearance` mantém as sete categorias do guard; `diagnostic_only=true`,
  `consistency=read_only_snapshot`, `clearance_proven=false` distinguem leitura
  de autorização. Cada categoria é limitada a 100000 linhas e cada SELECT a
  2000ms (mesmos limites de T08-5); `saturated` lista contagens que atingiram
  o limite (limites inferiores, nunca prova de zero pendências).
  Erro/timeout aborta o comando, nunca retorna zero artificial.
  `agent-scheduler show` também usa snapshot `READ ONLY`, sem locks, com
  `diagnostic_only=true` e `consistency=read_only_snapshot`; blockers são
  informativos, nunca autorização de opt-in. Transições/opt-in continuam
  revalidando sob seus locks; nenhum diagnóstico reutiliza a UoW depois.
- `enabling` depende de duas barreiras para efeitos `automated`: (1) o claim
  bruto do outbox (tickets 06–07, inalterado) é inalcançável, pois `enabling`
  só nasce de `disabled` com clearance completo (zero `automated` não terminal
  e zero lanes owned); (2) a reautorização pré-I/O recusa scheduler fora de
  `enabled|draining` (`preio_refused_runtime`, `retry` com backoff positivo,
  ambiguity inalterada, zero I/O, contador).
- Epoch NOT NULL nunca é apagado: clearance significa permissão de rotação
  na próxima `disabled→enabling`, que repete o guard completo. Nenhuma outra
  aresta pode mudar epoch. CLI container-only `scheduler-state show` e
  `scheduler-state enabling|enabled|draining|disabled --epoch UUID
  --expected-version N --actor ACTOR --reason REASON`; sem endpoint público.
  Transição e audit append-only em `delivery_operator_actions` são atômicos;
  `action=scheduler:<origem>:<destino>`, `reason` JSON versionado preserva
  versões, epochs anterior/novo, epoch delivery e motivo humano. Metadata
  que excede capacidade da coluna é recusada, nunca truncada. Nenhum schema
  novo. Gate de compatibilidade de réplicas: T08-3 abaixo.
- **T08-6 — opt-in por agente.** Único caminho de escrita da flag
  `inbox_scheduler_enabled`: CLI container-only
  `agent-scheduler opt-in AGENTE_ID --scheduler-epoch UUID --delivery-epoch
  UUID --actor ACTOR --reason REASON`, `agent-scheduler opt-out AGENTE_ID
  --actor --reason`, `agent-scheduler show AGENTE_ID` e diagnóstico paginado
  `agent-scheduler list --after-id N --limit N`; sem endpoint público
  (schemas REST de agente não têm o campo; valor enviado é ignorado).
  Opt-in abre UoW de controle própria (`begin_scheduler_control`, READ
  COMMITTED) e trava `delivery_runtime_state` (S) → `scheduler_runtime_state`
  (X) → `agentes` (X). Allowlist avaliada nas linhas travadas; cada falha é
  recusa nomeada, todas reportadas na ordem: `agent_not_found`,
  `scheduler_not_enabled` (estado ≠ `enabled`), `scheduler_epoch_mismatch`,
  `runtime_not_outbox`, `delivery_epoch_mismatch`,
  `provider_auth_not_enforced` (`PROVIDER_WEBHOOK_AUTH_MODE=enforced` no
  container do operador **e** snapshot de readiness `ready` com auth
  `enforced`; não há linha durável de auth), `agent_inactive`,
  `chatwoot_inbox_missing`, `agent_already_opted_in`. Flags locais
  (`INBOX_SCHEDULER_ENABLED`, epochs do `.env`) nunca autorizam sozinhas:
  epochs vêm do operador e conferem com o DB. Opt-out reutiliza
  `lifecycle_repo.update_agent` (recusa `scheduled_work_pending` com turn não
  terminal; `agent_not_opted_in`, `agent_not_found`) e é permitido em qualquer
  estado do scheduler. Sucesso grava audit append-only em
  `delivery_operator_actions` (`action=agent_scheduler:opt_in|opt_out`,
  `reason` JSON v1 com agente, from/to, estado/epoch/versão do scheduler, modo/
  epoch do delivery e motivo humano; >500 caracteres → recusa, nunca trunca)
  na mesma UoW. Recusa: nada muda nem é auditado, contador local por ação e
  `ERROR` com limiar 1. O X do scheduler serializa opt-in com toda transição
  auditada: depois de `enabled→draining` commitado nenhum opt-in passa; opt-in
  commitado antes não reabre intake (gate de intake novo exige `enabled`).
  `agent-scheduler list` usa snapshot `READ ONLY`, SELECT sem lock e página
  limitada; durante drain, depois da clearance e antes de `disabled`, operador
  deve listar e fazer opt-out de todos os agentes. Agente opted-in deixado em
  `disabled` recebe 503 `scheduler_not_enabled` em novo inbound.
  Inbox: `PUT /agentes/{id}` com `chatwoot_inbox_id=null` em agente opted-in
  → 409 sob os locks de `lifecycle_repo.update_agent` (mesma UoW/ordem; serializa
  com o opt-in pelo X do scheduler), contador `opted_in_inbox_clear_refused` +
  `ERROR` limiar 1; troca por outro inbox não nulo é permitida; opt-out antes
  libera a limpeza. Reativação: §10.15.
- **T08-3 — réplicas e gate de enable.** Todo processo `app` (lifespan),
  `delivery_worker` e `turn_worker` grava `replica_heartbeats` a cada
  `HEARTBEAT_SECONDS=15` (cadência fixa, transação própria, best-effort com
  contador + ERROR limiar 1) e marca `stopped_at` na saída limpa. Fresca =
  `stopped_at IS NULL` e `last_seen_at` dentro de `FRESH_SECONDS=60` pelo
  relógio do DB. `code_version` = `DELIVERY_WORKER_CODE_VERSION` (identidade
  do build, comum a todos os kinds); `schema_version` é compilado no binário
  (`EXPECTED_SCHEMA_VERSION`) e deve igualar `scheduler_runtime_state`
  (schema_version=3 após S1). Heartbeat fresco prova presença; ausente/velho nunca
  prova ausência (binário antigo não escreve a tabela).
- F0-T5: `HeartbeatLoop.stop(deadline_seconds=STOP_BEAT_TIMEOUT_SECONDS=5)`
  usa prazo monotônico total: evento → cancelar beat → drenar até deadline →
  marcador final somente com restante. Chamadas concorrentes/repetidas compartilham
  primeiro deadline e marcador; cancelamento do chamador propaga somente para
  ele, sem cancelar shutdown compartilhado. Beat cancelado
  invalida sua sessão/conexão antes do marcador (sem rollback reutilizável de
  socket interrompido). Cleanup pendente no deadline → sem marcador concorrente,
  cancelamento + observação do resultado tardio, `heartbeat_failures` + ERROR
  limiar 1. Não adiciona timeout global de socket/pool; ausência de marcador
  preserva expiração fail-closed por 60s. Prazo exige event loop cooperativo;
  watchdog de processo cobre bloqueio do event loop e teardown resistente.
  C nativo que retém o GIL indefinidamente não é preemptível por thread Python;
  SIGKILL do container permanece último limite externo.
- Readiness (allowlist de motivos, sem PII/valores): `replica_state_unavailable`,
  `invalid_heartbeat`, `schema_version_unsupported` (DB ≠ binário),
  `schema_version_mismatch`, `code_version_mismatch`,
  `delivery_mode_mismatch`, `delivery_epoch_mismatch`,
  `scheduler_flag_mismatch`, `scheduler_epoch_mismatch`,
  `webhook_auth_mismatch`, `dispatch_flag_mismatch` (campos compartilhados por
  todas as réplicas frescas e o leitor), `turn_processor_flag_mismatch` (só
  turn workers), `legacy_replica_detected` (heartbeat legado de delivery
  fresco sem linha da mesma réplica fresca ou parada), `delivery_epoch_not_current`
  (runtime outbox, réplica outbox em outro epoch), `scheduler_epoch_not_current`
  (scheduler ≠ disabled, réplica ligada em outro epoch),
  `scheduler_flag_not_current` (enabled/draining com réplica desligada).
  **T08-3b:** `/health`, `delivery_cli health` e `turn_worker health` usam
  `replica_compatibility.healthy`, não `compatible`, para compor o resultado.
  `BLOCKING_STATES = enabling|enabled|draining`, lidos do DB, nunca da flag
  local. Em `disabled`, divergência mantém `compatible=false` e motivos,
  contador de transição + ERROR (`threshold=1`), mas `healthy=true` preserva
  health legado. `ERROR` de compatibilidade/indisponibilidade só ocorre nas
  transições `compatible→incompatible`/`available→unavailable`, não por leitura.
  `state=null` se a leitura de state falhar: mesmo tratamento não bloqueante,
  sem segundo motivo de falha; checagens existentes de DB continuam valendo.
  Falha de leitura da frota depois de ler state preserva esse state e bloqueia
  se ativo. Leituras de health são consistentes, sem locks, na mesma sessão.
  Payload inclui `state` e `blocks_health`; gate de enable e refuse-start por
  schema divergente permanecem estritos, independentes desse resultado.
  Rolling deploy com scheduler ativo: heartbeat legado de réplica antiga
  parada sem marcador mantém readiness falha por até 60s.
- Gate `enabling→enabled` (`transition_scheduler_state`, mesma UoW READ
  COMMITTED): depois de runtime S → scheduler X e antes do audit e da
  clearance, leituras consistentes (sem lock; nunca esperam writer de
  heartbeat, que não segura outro lock) de réplicas, legado e autoridade.
  Recusa (`ReplicaGateRefused`, contador + ERROR limiar 1, nada gravado) com
  qualquer motivo de readiness para o fingerprint do operador mais
  `operator_attestation_missing`, `replica_kind_missing:app`,
  `replica_kind_missing:delivery_worker`, `replica_kind_missing:turn_worker`,
  `runtime_not_outbox`, `delivery_mode_not_outbox`,
  `delivery_epoch_not_runtime`, `scheduler_flag_disabled`,
  `scheduler_epoch_not_target`, `webhook_auth_not_enforced`,
  `dispatch_disabled`. CLI: `scheduler-state enabled … --attest-old-replicas-removed`
  (flag só nesse subcomando; sem ela recusa). Audit `reason` v2 grava
  `attested_old_replicas_removed`, `code_version`, `schema_version` e
  contagem por kind. Réplica nova que aparece depois do commit não é
  impedida (sem gap lock): readiness posterior a detecta; a atestação cobre
  réplicas antigas invisíveis.
- Turn worker recusa start (`EXIT_REFUSED`) com schema do DB ≠ binário;
  `python -m src.turn_worker health` exige snapshot local fresco, heartbeat
  próprio fresco e `replica_compatibility.healthy`. Start exige flags locais
  processor/scheduler ligadas e ambos epochs; não exige DB state enabled
  para manter o processo vivo. Claims só ocorrem em `enabled|draining` sob
  os gates existentes; health pode observar DB `disabled` durante transição.
  Compatibilidade não substitui os checks locais/heartbeat. Compose: serviço `turn-worker`, profile
  `inbox`, `restart: on-failure`, healthcheck com esse comando; default off.
- Ticket de fundação não liga rotas, daemon, provider, presença ou dispatch ao
  scheduler. Ativação operacional exige tickets e aprovação posteriores.

<!-- FU-2: locking footprint / accounting -->
**FU-2 — footprint por sessão e accounting sem gap especulativo.** Leituras
locking de `conversation_turns` por sessão fixam
`FORCE INDEX (idx_turn_session_status)` (intake, lifecycle, successor/promotion,
orphan, history de delete e retenção). Graphs não terminais limitam também
`status` conforme `ck_turn_slot_status`; índice de retenção nunca decide esse
acesso. History por agente fixa `idx_turn_ready_claim`; consultas por PK e
clearance global mantêm seus caminhos existentes. Retenção e delete usam RC
já estabelecido pelo caller; não trocar isolamento dentro do grafo.

`uq_generation_consumo_link` arbitra reutilização de ledger: nenhuma leitura
locking de link possivelmente inexistente. Attempt e ledger existente continuam
X-locked; escrita de evidência/link usa savepoint comum a accounting e
reconciliação. `1062` nesse índice → `False`, sem evidência parcial e com UoW externa
utilizável. Com ledger exato e provider ID não nulo, MySQL pode reportar primeiro
`uq_generation_provider_id` (mesmo conflito); também rejeitado. Demais
constraints/erros são re-lançados.
`1205`/`1213`/`1305` sempre escapam para retry da UoW inteira.
Consulta de intent `turn_resolve:abandon` no settlement não toma gap lock:
writers automatizados já compartilham scheduler/turn/effect locks adquiridos
antes do read view. Testes MySQL incluem histórico enviesado de 5000 terminais,
footprint observado por conexão, accounting tardio de dois agentes sem retries,
deadlock real dentro do savepoint e abandon estacionado com lane/claim próprios.

### 10.12 Processador de turns — geração e refresh (ticket 04)

- Processo separado do delivery worker. Uma geração autorizada por lane de
  agente (fence no DB, não só single-flight local); agentes distintos rodam em
  paralelo. Nenhum lock é mantido durante chamada LLM ou espera de typing.
- Transações owner usam só locking reads no prefixo canônico até
  `turn_revisions`; `sessao_id` vem da identidade imutável do claim. A primeira
  leitura consistente (soft limit, histórico, caps) ocorre depois de todos os
  locks. Identidade pré-carregada nunca concede autoridade.
- `generation_attempts` é commitado `started` antes da chamada. Soft limit é
  re-checado antes de toda chamada; atingido → draft `soft_limit_fallback` sem
  LLM. Cap de tentativas → `technical_fallback`.
- Cliente: `llm_client.completar_tentativa_unica` — uma requisição, sem retry.
  HTTP de erro do provider = `failed` conhecido (fallback técnico, sem nova
  chamada). Timeout/transporte/cancelamento/resposta ilegível =
  `outcome_unknown`: quarentena do agente, lane liberada, sem custo zero
  fictício; outros agentes seguem. Resolução auditada: §10.15.
- Owner válido: accounting exato (`consumo_llm` com modelo solicitado do
  attempt + `modelo_llm_reportado`, link do attempt) e aceitação do draft na
  mesma transação. Owner perdido: só accounting do attempt exato
  (`agentes` S → `sessoes` S → attempt X → consumo), sem lane/turn/draft/
  histórico. Lease perdido durante a chamada permite apenas drain limitado por
  `accounting_drain_seconds`. Resposta 200 sem `choices` ou sem `usage` é
  ilegível → `outcome_unknown`, nunca sucesso de custo zero.
- Discovery de lanes expiradas ignora lanes cujo owner expirado deixou
  quarentena (`reconciliation_required` não resolvido ou attempt `started`
  ainda dentro do drain): não ocupam a janela limitada de recovery nem tomam o
  mutex do scheduler a cada passada; a quarentena permanece durável na lane e
  no attempt para o ticket 07. Drain vencido volta a ser elegível.
- Contexto = histórico aceito limitado por `max_historico` (assistant, inputs
  legados e inputs de turns terminais) + lote selado inteiro por ID, uma vez.
- Input novo antes do final-candidate seal em revisão 1 → draft `superseded`
  (uso contado, nunca histórico) e revisão 2 com a janela 2s/5s a partir do
  primeiro input novo. Após selar a revisão 2, input vai ao sucessor. Contador
  `refresh_count` continua sendo gravado por `admit_revision`.
- Final-candidate seal (deadline atingido, sem input ativo pendente) congela o
  conteúdo, grava o reply `generated` no histórico assistant e incrementa
  `total_mensagens` uma vez. Fallbacks não entram no histórico nem no contador.
  Não é admissão de envio; nada é enviado ou enfileirado pelo ticket 04.
- Turn ativo **ordinário** que vira terminal sem admissão (cap de recovery de
  lease `lease_recovery_attempt_cap`, cap de entrega `delivery_attempt_cap`)
  promove o sucessor `pending` a ativo na mesma transação (lock do sucessor
  depois do turn, ordem crescente): nenhum sucessor fica órfão e o intake
  seguinte segue normal. Exceção: `final_candidate` durável `generated` com
  `needs_handoff=TRUE` é handoff decidido; `draft|typing` só é decidido quando
  não há membership `active` no turn que ainda obriga refresh, ou quando já é
  revisão 2 e o único refresh foi gasto. Revisão 1 `draft|typing` com input
  ativo no cap é `superseded` no mesmo prefixo locked, cria revisão 2 exatamente
  como o refresh ordinário e libera a lane em `retry`; preserva o `ready_at`
  2s/5s gravado pelo intake, a identidade imutável dos memberships e o contador
  de recovery já no cap. É uma única reprieve: revisão 2 nunca repete esse
  branch. Se revisão 2 chega ao próximo cap ainda sem draft, o predecessor
  `generated`/`needs_handoff` superseded exatamente por
  `refresh_input_at_scheduler_cap` mantém o handoff decidido e terminaliza em
  `falha_tecnica`; um draft novo da revisão 2 substitui essa intenção (handoff
  novo → decidido, não-handoff → ordinário). Supersession por qualquer outro
  reason não carrega intenção. `complete_turn_and_release_lane(
  promote_successor=False)` com sucessor existente falha fechado
  (`SchedulerConflictError`).
- Lifecycle não-bot/agente inativo/opt-out visto pelo owner → cancelamento
  formal do turn e do sucessor (§10.15), nunca release+backoff. Shutdown gracioso drena a chamada
  em voo e libera a lane preservando revision/deadline; restart retoma o mesmo
  deadline sem sortear de novo.

### 10.13 Handoff terminal e contexto tardio (ticket 05)

- Consumer (`services/turn_handoff.py`) aceita somente a revisão
  `final_candidate` `generated` com `needs_handoff`, selada após o typing
  ordinário e ainda não admitida. Candidate superseded/cancelado nunca resume,
  encerra sessão nem cria attempt/outbox (`StaleTurnOwnership` sob locks).
- Fluxo: `begin_handoff` (locks, soft limit, attempt `summary` `started`
  commitado) → chamada LLM sem locks → `complete_summary_and_finalize`
  (accounting exato + admissão do handoff na mesma UoW). Owner perdido,
  lifecycle alterado ou qualquer falha não allowlisted (todo `DBAPIError`,
  timeout de pool SQLAlchemy, `asyncio`/OS/rede e exceção desconhecida) → só
  `record_generation_accounting_only` do attempt exato quando ele existe; o
  reclaim pode repagar o resumo, limitado por `max_generation_attempts_per_turn`
  (limitação aceita). Só falha local determinística explicitamente allowlisted
  (`HandoffLocalInvariantError`, `HandoffAdmissionValidationError`,
  `OutboxConflictError`, `SchedulerConflictError` não-stale e
  `InvalidDeliveryPayload`) terminaliza. `AssertionError` e `ValueError`
  genéricos nunca entram por tipo: pymysql/aiomysql levantam `AssertionError`
  em dessincronia de protocolo e o SQLAlchemy não o encapsula, então ele segue
  o caminho retryable. Invariants próprios levantam `HandoffLocalInvariantError`
  explicitamente (nunca `assert`, removido por `-O`); `ValueError`/
  `AssertionError` do planner puro são promovidos a
  `HandoffAdmissionValidationError` apenas no limite da chamada do planner.
  Cancelamento (`CancelledError`) durante a admissão ou a terminalização, ou
  durante o registro do resultado da resposta no `turn_processor`, tenta gravar
  o accounting exato do attempt com outcome conhecido via `asyncio.shield`
  antes de propagar. Se essa transação conclui no cancelamento cooperativo,
  o attempt pago conhecido não fica `started`. Falha da própria transação de
  accounting (DB indisponível ou retries esgotados) também pode deixá-lo
  `started` → drain/recovery/quarentena. Hard deadline/crash segue F0-T5
  (§10.8): o turn-worker pode perder o ledger mesmo após resultado pago
  conhecido; attempt `started` → drain/recovery/quarentena/`outcome_unknown`,
  sem custo zero fictício.
  Para as falhas locais determinísticas allowlisted de admissão ou
  terminalização descritas acima (não para hard deadline/crash),
  `fail_handoff_terminally` contabiliza o attempt e marca revisão/turn `dead`
  (`handoff_admission_failed`), cancela o sucessor (nunca órfão), encerra a
  sessão como `falha_tecnica` (inbound posterior = sessão fechada), libera a
  lane e loga `ERROR`; inputs tardios ficam visíveis e não cobertos no turn
  cancelado para o operador. Se essa própria terminalização falhar depois de
  resumo pago, o consumer tenta o fallback de accounting exato, incrementa
  `terminal_failure_fallback_count` e emite `ERROR` com `threshold=1`.
  Fallback bem-sucedido persiste o accounting do attempt conhecido, inclusive
  quando acionado por exceção sem cancelamento; falha da transação de accounting
  ou hard deadline/crash continua sujeita às ressalvas acima.
  Auditoria dos paths ticket04/05: finalização owner do `turn_processor` já
  cai em accounting-only para toda exceção; repo de geração não classifica
  exceção em terminal; delivery de nota usa allowlist terminal e default
  retryable para exceção desconhecida, sem denylist-to-terminal adicional.
  Caps de recovery/yield decidem handoff somente da revisão durável corrente e
  dos memberships do turn, todos locked na mesma UoW do cap. `final_candidate`
  `generated` com `needs_handoff=TRUE` é definitivo. `draft|typing` equivalente
  só é definitivo sem input ativo capaz de invalidá-lo, ou na revisão 2;
  revisão 1 com input ativo gasta atomicamente o refresh e retoma como revisão
  2 sem mover memberships (a identidade `turn_id` é imutável). Revisão 2 ainda
  sem draft carrega a intenção apenas quando seu predecessor locked foi
  superseded por `refresh_input_at_scheduler_cap` e contém o draft imutável
  `generated`/`needs_handoff`; somente draft `generated` novo da revisão 2
  passa a ser a autoridade. `technical_fallback`/`soft_limit_fallback` mantêm
  a intenção carregada; supersession por outro reason não conta. Handoff decidido que
  esgota o cap reutiliza `terminalize_failed_handoff_locked`: revisão/turn
  `dead`, sucessor `cancelled`, sessão `falha_tecnica`, lane liberada;
  memberships aceitas permanecem `active`, visíveis e sem assignment. Ambos
  recebem `terminal_reason=handoff_recovery_cap`. Cada transição grava esse
  motivo durável e emite `ERROR` com contador do evento e `threshold=1`;
  inbound posterior vê sessão fechada. Attempts de resumo já conhecidos foram
  contabilizados exatamente antes do cap; attempt `started` sem outcome segue
  drain/quarentena e não é falsamente terminalizado. Turn ordinário mantém a
  promoção do sucessor.
  Soft limit atingido → `soft_limit_fallback` sem attempt; quarentena do agente,
  cap de attempts ou histórico vazio → `technical_fallback` sem LLM; falha
  conhecida/texto vazio → `technical_fallback` vinculado ao attempt.
- Prefixo canônico das transações owner (só locking reads): `delivery runtime
  (S) → scheduler → agent → lane → session → turns (ativo, sucessor; id asc) →
  revisions (id asc) → memberships do turn ativo/sucessor (id asc) →
  generation_attempts do turn`.
  Primeira leitura consistente (soft limit, histórico, conteúdo tardio) só
  depois disso. Inserts posteriores (`consumo_llm`, `handoff_attempts`,
  `scheduled_handoffs`, `handoff_context_assignments`, `outbox_deliveries`)
  só tocam pais já travados (locks implícitos de FK/unique sem inversão).
- Admissão: revisão `final_candidate → admitted` com `draft_text` preservado;
  turn fica `session_slot=active`, `status=awaiting_delivery`, claim limpo e
  lane liberada (setup Chatwoot não segura o agente). Sucessor → `cancelled`
  terminal; cada membership aceita vira `post_handoff_pending` com exatamente
  um `handoff_context_assignments`. Input aceito depois do commit terminal
  mantém o comportamento de sessão fechada (`ignored_nonbot`); sem bridge geral
  WhatsApp→humano. Turn admitido re-enfileirado nunca gera de novo: o handoff
  admitido da generation atual (sessão `encerrada` por `handoff`, attempt
  `pending|ready`) segue para a admissão de entrega do ticket 06 (§10.14);
  qualquer outro lifecycle fechado → recusa do ticket 04.
- Efeitos staged: open `chatwoot.open_handoff.v1` e notas
  `chatwoot.add_private_note.v1` são `provider_setup_context`; transição
  `evolution.send_text.v1` é `automated`, ligada a turn/revision admitidos,
  `blocked` no open. Completion, callback e `operator_retry` nunca a
  agendam; `operator_retry` recusa qualquer linha `automated`. Somente o ticket
  06 (lane reautorizada) pode admitir a transição.
- Ticket 06 fixa dois efeitos imutáveis quando há reply: `reply` pending antes
  de `handoff_transition` blocked; ambos referenciam a revisão admitida. Reply
  vazio não cria linha nem gap. Dispatch continua proibido sem a admissão/fence
  exata da lane descrita em §10.14.
- Nota privada: limite verificado de 150000 caracteres medido na nota
  renderizada exatamente como enviada (header + corpo com escape JSON:
  controles `\u00XX`, aspas, barras; marker e coverage incluídos). Chunking
  determinístico, ordem e Unicode preservados, sem truncar. Registro que não
  cabe numa nota é dividido por code points em partes ordenadas com coverage
  `membership:{id}#part{n:04d}/{total:04d}`; as partes concatenadas reconstroem
  o conteúdo exato. Para o limite verificado o planner é total (só levanta
  abaixo do overhead fixo do header, erro de programação), então tamanho nunca
  bloqueia a admissão. Marker
  `wppctx:v1:{attempt_id}:{ordinal:04d}:{total:04d}:{body_sha256}`; coverage
  `membership:{id}` exata, uma assignment por membership fixada na nota da
  primeira parte; payload, dependência e assignments imutáveis.
  Dispatch exige mapping `ready` atual (account, inbox, contact, conversation,
  attempt, generation) e coverage exata: memberships que começam na nota
  (inteiras ou parte 1) são exatamente as assignments fixadas nela, em ordem de
  `coverage_ordinal`, e continuações pertencem ao mesmo attempt; mismatch
  remoto → `ContextNoteOwnershipMismatch` terminal, sem POST.
- Recovery: create faz varredura exata (`after=<id>` até o fim) antes do POST
  e marca `post_sent` imediatamente antes dele. Falha antes do POST (GET de
  mapping/scan com timeout, 5xx ou resposta inválida) →
  `context_note_pre_post_failure`, retry com backoff (nada desta tentativa foi
  criado; o retry varre de novo). Depois do POST, qualquer falha sem match
  exato verificado (timeout, 5xx, scan falho, zero match, inclusive exceção
  desconhecida/não classificada) e toda lease
  perdida em `processing` (expiração, expiry do TTL ou cancel durante
  processing) viram `context_note_ambiguous` (`ambiguity_count ≥ 1`, `dead` ou
  `cancelled`), sem retry; múltiplos matches → `duplicate_remote_effect`. Toda
  terminalização ambígua de nota grava `terminal_reason=context_note_ambiguous`
  (worker, `reconcile` sem marker, maintenance). Saída somente por `reconcile`
  exato ou `resolve-context-note` auditado: `delivered` e `cancelled`
  (abandona só o conteúdo daquele chunk) liberam o próximo chunk e a cadeia
  continua; `retry-safe` devolve a nota a `retry`. `operator_retry` recusa nota
  cujo estado atual é `context_note_ambiguous` e só aceita dependência entregue.
  Reconcile, retry e resolução revivem a cadeia ordenada que a cascata
  `dependency_*` encerrou (só notas nunca ambíguas). Maintenance conta
  `context_note_ambiguous` (log `ERROR`, limiar 1); discovery limitada gira
  porque cada linha examinada sai da janela.
- Retenção: payload de nota só é scrubbed (7 dias) depois de resolvido
  (`delivered`, `operator_resolved_cancelled`, `operator_cancelled`,
  `cancel_requested`) ou quando a generation do handoff foi superada (passe por
  linha com locks canônicos session → attempt → delivery, antes de qualquer lock
  em massa); nota não resolvida da generation atual mantém o payload para
  reconcile/retry-safe. Delete de 90 dias só remove nota já scrubbed.
- FKs novas usam `RESTRICT`; purge genérico não remove handoff attempts,
  turns, memberships, attempts nem assignments; delete de sessão/agente com
  handoff agendado falha fechado até retenção segura T08-2 (§10.15).
  `handoff_attempts` e mappings nunca são apagados por T08-2. Turn `awaiting_delivery` conta nos caps de
  fila até o ticket 06 concluí-lo.
- `turn_worker.py` agora possui consumers de handoff e resposta normal. Default
  permanece desabilitado; start explícito ainda exige scheduler ligado e os
  epochs scheduler/delivery configurados.

### 10.14 Typing remoto e admissão automatizada (ticket 06)

- `typing_deadline_at` persistido é a única fonte do pacing 2–12s; restart não
  sorteia duração/jitter novamente e nenhum worker HTTP dorme aguardando o
  deadline. `sendPresence` (`composing`/`paused`) é best-effort, owner-scoped e
  não autoriza texto. Falha incrementa contador, sinaliza no primeiro evento e
  preserva o outbox. Timer remoto já aceito pode produzir pause/flicker tardio;
  essa limitação visual não relaxa fence de mensagem.
- Admissão exige lane/turn/revision/range/token e ambos epochs exatos. Claim do
  outbox commita primeiro; antes de Evolution I/O, o worker trava novamente na
  ordem `delivery_runtime_state (S) → scheduler → agent → lane → session → turn
  → revision → handoff_attempt (se houver) → deliveries (id asc)` e valida
  lease de ambos. Os links imutáveis vêm do resultado do claim; nenhuma leitura
  simples antecede a espera por locks e lane nunca é adquirida segurando delivery.
- Reply handoff usa a admissão automatizada ordinária sem esperar Chatwoot.
  Transition só muda `blocked→pending` depois de open `delivered`, attempt
  `ready` e nova reautorização da lane. FIFO usa `stream_sequence`; reply tem
  sequência menor. Reply falho nunca libera transition nem sucessor.
- Falha retryable usa backoff estritamente positivo, move o turn para o fim da
  lane por `scheduling_sequence` novo e libera owner. Discovery é limitada e
  exclui efeitos terminais/ineligíveis, impedindo linha presa de ocupar a página.
  Outros agentes continuam independentes.
- **P2-1 (decisão do usuário):** esperar o claim do delivery worker nunca
  consome `delivery_attempt_count`; só uma tentativa de envio que falhou conta
  (`fail_automated`, incremento antes da checagem: o cap é atingido na falha
  de número `max_delivery_attempts_per_turn`). Expiração de lane/yield de turn
  admitido abaixo do cap só libera o owner e deixa o turn `awaiting_delivery`.
  Turn ordinário no cap por falhas reais → `dead` (`delivery_attempt_cap`),
  efeitos não terminais `cancelled` e promoção do sucessor (§10.12); nunca
  estaciona. Somente handoff já `admitted` no cap estaciona (R5-a):
  `awaiting_delivery`, mantém `scheduled_handoffs`, open/notas e efeitos, libera
  a lane e emite contador/`ERROR` (`threshold=1`); resolução: `turn-resolve` (§10.15).
- Discovery/requeue cobre todo turn admitido (ordinário e handoff) com efeito
  acionável (`pending|retry` vencido ou `processing` com lease vencido), lane
  livre (sem owner: nem ocupada nem em quarentena), agente ativo e opt-in e
  lifecycle que ainda autoriza os efeitos. Requeue que encontra o turn
  inelegível sob locks o rotaciona para o fim (`scheduling_sequence`), então a
  página limitada nunca fica cheia de linhas que não progridem.
- Admissão nunca segura a lane esperando: efeito `processing` com lease de
  outro worker, backoff não vencido ou transition aguardando o open → turn
  `awaiting_delivery`, lane liberada, sem contar.
- No cap de refresh, somente draft revisão 2 `generated` substitui intenção de
  handoff carregada da revisão 1 superseded por
  `refresh_input_at_scheduler_cap`; fallbacks técnicos/soft-limit mantêm
  `falha_tecnica` + `handoff_recovery_cap`.
- Maintenance genérica não expira, recupera nem propaga dependência em linhas
  `automated`; esses paths exigem lane. Skip/degrade é contado e sinalizado.
  Health local do turn worker exporta falhas de presença e parks/caps.
- Admissão lane-owned resolve o efeito selecionado sob o prefixo canônico:
  `processing` com lease vigente → conflito (outro worker em I/O);
  `processing` com lease vencido → `retry` com `ambiguity_count+1`
  (`lease_expired_ambiguous`, at-least-once) ou `cancelled` se havia
  cancel_requested; `pending|retry` expirado → `dead` (`expired`). Efeito
  terminal na admissão: turn comum → `dead` + promoção do sucessor; handoff →
  estacionado `awaiting_delivery`. Contador `terminal_at_admission` + `ERROR`
  (`threshold=1`) no health do turn worker.
- Claim do delivery worker só seleciona linha `automated` cujo turn está
  `awaiting_delivery` com lane/turn lease vigente e owner idêntico. A
  reautorização pré-I/O e a renovação repetem todo fence do claim mais o
  lifecycle corrente: `cancel_requested_at` nulo, agente ativo e opt-in;
  reply ordinário exige sessão `bot`; efeito de handoff exige attempt da
  generation corrente `pending|ready` e sessão `encerrada` por `handoff`;
  transition exige ainda open `delivered` e attempt `ready`.
- **X1 — finalização registra a verdade remota.** `complete_automated` e
  `fail_automated` exigem só autoridade sobre o efeito (gate de runtime/epochs
  e posse exata `processing` + token + links, paridade com `_owns`); cancel,
  `/retomar`, `/transferir`, agente inativo/opt-out ou generation nova durante
  o I/O nunca recusam o registro de `delivered` ou da falha. Depois o turn é
  assentado pelo lifecycle corrente: ordinário → terminal (`delivered`/`dead`)
  + promoção do sucessor; handoff corrente → reply segue (`awaiting_delivery`),
  transition encerra (`handoff`), efeito morto estaciona; handoff superado →
  turn `cancelled` (`handoff_superseded`), efeitos restantes `cancelled` e
  promoção. Falha só volta a `retry` se o lifecycle ainda autoriza o efeito.
- **X2 — todo efeito terminal assenta seu turn na mesma UoW.** Recusa pré-I/O
  por cancel/lifecycle cancela o efeito e assenta o turn. Efeitos terminados
  fora da lane (`/retomar`, `/transferir`+`/retomar`, operador) são achados
  pela discovery (ramo de assentamento, independente de agente ativo) e
  assentados sob locks pelo requeue, sem acordar a lane. Turn admitido cujo
  lifecycle fechou é liberado para `awaiting_delivery`, nunca loop de
  `lifecycle_refused`. Nenhum turn admitido fica órfão.
- **Y1 — assentamento resolve todos os efeitos não terminais.** Antes de
  encerrar turn admitido (handoff superado, ordinário dead/cap, recusa pré-I/O,
  requeue ou cap pós-admissão), travar sucessor antes das revisões e efeitos
  por ID na ordem canônica. `processing` com lease vigente nunca é morto:
  turn fica `awaiting_delivery`, lane liberada; discovery de assentamento
  exclui leases vivos e reencontra o turn após vencimento, inclusive com
  agente inativo/opt-out. Lease vencido ou ausente → `cancelled`/`dead`,
  `ambiguity_count+1`, `last_error_code=lease_expired_ambiguous`, lease limpo;
  contador dedicado `orphan_effects_resolved` + `ERROR threshold=1` (verdade
  remota desconhecida, at-least-once). Cancelamento pré-I/O da própria linha
  continua sem ambiguidade: nenhum I/O começou. Turn só vira terminal após
  resolver todos os efeitos; sucessor é promovido na mesma UoW. Cap ordinário
  é assentado pela discovery, sem exigir novo claim/envio nem agente ativo.
  Admissão lane-owned também recupera lease ausente como vencido. Nenhum
  efeito `processing` abandonado por esses caminhos bloqueia FIFO para sempre.
- **X4 — lease × deadline.** Sempre que `DELIVERY_DISPATCH_ENABLED` ou
  `INBOX_SCHEDULER_ENABLED` estiver ligado no serviço,
  `DELIVERY_EVOLUTION_LEASE_SECONDS` deve exceder o deadline + 5s (cada
  serviço valida a própria config). A reautorização pré-I/O estende a própria
  lease da delivery, a lane e o turn até `now + deadline + 5s`; recusa por
  lane (`preio_refused_lane`) volta a `retry` com backoff positivo (1s) e
  contador.
- Admissão nunca segura a lane para efeito que o claim não pegaria: epoch de
  delivery diferente do runtime (adiado + contador `ERROR`; sem rollover, §10.15) ou predecessor FIFO não terminal no stream (adiado; discovery
  só relista quando o predecessor terminar). Toda admissão — ordinária
  (`admit_candidate`) e de handoff (`_finalize_locked`, UoW do ticket 05,
  autorizado pelo usuário) — zera o `delivery_attempt_count` compartilhado
  com recoveries de geração, sob os locks já tomados: o cap pós-admissão conta
  só falhas reais de envio (P2-1).
- A reautorização pré-I/O estende lane e turn até `now + deadline do provider
  + 5s` antes de liberar o I/O; a renovação nunca encurta essa extensão. Com
  `INBOX_SCHEDULER_ENABLED`, `DELIVERY_EVOLUTION_LEASE_SECONDS` deve exceder o
  deadline + margem. Recusa pré-I/O (nenhum I/O ocorreu) libera a linha sem
  ambiguidade: `cancelled` (cancel/lifecycle) ou `retry` (lane), contador e
  `ERROR` (`threshold=1`); recusa do gate de runtime/scheduler (frozen, epoch
  divergente, scheduler fora de enabled/draining) devolve a linha a `retry`
  com backoff positivo, `preio_refused_runtime`, contador e `ERROR`
  (`threshold=1`), ambiguity inalterada (ticket 07). Reautorização, renovação, finalização e falha
  repetem a transação inteira em `1205`/`1213`/`1305`.
- Presença: falha de `composing`/`paused` emite `ERROR` com contador e
  `threshold=1`; testes injetam presença fake (nenhum HTTP).
- Handoff admitido da generation atual atravessa o fence de lifecycle
  (sessão `encerrada` pelo próprio handoff) somente para entregar os efeitos
  já fixados; nunca gera, resume nem cria efeito novo.
- Gate mudou somente quanto à existência dos consumers: `turn_worker` pode
  executar quando explicitamente habilitado. Defaults continuam off; sem
  `INBOX_SCHEDULER_ENABLED=true`, `INBOX_TURN_PROCESSOR_ENABLED=true` e epochs
  válidos/correspondentes, não há claims.

### 10.15 Lifecycle e operador fenced (ticket 07)

- Toda mutação de lifecycle/operador usa `lifecycle_repo` com locking reads
  (`populate_existing`) na ordem `delivery_runtime_state (S, qualquer modo) →
  scheduler_runtime_state (X) → agentes → agent_service_lanes → sessoes (id
  asc) → conversation_turns → turn_revisions → generation_attempts →
  scheduled_handoffs → handoff_attempts → outbox_deliveries (id asc)`.
  Identidade que não vem de linha travada sai de discovery escalar com read
  view encerrado (`rollback`) antes do primeiro lock; o grafo travado
  revalida. Retry da transação inteira em `1205`/`1213`/`1305` (limite 4).
  Escrita ORM pendente na sessão → `PendingWritesError` antes de qualquer SQL
  (nunca autoflush fora de ordem nem sobrescrita por `populate_existing`).
- Cancelamento formal (`/transferir` → `session_transferred`, `/retomar` →
  `session_resumed`/`handoff_superseded`, `ativo` true→false →
  `agent_disabled`, owner vendo lifecycle fechado → `session_closed`/
  `agent_disabled`/`agent_opted_out`): todo turn não terminal da sessão/agente
  (ativo e sucessor) vira `cancelled`; revisões não terminais `cancelled`.
  Geração em voo: o owner mantém lane e claim até accounting exato ou drain
  (`recover_expired_lane` assenta turn terminal e só então libera); nunca
  attempt `started` órfão. Turn admitido: efeitos enfileirados `cancelled`;
  `processing` com lease vivo só recebe `cancel_requested_at` (finalização
  grava `delivered_after_cancel`/falha e assenta); lease vencido → ambíguo
  (Y1). Sem promoção de sucessor (todos cancelados). Reativação nunca
  reprocessa inputs cancelados (ficam no histórico). `/retomar` mantém o
  cancelamento legado de todo outbox da sessão, inclusive em sessão `bot`.
- Opt-out (`inbox_scheduler_enabled` true→false) com turn não terminal →
  recusa (`LifecycleRefused`, 409); não exposto pela API pública. Opt-in/
  opt-out auditados: §10.11 T08-6. Limpar `chatwoot_inbox_id` de agente
  opted-in → 409 (T08-6). Reativação (`ativo` false→true) de agente opted-in
  não reavalia os guards do opt-in, por decisão: o intake continua gated por
  state/epochs/auth/agente ativo+opt-in em todo request.
- Delete de sessão/agente com qualquer `conversation_turns` → 409
  ("historico de scheduler retido; aguarde retencao segura ou resolva pendencias"). Qualquer outro dependente
  `RESTRICT` (`1451`/`1217`) → 409, nunca 500.
- **S3 (P2-2) — delete de agente preserva evidência.** Delete de agente com
  ledger ou handoff é recusado: `DELETE /agentes/{id}` de agente com qualquer
  linha em `consumo_llm`, `handoff_attempts` ou `llm_usage_reconciliation` →
  409 ("evidencia de uso ou handoff retida; desative o agente (ativo=false) em
  vez de excluir"), para agentes legados e do scheduler, antes e depois do
  purge T08-2. `consumo_llm`, `llm_usage_reconciliation` e `handoff_attempts`
  nunca são apagados por delete via API; o `ON DELETE CASCADE` do schema fica
  (sem migration); desativar é `PUT ativo=false`. `delete_agent` abre a
  própria transação em READ COMMITTED (`SET TRANSACTION` como primeiro
  statement; read view anterior → `StaleTransactionError`, 1568), trava o
  prefixo de lifecycle (runtime S, guard de runtime do router sobre a linha
  travada → 503, scheduler X, agente X, lane, sessões) e só então lê a
  existência sem lock (`LIMIT 1` por `agente_id`). Todo insert nessas tabelas
  toma S no agente (FK), então nenhum fica em voo nem começa durante o X; RC
  torna a leitura corrente e todas as leituras locking do delete ficam em
  locks de registro — nenhum gap/next-key em índice secundário, insert de
  outro tenant nunca espera. Recusa: contador
  `agent_delete_evidence_refused` (health do app via `lifecycle_counts`) +
  `ERROR` limiar 1. Sessões (decisão do usuário (a)): `DELETE
  /sessoes/{telefone}` de sessão com qualquer `handoff_attempts` → 409
  ("historico de handoff retido; a sessao nao pode ser excluida"), inclusive
  sessões legadas v4 e depois do purge T08-2 e da limpeza legada do outbox
  (`handoff_attempts.sessao_id` continua `ON DELETE CASCADE` no schema; o guard
  é o que preserva attempts e mappings). Mesmo padrão: READ COMMITTED primeiro,
  prefixo de lifecycle + sessão X, leitura de existência sem lock; contador
  `session_delete_handoff_refused` + `ERROR` limiar 1. Sessão sem handoff
  continua deletável; o ledger sobrevive: `consumo_llm` e
  `llm_usage_reconciliation` ficam com `sessao_id=NULL` (FK `SET NULL`; a
  relação ORM `Sessao.consumos_llm` não cascateia delete). Mensagens da
  sessão são apagadas com ela, como antes. Correção por classe:
  `lifecycle_repo` é o único módulo de `app/src` que apaga `agentes` ou
  `sessoes` (ORM `delete`, `delete(Agente|Sessao)`, `__table__.delete()` ou
  SQL `DELETE FROM`/`TRUNCATE`); os helpers legados `agente_repo.delete`/
  `delete_locked` e `sessao_repo.delete`/`delete_locked` foram removidos e o
  teste estático `tests/test_delete_paths_static.py` (AST + SQL) falha em
  qualquer outro caminho.
- Operador (`delivery_cli`, container-only, auditado em
  `delivery_operator_actions` com snapshot de epoch/status/contadores):
  `cancel` de linha `automated` passa pelo prefixo canônico e assenta o turn
  na mesma UoW (funciona em `frozen`); `operator_cancel`/`operator_retry`
  legados recusam `automated`. `turn-resolve --resolution retry|abandon`
  resolve handoff admitido estacionado: `retry` exige agente ativo/opt-in,
  generation corrente, runtime `outbox` no epoch atual, payload não scrubbed,
  sem `duplicate_remote_effect` e `--accept-duplicate-risk` se
  `ambiguity_count>0`; re-arma reply (`retry`) e/ou transition (`blocked`),
  zera o cap; admissão lane-owned envia. `abandon` → turn `cancelled`
  (`operator_abandoned`), efeitos resolvidos; handoff/Chatwoot intactos.
- Quarentena de geração: `quarantine-list|inspect|resolve`. Resolve só o
  attempt exato (locks `agentes` S → `sessoes` S → attempt X → `consumo_llm`
  X); `succeeded` exige `--consumo-id` exato (tokens/custo do ledger, guard
  verifica); `failed` exige `--accept-duplicate-spend-risk`, sem custo.
  `accounting_state` permanece `reconciliation_required`; `reconciled_at` +
  reason estruturado (`evidence=…;risk=…;note=…`) registram a decisão. Não
  adquire lane/turn/sessão: discovery e recovery liberam a lane só depois.
  Replay com tupla idêntica é idempotente; diferente → recusa.
- §16.1 A1: `claim_ready_turn(candidate=…)`, `recover_expired_lane(
  expected_turn_id/expected_sessao_id)` e `_lock_owned_turn(sessao_id=…)`
  recebem identidade de discovery commitada ou do claim; o turn processor
  sempre passa. Chamadas diretas sem hint (testes legados) mantêm a leitura
  escalar sob o mutex do scheduler.
- Rollover de epoch: linhas `automated` nunca migram de epoch. Freeze exige
  zero `processing`; restore-direct/cutover exigem zero não terminal; rotação
  de scheduler epoch exige drain. Em `frozen`, o caminho é cancel auditado.
- Retenção: linhas `automated` só entram em scrub/delete quando o turn é
  terminal (imutável pelo guard), por discovery limitada e update/delete por
  id; o bulk legado exclui `automated`.
- Guard `trg_conversation_turn_lifecycle_v1` (BEFORE UPDATE, digest fixo):
  terminal → idêntico (status/slot/reason/at); `active` → `active|terminal`;
  `successor` → `successor|active|terminal`. Com `ck_turn_slot_status` é a
  allowlist completa de status.

#### T08-2 — retenção terminal do scheduler

- `scheduler_retention_repo.cleanup_retention` roda depois do commit do
  cleanup legado, em transações próprias por candidato, sem savepoint.
  **R2-S1:** task independente single-flight por processo, inclusive no boot;
  claim/heartbeat/replica beat nunca aguardam passada. Shutdown cancela task
  e aguarda cleanup dentro do drain/abort 30s/5s (stop grace 40s).
  Etapa legado/scheduler/réplicas com falha → contador local
  `retention_stage_errors` + ERROR limiar 1, sem traceback; demais etapas e
  próxima cadência seguem. Discovery falha → `discovery_failed`, sem avançar
  checkpoint da página incompleta.
  Efeito automated é scrubbed numa passada e apagado na seguinte; cadeia de
  notas por dependência encolhe a cada passada; grafo converge em passadas
  finitas ou fica retido com sinal.
  **S1:** páginas PRIMARY/id de até 100 linhas, até fim/página jovem ou budgets
  `SCHEDULER_RETENTION_MAX_ROWS=10000` e `SCHEDULER_RETENTION_MAX_SECONDS=30`.
  Validação: linhas 1..1000000; segundos finitos 0.1..300.
  Deadline cancela e reverte candidato inteiro; checkpoint e observação health
  têm até 3s adicionais cada. Retry integral 1205/1213/1305 (limite 4), sem I/O remoto.
  Cursor singleton em `scheduler_retention_progress` (id, last_turn_id, version):
  CAS versionado depois da página/candidato interrompido, em transação curta
  separada, sem lock de lifecycle. R2-S1 relê versão antes de cada candidato;
  perdedora encerra sem checkpoint/SLA stale. `checkpoint_contended` e
  `discovery_changed` → somente contador de concorrência esperada, sem ERROR.
  **FU-1:** `budget_exhausted` também é contador informativo (cobertura parcial
  esperada), sem ERROR; watchdog de task/sweep (§9) detecta atraso real.
  Leitura/escrita do checkpoint falha → `checkpoint_unavailable`/`checkpoint_failed`
  (contador + ERROR limiar 1, sem traceback), fim da passada; worker não cai.
  Wrap incrementa version (sem ABA); duas réplicas podem repetir trabalho
  idempotente, nunca regredir checkpoint de outra. Restart após cada passada
  preserva progresso; crash antes do checkpoint repete no máximo uma página.
  Não há garantia se todos os processos morrem antes do primeiro checkpoint.
  Linhas terminais <7 dias ou scrubbed <90 dias são puladas sem lock.
  Não terminais antigos → observação agregada por passada sem prefixo lifecycle;
  nenhum texto/grafo alterado. Discovery encerra read view antes do prefixo
  canônico. Cada candidato configura READ COMMITTED antes de qualquer lock e em
  cada retry; autoridade usa `populate_existing`, sem gap locks cross-tenant.
  Candidato com erro reverte inteiro, conta sinal, segue; log só `erro_tipo`,
  nunca traceback/valores da exceção.
- Turn terminal há ≥7 dias → `draft_text=NULL`, inclusive revisões superseded,
  cancelled/dead. Turn não terminal, lane ainda referenciada, efeito não
  terminal ou accounting desconhecido/pendente conservam texto e grafo.
- Purge ≥90 dias somente após outbox dependente desaparecer pelo cleanup
  existente. Preserva `consumo_llm`, `llm_usage_reconciliation`, `mensagens`,
  `handoff_attempts` e mappings. Attempts `started`/`outcome_unknown`,
  `reconciliation_required` sem `reconciled_at`, sucesso sem consumo exato,
  reconciliação não `resolved` da sessão (ou sem sessão no mesmo agente)
  conservam evidência. Outcome e decisão de reconciliação recentes também
  exigem seu próprio horizonte de 90 dias.
- Ordem FK-safe: assignments elegíveis → scheduled_handoffs → memberships →
  generation_attempts → limpar back-links nullable do turn → revisões da
  substituta para a original → turn → receipt `scheduled` exato. Nenhuma FK
  desabilitada; dependência inesperada `1451`/`1217` reverte candidato inteiro.
  Nota/contexto ainda referenciado e receipt recente ou divergente impedem purge.
- Mensagens de histórico permanecem; associação receipt→message desaparece
  junto do receipt, sem mudar disposition. Delete REST mantém 409 enquanto
  houver turns; após purge pode prosseguir pelas checagens existentes, sem
  tombstone artificial, sempre com locks de lifecycle e conversão RESTRICT→409.
  Delete de agente continua 409 após o purge porque `consumo_llm`,
  `llm_usage_reconciliation` e `handoff_attempts` ficam retidos (S3).
- **S1/R2-S1:** motivos de retenção (exceto concorrência esperada acima)
  incrementam contador local e emitem
  `ERROR threshold=1`; não terminal agrega N observações em um único ERROR por
  passada. Health do worker: `scheduler_retention`,
  `scheduler_retention_error_threshold=1`, `scheduler_retention_health` com
  `available`, `observed_at`, `oldest_draft_terminal_age_seconds`,
  `oldest_purge_terminal_age_seconds`, `draft_sla_exceeded`, `purge_sla_exceeded`.
  Purge elegível aqui significa horizonte terminal ≥90d, não autorização de
  apagar: blockers permanecem visíveis e checados na UoW. Ausência → null;
  falha de medição → available=false + contador/ERROR, nunca zero fictício.
  **FU-1:** purge usa `idx_turn_retention_terminal`, ORDER BY terminal_at LIMIT 1.
  Draft usa somente IDs da página corrente, turn PK + uq_turn_revision_number,
  até1000 IDs/probe, após commit dos candidatos; nenhum probe global de drafts.
  Cobertura parcial tem draft_available=false/reason=coverage_incomplete e
  progresso cursor/version/rows, sem falha/readiness. Mínimo acumulado do sweep
  em memória, reiniciado em restart/quebra de continuidade; idade do último
  sweep completo e watchdog em §9. Sem DDL. Primeiro pass não depende de uptime.
  SLA: 7d/90d + `SCHEDULER_RETENTION_SLA_TOLERANCE_SECONDS=86400` (1d;
  faixa 60..604800, ainda sujeita a limite de sweep+tolerância <7d).
  **FU-1:** violação SLA incrementa contador/ERROR uma vez por episódio. Draft
  acima do SLA observado na página sinaliza imediatamente, sem esperar o fim
  da passada/sweep. Cobertura parcial não prova recuperação; só cobertura
  completa sem violação rearma episódio.
  Resultado informa scanned, examined, scrub/purge, nonterminal_observed e
  budget_exhausted. Dimensionar conforme runbook; orçamento não garante vazão
  sem medir throughput/latência real.
  Limitações: nota ambígua corrente pode reter grafo indefinidamente;
  created_at fora de ordem de id atrás da página jovem aguarda envelhecimento.

Review round 1:
- Cancelamento operador reutiliza Y1: lease vivo → pedido de cancelamento;
  vencido/ausente → `cancelled`, ambiguidade +1, `lease_expired_ambiguous`,
  settlement na mesma UoW. `turn-resolve abandon` aplica a mesma regra;
  audit append-only por efeito preserva intenção de abandono até finalização
  de I/O vivo; retenção conserva esse audit enquanto `delivery_id` existir
  (FK `SET NULL` libera purge após retenção segura do efeito). Retenção do
  intent tem contador por execução e ERROR com limiar 1.
  Cancel isolado de handoff corrente mantém park e exige
  `turn-resolve abandon` explícito para terminar o handoff.
- `/retomar` mantém gate HTTP 503 em `frozen`; primitiva interna de lifecycle
  continua fenced em qualquer modo. Alterar gate exige decisão explícita.
- Health do app expõe contadores locais de lifecycle e limiar 1, sem chamadas
  adicionais a providers; retry de transação perdida também emite log.
- Retenção mantém exclusão pré-LIMIT; linhas antigas de turns não terminais
  têm contador, resultado de cleanup, ERROR e contador no health do worker.
- Cinco primitivas internas de scheduler exigem `sessao_id` explícito; nenhuma
  delas descobre identidade por plain read antes do lock da sessão.

#### T08-5 — observabilidade agregada e código técnico do 503

- `scheduler_metrics_repo.collect` roda numa transação própria curta
  `REPEATABLE READ, READ ONLY`, só leituras consistentes (sem `FOR SHARE`/
  `FOR UPDATE`): nenhum lock de registro/gap/next-key, nunca espera writer.
  Cada scan tem `LIMIT METRIC_ROW_BOUND` (100000) e `MAX_EXECUTION_TIME`
  2000 ms; bound atingido aparece em `saturated` (nunca como valor exato).
  Queries nomeadas (EXPLAIN registrado pela matriz): `state`,
  `nonterminal_turns`, `ready_turns`, `lanes`, `attempt_outcomes`,
  `unreconciled_attempts`, `usage_reconciliation`. Sem índice novo: status de
  turn via `idx_turn_claim_lease`, bytes via `idx_turn_input_range`, attempts
  por agente via `idx_generation_agent_unknown`/
  `idx_generation_agent_reconciliation`, reconciliação via
  `idx_usage_reconciliation_due`.
- **FU-4:** `/internal/health` e `delivery_cli health` expõem bloco `scheduler`: state/version,
  `epoch_configured`/`epoch_matches` (valor do epoch só no CLI container-only),
  turns não terminais (count/bytes/caps globais), `ready_turns` e idade do
  pronto mais antigo (epoch do DB), lanes (`owned`, `lease_expired`
  recuperável, `quarantined` = predicado do discovery), unknown usage
  (`outcome_unknown`, `started`, `reconciliation_required` sem
  `reconciled_at`, agentes em quarentena, backlog `pending|processing` e
  `dead` de `llm_usage_reconciliation`) e contadores de rejeição. Agregado
  ausente/ilegível → `available=false`, sem números, `metrics_unavailable`
  com contador de transição e ERROR limiar 1. Fail closed (`/health` 503, CLI unhealthy)
  só com state lido do DB em `enabling|enabled|draining`; em `disabled`
  (default off) health/status legado permanece inalterado, e state ilegível
  fica com a checagem de DB existente (sem segundo motivo).
  No coletor compartilhado de `/health` e `/internal/health`, scheduler e compatibilidade de réplicas usam cache
  single-flight pelo TTL finito de §9; expirado o TTL, não há fallback para
  snapshot healthy. `delivery_cli health` continua sem cache. Logs de
  indisponibilidade ocorrem só em `available→unavailable`; `/health` público
  emite `ERROR` (contador, limiar 1) só em `healthy→unhealthy`, nunca por request.
  `attention_required` (lease vencido, quarentena, unknown, dead, saturação)
  com contador de transição e ERROR limiar 1. Nenhum fan-out a provider;
  nenhum telefone, conteúdo, hash, token, turn key ou instance.
- 503 do `/webhook` mantém status, `detail` e `Retry-After: 5` (auth não
  provisionada mantém detail próprio e sem Retry-After) e acrescenta `code`
  de allowlist fechada: `scheduler_draining`, `scheduler_not_enabled`
  (`disabled`/`enabling`), `scheduler_not_ready`, `cap_exceeded`,
  `epoch_mismatch`, `conflict`, `runtime_mismatch`, `runtime_frozen`,
  `feature_disabled`, `auth_not_enforced`, `auth_unavailable`,
  `gate_changed`, `ownership_unavailable`, `receipt_config_unavailable`,
  `usage_invariant`, `transaction_lost`, `internal_error`. Contador por
  código e por cap (`session_inputs`, `session_bytes`, `agent_turns`,
  `agent_bytes`, `global_turns`, `global_bytes`, `unknown`), ERROR limiar 1.
  Classificação por mensagem exata com teste de paridade contra o módulo que
  levanta; mensagem renomeada falha teste, nunca vira código genérico calado.

---

## 11. Contratos implementados e limites atuais

### 11.1 Configuração do provider — implementada

`OPENROUTER_API_KEY` obrigatória e fora de `repr`; extras OpenAI antigos ignorados. Não logar dumps de Settings nem `ValidationError.errors()`, que podem conter valores de entrada sensíveis. `OPENROUTER_BASE_URL` vem de Settings.

### 11.2 Cliente central — implementado

Resposta e resumo passam por `services/llm_client.py`. API legado e tentativa única do scheduler não têm a mesma semântica de retry; ver §6.1. Não criar cliente paralelo.

### 11.3 Schema/consumo — implementados

`ConsumoLLM`, `LLMUsageReconciliation`, modelos solicitados/reportados e limites já constam no SQL/ORM/repositórios. Schema do scheduler esperado é `3`, validado na compatibilidade; não rebaixar versão futura nem usar `create_all()` como substituto de migration.

### 11.4 Accounting — obrigatório, com limitação legado

Falha de gravação/reconciliação pode interromper a aceitação (`UsagePersistenceInvariantError`). Não tratar accounting como métrica opcional. No legado, rollback pós-provider ainda apaga uso da UoW; sinal de shutdown não é recuperação financeira. Scheduler cobre outro ciclo de vida, sujeito a gates próprios.

### 11.5 Soft limit — implementado

Isolado por agente, antes de geração e resumo; fallback neutro. Não equivale a reserva estrita nem a faturamento. Preço comercial e hard limit são operações externas ao app.

### 11.6 Validação e exposição — pendentes de release

Testes/simulações e homologações históricas não provam WhatsApp pareado, backup restaurável, abertura de ingress autenticado ou estado de produção deste SHA. Release precisa de evidência por SHA e aprovação de cada fase; não usar checklists antigos marcados como teste recém-executado.

## 12. Status das Fases e Próximos Passos

Snapshot de código: **2026-11-09**, `main` = `dev` = `5908a94`; PR #14 integrado, main promovida por fast-forward, certificação integrada p55 aceita. A edição documental atual é Onda 0, não implementação das ondas seguintes.

| Frente | Estado na base `5908a94` |
| --- | --- |
| Conversa, consumo e handoff | Implementados; E2E pareado do release ainda pendente |
| Outbox e scheduler | Implementados; direct/dispatch off e scheduler/processor off continuam defaults |
| Shutdown/retention/replica compatibility | Implementados, incluindo PR #13 |
| F0-T1–T4 | Integrados via PR #14, gates no SHA integrado aceitos em p55 |
| T6–T10 e allowlist P3-R6/R7 | Planejados no plano pós-F0 aprovado em 2026-11-07; não implementados/ativos nesta base |
| FU-R1/FU-R2/FU-R3 | Lacunas do legado permanecem; estratégia aprovada para o release comercial abaixo, ainda não executada |
| Portal multi-empresa | Planejamento separado, sem Models/API de usuários/empresas |
| WhatsApp pareado + LLM real + observação | Gates operacionais pendentes de aprovação/setup |

### 12.1 Plano pós-F0 aprovado — PLANEJADO, não entregue

- **D10:** adotar o scheduler existente antes de tráfego comercial; legado somente bootstrap/rollback sem lead real. Exige rehearsal, frota/epochs e opt-in por agente; nenhuma flag foi ativada por esta decisão.
- **FU-R2/F6:** outbox obrigatório para lead real; rollback para direct exige ingress congelado. Outbox sozinho não corrige gasto pré-commit/locks da geração não agendada.
- **D9/T7:** volume nomeado compartilhado em `/app/storage` para Rails/Sidekiq e Redis Chatwoot com volume/AOF `everysec`; RPO de fila aceito ~1s em crash, host único. Esses dois volumes adicionais ainda não existem no Compose base e entrarão no backup F2 após implementação.
- **D11/T8:** deadline único no loader compartilhado, mesmo pool de negócio, `MAX_EXECUTION_TIME` nos SELECTs; 3s é valor inicial proposto a medir, TTL segue `HEALTH_OBSERVABILITY_CACHE_SECONDS`. Nenhum deadline global novo foi implementado.
- **P3-R6/R7:** allowlist fechada por serviço no Compose resolvido para mounts/tmpfs/configs/secrets/command/entrypoint/working_dir/PYTHONPATH, com exceção explícita `./:/workspace` do bootstrap. Contrato versionado e provas novos ainda pendentes; guard R5 atual não é essa allowlist completa.

### 12.2 Ordem planejada e gates separados

1. Onda 0: conciliar estes oito documentos sobre `5908a94`; revisão humana de AGENTS e dois revisores antes de PR/integração em dev.
2. Onda 1: T6 + T7 (um PR, dois commits) e T8 (PR próprio); superfícies disjuntas, dois revisores por PR. Nenhuma dessas implementações faz parte desta documentação.
3. Onda 2: T9, gate próprio de abertura de ingress; allowlist P3 depois de T6/T7. Auth enforced não garante entrega nem ledger.
4. Onda 3: T10, procedimento único/rehearsals, sem comandos fictícios; então F1 certifica o SHA final e promove main sob autorização própria.
5. F2–F5: backup/restore/migration na cópia, configuração, stack privada sem lead e auth enforced. T7 amplia backup; T6 deve preparar Chatwoot antes dos consumidores.
6. F6: cutover outbox + rollback drill; **F6b novo, planejado:** rehearsal sintético isolado do scheduler → enable na dev com attestation/frota/epochs e opt-in só do canary. Runbook atual limita-se a ambiente isolado; ativação na dev exige aprovação própria.
7. F7: E1–E10 no canary opted-in, incluindo crash durante geração → outcome_unknown → resolução auditada. D5 (número e teto de gasto) deve ser decidida antes; canary não é tráfego comercial.
8. F8: observação inclui quarentena e unknown usage; D6 (janela) antes dessa fase. Tráfego comercial exige F7/F8 e todos os agentes comerciais opted-in, além de backup T7. VPS pública, portal/RBAC e remoção de direct seguem escopos próprios.

Conferir branch/árvore/instruções antes de trabalhar; não editar worktree de outro agente. Nenhum gate, merge ou plano autoriza por inferência provider pago, DDL operacional, abertura de ingress ou ativação. Detalhes portáteis → [estado/roadmap](docs/mapa-do-projeto/estado-e-roadmap.md).

---

## 13. Convenções Obrigatórias

### Código

- Tipagem explícita em todas as funções, parâmetros e retornos
- `async/await` em toda a camada de banco e HTTP
- `Depends(get_db)` nas rotas FastAPI — nunca instanciar `AsyncSession` manualmente
- `await db.flush()` após escrita que necessita do ID gerado na mesma transação
- `loguru` para logs — nunca `print()`
- Pydantic v2 para schemas de request/response nas rotas
- `Decimal`, nunca `float`, para custo, créditos e comparação de limite financeiro

### Dados

- Telefones armazenados sem `@s.whatsapp.net`; adaptação fica no `evolution_client.py`
- Sempre remover `[TRANSFERIR]` antes de enviar qualquer texto ao lead
- `instance_name` sempre resolvido pelo banco — nunca hardcoded ou global
- `chatwoot_inbox_id` sempre resolvido pelo agente — nunca hardcoded
- `modelo_llm` validado por allowlist — nunca aceitar ID arbitrário do cliente
- Uso LLM persistido com custo real retornado pelo provider

### Estrutura

- Lógica de banco exclusivamente em `repositories/`
- Lógica de negócio nos `services/`
- Rotas em `routers/` apenas orquestram — sem queries ORM diretas
- `llm_client.py` é o único módulo que conhece endpoint, headers e detalhes de resposta do OpenRouter
- Persistir sessões em MySQL via repositórios

---

## 14. O que Evitar

| Proibido | Motivo |
|----------|--------|
| Round-robin manual de atendentes no Python | Responsabilidade do Chatwoot |
| Variável global para `instance_name` ou `chatwoot_inbox_id` | Quebra isolamento multi-tenant |
| Variável global/hardcode de modelo LLM | Impede planos por cliente e custo controlado |
| Aceitar qualquer model ID enviado por cliente | Risco financeiro, de qualidade e compatibilidade |
| Usar `float` para custo, crédito ou franquia | Erros de precisão financeira |
| Cobrar por contagem de mensagens sem usage real | Cada modelo e chamada tem custo diferente |
| Instanciar OpenAI/OpenRouter diretamente no `agent_runner.py` | Viola centralização do provider |
| Persistir sessões em JSON | MySQL é a fonte de verdade das sessões |
| Notificar atendentes sobre handoff por WhatsApp | Chatwoot gerencia o atendimento humano |
| SQL/ORM direto em router ou service | Viola separação de camadas |
| `print()` para logging | Usar Loguru |
| `handler='humano'` em novos fluxos | Substituído por `handler='encerrada'` |
| Expor API keys, custo interno ou motivo de limite ao lead | Segurança e experiência de atendimento |
| Google Calendar/agendamentos neste ciclo | Explicitamente adiado para reduzir escopo |
| Implementar portal/auth, CRM externo ou broadcast sem ticket aprovado | Roadmap não é autorização de alterar o runtime |

---

## 15. Ordem de Prioridade para Agentes de Código

Ao gerar ou alterar código, priorizar nesta ordem:

1. **Correção arquitetural** — respeitar camadas e separação de responsabilidades
2. **Isolamento multi-tenant e financeiro** — nenhum contexto, configuração ou custo pode vazar entre clientes
3. **Integridade transacional** — `flush()`, rollback, constraints e idempotência
4. **Rastreabilidade de consumo** — toda chamada bem-sucedida deve ser auditável por agente
5. **Resiliência** — falhas de OpenRouter, Chatwoot ou Evolution não devem travar o webhook
6. **Segurança de segredos e dados** — nunca logar chaves ou conteúdo sensível sem necessidade
7. **Clareza e manutenibilidade** — código legível e funções coesas
8. **Simplicidade** — evitar abstrações desnecessárias

---

## 16. Como o Agente Deve Trabalhar

Ao sugerir ou implementar código neste repositório:

- Preservar a arquitetura existente e os nomes de módulos definidos
- Propor mudanças pequenas, coesas e compatíveis com a fase atual
- Mencionar caminhos completos dos arquivos a criar ou modificar
- Indicar explicitamente quando a alteração exige atualização conjunta de `db/init.sql`, `models.py`, repositório, schema Pydantic e AGENTS.md
- Em ambiguidades entre comportamento legado (v3/v3b) e arquitetura atual, seguir a v4.0/4.1
- OpenRouter e consumo já existem; priorizar release/hardening e o ticket aprovado, sem recriar v4.1 ou antecipar portal/scheduler
- Antes de renomear/remover colunas existentes, propor migration retrocompatível, backfill, janela de convivência e rollback
- Antes de concluir qualquer mudança de LLM, validar resposta normal, resumo de handoff, persistência de consumo e limites
- Corrigir a **classe** do defeito, não só o caso reproduzido: ao achar um problema, procurar o mesmo padrão nos outros módulos e colunas antes de entregar
- Confirmar antes de commit, push, PR, merge, deploy, gasto de provider ou migração destrutiva. Testes usam recursos descartáveis e cleanup por nome; nunca prune ou volumes operacionais
- Atualizar README/mapas quando módulos, stack, fluxos ou estado de implementação mudarem; preservar contratos §10/§16.1 e paridade de defaults §9

### 16.1 Regras de prevenção (lições dos reviews dos tickets 03/04)

Regras obrigatórias derivadas de defeitos reais encontrados em review. Cada uma
cita a classe de causa do mapa de findings do épico.

**Concorrência MySQL/InnoDB**

- **Snapshot (A1).** Em REPEATABLE READ, a primeira leitura simples fixa o read
  view. Nenhuma leitura simples antes de esperar lock; decisões tomadas depois de
  lock usam locking read com `populate_existing` ou leitura posterior a todos os
  locks. Se houver leitura preliminar, encerrar o read view (`rollback`) antes de
  travar.
- **Upgrade S→X (A2).** Nunca pedir `FOR UPDATE` numa linha em que a mesma
  transação já possa ter lock S (INSERT duplicado, checagem de FK, `FOR SHARE`).
  Use `FOR SHARE` quando só for ler, ou adquira X primeiro.
- **Locks implícitos (A3).** A ordem canônica inclui os locks que o MySQL toma
  sozinho: INSERT/UPDATE com FK toma S no pai; INSERT com chave duplicada toma S
  no índice único. Antecipe-os explicitamente na ordem canônica.
- **Transação perdida (A4).** `1205`/`1213`/`1305` em qualquer ponto da cadeia
  de exceção (`orig`, `__cause__`, `__context__`, wrappers do domínio) nunca são
  absorvidos por savepoint, fallback ou wrapper: re-lançar para retry da
  transação inteira (`db_errors.transaction_lost`). Todo `begin_nested` novo tem
  teste com deadlock real forçado dentro do savepoint.

**Estado e contratos**

- **Verdade imutável (B).** Replay, retenção e reconciliação decidem a partir de
  coluna imutável gravada na mesma UoW do efeito (ex.: `receipt_disposition`),
  nunca de estado mutável atual (`handler`, links, contadores).
- **Máquina de estados por allowlist (G).** Guards e validações de status listam
  as transições **permitidas**; tudo o mais é rejeitado. Teste cobre a matriz
  completa status × status, não só os casos proibidos conhecidos.
- **Campo ausente não vira zero (J).** Dado ausente ou ilegível de provider
  (`usage`, `choices`, custo, IDs) vira `outcome_unknown`/falha conhecida, nunca
  default `0`, string vazia ou sucesso.
- **Uma fonte de verdade (H).** Contrato duplicado (AGENTS.md × ticket, dois
  `.env.example`, ORM × `init.sql`) exige teste de paridade. Mudança de contrato
  entra primeiro no AGENTS.md, depois no código.
- **Degradação com sinal (K).** Todo caminho novo de skip, fallback ou
  degradação segura tem contador, limiar e log/health que o tornem visível.
- **Varredura limitada (F).** Discovery com `LIMIT` exclui ou rotaciona linhas
  que não progridem (quarentena, backoff); teste com `limit + 1` linhas presas
  e uma vítima que precisa progredir.

**Migrations e guards**

- **Formato exato, não existência (C).** Toda coluna/índice/FK validada pela
  migration confere tipo, signedness, nulabilidade, default, charset/collation,
  posição ordinal; índices conferem colunas, ordem e unicidade; FKs conferem
  schema, tabela e coluna referenciados e regras `ON DELETE`/`ON UPDATE`.
  Divergência falha fechada com erro nomeado; nunca reescrever, reposicionar ou
  apagar dados para "convergir".
- **Novos dependentes (C1).** Ao criar FK/RESTRICT para tabela existente, auditar
  todos os caminhos de DELETE/purge/retenção dessa tabela.
- **DDL não atômico (D).** DDL no MySQL faz commit implícito: substituição de
  trigger/guard é sempre create-before-drop com nome versionado; nenhum instante
  sem guard. Fixtures de interrupção em cada fronteira de DDL.
- **Sem normalização com perda (E).** Validação de corpo de trigger/SQL compara
  bytes exatos (digest de `ACTION_STATEMENT`); nunca lowercase, remoção de espaço
  ou de aspas dentro de literais.

**Provas e testes**

- **Verde precisa ter rodado (I).** Scripts de matriz conferem a contagem
  esperada de casos executados; loops com `docker exec` não compartilham stdin
  (`--interactive` drena here-docs); `group_concat_max_len` ajustado em checks
  de shape.
- **Mutação executada.** Prova de que um teste pega a regressão roda o teste
  contra o código mutado no banco real; grep de texto não conta.
- **Correção por classe.** Ao corrigir um finding, adicionar fixtures para os
  atributos/caminhos vizinhos da mesma classe, não só o reproduzido.

**Review**

- Todo review de mudança de schema, concorrência ou contrato tem dois revisores
  de modelos diferentes: um verifica as alegações do implementer; o outro é
  adversarial ("o que o review anterior deixou passar") e não recebe o veredito
  do primeiro como premissa.
- Reviewer reproduz cada finding antes de reportar e reconta os casos da matriz
  no log, sem confiar no resumo.

---

## 17. Comandos de Setup e Verificação

### R5 — build certificado e recibo local

P2-R5: clean_head recusa flags de índice que ocultam alterações: qualquer tag
minúscula ou S/s em git ls-files -v -z → release_index_flags_forbidden.
core.sparseCheckout efetivo (incluindo config.worktree quando habilitada) →
release_sparse_checkout_forbidden. Git mantém fsmonitor desligado e untracked all.
Preflight valida mounts dos seis runtimes Python em config base e deploy:
nenhum target /app ou descendente, exceto volume app_logs:/app/logs exato;
nenhum bind source apontando a <repo>/app ou descendente, mesmo com outro target.
Config inválida → python_code_mount_forbidden, diagnóstico sem valores.
Configs/secrets resolvidos também são mounts: definições/referências/tipos inválidos
falham fechados. Linux: config sem target → /<source>; secret sem target ou com
nome relativo → /run/secrets/<nome>; target absoluto mantém caminho absoluto.
Normalizar caminhos antes de conferir /app e descendentes. Nenhuma config/secret
herda a exceção app_logs. Fonte file que resolve a repo/app ou descendente também
é recusada, preservando política de origem. Guard não abre nem imprime payload de segredo.
README separa dev (override explícito) de release certificado: builder → preflight
→ up --no-build, com projeto/versão/candidato coerentes e gates anteriores aprovados.


Release usa `scripts/release_build.sh --project "$COMPOSE_PROJECT_NAME" --expected-commit "$(git rev-parse HEAD)"`;
acrescente `--no-cache --pull` para clean build. `docker compose build app` isolado
é somente dev/teste, não produz recibo certificado. Não source/eval de .env no builder:
projeto e commit são argumentos explícitos. Preflight exige recibo antes de up/run.

Builder exige checkout limpo (tracked/staged/untracked não ignorados), exporta
blobs Git brutos de app/ do commit exato (sem filtros/atributos externos) para
contexto privado0700 e deixa o Docker aplicar .dockerignore. Ignorados locais
não entram no contexto. Links, submodules, LFS e entradas especiais não suportados
falham fechados; nenhum fallback para contexto vivo. Contrato git-blobs-v1:
manifesto ordenado de caminhos/modos/SHA256 dos bytes exportados, hash SHA256.

Tag por projeto e versão baked permanecem. Builder obtém Image ID via --iidfile,
confere tag/label/arquivo baked, revalida HEAD/limpeza/tag antes de publicar
atomicamente recibo0600 sob git rev-parse --git-path wpp-release/<projeto>.json.
Lock por projeto; erro/cancelamento não publicam recibo novo. Preflight só lê:
commit completo/projeto/versão/contrato/hash de contexto/ID devem coincidir;
recibo ausente/inválido/antigo ou tag substituída falham. Novo host/recibo perdido
exige rebuild, nunca adoção só por label/ARG. Build sujo seguido de restauração
não recebe recibo correspondente ao seu Image ID.

Confiança: Git, builder e daemon locais. Não é assinatura independente nem prova
bit-a-bit de imagem; host malicioso/recibo fabricado e troca de tag após gate
não estão atestados. Sem commit, diff não é release certificada: testes identificam
base + manifesto/hashes; gate clean e regressão no novo SHA aguardam commit autorizado.

Preflight/builder tratam SIGTERM143 e SIGINT130: encerram/reap filhos próprios
antes do cleanup do contexto/snapshot. Snapshot0700/candidato0600 desde criação;
erro de cleanup é falha nomeada. SIGKILL/queda do host não executam finally e
podem deixar cópia privada. Recuperação externa só pelo caminho exato conhecido
do job; nunca varredura de /tmp, glob ou prune.


Clean build e testes sem bind de código (testes/contratos montados só leitura):

```bash
docker build --no-cache --pull --build-arg DELIVERY_WORKER_CODE_VERSION=outbox-v1 -t p44-app:release app/
docker run --rm --name p44-unit --network none \
  -v "$PWD:/repo:ro" -e WPP_REPO_ROOT=/repo \
  -v "$PWD/app/tests:/app/tests:ro" \
  -e DELIVERY_WORKER_CODE_VERSION=outbox-v1 \
  p44-app:release python -m unittest discover -s tests
```

Suítes MySQL exigem banco descartável e `OUTBOX_TEST_DATABASE_URL`;
a execução sem MySQL seleciona somente casos independentes de banco.


`docker compose -p NOME` contorna a validação de `name:`; não substitui o preflight,
que compara projeto efetivo com `--expected-project`. Não usar `-p` divergente.
Profiles extras só por `--profile` explícito na CLI, nunca COMPOSE_PROFILES.
Release não aceita COMPOSE_FILE/COMPOSE_ENV_FILES nem override automático.
Allowlist de controles COMPOSE_* somente de apresentação/desempenho está no §9;
qualquer controle desconhecido é recusado no candidato e no shell.
O gate compara o config default sem -f ao base sem profiles, antes do up padrão.
Execução no snapshot com --env-file <snapshot>/.env equivale à descoberta
implícita do deploy (cwd=--project-directory; .env=candidato). Prova real compara
JSON com/sem --env-file; apontar outro arquivo deve falhar. Controles de seleção
de arquivos/env/profiles são bloqueados antes dessa comparação.

Docker29.9.2 limita remote path normalizado `library/<projeto>-app` a255 caracteres
(projeto até243 neste formato); domínio docker.io/ e tag não contam nesse limite.
Tag até128. Fronteiras são comparadas ao Docker real, não margem aproximada.

Scripts de teste usam image_test_build_version: HEAD limpo → SHA; arquivo alterado,
staged ou untracked → SHA-dirty. Não rotular teste de árvore suja como SHA limpo.
Compose de provider-auth-validation passa ARG nos cinco builds e usa tag por
projeto; serviços com bind/app emitem dev_bind_code_unattested. Release continua
exigindo SHA do HEAD; construir release somente em árvore limpa.

Imagem compartilhada é `${COMPOSE_PROJECT_NAME}-app:${DELIVERY_WORKER_CODE_VERSION}`;
build de outro projeto não altera a tag desta stack.

`COMPOSE_PROJECT_NAME` é obrigatório (name com :?) e identifica stack e volumes; nunca trocar o projeto de
uma instalação existente sem conferir os nomes de volume com `docker compose config`.
Nginx publica `${NGINX_BIND_ADDRESS:-127.0.0.1}:${NGINX_HTTP_PORT:-80}` e
`${NGINX_BIND_ADDRESS:-127.0.0.1}:${NGINX_HTTPS_PORT:-443}`. Dev local usa
`NGINX_BIND_ADDRESS=127.0.0.1`; portas altas distintas permitem rehearsals isolados.
`docker compose ps -q <serviço>` resolve containers; não usar nomes fixos.
`scripts/provision_provider_auth.sh` constrói a imagem compartilhada antes de
run/up e usa somente base release, com up --no-build e run --pull never (sem bind de código).

Operação CRUD somente pela rede Docker (sem ingress público):

```bash
docker compose exec app python -c 'import httpx; r=httpx.get("http://127.0.0.1:5000/agentes"); r.raise_for_status(); print(r.status_code)'
```

Mesmo limite para `/sessoes*`, `/internal/*`, `/health/*` e docs.
`delivery-ops` permanece CLI interna. Provider-auth/probes usam `app:5000`,
nunca passam pelo Nginx. Validação real da borda:
`WPP_TEST_RESOURCE_PREFIX=p42- scripts/run_fu4_health_nginx.sh` (descartável).

Runbook operacional do scheduler de inbox: [`docs/inbox-scheduler-runbook.md`](docs/inbox-scheduler-runbook.md).

```bash
# Entrar no repositório e usar a branch de desenvolvimento
cd wpp-ai-bot-oss
git checkout main

# Atualizar e criar branch de trabalho (§0)
git pull origin main
git checkout -b feat/minha-mudanca

# Criar/atualizar ambiente local
cp .env.example .env   # editar com valores reais; nunca commitar

# Release: versão do HEAD no .env, projeto explícito e gates anteriores aprovados.
# Build único ANTES de todo fluxo up/run; nunca confiar em force-recreate para build.
scripts/release_build.sh --project "$COMPOSE_PROJECT_NAME" --expected-commit "$(git rev-parse HEAD)"
scripts/release_preflight.sh .env --expected-project "$COMPOSE_PROJECT_NAME" --phase legacy
docker compose up -d --no-build

# Desenvolvimento: bind de código somente com override explícito
docker compose -f docker-compose.yml -f docker-compose.dev.yml build app
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --no-build

# Verificações
docker compose ps
docker compose logs -f app
curl http://127.0.0.1:${NGINX_HTTP_PORT:-80}/health

# Desligar sem apagar dados
docker compose down

# ATENÇÃO: apaga volumes e dados locais
docker compose down -v

# MySQL
# Use o nome real definido em MYSQL_DATABASE; no ambiente atual de exemplo: wpp_bot-f5
docker compose exec db mysql -u root -p wpp_bot-f5

# Chatwoot: primeira inicialização
docker compose exec chatwoot bundle exec rails db:chatwoot_prepare
```

---

## 18. Fora do Escopo Atual

Sem ticket e aprovação explícita, não implementar:

- Portal multi-empresa, usuários/MFA/RBAC, `empresa_id` ou migração de tenancy: direção planejada, não runtime entregue.
- Dashboard web próprio, SSO, JWT/OAuth ou outro mecanismo de login improvisado.
- Google Calendar, agendamentos, follow-ups, CRM externo e broadcast.
- Suporte do bot WhatsApp a áudio, imagem ou documentos.
- Billing/cobrança automática, pagamentos e reserva estrita de orçamento.
- Métricas de negócio além da medição técnica de consumo LLM.
- Troca de Evolution por outro gateway ou de provider/runtime LLM.
- CI, upgrade de dependências ou infraestrutura como efeito lateral de documentação.

Testes automatizados já são parte do desenvolvimento; não estão proibidos até uma validação manual. Ativação operacional, chamadas reais e mudanças destrutivas continuam exigindo aprovação própria.
