# Remoção de legado — 2026-11-19

## Escopo e limites

Branch → `chore/remove-legacy`.
Sem push, instalação de dependências, chamadas de rede ou acesso ao repositório privado.
Nenhum `.env` real foi lido ou alterado. Nenhum provider real foi chamado.

Remoção da superfície Python sem uso → executada.
Remoção de armazenamento histórico → não executada; não tratar esta entrega como eliminação integral de todo identificador legado.

## Inventário A — removido

| Arquivo/símbolo | Classificação documentada e evidência de uso |
| --- | --- |
| `app/src/services/session_manager.py` / `SessionManager` / singleton | AGENTS e mapa de código classificavam MVP JSON como legado/não expandir. `git grep` encontrou somente definição e documentação, nenhum import/caller de runtime ou teste. Fluxos atuais usam repositórios SQL. |
| `app/src/services/notificador.py` / `enviar_alerta_handoff` / `DeprecatedNotifierError` | Tombstone v3: só levantava erro de descontinuação. Nenhum import/caller; handoff atual usa Chatwoot. |
| `app/src/routers/atendentes.py` / schemas de criação, atualização e resposta / cinco rotas CRUD | AGENTS §5.5: cadastro v3b não participa do handoff v4. Única integração externa ao módulo era registro em `routers/__init__.py` e `main.py`; ambos removidos. API interna de cadastro retirada deliberadamente. |
| `app/src/database/repositories/atendente_repo.py` / CRUD e round-robin | Único consumidor era router acima; import/export do pacote removido. Não havia consumidor em orquestradores Chatwoot, Evolution, turns ou delivery. |
| `app/src/models/schemas.py` / `WebhookMessage`, `SessionInfo`, `TransferRequest`, `MessageResponse` | Mapa de código classificava arquivo como legado. Busca de módulo e dos quatro símbolos não encontrou consumidores. Schemas ativos continuam nos routers. |
| `sessao_repo.set_handler` | Nenhum caller. Único escritor de `motivo_handoff` nesse repositório; removido com seu caminho v3. Retomada e encerramento atuais usam lifecycle/encerramento próprios. |
| Parâmetro `atendente_id` e atribuições em `encerrar_locked` / `encerrar_sessao` | Busca não encontrou caller passando esse argumento. Encerramento atual e vínculo Chatwoot preservados. |

Nenhum script/fixture/teste exclusivo dos cinco módulos foi encontrado. Fixtures `Atendente` existentes são usadas como peso/locks em testes atuais de lifecycle/retention/processor, não como testes do CRUD removido; preservadas.

## Inventário B — mantido

| Item | Dependência atual / motivo | Pré-requisito para retirar |
| --- | --- | --- |
| Geração não agendada, direct e outbox não agendado | Caminhos atuais em `evolution_orchestrator`, `agent_runner`, `handoff_manager`. Defaults não ativam scheduler/dispatch. | Rehearsal/canary, frota/epochs/auth e opt-in comprovados; retirar bootstrap/rollback legado por decisão explícita. |
| `completar()` e retry do wrapper LLM | Chamadas atuais não agendadas; diferente de attempt fenced de chamada única. | Migrar todos os callers e política de uso/unknown; provar ausência de gasto duplicado. |
| Accounting pré-commit, resumo fallback, soft limit | Consumidos pelo caminho padrão e pelo handoff atual; remoção apagaria medição/proteções. | Accounting durável substituto e migração dos fluxos, sem perder ledger/reconciliação. |
| `--phase legacy` no preflight | Valida instalação default/direct e bootstrap/rollback documentados. | Retirar oficialmente essa configuração suportada e atualizar gates/rehearsal. |
| `legacy_unknown`, `legacy_automated`, `legacy_evolution`, classificação de deliveries/receipts e contexto histórico | Autoridade, dedupe, enablement/drain, retenção e histórico; ENUM/CHECK/guards protegidos. | Resolver filas antigas, provar ausência de linhas e aprovar migração auditada de contratos. Não regenerados digests. |
| Heartbeats e compatibilidade de réplicas legadas | Impedem frota incompatível de ganhar autoridade. | Drenar/retirar processos antigos e rever contrato de compatibilidade de forma auditada. |
| Health legado default-off e formato `detail` | Mantêm operação sem scheduler e resposta compatível. | Substituir consumidores/contrato HTTP e aprovar mudança de readiness. |
| Retenção legada, histórico aceito sem memberships, lifecycle não agendado e dependência `get_db` | Ainda usados por dados/rotas/fluxos suportados; comentário “legacy” não significa código morto. | Migrar dados e callers; preservar limites, lock order, fencing e proteção de deletes. |
| `modelo_openai` | Fallback real em `agent_runner`, POST/PUT e backfill de `modelo_llm` em `init.sql`. Não é apenas um nome sem uso. | Migrar/verificar todos os agentes, exigir `modelo_llm`, retirar fallback e atualizar fixtures/migration de upgrade. |
| Handler `humano` | Evolution reconhece sessões históricas e evita resposta automatizada indevida; filtro interno e ENUM ainda aceitam valor. | Resolver/migrar sessões históricas para estados atuais com backup, revisar enum e provar ausência de regressão. |
| Tabela/ORM `Atendente`, `atendente_id`, `motivo_handoff` | Histórico SQL com FK/índice e campos de leitura de sessão; fixtures atuais usam tabela em testes de locks. Escritores Python mortos removidos, armazenamento mantido. | Migração compatível de schema/ORM/fixtures e certificação MySQL. Sem ambiente de dependências funcional nesta execução, condição de coerência dos testes de schema não pôde ser comprovada. |
| `numero_notificacao` | Não envia notificações; continua campo obrigatório da API/ORM/DDL e fixtures. Mantido como dívida de contrato, não como funcionalidade de handoff. | Remover requisito público/inserção, coordenar schema/ORM e validar criação contra bancos novos e existentes. |
| Fixtures de migração e scripts com palavra legacy | Testam upgrade, fencing, gates, accounting e retenção ainda suportados. | Retirar versões/contratos testados somente após política explícita de suporte. |

**Limite de classificação:** persistência v3b e campo de notificação não se tornaram funcionalidades atuais. Sua retirada física permanece pendente de validação coordenada; não houve evidência de necessidade de regenerar digests para essas colunas. Não se alega bloqueio por trigger onde ele não foi demonstrado.

## Schema e instalações existentes

`db/init.sql` e `database/models.py` → sem alteração.
CREATE TABLE, FK, índices, ENUM, shape, schema_version, CHECKs, triggers e digests → preservados.
Instalações novas ainda recebem armazenamento histórico compatível.
Instalações existentes não recebem DROP, reescrita ou limpeza de histórico.
Nenhuma migration destrutiva automática.

### Limpeza manual opcional — somente após release de schema compatível

1. Congelar ingress e drenar workers; registrar versões e autoridade conforme runbooks.
2. Fazer backup completo consistente do MySQL e comprovar restauração em banco descartável. Backup deve incluir histórico de sessões e cadastro v3b.
3. Inventariar vínculos `sessoes.atendente_id`, handler `humano`, modelos sem `modelo_llm` e consumidores internos. Preservar/exportar histórico exigido pelo operador.
4. Preparar release separado que retire requisitos ORM/API, backfill e recriação do índice legado em `init.sql`; substituir fixtures de locks. Executar matrizes MySQL limpo, cópia restaurada, reaplicação e shape divergente.
5. Somente nessa versão, aplicar DDL manual revisado: retirar primeiro FK/índice dependentes, depois colunas sem consumidores e tabela de cadastro. Converter estados/modelos antes de retirar compatibilidade. DDL MySQL faz commit implícito: rollback exige restauração validada.
6. Revalidar readiness, accounting, auth, outbox, scheduler e histórico antes de reabrir ingress.

**Não executar DROP no release atual:** remover essas colunas/tabela agora quebra ORM e migration idempotente. Nenhum comando destrutivo pronto foi incluído como se fosse seguro hoje.

## Imagem e fronteiras

Inspecionados `app/Dockerfile`, `app/.dockerignore`, Compose e scripts de release para referências aos módulos removidos.
Dockerfile usa `COPY . .`; `.dockerignore` exclui testes, caches, `.env*` e logs. Não há allowlist nominal dos cinco arquivos a sincronizar; nenhuma mudança nesses contratos foi necessária.
Nginx permanece sem alteração. Bloqueios negativos para `/atendentes*` continuam válidos mesmo sem router, sem abrir acesso público.
Nenhuma dependência, subprocess, eval ou rede adicionada ao código de produção.

## Testes e verificações

Ambiente disponível → `Python 3.14.9`; runtime alvo documentado → Python 3.12.
Nenhuma instalação feita durante a implementação.

Primeira execução, antes de adicionar os novos testes:

```text
cd app && python -m unittest discover -s tests
Ran 87 tests in 35.494s
FAILED (failures=39, errors=37)
```

Log local → `/tmp/wpp-legacy-unittest.log`.
Erros observados incluem `ModuleNotFoundError: No module named 'sqlalchemy'`, `ModuleNotFoundError: No module named 'loguru'` e `ModuleNotFoundError: No module named 'pydantic_settings'`.
Falhas de Settings incluem subprocess que não consegue importar `pydantic_settings`; não interpretar como suite verde ou prova de ausência de regressões.

Contratos novos:

```text
cd app && python -m unittest discover -s tests -p test_legacy_removal.py
Ran 4 tests in 0.143s
OK
```

Verificam ausência dos módulos, imports, registro do router e writers/argumentos v3 no encerramento.
`git diff --check` → sem saída, exit 0.

Execução final, com novos contratos incluídos:

```text
cd app && python -m unittest discover -s tests
Ran 91 tests in 35.636s
FAILED (failures=39, errors=37)
```

Log local → `/tmp/wpp-legacy-final-unittest.log`.
Parse AST dos 66 arquivos Python de produção → `AST OK: 66 arquivos Python de produção`.
Nenhuma execução completa bem-sucedida é alegada.

## Busca final — ocorrências preservadas

Comando → `git grep -n -E 'atendente|notificador|session_manager|numero_notificacao|modelo_openai|motivo_handoff'`.

- `app/src/agents/agent_runner.py` → fallback real de modelo.
- `app/src/database/models.py` → armazenamento histórico, FK/índice e compatibilidade de modelo.
- `app/src/routers/agentes.py` → campos de API ainda suportados; remoção de requisito pendente.
- `app/src/routers/sessoes.py` → leitura de metadados históricos.
- `app/src/services/chatwoot_client.py` → atendente nativo Chatwoot, não cadastro v3b.
- `app/src/services/evolution_orchestrator.py` → mensagem de transição para atendente humano Chatwoot, não notificador removido.
- `db/init.sql` → schema/upgrade histórico preservado.
- `app/tests/test_*` → fixtures de agentes, persistência, autoridade, accounting, locks e privacidade atuais; novo teste cita nomes removidos como contrato negativo.
- `scripts/fixtures/ticket0{5,6,7}_populate.sql` → datasets de upgrade atuais.
- `scripts/check_inbox_ticket03_migrations.sh`, `scripts/run_*_mysql_matrix.sh` → contratos de migração/entrega/scheduler com schema compatível.
- `scripts/measure_fu1_retention_health.py`, `scripts/measure_outbox_claim_plan.sql`, `scripts/measure_scheduler_claim_plan.sql`, `scripts/validate_provider_auth_stack.sh` → fixtures/probes de operação suportada.
- `nginx/nginx.conf`, `nginx/nginx.dev.conf`, `scripts/check_fu4_health_nginx.py` → bloqueio público/teste negativo do antigo prefixo, preservado por segurança.
- `AGENTS.md`, `README.md`, mapas de arquitetura/dados → contrato histórico e atribuição Chatwoot; não apontam para arquivos apagados.
- Este relatório → inventário e justificativas. Arquivo de tarefa → especificação original, sem edição.

Busca de imports/referências aos módulos apagados no runtime → nenhuma ocorrência.

## Riscos e validação pendente

- Clientes internos do CRUD v3b passam a receber 404; mudança deliberada de API.
- Novas instalações ainda possuem dívida de schema histórico; limpeza integral pendente.
- Auth, health e rotas não foram validados por import/runtime devido às dependências ausentes.
- MySQL descartável, upgrade, fencing, retenção, soft limit, matriz completa, build de imagem e E2E não foram certificados nesta execução. Não houve deploy/ativação.
- Suíte completa repetida depois, com dependências e MySQL 8.0 descartável: **751 testes OK** (747 da linha de base + 4 novos), sem falhas.

## Commits

- `refactor: remove unused legacy code (atendentes CRUD, session_manager, notificador, old schemas)`
- `test: guard against reintroducing removed legacy modules`
- `docs: document legacy removal and remaining compatibility items`

Não houve alteração de schema.
