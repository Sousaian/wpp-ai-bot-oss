# Estado, objetivo e roadmap

Snapshot de código em **2026-11-09**: `main` = `dev` = `5908a94`, F0 integrado via PR #14 e main promovida por fast-forward. Certificação do SHA integrado p55 aceita. Este documento concilia a base real, não atesta deploy, ativação ou E2E real.

## Definição de produto

Plataforma open source (Apache-2.0) e self-hosted de atendimento por texto no WhatsApp, contextualizado por agente, com handoff para Chatwoot e rastreabilidade de uso/custo. Uma instalação pode ter vários números; instância Evolution identifica agente. Hoje cada instalação é configurada por um operador.

Direção planejada: espaços de trabalho e usuários autenticados numa mesma instalação, MFA/RBAC, auditoria e configuração de providers por empresa. Isso exige schema, repositórios, API e migração próprios; não existe só porque agentes atuais usam IDs distintos.

Não inclui sistema de billing nem painel próprio já entregue. Painel Evolution e Chatwoot são produtos externos; um painel próprio é outra entrega.

## Implementado em main

| Frente | Fonte/escopo |
| --- | --- |
| Conversa e histórico | MySQL, agente/instância/prompt/modelo, sessão/mensagens |
| LLM | OpenRouter, SDK compatível, allowlist e soft limit |
| Accounting | Consumo conhecido, reconciliação, modelo solicitado/reportado; limitação legado abaixo |
| Atendimento humano | Handoff Chatwoot e callback de resposta |
| Outbox | Enqueue atômico, worker fenced, TTL/retry/dependências, ops e cutover/rollback |
| Scheduler inbox 01–09 | Intake durável, lanes/turns/revisões/attempts, geração, typing, handoff/contexto, lifecycle |
| Operação scheduler | Caps, retenção, frota/readiness, diagnóstico e opt-in guardado |
| PR #13 | Deadline do shutdown/heartbeat/processo, drain HTTP, marcador e sinais de falha |

## Implementado não é ativado

- Delivery default: direct; dispatch off; outbox requer autoridade DB/epoch e configuração convergentes.
- Scheduler default: disabled, flags false, opt-in por agente false. Worker tem profile; não iniciar por padrão.
- Auth default: provisioning para bootstrap privado; enforced só após instalar/verificar segredos/contratos.
- E2E pareado/rollout/backup sobre dados reais não são consequência de merges ou testes sintéticos.

## F0 integrado e certificado na base

Origem F0 `8a902a3` aprovada por A R6 e B R7; merge PR #14 em `5908a94`, tree idêntica. Gates do SHA integrado foram executados novamente em p55: builds/recibo/versão5908a94 novos, preflight105/0, seis consumidores e integral746/0 skips/import errors/OK. Recontagem v1 falhou por intercalação stdout/stderr; v2 autorizado recontou o mesmo log imutável. Provas históricas não certificam o futuro commit desta documentação nem outro merge.

| Ticket | Implementado no código desta base |
| --- | --- |
| T1 | Diretas/lock/hashes, digest Python, imagem enxuta e versão baked |
| T2 | Código da imagem release, pins de infra, nomes/rede por projeto e override dev explícito |
| T3 | Paridade de templates, preflight candidato/config/imagem/recibo, controle env/override e prova Compose real |
| T4 | Allowlist exata Nginx; CRUD e demais caminhos não permitidos negados sem upstream |

T5 já estava em main via PR #13. F0 não ativa outbox/scheduler, não prepara automaticamente banco Chatwoot principal, não adiciona persistência completa das filas/uploads nem deadline total de readiness.

`requirements.lock`, builder canônico `release_build.sh`, preflight, recibo git-blobs-v1/Image ID, guards de índice/sparse-checkout e mounts Python/configs/secrets existem em main. Limite: contrato R5 não é atestação contra host malicioso nem allowlist completa de startup/mounts fora de /app; P3-R6/R7 continuam pendentes no plano abaixo.

## Hardening complementar planejado

| Item | Lacuna | Gate/prova esperada |
| --- | --- | --- |
| F0-T6 | Chatwoot principal sem barreira de preparo | Prepare antes Rails/Sidekiq, fresh/upgrade/idempotência, falha bloqueia consumidores |
| F0-T7 | Filas/uploads sem stores gerenciados completos | D9 aprovada; implementar volume /app/storage compartilhado + Redis AOF everysec, recriação/restore e callback/upload sintéticos |
| F0-T8 | Readiness sem deadline total/coalescing completo | D11 aprovada; loader com deadline único no mesmo pool + MAX_EXECUTION_TIME, DB/pool/half-open lentos → 503 limitado |
| F0-T9 | Gate verificável para abertura de callbacks | Bootstrap privado, enforced/contratos/frescor e prova da borda efetiva |
| F0-T10 | Procedimento de release/evidência fragmentado | README/AGENTS/runbooks ordenados, rehearsal e prova no SHA final |

Atualização documental desta branch concilia definição/inventário com o código F0 e preserva seus contratos de imagem/preflight/borda; **não conclui T10** enquanto serviços/gates T6–T9 e rehearsal definitivo não estiverem entregues.

## Lacunas do caminho legado

| Follow-up | Problema | O que não resolve sozinho |
| --- | --- | --- |
| FU-R1 | Resultado LLM pago revertido sem ledger em rollback/crash pré-commit | Deadline/sinal de shutdown ou apenas outbox |
| FU-R2 | Envio direto falho + receipt processado, sem retry automático durável | `partial_success`, devolver 500 ou repetir toda geração |
| FU-R3 | LLM/resumo sob locks na UoW não agendada | Dispatch outbox sem scheduler opt-in |

**D10 aprovada em 2026-11-07, estratégia ainda PLANEJADA:** adotar scheduler existente antes de tráfego comercial, em vez de adaptar o legado. Legado fica para bootstrap/rollback sem lead real. Não ativar scheduler nem alterar accounting por inferência; sem fallback: agente opted-in com scheduler desligado continua recusado (503). Outcome desconhecido exige resolução auditada antes de liberar agente.

F6 outbox pode resolver recuperação de **entregas commitadas**; não torna automaticamente crash-safe a geração anterior. Rollback para direct reintroduz suas limitações. Evolution continua bounded at-least-once, sem exatamente uma entrega remota garantida.

## Plano pós-F0 aprovado — PLANEJADO, não implementado

Decisões aprovadas em 2026-11-07 orientam tickets futuros; não alteram defaults, código ou estado operacional desta base.

| Decisão | Desenho aprovado para implementação/procedimento futuro | Limite/custo |
| --- | --- | --- |
| D10 | Scheduler antes de tráfego comercial; legado só bootstrap/rollback sem lead real | Scheduler nunca rodou com provider real; exige rehearsal/canary, epochs/frota/opt-in; unknown bloqueia até resolução auditada |
| FU-R2/F6 | Outbox obrigatório para lead real; rollback a direct com ingress congelado | Entrega at-least-once; não torna geração legado crash-safe |
| D9/T7 | Volume nomeado /app/storage compartilhado Rails/Sidekiq + Redis Chatwoot com volume/AOF everysec | Host único, ~1s de fila aceito em crash; dois volumes novos no backup F2, ainda ausentes no Compose |
| D11/T8 | Deadline único no loader compartilhado, mesmo pool, MAX_EXECUTION_TIME nos SELECTs | 3s inicial proposto a medir; TTL existente; pool saturado gera503; cancelamento/half-open exige prova |
| P3-R6/R7 | Allowlist versionada por serviço no Compose resolvido: mounts/tmpfs/configs/secrets/command/entrypoint/working_dir/PYTHONPATH | Exceção explícita ./ : /workspace do bootstrap (bind real `./:/workspace`); não protege contra host/Compose malicioso; ainda não implementada |
| DOC/Onda 0 | Reaplicar edição conciliada sobre5908a94 antes de T6–T10 | Revisão humana de AGENTS + dois revisores, sem alteração de runtime |

### Ondas de código e documentação futuras

1. **Onda 0:** esta conciliação de oito documentos; commit local/review não equivalem a merge ou deploy.
2. **Onda 1:** T6+T7 num PR com dois commits; T8 em PR próprio, superfícies disjuntas. Dois revisores de modelos diferentes por PR.
3. **Onda 2:** T9 (comando próprio de abertura de ingress/recibo por SHA) e allowlist P3 após T6/T7. Nada disso existe só porque o plano foi aprovado.
4. **Onda 3:** T10, procedimento único/rehearsals/paridade, sem comandos fictícios; merge em dev produz SHA para F1.

### Fases operacionais futuras, cada uma com aprovação própria

- **F1:** certificar SHA final, incluindo provas novas de T6–T10/P3; discovery recontada (746 é histórico, não contrato eterno); promover main por fast-forward.
- **F2–F3:** backup/restore/migration na cópia e configuração reconciliada; backup inclui os stores novos de T7 após implementação, preparo Chatwoot somente na cópia.
- **F4:** stack dev sem lead, após T6/T7/T8 e F1–F3; chatwoot-prepare antes de Rails/Sidekiq. Não autoriza callback aberto.
- **F5 + T9:** auth enforced, contratos/probes e gate antes de callbacks abertos. Auth não garante ledger nem entrega.
- **F6:** cutover outbox + rollback drill, sem lead em direct.
- **F6b nova:** rehearsal sintético isolado do scheduler → enabling→enabled na dev com atestação/frota/epochs e opt-in só do canary. Runbook atual limita-se a ambiente isolado; enable na dev exige aprovação própria.
- **F7:** E1–E10 no canary opted-in e crash durante geração → outcome_unknown → resolução auditada. Exige F6/F6b + D5 (número/teto de gasto); canary não é tráfego comercial.
- **F8:** observar quarentena/unknown usage; D6 (janela) antes dessa fase. Tráfego comercial exige F7/F8, agentes comerciais opted-in e backup T7 operando.

D5 e D6 permanecem abertas. D9/backend/RPO, D11/desenho de deadline e D10/estratégia scheduler foram decididas, mas não implementadas/ativadas. VPS/exposição pública exige release próprio; portal/RBAC e remoção física de direct continuam separados. CI remoto permanece decisão separada.

## Planejamento durável

Artifacts Traycer deste projeto contêm o detalhamento: `release-operacional`, `revisao-bloqueadores-2026-10-28`, T6–T10/FU-R1–R3 e `portal-multiempresa-auth`. São contexto da ferramenta, não arquivos necessários para clonar/executar o repo; este mapa resume o estado sem links absolutos de máquina como dependência do produto.

Fontes operacionais portáteis: [AGENTS](../../AGENTS.md), [delivery](../durable-delivery-runbook.md), [inbox](../inbox-scheduler-runbook.md). Mudança futura deve atualizar as três camadas: contrato, código e mapa correspondente.
