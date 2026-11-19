# Mapa do projeto

Base documental: `main` = `dev` = `5908a94`, snapshot de 2026-11-09. F0 integrado via PR #14, main promovida por fast-forward e certificação p55 aceita. Plano pós-F0 aprovado é planejado, não implementado; não atesta deployment.

## Por onde começar

| Pergunta | Documento |
| --- | --- |
| Definição e objetivo do produto | [README principal](../../README.md) |
| Contratos de implementação | [AGENTS.md](../../AGENTS.md), especialmente §10 e §16.1 |
| Sistemas, fronteiras e fluxos | [Arquitetura](arquitetura.md) |
| Responsabilidade de módulos/arquivos | [Código](codigo.md) |
| Tabelas, estados e fatos duráveis | [Dados](dados.md) |
| Serviços, imagens, volumes e validação | [Operação](operacao-e-validacao.md) |
| Entregue, desligado, em review ou planejado | [Estado e roadmap](estado-e-roadmap.md) |
| Cutover/rollback outbox | [Runbook delivery](../durable-delivery-runbook.md) |
| Enablement/drain scheduler | [Runbook inbox](../inbox-scheduler-runbook.md) |

## Fontes de verdade

1. Implementação → arquivos do checkout e `git rev-parse HEAD`.
2. Contratos → AGENTS.md; fontes e testes devem continuar consistentes com ele.
3. DDL/shape → `db/init.sql`; classes/acesso → ORM/repositórios.
4. Defaults → Settings, templates e testes de paridade. Environment real pode divergir, sem autorização para expô-lo.
5. Deployment → Compose efetivo, autoridades no banco e providers verificados sob procedimento autorizado.
6. Planejamento aberto → fora do repositório (AGENTS §0.8); resumo portátil no mapa de estado. Decisão aberta não é fato implementado.

Mapa não duplica bodies de triggers, schema completo, todas as flags ou comandos mutáveis: siga as fontes e os runbooks.

## Manutenção

- Mudou módulo/rota/CLI → atualizar `codigo.md` e links de entrada.
- Mudou fluxo/autoridade/I/O → atualizar `arquitetura.md`, AGENTS e testes.
- Mudou tabela/migration/lifecycle → atualizar `dados.md` e contratos SQL/ORM juntos.
- Mudou imagem/mount/volume/profile → atualizar `operacao-e-validacao.md`, README e procedimentos.
- Mergeou F0/portal ou ativou feature → verificar e atualizar `estado-e-roadmap.md`/snapshot; não manter pendência resolvida nem declarar deploy por inferência.
- Não copiar `.env`, dados de clientes, IDs operacionais ou logs com segredos.
- Documento de um SHA não prova outro SHA. Registrar validação efetiva e limitações.
