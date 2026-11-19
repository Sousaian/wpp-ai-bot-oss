# Contribuindo com o WPP AI Bot

Obrigado pelo interesse. Este projeto é open source (Apache-2.0) e self-hosted. As regras completas para pessoas e agentes de código estão em [AGENTS.md §0](AGENTS.md#0-repositório-open-source--regras-de-trabalho). Resumo:

1. **Fluxo:** crie uma branch a partir de `main` (`feat/…`, `fix/…`, `docs/…`, `chore/…`) e abra um pull request. Não há push direto em `main`.
2. **Nada sensível:** não inclua segredos, `.env` reais, números reais de WhatsApp, conversas reais, nomes de clientes ou caminhos da sua máquina, nem em código, nem em testes, issues ou logs. Use dados sintéticos (`example.invalid`, `55119999990xx`).
3. **Testes:** rode a suíte completa com um MySQL 8.0 descartável (veja a seção de testes do [AGENTS.md](AGENTS.md)). O PR precisa passar sem falhas, e testes pulados por falta de banco não contam.
4. **Contratos:** vários testes conferem trechos de `AGENTS.md`, do README e dos runbooks. Se mudar um contrato, atualize o documento e o teste no mesmo PR.
5. **Licença:** ao contribuir, você concorda que sua contribuição é licenciada sob Apache-2.0. Dependências novas precisam de licença compatível.
6. **Segurança:** não abra issue pública para vulnerabilidade. Use o recurso "Report a vulnerability" do GitHub (Security Advisories) do repositório.
