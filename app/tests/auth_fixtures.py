"""AUTH-1 helpers for suites that exercise /agentes or /sessoes over ASGI
for another contract: the credential check itself is covered by
``test_auth_api_mysql``; here the base dependency yields a fixed principal."""

from src.services.auth.contexto import Principal, autenticar


ADMIN = Principal(tipo="usuario", id=1, papel="admin", sessao_id=1)


def autenticar_como(app, principal: Principal = ADMIN) -> None:
    async def override() -> Principal:
        return principal

    app.dependency_overrides[autenticar] = override
