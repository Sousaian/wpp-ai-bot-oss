"""AUTH-1: container-only user and service-token administration.

    docker compose exec app python -m src.auth_cli <command> ...

No HTTP endpoint. Activation/reset and service tokens are printed exactly
once on stdout (JSON) and must be handed over out of band; only their
SHA-256 is stored. Access to this CLI is access to the container.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from fastapi import HTTPException

from src.config import settings
from src.database.connection import AsyncSessionLocal, engine
from src.services.auth import administracao
from src.services.auth.sessao import EmailInvalido
from src.utils.image_version import require_runtime_version
from src.utils.logger import setup_logger


def _emit(value: object) -> None:
    print(json.dumps(value, sort_keys=True, default=str))


def _nonblank(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("value must not be blank")
    return normalized


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.auth_cli")
    commands = parser.add_subparsers(dest="command", required=True)

    bootstrap = commands.add_parser("bootstrap-admin", help="first admin; refused once any admin exists")
    bootstrap.add_argument("--email", required=True, type=_nonblank)
    bootstrap.add_argument("--actor", required=True, type=_nonblank)

    usuario = commands.add_parser("usuario")
    usuarios = usuario.add_subparsers(dest="usuario_command", required=True)
    usuarios.add_parser("listar")
    criar = usuarios.add_parser("criar")
    criar.add_argument("--email", required=True, type=_nonblank)
    criar.add_argument("--papel", required=True, choices=["admin", "operador", "leitura"])
    criar.add_argument("--actor", required=True, type=_nonblank)
    papel = usuarios.add_parser("papel")
    papel.add_argument("usuario_id", type=int)
    papel.add_argument("--papel", required=True, choices=["admin", "operador", "leitura"])
    papel.add_argument("--actor", required=True, type=_nonblank)
    for name in ("desativar", "reativar", "desbloquear", "revogar-sessoes"):
        action = usuarios.add_parser(name)
        action.add_argument("usuario_id", type=int)
        action.add_argument("--actor", required=True, type=_nonblank)
    reset = usuarios.add_parser("reset")
    reset.add_argument("usuario_id", type=int)
    reset.add_argument("--tipo", required=True, choices=sorted(administracao.TIPOS_RESET))
    reset.add_argument("--actor", required=True, type=_nonblank)

    token = commands.add_parser("token")
    tokens = token.add_subparsers(dest="token_command", required=True)
    tokens.add_parser("listar")
    criar_token = tokens.add_parser("criar")
    criar_token.add_argument("--nome", required=True, type=_nonblank)
    criar_token.add_argument("--papel", required=True, choices=["operador", "leitura"])
    criar_token.add_argument("--dias", required=True, type=int)
    criar_token.add_argument("--actor", required=True, type=_nonblank)
    revogar = tokens.add_parser("revogar")
    revogar.add_argument("token_id", type=int)
    revogar.add_argument("--actor", required=True, type=_nonblank)
    return parser


def _token(emitido: administracao.TokenEmitido) -> dict[str, object]:
    return {
        "status": "emitido", "usuario_id": emitido.usuario_id, "tipo": emitido.tipo,
        "token": emitido.token, "expira_em": emitido.expira_em.isoformat(),
        "uso": "POST /auth/ativar {token, senha}; admin e reset_mfa seguem com /auth/totp/confirmar; exibido uma unica vez",
    }


async def run(args: argparse.Namespace, sessions=AsyncSessionLocal, emit=_emit) -> int:
    try:
        if args.command == "bootstrap-admin":
            emit(_token(await administracao.bootstrap_admin(sessions, ator=args.actor, email=args.email)))
        elif args.command == "usuario":
            command = args.usuario_command
            if command == "listar":
                emit({"usuarios": await administracao.listar_usuarios(sessions)})
            elif command == "criar":
                emit(_token(await administracao.criar_usuario(sessions, ator=args.actor, email=args.email, papel=args.papel)))
            elif command == "papel":
                emit(await administracao.alterar_papel(sessions, ator=args.actor, usuario_id=args.usuario_id, papel=args.papel))
            elif command in {"desativar", "reativar"}:
                emit(await administracao.alterar_status(
                    sessions, ator=args.actor, usuario_id=args.usuario_id, ativo=command == "reativar"))
            elif command == "desbloquear":
                emit(await administracao.desbloquear(sessions, ator=args.actor, usuario_id=args.usuario_id))
            elif command == "revogar-sessoes":
                emit(await administracao.revogar_sessoes(sessions, ator=args.actor, usuario_id=args.usuario_id))
            else:
                emit(_token(await administracao.emitir_reset(
                    sessions, ator=args.actor, usuario_id=args.usuario_id, tipo=args.tipo)))
        else:
            command = args.token_command
            if command == "listar":
                emit({"tokens": await administracao.listar_tokens_servico(sessions)})
            elif command == "criar":
                emit(await administracao.criar_token_servico(
                    sessions, ator=args.actor, nome=args.nome, papel=args.papel, dias=args.dias))
            else:
                emit(await administracao.revogar_token_servico(sessions, ator=args.actor, token_id=args.token_id))
    except administracao.Recusado as exc:
        emit({"status": "refused", "reason": exc.code})
        return 1
    except EmailInvalido:
        emit({"status": "refused", "reason": "email_invalido"})
        return 1
    except HTTPException:
        emit({"status": "error", "reason": "autenticacao_indisponivel"})
        return 1
    return 0


def main() -> int:
    require_runtime_version(settings.delivery_worker_code_version)
    setup_logger()
    args = _parser().parse_args()
    if not settings.auth_configured():
        _emit({"status": "error", "reason": "auth_keyrings_unconfigured"})
        return 2

    async def entrypoint() -> int:
        try:
            return await run(args)
        finally:
            await engine.dispose()

    try:
        return asyncio.run(entrypoint())
    except Exception as exc:
        _emit({"status": "error", "error_type": type(exc).__name__})
        return 1


if __name__ == "__main__":
    sys.exit(main())
