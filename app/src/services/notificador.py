"""Removed v3 WhatsApp-attendant notifier compatibility tombstone."""


class DeprecatedNotifierError(RuntimeError):
    pass


async def enviar_alerta_handoff(*args: object, **kwargs: object) -> None:
    raise DeprecatedNotifierError(
        "notificador.py was removed; use the durable Chatwoot handoff lifecycle"
    )
