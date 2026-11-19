from src.routers.agentes import router as agentes_router
from src.routers.atendentes import router as atendentes_router
from src.routers.sessoes import router as sessoes_router
from src.routers.chatwoot import router as chatwoot_router

__all__ = ["agentes_router", "atendentes_router", "sessoes_router", "chatwoot_router"]
