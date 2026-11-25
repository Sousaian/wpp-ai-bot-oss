from src.routers.agentes import router as agentes_router
from src.routers.auth import router as auth_router
from src.routers.sessoes import router as sessoes_router
from src.routers.chatwoot import router as chatwoot_router

__all__ = ["agentes_router", "auth_router", "sessoes_router", "chatwoot_router"]
