from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from loguru import logger

from src.config import settings


DATABASE_URL = settings.database_url

engine = create_async_engine(
    DATABASE_URL,
    echo=False,        # True mostra queries no log — útil para debug
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True # testa conexão antes de usar — evita conexões mortas
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def get_db() -> AsyncSession:
    """Dependency para injetar sessão do banco nas rotas FastAPI"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except SQLAlchemyError as e:
            await session.rollback()
            logger.error(f"Erro na sessão do banco: {e}")
            raise


async def check_db_connection() -> bool:
    """Verifica se o banco está acessível — usado no /health"""
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Banco inacessível: {e}")
        return False
