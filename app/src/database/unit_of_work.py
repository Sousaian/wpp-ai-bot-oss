from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.database.connection import AsyncSessionLocal


class UnitOfWork:
    """Explicit transaction boundary for webhook and worker orchestration.

    Unlike the legacy ``get_db`` dependency, leaving this context does not
    commit implicitly. Callers may only acknowledge queued work after
    ``commit`` has completed successfully.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] = AsyncSessionLocal,
    ) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._finished = False
        self._committed = False

    @property
    def session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("UnitOfWork has not been entered")
        return self._session

    @property
    def committed(self) -> bool:
        return self._committed

    async def __aenter__(self) -> "UnitOfWork":
        if self._session is not None:
            raise RuntimeError("UnitOfWork cannot be entered more than once")
        self._session = self._session_factory()
        return self

    async def commit(self) -> None:
        if self._finished:
            raise RuntimeError("UnitOfWork transaction is already finished")
        try:
            await self.session.commit()
        except BaseException:
            await self.session.rollback()
            self._finished = True
            raise
        self._committed = True
        self._finished = True

    async def rollback(self) -> None:
        if self._finished:
            return
        await self.session.rollback()
        self._finished = True

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if not self._finished:
            await self.session.rollback()
            self._finished = True
        await self.session.close()


async def get_unit_of_work() -> AsyncIterator[UnitOfWork]:
    """FastAPI dependency with explicit commit and rollback-only finalizer."""

    async with UnitOfWork() as unit_of_work:
        yield unit_of_work
