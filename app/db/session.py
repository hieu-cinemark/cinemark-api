"""Async engine + session factory + the `get_db` FastAPI dependency. Every
route/service gets a session already inside a transaction: committed once
if the request handler returns normally, rolled back if anything raises -
callers (routes, services, repositories) never call
session.commit()/rollback() themselves, so a service composing multiple
repository calls stays atomic under one commit at the end of the request."""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

engine = create_async_engine(settings.database_url, pool_pre_ping=True)

# expire_on_commit=False: without it, every attribute - including
# relationships already loaded during this request - is marked stale right
# after the commit in get_db() below. Touching them afterwards (e.g. a
# Pydantic response model reading a field while serializing the response)
# crashes with MissingGreenlet, since SQLAlchemy's async driver can't
# transparently re-fetch outside an awaited call. Confirmed by hand while
# building app/models - see PostEngagementSnapshot/ScrapeRunItem testing.
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def get_db() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
