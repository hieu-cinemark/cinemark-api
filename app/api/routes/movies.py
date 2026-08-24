from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import ConflictError
from app.db.session import get_db
from app.schemas.movie import MovieCreate, MovieRead
from app.services.movies import MovieRepository

router = APIRouter(prefix="/movies", tags=["movies"])


@router.get("", response_model=list[MovieRead])
async def list_movies(db: AsyncSession = Depends(get_db)) -> list[MovieRead]:
    repo = MovieRepository(db)
    return await repo.list()


@router.post("", response_model=MovieRead, status_code=201)
async def create_movie(payload: MovieCreate, db: AsyncSession = Depends(get_db)) -> MovieRead:
    repo = MovieRepository(db)
    if await repo.get_by_slug(payload.slug) is not None:
        raise ConflictError(f"A movie with slug {payload.slug!r} already exists.")
    return await repo.create(**payload.model_dump())


@router.get("/{movie_id}", response_model=MovieRead)
async def get_movie(movie_id: uuid.UUID, db: AsyncSession = Depends(get_db)) -> MovieRead:
    repo = MovieRepository(db)
    return await repo.get_or_404(movie_id)
