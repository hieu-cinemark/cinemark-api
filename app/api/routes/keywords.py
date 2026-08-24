"""Keyword CRUD - `POST /keywords` is get-or-create rather than erroring on
a duplicate (movie_id, platform, keyword): the Platforms page in cinemark-web
calls it right before triggering a run, so submitting the same search twice
should just reuse the existing tracked keyword instead of surfacing a
conflict the user didn't cause."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.enums import Platform
from app.schemas.keyword import KeywordCreate, KeywordRead
from app.services.keywords import KeywordRepository

router = APIRouter(prefix="/keywords", tags=["keywords"])


@router.get("", response_model=list[KeywordRead])
async def list_keywords(
    movie_id: uuid.UUID | None = None,
    platform: Platform | None = None,
    db: AsyncSession = Depends(get_db),
) -> list[KeywordRead]:
    repo = KeywordRepository(db)
    filters = {}
    if movie_id is not None:
        filters["movie_id"] = movie_id
    if platform is not None:
        filters["platform"] = platform
    return await repo.list(**filters)


@router.post("", response_model=KeywordRead)
async def create_keyword(
    payload: KeywordCreate, response: Response, db: AsyncSession = Depends(get_db)
) -> KeywordRead:
    repo = KeywordRepository(db)
    existing = await repo.get_by_unique(movie_id=payload.movie_id, platform=payload.platform, keyword=payload.keyword)
    if existing is not None:
        return existing
    response.status_code = 201
    return await repo.create(**payload.model_dump())
