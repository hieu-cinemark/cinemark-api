"""Read-only movie list - feeds the dashboard's inline "add a new keyword"
picker (see keywords.py's POST /<platform>/keywords). Movie CRUD itself
still lives in cinemark-scraper's admin, not here."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.d1 import list_movies

router = APIRouter(prefix="/movies", tags=["movies"])


class MovieOut(BaseModel):
    id: str
    title: str


@router.get("", response_model=list[MovieOut])
async def movies() -> list[MovieOut]:
    rows = await list_movies()
    return [MovieOut(**row) for row in rows]
