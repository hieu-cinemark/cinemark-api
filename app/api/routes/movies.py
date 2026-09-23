"""Movie list + dashboard CRUD. GET still feeds the keyword picker's movie
dropdown and the Movies page; POST/PATCH/DELETE let staff add or adjust
titles without leaving the dashboard. Delete is a soft disable so existing
keywords/posts keep their movie_id."""

from __future__ import annotations

import re

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.core.errors import NotFoundError, UpstreamError, ValidationError
from app.services.d1 import MIN_COMMENTS_FOR_REPORT, create_movie, disable_movie, list_movies, update_movie
from app.services.social_topic import generate_report_for_movie, get_movie_for_report

router = APIRouter(prefix="/movies", tags=["movies"])

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class MovieOut(BaseModel):
    id: str
    title: str
    slug: str | None = None
    released_at: str | None = None
    poster_url: str | None = None
    description: str | None = None
    director: str | None = None
    cast: str | None = None
    distributor: str | None = None


class MovieCreate(BaseModel):
    title: str = Field(min_length=1)
    slug: str | None = None
    released_at: str | None = None
    poster_url: str | None = None
    description: str | None = None
    director: str | None = None
    cast: str | None = None
    distributor: str | None = None


class MovieUpdate(BaseModel):
    title: str | None = None
    slug: str | None = None
    released_at: str | None = None
    poster_url: str | None = None
    description: str | None = None
    director: str | None = None
    cast: str | None = None
    distributor: str | None = None


def _check_released_at(value: str | None) -> None:
    if value and not _DATE.match(value.strip()):
        raise ValidationError("Release date must be YYYY-MM-DD")


@router.get("", response_model=list[MovieOut])
async def movies() -> list[MovieOut]:
    rows = await list_movies()
    return [MovieOut(**row) for row in rows]


@router.post("", response_model=MovieOut)
async def create(payload: MovieCreate) -> MovieOut:
    if not payload.title.strip():
        raise ValidationError("Title must not be empty")
    _check_released_at(payload.released_at)
    row = await create_movie(payload.model_dump())
    if row is None:
        raise UpstreamError("Could not save the movie")
    return MovieOut(**row)


@router.patch("/{movie_id}", response_model=MovieOut)
async def patch(movie_id: str, payload: MovieUpdate) -> MovieOut:
    fields = payload.model_dump(exclude_unset=True)
    if "released_at" in fields:
        _check_released_at(fields.get("released_at"))
    if "title" in fields and not (fields.get("title") or "").strip():
        raise ValidationError("Title must not be empty")
    row = await update_movie(movie_id, fields)
    if row is None:
        raise NotFoundError("Movie not found")
    return MovieOut(**row)


@router.delete("/{movie_id}")
async def delete(movie_id: str) -> dict[str, bool]:
    result = await disable_movie(movie_id)
    if result is None:
        raise UpstreamError("Could not remove the movie")
    if result is False:
        raise NotFoundError("Movie not found")
    return {"ok": True}


@router.post("/{movie_id}/generate-report")
async def generate_report(movie_id: str) -> dict[str, str]:
    """Manual "Tạo report" trigger (see spider-hub-dashboard's MoviesTable) -
    runs the exact same per-movie logic as scripts/
    generate_social_topic_reports.py's daily sweep, for one movie, on
    demand. Synchronous (two sequential Bee calls - confirmed live the
    topics-clustering call alone can take 2+ minutes on a movie with a
    large comment sample) - a manually-triggered admin action with its
    own loading spinner, not worth a background-job queue for."""
    movie = await get_movie_for_report(movie_id)
    if movie is None:
        raise NotFoundError("Movie not found")
    result = await generate_report_for_movie(movie)
    if result == "insufficient_data":
        raise ValidationError(
            f"Chưa đủ bình luận đã phân loại cảm xúc để tạo report (cần tối thiểu {MIN_COMMENTS_FOR_REPORT})."
        )
    if result in ("topics_failed", "upsert_failed"):
        raise UpstreamError("Không tạo được report - thử lại sau.")
    return {"status": result}
