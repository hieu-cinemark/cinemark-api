"""Cheap aggregate counts for the Overview page's stat row - plain COUNT(*)
queries, not a general-purpose analytics endpoint. Add more here only as
another stat card actually needs one; don't grow this into a dumping ground
for every possible aggregate."""

from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Date, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.comment import Comment
from app.models.keyword import Keyword
from app.models.movie import Movie
from app.models.post import Post
from app.schemas.stats import OverviewStats, PostsTimeseriesPoint

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("/overview", response_model=OverviewStats)
async def get_overview_stats(db: AsyncSession = Depends(get_db)) -> OverviewStats:
    # A single AsyncSession can't run several queries concurrently (no
    # asyncio.gather here), and four separate awaited round trips to a
    # cross-region Supabase instance measured ~1.5s in practice - one query
    # with four scalar subqueries costs one round trip instead of four.
    stmt = select(
        select(func.count()).select_from(Movie).scalar_subquery().label("movies"),
        select(func.count()).select_from(Keyword).scalar_subquery().label("keywords"),
        select(func.count()).select_from(Post).scalar_subquery().label("posts"),
        select(func.count()).select_from(Comment).scalar_subquery().label("comments"),
    )
    row = (await db.execute(stmt)).one()
    return OverviewStats(movies=row.movies, keywords=row.keywords, posts=row.posts, comments=row.comments)


@router.get("/posts-timeseries", response_model=list[PostsTimeseriesPoint])
async def get_posts_timeseries(
    days: int = Query(30, ge=1, le=365), db: AsyncSession = Depends(get_db)
) -> list[PostsTimeseriesPoint]:
    """Daily post count per platform for the Overview page's activity
    chart, most recent `days` days (today included). Grouped by platform
    even though only Facebook has a working spider today - free to compute
    and lets the chart pick up a second platform later with no query
    change."""
    since = date.today() - timedelta(days=days - 1)
    day_col = cast(Post.scraped_at, Date)
    stmt = (
        select(day_col.label("day"), Post.platform, func.count().label("count"))
        .where(day_col >= since)
        .group_by(day_col, Post.platform)
        .order_by(day_col)
    )
    rows = (await db.execute(stmt)).all()
    return [PostsTimeseriesPoint(day=row.day, platform=row.platform, count=row.count) for row in rows]
