"""Read-only listing for scraped posts - cinemark-web's "Bài đăng" page.
movie_title/keyword_text are joined in here (not left for the FE to
resolve from movie_id/keyword_id) since a post is meaningless in a table
without knowing which movie it's about."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.enums import Platform
from app.models.keyword import Keyword
from app.models.movie import Movie
from app.models.post import Post
from app.schemas.post import PostRead

router = APIRouter(prefix="/posts", tags=["posts"])

DEFAULT_LIMIT = 100
MAX_LIMIT = 500


@router.get("", response_model=list[PostRead])
async def list_posts(
    movie_id: uuid.UUID | None = None,
    keyword_id: uuid.UUID | None = None,
    platform: Platform | None = None,
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> list[PostRead]:
    stmt = (
        select(Post, Movie.title, Keyword.keyword)
        .join(Movie, Post.movie_id == Movie.id)
        # Outer join: keyword_id is nullable (a keyword can be deleted out
        # from under a post that already cited it - ON DELETE SET NULL, see
        # app/models/post.py), so a post can legitimately have none.
        .outerjoin(Keyword, Post.keyword_id == Keyword.id)
        .order_by(Post.scraped_at.desc())
        .limit(limit)
        .offset(offset)
    )
    if movie_id is not None:
        stmt = stmt.where(Post.movie_id == movie_id)
    if keyword_id is not None:
        stmt = stmt.where(Post.keyword_id == keyword_id)
    if platform is not None:
        stmt = stmt.where(Post.platform == platform)

    rows = (await db.execute(stmt)).all()
    return [
        PostRead(
            id=post.id,
            movie_id=post.movie_id,
            movie_title=movie_title,
            keyword_id=post.keyword_id,
            keyword_text=keyword_text,
            platform=post.platform,
            external_id=post.external_id,
            url=post.url,
            author=post.author,
            content=post.content,
            media=post.media,
            like_count=post.like_count,
            reply_count=post.reply_count,
            repost_count=post.repost_count,
            quote_count=post.quote_count,
            reshare_count=post.reshare_count,
            view_count=post.view_count,
            posted_at=post.posted_at,
            scraped_at=post.scraped_at,
            keyword_match=post.keyword_match,
        )
        for post, movie_title, keyword_text in rows
    ]
