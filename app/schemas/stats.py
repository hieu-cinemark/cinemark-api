from __future__ import annotations

from pydantic import BaseModel


class PlatformStat(BaseModel):
    platform: str
    count: int
    last_scraped_at: str | None = None
    count_today: int = 0
    count_prev: int = 0


class RelatedHashtag(BaseModel):
    id: str = ""
    title: str
    count: int = 0
    bfs_depth: int = 1


class KeywordVolume(BaseModel):
    keyword_id: str
    movie_id: str = ""
    keyword: str
    platform: str
    enabled: bool
    movie_title: str | None = None
    posts_total: int = 0
    posts_today: int = 0
    posts_prev: int = 0
    comments_total: int = 0
    comments_today: int = 0
    comments_prev: int = 0
    last_scraped_at: str | None = None
    related_hashtags: list[RelatedHashtag] = []


class TimeseriesPoint(BaseModel):
    day: str
    platform: str
    count: int


class Post(BaseModel):
    id: str
    platform: str
    external_id: str
    url: str | None = None
    author: str | None = None
    content: str | None = None
    media_type: str | int | None = None
    media_url: str | None = None
    like_count: int
    reply_count: int
    repost_count: int
    quote_count: int
    reshare_count: int
    view_count: int
    posted_at: str | None = None
    scraped_at: str
    keyword_match: bool
    keyword: str | None = None
    movie_title: str | None = None


class PostPage(BaseModel):
    items: list[Post]
    total: int
    limit: int
    offset: int


class Comment(BaseModel):
    id: str
    post_id: str
    platform: str
    external_id: str
    message: str | None = None
    author_name: str | None = None
    author_id: str | None = None
    author_url: str | None = None
    author_profile_picture: str | None = None
    reactions_count: int
    replies_count: int
    parent_external_id: str | None = None
    parent_message: str | None = None
    parent_author_name: str | None = None
    posted_at: str | None = None
    scraped_at: str


class CommentWithPost(Comment):
    post_content: str | None = None
    post_url: str | None = None
    post_author: str | None = None
    movie_title: str | None = None


class CommentPage(BaseModel):
    items: list[CommentWithPost]
    total: int
    limit: int
    offset: int
