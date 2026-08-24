"""Import every model module here so Base.metadata knows about all tables
before Alembic autogenerate or Base.metadata.create_all() runs - SQLAlchemy
only registers a table once its model class has actually been imported
somewhere."""

from app.models.base import Base
from app.models.comment import Comment
from app.models.engagement import PostEngagementSnapshot
from app.models.keyword import Keyword
from app.models.movie import Movie
from app.models.post import Post
from app.models.scope import Scope
from app.models.scrape_run import ScrapeRequestLog, ScrapeRun, ScrapeRunItem, ScrapeRunLog
from app.models.setting import Setting
from app.models.user import User

__all__ = [
    "Base",
    "Movie",
    "Keyword",
    "Post",
    "Comment",
    "PostEngagementSnapshot",
    "ScrapeRun",
    "ScrapeRunItem",
    "ScrapeRequestLog",
    "ScrapeRunLog",
    "Scope",
    "Setting",
    "User",
]
