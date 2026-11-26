from app.repositories.d1 import keywords as keyword_repo
from app.repositories.d1 import sql, youtube_hashtag_stats
from app.services.crawl.scheduler import crawl_slots


async def test_creates_title_and_new_hashtag_keywords_only_once(monkeypatch):
    queries: list[str] = []
    created: list[tuple[str, str]] = []

    async def fake_exec(stmt, **kwargs):
        text = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        queries.append(text)
        if "FROM movies" in text:
            return [{"id": "m2", "title": "Phim Hai "}]
        return [{"movie_id": "m1", "keyword": "#BoSuaBay"}]

    async def known():
        return [
            {"movie_id": "m1", "hashtag": "bosuabay"},  # đã có (khác chữ hoa)
            {"movie_id": "m1", "hashtag": "FlyingCows"},
            {"movie_id": "m1", "hashtag": "flyingcows"},  # trùng trong cùng lượt
            {"movie_id": "gone", "hashtag": "ngungtheodoi"},
        ]

    async def get_or_create(movie_id, platform, keyword):
        assert platform == "youtube"
        if movie_id == "gone":
            return None
        created.append((movie_id, keyword))
        return {"id": "k"}

    monkeypatch.setattr(sql, "d1_exec", fake_exec)
    monkeypatch.setattr(youtube_hashtag_stats, "known_hashtags", known)
    monkeypatch.setattr(keyword_repo, "get_or_create_keyword", get_or_create)
    assert await crawl_slots.ensure_youtube_keywords() == 2
    assert created == [("m2", "Phim Hai"), ("m1", "#FlyingCows")]
    # Phim chỉ có từ khoá hashtag vẫn phải được tạo từ khoá tên.
    assert "NOT LIKE '#%'" in queries[0]
