"""Kiểm tra generate_report_for_movie của app/services/social_topic.py với bản giả cho
mọi lời gọi D1/AI (không dùng D1 hay AI thật). Những điều đáng kiểm chứng: phim dưới
MIN_COMMENTS_FOR_REPORT bị bỏ qua mà không tốn lời gọi AI nào, upsert theo movie_id
với tỉ lệ thật do SQL tính (không phải con số LLM có thể đoán), và --dry-run không bao
giờ ghi. Dùng chung cho lượt quét của scripts/generate_social_topic_reports.py và nút
"Tạo report" bấm tay trên dashboard - xem test_generate_social_topic_reports.py cho
test cấp CLI của lượt quét."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

from app.ai.tasks.report import _restore_ids, build_topics_payload
from app.services.d1 import (
    CHANNEL_MIN_POSTS,
    MIN_COMMENTS_FOR_REPORT,
    REPORT_COMMENTS_PER_POST,
    get_movie_aspect_stats,
    get_report_sample_for_movie,
)
from app.services.social_topic import generate_report_for_movie, hydrate_report_comments

MOVIE = {"id": "movie_1", "title": "Phim Test"}

SAMPLE_COMMENTS = [
    {
        "id": "c1",
        "post_id": "post_1",
        "message": "Hay quá",
        "reactions_count": 50,
        "sentiment": "positive",
        "author_name": "Lan",
        "author_url": "https://example.com/lan",
        "author_profile_picture": "https://example.com/lan.jpg",
        "post_url": "https://example.com/p1",
        "post_content": "Trailer mới",
        "post_author": "Studio",
        "platform": "facebook",
    },
    {"id": "c2", "post_id": "post_2", "message": "Dở quá", "reactions_count": 10, "sentiment": "negative"},
]

POSTS = [
    {
        "id": "post_9",
        "post_url": "https://example.com/p9",
        "post_content": "Phim Test hay nhất năm, ai chưa xem thì đi ngay",
        "post_author": "reviewer",
        "platform": "threads",
        "post_likes": 900,
        "post_comments": 40,
        "post_shares": 12,
    }
]

SAMPLE = {"comments": SAMPLE_COMMENTS, "posts": POSTS}

ASPECT_STATS = {
    "comments": 2,
    "min_n": 10,
    "trend_days": 7,
    "aspects": [
        {
            "aspect": "dien_xuat",
            "label": "Diễn xuất",
            "mentions": 12,
            "mention_share": 40.0,
            "positive": 75,
            "negative": 25,
            "trend": {"from": 90, "to": 70},
            "slipping": True,
        }
    ],
    "stage": {"hong": 3, "da_xem": 9, "khac": 1},
    "expectation": None,
    "quotes": {},
}
ACCURACY = {"labeled_at": "2026-10-07", "sample_size": 150, "sentiment_accuracy": 81}

TOPICS_RESULT = {
    "top_10_topics": [
        {"topic_name": "Diễn xuất", "sentiment": "Positive", "insight_summary": "...", "evidence_comments": []}
    ],
    "top_10_verbatims": [{"text": "Hay quá", "likes": 50, "why_it_matters": "..."}],
}


async def test_movie_below_threshold_is_skipped_without_any_bee_call() -> None:
    counts = {"positive": MIN_COMMENTS_FOR_REPORT - 1}

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_report_sample_for_movie", AsyncMock()) as mock_sample,
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock()) as mock_topics,
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=False)

    assert result == "insufficient_data"
    mock_sample.assert_not_awaited()
    mock_topics.assert_not_awaited()
    mock_upsert.assert_not_awaited()


async def test_happy_path_upserts_with_sql_computed_percentages_not_llm_guessed() -> None:
    counts = {"positive": 30, "negative": 10}  # 75% / 25%, tổng 40

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_report_sample_for_movie", AsyncMock(return_value=SAMPLE)),
        patch("app.services.social_topic.get_movie_aspect_stats", AsyncMock(return_value=ASPECT_STATS)),
        patch("app.services.social_topic._latest_accuracy", AsyncMock(return_value=ACCURACY)),
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock(return_value=TOPICS_RESULT)),
        patch("app.services.social_topic.generate_narrative", AsyncMock(return_value="Nhìn chung tích cực.")),
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock(return_value=True)) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=False)

    assert result == "generated"
    mock_upsert.assert_awaited_once()
    kwargs = mock_upsert.await_args.kwargs
    assert kwargs["movie_id"] == "movie_1"
    assert kwargs["post_count"] == 3  # 2 post_id trong SAMPLE_COMMENTS + 1 bài trong POSTS

    data = json.loads(kwargs["dashboard_data_json"])
    assert data["overall_sentiment"]["positive_percent"] == 75.0
    assert data["overall_sentiment"]["negative_percent"] == 25.0
    assert data["overall_sentiment"]["neutral_percent"] == 0.0
    assert data["overall_sentiment"]["analysis"] == "Nhìn chung tích cực."
    assert data["top_10_topics"] == TOPICS_RESULT["top_10_topics"]
    assert data["top_10_verbatims"][0]["text"] == "Hay quá"
    assert data["top_10_verbatims"][0]["author_name"] == "Lan"
    assert data["top_10_verbatims"][0]["post_url"] == "https://example.com/p1"
    assert data["aspects"] == ASPECT_STATS
    assert data["accuracy"] == ACCURACY
    assert data["sampling"] == {"classified_comments": 40, "sample_comments": 2, "sample_posts": 1, "method": "random"}


async def test_topics_call_failure_skips_the_upsert() -> None:
    counts = {"positive": 20}

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_report_sample_for_movie", AsyncMock(return_value=SAMPLE)),
        patch("app.services.social_topic.get_movie_aspect_stats", AsyncMock(return_value=ASPECT_STATS)),
        patch("app.services.social_topic._latest_accuracy", AsyncMock(return_value=ACCURACY)),
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock(return_value=None)),
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=False)

    assert result == "topics_failed"
    mock_upsert.assert_not_awaited()


async def test_dry_run_never_writes() -> None:
    counts = {"positive": 20}

    with (
        patch("app.services.social_topic.get_movie_sentiment_counts", AsyncMock(return_value=counts)),
        patch("app.services.social_topic.get_report_sample_for_movie", AsyncMock(return_value=SAMPLE)),
        patch("app.services.social_topic.get_movie_aspect_stats", AsyncMock(return_value=ASPECT_STATS)),
        patch("app.services.social_topic._latest_accuracy", AsyncMock(return_value=ACCURACY)),
        patch("app.services.social_topic.generate_topics_and_verbatims", AsyncMock(return_value=TOPICS_RESULT)),
        patch("app.services.social_topic.generate_narrative", AsyncMock(return_value="...")),
        patch("app.services.social_topic.upsert_social_topic_report", AsyncMock()) as mock_upsert,
    ):
        result = await generate_report_for_movie(MOVIE, dry_run=True)

    assert result == "generated"
    mock_upsert.assert_not_awaited()


def test_post_evidence_is_hydrated_from_the_post_itself() -> None:
    topics = {
        "top_10_topics": [
            {
                "topic_name": "Khen phim",
                "evidence_comments": [
                    {"source": "post", "id": "post_9", "text": "Phim Test hay nhất năm…", "likes": 900},
                    {"source": "comment", "id": "c1", "text": "Hay quá", "likes": 50},
                ],
            }
        ],
        "top_10_verbatims": [{"id": "post_1", "text": "Trailer mới", "likes": 3}],
    }

    result = hydrate_report_comments(topics, SAMPLE_COMMENTS, POSTS)

    post, comment = result["top_10_topics"][0]["evidence_comments"]
    assert post["source"] == "post"
    assert post["text"] == "Phim Test hay nhất năm, ai chưa xem thì đi ngay"  # bản đầy đủ, không phải bản AI thấy
    assert post["author_name"] == "reviewer"
    assert post["post_url"] == "https://example.com/p9"
    assert post["post_content"] is None
    assert comment["source"] == "comment"
    assert comment["author_name"] == "Lan"
    # Không có "source" nhưng id là bài cha của một comment trong mẫu -> vẫn nhận ra là bài viết.
    assert result["top_10_verbatims"][0]["source"] == "post"
    assert result["top_10_verbatims"][0]["post_url"] == "https://example.com/p1"


def test_topics_payload_uses_short_ids_and_maps_them_back() -> None:
    posts, comments, aliases = build_topics_payload(SAMPLE_COMMENTS, POSTS)

    # Bài trong POSTS trước, rồi bài cha của comment trong mẫu; post_2 không có nội dung nên không có trong danh sách.
    assert [p["id"] for p in posts] == ["p1", "p2"]
    assert posts[0] == {
        "id": "p1",
        "platform": "threads",
        "author": "reviewer",
        "kind": "audience",
        "text": "Phim Test hay nhất năm, ai chưa xem thì đi ngay",
        "likes": 900,
        "comments": 40,
        "shares": 12,
    }
    assert comments[0] == {"id": "c1", "post_id": "p2", "message": "Hay quá", "likes": 50, "sentiment": "positive"}
    assert comments[1]["post_id"] is None

    restored = _restore_ids(
        {
            "top_10_topics": [{"topic_name": "x", "evidence_comments": [{"id": "p1", "text": "..."}]}],
            "top_10_verbatims": [{"id": "c2", "text": "Dở quá"}, {"id": "unknown", "text": "?"}],
        },
        aliases,
    )
    assert restored["top_10_topics"][0]["evidence_comments"][0] == {"id": "post_9", "source": "post", "text": "..."}
    assert restored["top_10_verbatims"][0] == {"id": "c2", "source": "comment", "text": "Dở quá"}
    assert restored["top_10_verbatims"][1] == {"id": "unknown", "text": "?"}


async def test_report_sample_is_random_audience_first_and_caps_each_post() -> None:
    def post(post_id: str, author: str) -> dict:
        return {
            "id": post_id,
            "post_url": f"https://example.com/{post_id}",
            "post_content": "#PhimTest review của mình",
            "post_author": author,
            "platform": "tiktok",
            "post_likes": 1,
            "post_comments": 0,
            "post_shares": 0,
            "post_keyword": "Phim Test",
        }

    # "studio" có CHANNEL_MIN_POSTS bài về phim -> kênh; "media" đã đăng về nhiều phim -> kênh.
    posts = [post(f"s{i}", "studio") for i in range(CHANNEL_MIN_POSTS)]
    posts += [post("m1", "media"), post("a1", "viewer1"), post("a2", "viewer2")]

    def comment(i: int, message: str, post_id: str = "s0") -> dict:
        return {
            "id": f"c{i}",
            "post_id": post_id,
            "message": message,
            "reactions_count": 100 - i,
            "sentiment": "neutral",
        }

    comments = [comment(0, "@Nguyễn Văn A"), comment(1, "😍😍"), comment(2, "ok")]
    comments += [comment(10 + i, f"cảnh {i} quay đẹp quá") for i in range(REPORT_COMMENTS_PER_POST + 5)]
    comments.append(comment(99, "diễn viên chính đóng hay", post_id="a1"))

    async def fake_query(sql: str, params: list) -> list[dict]:
        if "FROM movies" in sql:
            return [{"title": "Phim Test"}]
        if "FROM author_reputation" in sql:
            return [{"platform": "tiktok", "author": "media"}]
        if "FROM comments" in sql:
            return [dict(c) for c in comments]
        return [dict(p) for p in posts]

    with (
        patch("app.services.d1.d1_query", fake_query),
        patch("app.services.d1.reputable_authors", AsyncMock(return_value=set())),
        patch("app.services.d1.ensure_author_reputation_table", AsyncMock()),
    ):
        sample = await get_report_sample_for_movie("movie_1")
        again = await get_report_sample_for_movie("movie_1")

    ids = {c["id"] for c in sample["comments"]}
    assert not ids & {"c0", "c1", "c2"}  # chỉ tag/emoji/quá ngắn
    assert sum(1 for c in sample["comments"] if c["post_id"] == "s0") == REPORT_COMMENTS_PER_POST
    assert "c99" in ids
    by_id = {c["id"]: c for c in sample["comments"]}
    assert by_id["c99"]["post_author"] == "viewer1" and by_id["c99"]["is_channel"] is False
    assert all(c["is_channel"] for c in sample["comments"] if c["post_id"] == "s0")
    # Bài làm "tiếng nói": chỉ của khán giả, không có studio/media.
    assert {p["id"] for p in sample["posts"]} == {"a1", "a2"}
    # Cùng phim cùng ngày -> cùng mẫu.
    assert [c["id"] for c in again["comments"]] == [c["id"] for c in sample["comments"]]


async def test_aspect_stats_counts_all_tagged_comments_with_min_n_and_trend() -> None:
    post = {
        "id": "post_1",
        "post_url": "https://example.com/p1",
        "post_content": "#PhimTest",
        "post_author": "viewer",
        "platform": "tiktok",
        "post_likes": 1,
        "post_comments": 1,
        "post_shares": 0,
        "post_keyword": "Phim Test",
    }

    def comment(i: int, aspects: list[str], day: int, sentiment: str, stage: str) -> dict:
        return {
            "id": f"c{i}",
            "post_id": "post_1",
            "message": f"bình luận số {i} về phim này",
            "reactions_count": 100 - i,
            "sentiment": sentiment,
            "aspects": json.dumps(aspects),
            "audience_stage": stage,
            "posted_at": f"2026-10-{day:02d}T10:00:00+00:00",
        }

    rows = []
    # Diễn xuất: tuần trước 10 khen; tuần này 6 khen 4 chê -> 100% -> 60%, tụt 40 điểm.
    rows += [comment(i, ["dien_xuat:+"], 2, "positive", "da_xem") for i in range(10)]
    rows += [comment(10 + i, ["dien_xuat:+"], 10, "positive", "da_xem") for i in range(6)]
    rows += [comment(20 + i, ["dien_xuat:-"], 10, "negative", "da_xem") for i in range(4)]
    # Âm thanh: chỉ 3 lượt nhắc -> dưới min_n, không báo %.
    rows += [comment(30 + i, ["am_thanh:+"], 10, "positive", "hong") for i in range(3)]
    rows += [comment(40 + i, [], 10, "neutral", "hong") for i in range(7)]
    rows.append({**comment(99, [], 10, "neutral", "khac"), "message": "@Nguyễn Văn An"})  # chỉ tag: không tính

    async def fake_query(sql: str, params: list | None = None) -> list[dict]:
        if "FROM movies" in sql:
            return [{"title": "Phim Test"}]
        if "FROM author_reputation" in sql:
            return []
        if "FROM comments" in sql:
            return [dict(r) for r in rows]
        return [dict(post)]

    with (
        patch("app.services.d1.d1_query", fake_query),
        patch("app.services.d1.reputable_authors", AsyncMock(return_value=set())),
        patch("app.services.d1.ensure_author_reputation_table", AsyncMock()),
        patch("app.services.d1.ensure_comment_insight_columns", AsyncMock()),
    ):
        stats = await get_movie_aspect_stats("movie_1")

    assert stats["comments"] == 30
    acting, sound = stats["aspects"]
    assert acting["aspect"] == "dien_xuat" and acting["mentions"] == 20 and acting["mention_share"] == 66.7
    assert acting["positive"] == 80 and acting["negative"] == 20
    assert acting["trend"] == {"from": 100, "to": 60} and acting["slipping"] is True
    assert sound["positive"] is None and sound["trend"] is None
    assert stats["stage"] == {"hong": 10, "da_xem": 20, "khac": 0}
    assert stats["expectation"] is None  # nhóm đang hóng có 3 khen, 0 chê: dưới min_n
    assert [q["id"] for q in stats["quotes"]["dien_xuat"]["khen"]] == ["c0"]  # mỗi bài một câu
