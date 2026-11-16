import asyncio

from app.repositories.d1 import transcripts
from app.services.crawl import platforms

CON_MAT = {"title": "Con Mắt Thứ Ba", "slug": "con-mat-thu-ba", "director": "Hàm Trần"}


def _draft(text):
    return {"transcript": {"language": "vi", "generated": True, "text": text, "char_count": len(text)}}


def test_mapper_keeps_transcript_out_of_raw_fields():
    payload = {"video_id": "v1", "title": "Con Mắt Thứ Ba", "transcript": {"language": "vi", "text": "xin chào"}}
    draft = platforms.get_post_mapper("youtube")(payload)
    assert draft["transcript"] == {"language": "vi", "text": "xin chào"}
    assert platforms.get_post_mapper("youtube")({"video_id": "v2", "title": "x"})["transcript"] is None


def test_transcript_film_context_from_real_samples():
    # Đoạn đầu transcript thật (16/11/2026) của video về phim và video rác dưới phim Con Mắt Thứ Ba.
    assert transcripts.transcript_has_film_context(
        _draft(
            "Đường đua phim Việt cuối năm vừa có thêm một cái tên đáng chú ý khi Con Mắt Thứ Ba chính thức tung trailer"
        ),
        CON_MAT,
    )
    assert not transcripts.transcript_has_film_context(
        _draft("Người chồng vừa mới về đến nhà, tôi đã dùng con mắt thứ ba trên đầu soi xét. Rất nhanh chóng"),
        CON_MAT,
    )
    assert not transcripts.transcript_has_film_context({"transcript": None}, CON_MAT)


def test_save_transcript_upserts_once_table_ready(monkeypatch):
    calls = []

    async def fake_query(sql, params=None, **kwargs):
        calls.append((sql.split()[0], params))
        return []

    monkeypatch.setattr(transcripts, "d1_query", fake_query)
    monkeypatch.setattr(transcripts, "_ready", False)
    ok = asyncio.run(
        transcripts.save_transcript("youtube", "v1", {"language": "vi", "text": " abc ", "char_count": 90})
    )
    assert ok
    assert [c[0] for c in calls] == ["CREATE", "INSERT"]
    assert calls[1][1][:6] == ["youtube", "v1", "vi", 0, "abc", 90]
    assert not asyncio.run(transcripts.save_transcript("youtube", "v1", {"text": "  "}))


def test_backfill_relabels_only_strict_films(monkeypatch):
    from scripts.tools import backfill_youtube_transcripts as backfill

    film_talk = {"text": "Đạo diễn Hàm Trần gọi các hồn ma trong phim là bạn"}
    story = {"text": "Người chồng vừa mới về đến nhà, tôi đã dùng con mắt thứ ba trên đầu soi xét"}
    hidden = {"content": "Con mắt thứ ba | tập 1", "relevance_label": "uncertain"}
    shown = {"content": "Con mắt thứ ba soi chồng", "relevance_label": "related"}
    assert backfill._new_label(hidden, CON_MAT, film_talk) == "related"
    assert backfill._new_label(shown, CON_MAT, story) == "uncertain"
    assert backfill._new_label(hidden, CON_MAT, story) is None
    other = {"title": "Án Mạng Karaoke", "slug": "an-mang-karaoke"}
    assert backfill._new_label(shown, other, story) is None
