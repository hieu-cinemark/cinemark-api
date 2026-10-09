"""Đọc bài và comment đã crawl từ Kafka (do các spider của spider-hub publish - xem
social_crawler/clients/kafka.py bên đó) và ghi sang D1 (xem app/services/d1.py).
Chạy như một tiến trình sống lâu riêng, tách khỏi app FastAPI:

    python -m app.workers.ingest_consumer.main

Mỗi message được xử lý độc lập, và một message lỗi (từ khoá không tra được, lỗi D1,
payload sai định dạng) được log rồi bỏ qua thay vì giết cả consumer - một message
"thuốc độc" không được làm sập việc ingest của mọi message phía sau nó.

Hai vòng lặp consumer độc lập, mỗi topic một vòng (xem _run_topic_consumer), không
phải một consumer subscribe cả hai - tách ngày 2026-09-17 sau khi xác nhận thực tế
rằng raw_posts và raw_comments dùng chung một consumer group/semaphore khiến một bài
nhiều comment (hàng trăm comment, mỗi comment một message) có thể chiếm hết chỗ
chạy song song của việc ingest bài, và ngược lại. Mỗi vòng xử lý message của mình với
mức song song có giới hạn (theo _POST_MESSAGE_CONCURRENCY /
_COMMENT_MESSAGE_CONCURRENCY) thay vì từng cái một, vì giờ mỗi message là vài lượt
gọi HTTP tới D1 chứ không phải một lần ghi DB local.

Cảm xúc comment được phân loại riêng, không làm tại chỗ: một task thứ ba,
sentiment_sweep.sweep_forever(), gắn nhãn comment mới theo lô qua Kira."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from datetime import UTC, datetime
from typing import Any

import orjson
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from app.ai.tasks.post_relevance import classify_post_relevance_kira
from app.clients.kafka import publish_ingest_decision, start_kafka_producer, stop_kafka_producer
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.clients.telegram import send_telegram_message
from app.core.config import settings
from app.core.logging import enable_file_logging, get_logger
from app.services.d1 import (
    contains_keyword,
    d1_query,
    get_keyword,
    get_post_by_external_id,
    persist_comment,
    persist_post,
)
from app.services.platforms import get_comment_mapper, get_post_mapper
from app.services.relevance_rules import (
    foreign_language_reason,
    mentions_keyword_or_title,
    mentions_other_film,
    resolve_relevance,
)
from app.services.stats_summary import bump_ingest_decision
from app.workers.ingest_consumer.sentiment_sweep import sweep_forever

logger = get_logger(__name__)

RAW_POSTS_TOPIC = "raw_posts"
RAW_COMMENTS_TOPIC = "raw_comments"
# Consumer group riêng (không phải một group "cinemark-api.ingest" dùng chung
# subscribe cả hai topic như trước đây) - xem docstring của _run_topic_consumer để biết
# vì sao một đợt dồn comment và một đợt dồn bài không được tranh nhau cùng ngân sách
# chạy song song.
CONSUMER_GROUP_POSTS = "cinemark-api.ingest.posts"
CONSUMER_GROUP_COMMENTS = "cinemark-api.ingest.comments"
# Group dùng chung duy nhất mà hai group này thay thế (xem docstring module). Ở lần
# chạy đầu tiên, cả hai group id mới đều chưa có offset nào được commit, nên nếu không
# chuyển offset từ group này sang thì chúng sẽ bắt đầu ở cuối mỗi topic
# (auto_offset_reset="latest" bên dưới) và bỏ qua những gì group cũ chưa xử lý.
_LEGACY_CONSUMER_GROUP = "cinemark-api.ingest"

# Mỗi bài đang xử lý chủ yếu chờ phán quyết của Kira, vốn được gom lô
# (app/ai/tasks/post_relevance.py: tối đa BATCH_SIZE bài mỗi lời gọi, 3 lời gọi chạy
# cùng lúc) - đủ số bài chạy song song để lấp đầy các lô đó.
_POST_MESSAGE_CONCURRENCY = 24
# Comment không gọi AI tại chỗ (cảm xúc là việc của sentiment_sweep.py) - mỗi comment
# chỉ là một lần gọi mapper + một lần ghi D1, đủ rẻ để mức song song cao hơn thực sự
# được dùng tới.
_COMMENT_MESSAGE_CONCURRENCY = 24

# Một loạt bài bị loại âm thầm (chưa đăng ký mapper nền tảng, producer gửi payload sai
# định dạng, một từ khoá bị tắt/xoá giữa chừng) trước đây chỉ hiện thành một dòng log
# WARNING không ai theo dõi - đúng cách mà việc thiếu mapper của TikTok đã bị bỏ sót cho
# tới khi có người tình cờ xem dashboard. Phần này biến một loạt *kéo dài* của một lý
# do loại cụ thể thành cảnh báo Telegram, mà không báo động theo từng message một khi
# đã biết nền tảng đó đang hỏng.
DROP_ALERT_THRESHOLD = 10
# Cửa sổ trượt, không phải đếm cả đời - "10 bài bị loại trong giờ qua" là tín hiệu có
# ý nghĩa; "10 bài bị loại từ lúc key này xuất hiện lần đầu, có khi vài tuần trước" thì
# không. Reset bằng cách đặt lại TTL của key mỗi khi một cửa sổ mới bắt đầu (lần tăng
# đầu tiên sau khi hết hạn/tạo mới).
DROP_COUNTER_TTL_SECONDS = 3600

_DROP_ALERT_TEXT = {
    "mapper": "unregistered platform mapper (see app/services/platforms.py)",
    "missing_keyword_id": "posts arriving with no keyword_id (producer bug?)",
    "unknown_keyword_id": "posts referencing an unknown/disabled keyword_id",
    "d1_write_failed": "D1 write failed (see app/services/d1.py for details)",
}


# Các phim đang theo dõi (tên phim cho relevance_rules.mentions_other_film, thông tin
# cho prompt của Kira) - một bảng nhỏ chỉ đổi khi có người thêm phim, nên cache ngắn là
# đủ.
_TRACKED_MOVIES_TTL_SECONDS = 300.0
_tracked_movies_cache: tuple[float, dict[str, dict[str, Any]]] | None = None


async def _tracked_movies() -> dict[str, dict[str, Any]]:
    global _tracked_movies_cache
    now = time.monotonic()
    if _tracked_movies_cache is not None and now - _tracked_movies_cache[0] < _TRACKED_MOVIES_TTL_SECONDS:
        return _tracked_movies_cache[1]
    try:
        rows = (
            await d1_query(
                'SELECT id, title, slug, director, "cast", distributor, released_at, description FROM movies',
                quiet=True,
            )
            or []
        )
    except Exception as exc:  # noqa: BLE001 - quy tắc/Kira tạm đứng ngoài lượt này
        logger.warning("tracked_movies_load_failed", error=exc)
        return _tracked_movies_cache[1] if _tracked_movies_cache else {}
    movies = {r["id"]: {**r, "keywords": []} for r in rows if r.get("title")}
    # Mọi từ khoá đang bật của mỗi phim, cho cổng mentions_keyword_or_title. Lỗi thì chỉ còn từ
    # khoá đã tìm ra bài + tên phim.
    try:
        for row in await d1_query("SELECT movie_id, keyword FROM keywords WHERE enabled = 1", quiet=True) or []:
            if row["movie_id"] in movies and row.get("keyword"):
                movies[row["movie_id"]]["keywords"].append(row["keyword"])
    except Exception as exc:  # noqa: BLE001 - cổng vẫn chạy với từ khoá của bài
        logger.warning("tracked_keywords_load_failed", error=exc)
    _tracked_movies_cache = (now, movies)
    return movies


async def _tracked_titles() -> list[str]:
    return [m["title"] for m in (await _tracked_movies()).values()]


async def _note_drop(platform: str, reason: str, **context: Any) -> None:
    """Tăng nguyên tử bộ đếm cửa sổ trượt của (platform, reason) này và bắn đúng một cảnh
    báo Telegram ngay khi vượt DROP_ALERT_THRESHOLD - không phải mỗi message một lần sau
    đó, để sự cố đang kéo dài không spam kênh sau khi đã được báo."""
    client = get_redis_client()
    key = f"{REDIS_KEY_PREFIX}ingest_drop:{platform}:{reason}"
    count = await client.incr(key)
    if count == 1:
        await client.expire(key, DROP_COUNTER_TTL_SECONDS)
    if count == DROP_ALERT_THRESHOLD:
        details = " | ".join(f"{k}={v}" for k, v in context.items())
        await send_telegram_message(
            f"🚨 Ingest drop alert: {platform} - {_DROP_ALERT_TEXT[reason]}\n"
            f"{count} drops in the last {DROP_COUNTER_TTL_SECONDS // 60}m\n{details}"
        )


async def _decide(
    *, platform: str | None, post_id: Any, keyword_id: Any, decision: str, reason: str, **extra: Any
) -> None:
    """Mỗi bài một event trên topic ingest_decisions - được app/workers/lake_writer lưu lên
    lake R2, nên mọi quyết định giữ/loại và lý do đều có hồ sơ. Đồng thời cộng phễu ingest
    theo giờ cho dashboard (stats_summary.bump_ingest_decision)."""
    await bump_ingest_decision(platform, decision, reason)
    await publish_ingest_decision(
        {
            "post_id": post_id,
            "platform": platform,
            "keyword_id": keyword_id,
            "decision": decision,
            "reason": reason,
            "decided_at": datetime.now(UTC).isoformat(),
            **extra,
        }
    )


async def _drop(*, platform: str | None, post_id: Any, reason: str, keyword_id: Any = None, **extra: Any) -> None:
    """Ghi quyết định loại bài lên topic ingest_decisions. Không còn lưu gì trong D1 nữa:
    lake writer (app/workers/lake_writer/main.py) giữ payload thô từ raw_posts dưới
    bronze/entity=posts/ và quyết định này dưới bronze/entity=decisions/ - khi phát lại
    thì join hai bên theo post_id. Các bài bị loại từ trước khi có lake (bảng
    dropped_posts cũ của D1) được lưu trữ trên R2 dưới backfill/entity=dropped_posts/."""
    await _decide(platform=platform, post_id=post_id, keyword_id=keyword_id, decision="dropped", reason=reason, **extra)


async def handle_post(payload: dict[str, Any]) -> None:
    platform = payload.get("platform")
    # Payload TikTok mang id ở trường video_id - không có phương án dự phòng này thì mọi
    # quyết định ingest của TikTok đều gửi đi với post_id=None và không bao giờ join lại
    # được với bài của nó trong lake (app/lake/silver.py).
    post_id = payload.get("post_id") or payload.get("video_id")

    mapper = get_post_mapper(platform)
    if mapper is None:
        logger.warning("post_unregistered_platform", platform=platform, post_id=post_id)
        await _note_drop(platform, "mapper", post_id=post_id)
        await _drop(platform=platform, post_id=post_id, reason="mapper")
        return

    keyword_id = payload.get("keyword_id")
    if not keyword_id:
        logger.warning("post_missing_keyword_id", platform=platform, post_id=post_id)
        await _note_drop(platform, "missing_keyword_id", post_id=post_id)
        await _drop(platform=platform, post_id=post_id, reason="missing_keyword_id")
        return

    keyword = await get_keyword(keyword_id, platform=platform)
    if keyword is None:
        logger.warning("post_unknown_keyword_id", platform=platform, keyword_id=keyword_id, post_id=post_id)
        await _note_drop(platform, "unknown_keyword_id", post_id=post_id, keyword_id=keyword_id)
        await _drop(platform=platform, post_id=post_id, reason="unknown_keyword_id", keyword_id=keyword_id)
        return

    draft = mapper(payload)

    # Quy tắc chạy trước, trước khi lối tắt từ khoá bên dưới kịp nhận bài (xem
    # app/services/relevance_rules.py cho các trường hợp đã đo). Mọi bài bị quy tắc loại
    # đều đi qua _drop() và nằm trong luồng bronze/entity=decisions/ của lake, nên một lần
    # quy tắc sai vẫn khôi phục được bằng cách phát lại file NDJSON ingest_decisions tương
    # ứng.
    foreign = foreign_language_reason(draft.get("content"), payload.get("text_language"))
    if foreign:
        logger.info(
            "post_dropped_foreign_language", platform=platform, post_id=post_id, keyword_id=keyword_id, rule=foreign
        )
        await _drop(platform=platform, post_id=post_id, reason="non_vietnamese", keyword_id=keyword_id, rule=foreign)
        return

    # Bài phải chứa đầy đủ từ khoá (hoặc dạng hashtag của nó) hoặc đầy đủ tên phim - kết quả
    # tìm kiếm của nền tảng có cả bài không hề nhắc tới phim, và Kira từng gán "related" cho
    # chúng (xem relevance_rules.mentions_keyword_or_title). Loại trước khi tốn lời gọi Kira.
    movie = (await _tracked_movies()).get(keyword.get("movie_id")) or {"title": keyword.get("movie_title")}
    movie_keywords = [keyword["keyword"], *movie.get("keywords", [])]
    if not mentions_keyword_or_title(draft.get("content"), movie_keywords, keyword.get("movie_title")):
        other_film = mentions_other_film(
            draft.get("content"), keyword.get("movie_title"), keyword["keyword"], await _tracked_titles()
        )
        reason = "other_film" if other_film else "keyword_absent"
        logger.info(
            "post_dropped_other_film" if other_film else "post_dropped_keyword_absent",
            platform=platform,
            post_id=post_id,
            keyword_id=keyword_id,
            other_film=other_film,
        )
        await _drop(platform=platform, post_id=post_id, reason=reason, keyword_id=keyword_id, other_film=other_film)
        return

    # Kira phân loại mọi bài đã qua được các quy tắc. Kiểm tra chuỗi con theo từ khoá chỉ
    # là phương án dự phòng khi Kira không đưa ra phán quyết - bị tắt trên dashboard, vượt
    # settings.kira_post_relevance_daily_cap, hoặc lỗi - để một lần sự cố không bao giờ
    # loại hay giấu đi những bài lẽ ra được giữ.
    relevance_label = None
    relevance_confidence = None
    has_keyword = contains_keyword(draft.get("content"), keyword["keyword"])

    # settings.kira_ingest_relevance tắt: ingest không chờ Kira (qwen có lúc 60-180 giây một lô, đủ làm
    # Kafka tồn hàng chục nghìn message) - bài vào theo tín hiệu phim/từ khoá như lúc Kira không kết luận, rồi
    # scripts/relabel_post_relevance.py gán lại nhãn Kira ở nền.
    verdict = (
        await classify_post_relevance_kira(
            content=draft.get("content"),
            movie=movie,
            keyword=keyword["keyword"],
            platform=platform,
            other_titles=await _tracked_titles(),
        )
        if settings.kira_ingest_relevance
        else None
    )
    if verdict is not None:
        relevance_label = verdict["label"]
        relevance_confidence = verdict["confidence"]
    if relevance_label == "not_related":
        # Quyết định được ghi qua _drop() -> topic ingest_decisions -> lake
        # (bronze/entity=decisions/). Khôi phục một nhãn sai bằng cách phát lại NDJSON trong
        # lake, không phải từ D1.
        logger.info(
            "post_dropped_irrelevant",
            platform=platform,
            post_id=post_id,
            keyword_id=keyword_id,
            has_keyword=has_keyword,
            confidence=relevance_confidence,
            reason=verdict["reason"],
        )
        await _drop(
            platform=platform,
            post_id=post_id,
            reason="kira_irrelevant",
            keyword_id=keyword_id,
            confidence=relevance_confidence,
            kira_reason=verdict["reason"],
        )
        return
    # Kira "uncertain" / không kết luận / phim "chặt": xem relevance_rules.resolve_relevance.
    ai_relevant, relevance_label, context = resolve_relevance(
        relevance_label,
        draft.get("content"),
        movie,
        has_keyword=has_keyword,
        strict=movie.get("slug") in settings.strict_relevance_movies,
    )
    no_film_context = ai_relevant is False
    ok = await persist_post(
        movie_id=keyword["movie_id"],
        keyword_id=keyword_id,
        keyword=keyword["keyword"],
        platform=platform,
        draft=draft,
        ai_relevant=ai_relevant,
        relevance_label=relevance_label,
        relevance_confidence=relevance_confidence,
    )
    if not ok:
        await _note_drop(platform, "d1_write_failed", post_id=post_id)
        await _drop(platform=platform, post_id=post_id, reason="d1_write_failed", keyword_id=keyword_id)
        return
    logger.info("post_persisted", platform=platform, post_id=draft.get("external_id"))
    # kira_related, context_<lý do> khi tín hiệu phim quyết định thay Kira, hoặc no_verdict khi Kira
    # đứng ngoài (tắt, vượt hạn mức ngày, lỗi) và chỉ mình kiểm tra từ khoá quyết định.
    await _decide(
        platform=platform,
        post_id=post_id,
        keyword_id=keyword_id,
        decision="kept",
        reason="no_film_context"
        if no_film_context
        else f"context_{context}"
        if context
        else (f"kira_{relevance_label}" if relevance_label else "no_verdict"),
        confidence=relevance_confidence,
        has_keyword=has_keyword,
    )


async def handle_comment(payload: dict[str, Any]) -> None:
    platform = payload.get("platform")
    external_post_id = payload.get("post_id")

    mapper = get_comment_mapper(platform)
    if mapper is None:
        # Không lưu trữ comment của nền tảng chưa đăng ký: bài cha (mang ngữ cảnh nội dung của
        # comment) đã được lưu bền vững và lấy lại được theo post_id, nên mất một comment của
        # nền tảng lạ không phải loại mất mát không khôi phục được như mất cả một bài.
        logger.warning("comment_unregistered_platform", platform=platform, post_id=external_post_id)
        return

    post = await get_post_by_external_id(platform, external_post_id) if external_post_id else None
    if post is None:
        # Bài chứa comment này chưa có trong D1 (hoặc sẽ không bao giờ có - ví dụ bị
        # content_too_short bỏ qua, xem persist_post) - comment không thể tồn tại nếu không có
        # dòng cha (post_id có khoá ngoại NOT NULL, xem schema.ts của cinemark-scraper), nên
        # không có gì để gắn nó vào.
        logger.warning("comment_unknown_post", platform=platform, post_id=external_post_id)
        return

    draft = mapper(payload)
    # Ở đây sentiment vẫn là NULL - sentiment_sweep.py phân loại comment mới theo lô qua
    # Kira trong vòng một hai phút.
    ok = await persist_comment(post_id=post["id"], platform=platform, draft=draft, sentiment=None)
    if not ok:
        logger.warning("d1_comment_persist_failed", platform=platform, post_id=external_post_id)
        return
    logger.info("comment_persisted", platform=platform, post_id=external_post_id)


class _OffsetTracker:
    """Chỉ commit một offset khi mọi message tới và bao gồm nó đã thực sự xử lý xong -
    không theo bộ hẹn giờ mặc định của aiokafka, vốn commit dựa trên việc vòng
    `async for` đã duyệt tới đâu, bất kể task chạy song song cho message đó đã xong hay
    chưa. Không có cái này, một message được giao cho task còn đang chạy dở (ví dụ đang
    kẹt trong retry/backoff của Kira) có thể bị bộ hẹn giờ commit offset trước khi task
    xong; crash trong khoảng đó là mất message vĩnh viễn, vì Kafka không giao lại một
    offset đã commit qua rồi. Message trong một partition được giao theo đúng thứ tự
    aiokafka trả ra, nên một hàng đợi offset đã giao theo từng partition cộng một tập các
    offset đã xong là đủ để tìm offset đã xong *liên tục* cao nhất - offset duy nhất an
    toàn để commit."""

    def __init__(self) -> None:
        self._dispatched: dict[TopicPartition, deque[int]] = defaultdict(deque)
        self._finished: dict[TopicPartition, set[int]] = defaultdict(set)

    def dispatched(self, tp: TopicPartition, offset: int) -> None:
        self._dispatched[tp].append(offset)

    def finished(self, tp: TopicPartition, offset: int) -> dict[TopicPartition, int]:
        """Đánh dấu một offset đã xong và trả về các mục {partition: next_offset} vừa trở nên an
        toàn để commit nhờ đó."""
        self._finished[tp].add(offset)
        queue = self._dispatched[tp]
        done = self._finished[tp]
        advanced = None
        while queue and queue[0] in done:
            done.discard(queue[0])
            advanced = queue.popleft()
        if advanced is None:
            return {}
        return {tp: advanced + 1}


async def _process_message(
    message: Any, semaphore: asyncio.Semaphore, tracker: _OffsetTracker, consumer: AIOKafkaConsumer
) -> None:
    tp = TopicPartition(message.topic, message.partition)
    async with semaphore:
        try:
            if message.topic == RAW_POSTS_TOPIC:
                await handle_post(message.value)
            elif message.topic == RAW_COMMENTS_TOPIC:
                await handle_comment(message.value)
        except Exception as exc:
            logger.error("ingest_message_failed", topic=message.topic, error=str(exc))
        finally:
            # Commit bất kể thành công/thất bại - một message mà tiến trình này không xử lý được
            # (payload lỗi, từ khoá không tra được) được cố ý bỏ qua, không thử lại mãi (xem
            # docstring module); chỉ crash giữa lúc xử lý mới nên giao lại nó.
            to_commit = tracker.finished(tp, message.offset)
            if to_commit:
                try:
                    await consumer.commit(to_commit)
                except KafkaError as exc:
                    logger.error("kafka_commit_failed", error=str(exc))


async def _seed_offsets_from_legacy_group(consumer: AIOKafkaConsumer, topic: str, group_id: str) -> None:
    """Migrate một lần cho việc tách group mô tả trong docstring module: với mỗi partition
    mà consumer (mới) này vừa được giao, nếu group mới chưa có offset đã commit nhưng
    group cinemark-api.ingest đã nghỉ hưu thì có, seek tới đó và commit - để sau khi tách
    thì tiếp tục từ chỗ consumer dùng chung cũ dừng lại thay vì phát lại cả topic.
    Partition mà group cũ cũng không có offset (cluster mới, hoặc consumer cũ chưa bao
    giờ tới đó) thì để nguyên và rơi về auto_offset_reset như bình thường."""
    assignment = consumer.assignment()
    if not assignment:
        return
    unseeded = [tp for tp in assignment if await consumer.committed(tp) is None]
    if not unseeded:
        return

    legacy = AIOKafkaConsumer(bootstrap_servers=settings.kafka_bootstrap_servers, group_id=_LEGACY_CONSUMER_GROUP)
    await legacy.start()
    try:
        seeded: dict[TopicPartition, int] = {}
        for tp in unseeded:
            offset = await legacy.committed(tp)
            if offset is not None:
                consumer.seek(tp, offset)
                seeded[tp] = offset
    finally:
        await legacy.stop()

    if seeded:
        await consumer.commit(seeded)
        logger.info(
            "ingest_consumer_offsets_migrated",
            topic=topic,
            group=group_id,
            partitions={f"{tp.topic}-{tp.partition}": offset for tp, offset in seeded.items()},
        )


async def _run_topic_consumer(topic: str, group_id: str, concurrency: int) -> None:
    """Một vòng lặp consumer độc lập cho một topic (bài hoặc comment) - raw_posts và
    raw_comments từng dùng chung một consumer group và một ngân sách chạy song song, nên
    một đợt dồn comment (một bài có thể có hàng trăm) xếp hàng sau đúng các chỗ semaphore
    mà bài cần, và ngược lại. Consumer group riêng cũng có nghĩa là lag của từng topic
    được thấy độc lập qua kafka-consumer-groups.sh, thay vì một con số trộn chung không
    nói được topic nào thực sự đang chậm."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=orjson.loads,
        # Group không có offset đã commit dùng được thì bắt đầu ở CUỐI, không phải đầu: ngày
        # 2026-09-29 một lần khôi phục Kafka làm cả hai group mất vị trí và "earliest" đã xếp
        # lại khoảng 264 nghìn message đã ingest để chạy qua Kira và D1. Bỏ qua là kiểu lỗi rẻ
        # hơn - nếu cần phát lại gì, tự đặt lại offset của group bằng tay (hoặc đọc từ lake).
        auto_offset_reset="latest",
        # Commit thủ công theo từng message (xem _OffsetTracker) thay vì auto-commit theo bộ
        # hẹn giờ mặc định, vốn không gắn với việc task song song của message đó đã thực sự xong
        # hay chưa.
        enable_auto_commit=False,
    )

    try:
        await consumer.start()
    except KafkaError as exc:
        logger.error("kafka_connection_error", topic=topic, error=str(exc))
        return
    try:
        await _seed_offsets_from_legacy_group(consumer, topic, group_id)
    except KafkaError as exc:
        logger.error("kafka_connection_error", topic=topic, error=str(exc))
        await consumer.stop()
        return

    logger.info("ingest_consumer_started", topic=topic, group=group_id, message_concurrency=concurrency)
    message_semaphore = asyncio.Semaphore(concurrency)
    tracker = _OffsetTracker()
    pending: set[asyncio.Task[None]] = set()
    try:
        async for message in consumer:
            tracker.dispatched(TopicPartition(message.topic, message.partition), message.offset)
            task = asyncio.create_task(_process_message(message, message_semaphore, tracker, consumer))
            pending.add(task)
            task.add_done_callback(pending.discard)
            # Trần cao hơn khá nhiều so với mức song song (không bằng nó) - semaphore vốn đã giới
            # hạn số task chạy *cùng lúc*; cái này chỉ ngăn bản thân `pending` phình ra không giới
            # hạn nếu vòng đọc Kafka đưa vào nhanh hơn tốc độ task xong.
            if len(pending) >= concurrency * 4:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
    except KafkaError as exc:
        logger.error("kafka_connection_error", topic=topic, error=str(exc))
    finally:
        if pending:
            await asyncio.wait(pending)
        await consumer.stop()
        logger.info("ingest_consumer_stopped", topic=topic)


async def run() -> None:
    """Chạy song song vòng lặp consumer của cả hai topic, cộng lượt quét cảm xúc comment,
    trong cùng một tiến trình này. Cùng kiểu "huỷ cái còn sống rồi raise lại" như run()
    trong crawl_request_consumer.py của spider-hub (xem docstring module đó để biết đầy
    đủ lý do) - nếu một vòng lặp thoát bất ngờ, vòng còn lại bị huỷ và tiến trình thoát
    với mã khác 0 để Restart=on-failure của systemd khởi động lại cả hai, thay vì để
    việc ingest của một topic âm thầm dừng mãi trong khi tiến trình trông vẫn "đang
    chạy"."""
    # Trước các vòng lặp: các quyết định publish lúc nó đang sập sẽ bị bỏ qua âm thầm.
    await start_kafka_producer()
    try:
        await _run_loops()
    finally:
        await stop_kafka_producer()


async def _run_loops() -> None:
    loops = {
        asyncio.create_task(
            _run_topic_consumer(RAW_POSTS_TOPIC, CONSUMER_GROUP_POSTS, _POST_MESSAGE_CONCURRENCY), name="posts"
        ),
        asyncio.create_task(
            _run_topic_consumer(RAW_COMMENTS_TOPIC, CONSUMER_GROUP_COMMENTS, _COMMENT_MESSAGE_CONCURRENCY),
            name="comments",
        ),
        asyncio.create_task(sweep_forever(), name="sentiment_sweep"),
    }
    try:
        done, pending = await asyncio.wait(loops, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError, KeyboardInterrupt:
        for task in loops:
            task.cancel()
        await asyncio.gather(*loops, return_exceptions=True)
        logger.info("ingest_consumer_all_stopped")
        return

    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    finished = next(iter(done))
    exc = finished.exception()
    if exc is not None:
        logger.error("ingest_consumer_loop_crashed", loop=finished.get_name(), error=str(exc))
        raise exc
    logger.error("ingest_consumer_loop_exited_unexpectedly", loop=finished.get_name())


if __name__ == "__main__":
    # Trang Nhật ký của dashboard đọc file này (GET /logs/ingest).
    enable_file_logging(settings.ingest_consumer_log_path)
    asyncio.run(run())
