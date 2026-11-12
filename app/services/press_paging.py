"""Lấy tiếp các trang kết quả Google cho khối "Góc nhìn sau công chiếu".

Bản tin lưu trong D1 mới có trang đầu của mỗi nguồn (Google Search + Google News Light).
Module này gọi tiếp các trang sau và trả về phần bài MỚI, đánh số nguồn tiếp nối
bản đang lưu để số [1], [2] của Cinemark AI không bị lệch.
"""

import asyncio

from app.core.errors import NotFoundError, UpstreamError
from app.repositories.d1 import press_reviews
from app.schemas.research import GoogleNewsRequest
from app.services import google_news, press_groups

PAGE_SIZE = 10
MAX_PAGES = 6


def _params(base: dict, start: int) -> GoogleNewsRequest:
    """Dựng lại tham số tìm của trang đầu, chỉ đổi vị trí bắt đầu."""
    return GoogleNewsRequest(
        q=str(base.get("q") or ""),
        gl=str(base.get("gl") or "vn"),
        hl=str(base.get("hl") or "vi"),
        device=str(base.get("device") or "desktop"),
        start=start,
        no_cache=False,
    )


def _merge(
    existing: list[dict], incoming: list[dict], start_index: int
) -> tuple[list[dict], list[dict], int]:
    """Ghép bài mới vào danh sách cũ.

    Trả về (danh sách đã gộp, danh sách chỉ gồm bài MỚI, số nguồn cuối cùng).
    Bài trùng link với bản đang lưu hoặc với trang trước bị bỏ qua.
    """
    seen = {press_groups.identity(item) for item in existing}
    merged = list(existing)
    added: list[dict] = []
    index = start_index
    for item in incoming:
        key = press_groups.identity(item)
        if not key or key in seen:
            continue
        seen.add(key)
        index += 1
        numbered = {**item, "source_index": index}
        merged.append(numbered)
        added.append(numbered)
    return merged, added, index


def _has_next(data: dict) -> bool:
    """SerpApi báo còn trang kế tiếp thì mới đáng gọi tiếp."""
    pagination = data.get("serpapi_pagination") or {}
    return bool(pagination.get("next_link"))


async def more_pages(movie_id: str, page: int, max_pages: int = MAX_PAGES, source_offset: int = 0) -> dict:
    """Trả về các bài MỚI của các trang kế tiếp (bắt đầu từ trang `page`, 1 = trang đầu).

    `hasMore` cho biết còn trang nữa không, để frontend gọi tiếp tới khi hết kết quả.
    `source_offset` là số bài đã giao ở các lượt trước, dùng để đánh số nguồn tiếp nối
    mà không phải tải lại các trang đã đọc.
    """
    snapshot = await press_reviews.load(movie_id)
    if not snapshot:
        raise NotFoundError("Chưa có bản tin để lấy thêm.")
    data = snapshot["data"]
    web_base = data.get("search_parameters") or {}
    news_base = data.get("news_search_parameters") or {}
    if not web_base.get("q") or not news_base.get("q"):
        raise UpstreamError("Bản tin hiện có không lưu câu tìm nên không lấy thêm được trang.")

    organic = press_groups.flatten(data.get("organic_results", []))
    press = press_groups.flatten(data.get("news_results", []))
    all_sources = data.get("sources", []) or []
    next_index = max((int(item.get("source_index") or 0) for item in all_sources), default=0)
    # Số bài đã giao ở các lượt trước: nguồn tiếp nối phải đánh số sau cả phần đó.
    next_index += max(0, source_offset)

    added_organic: list[dict] = []
    added_press: list[dict] = []
    pages_read = 0
    has_more = False
    for offset in range(max(1, page) - 1, max(1, page) - 1 + max(1, max_pages)):
        start = offset * PAGE_SIZE
        web_data, news_data = await asyncio.gather(
            google_news.search_web(_params(web_base, start)),
            google_news.search(_params(news_base, start)),
        )
        organic, new_organic, next_index = _merge(
            organic, press_groups.flatten(web_data.get("organic_results", [])), next_index
        )
        press, new_press, next_index = _merge(
            press, press_groups.flatten(news_data.get("news_results", [])), next_index
        )
        added_organic.extend(new_organic)
        added_press.extend(new_press)
        pages_read += 1
        # Còn trang kế tiếp ở một trong hai nguồn thì cần gọi tiếp.
        has_more = _has_next(web_data) or _has_next(news_data)
        if not has_more:
            break

    return {
        "movieId": movie_id,
        "pagesRead": pages_read,
        "hasMore": has_more,
        "organic_results": added_organic,
        "news_results": added_press,
    }
