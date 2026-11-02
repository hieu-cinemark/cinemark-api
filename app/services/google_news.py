"""Google News Light qua SerpApi, gọi theo yêu cầu và giữ nguyên các khối kết quả."""

import logging
import re
from typing import Any

import httpx

from app.core.config import settings
from app.core.errors import AppError, UpstreamError
from app.schemas.research import GoogleNewsRequest

SEARCH_URL = "https://serpapi.com/search"


class _RedactSerpApiKey(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "serpapi.com" in message and "api_key=" in message:
            record.msg = re.sub(r"api_key=[^&\s\"']+", "api_key=[REDACTED]", message)
            record.args = ()
        return True


# httpx tự log URL ở INFO, kể cả khi ứng dụng không log request.
logging.getLogger("httpx").addFilter(_RedactSerpApiKey())


class SerpApiNotConfigured(AppError):
    status_code = 503
    code = "serpapi_not_configured"


async def search(payload: GoogleNewsRequest) -> dict[str, Any]:
    key = (settings.serpapi_api_key or "").strip()
    if not key:
        raise SerpApiNotConfigured("Chưa cấu hình SERPAPI_API_KEY.")
    params = payload.model_dump(exclude_none=True)
    params.update(engine="google_news_light", api_key=key)
    params["no_cache"] = "true" if payload.no_cache else "false"
    # Không retry tự động: mỗi request có thể dùng quota. Không log URL/exception
    # httpx vì query string có chứa api_key.
    try:
        async with httpx.AsyncClient(timeout=settings.serpapi_timeout_seconds, follow_redirects=False) as client:
            response = await client.get(SEARCH_URL, params=params)
            response.raise_for_status()
    except httpx.TimeoutException:
        raise UpstreamError("SerpApi Google News Light hết thời gian chờ.") from None
    except httpx.HTTPError:
        raise UpstreamError("Không gọi được SerpApi Google News Light.") from None
    try:
        data = response.json()
    except ValueError:
        raise UpstreamError("SerpApi trả về JSON không hợp lệ.") from None
    metadata = data.get("search_metadata") if isinstance(data, dict) else None
    if (
        not isinstance(data, dict)
        or data.get("error")
        or not isinstance(metadata, dict)
        or metadata.get("status") != "Success"
    ):
        raise UpstreamError("SerpApi chưa trả về kết quả Google News Light thành công.")
    # Giữ danh sách tin và metadata/pagination do SerpApi trả về.
    # Không trả lại credential nếu nhà cung cấp echo nó trong search_parameters.
    if isinstance(data.get("search_parameters"), dict):
        data["search_parameters"].pop("api_key", None)
    return data
