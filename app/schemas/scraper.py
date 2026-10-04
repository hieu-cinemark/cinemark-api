from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class RunScraperRequest(BaseModel):
    """Mọi trường đều không bắt buộc và trên thực tế loại trừ nhau:
    - có keyword_id: chỉ kích hoạt đúng từ khoá đó.
    - có movie_id (không có keyword_id): kích hoạt mọi từ khoá đang bật của phim đó.
    - không có cái nào: kích hoạt mọi từ khoá đang bật của mọi phim (trường hợp của job hằng ngày).

    keyword_id/movie_id là id của D1 (xem app/services/d1.py), không phải UUID.

    start_date/end_date chỉ có nghĩa với tìm kiếm Facebook (quét theo ngày đăng).
    Threads không có bộ lọc ngày; TikTok chỉ tìm theo hashtag. API bỏ qua các trường này
    với nền tảng không phải Facebook.
    """

    keyword_id: str | None = None
    movie_id: str | None = None
    max_pages: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    # Bước nhảy sang hashtag liên quan của TikTok đã được người vận hành duyệt. Bị bỏ qua
    # nếu không có keyword_id. spider-hub giới hạn số trang và ngừng gợi ý thêm tag ở độ
    # sâu 2.
    bfs_depth: int | None = None


class RunScraperResponse(BaseModel):
    requested: int
    published: int


class JobStatus(BaseModel):
    running: bool
    keyword: str | None = None
    keyword_id: str | None = None
    started_at: int | None = None
    type: str | None = None
    account: str | None = None
    post_id: str | None = None
    # Handle kênh TikTok nhập tự do cho job type=channel_videos.
    username: str | None = None


class StopScraperResponse(BaseModel):
    stopped: bool


class RunCommentsResponse(BaseModel):
    published: bool


class RunChannelVideosRequest(BaseModel):
    """Crawl lưới video của một kênh TikTok - username có hoặc không có @ ở đầu."""

    username: str
    max_pages: int | None = None
    keyword_id: str | None = None


class RunChannelVideosResponse(BaseModel):
    published: bool


class TriggerTokenRefreshResponse(BaseModel):
    ok: bool


class TokenStatus(BaseModel):
    valid: bool
    account: str | None = None
    expires_in_seconds: int | None = None


class ImportCookiesRequest(BaseModel):
    # platform_accounts.id (id số của dòng) - phía server tra ra account_key thật (email
    # hoặc account_id) mà --account của bootstrap.py cần, xem
    # app/api/routes/token_refresh.py.
    account_id: int
    # Text JSON thô xuất từ DevTools - hoặc {"c_user": "...", "xs": "...", ...} hoặc một
    # danh sách cookie đầy đủ kiểu Playwright. Chuyển nguyên vẹn sang import_cookies() của
    # spider-hub, nơi kiểm tra có đủ các tên cookie bắt buộc.
    cookies: str


class RestoreSessionRequest(BaseModel):
    # Cùng id dòng với ImportCookiesRequest - spider-hub dùng lại storage_state trong Redis
    # (hoặc cột cookie) của tài khoản đó và bắt lại token GraphQL. Người vận hành không
    # cần đưa cookie mới.
    account_id: int


class KeywordEnabledUpdate(BaseModel):
    enabled: bool


class KeywordOut(BaseModel):
    id: str
    movie_id: str
    movie_title: str
    keyword: str
