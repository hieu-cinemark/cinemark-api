"""Đọc/ghi các key Redis crawl_job:<platform> và crawl_job_cancel:<run_id> của
spider-hub - nửa phía dashboard của cơ chế "dừng một job đang chạy" mà
crawl_request_consumer.py của spider-hub cài đặt ở phía bên kia (xem
_run_subprocess/_run_spider bên đó). Cùng kiểu dùng chung Redis như
platform_token.py: phía này không bao giờ tự chạy tiến trình con, chỉ đọc/ghi các
key điều phối mà consumer của spider-hub đặt trước khi crawl và kiểm tra định kỳ
trong lúc crawl."""

from __future__ import annotations

import json
import time
from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.services.task_queue import clear_pending, remove_pending

# Cờ dừng còn hiệu lực trong bao lâu - phủ trường hợp consumer của spider-hub tạm sập/
# chậm nhận ra cờ, mà không để một cờ cũ nằm đó mãi nếu nó không bao giờ nhận ra. Lớn
# hơn khá nhiều so với JOB_CANCEL_POLL_SECONDS của crawl_request_consumer.py - đây là
# trần an toàn, không phải độ trễ dự kiến.
STOP_FLAG_TTL_SECONDS = 3600

# Một lần bấm Dừng tiếp tục bỏ qua (không chạy) các crawl_requests còn trong hàng đợi
# của nền tảng này (comment, tìm theo từ khoá, các bước BFS tiếp theo) trong bao lâu.
# Đủ dài để xả hết một lần bấm "lấy comment" hàng loạt.
BFS_DRAIN_TTL_SECONDS = 900


async def get_running_job(platform: str) -> dict[str, Any] | None:
    """None nếu hiện không có job nào đang chạy cho nền tảng này - xem _run_spider trong
    crawl_request_consumer.py, nơi đặt/xoá key này quanh mỗi tiến trình crawl con được
    kích hoạt từ dashboard (có run_id)."""
    client = get_redis_client()
    raw = await client.get(f"{REDIS_KEY_PREFIX}crawl_job:{platform}")
    return json.loads(raw) if raw else None


async def is_platform_draining(platform: str) -> bool:
    """True sau khi bấm Dừng cho tới khi hết TTL hoặc một lần Run mới xoá các cờ. Trong lúc
    cờ còn, công việc hàng loạt đang xếp hàng bị bỏ qua; các lần kích hoạt có chủ đích
    (bypass_drain) bấm sau lần Dừng vẫn chạy, và job nào đang chạy dở cũng vậy -
    dashboard vẫn hiển thị cả hai."""
    client = get_redis_client()
    return bool(await client.exists(f"{REDIS_KEY_PREFIX}platform_drain:{platform}"))


async def request_stop(platform: str) -> bool:
    """Huỷ tiến trình con đang chạy và bỏ qua phần công việc còn lại trong hàng đợi của nền
    tảng này (comment + tìm kiếm) cho tới khi hết TTL drain. Luôn trả True sau khi bật
    drain - bấm Dừng khi không có gì đang chạy vẫn xoá được danh sách chờ.

    Bật cả crawl_job_cancel:<run_id> (chính xác) lẫn
    crawl_job_cancel_platform:<platform> (phủ khoảng trống trước khi crawl_job được ghi -
    ví dụ bootstrap comment Facebook - và mọi trường hợp đua run_id nếu một job mới bắt
    đầu giữa lúc Dừng)."""
    client = get_redis_client()
    await client.set(f"{REDIS_KEY_PREFIX}bfs_drain:{platform}", "1", ex=BFS_DRAIN_TTL_SECONDS)
    await client.set(f"{REDIS_KEY_PREFIX}comments_drain:{platform}", "1", ex=BFS_DRAIN_TTL_SECONDS)
    # Giá trị là thời điểm bấm Dừng: spider-hub bỏ qua các message bypass_drain được
    # publish trước thời điểm đó (xem crawl_request_consumer._handle_request bên đó).
    await client.set(f"{REDIS_KEY_PREFIX}platform_drain:{platform}", str(time.time()), ex=BFS_DRAIN_TTL_SECONDS)
    await client.set(
        f"{REDIS_KEY_PREFIX}crawl_job_cancel_platform:{platform}",
        "1",
        ex=STOP_FLAG_TTL_SECONDS,
    )
    await clear_pending(platform)

    job = await get_running_job(platform)
    if job is not None and job.get("run_id"):
        await client.set(
            f"{REDIS_KEY_PREFIX}crawl_job_cancel:{job['run_id']}",
            "1",
            ex=STOP_FLAG_TTL_SECONDS,
        )
    return True


async def cancel_job(platform: str, run_id: str) -> bool:
    """Dừng đúng một job - nút Dừng trên từng dòng của dashboard. Cố ý KHÔNG đụng tới
    bfs_drain/comments_drain/platform_drain và không gọi clear_pending: đó là hành vi
    "Dừng tất cả" của request_stop, và việc dùng lại chúng ở đây chính là lỗi thật (xem
    cách nối nút Dừng từng dòng ban đầu trong JobsPageView.tsx) - bấm Dừng trên một dòng
    âm thầm huỷ cả job đang chạy LẪN xoá sạch mục đang chờ của mọi nền tảng khác.

    Dù thế nào cũng bật cờ chính xác crawl_job_cancel:<run_id> (cùng key mà request_stop
    vốn đặt cho trường hợp job đang chạy) - _handle_request trong
    crawl_request_consumer.py giờ kiểm tra cờ này cho mọi message Kafka *trước khi* bắt
    đầu chạy, không chỉ giữa chừng qua việc kiểm tra định kỳ của _run_subprocess, nên nó
    hoạt động dù run_id còn đang chờ hay đã chạy. remove_pending chỉ để dashboard phản
    hồi ngay trên một dòng còn đang chờ (nếu không nó sẽ nằm đó cho tới khi consumer tới
    nơi và bỏ qua) - mọi mục đang chờ khác của nền tảng giữ nguyên và chạy bình thường."""
    if not run_id:
        return False

    was_queued = await remove_pending(platform, run_id)
    job = await get_running_job(platform)
    was_running = job is not None and job.get("run_id") == run_id

    if not was_queued and not was_running:
        return False

    client = get_redis_client()
    await client.set(f"{REDIS_KEY_PREFIX}crawl_job_cancel:{run_id}", "1", ex=STOP_FLAG_TTL_SECONDS)
    return True


async def clear_drain(platform: str) -> None:
    """Xoá các cờ bỏ qua của lần Dừng để một lần kích hoạt mới từ dashboard thực sự chạy.
    Các message Kafka đã bị consume trong lúc drain thì mất rồi; việc này chỉ gỡ chặn cho
    công việc được publish sau khi người dùng chủ động bắt đầu crawl lại."""
    client = get_redis_client()
    await client.delete(
        f"{REDIS_KEY_PREFIX}bfs_drain:{platform}",
        f"{REDIS_KEY_PREFIX}comments_drain:{platform}",
        f"{REDIS_KEY_PREFIX}platform_drain:{platform}",
        f"{REDIS_KEY_PREFIX}crawl_job_cancel_platform:{platform}",
    )
