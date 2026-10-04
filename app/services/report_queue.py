"""Hàng đợi nền trong tiến trình cho việc tạo report social-topic - phục vụ nút "Tạo
report" trên dashboard (POST /movies/{id}/generate-report,
app/api/routes/movies.py). Thêm ngày 2026-09-25: route đó từng chạy
generate_report_for_movie đồng bộ ngay trong request handler (hai lời gọi Bee/Kira
tuần tự, đã xác nhận thực tế riêng lời gọi gom topic có thể mất hơn 2 phút), và
trạng thái mutation của dashboard khoá nút của MỌI dòng KHÁC trong lúc một dòng đang
chạy - nên người vận hành không bao giờ tạo được quá một report cùng lúc. Đưa vào
hàng đợi ở đây cho lời gọi HTTP trả về ngay và cho report của nhiều phim chạy song
song.

Trong bộ nhớ, không dùng Redis: bản deploy này là một tiến trình uvicorn (xem CMD
trong Dockerfile - không có --workers), nên một dict thường là mọi request đều thấy,
không tốn chi phí serialize của kho dùng chung; crawl_jobs.py/task_queue.py chọn
ngược lại chỉ vì CHÚNG phải phối hợp giữa tiến trình này và tiến trình riêng của
spider-hub. Nếu sau này API chạy nhiều worker/replica, phần này phải chuyển sang Redis
(cùng dạng với crawl_jobs.py) - nếu không, một lần GET trạng thái rơi vào "nhầm"
worker sẽ thấy trạng thái cũ/thiếu.

Không có giới hạn đồng thời riêng ở đây - semaphore theo provider của app.ai.client
(asyncio.Semaphore(2), xem call_ai) vốn đã giới hạn số lời gọi Bee/Kira chạy cùng
lúc; đưa vào hàng đợi nhiều phim hơn mức đó chỉ có nghĩa là các phim dư nằm chờ
semaphore đó bên trong generate_report_for_movie, đúng cùng hành vi xếp hàng, chỉ là
dùng điểm nghẽn duy nhất sẵn có thay vì thêm một điểm thứ hai lặp lại nó ở đây."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from app.core.logging import get_logger
from app.services.social_topic import ReportResult, generate_report_for_movie, get_movie_for_report

logger = get_logger(__name__)

JobStatus = Literal["queued", "running", "done", "failed"]

# Dọn lười (xem _prune) thay vì theo hẹn giờ - mục của một job chỉ cần sống đủ lâu để
# lượt hỏi định kỳ sau khi đưa vào hàng đợi của dashboard thấy được trạng thái cuối
# cùng, không cần mãi mãi.
_JOB_TTL_SECONDS = 3600.0

_jobs: dict[str, dict[str, Any]] = {}


def _prune() -> None:
    cutoff = time.monotonic() - _JOB_TTL_SECONDS
    stale = [
        movie_id
        for movie_id, job in _jobs.items()
        if job["status"] in ("done", "failed") and job["updated_at"] < cutoff
    ]
    for movie_id in stale:
        del _jobs[movie_id]


def get_report_job(movie_id: str) -> dict[str, Any] | None:
    """None nếu chưa từng có job nào được đưa vào hàng đợi cho phim này (hoặc mục của nó đã
    hết hạn - xem _JOB_TTL_SECONDS) - dashboard coi đó là "không có gì để hiện", không
    phải lỗi."""
    return _jobs.get(movie_id)


async def enqueue_report_job(movie_id: str) -> dict[str, Any]:
    """Idempotent: phim đang trong hàng đợi/đang chạy thì chỉ trả về job hiện có thay vì
    chạy thêm một lượt song song nữa của chính nó - bấm đúp không được để hai lời gọi
    generate_report_for_movie đua nhau trên cùng một dòng social_topic_reports."""
    _prune()
    existing = _jobs.get(movie_id)
    if existing is not None and existing["status"] in ("queued", "running"):
        return existing

    job: dict[str, Any] = {
        "movie_id": movie_id,
        "status": "queued",
        "result": None,
        "error": None,
        "queued_at": time.monotonic(),
        "started_at": None,
        "finished_at": None,
        "updated_at": time.monotonic(),
    }
    _jobs[movie_id] = job
    asyncio.create_task(_run(movie_id))
    return job


async def _run(movie_id: str) -> None:
    job = _jobs[movie_id]
    job["status"] = "running"
    job["started_at"] = time.monotonic()
    job["updated_at"] = time.monotonic()
    try:
        movie = await get_movie_for_report(movie_id)
        if movie is None:
            raise ValueError("movie_not_found")
        result: ReportResult = await generate_report_for_movie(movie)
        job["result"] = result
        job["status"] = "failed" if result != "generated" else "done"
        if result != "generated":
            job["error"] = result
    except Exception as exc:
        logger.exception("report_job_failed", movie_id=movie_id)
        job["status"] = "failed"
        job["error"] = str(exc) or repr(exc)
    finally:
        job["finished_at"] = time.monotonic()
        job["updated_at"] = time.monotonic()
