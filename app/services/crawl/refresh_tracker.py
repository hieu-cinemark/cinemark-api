"""Bộ theo dõi trong bộ nhớ (không bao giờ lưu xuống đâu - restart tiến trình là mất
hết, theo thiết kế) cho một lần refresh token kích hoạt từ dashboard, theo từng nền
tảng. spider-hub không có tín hiệu trực tiếp nào báo khi refresh xong - request chỉ
đi qua Kafka và một tiến trình riêng (crawl_request_consumer.py) nhận lấy - nên bộ
này hoạt động giống GET /logs/spider-hub: đọc dần consumer.log của spider-hub trên
đĩa, bắt đầu từ offset ngay lúc refresh được kích hoạt.

Lọc theo run_id, không chỉ theo nền tảng: crawl_request_consumer.py chạy song song
mỗi nền tảng *một* asyncio task (xem
`asyncio.create_task(_run_platform_consumer(platform, ...))` bên đó), không phải một
vòng lặp tuần tự toàn cục cho mọi nền tảng - nên một lần refresh Facebook và một lượt
crawl Threads thật sự có thể cùng ghi vào consumer.log dùng chung một lúc. Chỉ khớp
dòng theo platform=<x> (một phiên bản cũ của docstring này giả định một hàng đợi toàn
cục duy nhất và cho rằng làm vậy an toàn mà không cần truyền run_id - không đúng nữa
khi có chạy song song theo nền tảng) đã để các dòng log của một nền tảng *khác* đang
chạy cùng lúc lẫn vào panel của nền tảng nào đang mở. run_id được sinh một lần cho mỗi
lần refresh được kích hoạt (publish_cookie_import_request trong app/clients/kafka.py -
nút kích hoạt duy nhất còn lại, xem docstring của app/api/routes/token_refresh.py để
biết vì sao không còn nút "refresh now" riêng nữa) và được truyền đi suốt: chuyển cho
tiến trình con của spider-hub qua --run-id, bind vào context structlog của tiến trình
đó (xem auth/bootstrap.py của facebook/threads bên spider-hub), nên mọi dòng nó log -
bất kể module nào của nó log - đều mang `run_id=<x>` và chỉ những dòng đó được hiển
thị ở đây.

Mọi subscriber (một WebSocket đang mở của dashboard) nhận cùng một bản phát - trước
tiên là "snapshot" của những gì đã biết (trạng thái + các dòng đã đệm tới lúc đó),
rồi tới các message "line"/"status" trực tiếp khi chúng xảy ra. Snapshot là thứ làm
cho việc tải lại trang an toàn: kết nối lại giữa lúc refresh sẽ phát lại mọi thứ đã
thấy thay vì mất đi."""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

Status = Literal["idle", "running", "success", "failed"]

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_RUN_ID_RE = re.compile(r"\brun_id=(\S+)")
_SUCCESS_EVENTS = frozenset({"token_refresh_finished", "tiktok_identity_refreshed"})
_FAILURE_EVENTS = frozenset({"token_refresh_failed"})
_MAX_BUFFER_LINES = 500
_POLL_INTERVAL_SECONDS = 0.3
# Các lần refresh thường ngày (session đã lưu, auto-login) xong trong chưa tới một
# phút - đây là khoảng dư rộng rãi, không phải thời lượng thực tế dự kiến. Nếu
# spider-hub thật sự cần lâu hơn (ví dụ đăng nhập tay mới), bộ theo dõi này chỉ ngừng
# theo dõi và báo "failed" cho mục đích hiển thị trên dashboard - tiến trình con thật
# bên spider-hub không bị ảnh hưởng và vẫn tiếp tục log/chạy, đây chỉ là phía giao diện
# bỏ cuộc.
_WATCH_TIMEOUT_SECONDS = 180
# Dưới mức tuổi này, lần start_refresh() thứ hai cho cùng nền tảng được coi là bấm đúp
# / hai tab dashboard đang mở cùng bắn cho một thao tác của người dùng, không phải cố ý
# thử lại khi bị kẹt - xem docstring của start_refresh. Lớn hơn khá nhiều so với độ
# nhiễu thực tế của bấm đúp/mạng, và nhỏ hơn khá nhiều so với thời gian ngay cả một
# lần refresh thường ngày chậm cần để ra dòng log đầu tiên.
_MIN_RUNNING_SECONDS_BEFORE_RESTART = 5.0


def _parse_log_line(line: str) -> tuple[str, str | None, str | None] | None:
    """(text hiển thị, run_id, event) cho một dòng log của spider-hub, ở một trong hai
    LOG_FORMAT của nó (xem social_crawler/logger.py bên spider-hub): mỗi dòng một object
    JSON, hoặc một dòng console với các trường key=value và tên event là token đầu tiên
    sau "[level]". None nếu dòng trống."""
    if not line.strip():
        return None
    if line.lstrip().startswith("{"):
        try:
            obj = json.loads(line)
        except ValueError:
            obj = None
        if isinstance(obj, dict):
            event = str(obj.get("event", ""))
            run_id = obj.get("run_id")
            details = " ".join(
                f"{k}={v}"
                for k, v in obj.items()
                if k not in {"event", "timestamp", "level", "run_id", "service", "logger"}
            )
            display = f"{obj.get('timestamp', '')} [{obj.get('level', '')}] {event} {details}".strip()
            return display, str(run_id) if run_id is not None else None, event
    match = _RUN_ID_RE.search(line)
    run_id = match.group(1).strip("'\"") if match else None
    # Dạng console: "<timestamp> [<level>  ] <event>   [<logger>] k=v ...".
    # Các bản spider-hub cũ thêm tiền tố "[PLATFORM] [STATUS] " trước event.
    event = None
    for token in line.split():
        if token.startswith("[") or token.endswith("]") or token[0].isdigit():
            continue
        event = token
        break
    return line, run_id, event


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass
class _RefreshState:
    status: Status = "idle"
    started_at: str | None = None
    finished_at: str | None = None
    lines: list[str] = field(default_factory=list)
    subscribers: set[asyncio.Queue[dict[str, Any] | None]] = field(default_factory=set)
    task: asyncio.Task[None] | None = None
    # run_id mà các dòng của state này đang được giới hạn theo - xem docstring module. Chỉ
    # là None trong khoảnh khắc rất ngắn, giữa lúc _state_for() vừa tạo mục cho một nền
    # tảng và lúc start_refresh() đặt giá trị.
    run_id: str | None = None
    # time.monotonic() lúc `task` được tạo - cho start_refresh phân biệt được restart do
    # bấm đúp/hai tab đang mở với cố ý thử lại khi bị kẹt (xem
    # _MIN_RUNNING_SECONDS_BEFORE_RESTART).
    task_started_monotonic: float | None = None


_states: dict[str, _RefreshState] = {}


def _state_for(platform: str) -> _RefreshState:
    if platform not in _states:
        _states[platform] = _RefreshState()
    return _states[platform]


def snapshot(platform: str) -> dict[str, Any]:
    state = _state_for(platform)
    return {
        "type": "snapshot",
        "status": state.status,
        "started_at": state.started_at,
        "finished_at": state.finished_at,
        "lines": list(state.lines),
    }


def subscribe(platform: str) -> asyncio.Queue[dict[str, Any] | None]:
    queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    _state_for(platform).subscribers.add(queue)
    return queue


def unsubscribe(platform: str, queue: asyncio.Queue[dict[str, Any] | None]) -> None:
    _state_for(platform).subscribers.discard(queue)


def shutdown() -> None:
    """Đánh thức mọi chỗ đang chờ WebSocket refresh-token để `uvicorn --reload` thực sự
    thoát được. Các handler đó nằm chờ `await queue.get()` không có timeout; không có
    giá trị báo hiệu thì WatchFiles treo ở "Waiting for background tasks" và các lời gọi
    /stats và /job-status tiếp theo của dashboard không bao giờ nhận được response."""
    for state in _states.values():
        if state.task is not None and not state.task.done():
            state.task.cancel()
        for queue in list(state.subscribers):
            queue.put_nowait(None)


def _broadcast(platform: str, message: dict[str, Any]) -> None:
    for queue in list(_state_for(platform).subscribers):
        queue.put_nowait(message)


def start_refresh(platform: str, run_id: str) -> bool:
    """Bắt đầu đọc dần consumer.log của spider-hub theo run_id này. Bấm khi đang có một lượt
    theo dõi chạy thì huỷ lượt đó và chuyển sang theo dõi run_id mới - giúp gỡ một lượt
    theo dõi bị kẹt (TikTok từng xong mà không có run_id trên token_refresh_finished,
    khiến status=running mãi).

    Trả về False (giữ nguyên lượt theo dõi hiện có, không publish gì mới) nếu lượt đó vừa
    đang chạy vừa trẻ hơn _MIN_RUNNING_SECONDS_BEFORE_RESTART - đó là bấm đúp hoặc hai tab
    dashboard đang mở sinh ra hai run_id cho một thao tác, không phải thật sự thử lại khi
    bị kẹt. Huỷ trong trường hợp đó sẽ âm thầm bỏ rơi run_id đầu tiên:
    crawl_request_consumer.py vẫn chạy nó tới cùng bên spider-hub, nhưng ở đây không còn
    gì theo dõi run_id của nó nữa, nên kết quả thành công/thất bại cuối cùng của nó không
    bao giờ hiện lên dashboard."""
    state = _state_for(platform)
    if (
        state.task is not None
        and not state.task.done()
        and state.task_started_monotonic is not None
        and time.monotonic() - state.task_started_monotonic < _MIN_RUNNING_SECONDS_BEFORE_RESTART
    ):
        logger.info("refresh_watch_restart_debounced", platform=platform, run_id=run_id, existing_run_id=state.run_id)
        return False

    if state.task is not None and not state.task.done():
        state.task.cancel()

    state.status = "running"
    state.started_at = _now_iso()
    state.task_started_monotonic = time.monotonic()
    state.finished_at = None
    state.lines = []
    state.run_id = run_id
    _broadcast(platform, {"type": "status", "status": "running", "started_at": state.started_at})

    path = Path(settings.spider_hub_consumer_log_path)
    start_offset = path.stat().st_size if path.is_file() else 0
    logger.info(
        "refresh_watch_started", platform=platform, run_id=run_id, log_path=str(path), start_offset=start_offset
    )
    state.task = asyncio.create_task(_tail_until_done(platform, run_id, path, start_offset))
    return True


def _finish(platform: str, status: Status, extra_line: str | None = None) -> None:
    state = _state_for(platform)
    state.status = status
    state.finished_at = _now_iso()
    if extra_line:
        state.lines.append(extra_line)
        _broadcast(platform, {"type": "line", "line": extra_line})
    _broadcast(platform, {"type": "status", "status": status, "finished_at": state.finished_at})


async def _redis_refresh_result(run_id: str) -> bool | None:
    """True/False nếu spider-hub đã ghi token_refresh_result:{run_id}, ngược lại là None."""
    raw = await get_redis_client().get(f"{REDIS_KEY_PREFIX}token_refresh_result:{run_id}")
    if not raw:
        return None
    try:
        return bool(json.loads(raw).get("ok"))
    except TypeError, ValueError:
        return None


async def _tail_until_done(platform: str, run_id: str, path: Path, start_offset: int) -> None:
    state = _state_for(platform)
    deadline = time.monotonic() + _WATCH_TIMEOUT_SECONDS
    offset = start_offset

    try:
        while time.monotonic() < deadline:
            redis_ok = await _redis_refresh_result(run_id)
            if redis_ok is True:
                _finish(platform, "success")
                return
            if redis_ok is False:
                _finish(platform, "failed")
                return

            if not path.is_file():
                await asyncio.sleep(_POLL_INTERVAL_SECONDS)
                continue

            size = path.stat().st_size
            if size < offset:
                # File log bị xoay vòng/cắt ngắn trong lúc đang đọc - đồng bộ lại từ đầu thay vì lỗi
                # khi seek tới vị trí âm.
                offset = 0
            if size > offset:
                with path.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(offset)
                    chunk = f.read()
                    offset = f.tell()

                for raw_line in chunk.splitlines():
                    parsed = _parse_log_line(_ANSI_RE.sub("", raw_line))
                    if parsed is None:
                        continue
                    line, line_run_id, event = parsed
                    # Bỏ qua mọi dòng không gắn đúng run_id của lượt chạy này - xem docstring module để
                    # biết vì sao khớp chuỗi con theo nền tảng trần đã để dòng của một nền tảng khác đang
                    # chạy song song lẫn vào panel này.
                    if line_run_id != run_id:
                        continue
                    state.lines.append(line)
                    if len(state.lines) > _MAX_BUFFER_LINES:
                        state.lines = state.lines[-_MAX_BUFFER_LINES:]
                    _broadcast(platform, {"type": "line", "line": line})

                    if event in _SUCCESS_EVENTS:
                        _finish(platform, "success")
                        return
                    if event in _FAILURE_EVENTS:
                        _finish(platform, "failed")
                        return
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

        _finish(
            platform,
            "failed",
            extra_line=f"(dashboard) gave up watching after {_WATCH_TIMEOUT_SECONDS}s - check the log directly",
        )
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.error("refresh_tracker_tail_failed", platform=platform, error=str(exc))
        state.status = "failed"
        state.finished_at = _now_iso()
        _broadcast(platform, {"type": "status", "status": "failed", "finished_at": state.finished_at})
