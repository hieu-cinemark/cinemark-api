"""In-memory (never persisted - nothing here survives a process restart, by
design) tracker for a dashboard-triggered token refresh, per platform. There
is no direct signal from spider-hub when a refresh finishes - the request
just goes over Kafka and a separate process (crawl_request_consumer.py)
picks it up - so this works the same way GET /logs/spider-hub does: it tails
spider-hub's consumer.log off disk, starting from the offset at the moment
the refresh was triggered, and watches for the
"token_refresh_{started,finished,failed} ... platform=<x>" lines that
_refresh_token() in that file logs. Since that consumer processes Kafka
messages one at a time (see its own docstring), the log between a
"started" and the next "finished"/"failed" for a platform can only belong
to that one refresh - no request-id plumbing needed to disambiguate.

Every subscriber (a dashboard's open WebSocket) gets the same broadcast -
first a "snapshot" of whatever's already known (status + buffered lines so
far), then live "line"/"status" messages as they happen. The snapshot is
what makes a page reload safe: reconnecting mid-refresh replays everything
seen so far instead of losing it."""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

Status = Literal["idle", "running", "success", "failed"]

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_MAX_BUFFER_LINES = 500
_POLL_INTERVAL_SECONDS = 0.3
# Routine refreshes (saved session, auto-login) finish in well under a
# minute - this is generous headroom, not a realistic expected duration. If
# spider-hub genuinely needs longer (e.g. a fresh manual login), this tracker
# just stops watching and reports "failed" for dashboard purposes - the
# actual subprocess in spider-hub is unaffected and keeps logging/running
# regardless, this is a UI-side give-up only.
_WATCH_TIMEOUT_SECONDS = 180


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass
class _RefreshState:
    status: Status = "idle"
    started_at: str | None = None
    finished_at: str | None = None
    lines: list[str] = field(default_factory=list)
    subscribers: set[asyncio.Queue[dict[str, Any]]] = field(default_factory=set)
    task: asyncio.Task[None] | None = None


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


def subscribe(platform: str) -> asyncio.Queue[dict[str, Any]]:
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    _state_for(platform).subscribers.add(queue)
    return queue


def unsubscribe(platform: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
    _state_for(platform).subscribers.discard(queue)


def _broadcast(platform: str, message: dict[str, Any]) -> None:
    for queue in list(_state_for(platform).subscribers):
        queue.put_nowait(message)


def start_refresh(platform: str) -> bool:
    """Begins tailing spider-hub's consumer.log for this platform's next
    token-refresh cycle. No-op (returns False) if one's already being
    watched - crawl_request_consumer.py handles one Kafka message at a time
    regardless, so a second concurrent trigger wouldn't do anything a single
    watch doesn't already cover; the dashboard button is disabled while
    running anyway, this is just belt-and-suspenders against a second tab."""
    state = _state_for(platform)
    if state.status == "running":
        return False

    state.status = "running"
    state.started_at = _now_iso()
    state.finished_at = None
    state.lines = []
    _broadcast(platform, {"type": "status", "status": "running", "started_at": state.started_at})

    path = Path(settings.spider_hub_consumer_log_path)
    start_offset = path.stat().st_size if path.is_file() else 0
    state.task = asyncio.create_task(_tail_until_done(platform, path, start_offset))
    return True


async def _tail_until_done(platform: str, path: Path, start_offset: int) -> None:
    state = _state_for(platform)
    deadline = time.monotonic() + _WATCH_TIMEOUT_SECONDS
    offset = start_offset

    try:
        while time.monotonic() < deadline:
            if not path.is_file():
                await asyncio.sleep(_POLL_INTERVAL_SECONDS)
                continue

            size = path.stat().st_size
            if size < offset:
                # Log file was rotated/truncated under us - resync from the
                # top rather than raising on a negative seek.
                offset = 0
            if size > offset:
                with path.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(offset)
                    chunk = f.read()
                    offset = f.tell()

                for raw_line in chunk.splitlines():
                    line = _ANSI_RE.sub("", raw_line)
                    if not line.strip():
                        continue
                    state.lines.append(line)
                    if len(state.lines) > _MAX_BUFFER_LINES:
                        state.lines = state.lines[-_MAX_BUFFER_LINES:]
                    _broadcast(platform, {"type": "line", "line": line})

                    platform_marker = f"platform={platform}"
                    if "token_refresh_finished" in line and platform_marker in line:
                        state.status = "success"
                        state.finished_at = _now_iso()
                        _broadcast(
                            platform, {"type": "status", "status": "success", "finished_at": state.finished_at}
                        )
                        return
                    if "token_refresh_failed" in line and platform_marker in line:
                        state.status = "failed"
                        state.finished_at = _now_iso()
                        _broadcast(
                            platform, {"type": "status", "status": "failed", "finished_at": state.finished_at}
                        )
                        return
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

        state.status = "failed"
        state.finished_at = _now_iso()
        timeout_line = f"(dashboard) gave up watching after {_WATCH_TIMEOUT_SECONDS}s - check the log directly"
        state.lines.append(timeout_line)
        _broadcast(platform, {"type": "line", "line": timeout_line})
        _broadcast(platform, {"type": "status", "status": "failed", "finished_at": state.finished_at})
    except Exception as exc:
        logger.error("refresh_tracker_tail_failed", platform=platform, error=str(exc))
        state.status = "failed"
        state.finished_at = _now_iso()
        _broadcast(platform, {"type": "status", "status": "failed", "finished_at": state.finished_at})
