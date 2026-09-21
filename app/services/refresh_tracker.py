"""In-memory (never persisted - nothing here survives a process restart, by
design) tracker for a dashboard-triggered token refresh, per platform. There
is no direct signal from spider-hub when a refresh finishes - the request
just goes over Kafka and a separate process (crawl_request_consumer.py)
picks it up - so this works the same way GET /logs/spider-hub does: it tails
spider-hub's consumer.log off disk, starting from the offset at the moment
the refresh was triggered.

Filtered by run_id, not just by platform: crawl_request_consumer.py runs
one asyncio task *per platform* concurrently (see its own
`asyncio.create_task(_run_platform_consumer(platform, ...))`), not one
globally-serial loop across every platform - so a Facebook refresh and a
Threads crawl can genuinely be writing to the same shared consumer.log at
the same time. Matching lines by platform=<x> alone (an older version of
this docstring assumed a single global queue and called this safe without
run_id plumbing - it wasn't, once per-platform concurrency landed) let a
concurrently-running *different* platform's own log lines bleed into
whichever platform's panel happened to be open. run_id is generated once
per triggered refresh (app/services/kafka.py's publish_cookie_import_request
- the only trigger left, see app/api/routes/token_refresh.py's own docstring
for why there's no separate standalone "refresh now" anymore) and threaded
all the way through: passed to the spider-hub subprocess as
--run-id, bound into that process's structlog context (see spider-hub's
facebook/threads auth/bootstrap.py), so every line it logs - regardless of
which of its own modules logged it - carries `run_id=<x>` and only those
lines are shown here.

Every subscriber (a dashboard's open WebSocket) gets the same broadcast -
first a "snapshot" of whatever's already known (status + buffered lines so
far), then live "line"/"status" messages as they happen. The snapshot is
what makes a page reload safe: reconnecting mid-refresh replays everything
seen so far instead of losing it."""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from app.core.config import settings
from app.core.logging import get_logger
from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

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
# Below this age, a second start_refresh() for the same platform is treated
# as a double-click / two open dashboard tabs firing for the same user
# action, not a deliberate stuck-watch retry - see start_refresh's own
# docstring. Comfortably above realistic double-click/network jitter,
# comfortably below how long even a slow routine refresh takes to produce
# its first log line.
_MIN_RUNNING_SECONDS_BEFORE_RESTART = 5.0


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
    # The run_id this state's lines are currently scoped to - see module
    # docstring. None only very briefly, between _state_for() first creating
    # a platform's entry and start_refresh() setting it.
    run_id: str | None = None
    # time.monotonic() when `task` was created - lets start_refresh tell a
    # double-click/two-open-tabs restart apart from a deliberate stuck-watch
    # retry (see _MIN_RUNNING_SECONDS_BEFORE_RESTART).
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
    """Wakes every refresh-token WebSocket waiter so uvicorn --reload can
    actually exit. Those handlers sit on `await queue.get()` with no
    timeout; without a sentinel, WatchFiles hangs on "Waiting for
    background tasks" and the dashboard's next /stats and /job-status
    calls never get a response."""
    for state in _states.values():
        if state.task is not None and not state.task.done():
            state.task.cancel()
        for queue in list(state.subscribers):
            queue.put_nowait(None)


def _broadcast(platform: str, message: dict[str, Any]) -> None:
    for queue in list(_state_for(platform).subscribers):
        queue.put_nowait(message)


def start_refresh(platform: str, run_id: str) -> bool:
    """Begins tailing spider-hub's consumer.log for this run_id. A click
    against an already-running watch cancels it and starts tracking the new
    run_id instead - recovers a stuck watch (TikTok used to finish without
    run_id on token_refresh_finished, which left status=running forever).

    Returns False (leaving the existing watch untouched, publishing nothing
    new) if that existing watch is both running and younger than
    _MIN_RUNNING_SECONDS_BEFORE_RESTART - a double-click or two open
    dashboard tabs producing two run_ids for one user action, not a genuine
    stuck-watch retry. Cancelling in that case would silently strand the
    first run_id: crawl_request_consumer.py still runs it to completion in
    spider-hub, but nothing here is tailing for its run_id anymore, so its
    eventual success/failure is never reflected in the dashboard."""
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
    logger.info("refresh_watch_started", platform=platform, run_id=run_id, log_path=str(path), start_offset=start_offset)
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
    """True/False if spider-hub wrote token_refresh_result:{run_id}, else None."""
    raw = await get_redis_client().get(f"{REDIS_KEY_PREFIX}token_refresh_result:{run_id}")
    if not raw:
        return None
    try:
        return bool(json.loads(raw).get("ok"))
    except (TypeError, ValueError):
        return None


async def _tail_until_done(platform: str, run_id: str, path: Path, start_offset: int) -> None:
    state = _state_for(platform)
    deadline = time.monotonic() + _WATCH_TIMEOUT_SECONDS
    offset = start_offset
    run_id_marker = f"run_id={run_id}"

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
                    # Skip anything that isn't tagged with this exact run's
                    # run_id - see module docstring for why a bare platform
                    # substring match let a concurrently-running different
                    # platform's own lines bleed into this panel.
                    if run_id_marker not in line:
                        continue
                    state.lines.append(line)
                    if len(state.lines) > _MAX_BUFFER_LINES:
                        state.lines = state.lines[-_MAX_BUFFER_LINES:]
                    _broadcast(platform, {"type": "line", "line": line})

                    if "token_refresh_finished" in line or "tiktok_identity_refreshed" in line:
                        _finish(platform, "success")
                        return
                    if "token_refresh_failed" in line:
                        _finish(platform, "failed")
                        return
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

        _finish(platform, "failed", extra_line=f"(dashboard) gave up watching after {_WATCH_TIMEOUT_SECONDS}s - check the log directly")
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.error("refresh_tracker_tail_failed", platform=platform, error=str(exc))
        state.status = "failed"
        state.finished_at = _now_iso()
        _broadcast(platform, {"type": "status", "status": "failed", "finished_at": state.finished_at})
