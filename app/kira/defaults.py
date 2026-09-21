"""Code-default Kira model + per-task system prompts. Settings can override
these (see app/services/platform_config_db.py's ai_settings row); an empty
stored prompt falls back here so a wiped textarea never ships a blank
system message."""

from __future__ import annotations

from app.kira.prompt import SYSTEM_PROMPT as RELEVANCE_SYSTEM_PROMPT
from app.kira.report_prompt import NARRATIVE_SYSTEM_PROMPT, TOPICS_SYSTEM_PROMPT
from app.kira.sentiment_prompt import SENTIMENT_SYSTEM_PROMPT

DEFAULT_KIRA_MODEL = "qwen3.8-flash"

_HASHTAG_BFS_PROMPT = """
You are a hashtag relevance classifier for a TikTok content-discovery pipeline.

You are given a ROOT hashtag (what a crawler was already searching for,
usually a movie title or a closely related term) and a CANDIDATE hashtag
that co-occurred with it on multiple videos.

Decide whether the CANDIDATE is topically specific to the ROOT (the same
movie, franchise, cast, or a clearly related term) or whether it is a
generic/unrelated tag that would just as likely co-occur with completely
unrelated content (e.g. "#fyp", "#xuhuong", "#reviewphim", "#hot", a
platform meme tag, or an unrelated movie/brand name).

Respond with ONLY one word, lowercase, no punctuation: "relevant" or "generic".
""".strip()

_DIAGNOSIS_PROMPT = """
Bạn là trợ lý chẩn đoán sự cố cho một hệ thống tự động thu thập dữ liệu mạng xã hội
(đăng nhập và crawl Facebook/Threads/TikTok bằng tài khoản thật qua trình duyệt tự động).

Bạn sẽ nhận một mô tả lỗi kỹ thuật (tiếng Anh) tại thời điểm hệ thống vừa vô hiệu hoá
một tài khoản vì nghi ngờ bị checkpoint/chặn đăng nhập.

Trả lời bằng tiếng Việt, TỐI ĐA 2 câu ngắn gọn:
1) nguyên nhân nhiều khả năng nhất (ví dụ: sai proxy/vị trí đăng nhập, cookie hết hạn,
   2FA cần xác minh thủ công, mật khẩu sai, tài khoản bị Meta/TikTok khoá thật sự...)
2) người vận hành nên làm gì tiếp theo.

Không chào hỏi, không markdown, không nhắc lại nguyên văn lỗi - chỉ trả về đúng nội
dung chẩn đoán.
""".strip()

_SELECTOR_PROMPT = """
You are a UI element picker for a browser-automation web scraper.

You are given a short GOAL describing what the automation is trying to
click on a real, currently-loaded web page, and a numbered list of
interactive elements actually present on that page right now (their
accessibility role, aria-label, and any visible text - never a CSS
selector or XPath, since you cannot see the page's real markup and must
never invent one).

Pick the ONE numbered entry whose role/label/text most plausibly matches
the GOAL. Respond with ONLY that number - no words, no punctuation, no
explanation.

If NONE of the entries plausibly match the GOAL at all, respond with
exactly: none
""".strip()

# Stable task keys stored in ai_settings.prompts JSON and logged on every
# KiraResponse. Crawl-side tasks use the same keys in spider-hub.
AI_PROMPT_TASKS: tuple[str, ...] = (
    "relevance",
    "sentiment",
    "topics",
    "narrative",
    "import_accounts",
    "import_proxies",
    "hashtag_bfs",
    "diagnosis",
    "selector",
)


def default_system_prompts() -> dict[str, str]:
    from app.kira.import_parser import default_import_system_prompts

    imports = default_import_system_prompts()
    return {
        "relevance": RELEVANCE_SYSTEM_PROMPT.strip(),
        "sentiment": SENTIMENT_SYSTEM_PROMPT.strip(),
        "topics": TOPICS_SYSTEM_PROMPT.strip(),
        "narrative": NARRATIVE_SYSTEM_PROMPT.strip(),
        "import_accounts": imports["accounts"].strip(),
        "import_proxies": imports["proxies"].strip(),
        "hashtag_bfs": _HASHTAG_BFS_PROMPT,
        "diagnosis": _DIAGNOSIS_PROMPT,
        "selector": _SELECTOR_PROMPT,
    }
