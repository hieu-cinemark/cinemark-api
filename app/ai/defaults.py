"""System prompt mặc định trong code cho từng task (tên model chỉ nằm trong các dòng
ai_providers trên Supabase). Settings có thể ghi đè các prompt này (xem dòng
ai_settings trong app/services/accounts/platform_config_db.py); prompt lưu mà rỗng thì quay
về giá trị ở đây, để một ô textarea bị xoá trắng không bao giờ gửi đi system
message rỗng."""

from __future__ import annotations

from app.ai.prompts.post_relevance import POST_RELEVANCE_SYSTEM_PROMPT
from app.ai.prompts.relevance import SYSTEM_PROMPT as RELEVANCE_SYSTEM_PROMPT
from app.ai.prompts.report import NARRATIVE_SYSTEM_PROMPT, TOPICS_SYSTEM_PROMPT
from app.ai.prompts.sentiment import SENTIMENT_SYSTEM_PROMPT

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

# Các key task cố định, lưu trong JSON ai_settings.prompts và được ghi vào log của
# mọi KiraResponse. Các task phía crawl dùng cùng key này bên spider-hub.
AI_PROMPT_TASKS: tuple[str, ...] = (
    "relevance",
    "post_relevance",
    "sentiment",
    "topics",
    "narrative",
    "import_accounts",
    "import_proxies",
    "diagnosis",
    "selector",
)


def default_system_prompts() -> dict[str, str]:
    from app.ai.tasks.import_parser import default_import_system_prompts

    imports = default_import_system_prompts()
    return {
        "relevance": RELEVANCE_SYSTEM_PROMPT.strip(),
        "post_relevance": POST_RELEVANCE_SYSTEM_PROMPT,
        "sentiment": SENTIMENT_SYSTEM_PROMPT.strip(),
        "topics": TOPICS_SYSTEM_PROMPT.strip(),
        "narrative": NARRATIVE_SYSTEM_PROMPT.strip(),
        "import_accounts": imports["accounts"].strip(),
        "import_proxies": imports["proxies"].strip(),
        "diagnosis": _DIAGNOSIS_PROMPT,
        "selector": _SELECTOR_PROMPT,
    }
