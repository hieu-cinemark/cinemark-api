import pytest

from app.ai.tasks.sentiment_rules import rule_sentiment


@pytest.mark.parametrize(
    "message,expected",
    [
        ("❤️❤️❤️", "positive"),
        ("🔥🔥", "positive"),
        ("😡😡", "negative"),
        ("😂😂😂", "neutral"),
        ("@Nguyễn Văn A", "neutral"),
        ("ok", "neutral"),
        ("@Lan @Hoa", "neutral"),
        ("@Lan đẹp quá", None),
        ("Đỉnh quá chị ngọc ơi", None),
        ("Chúc phim cháy vé nha", None),  # có chữ -> Kira
    ],
)
def test_rule_sentiment(message, expected):
    assert rule_sentiment(message) == expected
