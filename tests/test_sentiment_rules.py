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
        # nhiễu: rỗng, sticker, tiếng cười, tag tên không có @
        ("", "neutral"),
        (None, "neutral"),
        ("None", "neutral"),
        ("[Sticker]", "neutral"),
        ("[Sticker] 😍😍", "positive"),
        ("[Sticker] Đã zịiiiiiii", None),
        ("Haha 😂", "neutral"),
        ("hahaha", "neutral"),
        ("kkkkk", "neutral"),
        ("=)))", "neutral"),
        ("hihi ❤️", "positive"),
        ("@emiuu ~", "neutral"),
        ("Nguyễn Văn A", "neutral"),
        ("Trần Thị Bích Lê Văn Cường", "neutral"),
        ("Nguyễn Thanh Tùng 😂😂", "neutral"),
        ("Thái Hoà khó mời lắm", None),
        ("Phu nhân xinh quá", None),
        ("hay mà", None),
        ("haha phim hay quá", None),
    ],
)
def test_rule_sentiment(message, expected):
    assert rule_sentiment(message) == expected
