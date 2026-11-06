from app.ai import jev
from app.ai.aspects import aspect_prompt_lines, parse_aspects
from app.ai.cinema_visit import cinema_visit_aspect
from app.ai.tasks.sentiment import _label, _user_prompt, _with_cinema_visits


def test_cinema_mentions_can_be_neutral_without_changing_existing_polarities():
    assert parse_aspects(
        [
            "di_rap:0",
            "di_rap:+",
            "di_rap:-",
            "di_rap:0",
            {"a": "di_rap", "p": "0"},
            "dien_xuat:0",
            "made_up:0",
            "san_phim:-",
        ]
    ) == ["di_rap:0", "di_rap:+", "di_rap:-", "san_phim:-"]
    assert _label({"sentiment": "neutral", "aspects": ["di_rap:0"], "stage": "hong"}) == {
        "sentiment": "neutral",
        "aspects": ["di_rap:0"],
        "stage": "hong",
    }


def test_both_classifiers_receive_cinema_topic_and_neutral_instructions():
    assert '"di_rap"' in aspect_prompt_lines()
    prompt = _user_prompt(["Ai đi xem phim ở rạp tối nay không?"])
    assert "di_rap:0" in prompt
    assert "ticket prices/showtimes" in prompt
    questions = jev._questions()
    assert "nhac_den" in questions["a_di_rap"]["criteria"]
    assert "nhac_den" not in questions["a_rap_ve"]["criteria"]


def test_jev_does_not_turn_neutral_cinema_mentions_into_praise_or_criticism():
    answers = {
        "sentiment": {"choice": "neutral", "confidence": 0.99},
        "stage": {"choice": "hong", "confidence": 0.99},
        "a_di_rap": {"choice": "nhac_den", "confidence": 0.99},
        "a_rap_ve": {"choice": "nhac_den", "confidence": 0.99},
        "a_unknown": {"choice": "khen", "confidence": 0.99},
    }
    assert jev._label(answers) == {"sentiment": "neutral", "aspects": ["di_rap:0"], "stage": "hong"}
    answers["a_di_rap"]["confidence"] = 0.01
    assert jev._label(answers)["aspects"] == []


def test_local_detection_distinguishes_moviegoing_from_reviews_and_logistics():
    for message in [
        "Ai đi xem phim tối nay không?",
        "Cuối tuần mình ra rạp",
        "Hôm qua đi rạp với bạn",
        "Check-in ở rạp với hội bạn",
        "Tối nay đi xem phim lần hai",
    ]:
        assert cinema_visit_aspect(message) == "di_rap:0"
    for message in [
        "Phim này bao giờ ra rạp?",
        "Phim đã ra rạp chưa?",
        "Giá vé bao nhiêu?",
        "Suất chiếu lúc mấy giờ?",
        "Xem phim này rồi, diễn xuất quá tốt",
        "Đi xem phim trên Netflix ở nhà thôi",
        "Hình ảnh đẹp và cốt truyện rất hay",
    ]:
        assert cinema_visit_aspect(message) is None
    assert cinema_visit_aspect("Mình không muốn ra rạp xem phim này") == "di_rap:-"
    assert cinema_visit_aspect("Rất háo hức đi xem phim với mọi người") == "di_rap:+"
    assert cinema_visit_aspect("Không còn háo hức đi xem phim này") == "di_rap:0"


def test_local_detection_enriches_existing_labels_without_overwriting_sentiment_or_ai_tags():
    message = "Ai đi xem phim tối nay không?"
    label = {"sentiment": "neutral", "aspects": [], "stage": "khac", "source": "visobert"}
    assert _with_cinema_visits([message], [label], "comment") == [{**label, "aspects": ["di_rap:0"]}]
    ai_label = {"sentiment": "positive", "aspects": ["di_rap:+"], "stage": "hong"}
    assert _with_cinema_visits([message], [ai_label], "comment") == [ai_label]
    assert _with_cinema_visits([message], [None], "comment") == [None]
    assert _with_cinema_visits([message], [{**label, "aspects": []}], "post")[0]["aspects"] == []
