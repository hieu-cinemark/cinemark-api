from app.services.relevance.trend_posts import caption_key, is_trend_post, parse_cast, repeated_captions, title_keys

TITLE = "Bò Sữa Bay"
CAST = ["Nguyễn Hùng", "Võ Điền Gia Huy"]


def post(content, author="someone"):
    return {"id": "p", "author": author, "content": content}


def trend(item, repeated=frozenset()):
    return is_trend_post(item, TITLE, CAST, set(repeated))


def test_hashtag_only_mention_of_the_film_is_trend():
    assert trend(post("Ngày mẹ đồng ý  #bosuabay #xuhuongtiktok"))
    assert trend(post("#bosuabay #fyp"))


def test_actor_hashtag_is_not_trend():
    assert not trend(post("Lớp mầm tập nhảy #VoDienGiaHuy #bsb #bosuabay #lothuongnhau"))


def test_film_title_in_caption_is_not_trend():
    assert not trend(post('"Bò Sữa Bay" dự kiến khởi chiếu 06.11.2026 #kenhquaysao'))
    assert not trend(post("Hài thật sự Bò Sữa Bay ơiiiii #phimdienanh"))


def test_film_words_are_not_trend():
    assert not trend(post("Đi xem phim về khóc sưng mắt #bosuabay"))
    assert not trend(post("Review thật lòng #bosuabay"))


def test_film_page_is_never_trend():
    assert not trend(post("Tối nay gặp nhau nhé #bosuabay", author="Phim Bò Sữa Bay"))


def test_caption_repeated_by_many_authors_is_trend_even_with_singer_hashtag():
    item = post("Ngày mẹ đồng ý 😂#nguyenhung#bosuabay")
    assert not trend(item)
    assert trend(item, {caption_key(item["content"])})


def test_repeated_captions_counts_distinct_authors():
    posts = [post("Ngày mẹ đồng ý #a", author=name) for name in ("a", "b", "c")] + [post("Câu khác", author="a")] * 3
    assert repeated_captions(posts) == {"ngay me dong y"}


def test_parse_cast_tolerates_bad_data():
    assert parse_cast('["A", 1]') == ["A"]
    assert parse_cast("not json") == []
    assert parse_cast(None) == []


def test_title_keys_cover_short_names():
    assert {"shaunthesheep", "quailaghenha"} <= set(title_keys("Shaun the Sheep: “Quái Lạ” Ghé Nhà"))
    assert {"quyantang4", "quyantang", "hotinh", "teeyod"} <= set(title_keys("QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)"))
    assert {"chichiemem3", "chichiemem"} <= set(title_keys("Chị Chị Em Em 3"))
    assert "tham tu kien".replace(" ", "") in title_keys("Thám Tử Kiên: Lời Nguyền Hoàng Kim")


def test_short_title_in_caption_is_not_trend():
    title = "Shaun the Sheep: “Quái Lạ” Ghé Nhà"
    assert not is_trend_post(post("Shaun the Sheep is back this Halloween"), title, [], set())
    assert not is_trend_post(post("Bữa nay coi Shaun"), "Bữa Nay", [], set())


def test_review_hashtag_is_not_trend():
    assert not trend(post("#review #reviewphim #bosuabay"))
