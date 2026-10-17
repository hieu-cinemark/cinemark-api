"""Các bài thật gặp trên production (2026-09-28) cho app/services/relevance_rules.py."""

from __future__ import annotations

import pytest

from app.services.relevance_rules import (
    film_context_reason,
    foreign_language_reason,
    has_film_context,
    mentions_keyword_or_title,
    mentions_other_film,
)

TRACKED = ["Án Mạng Karaoke", "Án Mạng Xém Hoàn Hảo", "Trại Buôn Người", "Trại Giam Hạnh Phúc", "Út Lan", "Út Lan 2"]


@pytest.mark.parametrize(
    "text",
    [
        "que no ocurra eso por favor #fiesta #astonmartin #gerardromero #memin #f1",
        "Muy deliciosa la comida.. siempre me encanta ir ai a comer.. se los super recomiendo #galerias #MeMin",
        "Grateful for the people around and a big shoutout to the team keralastartupfest #startup #memin",
        "🚨  ఇంత దిక్కుమాలిన సినిమా ఎప్పుడూ చూడలేదు..   సంఘం శరత్ థియేటర్ లో టికెట్ #BongMaNhaHat",
    ],
)
def test_foreign_posts_are_flagged(text: str) -> None:
    assert foreign_language_reason(text) is not None


@pytest.mark.parametrize(
    "text",
    [
        "Phim hay qua di xem di moi nguoi oi #memin",  # tiếng Việt không dấu
        "𝐞𝐛𝐞́ 🍓 𝟖 𝐭𝐮𝐨̂̉𝐢 𝐜𝐮̉𝐚 𝐭𝐮𝐢 𝐯𝐚̀𝐨 𝐯𝐚𝐢 𝐇𝐨𝐚̀𝐧𝐠 𝐇𝐚̣̂𝐮",  # tiếng Việt bằng Unicode trang trí
        "One more time for cinetourrr😭 “ a memory to keep “ ❤️ #nghihesonghihuu",  # tiếng Anh nói về phim
        "#memin #fyp",  # chỉ có hashtag: không có phán quyết nếu không có ngôn ngữ của nền tảng
        "Pass vé ngày mai 100k 2 vé PHIM TRẠI BUÔN NGƯỜIIIII 22H30",
    ],
)
def test_vietnamese_or_undecidable_posts_are_kept(text: str) -> None:
    assert foreign_language_reason(text) is None


def test_platform_language_decides_hashtag_only_posts() -> None:
    assert foreign_language_reason("#memin #fyp", "es") == "platform_language=es"
    assert foreign_language_reason("#memin #fyp", "vi") is None
    # Dấu tiếng Việt được ưu tiên hơn tag ngôn ngữ sai của nền tảng.
    assert foreign_language_reason("Phim này hay quá #memin", "en") is None


def test_other_film_detected_when_target_absent() -> None:
    other = mentions_other_film(
        "Và đây là Lan Trinh trong Án Mạng Xém Hoàn Hảo", "Án Mạng Karaoke", "Án Mạng Karaoke", TRACKED
    )
    assert other == "Án Mạng Xém Hoàn Hảo"
    assert (
        mentions_other_film(
            "PASS VÉ PHIM RẠP CINESTAR... pass vé xem phim TRẠI BUÔN NGƯỜI",
            "Trại Giam Hạnh Phúc",
            "Trại Giam Hạnh Phúc",
            TRACKED,
        )
        == "Trại Buôn Người"
    )


def test_target_mention_wins_over_other_film() -> None:
    assert (
        mentions_other_film("Án Mạng Karaoke vs Án Mạng Xém Hoàn Hảo", "Án Mạng Karaoke", "#AnMangKaraoke", TRACKED)
        is None
    )
    # Dạng hashtag của từ khoá mục tiêu cũng được tính là có nhắc tới.
    assert (
        mentions_other_film(
            "xem #AnMangKaraoke rồi mới tới Án Mạng Xém Hoàn Hảo", "Án Mạng Karaoke", "#AnMangKaraoke", TRACKED
        )
        is None
    )


def test_nested_and_short_titles_are_ignored() -> None:
    assert mentions_other_film("Út Lan hay quá", "Út Lan 2", "#UtLan2", TRACKED) is None


# Bài thật của phim "Người Được Chọn" (2026-10-05): tên phim là cụm từ thường ngày, nên khi Kira
# không kết luận chỉ những bài có tín hiệu gắn với ĐÚNG phim này mới được giữ.
NGUOI_DUOC_CHON = {"title": "Người Được Chọn"}


@pytest.mark.parametrize(
    "content",
    [
        "Phim điện ảnh Người được chọn khởi chiếu từ 6/11/2026 #nguoiduocchon #haudong",
        "Người Được Chọn dự kiến khởi chiếu: 06.11.2026 #nhatkydusao #nguoiduocchon",
        "hóng quá, tháng 11 đến luôn được không nhỉ #phimnguoiduocchon #nguoiduocchon",
        "NSUT Hoài Linh góp mặt trong phim “Người Được Chọn”, dự án điện ảnh về hầu đồng",
        "+1 phim điện ảnh tham gia đường đua phòng vé 2026 #nguoiduocchon #haudong",
        'Chút hậu trường của NSƯT Hạnh Thuý "Người Được Chọn" là phim điện ảnh đầu tiên về hầu đồng',
        'Chúc mừng dự án "Người được chọn" góp mặt tại LHP Cannes 2026✨',
        "Bùng Cháy hoyy🔥♥️ Con chúc mừng Lão và cả ekip NGƯỜI ĐƯỢC CHỌN #nguoiduocchon #TheChosenOne",
    ],
)
def test_has_film_context_keeps_target_film_posts(content: str) -> None:
    assert has_film_context(content, NGUOI_DUOC_CHON)


@pytest.mark.parametrize(
    "content",
    [
        "[BEAT] Người Được Chọn - Team B Ray #nguoiduocchon #bray #rapviet #beat #rap",
        "Vì anh không phải là NGƯỜI ĐƯỢC CHỌN #nguoiduocchon #alihoangduong",
        "Người được chọn đây là sứ mệnh cuộc đời bạn! #baihoccuocsong #nguoiduocchon",
        "#nguoiduocchon #nguoiduoctroichon",
        # Tóm tắt phim khác: có chữ "PHIM" và cụm "người được chọn" theo nghĩa thường.
        "Nhà họ Lâm ép mẹ Viên nhượng bộ về người được chọn. 📍 PHIM: LAN HƯƠNG NHƯ CỐ #Cdrama",
        # Tên bài hát trùng tên phim, đặt trong ngoặc kép.
        "‘Người Được Chọn’ is OUT NOW ✨ Cùng thưởng thức ca khúc mới của Ali Hoàng Dương",
        'Bray và Captain tập luyện ver của mình trong bài " Người Được Chọn " #bray #rapviet',
        # Phim khác có "khởi chiếu", nhưng nằm xa cụm từ trùng tên.
        (
            "Yêu là mong người ấy hạnh phúc, kể cả những năm tháng mình chưa phải người được chọn.\n\n"
            "Phim Madam T Xuất Chiêu | Khởi chiếu 25.09.2026 #MDTXC #MadamTXuatChieu"
        ),
        # Review phim khác dùng cụm từ - Kira từng gán "related" 0,95 (2026-10-05).
        (
            "CÁI KẾT KHÓ CHỊU NHẤT CỦA BỘ PHIM!!! Lâm tốt — tốt đến mức chấp nhận đứng ngoài câu chuyện. "
            "Còn Thương… lại chọn quay về. Đôi khi người được chọn không phải người tốt nhất"
        ),
        "Quốc Bảo và áp lực của người được chọn. Bận xù đầu mà nghe nói phim sắp hết chiếu nên phải đi coi.",
        # Có chữ phim nhưng không nhắc phim đích.
        "#PhimNgan #KhoaHocVienTuong #PhimVienTuong #ThucTinh",
    ],
)
def test_has_film_context_rejects_other_content(content: str) -> None:
    assert not has_film_context(content, NGUOI_DUOC_CHON)


def test_has_film_context_accepts_cast_name() -> None:
    movie = {**NGUOI_DUOC_CHON, "cast": "Hoài Linh, Hạnh Thuý", "director": None}
    assert has_film_context("NSƯT Hoài Linh xuất hiện tại buổi lễ hôm nay #haudong", movie)
    assert not has_film_context("Linh ơi về đi #nguoiduocchon", movie)  # tên một chữ không tính


def test_has_film_context_needs_a_title() -> None:
    assert not has_film_context("Phim điện ảnh khởi chiếu tại rạp tháng 11", {})


@pytest.mark.parametrize(
    "content",
    [
        "Bộ phim #PhimNguoiDuocChon khởi chiếu 06.11",
        "Hóng phim Người Được Chọn quá trời",
        "NGƯỜI ĐƯỢC CHỌN - phim điện ảnh về hầu đồng",  # đủ tên phim dù thiếu chữ "phim" của từ khoá
    ],
)
def test_mentions_keyword_or_title_accepts_full_keyword_or_title(content: str) -> None:
    assert mentions_keyword_or_title(content, ["#PhimNguoiDuocChon"], "Người Được Chọn")


@pytest.mark.parametrize(
    "content",
    [
        # Bài phim truyền hình khác lọt qua tìm kiếm Facebook ngày 2026-10-05, Kira gán "related".
        "PHƯƠNG VỪA CHỐT CHỌN KHÁNH, TÙNG ĐÃ NHẬN 400 TRIỆU TỪ VƯƠNG? CHUYỆN GÌ ĐÂY?",
        "Người ấy đã được chọn làm nữ chính",  # đủ chữ nhưng không liền thành tên phim
        "",
    ],
)
def test_mentions_keyword_or_title_rejects_partial_matches(content: str) -> None:
    assert not mentions_keyword_or_title(content, ["#PhimNguoiDuocChon"], "Người Được Chọn")


def test_mentions_keyword_or_title_needs_every_plus_part() -> None:
    assert mentions_keyword_or_title("Đất Rừng Phương Nam bản phim điện ảnh", ["đất rừng+điện ảnh"], None)
    assert not mentions_keyword_or_title("Đất Rừng Phương Nam bản truyền hình", ["đất rừng+điện ảnh"], None)


def test_mentions_keyword_or_title_accepts_another_keyword_of_the_movie() -> None:
    title = "Thám Tử Kiên: Lời Nguyền Hoàng Kim"
    content = "Thám Tử Kiên 2 tung trailer mới"
    assert mentions_keyword_or_title(content, ["#ThamTuKien"], title)  # "thamtukien" có trong bài
    assert mentions_keyword_or_title(content, ["Thám Tử Kiên 2"], title)
    assert not mentions_keyword_or_title("Thám tử lừng danh Conan", ["Thám Tử Kiên 2"], title)


TRAI_BUON_NGUOI = {"title": "Trại Buôn Người", "cast": "Steven Nguyễn, Quách Ngọc Ngoan"}


@pytest.mark.parametrize(
    "content",
    [
        # Caption TikTok thật (2026-10-06) - Kira trả "uncertain", nội dung phim nằm trong video.
        "#traibuonnguoi ",
        "Cổ khổ  #traibuonnguoi",
        "Ba ơi cứu em 😥😥😥 #traibuonnguoi ",
        "Trả lời @Ủn ỉn bay lên Trời video cận cảnh kkk #traibuonnguoi #cinetour #xuhuong",
        "Ảnh ngầu ảnh đẹp #TraiBuonNguoi",
    ],
)
def test_title_hashtag_counts_when_allowed(content: str) -> None:
    assert film_context_reason(content, TRAI_BUON_NGUOI, allow_title_hashtag=True) == "title_hashtag"
    # Mặc định (phim "chặt") hashtag tên phim không đủ.
    assert film_context_reason(content, {"title": "Trại Buôn Người"}) is None


def test_title_hashtag_ignores_short_common_titles() -> None:
    assert film_context_reason("Hôm nay vui quá #anhhung", {"title": "Anh Hùng"}, allow_title_hashtag=True) is None
    # Tên ba chữ dù ngắn vẫn đặc trưng.
    assert film_context_reason("Cười xỉu #utlan2", {"title": "Út Lan 2"}, allow_title_hashtag=True) == "title_hashtag"


def test_title_hashtag_needs_the_whole_title_as_one_tag() -> None:
    # Tên phim nằm rải trong câu chữ thường, không phải hashtag: không tính.
    assert (
        film_context_reason("trại buôn người là vấn nạn xã hội", {"title": "Trại Buôn Người"}, allow_title_hashtag=True)
        is None
    )


def test_cast_hashtag_counts() -> None:
    assert film_context_reason("Đỉnh. #stevennguyen #fyp", TRAI_BUON_NGUOI) == "cast"
    assert film_context_reason("Chúc đủ thứ #QuachNgocNgoan", TRAI_BUON_NGUOI) == "cast"
    assert film_context_reason("Đỉnh. #steven #fyp", TRAI_BUON_NGUOI) is None


def test_names_can_be_ignored_for_strict_movies() -> None:
    gossip = "Thanh Hương chia sẻ về nguyên nhân đổ vỡ hôn nhân #nguoiduocchon"
    movie = {"title": "Người Được Chọn", "cast": "Hoài Linh, Thanh Hương"}
    assert film_context_reason(gossip, movie) == "cast"
    assert film_context_reason(gossip, movie, allow_names=False) is None
    assert film_context_reason("Phim Người Được Chọn có Thanh Hương", movie, allow_names=False) == "phim_title"


_UT_LAN_2 = {"title": "Út Lan 2", "keywords": ["#PhimUtLan", "#UtLan2", "#UtLan2Movie", "Út Lan 2"]}
_TRAI_BUON_NGUOI = {"title": "Trại Buôn Người", "keywords": ["#TraiBuonNguoi2026", "#TraiBuonNguoiMovie"]}
_SHAUN = {"title": "Shaun the Sheep: “Quái Lạ” Ghé Nhà", "keywords": ["#ShaunTheSheep", "#QuaiLaGheNha"]}
_HHCC = {"title": "Hoàng Hậu Cuối Cùng", "keywords": ["#hhcccasting", "#HoangHauCuoiCungMovie"]}


@pytest.mark.parametrize(
    ("content", "movie"),
    [
        ("#traibuonnguoimovie #xuhuong", _TRAI_BUON_NGUOI),
        ("visual quá #UtLan2Movie #fyp", _UT_LAN_2),
        ("cừu dễ thương #QuaiLaGheNha #CGV", _SHAUN),
    ],
)
def test_film_specific_keyword_hashtag_counts(content: str, movie: dict) -> None:
    assert film_context_reason(content, movie, allow_title_hashtag=True) == "keyword_hashtag"


@pytest.mark.parametrize(
    ("content", "movie"),
    [
        # phim phần 2, từ khoá không số -> bài phần 1
        ("dàn cast giao lưu #PhimUtLan #UtLanOanLinhGiuCua", _UT_LAN_2),
        # tên loạt phim
        ("#shaunthesheep #cartoon", _SHAUN),
        # viết tắt dễ trùng
        ("#CapCut #hhcccasting", _HHCC),
    ],
)
def test_ambiguous_keyword_hashtags_do_not_count(content: str, movie: dict) -> None:
    assert film_context_reason(content, movie, allow_title_hashtag=True) is None


def test_keyword_hashtag_off_for_strict_movies() -> None:
    assert film_context_reason("#traibuonnguoimovie", _TRAI_BUON_NGUOI, allow_title_hashtag=False) is None


def test_full_title_lifts_uncertain_for_long_titles():
    from app.services.relevance_rules import mentions_full_title, resolve_relevance

    movie = {"title": "Bóng Ma Nhà Hát", "slug": "bong-ma-nha-hat"}
    text = "ai đang lăn tăn tối nay coi gì thì Bóng Ma Nhà Hát chiếu rồi đó, t vừa coi suất chiều"
    assert mentions_full_title(text, movie["title"])
    assert mentions_full_title("#BongMaNhaHat hay", movie["title"])
    assert resolve_relevance("uncertain", text, movie, has_keyword=True, strict=False) == (
        True,
        "related",
        "full_title",
    )
    # tên ngắn (trùng từ thường ngày) và phim tên chung chung thì không nâng
    assert not mentions_full_title("bình yên đến lạ #memin", "Mẹ Mìn")
    assert (
        resolve_relevance(
            "uncertain",
            "Cộng, Trừ, Nhân, Chia có đủ",
            {"title": "Cộng Trừ Nhân Chia"},
            has_keyword=True,
            strict=False,
            full_title_rule=False,
        )[0]
        is False
    )


def test_full_title_accepts_numbered_head_before_colon():
    from app.services.relevance_rules import mentions_full_title

    title = "QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)"
    assert mentions_full_title("Xem QUỶ ĂN TẠNG 4 chỉ từ 40.000Đ/vé tại App CGV", title)
    assert mentions_full_title("đi xem #QuyAnTang4 thôi", title)
    assert not mentions_full_title("quỷ ăn tạng 3 xem lại vẫn sợ", title)
    assert not mentions_full_title("Quỷ Ăn Tạng 40 năm trước", title)


def test_full_title_ignores_unnumbered_series_head():
    from app.services.relevance_rules import mentions_full_title

    assert not mentions_full_title("#thamtukien cinetour Cần Thơ", "Thám Tử Kiên: Lời Nguyền Hoàng Kim")
    assert not mentions_full_title("Shaun the Sheep hay quá", "Shaun the Sheep: “Quái Lạ” Ghé Nhà")


def test_director_split_on_va():
    from app.services.relevance_rules import film_context_reason

    movie = {"title": "Lên Hương", "director": "Khương Ngọc và Tấn Hoàng Thông", "cast": '["Thu Trang"]'}
    assert film_context_reason("Coi hài mà cũng buồn nữa #LenHuong #KhuongNgoc", movie) == "director"


def test_release_window_lifts_series_name_with_cinema_words_near_release():
    from app.services.relevance_rules import franchise_names, in_release_window

    shaun = {"title": "Shaun the Sheep: “Quái Lạ” Ghé Nhà", "released_at": "2026-10-16"}
    text = "SHAUN THE SHEEP hôm nay có buổi ra mắt cực vui tại Vạn Hạnh Mall. Phim có suất sớm cuối tuần này"
    assert "shaun the sheep" in franchise_names(shaun["title"])
    assert in_release_window(text, shaun, "2026-10-12T10:00:00")
    assert not in_release_window(text, shaun, "2025-06-01T10:00:00")  # video cũ
    assert not in_release_window("Shaun the Sheep funny clip #cartoon", shaun, "2026-10-17")  # không có từ rạp
    tee = {"title": "QUỶ ĂN TẠNG 4: HỔ TINH (Tee Yod)", "released_at": "2026-10-09"}
    assert "tee yod" in franchise_names(tee["title"])
    assert in_release_window("buổi ra mắt phim Tee Yod 4 #premiere", tee, "2026-10-08")
