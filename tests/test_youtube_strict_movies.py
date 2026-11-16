from app.core.config import settings
from app.services.relevance.relevance_rules import has_film_context

CON_MAT = {"title": "Con Mắt Thứ Ba", "slug": "con-mat-thu-ba"}
CHUYEN_DI = {"title": "Chuyến Đi Nhớ Đời", "slug": "chuyen-di-nho-doi"}


def test_youtube_only_strict_movies():
    assert settings.is_strict_movie("con-mat-thu-ba", "youtube")
    assert not settings.is_strict_movie("con-mat-thu-ba", "facebook")
    assert not settings.is_strict_movie("con-mat-thu-ba", "tiktok")
    # Phim chặt mọi nền tảng vẫn chặt trên YouTube.
    assert settings.is_strict_movie("cong-tru-nhan-chia", "youtube")
    assert settings.is_strict_movie("cong-tru-nhan-chia", "threads")
    assert not settings.is_strict_movie("an-mang-karaoke", "youtube")


def test_spiritual_and_tv_show_videos_have_no_film_context():
    # Mẫu video YouTube đang hiện dưới phim (16/11/2026) nhưng không nói về phim.
    for text in (
        "Nhìn thế giới qua con mắt thứ ba - Giải mã thần thoại - Phần 6. Nền văn minh tiền sử | Khoa học",
        "Con mắt thứ ba của Nhị Lang Thần có khả năng gì đặc biệt?",
        "Đoạn kinh đó chỉ dạy luyện Tam Bửu, Nam hay Nữ ai cũng có con mắt thứ ba 👍",
    ):
        assert not has_film_context(text, CON_MAT), text
    assert not has_film_context(
        "Chuyến đi nhớ đời: Khi nhà trường 'vô tình' đặt nhầm homestay ma ám #lenhankechuyenma", CHUYEN_DI
    )


def test_real_film_videos_keep_film_context():
    assert has_film_context(
        "Con Mắt Thứ Ba hậu trường: kịch bản ba hồi nhân bốn của đạo diễn Hàm Trần 🇻🇳 Nói thật, đi xem", CON_MAT
    )
    assert has_film_context('Phim "Con Mắt Thứ Ba" trailer chính thức, khởi chiếu tại rạp', CON_MAT)
