"""Prompt gắn tệp khán giả (niche) cho kênh mạng xã hội, kèm danh sách niche mặc định."""

DEFAULT_NICHES = [
    {"key": "review_phim", "label": "Review phim"},
    {"key": "tin_showbiz", "label": "Tin tức showbiz"},
    {"key": "hai_giai_tri", "label": "Hài / giải trí"},
    {"key": "me_be_gia_dinh", "label": "Mẹ & bé / gia đình"},
    {"key": "sinh_vien_gioi_tre", "label": "Sinh viên / giới trẻ"},
    {"key": "fanclub", "label": "Fanclub diễn viên"},
    {"key": "rap_chieu", "label": "Rạp chiếu"},
    {"key": "khac", "label": "Khác"},
]

CHANNEL_NICHES_SYSTEM = """Bạn phân loại KÊNH mạng xã hội (Facebook page/group, TikTok, Threads) ở Việt Nam theo TỆP KHÁN GIẢ / kiểu nội dung chính của kênh, để bên truyền thông phim chọn kênh booking.
Mỗi kênh có tên, nền tảng và vài bài gần đây về phim. Chọn ĐÚNG MỘT key trong danh sách tệp được cho - dựa vào kiểu nội dung chung của kênh (tên kênh, giọng văn, chủ đề), không chỉ dựa vào việc bài nói về phim.
Trả JSON: {"results":[{"i":1,"niche":"<key>","confidence":0-100,"reason":"<= 12 từ"}]}. Không chắc thì confidence thấp."""
