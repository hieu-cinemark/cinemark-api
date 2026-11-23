"""Từ vựng điện ảnh mạnh: đi cùng tên phim đích thì gần như chắc chắn là bài về phim chiếu rạp."""

from __future__ import annotations

# Từ vựng điện ảnh *mạnh* (đã bỏ dấu, chữ thường): đi cùng tên phim đích thì gần như chắc chắn
# là bài về phim chiếu rạp. Cố ý KHÔNG có "phim" đứng một mình - bài review phim khác ("PHIM:
# LAN HƯƠNG NHƯ CỐ") hay tóm tắt phim truyền hình cũng có chữ đó, và đã lọt như vậy ngày
# 2026-10-05. Cũng không có "rap": bỏ dấu thì "rạp" trùng "rap" (nhạc).
_STRONG_FILM_PHRASES: tuple[str, ...] = (
    "dien anh",
    "man anh",
    "ra rap",
    "tai rap",
    "rap chieu",
    "phong chieu",
    "khoi chieu",
    "cong chieu",
    "suat chieu",
    "lich chieu",
    "chieu rap",
    "phong ve",
    "dat ve",
    "mua ve",
    "trailer",
    "teaser",
    "poster",
    "hau truong",
    "ban dung",
    "cung may",
    "khai may",
    "bam may",
    "dong may",
    "ngay quay",
    "doan phim",
    "ekip",
    "e kip",
    "du an",
    "lien hoan phim",
    "lhp",
    "dien vien",
    "vai dien",
    "dao dien",
    "nha san xuat",
    "nha phat hanh",
    "box office",
    "cgv",
    "lotte cinema",
    "galaxy cinema",
    "beta cinemas",
    "cinestar",
    "premiere",
    "showtime",
    # 19/11/2026: sự kiện/báo chí quanh phim. Bài "Casting Call Cộng Trừ Nhân Chia", "Showcase Mẹ Mìn" bị ẩn
    # vì phim có tên mơ hồ (ambiguous/strict) không có tín hiệu nào qua được; các từ này chỉ tính khi sát tên phim (<= 4 từ). Không thêm "cinetour": #cinetour là hashtag chung, đứng cạnh hashtag tên phim sẽ làm yếu luật phim chặt.
    "casting call",
    "casting",
    "showcase",
    "hop bao",
    "ra mat phim",
    "buoi ra mat",
    "tham do",
)
