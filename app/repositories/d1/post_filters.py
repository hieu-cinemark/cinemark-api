"""Điều kiện lọc và điểm tương tác dùng chung cho mọi truy vấn bài: bài liên quan, độ dài tối thiểu, công thức engagement."""

from __future__ import annotations

from sqlalchemy import and_, or_

from app.repositories.d1.tables import ENGAGEMENT_COLUMNS

ENGAGEMENT_FIELDS = ENGAGEMENT_COLUMNS

# Dưới số ký tự này (sau khi trim), nội dung bài bị coi là rác - một reaction/emoji/
# comment một chữ trơ trọi, không có gì để phân tích. Chỉnh thoải mái; đây là con số
# cảm tính, không suy ra từ đâu cả.
MIN_CONTENT_LENGTH = 10

# Chỉ tính tương tác, cố ý bỏ view_count: lượt xem là hiển thị thụ động, không phải
# "tương tác" - nếu không, số lượt xem lớn sẽ áp đảo hoàn toàn thứ tự sắp xếp, vì nó
# thường lớn gấp 10-100 lần tổng số like/reply/repost/quote/reshare cộng lại, biến
# cách sắp xếp này thành "xem nhiều nhất" khoác nhãn "tương tác".
#
# Mỗi comment (reply_count) nặng gấp COMMENT_WEIGHT lần một like/share: bài nhiều comment
# nhưng ít like trước đây rơi khỏi top 100, nên lượt lấy comment hằng ngày (list_posts_needing_comments)
# bỏ qua đúng những bài có nhiều ý kiến khán giả nhất. Mô phỏng trên D1 thật: với hệ số 3, top 100 của các
# từ khoá Facebook có thêm ~13.300 comment, đổi lại mất ~1.400. Danh sách top 100 trên dashboard dùng
# chung công thức này để thứ tự hiển thị khớp với thứ tự crawl.
COMMENT_WEIGHT = 3

ENGAGEMENT_SCORE_SQL = (
    f"(p.like_count + {COMMENT_WEIGHT} * p.reply_count + p.repost_count + p.quote_count + p.reshare_count)"
)

# Bỏ khoảng trắng / dấu câu để "#Anh Hùng" và "#AnhHung" đều khớp tên phim "Anh Hùng".

# "Bài này được tính là nói về phim của nó" cho các query report/danh sách top/quét
# comment. relevance_label chỉ được ghi khi bộ phân loại thực sự chạy: bài được nhận
# qua lối tắt chuỗi con lúc ingest (nội dung chứa từ khoá, ví dụ mọi bài tìm được qua
# chính hashtag của phim) giữ nhãn NULL với keyword_match=1. Chỉ đòi
# relevance_label='related' thì đã âm thầm loại hết những bài đó - đã xác nhận
# 2026-09-28 với "SCOTTY: GIẢI CỨU HOÀNG THƯỢNG": 115 comment đã phân loại cảm xúc,
# tất cả đều nằm dưới bài có nhãn NULL, nên report của phim báo "không đủ comment".
# Nhãn mà bộ phân loại hoặc quy tắc đã ghi (not_related/uncertain) vẫn được ưu tiên.
RELEVANT_POST_SQL = "(p.relevance_label = 'related' OR (p.relevance_label IS NULL AND p.keyword_match > 0))"


def relevant_post(p):
    """RELEVANT_POST_SQL cho query SQLAlchemy (p: bảng posts hoặc alias của nó)."""
    return or_(p.c.relevance_label == "related", and_(p.c.relevance_label.is_(None), p.c.keyword_match > 0))


def engagement_score(p):
    """ENGAGEMENT_SCORE_SQL cho query SQLAlchemy."""
    return p.c.like_count + COMMENT_WEIGHT * p.c.reply_count + p.c.repost_count + p.c.quote_count + p.c.reshare_count
