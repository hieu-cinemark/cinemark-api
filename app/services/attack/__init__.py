"""Attack Detection: lượng chê có dấu hiệu bất thường hoặc phối hợp không (docs/review-angles-va-attack.md mục 2).

Trạng thái do quy tắc cố định quyết định (verdict.py), không do AI. Mỗi dấu hiệu là một hàm thuần nhận dữ liệu đã đọc:
  spikes.py   A - chê dồn dập theo giờ (bảng movie_comment_hourly)
  similar.py  B - nhiều tài khoản đăng bình luận chê gần như giống nhau (embedding e5)
  data.py     đọc D1 + file embedding; store.py ghi/đọc bảng attack_signals
Job chạy hằng giờ: app/services/ai_jobs/attack_detect.py. Dấu hiệu C (nhóm tài khoản), D (lan rộng), E (khẳng định, AI) chưa làm.
"""
