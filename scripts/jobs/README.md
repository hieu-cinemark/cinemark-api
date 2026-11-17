# scripts/jobs

Job gán nhãn, tổng hợp và dọn dữ liệu. Phần lớn chạy theo timer trong `deploy/systemd/`; mỗi file có `--help`.

Truy vấn D1 viết bằng SQLAlchemy Core rồi chạy qua `d1_exec` (`app/repositories/d1/sql.py`) - xem
`app/repositories/d1/README.md`. SQL thuần (`d1_query`) chỉ còn ở các câu cần mẹo riêng của D1: DDL/`PRAGMA`, dấu `+`
chặn index (vd. `visobert_label._save`, `aspect_label._save`), và `INSERT ... SELECT ... FROM json_each` (ghi hàng loạt).

## Gom nhóm bình luận tự động

`topic_clusters.py` chạy chuỗi `scripts/tools/{cluster_film_comments,name_clusters,summarize_clusters,publish_clusters}` cho phim đang chiếu (timer `cinemark-clusters`, 03:30 hằng đêm; mỗi phim làm lại khi nhóm cũ hơn 24 giờ, tối đa 3 phim mỗi lượt). Cần embedding trên cùng máy (`embed_comments`).
