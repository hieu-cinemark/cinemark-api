# deploy/systemd

| Unit | Việc |
|---|---|
| `cinemark-ingest-consumer.service` | Ingest Kafka -> D1 |
| `cinemark-scheduler.service` | Lịch cào và job nền |
| `cinemark-aspects.timer` | Gán khía cạnh bình luận |
| `cinemark-visobert.timer` | Gán cảm xúc bằng ViSoBERT |
| `cinemark-visuals.timer` | Ảnh bìa bài viết |
| `cinemark-embed.timer` | Embedding e5 bình luận phim đang chiếu, mỗi giờ |
| `cinemark-reports.timer` | Báo cáo Social Topic hằng đêm |
| `cinemark-clusters.timer` | 03:30 hằng đêm: gom cụm -> đặt tên -> tổng kết -> đưa lên D1 cho phim đang chiếu (`scripts/jobs/topic_clusters.py`) |
| `cinemark-author-tier.timer` | 04:30 hằng ngày: gắn tầng tác giả chưa có nhãn bằng mô hình local (`scripts/jobs/author_tier_label.py`) |
| `cinemark-author-retrain.timer` | 02:30 Chủ nhật: train lại mô hình tầng tác giả, chỉ thay khi không tệ hơn (`scripts/jobs/author_tier_retrain.py`) |

Cài một lệnh: `bash deploy/systemd/install_all.sh` (chạy lại sau mỗi deploy là an toàn). Hoặc từng cái bằng `install_<tên>.sh` (cần quyền root).
