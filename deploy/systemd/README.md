# deploy/systemd

| Unit | Việc |
|---|---|
| `cinemark-ingest-consumer.service` | Ingest Kafka -> D1 |
| `cinemark-scheduler.service` | Lịch cào và job nền |
| `cinemark-aspects.timer` | Gán khía cạnh bình luận |
| `cinemark-visobert.timer` | Gán cảm xúc bằng ViSoBERT |
| `cinemark-visuals.timer` | Ảnh bìa bài viết |
| `cinemark-reports.timer` | Báo cáo Social Topic hằng đêm |

Cài bằng `install*.sh` tương ứng (cần quyền root).
