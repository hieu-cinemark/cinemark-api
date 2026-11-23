# Chuẩn log và lỗi

Một quy tắc chung cho cinemark-api, spider-hub và Worker cinemark: **log tiếng Việt, lỗi tiếng Anh, cùng một cấu trúc**.
`tests/test_logging_standard.py` (cinemark-api) và test cùng tên ở spider-hub quét mã nguồn, nên lệch chuẩn là CI đỏ.

## 1. Log (tiếng Việt)

Mỗi sự kiện là một dòng, gồm **mã sự kiện** cố định bằng tiếng Anh và **câu tiếng Việt** cho người đọc:

```python
logger.warning("proxy_degraded", platform=platform, proxy_url=url, error=exc)
```
```
2026-11-23T04:39:52Z [warning] proxy_degraded  Proxy suy giảm, tạm nghỉ  platform=facebook proxy_url=… error=timeout error_type=ReadTimeout service=spider-hub
```

| Trường | Ý nghĩa |
|---|---|
| `timestamp`, `level`, `service`, `logger` | Tự thêm. Mọi dòng đều có. |
| `event` | Mã sự kiện, `snake_case`, tiếng Anh, **literal** (không f-string, không biến). |
| `msg` | Câu tiếng Việt, tự thêm từ `core/log_messages.py`. Ở console nằm ngay sau mã sự kiện. |
| `request_id` / `run_id` | Tự thêm theo ngữ cảnh. |
| `error`, `error_type`, `error_code` | Chỉ xuất hiện khi có lỗi, xem mục 3. |
| còn lại | Dữ liệu thay đổi (`platform`, `movie_id`, `attempt`...) đặt vào key=value, **không ghép vào câu**. |

**Đặt mã sự kiện:** `<đối tượng>_<hành động ở dạng đã xong>`, hậu tố cố định: `_started`, `_finished`, `_failed`, `_skipped`,
`_blocked`, `_timeout`, `_updated`, `_deleted`. Phần thay đổi theo ngữ cảnh (tên job, tên nhịp) đặt vào trường
(`scheduler_tick_failed tick=comment_hourly`), không đưa vào mã.

**Câu tiếng Việt trong catalog:** có dấu, không chấm cuối, tối đa 90 ký tự. Mở đầu bằng tiền tố miền (`Facebook:`, `Omar:`, `D1:`) hoặc
`Đã` / `Không` / `Bỏ`. Câu của sự kiện `_failed` phải nói rõ lỗi (`Không ... được`, `... thất bại`). Catalog chia nhóm
`# --- tên nhóm ---` và xếp theo chữ cái trong nhóm.

**Thêm một sự kiện:** viết `logger.info("ten_su_kien", ...)`, thêm một dòng vào `MESSAGES` (đúng nhóm, đúng thứ tự), chạy test.

**Mức log:**

| Mức | Dùng khi |
|---|---|
| `debug` | Chẩn đoán, bản ghi hỏng bị bỏ qua (`json_decode_skipped`), lỗi từng phần không ảnh hưởng kết quả. |
| `info` | Mốc bình thường: bắt đầu, xong, đã cập nhật. |
| `warning` | Lỗi tạm thời đã retry hoặc phần việc suy giảm nhưng hệ thống vẫn chạy. |
| `error` | Thao tác hỏng, cần người xử lý (kèm `telegram=True` khi cần báo). |

## 2. Lỗi (tiếng Anh)

Mọi exception và message trả cho client **bằng tiếng Anh**: câu đầy đủ, viết hoa đầu, không chấm cuối, có định danh khi hữu ích.

```python
raise NotFoundError(f"Account {account_id} not found")
raise ValidationError("Run times must use the HH:MM format")
raise UpstreamError(f"Google returned HTTP {resp.status_code} at {label}")
```

- cinemark-api: dùng cây `AppError` (`app/core/errors.py`): `NotFoundError` 404, `ValidationError` 400, `ConflictError` 409,
  `UpstreamError` 502. Body HTTP luôn là JSON phẳng `{"error": "<message tiếng Anh>", "code": "not_found", "request_id": "..."}`,
  cùng dạng với Worker cinemark; `code` là mã máy đọc được, `request_id` có khi request đi qua middleware.
- Giao diện hiện nguyên message tiếng Anh của lỗi. Chữ tiếng Việt trên giao diện là nhãn của giao diện, không đi qua message lỗi.
- Không dùng `HTTPException` trực tiếp trong code mới.

## 3. Ghi lỗi vào log

Luôn truyền **chính exception** vào `error=`:

```python
except SomeError as exc:
    logger.warning("some_thing_failed", platform=platform, error=exc)
```

Bộ xử lý log tự điền `error` (nội dung, cắt ở 300 ký tự), `error_type` (tên class) và `error_code` (thuộc tính `code` của lỗi ứng dụng, nếu
có). Vì vậy **không** viết `error=str(exc)`, `error=str(exc)[:200]`, `error=repr(exc)`, và không dùng `exc=`, `err=`, `message=`, `detail=`.
Trường `reason` chỉ dành cho lý do nghiệp vụ không phải exception (`reason="content_too_short"`).

## 4. Không nuốt lỗi im lặng

`except ...: pass` / `continue` phải có chú thích lý do ngay cạnh, hoặc thay bằng log/raise:

```python
except WebSocketDisconnect:
    pass  # client đóng WebSocket: kết thúc bình thường
```

Đọc JSON từ Redis/D1 dùng `app.core.jsonutil.loads_or_none(raw, source="...")`: bản hỏng được log `json_decode_skipped` (debug)
thay vì biến mất.

## 5. Ngoại lệ có lý do

- `print()` chỉ trong script CLI; trong `app/` chỉ `app/services/ai_jobs/attack_detect.py` (CLI in kết quả ra terminal).
- `logging.getLogger` chỉ để cấu hình logger của thư viện bên thứ ba (`app/services/google_news.py` che API key trong log của httpx).

Danh sách ngoại lệ nằm ở đầu `tests/test_logging_standard.py`; thêm ngoại lệ mới phải sửa test cùng lý do.
