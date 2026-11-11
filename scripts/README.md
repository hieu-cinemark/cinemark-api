# scripts

Chạy bằng `python -m scripts.<thư mục>.<tên>` từ gốc repo.

- `jobs/`: job chạy định kỳ (systemd timer) hoặc chạy tay khi cần.
- `ml/`: gán nhãn dữ liệu và train model.
- `tools/`: công cụ chạy tay: thử nghiệm, dọn dữ liệu, backfill.

Các job trong `jobs/` và các script chạy tay trong `tools/`, `ml/` dựng query D1 bằng SQLAlchemy Core (`d1_exec`,
xem `app/repositories/d1/README.md`). Script viết SQL thuần chỉ khi cần mẹo riêng của D1: DDL
(`CREATE`/`ALTER`/`PRAGMA`), dấu `+` chặn index, và `INSERT ... SELECT` / upsert hàng loạt qua `json_each`.
