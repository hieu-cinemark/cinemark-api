# tests

Test pytest (`pytest` từ gốc repo). `conftest.py` chặn mọi kết nối Redis thật và gọi AI thật; test cần Redis / D1 / AI
thì dùng bản giả qua `monkeypatch`.
