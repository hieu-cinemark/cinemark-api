# app/api

Tầng HTTP. Route nằm trong `routes/`, được đăng ký trong `app/main.py`. Mọi route (trừ health) yêu cầu header
`X-API-Key` (xem `app/core/auth.py`).
