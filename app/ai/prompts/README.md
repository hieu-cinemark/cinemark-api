# app/ai/prompts

System prompt và khung user prompt của từng task. Chỉ chứa chuỗi và hằng số, không gọi mạng.

- `defaults.py`: prompt mặc định theo key task; người vận hành có thể ghi đè trong `ai_settings.prompts`.
- Phần định dạng đầu ra bắt buộc được lặp lại trong user prompt, để bản ghi đè system prompt không làm hỏng bộ parse.
- `author_tiers.py`, `channel_niches.py`, `review_angles.py`: prompt của các job AI chạy nền.
