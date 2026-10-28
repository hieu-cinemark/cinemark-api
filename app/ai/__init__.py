"""Mọi thứ liên quan tới LLM gom về một chỗ. Mọi lời gọi chạy trên Bee (Beeknoee) - Kira đã bỏ từ 2026-10-21;
Jev (TypeSafe, app/ai/jev.py) chấm sentiment comment:

  client.py    HTTP client tương thích OpenAI, dùng chung - thông tin đăng nhập và model
               (bảng ai_providers, không ghi tên model trong code), retry/backoff, giới
               hạn số lời gọi đồng thời, parse JSON
  runtime.py   chính sách theo task: bật/tắt + system prompt ghi đè từ bảng ai_settings,
               call_llm (Bee)
  bee.py       lớp mỏng call_bee (report chọn model riêng qua settings.bee_report_model)
  defaults.py  system prompt mặc định trong code cho từng task
  prompts/     nội dung prompt của từng task
  tasks/       các bộ phân loại/sinh nội dung xây trên đó: post_relevance (ingest),
               sentiment (sweep comment), import_parser (import hàng loạt ở Settings)
               và report (report social topic)

Bộ phân loại cảm xúc dự phòng không dùng AI (theo từ điển) nằm ở
app/services/sentiment/sentiment_lexicon.py.
"""
