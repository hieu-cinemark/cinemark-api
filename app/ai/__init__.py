"""Mọi thứ liên quan tới LLM, cả hai provider (Kira, Bee), gom về một chỗ:

  client.py    HTTP client tương thích OpenAI, dùng chung cho mọi provider - thông tin
               đăng nhập và model (bảng ai_providers, không ghi tên model trong code),
               retry/backoff, giới hạn số lời gọi đồng thời, parse JSON
  kira.py      chính sách theo task của Kira: bật/tắt theo task + system prompt ghi đè
               từ bảng ai_settings, call_kira
  bee.py       lớp mỏng cho Beeknoee (call_bee) - chỉ dùng khi chọn Bee viết report
  defaults.py  system prompt mặc định trong code cho từng task
  prompts/     nội dung prompt của từng task
  tasks/       các bộ phân loại/sinh nội dung xây trên đó: post_relevance (ingest),
               sentiment (sweep comment), import_parser (import hàng loạt ở Settings)
               và report (report social topic) đều chạy trên Kira; report có thể
               chuyển sang Bee từ dashboard, khi đó Kira làm dự phòng

Bộ phân loại cảm xúc dự phòng không dùng AI (theo từ điển) nằm ở
app/services/sentiment_lexicon.py.
"""
