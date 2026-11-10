"""Job AI chuyển từ Worker cinemark sang (10/11/2026): gắn nhãn tác giả, niche kênh, Review Angles.

Worker chỉ còn đọc kết quả (nhãn trong cinemark-db, channel_profiles trong cinemark-scraper, state.json của Review Angles trên
R2); việc gọi AI chạy ở đây cùng Bee client và ngân sách AI của cinemark-api. Logic giữ đúng như bản TS cũ.
"""
