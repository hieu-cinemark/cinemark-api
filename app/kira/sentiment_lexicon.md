# Từ điển gán nhãn sentiment (Social Topic)

Dùng cho comment FB / TikTok / Threads về **phim Việt** (trailer, diễn viên, rạp, studio).
Label ghi vào DB đúng 3 giá trị pipeline đang dùng: `positive` | `negative` | `neutral`.

Social Topic đọc cột `comments.sentiment` → % overall + sample cho Kira cluster topic.
Comment ngắn hơn ~10 ký tự thường bỏ qua (không gán).

---

## Quy tắc quyết định nhanh (30 giây / comment)

1. Comment **có thái độ rõ về phim / cast / trailer / giá vé / lịch chiếu** không?
   - Có + khen / muốn xem / ủng hộ → `positive`
   - Có + chê / không xem / thất vọng / chế giễu → `negative`
   - Không (hỏi thông tin, spam, lạc đề, mơ hồ) → `neutral`
2. **Sarcasm / đá xoáy**: đọc ý thật, không đọc literal.
   - “Hay quá =)) flop chắc” → `negative`
   - “Chán thế, phải đi xem liền” (đùa ủng hộ) → `positive`
3. **Vừa khen vừa chê**: chọn phía **chủ đạo** (thường là mệnh đề kết / “nhưng…”).
   - “Diễn ổn nhưng kịch bản tệ” → `negative`
   - “Kịch bản hơi dài nhưng khóc thật” → `positive`
4. Chỉ emoji / “kk” / “+1” / tag bạn không rõ stance → `neutral` (hoặc bỏ nếu quá ngắn).
5. Không đoán ý khi không chắc → `neutral`.

---

## POSITIVE — tín hiệu hay gặp

### Khen / xúc động
hay, đỉnh, đỉnh cao, xuất sắc, tuyệt, đã quá, khóc, cảm động, chill, cuốn, đã mắt, đã tai, chất, xịn, nét, mãn nhãn, đã đời, ưng, mê, thích, yêu, tâm đắc

### Muốn xem / ủng hộ
ủng hộ, phải xem, đi xem, chờ phim, mong chờ, hóng, book vé, mua vé, rạp nào cũng xem, trailer xong là muốn ra rạp, 10 điểm, 9/10

### Khen cast / crew
diễn đạt, đóng hay, diễn sâu, hoá thân, chemistry, đẹp trai, xinh, visual, OST hay, nhạc phim đã, quay đẹp, hình ảnh đẹp

### Teencode / mạng
đỉnh vcl, đỉnh vc, đỉnh thật, hay vl, hay vcl, best, love, goat, fire, 🔥, ❤️, 😍, 😭 (khóc vì hay — kèm chữ khen)

### Cụm mẫu
- “Xem xong khóc muốn xỉu”
- “Ủng hộ phim Việt”
- “Trailer đỉnh, chờ ngày công chiếu”
- “Diễn viên đóng đạt quá”

---

## NEGATIVE — tín hiệu hay gặp

### Chê nội dung / kỹ thuật
dở, tệ, chán, nhạt, lạc đề, lê thê, dài dòng, kịch bản lỏng, logic kém, sáo, nhàm, kém, tệ hại, flop, fail, phí tiền, phí thời gian, xem không nổi, tắt ngang, bỏ giữa chừng

### Không xem / thất vọng
không xem, chắc không đi, hết muốn xem, thất vọng, thất vọng nặng, quảng cáo lố, overhyped, thổi quá, fake trailer, lừa, gạt

### Chê cast / sản xuất
diễn đơ, diễn gỗ, diễn tệ, casting sai, OST dở, quay xấu, CGI xấu, hiệu ứng rẻ tiền, làm lố

### Chế giễu / mỉa
=)) (kèm đá), vcl (kèm chê), trash, garbage, nản, ngán, ói, hết cứu, toang, sập, bay màu

### Cụm mẫu
- “Kịch bản lỏng, xem phí tiền”
- “Trailer chán, không đi xem đâu”
- “Quảng cáo lố quá, coi mà thất vọng”
- “Flop chắc rồi =))”

---

## NEUTRAL — tín hiệu hay gặp

### Hỏi thông tin (không opinion)
rạp nào, suất mấy giờ, giá vé, bao nhiêu tiền, ngày nào chiếu, chiếu chưa, ở đâu xem, link đâu, ai đóng, tên phim gì, bao giờ ra

### Thuần sự kiện / nhắc tên
đưa lịch chiếu, nhắc tên diễn viên không khen/chê, quote trailer không thái độ, “có phim này à”

### Spam / lạc đề / quá ngắn
bán hàng, link lạ, tag bạn không nội dung, “ok”, “hmm”, “?”, sticker đơn

### Cụm mẫu
- “Suất 8h tối ở rạp nào?”
- “Ai đóng vai chính vậy?”
- “Giá vé bao nhiêu”
- “Bao giờ chiếu vậy mọi người”

---

## Bẫy thường gặp (đọc kỹ)

| Comment | Dễ nhầm | Đúng |
|---|---|---|
| “Hay quá trời =)) chắc flop” | positive | **negative** (mỉa) |
| “Không hay sao được” | negative | **positive** (khẳng định hay) |
| “Chán phải không? Không, hay lắm” | negative | **positive** |
| “Giá vé 100k” | negative (đắt?) | **neutral** (chỉ nêu số, không chê) |
| “Giá vé đắt quá không đi” | neutral | **negative** |
| “Khóc quá” | ambiguous | xem ngữ cảnh trailer cảm động → **positive**; bị spoil/ghét → **negative** |
| “Xem chưa?” | — | **neutral** |
| “Xem chưa? Hay lắm vào đi” | — | **positive** |

---

## Workflow gán nhanh (Excel / Sheets / DB)

Cột gợi ý: `id` | `message` | `sentiment` | `note`

1. Sort theo `reactions_count` DESC — gán trước comment engagement cao (Social Topic ưu tiên sample này).
2. Scan từ khoá ở trên (Ctrl+F từng cụm mạnh: phí tiền, ủng hộ, rạp nào, flop…).
3. Chỉ mở đọc full khi:
   - có “nhưng / mà / =)) / chắc”
   - vừa có từ positive + negative
4. Ghi đúng: `positive` / `negative` / `neutral` (tiếng Anh, lowercase — khớp DB + UI).
5. Xong batch → chạy lại report:

```bash
cd cinemark-api && source .venv/bin/activate
python -m scripts.generate_social_topic_reports --movie-id <id>
```

### Ghi thẳng SQLite local

```sql
UPDATE comments
SET sentiment = 'positive',  -- hoặc negative / neutral
    sentiment_classified_at = CURRENT_TIMESTAMP
WHERE id = '<comment_id>';
```

Hoặc backfill AI (không dùng từ điển tay):

```bash
python -m scripts.backfill_comment_sentiment --limit 100
```

---

## Phạm vi từ điển này

- **Có:** slang xem phim VN trên mạng, teencode phổ biến, sarcasm thường gặp.
- **Không phải:** model ML — chỉ hỗ trợ người gán / QA / chỉnh tay khi Kira sai.
- Bổ sung từ mới vào đúng nhóm khi gặp pattern lặp ≥ vài lần trên feed thật.
