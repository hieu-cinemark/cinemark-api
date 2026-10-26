"""Thử gom cụm chủ đề comment của một phim bằng embedding + k-means, để so với 10 chủ đề AI đang tạo.

python -m scripts.topic_cluster_probe
python -m scripts.topic_cluster_probe --k 15,20,25 --sample 40000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
from collections import Counter
from pathlib import Path

import numpy as np
import torch

from app.clients.d1 import d1_query
from app.repositories.d1.posts import RELEVANT_POST_SQL
from scripts.aspect_probe import MODELS, _encode, _load

MOVIE_ID = "movie_dde1ac0a-9a42-43e1-8671-70fbcb00a296"  # Trại Buôn Người
DATA = Path.home() / "cinemark-ml" / "topics"


# --- 1. Tải comment (cache ra file, chỉ tải một lần) ---
async def fetch(movie_id: str) -> list[dict]:
    rows, last = [], ""
    while True:
        page = await d1_query(
            f"""SELECT c.id, c.message, c.sentiment, c.author_id, c.posted_at, c.platform
            FROM comments c JOIN posts p ON p.id = c.post_id
            WHERE p.movie_id = ?1 AND {RELEVANT_POST_SQL} AND c.id > ?2 AND length(c.message) >= 15
            ORDER BY c.id LIMIT 2000""",
            [movie_id, last],
            timeout=120.0,
        )
        if not page:
            return rows
        rows += page
        last = page[-1]["id"]
        print(f"  {len(rows)} comment", end="\r", flush=True)


def load_rows(movie_id: str) -> list[dict]:
    path = DATA / f"{movie_id}.jsonl"
    if path.exists():
        return [json.loads(line) for line in path.open()]
    rows = asyncio.run(fetch(movie_id))  # chỉ một lần asyncio.run cho mọi lần đọc D1
    DATA.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return rows


# --- 2. Lọc ---
URL_RE = re.compile(r"https?://\S+|www\.\S+")
MENTION_RE = re.compile(r"@\S+")
TAG_RE = re.compile(r"\[[^\]]{1,40}\]")  # [Sticker], [Photo], [Cười chảy nước mắt], [尴尬]...
# emoji, ký hiệu, chữ kiểu cách unicode (𝕹𝖌𝖔...), ký tự nối emoji
EMOJI_RE = re.compile(
    "[\U0001f000-\U0001faff\U0001d400-\U0001d7ff\u2190-\u21ff\u2300-\u27bf\u2b00-\u2bff\ufe0f\u200d\u2764♡♥]+"
)
LAUGH_RE = re.compile(
    r"(?<!\w)(?:k{2,}|ha(?:ha)+|hi(?:hi)+|he(?:he)+|hu(?:hu)+|kk+)(?!\w)|[=:;]+[)(dDpP]+|\)\)+|\(\(+", re.IGNORECASE
)
MIN_CHARS = 30


def normalize(text: str) -> str:
    """Bản dùng để tính embedding: bỏ những thứ khiến máy gom theo HÌNH THỨC (link, tag, sticker, emoji, từ cười)."""
    for pattern in (URL_RE, MENTION_RE, TAG_RE, EMOJI_RE, LAUGH_RE):
        text = pattern.sub(" ", text)
    text = re.sub(r"([!?.,~])\1+", r"\1", text)  # "!!!!" -> "!"
    return " ".join(text.split())


def clean(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    stats: Counter[str] = Counter()
    short_examples: list[str] = []
    out = []
    for r in rows:
        text = " ".join((r["message"] or "").split())
        norm = normalize(text)
        key = norm.casefold()
        if key in seen:  # trùng nội dung (sau khi bỏ emoji/tag)
            stats["trùng"] += 1
            continue
        seen.add(key)
        if len(norm) < MIN_CHARS:
            stats[f"ngắn hơn {MIN_CHARS} ký tự sau làm sạch"] += 1
            short_examples.append(text)
            continue
        out.append({**r, "message": text, "embed_text": norm})
    print(f"{len(rows)} comment -> giữ {len(out)} ({len(out) * 100 / max(len(rows), 1):.0f}%); bỏ: {dict(stats)}")
    print("ví dụ câu bị bỏ vì quá ngắn:")
    for t in random.Random(1).sample(short_examples, min(15, len(short_examples))):
        print("  -", t[:100])
    print("ví dụ câu giữ lại (gốc -> bản tính embedding):")
    for r in random.Random(2).sample(out, min(8, len(out))):
        print(f"  - {r['message'][:90]}\n    -> {r['embed_text'][:90]}")
    return out


# --- 3. Embedding (cache ra file) ---
def embed(rows: list[dict], tag: str) -> np.ndarray:
    path = DATA / f"{MOVIE_ID}-{tag}-e5.npy"
    if path.exists():
        return np.load(path).astype(np.float32)
    device = "mps" if torch.backends.mps.is_available() else "cpu"
    tokenizer, model = _load("e5-small", device)
    _, prefix, max_len = MODELS["e5-small"]
    vectors = _encode(tokenizer, model, [r["embed_text"] for r in rows], prefix, max_len, device)
    np.save(path, vectors.astype(np.float16))
    return vectors


# --- 4. K-means theo cosine ---
def kmeans(x: np.ndarray, k: int, iters: int = 30, seed: int = 13) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    centers = x[rng.choice(len(x), k, replace=False)].copy()
    for _ in range(iters):
        assign = (x @ centers.T).argmax(1)
        for j in range(k):
            members = x[assign == j]
            if len(members):
                c = members.mean(0)
                centers[j] = c / np.linalg.norm(c)
    return centers, (x @ centers.T).argmax(1)


# --- 5. In cụm ra markdown ---
def report(rows: list[dict], x: np.ndarray, centers: np.ndarray, assign: np.ndarray, k: int, tag: str) -> Path:
    total = len(rows)
    lines = [
        f"# Cụm chủ đề {tag} k={k} - {total} comment (đã làm sạch link/tag/sticker/emoji/từ cười, >= {MIN_CHARS} ký tự)",
        "",
    ]
    for j in sorted(range(k), key=lambda j: -(assign == j).sum()):
        idx = np.where(assign == j)[0]
        sentiments = Counter(rows[i]["sentiment"] for i in idx)
        authors = len({rows[i]["author_id"] for i in idx})
        closest = idx[np.argsort(-(x[idx] @ centers[j]))][:10]
        lines.append(
            f"## Cụm {j}: {len(idx)} comment ({len(idx) * 100 / total:.1f}%) · {authors} người đăng · "
            f"khen {sentiments.get('positive', 0)} / chê {sentiments.get('negative', 0)} / trung lập {sentiments.get('neutral', 0)}"
        )
        lines.append("Chấm: [ ] Rõ  [ ] Lẫn  [ ] Rác   Tên chủ đề: ............")
        lines += [f"- {rows[i]['message'][:200]}" for i in closest]
        lines.append("")
    path = DATA / f"{MOVIE_ID}-{tag}-k{k}.md"
    path.write_text("\n".join(lines))
    return path


# --- 6. Bee đặt tên + gộp cụm (thử hướng kết hợp máy gom cụm, AI đặt tên) ---
NAME_SYSTEM = """Bạn là chuyên viên social listening cho phim chiếu rạp Việt Nam. Bạn nhận các CỤM bình luận đã được máy gom
theo độ giống nhau về nghĩa. Mỗi cụm có số liệu thật và vài câu mẫu. Nhiệm vụ:
1. Đặt tên ngắn (tiếng Việt) cho từng cụm theo NỘI DUNG khán giả bàn, đánh dấu cụm rác (tán gẫu, không rõ ý) hoặc
   ngoài lề (không nói về phim).
2. Gộp các cụm cùng một chủ đề (ví dụ nhiều cụm cùng bàn một vai diễn) thành CHỦ ĐỀ; chỉ dùng cụm về phim, không rác.
   Mỗi cụm thuộc tối đa một chủ đề.
3. Ghi lại các chủ đề HẸP có tên riêng (nhân vật, diễn viên, bài hát, câu thoại, cảnh cụ thể) bạn thấy lẫn trong câu mẫu.
Chỉ dựa vào câu mẫu, không bịa. Trả về JSON đúng dạng, không thêm chữ nào khác:
{"clusters": [{"id": 0, "label": "...", "kind": "topic|junk|offtopic"}],
 "topics": [{"name": "...", "cluster_ids": [0, 3], "sentiment": "positive|negative|mixed|neutral", "insight": "1-2 câu cho đội truyền thông"}],
 "narrow_topics": [{"name": "...", "cluster_ids": [2], "example": "một câu mẫu trích nguyên văn"}]}"""


def cluster_payload(rows: list[dict], x: np.ndarray, centers: np.ndarray, assign: np.ndarray) -> list[dict]:
    rng = random.Random(7)
    out = []
    for j in range(len(centers)):
        idx = np.where(assign == j)[0]
        closest = list(idx[np.argsort(-(x[idx] @ centers[j]))][:15])
        rest = [i for i in idx if i not in set(closest)]
        picked = closest + rng.sample(rest, min(10, len(rest)))
        sentiments = Counter(rows[i]["sentiment"] for i in idx)
        out.append(
            {
                "id": j,
                "comments": len(idx),
                "share_percent": round(len(idx) * 100 / len(rows), 1),
                "authors": len({rows[i]["author_id"] for i in idx}),
                "positive": sentiments.get("positive", 0),
                "negative": sentiments.get("negative", 0),
                "samples": [rows[i]["message"][:200] for i in picked],
            }
        )
    return out


def name_clusters(rows: list[dict], x: np.ndarray, k: int, tag: str) -> Path:
    from app.ai.bee import call_bee
    from app.ai.runtime import parse_json_response

    centers, assign = kmeans(x, k)
    payload = cluster_payload(rows, x, centers, assign)
    prompt = "Phim: Trại Buôn Người\nCác cụm (JSON):\n" + json.dumps(payload, ensure_ascii=False)
    print(f"gửi {k} cụm, {sum(len(c['samples']) for c in payload)} câu mẫu cho Bee ({len(prompt)} ký tự)...")
    raw = asyncio.run(
        call_bee(system_prompt=NAME_SYSTEM, user_prompt=prompt, max_tokens=20000, temperature=0.3, task="topic_naming")
    )
    result = parse_json_response(raw)
    assert isinstance(result, dict), "Bee phải trả về một object JSON"
    (DATA / f"{MOVIE_ID}-{tag}-k{k}-named.json").write_text(json.dumps(result, ensure_ascii=False, indent=1))

    size = {c["id"]: c for c in payload}
    lines = [f"# Chủ đề sau khi Bee đặt tên + gộp ({tag}, k={k}) - Trại Buôn Người", ""]
    topics = sorted(
        result.get("topics") or [], key=lambda t: -sum(size[i]["comments"] for i in t["cluster_ids"] if i in size)
    )
    lines.append("## Chủ đề (số liệu = tổng các cụm đã gộp, trên mẫu 40.000 comment đã làm sạch)")
    for n, t in enumerate(topics, 1):
        ids = [i for i in t["cluster_ids"] if i in size]
        total = sum(size[i]["comments"] for i in ids)
        pos = sum(size[i]["positive"] for i in ids)
        neg = sum(size[i]["negative"] for i in ids)
        lines.append(
            f"{n}. **{t['name']}** - {total} comment ({total * 100 / len(rows):.1f}%) · khen {pos} / chê {neg} · "
            f"cụm {ids} · {t.get('sentiment')}\n   {t.get('insight', '')}"
        )
    lines += ["", "## Chủ đề hẹp có tên riêng Bee thấy lẫn trong câu mẫu"]
    for t in result.get("narrow_topics") or []:
        lines.append(f'- **{t["name"]}** (cụm {t.get("cluster_ids")}): "{t.get("example", "")}"')
    lines += ["", "## Từng cụm"]
    for c in sorted(result.get("clusters") or [], key=lambda c: -size.get(c["id"], {"comments": 0})["comments"]):
        lines.append(f"- Cụm {c['id']} ({size.get(c['id'], {}).get('comments')} comment): {c['label']} [{c['kind']}]")
    ai = DATA / f"{MOVIE_ID}-ai-topics.md"
    if ai.exists():
        lines += ["", "## 10 chủ đề AI hiện tại (report 20/10, mẫu 2.000 comment)"]
        lines += [line[3:] for line in ai.read_text().splitlines() if line.startswith("## ")]
    path = DATA / f"{MOVIE_ID}-{tag}-k{k}-named.md"
    path.write_text("\n".join(lines))
    return path


# --- 7. Đếm chủ đề hẹp có tên riêng bằng từ khoá (bù phần embedding bỏ sót) ---
# Thử cho Trại Buôn Người: tên Bee tìm thấy trong câu mẫu + cột cast của movies + chủ đề hẹp của report AI cũ. Tên ngắn dễ
# trùng nghĩa ("tài" = tài năng) phải kèm từ xưng hô. Khớp cả không dấu cho tên đặc trưng.
ENTITIES: dict[str, list[str]] = {
    "Mony (Kore Hải Anh)": ["mony", "kore hải anh", "hải anh"],
    "Tài (vai Quách Ngọc Ngoan)": ["anh tài", "a tài", "ông tài", "chú tài", "thằng tài", "tk tài", "ổng tài"],
    "Quách Ngọc Ngoan": ["quách ngọc ngoan", "ngọc ngoan", "quach ngoc ngoan", "anh ngoan", "a ngoan"],
    "Mao Đại": ["mao đại", "mao dai"],
    "Đinh Viết Tường": ["đinh viết tường", "viết tường", "anh tường", "a tường", "bạn tường"],
    "Quách Ngọc Tuyên": ["quách ngọc tuyên", "ngọc tuyên", "anh tuyên", "a tuyên"],
    "Khánh My": ["khánh my", "khanh my"],
    "Steven Nguyễn": ["steven"],
    "Út Đẹt": ["út đẹt", "ut det"],
    "Hai Ny / anh Ny": ["hai ny", "anh ny", "a ny"],
    "Hắc Hưng / Long Ca": ["hắc hưng", "hac hung", "long ca"],
    "Nhạc phim / OST": ["ost", "nhạc phim", "ánh sao đêm", "bài hát", "giọng hát", "ca khúc", "hát hay"],
    "Câu thoại 'Mày may lại cho tao'": ["may lại cho tao", "may lại cho t", "mày may lại", "may lai cho tao"],
    "Cảnh vợ chồng mang bầu": ["mang bầu", "có bầu", "có thai", "bụng bầu"],
    "Công an / cảnh sát chìm": ["công an chìm", "cảnh sát chìm", "ca chìm"],
    "So với 'Thiên đường máu'": ["thiên đường máu", "thien duong mau"],
    "Việc nhẹ lương cao / lừa qua Cam": ["việc nhẹ lương cao", "viec nhe luong cao", "qua cam", "campuchia", "bên cam"],
    "Sự kiện thật Tịnh Biên": ["tịnh biên", "tinh bien"],
    "Bệnh tic": ["tic", "tourette"],
    "Seeding / PR": ["seeding", "seed", "pr lố", "pr quá"],
    "Spoil": ["spoil", "spoiler"],
    "Sạn phim": ["sạn", "lỗi phim", "vô lý"],
    "Đạo diễn": ["đạo diễn", "toni"],
}


def count_entities(raw_rows: list[dict]) -> Path:
    seen: set[str] = set()
    rows = []
    for r in raw_rows:
        text = " ".join((r["message"] or "").split())
        key = normalize(text).casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append({**r, "message": text, "low": text.casefold()})
    patterns = {
        name: re.compile("|".join(rf"(?<!\w){re.escape(a)}(?!\w)" for a in aliases))
        for name, aliases in ENTITIES.items()
    }
    lines = [f"# Chủ đề hẹp đếm bằng từ khoá - Trại Buôn Người ({len(rows)} comment không trùng)", ""]
    lines.append("| Chủ đề hẹp | Comment | % | Người đăng | Khen | Chê |")
    lines.append("|---|---|---|---|---|---|")
    examples: dict[str, list[str]] = {}
    results = []
    for name, pattern in patterns.items():
        hits = [r for r in rows if pattern.search(r["low"])]
        sentiments = Counter(r["sentiment"] for r in hits)
        results.append((len(hits), name, len({r["author_id"] for r in hits}), sentiments))
        examples[name] = [r["message"][:160] for r in random.Random(5).sample(hits, min(5, len(hits)))]
    for n, name, authors, sentiments in sorted(results, reverse=True):
        lines.append(
            f"| {name} | {n} | {n * 100 / len(rows):.2f}% | {authors} | {sentiments.get('positive', 0)} | {sentiments.get('negative', 0)} |"
        )
    lines += ["", "## Câu mẫu (kiểm tra từ khoá có bắt nhầm không)"]
    for _, name, _, _ in sorted(results, reverse=True):
        lines.append(f"### {name}")
        lines += [f"- {t}" for t in examples[name]]
    path = DATA / f"{MOVIE_ID}-entities.md"
    path.write_text("\n".join(lines))
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--k", default="15,20,25")
    parser.add_argument("--sample", type=int, default=40000)
    parser.add_argument("--tag", default="v2", help="hậu tố tên file kết quả và cache vector")
    parser.add_argument("--name-k", type=int, default=0, help="chỉ gom k cụm rồi nhờ Bee đặt tên + gộp (0 = tắt)")
    parser.add_argument("--entities", action="store_true", help="chỉ đếm chủ đề hẹp theo từ khoá trên toàn bộ comment")
    args = parser.parse_args()

    if args.entities:
        print("đã ghi", count_entities(load_rows(MOVIE_ID)))
        return
    rows = clean(load_rows(MOVIE_ID))
    print(f"\n{len(rows)} comment sau khi lọc")
    random.Random(13).shuffle(rows)
    rows = rows[: args.sample]
    x = embed(rows, f"{args.tag}-s{args.sample}")
    if args.name_k:
        print("đã ghi", name_clusters(rows, x, args.name_k, args.tag))
        return
    for k in (int(v) for v in args.k.split(",")):
        centers, assign = kmeans(x, k)
        print("đã ghi", report(rows, x, centers, assign, k, args.tag))


if __name__ == "__main__":
    main()
