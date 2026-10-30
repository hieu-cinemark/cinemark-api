"""Gom chủ đề riêng từng phim (Social Topic bước 4). Ý chính: chủ đề đã có GIỮ NGUYÊN giữa các mốc đo -
comment mới gần tâm một chủ đề cũ thì gán vào đó (giữ tên, cập nhật tâm); phần còn lại mới gom cụm, cụm đủ lớn
thành chủ đề MỚI do Bee đặt tên. Tránh hẳn bài toán "ghép tên giữa hai lần gom lại từ đầu".

Hàm ở đây không ghi DB - scripts/jobs/topic_cluster.py lo đọc/ghi."""

from __future__ import annotations

import json
import random
from collections import Counter
from dataclasses import dataclass

import numpy as np

from app.ai.bee import call_bee
from app.ai.runtime import parse_json_response

# e5-small: hai câu bất kỳ đã có cosine ~0,80-0,87, nên ngưỡng gán phải cao. Giá trị khởi đầu - đo lại bằng calibrate().
ASSIGN_MIN = 0.90
ASSIGN_MARGIN = 0.005  # gần chủ đề nhất phải hơn chủ đề nhì ít nhất chừng này, không thì coi là chưa rõ
MIN_NEW_TOPIC = 40  # cụm phần dư nhỏ hơn thì chờ thêm comment, chưa thành chủ đề
SAMPLES_FOR_BEE = 25  # câu mẫu mỗi cụm gửi Bee (15 gần tâm + 10 ngẫu nhiên)
SAMPLES_TO_KEEP = 8  # câu tiêu biểu lưu vào movie_topics.samples cho UI


@dataclass
class Cluster:
    id: int
    indices: np.ndarray  # chỉ số dòng trong rows/x
    centroid: np.ndarray


def pick_k(n: int) -> int:
    """Số cụm cho k-means: ~150 câu một cụm, 6-30 cụm. Gom dư rồi để Bee gộp tốt hơn gom thiếu."""
    return int(np.clip(n // 150, 6, 30))


def kmeans(x: np.ndarray, k: int, iters: int = 30, seed: int = 13) -> list[Cluster]:
    rng = np.random.default_rng(seed)
    k = min(k, len(x))
    centers = x[rng.choice(len(x), k, replace=False)].copy()
    for _ in range(iters):
        assign = (x @ centers.T).argmax(1)
        for j in range(k):
            members = x[assign == j]
            if len(members):
                c = members.mean(0)
                centers[j] = c / np.linalg.norm(c)
    assign = (x @ centers.T).argmax(1)
    return [Cluster(j, np.where(assign == j)[0], centers[j]) for j in range(k) if (assign == j).any()]


def assign_existing(x: np.ndarray, centroids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """-> (chỉ số chủ đề hoặc -1, cosine với chủ đề gần nhất) cho từng dòng của x."""
    if len(centroids) == 0 or len(x) == 0:
        return np.full(len(x), -1), np.zeros(len(x))
    sims = x @ centroids.T
    order = np.argsort(-sims, axis=1)
    best = sims[np.arange(len(x)), order[:, 0]]
    second = sims[np.arange(len(x)), order[:, 1]] if centroids.shape[0] > 1 else np.zeros(len(x))
    ok = (best >= ASSIGN_MIN) & (best - second >= ASSIGN_MARGIN)
    return np.where(ok, order[:, 0], -1), best


def merged_centroid(old: np.ndarray, old_count: int, new_vectors: np.ndarray) -> np.ndarray:
    """Tâm mới = trung bình có trọng số (tâm cũ đại diện old_count câu), chuẩn hoá lại."""
    total = old * old_count + new_vectors.sum(0)
    return total / np.linalg.norm(total)


def representative(rows: list[dict], x: np.ndarray, idx: np.ndarray, centroid: np.ndarray, n: int) -> list[str]:
    """n câu gần tâm nhất - câu tiêu biểu cho UI."""
    closest = idx[np.argsort(-(x[idx] @ centroid))][:n]
    return [rows[i]["message"][:300] for i in closest]


def cluster_payload(rows: list[dict], x: np.ndarray, clusters: list[Cluster]) -> list[dict]:
    rng = random.Random(7)
    total = sum(len(c.indices) for c in clusters) or 1
    out = []
    for c in clusters:
        closest = list(c.indices[np.argsort(-(x[c.indices] @ c.centroid))][:15])
        rest = [i for i in c.indices if i not in set(closest)]
        picked = closest + rng.sample(rest, min(SAMPLES_FOR_BEE - len(closest), len(rest)))
        sentiments = Counter(rows[i].get("sentiment") for i in c.indices)
        out.append({
            "id": c.id,
            "comments": len(c.indices),
            "share_percent": round(len(c.indices) * 100 / total, 1),
            "positive": sentiments.get("positive", 0),
            "negative": sentiments.get("negative", 0),
            "samples": [rows[i]["message"][:200] for i in picked],
        })
    return out


NAME_SYSTEM = """Bạn là chuyên viên social listening cho phim chiếu rạp Việt Nam. Bạn nhận các CỤM bình luận MỚI đã được máy
gom theo độ giống nhau về nghĩa (mỗi cụm có số liệu thật và câu mẫu), cùng danh sách CHỦ ĐỀ ĐÃ CÓ của phim.
Nhiệm vụ:
1. Đánh dấu từng cụm: "topic" (khán giả bàn một nội dung về phim), "junk" (tán gẫu, tag bạn, không rõ ý) hoặc "offtopic"
   (không nói về phim này).
2. Cụm "topic" trùng ý một chủ đề đã có -> ghi id chủ đề đó vào "same_as" (không tạo chủ đề mới).
3. Các cụm "topic" còn lại: gộp cụm cùng ý thành CHỦ ĐỀ MỚI. Tên chủ đề ngắn (tối đa 8 chữ), CỤ THỂ cho phim này - nêu
   nhân vật/diễn viên/cảnh/chi tiết nếu câu mẫu có ("Diễn xuất của Ngọc Trinh", "Cảnh đám tang cuối phim"), tránh tên
   chung chung ("Đánh giá phim", "Diễn xuất"). summary: 1-2 câu mô tả khán giả nói gì, không khuyên nhủ.
Chỉ dựa vào câu mẫu, không bịa tên người. Trả về JSON đúng dạng, không thêm chữ nào khác:
{"clusters": [{"id": 0, "kind": "topic|junk|offtopic", "same_as": "topic_xxx hoặc null"}],
 "new_topics": [{"name": "...", "summary": "...", "cluster_ids": [0, 3]}]}"""


async def name_clusters(movie_title: str, payload: list[dict], existing: list[dict]) -> dict:
    """existing: [{"id", "name", "summary"}] của chủ đề đã có. Raise nếu Bee lỗi / JSON hỏng - bên gọi bỏ qua lượt này."""
    prompt = (
        f"Phim: {movie_title}\n"
        f"Chủ đề đã có (JSON): {json.dumps(existing, ensure_ascii=False)}\n"
        f"Các cụm mới (JSON): {json.dumps(payload, ensure_ascii=False)}"
    )
    raw = await call_bee(system_prompt=NAME_SYSTEM, user_prompt=prompt, max_tokens=12000, task="topic_naming")
    result = parse_json_response(raw)
    if not isinstance(result, dict) or "clusters" not in result:
        raise ValueError("topic_naming_bad_json")
    return result


def calibrate(x: np.ndarray, clusters: list[Cluster]) -> dict[str, float]:
    """Phân bố cosine với tâm cụm của chính câu đó và với tâm cụm gần nhì - dùng để chọn ASSIGN_MIN / ASSIGN_MARGIN."""
    centers = np.stack([c.centroid for c in clusters])
    sims = np.sort(x @ centers.T, axis=1)
    own, second = sims[:, -1], sims[:, -2]
    pct = lambda a, p: round(float(np.percentile(a, p)), 3)  # noqa: E731
    return {
        "own_p10": pct(own, 10), "own_p25": pct(own, 25), "own_p50": pct(own, 50),
        "second_p50": pct(second, 50), "second_p90": pct(second, 90),
        "gap_p25": pct(own - second, 25), "gap_p50": pct(own - second, 50),
    }
