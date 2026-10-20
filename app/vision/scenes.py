"""Gom ảnh "chụp màn hình rạp" (post_visuals.label = 'screen') của một phim thành các cảnh - "cảnh được bàn nhiều" ở
Social Topic. Nhiều người cùng chụp một cảnh thì vector SigLIP của ảnh rất gần nhau.

Gom phân cấp liên kết trung bình (average linkage) trên khoảng cách cosine, dừng ở SCENE_DISTANCE; chỉ giữ cụm đủ đồng
nhất (độ giống trung bình >= MIN_COHESION - bỏ cụm chỉ giống nhau ở "màn hình tối có chữ đè lên") và có từ MIN_AUTHORS
người đăng khác nhau (một trang đăng lại cùng clip nhiều lần không tính là nhiều người quan tâm). Ngưỡng chọn ngày
2026-10-20 trên Trại Buôn Người / Án Mạng Xém Hoàn Hảo: 0,12 gộp được hai nửa của cảnh "Where is my phone?" (42 bài)
mà chưa trộn cảnh khác.

Chỉ cần numpy (VPS không có scikit-learn). Vài trăm tới khoảng 2.000 ảnh mỗi phim: ma trận khoảng cách n x n và n lần
gộp, mỗi lần một argmin - vài giây."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

SCENE_DISTANCE = 0.12
MIN_COHESION = 0.9
MIN_AUTHORS = 2
# Hai cụm có tâm giống nhau từ mức này là một cảnh chụp khác độ mờ / có chữ đè (Trại Buôn Người: cảnh điện thoại tách
# thành 3 cụm, tâm giống 0,90-0,92) - gộp vào cụm lớn hơn để khối "cảnh tiêu biểu" không hiện hai ảnh của cùng một cảnh.
# Phim toàn cảnh hội thoại trong phòng tối có thể bị gộp quá tay (ít cảnh hơn) - chấp nhận, trùng trông tệ hơn.
MERGE_SIMILARITY = 0.89
MAX_IMAGES = 2500


@dataclass
class SceneCluster:
    members: list[int]  # chỉ số trong mảng đầu vào
    representative: int  # ảnh gần tâm cụm nhất
    cohesion: float


def cluster_scenes(embeddings: np.ndarray, authors: list[str]) -> list[SceneCluster]:
    """embeddings: (n, d) đã chuẩn hoá L2. Trả về các cụm đạt chuẩn, nhiều người đăng nhất trước."""
    n = len(embeddings)
    if n < 2:
        return []
    if n > MAX_IMAGES:
        raise ValueError(f"quá nhiều ảnh để gom ({n} > {MAX_IMAGES})")
    sim = (embeddings @ embeddings.T).astype(np.float64)
    dist = 1.0 - sim
    np.fill_diagonal(dist, np.inf)
    sizes = np.ones(n)
    members: dict[int, list[int]] = {i: [i] for i in range(n)}

    while True:
        flat = int(np.argmin(dist))
        a, b = divmod(flat, n)
        if not np.isfinite(dist[a, b]) or dist[a, b] > SCENE_DISTANCE:
            break
        # Lance-Williams cho liên kết trung bình: d(a+b, k) = (|a| d(a,k) + |b| d(b,k)) / (|a| + |b|)
        merged = (sizes[a] * dist[a] + sizes[b] * dist[b]) / (sizes[a] + sizes[b])
        merged[a] = np.inf
        dist[a] = merged
        dist[:, a] = merged
        dist[b] = np.inf
        dist[:, b] = np.inf
        sizes[a] += sizes[b]
        members[a].extend(members.pop(b))

    out: list[SceneCluster] = []
    for idx in members.values():
        if len(idx) < 2 or len({authors[i].strip().lower() for i in idx}) < MIN_AUTHORS:
            continue
        sub = sim[np.ix_(idx, idx)]
        cohesion = float((sub.sum() - len(idx)) / (len(idx) * (len(idx) - 1)))
        if cohesion < MIN_COHESION:
            continue
        centre = embeddings[idx].mean(axis=0)
        out.append(
            SceneCluster(members=idx, representative=idx[int(np.argmax(embeddings[idx] @ centre))], cohesion=cohesion)
        )
    out.sort(key=lambda c: (-len({authors[i].strip().lower() for i in c.members}), -len(c.members)))

    merged: list[SceneCluster] = []
    centres: list[np.ndarray] = []
    for cluster in out:
        centre = embeddings[cluster.members].mean(axis=0)
        centre /= np.linalg.norm(centre)
        twin = next((k for k, other in enumerate(centres) if float(centre @ other) >= MERGE_SIMILARITY), None)
        if twin is None:
            merged.append(cluster)
            centres.append(centre)
        else:
            merged[twin].members.extend(cluster.members)
    # Gộp làm cụm lớn thêm - xếp lại theo số người đăng.
    merged.sort(key=lambda c: (-len({authors[i].strip().lower() for i in c.members}), -len(c.members)))
    return merged
