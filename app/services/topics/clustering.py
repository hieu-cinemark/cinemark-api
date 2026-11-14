"""K-means trên vector bình luận (đã chuẩn hoá độ dài 1) cho bước gom cụm Social Topic - scripts/tools/cluster_film_comments.py.

14/11/2026: phần gán chủ đề cũ / gộp tâm / gọi Bee đặt tên của pipeline chủ đề trong API đã bỏ (pipeline hiện chạy trên Mac:
embed_comments -> cluster_film_comments -> name_clusters -> summarize_clusters -> publish_clusters)."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass
class Cluster:
    id: int
    indices: np.ndarray  # chỉ số dòng trong rows/x
    centroid: np.ndarray


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
