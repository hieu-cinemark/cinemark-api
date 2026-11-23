"""Dấu hiệu C: nhóm tài khoản có hành vi giống nhau (docs mục 2.4 C)."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from app.services.attack.signals import Signal, mask_name
from app.services.attack.similar import vn_hour

MANY_POSTS, MANY_POSTS_WINDOW = 5, timedelta(hours=24)
PAIR_POSTS, PAIR_GAP, PAIR_JACCARD = 3, timedelta(minutes=10), 0.5
MULTI_CLUSTER = 2
STRONG_ACCOUNTS = 5
MAX_PER_POST = 400  # bài viral nghìn bình luận: chỉ xét chừng này bình luận gần nhất để số cặp không bùng nổ


def _at(row: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(row["posted_at"])


def _many_posts(posts: dict[str, list[datetime]]) -> bool:
    """L1: có cửa sổ 24 giờ chứa >= MANY_POSTS bài khác nhau (lấy lần bình luận đầu tiên ở mỗi bài)."""
    firsts = sorted(min(times) for times in posts.values())
    j = 0
    for i, t in enumerate(firsts):
        while t - firsts[j] > MANY_POSTS_WINDOW:
            j += 1
        if i - j + 1 >= MANY_POSTS:
            return True
    return False


def account_groups(rows: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """rows: bình luận chê như data.load_negative_comments. clusters: kết quả similar_clusters (có "member_ids")."""
    rows = [r for r in rows if r.get("author_id") and r.get("posted_at")]
    by_acc: dict[str, dict[str, list[datetime]]] = defaultdict(lambda: defaultdict(list))
    by_post: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    for r in rows:
        by_acc[r["author_id"]][r["post_id"]].append(_at(r))
        by_post[r["post_id"]].append((_at(r), r["author_id"]))

    rules: dict[str, set[str]] = defaultdict(set)  # tài khoản -> luật đã thoả
    for acc, posts in by_acc.items():
        if _many_posts(posts):
            rules[acc].add("many_posts")

    # L2: cặp tài khoản bình luận cách nhau <= PAIR_GAP dưới cùng một bài, lặp lại ở nhiều bài.
    shared: dict[tuple[str, str], set[str]] = defaultdict(set)
    for post_id, items in by_post.items():
        items = sorted(items)[-MAX_PER_POST:]
        j = 0
        for i, (t, a) in enumerate(items):
            while t - items[j][0] > PAIR_GAP:
                j += 1
            for _, b in items[j:i]:
                if a != b:
                    shared[tuple(sorted((a, b)))].add(post_id)
    pairs: list[tuple[str, str]] = []
    for (a, b), posts in shared.items():
        union = set(by_acc[a]) | set(by_acc[b])
        if len(posts) >= PAIR_POSTS and len(posts) / len(union) >= PAIR_JACCARD:
            pairs.append((a, b))
            rules[a].add("pair")
            rules[b].add("pair")

    # L3: có mặt trong nhiều cụm của dấu hiệu B.
    author_of = {r["id"]: r["author_id"] for r in rows}
    clusters_of: dict[str, set[int]] = defaultdict(set)
    for index, cluster in enumerate(clusters):
        for comment_id in cluster.get("member_ids", []):
            if comment_id in author_of:
                clusters_of[author_of[comment_id]].add(index)
    for acc, seen in clusters_of.items():
        if len(seen) >= MULTI_CLUSTER:
            rules[acc].add("multi_cluster")

    # Ghép nhóm bằng union-find: nối hai tài khoản khi là một cặp L2, hoặc cùng một cụm B và cả hai thoả L3.
    parent = {acc: acc for acc in by_acc}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in pairs:
        parent[find(a)] = find(b)
    for index in range(len(clusters)):
        members = [a for a, seen in clusters_of.items() if index in seen and "multi_cluster" in rules[a]]
        for other in members[1:]:
            parent[find(other)] = find(members[0])

    groups: dict[str, list[str]] = defaultdict(list)
    for acc in by_acc:
        groups[find(acc)].append(acc)

    out = []
    for members in groups.values():
        qualifying = [a for a in members if len(rules[a]) >= 2]
        if len(qualifying) < 2:
            continue
        member_set = set(qualifying)
        items = [r for r in rows if r["author_id"] in member_set]
        times = sorted(_at(r) for r in items)
        out.append(
            {
                "accounts": len(members),
                "qualifying": len(qualifying),
                "posts": len({r["post_id"] for r in items}),
                "comments": len(items),
                "platforms": sorted({r.get("platform") or "" for r in items} - {""}),
                "window": (vn_hour(times[0].isoformat()), vn_hour((times[-1] + timedelta(hours=1)).isoformat())),
                "rules": {
                    name: sum(name in rules[a] for a in qualifying) for name in ("many_posts", "pair", "multi_cluster")
                },
                "members": [
                    {
                        "author": mask_name(next(r.get("author_name") for r in items if r["author_id"] == a)),
                        "posts": len({r["post_id"] for r in items if r["author_id"] == a}),
                        "rules": sorted(rules[a]),
                    }
                    for a in qualifying
                ][:20],
                "evidence": [
                    {
                        "comment_id": r["id"],
                        "author": mask_name(r.get("author_name")),
                        "platform": r.get("platform"),
                        "post_url": r.get("post_url"),
                        "posted_at": r.get("posted_at"),
                        "text": (r.get("message") or "")[:300],
                        "likes": r.get("likes") or 0,
                    }
                    for r in sorted(items, key=lambda r: r["posted_at"])[:10]
                ],
            }
        )
    return sorted(out, key=lambda g: -g["qualifying"])


def account_signal(rows: list[dict[str, Any]], clusters: list[dict[str, Any]]) -> Signal:
    groups = account_groups(rows, clusters)
    if not groups:
        return Signal(key="accounts", fired=False, strong=False, score=0.0)
    top = groups[0]
    return Signal(
        key="accounts",
        fired=True,
        strong=top["qualifying"] >= STRONG_ACCOUNTS,
        score=float(top["qualifying"]),
        window=top["window"],
        evidence=top["evidence"],
        detail={"groups": [{k: v for k, v in g.items() if k != "evidence"} for g in groups[:5]]},
    )
