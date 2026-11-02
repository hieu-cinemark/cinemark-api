"""Hai nhóm Google Search, cùng bộ nguồn tối đa 10 bài để giữ số dẫn nguồn nhất quán."""


def flatten(value, depth=0) -> list[dict]:
    if depth > 4:
        return []
    if isinstance(value, list):
        return [item for child in value for item in flatten(child, depth + 1)]
    if not isinstance(value, dict):
        return []
    if value.get("link"):
        item = dict(value)
        source = item.get("source")
        if isinstance(source, dict):
            item["source"] = source.get("name") or ""
        return [item]
    return flatten(value.get("stories") or value.get("items") or [], depth + 1)


def identity(item: dict) -> str:
    return str(item.get("link") or "").strip().split("#", 1)[0]


def select_groups(data: dict, extra: dict | None = None) -> list[dict]:
    extra = extra or {}
    organic = flatten(data.get("organic_results", [])) + flatten(extra.get("organic_results", []))
    stories = flatten(data.get("top_stories", [])) + flatten(extra.get("top_stories", []))
    # Giữ chỗ cho tối đa 3 tin nổi bật; phần còn lại lấy từ organic, tổng nguồn không quá 10.
    selected = {}
    for item in stories[:3] + organic + stories:
        key = identity(item)
        if key and key not in selected and len(selected) < 10:
            selected[key] = item
    canonical = {}
    for item in organic + stories:
        key = identity(item)
        if key in selected and key not in canonical:
            canonical[key] = dict({**selected[key], **item}, source_index=len(canonical) + 1)
    for name, items in [("organic_results", organic), ("top_stories", stories)]:
        keys = list(dict.fromkeys(identity(item) for item in items))
        data[name] = [canonical[key] for key in keys if key in canonical]
    data["news_results"] = list(canonical.values())  # bộ nguồn chung cho trích xuất và AI
    data["groups_version"] = 1
    return data["news_results"]


def sync_enriched(data: dict) -> None:
    canonical = {identity(item): item for item in data.get("news_results", [])}
    for name in ("organic_results", "top_stories"):
        data[name] = [canonical[identity(item)] for item in data.get(name, []) if identity(item) in canonical]
