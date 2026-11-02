"""Google Search và News Light dùng chung tối đa 10 nguồn, giữ số dẫn nguồn nhất quán."""


def flatten(value, depth=0) -> list[dict]:
    if depth > 4:
        return []
    if isinstance(value, list):
        return [item for child in value for item in flatten(child, depth + 1)]
    if not isinstance(value, dict):
        return []
    if value.get("link"):
        item = dict(value)
        if isinstance(item.get("source"), dict):
            item["source"] = item["source"].get("name") or ""
        return [item]
    return flatten(value.get("stories") or value.get("items") or [], depth + 1)


def identity(item: dict) -> str:
    return str(item.get("link") or "").strip().split("#", 1)[0]


def select_groups(data: dict, news_data: dict) -> list[dict]:
    organic = flatten(data.get("organic_results", []))
    press = flatten(news_data.get("news_results", []))
    # Giữ chỗ cân bằng cho hai nguồn, bỏ trùng URL trước khi trích xuất/phân tích.
    selected = {}
    for item in press[:5] + organic[:5] + organic + press:
        key = identity(item)
        if key and key not in selected and len(selected) < 10:
            selected[key] = item
    canonical = {}
    for item in organic + press:
        key = identity(item)
        if key in selected and key not in canonical:
            canonical[key] = dict({**item, **selected[key]}, source_index=len(canonical) + 1)
    for name, items in [("organic_results", organic), ("news_results", press)]:
        keys = list(dict.fromkeys(identity(item) for item in items))
        data[name] = [canonical[key] for key in keys if key in canonical]
    data.pop("top_stories", None)
    data["sources"] = list(canonical.values())
    data["news_search_metadata"] = news_data.get("search_metadata", {})
    data["news_search_parameters"] = news_data.get("search_parameters", {})
    data["groups_version"] = 2
    return data["sources"]


def sync_enriched(data: dict) -> None:
    canonical = {identity(item): item for item in data.get("sources", [])}
    for name in ("organic_results", "news_results"):
        data[name] = [canonical[identity(item)] for item in data.get(name, []) if identity(item) in canonical]
