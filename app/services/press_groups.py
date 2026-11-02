"""Giữ đầy đủ một trang mỗi nguồn; chỉ giới hạn bộ nguồn đưa vào AI."""


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
    preferred = {identity(item): item for item in organic + press}
    canonical = {}
    for item in organic + press:
        key = identity(item)
        if key and key not in canonical:
            canonical[key] = dict(preferred[key], source_index=len(canonical) + 1)
    analysis = {}
    for item in press[:5] + organic[:5] + organic + press:
        key = identity(item)
        if key in canonical and key not in analysis and len(analysis) < 10:
            analysis[key] = canonical[key]
    for name, items in [("organic_results", organic), ("news_results", press)]:
        keys = [identity(item) for item in items]
        data[name] = [canonical[key] for key in keys if key in canonical]
    data.pop("top_stories", None)
    data["sources"] = list(canonical.values())
    data["analysis_sources"] = sorted(analysis.values(), key=lambda item: item["source_index"])
    data["news_search_metadata"] = news_data.get("search_metadata", {})
    data["news_search_parameters"] = news_data.get("search_parameters", {})
    data["groups_version"] = 3
    return data["sources"]


def sync_enriched(data: dict) -> None:
    canonical = {identity(item): item for item in data.get("sources", [])}
    for item in data.get("analysis_sources", []):
        if identity(item) in canonical:
            canonical[identity(item)].update(item)
    for name in ("organic_results", "news_results", "analysis_sources"):
        data[name] = [canonical[identity(item)] for item in data.get(name, []) if identity(item) in canonical]
