from app.services.press_groups import select_groups, sync_enriched


def test_keeps_real_groups_and_deduplicates_shared_sources():
    data = {
        "organic_results": [{"link": "https://a.vn/a", "title": "A"}],
        "top_stories": [
            {
                "stories": [
                    {
                        "link": "https://a.vn/a#part",
                        "title": "A",
                        "source": {"name": "Báo A"},
                        "thumbnail": "https://cdn/a.jpg",
                    },
                    {"link": "https://b.vn/b", "title": "B"},
                ]
            }
        ],
    }
    articles = select_groups(data)
    assert len(articles) == 2
    assert len(data["organic_results"]) == 1
    assert len(data["top_stories"]) == 2
    assert data["organic_results"][0]["source_index"] == data["top_stories"][0]["source_index"] == 1
    assert data["organic_results"][0]["thumbnail"] == "https://cdn/a.jpg"
    assert data["top_stories"][1]["source_index"] == 2


def test_reserves_space_for_top_stories_and_caps_total_sources_at_ten():
    data = {
        "organic_results": [{"link": f"https://example.com/o/{i}"} for i in range(10)],
        "top_stories": [{"link": f"https://example.com/t/{i}"} for i in range(3)],
    }
    articles = select_groups(data)
    assert len(articles) == 10
    assert len(data["top_stories"]) == 3
    assert len(data["organic_results"]) == 7


def test_missing_group_is_empty_and_enrichment_syncs_after_json_storage():
    data = {"organic_results": [{"link": "https://a.vn/a"}]}
    select_groups(data)
    data["news_results"][0] = {**data["news_results"][0], "published_at": "2026-09-28"}
    sync_enriched(data)
    assert data["top_stories"] == []
    assert data["organic_results"][0]["published_at"] == "2026-09-28"
