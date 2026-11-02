from app.services.press_groups import select_groups, sync_enriched


def test_keeps_search_and_news_separate_with_shared_references():
    data = {
        "organic_results": [{"link": "https://a.vn/a", "title": "A"}],
        "top_stories": [{"link": "https://not-news-light.vn/never-use"}],
    }
    press = {
        "news_results": [
            {
                "link": "https://a.vn/a#part",
                "title": "A",
                "source": {"name": "Báo A"},
                "thumbnail": "https://cdn/a.jpg",
            },
            {"link": "https://b.vn/b", "title": "B"},
        ],
        "search_parameters": {"engine": "google_news_light"},
    }
    articles = select_groups(data, press)
    assert len(articles) == 2
    assert len(data["organic_results"]) == 1
    assert len(data["news_results"]) == 2
    assert data["organic_results"][0]["source_index"] == data["news_results"][0]["source_index"] == 1
    assert data["organic_results"][0]["thumbnail"] == "https://cdn/a.jpg"
    assert "top_stories" not in data
    assert data["news_search_parameters"]["engine"] == "google_news_light"


def test_displays_full_pages_but_balances_only_ai_sources():
    data = {"organic_results": [{"link": f"https://example.com/o/{i}"} for i in range(10)]}
    press = {"news_results": [{"link": f"https://example.com/n/{i}"} for i in range(10)]}
    articles = select_groups(data, press)
    assert len(articles) == 20
    assert len(data["news_results"]) == 10
    assert len(data["organic_results"]) == 10
    assert len(data["analysis_sources"]) == 10
    assert [item["source_index"] for item in data["analysis_sources"]] == [1, 2, 3, 4, 5, 11, 12, 13, 14, 15]


def test_empty_news_is_not_filled_from_search_and_metadata_is_synced():
    data = {"organic_results": [{"link": "https://a.vn/a"}]}
    select_groups(data, {"news_results": []})
    data["sources"][0] = {**data["sources"][0], "published_at": "2026-09-28"}
    sync_enriched(data)
    assert data["news_results"] == []
    assert data["organic_results"][0]["published_at"] == "2026-09-28"
