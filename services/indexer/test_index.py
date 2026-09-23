from index import InvertedIndex


def test_tfidf_ranks_running_shoes():
    idx = InvertedIndex()
    idx.apply_event(
        {
            "event_id": "1",
            "campaign": {
                "id": "A",
                "name": "Campaign A",
                "headline": "Lightweight running shoes for daily miles",
                "keywords": ["running", "shoes", "lightweight"],
                "status": "active",
                "bid_micros": 4,
            },
        }
    )
    idx.apply_event(
        {
            "event_id": "2",
            "campaign": {
                "id": "B",
                "name": "Campaign B",
                "headline": "Pro racing running shoes",
                "keywords": ["running", "shoes", "racing"],
                "status": "active",
                "bid_micros": 6,
            },
        }
    )
    ranked = idx.search("running shoes")
    ids = {cid for cid, _ in ranked}
    assert {"A", "B"} <= ids
    assert all(0.0 <= rel <= 1.0001 for _, rel in ranked)


def test_idempotent_duplicate_event():
    idx = InvertedIndex()
    event = {
        "event_id": "event123",
        "campaign": {
            "id": "A",
            "name": "A",
            "headline": "running shoes",
            "keywords": ["running"],
            "status": "active",
        },
    }
    assert idx.apply_event(event) is True
    assert idx.apply_event(event) is False
    assert idx.apply_event({**event, "event_id": "event124"}) is True
    assert idx.apply_event({"event_id": "event123", "campaign": event["campaign"]}) is False


def test_paused_removed_from_index():
    idx = InvertedIndex()
    idx.apply_event(
        {
            "event_id": "e1",
            "campaign": {
                "id": "A",
                "name": "A",
                "headline": "running shoes",
                "keywords": ["running"],
                "status": "active",
            },
        }
    )
    idx.apply_event(
        {
            "event_id": "e2",
            "campaign": {
                "id": "A",
                "name": "A",
                "headline": "running shoes",
                "keywords": ["running"],
                "status": "paused",
            },
        }
    )
    assert idx.search("running") == []
