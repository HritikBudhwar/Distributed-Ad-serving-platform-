def score(relevance: float, bid_micros: int) -> float:
    return max(relevance, 0.0) * max(bid_micros, 0)


def run_auction(cands: list[tuple[str, float, int]]) -> dict | None:
    cands = [(i, r, b) for i, r, b in cands if r > 0 and b > 0]
    if not cands:
        return None
    ranked = sorted(cands, key=lambda x: score(x[1], x[2]), reverse=True)
    winner = ranked[0]
    win_score = score(winner[1], winner[2])
    second = score(ranked[1][1], ranked[1][2]) if len(ranked) > 1 else None
    if second is not None and winner[1] > 0:
        charged = int(__import__("math").ceil(second / winner[1]))
    else:
        charged = int(__import__("math").ceil(winner[2] * 0.8))
    return {"id": winner[0], "charged": max(charged, 1), "score": win_score}


def test_running_shoes_example():
    result = run_auction([("A", 0.91, 4_000_000), ("B", 0.84, 6_000_000), ("C", 0.77, 5_000_000)])
    assert result is not None
    assert result["id"] == "B"
    import math

    expected = math.ceil(score(0.77, 5_000_000) / 0.84)
    assert result["charged"] == expected
