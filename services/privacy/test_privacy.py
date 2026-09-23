"""Privacy k-anonymity rules (unit-level, no network)."""


def cohort_eligible(member_count: int, k_threshold: int = 50) -> bool:
    return member_count >= k_threshold


def test_k_threshold():
    assert cohort_eligible(1280) is True
    assert cohort_eligible(12) is False
    assert cohort_eligible(50) is True
