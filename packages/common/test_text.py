from adpulse_common import tokenize


def test_tokenize_running_shoes():
    assert tokenize("Running Shoes") == ["running", "shoes"]
