"""
NOTE: monkeypatching `pipeline.discovery.tiktok_hashtag.httpx.post` runs
into the same limitation as tests/discovery/test_candidate_boundary.py's
fetcher tests when run under this repo's dependency-free CI shim — verify
these manually (as was done during development, see conversation history)
or run under real pytest + pytest-mock, where this pattern works normally.
"""
from unittest.mock import MagicMock, patch

from pipeline.discovery.tiktok_hashtag import TikTokHashtagDiscovery

FAKE_ITEMS = [
    {"id": "v1", "authorMeta": {"id": "a1", "name": "big_creator", "fans": 50000}},
    {"id": "v2", "authorMeta": {"id": "a2", "name": "tiny_account", "fans": 200}},
    {"id": "v3", "authorMeta": {"id": "a3", "name": "no_follower_data"}},  # missing 'fans'
]


def _fake_response():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = FAKE_ITEMS
    return resp


def test_no_filter_by_default_returns_every_candidate():
    with patch("pipeline.discovery.tiktok_hashtag.httpx.post", return_value=_fake_response()):
        discovery = TikTokHashtagDiscovery(api_token="x", actor_id="a/b")
        result = discovery.discover("q", max_results=10)
    assert len(result) == 3


def test_min_follower_count_filters_out_small_and_unknown_accounts():
    with patch("pipeline.discovery.tiktok_hashtag.httpx.post", return_value=_fake_response()):
        discovery = TikTokHashtagDiscovery(api_token="x", actor_id="a/b", min_follower_count=5000)
        result = discovery.discover("q", max_results=10)
    assert len(result) == 1
    assert result[0].handle == "big_creator"
