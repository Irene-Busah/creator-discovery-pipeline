"""
These tests exist specifically to lock in the boundary fix: a Candidate
carries identity only, and a profile fetch is always ingestion's own
deliberate call — never data smuggled in from discovery.
"""
import dataclasses
from unittest.mock import MagicMock, patch

from pipeline.discovery.base import Candidate
from pipeline.ingestion.fetcher import FetchResult, ProfileFetcher


def test_candidate_has_no_payload_field():
    """If someone re-adds a raw_payload field to Candidate, this test fails
    loudly instead of the boundary quietly eroding again.
    """
    field_names = {f.name for f in dataclasses.fields(Candidate)}
    assert "raw_payload" not in field_names
    assert field_names == {"platform", "external_id", "handle", "source", "query"}


def test_actor_id_slash_is_converted_to_tilde_for_apify_url():
    """Regression test for a real bug: Apify's REST API requires
    'username/actor-name' style actor IDs to use '~' instead of '/' in the
    URL path — the raw slash gets parsed as extra path segments and
    returns a 404 (not an auth error), which is easy to misdiagnose as a
    bad token. Both discovery and ingestion build this URL independently;
    both must apply the same fix.
    """
    from pipeline.discovery.tiktok_hashtag import _url_safe_actor_id as discovery_fn
    from pipeline.ingestion.fetcher import _url_safe_actor_id as ingestion_fn

    assert discovery_fn("clockworks/tiktok-scraper") == "clockworks~tiktok-scraper"
    assert ingestion_fn("clockworks/tiktok-scraper") == "clockworks~tiktok-scraper"
    # An actor ID with no slash (already in raw ID form) must pass through unchanged
    assert discovery_fn("aYG0l9s7dbB7j3gbS") == "aYG0l9s7dbB7j3gbS"


def test_fetch_returns_ok_false_with_reason_on_empty_dataset():
    """An empty Apify response (private account, deleted handle, no posts)
    is a fact about the creator, not a pipeline error — it must come back
    as a soft FetchResult, not raise.
    """
    fetcher = ProfileFetcher(api_token="fake", actor_id="fake/actor")

    fake_response = MagicMock()
    fake_response.raise_for_status.return_value = None
    fake_response.json.return_value = []

    candidate = Candidate(
        platform="tiktok",
        external_id="123",
        handle="somecreator",
        source="tiktok_hashtag",
        query="skincare",
    )

    with patch("pipeline.ingestion.fetcher.httpx.post", return_value=fake_response):
        result = fetcher.fetch(candidate)

    assert result.ok is False
    assert result.reason == "no_data"
    assert result.payload is None


def test_fetch_returns_ok_true_with_full_item_list():
    fetcher = ProfileFetcher(api_token="fake", actor_id="fake/actor")

    fake_items = [{"id": "1", "authorMeta": {"id": "123"}}, {"id": "2", "authorMeta": {"id": "123"}}]
    fake_response = MagicMock()
    fake_response.raise_for_status.return_value = None
    fake_response.json.return_value = fake_items

    candidate = Candidate(
        platform="tiktok",
        external_id="123",
        handle="somecreator",
        source="tiktok_hashtag",
        query="skincare",
    )

    with patch("pipeline.ingestion.fetcher.httpx.post", return_value=fake_response):
        result = fetcher.fetch(candidate)

    assert result.ok is True
    assert result.payload["profile_item"] == fake_items[0]
    assert result.payload["video_items"] == fake_items
