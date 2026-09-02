"""
TikTok hashtag discovery via the Apify TikTok Scraper actor
(clockworks/tiktok-scraper).

This talks to Apify's REST API directly with `httpx` rather than the Apify
Python SDK — one fewer dependency, and the actor's HTTP contract (run
synchronously, read the dataset) is simple enough not to need a client
library. If Apify's SDK gains something worth the dependency later, only
this file changes — that's the point of the DiscoverySource boundary.

Flow this class implements (identity extraction only — no profile data
is kept or passed on):

    hashtag search -> video results -> extract unique authors -> Candidate

A hashtag search actually returns *videos*, each with an embedded author.
That embedded author data is real profile information, but it's a byproduct
of the search, not a deliberate profile fetch — it may be stale, partial, or
scoped differently than a direct profile scrape would return. So it is used
ONLY to pull out (external_id, handle) pairs and is then discarded. If we
want this creator's current profile state, that's a separate, deliberate
ingestion-layer fetch (pipeline/ingestion/fetcher.py) — same as it would be
for a scheduled refresh of a creator we already know about. One fetch path,
not two.
"""
from __future__ import annotations

import httpx

from pipeline.discovery.base import Candidate, DiscoverySource

APIFY_RUN_SYNC_URL = (
    "https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
)


def _url_safe_actor_id(actor_id: str) -> str:
    """Apify's REST API requires actor IDs in 'username/actor-name' form to
    have the slash replaced with '~' when used in a URL path — e.g.
    'clockworks/tiktok-scraper' becomes 'clockworks~tiktok-scraper'. Using
    the raw slash makes the actor name look like two URL path segments and
    returns a 404, not an auth error — easy to misdiagnose as a bad token.
    Kept as a small helper (not just inlined) so both discovery and
    ingestion apply the same transformation identically.
    """
    return actor_id.replace("/", "~")


class TikTokHashtagDiscovery(DiscoverySource):
    def __init__(
        self,
        *,
        api_token: str,
        actor_id: str,
        timeout_seconds: int = 60,
        min_follower_count: int = 0,
    ):
        self._api_token = api_token
        self._actor_id = actor_id
        self._timeout = timeout_seconds
        # Cheap pre-qualification gate, added after a real Apify quota
        # exhaustion incident during development: the hashtag search
        # response already carries each author's follower count
        # (authorMeta.fans) as a free byproduct. If that count is already
        # below the follower floor tiering.MIN_FOLLOWERS_FOR_OUTREACH would
        # reject on anyway, there is no reason to spend a billed ingestion
        # fetch confirming what we already know. Default 0 = no filtering,
        # so existing callers/tests are unaffected unless they opt in.
        self._min_follower_count = min_follower_count

    def discover(self, query: str, *, max_results: int) -> list[Candidate]:
        """Run the actor synchronously against one hashtag and return
        candidate identities, deduplicated by author and pre-qualified by
        follower count (see __init__).

        Deliberately extracts only (external_id, handle) from each video's
        embedded authorMeta and discards the rest of the item — the video
        data and the embedded profile snapshot are not this method's
        responsibility to preserve (see module docstring). The follower
        count is the one exception: it's inspected to decide INCLUSION,
        never stored or passed downstream on the Candidate itself — that
        distinction is what keeps this consistent with the Candidate
        boundary rule rather than quietly reopening it.
        """
        response = httpx.post(
            APIFY_RUN_SYNC_URL.format(actor_id=_url_safe_actor_id(self._actor_id)),
            params={"token": self._api_token},
            json={
                "hashtags": [query],
                "resultsPerPage": max_results,
                "shouldDownloadVideos": False,
                "shouldDownloadCovers": False,
                "shouldDownloadAvatars": False,
            },
            timeout=self._timeout,
        )
        response.raise_for_status()
        items = response.json()

        seen_authors: set[str] = set()
        candidates: list[Candidate] = []

        for item in items:
            author_id = _get(item, "authorMeta.id", "authorMeta", "id")
            author_name = _get(item, "authorMeta.name", "authorMeta", "name")
            follower_count = _get(item, "authorMeta.fans", "authorMeta", "fans")

            if not author_id or author_id in seen_authors:
                continue
            seen_authors.add(author_id)

            if self._min_follower_count and (
                follower_count is None or follower_count < self._min_follower_count
            ):
                continue

            candidates.append(
                Candidate(
                    platform="tiktok",
                    external_id=str(author_id),
                    handle=author_name or "",
                    source="tiktok_hashtag",
                    query=query,
                )
            )

        return candidates


def _get(item: dict, dotted_key: str, nested_key: str, leaf_key: str):
    """Handle both payload shapes seen from Apify: flattened dotted keys
    (e.g. 'authorMeta.id') from some hashtag responses, and nested objects
    (item['authorMeta']['id']) from others. See pipeline/parsing for the
    equivalent handling on the full-profile side.
    """
    if dotted_key in item:
        return item[dotted_key]
    nested = item.get(nested_key)
    if isinstance(nested, dict):
        return nested.get(leaf_key)
    return None
