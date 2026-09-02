"""
Profile fetching — ingestion's only job: given a creator identity, get the
CURRENT state of that creator from the source, as its own deliberate call.

This is the direct fix for the discovery/ingestion boundary: discovery
(pipeline/discovery/tiktok_hashtag.py) extracts identities from hashtag
search results and throws away the embedded profile data it saw along the
way. This module is where a creator's profile data is actually meant to be
fetched — once for a brand-new candidate, and again on every scheduled
refresh. Same code path both times, which is the point: a "refresh" isn't a
different operation from an "initial fetch," it's the same fetch run again
later. Nothing here writes to the database — it returns a raw payload, and
the caller (an ingestion DAG task) is responsible for landing it via
Repository.save_raw_payload. Fetching and persisting are different
responsibilities.
"""
from __future__ import annotations

from dataclasses import dataclass

import httpx

from pipeline.discovery.base import Candidate

APIFY_RUN_SYNC_URL = (
    "https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
)


def _url_safe_actor_id(actor_id: str) -> str:
    """Same fix as pipeline/discovery/tiktok_hashtag.py's helper of the same
    name — Apify requires 'username/actor-name' style IDs to use '~'
    instead of '/' in the URL path. Duplicated here rather than imported
    from the discovery module: it's a 3-line pure function, and importing
    across the discovery/ingestion boundary for something this small isn't
    worth the coupling.
    """
    return actor_id.replace("/", "~")


@dataclass(frozen=True)
class FetchResult:
    """What a fetch produced, ready to hand to Repository.save_raw_payload.
    `ok=False` means the source responded but returned nothing usable (e.g.
    a private account, or a handle that no longer exists) — that's a real,
    distinct outcome from a network/API failure, which raises instead of
    returning FetchResult at all. See ProfileFetcher.fetch()'s docstring.
    """

    platform: str
    external_id: str
    source_type: str      # 'profile' — distinguishes this from a hashtag/video landing
    payload: dict | None
    ok: bool
    reason: str | None = None   # set when ok=False, e.g. 'no_data', 'private_account'


class ProfileFetcher:
    def __init__(self, *, api_token: str, actor_id: str, timeout_seconds: int = 60):
        self._api_token = api_token
        self._actor_id = actor_id
        self._timeout = timeout_seconds

    def fetch(self, candidate: Candidate) -> FetchResult:
        """Fetch the current profile + recent videos for one creator.

        Raises httpx.HTTPStatusError on a network/API-level failure (rate
        limit, actor error, timeout) — the caller decides whether to retry
        or write a parse_errors-style record for that. This method only
        returns a "soft" FetchResult(ok=False, ...) when Apify *responded
        successfully* but there is nothing to parse (empty dataset), because
        that is a fact about the creator (private, deleted, no posts), not
        a pipeline failure — distinguishing these two is exactly the
        no_data vs fetch_failed split the brief asks for in scrape_status.
        """
        response = httpx.post(
            APIFY_RUN_SYNC_URL.format(actor_id=_url_safe_actor_id(self._actor_id)),
            params={"token": self._api_token},
            json={
                "profiles": [candidate.handle],
                "profileScrapeSections": ["videos"],
                "profileSorting": "latest",
                "resultsPerPage": 30,
                "shouldDownloadVideos": False,
                "shouldDownloadCovers": False,
                "shouldDownloadAvatars": False,
            },
            timeout=self._timeout,
        )
        response.raise_for_status()
        items = response.json()

        if not items:
            return FetchResult(
                platform=candidate.platform,
                external_id=candidate.external_id,
                source_type="profile",
                payload=None,
                ok=False,
                reason="no_data",
            )

        # A profile scrape returns one item per video, each carrying the
        # same embedded authorMeta. We land the first item as the
        # authoritative snapshot for this fetch; parsing (tiktok_profile_
        # parser.py) is responsible for pulling every video out of the full
        # `items` list separately — fetcher's job stops at "here is what
        # the source returned," not "here is the normalized result."
        return FetchResult(
            platform=candidate.platform,
            external_id=candidate.external_id,
            source_type="profile",
            payload={"profile_item": items[0], "video_items": items},
            ok=True,
        )
