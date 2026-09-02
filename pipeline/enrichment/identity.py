"""
Creator identity resolution: given either a successfully parsed profile or
a failed fetch, resolve/create the creator record and write the current-
state snapshot. This is the ONLY place profile_snapshots/videos/
video_snapshots get written from — every enrichment DAG task calls into
here rather than writing history directly, so "how a snapshot gets
created" has exactly one implementation to reason about.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pipeline.parsing.base import ParsedProfile, ParsedVideo

if TYPE_CHECKING:
    from pipeline.db.repository import Repository


def enrich_from_successful_fetch(
    repository: "Repository",
    *,
    platform: str,
    external_id: str,
    profile: ParsedProfile,
    videos: list[ParsedVideo],
) -> str:
    """Upserts the creator identity and appends one profile snapshot plus
    one video + video snapshot per parsed video. Returns creator_id as a
    string — callers crossing an Airflow task boundary need a
    JSON-serializable value; Repository accepts either (see _as_uuid).
    """
    creator_id = repository.upsert_creator(platform=platform, external_id=external_id)

    repository.add_profile_snapshot(
        creator_id,
        handle=profile.handle,
        follower_count=profile.follower_count,
        following_count=profile.following_count,
        lifetime_likes=profile.lifetime_likes,
        video_count=profile.video_count,
        bio=profile.bio,
        bio_link=profile.bio_link,
        is_verified=profile.is_verified,
        is_private=profile.is_private,
        is_commerce_account=profile.is_commerce_account,
        scrape_status="success",
    )

    for video in videos:
        repository.upsert_video(
            video.video_id,
            creator_id,
            posted_at=video.posted_at,
            is_ad=video.is_ad,
            text_language=video.text_language,
            web_video_url=video.web_video_url,
        )
        repository.add_video_snapshot(
            video.video_id,
            play_count=video.play_count,
            digg_count=video.digg_count,
            comment_count=video.comment_count,
            share_count=video.share_count,
            collect_count=video.collect_count,
        )

    return str(creator_id)


def enrich_from_failed_fetch(
    repository: "Repository", *, platform: str, external_id: str, handle: str, reason: str
) -> str:
    """A fetch that came back empty or failed is still recorded as a
    profile_snapshot — NOT skipped — with scrape_status set to the failure
    reason ('no_data' or 'fetch_failed'). Two things depend on this:

      1. Tiering (pipeline/scoring/tiering.py) needs scrape_status to tell
         apart "creator genuinely has no data" from "we failed to fetch it,"
         per the brief's explicit ask.
      2. Monitoring's freshness check reads profile_snapshots to know a
         creator was attempted at all — a creator that only ever fails
         silently, with no row written, would look identical to one nobody
         ever tried to fetch. That distinction matters for debugging a
         string of scraper failures.
    """
    creator_id = repository.upsert_creator(platform=platform, external_id=external_id)
    repository.add_profile_snapshot(
        creator_id,
        handle=handle,
        follower_count=None,
        following_count=None,
        lifetime_likes=None,
        video_count=None,
        bio=None,
        bio_link=None,
        is_verified=False,
        is_private=False,
        is_commerce_account=False,
        scrape_status=reason,
    )
    return str(creator_id)
