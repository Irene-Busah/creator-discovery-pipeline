"""
Builds the inputs pipeline/scoring/tiering.py needs, by reading a creator's
stored history through the repository and handing plain numbers to the
pure functions in pipeline/scoring/signals.py. This is the seam between
"what's in the database" and "what the scoring math needs" — database
access lives here; signals.py and tiering.py stay pure and DB-free.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pipeline.scoring.signals import (
    VideoEngagement,
    classify_account_type,
    growth_rate,
    is_suspicious_growth,
    lifetime_engagement_rate,
    posting_consistency,
    recent_engagement_rate,
)
from pipeline.scoring.tiering import CreatorSignals

if TYPE_CHECKING:
    # Only needed for the type hints below — importing Repository at
    # runtime would pull in SQLAlchemy as a hard dependency of this module,
    # when in practice these functions just need something duck-typed like
    # Repository (see tests/enrichment/test_enrichment.py's FakeRepository).
    from pipeline.db.repository import Repository


def build_creator_signals(repository: "Repository", creator_id: str) -> CreatorSignals:
    """The main entry point for scoring: turns a creator's stored history
    into the struct assign_tier() needs. Handles the "no snapshot at all"
    and "last fetch failed" cases explicitly — those short-circuit before
    touching any of the engagement math, which is exactly the ordering
    pipeline/scoring/tiering.py's assign_tier() also relies on.
    """
    snapshots = repository.latest_snapshots_for_scoring(creator_id, n=2)

    if not snapshots:
        # No snapshot exists yet — shouldn't happen if enrichment ran first
        # for this creator, but fail safe (needs_review) rather than crash
        # the scoring DAG over one inconsistent creator.
        return CreatorSignals(
            follower_count=None,
            engagement_rate=None,
            growth_rate=None,
            account_type="creator",
            scrape_status="fetch_failed",
        )

    current = snapshots[0]
    previous = snapshots[1] if len(snapshots) > 1 else None

    if current.scrape_status != "success":
        return CreatorSignals(
            follower_count=current.follower_count,
            engagement_rate=None,
            growth_rate=None,
            account_type="creator",
            scrape_status=current.scrape_status,
        )

    videos = repository.recent_video_engagement(creator_id)

    engagement = lifetime_engagement_rate(
        lifetime_likes=current.lifetime_likes,
        video_count=current.video_count,
        follower_count=current.follower_count,
    )
    if engagement is None:
        # Fall back to the recent-videos average when lifetime figures are
        # missing (e.g. Apify didn't return `heart` for this account) —
        # better than scoring with no engagement signal at all.
        engagement = recent_engagement_rate(
            videos=[
                VideoEngagement(
                    is_ad=v["is_ad"],
                    play_count=v["play_count"],
                    digg_count=v["digg_count"],
                    comment_count=v["comment_count"],
                    share_count=v["share_count"],
                )
                for v in videos
            ],
            follower_count=current.follower_count,
        )

    growth = growth_rate(
        previous_follower_count=previous.follower_count if previous else None,
        previous_date=previous.snapshot_date if previous else None,
        current_follower_count=current.follower_count,
        current_date=current.snapshot_date,
    )

    account_type = classify_account_type(
        bio=current.bio,
        bio_link=current.bio_link,
        is_commerce_account=bool(current.is_commerce_account),
    )

    suspicious = is_suspicious_growth(growth_rate_value=growth, engagement_rate_value=engagement)

    return CreatorSignals(
        follower_count=current.follower_count,
        engagement_rate=engagement,
        growth_rate=growth,
        account_type=account_type,
        scrape_status=current.scrape_status,
        suspicious_growth=suspicious,
    )


def compute_posting_consistency_for_creator(repository: "Repository", creator_id: str):
    """Separate from build_creator_signals deliberately: posting_consistency
    is stored on creator_scores for future tiering revisions (the brief
    lists 'posting consistency' as a signal worth having) but is NOT yet
    part of assign_tier()'s decision — v1 tiers on engagement rate alone,
    documented explicitly rather than silently included and unused. Keeping
    the computation separate means adding it to tiering later is a change
    to tiering.py's inputs, not a rewrite of this function.
    """
    from datetime import date as date_cls

    videos = repository.recent_video_engagement(creator_id)
    posted_dates = [v["posted_at"].date() for v in videos if v["posted_at"] is not None]
    return posting_consistency(posted_dates=posted_dates, as_of=date_cls.today())
