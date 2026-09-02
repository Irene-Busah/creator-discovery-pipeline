"""
Discovery DAG — daily. Two independent jobs, sequenced but not dependent
on each other's data: (1) search configured hashtags for new candidates,
(2) enqueue existing creators whose tier-based refresh interval has
elapsed. Writes only to discovery_events + ingestion_queue — never fetches
a full profile itself (that boundary is the point of pipeline/discovery vs
pipeline/ingestion; see pipeline/discovery/tiktok_hashtag.py's docstring).
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from pipeline.config.settings import Settings
from pipeline.db.repository import Repository
from pipeline.db.session import get_session
from pipeline.discovery.tiktok_hashtag import TikTokHashtagDiscovery
from pipeline.scoring.tiering import MIN_FOLLOWERS_FOR_OUTREACH

# Niche queries this pipeline is scoped to for the take-home. In production
# this would be data-driven (campaign config in its own table), not a
# hardcoded list — named here as a known v1 simplification.
DISCOVERY_QUERIES = ["skincare", "skincareroutine", "skincaretips"]


@dag(
    dag_id="dag_discovery",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["discovery"],
)
def dag_discovery():
    @task
    def discover_new_candidates() -> int:
        settings = Settings.from_env()
        source = TikTokHashtagDiscovery(
            api_token=settings.apify_api_token,
            actor_id=settings.apify_tiktok_actor_id,
            # Reuses tiering's own follower floor rather than a separate
            # config value — a candidate below this is guaranteed to be
            # Rejected once scored anyway, so there's no reason to spend a
            # billed ingestion fetch confirming that. See
            # tiktok_hashtag.py's docstring for why this doesn't reopen the
            # Candidate identity-only boundary.
            min_follower_count=MIN_FOLLOWERS_FOR_OUTREACH,
        )
        total_found = 0
        with get_session(settings) as session:
            repo = Repository(session)
            for query in DISCOVERY_QUERIES:
                candidates = source.discover(query, max_results=settings.max_profiles_per_query)
                for candidate in candidates:
                    repo.record_discovery_event(
                        platform=candidate.platform,
                        external_id=candidate.external_id,
                        source=candidate.source,
                        query=candidate.query,
                    )
                    repo.enqueue_for_ingestion(
                        platform=candidate.platform,
                        external_id=candidate.external_id,
                        handle=candidate.handle,
                        reason="new_candidate",
                        priority=5,
                    )
                total_found += len(candidates)
            repo.commit()
        return total_found

    @task
    def enqueue_scheduled_refreshes() -> int:
        settings = Settings.from_env()
        with get_session(settings) as session:
            repo = Repository(session)
            due = repo.creators_due_for_refresh(settings.refresh_interval_days)
            for entry in due:
                # Priority 1 (highest) regardless of which tier triggered
                # the refresh — a refresh, once due, is time-sensitive; the
                # tier already determined the interval, it doesn't need to
                # also determine queue priority relative to other refreshes.
                repo.enqueue_for_ingestion(
                    platform=entry["platform"],
                    external_id=entry["external_id"],
                    handle=entry["handle"],
                    reason="scheduled_refresh",
                    priority=1,
                )
            repo.commit()
        return len(due)

    discover_new_candidates() >> enqueue_scheduled_refreshes()


dag_discovery()
