"""
Scoring DAG — hourly. Recomputes signals and assigns a tier for every
active creator. v1 rescoring is intentionally simple: score everyone,
every cycle, rather than tracking "who changed since last score" — at
10K creators/month this is cheap; the incremental version (only rescore
creators enrichment touched this cycle) is a named optimization for scale,
not a v1 requirement.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.config.settings import Settings
from pipeline.db.repository import Repository
from pipeline.db.session import get_session
from pipeline.enrichment.features import build_creator_signals, compute_posting_consistency_for_creator
from pipeline.scoring.tiering import assign_tier

SCORE_VERSION = "v1"


@dag(
    dag_id="dag_scoring",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["scoring"],
)
def dag_scoring():
    @task
    def list_active_creators() -> list[str]:
        settings = Settings.from_env()
        with get_session(settings) as session:
            repo = Repository(session)
            return repo.active_creator_ids()

    @task
    def score_one(creator_id: str) -> None:
        settings = Settings.from_env()
        with get_session(settings) as session:
            repo = Repository(session)

            signals = build_creator_signals(repo, creator_id)
            result = assign_tier(signals)
            posting_consistency = compute_posting_consistency_for_creator(repo, creator_id)

            repo.save_score(
                creator_id,
                score_version=SCORE_VERSION,
                engagement_rate=signals.engagement_rate,
                growth_rate=signals.growth_rate,
                posting_consistency=posting_consistency,
                account_type=signals.account_type,
                tier=result.tier,
                flags=result.flags,
            )
            repo.commit()

    creator_ids = list_active_creators()
    score_one.expand(creator_id=creator_ids)


dag_scoring()
