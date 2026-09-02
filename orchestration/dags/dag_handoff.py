"""
Handoff DAG — every 15 minutes. Pushes newly-qualified (tier A/B) creators
into outreach_queue. Deliberately simple, single-task: unlike ingestion/
enrichment/scoring, handoff has no per-item external call to isolate
failures around — it's one idempotent bulk operation against the database,
so it doesn't need dynamic task mapping.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.config.settings import Settings
from pipeline.db.repository import Repository
from pipeline.db.session import get_session
from pipeline.handoff.outreach import push_qualified_creators


@dag(
    dag_id="dag_handoff",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["handoff"],
)
def dag_handoff():
    @task
    def push_qualified() -> int:
        settings = Settings.from_env()
        with get_session(settings) as session:
            repo = Repository(session)
            pushed = push_qualified_creators(repo)
            repo.commit()
            return pushed

    push_qualified()


dag_handoff()
