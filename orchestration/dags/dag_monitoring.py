"""
Monitoring DAG — hourly, deliberately independent of the other five DAGs'
schedules and failures. If ingestion is having a bad day, monitoring still
needs to run and report that — it must never be blocked by the thing it's
checking.

Each check task raises if its CheckResult failed. That's the alerting
mechanism for this take-home: a failed task is a red task in the Airflow
UI and (with Airflow's own email/Slack-on-failure config, not built here)
an alert. A production version would push each CheckResult to a real
alerting channel directly rather than relying solely on task-failure
notifications — named here as the next thing to build, not hidden.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.config.settings import Settings
from pipeline.db.repository import Repository
from pipeline.db.session import get_session
from pipeline.monitoring.checks import (
    check_discovery_freshness,
    check_ingestion_freshness,
    check_ingestion_health,
    check_parse_error_rate,
    check_quota_exhaustion,
    check_tier_freshness,
)


def _raise_if_failed(result) -> None:
    if not result.passed:
        raise RuntimeError(f"[{result.name}] {result.message}")


@dag(
    dag_id="dag_monitoring",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["monitoring"],
)
def dag_monitoring():
    @task
    def discovery_freshness() -> None:
        settings = Settings.from_env()
        with get_session(settings) as session:
            _raise_if_failed(check_discovery_freshness(Repository(session)))

    @task
    def ingestion_freshness() -> None:
        settings = Settings.from_env()
        with get_session(settings) as session:
            _raise_if_failed(check_ingestion_freshness(Repository(session)))

    @task
    def ingestion_health() -> None:
        settings = Settings.from_env()
        with get_session(settings) as session:
            _raise_if_failed(check_ingestion_health(Repository(session)))

    @task
    def quota_exhaustion() -> None:
        settings = Settings.from_env()
        with get_session(settings) as session:
            _raise_if_failed(check_quota_exhaustion(Repository(session)))

    @task
    def parse_error_rate() -> None:
        settings = Settings.from_env()
        with get_session(settings) as session:
            _raise_if_failed(check_parse_error_rate(Repository(session)))

    @task
    def tier_freshness() -> None:
        settings = Settings.from_env()
        with get_session(settings) as session:
            results = check_tier_freshness(Repository(session), settings)
            failures = [r.message for r in results if not r.passed]
            if failures:
                raise RuntimeError("; ".join(failures))

    # All four checks are independent of each other — no reason for one
    # slow check to hold up the others, so no explicit ordering between them.
    discovery_freshness()
    ingestion_freshness()
    ingestion_health()
    quota_exhaustion()
    parse_error_rate()
    tier_freshness()


dag_monitoring()
