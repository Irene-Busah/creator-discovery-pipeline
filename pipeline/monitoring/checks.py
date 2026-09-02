"""
Monitoring checks. Each function returns a CheckResult (pass/fail +
message) rather than raising or alerting directly — dag_monitoring.py
decides what to do with a failed check (fail the task, which Airflow's own
alerting picks up; a real deployment would add a Slack/email hook here,
deliberately out of scope for this take-home). Keeping checks pure return
values also makes them trivially unit-testable against a fake repository.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from pipeline.config.settings import Settings

if TYPE_CHECKING:
    from pipeline.db.repository import Repository


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    message: str


def check_discovery_freshness(
    repository: "Repository", *, lookback_hours: int = 24, min_expected: int = 1
) -> CheckResult:
    """Directly answers the brief's 'how would you notice within a day that
    discovery had silently stopped working' — a single row-count query
    against discovery_events. If a hashtag search endpoint changes shape,
    an API key expires, or Apify starts returning empty results, this drops
    to zero and the check fails within one monitoring cycle.
    """
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    count = repository.count_discovery_events_since(cutoff)
    passed = count >= min_expected
    return CheckResult(
        name="discovery_freshness",
        passed=passed,
        message=f"{count} discovery events in the last {lookback_hours}h (expected >= {min_expected})",
    )


def check_ingestion_freshness(
    repository: "Repository", *, lookback_hours: int = 2, min_expected: int = 1
) -> CheckResult:
    """The brief asks explicitly for 'a flag when a table stops growing' —
    check_discovery_freshness covers discovery_events; this is the same
    pattern applied to raw_payloads. dag_ingestion runs every 15 minutes,
    so a 2-hour window with zero new rows means ingestion has been stuck
    (or the queue has been empty) for 8 consecutive cycles — worth a flag
    on its own, distinct from check_ingestion_health's failure-RATE signal,
    which stays silent if the queue is simply empty rather than failing.
    """
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    count = repository.count_raw_payloads_since(cutoff)
    passed = count >= min_expected
    return CheckResult(
        name="ingestion_freshness",
        passed=passed,
        message=f"{count} raw payloads landed in the last {lookback_hours}h (expected >= {min_expected})",
    )


def check_ingestion_health(repository: "Repository", *, max_failure_rate: float = 0.2) -> CheckResult:
    """Failed / total in ingestion_queue. A spike here usually means Apify
    rate-limiting or an actor input change — worth catching before the
    queue backs up silently. See check_quota_exhaustion() for the
    narrower, more actionable version of this signal.
    """
    counts = repository.count_ingestion_queue_by_status()
    total = sum(counts.values())
    failed = counts.get("failed", 0)

    if total == 0:
        return CheckResult(name="ingestion_health", passed=True, message="No ingestion queue activity yet")

    failure_rate = failed / total
    passed = failure_rate <= max_failure_rate
    return CheckResult(
        name="ingestion_health",
        passed=passed,
        message=f"{failed}/{total} ingestion items failed ({failure_rate:.1%}, threshold {max_failure_rate:.1%})",
    )


def check_quota_exhaustion(repository: "Repository", *, max_quota_errors: int = 5) -> CheckResult:
    """Distinct from check_ingestion_health deliberately: a generic
    failure-rate spike and running out of Apify budget look identical as a
    raw number, but call for completely different responses. This ran
    into the real case it exists for during development — 54 ingestion
    failures all traced back to Apify's free tier running out mid-run
    (402 Payment Required), and without this check that would have looked
    like "something's broken with the source" rather than "we need more
    budget or to slow down."
    """
    quota_errors = repository.count_quota_exhaustion_errors()
    passed = quota_errors <= max_quota_errors
    return CheckResult(
        name="quota_exhaustion",
        passed=passed,
        message=f"{quota_errors} ingestion failures look like quota/rate-limit errors (402/429)"
        + ("" if passed else " — check Apify account usage/billing"),
    )


def check_parse_error_rate(repository: "Repository", *, max_failure_rate: float = 0.1) -> CheckResult:
    """Failed / total in raw_payloads. This is the data-quality half of
    monitoring — a rising parse-failure rate is usually a payload-shape
    change from the source (see the tiktok_hashtag_parser vs
    tiktok_profile_parser split), caught here before it silently drops
    creators from scoring.
    """
    counts = repository.count_raw_payloads_by_parse_status()
    total = sum(counts.values())
    failed = counts.get("failed", 0)

    if total == 0:
        return CheckResult(name="parse_error_rate", passed=True, message="No raw payloads processed yet")

    failure_rate = failed / total
    passed = failure_rate <= max_failure_rate
    return CheckResult(
        name="parse_error_rate",
        passed=passed,
        message=f"{failed}/{total} raw payloads failed to parse ({failure_rate:.1%}, threshold {max_failure_rate:.1%})",
    )


def check_tier_freshness(repository: "Repository", settings: Settings) -> list[CheckResult]:
    """One result per tier: is the most recent profile_snapshot reachable
    through that tier's creators within the tier's own refresh SLA? This is
    the concrete check behind 'tier A creators aren't getting refreshed on
    time' rather than trusting the scheduler ran without verifying it.
    """
    latest_by_tier = repository.latest_snapshot_date_by_tier()
    results = []
    today = datetime.utcnow().date()

    for tier, interval_days in settings.refresh_interval_days.items():
        latest_date = latest_by_tier.get(tier)
        if latest_date is None:
            results.append(
                CheckResult(name=f"tier_freshness_{tier}", passed=True, message=f"No creators in tier {tier} yet")
            )
            continue

        age_days = (today - latest_date).days
        # Allow a small grace window over the raw interval — a refresh due
        # exactly on the boundary shouldn't page anyone.
        passed = age_days <= interval_days + 2
        results.append(
            CheckResult(
                name=f"tier_freshness_{tier}",
                passed=passed,
                message=f"Tier {tier} most recent snapshot is {age_days}d old (SLA {interval_days}d)",
            )
        )

    return results


def run_all_checks(repository: "Repository", settings: Settings) -> list[CheckResult]:
    return [
        check_discovery_freshness(repository),
        check_ingestion_freshness(repository),
        check_ingestion_health(repository),
        check_quota_exhaustion(repository),
        check_parse_error_rate(repository),
        *check_tier_freshness(repository, settings),
    ]
