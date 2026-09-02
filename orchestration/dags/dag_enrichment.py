"""
Enrichment DAG — every 15 minutes. Reads raw_payloads still marked
'pending', parses each, and writes normalized creator/profile/video
history via pipeline/enrichment/identity.py.

Same isolated-failure pattern as ingestion: one mapped task instance per
raw payload. A parser bug or a malformed single record marks that instance
failed and writes to parse_errors — it does not stop the rest of the batch
from being enriched.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.config.settings import Settings
from pipeline.db.repository import Repository
from pipeline.db.session import get_session
from pipeline.enrichment.identity import enrich_from_failed_fetch, enrich_from_successful_fetch
from pipeline.observability.logging_utils import get_logger
from pipeline.parsing.base import ParseError
from pipeline.parsing.tiktok_profile_parser import TikTokProfileParser

logger = get_logger(__name__)


@dag(
    dag_id="dag_enrichment",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["enrichment"],
)
def dag_enrichment():
    @task
    def list_pending_raw_payloads() -> list[dict]:
        settings = Settings.from_env()
        with get_session(settings) as session:
            repo = Repository(session)
            pending = repo.pending_raw_payloads(limit=200)
            return [
                {
                    "raw_id": str(p.raw_id),
                    "platform": p.platform,
                    "external_id": p.external_id,
                    "payload": p.payload,
                }
                for p in pending
            ]

    @task
    def parse_one(raw: dict) -> None:
        settings = Settings.from_env()
        parser = TikTokProfileParser()

        with get_session(settings) as session:
            repo = Repository(session)
            payload = raw["payload"]

            try:
                if payload.get("ok") is False:
                    # This came from a soft fetch failure (see
                    # ProfileFetcher.fetch / dag_ingestion) — not a parse
                    # error, a recorded fact about the creator. Still needs
                    # a profile_snapshot row (see identity.py's docstring
                    # on why failures aren't skipped).
                    enrich_from_failed_fetch(
                        repo,
                        platform=raw["platform"],
                        external_id=raw["external_id"],
                        handle=raw["external_id"],  # handle unknown when the fetch itself failed
                        reason=payload.get("reason", "fetch_failed"),
                    )
                else:
                    profile = parser.parse_profile(payload)
                    videos = parser.parse_videos(payload)
                    enrich_from_successful_fetch(
                        repo,
                        platform=raw["platform"],
                        external_id=raw["external_id"],
                        profile=profile,
                        videos=videos,
                    )

                repo.mark_raw_payload(raw["raw_id"], status="parsed")
                repo.commit()

            except ParseError as e:
                logger.warning(
                    "parse failed | platform=%s external_id=%s | %s",
                    raw["platform"], raw["external_id"], str(e),
                )
                repo.record_parse_error(raw["raw_id"], str(e))
                repo.mark_raw_payload(raw["raw_id"], status="failed")
                repo.commit()
                # Deliberately NOT re-raised: a parse failure is an expected,
                # handled outcome (that's the whole point of parse_errors),
                # not a task failure Airflow should retry — retrying the
                # same malformed payload would fail identically every time.

    pending = list_pending_raw_payloads()
    parse_one.expand(raw=pending)


dag_enrichment()
