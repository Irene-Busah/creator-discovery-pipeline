"""
Ingestion DAG — every 15 minutes. Pulls a batch off ingestion_queue and
performs the actual profile fetch for each item.

Each creator is its own mapped task instance via .expand() — this is the
concrete mechanism (not just the intention) for "what happens when a step
fails halfway through a batch": one fetch failing marks only that mapped
instance failed and retries it on Airflow's normal retry/backoff, while the
rest of the batch's mapped instances complete independently.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.config.settings import Settings
from pipeline.db.repository import Repository
from pipeline.db.session import get_session
from pipeline.discovery.base import Candidate
from pipeline.ingestion.fetcher import ProfileFetcher
from pipeline.observability.errors import summarize_exception
from pipeline.observability.logging_utils import get_logger

logger = get_logger(__name__)


@dag(
    dag_id="dag_ingestion",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion"],
)
def dag_ingestion():
    @task
    def dequeue_batch() -> list[dict]:
        settings = Settings.from_env()
        with get_session(settings) as session:
            repo = Repository(session)
            items = repo.dequeue_batch(limit=settings.ingestion_batch_size)
            repo.commit()
            return [
                {
                    "queue_id": str(item.queue_id),
                    "platform": item.platform,
                    "external_id": item.external_id,
                    "handle": item.handle,
                }
                for item in items
            ]

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def fetch_one(item: dict) -> None:
        """One mapped instance per queue item. Landing the raw payload
        happens here, immediately after fetch — parsing happens later, in
        dag_enrichment, deliberately separated so a parser bug never means
        a re-fetch (see sql/schema.sql's raw_payloads comment).
        """
        settings = Settings.from_env()
        fetcher = ProfileFetcher(
            api_token=settings.apify_api_token, actor_id=settings.apify_tiktok_actor_id
        )
        candidate = Candidate(
            platform=item["platform"],
            external_id=item["external_id"],
            handle=item["handle"],
            source="ingestion_queue",
            query="",
        )

        with get_session(settings) as session:
            repo = Repository(session)
            try:
                result = fetcher.fetch(candidate)
                payload = result.payload if result.ok else {"ok": False, "reason": result.reason}
                repo.save_raw_payload(
                    platform=candidate.platform,
                    source_type="profile",
                    external_id=candidate.external_id,
                    payload=payload,
                )
                repo.mark_queue_item(item["queue_id"], status="done")
                repo.commit()
            except Exception as e:
                # One line, human-readable, in the task log immediately —
                # no more digging through a raw traceback to find out this
                # was a 402. Same summary also lands in ingestion_queue.error
                # (compact, queryable) via the same function, so the two
                # never say different things about the same failure.
                summary = summarize_exception(e)
                logger.error(
                    "fetch failed | platform=%s external_id=%s handle=%s | %s",
                    candidate.platform, candidate.external_id, candidate.handle, summary,
                )
                repo.mark_queue_item(item["queue_id"], status="failed", error=summary)
                repo.commit()
                raise

    items = dequeue_batch()
    fetch_one.expand(item=items)


dag_ingestion()
