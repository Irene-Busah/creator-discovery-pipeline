"""
Repository layer — the ONLY module that issues SQL/ORM queries.

Why this boundary exists: every other module (discovery, parsing, scoring...)
depends on `Repository`, never on `models.py` or a raw session directly. That
means:
  - swapping Postgres for something else touches one file
  - upsert/idempotency logic lives in exactly one place, so it can't drift
    between "how ingestion writes a creator" and "how enrichment writes one"
  - unit tests can mock `Repository` entirely without a real database
"""
from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from pipeline.db.models import (
    Creator,
    CreatorScore,
    DiscoveryEvent,
    IngestionQueueItem,
    OutreachQueueItem,
    ParseError,
    ProfileSnapshot,
    RawPayload,
    Video,
    VideoSnapshot,
)


def _as_uuid(value) -> uuid.UUID:
    """Airflow tasks pass creator_id/queue_id across task boundaries via
    XCom, which serializes to JSON — so a UUID generated in one task
    arrives as a plain string in the next. Every method that accepts an ID
    from outside this process runs it through this first, so callers never
    need to remember to convert.
    """
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


class Repository:
    def __init__(self, session: Session):
        self._session = session

    # -------------------- Discovery / queueing --------------------

    def record_discovery_event(
        self, *, platform: str, external_id: str, source: str, query: str
    ) -> None:
        self._session.add(
            DiscoveryEvent(
                platform=platform, external_id=external_id, source=source, query=query
            )
        )

    def enqueue_for_ingestion(
        self, *, platform: str, external_id: str, handle: str, reason: str, priority: int = 5
    ) -> None:
        """Idempotent enqueue: ON CONFLICT DO NOTHING on (platform, external_id, status).

        This is what prevents the same candidate from being enqueued twice by
        an overlapping discovery run — the natural key does the deduplication,
        not application-level "have I seen this before" logic.
        """
        stmt = (
            pg_insert(IngestionQueueItem)
            .values(
                platform=platform,
                external_id=external_id,
                handle=handle,
                reason=reason,
                priority=priority,
                status="pending",
            )
            .on_conflict_do_nothing(
                index_elements=["platform", "external_id", "status"]
            )
        )
        self._session.execute(stmt)

    def dequeue_batch(self, *, limit: int) -> list[IngestionQueueItem]:
        """Pull the highest-priority pending items and mark them in_progress.

        Marking in_progress at dequeue time (not after fetch succeeds) is what
        stops two concurrent workers from double-processing the same item —
        the UPDATE ... RETURNING is effectively a lightweight lock.
        """
        subq = (
            select(IngestionQueueItem.queue_id)
            .where(IngestionQueueItem.status == "pending")
            .order_by(IngestionQueueItem.priority.asc(), IngestionQueueItem.enqueued_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        ids = self._session.execute(subq).scalars().all()
        if not ids:
            return []

        self._session.execute(
            update(IngestionQueueItem)
            .where(IngestionQueueItem.queue_id.in_(ids))
            .values(status="in_progress")
        )
        return (
            self._session.execute(
                select(IngestionQueueItem).where(IngestionQueueItem.queue_id.in_(ids))
            )
            .scalars()
            .all()
        )

    def mark_queue_item(self, queue_id, *, status: str, error: str | None = None) -> None:
        values = {"status": status}
        if error is not None:
            values["error"] = error
        self._session.execute(
            update(IngestionQueueItem)
            .where(IngestionQueueItem.queue_id == _as_uuid(queue_id))
            .values(**values)
        )

    # -------------------- Raw landing --------------------

    def save_raw_payload(
        self, *, platform: str, source_type: str, external_id: str, payload: dict
    ) -> uuid.UUID:
        raw = RawPayload(
            platform=platform,
            source_type=source_type,
            external_id=external_id,
            payload=payload,
        )
        self._session.add(raw)
        self._session.flush()  # populate raw.raw_id without committing
        return raw.raw_id

    def mark_raw_payload(self, raw_id: uuid.UUID, *, status: str) -> None:
        self._session.execute(
            update(RawPayload).where(RawPayload.raw_id == raw_id).values(parse_status=status)
        )

    def record_parse_error(self, raw_id: uuid.UUID, error: str) -> None:
        self._session.add(ParseError(raw_id=raw_id, error=error))

    def pending_raw_payloads(self, *, limit: int = 100) -> list[RawPayload]:
        return (
            self._session.execute(
                select(RawPayload)
                .where(RawPayload.parse_status == "pending")
                .limit(limit)
            )
            .scalars()
            .all()
        )

    # -------------------- Identity --------------------

    def upsert_creator(self, *, platform: str, external_id: str) -> uuid.UUID:
        """Upsert on (platform, external_id) — never on handle. Handles change;
        this key doesn't. Returns the creator_id whether newly created or existing.
        """
        stmt = (
            pg_insert(Creator)
            .values(platform=platform, external_id=external_id, status="active")
            .on_conflict_do_update(
                index_elements=["platform", "external_id"],
                set_={"last_seen_at": datetime.utcnow(), "status": "active"},
            )
            .returning(Creator.creator_id)
        )
        return self._session.execute(stmt).scalar_one()

    # -------------------- Snapshots --------------------

    def add_profile_snapshot(self, creator_id, **fields) -> None:
        self._session.add(
            ProfileSnapshot(creator_id=_as_uuid(creator_id), snapshot_date=date.today(), **fields)
        )

    def upsert_video(self, video_id: str, creator_id, **fields) -> None:
        stmt = (
            pg_insert(Video)
            .values(video_id=video_id, creator_id=_as_uuid(creator_id), **fields)
            .on_conflict_do_nothing(index_elements=["video_id"])
        )
        self._session.execute(stmt)

    def add_video_snapshot(self, video_id: str, **fields) -> None:
        self._session.add(VideoSnapshot(video_id=video_id, snapshot_date=date.today(), **fields))

    # -------------------- Refresh scheduling --------------------

    def creators_due_for_refresh(self, refresh_interval_days: dict) -> list[dict]:
        """Active creators whose most recent profile snapshot is older than
        their current tier's refresh interval (or who have never been
        scored, using the 'needs_review' interval as the default).

        Implemented as one query per active creator rather than a single
        join, deliberately: at 10K creators/month this is well within
        Postgres's comfort zone and the logic reads top-to-bottom without a
        multi-way join to defend live. This N+1 pattern is exactly the kind
        of thing that would need to become a single query at 1M/month scale
        (see README trade-offs) — named here as a known limitation, not
        hidden.
        """
        creators = (
            self._session.execute(select(Creator).where(Creator.status == "active"))
            .scalars()
            .all()
        )
        today = date.today()
        due: list[dict] = []

        for creator in creators:
            latest_score = (
                self._session.execute(
                    select(CreatorScore)
                    .where(CreatorScore.creator_id == creator.creator_id)
                    .order_by(CreatorScore.computed_at.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            tier = latest_score.tier if latest_score else "needs_review"
            interval = refresh_interval_days.get(tier, 30)

            latest_snapshot = (
                self._session.execute(
                    select(ProfileSnapshot)
                    .where(ProfileSnapshot.creator_id == creator.creator_id)
                    .order_by(ProfileSnapshot.snapshot_date.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )

            is_due = latest_snapshot is None or (today - latest_snapshot.snapshot_date).days >= interval
            if is_due:
                due.append(
                    {
                        "platform": creator.platform,
                        "external_id": creator.external_id,
                        # Fall back to external_id if we've genuinely never
                        # captured a handle — Apify will fail that fetch
                        # cleanly (no_data) rather than the pipeline crashing.
                        "handle": latest_snapshot.handle if latest_snapshot else creator.external_id,
                    }
                )

        return due

    # -------------------- Scoring / handoff --------------------

    def active_creator_ids(self) -> list[str]:
        rows = self._session.execute(select(Creator.creator_id).where(Creator.status == "active"))
        return [str(row[0]) for row in rows]

    def save_score(self, creator_id, *, score_version: str, **fields) -> None:
        self._session.add(
            CreatorScore(creator_id=_as_uuid(creator_id), score_version=score_version, **fields)
        )

    def latest_snapshots_for_scoring(self, creator_id, n: int = 2) -> list[ProfileSnapshot]:
        return (
            self._session.execute(
                select(ProfileSnapshot)
                .where(ProfileSnapshot.creator_id == _as_uuid(creator_id))
                .order_by(ProfileSnapshot.snapshot_date.desc())
                .limit(n)
            )
            .scalars()
            .all()
        )

    def recent_video_engagement(self, creator_id, limit: int = 30) -> list[dict]:
        """Each creator's most recent videos with their latest metric
        snapshot. One query for the video list, then one snapshot lookup
        per video (same N+1 trade-off as creators_due_for_refresh, same
        justification: fine at this scale, first thing to optimize at 1M/mo).
        """
        videos = (
            self._session.execute(
                select(Video)
                .where(Video.creator_id == _as_uuid(creator_id))
                .order_by(Video.posted_at.desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )

        results: list[dict] = []
        for video in videos:
            latest_snapshot = (
                self._session.execute(
                    select(VideoSnapshot)
                    .where(VideoSnapshot.video_id == video.video_id)
                    .order_by(VideoSnapshot.snapshot_date.desc())
                    .limit(1)
                )
                .scalars()
                .first()
            )
            if latest_snapshot is None:
                continue
            results.append(
                {
                    "is_ad": video.is_ad,
                    "posted_at": video.posted_at,
                    "play_count": latest_snapshot.play_count,
                    "digg_count": latest_snapshot.digg_count,
                    "comment_count": latest_snapshot.comment_count,
                    "share_count": latest_snapshot.share_count,
                }
            )
        return results

    def creators_ready_for_outreach(self) -> list[dict]:
        """Creators whose most recent score is tier A/B and who are not
        already in outreach_queue. The A/B filter here is a query
        convenience — the actual business rule of "which tiers qualify for
        outreach" is owned by pipeline/handoff/outreach.py, not this query,
        so that rule can change without touching the repository.
        """
        latest_per_creator = (
            select(
                CreatorScore.creator_id,
                func.max(CreatorScore.computed_at).label("max_computed_at"),
            )
            .group_by(CreatorScore.creator_id)
            .subquery()
        )

        rows = self._session.execute(
            select(CreatorScore.creator_id, CreatorScore.tier)
            .join(
                latest_per_creator,
                (CreatorScore.creator_id == latest_per_creator.c.creator_id)
                & (CreatorScore.computed_at == latest_per_creator.c.max_computed_at),
            )
            .where(CreatorScore.tier.in_(["A", "B"]))
            .where(~CreatorScore.creator_id.in_(select(OutreachQueueItem.creator_id)))
        )
        return [{"creator_id": str(creator_id), "tier": tier} for creator_id, tier in rows]

    def push_to_outreach(self, creator_id, *, tier: str) -> None:
        stmt = (
            pg_insert(OutreachQueueItem)
            .values(creator_id=_as_uuid(creator_id), tier=tier, status="pending")
            .on_conflict_do_nothing(index_elements=["creator_id"])
        )
        self._session.execute(stmt)

    # -------------------- Monitoring --------------------

    def count_discovery_events_since(self, cutoff: datetime) -> int:
        return self._session.execute(
            select(func.count()).select_from(DiscoveryEvent).where(DiscoveryEvent.discovered_at >= cutoff)
        ).scalar_one()

    def count_raw_payloads_since(self, cutoff: datetime) -> int:
        return self._session.execute(
            select(func.count()).select_from(RawPayload).where(RawPayload.fetched_at >= cutoff)
        ).scalar_one()

    def count_ingestion_queue_by_status(self) -> dict[str, int]:
        rows = self._session.execute(
            select(IngestionQueueItem.status, func.count()).group_by(IngestionQueueItem.status)
        )
        return {status: count for status, count in rows}

    def count_quota_exhaustion_errors(self) -> int:
        """Failed ingestion_queue items whose error looks like a billing/
        rate-limit response (402 Payment Required, 429 Too Many Requests)
        rather than a genuine source or network failure. Deliberately a
        separate signal from count_ingestion_queue_by_status's generic
        'failed' count: running out of Apify budget and the source being
        broken look identical as a raw failure-rate number, but need
        completely different responses — one needs a bigger budget or
        backoff, the other needs someone paged. Simple substring match on
        the stored error text rather than a separate error-code column;
        good enough for a monitoring signal, not meant to be a full error
        taxonomy.
        """
        return self._session.execute(
            select(func.count())
            .select_from(IngestionQueueItem)
            .where(IngestionQueueItem.status == "failed")
            .where(
                IngestionQueueItem.error.ilike("%402%")
                | IngestionQueueItem.error.ilike("%429%")
                | IngestionQueueItem.error.ilike("%payment required%")
                | IngestionQueueItem.error.ilike("%rate limit%")
            )
        ).scalar_one()

    def count_raw_payloads_by_parse_status(self) -> dict[str, int]:
        rows = self._session.execute(
            select(RawPayload.parse_status, func.count()).group_by(RawPayload.parse_status)
        )
        return {status: count for status, count in rows}

    def latest_snapshot_date_by_tier(self) -> dict[str, Optional[date]]:
        """Most recent profile_snapshot date reachable via each tier's
        creators — used by the freshness-by-tier monitoring check to catch
        'tier A creators haven't been refreshed on SLA.'
        """
        rows = self._session.execute(
            select(CreatorScore.tier, func.max(ProfileSnapshot.snapshot_date))
            .join(ProfileSnapshot, ProfileSnapshot.creator_id == CreatorScore.creator_id)
            .group_by(CreatorScore.tier)
        )
        return {tier: max_date for tier, max_date in rows}

    def commit(self) -> None:
        self._session.commit()
