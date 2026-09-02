"""
SQLAlchemy ORM models mirroring sql/schema.sql.

schema.sql is the source of truth for the live-SQL walkthrough (it's what
you'd paste into psql). These models exist so the Python layer has typed,
testable access to the same tables — kept in sync by hand for this project's
scale; a larger system would generate one from the other via Alembic.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    BigInteger,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def _uuid_col():
    return Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


class Creator(Base):
    __tablename__ = "creators"

    creator_id = _uuid_col()
    platform = Column(String, nullable=False)
    external_id = Column(String, nullable=False)
    first_discovered_at = Column(DateTime(timezone=True), server_default=func.now())
    last_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String, nullable=False, default="active")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint("platform", "external_id"),)


class DiscoveryEvent(Base):
    __tablename__ = "discovery_events"

    discovery_event_id = _uuid_col()
    creator_id = Column(UUID(as_uuid=True), ForeignKey("creators.creator_id"), nullable=True)
    platform = Column(String, nullable=False)
    external_id = Column(String, nullable=False)
    source = Column(String, nullable=False)
    query = Column(String, nullable=False)
    discovered_at = Column(DateTime(timezone=True), server_default=func.now())


class IngestionQueueItem(Base):
    __tablename__ = "ingestion_queue"

    queue_id = _uuid_col()
    platform = Column(String, nullable=False)
    external_id = Column(String, nullable=False)
    handle = Column(String, nullable=False)  # Apify fetches by handle, not external_id
    reason = Column(String, nullable=False)  # new_candidate | scheduled_refresh
    priority = Column(SmallInteger, nullable=False, default=5)
    status = Column(String, nullable=False, default="pending")
    error = Column(Text)  # last failure message, e.g. HTTP status + body
    enqueued_at = Column(DateTime(timezone=True), server_default=func.now())
    attempts = Column(SmallInteger, nullable=False, default=0)

    __table_args__ = (UniqueConstraint("platform", "external_id", "status"),)


class RawPayload(Base):
    __tablename__ = "raw_payloads"

    raw_id = _uuid_col()
    platform = Column(String, nullable=False)
    source_type = Column(String, nullable=False)  # hashtag | profile | video_url
    external_id = Column(String, nullable=False)
    payload = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())
    parse_status = Column(String, nullable=False, default="pending")


class ParseError(Base):
    __tablename__ = "parse_errors"

    parse_error_id = _uuid_col()
    raw_id = Column(UUID(as_uuid=True), ForeignKey("raw_payloads.raw_id"))
    error = Column(Text, nullable=False)
    occurred_at = Column(DateTime(timezone=True), server_default=func.now())


class ProfileSnapshot(Base):
    __tablename__ = "profile_snapshots"

    snapshot_id = _uuid_col()
    creator_id = Column(UUID(as_uuid=True), ForeignKey("creators.creator_id"), nullable=False)
    snapshot_date = Column(Date, nullable=False)
    handle = Column(String, nullable=False)
    follower_count = Column(Integer)
    following_count = Column(Integer)
    lifetime_likes = Column(BigInteger)
    video_count = Column(Integer)
    bio = Column(Text)
    bio_link = Column(Text)
    is_verified = Column(Boolean)
    is_private = Column(Boolean)
    is_commerce_account = Column(Boolean)
    scrape_status = Column(String, nullable=False)  # success | no_data | fetch_failed
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Video(Base):
    __tablename__ = "videos"

    video_id = Column(String, primary_key=True)
    creator_id = Column(UUID(as_uuid=True), ForeignKey("creators.creator_id"), nullable=False)
    posted_at = Column(DateTime(timezone=True))
    is_ad = Column(Boolean, nullable=False, default=False)
    text_language = Column(String)
    web_video_url = Column(Text)
    first_seen_at = Column(DateTime(timezone=True), server_default=func.now())


class VideoSnapshot(Base):
    __tablename__ = "video_snapshots"

    snapshot_id = _uuid_col()
    video_id = Column(String, ForeignKey("videos.video_id"), nullable=False)
    snapshot_date = Column(Date, nullable=False)
    play_count = Column(Integer)
    digg_count = Column(Integer)
    comment_count = Column(Integer)
    share_count = Column(Integer)
    collect_count = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class CreatorScore(Base):
    __tablename__ = "creator_scores"

    score_id = _uuid_col()
    creator_id = Column(UUID(as_uuid=True), ForeignKey("creators.creator_id"), nullable=False)
    score_version = Column(String, nullable=False)
    computed_at = Column(DateTime(timezone=True), server_default=func.now())
    engagement_rate = Column(Numeric)
    growth_rate = Column(Numeric)
    posting_consistency = Column(Numeric)
    account_type = Column(String)
    tier = Column(String, nullable=False)
    flags = Column(ARRAY(String))


class OutreachQueueItem(Base):
    __tablename__ = "outreach_queue"

    outreach_id = _uuid_col()
    creator_id = Column(UUID(as_uuid=True), ForeignKey("creators.creator_id"), nullable=False)
    tier = Column(String, nullable=False)
    pushed_at = Column(DateTime(timezone=True), server_default=func.now())
    status = Column(String, nullable=False, default="pending")

    __table_args__ = (UniqueConstraint("creator_id"),)
