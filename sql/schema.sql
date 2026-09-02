-- creator-discovery-pipeline schema
--
-- Design principles (defend these live):
--   1. Identity vs. history are separate tables. `creators` never changes shape;
--      `*_snapshots` are append-only and grow forever. Overwriting a follower
--      count would destroy the growth-rate signal and the ability to detect
--      suspicious jumps.
--   2. Every upsert key is a stable platform ID, never a @handle (handles change).
--   3. Raw payloads are landed before parsing, and parse failures never block
--      the pipeline — they go to parse_errors and get retried independently.
--   4. Scores are versioned so re-tuning thresholds doesn't destroy history.

-- ============================================================
-- 1. Identity
-- ============================================================

CREATE TABLE creators (
    creator_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform            TEXT NOT NULL,              -- 'tiktok' | 'instagram'
    external_id         TEXT NOT NULL,               -- platform's stable numeric/string ID
    first_discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    status               TEXT NOT NULL DEFAULT 'active',  -- active | inactive | needs_review
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (platform, external_id)
);

CREATE INDEX idx_creators_status ON creators (status);

-- ============================================================
-- 2. Discovery provenance (separate from video data — this is
--    "which query found this candidate", not a content record)
-- ============================================================

CREATE TABLE discovery_events (
    discovery_event_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id           UUID REFERENCES creators(creator_id),
    platform             TEXT NOT NULL,
    external_id          TEXT NOT NULL,               -- candidate id before creator_id may exist
    source                TEXT NOT NULL,               -- 'tiktok_hashtag' | 'tiktok_search' | ...
    query                 TEXT NOT NULL,               -- the hashtag/keyword used
    discovered_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_discovery_events_creator ON discovery_events (creator_id);
CREATE INDEX idx_discovery_events_discovered_at ON discovery_events (discovered_at);

-- ============================================================
-- 3. Candidate / refresh queue — the boundary between discovery
--    and ingestion. Plain Postgres table, not an external broker:
--    queryable live, adequate at 10K creators/month.
-- ============================================================

CREATE TABLE ingestion_queue (
    queue_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform      TEXT NOT NULL,
    external_id   TEXT NOT NULL,
    handle        TEXT NOT NULL,             -- last-known @handle; Apify's profile
                                              -- actor fetches by handle, not by the
                                              -- stable external_id, so this must
                                              -- travel with the queue item
    reason        TEXT NOT NULL,             -- 'new_candidate' | 'scheduled_refresh'
    priority      SMALLINT NOT NULL DEFAULT 5,  -- lower = higher priority (tier A refreshes = 1)
    status        TEXT NOT NULL DEFAULT 'pending',  -- pending | in_progress | done | failed
    error         TEXT,                       -- last failure message, e.g. an HTTP status
                                                -- and body from a fetch failure. NULL for
                                                -- items that never failed. Lets a query
                                                -- distinguish quota exhaustion (402/429) from
                                                -- other failure modes without digging through logs.
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      SMALLINT NOT NULL DEFAULT 0,

    UNIQUE (platform, external_id, status)   -- prevents duplicate pending entries for the same creator
);

CREATE INDEX idx_ingestion_queue_status_priority ON ingestion_queue (status, priority);

-- ============================================================
-- 4. Raw landing zone — nothing is parsed before it's landed here.
--    This is what makes "Apify changed the payload shape" a parser
--    bug, not data loss.
-- ============================================================

CREATE TABLE raw_payloads (
    raw_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform       TEXT NOT NULL,
    source_type    TEXT NOT NULL,             -- 'hashtag' | 'profile' | 'video_url'
    external_id    TEXT NOT NULL,             -- creator or video id this payload is about
    payload        JSONB NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    parse_status   TEXT NOT NULL DEFAULT 'pending'  -- pending | parsed | failed
);

CREATE INDEX idx_raw_payloads_parse_status ON raw_payloads (parse_status);

CREATE TABLE parse_errors (
    parse_error_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_id          UUID REFERENCES raw_payloads(raw_id),
    error           TEXT NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- 5. Normalized history (append-only snapshots)
-- ============================================================

CREATE TABLE profile_snapshots (
    snapshot_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id        UUID NOT NULL REFERENCES creators(creator_id),
    snapshot_date      DATE NOT NULL,
    handle              TEXT NOT NULL,           -- captured per-snapshot; handles change over time
    follower_count      INTEGER,
    following_count     INTEGER,
    lifetime_likes      BIGINT,                  -- 'heart' in Apify output — used for lifetime engagement proxy
    video_count         INTEGER,
    bio                  TEXT,
    bio_link             TEXT,
    is_verified          BOOLEAN,
    is_private            BOOLEAN,
    is_commerce_account   BOOLEAN,                -- platform-native "sells things" flag — better than bio keyword guessing
    scrape_status         TEXT NOT NULL,           -- success | no_data | fetch_failed
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_profile_snapshots_creator_date ON profile_snapshots (creator_id, snapshot_date DESC);

CREATE TABLE videos (
    video_id       TEXT PRIMARY KEY,           -- platform's video id — stable, no synthetic key needed
    creator_id      UUID NOT NULL REFERENCES creators(creator_id),
    posted_at        TIMESTAMPTZ,
    is_ad             BOOLEAN NOT NULL DEFAULT false,   -- excluded from organic engagement calc
    text_language     TEXT,
    web_video_url      TEXT,
    first_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_videos_creator ON videos (creator_id);

CREATE TABLE video_snapshots (
    snapshot_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    video_id         TEXT NOT NULL REFERENCES videos(video_id),
    snapshot_date      DATE NOT NULL,
    play_count          INTEGER,
    digg_count           INTEGER,          -- likes
    comment_count         INTEGER,
    share_count            INTEGER,
    collect_count            INTEGER,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_video_snapshots_video_date ON video_snapshots (video_id, snapshot_date DESC);

-- ============================================================
-- 6. Scoring (versioned, so re-tuning doesn't erase history)
-- ============================================================

CREATE TABLE creator_scores (
    score_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id          UUID NOT NULL REFERENCES creators(creator_id),
    score_version         TEXT NOT NULL,          -- e.g. 'v1' — bump when scoring logic changes
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    engagement_rate           NUMERIC,
    growth_rate                 NUMERIC,             -- null until 2+ snapshots exist
    posting_consistency           NUMERIC,             -- videos/week, trailing 30d
    account_type                    TEXT,               -- creator | brand | reseller | unknown
    tier                              TEXT NOT NULL,      -- A | B | C | Reject | needs_review
    flags                              TEXT[]              -- e.g. {'suspicious_growth','insufficient_data'}
);

CREATE INDEX idx_creator_scores_creator_version ON creator_scores (creator_id, score_version, computed_at DESC);
CREATE INDEX idx_creator_scores_tier ON creator_scores (tier);

-- ============================================================
-- 7. Handoff
-- ============================================================

CREATE TABLE outreach_queue (
    outreach_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id        UUID NOT NULL REFERENCES creators(creator_id),
    tier                TEXT NOT NULL,
    pushed_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    status                  TEXT NOT NULL DEFAULT 'pending',  -- pending | contacted | responded | skipped

    UNIQUE (creator_id)   -- a creator enters outreach once; status tracks progress from there
);

-- ============================================================
-- Sample live queries to rehearse
-- ============================================================

-- "Show me today's A-tier candidates ready for outreach"
-- SELECT c.creator_id, ps.handle, cs.engagement_rate, cs.tier
-- FROM creator_scores cs
-- JOIN creators c ON c.creator_id = cs.creator_id
-- JOIN LATERAL (
--     SELECT handle FROM profile_snapshots
--     WHERE creator_id = c.creator_id ORDER BY snapshot_date DESC LIMIT 1
-- ) ps ON true
-- WHERE cs.tier = 'A' AND cs.score_version = 'v1'
-- AND c.creator_id NOT IN (SELECT creator_id FROM outreach_queue)
-- ORDER BY cs.engagement_rate DESC;

-- "Did discovery run today?"
-- SELECT source, count(*) FROM discovery_events
-- WHERE discovered_at >= current_date GROUP BY source;
