"""
FakeRepository implements just enough of Repository's interface to test
enrichment/identity.py and enrichment/features.py without a real database
connection (unavailable in this sandbox — no network access to install
sqlalchemy/psycopg2 or run Postgres). This is a stand-in for what would
normally be an integration test against a real Postgres instance; run
tests/enrichment/ against a real DB before trusting this in production.
"""
from datetime import date, datetime

from pipeline.enrichment.features import build_creator_signals, compute_posting_consistency_for_creator
from pipeline.enrichment.identity import enrich_from_failed_fetch, enrich_from_successful_fetch
from pipeline.parsing.base import ParsedProfile, ParsedVideo


class FakeRepository:
    def __init__(self):
        self.creators = {}  # (platform, external_id) -> creator_id
        self.profile_snapshots = {}  # creator_id -> list[dict], newest first
        self.videos = {}  # creator_id -> list[dict] with is_ad/posted_at/metrics
        self._next_id = 1

    def upsert_creator(self, *, platform, external_id):
        key = (platform, external_id)
        if key not in self.creators:
            self.creators[key] = f"creator-{self._next_id}"
            self._next_id += 1
        return self.creators[key]

    def add_profile_snapshot(self, creator_id, **fields):
        self.profile_snapshots.setdefault(creator_id, []).insert(0, {"snapshot_date": date.today(), **fields})

    def upsert_video(self, video_id, creator_id, **fields):
        self.videos.setdefault(creator_id, []).append({"video_id": video_id, **fields, "_snapshot": {}})

    def add_video_snapshot(self, video_id, **fields):
        for creator_videos in self.videos.values():
            for v in creator_videos:
                if v["video_id"] == video_id:
                    v["_snapshot"] = fields

    def latest_snapshots_for_scoring(self, creator_id, n=2):
        class Snap:
            def __init__(self, d):
                self.__dict__.update(d)

        return [Snap(s) for s in self.profile_snapshots.get(creator_id, [])[:n]]

    def recent_video_engagement(self, creator_id, limit=30):
        out = []
        for v in self.videos.get(creator_id, [])[:limit]:
            snap = v["_snapshot"]
            out.append(
                {
                    "is_ad": v["is_ad"],
                    "posted_at": v["posted_at"],
                    "play_count": snap.get("play_count"),
                    "digg_count": snap.get("digg_count"),
                    "comment_count": snap.get("comment_count"),
                    "share_count": snap.get("share_count"),
                }
            )
        return out


def test_enrich_from_successful_fetch_creates_snapshot_and_videos():
    repo = FakeRepository()
    profile = ParsedProfile(
        platform="tiktok", external_id="123", handle="skincarefan",
        follower_count=100_000, following_count=50, lifetime_likes=5_000_000,
        video_count=200, bio="skincare tips", bio_link=None,
        is_verified=False, is_private=False, is_commerce_account=False,
    )
    videos = [
        ParsedVideo(
            video_id="v1", creator_external_id="123", posted_at=datetime(2026, 8, 1),
            is_ad=False, text_language="en", web_video_url="http://x",
            play_count=10000, digg_count=500, comment_count=20, share_count=10,
            collect_count=5,
        )
    ]

    creator_id = enrich_from_successful_fetch(repo, platform="tiktok", external_id="123", profile=profile, videos=videos)

    assert creator_id == "creator-1"
    assert repo.profile_snapshots["creator-1"][0]["scrape_status"] == "success"
    assert repo.profile_snapshots["creator-1"][0]["follower_count"] == 100_000
    assert len(repo.videos["creator-1"]) == 1


def test_enrich_from_failed_fetch_still_writes_a_snapshot():
    repo = FakeRepository()
    creator_id = enrich_from_failed_fetch(
        repo, platform="tiktok", external_id="999", handle="ghost", reason="no_data"
    )
    snap = repo.profile_snapshots[creator_id][0]
    assert snap["scrape_status"] == "no_data"
    assert snap["follower_count"] is None


def test_build_creator_signals_end_to_end_via_enrichment():
    """Exercises the full enrichment -> features seam: enrich a creator,
    then build_creator_signals should read that exact data back out and
    compute a sane engagement rate from it.
    """
    repo = FakeRepository()
    profile = ParsedProfile(
        platform="tiktok", external_id="123", handle="skincarefan",
        follower_count=100_000, following_count=50, lifetime_likes=8_000_000,
        video_count=200, bio="daily skincare routines and tips", bio_link=None,
        is_verified=True, is_private=False, is_commerce_account=False,
    )
    creator_id = enrich_from_successful_fetch(repo, platform="tiktok", external_id="123", profile=profile, videos=[])

    signals = build_creator_signals(repo, creator_id)

    assert signals.scrape_status == "success"
    assert signals.account_type == "creator"
    # 8,000,000 / 200 videos / 100,000 followers = 0.4 engagement rate
    assert signals.engagement_rate == 0.4
    assert signals.growth_rate is None  # only one snapshot exists


def test_build_creator_signals_respects_failed_scrape_status():
    repo = FakeRepository()
    creator_id = enrich_from_failed_fetch(
        repo, platform="tiktok", external_id="1", handle="x", reason="fetch_failed"
    )
    signals = build_creator_signals(repo, creator_id)
    assert signals.scrape_status == "fetch_failed"
    assert signals.engagement_rate is None
