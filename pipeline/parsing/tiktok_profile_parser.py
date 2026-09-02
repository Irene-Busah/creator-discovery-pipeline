"""
Parses the output of pipeline/ingestion/fetcher.py — a profile scrape,
shaped as {"profile_item": <video item>, "video_items": [<video item>, ...]}.

Key observation from the real Apify samples: a profile-scrape *item* has the
exact same shape as a hashtag-search *item* (nested `authorMeta`, per-video
metrics) — the difference between "hashtag result" and "profile result" is
which Apify input produced it and how many items come back per creator, not
the shape of each item. So this parser does NOT duplicate field-extraction
logic; it composes TikTokHashtagParser for per-item work and adds the
multi-video handling a profile fetch needs that a single hashtag video
result doesn't.
"""
from __future__ import annotations

from typing import Optional

from pipeline.parsing.base import ParsedProfile, ParsedVideo, Parser, ParseError
from pipeline.parsing.tiktok_hashtag_parser import TikTokHashtagParser


class TikTokProfileParser(Parser):
    def __init__(self):
        # Composition, not inheritance — the item-level parsing rules belong
        # to one implementation; if the item shape ever diverges between
        # hashtag and profile scrapes, this composition point is where that
        # split would happen, without duplicating the field-mapping logic.
        self._item_parser = TikTokHashtagParser()

    def parse_profile(self, raw: dict) -> ParsedProfile:
        profile_item = raw.get("profile_item")
        if not profile_item:
            raise ParseError("Profile fetch payload missing 'profile_item'")
        return self._item_parser.parse_profile(profile_item)

    def parse_video(self, raw: dict) -> Optional[ParsedVideo]:
        """Single-video accessor, required by the Parser interface. A profile
        fetch normally has many videos — use parse_videos() for the real
        multi-video case. This returns the first video only, mainly useful
        for tests/debugging a single fetch result.
        """
        video_items = raw.get("video_items") or []
        if not video_items:
            return None
        return self._item_parser.parse_video(video_items[0])

    def parse_videos(self, raw: dict) -> list[ParsedVideo]:
        """The real entry point for a profile fetch: every video in one
        fetch, each parsed independently. One bad video item raising
        ParseError does not stop the rest — the caller (enrichment) can
        choose to skip-and-log a single malformed item rather than fail
        the whole profile fetch over one bad record.
        """
        video_items = raw.get("video_items") or []
        videos: list[ParsedVideo] = []
        for item in video_items:
            parsed = self._item_parser.parse_video(item)
            if parsed is not None:
                videos.append(parsed)
        return videos
