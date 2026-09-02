"""
Parser interface.

One parser per raw payload shape, not per platform — that's the lesson
from the Apify samples: a hashtag-search result, a profile-scrape result,
and a video-URL result are three different shapes even from the same actor.
Each parser's only job is raw dict -> typed record. No I/O, no DB writes —
that keeps them trivially unit-testable against fixture JSON.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ParsedProfile:
    platform: str
    external_id: str
    handle: str
    follower_count: Optional[int]
    following_count: Optional[int]
    lifetime_likes: Optional[int]
    video_count: Optional[int]
    bio: Optional[str]
    bio_link: Optional[str]
    is_verified: bool
    is_private: bool
    is_commerce_account: bool


@dataclass(frozen=True)
class ParsedVideo:
    video_id: str
    creator_external_id: str
    posted_at: Optional[str]     # ISO string; converted to datetime in enrichment
    is_ad: bool
    text_language: Optional[str]
    web_video_url: Optional[str]
    play_count: Optional[int]
    digg_count: Optional[int]
    comment_count: Optional[int]
    share_count: Optional[int]
    collect_count: Optional[int]


class ParseError(Exception):
    """Raised when a payload doesn't match the shape this parser expects.
    Caught by the enrichment DAG task, which writes it to parse_errors and
    moves on — one bad record never blocks the batch.
    """


class Parser(ABC):
    @abstractmethod
    def parse_profile(self, raw: dict) -> ParsedProfile:
        ...

    @abstractmethod
    def parse_video(self, raw: dict) -> Optional[ParsedVideo]:
        """Return None if this payload has no video to extract (e.g. a
        profile-only fetch) — that's a valid outcome, not a ParseError.
        """

    def parse_videos(self, raw: dict) -> list[ParsedVideo]:
        """Extract every video present in this payload. Default
        implementation covers single-video sources (hashtag search, one
        video per item) by wrapping parse_video(). A source that returns
        many videos per fetch (a profile scrape, which pulls a creator's
        recent videos in one call) overrides this instead of forcing a
        one-video-at-a-time caller loop on the enrichment side.
        """
        video = self.parse_video(raw)
        return [video] if video else []
