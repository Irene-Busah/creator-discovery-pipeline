"""
Parses the hashtag/search-result payload shape from Apify's TikTok Scraper —
one item per video, with a nested `authorMeta` object (see the "Scraped
TikTok search queries" example in Apify's docs). Ad posts (`isAd`) are
parsed but flagged, not dropped — scoring decides to exclude them from the
engagement calc, parsing's job is just faithful extraction.
"""
from __future__ import annotations

from typing import Optional

from pipeline.parsing.base import ParsedProfile, ParsedVideo, Parser, ParseError


class TikTokHashtagParser(Parser):
    def parse_profile(self, raw: dict) -> ParsedProfile:
        author = raw.get("authorMeta")
        if not isinstance(author, dict) or not author.get("id"):
            raise ParseError("Missing or malformed authorMeta in hashtag payload")

        commerce = author.get("commerceUserInfo") or {}

        return ParsedProfile(
            platform="tiktok",
            external_id=str(author["id"]),
            handle=author.get("name", ""),
            follower_count=_to_int(author.get("fans")),
            following_count=_to_int(author.get("following")),
            lifetime_likes=_to_int(author.get("heart")),
            video_count=_to_int(author.get("video")),
            bio=author.get("signature"),
            bio_link=author.get("bioLink"),
            is_verified=bool(author.get("verified", False)),
            is_private=bool(author.get("privateAccount", False)),
            is_commerce_account=bool(
                commerce.get("commerceUser", False) or author.get("ttSeller", False)
            ),
        )

    def parse_video(self, raw: dict) -> Optional[ParsedVideo]:
        video_id = raw.get("id")
        if not video_id:
            return None

        return ParsedVideo(
            video_id=str(video_id),
            creator_external_id=str((raw.get("authorMeta") or {}).get("id", "")),
            posted_at=raw.get("createTimeISO"),
            is_ad=bool(raw.get("isAd", False)),
            text_language=raw.get("textLanguage"),
            web_video_url=raw.get("webVideoUrl"),
            play_count=_to_int(raw.get("playCount")),
            digg_count=_to_int(raw.get("diggCount")),
            comment_count=_to_int(raw.get("commentCount")),
            share_count=_to_int(raw.get("shareCount")),
            collect_count=_to_int(raw.get("collectCount")),
        )


def _to_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
