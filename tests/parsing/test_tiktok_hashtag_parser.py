"""
Fixture below is a trimmed version of a real Apify TikTok Scraper hashtag-
search response (captured during discovery-source evaluation). Using real
shape, not an invented one, is what catches bugs like the commerceUserInfo
nesting or the fans/heart naming before they hit production.
"""
import pytest

from pipeline.parsing.base import ParseError
from pipeline.parsing.tiktok_hashtag_parser import TikTokHashtagParser

RESELLER_VIDEO_PAYLOAD = {
    "id": "7535448384170331414",
    "createTimeISO": "2025-08-06T12:28:22.000Z",
    "isAd": False,
    "textLanguage": "un",
    "webVideoUrl": "https://www.tiktok.com/@shaiie_foeva/video/7535448384170331414",
    "authorMeta": {
        "id": "7002169437214213125",
        "name": "shaiie_foeva",
        "verified": False,
        "signature": "SHOP THE E-BOOK OUT NOW",
        "bioLink": "https://fkrtrz-v0.myshopify.com/collections/all",
        "commerceUserInfo": {"commerceUser": False},
        "privateAccount": False,
        "ttSeller": False,
        "following": 0,
        "fans": 2200000,
        "heart": 126000000,
        "video": 1036,
    },
    "diggCount": 3951,
    "shareCount": 38,
    "playCount": 348100,
    "collectCount": 105,
    "commentCount": 111,
}


def test_parse_profile_extracts_core_fields():
    parser = TikTokHashtagParser()
    profile = parser.parse_profile(RESELLER_VIDEO_PAYLOAD)

    assert profile.external_id == "7002169437214213125"
    assert profile.handle == "shaiie_foeva"
    assert profile.follower_count == 2_200_000
    assert profile.lifetime_likes == 126_000_000
    assert profile.bio_link == "https://fkrtrz-v0.myshopify.com/collections/all"


def test_parse_profile_flags_reseller_via_bio_link_not_keyword_guessing():
    """This account is a reseller by every signal (storefront bio link,
    'SHOP THE E-BOOK' bio) but commerceUserInfo.commerceUser is False and
    ttSeller is False — the platform-native flags aren't reliable on their
    own. This test documents that gap: parsing extracts bio_link faithfully,
    account_type classification (scoring/features.py) is what actually has
    to combine bio_link + bio text, not trust commerceUser blindly.
    """
    parser = TikTokHashtagParser()
    profile = parser.parse_profile(RESELLER_VIDEO_PAYLOAD)

    assert profile.is_commerce_account is False  # platform flag alone misses this
    assert profile.bio_link is not None  # but the signal parsing needs is right here


def test_parse_video_extracts_engagement_fields():
    parser = TikTokHashtagParser()
    video = parser.parse_video(RESELLER_VIDEO_PAYLOAD)

    assert video.video_id == "7535448384170331414"
    assert video.play_count == 348_100
    assert video.is_ad is False


def test_missing_author_meta_raises_parse_error():
    parser = TikTokHashtagParser()
    with pytest.raises(ParseError):
        parser.parse_profile({"id": "123", "diggCount": 5})


def test_parse_video_returns_none_when_no_video_id():
    parser = TikTokHashtagParser()
    assert parser.parse_video({"authorMeta": {"id": "1"}}) is None
