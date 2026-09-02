from pipeline.parsing.tiktok_profile_parser import TikTokProfileParser

PROFILE_ITEM = {
    "id": "7535448384170331414",
    "createTimeISO": "2025-08-06T12:28:22.000Z",
    "isAd": False,
    "webVideoUrl": "https://www.tiktok.com/@shaiie_foeva/video/7535448384170331414",
    "authorMeta": {
        "id": "7002169437214213125",
        "name": "shaiie_foeva",
        "signature": "SHOP THE E-BOOK OUT NOW",
        "bioLink": "https://fkrtrz-v0.myshopify.com/collections/all",
        "commerceUserInfo": {"commerceUser": False},
        "ttSeller": False,
        "fans": 2200000,
        "heart": 126000000,
        "video": 1036,
    },
    "diggCount": 3951, "shareCount": 38, "playCount": 348100, "commentCount": 111,
}

SECOND_VIDEO_ITEM = {
    "id": "7533731959172861206",
    "createTimeISO": "2025-08-01T21:27:45.000Z",
    "isAd": False,
    "webVideoUrl": "https://www.tiktok.com/@shaiie_foeva/video/7533731959172861206",
    "authorMeta": PROFILE_ITEM["authorMeta"],
    "diggCount": 25900, "shareCount": 352, "playCount": 573900, "commentCount": 346,
}


def test_parse_profile_uses_first_item():
    parser = TikTokProfileParser()
    fetch_payload = {"profile_item": PROFILE_ITEM, "video_items": [PROFILE_ITEM, SECOND_VIDEO_ITEM]}

    profile = parser.parse_profile(fetch_payload)
    assert profile.handle == "shaiie_foeva"
    assert profile.follower_count == 2_200_000


def test_parse_videos_returns_every_video_in_the_fetch():
    parser = TikTokProfileParser()
    fetch_payload = {"profile_item": PROFILE_ITEM, "video_items": [PROFILE_ITEM, SECOND_VIDEO_ITEM]}

    videos = parser.parse_videos(fetch_payload)
    assert len(videos) == 2
    assert {v.video_id for v in videos} == {"7535448384170331414", "7533731959172861206"}
    assert videos[1].play_count == 573_900


def test_parse_videos_returns_empty_list_when_no_videos():
    parser = TikTokProfileParser()
    assert parser.parse_videos({"profile_item": PROFILE_ITEM, "video_items": []}) == []
