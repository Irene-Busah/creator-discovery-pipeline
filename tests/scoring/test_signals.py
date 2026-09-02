from datetime import date

from pipeline.scoring.signals import (
    VideoEngagement,
    classify_account_type,
    growth_rate,
    is_suspicious_growth,
    lifetime_engagement_rate,
    posting_consistency,
    recent_engagement_rate,
)


def test_lifetime_engagement_rate_basic():
    rate = lifetime_engagement_rate(lifetime_likes=1_000_000, video_count=100, follower_count=500_000)
    assert rate == (1_000_000 / 100) / 500_000


def test_lifetime_engagement_rate_none_when_missing_inputs():
    assert lifetime_engagement_rate(lifetime_likes=None, video_count=10, follower_count=1000) is None
    assert lifetime_engagement_rate(lifetime_likes=100, video_count=0, follower_count=1000) is None


def test_recent_engagement_rate_excludes_ads():
    videos = [
        VideoEngagement(is_ad=False, play_count=1000, digg_count=100, comment_count=10, share_count=5),
        VideoEngagement(is_ad=True, play_count=999999, digg_count=99999, comment_count=9999, share_count=9999),
    ]
    rate = recent_engagement_rate(videos=videos, follower_count=1000)
    # only the organic video counts: (100+10+5)/1000
    assert rate == 0.115


def test_recent_engagement_rate_none_when_all_ads():
    videos = [VideoEngagement(is_ad=True, play_count=1, digg_count=1, comment_count=1, share_count=1)]
    assert recent_engagement_rate(videos=videos, follower_count=1000) is None


def test_growth_rate_none_without_previous_snapshot():
    assert growth_rate(
        previous_follower_count=None, previous_date=None,
        current_follower_count=1000, current_date=date(2026, 8, 1)
    ) is None


def test_growth_rate_normalizes_to_30_days():
    rate = growth_rate(
        previous_follower_count=1000, previous_date=date(2026, 7, 1),
        current_follower_count=1100, current_date=date(2026, 7, 16),  # 15 days later, +10%
    )
    # +10% over 15 days -> normalized to 30 days -> +20%
    assert round(rate, 4) == 0.2


def test_posting_consistency_counts_only_recent_videos():
    posted = [date(2026, 8, 1), date(2026, 8, 15), date(2026, 1, 1)]  # last one outside 30d window
    result = posting_consistency(posted_dates=posted, as_of=date(2026, 8, 29), window_days=30)
    assert result == 2 / (30 / 7)


def test_classify_account_type_commerce_flag_wins():
    assert classify_account_type(bio="just a normal bio", bio_link=None, is_commerce_account=True) == "brand"


def test_classify_account_type_catches_reseller_platform_flag_misses():
    """Regression test for the real finding: commerceUser/ttSeller were both
    False for an obvious reseller. Bio-link + keyword heuristic must catch
    what the platform flag didn't.
    """
    result = classify_account_type(
        bio="SHOP THE E-BOOK OUT NOW",
        bio_link="https://fkrtrz-v0.myshopify.com/collections/all",
        is_commerce_account=False,
    )
    assert result == "reseller"


def test_classify_account_type_defaults_to_creator():
    assert classify_account_type(bio="just here for fun", bio_link=None, is_commerce_account=False) == "creator"


def test_classify_account_type_known_limitation_negated_keyword_false_positives():
    """Documents a real, known limitation (found via tests/enrichment/):
    naive substring matching has no concept of negation, so a bio that
    happens to contain the word 'shop' inside an unrelated or negated
    phrase gets misclassified as a reseller. This test exists so the
    limitation is visible and intentional, not an accidental gap — see
    classify_account_type()'s docstring.
    """
    result = classify_account_type(bio="skincare tips, no shop, just me", bio_link=None, is_commerce_account=False)
    assert result == "reseller"  # known false positive, documented above


def test_is_suspicious_growth_flags_high_engagement():
    assert is_suspicious_growth(growth_rate_value=0.1, engagement_rate_value=0.30) is True


def test_is_suspicious_growth_false_for_normal_values():
    assert is_suspicious_growth(growth_rate_value=0.05, engagement_rate_value=0.05) is False
