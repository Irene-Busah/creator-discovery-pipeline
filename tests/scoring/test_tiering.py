"""
Each test here maps directly to a case from the assignment brief's
'Signals and tiering' section — this file IS the rehearsal script for
those questions.
"""
from pipeline.scoring.tiering import (
    TIER_A,
    TIER_B,
    TIER_C,
    TIER_NEEDS_REVIEW,
    TIER_REJECT,
    CreatorSignals,
    assign_tier,
)


def _signals(**overrides) -> CreatorSignals:
    defaults = dict(
        follower_count=50_000,
        engagement_rate=0.08,
        growth_rate=0.05,
        account_type="creator",
        scrape_status="success",
        suspicious_growth=False,
    )
    defaults.update(overrides)
    return CreatorSignals(**defaults)


def test_high_engagement_creator_gets_tier_a():
    result = assign_tier(_signals(engagement_rate=0.08))
    assert result.tier == TIER_A


def test_moderate_engagement_gets_tier_b():
    result = assign_tier(_signals(engagement_rate=0.04))
    assert result.tier == TIER_B


def test_low_but_present_engagement_gets_tier_c():
    result = assign_tier(_signals(engagement_rate=0.015))
    assert result.tier == TIER_C


def test_below_threshold_engagement_is_rejected():
    result = assign_tier(_signals(engagement_rate=0.005))
    assert result.tier == TIER_REJECT
    assert "below_engagement_threshold" in result.flags


def test_brief_case_200k_followers_no_video_data_goes_to_review_not_reject():
    """'a creator with 200K followers and no video data at all' — must not
    be silently rejected, since this could be our scraper's fault, not the
    creator's. High follower count is the signal that makes this worth a
    second look instead of an automatic write-off.
    """
    result = assign_tier(
        _signals(follower_count=200_000, engagement_rate=None, scrape_status="no_data")
    )
    assert result.tier == TIER_NEEDS_REVIEW
    assert "no_data_high_followers" in result.flags


def test_brief_case_no_data_and_small_follower_count_is_rejected():
    """Same 'no_data' scrape status, but a small creator — not worth the
    manual-review cost the way a 200K-follower account is.
    """
    result = assign_tier(
        _signals(follower_count=1_000, engagement_rate=None, scrape_status="no_data")
    )
    assert result.tier == TIER_REJECT
    assert "no_data_low_followers" in result.flags


def test_brief_case_creator_vanished_is_handled_upstream_not_here():
    """'a creator who was in your data last month and isn't there this
    month' is a discovery/enrichment-layer decision (soft-delete via
    creators.status after N missed cycles — see repository.py /
    schema.sql), not a tiering decision. Documenting that boundary here:
    tiering only runs against a creator we successfully have signals for.
    """
    assert True  # intentionally no tiering logic to test — see docstring


def test_brief_case_brand_or_reseller_account_is_rejected():
    """'an account that's technically a creator profile but is clearly a
    brand or a reseller' — excluded regardless of how good its engagement
    numbers look.
    """
    result = assign_tier(_signals(account_type="reseller", engagement_rate=0.20))
    assert result.tier == TIER_REJECT
    assert "excluded_account_type:reseller" in result.flags


def test_brief_case_engagement_looks_excellent_followers_look_bought():
    """'a creator whose engagement rate looks excellent and whose follower
    count looks bought' — flagged for manual review, NOT auto-tiered A
    (would reward the anomaly) and NOT auto-rejected (would discard a
    creator who might be genuinely excellent).
    """
    result = assign_tier(_signals(engagement_rate=0.30, suspicious_growth=True))
    assert result.tier == TIER_NEEDS_REVIEW
    assert "suspicious_growth" in result.flags


def test_fetch_failed_always_goes_to_review_regardless_of_other_signals():
    result = assign_tier(_signals(scrape_status="fetch_failed", engagement_rate=None))
    assert result.tier == TIER_NEEDS_REVIEW
    assert "fetch_failed" in result.flags


def test_small_follower_count_rejected_even_with_great_engagement():
    result = assign_tier(_signals(follower_count=500, engagement_rate=0.5))
    assert result.tier == TIER_REJECT
    assert "below_minimum_followers" in result.flags


def test_first_snapshot_creator_still_tiered_but_flagged_provisional():
    result = assign_tier(_signals(growth_rate=None, engagement_rate=0.08))
    assert result.tier == TIER_A
    assert "growth_unknown_first_snapshot" in result.flags
