"""
Tiering: given already-computed signals, decide A / B / C / Reject /
needs_review. Deliberately separate from signals.py — signal computation
("what is true about this creator") and tiering ("what do we do about it")
are different concerns and different kinds of decisions. Signals are mostly
objective math; tiering is where the judgment calls the brief asks for live.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

TIER_A = "A"
TIER_B = "B"
TIER_C = "C"
TIER_REJECT = "Reject"
TIER_NEEDS_REVIEW = "needs_review"

# GUESSING at these thresholds — starting point based on general creator-
# marketing benchmarks (organic engagement above ~6% is strong, below ~1%
# is weak), NOT validated against this niche's actual distribution. First
# thing I'd revisit with real outreach conversion data.
ENGAGEMENT_THRESHOLD_A = 0.06
ENGAGEMENT_THRESHOLD_B = 0.03
ENGAGEMENT_THRESHOLD_C = 0.01

# Below this, even excellent engagement isn't worth outreach at this
# company's scale — GUESSING, would tune per campaign/budget.
MIN_FOLLOWERS_FOR_OUTREACH = 5_000


@dataclass(frozen=True)
class CreatorSignals:
    """Everything tiering needs, already computed by signals.py. Kept as a
    plain input struct so assign_tier() has no hidden dependencies — every
    input it uses is visible in this one class.
    """

    follower_count: Optional[int]
    engagement_rate: Optional[float]        # prefer lifetime_engagement_rate; fall back to recent
    growth_rate: Optional[float]              # None if <2 snapshots exist yet
    account_type: str                          # 'creator' | 'brand' | 'reseller'
    scrape_status: str                            # 'success' | 'no_data' | 'fetch_failed'
    suspicious_growth: bool = False


@dataclass(frozen=True)
class TierResult:
    tier: str
    flags: list[str] = field(default_factory=list)


def assign_tier(signals: CreatorSignals) -> TierResult:
    flags: list[str] = []

    # --- Case: the fetch itself didn't succeed cleanly ---
    # This must be checked before anything else — a bad scrape produces
    # None/garbage signals that would otherwise masquerade as "low
    # engagement" and get silently auto-rejected. Distinguishing "creator
    # has no videos" from "our scraper failed" (the brief's explicit ask)
    # happens here: fetch_failed always goes to review; no_data on an
    # account with meaningful follower count also goes to review rather
    # than being scored on zero data.
    if signals.scrape_status == "fetch_failed":
        return TierResult(TIER_NEEDS_REVIEW, ["fetch_failed"])

    if signals.scrape_status == "no_data":
        if signals.follower_count and signals.follower_count >= MIN_FOLLOWERS_FOR_OUTREACH:
            # e.g. the brief's "200K followers, no video data at all" case —
            # too big to write off on what might be our own scrape gap.
            return TierResult(TIER_NEEDS_REVIEW, ["no_data_high_followers"])
        return TierResult(TIER_REJECT, ["no_data_low_followers"])

    # --- Case: excluded account types ---
    # ASSUMPTION stated plainly: brand/reseller accounts are out of scope
    # for creator outreach by default. A campaign that specifically wants
    # brand partnerships would flip this — that's a config decision, not a
    # tiering-logic change, which is why account_type is a separate signal
    # rather than baked into the engagement thresholds.
    if signals.account_type in ("brand", "reseller"):
        return TierResult(TIER_REJECT, [f"excluded_account_type:{signals.account_type}"])

    # --- Case: too small to be worth outreach regardless of engagement ---
    if not signals.follower_count or signals.follower_count < MIN_FOLLOWERS_FOR_OUTREACH:
        return TierResult(TIER_REJECT, ["below_minimum_followers"])

    # --- Case: suspicious growth/engagement — manual review, not auto-anything ---
    # This is the brief's hardest case ('engagement looks excellent, follower
    # count looks bought'). Deliberately NOT auto-rejected: a false positive
    # here throws away a creator who might be genuinely excellent. Deliberately
    # NOT auto-tiered A either, for the obvious reason. needs_review is the
    # only defensible default when the signal itself is what's in question.
    if signals.suspicious_growth:
        flags.append("suspicious_growth")
        return TierResult(TIER_NEEDS_REVIEW, flags)

    # --- Case: no engagement data to score on at all ---
    if signals.engagement_rate is None:
        return TierResult(TIER_NEEDS_REVIEW, ["insufficient_engagement_data"])

    # --- Case: growth rate not yet computable (first-seen creator) ---
    # Still tiered on engagement alone — not held in review just for being
    # new — but flagged so a human (or next scoring pass) knows this tier
    # is provisional until a second snapshot exists.
    if signals.growth_rate is None:
        flags.append("growth_unknown_first_snapshot")

    # --- Normal case: threshold-based tiering on engagement rate ---
    if signals.engagement_rate >= ENGAGEMENT_THRESHOLD_A:
        return TierResult(TIER_A, flags)
    if signals.engagement_rate >= ENGAGEMENT_THRESHOLD_B:
        return TierResult(TIER_B, flags)
    if signals.engagement_rate >= ENGAGEMENT_THRESHOLD_C:
        return TierResult(TIER_C, flags)

    return TierResult(TIER_REJECT, flags + ["below_engagement_threshold"])
