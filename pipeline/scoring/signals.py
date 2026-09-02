"""
Pure signal calculations. No database access, no I/O — every function takes
plain values and returns a plain value. That's deliberate: these are the
functions most likely to get picked apart live ("walk me through how you
compute X"), so they need to be readable top to bottom with nothing hidden
behind an ORM call or an API response.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class VideoEngagement:
    is_ad: bool
    play_count: Optional[int]
    digg_count: Optional[int]
    comment_count: Optional[int]
    share_count: Optional[int]


def lifetime_engagement_rate(
    *, lifetime_likes: Optional[int], video_count: Optional[int], follower_count: Optional[int]
) -> Optional[float]:
    """heart / video_count / follower_count — average likes-per-video as a
    fraction of followers, using the platform's lifetime counters.

    Chosen as the PRIMARY engagement signal (over averaging only the last
    N scraped videos) because it's stable: a creator's average over their
    entire posting history isn't skewed by catching them mid-slump or
    mid-viral-hit in whatever window happened to be scraped. The trade-off,
    worth saying out loud: it can't detect a *recent* decline — a creator
    who was great for years and has gone quiet for the last month looks
    identical to one who's still active. `posting_consistency` below is
    what catches that case instead.
    """
    if not lifetime_likes or not video_count or not follower_count:
        return None
    avg_likes_per_video = lifetime_likes / video_count
    return avg_likes_per_video / follower_count


def recent_engagement_rate(
    *, videos: list[VideoEngagement], follower_count: Optional[int]
) -> Optional[float]:
    """Average (likes+comments+shares)/follower_count across recently
    scraped ORGANIC videos only (isAd excluded — sponsored posts are often
    boosted/promoted and don't reflect organic pull, would overstate the
    signal if included).

    SECONDARY signal, used alongside lifetime_engagement_rate to catch a
    recent decline that the lifetime average would miss. Returns None if
    there are no organic videos to measure (all ads, or zero videos) —
    the caller must treat that as "insufficient recent data," not zero.
    """
    if not follower_count:
        return None

    organic = [v for v in videos if not v.is_ad]
    if not organic:
        return None

    rates = []
    for v in organic:
        engagement = (v.digg_count or 0) + (v.comment_count or 0) + (v.share_count or 0)
        rates.append(engagement / follower_count)

    return sum(rates) / len(rates)


def growth_rate(
    *,
    previous_follower_count: Optional[int],
    previous_date: Optional[date],
    current_follower_count: Optional[int],
    current_date: date,
) -> Optional[float]:
    """Fractional follower change since the previous snapshot, normalized
    to a 30-day rate so snapshots at irregular intervals stay comparable.

    Returns None when there's no previous snapshot to compare against —
    this is the mechanism behind "a first-seen creator can't be scored on
    growth yet," not a special case bolted on elsewhere.
    """
    if previous_follower_count is None or previous_date is None or not current_follower_count:
        return None
    if previous_follower_count == 0:
        return None

    days_elapsed = (current_date - previous_date).days
    if days_elapsed <= 0:
        return None

    raw_change = (current_follower_count - previous_follower_count) / previous_follower_count
    return raw_change * (30 / days_elapsed)


def posting_consistency(*, posted_dates: list[date], as_of: date, window_days: int = 30) -> Optional[float]:
    """Videos per week over the trailing window. None if there's no video
    history at all to measure (distinct from zero — zero means we have
    dates but none fall in the window, which is a real 'gone quiet' signal;
    None means we never got video data to begin with).
    """
    if not posted_dates:
        return None

    cutoff_days = window_days
    recent = [d for d in posted_dates if (as_of - d).days <= cutoff_days and (as_of - d).days >= 0]
    weeks = window_days / 7
    return len(recent) / weeks


def classify_account_type(*, bio: Optional[str], bio_link: Optional[str], is_commerce_account: bool) -> str:
    """Heuristic account-type classification. GUESSING here, flagged as such:
    thresholds/keywords below are a starting point, not validated against
    labeled data. Worth saying explicitly in the interview.

    Finding from real data (tests/parsing/test_tiktok_hashtag_parser.py):
    the platform's own `is_commerce_account` flag missed an obvious
    reseller (bio said 'SHOP THE E-BOOK', had a Shopify bio_link, but
    commerceUser/ttSeller were both False). So this function does NOT
    trust is_commerce_account alone — it's one input among several.

    KNOWN LIMITATION, found while writing tests/enrichment/test_enrichment.py:
    substring keyword matching has no concept of negation or context — a
    bio reading "skincare tips, no shop, just me" would false-positive on
    "shop" exactly like a real storefront bio would. This is the honest
    trade-off of a v1 keyword heuristic instead of real NLP: cheap and
    explainable, but not precise. Worth naming directly if asked "how
    confident are you in this classifier" — not confident on edge phrasing,
    confident on the common case (an actual product link or an explicit
    "shop now" CTA).
    """
    if is_commerce_account:
        return "brand"

    bio_lower = (bio or "").lower()
    reseller_keywords = ("shop", "store", "discount code", "use code", "link in bio")
    has_reseller_keyword = any(kw in bio_lower for kw in reseller_keywords)

    looks_like_storefront_link = bool(bio_link) and any(
        domain in (bio_link or "").lower()
        for domain in ("shopify", "etsy.com", "gumroad", "linktr.ee/shop")
    )

    if has_reseller_keyword or looks_like_storefront_link:
        return "reseller"

    return "creator"


def is_suspicious_growth(
    *, growth_rate_value: Optional[float], engagement_rate_value: Optional[float]
) -> bool:
    """Flags for manual review, does not auto-reject or auto-tier.

    GUESSING at the threshold: engagement rate above 25% is unusually high
    for organic content at any follower size in most niches — I'd want to
    validate this against the actual sample pulled for this niche rather
    than trust a single hardcoded number, but as a first pass it catches
    the case in the brief ('engagement rate looks excellent, follower count
    looks bought') without silently rejecting a creator who is just
    genuinely doing well.
    """
    SUSPICIOUSLY_HIGH_ENGAGEMENT = 0.25
    SUSPICIOUSLY_FAST_GROWTH = 1.0  # +100% in a normalized 30-day window

    if engagement_rate_value is not None and engagement_rate_value > SUSPICIOUSLY_HIGH_ENGAGEMENT:
        return True
    if growth_rate_value is not None and growth_rate_value > SUSPICIOUSLY_FAST_GROWTH:
        return True
    return False
