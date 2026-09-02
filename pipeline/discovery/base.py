"""
DiscoverySource interface.

Why an interface at all: discovery today is "TikTok hashtag search via
Apify," but the brief explicitly says pick a source and defend it — that
choice should be swappable without touching anything downstream. Every
discovery implementation returns the same `Candidate` shape regardless of
platform or method (hashtag, keyword search, open dataset), so ingestion
never needs to know which source found a given creator.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Candidate:
    """A creator identity worth fetching, plus where it came from.

    Deliberately does NOT carry a raw payload. Discovery answers "who should
    we fetch" — ingestion answers "what does the source currently tell us
    about them." Those are different questions with different lifecycles:
    discovery runs once per candidate (they're found), ingestion runs
    repeatedly (they're refreshed on a cadence). If Candidate carried the
    payload from the discovery call, a "refresh" would have no clean way to
    re-fetch — you'd either stuff a second payload onto the same object or
    invent a different path for refreshes than for new candidates. Ingestion
    always does its own fetch, for both new candidates and refreshes, and
    that fetch result is what lands in raw_payloads — never data discovery
    happened to see along the way.
    """

    platform: str
    external_id: str
    handle: str
    source: str          # e.g. 'tiktok_hashtag'
    query: str            # the hashtag/keyword that surfaced it


class DiscoverySource(ABC):
    """One method. Discovery's only job is: given a query, return candidate
    identities. It does NOT fetch full profiles, parse metrics, or write to
    the database — that's ingestion's and parsing's job (single responsibility).
    """

    @abstractmethod
    def discover(self, query: str, *, max_results: int) -> list[Candidate]:
        ...
