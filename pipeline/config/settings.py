"""
Centralized configuration.

Design decision: every other module reads config through this object,
never via a bare `os.environ.get(...)` scattered through the codebase.
That means (a) there's one place to see every required env var, and
(b) tests can construct a Settings object directly instead of mutating
process environment variables.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # -- Database --
    database_url: str

    # -- Apify (discovery/ingestion source) --
    apify_api_token: str
    apify_tiktok_actor_id: str = "clockworks/tiktok-scraper"

    # -- Pipeline behavior --
    max_profiles_per_query: int = 50
    ingestion_batch_size: int = 25
    request_timeout_seconds: int = 30

    # -- Refresh cadence (days), keyed by tier --
    refresh_interval_days: dict = None  # set in __post_init__

    def __post_init__(self):
        if self.refresh_interval_days is None:
            object.__setattr__(
                self,
                "refresh_interval_days",
                {"A": 7, "B": 14, "C": 30, "Reject": 90, "needs_review": 3},
            )

    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment variables (used by orchestration/DAGs).

        Raises KeyError with a clear message if a required var is missing —
        fail at startup, not three tasks deep into a DAG run.
        """
        required = ["DATABASE_URL", "APIFY_API_TOKEN"]
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise KeyError(f"Missing required environment variables: {missing}")

        return cls(
            database_url=os.environ["DATABASE_URL"],
            apify_api_token=os.environ["APIFY_API_TOKEN"],
            apify_tiktok_actor_id=os.environ.get(
                "APIFY_TIKTOK_ACTOR_ID", "clockworks/tiktok-scraper"
            ),
            max_profiles_per_query=int(os.environ.get("MAX_PROFILES_PER_QUERY", 50)),
            ingestion_batch_size=int(os.environ.get("INGESTION_BATCH_SIZE", 25)),
        )
