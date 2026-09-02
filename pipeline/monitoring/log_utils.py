"""
Structured failure logging. Routes through Python's standard logging
module — which Airflow already captures per-task into its own log files,
visible directly in the UI — rather than print(). Consistent key=value
formatting is what makes this "neat": you can eyeball it or grep it, and
it costs nothing extra to build since Airflow's log capture already exists.

Deliberately NOT a replacement for storing errors on ingestion_queue.error:
pipeline/monitoring/checks.py's check_quota_exhaustion() runs a SQL query
against that column (WHERE error ILIKE '%402%' OR ...) — that's only
possible against queryable state, not log output. This module is the
"neat to read" half; the DB column is the "queryable for monitoring" half.
Same information, two purposes, both real.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("pipeline")


def log_ingestion_failure(*, platform: str, external_id: str, handle: str, error: str) -> None:
    logger.error(
        "INGESTION_FAILURE platform=%s external_id=%s handle=%s error=%s",
        platform, external_id, handle, error,
    )
