"""
Turns an exception into a short, human-readable summary — used for BOTH
the log line an engineer reads live and the compact string stored in
ingestion_queue.error for later querying. One function, two consumers,
so the printed version and the stored version can never drift apart.

The status-code labels below are not hypothetical — every one of them
maps to a real failure hit while building this pipeline (404 from the
Apify actor-ID slash bug, 402 from running out of free-tier quota). That's
exactly the point of a shared summarizer: the next time one of these
happens, the log line says what it is immediately instead of requiring
another round of digging through a raw traceback.
"""
from __future__ import annotations

import httpx

_STATUS_LABELS = {
    401: "auth failed — check APIFY_API_TOKEN",
    402: "Apify quota/billing exhausted — check account usage",
    403: "forbidden — check actor access/plan",
    404: "actor not found — check APIFY_TIKTOK_ACTOR_ID (slash vs '~' form?)",
    429: "rate limited",
    500: "Apify server error",
    503: "Apify service unavailable",
}


def summarize_exception(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        label = _STATUS_LABELS.get(status)
        return f"HTTP {status}" + (f" ({label})" if label else "")
    if isinstance(exc, httpx.TimeoutException):
        return "request timed out"
    return f"{type(exc).__name__}: {str(exc)[:200]}"
