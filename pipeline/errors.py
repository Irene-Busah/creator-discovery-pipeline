"""
Typed exceptions for the pipeline. Purpose: a bare `except Exception` and a
raw traceback tell you THAT something failed, not WHAT KIND of failure it
was or what to do about it. Every exception here carries the context
needed to log it usefully and to let a caller decide how to react —
retry, back off, or page someone — without re-parsing an error string.

This is the layer that turns "httpx.HTTPStatusError: 402" into something
a monitoring check or a human can act on immediately.
"""
from __future__ import annotations


class PipelineError(Exception):
    """Base class for every error raised by pipeline/ code. Lets callers
    that don't care about the specific failure mode catch one thing.
    """


class FetchError(PipelineError):
    """A source fetch (Apify) failed. Carries enough context to log a
    useful one-liner without the caller needing to know about httpx.
    """

    def __init__(self, *, platform: str, external_id: str, status_code: int | None, detail: str):
        self.platform = platform
        self.external_id = external_id
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"[{platform}:{external_id}] fetch failed ({status_code}): {detail}")


class QuotaExceededError(FetchError):
    """A fetch failed specifically because of billing/rate-limit (402/429).
    Split from FetchError deliberately: an operator's response to "we're
    out of budget" (add credit, back off, wait) is completely different
    from their response to "the source is broken" (investigate, page
    someone). Collapsing both into one generic error is exactly what made
    the real quota-exhaustion incident during development briefly look
    like an unexplained ingestion failure spike instead of an obvious,
    single-cause billing issue.
    """


class ParseFailureError(PipelineError):
    """A raw payload could not be parsed into the expected shape. Carries
    the raw_id so the caller can write to parse_errors without re-deriving
    it, and the underlying parser exception's message for the actual cause.
    """

    def __init__(self, *, raw_id: str, detail: str):
        self.raw_id = raw_id
        self.detail = detail
        super().__init__(f"[raw_id={raw_id}] parse failed: {detail}")
