"""
Handoff: the business rule for which tiers get pushed to outreach lives
here — currently A and B — separate from the mechanical insert in
Repository.push_to_outreach. Changing which tiers qualify (e.g. a campaign
that also wants C-tier for volume) is a one-line change in this file, not
a schema or repository change.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipeline.db.repository import Repository

QUALIFYING_TIERS = ("A", "B")


def push_qualified_creators(repository: "Repository") -> int:
    """Finds every creator whose latest score qualifies and isn't already
    in the outreach queue, and pushes them. Idempotent — push_to_outreach
    no-ops for a creator already present (ON CONFLICT DO NOTHING on
    creator_id), so re-running this task is always safe.
    """
    ready = repository.creators_ready_for_outreach()
    pushed = 0
    for entry in ready:
        if entry["tier"] not in QUALIFYING_TIERS:
            # Defensive — creators_ready_for_outreach() already filters to
            # A/B at the query level, but the qualifying-tier RULE is owned
            # here, not there. If that query's filter and this constant
            # ever drift, this line is what stops a stray C-tier assignment
            # from being taken.
            continue
        repository.push_to_outreach(entry["creator_id"], tier=entry["tier"])
        pushed += 1
    return pushed
