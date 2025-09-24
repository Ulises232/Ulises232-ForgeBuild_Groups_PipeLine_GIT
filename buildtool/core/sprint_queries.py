from __future__ import annotations

from typing import Iterable, List, Optional

from .branch_store import (
    Sprint,
    Card,
    list_sprints,
    list_cards,
)


def branch_key(group: Optional[str], project: Optional[str], branch: str) -> str:
    """Build a branch key compatible with the NAS index."""

    group_part = group or ""
    project_part = project or ""
    return f"{group_part}/{project_part}/{branch}".strip()


def sprints_for_branch(branch_identifier: str) -> List[Sprint]:
    """Return sprints linked to a branch key."""

    return list_sprints(branch_keys=[branch_identifier])


def cards_for_branch(branch: str) -> List[Card]:
    """Return all cards that create branches derived from the provided branch name."""

    return list_cards(branches=[branch])


def cards_for_sprint(sprint_id: int) -> List[Card]:
    return list_cards(sprint_ids=[sprint_id])


def find_card_by_branch(branch_name: str) -> Optional[Card]:
    cards = cards_for_branch(branch_name)
    return cards[0] if cards else None


def is_card_ready_for_merge(card: Card, *, allow_qa_missing: bool = False) -> bool:
    if not card.unit_tests_done:
        return False
    if allow_qa_missing:
        return True
    return card.qa_done


def filter_cards(cards: Iterable[Card], *, status: Optional[str] = None) -> List[Card]:
    if status is None:
        return list(cards)
    wanted = status.lower()
    return [card for card in cards if (card.status or "").lower() == wanted]
