from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from .branch_store import (
    Sprint,
    Card,
    list_sprints,
    list_cards,
    load_index,
    BranchRecord,
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


def branches_by_group(
    *, path: Optional["Path"] = None, include_empty_group: bool = False
) -> Dict[str, List[BranchRecord]]:
    """Return NAS branches grouped by their owning group.

    Parameters
    ----------
    path:
        Optional base directory that hosts the NAS SQLite files. Tests can
        provide an isolated directory; production callers rely on the default
        resolution from :func:`load_index`.
    include_empty_group:
        When ``True`` branches without ``group`` metadata are included under
        the key ``""``. The default keeps the UI focused on named groups.
    """

    # ``load_index`` already returns the latest branch metadata keyed by the
    # fully-qualified branch identifier. We regroup that information so the UI
    # can offer a two-step selection (group -> branch).
    index = load_index(path)
    grouped: Dict[str, List[BranchRecord]] = defaultdict(list)
    for record in index.values():
        group = record.group or ""
        if not group and not include_empty_group:
            continue
        grouped[group].append(record)

    # Sort the groups and their branches to keep the selection dialog stable.
    for records in grouped.values():
        records.sort(key=lambda rec: ((rec.project or "").lower(), rec.branch.lower()))
    return dict(sorted(grouped.items(), key=lambda item: item[0].lower()))
