from __future__ import annotations

from pathlib import Path

import pytest

from buildtool.core.branch_history_db import BranchHistoryDB
from buildtool.core.sprint_board import (
    STATUS_APPROVED,
    STATUS_PENDING,
    SprintBoardStore,
    compose_sprint_key,
)


def _make_db(tmp_path: Path) -> BranchHistoryDB:
    db_path = tmp_path / "board.sqlite3"
    return BranchHistoryDB(db_path)


def test_card_flow_requires_qa(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    store = SprintBoardStore(db)

    store.upsert_role("leader")
    store.upsert_role("developer")
    store.upsert_role("qa")
    store.upsert_user("ulises", role_name="leader")
    store.upsert_user("ana", role_name="developer")
    store.upsert_user("carl", role_name="qa")

    sprint = store.create_sprint(
        name="Sprint 3",
        version_branch="2.68",
        group_name="ELLiS",
        project_name="ELLiS",
        created_by="ulises",
    )

    card = store.add_card(
        sprint.key,
        title="Tarjeta 1",
        branch_name="feature/t1",
        assignee="ana",
        created_by="ulises",
    )

    assert card.unit_status == STATUS_PENDING
    assert not card.ready_for_merge()

    store.set_unit_status(card.key, True, by="ana")
    card = store.get_card(card.key)
    assert card.unit_status == STATUS_APPROVED
    assert card.qa_status == STATUS_PENDING
    assert not card.ready_for_merge()

    store.set_qa_status(card.key, "approved", by="carl")
    card = store.get_card(card.key)
    assert card.ready_for_merge()


def test_qa_branch_only_needs_unit(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    store = SprintBoardStore(db)

    store.upsert_user("qauser")

    sprint_key = compose_sprint_key("2.68_Central", "ELLiS", "ELLiS", "Sprint 3_Central")
    sprint = store.create_sprint(
        name="Sprint 3_Central",
        version_branch="2.68_Central",
        group_name="ELLiS",
        project_name="ELLiS",
        created_by="qauser",
        key=sprint_key,
    )

    qa_card = store.add_card(
        sprint.key,
        title="Tarjeta QA",
        branch_name="qa/tarjeta1",
        assignee="qauser",
        created_by="qauser",
        is_qa_branch=True,
    )

    assert not qa_card.ready_for_merge()

    store.set_unit_status(qa_card.key, "approved", by="qauser")
    qa_card = store.get_card(qa_card.key)
    assert qa_card.ready_for_merge()


def test_invalid_status_raises(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    store = SprintBoardStore(db)
    store.upsert_user("dev")
    sprint = store.create_sprint(name="Sprint", version_branch="2.70", created_by="dev")
    card = store.add_card(
        sprint.key,
        title="Card",
        branch_name="feature/card",
        assignee="dev",
        created_by="dev",
    )

    with pytest.raises(ValueError):
        store.set_unit_status(card.key, "unknown", by="dev")


def test_cards_ready_for_merge_bulk(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    store = SprintBoardStore(db)
    store.upsert_user("lead")
    store.upsert_user("qa")
    sprint = store.create_sprint(name="Sprint", version_branch="2.69", created_by="lead")

    c1 = store.add_card(
        sprint.key,
        title="Card 1",
        branch_name="feature/card1",
        assignee="lead",
        created_by="lead",
    )
    c2 = store.add_card(
        sprint.key,
        title="Card 2",
        branch_name="feature/card2",
        assignee="lead",
        created_by="lead",
        is_qa_branch=True,
    )

    store.set_unit_status(c1.key, True, by="lead")
    store.set_qa_status(c1.key, True, by="qa")
    store.set_unit_status(c2.key, True, by="lead")

    ready_map = store.cards_ready_for_merge([c1.key, c2.key])
    assert ready_map == {c1.key: True, c2.key: True}

