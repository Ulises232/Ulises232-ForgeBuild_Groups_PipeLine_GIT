import time
from pathlib import Path

from buildtool.core import branch_store
from buildtool.core.branch_store import BranchRecord, save_index
from buildtool.core import sprint_queries


def test_branches_by_group_groups_and_orders_branches(tmp_path):
    base = Path(tmp_path)
    records = {
        rec.key(): rec
        for rec in [
            BranchRecord(branch="2.68", group="ELLiS", project="ELLiS"),
            BranchRecord(branch="2.68_Central", group="ELLiS", project="Central"),
            BranchRecord(branch="2.69", group="GSA", project="GSA"),
            BranchRecord(branch="hotfix", group=None, project=None),
        ]
    }
    save_index(records, path=base)

    grouped = sprint_queries.branches_by_group(path=base)

    assert list(grouped.keys()) == ["ELLiS", "GSA"]
    ellis_keys = [rec.key() for rec in grouped["ELLiS"]]
    assert ellis_keys == ["ELLiS/Central/2.68_Central", "ELLiS/ELLiS/2.68"]
    gsa_keys = [rec.key() for rec in grouped["GSA"]]
    assert gsa_keys == ["GSA/GSA/2.69"]


def test_branches_by_group_includes_empty_group_when_requested(tmp_path):
    base = Path(tmp_path)
    records = {
        rec.key(): rec
        for rec in [
            BranchRecord(branch="feature", group=None, project=None),
        ]
    }
    save_index(records, path=base)

    grouped = sprint_queries.branches_by_group(
        path=base, include_empty_group=True
    )

    assert "" in grouped
    assert [rec.key() for rec in grouped[""]] == ["//feature"]


def test_cards_pending_release_filters_cards(tmp_path):
    branch_store._DB_CACHE.clear()
    base = Path(tmp_path)
    now = int(time.time())
    sprint = branch_store.Sprint(
        id=None,
        branch_key="ellis/proyecto/v2.68",
        qa_branch_key="ellis/proyecto/v2.68_QA",
        name="Sprint QA",
        version="2.68",
        created_at=now,
        created_by="alice",
        updated_at=now,
        updated_by="alice",
    )
    branch_store.upsert_sprint(sprint, path=base)
    assert sprint.id is not None

    done = branch_store.Card(
        id=None,
        sprint_id=sprint.id,
        title="Completa",
        branch="feature/done",
        unit_tests_done=True,
        qa_done=True,
        created_by="alice",
        updated_by="alice",
    )
    branch_store.upsert_card(done, path=base)

    pending = branch_store.Card(
        id=None,
        sprint_id=sprint.id,
        title="Pendiente QA",
        branch="feature/pending",
        unit_tests_done=True,
        qa_done=False,
        created_by="alice",
        updated_by="alice",
    )
    branch_store.upsert_card(pending, path=base)

    pending_cards = sprint_queries.cards_pending_release(sprint.id, path=base)
    assert [card.title for card in pending_cards] == ["Pendiente QA"]
