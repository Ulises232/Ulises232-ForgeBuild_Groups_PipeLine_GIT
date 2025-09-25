from pathlib import Path

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
