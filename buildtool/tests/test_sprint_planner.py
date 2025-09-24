from __future__ import annotations

import pytest

from buildtool.core.branch_history_db import BranchHistoryDB
from buildtool.core.sprint_planner import (
    ApprovalState,
    AuthorizationError,
    MergeBlockedError,
    SprintPlanner,
    SprintRecord,
    TicketRecord,
    TicketValidationError,
    UserAccount,
)


def _planner(tmp_path, clock_start: int = 10) -> SprintPlanner:
    counter = {"now": clock_start}

    def fake_clock() -> int:
        counter["now"] += 1
        return counter["now"]

    db = BranchHistoryDB(tmp_path / "branches_history.sqlite3")
    return SprintPlanner(db=db, clock=fake_clock)


def test_default_roles_available(tmp_path):
    planner = _planner(tmp_path)
    roles = planner.list_roles()
    keys = {role.key for role in roles}
    assert {"leader", "developer", "qa", "admin"}.issubset(keys)


def test_sprint_and_ticket_flow(tmp_path):
    planner = _planner(tmp_path)

    # alta de usuarios mínimos
    planner.save_user(UserAccount(username="lidia", role_key="leader", display_name="Líder"))
    planner.save_user(UserAccount(username="dev", role_key="developer", display_name="Dev"))
    planner.save_user(UserAccount(username="qa", role_key="qa", display_name="QA"))

    sprint = SprintRecord(
        key="ELLIS-2.68-S3",
        name="Sprint 3",
        version="2.68",
        base_branch="2.68",
        group_name="ELLiS",
        project="ELLiS",
        created_by="lidia",
    )

    # Un desarrollador no puede crear sprints
    with pytest.raises(AuthorizationError):
        planner.save_sprint(sprint, actor="dev")

    planner.save_sprint(sprint, actor="lidia")

    ticket = TicketRecord(
        key="ELLIS-2.68-S3-T1",
        sprint_key=sprint.key,
        title="Tarjeta 1",
        branch_name="2.68/feature/tarjeta-1",
        assignee="dev",
        qa_owner="qa",
        requires_qa=True,
        created_by="lidia",
    )

    planner.save_ticket(ticket, actor="lidia")
    stored = planner.get_ticket(ticket.key)
    assert stored is not None
    assert stored.unit_status is ApprovalState.PENDING
    assert stored.qa_status is ApprovalState.PENDING

    with pytest.raises(MergeBlockedError):
        planner.ensure_merge_allowed(ticket.key)

    # el líder puede forzar merge aunque no haya checks completos
    override = planner.ensure_merge_allowed(ticket.key, actor="lidia")
    assert override.key == ticket.key

    # desarrollador aprueba pruebas unitarias
    updated = planner.mark_unit_status(ticket.key, ApprovalState.APPROVED, actor="dev")
    assert updated.unit_status is ApprovalState.APPROVED
    assert not updated.can_merge()

    # un desarrollador no puede aprobar QA
    with pytest.raises(AuthorizationError):
        planner.mark_qa_status(ticket.key, ApprovalState.APPROVED, actor="dev")

    updated = planner.mark_qa_status(ticket.key, ApprovalState.APPROVED, actor="qa")
    assert updated.qa_status is ApprovalState.APPROVED
    assert updated.can_merge()

    ready = planner.ensure_merge_allowed(ticket.key)
    assert ready.can_merge()


def test_branch_validation_and_exceptions(tmp_path):
    planner = _planner(tmp_path)
    planner.save_user(UserAccount(username="boss", role_key="leader"))

    sprint = SprintRecord(
        key="GSA-2.70-S1",
        name="Sprint 38",
        version="2.70",
        base_branch="2.70",
        created_by="boss",
    )
    planner.save_sprint(sprint, actor="boss")

    bad_branch_ticket = TicketRecord(
        key="GSA-2.70-S1-T1",
        sprint_key=sprint.key,
        title="Tarjeta",
        branch_name="feature/tarjeta",
    )

    with pytest.raises(TicketValidationError):
        planner.save_ticket(bad_branch_ticket, actor="boss")

    qa_exempt = TicketRecord(
        key="GSA-2.70-S1-TQA",
        sprint_key=sprint.key,
        title="QA Solo",
        branch_name="2.70/qa/revision",
        requires_qa=False,
    )
    planner.save_ticket(qa_exempt, actor="boss")
    planner.save_user(UserAccount(username="dev2", role_key="developer"))

    approved = planner.mark_unit_status(qa_exempt.key, ApprovalState.APPROVED, actor="dev2")
    assert approved.can_merge()
    ready = planner.ensure_merge_allowed(qa_exempt.key)
    assert ready.can_merge()

