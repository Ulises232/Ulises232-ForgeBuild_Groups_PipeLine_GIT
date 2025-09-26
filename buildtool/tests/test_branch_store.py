import os
import tempfile
import time
import unittest
from pathlib import Path

from buildtool.core import branch_store
from buildtool.core.branch_store import (
    BranchRecord,
    Card,
    Sprint,
    load_activity_log,
    load_index,
)


class FakeBranchHistory:
    def __init__(self) -> None:
        self.branch_rows: dict[str, dict] = {}
        self.activity_rows: list[dict] = []
        self.sprints: dict[int, dict] = {}
        self.cards: dict[int, dict] = {}
        self.users: dict[str, dict] = {}
        self.roles: dict[str, dict] = {}
        self.user_roles: list[dict] = []
        self.next_sprint_id = 1
        self.next_card_id = 1

    # Branches ---------------------------------------------------------
    def fetch_branches(self, filter_origin: bool = False) -> list[dict]:
        rows = list(self.branch_rows.values())
        if filter_origin:
            rows = [row for row in rows if row.get("exists_origin")]
        return [row.copy() for row in rows]

    def replace_branches(self, records: list[dict]) -> None:
        self.branch_rows = {rec["key"]: rec.copy() for rec in records}

    def upsert_branch(self, payload: dict) -> None:
        self.branch_rows[payload["key"]] = payload.copy()

    def delete_branch(self, key: str) -> None:
        self.branch_rows.pop(key, None)

    # Activity ---------------------------------------------------------
    def append_activity(self, entries: list[dict]) -> None:
        for entry in entries:
            self.activity_rows.append(entry.copy())

    def fetch_activity(self, branch_keys: list[str] | None = None) -> list[dict]:
        if branch_keys:
            allowed = set(branch_keys)
            return [row.copy() for row in self.activity_rows if row.get("branch_key") in allowed]
        return [row.copy() for row in self.activity_rows]

    def prune_activity(self, valid_keys) -> None:  # pragma: no cover - defensive
        keys = {key for key in valid_keys if key}
        if not keys:
            self.activity_rows.clear()
        else:
            self.activity_rows = [row for row in self.activity_rows if row.get("branch_key") in keys]

    # Sprints ----------------------------------------------------------
    def fetch_sprints(self, branch_keys: list[str] | None = None) -> list[dict]:
        rows = list(self.sprints.values())
        if branch_keys:
            allowed = set(branch_keys)
            rows = [
                row
                for row in rows
                if row.get("branch_key") in allowed or row.get("qa_branch_key") in allowed
            ]
        return [row.copy() for row in rows]

    def fetch_sprint(self, sprint_id: int) -> dict | None:
        row = self.sprints.get(int(sprint_id))
        return row.copy() if row else None

    def fetch_sprint_by_branch_key(self, branch_key: str) -> dict | None:
        for row in self.sprints.values():
            if row.get("branch_key") == branch_key or row.get("qa_branch_key") == branch_key:
                return row.copy()
        return None

    def upsert_sprint(self, payload: dict) -> int:
        ident = payload.get("id")
        if ident:
            ident = int(ident)
        else:
            ident = self.next_sprint_id
            self.next_sprint_id += 1
        stored = payload.copy()
        stored["id"] = ident
        self.sprints[ident] = stored
        return ident

    def delete_sprint(self, sprint_id: int) -> None:
        self.sprints.pop(int(sprint_id), None)

    # Cards ------------------------------------------------------------
    def fetch_cards(
        self,
        sprint_ids: list[int] | None = None,
        branches: list[str] | None = None,
    ) -> list[dict]:
        rows = list(self.cards.values())
        if sprint_ids:
            allowed = {int(x) for x in sprint_ids}
            rows = [row for row in rows if int(row.get("sprint_id") or 0) in allowed]
        if branches:
            allowed = set(branches)
            rows = [
                row
                for row in rows
                if row.get("branch") in allowed or row.get("branch_key") in allowed
            ]
        return [row.copy() for row in rows]

    def upsert_card(self, payload: dict) -> int:
        ident = payload.get("id")
        if ident:
            ident = int(ident)
        else:
            ident = self.next_card_id
            self.next_card_id += 1
        stored = payload.copy()
        stored["id"] = ident
        self.cards[ident] = stored
        return ident

    def delete_card(self, card_id: int) -> None:
        self.cards.pop(int(card_id), None)

    # Users & roles ----------------------------------------------------
    def fetch_users(self) -> list[dict]:
        return [row.copy() for row in self.users.values()]

    def upsert_user(self, payload: dict) -> None:
        self.users[payload["username"]] = payload.copy()

    def delete_user(self, username: str) -> None:
        self.users.pop(username, None)

    def fetch_roles(self) -> list[dict]:
        return [row.copy() for row in self.roles.values()]

    def upsert_role(self, payload: dict) -> None:
        self.roles[payload["key"]] = payload.copy()

    def delete_role(self, role_key: str) -> None:
        self.roles.pop(role_key, None)

    def fetch_user_roles(self, username: str | None = None) -> list[dict]:
        rows = self.user_roles
        if username:
            rows = [row for row in rows if row["username"] == username]
        return [row.copy() for row in rows]

    def set_user_roles(self, username: str, roles: list[str]) -> None:
        self.user_roles = [row for row in self.user_roles if row["username"] != username]
        for role in roles:
            self.user_roles.append({"username": username, "role_key": role})


class BranchStoreSqlServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.old_appdata = os.environ.get("APPDATA")
        self.old_xdg = os.environ.get("XDG_DATA_HOME")
        os.environ["APPDATA"] = self.tmp.name
        os.environ["XDG_DATA_HOME"] = self.tmp.name
        self.fake = FakeBranchHistory()
        branch_store._DB_CACHE.clear()
        branch_store._DB_CACHE[branch_store._SERVER_CACHE_KEY] = self.fake
        self.base_path = Path(self.tmp.name)

    def tearDown(self) -> None:
        branch_store._DB_CACHE.clear()
        if self.old_appdata is None:
            os.environ.pop("APPDATA", None)
        else:
            os.environ["APPDATA"] = self.old_appdata
        if self.old_xdg is None:
            os.environ.pop("XDG_DATA_HOME", None)
        else:
            os.environ["XDG_DATA_HOME"] = self.old_xdg
        self.tmp.cleanup()

    def test_load_index_normalizes_fields(self) -> None:
        self.fake.branch_rows = {
            "g/p/feature/x": {
                "key": "g/p/feature/x",
                "branch": "feature/x",
                "group_name": "g",
                "project": "p",
                "created_at": "1700000000",
                "created_by": "alice",
                "exists_origin": 1,
                "exists_local": 0,
                "merge_status": None,
                "diverged": 0,
                "stale_days": "5",
                "last_action": "create",
                "last_updated_at": "1700000001",
                "last_updated_by": "alice",
            }
        }
        index = load_index(self.base_path)
        rec = index["g/p/feature/x"]
        self.assertEqual(rec.branch, "feature/x")
        self.assertEqual(rec.group, "g")
        self.assertFalse(rec.exists_local)
        self.assertTrue(rec.exists_origin)
        self.assertEqual(rec.stale_days, 5)
        self.assertEqual(rec.last_updated_at, 1700000001)

    def test_record_activity_appends_entry(self) -> None:
        rec = BranchRecord(
            branch="feature/new",
            group="g",
            project="p",
            created_by="alice",
            exists_local=True,
        )
        branch_store.record_activity("create", rec)
        self.assertEqual(len(self.fake.activity_rows), 1)
        stored = self.fake.activity_rows[0]
        self.assertEqual(stored["action"], "create")
        self.assertEqual(stored["branch_key"], "g/p/feature/new")

    def test_load_activity_log_reads_from_backend(self) -> None:
        self.fake.activity_rows = [
            {
                "ts": 1700000001,
                "user": "bob",
                "group_name": "g",
                "project": "p",
                "branch": "feature/z",
                "action": "create",
                "result": "ok",
                "message": "created",
            }
        ]
        entries = load_activity_log(self.base_path)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["user"], "bob")
        self.assertEqual(entries[0]["action"], "create")

    def test_upsert_card_adds_version_prefix(self) -> None:
        now = int(time.time())
        sprint = Sprint(
            id=None,
            branch_key="ellis/proyecto/v2.68",
            qa_branch_key="ellis/proyecto/v2.68_QA",
            name="Sprint 3",
            version="2.68",
            created_at=now,
            created_by="alice",
            updated_at=now,
            updated_by="alice",
        )
        branch_store.upsert_sprint(sprint, path=self.base_path)
        card = Card(
            id=None,
            sprint_id=sprint.id,
            title="Tarjeta 1",
            ticket_id="ELASS-40",
            branch="feature/login",
            created_by="alice",
            updated_by="alice",
        )
        branch_store.upsert_card(card, path=self.base_path)
        expected_prefix = "v2.68_QA_ELASS-40"
        self.assertTrue(card.branch.startswith(expected_prefix))
        expected_key = "ellis/proyecto/v2.68_QA_ELASS-40_feature/login"
        self.assertEqual(card.branch_key, expected_key)
        stored = branch_store.list_cards(path=self.base_path, sprint_ids=[sprint.id])
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].branch, "v2.68_QA_ELASS-40_feature/login")
        self.assertEqual(stored[0].branch_key, expected_key)

    def test_card_branch_key_uses_qa_branch_scope(self) -> None:
        now = int(time.time())
        sprint = Sprint(
            id=None,
            branch_key="",
            qa_branch_key="ellis/proyecto/v2.68_QA",
            name="Sprint QA",
            version="2.68",
            created_at=now,
            created_by="alice",
            updated_at=now,
            updated_by="alice",
        )
        branch_store.upsert_sprint(sprint, path=self.base_path)
        card = Card(
            id=None,
            sprint_id=sprint.id,
            title="Tarjeta QA",
            ticket_id="ELASS-50",
            branch="feature/test",
            created_by="alice",
            updated_by="alice",
        )
        branch_store.upsert_card(card, path=self.base_path)
        expected_key = "ellis/proyecto/v2.68_QA_ELASS-50_feature/test"
        self.assertEqual(card.branch_key, expected_key)
        stored = branch_store.list_cards(path=self.base_path, sprint_ids=[sprint.id])
        self.assertEqual(stored[0].branch_key, expected_key)

    def test_card_urls_and_unmarking_reset_fields(self) -> None:
        now = int(time.time())
        sprint = Sprint(
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
        branch_store.upsert_sprint(sprint, path=self.base_path)
        card = Card(
            id=None,
            sprint_id=sprint.id,
            title="Tarjeta QA",
            ticket_id="ELASS-50",
            branch="feature/test",
            unit_tests_url="https://example.test/unit",
            qa_url="https://example.test/qa",
            created_by="alice",
            updated_by="alice",
        )
        branch_store.upsert_card(card, path=self.base_path)
        card.unit_tests_done = True
        card.unit_tests_by = "alice"
        card.unit_tests_at = now
        card.qa_done = True
        card.qa_by = "bob"
        card.qa_at = now
        branch_store.upsert_card(card, path=self.base_path)
        card.unit_tests_done = False
        card.unit_tests_by = None
        card.unit_tests_at = None
        card.qa_done = False
        card.qa_by = None
        card.qa_at = None
        branch_store.upsert_card(card, path=self.base_path)
        stored = branch_store.list_cards(path=self.base_path, sprint_ids=[sprint.id])[0]
        self.assertFalse(stored.unit_tests_done)
        self.assertIsNone(stored.unit_tests_by)
        self.assertIsNone(stored.unit_tests_at)
        self.assertFalse(stored.qa_done)
        self.assertIsNone(stored.qa_by)
        self.assertIsNone(stored.qa_at)

    def test_find_sprint_by_branch_key_returns_qa_match(self) -> None:
        now = int(time.time())
        sprint = Sprint(
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
        branch_store.upsert_sprint(sprint, path=self.base_path)
        found = branch_store.find_sprint_by_branch_key("ellis/proyecto/v2.68_QA", path=self.base_path)
        self.assertIsNotNone(found)
        if found:
            self.assertEqual(found.id, sprint.id)


if __name__ == "__main__":
    unittest.main()
