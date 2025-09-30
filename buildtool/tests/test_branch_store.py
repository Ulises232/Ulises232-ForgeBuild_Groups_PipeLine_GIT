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
        self.branch_local_users: dict[tuple[str, str], dict] = {}
        self.activity_rows: list[dict] = []
        self.sprints: dict[int, dict] = {}
        self.cards: dict[int, dict] = {}
        self.users: dict[str, dict] = {}
        self.roles: dict[str, dict] = {}
        self.user_roles: list[dict] = []
        self.next_sprint_id = 1
        self.next_card_id = 1

    # Branches ---------------------------------------------------------
    def fetch_branches(self, filter_origin: bool = False, username: str | None = None) -> list[dict]:
        rows: list[dict] = []
        for row in self.branch_rows.values():
            if filter_origin and not row.get("exists_origin"):
                continue
            data = row.copy()
            entry = None
            if username:
                entry = self.branch_local_users.get((row.get("key"), username))
                is_owner = (
                    (row.get("created_by") or "") == username
                    or (row.get("last_updated_by") or "") == username
                )
                if not row.get("exists_origin") and not entry and not is_owner:
                    continue
            if entry:
                data["local_state"] = entry.get("state")
                data["local_location"] = entry.get("location")
                data["local_updated_at"] = entry.get("updated_at")
            else:
                data["local_state"] = None
                data["local_location"] = None
                data["local_updated_at"] = None
            rows.append(data)
        return rows

    def replace_branches(self, records: list[dict]) -> None:
        self.branch_rows = {rec["key"]: rec.copy() for rec in records}

    def upsert_branch(self, payload: dict) -> None:
        self.branch_rows[payload["key"]] = payload.copy()

    def delete_branch(self, key: str) -> None:
        self.branch_rows.pop(key, None)
        to_remove = [entry for entry in self.branch_local_users if entry[0] == key]
        for entry in to_remove:
            self.branch_local_users.pop(entry, None)

    def fetch_branch_local_users(
        self,
        branch_keys: list[str] | None = None,
        username: str | None = None,
    ) -> list[dict]:
        results: list[dict] = []
        keys = set(branch_keys or []) if branch_keys else None
        for (branch_key, user), payload in self.branch_local_users.items():
            if keys and branch_key not in keys:
                continue
            if username and user != username:
                continue
            entry = payload.copy()
            entry["branch_key"] = branch_key
            entry["username"] = user
            results.append(entry)
        return results

    def upsert_branch_local_user(
        self,
        branch_key: str,
        username: str,
        state: str,
        location: str | None,
        updated_at: int,
    ) -> None:
        self.branch_local_users[(branch_key, username)] = {
            "state": state,
            "location": location,
            "updated_at": updated_at,
        }

    def delete_branch_local_user(self, branch_key: str, username: str) -> None:
        self.branch_local_users.pop((branch_key, username), None)

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
        rows: list[dict] = []
        for data in self.users.values():
            row = data.copy()
            row.setdefault("active", 1)
            row.setdefault("require_password_reset", 0)
            row.setdefault("password_changed_at", None)
            row.setdefault("active_since", None)
            row.setdefault("password_hash", data.get("password_hash"))
            row.setdefault("has_password", 1 if data.get("password_hash") else 0)
            rows.append(row)
        return rows

    def fetch_user(self, username: str) -> dict | None:
        data = self.users.get(username)
        return data.copy() if data else None

    def upsert_user(self, payload: dict) -> None:
        stored = self.users.get(payload["username"], {}).copy()
        stored.update(payload)
        stored.setdefault("require_password_reset", 0)
        stored.setdefault("active", 1)
        stored.setdefault("password_hash", None)
        stored.setdefault("password_salt", None)
        stored.setdefault("password_algo", None)
        stored.setdefault("password_changed_at", None)
        stored.setdefault("active_since", int(time.time()))
        self.users[payload["username"]] = stored

    def update_user_password(
        self,
        username: str,
        *,
        password_hash: str | None,
        password_salt: str | None,
        password_algo: str | None,
        password_changed_at: int | None,
        require_password_reset: bool,
    ) -> None:
        user = self.users.setdefault(username, {})
        user["password_hash"] = password_hash
        user["password_salt"] = password_salt
        user["password_algo"] = password_algo
        user["password_changed_at"] = password_changed_at
        user["require_password_reset"] = 1 if require_password_reset else 0

    def mark_password_reset(self, username: str, require_password_reset: bool) -> None:
        user = self.users.setdefault(username, {})
        user["require_password_reset"] = 1 if require_password_reset else 0

    def set_user_active(self, username: str, active: bool, *, timestamp: int | None = None) -> None:
        user = self.users.setdefault(username, {})
        user["active"] = 1 if active else 0
        if active and timestamp:
            user["active_since"] = timestamp

    def update_user_profile(self, username: str, display_name: str | None, email: str | None) -> None:
        user = self.users.setdefault(username, {})
        if display_name is not None:
            user["display_name"] = display_name
        if email is not None:
            user["email"] = email

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
        self.old_username = os.environ.get("USERNAME")
        os.environ["APPDATA"] = self.tmp.name
        os.environ["XDG_DATA_HOME"] = self.tmp.name
        os.environ["USERNAME"] = "alice"
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
        if self.old_username is None:
            os.environ.pop("USERNAME", None)
        else:
            os.environ["USERNAME"] = self.old_username
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
        self.assertFalse(rec.has_local_copy())
        self.assertTrue(rec.exists_origin)
        self.assertEqual(rec.stale_days, 5)
        self.assertEqual(rec.last_updated_at, 1700000001)

    def test_load_index_hides_local_only_branches_for_other_users(self) -> None:
        key = "g/p/feature/local"
        self.fake.branch_rows = {
            key: {
                "key": key,
                "branch": "feature/local",
                "group_name": "g",
                "project": "p",
                "created_at": int(time.time()),
                "created_by": "alice",
                "exists_origin": 0,
                "exists_local": 1,
                "merge_status": None,
                "diverged": 0,
                "stale_days": None,
                "last_action": "create",
                "last_updated_at": int(time.time()),
                "last_updated_by": "alice",
            }
        }
        self.fake.branch_local_users[(key, "alice")] = {
            "state": "present",
            "location": None,
            "updated_at": int(time.time()),
        }

        original_user = os.environ.get("USERNAME", "")
        try:
            os.environ["USERNAME"] = "alice"
            owner_index = load_index(self.base_path)
            self.assertIn(key, owner_index)

            os.environ["USERNAME"] = "bob"
            other_index = load_index(self.base_path)
            self.assertNotIn(key, other_index)
        finally:
            os.environ["USERNAME"] = original_user or "alice"

    def test_record_activity_appends_entry(self) -> None:
        rec = BranchRecord(
            branch="feature/new",
            group="g",
            project="p",
            created_by="alice",
        )
        rec.mark_local(True)
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

    def test_upsert_registers_local_user_state(self) -> None:
        rec = BranchRecord(branch="feature/state", group="g", project="p", created_by="alice")
        rec.mark_local(True)
        branch_store.upsert(rec, action="test")

        entry = self.fake.branch_local_users.get(("g/p/feature/state", "alice"))
        self.assertIsNotNone(entry)
        assert entry  # for type checker
        self.assertEqual(entry["state"], "present")
        self.assertGreater(entry["updated_at"], 0)

    def test_save_index_synchronizes_local_states(self) -> None:
        rec_a = BranchRecord(branch="feature/a", group="g", project="p", created_by="alice")
        rec_a.mark_local(True)
        rec_b = BranchRecord(branch="feature/b", group="g", project="p", created_by="alice")
        rec_b.mark_local(False)
        index = {
            rec_a.key(): rec_a,
            rec_b.key(): rec_b,
        }
        branch_store.save_index(index, path=self.base_path)

        states = branch_store.load_local_states(username="alice", path=self.base_path)
        state_map = {(entry["branch_key"], entry["username"]): entry for entry in states}
        self.assertIn(("g/p/feature/a", "alice"), state_map)
        self.assertIn(("g/p/feature/b", "alice"), state_map)
        self.assertEqual(state_map[("g/p/feature/a", "alice")]["state"], "present")
        self.assertEqual(state_map[("g/p/feature/b", "alice")]["state"], "absent")

    def test_create_user_without_password_sets_reset_flag(self) -> None:
        branch_store.create_user("ana", "Ana Pruebas")
        users = branch_store.list_users()
        self.assertEqual(len(users), 1)
        user = users[0]
        self.assertFalse(user.has_password)
        self.assertTrue(user.require_password_reset)

    def test_set_user_password_allows_authentication(self) -> None:
        branch_store.create_user("ana", "Ana Pruebas")
        result = branch_store.authenticate_user("ana", None)
        self.assertEqual(result.status, "password_required")
        branch_store.set_user_password("ana", "Segura!1A")
        ok = branch_store.authenticate_user("ana", "Segura!1A")
        self.assertTrue(ok.success)

    def test_authenticate_requires_reset_when_flagged(self) -> None:
        branch_store.create_user("ana", "Ana Pruebas", password="Segura!1A")
        branch_store.mark_user_password_reset("ana", require_reset=True)
        status = branch_store.authenticate_user("ana", "Segura!1A")
        self.assertEqual(status.status, "reset_required")

    def test_update_user_can_disable_account(self) -> None:
        branch_store.create_user("ana", "Ana Pruebas", password="Segura!1A")
        branch_store.update_user("ana", active=False)
        self.assertFalse(branch_store.list_users())
        users = branch_store.list_users(include_inactive=True)
        self.assertFalse(users[0].active)
        result = branch_store.authenticate_user("ana", "Segura!1A")
        self.assertEqual(result.status, "disabled")

    def test_create_user_assigns_roles(self) -> None:
        branch_store.create_user("ana", "Ana Pruebas", roles=["developer", "qa"])
        roles = branch_store.list_user_roles("ana")
        self.assertEqual(set(roles.get("ana", [])), {"developer", "qa"})

    def test_remove_branch_deletes_backend_rows(self) -> None:
        rec = BranchRecord(branch="feature/remove", group="g", project="p", created_by="alice")
        rec.mark_local(True)
        branch_store.upsert(rec)

        key = rec.key()
        self.assertIn(key, self.fake.branch_rows)
        self.assertIn((key, "alice"), self.fake.branch_local_users)

        branch_store.remove(rec)

        self.assertNotIn(key, self.fake.branch_rows)
        self.assertNotIn((key, "alice"), self.fake.branch_local_users)

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
