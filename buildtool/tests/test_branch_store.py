import sqlite3
import tempfile
import time
from pathlib import Path
import unittest

import json

from buildtool.core import branch_store
from buildtool.core.branch_history_db import BranchHistoryDB
from buildtool.core.branch_store import load_index, load_activity_log


class LoadIndexTest(unittest.TestCase):

    def setUp(self):
        branch_store._DB_CACHE.clear()

    def tearDown(self):
        branch_store._DB_CACHE.clear()

    def _write_index(self, base: Path, payload: dict) -> Path:
        path = base / "branches_index.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    def test_load_index_ignores_unknown_fields(self):
        payload = {
            "version": 1,
            "items": [
                {
                    "branch": "feature/x",
                    "group": "g",
                    "project": "p",
                    "exists_origin": True,
                    "exists_local": False,
                    "extra": "ignore-me",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            path = self._write_index(base, payload)
            index = load_index(path)
        self.assertIn("g/p/feature/x", index)

    def test_load_index_accepts_legacy_names_and_types(self):
        payload = {
            "version": 1,
            "items": [
                {
                    "branch": "feature/y",
                    "group": "g",
                    "project": "p",
                    "exists_origin": "yes",
                    "exists_local": "no",
                    "last_update": "1700000000",
                    "last_update_by": "alice",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            path = self._write_index(base, payload)
            index = load_index(path)
        rec = index.get("g/p/feature/y")
        self.assertIsNotNone(rec)
        if rec:
            self.assertTrue(rec.exists_origin)
            self.assertFalse(rec.exists_local)
            self.assertEqual(rec.last_updated_at, 1700000000)
            self.assertEqual(rec.last_updated_by, "alice")

    def test_migration_renames_index_after_import(self):
        payload = {
            "version": 1,
            "items": [
                {
                    "branch": "feature/migrated",
                    "group": "g",
                    "project": "p",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            path = self._write_index(base, payload)
            index = load_index(path)
            migrated = path.with_suffix(path.suffix + ".migrated")
            self.assertIn("g/p/feature/migrated", index)
            self.assertFalse(path.exists())
            self.assertTrue(migrated.exists())

    def test_load_activity_log_reads_from_sqlite(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db = BranchHistoryDB(base / "branches_history.sqlite3")
            db.append_activity(
                [
                    {
                        "ts": 1700000001,
                        "user": "bob",
                        "group": "g",
                        "project": "p",
                        "branch": "feature/z",
                        "action": "create",
                        "result": "ok",
                        "message": "created",
                    }
                ]
            )
            entries = load_activity_log(base)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["action"], "create")
        self.assertEqual(entries[0]["user"], "bob")

    def test_migrates_activity_log_branch_key(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "branches_history.sqlite3"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE activity_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts INTEGER NOT NULL,
                        user TEXT,
                        group_name TEXT,
                        project TEXT,
                        branch TEXT,
                        action TEXT,
                        result TEXT,
                        message TEXT
                    );
                    INSERT INTO activity_log (
                        ts, user, group_name, project, branch, action, result, message
                    ) VALUES (1700000002, 'carol', 'g', 'p', 'feature/old', 'merge', 'ok', 'done');
                    """
                )
            BranchHistoryDB(db_path)
            with sqlite3.connect(db_path) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(activity_log)")}
                self.assertIn("branch_key", columns)
                row = conn.execute("SELECT branch_key FROM activity_log").fetchone()
        self.assertEqual(row[0], "g/p/feature/old")

    def test_upgrades_legacy_sprints_schema(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "branches_history.sqlite3"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE sprints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        version TEXT NOT NULL
                    );
                    INSERT INTO sprints (name, version) VALUES ('Sprint 1', '1.0');
                    """
                )

            BranchHistoryDB(db_path)

            with sqlite3.connect(db_path) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(sprints)")}
                expected = {
                    "id",
                    "branch_key",
                    "name",
                    "version",
                    "lead_user",
                    "qa_user",
                    "description",
                    "created_at",
                    "created_by",
                    "updated_at",
                    "updated_by",
                }
                self.assertTrue(expected.issubset(columns))
                row = conn.execute(
                    "SELECT name, version, branch_key, lead_user FROM sprints"
                ).fetchone()

        self.assertEqual(row[0], "Sprint 1")
        self.assertEqual(row[1], "1.0")
        self.assertEqual(row[2], "")
        self.assertIsNone(row[3])

    def test_upgrades_legacy_cards_schema(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "branches_history.sqlite3"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE sprints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        version TEXT NOT NULL
                    );
                    INSERT INTO sprints (name, version) VALUES ('Sprint 1', '1.0');
                    CREATE TABLE cards (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sprint_id INTEGER NOT NULL,
                        title TEXT NOT NULL,
                        branch TEXT NOT NULL
                    );
                    INSERT INTO cards (sprint_id, title, branch)
                    VALUES (1, 'Card 1', 'feature/foo');
                    """
                )

            BranchHistoryDB(db_path)

            with sqlite3.connect(db_path) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(cards)")}
                expected = {
                    "id",
                    "sprint_id",
                    "title",
                    "branch",
                    "assignee",
                    "qa_assignee",
                    "description",
                    "unit_tests_done",
                    "qa_done",
                    "unit_tests_by",
                    "qa_by",
                    "unit_tests_at",
                    "qa_at",
                    "status",
                }
                self.assertTrue(expected.issubset(columns))
                row = conn.execute(
                    "SELECT title, status, unit_tests_done, qa_done FROM cards"
                ).fetchone()

        self.assertEqual(row[0], "Card 1")
        self.assertEqual(row[1], "pending")
        self.assertEqual(row[2], 0)
        self.assertEqual(row[3], 0)

    def test_upsert_card_adds_version_prefix(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            now = int(time.time())
            sprint = branch_store.Sprint(
                id=None,
                branch_key="ellis/proyecto/v2.68",
                name="Sprint 3",
                version="2.68",
                created_at=now,
                created_by="alice",
                updated_at=now,
                updated_by="alice",
            )
            branch_store.upsert_sprint(sprint, path=base)
            self.assertIsNotNone(sprint.id)
            card = branch_store.Card(
                id=None,
                sprint_id=sprint.id,
                title="Tarjeta 1",
                branch="feature/login",
            )
            branch_store.upsert_card(card, path=base)
            self.assertTrue(card.branch.startswith("v2.68_"))
            stored = branch_store.list_cards(path=base, sprint_ids=[sprint.id])
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].branch, "v2.68_feature/login")


if __name__ == "__main__":
    unittest.main()
