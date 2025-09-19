import tempfile
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


if __name__ == "__main__":
    unittest.main()
