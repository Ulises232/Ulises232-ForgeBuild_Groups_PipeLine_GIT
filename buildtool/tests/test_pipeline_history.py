import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from buildtool.core.pipeline_history import PipelineHistory


class PipelineHistoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "history.sqlite3"
        os.environ["FORGEBUILD_HISTORY_DB"] = str(self.db_path)
        self.history = PipelineHistory()

    def tearDown(self) -> None:
        os.environ.pop("FORGEBUILD_HISTORY_DB", None)
        self.tmp.cleanup()

    def test_start_finish_and_logs(self) -> None:
        run_id = self.history.start_run(
            "build",
            user="tester",
            group_key="grp",
            project_key="proj",
            profiles=["dev", "qa"],
            modules=["core", "api"],
            version=None,
            hotfix=None,
        )
        self.history.log_message(run_id, "Iniciando")
        self.history.finish_run(run_id, "success", "Completado")

        runs = self.history.list_runs(pipeline="build")
        self.assertEqual(len(runs), 1)
        rec = runs[0]
        self.assertEqual(rec.pipeline, "build")
        self.assertEqual(rec.user, "tester")
        self.assertEqual(rec.group_key, "grp")
        self.assertIn("dev", rec.profiles)
        self.assertEqual(rec.status, "success")
        self.assertEqual(rec.message, "Completado")
        self.assertIsNone(rec.card_id)
        self.assertIsNone(rec.unit_tests_status)
        self.assertIsNone(rec.qa_status)

        logs = self.history.get_logs(run_id)
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0][1], "Iniciando")

        csv_path = Path(self.tmp.name) / "out.csv"
        self.history.export_csv(csv_path)
        self.assertTrue(csv_path.exists())

        self.history.clear()
        self.assertEqual(self.history.list_runs(), [])

    def test_legacy_database_adds_card_columns_and_indexes(self) -> None:
        legacy_db = Path(self.tmp.name) / "legacy.sqlite3"
        with sqlite3.connect(legacy_db) as cx:
            cx.executescript(
                """
                CREATE TABLE pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pipeline TEXT NOT NULL,
                    user TEXT,
                    group_key TEXT,
                    project_key TEXT,
                    profiles TEXT,
                    modules TEXT,
                    version TEXT,
                    hotfix INTEGER,
                    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    finished_at DATETIME,
                    status TEXT,
                    message TEXT
                );
                CREATE TABLE pipeline_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                    ts DATETIME DEFAULT CURRENT_TIMESTAMP,
                    message TEXT NOT NULL
                );
                """
            )

        history = PipelineHistory(legacy_db)

        with sqlite3.connect(legacy_db) as cx:
            columns = {
                row[1]
                for row in cx.execute("PRAGMA table_info(pipeline_runs)").fetchall()
            }
            self.assertIn("card_id", columns)
            self.assertIn("unit_tests_status", columns)
            self.assertIn("qa_status", columns)
            self.assertIn("approved_by", columns)

            indexes = {
                row[1]
                for row in cx.execute("PRAGMA index_list('pipeline_runs')").fetchall()
            }
            self.assertIn("ix_pipeline_runs_card", indexes)

        history.clear()


if __name__ == "__main__":
    unittest.main()
