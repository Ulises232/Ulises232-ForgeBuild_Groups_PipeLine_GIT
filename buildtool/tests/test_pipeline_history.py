import os
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

        logs = self.history.get_logs(run_id)
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0][1], "Iniciando")

        csv_path = Path(self.tmp.name) / "out.csv"
        self.history.export_csv(csv_path)
        self.assertTrue(csv_path.exists())

        self.history.clear()
        self.assertEqual(self.history.list_runs(), [])


if __name__ == "__main__":
    unittest.main()
