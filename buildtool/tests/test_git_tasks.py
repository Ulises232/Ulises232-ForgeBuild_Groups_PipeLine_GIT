import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from buildtool.core.gitwrap import GitResult
import buildtool.core.git_tasks as git_tasks


class MergeIntoCurrentBranchTests(unittest.TestCase):
    def test_fetch_failure_aborts_remote_branch_setup(self):
        cfg = SimpleNamespace()
        repo_path = Path("/tmp/repo")
        module_tuple = (None, SimpleNamespace(key="proj"), SimpleNamespace(name="mod"), repo_path)
        logs: list[str] = []

        with patch.object(git_tasks, "_iter_modules", return_value=[module_tuple]), \
             patch.object(git_tasks, "current_branch", return_value="main"), \
             patch.object(git_tasks, "fetch", return_value=GitResult(1, "fetch failed")) as fetch_mock, \
             patch.object(git_tasks, "local_branch_exists") as local_exists_mock, \
             patch.object(git_tasks, "remote_branch_exists") as remote_exists_mock, \
             patch.object(git_tasks, "checkout") as checkout_mock, \
             patch.object(git_tasks, "merge_into_current") as merge_mock, \
             patch.object(git_tasks, "push_current"):
            git_tasks.merge_into_current_branch(cfg, None, None, "feature", False, logs.append)

        fetch_mock.assert_called_once_with(str(repo_path))
        local_exists_mock.assert_not_called()
        remote_exists_mock.assert_not_called()
        checkout_mock.assert_not_called()
        merge_mock.assert_not_called()
        self.assertTrue(any("fetch falló" in entry for entry in logs))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
