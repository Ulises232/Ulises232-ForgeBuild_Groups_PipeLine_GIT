import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from buildtool.core import discover
from buildtool.core.config import Config, Paths, Group, Project, Module


class DiscoverTests(unittest.TestCase):
    def test_iter_cfg_entries_uses_user_module_overrides(self):
        base_cfg = Config(
            paths=Paths(workspaces={}, output_base="", nas_dir=""),
            groups=[],
        )
        with TemporaryDirectory() as tmpdir:
            global_path = Path(tmpdir) / "global_mod"
            user_path = Path(tmpdir) / "user_mod"

            project = Project(
                key="PRJ",
                modules=[Module(name="mod", path=str(global_path))],
            )
            group = Group(
                key="GRP",
                repos={},
                output_base="",
                profiles=[],
                projects=[project],
                deploy_targets=[],
            )
            base_cfg.groups = [group]

            override_project = Project(
                key="PRJ",
                modules=[Module(name="mod", path=str(user_path))],
            )
            override_group = Group(
                key="GRP",
                repos={},
                output_base="",
                profiles=[],
                projects=[override_project],
                deploy_targets=[],
            )

            with patch("buildtool.core.discover.groups_for_user", return_value=[override_group]):
                entries = discover._iter_cfg_entries(base_cfg, "GRP", "PRJ")

        resolved_paths = {path for _, path in entries}
        self.assertIn(user_path.resolve(strict=False), resolved_paths)
        self.assertNotIn(global_path.resolve(strict=False), resolved_paths)


if __name__ == "__main__":
    unittest.main()
