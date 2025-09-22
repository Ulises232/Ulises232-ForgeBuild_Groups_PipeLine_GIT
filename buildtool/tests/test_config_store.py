import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import yaml

from buildtool.core.config import Config, Paths, Group, Project, Module, load_config, save_config
from buildtool.core.config_store import ConfigStore


class ConfigStoreMigrationTests(unittest.TestCase):
    def test_migrates_groups_from_yaml_to_sqlite(self):
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state"
            cfg_file = state_dir / "config.yaml"
            cfg_file.parent.mkdir(parents=True, exist_ok=True)
            cfg_file.write_text(
                yaml.safe_dump(
                    {
                        "paths": {
                            "workspaces": {},
                            "output_base": "/tmp/output",
                            "nas_dir": "",
                        },
                        "groups": [
                            {
                                "key": "G1",
                                "repos": {},
                                "output_base": "",
                                "profiles": ["dev"],
                                "projects": [
                                    {
                                        "key": "P1",
                                        "modules": [],
                                    }
                                ],
                                "deploy_targets": [],
                            }
                        ],
                    },
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with patch("buildtool.core.config._state_dir", return_value=state_dir), patch(
                "buildtool.core.config_store._state_dir", return_value=state_dir
            ):
                cfg = load_config()
                self.assertEqual(1, len(cfg.groups))
                self.assertEqual("G1", cfg.groups[0].key)
                self.assertEqual(1, len(cfg.groups[0].projects))

                data = yaml.safe_load(cfg_file.read_text())
                self.assertTrue("groups" not in data or not data["groups"])

                store = ConfigStore(state_dir / "config.sqlite3")
                stored = store.list_groups()
                self.assertEqual(1, len(stored))
                self.assertEqual("G1", stored[0].key)

    def test_save_config_persists_groups_into_store(self):
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state"
            with patch("buildtool.core.config._state_dir", return_value=state_dir), patch(
                "buildtool.core.config_store._state_dir", return_value=state_dir
            ):
                cfg = Config(
                    paths=Paths(workspaces={}, output_base="/tmp/out", nas_dir=""),
                    groups=[
                        Group(
                            key="GX",
                            repos={},
                            output_base="",
                            profiles=[],
                            projects=[
                                Project(
                                    key="PX",
                                    modules=[Module(name="mod", path=".")],
                                )
                            ],
                        )
                    ],
                )
                save_config(cfg)

                store = ConfigStore(state_dir / "config.sqlite3")
                stored = store.list_groups()
                self.assertEqual(["GX"], [g.key for g in stored])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
