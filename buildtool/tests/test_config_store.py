import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import yaml

from buildtool.core.config import (
    Config,
    Paths,
    Group,
    Project,
    Module,
    DeployTarget,
    load_config,
    save_config,
)
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

    def test_store_persists_modules_profiles_and_targets(self):
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state"
            module = Module(
                name="mod",
                path="./module",
                goals=["clean", "package"],
                version_files=["pom.xml"],
                optional=True,
                copy_to_profile_war=True,
                copy_to_profile_ui=False,
                copy_to_root=True,
            )
            project = Project(
                key="PX",
                modules=[module],
                profiles=["dev", "qa"],
                execution_mode="integrated",
            )
            deploy = DeployTarget(
                name="app",
                project_key="PX",
                profiles=["dev"],
                path_template="/tmp/{profile}",
            )
            group = Group(
                key="GX",
                repos={"PX": "C:/repo"},
                output_base="/tmp/out",
                profiles=["dev", "qa"],
                projects=[project],
                deploy_targets=[deploy],
            )

            with patch("buildtool.core.config._state_dir", return_value=state_dir), patch(
                "buildtool.core.config_store._state_dir", return_value=state_dir
            ):
                cfg = Config(
                    paths=Paths(workspaces={}, output_base="/tmp/out", nas_dir=""),
                    groups=[group],
                )
                save_config(cfg)

                db_path = state_dir / "config.sqlite3"
                with sqlite3.connect(db_path) as cx:
                    cx.row_factory = sqlite3.Row
                    groups = cx.execute("SELECT * FROM groups").fetchall()
                    projects = cx.execute("SELECT * FROM projects").fetchall()
                    modules = cx.execute("SELECT * FROM project_modules").fetchall()
                    group_profiles = cx.execute("SELECT * FROM group_profiles").fetchall()
                    deploys = cx.execute("SELECT * FROM deploy_targets").fetchall()
                    deploy_profiles = cx.execute(
                        "SELECT * FROM deploy_target_profiles"
                    ).fetchall()

                self.assertEqual(1, len(groups))
                self.assertEqual(1, len(projects))
                self.assertEqual(1, len(modules))
                self.assertEqual(2, len(group_profiles))
                self.assertEqual(1, len(deploys))
                self.assertEqual(1, len(deploy_profiles))

                project_config = json.loads(projects[0]["config_json"])
                self.assertEqual(["dev", "qa"], project_config.get("profiles"))

                store = ConfigStore(db_path)
                stored = store.list_groups()
                self.assertEqual(["GX"], [g.key for g in stored])
                self.assertEqual(["PX"], [p.key for p in stored[0].projects])
                self.assertEqual(["dev", "qa"], stored[0].projects[0].profiles)
                self.assertEqual(["mod"], [m.name for m in stored[0].projects[0].modules])

    def test_replace_groups_updates_existing_rows(self):
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state"
            initial_group = Group(
                key="GX",
                repos={"PX": "C:/repo"},
                output_base="/tmp/out",
                profiles=["dev"],
                projects=[
                    Project(
                        key="PX",
                        modules=[Module(name="mod", path="./module")],
                        profiles=["dev"],
                    )
                ],
                deploy_targets=[
                    DeployTarget(
                        name="deploy",
                        project_key="PX",
                        profiles=["dev"],
                        path_template="/tmp/{profile}",
                    )
                ],
            )

            updated_group = Group(
                key="GX",
                repos={"PX": "D:/repo"},
                output_base="/tmp/new",
                profiles=["dev", "qa"],
                projects=[
                    Project(
                        key="PX",
                        modules=[
                            Module(
                                name="mod",
                                path="./module",
                                goals=["clean", "package"],
                            )
                        ],
                        profiles=["dev", "qa"],
                    )
                ],
                deploy_targets=[
                    DeployTarget(
                        name="deploy",
                        project_key="PX",
                        profiles=["qa", "dev"],
                        path_template="/tmp/{profile}",
                    )
                ],
            )

            with patch("buildtool.core.config._state_dir", return_value=state_dir), patch(
                "buildtool.core.config_store._state_dir", return_value=state_dir
            ):
                cfg = Config(
                    paths=Paths(workspaces={}, output_base="/tmp/out", nas_dir=""),
                    groups=[initial_group],
                )
                save_config(cfg)

                db_path = state_dir / "config.sqlite3"
                with sqlite3.connect(db_path) as cx:
                    project_id = cx.execute("SELECT id FROM projects").fetchone()[0]
                    module_id = cx.execute(
                        "SELECT id FROM project_modules"
                    ).fetchone()[0]
                    deploy_id = cx.execute("SELECT id FROM deploy_targets").fetchone()[0]

                cfg.groups = [updated_group]
                save_config(cfg)

                with sqlite3.connect(db_path) as cx:
                    project_row = cx.execute("SELECT id, config_json FROM projects").fetchone()
                    module_row = cx.execute(
                        "SELECT id, goals FROM project_modules"
                    ).fetchone()
                    deploy_row = cx.execute(
                        "SELECT id FROM deploy_targets"
                    ).fetchone()
                    profiles = cx.execute(
                        "SELECT profile FROM group_profiles ORDER BY position"
                    ).fetchall()

                self.assertEqual(project_id, project_row[0])
                self.assertEqual(module_id, module_row[0])
                self.assertEqual(deploy_id, deploy_row[0])
                self.assertEqual(["dev", "qa"], [p[0] for p in profiles])
                project_config = json.loads(project_row[1])
                self.assertEqual(["dev", "qa"], project_config.get("profiles"))
                self.assertEqual(["clean", "package"], json.loads(module_row[1]))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
