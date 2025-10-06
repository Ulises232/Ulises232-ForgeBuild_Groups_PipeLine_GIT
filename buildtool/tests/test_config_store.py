import json
import sqlite3
import unittest
from contextlib import contextmanager
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
    set_config_repo_factory,
    groups_for_user,
)
from buildtool.core.config_store import ConfigStore


class ConfigStoreMigrationTests(unittest.TestCase):
    class FakeBranchHistoryRepo:
        backend_name = "sqlite"

        def __init__(self, path: Path):
            self.path = Path(path)
            self.path.parent.mkdir(parents=True, exist_ok=True)

        @contextmanager
        def connection(self):
            conn = sqlite3.connect(self.path)
            conn.execute("PRAGMA foreign_keys = ON")
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()

    def _use_repo(self, db_path: Path) -> "ConfigStoreMigrationTests.FakeBranchHistoryRepo":
        repo = self.FakeBranchHistoryRepo(db_path)
        set_config_repo_factory(lambda: repo)
        self.addCleanup(set_config_repo_factory, None)
        return repo

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
                repo = self._use_repo(state_dir / "config.sqlite3")
                cfg = load_config()
                self.assertEqual(1, len(cfg.groups))
                self.assertEqual("G1", cfg.groups[0].key)
                self.assertEqual(1, len(cfg.groups[0].projects))

                data = yaml.safe_load(cfg_file.read_text())
                self.assertTrue("groups" not in data or not data["groups"])

                store = ConfigStore(repo=repo)
                stored = store.list_groups()
                self.assertEqual(1, len(stored))
                self.assertEqual("G1", stored[0].key)

    def test_upgrades_legacy_sprints_table_without_branch_key(self):
        with TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "config.sqlite3"
            with sqlite3.connect(db_path) as cx:
                cx.executescript(
                    """
                    CREATE TABLE sprints (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        version TEXT NOT NULL
                    );
                    INSERT INTO sprints (name, version) VALUES ('Sprint 1', '1.0');
                    """
                )

            store = ConfigStore(db_path)
            sprints = store.list_sprints()

            self.assertEqual(1, len(sprints))

            with sqlite3.connect(db_path) as cx:
                columns = {row[1] for row in cx.execute("PRAGMA table_info(sprints)")}
                self.assertIn("branch_key", columns)
                self.assertIn("metadata", columns)

    def test_save_config_persists_groups_into_store(self):
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state"
            with patch("buildtool.core.config._state_dir", return_value=state_dir), patch(
                "buildtool.core.config_store._state_dir", return_value=state_dir
            ):
                repo = self._use_repo(state_dir / "config.sqlite3")
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

                store = ConfigStore(repo=repo)
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
                repo = self._use_repo(state_dir / "config.sqlite3")
                cfg = Config(
                    paths=Paths(workspaces={}, output_base="/tmp/out", nas_dir=""),
                    groups=[group],
                )
                save_config(cfg)

                db_path = state_dir / "config.sqlite3"
                with sqlite3.connect(db_path) as cx:
                    cx.row_factory = sqlite3.Row
                    groups = cx.execute("SELECT * FROM config_groups").fetchall()
                    projects = cx.execute("SELECT * FROM config_projects").fetchall()
                    modules = cx.execute("SELECT * FROM config_project_modules").fetchall()
                    group_profiles = cx.execute("SELECT * FROM config_group_profiles").fetchall()
                    deploys = cx.execute("SELECT * FROM config_deploy_targets").fetchall()
                    deploy_profiles = cx.execute(
                        "SELECT * FROM config_deploy_target_profiles"
                    ).fetchall()

                self.assertEqual(1, len(groups))
                self.assertEqual(1, len(projects))
                self.assertEqual(1, len(modules))
                self.assertEqual(2, len(group_profiles))
                self.assertEqual(1, len(deploys))
                self.assertEqual(1, len(deploy_profiles))

                project_config = json.loads(projects[0]["config_json"])
                self.assertEqual(["dev", "qa"], project_config.get("profiles"))

    def test_user_overrides_are_combined_with_global_configuration(self):
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "state"
            with patch("buildtool.core.config._state_dir", return_value=state_dir), patch(
                "buildtool.core.config_store._state_dir", return_value=state_dir
            ):
                repo = self._use_repo(state_dir / "config.sqlite3")
                module = Module(name="mod", path="src/module")
                project = Project(key="PX", modules=[module], profiles=["dev", "qa"])
                deploy = DeployTarget(
                    name="app",
                    project_key="PX",
                    profiles=["dev"],
                    path_template="/deploy/{profile}",
                )
                group = Group(
                    key="GX",
                    repos={"PX": "/repos/global"},
                    output_base="/output/global",
                    profiles=["dev"],
                    projects=[project],
                    deploy_targets=[deploy],
                )

                cfg = Config(
                    paths=Paths(
                        workspaces={"PX": "/ws/global"},
                        output_base="/output/default",
                        nas_dir="",
                    ),
                    groups=[group],
                )
                save_config(cfg)

                store = ConfigStore(repo=repo)
                store.set_group_user_paths(
                    "GX",
                    "alice",
                    repos={"PX": "/repos/alice"},
                    output_base="/output/alice",
                )
                store.set_module_user_path("GX", "PX", "mod", "alice", "/src/alice")
                store.set_deploy_user_paths(
                    "GX",
                    "app",
                    "alice",
                    path_template="/deploy/alice/{profile}",
                    hotfix_path_template="/hotfix/alice/{profile}",
                )

                resolved = store.list_groups(username="alice")
                self.assertEqual(1, len(resolved))
                grp = resolved[0]
                self.assertEqual("/repos/alice", grp.repos["PX"])
                self.assertEqual("/output/alice", grp.output_base)
                self.assertEqual("/src/alice", grp.projects[0].modules[0].path)
                self.assertEqual("/deploy/alice/{profile}", grp.deploy_targets[0].path_template)
                self.assertEqual(
                    "/hotfix/alice/{profile}", grp.deploy_targets[0].hotfix_path_template
                )

                global_groups = store.list_groups()
                self.assertEqual("/repos/global", global_groups[0].repos["PX"])
                self.assertEqual("/output/global", global_groups[0].output_base)

                cfg_loaded = load_config()
                resolved_for_user = groups_for_user(cfg_loaded, username="alice")
                self.assertEqual("/repos/alice", resolved_for_user[0].repos["PX"])

                store = ConfigStore(repo=repo)
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
                repo = self._use_repo(state_dir / "config.sqlite3")
                cfg = Config(
                    paths=Paths(workspaces={}, output_base="/tmp/out", nas_dir=""),
                    groups=[initial_group],
                )
                save_config(cfg)

                db_path = state_dir / "config.sqlite3"
                cfg.groups = [updated_group]
                save_config(cfg)

                with sqlite3.connect(db_path) as cx:
                    project_row = cx.execute(
                        "SELECT id, config_json FROM config_projects"
                    ).fetchone()
                    module_row = cx.execute(
                        "SELECT id, goals FROM config_project_modules"
                    ).fetchone()
                    deploy_row = cx.execute(
                        "SELECT id FROM config_deploy_targets"
                    ).fetchone()
                    profiles = cx.execute(
                        "SELECT profile FROM config_group_profiles ORDER BY position"
                    ).fetchall()

                self.assertIsNotNone(project_row)
                self.assertIsNotNone(module_row)
                self.assertIsNotNone(deploy_row)
                self.assertEqual(["dev", "qa"], [p[0] for p in profiles])
                project_config = json.loads(project_row[1])
                self.assertEqual(["dev", "qa"], project_config.get("profiles"))
                self.assertEqual(["clean", "package"], json.loads(module_row[1]))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
