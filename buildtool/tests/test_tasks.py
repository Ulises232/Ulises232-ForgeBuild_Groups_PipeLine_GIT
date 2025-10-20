import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

if "buildtool.core.branch_store" not in sys.modules:
    branch_store_stub = types.ModuleType("buildtool.core.branch_store")

    class _User:
        def __init__(self, username: str = "") -> None:
            self.username = username

    branch_store_stub.User = _User
    sys.modules["buildtool.core.branch_store"] = branch_store_stub

from buildtool.core.tasks import build_project_for_profile


class BuildProjectCleaningTests(unittest.TestCase):
    def test_cleans_destinations_when_module_becomes_optional(self):
        with TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            repo_dir = base_path / "repo"
            repo_dir.mkdir()
            target_dir = repo_dir / "target"
            target_dir.mkdir()
            artifact = target_dir / "artifact-jar-with-dependencies.jar"
            artifact.write_text("data", encoding="utf-8")

            output_dir = base_path / "output"
            module = SimpleNamespace(
                name="module",
                path=".",
                goals=["package"],
                optional=False,
                copy_to_profile_war=False,
                copy_to_profile_ui=False,
                copy_to_subfolder=None,
                rename_jar_to="app.jar",
                select_pattern=None,
                copy_to_root=False,
                run_once=False,
                no_profile=False,
                profile_override=None,
            )
            project = SimpleNamespace(
                key="proj",
                modules=[module],
                repo="repo",
                workspace=None,
            )
            group = SimpleNamespace(
                key="grp",
                projects=[project],
                repos={"repo": str(repo_dir)},
                output_base=str(output_dir),
                profiles=[],
                deploy_targets=[],
            )
            cfg = SimpleNamespace(
                groups=[group],
                paths=SimpleNamespace(
                    workspaces={"repo": str(repo_dir)},
                    output_base=str(output_dir),
                ),
                default_execution_mode="integrated",
            )

            logs: list[str] = []
            with patch("buildtool.core.tasks.run_maven", return_value=0):
                self.assertTrue(
                    build_project_for_profile(cfg, "proj", "qa", True, log_cb=logs.append)
                )

            dest_dir = output_dir / "proj" / "qa"
            copied = dest_dir / "app.jar"
            self.assertTrue(copied.exists())
            obsolete = dest_dir / "obsolete.txt"
            obsolete.write_text("stale", encoding="utf-8")

            module.optional = True
            with patch("buildtool.core.tasks.run_maven", return_value=0):
                self.assertTrue(
                    build_project_for_profile(cfg, "proj", "qa", False, log_cb=logs.append)
                )

            self.assertTrue(dest_dir.exists())
            self.assertFalse(obsolete.exists())
            self.assertFalse(copied.exists())
            self.assertEqual([], list(dest_dir.iterdir()))

    def test_filtered_modules_do_not_clean_other_outputs(self):
        with TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            repo_dir = base_path / "repo"
            repo_dir.mkdir()

            module_a_dir = repo_dir / "module_a"
            module_b_dir = repo_dir / "module_b"
            for module_dir, artifact_name in (
                (module_a_dir, "module_a.war"),
                (module_b_dir, "module_b.war"),
            ):
                target_dir = module_dir / "target"
                target_dir.mkdir(parents=True)
                (target_dir / artifact_name).write_text("data", encoding="utf-8")

            output_dir = base_path / "output"

            def _module(name: str) -> SimpleNamespace:
                return SimpleNamespace(
                    name=name,
                    path=name,
                    goals=["package"],
                    optional=False,
                    profile_override=None,
                    only_if_profile_equals=None,
                    copy_to_profile_war=True,
                    copy_to_profile_ui=False,
                    copy_to_subfolder=None,
                    rename_jar_to=None,
                    select_pattern=None,
                    copy_to_root=True,
                    run_once=False,
                    no_profile=False,
                    serial_across_profiles=False,
                )

            module_a = _module("module_a")
            module_b = _module("module_b")

            project = SimpleNamespace(
                key="proj",
                modules=[module_a, module_b],
                repo="repo",
                workspace=None,
            )
            group = SimpleNamespace(
                key="grp",
                projects=[project],
                repos={"repo": str(repo_dir)},
                output_base=str(output_dir),
                profiles=[],
                deploy_targets=[],
            )
            cfg = SimpleNamespace(
                groups=[group],
                paths=SimpleNamespace(
                    workspaces={"repo": str(repo_dir)},
                    output_base=str(output_dir),
                ),
                default_execution_mode="integrated",
            )

            logs: list[str] = []
            with patch("buildtool.core.tasks.run_maven", return_value=0):
                self.assertTrue(
                    build_project_for_profile(
                        cfg,
                        "proj",
                        "qa",
                        True,
                        log_cb=logs.append,
                        modules_filter={"module_a"},
                    )
                )

            dest_dir = output_dir / "proj" / "qa"
            first_artifact = dest_dir / "module_a.war"
            self.assertTrue(first_artifact.exists())

            with patch("buildtool.core.tasks.run_maven", return_value=0):
                self.assertTrue(
                    build_project_for_profile(
                        cfg,
                        "proj",
                        "qa",
                        True,
                        log_cb=logs.append,
                        modules_filter={"module_b"},
                    )
                )

            second_artifact = dest_dir / "module_b.war"
            self.assertTrue(first_artifact.exists())
            self.assertTrue(second_artifact.exists())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
