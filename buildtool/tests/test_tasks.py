import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

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
            cfg = SimpleNamespace(
                groups=[
                    SimpleNamespace(
                        key="grp",
                        projects=[project],
                        repos={"repo": str(repo_dir)},
                        output_base=str(output_dir),
                    )
                ],
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


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
