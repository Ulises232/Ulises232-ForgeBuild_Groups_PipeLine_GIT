from __future__ import annotations

import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional, Any, Dict

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS groups (
    key TEXT PRIMARY KEY,
    position INTEGER NOT NULL,
    output_base TEXT NOT NULL,
    config_json TEXT DEFAULT '{}',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TRIGGER IF NOT EXISTS trg_groups_updated
AFTER UPDATE ON groups
FOR EACH ROW
BEGIN
    UPDATE groups SET updated_at = CURRENT_TIMESTAMP WHERE key = OLD.key;
END;
CREATE TABLE IF NOT EXISTS group_repos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_key TEXT NOT NULL,
    repo_key TEXT NOT NULL,
    path TEXT NOT NULL,
    FOREIGN KEY(group_key) REFERENCES groups(key) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS group_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_key TEXT NOT NULL,
    position INTEGER NOT NULL,
    profile TEXT NOT NULL,
    FOREIGN KEY(group_key) REFERENCES groups(key) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_key TEXT NOT NULL,
    project_key TEXT NOT NULL,
    position INTEGER NOT NULL,
    execution_mode TEXT,
    workspace TEXT,
    repo TEXT,
    config_json TEXT DEFAULT '{}',
    FOREIGN KEY(group_key) REFERENCES groups(key) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_group_key ON projects(group_key, project_key);
CREATE TABLE IF NOT EXISTS project_modules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    version_files TEXT DEFAULT '[]',
    goals TEXT DEFAULT '[]',
    optional INTEGER NOT NULL DEFAULT 0,
    profile_override TEXT,
    only_if_profile_equals TEXT,
    copy_to_profile_war INTEGER NOT NULL DEFAULT 0,
    copy_to_profile_ui INTEGER NOT NULL DEFAULT 0,
    copy_to_subfolder TEXT,
    rename_jar_to TEXT,
    no_profile INTEGER NOT NULL DEFAULT 0,
    run_once INTEGER NOT NULL DEFAULT 0,
    select_pattern TEXT,
    serial_across_profiles INTEGER NOT NULL DEFAULT 0,
    copy_to_root INTEGER NOT NULL DEFAULT 0,
    config_json TEXT DEFAULT '{}',
    FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS deploy_targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_key TEXT NOT NULL,
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    project_key TEXT NOT NULL,
    path_template TEXT NOT NULL,
    hotfix_path_template TEXT,
    config_json TEXT DEFAULT '{}',
    FOREIGN KEY(group_key) REFERENCES groups(key) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS deploy_target_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    profile TEXT NOT NULL,
    FOREIGN KEY(target_id) REFERENCES deploy_targets(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS sprints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_key TEXT NOT NULL,
    sprint_key TEXT NOT NULL,
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    goal TEXT,
    start_date TEXT,
    end_date TEXT,
    config_json TEXT DEFAULT '{}',
    FOREIGN KEY(group_key) REFERENCES groups(key) ON DELETE CASCADE,
    UNIQUE(group_key, sprint_key)
);
CREATE TABLE IF NOT EXISTS cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sprint_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    card_key TEXT NOT NULL,
    title TEXT NOT NULL,
    project_key TEXT,
    version TEXT,
    owners TEXT DEFAULT '[]',
    tests_ready INTEGER NOT NULL DEFAULT 0,
    qa_ready INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    config_json TEXT DEFAULT '{}',
    FOREIGN KEY(sprint_id) REFERENCES sprints(id) ON DELETE CASCADE,
    UNIQUE(sprint_id, card_key)
);
"""


_ENV_DB_PATH = "FORGEBUILD_CONFIG_DB"
_DB_FILENAME = "config.sqlite3"


def _state_dir() -> Path:
    base = os.environ.get("APPDATA")
    if base:
        return Path(base) / "ForgeBuild"
    return Path.home() / ".forgebuild"


def _default_db_path() -> Path:
    return _state_dir() / _DB_FILENAME


def _serialize_model(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    if isinstance(obj, dict):
        return obj
    raise TypeError(f"Cannot serialize object of type {type(obj)!r}")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: Optional[str], default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


class ConfigStore:
    """Persistencia seccionada de grupos, proyectos y despliegues en SQLite."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        resolved: Path
        if db_path is not None:
            resolved = Path(db_path)
        else:
            env_path = os.environ.get(_ENV_DB_PATH)
            if env_path:
                resolved = Path(env_path)
            else:
                resolved = _default_db_path()

        default_db = _default_db_path()

        try:
            resolved.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RuntimeError(
                f"No se pudo preparar el directorio para la base de configuración en '{resolved.parent}': {exc}"
            ) from exc

        if (
            resolved != default_db
            and not resolved.exists()
            and default_db.exists()
        ):
            try:
                shutil.copy2(default_db, resolved)
            except OSError as exc:
                raise RuntimeError(
                    f"No se pudo copiar la base de configuración existente hacia '{resolved}': {exc}"
                ) from exc

        self.db_path = resolved
        os.environ[_ENV_DB_PATH] = str(self.db_path)

        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            legacy_groups = self._extract_legacy_groups(cx)
            self._inline_project_profiles(cx)
            cx.executescript(SCHEMA)
            if legacy_groups:
                self._replace_groups_with_connection(cx, legacy_groups)
            cx.commit()

    # ------------------------------------------------------------------
    def _extract_legacy_groups(self, cx: sqlite3.Connection) -> List[Any]:
        cursor = cx.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='groups'"
        )
        if not cursor.fetchone():
            return []
        info = cx.execute("PRAGMA table_info(groups)").fetchall()
        columns = {row[1] for row in info}
        if "data" not in columns:
            return []
        rows = cx.execute("SELECT data FROM groups ORDER BY key").fetchall()
        legacy_data: List[Any] = []
        for (raw,) in rows:
            try:
                legacy_data.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
        cx.executescript(
            """
DROP TRIGGER IF EXISTS trg_groups_updated;
DROP TABLE IF EXISTS groups;
"""
        )
        return legacy_data

    # ------------------------------------------------------------------
    def _inline_project_profiles(self, cx: sqlite3.Connection) -> None:
        cursor = cx.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='project_profiles'"
        )
        if not cursor.fetchone():
            return

        rows = cx.execute(
            "SELECT project_id, profile FROM project_profiles ORDER BY project_id, position"
        ).fetchall()
        profiles_by_project: Dict[int, List[str]] = {}
        for project_id, profile in rows:
            profiles_by_project.setdefault(int(project_id), []).append(str(profile))

        for project_id, profiles in profiles_by_project.items():
            current = cx.execute(
                "SELECT config_json FROM projects WHERE id = ?",
                (int(project_id),),
            ).fetchone()
            if not current:
                continue
            config = _json_loads(current[0], {})
            config["profiles"] = profiles
            cx.execute(
                "UPDATE projects SET config_json = ? WHERE id = ?",
                (_json_dumps(config), int(project_id)),
            )

        cx.execute("DROP TABLE IF EXISTS project_profiles")

    # ------------------------------------------------------------------
    def _replace_groups_with_connection(
        self, cx: sqlite3.Connection, groups: Iterable[Any]
    ) -> None:
        from .config import Group  # import diferido

        payload: List[Group] = []
        for group in groups:
            if group is None:
                continue
            if isinstance(group, Group):
                payload.append(group)
            else:
                try:
                    payload.append(Group(**group))
                except Exception:
                    continue
        self._clear_all(cx)
        for position, group in enumerate(payload):
            self._insert_group(cx, group, position)


    # ------------------------------------------------------------------
    def _clear_all(self, cx: sqlite3.Connection) -> None:
        cx.execute("DELETE FROM groups")

    # ------------------------------------------------------------------
    def _next_group_position(self, cx: sqlite3.Connection) -> int:
        row = cx.execute("SELECT MAX(position) FROM groups").fetchone()
        if not row or row[0] is None:
            return 0
        return int(row[0]) + 1

    # ------------------------------------------------------------------
    def _next_sprint_position(self, cx: sqlite3.Connection, group_key: str) -> int:
        row = cx.execute(
            "SELECT MAX(position) FROM sprints WHERE group_key = ?",
            (str(group_key),),
        ).fetchone()
        if not row or row[0] is None:
            return 0
        return int(row[0]) + 1

    # ------------------------------------------------------------------
    def _next_card_position(self, cx: sqlite3.Connection, sprint_id: int) -> int:
        row = cx.execute(
            "SELECT MAX(position) FROM cards WHERE sprint_id = ?",
            (int(sprint_id),),
        ).fetchone()
        if not row or row[0] is None:
            return 0
        return int(row[0]) + 1

    # ------------------------------------------------------------------
    def _delete_group_rows(self, cx: sqlite3.Connection, key: str) -> None:
        cx.execute("DELETE FROM groups WHERE key = ?", (key,))

    # ------------------------------------------------------------------
    def _insert_group(self, cx: sqlite3.Connection, group: Any, position: int) -> None:
        data = _serialize_model(group)
        key = data.get("key")
        if not key:
            return
        repos = data.get("repos") or {}
        profiles = list(data.get("profiles") or [])
        projects = list(data.get("projects") or [])
        deploy_targets = list(data.get("deploy_targets") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"key", "repos", "output_base", "profiles", "projects", "deploy_targets"}
        }

        cx.execute(
            "INSERT INTO groups(key, position, output_base, config_json) "
            "VALUES(?, ?, ?, ?)",
            (
                str(key),
                int(position),
                str(data.get("output_base", "")),
                _json_dumps(extras) if extras else "{}",
            ),
        )

        if repos:
            cx.executemany(
                "INSERT INTO group_repos(group_key, repo_key, path) VALUES(?, ?, ?)",
                [(str(key), str(repo_key), str(path)) for repo_key, path in repos.items()],
            )

        for idx, profile in enumerate(profiles):
            cx.execute(
                "INSERT INTO group_profiles(group_key, position, profile) VALUES(?, ?, ?)",
                (str(key), int(idx), str(profile)),
            )

        for proj_idx, project in enumerate(projects):
            self._insert_project(cx, str(key), project, proj_idx)

        for dep_idx, deploy in enumerate(deploy_targets):
            self._insert_deploy_target(cx, str(key), deploy, dep_idx)

    # ------------------------------------------------------------------
    def _insert_project(
        self, cx: sqlite3.Connection, group_key: str, project: Any, position: int
    ) -> Optional[int]:
        data = _serialize_model(project)
        key = data.get("key")
        if not key:
            return None
        modules = list(data.get("modules") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"key", "modules", "execution_mode", "workspace", "repo"}
        }

        cursor = cx.execute(
            "INSERT INTO projects("
            "group_key, project_key, position, execution_mode, workspace, repo, config_json"
            ") VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                group_key,
                str(key),
                int(position),
                data.get("execution_mode"),
                data.get("workspace"),
                data.get("repo"),
                _json_dumps(extras) if extras else "{}",
            ),
        )
        project_id = int(cursor.lastrowid)

        for mod_idx, module in enumerate(modules):
            self._insert_module(cx, project_id, module, mod_idx)

        return project_id

    # ------------------------------------------------------------------
    def _update_group(self, cx: sqlite3.Connection, group: Any, position: int) -> None:
        data = _serialize_model(group)
        key = data.get("key")
        if not key:
            return

        repos = data.get("repos") or {}
        profiles = list(data.get("profiles") or [])
        projects = list(data.get("projects") or [])
        deploy_targets = list(data.get("deploy_targets") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"key", "repos", "output_base", "profiles", "projects", "deploy_targets"}
        }

        cx.execute(
            "UPDATE groups SET position = ?, output_base = ?, config_json = ? WHERE key = ?",
            (
                int(position),
                str(data.get("output_base", "")),
                _json_dumps(extras) if extras else "{}",
                str(key),
            ),
        )

        # repos
        current_repos = {
            row[0]: row[1]
            for row in cx.execute(
                "SELECT repo_key, path FROM group_repos WHERE group_key = ?",
                (str(key),),
            )
        }
        repos_to_remove = set(current_repos) - set(repos)
        if repos_to_remove:
            cx.executemany(
                "DELETE FROM group_repos WHERE group_key = ? AND repo_key = ?",
                [(str(key), str(repo_key)) for repo_key in repos_to_remove],
            )
        for repo_key, path in repos.items():
            if repo_key in current_repos:
                cx.execute(
                    "UPDATE group_repos SET path = ? WHERE group_key = ? AND repo_key = ?",
                    (str(path), str(key), str(repo_key)),
                )
            else:
                cx.execute(
                    "INSERT INTO group_repos(group_key, repo_key, path) VALUES(?, ?, ?)",
                    (str(key), str(repo_key), str(path)),
                )

        # profiles
        current_profiles = {
            str(row[1]): int(row[0])
            for row in cx.execute(
                "SELECT id, profile FROM group_profiles WHERE group_key = ?",
                (str(key),),
            ).fetchall()
        }
        seen_profile_ids = set()
        for idx, profile in enumerate(profiles):
            profile_key = str(profile)
            if profile_key in current_profiles:
                profile_id = current_profiles[profile_key]
                seen_profile_ids.add(profile_id)
                cx.execute(
                    "UPDATE group_profiles SET position = ?, profile = ? WHERE id = ?",
                    (int(idx), profile_key, int(profile_id)),
                )
            else:
                cursor = cx.execute(
                    "INSERT INTO group_profiles(group_key, position, profile) VALUES(?, ?, ?)",
                    (str(key), int(idx), profile_key),
                )
                seen_profile_ids.add(int(cursor.lastrowid))

        if current_profiles:
            to_delete = [
                (int(profile_id),)
                for profile_id in current_profiles.values()
                if profile_id not in seen_profile_ids
            ]
            if to_delete:
                cx.executemany(
                    "DELETE FROM group_profiles WHERE id = ?",
                    to_delete,
                )

        # projects
        old_row_factory = cx.row_factory
        try:
            cx.row_factory = sqlite3.Row
            current_projects = {
                row["project_key"]: row
                for row in cx.execute(
                    "SELECT id, project_key FROM projects WHERE group_key = ?",
                    (str(key),),
                ).fetchall()
            }
        finally:
            cx.row_factory = old_row_factory
        seen_project_ids: set[int] = set()
        for idx, project in enumerate(projects):
            project_key = _serialize_model(project).get("key")
            if not project_key:
                continue
            if project_key in current_projects:
                project_id = int(current_projects[project_key]["id"])
                seen_project_ids.add(project_id)
                self._update_project(cx, project_id, project, idx)
            else:
                project_id = self._insert_project(cx, str(key), project, idx)
                if project_id is not None:
                    seen_project_ids.add(project_id)

        for row in current_projects.values():
            project_id = int(row["id"])
            if project_id not in seen_project_ids:
                cx.execute("DELETE FROM projects WHERE id = ?", (project_id,))

        # deploy targets
        old_row_factory = cx.row_factory
        try:
            cx.row_factory = sqlite3.Row
            current_deploys = {
                row["name"]: row
                for row in cx.execute(
                    "SELECT id, name FROM deploy_targets WHERE group_key = ?",
                    (str(key),),
                ).fetchall()
            }
        finally:
            cx.row_factory = old_row_factory
        seen_target_ids: set[int] = set()
        for idx, deploy in enumerate(deploy_targets):
            deploy_name = _serialize_model(deploy).get("name")
            if not deploy_name:
                continue
            if deploy_name in current_deploys:
                target_id = int(current_deploys[deploy_name]["id"])
                seen_target_ids.add(target_id)
                self._update_deploy_target(cx, target_id, deploy, idx)
            else:
                target_id = self._insert_deploy_target(cx, str(key), deploy, idx)
                if target_id is not None:
                    seen_target_ids.add(target_id)

        for row in current_deploys.values():
            target_id = int(row["id"])
            if target_id not in seen_target_ids:
                cx.execute("DELETE FROM deploy_targets WHERE id = ?", (target_id,))

    # ------------------------------------------------------------------
    def _update_project(
        self,
        cx: sqlite3.Connection,
        project_id: int,
        project: Any,
        position: int,
    ) -> None:
        data = _serialize_model(project)
        key = data.get("key")
        if not key:
            return

        modules = list(data.get("modules") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"key", "modules", "execution_mode", "workspace", "repo"}
        }

        cx.execute(
            "UPDATE projects SET position = ?, execution_mode = ?, workspace = ?, repo = ?, config_json = ? "
            "WHERE id = ?",
            (
                int(position),
                data.get("execution_mode"),
                data.get("workspace"),
                data.get("repo"),
                _json_dumps(extras) if extras else "{}",
                int(project_id),
            ),
        )

        self._sync_modules(cx, int(project_id), modules)

    # ------------------------------------------------------------------
    def _sync_modules(
        self, cx: sqlite3.Connection, project_id: int, modules: List[Any]
    ) -> None:
        old_row_factory = cx.row_factory
        try:
            cx.row_factory = sqlite3.Row
            current_modules = {
                row["name"]: row
                for row in cx.execute(
                    "SELECT id, name FROM project_modules WHERE project_id = ?",
                    (int(project_id),),
                ).fetchall()
            }
        finally:
            cx.row_factory = old_row_factory
        seen_ids: set[int] = set()
        for idx, module in enumerate(modules):
            data = _serialize_model(module)
            name = data.get("name")
            path = data.get("path")
            if not name or not path:
                continue
            if name in current_modules:
                module_id = int(current_modules[name]["id"])
                seen_ids.add(module_id)
                self._update_module(cx, module_id, data, idx)
            else:
                module_id = self._insert_module(cx, project_id, data, idx)
                if module_id is not None:
                    seen_ids.add(module_id)

        for row in current_modules.values():
            module_id = int(row["id"])
            if module_id not in seen_ids:
                cx.execute("DELETE FROM project_modules WHERE id = ?", (module_id,))

    # ------------------------------------------------------------------
    def _update_module(
        self,
        cx: sqlite3.Connection,
        module_id: int,
        data: Dict[str, Any],
        position: int,
    ) -> None:
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {
                "name",
                "path",
                "version_files",
                "goals",
                "optional",
                "profile_override",
                "only_if_profile_equals",
                "copy_to_profile_war",
                "copy_to_profile_ui",
                "copy_to_subfolder",
                "rename_jar_to",
                "no_profile",
                "run_once",
                "select_pattern",
                "serial_across_profiles",
                "copy_to_root",
            }
        }

        cx.execute(
            "UPDATE project_modules SET position = ?, name = ?, path = ?, version_files = ?, goals = ?, "
            "optional = ?, profile_override = ?, only_if_profile_equals = ?, copy_to_profile_war = ?, "
            "copy_to_profile_ui = ?, copy_to_subfolder = ?, rename_jar_to = ?, no_profile = ?, run_once = ?, "
            "select_pattern = ?, serial_across_profiles = ?, copy_to_root = ?, config_json = ? WHERE id = ?",
            (
                int(position),
                str(data.get("name")),
                str(data.get("path")),
                _json_dumps(data.get("version_files") or []),
                _json_dumps(data.get("goals") or []),
                1 if data.get("optional") else 0,
                data.get("profile_override"),
                data.get("only_if_profile_equals"),
                1 if data.get("copy_to_profile_war") else 0,
                1 if data.get("copy_to_profile_ui") else 0,
                data.get("copy_to_subfolder"),
                data.get("rename_jar_to"),
                1 if data.get("no_profile") else 0,
                1 if data.get("run_once") else 0,
                data.get("select_pattern"),
                1 if data.get("serial_across_profiles") else 0,
                1 if data.get("copy_to_root") else 0,
                _json_dumps(extras) if extras else "{}",
                int(module_id),
            ),
        )

    # ------------------------------------------------------------------
    def _update_deploy_target(
        self,
        cx: sqlite3.Connection,
        target_id: int,
        deploy: Any,
        position: int,
    ) -> None:
        data = _serialize_model(deploy)
        name = data.get("name")
        project_key = data.get("project_key")
        path_template = data.get("path_template")
        if not name or not project_key or not path_template:
            return

        profiles = list(data.get("profiles") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"name", "project_key", "profiles", "path_template", "hotfix_path_template"}
        }

        cx.execute(
            "UPDATE deploy_targets SET position = ?, name = ?, project_key = ?, path_template = ?, "
            "hotfix_path_template = ?, config_json = ? WHERE id = ?",
            (
                int(position),
                str(name),
                str(project_key),
                str(path_template),
                data.get("hotfix_path_template"),
                _json_dumps(extras) if extras else "{}",
                int(target_id),
            ),
        )

        current_profiles = {
            str(row[1]): int(row[0])
            for row in cx.execute(
                "SELECT id, profile FROM deploy_target_profiles WHERE target_id = ?",
                (int(target_id),),
            ).fetchall()
        }
        seen_profile_ids: set[int] = set()
        for idx, profile in enumerate(profiles):
            profile_key = str(profile)
            if profile_key in current_profiles:
                profile_id = int(current_profiles[profile_key])
                seen_profile_ids.add(profile_id)
                cx.execute(
                    "UPDATE deploy_target_profiles SET position = ?, profile = ? WHERE id = ?",
                    (int(idx), profile_key, profile_id),
                )
            else:
                cursor = cx.execute(
                    "INSERT INTO deploy_target_profiles(target_id, position, profile) VALUES(?, ?, ?)",
                    (int(target_id), int(idx), profile_key),
                )
                seen_profile_ids.add(int(cursor.lastrowid))

        to_delete = [
            (int(profile_id),)
            for profile_id in current_profiles.values()
            if profile_id not in seen_profile_ids
        ]
        if to_delete:
            cx.executemany(
                "DELETE FROM deploy_target_profiles WHERE id = ?",
                to_delete,
            )
    # ------------------------------------------------------------------
    def _insert_module(
        self, cx: sqlite3.Connection, project_id: int, module: Any, position: int
    ) -> Optional[int]:
        data = _serialize_model(module)
        name = data.get("name")
        path = data.get("path")
        if not name or not path:
            return None
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {
                "name",
                "path",
                "version_files",
                "goals",
                "optional",
                "profile_override",
                "only_if_profile_equals",
                "copy_to_profile_war",
                "copy_to_profile_ui",
                "copy_to_subfolder",
                "rename_jar_to",
                "no_profile",
                "run_once",
                "select_pattern",
                "serial_across_profiles",
                "copy_to_root",
            }
        }

        cursor = cx.execute(
            "INSERT INTO project_modules("
            "project_id, position, name, path, version_files, goals, optional, "
            "profile_override, only_if_profile_equals, copy_to_profile_war, "
            "copy_to_profile_ui, copy_to_subfolder, rename_jar_to, no_profile, run_once, "
            "select_pattern, serial_across_profiles, copy_to_root, config_json"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                project_id,
                int(position),
                str(name),
                str(path),
                _json_dumps(data.get("version_files") or []),
                _json_dumps(data.get("goals") or []),
                1 if data.get("optional") else 0,
                data.get("profile_override"),
                data.get("only_if_profile_equals"),
                1 if data.get("copy_to_profile_war") else 0,
                1 if data.get("copy_to_profile_ui") else 0,
                data.get("copy_to_subfolder"),
                data.get("rename_jar_to"),
                1 if data.get("no_profile") else 0,
                1 if data.get("run_once") else 0,
                data.get("select_pattern"),
                1 if data.get("serial_across_profiles") else 0,
                1 if data.get("copy_to_root") else 0,
                _json_dumps(extras) if extras else "{}",
            ),
        )

        return int(cursor.lastrowid)

    # ------------------------------------------------------------------
    def _insert_deploy_target(
        self, cx: sqlite3.Connection, group_key: str, deploy: Any, position: int
    ) -> Optional[int]:
        data = _serialize_model(deploy)
        name = data.get("name")
        project_key = data.get("project_key")
        path_template = data.get("path_template")
        if not name or not project_key or not path_template:
            return None
        profiles = list(data.get("profiles") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"name", "project_key", "profiles", "path_template", "hotfix_path_template"}
        }

        cursor = cx.execute(
            "INSERT INTO deploy_targets("
            "group_key, position, name, project_key, path_template, hotfix_path_template, config_json"
            ") VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                group_key,
                int(position),
                str(name),
                str(project_key),
                str(path_template),
                data.get("hotfix_path_template"),
                _json_dumps(extras) if extras else "{}",
            ),
        )
        target_id = int(cursor.lastrowid)

        for idx, profile in enumerate(profiles):
            cx.execute(
                "INSERT INTO deploy_target_profiles(target_id, position, profile) VALUES(?, ?, ?)",
                (target_id, int(idx), str(profile)),
            )

        return target_id

    # ------------------------------------------------------------------
    def _insert_sprint(
        self, cx: sqlite3.Connection, sprint: Any, position: int
    ) -> Optional[int]:
        data = _serialize_model(sprint)
        key = data.get("key")
        name = data.get("name")
        group_key = data.get("group_key")
        if not key or not name or not group_key:
            return None
        cards = list(data.get("cards") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"key", "name", "group_key", "goal", "start_date", "end_date", "cards"}
        }

        cursor = cx.execute(
            "INSERT INTO sprints("
            "group_key, sprint_key, position, name, goal, start_date, end_date, config_json"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(group_key),
                str(key),
                int(position),
                str(name),
                data.get("goal"),
                data.get("start_date"),
                data.get("end_date"),
                _json_dumps(extras) if extras else "{}",
            ),
        )
        sprint_id = int(cursor.lastrowid)

        for idx, card in enumerate(cards):
            self._insert_card(cx, sprint_id, card, idx)

        return sprint_id

    # ------------------------------------------------------------------
    def _update_sprint(
        self, cx: sqlite3.Connection, sprint_id: int, sprint: Any, position: int
    ) -> None:
        data = _serialize_model(sprint)
        key = data.get("key")
        name = data.get("name")
        group_key = data.get("group_key")
        if not key or not name or not group_key:
            return
        cards = list(data.get("cards") or [])
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {"key", "name", "group_key", "goal", "start_date", "end_date", "cards"}
        }

        cx.execute(
            "UPDATE sprints SET position = ?, name = ?, goal = ?, start_date = ?, end_date = ?, config_json = ?"
            " WHERE id = ?",
            (
                int(position),
                str(name),
                data.get("goal"),
                data.get("start_date"),
                data.get("end_date"),
                _json_dumps(extras) if extras else "{}",
                int(sprint_id),
            ),
        )

        self._sync_cards(cx, int(sprint_id), cards)

    # ------------------------------------------------------------------
    def _sync_cards(
        self, cx: sqlite3.Connection, sprint_id: int, cards: List[Any]
    ) -> None:
        old_row_factory = cx.row_factory
        try:
            cx.row_factory = sqlite3.Row
            current_cards = {
                row["card_key"]: row
                for row in cx.execute(
                    "SELECT id, card_key FROM cards WHERE sprint_id = ?",
                    (int(sprint_id),),
                ).fetchall()
            }
        finally:
            cx.row_factory = old_row_factory

        seen_ids: set[int] = set()
        for idx, card in enumerate(cards):
            card_key = _serialize_model(card).get("key")
            if not card_key:
                continue
            if card_key in current_cards:
                card_id = int(current_cards[card_key]["id"])
                seen_ids.add(card_id)
                self._update_card(cx, card_id, card, idx)
            else:
                card_id = self._insert_card(cx, int(sprint_id), card, idx)
                if card_id is not None:
                    seen_ids.add(card_id)

        for row in current_cards.values():
            card_id = int(row["id"])
            if card_id not in seen_ids:
                cx.execute("DELETE FROM cards WHERE id = ?", (card_id,))

    # ------------------------------------------------------------------
    def _insert_card(
        self, cx: sqlite3.Connection, sprint_id: int, card: Any, position: int
    ) -> Optional[int]:
        data = _serialize_model(card)
        key = data.get("key")
        title = data.get("title")
        if not key or not title:
            return None
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {
                "key",
                "title",
                "project_key",
                "version",
                "owners",
                "tests_ready",
                "qa_ready",
                "notes",
            }
        }

        cursor = cx.execute(
            "INSERT INTO cards(" 
            "sprint_id, position, card_key, title, project_key, version, owners, tests_ready, qa_ready, notes, config_json"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                int(sprint_id),
                int(position),
                str(key),
                str(title),
                data.get("project_key"),
                data.get("version"),
                _json_dumps(data.get("owners") or []),
                1 if data.get("tests_ready") else 0,
                1 if data.get("qa_ready") else 0,
                data.get("notes"),
                _json_dumps(extras) if extras else "{}",
            ),
        )
        return int(cursor.lastrowid)

    # ------------------------------------------------------------------
    def _update_card(
        self, cx: sqlite3.Connection, card_id: int, card: Any, position: int
    ) -> None:
        data = _serialize_model(card)
        key = data.get("key")
        title = data.get("title")
        if not key or not title:
            return
        extras = {
            k: v
            for k, v in data.items()
            if k
            not in {
                "key",
                "title",
                "project_key",
                "version",
                "owners",
                "tests_ready",
                "qa_ready",
                "notes",
            }
        }

        cx.execute(
            "UPDATE cards SET position = ?, card_key = ?, title = ?, project_key = ?, version = ?, owners = ?, "
            "tests_ready = ?, qa_ready = ?, notes = ?, config_json = ? WHERE id = ?",
            (
                int(position),
                str(key),
                str(title),
                data.get("project_key"),
                data.get("version"),
                _json_dumps(data.get("owners") or []),
                1 if data.get("tests_ready") else 0,
                1 if data.get("qa_ready") else 0,
                data.get("notes"),
                _json_dumps(extras) if extras else "{}",
                int(card_id),
            ),
        )

    # ------------------------------------------------------------------
    def replace_sprints(self, sprints: Iterable[Any]) -> None:
        from .config import Sprint  # import diferido

        normalized: List[Sprint] = []
        for sprint in sprints:
            if sprint is None:
                continue
            if isinstance(sprint, Sprint):
                normalized.append(sprint)
            else:
                try:
                    normalized.append(Sprint(**_serialize_model(sprint)))
                except Exception:
                    continue

        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            old_factory = cx.row_factory
            try:
                cx.row_factory = sqlite3.Row
                current = {
                    (str(row["group_key"]), str(row["sprint_key"])): row
                    for row in cx.execute(
                        "SELECT id, group_key, sprint_key FROM sprints"
                    ).fetchall()
                }
            finally:
                cx.row_factory = old_factory

            incoming_keys = {(s.group_key, s.key) for s in normalized}
            for key in set(current) - incoming_keys:
                cx.execute("DELETE FROM sprints WHERE id = ?", (int(current[key]["id"]),))

            positions: Dict[str, int] = {}
            for sprint in normalized:
                position = positions.setdefault(sprint.group_key, 0)
                positions[sprint.group_key] = position + 1
                lookup = (sprint.group_key, sprint.key)
                if lookup in current:
                    self._update_sprint(cx, int(current[lookup]["id"]), sprint, position)
                else:
                    self._insert_sprint(cx, sprint, position)

            cx.commit()

    # ------------------------------------------------------------------
    def list_sprints(self, group_key: Optional[str] = None) -> List[Any]:
        from .config import Sprint

        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            cx.execute("PRAGMA foreign_keys = ON")
            if group_key:
                sprint_rows = cx.execute(
                    "SELECT id, group_key, sprint_key, position, name, goal, start_date, end_date, config_json "
                    "FROM sprints WHERE group_key = ? ORDER BY position, sprint_key",
                    (str(group_key),),
                ).fetchall()
            else:
                sprint_rows = cx.execute(
                    "SELECT id, group_key, sprint_key, position, name, goal, start_date, end_date, config_json "
                    "FROM sprints ORDER BY group_key, position, sprint_key"
                ).fetchall()
            card_rows = cx.execute(
                "SELECT sprint_id, position, card_key, title, project_key, version, owners, tests_ready, qa_ready, notes, config_json "
                "FROM cards ORDER BY position, card_key"
            ).fetchall()

        cards_by_sprint: Dict[int, List[sqlite3.Row]] = {}
        for row in card_rows:
            cards_by_sprint.setdefault(int(row["sprint_id"]), []).append(row)

        result: List[Any] = []
        for row in sprint_rows:
            payload: Dict[str, Any] = {
                "key": str(row["sprint_key"]),
                "name": str(row["name"]),
                "group_key": str(row["group_key"]),
                "goal": row["goal"],
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "cards": [],
            }
            payload.update(_json_loads(row["config_json"], {}))

            for card_row in cards_by_sprint.get(int(row["id"]), []):
                card_payload: Dict[str, Any] = {
                    "key": str(card_row["card_key"]),
                    "title": str(card_row["title"]),
                    "project_key": card_row["project_key"],
                    "version": card_row["version"],
                    "owners": _json_loads(card_row["owners"], []),
                    "tests_ready": bool(card_row["tests_ready"]),
                    "qa_ready": bool(card_row["qa_ready"]),
                    "notes": card_row["notes"],
                }
                card_payload.update(_json_loads(card_row["config_json"], {}))
                payload["cards"].append(card_payload)

            try:
                result.append(Sprint(**payload))
            except Exception:
                continue

        return result

    # ------------------------------------------------------------------
    def upsert_sprint(self, sprint: Any) -> None:
        from .config import Sprint

        data = _serialize_model(sprint)
        key = data.get("key")
        group_key = data.get("group_key")
        if not key or not group_key:
            raise ValueError("El sprint debe tener 'key' y 'group_key' válidos")
        try:
            normalized = sprint if isinstance(sprint, Sprint) else Sprint(**data)
        except Exception as exc:
            raise ValueError("Datos de sprint inválidos") from exc

        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            current = cx.execute(
                "SELECT id, position FROM sprints WHERE group_key = ? AND sprint_key = ?",
                (normalized.group_key, normalized.key),
            ).fetchone()
            if current:
                self._update_sprint(cx, int(current[0]), normalized, int(current[1]))
            else:
                position = self._next_sprint_position(cx, normalized.group_key)
                self._insert_sprint(cx, normalized, position)
            cx.commit()

    # ------------------------------------------------------------------
    def delete_sprint(self, group_key: str, sprint_key: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            cx.execute(
                "DELETE FROM sprints WHERE group_key = ? AND sprint_key = ?",
                (str(group_key), str(sprint_key)),
            )
            cx.commit()

    # ------------------------------------------------------------------
    def upsert_card(self, group_key: str, sprint_key: str, card: Any) -> None:
        from .config import Card

        data = _serialize_model(card)
        key = data.get("key")
        if not key:
            raise ValueError("La tarjeta debe tener una clave válida")
        try:
            normalized = card if isinstance(card, Card) else Card(**data)
        except Exception as exc:
            raise ValueError("Datos de tarjeta inválidos") from exc

        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            sprint_row = cx.execute(
                "SELECT id FROM sprints WHERE group_key = ? AND sprint_key = ?",
                (str(group_key), str(sprint_key)),
            ).fetchone()
            if not sprint_row:
                raise ValueError("Sprint inexistente")
            sprint_id = int(sprint_row[0])
            current = cx.execute(
                "SELECT id, position FROM cards WHERE sprint_id = ? AND card_key = ?",
                (sprint_id, normalized.key),
            ).fetchone()
            if current:
                self._update_card(cx, int(current[0]), normalized, int(current[1]))
            else:
                position = self._next_card_position(cx, sprint_id)
                self._insert_card(cx, sprint_id, normalized, position)
            cx.commit()

    # ------------------------------------------------------------------
    def delete_card(self, group_key: str, sprint_key: str, card_key: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            sprint_row = cx.execute(
                "SELECT id FROM sprints WHERE group_key = ? AND sprint_key = ?",
                (str(group_key), str(sprint_key)),
            ).fetchone()
            if not sprint_row:
                return
            sprint_id = int(sprint_row[0])
            cx.execute(
                "DELETE FROM cards WHERE sprint_id = ? AND card_key = ?",
                (sprint_id, str(card_key)),
            )
            cx.commit()

    # ------------------------------------------------------------------
    def is_empty(self) -> bool:
        with sqlite3.connect(self.db_path) as cx:
            cur = cx.execute("SELECT COUNT(*) FROM groups")
            (count,) = cur.fetchone()
        return int(count) == 0

    # ------------------------------------------------------------------
    def replace_groups(self, groups: Iterable[Any]) -> None:
        from .config import Group  # import diferido para evitar ciclos

        normalized: List[Group] = []
        for group in groups:
            if group is None:
                continue
            if isinstance(group, Group):
                normalized.append(group)
            else:
                try:
                    normalized.append(Group(**_serialize_model(group)))
                except Exception:
                    continue

        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            existing_keys = {
                row[0]
                for row in cx.execute("SELECT key FROM groups")
            }
            incoming_keys = {g.key for g in normalized}

            removed = existing_keys - incoming_keys
            for key in removed:
                self._delete_group_rows(cx, key)

            for position, group in enumerate(normalized):
                if group.key in existing_keys:
                    self._update_group(cx, group, position)
                else:
                    self._insert_group(cx, group, position)
            cx.commit()

    # ------------------------------------------------------------------
    def list_groups(self) -> List[Any]:
        from .config import Group

        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            cx.execute("PRAGMA foreign_keys = ON")
            group_rows = cx.execute(
                "SELECT key, position, output_base, config_json FROM groups "
                "ORDER BY position, key"
            ).fetchall()
            repo_rows = cx.execute(
                "SELECT group_key, repo_key, path FROM group_repos"
            ).fetchall()
            group_profiles = cx.execute(
                "SELECT group_key, position, profile FROM group_profiles ORDER BY position"
            ).fetchall()
            project_rows = cx.execute(
                "SELECT id, group_key, project_key, position, execution_mode, "
                "workspace, repo, config_json FROM projects ORDER BY position"
            ).fetchall()
            module_rows = cx.execute(
                "SELECT project_id, position, name, path, version_files, goals, optional, "
                "profile_override, only_if_profile_equals, copy_to_profile_war, "
                "copy_to_profile_ui, copy_to_subfolder, rename_jar_to, no_profile, run_once, "
                "select_pattern, serial_across_profiles, copy_to_root, config_json "
                "FROM project_modules ORDER BY position"
            ).fetchall()
            deploy_rows = cx.execute(
                "SELECT id, group_key, position, name, project_key, path_template, "
                "hotfix_path_template, config_json FROM deploy_targets ORDER BY position"
            ).fetchall()
            deploy_profiles = cx.execute(
                "SELECT target_id, position, profile FROM deploy_target_profiles ORDER BY position"
            ).fetchall()

        repos_by_group: Dict[str, Dict[str, str]] = {}
        for row in repo_rows:
            repos_by_group.setdefault(row[0], {})[str(row[1])] = str(row[2])

        profiles_by_group: Dict[str, List[str]] = {}
        for row in group_profiles:
            profiles_by_group.setdefault(row[0], []).append(str(row[2]))

        projects_by_group: Dict[str, List[sqlite3.Row]] = {}
        for row in project_rows:
            projects_by_group.setdefault(row[1], []).append(row)

        modules_by_project: Dict[int, List[sqlite3.Row]] = {}
        for row in module_rows:
            modules_by_project.setdefault(row[0], []).append(row)

        deploys_by_group: Dict[str, List[sqlite3.Row]] = {}
        for row in deploy_rows:
            deploys_by_group.setdefault(row[1], []).append(row)

        deploy_profiles_map: Dict[int, List[str]] = {}
        for row in deploy_profiles:
            deploy_profiles_map.setdefault(row[0], []).append(str(row[2]))

        groups: List[Any] = []
        for row in group_rows:
            key = str(row["key"])
            group_payload: Dict[str, Any] = {
                "key": key,
                "repos": repos_by_group.get(key, {}),
                "output_base": str(row["output_base"] or ""),
                "profiles": profiles_by_group.get(key, []),
                "projects": [],
                "deploy_targets": [],
            }
            group_payload.update(_json_loads(row["config_json"], {}))

            for project_row in projects_by_group.get(key, []):
                project_id = int(project_row["id"])
                project_extras = _json_loads(project_row["config_json"], {})
                project_profiles = project_extras.pop("profiles", None)
                project_payload: Dict[str, Any] = {
                    "key": str(project_row["project_key"]),
                    "execution_mode": project_row["execution_mode"],
                    "workspace": project_row["workspace"],
                    "repo": project_row["repo"],
                    "profiles": project_profiles,
                    "modules": [],
                }
                project_payload.update(project_extras)

                for module_row in modules_by_project.get(project_id, []):
                    module_payload: Dict[str, Any] = {
                        "name": str(module_row["name"]),
                        "path": str(module_row["path"]),
                        "version_files": _json_loads(module_row["version_files"], []),
                        "goals": _json_loads(module_row["goals"], []),
                        "optional": bool(module_row["optional"]),
                        "profile_override": module_row["profile_override"],
                        "only_if_profile_equals": module_row["only_if_profile_equals"],
                        "copy_to_profile_war": bool(module_row["copy_to_profile_war"]),
                        "copy_to_profile_ui": bool(module_row["copy_to_profile_ui"]),
                        "copy_to_subfolder": module_row["copy_to_subfolder"],
                        "rename_jar_to": module_row["rename_jar_to"],
                        "no_profile": bool(module_row["no_profile"]),
                        "run_once": bool(module_row["run_once"]),
                        "select_pattern": module_row["select_pattern"],
                        "serial_across_profiles": bool(
                            module_row["serial_across_profiles"]
                        ),
                        "copy_to_root": bool(module_row["copy_to_root"]),
                    }
                    module_payload.update(
                        _json_loads(module_row["config_json"], {})
                    )
                    project_payload["modules"].append(module_payload)

                group_payload["projects"].append(project_payload)

            for deploy_row in deploys_by_group.get(key, []):
                deploy_id = int(deploy_row["id"])
                deploy_payload: Dict[str, Any] = {
                    "name": str(deploy_row["name"]),
                    "project_key": str(deploy_row["project_key"]),
                    "profiles": deploy_profiles_map.get(deploy_id, []),
                    "path_template": str(deploy_row["path_template"]),
                    "hotfix_path_template": deploy_row["hotfix_path_template"],
                }
                deploy_payload.update(
                    _json_loads(deploy_row["config_json"], {})
                )
                group_payload["deploy_targets"].append(deploy_payload)

            try:
                groups.append(Group(**group_payload))
            except Exception:
                continue
        return groups

    # ------------------------------------------------------------------
    def update_group(self, group: Any) -> None:
        from .config import Group  # import diferido

        data = _serialize_model(group)
        key = data.get("key")
        if not key:
            raise ValueError("El grupo debe tener un 'key' válido")
        try:
            normalized = group if isinstance(group, Group) else Group(**data)
        except Exception as exc:  # pragma: no cover - validaciones
            raise ValueError("Datos de grupo inválidos") from exc

        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            current = cx.execute(
                "SELECT position FROM groups WHERE key = ?", (normalized.key,)
            ).fetchone()
            position = int(current[0]) if current else self._next_group_position(cx)
            self._delete_group_rows(cx, normalized.key)
            self._insert_group(cx, normalized, position)
            cx.commit()

    # ------------------------------------------------------------------
    def delete_group(self, key: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            self._delete_group_rows(cx, key)
            cx.commit()

    # ------------------------------------------------------------------
    def load_metadata(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as cx:
            cur = cx.execute("SELECT value FROM metadata WHERE key = ?", (key,))
            row = cur.fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    def save_metadata(self, key: str, value: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            cx.commit()


__all__ = ["ConfigStore"]
