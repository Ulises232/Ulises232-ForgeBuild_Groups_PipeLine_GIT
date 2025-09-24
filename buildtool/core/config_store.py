from __future__ import annotations

import json
import os
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
    branch_key TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    metadata TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_cfg_sprints_branch ON sprints(branch_key);
CREATE TABLE IF NOT EXISTS cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sprint_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    branch TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    metadata TEXT DEFAULT '{}',
    FOREIGN KEY(sprint_id) REFERENCES sprints(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cfg_cards_sprint ON cards(sprint_id);
CREATE TABLE IF NOT EXISTS card_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    role TEXT NOT NULL,
    UNIQUE(card_id, username, role),
    FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
);
"""


def _state_dir() -> Path:
    base = os.environ.get("APPDATA")
    if base:
        return Path(base) / "ForgeBuild"
    return Path.home() / ".forgebuild"


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
        self.db_path = Path(db_path or (_state_dir() / "config.sqlite3"))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            legacy_groups = self._extract_legacy_groups(cx)
            self._inline_project_profiles(cx)
            self._migrate_sprint_tables(cx)
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
    def _migrate_sprint_tables(self, cx: sqlite3.Connection) -> None:
        """Ensure legacy sprint/card tables expose the new columns used by the app."""

        sprint_exists = cx.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sprints'"
        ).fetchone()
        if sprint_exists:
            columns = {row[1] for row in cx.execute("PRAGMA table_info(sprints)")}
            if "branch_key" not in columns:
                cx.execute("ALTER TABLE sprints ADD COLUMN branch_key TEXT")
            if "metadata" not in columns:
                cx.execute(
                    "ALTER TABLE sprints ADD COLUMN metadata TEXT DEFAULT '{}'"
                )

        cards_exists = cx.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cards'"
        ).fetchone()
        if cards_exists:
            columns = {row[1] for row in cx.execute("PRAGMA table_info(cards)")}
            if "metadata" not in columns:
                cx.execute(
                    "ALTER TABLE cards ADD COLUMN metadata TEXT DEFAULT '{}'"
                )

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
    def list_sprints(self, branch_key: Optional[str] = None) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            sql = "SELECT id, branch_key, name, version, metadata FROM sprints"
            params: List[Any] = []
            if branch_key:
                sql += " WHERE branch_key = ?"
                params.append(branch_key)
            sql += " ORDER BY id DESC"
            rows = cx.execute(sql, params).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            data["metadata"] = _json_loads(row["metadata"], {})
            result.append(data)
        return result

    # ------------------------------------------------------------------
    def upsert_sprint(self, payload: Dict[str, Any]) -> int:
        metadata = _json_dumps(_serialize_model(payload.get("metadata")))
        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            cursor = cx.execute(
                """
                INSERT INTO sprints(id, branch_key, name, version, metadata)
                VALUES(:id, :branch_key, :name, :version, :metadata)
                ON CONFLICT(id) DO UPDATE SET
                    branch_key = excluded.branch_key,
                    name = excluded.name,
                    version = excluded.version,
                    metadata = excluded.metadata
                """,
                {
                    "id": payload.get("id"),
                    "branch_key": payload.get("branch_key"),
                    "name": payload.get("name"),
                    "version": payload.get("version"),
                    "metadata": metadata,
                },
            )
            cx.commit()
            if payload.get("id"):
                return int(payload["id"])
            return int(cursor.lastrowid)

    # ------------------------------------------------------------------
    def delete_sprint(self, sprint_id: int) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("DELETE FROM sprints WHERE id = ?", (int(sprint_id),))
            cx.commit()

    # ------------------------------------------------------------------
    def list_cards(
        self,
        *,
        sprint_id: Optional[int] = None,
        branch: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            sql = "SELECT id, sprint_id, title, branch, status, metadata FROM cards"
            params: List[Any] = []
            clauses: List[str] = []
            if sprint_id is not None:
                clauses.append("sprint_id = ?")
                params.append(int(sprint_id))
            if branch:
                clauses.append("branch = ?")
                params.append(branch)
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY id DESC"
            rows = cx.execute(sql, params).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            data["metadata"] = _json_loads(row["metadata"], {})
            items.append(data)
        return items

    # ------------------------------------------------------------------
    def upsert_card(self, payload: Dict[str, Any]) -> int:
        metadata = _json_dumps(_serialize_model(payload.get("metadata")))
        with sqlite3.connect(self.db_path) as cx:
            cursor = cx.execute(
                """
                INSERT INTO cards(id, sprint_id, title, branch, status, metadata)
                VALUES(:id, :sprint_id, :title, :branch, :status, :metadata)
                ON CONFLICT(id) DO UPDATE SET
                    sprint_id = excluded.sprint_id,
                    title = excluded.title,
                    branch = excluded.branch,
                    status = excluded.status,
                    metadata = excluded.metadata
                """,
                {
                    "id": payload.get("id"),
                    "sprint_id": payload.get("sprint_id"),
                    "title": payload.get("title"),
                    "branch": payload.get("branch"),
                    "status": payload.get("status", "pending"),
                    "metadata": metadata,
                },
            )
            cx.commit()
            if payload.get("id"):
                return int(payload["id"])
            return int(cursor.lastrowid)

    # ------------------------------------------------------------------
    def delete_card(self, card_id: int) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("DELETE FROM cards WHERE id = ?", (int(card_id),))
            cx.commit()

    # ------------------------------------------------------------------
    def list_card_assignments(self, card_id: Optional[int] = None) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            sql = "SELECT id, card_id, username, role FROM card_assignments"
            params: List[Any] = []
            if card_id is not None:
                sql += " WHERE card_id = ?"
                params.append(int(card_id))
            rows = cx.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    def upsert_card_assignment(self, payload: Dict[str, Any]) -> int:
        with sqlite3.connect(self.db_path) as cx:
            cursor = cx.execute(
                """
                INSERT INTO card_assignments(id, card_id, username, role)
                VALUES(:id, :card_id, :username, :role)
                ON CONFLICT(card_id, username, role) DO UPDATE SET
                    username = excluded.username
                """,
                {
                    "id": payload.get("id"),
                    "card_id": payload.get("card_id"),
                    "username": payload.get("username"),
                    "role": payload.get("role"),
                },
            )
            cx.commit()
            if payload.get("id"):
                return int(payload["id"])
            return int(cursor.lastrowid)

    # ------------------------------------------------------------------
    def delete_card_assignment(self, assignment_id: int) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("DELETE FROM card_assignments WHERE id = ?", (int(assignment_id),))
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
