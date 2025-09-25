from __future__ import annotations

import csv
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    user TEXT,
    group_key TEXT,
    project_key TEXT,
    profiles TEXT,
    modules TEXT,
    version TEXT,
    hotfix INTEGER,
    card_id INTEGER,
    unit_tests_status TEXT,
    qa_status TEXT,
    approved_by TEXT,
    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME,
    status TEXT,
    message TEXT
);
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    ts DATETIME DEFAULT CURRENT_TIMESTAMP,
    message TEXT NOT NULL
);
"""


def _state_dir() -> Path:
    base = os.environ.get("APPDATA")
    if base:
        return Path(base) / "ForgeBuild"
    return Path.home() / ".forgebuild"


def history_db_path() -> Path:
    override = os.environ.get("FORGEBUILD_HISTORY_DB")
    if override:
        return Path(override)
    return _state_dir() / "pipeline_history.sqlite3"


@dataclass
class RunRecord:
    id: int
    pipeline: str
    user: Optional[str]
    group_key: Optional[str]
    project_key: Optional[str]
    profiles: List[str]
    modules: List[str]
    version: Optional[str]
    hotfix: Optional[bool]
    card_id: Optional[int]
    unit_tests_status: Optional[str]
    qa_status: Optional[str]
    approved_by: Optional[str]
    started_at: str
    finished_at: Optional[str]
    status: Optional[str]
    message: Optional[str]


class PipelineHistory:
    """Gestiona el almacenamiento del historial de pipelines en SQLite."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = Path(db_path or history_db_path())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            cx.executescript(SCHEMA)
            columns = self._ensure_columns(cx)
            self._ensure_indexes(cx, columns)

    def _ensure_columns(self, cx: sqlite3.Connection) -> set[str]:
        info = cx.execute("PRAGMA table_info(pipeline_runs)").fetchall()
        columns = {row[1] for row in info}
        statements: list[str] = []
        if "card_id" not in columns:
            statements.append("ALTER TABLE pipeline_runs ADD COLUMN card_id INTEGER")
        if "unit_tests_status" not in columns:
            statements.append("ALTER TABLE pipeline_runs ADD COLUMN unit_tests_status TEXT")
        if "qa_status" not in columns:
            statements.append("ALTER TABLE pipeline_runs ADD COLUMN qa_status TEXT")
        if "approved_by" not in columns:
            statements.append("ALTER TABLE pipeline_runs ADD COLUMN approved_by TEXT")
        for stmt in statements:
            cx.execute(stmt)
        if statements:
            cx.commit()
            info = cx.execute("PRAGMA table_info(pipeline_runs)").fetchall()
            columns = {row[1] for row in info}
        return columns

    def _ensure_indexes(self, cx: sqlite3.Connection, columns: set[str]) -> None:
        cx.execute(
            "CREATE INDEX IF NOT EXISTS ix_pipeline_runs_pipeline"
            " ON pipeline_runs(pipeline, started_at DESC)"
        )
        cx.execute(
            "CREATE INDEX IF NOT EXISTS ix_pipeline_runs_group"
            " ON pipeline_runs(group_key)"
        )
        cx.execute(
            "CREATE INDEX IF NOT EXISTS ix_pipeline_runs_project"
            " ON pipeline_runs(project_key)"
        )
        if "card_id" in columns:
            cx.execute(
                "CREATE INDEX IF NOT EXISTS ix_pipeline_runs_card"
                " ON pipeline_runs(card_id)"
            )
        cx.execute(
            "CREATE INDEX IF NOT EXISTS ix_pipeline_logs_run"
            " ON pipeline_logs(run_id, ts)"
        )
        cx.commit()

    # ------------------------------------------------------------------
    def start_run(
        self,
        pipeline: str,
        *,
        user: Optional[str],
        group_key: Optional[str],
        project_key: Optional[str],
        profiles: Iterable[str],
        modules: Iterable[str],
        version: Optional[str] = None,
        hotfix: Optional[bool] = None,
        card_id: Optional[int] = None,
        unit_tests_status: Optional[str] = None,
        qa_status: Optional[str] = None,
        approved_by: Optional[str] = None,
        started_at: Optional[datetime] = None,
    ) -> int:
        started_at = started_at or datetime.utcnow()
        with sqlite3.connect(self.db_path) as cx:
            cx.execute(
                "PRAGMA foreign_keys = ON",
            )
            cur = cx.execute(
                """
                INSERT INTO pipeline_runs (
                    pipeline, user, group_key, project_key, profiles, modules,
                    version, hotfix, card_id, unit_tests_status, qa_status, approved_by,
                    started_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pipeline,
                    user,
                    group_key,
                    project_key,
                    json.dumps(list(profiles)),
                    json.dumps(list(modules)),
                    version,
                    1 if hotfix else (0 if hotfix is not None else None),
                    card_id,
                    unit_tests_status,
                    qa_status,
                    approved_by,
                    started_at.isoformat(timespec="seconds"),
                    "running",
                ),
            )
            run_id = cur.lastrowid
            cx.commit()
        return int(run_id)

    def finish_run(
        self,
        run_id: int,
        status: str,
        message: Optional[str] = None,
        *,
        unit_tests_status: Optional[str] = None,
        qa_status: Optional[str] = None,
        approved_by: Optional[str] = None,
        finished_at: Optional[datetime] = None,
    ) -> None:
        finished_at = finished_at or datetime.utcnow()
        with sqlite3.connect(self.db_path) as cx:
            cx.execute(
                "PRAGMA foreign_keys = ON",
            )
            cx.execute(
                """
                UPDATE pipeline_runs
                   SET status = ?,
                       message = ?,
                       finished_at = ?,
                       unit_tests_status = COALESCE(?, unit_tests_status),
                       qa_status = COALESCE(?, qa_status),
                       approved_by = COALESCE(?, approved_by)
                 WHERE id = ?
                """,
                (
                    status,
                    message,
                    finished_at.isoformat(timespec="seconds"),
                    unit_tests_status,
                    qa_status,
                    approved_by,
                    run_id,
                ),
            )
            cx.commit()

    def log_message(self, run_id: int, message: str) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            cx.execute(
                "INSERT INTO pipeline_logs(run_id, message) VALUES(?, ?)",
                (run_id, message),
            )
            cx.commit()

    def list_runs(
        self,
        *,
        pipeline: Optional[str] = None,
        group_key: Optional[str] = None,
        project_key: Optional[str] = None,
        status: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[RunRecord]:
        clauses = []
        params: list = []
        if pipeline:
            clauses.append("pipeline = ?")
            params.append(pipeline)
        if group_key:
            clauses.append("group_key = ?")
            params.append(group_key)
        if project_key:
            clauses.append("project_key = ?")
            params.append(project_key)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if start:
            clauses.append("started_at >= ?")
            params.append(start.isoformat(timespec="seconds"))
        if end:
            clauses.append("started_at <= ?")
            params.append(end.isoformat(timespec="seconds"))

        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        sql = (
            "SELECT id, pipeline, user, group_key, project_key, profiles, modules, "
            "version, hotfix, card_id, unit_tests_status, qa_status, approved_by, "
            "started_at, finished_at, status, message "
            "FROM pipeline_runs"
            f"{where} ORDER BY started_at DESC LIMIT ?"
        )
        params.append(limit)

        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            cur = cx.execute(sql, params)
            rows = cur.fetchall()

        records: List[RunRecord] = []
        for row in rows:
            records.append(
                RunRecord(
                    id=row["id"],
                    pipeline=row["pipeline"],
                    user=row["user"],
                    group_key=row["group_key"],
                    project_key=row["project_key"],
                    profiles=json.loads(row["profiles"] or "[]"),
                    modules=json.loads(row["modules"] or "[]"),
                    version=row["version"],
                    hotfix=None if row["hotfix"] is None else bool(row["hotfix"]),
                    card_id=row["card_id"],
                    unit_tests_status=row["unit_tests_status"],
                    qa_status=row["qa_status"],
                    approved_by=row["approved_by"],
                    started_at=row["started_at"],
                    finished_at=row["finished_at"],
                    status=row["status"],
                    message=row["message"],
                )
            )
        return records

    def get_logs(self, run_id: int) -> List[tuple[str, str]]:
        with sqlite3.connect(self.db_path) as cx:
            cx.row_factory = sqlite3.Row
            cur = cx.execute(
                "SELECT ts, message FROM pipeline_logs WHERE run_id = ? ORDER BY ts ASC",
                (run_id,),
            )
            return [(row["ts"], row["message"]) for row in cur.fetchall()]

    def clear(self) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            cx.execute("DELETE FROM pipeline_logs")
            cx.execute("DELETE FROM pipeline_runs")
            cx.commit()

    def update_card_status(
        self,
        card_id: int,
        *,
        unit_tests_status: Optional[str] = None,
        qa_status: Optional[str] = None,
        approved_by: Optional[str] = None,
    ) -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("PRAGMA foreign_keys = ON")
            row = cx.execute(
                "SELECT id FROM pipeline_runs WHERE card_id = ? ORDER BY started_at DESC LIMIT 1",
                (card_id,),
            ).fetchone()
            if row:
                cx.execute(
                    """
                    UPDATE pipeline_runs
                       SET unit_tests_status = COALESCE(?, unit_tests_status),
                           qa_status = COALESCE(?, qa_status),
                           approved_by = COALESCE(?, approved_by)
                     WHERE id = ?
                    """,
                    (unit_tests_status, qa_status, approved_by, row[0]),
                )
            else:
                cx.execute(
                    """
                    INSERT INTO pipeline_runs(
                        pipeline, user, group_key, project_key, profiles, modules,
                        version, hotfix, card_id, unit_tests_status, qa_status, approved_by,
                        started_at, status
                    ) VALUES('card-check', NULL, NULL, NULL, '[]', '[]', NULL, NULL,
                             ?, ?, ?, ?, datetime('now'), 'running')
                    """,
                    (card_id, unit_tests_status, qa_status, approved_by),
                )
            cx.commit()

    def export_csv(
        self,
        destination: Path,
        **filters,
    ) -> Path:
        records = self.list_runs(**filters)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "id",
                    "pipeline",
                    "user",
                    "group",
                    "project",
                    "profiles",
                    "modules",
                    "version",
                    "hotfix",
                    "card_id",
                    "unit_tests_status",
                    "qa_status",
                    "approved_by",
                    "started_at",
                    "finished_at",
                    "status",
                    "message",
                ]
            )
            for rec in records:
                writer.writerow(
                    [
                        rec.id,
                        rec.pipeline,
                        rec.user or "",
                        rec.group_key or "",
                        rec.project_key or "",
                        ", ".join(rec.profiles),
                        ", ".join(rec.modules),
                        rec.version or "",
                        "sí" if rec.hotfix else "no",
                        rec.card_id or "",
                        rec.unit_tests_status or "",
                        rec.qa_status or "",
                        rec.approved_by or "",
                        rec.started_at,
                        rec.finished_at or "",
                        rec.status or "",
                        rec.message or "",
                    ]
                )
        return destination
