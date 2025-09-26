from __future__ import annotations

import csv
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

from .branch_history_db import _SqlServerConnectionPool


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
    """Gestiona el almacenamiento del historial de pipelines en SQL Server."""

    def __init__(self, url: Optional[str] = None, *, pool_size: int = 5) -> None:
        url = url or os.environ.get("PIPELINE_HISTORY_DB_URL") or os.environ.get(
            "BRANCH_HISTORY_DB_URL"
        )
        if not url:
            raise ValueError(
                "Se requiere la cadena BRANCH_HISTORY_DB_URL o PIPELINE_HISTORY_DB_URL para inicializar PipelineHistory."
            )
        self._pool = _SqlServerConnectionPool(url, max_size=pool_size)
        self._ensure_schema()

    @contextmanager
    def _connect(self):
        with self._pool.connection() as conn:
            yield conn

    def _ensure_schema(self) -> None:
        statements = [
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'pipeline_runs')
            BEGIN
                CREATE TABLE pipeline_runs (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    pipeline NVARCHAR(255) NOT NULL,
                    [user] NVARCHAR(255) NULL,
                    group_key NVARCHAR(255) NULL,
                    project_key NVARCHAR(255) NULL,
                    profiles NVARCHAR(MAX) NULL,
                    modules NVARCHAR(MAX) NULL,
                    version NVARCHAR(128) NULL,
                    hotfix BIT NULL,
                    card_id INT NULL,
                    unit_tests_status NVARCHAR(64) NULL,
                    qa_status NVARCHAR(64) NULL,
                    approved_by NVARCHAR(255) NULL,
                    started_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    finished_at DATETIME2 NULL,
                    status NVARCHAR(64) NULL,
                    message NVARCHAR(MAX) NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'pipeline_logs')
            BEGIN
                CREATE TABLE pipeline_logs (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    run_id INT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                    ts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    message NVARCHAR(MAX) NOT NULL
                );
            END
            """,
            """
            IF COL_LENGTH('pipeline_runs', 'card_id') IS NULL
                ALTER TABLE pipeline_runs ADD card_id INT NULL;
            """,
            """
            IF COL_LENGTH('pipeline_runs', 'unit_tests_status') IS NULL
                ALTER TABLE pipeline_runs ADD unit_tests_status NVARCHAR(64) NULL;
            """,
            """
            IF COL_LENGTH('pipeline_runs', 'qa_status') IS NULL
                ALTER TABLE pipeline_runs ADD qa_status NVARCHAR(64) NULL;
            """,
            """
            IF COL_LENGTH('pipeline_runs', 'approved_by') IS NULL
                ALTER TABLE pipeline_runs ADD approved_by NVARCHAR(255) NULL;
            """,
        ]
        with self._connect() as conn:
            cursor = conn.cursor()
            for stmt in statements:
                cursor.execute(stmt)
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes WHERE name = 'ix_pipeline_runs_pipeline'
                )
                BEGIN
                    CREATE INDEX ix_pipeline_runs_pipeline ON pipeline_runs(pipeline, started_at DESC);
                END
                """
            )
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes WHERE name = 'ix_pipeline_runs_group'
                )
                BEGIN
                    CREATE INDEX ix_pipeline_runs_group ON pipeline_runs(group_key);
                END
                """
            )
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes WHERE name = 'ix_pipeline_runs_project'
                )
                BEGIN
                    CREATE INDEX ix_pipeline_runs_project ON pipeline_runs(project_key);
                END
                """
            )
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes WHERE name = 'ix_pipeline_runs_card'
                )
                BEGIN
                    CREATE INDEX ix_pipeline_runs_card ON pipeline_runs(card_id);
                END
                """
            )
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1 FROM sys.indexes WHERE name = 'ix_pipeline_logs_run'
                )
                BEGIN
                    CREATE INDEX ix_pipeline_logs_run ON pipeline_logs(run_id, ts);
                END
                """
            )

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
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO pipeline_runs (
                    pipeline, [user], group_key, project_key, profiles, modules,
                    version, hotfix, card_id, unit_tests_status, qa_status, approved_by,
                    started_at, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    started_at,
                    "running",
                ),
            )
            cursor.execute("SELECT SCOPE_IDENTITY() AS id")
            row = cursor.fetchone() or {"id": 0}
            run_id = int(row.get("id") or 0)
        return run_id

    # ------------------------------------------------------------------
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
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE pipeline_runs
                   SET status = %s,
                       message = %s,
                       finished_at = %s,
                       unit_tests_status = COALESCE(%s, unit_tests_status),
                       qa_status = COALESCE(%s, qa_status),
                       approved_by = COALESCE(%s, approved_by)
                 WHERE id = %s
                """,
                (
                    status,
                    message,
                    finished_at,
                    unit_tests_status,
                    qa_status,
                    approved_by,
                    int(run_id),
                ),
            )

    # ------------------------------------------------------------------
    def log_message(self, run_id: int, message: str) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO pipeline_logs(run_id, message) VALUES(%s, %s)",
                (int(run_id), message),
            )

    # ------------------------------------------------------------------
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
        clauses: List[str] = []
        params: List[object] = []
        if pipeline:
            clauses.append("pipeline = %s")
            params.append(pipeline)
        if group_key:
            clauses.append("group_key = %s")
            params.append(group_key)
        if project_key:
            clauses.append("project_key = %s")
            params.append(project_key)
        if status:
            clauses.append("status = %s")
            params.append(status)
        if start:
            clauses.append("started_at >= %s")
            params.append(start)
        if end:
            clauses.append("started_at <= %s")
            params.append(end)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        safe_limit = max(1, int(limit or 1))
        sql = (
            "SELECT id, pipeline, [user], group_key, project_key, profiles, modules, version, hotfix, "
            "card_id, unit_tests_status, qa_status, approved_by, started_at, finished_at, status, message "
            f"FROM pipeline_runs{where} ORDER BY started_at DESC OFFSET 0 ROWS FETCH NEXT {safe_limit} ROWS ONLY"
        )
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        records: List[RunRecord] = []
        for row in rows:
            started = row.get("started_at")
            finished = row.get("finished_at")
            records.append(
                RunRecord(
                    id=int(row.get("id") or 0),
                    pipeline=row.get("pipeline") or "",
                    user=row.get("user"),
                    group_key=row.get("group_key"),
                    project_key=row.get("project_key"),
                    profiles=json.loads(row.get("profiles") or "[]"),
                    modules=json.loads(row.get("modules") or "[]"),
                    version=row.get("version"),
                    hotfix=None if row.get("hotfix") is None else bool(row.get("hotfix")),
                    card_id=row.get("card_id"),
                    unit_tests_status=row.get("unit_tests_status"),
                    qa_status=row.get("qa_status"),
                    approved_by=row.get("approved_by"),
                    started_at=(
                        started.isoformat(timespec="seconds") if isinstance(started, datetime) else str(started or "")
                    ),
                    finished_at=(
                        finished.isoformat(timespec="seconds") if isinstance(finished, datetime) else (str(finished) if finished else None)
                    ),
                    status=row.get("status"),
                    message=row.get("message"),
                )
            )
        return records

    # ------------------------------------------------------------------
    def get_logs(self, run_id: int) -> List[tuple[str, str]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT ts, message FROM pipeline_logs WHERE run_id = %s ORDER BY ts ASC",
                (int(run_id),),
            )
            rows = cursor.fetchall()
        result: List[tuple[str, str]] = []
        for row in rows:
            ts = row.get("ts")
            timestamp = ts.isoformat(timespec="seconds") if isinstance(ts, datetime) else str(ts or "")
            result.append((timestamp, row.get("message") or ""))
        return result

    # ------------------------------------------------------------------
    def clear(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM pipeline_logs")
            cursor.execute("DELETE FROM pipeline_runs")

    # ------------------------------------------------------------------
    def update_card_status(
        self,
        card_id: int,
        *,
        unit_tests_status: Optional[str] = None,
        qa_status: Optional[str] = None,
        approved_by: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TOP (1) id FROM pipeline_runs WHERE card_id = %s ORDER BY started_at DESC",
                (int(card_id),),
            )
            row = cursor.fetchone()
            if row:
                cursor.execute(
                    """
                    UPDATE pipeline_runs
                       SET unit_tests_status = COALESCE(%s, unit_tests_status),
                           qa_status = COALESCE(%s, qa_status),
                           approved_by = COALESCE(%s, approved_by)
                     WHERE id = %s
                    """,
                    (
                        unit_tests_status,
                        qa_status,
                        approved_by,
                        int(row.get("id") or 0),
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO pipeline_runs (
                        pipeline, [user], group_key, project_key, profiles, modules,
                        version, hotfix, card_id, unit_tests_status, qa_status, approved_by,
                        started_at, status
                    ) VALUES('card-check', NULL, NULL, NULL, '[]', '[]', NULL, NULL,
                             %s, %s, %s, %s, SYSUTCDATETIME(), 'running')
                    """,
                    (int(card_id), unit_tests_status, qa_status, approved_by),
                )

    # ------------------------------------------------------------------
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
