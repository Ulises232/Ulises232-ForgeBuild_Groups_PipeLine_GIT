"""Herramienta de línea de comandos para migrar branch_history hacia SQL Server."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, List

from buildtool.core.branch_history_db import BranchHistoryDB


def _load_sqlite_db(sqlite_path: Path) -> BranchHistoryDB:
    url = f"file://{sqlite_path.resolve()}"
    return BranchHistoryDB(sqlite_path, connection_url=url)


def _load_sqlserver_db(connection_url: str) -> BranchHistoryDB:
    dummy = Path(os.getcwd()) / "_branch_history_placeholder.sqlite3"
    return BranchHistoryDB(dummy, connection_url=connection_url)


def _truncate_destination(dst: BranchHistoryDB) -> None:
    tables = [
        "user_roles",
        "roles",
        "users",
        "cards",
        "sprints",
        "activity_log",
        "branches",
    ]
    for table in tables:
        dst._backend.execute(f"DELETE FROM {table}")  # type: ignore[attr-defined]


def migrate(sqlite_path: Path, connection_url: str, *, truncate: bool = False) -> None:
    src = _load_sqlite_db(sqlite_path)
    dst = _load_sqlserver_db(connection_url)

    if truncate:
        _truncate_destination(dst)

    branches = src.fetch_branches()
    if branches:
        if truncate:
            dst.replace_branches(branches)
        else:
            for branch in branches:
                dst.upsert_branch(branch)

    activity = src.fetch_activity()
    if activity:
        dst.append_activity(activity)

    for sprint in src.fetch_sprints():
        dst.upsert_sprint(sprint)

    for card in src.fetch_cards():
        dst.upsert_card(card)

    for user in src.fetch_users():
        dst.upsert_user(user)

    for role in src.fetch_roles():
        dst.upsert_role(role)

    user_roles: Dict[str, List[str]] = {}
    for row in src.fetch_user_roles():
        username = row.get("username") or ""
        role_key = row.get("role_key") or ""
        if not username or not role_key:
            continue
        user_roles.setdefault(username, []).append(role_key)
    for username, roles in user_roles.items():
        dst.set_user_roles(username, roles)


def main() -> None:
    parser = argparse.ArgumentParser(description="Migra branch_history hacia SQL Server")
    parser.add_argument(
        "--sqlite",
        required=True,
        help="Ruta al archivo branches_history.sqlite3 de origen",
    )
    parser.add_argument(
        "--sqlserver",
        required=True,
        help=(
            "Cadena de conexión ODBC (por ejemplo mssql+pyodbc:///?odbc_connect=...) o URL mssql+pyodbc"
        ),
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Limpia las tablas destino antes de migrar",
    )
    args = parser.parse_args()

    migrate(Path(args.sqlite), args.sqlserver, truncate=args.truncate)


if __name__ == "__main__":
    main()
