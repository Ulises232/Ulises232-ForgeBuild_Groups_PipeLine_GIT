#!/usr/bin/env python
"""Herramienta de migración de SQLite a SQL Server para el historial de ramas."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, Iterable, List

from buildtool.core.branch_history_db import BranchHistoryRepo


def _import_sprints(source: BranchHistoryRepo, target: BranchHistoryRepo) -> None:
    for row in source.fetch_sprints():
        target.upsert_sprint(row)


def _import_cards(source: BranchHistoryRepo, target: BranchHistoryRepo) -> None:
    for row in source.fetch_cards():
        target.upsert_card(row)


def _import_card_scripts(source: BranchHistoryRepo, target: BranchHistoryRepo) -> None:
    for row in source.fetch_cards():
        card_id = row.get("id")
        if not card_id:
            continue
        script = source.fetch_card_script(int(card_id))
        if not script:
            continue
        payload = script.copy()
        payload["card_id"] = int(card_id)
        target.upsert_card_script(payload)


def _import_users(source: BranchHistoryRepo, target: BranchHistoryRepo) -> None:
    for row in source.fetch_users():
        target.upsert_user(row)
    for row in source.fetch_roles():
        target.upsert_role(row)
    assignments: Dict[str, List[str]] = defaultdict(list)
    for row in source.fetch_user_roles():
        user = row.get("username") or ""
        role = row.get("role_key") or ""
        if user and role:
            assignments[user].append(role)
    for user, roles in assignments.items():
        target.set_user_roles(user, roles)


def _import_activity(source: BranchHistoryRepo, target: BranchHistoryRepo) -> None:
    entries = source.fetch_activity()
    target.append_activity(entries)


def migrate(sqlite_path: Path, sqlserver_url: str, *, pool_size: int = 5) -> None:
    if not sqlserver_url:
        raise SystemExit("Debe proporcionar la URL de destino de SQL Server.")

    source = BranchHistoryRepo(sqlite_path, backend="sqlite")
    target = BranchHistoryRepo(sqlite_path, backend="sqlserver", url=sqlserver_url, pool_size=pool_size)

    branches = source.fetch_branches()
    target.replace_branches(branches)
    _import_sprints(source, target)
    _import_cards(source, target)
    _import_card_scripts(source, target)
    _import_users(source, target)
    _import_activity(source, target)


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sqlite",
        type=Path,
        default=Path.cwd() / "branches_history.sqlite3",
        help="Ruta del archivo SQLite fuente (por defecto: ./branches_history.sqlite3)",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("BRANCH_HISTORY_DB_URL"),
        help="Cadena de conexión de SQL Server (por defecto BRANCH_HISTORY_DB_URL)",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=5,
        help="Tamaño máximo del pool de conexiones del destino",
    )
    args = parser.parse_args(argv)

    migrate(args.sqlite, args.url, pool_size=args.pool_size)
    print("Migración completada correctamente.")


if __name__ == "__main__":
    main()
