"""Utility script to migrate branch history data into SQL Server."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Iterable, List

from buildtool.core.branch_history_db import BranchHistoryDB


def _chunked(iterable: Iterable[dict], size: int) -> Iterable[List[dict]]:
    batch: List[dict] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def migrate(sqlite_path: Path, connection_url: str, batch_size: int = 500) -> None:
    source = BranchHistoryDB(sqlite_path, connection_url="")
    target = BranchHistoryDB(sqlite_path, connection_url=connection_url)

    branches = source.fetch_branches()
    target.replace_branches(branches)

    for chunk in _chunked(source.fetch_activity(), batch_size):
        target.append_activity(chunk)

    for sprint in source.fetch_sprints():
        target.upsert_sprint(sprint)

    for card in source.fetch_cards():
        target.upsert_card(card)

    for user in source.fetch_users():
        target.upsert_user(user)

    for role in source.fetch_roles():
        target.upsert_role(role)

    roles = defaultdict(list)
    for item in source.fetch_user_roles():
        roles[item["username"]].append(item["role_key"])
    for username, assigned in roles.items():
        target.set_user_roles(username, assigned)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite", type=Path, help="Ruta al archivo branches_history.sqlite3 existente")
    parser.add_argument("url", help="Cadena de conexión SQLAlchemy hacia SQL Server")
    parser.add_argument(
        "--batch",
        type=int,
        default=500,
        help="Tamaño de lote para migrar el activity log (default: 500)",
    )
    args = parser.parse_args()
    migrate(args.sqlite, args.url, args.batch)


if __name__ == "__main__":  # pragma: no cover - script entry point
    main()
