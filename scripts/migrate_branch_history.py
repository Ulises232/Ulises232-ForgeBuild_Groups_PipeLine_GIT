#!/usr/bin/env python3
"""Migración de historial de ramas desde SQLite hacia SQL Server."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from buildtool.core.branch_history_db import BranchHistoryDB


DEFAULT_SQLITE_PATH = (
    Path(os.environ.get("APPDATA", Path.home() / ".forgebuild")) / "branches_history.sqlite3"
)


def _chunk(items: Sequence[dict], size: int) -> Iterable[Sequence[dict]]:
    if size <= 0:
        yield items
        return
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _wipe_target(db: BranchHistoryDB) -> None:
    """Elimina todos los registros existentes del backend de destino."""

    db.prune_activity([])
    for sprint in db.fetch_sprints():
        db.delete_sprint(int(sprint.get("id")))
    for branch in db.fetch_branches():
        db.delete_branch(branch.get("key", ""))
    for user in db.fetch_users():
        db.delete_user(user.get("username", ""))
    for role in db.fetch_roles():
        db.delete_role(role.get("key", ""))


def migrate(sqlite_path: Path, sqlserver_url: str, *, pool_size: int, skip_wipe: bool, batch_size: int) -> None:
    source = BranchHistoryDB(sqlite_path, backend="sqlite")
    target = BranchHistoryDB(None, backend="sqlserver", url=sqlserver_url, pool_size=pool_size)

    if not skip_wipe:
        _wipe_target(target)

    branches = source.fetch_branches()
    target.replace_branches(branches)
    print(f"Branches migrados: {len(branches)}")

    activities = source.fetch_activity()
    if not skip_wipe:
        target.prune_activity([])
    for batch in _chunk(activities, batch_size):
        target.append_activity(batch)
    print(f"Actividades migradas: {len(activities)}")

    sprints = source.fetch_sprints()
    for sprint in sprints:
        target.upsert_sprint(sprint)
    print(f"Sprints migrados: {len(sprints)}")

    cards = source.fetch_cards()
    for card in cards:
        target.upsert_card(card)
    print(f"Tarjetas migradas: {len(cards)}")

    users = source.fetch_users()
    for user in users:
        target.upsert_user(user)
    print(f"Usuarios migrados: {len(users)}")

    roles = source.fetch_roles()
    for role in roles:
        target.upsert_role(role)
    print(f"Roles migrados: {len(roles)}")

    user_roles = source.fetch_user_roles()
    assignments: dict[str, list[str]] = {}
    for item in user_roles:
        username = item.get("username")
        if not username:
            continue
        assignments.setdefault(username, []).append(item.get("role_key", ""))
    for username, roles_keys in assignments.items():
        target.set_user_roles(username, roles_keys)
    print(f"Asignaciones de roles migradas: {len(user_roles)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sqlite-path",
        default=str(DEFAULT_SQLITE_PATH),
        help="Ruta al archivo SQLite fuente (branches_history.sqlite3)",
    )
    parser.add_argument(
        "--sqlserver-url",
        required=True,
        help="Cadena de conexión mssql://usuario:password@host:puerto/base",
    )
    parser.add_argument(
        "--pool-size",
        type=int,
        default=8,
        help="Tamaño del pool de conexiones hacia SQL Server",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Tamaño del lote para insertar actividades",
    )
    parser.add_argument(
        "--skip-wipe",
        action="store_true",
        help="No limpia previamente el backend de destino (sobrescribe registros existentes)",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path).expanduser()
    if not sqlite_path.exists():
        raise SystemExit(f"No se encontró la base SQLite en {sqlite_path}")

    migrate(
        sqlite_path,
        args.sqlserver_url,
        pool_size=max(1, args.pool_size),
        skip_wipe=args.skip_wipe,
        batch_size=max(1, args.batch_size),
    )


if __name__ == "__main__":
    main()
