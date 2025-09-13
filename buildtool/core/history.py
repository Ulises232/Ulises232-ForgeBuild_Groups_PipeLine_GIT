
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS branch_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  repo TEXT NOT NULL,
  branch TEXT NOT NULL,
  estado TEXT NOT NULL,     -- 'local', 'origin', 'eliminada_local', 'checkout', 'commit'
  detalle TEXT,
  ts DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_history_repo_branch ON branch_history(repo, branch, ts DESC);
"""

class HistoryDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as cx:
            cx.executescript(SCHEMA)

    def add(self, repo: str, branch: str, estado: str, detalle: str = "") -> None:
        with sqlite3.connect(self.db_path) as cx:
            cx.execute("INSERT INTO branch_history(repo, branch, estado, detalle) VALUES(?,?,?,?)",
                       (repo, branch, estado, detalle))

    def last_rows(self, limit: int = 100) -> Iterable[Tuple]:
        with sqlite3.connect(self.db_path) as cx:
            cur = cx.execute("""
                SELECT ts, repo, branch, estado, detalle
                FROM branch_history
                ORDER BY ts DESC
                LIMIT ?
            """, (limit,))
            return list(cur.fetchall())
