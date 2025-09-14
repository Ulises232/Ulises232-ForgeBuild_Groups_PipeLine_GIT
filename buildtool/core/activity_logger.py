from __future__ import annotations
from pathlib import Path
from typing import Optional, Dict, Any
import json, time
from .branch_models import now_iso

class ActivityLogger:
    """
    Escribe eventos en 'activity_log.jsonl' (append-only).
    Uso:
        logger = ActivityLogger(nas_dir, tz="-06:00", app_version="1.0.0")
        logger.log(user="ulises", project="lease-core", group="GSA",
                   branch="feature/PIPE-123", action="create_branch",
                   result="ok", detail="Base develop", sha_from="abc", sha_to=None)
    """
    def __init__(self, nas_dir: Path, tz: str = "-06:00", app_version: str = "1.0.0"):
        self.nas_dir = nas_dir
        self.tz = tz
        self.app_version = app_version
        self.path = nas_dir / "activity_log.jsonl"

    def log(self, *, user: str, project: str, group: str, branch: str,
            action: str, result: str = "ok", detail: Optional[str] = None,
            sha_from: Optional[str] = None, sha_to: Optional[str] = None,
            extra: Optional[Dict[str, Any]] = None) -> None:
        if not self.nas_dir.exists():
            self.nas_dir.mkdir(parents=True, exist_ok=True)
        ev = {
            "ts": now_iso(self.tz),
            "user": user,
            "project": project,
            "group": group,
            "branch": branch,
            "action": action,
            "result": result,
            "detail": detail,
            "sha_from": sha_from,
            "sha_to": sha_to,
            "app_version": self.app_version,
        }
        if extra:
            ev.update(extra)
        line = json.dumps(ev, ensure_ascii=False)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
