from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

MERGE_STATUS = {"none", "merged", "partial"}

def now_iso(tz_offset: str = "-06:00") -> str:
    # Simple: datetime.utcnow() + sufijo tz. Para producción: usar zoneinfo/pytz.
    return datetime.utcnow().strftime(f"%Y-%m-%dT%H:%M:%S{tz_offset}")

def new_branch_id() -> str:
    return f"b-{uuid.uuid4().hex[:6]}"

@dataclass
class BranchRecord:
    branch_id: str
    name: str
    project: str
    group: str
    created_by: str
    created_at: str

    exists_local: bool
    exists_origin: bool

    merge_status: str = "none"     # none | merged | partial
    merge_target: Optional[str] = None
    merge_commit: Optional[str] = None
    merged_at: Optional[str] = None

    diverged: bool = False
    stale_days: int = 0

    last_activity_at: str = field(default_factory=now_iso)
    last_activity_user: str = ""

    notes: Optional[str] = None
    record_version: int = 1

    def validate(self) -> None:
        if self.merge_status not in MERGE_STATUS:
            raise ValueError(f"merge_status inválido: {self.merge_status}")
        if not self.name or "/" not in self.name:
            # Opcional: exigir convención tipo feature/*, hotfix/*, etc.
            pass
        if self.stale_days < 0:
            self.stale_days = 0

    def to_dict(self) -> Dict[str, Any]:
        self.validate()
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BranchRecord":
        rec = BranchRecord(**d)
        rec.validate()
        return rec

@dataclass
class BranchIndex:
    version: int
    updated_at: str
    branches: List[BranchRecord] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "updated_at": self.updated_at,
            "branches": [b.to_dict() for b in self.branches],
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "BranchIndex":
        branches = [BranchRecord.from_dict(x) for x in d.get("branches", [])]
        return BranchIndex(
            version=int(d.get("version", 1)),
            updated_at=d.get("updated_at", now_iso()),
            branches=branches,
        )

    def find_by_id(self, branch_id: str) -> Optional[BranchRecord]:
        return next((b for b in self.branches if b.branch_id == branch_id), None)

    def find_by_name(self, name: str) -> Optional[BranchRecord]:
        return next((b for b in self.branches if b.name == name), None)

    def upsert(self, rec: BranchRecord) -> None:
        existing = self.find_by_id(rec.branch_id) or self.find_by_name(rec.name)
        if existing:
            # Control de concurrencia básico (el store se encargará de la versión global)
            existing.__dict__.update(rec.to_dict())
            existing.record_version += 1
        else:
            self.branches.append(rec)

    def remove_if_pure_local_deleted(self, name: str) -> bool:
        """
        Si la rama nunca estuvo en origin y se borró local -> eliminar del historial.
        Devuelve True si se eliminó.
        """
        b = self.find_by_name(name)
        if not b:
            return False
        if (not b.exists_origin) and (not b.exists_local):
            self.branches = [x for x in self.branches if x.name != name]
            return True
        return False
