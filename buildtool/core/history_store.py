from __future__ import annotations
from pathlib import Path
from typing import Optional
import json, time
from .branch_models import BranchIndex, BranchRecord, now_iso

class HistoryStoreError(Exception):
    pass

class HistoryStore:
    """
    Responsabilidad:
      - Cargar/guardar 'branches_index.json' desde/hacia NAS.
      - Escribir de forma atómica usando archivo temporal.
      - Lock de escritura simple mediante archivo .lock.
      - Merge por registro (estrategia simple: latest 'last_activity_at' gana).
    """
    def __init__(self, nas_dir: Path, timezone: str = "-06:00"):
        self.nas_dir = nas_dir
        self.tz = timezone
        self.index_path = nas_dir / "branches_index.json"
        self.lock_path  = nas_dir / "branches_index.lock"

    def _read_raw(self) -> dict:
        if not self.index_path.exists():
            return {"version": 1, "updated_at": now_iso(self.tz), "branches": []}
        with self.index_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def load(self) -> BranchIndex:
        data = self._read_raw()
        return BranchIndex.from_dict(data)

    def _acquire_lock(self, timeout_s: int = 8) -> bool:
        start = time.time()
        while time.time() - start < timeout_s:
            try:
                # modo x: falla si existe
                fd = self.lock_path.open("x")
                fd.close()
                return True
            except FileExistsError:
                time.sleep(0.2)
        return False

    def _release_lock(self) -> None:
        try:
            self.lock_path.unlink(missing_ok=True)
        except Exception:
            pass

    def save_atomic(self, index: BranchIndex) -> None:
        if not self.nas_dir.exists():
            self.nas_dir.mkdir(parents=True, exist_ok=True)

        if not self._acquire_lock():
            raise HistoryStoreError("No se pudo adquirir lock para escribir branches_index.json")

        try:
            index.updated_at = now_iso(self.tz)
            tmp = self.index_path.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(index.to_dict(), f, ensure_ascii=False, indent=2)
            tmp.replace(self.index_path)
        finally:
            self._release_lock()

    @staticmethod
    def _is_newer(a: str, b: str) -> bool:
        # Compara strings ISO 8601 simples. Para máxima precisión, parsear a datetime.
        return a > b

    def merge_and_save(self, local: BranchIndex) -> None:
        """
        Estrategia de merge por registro:
          - Empatar por branch_id o por name.
          - Para conflictos campo a campo, gana el que tenga 'last_activity_at' mayor.
          - 'exists_origin=true' prevalece si hay contradicción dura.
        """
        remote = self.load()
        merged = BranchIndex(version=max(local.version, remote.version), updated_at=now_iso(self.tz))

        # map por name (clave estable en git). Si hay branch_id, puedes mapear doble.
        by_name = {b.name: b for b in remote.branches}
        for lb in local.branches:
            rb = by_name.get(lb.name)
            if not rb:
                merged.branches.append(lb)
                continue

            # Resolver campo a campo:
            winner = rb
            if self._is_newer(lb.last_activity_at, rb.last_activity_at):
                winner = lb

            # exists_origin=true gana
            exists_origin = lb.exists_origin or rb.exists_origin
            exists_local  = lb.exists_local or rb.exists_local  # permisivo

            # construir registro resultante
            res = BranchRecord.from_dict({
                **winner.to_dict(),
                "exists_origin": exists_origin,
                "exists_local": exists_local,
                "record_version": max(lb.record_version, rb.record_version) + 1,
            })
            merged.branches.append(res)

            # elimina del mapa para que al final queden los solo-remotos restantes
            by_name.pop(lb.name, None)

        # Agregar los que solo están en remoto
        for name, rb in by_name.items():
            merged.branches.append(rb)

        self.save_atomic(merged)
