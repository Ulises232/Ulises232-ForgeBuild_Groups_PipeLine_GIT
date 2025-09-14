# buildtool/tests/branch_history_smoke.py
from __future__ import annotations
import os
from pathlib import Path
import uuid
from pprint import pprint

# Importa los stubs que creamos en esta rama
from buildtool.core.branch_models import BranchIndex, BranchRecord, now_iso, new_branch_id
from buildtool.core.history_store import HistoryStore, HistoryStoreError
from buildtool.core.activity_logger import ActivityLogger

def _default_nas_dir() -> Path:
    """
    Usa variable de entorno NAS_DIR si existe, si no, crea un folder local ./_nas_dev
    para no depender aún de la NAS real en el smoke.
    """
    env_dir = os.environ.get("NAS_DIR")
    if env_dir:
        return Path(env_dir)
    return Path("./_nas_dev")

def create_sample_record(name: str, project: str, group: str, user: str) -> BranchRecord:
    return BranchRecord(
        branch_id=new_branch_id(),
        name=name,
        project=project,
        group=group,
        created_by=user,
        created_at=now_iso(),

        exists_local=True,
        exists_origin=False,

        merge_status="none",
        merge_target=None,
        merge_commit=None,
        merged_at=None,

        diverged=False,
        stale_days=0,

        last_activity_at=now_iso(),
        last_activity_user=user,

        notes="Smoke test record",
        record_version=1,
    )

def run_smoke() -> int:
    print("[SMOKE] branch-history-schema")
    nas_dir = _default_nas_dir()
    tz = "-06:00"

    print(f"[SMOKE] usando NAS_DIR: {nas_dir.resolve()}")
    nas_dir.mkdir(parents=True, exist_ok=True)

    store = HistoryStore(nas_dir=nas_dir, timezone=tz)
    logger = ActivityLogger(nas_dir=nas_dir, tz=tz, app_version="0.1.0-smoke")

    # 1) Cargar índice actual (si no existe, retorna vació por diseño)
    idx = store.load()
    print(f"[SMOKE] index version={idx.version} branches_iniciales={len(idx.branches)}")

    # 2) Agregar 2 ramas de ejemplo
    rec1 = create_sample_record("feature/PIPE-123-ajustes", "lease-core", "GSA", "ulises")
    rec2 = create_sample_record("hotfix/PIPE-222-npe", "lease-admin", "GSA", "ana")

    idx.upsert(rec1)
    idx.upsert(rec2)

    # 3) Guardar de forma atómica
    store.save_atomic(idx)
    print("[SMOKE] save_atomic OK")

    # 4) Simular una actualización local y luego merge con remoto
    #    (a) marcar que rec1 ahora existe en origin
    rec1.exists_origin = True
    rec1.last_activity_user = "ulises"
    rec1.last_activity_at = now_iso()
    idx.upsert(rec1)

    #    (b) merge con remoto y guardar
    store.merge_and_save(idx)
    print("[SMOKE] merge_and_save OK")

    # 5) Registrar actividad en jsonl
    logger.log(
        user="ulises",
        project=rec1.project,
        group=rec1.group,
        branch=rec1.name,
        action="push",
        result="ok",
        detail="create remote",
        sha_from=None,
        sha_to=uuid.uuid4().hex[:8]
    )
    logger.log(
        user="ana",
        project=rec2.project,
        group=rec2.group,
        branch=rec2.name,
        action="create_branch",
        result="ok",
        detail="from main"
    )
    print("[SMOKE] activity_log append OK")

    # 6) Recargar y mostrar un resumen
    idx2 = store.load()
    print(f"[SMOKE] index recargado: version={idx2.version} branches={len(idx2.branches)}")
    for b in idx2.branches:
        print("  -", b.name, "| local:", b.exists_local, "origin:", b.exists_origin, "merge:", b.merge_status)

    print("[SMOKE] OK ✅")
    return 0

if __name__ == "__main__":
    raise SystemExit(run_smoke())
