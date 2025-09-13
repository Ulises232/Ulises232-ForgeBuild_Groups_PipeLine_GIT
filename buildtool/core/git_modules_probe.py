
# buildtool/core/git_modules_probe.py
# Utilidad de consola para listar cómo el addon detecta rutas y repos.
from __future__ import annotations
from pathlib import Path
import os
from buildtool.core.git_tasks_local import _discover_repos  # type: ignore
from buildtool.core.git_console_trace import clog

class _DummyCfg: pass

if __name__ == "__main__":
    cfg = _DummyCfg()
    # Permite pasar HERR_REPO como raíz de escaneo por FS
    base = os.environ.get("HERR_REPO", "") or "."
    clog(f"[probe] usando HERR_REPO/cwd base: {Path(base).resolve()}")
    repos = _discover_repos(cfg, None, None, None)
    if not repos:
        clog("[probe] No se detectaron repos.")
    else:
        clog("[probe] Repos detectados:")
        for name, path in repos:
            clog(f"  - {name}: {path}")
