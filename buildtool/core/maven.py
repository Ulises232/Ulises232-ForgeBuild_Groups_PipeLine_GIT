
from __future__ import annotations
import subprocess, os, shlex

def run_maven(module_path: str, goals, profile: str|None=None, env: dict|None=None, log_cb=print, separate_window: bool=False) -> int:
    mvn_exe = "mvn.cmd" if os.name == "nt" else "mvn"
    mvn_cmd = [mvn_exe, *goals]
    if profile:
        mvn_cmd += ["-P", profile]

    log_cb(f"$ cd {module_path}")
    log_cb("$ " + " ".join(shlex.quote(x) for x in mvn_cmd))

    creationflags = 0
    if separate_window and os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)

    proc = subprocess.Popen(
        mvn_cmd,
        cwd=module_path,
        stdout=None if separate_window else subprocess.PIPE,
        stderr=None if separate_window else subprocess.STDOUT,
        text=True,
        env={**os.environ, **(env or {})},
        creationflags=creationflags
    )

    if not separate_window:
        assert proc.stdout is not None
        for line in proc.stdout:
            log_cb(line.rstrip())
    else:
        log_cb("[Ventana separada lanzada]")

    return proc.wait()
