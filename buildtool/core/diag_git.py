
# buildtool/core/diag_git.py
from __future__ import annotations
from pathlib import Path
import subprocess, shutil, sys

def _run(cmd, cwd):
    p = subprocess.Popen(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=False)
    out = p.communicate()[0]
    return p.returncode, out

def diag(repo_path: str) -> str:
    lines = []
    lines.append("== DIAGNÓSTICO GIT ==")
    git_exe = shutil.which("git")
    lines.append(f"git: {git_exe or 'NO ENCONTRADO'}")
    if not git_exe:
        return "\n".join(lines)

    repo = Path(repo_path)
    lines.append(f"repo_path: {repo} exists={repo.exists()} is_dir={repo.is_dir()}")

    rc, out = _run(["git", "rev-parse", "--is-inside-work-tree"], repo)
    lines.append(f"rev-parse rc={rc} out={out.strip()}")
    if rc != 0:
        return "\n".join(lines + ["FALLO: No es un repo Git."])

    rc, out = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], repo)
    lines.append(f"current-branch: {out.strip()}")

    rc, out = _run(["git", "branch", "--list"], repo)
    lines.append("branches:")
    lines.extend([f"  {ln}" for ln in out.splitlines() if ln.strip()])

    tmp = "_addon_diag_tmp_"
    _run(["git", "branch", "-D", tmp], repo)
    rc, _ = _run(["git", "switch", "-c", tmp], repo)
    lines.append(f"create tmp branch rc={rc}")
    _run(["git", "switch", "-"], repo)
    _run(["git", "branch", "-D", tmp], repo)

    return "\n".join(lines)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python -m buildtool.core.diag_git C:\\ruta\\repo")
        sys.exit(1)
    print(diag(sys.argv[1]))
