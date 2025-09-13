
from __future__ import annotations
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple

def run(cmd: List[str], cwd: str) -> Tuple[int, str, str]:
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=False)
    out, err = p.communicate()
    return p.returncode, out.strip(), err.strip()

@dataclass
class BranchInfo:
    name: str
    is_current: bool

class GitLocal:
    def __init__(self, repo_path: str) -> None:
        self.repo_path = repo_path

    def is_repo(self) -> bool:
        code, _, _ = run(["git", "rev-parse", "--is-inside-work-tree"], self.repo_path)
        return code == 0

    def current_branch(self) -> Optional[str]:
        code, out, err = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], self.repo_path)
        if code == 0:
            return out
        raise RuntimeError(f"Error leyendo rama actual: {err}")

    def local_branches(self) -> List[BranchInfo]:
        code, out, err = run(["git", "branch", "--list"], self.repo_path)
        if code != 0:
            raise RuntimeError(f"Error listando ramas: {err}")
        res: List[BranchInfo] = []
        for line in out.splitlines():
            line = line.rstrip()
            if not line:
                continue
            is_current = line.startswith("* ")
            name = line[2:] if is_current else line.strip()
            res.append(BranchInfo(name=name, is_current=is_current))
        return res

    def create_branch_local(self, branch: str, base: Optional[str] = None) -> None:
        cmd = ["git", "branch", branch] if base is None else ["git", "branch", branch, base]
        code, _, err = run(cmd, self.repo_path)
        if code != 0 and "already exists" not in err:
            raise RuntimeError(f"No se pudo crear la rama '{branch}': {err}")

    def switch(self, branch: str, create_if_missing: bool = False) -> None:
        if create_if_missing:
            code, _, err = run(["git", "checkout", "-B", branch, "--no-track"], self.repo_path)
        else:
            code, _, err = run(["git", "checkout", branch], self.repo_path)
        if code != 0:
            raise RuntimeError(f"No se pudo cambiar a '{branch}': {err}")

    def delete_branch_local(self, branch: str, force: bool = False) -> None:
        code, _, err = run(["git", "branch", "-D" if force else "-d", branch], self.repo_path)
        if code != 0:
            raise RuntimeError(f"No se pudo eliminar '{branch}': {err}")

    def commit_all(self, message: str) -> None:
        code, _, err = run(["git", "add", "-A"], self.repo_path)
        if code != 0:
            raise RuntimeError(f"git add falló: {err}")
        code, _, err = run(["git", "commit", "-m", message], self.repo_path)
        if code != 0 and "nothing to commit" not in err.lower():
            raise RuntimeError(f"git commit falló: {err}")

    def push_branch(self, branch: str, remote: str = "origin") -> None:
        code, _, err = run(["git", "push", remote, branch], self.repo_path)
        if code != 0:
            raise RuntimeError(f"push falló: {err}")
