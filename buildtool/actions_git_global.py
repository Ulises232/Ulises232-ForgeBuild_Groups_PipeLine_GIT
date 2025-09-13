
from pathlib import Path
from buildtool.core.git_local import GitLocal
from buildtool.core.history import HistoryDB

HISTORY_DB = HistoryDB(Path.home() / ".forgebuild" / "history.sqlite3")

def crear_rama_global(repo_path: str, branch: str, base: str | None = None):
    git = GitLocal(repo_path)
    if not git.is_repo():
        raise RuntimeError(f"No es repo git: {repo_path}")
    git.create_branch_local(branch, base=base)
    git.switch(branch)
    HISTORY_DB.add(repo_path, branch, "local", "creada y checkout")

def switch_rama_global(repo_path: str, branch: str):
    git = GitLocal(repo_path)
    git.switch(branch)
    HISTORY_DB.add(repo_path, branch, "checkout", "switch")

def eliminar_rama_local_global(repo_path: str, branch: str, force: bool = False):
    git = GitLocal(repo_path)
    git.delete_branch_local(branch, force=force)
    HISTORY_DB.add(repo_path, branch, "eliminada_local", "borrada localmente")

def push_rama_global(repo_path: str, branch: str):
    git = GitLocal(repo_path)
    git.push_branch(branch)
    HISTORY_DB.add(repo_path, branch, "origin", "push manual")

def commit_todo_global(repo_path: str, mensaje: str):
    git = GitLocal(repo_path)
    git.commit_all(mensaje)
    cur = git.current_branch() or ""
    HISTORY_DB.add(repo_path, cur, "commit", mensaje)
