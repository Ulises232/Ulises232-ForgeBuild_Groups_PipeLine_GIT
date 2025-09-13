
from buildtool.core.git_local import GitLocal
from buildtool.core.history import HistoryDB

def build_local_summary(repo_path: str, history: HistoryDB, limit_hist: int = 50):
    git = GitLocal(repo_path)
    branches = git.local_branches()
    current = git.current_branch()
    rows = []
    for b in branches:
        rows.append({
            "repo": repo_path,
            "rama": b.name,
            "estado": "local (actual)" if b.is_current else "local",
        })
    for ts, repo, branch, estado, detalle in history.last_rows(limit_hist):
        if repo == repo_path:
            rows.append({"repo": repo, "rama": branch, "estado": estado, "detalle": f"{ts} {detalle or ''}".strip()})
    return rows
