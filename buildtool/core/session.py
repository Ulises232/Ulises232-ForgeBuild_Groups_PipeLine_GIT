from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Set

from .branch_store import User


@dataclass
class SessionState:
    user: Optional[User] = None
    roles: Optional[Set[str]] = None


_STATE = SessionState()


def set_active_user(user: Optional[User], roles: Optional[Set[str]] = None) -> None:
    _STATE.user = user
    _STATE.roles = set(roles) if roles else set()


def get_active_user() -> Optional[User]:
    return _STATE.user


def current_username(fallback: str = "") -> str:
    user = get_active_user()
    if user:
        return user.username
    return fallback


def user_has_role(role: str) -> bool:
    if not role:
        return True
    roles = _STATE.roles or set()
    return role in roles


def require_roles(*roles: str) -> bool:
    if not roles:
        return True
    current = _STATE.roles or set()
    return any(role in current for role in roles)
