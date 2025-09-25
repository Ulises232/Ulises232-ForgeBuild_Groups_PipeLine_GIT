from __future__ import annotations

from typing import Dict, Iterable, List, Optional


def filter_users_by_role(
    users: Iterable[str], roles_map: Dict[str, List[str]], required_role: Optional[str]
) -> List[str]:
    """Return usernames that match the required role, including leaders."""

    if not required_role:
        return list(users)
    required = required_role.lower()
    filtered: List[str] = []
    seen = set()
    for name in users:
        if name in seen:
            continue
        roles = [role.lower() for role in roles_map.get(name, []) if role]
        if required in roles or "leader" in roles:
            filtered.append(name)
            seen.add(name)
    return filtered
