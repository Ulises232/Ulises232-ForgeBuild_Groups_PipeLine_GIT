"""Utility helpers for environment configuration."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional
import os


@lru_cache(maxsize=1)
def load_dotenv(extra_paths: Optional[Iterable[Path]] = None) -> None:
    """Load environment variables from ``.env`` files if present.

    Parameters
    ----------
    extra_paths:
        Optional additional paths to inspect. Default paths include the project
        root (two levels above this file) and the state directory
        ``~/.forgebuild``.
    """

    default_paths = [
        Path(__file__).resolve().parents[2] / ".env",
        Path.home() / ".forgebuild" / ".env",
    ]
    if extra_paths:
        default_paths.extend(extra_paths)

    for path in default_paths:
        try:
            if not path.exists():
                continue
            for raw in path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key:
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
        except OSError:
            continue
