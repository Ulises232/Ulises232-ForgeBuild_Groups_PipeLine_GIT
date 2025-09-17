"""Utilities to expose the ForgeBuild application version."""

from __future__ import annotations

from pathlib import Path

__all__ = ["__version__", "get_version"]


def _read_version() -> str:
    """Return the application version string.

    The canonical source of truth lives in the repository level ``VERSION`` file
    so that other tooling (PyInstaller, CI, packaging) can consume the same
    value.  A small ``0.dev0`` fallback keeps the UI usable even if the file is
    missing in non-standard deployments.
    """

    version_file = Path(__file__).resolve().parent.parent / "VERSION"
    try:
        text = version_file.read_text(encoding="utf-8")
    except OSError:
        return "0.dev0"
    version = text.strip()
    return version or "0.dev0"


__version__ = _read_version()


def get_version() -> str:
    """Public helper to obtain the current version string."""

    return __version__
