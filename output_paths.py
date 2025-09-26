"""Utility helpers for consistent output directory management."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

DEFAULT_LOG_DIR_NAME = "logs"
DEFAULT_CSV_DIR_NAME = "CSVs"


def _resolve_directory(base: Path, override: Optional[str], default_name: str) -> Path:
    """Resolve an output directory relative to *base* and ensure it exists."""

    base = base.resolve()
    if override:
        candidate = Path(override)
        if not candidate.is_absolute():
            candidate = base / candidate
    else:
        candidate = base / default_name
    candidate.mkdir(parents=True, exist_ok=True)
    return candidate


def resolve_log_dir(base: Path, override: Optional[str]) -> Path:
    """Return the resolved log directory (defaulting to ``logs/``)."""

    return _resolve_directory(base, override, DEFAULT_LOG_DIR_NAME)


def resolve_csv_dir(base: Path, override: Optional[str]) -> Path:
    """Return the resolved CSV/XLSX directory (defaulting to ``CSVs/``)."""

    return _resolve_directory(base, override, DEFAULT_CSV_DIR_NAME)


def resolve_named_dir(base: Path, override: Optional[str], default_name: str) -> Path:
    """Resolve an arbitrary named directory relative to *base* with a default."""

    return _resolve_directory(base, override, default_name)
