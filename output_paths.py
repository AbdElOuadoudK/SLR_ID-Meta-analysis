"""Utility helpers for consistent output directory management."""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

DEFAULT_LOG_DIR_NAME = "logs"
DEFAULT_CSV_DIR_NAME = "CSVs"

# The repository root is the directory containing this helper module.
PROJECT_ROOT = Path(__file__).resolve().parent


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


def get_csv_dir() -> Path:
    """Return the root-level CSV directory, creating it when necessary."""

    path = PROJECT_ROOT / DEFAULT_CSV_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_logs_dir() -> Path:
    """Return the root-level logs directory, creating it when necessary."""

    path = PROJECT_ROOT / DEFAULT_LOG_DIR_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_output_directories() -> None:
    """Ensure the standardized CSV and log directories exist."""

    get_csv_dir()
    get_logs_dir()


def fail_on_removed_output_argument(argv: Sequence[str]) -> None:
    """Fail fast when legacy ``--output`` arguments are provided."""

    for token in argv:
        if token == "--output" or token.startswith("--output="):
            raise SystemExit(
                "The --output option has been removed. Files now save to /CSVs and logs to /logs."
            )
