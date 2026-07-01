"""Helpers for validating optional runtime dependencies used by CLI scripts."""
from __future__ import annotations

import importlib.util
from collections.abc import Iterable


def missing_modules(module_names: Iterable[str]) -> list[str]:
    """Return the import names that are not available in the current Python environment."""
    return [name for name in module_names if importlib.util.find_spec(name) is None]


def format_missing_dependency_message(
    missing: Iterable[str], *, command: str = "python -m pip install -r requirements.txt"
) -> str:
    """Build an actionable error message for missing runtime dependencies."""
    missing_list = list(missing)
    dependency_label = "dependency" if len(missing_list) == 1 else "dependencies"
    return (
        f"Missing required Python {dependency_label}: {', '.join(missing_list)}.\n"
        f"Install the project dependencies with:\n  {command}"
    )
