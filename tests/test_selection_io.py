from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import output_paths
import select_papers


def _seed_inputs(csv_dir: Path) -> None:
    csv_dir.mkdir(parents=True, exist_ok=True)
    precise_df = pd.DataFrame(
        [
            {
                "mode": "precise",
                "paperId": "p-001",
                "title": "Precise Sample",
            }
        ]
    )
    broad_df = pd.DataFrame(
        [
            {
                "mode": "broad",
                "paperId": "b-001",
                "title": "Broad Sample",
            }
        ]
    )
    precise_df.to_csv(csv_dir / "precise.csv", index=False)
    broad_df.to_csv(csv_dir / "broad.csv", index=False)


@pytest.fixture()
def selection_env(monkeypatch, tmp_path: Path) -> SimpleNamespace:
    monkeypatch.setattr(output_paths, "PROJECT_ROOT", tmp_path)
    csv_dir = output_paths.get_csv_dir()
    logs_dir = output_paths.get_logs_dir()
    _seed_inputs(csv_dir)
    return SimpleNamespace(root=tmp_path, csv_dir=csv_dir, logs_dir=logs_dir)


def _stubbed_params(_: str | None) -> dict:
    return {
        "s2": {
            "base_url": "https://example.invalid",
            "paper_batch_fields": "",
            "author_batch_fields": "",
            "references_fields": "",
            "retry_sleep_seconds": 0,
            "timeout_seconds": 1,
            "max_retries": 1,
            "batch_size": 1,
        }
    }


def _stubbed_enrich(df, params, logs_dir):
    provenance_dir = Path(logs_dir) / "provenance"
    provenance_dir.mkdir(parents=True, exist_ok=True)
    (provenance_dir / "run.json").write_text("{}", encoding="utf-8")
    return df


def _assert_outputs(env: SimpleNamespace) -> None:
    # All Excel artifacts should live directly within the CSV directory.
    excel_outputs = list(env.root.rglob("*.xlsx"))
    assert excel_outputs, "expected XLSX outputs"
    assert all(path.parent == env.csv_dir for path in excel_outputs)

    # Provenance text/log artifacts should live under the root logs directory.
    provenance_txt = env.logs_dir / "provenance.txt"
    assert provenance_txt.exists()
    assert (env.logs_dir / "provenance" / "run.json").exists()

    # The legacy ./output hierarchy must not be recreated.
    assert not (env.root / "output").exists()


def test_default_cli_uses_root_directories(monkeypatch, selection_env: SimpleNamespace) -> None:
    monkeypatch.setattr(select_papers, "load_params", _stubbed_params)
    monkeypatch.setattr(select_papers, "enrich_extract", _stubbed_enrich)

    select_papers.main([])

    _assert_outputs(selection_env)


def test_explicit_input_argument_respected(monkeypatch, selection_env: SimpleNamespace) -> None:
    monkeypatch.setattr(select_papers, "load_params", _stubbed_params)
    monkeypatch.setattr(select_papers, "enrich_extract", _stubbed_enrich)

    select_papers.main(["--input", str(selection_env.csv_dir)])

    _assert_outputs(selection_env)


def test_removed_output_argument_fails(monkeypatch, selection_env: SimpleNamespace) -> None:
    monkeypatch.setattr(select_papers, "load_params", _stubbed_params)
    monkeypatch.setattr(select_papers, "enrich_extract", _stubbed_enrich)

    with pytest.raises(SystemExit) as excinfo:
        select_papers.main(["--output", "ignored"])

    assert "removed" in str(excinfo.value)
