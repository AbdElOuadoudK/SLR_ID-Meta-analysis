import json
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import csv

import collect
from slr_meta.collection import semantic_scholar


class FakeResponse:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text
        self.headers = {}

    def json(self):
        return self._payload


def test_collect_import_alias_exposes_collection_module():
    assert collect is semantic_scholar
    assert callable(collect.main)


def test_collect_script_entrypoint_invokes_main(monkeypatch):
    called = []

    def fake_main():
        called.append(True)

    monkeypatch.setattr(semantic_scholar, "main", fake_main)
    runpy.run_path("collect.py", run_name="__main__")

    assert called == [True]


def test_run_mode_fetches_token_pages_and_writes_outputs(tmp_path, monkeypatch):
    responses = [
        FakeResponse(
            {
                "data": [
                    {
                        "paperId": "p1",
                        "title": "First",
                        "publicationDate": "2024-03-01",
                        "publicationTypes": ["Review"],
                        "fieldsOfStudy": ["Computer Science"],
                        "influentialCitationCount": 5,
                    }
                ],
                "token": "next",
            }
        ),
        FakeResponse(
            {
                "data": [
                    {
                        "paperId": "p2",
                        "title": "Second",
                        "publicationDate": "2025",
                        "publicationTypes": None,
                        "fieldsOfStudy": ["Computer Science"],
                        "influentialCitationCount": 2,
                    }
                ]
            }
        ),
    ]
    captured_params = []

    def fake_fetch(endpoint, params, headers, timeout=60):
        captured_params.append(dict(params))
        return responses.pop(0)

    monkeypatch.setattr(semantic_scholar, "fetch_with_retries", fake_fetch)

    cfg = {
        "endpoint": "https://example.test/bulk",
        "query": "intrusion detection",
        "year": "2022-2026",
        "fieldsOfStudy": "Computer Science",
        "fields": "paperId,title,publicationDate",
        "limit": 1000,
        "publicationTypes": "Review",
        "headers": {},
    }

    ledger = semantic_scholar.run_mode(
        cfg,
        "broad",
        "2026-06-30T00:00:00Z",
        tmp_path / "raw",
        tmp_path / "csv",
    )

    assert captured_params[0]["query"] == "intrusion detection"
    assert "token" not in captured_params[0]
    assert captured_params[1]["token"] == "next"
    assert ledger["hits_retrieved"] == 2
    assert ledger["notes"] == []

    merged = json.loads(
        (tmp_path / "raw" / "broad-bulk-raw.json").read_text(encoding="utf-8")
    )
    assert [record["paperId"] for record in merged["data"]] == ["p1", "p2"]

    with open(tmp_path / "csv" / "broad.csv", encoding="utf-8", newline="") as handle:
        csv_rows = list(csv.DictReader(handle))
    assert [row["mode"] for row in csv_rows] == ["BROAD", "BROAD"]
    assert [row["year"] for row in csv_rows] == ["2024", "2025"]
