import json
import runpy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

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


def test_semantic_scholar_headers_adds_api_key(monkeypatch):
    monkeypatch.setenv(semantic_scholar.SEMANTIC_SCHOLAR_API_KEY_ENV, "  secret  ")

    headers = semantic_scholar.semantic_scholar_headers({"Accept": "application/json"})

    assert headers == {"Accept": "application/json", "x-api-key": "secret"}


def test_normalize_bulk_query_converts_word_boolean_operators():
    query = '("intrusion" OR "intrusion detection") AND (review OR survey)'

    assert semantic_scholar.normalize_bulk_query(query) == '("intrusion" | "intrusion detection") + (review | survey)'


def test_mode_config_merges_shared_and_mode_specific_values():
    unified_config = {
        "endpoint": "https://example.test/bulk",
        "limit": 1000,
        "fieldsOfStudy": "Computer Science",
        "year": "2022-2026",
        "fields": "paperId,title,publicationDate",
        "publicationTypes": "Review",
        "headers": {},
        "modes": {
            "broad": {
                "mode": "BROAD",
                "query": "broad query",
            },
            "precise": {
                "mode": "PRECISE",
                "query": "precise query",
            },
        },
    }

    cfg = semantic_scholar.mode_config(unified_config, "precise")

    assert cfg["endpoint"] == "https://example.test/bulk"
    assert cfg["limit"] == 1000
    assert cfg["publicationTypes"] == "Review"
    assert cfg["mode"] == "PRECISE"
    assert cfg["query"] == "precise query"
    assert "modes" not in cfg


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

    class FakeClient:
        def get(self, endpoint, params):
            captured_params.append(dict(params))
            return responses.pop(0)

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
        client=FakeClient(),
    )

    assert captured_params[0]["query"] == "intrusion detection"
    assert "token" not in captured_params[0]
    assert captured_params[1]["token"] == "next"
    assert ledger["query"] == "intrusion detection"
    assert ledger["params_json"]["original_query"] == "intrusion detection"
    assert ledger["hits_reported"] is None
    assert ledger["hits_retrieved"] == 2
    assert ledger["http_status_codes"] == [200, 200]
    assert ledger["notes"] == []

    merged = json.loads(
        (tmp_path / "raw" / "broad-bulk-raw.json").read_text(encoding="utf-8")
    )
    assert [record["paperId"] for record in merged["data"]] == ["p1", "p2"]

    csv = pd.read_csv(tmp_path / "csv" / "broad.csv")
    assert csv["mode"].tolist() == ["BROAD", "BROAD"]
    assert csv["year"].tolist() == [2024, 2025]

def test_run_mode_raises_on_http_error_before_writing_empty_csv(tmp_path):
    class ErrorResponse(FakeResponse):
        def __init__(self):
            super().__init__({}, status_code=403, text="forbidden")
            self.url = "https://example.test/bulk?query=test"

    class FakeClient:
        def get(self, endpoint, params):
            return ErrorResponse()

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

    try:
        semantic_scholar.run_mode(
            cfg,
            "broad",
            "2026-06-30T00:00:00Z",
            tmp_path / "raw",
            tmp_path / "csv",
            client=FakeClient(),
        )
    except RuntimeError as exc:
        assert "HTTP 403" in str(exc)
    else:
        raise AssertionError("run_mode should fail instead of writing an empty CSV on HTTP errors")

    assert not (tmp_path / "csv" / "broad.csv").exists()
    error_payload = json.loads((tmp_path / "raw" / "broad-bulk-p01.json").read_text(encoding="utf-8"))
    assert error_payload["http_status"] == 403

def test_client_retries_unauthenticated_after_authenticated_403(monkeypatch):
    monkeypatch.setenv(semantic_scholar.SEMANTIC_SCHOLAR_API_KEY_ENV, "secret")
    calls = []

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, endpoint, params, timeout):
            calls.append((dict(self.headers), dict(params)))
            if len(calls) == 1:
                response = FakeResponse({}, status_code=403, text="forbidden")
            else:
                response = FakeResponse({"data": []}, status_code=200)
            response.url = endpoint
            return response

    monkeypatch.setattr(semantic_scholar.requests, "Session", FakeSession)

    client = semantic_scholar.SemanticScholarClient(max_retries=2)
    response = client.get("https://example.test/bulk", {"query": "test"})

    assert response.status_code == 200
    assert calls[0][0]["x-api-key"] == "secret"
    assert "x-api-key" not in calls[1][0]
    assert client.auth_fallback_used is True
