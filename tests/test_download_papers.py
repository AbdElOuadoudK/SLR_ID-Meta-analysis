from pathlib import Path
from unittest import mock
import sys

import pandas as pd
import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import download_papers as dp


class FakeResponse:
    def __init__(self, status_code=200, headers=None, content=b"", stream_chunks=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._content = content
        self._stream_chunks = list(stream_chunks or [content])
        self.url = "http://example.com/document.pdf"

    @property
    def content(self):
        return self._content

    def iter_content(self, chunk_size=1):
        for chunk in self._stream_chunks:
            yield chunk

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error for {self.url}")

    def close(self):
        pass


class FakeSession:
    def __init__(self, queue):
        self._queue = list(queue)
        self.calls = []

    def get(self, url, stream=False, timeout=None, headers=None):
        self.calls.append({"url": url, "stream": stream, "timeout": timeout, "headers": headers})
        if not self._queue:
            raise AssertionError("No response queued for get call")
        expected_stream, response = self._queue.pop(0)
        assert expected_stream == stream, "Stream flag mismatch"
        return response

    def close(self):
        pass


class AlwaysFailSession:
    def __init__(self):
        self.call_count = 0

    def get(self, url, stream=False, timeout=None, headers=None):
        self.call_count += 1
        return FakeResponse(status_code=503, headers={"Content-Type": "text/html"})

    def close(self):
        pass


@pytest.mark.parametrize("status_code", [503, 429])
def test_download_single_recovers_after_retry(status_code):
    pdf_head = b"%PDF-1.5\n"
    responses = [
        (True, FakeResponse(status_code=status_code, headers={"Content-Type": "text/html"})),
        (True, FakeResponse(headers={"Content-Type": "application/pdf"}, stream_chunks=[pdf_head])),
        (False, FakeResponse(headers={"Content-Type": "application/pdf"}, content=pdf_head + b"data")),
    ]
    session = FakeSession(responses)
    with mock.patch.object(dp, "time", wraps=dp.time) as time_mod:
        time_mod.sleep = mock.Mock()
        result = dp.download_single(session, "http://example.com/doc.pdf", "paper-1", 1)
    assert result is not None
    assert result.content.startswith(pdf_head)
    assert len(session.calls) == 3


def test_download_task_httpx_fallback(tmp_path, monkeypatch):
    failing_session = AlwaysFailSession()
    pdf_bytes = b"%PDF-1.7 test pdf"

    monkeypatch.setattr(dp, "_create_requests_session", lambda: failing_session)
    monkeypatch.setattr(dp, "_httpx_fetch_pdf", lambda url: pdf_bytes)

    def _raise_selenium(_url):
        raise AssertionError("selenium should not run")

    monkeypatch.setattr(dp, "_selenium_fetch_pdf", _raise_selenium)

    with mock.patch.object(dp, "time", wraps=dp.time) as time_mod:
        time_mod.sleep = mock.Mock()
        identifier, url, reason = dp._download_task(0, "paper-1", "http://example.com/doc.pdf", tmp_path)

    assert reason is None
    assert identifier == "paper-1"
    out_path = tmp_path / "paper-1.pdf"
    assert out_path.exists()
    assert out_path.read_bytes() == pdf_bytes
    assert failing_session.call_count == dp.MAX_RETRIES


def test_download_papers_records_failure_when_all_strategies_fail(tmp_path, monkeypatch):
    failing_session = AlwaysFailSession()

    monkeypatch.setattr(dp, "_create_requests_session", lambda: failing_session)
    monkeypatch.setattr(dp, "_httpx_fetch_pdf", lambda url: None)
    monkeypatch.setattr(dp, "_selenium_fetch_pdf", lambda url: None)

    url = "http://example.com/doc.pdf"
    data = pd.DataFrame([
        {"paperId": "paper-1", "open_access_pdf_url": url}
    ])

    with mock.patch.object(dp, "time", wraps=dp.time) as time_mod:
        time_mod.sleep = mock.Mock()
        failures = dp.download_papers(data, tmp_path, workers=1)

    expected_reason = f"download failed (all retries) for {url}"
    assert failures == [("paper-1", url, expected_reason)]
    out_path = Path(tmp_path) / "paper-1.pdf"
    assert not out_path.exists()
    assert failing_session.call_count == dp.MAX_RETRIES
