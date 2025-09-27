#!/usr/bin/env python3
"""
download_papers.py — improved PDF validation & failure tracking.

Key behavior:
- Attempts where content-type / magic-bytes don't look like a PDF are treated as failed attempts (and retried).
- Retry diagnostics at DEBUG (file-only). Suspicious MIME/small-size as WARNING (file-only).
- Final per-download visible INFO/ERROR emitted only by the worker `_download_task`.
- After run, failures_summary.csv and failures_ids.txt are written into the configured CSVs/ directory.
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from contextlib import suppress
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import FileHandler, Formatter, StreamHandler
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlsplit

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from output_paths import resolve_csv_dir, resolve_log_dir

# Constants
REQUIRED_COLUMNS = {"paperId", "open_access_pdf_url"}
XLSX_EXTS = {".xlsx", ".xlsm", ".xls"}
REQUEST_TIMEOUT = 2  # seconds for each request attempt
MAX_RETRIES = 4
RETRY_SLEEP = 2  # fixed wait between attempts (seconds)
CHUNK_SIZE = 1 << 16  # 64 KiB
DEFAULT_WORKERS = 8
DEFAULT_ERROR_LOG = "failures.log"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)
BROWSER_BASE_HEADERS = {
    "User-Agent": BROWSER_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;"
    "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/pdf;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}


class ConsoleFilter(logging.Filter):
    """
    Allow only INFO, ERROR, and CRITICAL records to pass to the console,
    unless the record has 'suppress_console' set to True.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "suppress_console", False):
            return False
        return record.levelno in (logging.INFO, logging.ERROR, logging.CRITICAL)


def setup_logging(error_log_path: Path) -> None:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    for h in list(root.handlers):
        root.removeHandler(h)

    sh = StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    sh.addFilter(ConsoleFilter())
    root.addHandler(sh)

    fh = FileHandler(str(error_log_path), encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    root.addHandler(fh)

    # Reduce verbosity of noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def _resolve_path(p: str | Path) -> Path:
    p = Path(p)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    return p


def read_spreadsheet(path: str | Path, sheet_name: Optional[str] = None) -> Optional[pd.DataFrame]:
    src_path = _resolve_path(path)
    logging.info("Reading spreadsheet from %s", src_path)

    if not src_path.exists():
        logging.error("Spreadsheet not found at: %s", src_path)
        return None

    ext = src_path.suffix.lower()
    try:
        if ext in XLSX_EXTS:
            try:
                import openpyxl  # noqa: F401
                engine = "openpyxl"
            except Exception:
                logging.error("Missing Excel engine 'openpyxl'. Install it with: pip install openpyxl")
                return None

            xl = pd.ExcelFile(src_path, engine=engine)
            candidate_sheets = [sheet_name] if sheet_name else xl.sheet_names
            picked_sheet = None
            last_exc = None
            for s in candidate_sheets:
                if s is None:
                    continue
                try:
                    df_try = xl.parse(s)
                    cols = {str(c).strip() for c in df_try.columns}
                    if REQUIRED_COLUMNS.issubset(cols):
                        picked_sheet = s
                        df = df_try
                        break
                except Exception as e:
                    last_exc = e
                    logging.debug("Sheet parse failed for '%s': %s", s, e)
                    continue

            if picked_sheet is None:
                for s in xl.sheet_names:
                    try:
                        df_try = xl.parse(s)
                        cols = {str(c).strip() for c in df_try.columns}
                        if REQUIRED_COLUMNS.issubset(cols):
                            picked_sheet = s
                            df = df_try
                            break
                    except Exception as e:
                        last_exc = e
                        logging.debug("Sheet parse failed for '%s' during fallback scan: %s", s, e)
                        continue

            if picked_sheet is None:
                if last_exc:
                    logging.error(
                        "Failed to find a worksheet with required columns %s. Last error: %s",
                        sorted(REQUIRED_COLUMNS),
                        last_exc,
                    )
                else:
                    logging.error("No worksheet contains required columns %s.", sorted(REQUIRED_COLUMNS))
                logging.info("Sheets available: %s", ", ".join(xl.sheet_names))
                return None

            if sheet_name and picked_sheet != sheet_name:
                logging.info(
                    "Requested sheet '%s' not suitable; using detected sheet '%s' with required columns.",
                    sheet_name,
                    picked_sheet,
                )
            else:
                logging.info("Using sheet '%s'.", picked_sheet)
        else:
            df = pd.read_csv(src_path)

        df.columns = [str(c).strip() for c in df.columns]
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            logging.error("Spreadsheet is missing required columns: %s", sorted(missing))
            return None

        before = len(df)
        df = df.dropna(subset=["paperId", "open_access_pdf_url"]).copy()
        after = len(df)
        if after == 0:
            logging.error("No valid rows after filtering; check 'paperId' and 'open_access_pdf_url'.")
            return None
        if after < before:
            logging.info("Filtered out %d row(s) with missing id/url.", before - after)

        df.loc[:, "paperId"] = df["paperId"].astype(str).str.strip()
        df.loc[:, "open_access_pdf_url"] = df["open_access_pdf_url"].astype(str).str.strip()

        return df

    except Exception as e:
        logging.error("Cannot continue without a valid spreadsheet. %s: %s", type(e).__name__, e)
        return None


def ensure_output_dir(path: str | Path) -> Path:
    out = _resolve_path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def _is_probably_pdf(response: requests.Response, first_chunk: bytes | None) -> bool:
    """Try to determine if content is a PDF by header or content-type."""
    ctype = response.headers.get("Content-Type", "") or ""
    if "pdf" in ctype.lower():
        return True
    if first_chunk:
        return first_chunk.startswith(b"%PDF-")
    return False


def _build_browser_headers(url: str) -> dict[str, str]:
    headers = dict(BROWSER_BASE_HEADERS)
    parsed = urlsplit(url)
    if parsed.scheme and parsed.netloc:
        origin = f"{parsed.scheme}://{parsed.netloc}"
        headers.setdefault("Referer", origin + "/")
        headers.setdefault("Origin", origin)
    return headers


def _create_chrome_driver(download_dir: Path) -> webdriver.Chrome:
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
    }

    def _build_options(headless_arg: str) -> ChromeOptions:
        opts = ChromeOptions()
        opts.add_argument(headless_arg)
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-popup-blocking")
        opts.add_argument("--window-size=1920,1080")
        opts.add_experimental_option("prefs", prefs)
        return opts

    last_exc: Optional[Exception] = None
    for headless_flag in ("--headless=new", "--headless"):
        try:
            options = _build_options(headless_flag)
            service = ChromeService(executable_path=ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            with suppress(Exception):
                driver.execute_cdp_cmd(
                    "Page.setDownloadBehavior",
                    {"behavior": "allow", "downloadPath": str(download_dir)},
                )
            return driver
        except WebDriverException as exc:  # pragma: no cover - depends on runtime drivers
            last_exc = exc
            continue
    if last_exc:
        raise last_exc
    raise WebDriverException("Unable to initialize headless Chrome driver")


def _selenium_fetch_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    with TemporaryDirectory() as tmpdir:
        download_dir = Path(tmpdir)
        driver = None
        try:
            driver = _create_chrome_driver(download_dir)
            driver.get(url)

            deadline = time.monotonic() + timeout
            pdf_path: Optional[Path] = None
            while time.monotonic() < deadline:
                pending = list(download_dir.glob("*.crdownload"))
                candidate_files: list[tuple[float, Path]] = []
                for candidate in download_dir.glob("*.pdf"):
                    try:
                        candidate_files.append((candidate.stat().st_mtime, candidate))
                    except OSError:
                        continue
                if candidate_files and not pending:
                    candidate_files.sort(key=lambda item: item[0], reverse=True)
                    candidate = candidate_files[0][1]
                    if candidate.exists():
                        pdf_path = candidate
                        break
                time.sleep(0.5)

            if pdf_path and pdf_path.exists():
                try:
                    return pdf_path.read_bytes()
                finally:
                    with suppress(Exception):
                        pdf_path.unlink()
            return None
        except WebDriverException:
            return None
        finally:
            if driver is not None:
                with suppress(Exception):
                    driver.quit()


def download_single(session: requests.Session, url: str, paper_id: str, idx: int) -> Optional[requests.Response]:
    """
    Download a URL with retries. Return Response if successful and looks like a PDF (by content-type/magic bytes),
    else None.

    If the initial streamed chunk or header doesn't look like a PDF, treat the attempt as failed (and retry).
    This prevents saving HTML error pages as PDFs.
    """
    identifier = paper_id if paper_id else f"<row-{idx}>"
    for attempt in range(1, MAX_RETRIES + 1):
        headers = _build_browser_headers(url)
        resp = None
        try:
            # Stream a small chunk to inspect content-type / magic bytes
            resp = session.get(url, stream=True, timeout=REQUEST_TIMEOUT, headers=headers)
            resp.raise_for_status()
            first_chunk = next(resp.iter_content(chunk_size=64), b"")

            # If it doesn't look like a PDF, treat this attempt as failed and retry.
            if not _is_probably_pdf(resp, first_chunk):
                # This diagnostic should be file-only (WARN) and the per-attempt detail at DEBUG.
                logging.warning(
                    "[%s] Attempt %d: content-type/header/initial-bytes do not look like a PDF: %s",
                    identifier,
                    attempt,
                    url,
                    extra={"suppress_console": True},
                )
                logging.debug(
                    "[%s] Attempt %d/%d treated as non-PDF (will retry). Headers: %s; first_bytes=%r",
                    identifier,
                    attempt,
                    MAX_RETRIES,
                    dict(resp.headers),
                    first_chunk[:64],
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)
                continue

            # If the first chunk looks like PDF, re-request full content (non-streaming) to get whole content reliably
            resp_full = session.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
            resp_full.raise_for_status()
            # Double-check the returned content by reading small prefix (defensive)
            prefix = resp_full.content[:5]
            if not prefix.startswith(b"%PDF-"):
                # If the full content doesn't start with PDF magic bytes, treat as failed attempt.
                logging.warning(
                    "[%s] Attempt %d: full content does not start with PDF magic bytes (prefix=%r): %s",
                    identifier,
                    attempt,
                    prefix,
                    url,
                    extra={"suppress_console": True},
                )
                logging.debug("[%s] Attempt %d/%d full content headers: %s", identifier, attempt, MAX_RETRIES, dict(resp_full.headers))
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_SLEEP)
                continue

            # passed checks — return the full response
            return resp_full

        except requests.RequestException as e:
            logging.debug("[%s] Attempt %d/%d failed (network/http): %s", identifier, attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
            continue
        except Exception as e:
            logging.debug("[%s] Unexpected error during attempt %d: %s", identifier, attempt, e, exc_info=True)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
            continue
        finally:
            if resp is not None:
                try:
                    resp.close()
                except Exception:
                    pass

    # Final failure: log to file (suppressed on console here), worker will emit visible ERROR
    logging.error("[%s] All %d attempts failed for %s", identifier, MAX_RETRIES, url, extra={"suppress_console": True})
    return None


def save_response_to_file(resp: requests.Response, path: Path, paper_id: str, idx: int) -> bool:
    """Save response content to path. After saving, validate magic bytes; delete partial if invalid."""
    identifier = paper_id if paper_id else f"<row-{idx}>"
    try:
        with path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
        # validate saved file head to be extra-safe
        try:
            with path.open("rb") as fh:
                head = fh.read(5)
            if not head.startswith(b"%PDF-"):
                # suspicious saved file — remove and signal failure; log as WARNING (file-only)
                logging.warning(
                    "[%s] Saved file does not begin with PDF magic bytes (head=%r). Removing partial file.",
                    identifier,
                    head,
                    extra={"suppress_console": True},
                )
                try:
                    path.unlink()
                except Exception:
                    logging.debug("[%s] Failed to remove invalid saved file %s", identifier, path)
                return False
        except Exception:
            logging.debug("[%s] Could not validate saved file %s after download.", identifier, path)
        return True
    except Exception as e:
        logging.error("[%s] Failed to save to %s: %s", identifier, path, e, extra={"suppress_console": True})
        try:
            if path.exists():
                path.unlink()
        except Exception:
            logging.debug("[%s] Failed to remove partial file %s after save error.", identifier, path)
        return False


def save_bytes_to_file(data: bytes, path: Path, paper_id: str, idx: int) -> bool:
    identifier = paper_id if paper_id else f"<row-{idx}>"
    if not data.startswith(b"%PDF-"):
        logging.warning(
            "[%s] Selenium fallback content does not begin with PDF magic bytes.",
            identifier,
            extra={"suppress_console": True},
        )
        return False

    try:
        with path.open("wb") as f:
            f.write(data)
        try:
            with path.open("rb") as fh:
                head = fh.read(5)
            if not head.startswith(b"%PDF-"):
                logging.warning(
                    "[%s] Saved Selenium fallback file missing PDF magic bytes; removing partial file.",
                    identifier,
                    extra={"suppress_console": True},
                )
                with suppress(Exception):
                    path.unlink()
                return False
        except Exception:
            logging.debug("[%s] Could not validate Selenium fallback file %s.", identifier, path)
        return True
    except Exception as e:
        logging.error(
            "[%s] Failed to save Selenium fallback data to %s: %s",
            identifier,
            path,
            e,
            extra={"suppress_console": True},
        )
        with suppress(Exception):
            if path.exists():
                path.unlink()
        return False


def _download_task(idx: int, paper_id: str, url: str, out_dir: Path) -> Tuple[str, str, Optional[str]]:
    """
    Worker task executed in a thread:
    Returns (paper_identifier, url, None) on success or (paper_identifier, url, reason) on failure.
    """
    identifier = paper_id if paper_id else f"<row-{idx}>"
    session = requests.Session()

    adapter = HTTPAdapter(pool_connections=8, pool_maxsize=16, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.clear()
    session.headers.update(BROWSER_BASE_HEADERS)
    try:
        out_path = out_dir / f"{paper_id}.pdf" if paper_id else out_dir / f"row-{idx}.pdf"
        if out_path.exists() and out_path.stat().st_size > 0:
            logging.info("[%s] Skipping (already exists) -> %s", identifier, out_path.name)
            return identifier, url, None

        logging.info("[%s] Starting download from %s", identifier, url)
        resp = download_single(session, url, paper_id, idx)
        if resp is not None:
            saved = save_response_to_file(resp, out_path, paper_id, idx)
            with suppress(Exception):
                resp.close()
        else:
            fallback_data = _selenium_fetch_pdf(url)
            if fallback_data is None:
                reason = f"download failed (all retries) for {url}"
                logging.error("[%s] %s", identifier, reason)
                return identifier, url, reason
            saved = save_bytes_to_file(fallback_data, out_path, paper_id, idx)

        if not saved:
            reason = f"save_failed_or_invalid_pdf for {out_path}"
            logging.error("[%s] %s", identifier, reason)
            return identifier, url, reason

        try:
            size = out_path.stat().st_size
            if size < 1024:
                # suspiciously small: WARNING (file-only)
                logging.warning(
                    "[%s] Downloaded file is small (%d bytes). Might not be a valid PDF.",
                    identifier,
                    size,
                    extra={"suppress_console": True},
                )
        except Exception:
            logging.debug("[%s] Could not stat the downloaded file %s to verify size.", identifier, out_path)

        logging.info("[%s] Success", identifier)
        return identifier, url, None
    except Exception as e:
        logging.exception("[%s] Unexpected error while processing: %s", identifier, e)
        return identifier, url, f"unexpected_error: {e}"


def download_papers(df: pd.DataFrame, output_dir: str | Path, workers: int = 1) -> List[Tuple[str, str, str]]:
    """
    Download papers from DataFrame into output_dir using up to `workers` concurrent threads.
    Returns list of failures as tuples (paperId, url, reason).
    """
    out_dir = ensure_output_dir(output_dir)
    failures: List[Tuple[str, str, str]] = []

    total = len(df)
    logging.info("Starting downloads: %d items -> %s (workers=%d)", total, out_dir, workers)

    # Prepare tasks
    tasks = []
    for idx, row in df.iterrows():
        raw_pid = row.get("paperId", "")
        pid = str(raw_pid).strip() if pd.notna(raw_pid) else ""
        raw_url = row.get("open_access_pdf_url", "")
        url = str(raw_url).strip() if pd.notna(raw_url) else ""
        identifier = pid if pid else f"<row-{idx}>"

        if not pid:
            logging.error("[%s] empty paperId (row filtered)", identifier)
            failures.append((identifier, url, "empty paperId"))
            continue
        if not url or url.lower() in {"nan", "none", ""}:
            logging.error("[%s] missing/invalid URL (row filtered)", identifier)
            failures.append((identifier, url, "missing/invalid URL"))
            continue

        tasks.append((idx, pid, url))

    if not tasks:
        logging.info("No valid tasks to download.")
        return failures

    with ThreadPoolExecutor(max_workers=workers) as exe:
        future_to_task = {exe.submit(_download_task, idx, pid, url, out_dir): (idx, pid, url) for idx, pid, url in tasks}
        for future in as_completed(future_to_task):
            idx, pid, url = future_to_task[future]
            identifier = pid if pid else f"<row-{idx}>"
            try:
                paper_identifier, paper_url, reason = future.result()
                if reason:
                    # worker already emitted console-visible ERROR; just collect details
                    failures.append((paper_identifier, paper_url, reason))
            except Exception as e:
                logging.exception("[%s] Task raised an exception: %s", identifier, e)
                failures.append((identifier, url, f"exception: {e}"))

    return failures


def write_failures_files(out_dir: Path, failures: List[Tuple[str, str, str]]) -> None:
    """
    Write failures_summary.csv and failures_ids.txt into out_dir.
    Overwrites any existing files.
    """
    out_dir = ensure_output_dir(out_dir)
    summary_path = out_dir / "failures_summary.csv"
    ids_path = out_dir / "failures_ids.txt"

    try:
        with summary_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["paperId", "url", "reason"])
            for pid, url, reason in failures:
                writer.writerow([pid, url, reason])
        with ids_path.open("w", encoding="utf-8") as fh:
            for pid, _, _ in failures:
                fh.write(f"{pid}\n")
        logging.info("Wrote %d failure records to %s and %s", len(failures), summary_path, ids_path)
    except Exception as e:
        logging.error("Failed to write failures files to %s: %s", out_dir, e)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spreadsheet", help="Path to the Excel/CSV file listing papers to download.")
    parser.add_argument(
        "--sheet",
        default=None,
        help="Worksheet name (Excel only). If not provided, auto-detects a sheet containing required headers.",
    )
    parser.add_argument(
        "--output",
        dest="output_dir",
        default="papers",
        help="Directory where PDFs will be saved. Default: ./papers",
    )
    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of concurrent worker threads to use for downloads (default: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Directory where the log file will be written (defaults to ./logs).",
    )
    parser.add_argument(
        "--log-file-name",
        default=DEFAULT_ERROR_LOG,
        help=f"File name for the log output (default: {DEFAULT_ERROR_LOG}).",
    )
    parser.add_argument(
        "--csv-dir",
        default=None,
        help="Directory where CSV/XLSX outputs (e.g., failure summaries) are saved (defaults to ./CSVs).",
    )
    args = parser.parse_args(argv)

    base_dir = Path(__file__).resolve().parent
    log_dir = resolve_log_dir(base_dir, args.log_dir)
    csv_dir = resolve_csv_dir(base_dir, args.csv_dir)
    log_path = log_dir / args.log_file_name

    # Configure logging to target the resolved log file in the dedicated logs/ directory.
    setup_logging(log_path)
    df = read_spreadsheet(args.spreadsheet, sheet_name=args.sheet)
    if df is None:
        return

    workers = max(1, int(args.workers))
    failures = download_papers(df, args.output_dir, workers=workers)

    # Write failures files into output directory for easy tracking/debugging
    try:
        write_failures_files(csv_dir, failures)
    except Exception:
        logging.debug("Could not write failure summary files.", exc_info=True)

    # Concise console summary
    if failures:
        logging.info(
            "Completed with %d failures (see %s and %s for details).",
            len(failures),
            csv_dir / "failures_summary.csv",
            log_path,
        )
    else:
        logging.info("All downloads completed successfully.")


if __name__ == "__main__":
    main()
