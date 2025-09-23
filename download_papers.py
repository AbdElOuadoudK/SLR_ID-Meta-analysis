"""Utility script for downloading PDF documents listed in an Excel spreadsheet.

The spreadsheet is expected to contain at least two columns:
    - ``paperId``: unique identifier used as the output filename.
    - ``url``: direct link to the PDF document.

The script downloads each document and stores it in ``/papers`` using the
filename ``{paperId}.pdf``. Failed downloads are retried with exponential
backoff and reported via logging.
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import requests
from requests import Response

# Absolute directory where downloaded PDFs will be stored.
DOWNLOAD_DIR = Path("/papers")

# Number of download attempts and the base for exponential backoff between them.
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0
REQUEST_TIMEOUT = 20  # seconds


def setup_logging(log_file: Optional[Path] = None) -> None:
    """Configure application-wide logging.

    Parameters
    ----------
    log_file:
        Optional path to a file where logs should also be written. If omitted,
        logs are only sent to the console.
    """
    handlers = [logging.StreamHandler()]
    if log_file is not None:
        handlers.append(logging.FileHandler(log_file, mode="a", encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


def read_spreadsheet(path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Load the spreadsheet into a DataFrame.

    Parameters
    ----------
    path:
        Path to the Excel file.
    sheet_name:
        Name of the sheet that should be read. If ``None`` the first sheet is
        used.
    """
    logging.info("Reading spreadsheet from %s", path)
    try:
        df = pd.read_excel(path, sheet_name=sheet_name)
    except FileNotFoundError:
        logging.error("Spreadsheet %s not found.", path)
        raise
    except Exception:
        logging.exception("Failed to read spreadsheet %s", path)
        raise

    missing_columns = {"paperId", "url"} - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Spreadsheet {path} is missing required columns: {sorted(missing_columns)}"
        )

    return df


def ensure_download_dir(directory: Path) -> None:
    """Ensure that the output directory exists."""
    if not directory.exists():
        logging.info("Creating download directory at %s", directory)
        directory.mkdir(parents=True, exist_ok=True)


def save_pdf(content: bytes, destination: Path) -> None:
    """Persist the PDF content to disk."""
    destination.write_bytes(content)
    logging.info("Saved PDF to %s", destination)


def download_with_retries(session: requests.Session, url: str) -> Optional[Response]:
    """Attempt to download a resource with retry support.

    Parameters
    ----------
    session:
        ``requests.Session`` used to perform the HTTP requests.
    url:
        URL of the resource to download.

    Returns
    -------
    Optional[requests.Response]
        The successful ``Response`` object, or ``None`` if all attempts failed.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            if "application/pdf" not in response.headers.get("Content-Type", ""):  # type: ignore[arg-type]
                logging.warning(
                    "URL %s did not return a PDF (Content-Type: %s)",
                    url,
                    response.headers.get("Content-Type"),
                )
            return response
        except (requests.RequestException, requests.Timeout) as exc:
            logging.warning(
                "Attempt %s/%s failed for %s: %s", attempt, MAX_RETRIES, url, exc
            )
            if attempt == MAX_RETRIES:
                break
            sleep_time = BACKOFF_FACTOR ** (attempt - 1)
            logging.info("Retrying in %.1f seconds...", sleep_time)
            time.sleep(sleep_time)

    return None


def iter_rows(df: pd.DataFrame) -> Iterable[tuple[str, str]]:
    """Yield ``(paper_id, url)`` tuples from the DataFrame, skipping invalid rows."""
    for idx, row in df.iterrows():
        paper_id = str(row.get("paperId", "")).strip()
        url = str(row.get("url", "")).strip()

        if not paper_id or paper_id.lower() == "nan":
            logging.warning("Row %s skipped: missing paperId", idx)
            continue
        if not url or url.lower() == "nan":
            logging.warning("Row %s skipped: missing url", idx)
            continue

        yield paper_id, url


def download_papers(df: pd.DataFrame, output_dir: Path) -> List[str]:
    """Download all papers listed in the DataFrame.

    Returns a list of descriptive error messages for downloads that ultimately
    failed.
    """
    ensure_download_dir(output_dir)
    failures: List[str] = []

    with requests.Session() as session:
        for paper_id, url in iter_rows(df):
            destination = output_dir / f"{paper_id}.pdf"
            logging.info("Downloading %s -> %s", url, destination)
            response = download_with_retries(session, url)
            if response is None:
                message = f"{paper_id}: Failed to download from {url}"
                logging.error(message)
                failures.append(message)
                continue

            try:
                save_pdf(response.content, destination)
            except Exception:
                logging.exception("Failed to save PDF for %s", paper_id)
                failures.append(f"{paper_id}: Failed to save PDF to {destination}")

    return failures


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments for the script."""
    parser = argparse.ArgumentParser(description="Download PDFs listed in a spreadsheet.")
    parser.add_argument(
        "spreadsheet",
        type=Path,
        help="Path to the Excel spreadsheet (.xlsx) containing paperId and url columns.",
    )
    parser.add_argument(
        "--sheet",
        dest="sheet_name",
        default=None,
        help="Optional sheet name to read from the workbook (defaults to the first sheet).",
    )
    parser.add_argument(
        "--log-file",
        dest="log_file",
        type=Path,
        default=None,
        help="Optional path to a log file to record progress and errors.",
    )
    parser.add_argument(
        "--output",
        dest="output_dir",
        type=Path,
        default=DOWNLOAD_DIR,
        help="Directory where PDFs will be saved (defaults to /papers).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    setup_logging(args.log_file)

    try:
        dataframe = read_spreadsheet(args.spreadsheet, sheet_name=args.sheet_name)
    except Exception:
        logging.error("Cannot continue without a valid spreadsheet.")
        return

    failures = download_papers(dataframe, args.output_dir)

    if failures:
        logging.info("Completed with %s failures:", len(failures))
        for failure in failures:
            logging.info("  - %s", failure)
    else:
        logging.info("All downloads completed successfully.")


if __name__ == "__main__":
    main()
