from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys

from _xtract_n_xport.export import export_extracted
from _xtract_n_xport.io_utils import load_csv
from _xtract_n_xport.s2 import enrich_extract

from output_paths import (
    ensure_output_directories,
    fail_on_removed_output_argument,
    get_csv_dir,
    get_logs_dir,
)


logger = logging.getLogger(__name__)

def load_params(path: str|None) -> dict:
    candidates = [path] if path else []
    here = os.path.dirname(os.path.abspath(__file__))
    candidates += [os.path.join(here,'params.json'), os.path.join(os.getcwd(),'params.json'), os.path.join(os.path.dirname(here),'params.json')]
    for cand in candidates:
        if cand and os.path.exists(cand):
            with open(cand, "r", encoding="utf-8") as f:
                return json.load(f)
    raise FileNotFoundError("params.json not found. Provide --params or place it next to run_extract.py.")

def main(argv: list[str] | None = None) -> None:
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.setLevel(logging.INFO)

    argv = list(argv) if argv is not None else sys.argv[1:]
    fail_on_removed_output_argument(argv)
    ensure_output_directories()

    csv_dir = get_csv_dir()
    logs_dir = get_logs_dir()

    logger.info("Starting selection workflow")

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default=str(csv_dir),
        help="Directory containing precise.csv and broad.csv (defaults to /CSVs).",
    )
    ap.add_argument("--params", required=False)
    args = ap.parse_args(argv)

    params = load_params(args.params)
    logger.info("Loaded configuration parameters")

    provenance_path = logs_dir / "provenance.txt"
    # timezone-aware UTC timestamp (avoids deprecated utcnow)
    with open(provenance_path, "a", encoding="utf-8") as f:
        f.write(f"Run start: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    csv_df = load_csv(args.input)
    logger.info("Loaded CSV inputs from %s", args.input)
    enriched = enrich_extract(csv_df, params, logs_dir=logs_dir)
    logger.info("Completed enrichment of extracted data")
    export_extracted(enriched, csv_dir=csv_dir)
    logger.info("Exported enriched data to %s", csv_dir)

    # Build the single-sheet template and prefill it with the extracted data
    from sheet_builder import build_template_with_data
    build_template_with_data(params, csv_dir=csv_dir)
    logger.info("Updated single-sheet template with extracted data")

    with open(provenance_path, "a", encoding="utf-8") as f:
        f.write(f"Run end: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    print("Extraction complete. Data saved to:", csv_dir)
    print("Logs saved to:", logs_dir)

if __name__ == "__main__":
    main()
