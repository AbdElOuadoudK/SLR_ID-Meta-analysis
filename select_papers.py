from __future__ import annotations
import os, json, argparse, pandas as pd, datetime
from pathlib import Path
from _xtract_n_xport.io_utils import load_csv
from _xtract_n_xport.s2 import enrich_extract
from _xtract_n_xport.export import export_extracted

from output_paths import resolve_csv_dir, resolve_log_dir

def load_params(path: str|None) -> dict:
    candidates = [path] if path else []
    here = os.path.dirname(os.path.abspath(__file__))
    candidates += [os.path.join(here,'params.json'), os.path.join(os.getcwd(),'params.json'), os.path.join(os.path.dirname(here),'params.json')]
    for cand in candidates:
        if cand and os.path.exists(cand):
            with open(cand, "r", encoding="utf-8") as f:
                return json.load(f)
    raise FileNotFoundError("params.json not found. Provide --params or place it next to run_extract.py.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--params", required=False)
    ap.add_argument("--log-dir", default=None, help="Directory for log/provenance output (defaults to ./logs relative to output).")
    ap.add_argument("--csv-dir", default=None, help="Directory for CSV/XLSX outputs (defaults to ./CSVs relative to output).")
    args = ap.parse_args()

    params = load_params(args.params)

    output_root = Path(args.output)
    output_root.mkdir(parents=True, exist_ok=True)
    logs_dir = resolve_log_dir(output_root, args.log_dir)
    csv_dir = resolve_csv_dir(output_root, args.csv_dir)
    provenance_path = logs_dir / "provenance.txt"
    # timezone-aware UTC timestamp (avoids deprecated utcnow)
    with open(provenance_path, "a", encoding="utf-8") as f:
        f.write(f"Run start: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    csv_df = load_csv(args.input)
    enriched = enrich_extract(csv_df, params, str(output_root))
    export_extracted(enriched, str(output_root), csv_dir=str(csv_dir))

    # Build the single-sheet template and prefill it with the extracted data
    from sheet_builder import build_template_with_data
    build_template_with_data(str(output_root), params, csv_dir=str(csv_dir))

    with open(provenance_path, "a", encoding="utf-8") as f:
        f.write(f"Run end: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    print("Extraction complete. Outputs in:", output_root)

if __name__ == "__main__":
    main()
