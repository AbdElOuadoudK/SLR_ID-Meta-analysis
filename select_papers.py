from __future__ import annotations
import os, json, argparse, pandas as pd, datetime
from _xtract_n_xport.io_utils import load_csv
from _xtract_n_xport.s2 import enrich_extract
from _xtract_n_xport.export import export_extracted

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
    args = ap.parse_args()

    params = load_params(args.params)

    os.makedirs(args.output, exist_ok=True)
    # timezone-aware UTC timestamp (avoids deprecated utcnow)
    with open(os.path.join(args.output, "provenance.txt"), "a", encoding="utf-8") as f:
        f.write(f"Run start: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    csv_df = load_csv(args.input)
    enriched = enrich_extract(csv_df, params, args.output)
    export_extracted(enriched, args.output)

    # Build the single-sheet template and prefill it with the extracted data
    from sheet_builder import build_template_with_data
    build_template_with_data(args.output, params)

    with open(os.path.join(args.output, "provenance.txt"), "a", encoding="utf-8") as f:
        f.write(f"Run end: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    print("Extraction complete. Outputs in:", args.output)

if __name__ == "__main__":
    main()
