#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import os, json, hashlib, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from output_paths import resolve_csv_dir, resolve_log_dir, resolve_named_dir

BASE=Path(__file__).resolve().parent

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''): h.update(chunk)
    return h.hexdigest()

def run(cmd):
    print(">>", " ".join(cmd))
    try:
        subprocess.check_call(cmd, cwd=str(BASE))
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Command failed with code {e.returncode}: {' '.join(cmd)}")

def collect_artifacts(directories: Iterable[Path]) -> List[Path]:
    artifacts: List[Path] = []
    for abs_dir in directories:
        if not abs_dir.is_dir():
            # Skip directories that are not produced in the current run (prevents FileNotFoundError).
            continue
        for root, _, files in os.walk(abs_dir):
            for fn in files:
                artifacts.append(Path(root) / fn)
    # Sort to keep checksums.md deterministic regardless of filesystem traversal order.
    return sorted(artifacts)

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run both collection modes and aggregate ledgers.")
    parser.add_argument("--log-dir", default=None, help="Directory for ledger/log outputs (defaults to ./logs).")
    parser.add_argument("--csv-dir", default=None, help="Directory for CSV exports (defaults to ./CSVs).")
    parser.add_argument("--raw-dir", default=None, help="Directory for raw JSON pages (defaults to ./raw).")
    parser.add_argument("--intermediate-dir", default=None, help="Directory for merged JSON (defaults to ./intermediate).")
    parser.add_argument("--converted-dir", default=None, help="Directory for converted outputs (defaults to ./converted).")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    raw_dir = resolve_named_dir(BASE, args.raw_dir, 'raw')
    interm_dir = resolve_named_dir(BASE, args.intermediate_dir, 'intermediate')
    csv_dir = resolve_csv_dir(BASE, args.csv_dir)
    logs_dir = resolve_log_dir(BASE, args.log_dir)
    converted_dir = resolve_named_dir(BASE, args.converted_dir, 'converted')

    run([sys.executable,'collect_broad.py',
         '--log-dir', str(logs_dir),
         '--csv-dir', str(csv_dir),
         '--raw-dir', str(raw_dir),
         '--intermediate-dir', str(interm_dir)])
    run([sys.executable,'collect_precise.py',
         '--log-dir', str(logs_dir),
         '--csv-dir', str(csv_dir),
         '--raw-dir', str(raw_dir),
         '--intermediate-dir', str(interm_dir)])
    ledgers=[]
    for mode in ['broad','precise']:
        with open(logs_dir / f'ledger_{mode}.json','r') as f:
            ledgers.append(json.load(f))
    unified={
        'date_time_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'modes': ledgers,
        'notes': ['Unified package; /bulk endpoint; token-based paging; limit=1000; no dedup in this phase.']
    }
    with open(logs_dir / 'harvest_ledger.json','w') as f:
        json.dump(unified,f,indent=2)
    artifacts=collect_artifacts([raw_dir, interm_dir, csv_dir, converted_dir])
    checksums_path = BASE / 'checksums.md'
    with open(checksums_path,'w') as f:
        for p in artifacts:
            f.write(f"{sha256_file(p)}  {os.path.relpath(str(p),str(BASE))}\n")
    print('Unified run complete.')

if __name__=='__main__': main()
