#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path

from output_paths import resolve_csv_dir, resolve_log_dir, resolve_named_dir

BASE=Path(__file__).resolve().parent

def run(cmd):
    print(">>", " ".join(cmd))
    try:
        subprocess.check_call(cmd, cwd=str(BASE))
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Command failed with code {e.returncode}: {' '.join(cmd)}")

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run both collection modes and aggregate ledgers.")
    parser.add_argument("--log-dir", default=None, help="Directory for ledger/log outputs (defaults to ./logs).")
    parser.add_argument("--csv-dir", default=None, help="Directory for CSV exports (defaults to ./CSVs).")
    parser.add_argument("--raw-dir", default=None, help="Directory for raw JSON pages (defaults to ./raw).")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    raw_dir = resolve_named_dir(BASE, args.raw_dir, 'raw')
    csv_dir = resolve_csv_dir(BASE, args.csv_dir)
    logs_dir = resolve_log_dir(BASE, args.log_dir)
    run([sys.executable,'collect_broad.py',
         '--log-dir', str(logs_dir),
         '--csv-dir', str(csv_dir),
         '--raw-dir', str(raw_dir)])
    run([sys.executable,'collect_precise.py',
         '--log-dir', str(logs_dir),
         '--csv-dir', str(csv_dir),
         '--raw-dir', str(raw_dir)])
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
    print('Unified run complete.')

if __name__=='__main__': main()
