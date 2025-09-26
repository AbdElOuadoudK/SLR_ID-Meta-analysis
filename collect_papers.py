#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, hashlib, subprocess, sys
from datetime import datetime, timezone
BASE=os.path.abspath(os.path.dirname(__file__))

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''): h.update(chunk)
    return h.hexdigest()

def run(cmd):
    print(">>", " ".join(cmd))
    try:
        subprocess.check_call(cmd, cwd=BASE)
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Command failed with code {e.returncode}: {' '.join(cmd)}")

def collect_artifacts():
    artifacts = []
    # Data previously staged across /raw and /intermediate is now consolidated under /raw.
    for d in ["raw", "CSVs", "converted"]:
        abs_dir = os.path.join(BASE, d)
        if not os.path.isdir(abs_dir):
            # Skip directories that are not produced in the current run (prevents FileNotFoundError).
            continue
        for root, _, files in os.walk(abs_dir):
            for fn in files:
                artifacts.append(os.path.join(root, fn))
    # Sort to keep checksums.md deterministic regardless of filesystem traversal order.
    return sorted(artifacts)

def main():
    run([sys.executable,'collect_broad.py'])
    run([sys.executable,'collect_precise.py'])
    ledgers=[]
    for mode in ['broad','precise']:
        with open(os.path.join(BASE,'logs',f'ledger_{mode}.json'),'r') as f:
            ledgers.append(json.load(f))
    unified={
        'date_time_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'modes': ledgers,
        'notes': ['Unified package; /bulk endpoint; token-based paging; limit=1000; no dedup in this phase.']
    }
    with open(os.path.join(BASE,'logs','harvest_ledger.json'),'w') as f:
        json.dump(unified,f,indent=2)
    artifacts=collect_artifacts()
    with open(os.path.join(BASE,'checksums.md'),'w') as f:
        for p in artifacts:
            f.write(f"{sha256_file(p)}  {os.path.relpath(p,BASE)}\n")
    print('Unified run complete.')

if __name__=='__main__': main()
