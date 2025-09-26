#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os, json, hashlib, subprocess, sys

from datetime import datetime, timezone
from pathlib import Path

from typing import Iterable, List

from output_paths import (
    resolve_csv_dir,
    resolve_log_dir,
    resolve_named_dir,
)

BASE=Path(__file__).resolve().parent

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''): h.update(chunk)
    return h.hexdigest()

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.setLevel(logging.INFO)



def run(cmd):
    logger.info("Executing command: %s", " ".join(cmd))
    try:
        subprocess.check_call(cmd, cwd=str(BASE))
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"Command failed with code {e.returncode}: {' '.join(cmd)}")

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run both collection modes and aggregate ledgers.")
    parser.add_argument("--log-dir", default=None, help="Directory for ledger/log outputs (defaults to ./logs).")
    parser.add_argument("--csv-dir", default=None, help="Directory for CSV exports (defaults to ./CSVs).")
    parser.add_argument("--raw-dir", default=None, help="Directory for raw JSON pages (defaults to ./raw).")
    parser.add_argument(
        "--converted-dir",
        default=None,
        help="Directory containing converted artifacts (defaults to ./converted when present).",
    )
    return parser.parse_args(argv)


def collect_artifacts(sources: Iterable[Path]) -> List[Path]:
    """Return a sorted list of artifact files from the provided *sources*.

    Each entry in *sources* may be a directory or a file. Missing paths are
    ignored. Directories are traversed recursively.
    """

    files: List[Path] = []
    for src in sources:
        if src is None:
            continue
        if src.is_file():
            files.append(src)
            continue
        if not src.is_dir():
            continue
        for path in sorted(p for p in src.rglob("*") if p.is_file()):
            files.append(path)
    return files


def main(argv=None):
    configure_logging()

    args = parse_args(argv)

    raw_dir = resolve_named_dir(BASE, args.raw_dir, 'raw')
    csv_dir = resolve_csv_dir(BASE, args.csv_dir)
    logs_dir = resolve_log_dir(BASE, args.log_dir)
    converted_dir = None
    if args.converted_dir:
        candidate = Path(args.converted_dir)
        if not candidate.is_absolute():
            candidate = BASE / candidate
    else:
        candidate = BASE / 'converted'
    if candidate.exists():
        converted_dir = candidate

    run([sys.executable,'collect_broad.py',
         '--log-dir', str(logs_dir),
         '--csv-dir', str(csv_dir),
         '--raw-dir', str(raw_dir)])
    logger.info("Finished broad collection phase")
    run([sys.executable,'collect_precise.py',
         '--log-dir', str(logs_dir),
         '--csv-dir', str(csv_dir),
         '--raw-dir', str(raw_dir)])
    logger.info("Finished precise collection phase")

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
    artifact_sources = [raw_dir, csv_dir, logs_dir]
    if converted_dir is not None:
        artifact_sources.append(converted_dir)
    artifacts=collect_artifacts(artifact_sources)
    checksums_path = BASE / 'checksums.md'
    with open(checksums_path,'w') as f:
        for p in artifacts:
            f.write(f"{sha256_file(p)}  {os.path.relpath(str(p),str(BASE))}\n")
    logger.info('Generated checksum manifest at %s', checksums_path)
    logger.info('Unified run complete.')


if __name__=='__main__': main()
