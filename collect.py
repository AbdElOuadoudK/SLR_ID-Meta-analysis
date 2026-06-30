#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run Semantic Scholar collection for broad and/or precise search modes.

Default usage runs both configured modes and writes per-mode ledgers plus an
aggregate harvest ledger. A single mode can be selected with, for example:

    python collect.py --mode broad
"""
import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from output_paths import resolve_csv_dir, resolve_log_dir, resolve_named_dir

BASE = Path(__file__).resolve().parent
CSV_COLUMNS = [
    'mode', 'paperId', 'title', 'publicationDate', 'year',
    'publicationTypes', 'fieldsOfStudy', 'influentialCitationCount',
]
DEFAULT_CONFIGS = {
    'broad': 'config_broad.json',
    'precise': 'config_precise.json',
}

logger = logging.getLogger(__name__)
SEMANTIC_SCHOLAR_API_KEY_ENV = 'SEMANTIC_SCHOLAR_API_KEY'


def configure_logging() -> None:
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logger.setLevel(logging.INFO)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value.strip())
    except ValueError:
        return None


def semantic_scholar_headers(headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    api_key = os.environ.get(SEMANTIC_SCHOLAR_API_KEY_ENV)
    if not api_key:
        return headers or {}
    request_headers = dict(headers or {})
    request_headers['x-api-key'] = api_key
    return request_headers


def fetch_with_retries(endpoint: str, params: Dict[str, Any], headers: Dict[str, str], timeout: int = 60):
    while True:
        try:
            response = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
        except requests.RequestException:
            time.sleep(1.5)
            continue

        if response.status_code == 429:
            time.sleep(parse_retry_after(response.headers.get('Retry-After')) or 2.0)
            continue
        if response.status_code in (500, 502, 503, 504):
            time.sleep(1.0)
            continue
        return response


def parse_year(publication_date: Optional[str]) -> Optional[int]:
    if not publication_date:
        return None
    match = re.match(r'(\d{4})', str(publication_date))
    return int(match.group(1)) if match else None


def join_if_list(value: Any) -> Any:
    return '; '.join(value or []) if isinstance(value, list) else value


def to_csv_rows(data: List[Dict[str, Any]], mode_tag: str) -> List[Dict[str, Any]]:
    return [
        {
            'mode': mode_tag.upper(),
            'paperId': record.get('paperId'),
            'title': record.get('title'),
            'publicationDate': record.get('publicationDate'),
            'year': parse_year(record.get('publicationDate')),
            'publicationTypes': join_if_list(record.get('publicationTypes')),
            'fieldsOfStudy': join_if_list(record.get('fieldsOfStudy')),
            'influentialCitationCount': record.get('influentialCitationCount'),
        }
        for record in data
    ]


def write_csv(out_path: Path, data: List[Dict[str, Any]], mode_tag: str) -> None:
    pd.DataFrame(to_csv_rows(data, mode_tag), columns=CSV_COLUMNS).to_csv(out_path, index=False)


def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def mode_config_path(mode: str, config_override: Optional[str]) -> Path:
    config_path = Path(config_override) if config_override else Path(DEFAULT_CONFIGS[mode])
    return config_path if config_path.is_absolute() else BASE / config_path


def run_mode(cfg: Dict[str, Any], mode_tag: str, run_time_iso: str, raw_dir: Path, csv_dir: Path) -> Dict[str, Any]:
    """Fetch all token-paginated /bulk pages for one mode and export JSON/CSV."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    base_params = {
        'query': cfg['query'],
        'year': cfg['year'],
        'fieldsOfStudy': cfg['fieldsOfStudy'],
        'fields': cfg['fields'],
        'limit': int(cfg.get('limit', 1000)),
    }
    if cfg.get('publicationTypes'):
        base_params['publicationTypes'] = cfg['publicationTypes']

    data_buffer: List[Dict[str, Any]] = []
    page_files: List[Path] = []
    notes: List[str] = []
    token = None
    page_idx = 0

    while True:
        page_idx += 1
        params = dict(base_params)
        if token:
            params['token'] = token

        response = fetch_with_retries(cfg['endpoint'], params, semantic_scholar_headers(cfg.get('headers')), timeout=60)
        page_path = raw_dir / f'{mode_tag}-bulk-p{page_idx:02d}.json'
        page_files.append(page_path)

        if response.status_code != 200:
            with open(page_path, 'w', encoding='utf-8') as handle:
                json.dump({'http_status': response.status_code, 'error': response.text}, handle, ensure_ascii=False, indent=2)
            notes.append(f'HTTP {response.status_code} during bulk fetch; saved error page and aborted.')
            break

        page_json = response.json()
        with open(page_path, 'w', encoding='utf-8') as handle:
            json.dump(page_json, handle, ensure_ascii=False, separators=(',', ':'))
        data_buffer.extend(page_json.get('data') or [])

        token = page_json.get('token')
        if not token:
            break

    merged_path = raw_dir / f'{mode_tag}-bulk-raw.json'
    with open(merged_path, 'w', encoding='utf-8') as handle:
        json.dump({'data': data_buffer}, handle, ensure_ascii=False, separators=(',', ':'))
    write_csv(csv_dir / f'{mode_tag}.csv', data_buffer, mode_tag)

    return {
        'mode': mode_tag.upper(),
        'date_time_utc': run_time_iso,
        'endpoint': '/graph/v1/paper/search/bulk',
        'query': cfg['query'],
        'params_json': {
            'year': cfg['year'],
            'fieldsOfStudy': cfg['fieldsOfStudy'],
            'fields': cfg['fields'],
            'limit': int(cfg.get('limit', 1000)),
            'publicationTypes': cfg.get('publicationTypes'),
        },
        'raw_export_files': [str(path) for path in page_files],
        'merged_file': str(merged_path),
        'export_formats': ['json', 'csv'],
        'notes': notes,
        'hits_reported': None,
        'hits_retrieved': len(data_buffer),
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run Semantic Scholar collection modes and aggregate ledgers.')
    parser.add_argument('-mode', '--mode', choices=['both', 'broad', 'precise'], default='both',
                        help='Collection mode to run (default: both broad and precise).')
    parser.add_argument('--config', default=None,
                        help='Override configuration JSON. Only valid when --mode is broad or precise.')
    parser.add_argument('--log-dir', default=None, help='Directory for ledger/log outputs (defaults to ./logs).')
    parser.add_argument('--csv-dir', default=None, help='Directory for CSV exports (defaults to ./CSVs).')
    parser.add_argument('--raw-dir', default=None, help='Directory for raw JSON pages (defaults to ./raw).')
    args = parser.parse_args(argv)
    if args.config and args.mode == 'both':
        parser.error('--config can only be used with --mode broad or --mode precise')
    return args


def main(argv: Optional[List[str]] = None) -> None:
    configure_logging()
    args = parse_args(argv)
    modes = ['broad', 'precise'] if args.mode == 'both' else [args.mode]
    raw_dir = resolve_named_dir(BASE, args.raw_dir, 'raw')
    csv_dir = resolve_csv_dir(BASE, args.csv_dir)
    logs_dir = resolve_log_dir(BASE, args.log_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    ledgers = []
    for mode in modes:
        cfg = load_config(mode_config_path(mode, args.config))
        mode_tag = (cfg.get('mode') or mode).lower()
        logger.info('Starting %s collection phase', mode_tag)
        ledger = run_mode(cfg, mode_tag, utc_now_iso(), raw_dir, csv_dir)
        with open(logs_dir / f'ledger_{mode_tag}.json', 'w', encoding='utf-8') as handle:
            json.dump(ledger, handle, indent=2)
        ledgers.append(ledger)
        logger.info('Finished %s collection phase', mode_tag)

    unified = {
        'date_time_utc': utc_now_iso(),
        'modes': ledgers,
        'notes': ['Unified package; /bulk endpoint; token-based paging; limit=1000; no dedup in this phase.'],
    }
    with open(logs_dir / 'harvest_ledger.json', 'w', encoding='utf-8') as handle:
        json.dump(unified, handle, indent=2)
    logger.info('Collection complete.')


if __name__ == '__main__':
    main()
