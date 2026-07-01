#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run Semantic Scholar collection for broad and/or precise search modes.

Default usage runs both configured modes and writes per-mode ledgers plus an
aggregate harvest ledger. A single mode can be selected with, for example:

    python collect.py --mode broad
"""
import argparse
import csv
import json
import logging
import os
import re
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from slr_meta.shared.paths import PROJECT_ROOT, resolve_csv_dir, resolve_log_dir, resolve_named_dir

BASE = PROJECT_ROOT
CSV_COLUMNS = [
    'mode', 'paperId', 'title', 'publicationDate', 'year',
    'publicationTypes', 'fieldsOfStudy', 'influentialCitationCount',
]
DEFAULT_CONFIG = 'request_config.json'

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
    api_key = (os.environ.get(SEMANTIC_SCHOLAR_API_KEY_ENV) or '').strip()
    if not api_key:
        return headers or {}
    request_headers = dict(headers or {})
    request_headers['x-api-key'] = api_key
    return request_headers


def _normalize_query_token(token: str) -> str:
    """Return a Semantic Scholar bulk-search boolean token."""
    upper = token.upper()
    if upper == 'OR':
        return '|'
    if upper == 'AND':
        return '+'
    return token


def normalize_bulk_query(query: str) -> str:
    """Normalize legacy boolean syntax for Semantic Scholar bulk search.

    Semantic Scholar documents symbolic operators for bulk query syntax, for
    example ``|`` for OR and ``+`` for required terms. Preserve quoted phrases
    while converting common word operators into those documented symbols.
    """
    lexer = shlex.shlex(query, posix=False, punctuation_chars='|+()')
    lexer.whitespace_split = True
    lexer.commenters = ''
    normalized = ' '.join(_normalize_query_token(token) for token in lexer)
    return normalized.replace('( ', '(').replace(' )', ')')


class SemanticScholarClient:
    """Small requests-based client that keeps auth, retries, and logging together."""

    def __init__(self, headers: Optional[Dict[str, str]] = None, timeout: int = 60, max_retries: int = 5, allow_unauthenticated_fallback: bool = True):
        self.timeout = timeout
        self.max_retries = max_retries
        self.allow_unauthenticated_fallback = allow_unauthenticated_fallback
        self.auth_fallback_used = False
        self.session = requests.Session()
        self.session.headers.update(semantic_scholar_headers(headers))
        self.authenticated = 'x-api-key' in self.session.headers

    def get(self, endpoint: str, params: Dict[str, Any]):
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info('Semantic Scholar request attempt %s/%s: %s params=%s authenticated=%s',
                            attempt, self.max_retries, endpoint, params, self.authenticated)
                response = self.session.get(endpoint, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise RuntimeError(f'Semantic Scholar request failed after {attempt} attempts: {exc}') from exc
                sleep_seconds = min(1.5 * attempt, 10.0)
                logger.warning('Semantic Scholar request exception on attempt %s/%s: %s; retrying in %.1fs',
                               attempt, self.max_retries, exc, sleep_seconds)
                time.sleep(sleep_seconds)
                continue

            if response.status_code in (401, 403) and self.authenticated and self.allow_unauthenticated_fallback:
                logger.warning(
                    'Semantic Scholar returned HTTP %s for an authenticated request. '
                    'Retrying without x-api-key because this endpoint is publicly accessible and '
                    'some keys may be denied for specific resources.',
                    response.status_code,
                )
                self.session.headers.pop('x-api-key', None)
                self.authenticated = False
                self.auth_fallback_used = True
                continue

            if response.status_code == 429 or response.status_code in (500, 502, 503, 504):
                if attempt == self.max_retries:
                    return response
                sleep_seconds = parse_retry_after(response.headers.get('Retry-After')) or min(2.0 * attempt, 30.0)
                logger.warning('Semantic Scholar returned HTTP %s on attempt %s/%s; retrying in %.1fs',
                               response.status_code, attempt, self.max_retries, sleep_seconds)
                time.sleep(sleep_seconds)
                continue

            return response

        raise RuntimeError('unreachable Semantic Scholar retry state')


def fetch_with_retries(endpoint: str, params: Dict[str, Any], headers: Dict[str, str], timeout: int = 60):
    return SemanticScholarClient(headers=headers, timeout=timeout).get(endpoint, params)


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
    with open(out_path, 'w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(to_csv_rows(data, mode_tag))


def load_config(config_path: Path) -> Dict[str, Any]:
    with open(config_path, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def config_path(config_override: Optional[str]) -> Path:
    config_path = Path(config_override) if config_override else Path(DEFAULT_CONFIG)
    return config_path if config_path.is_absolute() else BASE / config_path


def mode_config(unified_config: Dict[str, Any], mode: str) -> Dict[str, Any]:
    """Merge shared config with the query/mode values for one configured mode."""
    modes = unified_config.get('modes')
    if not isinstance(modes, dict):
        raise ValueError("Configuration must contain a 'modes' object.")
    if mode not in modes:
        available = ', '.join(sorted(modes)) or '<none>'
        raise ValueError(f"Mode '{mode}' is not configured. Available modes: {available}.")
    mode_values = modes[mode]
    if not isinstance(mode_values, dict):
        raise ValueError(f"Configuration for mode '{mode}' must be an object.")

    shared_config = {key: value for key, value in unified_config.items() if key != 'modes'}
    cfg = {**shared_config, **mode_values}
    if 'query' not in cfg:
        raise ValueError(f"Configuration for mode '{mode}' must define 'query'.")
    cfg.setdefault('mode', mode.upper())
    return cfg


def run_mode(cfg: Dict[str, Any], mode_tag: str, run_time_iso: str, raw_dir: Path, csv_dir: Path, client: Optional[SemanticScholarClient] = None) -> Dict[str, Any]:
    """Fetch all token-paginated /bulk pages for one mode and export JSON/CSV."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)

    base_params = {
        'query': normalize_bulk_query(cfg['query']),
        'year': cfg['year'],
        'fieldsOfStudy': cfg['fieldsOfStudy'],
        'fields': cfg['fields'],
        'limit': int(cfg.get('limit', 1000)),
    }
    if cfg.get('publicationTypes'):
        base_params['publicationTypes'] = cfg['publicationTypes']

    if base_params['query'] != cfg['query']:
        logger.info('Normalized %s query for Semantic Scholar bulk syntax: %s', mode_tag, base_params['query'])

    if client is None:
        client = SemanticScholarClient(
            headers=cfg.get('headers'),
            timeout=int(cfg.get('timeout', 60)),
            allow_unauthenticated_fallback=bool(cfg.get('allowUnauthenticatedFallback', True)),
        )
    data_buffer: List[Dict[str, Any]] = []
    page_files: List[Path] = []
    notes: List[str] = []
    total_reported = None
    status_codes: List[int] = []
    token = None
    page_idx = 0

    while True:
        page_idx += 1
        params = dict(base_params)
        if token:
            params['token'] = token

        response = client.get(cfg['endpoint'], params)
        page_path = raw_dir / f'{mode_tag}-bulk-p{page_idx:02d}.json'
        page_files.append(page_path)

        status_codes.append(response.status_code)
        if response.status_code != 200:
            error_payload = {'http_status': response.status_code, 'error': response.text, 'request_url': response.url}
            with open(page_path, 'w', encoding='utf-8') as handle:
                json.dump(error_payload, handle, ensure_ascii=False, indent=2)
            detail = response.text[:500] if response.text else '<empty response body>'
            raise RuntimeError(f"Semantic Scholar bulk fetch failed for {mode_tag}: HTTP {response.status_code}: {detail}. Details saved to {page_path}.")

        try:
            page_json = response.json()
        except ValueError as exc:
            with open(page_path, 'w', encoding='utf-8') as handle:
                json.dump({'http_status': response.status_code, 'error': response.text, 'request_url': response.url}, handle, ensure_ascii=False, indent=2)
            raise RuntimeError(f'Semantic Scholar returned non-JSON response for {mode_tag}; details saved to {page_path}.') from exc
        with open(page_path, 'w', encoding='utf-8') as handle:
            json.dump(page_json, handle, ensure_ascii=False, separators=(',', ':'))
        if total_reported is None:
            total_reported = page_json.get('total')
            if total_reported is not None:
                logger.info('Semantic Scholar reports approximately %s hits for %s.', total_reported, mode_tag)
        page_data = page_json.get('data') or []
        logger.info('Retrieved %s records for %s page %s (cumulative %s).',
                    len(page_data), mode_tag, page_idx, len(data_buffer) + len(page_data))
        data_buffer.extend(page_data)
        if page_idx == 1 and not page_data:
            notes.append('First Semantic Scholar page contained no records; check query syntax and filters.')
            logger.warning('Semantic Scholar returned no records for %s on the first page. Query sent: %s', mode_tag, base_params['query'])

        token = page_json.get('token')
        if not token:
            break

    merged_path = raw_dir / f'{mode_tag}-bulk-raw.json'
    with open(merged_path, 'w', encoding='utf-8') as handle:
        json.dump({'data': data_buffer}, handle, ensure_ascii=False, separators=(',', ':'))
    write_csv(csv_dir / f'{mode_tag}.csv', data_buffer, mode_tag)

    if getattr(client, 'auth_fallback_used', False):
        notes.append('Authenticated request received 401/403; retried without x-api-key and succeeded.')

    return {
        'mode': mode_tag.upper(),
        'date_time_utc': run_time_iso,
        'endpoint': '/graph/v1/paper/search/bulk',
        'query': normalize_bulk_query(cfg['query']),
        'params_json': {
            'original_query': cfg['query'],
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
        'hits_reported': total_reported,
        'hits_retrieved': len(data_buffer),
        'http_status_codes': status_codes,
    }


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run Semantic Scholar collection modes and aggregate ledgers.')
    parser.add_argument('-mode', '--mode', choices=['both', 'broad', 'precise'], default='both',
                        help='Collection mode to run (default: both broad and precise).')
    parser.add_argument('--config', default=None,
                        help='Path to unified configuration JSON (default: ./request_config.json).')
    parser.add_argument('--log-dir', default=None, help='Directory for ledger/log outputs (defaults to ./logs).')
    parser.add_argument('--csv-dir', default=None, help='Directory for CSV exports (defaults to ./CSVs).')
    parser.add_argument('--raw-dir', default=None, help='Directory for raw JSON pages (defaults to ./raw).')
    args = parser.parse_args(argv)
    return args


def main(argv: Optional[List[str]] = None) -> None:
    configure_logging()
    args = parse_args(argv)
    modes = ['broad', 'precise'] if args.mode == 'both' else [args.mode]
    raw_dir = resolve_named_dir(BASE, args.raw_dir, 'raw')
    csv_dir = resolve_csv_dir(BASE, args.csv_dir)
    logs_dir = resolve_log_dir(BASE, args.log_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    unified_config = load_config(config_path(args.config))
    if (os.environ.get(SEMANTIC_SCHOLAR_API_KEY_ENV) or '').strip():
        logger.info('Using Semantic Scholar API key from %s.', SEMANTIC_SCHOLAR_API_KEY_ENV)
    else:
        logger.warning('No %s environment variable found; requests will use unauthenticated rate limits.', SEMANTIC_SCHOLAR_API_KEY_ENV)

    ledgers = []
    for mode in modes:
        cfg = mode_config(unified_config, mode)
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
