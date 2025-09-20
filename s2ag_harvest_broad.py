#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S2AG BROAD harvester (unified package)
- Endpoint: GET /graph/v1/paper/search/bulk (relevance-ranked; token paging)
- Fields: paperId,title,publicationDate,publicationTypes,fieldsOfStudy,influentialCitationCount
- Limit: 1000 per call; continue using the opaque `token` returned by each page until exhausted.
- No deduplication; preserve server relevance order within each page sequence.
- Save raw page JSONs, merge per mode, convert to CSV/RIS/BibTeX; per-mode ledger is written for aggregation.
"""
import os, sys, json, time, hashlib, re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import requests
import pandas as pd

def ensure_dir(p: str): os.makedirs(p, exist_ok=True)
def utc_now_iso() -> str: return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
def sha256_file(path: str) -> str:
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''): h.update(chunk)
    return h.hexdigest()

def parse_retry_after(v: Optional[str]) -> Optional[float]:
    if not v: return None
    try: return float(v.strip())
    except: return None

def fetch_with_retries(endpoint, params, headers, timeout=60):
    while True:
        try:
            resp = requests.get(endpoint, params=params, headers=headers, timeout=timeout)
        except Exception:
            time.sleep(1.5)
            continue
        status = resp.status_code
        if status == 429:
            ra = parse_retry_after(resp.headers.get('Retry-After'))
            time.sleep(ra if ra is not None else 2.0)
            continue
        if status == 500:
            time.sleep(1.0)
            continue
        if status in (502,503,504):
            time.sleep(1.0)
            continue
        return resp

def parse_year(pubdate: Optional[str]) -> Optional[int]:
    if not pubdate: return None
    m = re.match(r"(\d{4})", str(pubdate))
    return int(m.group(1)) if m else None

def to_csv_rows(data: List[Dict[str, Any]]):
    rows=[]
    for r in data:
        rows.append({
            'mode': "BROAD",
            'paperId': r.get('paperId'),
            'title': r.get('title'),
            'publicationDate': r.get('publicationDate'),
            'year': parse_year(r.get('publicationDate')),
            'publicationTypes': '; '.join(r.get('publicationTypes') or []) if isinstance(r.get('publicationTypes'), list) else r.get('publicationTypes'),
            'fieldsOfStudy': '; '.join(r.get('fieldsOfStudy') or []) if isinstance(r.get('fieldsOfStudy'), list) else r.get('fieldsOfStudy'),
            'influentialCitationCount': r.get('influentialCitationCount'),
        })
    return rows

def write_csv(out_path,data):
    import pandas as pd
    rows=to_csv_rows(data)
    df=pd.DataFrame(rows, columns=['mode', 'paperId','title','publicationDate','year','publicationTypes','fieldsOfStudy','influentialCitationCount'])
    df.to_csv(out_path, index=False)

def run_mode(base_dir, cfg, mode_tag, run_time_iso):
    """
    Fetch all pages for a mode via the /bulk endpoint.
    The first request includes the full search context (query, year,
    fieldsOfStudy, fields, limit and optionally publicationTypes). Each
    subsequent request repeats the same search parameters and adds the
    opaque `token` returned by the prior response. This token indicates
    continuation; pagination stops when no token is present. No
    deduplication is performed here.
    """
    raw_dir = os.path.join(base_dir, 'raw')
    interm_dir = os.path.join(base_dir, 'intermediate')
    conv_dir = os.path.join(base_dir, 'CSVs')
    logs_dir = os.path.join(base_dir, 'logs')
    for d in (raw_dir, interm_dir, conv_dir, logs_dir):
        os.makedirs(d, exist_ok=True)
    endpoint = cfg['endpoint']
    # Construct the base parameters for the first page: full query context
    base_params = {
        'query': cfg['query'],
        'year': cfg['year'],
        'fieldsOfStudy': cfg['fieldsOfStudy'],
        'fields': cfg['fields'],
        'limit': int(cfg.get('limit', 1000))
    }
    if cfg.get('publicationTypes'):
        base_params['publicationTypes'] = cfg['publicationTypes']
    headers = cfg.get('headers') or {}
    page_idx = 0
    token = None
    data_buffer: List[Dict[str, Any]] = []
    page_files: List[str] = []
    notes: List[str] = []
    # Fetch pages until the continuation `token` is absent
    while True:
        page_idx += 1
        if token is None:
            # First call: include the full search parameters
            this_params = dict(base_params)
        else:
            # Subsequent calls: repeat the search parameters and add the token
            this_params = dict(base_params)
            this_params['token'] = token
        resp = fetch_with_retries(endpoint, this_params, headers, timeout=60)
        status = resp.status_code
        page_name = f"{mode_tag}-bulk-p{page_idx:02d}.json"
        page_path = os.path.join(raw_dir, page_name)
        if status != 200:
            # Persist the error page for audit and abort this mode
            with open(page_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps({'http_status': status, 'error': resp.text}, ensure_ascii=False, indent=2))
            page_files.append(page_path)
            notes.append(f"HTTP {status} during bulk fetch; saved error page and aborted.")
            break
        page_json = resp.json()
        # Save raw page verbatim
        with open(page_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(page_json, ensure_ascii=False, separators=(',', ':')))
        page_files.append(page_path)
        # Append records
        data_buffer.extend(page_json.get('data') or [])
        # Retrieve the continuation token for the next page
        token = page_json.get('token')
        if not token:
            # No further pages
            break
    # Write merged raw file
    merged_name = f"{mode_tag}-bulk-raw.json"
    merged_path = os.path.join(interm_dir, merged_name)
    with open(merged_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'data': data_buffer}, ensure_ascii=False, separators=(',', ':')))
    # Write CSV, RIS, and BibTeX
    csv_path = os.path.join(conv_dir, f"{mode_tag}.csv")
    write_csv(csv_path, data_buffer)
    # Build and return a ledger entry summarising this mode
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
            # No explicit ordering for /bulk endpoint
            'publicationTypes': cfg.get('publicationTypes')
        },
        'raw_export_files': [os.path.relpath(p, base_dir) for p in page_files],
        'merged_file': os.path.relpath(merged_path, base_dir),
        'export_formats': ['json', 'csv'],
        'notes': notes,
        'hits_reported': None,
        'hits_retrieved': len(data_buffer)
    }

def main():
    base_dir=os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(base_dir,'config_broad.json'),'r',encoding='utf-8') as f:
        cfg=json.load(f)
    mode_tag=(cfg.get('mode') or 'BROAD').lower()
    for d in ['raw','intermediate','CSVs','logs']:
        os.makedirs(os.path.join(base_dir,d),exist_ok=True)
    run_time_iso=utc_now_iso()
    ledger=run_mode(base_dir,cfg,mode_tag, run_time_iso)
    with open(os.path.join(base_dir,'logs',f'ledger_{mode_tag}.json'),'w',encoding='utf-8') as f:
        json.dump(ledger,f,indent=2)
    print('BROAD harvest complete.')

if __name__=='__main__':
    main()
