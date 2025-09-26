from __future__ import annotations

import datetime
import json
import re
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from numpy import mean

from output_paths import get_logs_dir

from .utils import deterministic_json, normalize_doi


class S2Client:
    def __init__(self, params: dict, logs_dir: Optional[Path] = None):
        self.base = params["s2"]["base_url"].rstrip("/")
        self.params = params
        self.session = requests.Session()
        self.logs_dir = Path(logs_dir) if logs_dir else get_logs_dir()
        self.provenance_root = self.logs_dir / "provenance"
        self.provenance_root.mkdir(parents=True, exist_ok=True)
        self.outdir = self.provenance_root / "raw_s2"
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.sleep_s = params["s2"]["retry_sleep_seconds"]
        self.timeout = params["s2"]["timeout_seconds"]
        self.max_retries = params["s2"]["max_retries"]
        self._log_path = self.provenance_root / "s2_client.log"

    def _log(self, msg: str):
        # timezone-aware UTC timestamp for log lines
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._log_path.open("a", encoding="utf-8") as f:
            f.write(f"{ts} | {msg}\n")

    def _post_json(self, url: str, body: dict):
        attempt = 0
        while True:
            attempt += 1
            try:
                r = self.session.post(url, json=body, timeout=self.timeout)
                s = r.status_code
                if s == 200:
                    return r.json()
                if s in (429, 500):
                    self._log(f"POST {url} -> {s} (attempt {attempt}); infinite retry after {self.sleep_s}s")
                    time.sleep(self.sleep_s); continue
                if s in (502, 503):
                    self._log(f"POST {url} -> {s} (attempt {attempt}); retry up to max_retries={self.max_retries}")
                    if attempt >= max(1, self.max_retries):
                        self._log(f"POST {url} exceeded max_retries for {s}"); return None
                    time.sleep(self.sleep_s); continue
                self._log(f"POST {url} non-retriable {s}: {r.text[:1000]}"); return None
            except requests.RequestException as e:
                self._log(f"POST {url} exception on attempt {attempt}: {repr(e)}")
                if attempt >= max(1, self.max_retries):
                    self._log("POST exceeded max_retries on exception"); return None
                time.sleep(self.sleep_s)

    def paper_batch(self, ids: List[str]):
        url = f"{self.base}/graph/v1/paper/batch?fields={self.params['s2']['paper_batch_fields']}"
        data = self._post_json(url, {"ids": ids})
        if data is None: return None
        with (self.outdir / "papers_batch.jsonl").open("a", encoding="utf-8") as f:
            for rec in data: f.write(json.dumps(rec, ensure_ascii=False)+"\n")
        return data

    def author_batch(self, ids: List[str]):
        if not ids: return []
        url = f"{self.base}/graph/v1/author/batch?fields={self.params['s2']['author_batch_fields']}"
        data = self._post_json(url, {"ids": ids})
        if data is None: return None
        with (self.outdir / "authors_batch.jsonl").open("a", encoding="utf-8") as f:
            for rec in data: f.write(json.dumps(rec, ensure_ascii=False)+"\n")
        return data

    def references_years(self, pid: str):
        url = f"{self.base}/graph/v1/paper/{pid}/references?fields={self.params['s2']['references_fields']}"
        attempt = 0
        while True:
            attempt += 1
            try:
                r = self.session.get(url, timeout=self.timeout)
                s = r.status_code
                if s == 200:
                    js = r.json()
                    with (self.outdir / f"references_{pid}.json").open("w", encoding="utf-8") as f:
                        json.dump(js, f, ensure_ascii=False)
                    data = js.get("data") or js.get("references") or []
                    years = []
                    for it in data:
                        y = None
                        if isinstance(it, dict):
                            if "citedPaper" in it and isinstance(it["citedPaper"], dict):
                                y = it["citedPaper"].get("year")
                            if y is None: y = it.get("year")
                        if isinstance(y, int): years.append(y)
                    return years
                if s in (429, 500):
                    self._log(f"GET {url} -> {s} (attempt {attempt}); infinite retry after {self.sleep_s}s")
                    time.sleep(self.sleep_s); continue
                if s in (502, 503):
                    self._log(f"GET {url} -> {s} (attempt {attempt}); retry up to max_retries={self.max_retries}")
                    if attempt >= max(1, self.max_retries):
                        self._log(f"GET {url} exceeded max_retries for {s}"); return None
                    time.sleep(self.sleep_s); continue
                self._log(f"GET {url} non-retriable {s}: {r.text[:1000]}"); return None
            except requests.RequestException as e:
                self._log(f"GET {url} exception on attempt {attempt}: {repr(e)}")
                if attempt >= max(1, self.max_retries):
                    self._log("GET exceeded max_retries on exception"); return None
                time.sleep(self.sleep_s)

def enrich_extract(
    df: pd.DataFrame, params: dict, logs_dir: Optional[Path | str] = None
) -> pd.DataFrame:
    """
    Enrich the dataframe using S2: paper batch, author batch, references -> max cited year.
    Writes raw responses to provenance/raw_s2/*.jsonl and per-paper references files.
    Returns a copy of df with new raw fields filled (no computations).
    """
    logs_base = Path(logs_dir) if logs_dir else get_logs_dir()
    (logs_base / "provenance").mkdir(parents=True, exist_ok=True)
    client = S2Client(params, logs_base)

    # gather paper ids from input CSV (paperId column required for enrichment; otherwise no enrichment)
    ids = [str(x) for x in df.get("paperId", pd.Series([], dtype=str)).astype(str).tolist() if x]
    bs = params["s2"]["batch_size"]
    papers = []

    if ids:
        for i in range(0, len(ids), bs):
            ch = ids[i:i+bs]
            data = client.paper_batch(ch)
            if data is None:
                df["s2_enriched"] = False
                return df
            papers.extend(data)

    by_id = {str(p.get("paperId") or p.get("paper", {}).get("paperId") or ""): p for p in papers}

    # collect author ids and fetch author batch info
    author_ids = []
    for p in papers:
        if not isinstance(p, dict):
            continue
        for a in (p.get("authors") or []):
            if not isinstance(a, dict):
                continue
            aid = a.get("authorId")
            if not aid:
                raw_author = a.get("author")
                if isinstance(raw_author, dict):
                    aid = raw_author.get("authorId")
            if aid is not None:
                aid_str = str(aid).strip()
                if aid_str:
                    author_ids.append(aid_str)
    author_ids = sorted(set(author_ids))

    author_info = {}
    for i in range(0, len(author_ids), bs):
        ch = author_ids[i:i+bs]
        adata = client.author_batch(ch)
        if adata is None:
            df["s2_enriched"] = False
            return df
        for rec in (adata or []):
            if not isinstance(rec, dict):
                continue
            aid = rec.get("authorId")
            if not aid:
                raw_author = rec.get("author")
                if isinstance(raw_author, dict):
                    aid = raw_author.get("authorId")
            if aid is None:
                continue
            aid_str = str(aid).strip()
            if not aid_str:
                continue
            author_info[aid_str] = rec

    # --- compute per-paper maximum cited year, but only from references published
    #     on or before the citing paper's publication year (to avoid future-year artifacts)
    max_cited_year = {}
    for pid in ids:
        yrs = client.references_years(pid)
        if not yrs:
            continue

        # normalize yrs to ints when possible
        yrs_int = [y for y in yrs if isinstance(y, int)]
        if not yrs_int:
            # nothing usable; skip
            continue

        # attempt to read the citing paper's year from the input dataframe
        citing_year = None
        try:
            # df may have string years; find first row with matching paperId
            matches = df.loc[df.get("paperId", pd.Series([], dtype=str)).astype(str) == str(pid)]
            if not matches.empty:
                yv = matches.iloc[0].get("year")
                if yv is not None and str(yv).strip() != "":
                    try:
                        # allow floats or strings like "2013.0"
                        citing_year = int(float(str(yv).strip()))
                    except Exception:
                        citing_year = None
        except Exception:
            citing_year = None

        if citing_year is not None:
            # only consider reference years that are <= citing_year
            yrs_before = [y for y in yrs_int if y <= citing_year]
            if yrs_before:
                max_cited_year[pid] = max(yrs_before)
            else:
                # no references with year <= citing_year: set to citing_year (so delay = 0)
                max_cited_year[pid] = citing_year
        else:
            # fallback: use the maximum available reference year (original behavior)
            max_cited_year[pid] = max(yrs_int)


    out = df.copy()

    # ensure explicit columns exist with sensible empty defaults
    for c in [
        "abstract", "external_ids", "doi", "is_open_access", "open_access_pdf_url", "journal_pages_range",
        "pages_total", "references_pages", "references_count", "_max_cited_year", "authors_hindex_list",
        "mean_author_hindex", "citation_count"
    ]:
        if c not in out.columns:
            out[c] = ""

    # populate fields per-paper from S2 responses (no metric computations here)
    for i, row in out.iterrows():
        pid = str(row.get("paperId", ""))
        p = by_id.get(pid)
        if not p:
            continue

        if p.get("abstract"):
            out.at[i, "abstract"] = p["abstract"]

        ext = p.get("externalIds")
        if isinstance(ext, dict) and len(ext) > 0:
            out.at[i, "external_ids"] = deterministic_json(ext)
            # DOI preference: CSV doi already, otherwise S2.externalIds.DOI
            if not str(out.at[i, "doi"]).strip():
                doi_val = ext.get("DOI") or ext.get("doi")
                if isinstance(doi_val, str):
                    norm = normalize_doi(doi_val)
                    if norm:
                        out.at[i, "doi"] = norm

        ioa = p.get("isOpenAccess")
        if isinstance(ioa, bool):
            out.at[i, "is_open_access"] = "TRUE" if ioa else "FALSE"

        oap = p.get("openAccessPdf") or {}
        if isinstance(oap, dict) and oap.get("url"):
            url = oap.get("url")
            out.at[i, "open_access_pdf_url"] = url
            if not str(out.at[i, "doi"]).strip():
                m = re.search(r'https?://(?:dx\.)?doi\.org/([^\s]+)', str(url), flags=re.I)
                if m:
                    norm = normalize_doi(m.group(1))
                    if norm:
                        out.at[i, "doi"] = norm

        journal = p.get("journal") or {}
        if isinstance(journal, dict):
            pages_s = journal.get("pages")
            if isinstance(pages_s, str):
                out.at[i, "journal_pages_range"] = pages_s
                m = re.match(r"\s*(\d+)\s*-\s*(\d+)\s*$", pages_s)
                if m:
                    a, b = int(m.group(1)), int(m.group(2))
                    if b >= a:
                        out.at[i, "pages_total"] = int(b - a + 1)

        rc = p.get("referenceCount")
        if isinstance(rc, int):
            out.at[i, "references_count"] = rc

        cc = p.get("citationCount")
        if isinstance(cc, int):
            out.at[i, "citation_count"] = cc

        # build authors' hIndex list (from author batch)
        hvals = []
        for a in (p.get("authors") or []):
            aid = str(a.get("authorId") or a.get("author", {}).get("authorId") or "")
            if aid and aid in author_info:
                hv = author_info[aid].get("hIndex")
                if isinstance(hv, int):
                    hvals.append(hv)
        if hvals:
            out.at[i, "authors_hindex_list"] = "; ".join(str(int(v)) for v in hvals)
            out.at[i, "mean_author_hindex"] = mean(hvals)

        # max cited year from references endpoint
        if pid in max_cited_year:
            out.at[i, "_max_cited_year"] = int(max_cited_year[pid])

    out["s2_enriched"] = True

    # provenance run.json (best-effort)
    run = {
        "phase": "extractor_only_csv_single_sheet",
        "records_in": int(len(df)),
        "records_out": int(len(out)),
        "paper_ids_requested": len(ids),
        "paper_ids_unique": len(set(ids)),
        "weights": params.get("weights", {}),
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    try:
        provenance_dir = logs_base / "provenance"
        provenance_dir.mkdir(parents=True, exist_ok=True)
        with (provenance_dir / "run.json").open("w", encoding="utf-8") as f:
            json.dump(run, f, indent=2)
    except Exception:
        # non-fatal if writing provenance fails
        pass

    return out
