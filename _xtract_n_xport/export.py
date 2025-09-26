from __future__ import annotations
from pathlib import Path
from typing import Optional

import pandas as pd

from output_paths import resolve_csv_dir

DATA_COLUMNS = [
    "mode","paperId","title","publication_date","year",
    "publication_types","fields_of_study",
    "influential_citation_count","citation_count",
    "abstract","external_ids","doi","is_open_access","open_access_pdf_url",
    "journal_pages_range","pages_total","references_pages","references_count","_max_cited_year",
    "authors_hindex_list","mean_author_hindex",
    "_prov_csv_row","_mode_display"
]

def export_extracted(df: pd.DataFrame, outdir: str, csv_dir: Optional[str] = None):
    base = Path(outdir)
    base.mkdir(parents=True, exist_ok=True)
    # Ensure data exports reside under the dedicated CSV directory.
    target_dir = resolve_csv_dir(base, csv_dir)
    for c in DATA_COLUMNS:
        if c not in df.columns: df[c] = ""
    df = df[DATA_COLUMNS].copy()
    df.to_excel(target_dir / "extracted_dataset.xlsx", index=False)
