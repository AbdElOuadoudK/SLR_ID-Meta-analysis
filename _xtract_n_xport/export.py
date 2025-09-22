from __future__ import annotations
import os, pandas as pd

DATA_COLUMNS = [
    "mode","paperId","title","publication_date","year",
    "publication_types","fields_of_study",
    "influential_citation_count","citation_count",
    "abstract","external_ids","doi","is_open_access","open_access_pdf_url",
    "journal_pages_range","pages_total","references_pages","references_count","_max_cited_year",
    "authors_hindex_list","mean_author_hindex",
    "_prov_csv_row","_mode_display"
]

def export_extracted(df: pd.DataFrame, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    for c in DATA_COLUMNS:
        if c not in df.columns: df[c] = ""
    df = df[DATA_COLUMNS].copy()
    df.to_excel(os.path.join(outdir, "extracted_dataset.xlsx"), index=False)
