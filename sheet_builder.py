from __future__ import annotations

import os
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment

# Single-sheet column layout (merged table)
COLUMNS = [
    "mode", "paperId", "title", "publication_date", "year",
    "publication_types", "fields_of_study",
    "influential_citation_count", "citation_count",
    "abstract", "external_ids", "doi", "is_open_access", "open_access_pdf_url",
    "journal_pages_range", "pages_total", "references_pages", "references_count", "_max_cited_year",
    "authors_hindex_list", "mean_author_hindex",

    "cites_per_day", "recency_delay_years", "pages_minus_refs",
    "normalized_pages_minus_refs", "normalized_references_count", "normalized_recency_delay",
    "normalized_cites_per_day", "normalized_mean_author_hindex", "composite_score",
]

# Helpers
def _write_headers(ws, headers):
    for j, h in enumerate(headers, start=1):
        ws.cell(row=1, column=j, value=h)


def build_template(path: str, params: dict):
    """
    Build an .xlsx with a single sheet 'Paperset' that contains both the raw
    input columns and all metric/formula columns. This preserves the original
    semantics but keeps everything in one table.
    """
    wb = Workbook()
    default = wb.active
    wb.remove(default)

    ws = wb.create_sheet("Paperset", 0)
    _write_headers(ws, COLUMNS)
    ws.freeze_panes = "A2"

    # Column index map
    idx = {h: i for i, h in enumerate(COLUMNS, start=1)}

    def col(h): return get_column_letter(idx[h])

    # Open-ended ranges for ARRAYFORMULA-like formulas (keeps original strings)
    def rng(h): return f"{col(h)}2:{col(h)}"

    # Row where formulas are placed (row 2 using ARRAYFORMULA semantics)
    r = 2

    # Params (percentiles, weights)
    q05 = params.get("q05", 0.05)
    q95 = params.get("q95", 0.95)
    w = params.get("weights", {})
    w_pmr = w.get("pages_minus_refs", 0.30)
    w_ref = w.get("references_count", 0.25)
    w_rec = w.get("recency_delay_years", 0.20)
    w_cpd = w.get("cites_per_day", 0.15)
    w_hix = w.get("mean_author_hindex", 0.10)

    # ---------------------------
    # Metric formulas (written into the same sheet)
    # ---------------------------

    # paperId: copy from same sheet (keeps row alignment)
    ws[f"{col('paperId')}{r}"] = f"=ARRAYFORMULA(IF(LEN({rng('paperId')})=0, \"\", {rng('paperId')}))"
    ws[f"{col('paperId')}{r}"].alignment = Alignment(wrapText=True)

    # cites_per_day: VALUE(citation_count) / max(1, days since DATEVALUE(publication_date))
    ws[f"{col('cites_per_day')}{r}"] = f"""=ARRAYFORMULA(
IF(LEN({rng('paperId')})=0, "",
IFERROR(
  VALUE({rng('citation_count')}) /
    IF( (TODAY() - DATEVALUE({rng('publication_date')})) < 1, 1, (TODAY() - DATEVALUE({rng('publication_date')})) ),
  ""
)
)
)"""
    ws[f"{col('cites_per_day')}{r}"].alignment = Alignment(wrapText=True)

    # recency_delay_years: max(0, year - _max_cited_year)
    ws[f"{col('recency_delay_years')}{r}"] = f"""=ARRAYFORMULA(
IF(LEN({rng('paperId')})=0, "",
IF( (LEN({rng('year')})=0) + (LEN({rng('_max_cited_year')})=0), "",
  IF( (VALUE({rng('year')}) - VALUE({rng('_max_cited_year')})) < 0, 0, VALUE({rng('year')}) - VALUE({rng('_max_cited_year')}) )
)
)"""
    ws[f"{col('recency_delay_years')}{r}"].alignment = Alignment(wrapText=True)

    # pages_minus_refs = pages_total - references_pages
    ws[f"{col('pages_minus_refs')}{r}"] = f"""=ARRAYFORMULA(
IF(LEN({rng('paperId')})=0, "",
IF( (LEN({rng('pages_total')})=0) + (LEN({rng('references_pages')})=0), "",
  IFERROR(VALUE({rng('pages_total')}),"") - IFERROR(VALUE({rng('references_pages')}),"")
)
)"""
    ws[f"{col('pages_minus_refs')}{r}"].alignment = Alignment(wrapText=True)

    # normalized_pages_minus_refs (clamped at 0; robust percentiles)
    base_pmr = f"IFERROR(VALUE({rng('pages_minus_refs')}),)"
    vec_pmr = f"FILTER({base_pmr}, ISNUMBER({base_pmr}))"
    P05_pmr = f"PERCENTILE({vec_pmr}, {q05})"
    P95_pmr = f"PERCENTILE({vec_pmr}, {q95})"
    expr_pmr = (
        f"IF({P05_pmr}={P95_pmr}, 1, "
        f"IF( (({base_pmr}-{P05_pmr})/({P95_pmr}-{P05_pmr}))<0, 0, "
        f"IF( (({base_pmr}-{P05_pmr})/({P95_pmr}-{P05_pmr}))>1, 1, "
        f"(({base_pmr}-{P05_pmr})/({P95_pmr}-{P05_pmr})) )))"
    )
    ws[f"{col('normalized_pages_minus_refs')}{r}"] = (
        f"=ARRAYFORMULA(IF(LEN({rng('paperId')})=0, \"\", IF(LEN({rng('pages_minus_refs')})=0, \"\", {expr_pmr}) ))"
    )
    ws[f"{col('normalized_pages_minus_refs')}{r}"].alignment = Alignment(wrapText=True)

    # normalized_references_count = log1p(references_count), robust percentiles
    num_rc = f"IFERROR(VALUE({rng('references_count')}),)"
    vec_rc = f"FILTER({num_rc}, ISNUMBER({num_rc}))"
    P05_rc = f"PERCENTILE(LN(1+{vec_rc}), {q05})"
    P95_rc = f"PERCENTILE(LN(1+{vec_rc}), {q95})"
    frac_rc = f"( (LN(1+IFERROR(VALUE({rng('references_count')}),)) - {P05_rc}) / ({P95_rc} - {P05_rc}) )"
    expr_rc = f"IF({P05_rc}={P95_rc}, 1, IF({frac_rc}<0, 0, IF({frac_rc}>1, 1, {frac_rc})))"
    ws[f"{col('normalized_references_count')}{r}"] = (
        f"=ARRAYFORMULA(IF(LEN({rng('paperId')})=0, \"\", IF(LEN({rng('references_count')})=0, \"\", {expr_rc}) ))"
    )
    ws[f"{col('normalized_references_count')}{r}"].alignment = Alignment(wrapText=True)

    # normalized_recency_delay (lower is better)
    num_rec = f"IFERROR(VALUE({rng('recency_delay_years')}),)"
    vec_rec = f"FILTER({num_rec}, ISNUMBER({num_rec}))"
    P05_rec = f"PERCENTILE({vec_rec}, {q05})"
    P95_rec = f"PERCENTILE({vec_rec}, {q95})"
    frac_rec = f"( ({num_rec} - {P05_rec}) / ({P95_rec} - {P05_rec}) )"
    expr_rec = f"IF({P05_rec}={P95_rec}, 1, 1 - IF({frac_rec}<0, 0, IF({frac_rec}>1, 1, {frac_rec})) )"
    ws[f"{col('normalized_recency_delay')}{r}"] = (
        f"=ARRAYFORMULA(IF(LEN({rng('paperId')})=0, \"\", IF(LEN({rng('recency_delay_years')})=0, \"\", {expr_rec}) ))"
    )
    ws[f"{col('normalized_recency_delay')}{r}"].alignment = Alignment(wrapText=True)

    # normalized_cites_per_day
    num_cpd = f"IFERROR(VALUE({rng('cites_per_day')}),)"
    vec_cpd = f"FILTER({num_cpd}, ISNUMBER({num_cpd}))"
    P05_cpd = f"PERCENTILE({vec_cpd}, {q05})"
    P95_cpd = f"PERCENTILE({vec_cpd}, {q95})"
    frac_cpd = f"( ({num_cpd} - {P05_cpd}) / ({P95_cpd} - {P05_cpd}) )"
    expr_cpd = f"IF({P05_cpd}={P95_cpd}, 1, IF({frac_cpd}<0, 0, IF({frac_cpd}>1, 1, {frac_cpd})) )"
    ws[f"{col('normalized_cites_per_day')}{r}"] = (
        f"=ARRAYFORMULA(IF(LEN({rng('paperId')})=0, \"\", IF(LEN({rng('cites_per_day')})=0, \"\", {expr_cpd}) ))"
    )
    ws[f"{col('normalized_cites_per_day')}{r}"].alignment = Alignment(wrapText=True)

    # normalized_mean_author_hindex = log1p(mean_author_hindex), robust percentiles
    num_hix = f"IFERROR(VALUE({rng('mean_author_hindex')}),)"
    vec_hix = f"FILTER({num_hix}, ISNUMBER({num_hix}))"
    P05_hix = f"PERCENTILE(LN(1+{vec_hix}), {q05})"
    P95_hix = f"PERCENTILE(LN(1+{vec_hix}), {q95})"
    frac_hix = f"( (LN(1+IFERROR(VALUE({rng('mean_author_hindex')}),)) - {P05_hix}) / ({P95_hix} - {P05_hix}) )"
    expr_hix = f"IF({P05_hix}={P95_hix}, 1, IF({frac_hix}<0, 0, IF({frac_hix}>1, 1, {frac_hix})))"
    ws[f"{col('normalized_mean_author_hindex')}{r}"] = (
        f"=ARRAYFORMULA(IF(LEN({rng('paperId')})=0, \"\", IF(LEN({rng('mean_author_hindex')})=0, \"\", {expr_hix}) ))"
    )
    ws[f"{col('normalized_mean_author_hindex')}{r}"].alignment = Alignment(wrapText=True)

    # composite score (weights)
    ws[f"{col('composite_score')}{r}"] = (
        f"=ARRAYFORMULA(IF(LEN({rng('paperId')})=0, \"\", "
        f"{w_pmr}*{rng('normalized_pages_minus_refs')} + "
        f"{w_ref}*{rng('normalized_references_count')} + "
        f"{w_rec}*{rng('normalized_recency_delay')} + "
        f"{w_cpd}*{rng('normalized_cites_per_day')} + "
        f"{w_hix}*{rng('normalized_mean_author_hindex')} ))"
    )
    ws[f"{col('composite_score')}{r}"].alignment = Alignment(wrapText=True)

    # Save template
    wb.save(path)


def build_template_with_data(outdir: str, params: dict):
    """
    Create the single-sheet template, then populate 'Paperset' with extracted raw data
    from extracted_dataset.xlsx when available, and save the final workbook as metrics_with_formulas_single_sheet.xlsx.
    """
    template_path = os.path.join(outdir, "metrics_template_single_sheet.xlsx")
    build_template(template_path, params)

    data_path = os.path.join(outdir, "extracted_dataset.xlsx")
    if not os.path.exists(data_path):
        # Produce the template even if no extracted data is present yet.
        return

    df = pd.read_excel(data_path, dtype=str).fillna("")
    wb = load_workbook(template_path)
    ws = wb["Paperset"]

    headers = [c.value for c in ws[1]]
    col_idx = {h: i + 1 for i, h in enumerate(headers)}
    start = 2

    for i, row in df.iterrows():
        r = start + i
        for h, val in row.items():
            if h in col_idx:
                ws.cell(row=r, column=col_idx[h], value=("" if pd.isna(val) else val))

    wb.save(os.path.join(outdir, "paperset.xlsx"))
