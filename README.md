# SLR ID Meta-analysis

This repository contains a Python workflow for collecting, enriching, downloading, and parsing literature records for a systematic literature review/meta-analysis dataset. The current default configuration targets review papers about intrusion detection systems (IDS) in Semantic Scholar, but the search query and enrichment parameters are configurable through JSON files.

The implementation is organized as command-line entry points at the repository root (`collect.py`, `select_papers.py`, `download_papers.py`, and `parse_papers.py`) backed by the `slr_meta` package. Root-level scripts are compatibility aliases that call the package modules.

## Project objectives

The codebase supports these objectives:

1. Query Semantic Scholar's bulk paper-search endpoint in broad and precise modes.
2. Export raw API pages and normalized CSV search ledgers.
3. Merge and preprocess the broad/precise search outputs, including trimming each set to the highest influential-citation records.
4. Enrich selected papers with additional Semantic Scholar metadata, author h-index values, open-access PDF URLs, citation counts, and reference-year information.
5. Produce Excel outputs for downstream review and metric work.
6. Download open-access PDFs from enriched records when URLs are available.
7. Convert GROBID TEI XML files into cleaned plain-text files.

## Key features

- **Semantic Scholar collection modes**: `broad`, `precise`, or `both`, with token-based paging against `/graph/v1/paper/search/bulk`.
- **Configurable search package**: endpoint, year range, fields, publication type, field of study, page limit, and per-mode queries are read from `request_config.json` by default.
- **API key support**: if `SEMANTIC_SCHOLAR_API_KEY` is set, requests include it as the `x-api-key` header.
- **Standard output directories**: generated CSV/XLSX files default to `CSVs/`; logs default to `logs/`; collection raw JSON pages default to `raw/`.
- **Selection and enrichment workflow**: reads `precise.csv` and `broad.csv`, trims each by influential citation count, enriches records with Semantic Scholar batch endpoints, and writes `extracted_dataset.xlsx` plus `paperset.xlsx`.
- **Raw provenance capture**: enrichment responses are written under `logs/provenance/raw_s2/`, and run timestamps are appended to `logs/provenance.txt`.
- **Robust PDF downloader**: validates PDF content by content type and `%PDF-` magic bytes, retries downloads, skips existing non-empty files, uses HTTPX and Selenium fallbacks, and writes failure summaries.
- **GROBID TEI text extraction**: extracts title, abstract, and body text while removing references, captions, citation markers, and other TEI elements according to CLI flags.
- **Compatibility modules**: the older `_xtract_n_xport` package remains present as a compatibility layer/legacy mirror, while the active package is `slr_meta`.

## Repository structure

```text
.
├── collect.py                    # Compatibility CLI for Semantic Scholar collection
├── select_papers.py              # Compatibility CLI for selection/enrichment workflow
├── download_papers.py            # Compatibility CLI for PDF downloading
├── parse_papers.py               # Compatibility CLI for TEI-to-text parsing
├── sheet_builder.py              # Compatibility module for workbook generation
├── request_config.json           # Default Semantic Scholar bulk-search configuration
├── params.json                   # Default enrichment/API retry and metric-weight parameters
├── grobid_config.json            # Client-side GROBID processing configuration
├── grobid.yaml                   # GROBID server configuration example
├── requirements.txt              # Python dependencies
├── slr_meta/
│   ├── collection/semantic_scholar.py  # Bulk Semantic Scholar collection implementation
│   ├── downloads/papers.py             # PDF download implementation
│   ├── extraction/io_utils.py          # CSV loading, trimming, and normalization helpers
│   ├── extraction/s2.py                # Semantic Scholar enrichment client
│   ├── extraction/export.py            # Extracted dataset export
│   ├── extraction/workflow.py          # End-to-end selection/enrichment workflow
│   ├── parsing/grobid.py               # GROBID TEI XML text extractor
│   ├── sheets/builder.py               # Excel workbook/template builder
│   └── shared/paths.py                 # Shared output-directory helpers
├── _xtract_n_xport/              # Legacy/compatibility extraction package
└── tests/                        # Pytest tests for collection, download, and CSV helpers
```

Generated runtime directories such as `CSVs/`, `logs/`, `raw/`, and `papers/` are created by the scripts as needed and are not required to exist before first use.

## Prerequisites

- Python 3.10+ is recommended. The code uses modern type-hint syntax such as `str | Path`.
- Network access for Semantic Scholar collection/enrichment and PDF downloading.
- Optional but recommended: a Semantic Scholar API key stored in `SEMANTIC_SCHOLAR_API_KEY`.
- For `download_papers.py` Selenium fallback support:
  - Google Chrome or Chromium must be available in the runtime environment.
  - `webdriver-manager` downloads/manages the matching ChromeDriver.
- For GROBID workflows:
  - `parse_papers.py` expects existing GROBID TEI XML files. It does not call a GROBID server itself.
  - `grobid_config.json` and `grobid.yaml` are configuration files for external GROBID-related processing/server setup, but no repository CLI currently invokes the GROBID server to create TEI files.

## Installation

Clone the repository, create a virtual environment, and install dependencies:

```bash
git clone <repository-url>
cd SLR_ID-Meta-analysis
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

The dependency list includes `pandas`, `numpy`, `requests`, `openpyxl`, `lxml`, `selenium`, `webdriver-manager`, `httpx`, `grobid-client-python`, and `pytest`.

## Environment variables

### `SEMANTIC_SCHOLAR_API_KEY` (optional)

Used by both collection and enrichment code. When present, requests include:

```http
x-api-key: <SEMANTIC_SCHOLAR_API_KEY>
```

Set it in your shell before running collection or enrichment:

```bash
export SEMANTIC_SCHOLAR_API_KEY="your-api-key"
```

If this variable is not set, the scripts still send requests without the API key header. This may work for public endpoints but is more likely to encounter rate limits.

### Other environment variables

No other environment variables are read by the current implementation.

## Configuration files

### `request_config.json`

Default input for `collect.py`. It defines:

- `endpoint`: Semantic Scholar bulk-search URL.
- `limit`: page size sent to the endpoint.
- `fieldsOfStudy`: configured field filter.
- `year`: configured year filter.
- `fields`: response fields requested from Semantic Scholar.
- `publicationTypes`: optional publication type filter.
- `headers`: additional static headers; API key handling is separate.
- `modes`: `broad` and `precise` query definitions.

The current default searches Computer Science review papers from `2021-2025` and requests `paperId`, `title`, `publicationDate`, `publicationTypes`, `fieldsOfStudy`, and `influentialCitationCount`.

### `params.json`

Default input for `select_papers.py`. The implemented enrichment code uses the `s2` section:

- `base_url`
- `paper_batch_fields`
- `author_batch_fields`
- `references_fields`
- `batch_size`
- `retry_sleep_seconds`
- `timeout_seconds`
- `max_retries`

The file also contains `weights`, `q05`, and `q95`. In the current implementation, these are passed to workbook-building functions but no metric computation code currently uses them in Python.

### `grobid_config.json` and `grobid.yaml`

These files describe GROBID-related settings. The active `parse_papers.py` command reads TEI XML files from disk and does not read either configuration file. If you use an external GROBID client/server to create TEI files, these files can serve as configuration starting points.

## Usage

### 1. Collect Semantic Scholar search results

Run both configured modes:

```bash
python collect.py
```

Run only one mode:

```bash
python collect.py --mode broad
python collect.py --mode precise
```

Use a custom collection config and output directories:

```bash
python collect.py \
  --config path/to/request_config.json \
  --raw-dir raw \
  --csv-dir CSVs \
  --log-dir logs
```

Outputs:

- `raw/<mode>-bulk-pNN.json`: raw page responses or saved error payloads.
- `raw/<mode>-bulk-raw.json`: merged raw records for the mode.
- `CSVs/<mode>.csv`: normalized search results.
- `logs/ledger_<mode>.json`: per-mode run metadata.
- `logs/harvest_ledger.json`: aggregate collection ledger.

### 2. Select, merge, and enrich papers

After `CSVs/broad.csv` and `CSVs/precise.csv` exist, run:

```bash
python select_papers.py
```

Use a custom input CSV directory and params file:

```bash
python select_papers.py --input CSVs --params params.json
```

Important behavior:

- The workflow expects `precise.csv` and `broad.csv` in the input directory.
- Each input CSV must include `influentialCitationCount`.
- Each mode is sorted by `influentialCitationCount` and trimmed to at most 300 rows. If a tie group crosses the 300-row boundary, that whole boundary group is excluded so ties are not split and row counts do not exceed the limit.
- Legacy `--output` is intentionally rejected; outputs go to `CSVs/` and logs to `logs/`.

Outputs:

- `CSVs/extracted_dataset.xlsx`
- `CSVs/metrics_template_single_sheet.xlsx`
- `CSVs/paperset.xlsx`
- `logs/provenance.txt`
- `logs/provenance/s2_client.log`
- `logs/provenance/raw_s2/*.jsonl` and `references_<paperId>.json`

### 3. Download open-access PDFs

Use the enriched dataset or another CSV/XLSX containing `paperId` and `open_access_pdf_url` columns:

```bash
python download_papers.py CSVs/extracted_dataset.xlsx --output papers --workers 8
```

For Excel input, choose a sheet explicitly if needed:

```bash
python download_papers.py CSVs/extracted_dataset.xlsx --sheet Sheet1 --output papers
```

The downloader automatically scans Excel workbooks for a worksheet containing the required columns if `--sheet` is not supplied.

Outputs:

- `papers/*.pdf`: downloaded PDFs. Filenames are based on the title when available, otherwise `paperId`.
- `CSVs/failures_summary.csv`: `paperId`, `url`, and failure reason for failed downloads.
- `CSVs/failures_ids.txt`: one failed paper ID per line.
- `logs/failures.log`: detailed download diagnostics.

### 4. Parse GROBID TEI XML into text

Parse one file:

```bash
python parse_papers.py path/to/paper.grobid.tei.xml
```

Parse a directory recursively and write outputs elsewhere:

```bash
python parse_papers.py tei_xml_dir --recursive --outdir parsed_text
```

Optional parsing flags:

```bash
python parse_papers.py tei_xml_dir \
  --recursive \
  --remove-citations \
  --include-captions \
  --keep-references \
  --debug
```

Outputs:

- One `.txt` file per input XML file. For example, `paper.grobid.tei.xml` becomes `paper.txt`.
- `logs/parse_papers.log` by default, or the file specified by `--log-file-name`.

## Data collection and processing pipeline

1. **Bulk search collection**
   - `collect.py` loads `request_config.json`.
   - It merges shared settings with each mode's query settings.
   - It pages through Semantic Scholar's bulk search endpoint using returned `token` values.
   - Raw pages, merged raw JSON, CSV exports, and ledgers are written.

2. **CSV preprocessing**
   - `select_papers.py` loads `CSVs/precise.csv` and `CSVs/broad.csv`.
   - Each CSV is sorted by numeric `influentialCitationCount` descending.
   - Each CSV is trimmed to at most 300 rows by default. Ties are kept only when the full tie group fits within the limit; if a boundary tie would exceed the limit, that whole boundary tie group is excluded.
   - The two DataFrames are concatenated. This phase does not deduplicate papers.

3. **Semantic Scholar enrichment**
   - The enrichment client uses the paper batch endpoint to fetch configured paper fields.
   - It gathers author IDs from paper records and uses the author batch endpoint for h-index values.
   - It calls each paper's references endpoint and computes `_max_cited_year`, restricted to references with years no later than the citing paper's year when that year is available.
   - Raw API responses and client logs are stored under `logs/provenance/`.

4. **Export and workbook generation**
   - `extracted_dataset.xlsx` is written with a fixed set of raw/enriched columns.
   - A single-sheet workbook template is generated.
   - If extracted data exists, `paperset.xlsx` is populated on a `rawdata` sheet.

5. **Optional PDF download**
   - `download_papers.py` reads CSV/XLSX rows with `paperId` and `open_access_pdf_url`.
   - It downloads PDFs concurrently and validates downloaded content.
   - Failures are collected in CSV/TXT logs.

6. **Optional full-text extraction**
   - External GROBID processing must first produce TEI XML files.
   - `parse_papers.py` converts those TEI XML files into cleaned plain text.

## Generated outputs and column contents

### `CSVs/broad.csv` and `CSVs/precise.csv`

Written by collection. Columns:

- `mode`: mode label, usually `BROAD` or `PRECISE`.
- `paperId`: Semantic Scholar paper ID.
- `title`: paper title.
- `publicationDate`: publication date as returned by Semantic Scholar.
- `year`: parsed four-digit year from `publicationDate`.
- `publicationTypes`: semicolon-separated list when the API returns a list.
- `fieldsOfStudy`: semicolon-separated list when the API returns a list.
- `influentialCitationCount`: Semantic Scholar influential citation count.

### `CSVs/extracted_dataset.xlsx`

Written by selection/enrichment. Columns are fixed by `slr_meta/extraction/export.py`:

- Search/provenance fields: `mode`, `paperId`, `title`, `publication_date`, `year`, `publication_types`, `fields_of_study`, `influential_citation_count`, `_prov_csv_row`, `_mode_display`.
- Citation/enrichment fields: `citation_count`, `abstract`, `external_ids`, `doi`, `is_open_access`, `open_access_pdf_url`, `journal_pages_range`, `pages_total`, `references_pages`, `references_count`, `_max_cited_year`, `authors_hindex_list`, `mean_author_hindex`.

`references_pages` is included in the export schema but is not populated by the current enrichment implementation.

### `CSVs/paperset.xlsx`

Written by the workbook builder. It contains one sheet named `rawdata` with the main paper metadata/enrichment columns. The current template-building code creates only this single sheet.

### Failure and log outputs

- `CSVs/failures_summary.csv`: PDF download failures with `paperId`, `url`, and `reason`.
- `CSVs/failures_ids.txt`: failed IDs for quick retry/filtering.
- `logs/*.log`: command-specific logs.
- `logs/ledger_*.json` and `logs/harvest_ledger.json`: collection provenance.
- `logs/provenance/raw_s2/`: raw enrichment responses.

## Command-line options

### `collect.py`

```text
--mode {both,broad,precise}   Collection mode. Default: both.
--config PATH                 Unified request configuration. Default: ./request_config.json.
--log-dir DIR                 Ledger/log output directory. Default: ./logs.
--csv-dir DIR                 CSV export directory. Default: ./CSVs.
--raw-dir DIR                 Raw JSON output directory. Default: ./raw.
```

### `select_papers.py`

```text
--input DIR       Directory containing precise.csv and broad.csv. Default: CSVs/.
--params PATH     Optional params JSON path. If omitted, the workflow searches known params.json locations.
```

`--output` is not supported and causes an immediate failure.

### `download_papers.py`

```text
spreadsheet                  Required CSV/XLSX input path.
--sheet NAME                 Excel worksheet name. Optional; auto-detects a suitable sheet when omitted.
--output DIR                 PDF output directory. Default: ./papers.
--workers N                  Concurrent worker count. Default: 8.
--log-dir DIR                Log directory. Default is intended to be ./logs.
--log-file-name NAME         Log file name. Default: failures.log.
--csv-dir DIR                Directory for failure summary outputs. Default is intended to be ./CSVs.
```

Known implementation detail: `download_papers.py` resolves default `--log-dir` and `--csv-dir` relative to the package module directory, not the repository root, when no override is supplied. To force repository-root outputs, pass `--log-dir logs --csv-dir CSVs`.

### `parse_papers.py`

```text
inputs...                    Files, globs, or directories.
-o, --outdir DIR             Output directory. Default: each input file's directory.
--recursive                  Recursively search input directories.
--debug                      Enable debug logging.
--include-captions           Keep figure/table captions instead of stripping them.
--keep-references            Keep references/bibliography instead of stripping them.
--remove-citations           Remove simple bracket and author-year citation markers.
--log-dir DIR                Log directory. Default is intended to be ./logs.
--log-file-name NAME         Log file name. Default: parse_papers.log.
```

Known implementation detail: like `download_papers.py`, `parse_papers.py` resolves the default log directory relative to the package module directory when no override is supplied. Pass `--log-dir logs` to force repository-root logs.

## Error handling and troubleshooting

### Semantic Scholar rate limits or server errors

- Collection retries indefinitely for HTTP `429`, `500`, `502`, `503`, and `504` responses.
- Enrichment retries indefinitely for `429` and `500`, and retries `502`/`503` up to `max_retries` from `params.json`.
- Set `SEMANTIC_SCHOLAR_API_KEY` to reduce rate-limit issues.
- Check `logs/ledger_*.json`, `logs/harvest_ledger.json`, and `logs/provenance/s2_client.log` for details.

### Missing collection CSVs

`select_papers.py` expects both `precise.csv` and `broad.csv` in the input directory. Run `python collect.py` first or provide an input directory containing both files.

### Missing or invalid `influentialCitationCount`

The selection preprocessing step requires `influentialCitationCount` and parses it numerically. If the column is missing or contains non-numeric values, the workflow fails during preprocessing.

### PDF downloads fail or save no files

- Ensure the spreadsheet has `paperId` and `open_access_pdf_url` columns.
- Some URLs return landing pages, require browser behavior, block automated clients, or do not serve actual PDFs.
- Inspect `CSVs/failures_summary.csv` and `logs/failures.log`.
- For Selenium fallback, ensure Chrome/Chromium is installed.
- Increase reliability by lowering `--workers` if remote hosts throttle concurrent requests.

### TEI parsing finds no files

`parse_papers.py` searches for `*.grobid.tei.xml`, `*.tei.xml`, and `*.xml` in directories. Use `--recursive` for nested folders or pass explicit file paths/globs.

### GROBID server confusion

The repository includes GROBID configuration files and the `grobid-client-python` dependency, but no active CLI in this repository currently converts PDFs into TEI XML. You need an external GROBID processing step before running `parse_papers.py`.

## Development notes

- Root-level scripts are thin compatibility aliases that replace their module entry with the corresponding `slr_meta` package module.
- Shared output directory behavior lives in `slr_meta/shared/paths.py`.
- Tests are written with `pytest` under `tests/`.
- The code intentionally avoids deduplication during collection and merge phases; duplicates may remain when the same paper appears in both modes.
- `slr_meta/extraction/export.py` controls the exact column order for `extracted_dataset.xlsx`.
- `slr_meta/sheets/builder.py` currently creates a single-sheet workbook rather than a multi-sheet analytics workbook.

Run tests with:

```bash
pytest
```

## Limitations and known issues

- No repository script currently runs GROBID over downloaded PDFs; TEI XML generation is external.
- The selection workflow does not deduplicate broad and precise results.
- `references_pages` is exported but not populated by current code.
- `weights`, `q05`, and `q95` in `params.json` are not used for Python-side metric computation in the current implementation.
- Default log/CSV directory resolution in `download_papers.py` and `parse_papers.py` is relative to package subdirectories unless explicit overrides are supplied.
- Semantic Scholar retry behavior can wait indefinitely for some collection/enrichment status codes.
- PDF downloading depends on third-party publisher behavior and may fail for valid records if hosts block automation or return non-PDF content.
- The workbook template currently contains a formula in the `paperId` column, but `paperset.xlsx` population overwrites cells with extracted data when available.

## Suggestions for future improvements

- Add a CLI step to submit downloaded PDFs to GROBID and produce TEI XML files.
- Add deduplication by `paperId` or DOI between broad and precise modes.
- Make preprocessing limits such as the top-300 influential-citation trim configurable from `params.json` or CLI flags.
- Normalize all output-directory resolution to the repository root across commands.
- Populate `references_pages` or remove it from the export schema if it is not needed.
- Implement documented metric computations using the `weights`, `q05`, and `q95` settings, or remove unused parameters.
- Add integration tests for the full collect → select/enrich → download/parse workflow with mocked network calls.
- Add project metadata such as `pyproject.toml`, package versioning, and console-script entry points.

## License

No license file is present in the repository at the time this README was written. Add a license before publishing or redistributing the project.

## Acknowledgments

This project uses Semantic Scholar API data and includes configuration for GROBID-based document processing. The implementation depends on open-source Python packages listed in `requirements.txt`.
