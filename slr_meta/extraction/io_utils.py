from __future__ import annotations

from pathlib import Path

import pandas as pd

from slr_meta.shared.paths import get_csv_dir

from .utils import deterministic_serialize_list


INFLUENTIAL_CITATION_COUNT_COLUMN = "influentialCitationCount"
DEFAULT_PREPROCESS_LIMIT = 300


def trim_by_influential_citation_count(
    df: pd.DataFrame,
    *,
    limit: int = DEFAULT_PREPROCESS_LIMIT,
) -> pd.DataFrame:
    """Sort by influential citations and retain top complete citation groups.

    Rows are sorted descending by influential citation count. If the citation
    count at the requested limit is tied by later rows, that whole boundary tie
    group is excluded so the returned row count never exceeds the limit and ties
    are not split across the trim boundary.
    """
    if limit < 1:
        raise ValueError("limit must be at least 1")
    if INFLUENTIAL_CITATION_COUNT_COLUMN not in df.columns:
        raise KeyError(f"Missing required column: {INFLUENTIAL_CITATION_COUNT_COLUMN}")
    if df.empty:
        return df.copy()

    result = df.copy()
    citation_counts = pd.to_numeric(
        result[INFLUENTIAL_CITATION_COUNT_COLUMN],
        errors="raise",
        downcast="integer",
    )
    result = result.assign(_influential_citation_count_sort=citation_counts)
    result = result.sort_values(
        by="_influential_citation_count_sort",
        ascending=False,
        kind="mergesort",
    )

    if len(result) > limit:
        boundary_count = result.iloc[limit - 1]["_influential_citation_count_sort"]
        keep_at_boundary = result["_influential_citation_count_sort"] >= boundary_count
        if int(keep_at_boundary.sum()) <= limit:
            result = result[keep_at_boundary]
        else:
            result = result[
                result["_influential_citation_count_sort"] > boundary_count
            ]

    return result.drop(columns=["_influential_citation_count_sort"]).reset_index(drop=True)


def _read_and_preprocess_input_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    return trim_by_influential_citation_count(df)


def load_csv(csv_path: str | Path | None = None) -> pd.DataFrame:

    base = Path(csv_path) if csv_path else get_csv_dir()
    base.mkdir(parents=True, exist_ok=True)

    # Load, numerically sort, and trim each CSV before merging them. If a tie
    # group crosses the 300-row boundary, the whole boundary group is excluded so
    # no CSV contributes more than the configured preprocessing limit.
    df1 = _read_and_preprocess_input_csv(base / "precise.csv")
    df2 = _read_and_preprocess_input_csv(base / "broad.csv")

    # Merge them into one DataFrame
    df = pd.concat([df1, df2], ignore_index=True)

    df["_prov_csv_row"] = (df.index + 1).astype(int)
    if df.columns[0].lower() == "mode":
        df["_mode_display"] = df.iloc[:,0]
    # Canonicalize known columns if present
    if "publicationDate" in df.columns and "publication_date" not in df.columns:
        df["publication_date"] = df["publicationDate"]
    if "publicationTypes" in df.columns and "publication_types" not in df.columns:
        df["publication_types"] = df["publicationTypes"].apply(deterministic_serialize_list)
    if "fieldsOfStudy" in df.columns and "fields_of_study" not in df.columns:
        df["fields_of_study"] = df["fieldsOfStudy"].apply(deterministic_serialize_list)
    if "influentialCitationCount" in df.columns and "influential_citation_count" not in df.columns:
        df["influential_citation_count"] = df["influentialCitationCount"]
    return df
