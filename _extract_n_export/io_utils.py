from __future__ import annotations
import os
import pandas as pd
from .utils import deterministic_serialize_list



def load_csv(csv_path: str = "input") -> pd.DataFrame:


    # Load the CSVs
    csv_frames: list[pd.DataFrame] = []

    if os.path.isdir(csv_path):
        for name in ("broad.csv", "precise.csv"):
            file_path = os.path.join(csv_path, name)
            if os.path.exists(file_path):
                csv_frames.append(pd.read_csv(file_path, dtype=str, keep_default_na=False).fillna(""))
    else:
        if os.path.exists(csv_path):
            csv_frames.append(pd.read_csv(csv_path, dtype=str, keep_default_na=False).fillna(""))

    if not csv_frames:
        raise FileNotFoundError(
            "No CSV inputs found. Provide a directory containing broad.csv/precise.csv or a single CSV file."
        )

    # Merge them into one DataFrame
    df = pd.concat(csv_frames, ignore_index=True) if len(csv_frames) > 1 else csv_frames[0]

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
