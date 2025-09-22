from __future__ import annotations
import pandas as pd
from .utils import deterministic_serialize_list



def load_csv(csv_path: str = "input") -> pd.DataFrame:


    # Load the CSVs
    df1 = pd.read_csv(f"{csv_path}/broad.csv", dtype=str, keep_default_na=False).fillna("")
    df2 = pd.read_csv(f"{csv_path}/precise.csv", dtype=str, keep_default_na=False).fillna("")

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
