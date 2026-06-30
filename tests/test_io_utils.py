from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from slr_meta.extraction.io_utils import load_csv, trim_by_influential_citation_count


def test_trim_by_influential_citation_count_sorts_descending_and_keeps_boundary_ties():
    df = pd.DataFrame(
        [
            {"paperId": "low", "influentialCitationCount": "1"},
            *(
                {"paperId": f"high-{i}", "influentialCitationCount": "10"}
                for i in range(299)
            ),
            {"paperId": "tie-a", "influentialCitationCount": "5"},
            {"paperId": "tie-b", "influentialCitationCount": "5"},
        ]
    )

    trimmed = trim_by_influential_citation_count(df, limit=300)

    assert len(trimmed) == 301
    assert trimmed["paperId"].tolist()[:299] == [f"high-{i}" for i in range(299)]
    assert trimmed["paperId"].tolist()[299:] == ["tie-a", "tie-b"]
    assert "low" not in trimmed["paperId"].tolist()


def test_trim_by_influential_citation_count_uses_numeric_sorting():
    df = pd.DataFrame(
        [
            {"paperId": "two", "influentialCitationCount": "2"},
            {"paperId": "ten", "influentialCitationCount": "10"},
            {"paperId": "one", "influentialCitationCount": "1"},
        ]
    )

    trimmed = trim_by_influential_citation_count(df, limit=2)

    assert trimmed["paperId"].tolist() == ["ten", "two"]


def test_trim_by_influential_citation_count_rejects_non_numeric_values():
    df = pd.DataFrame([{"paperId": "bad", "influentialCitationCount": "not-a-number"}])

    with pytest.raises(ValueError):
        trim_by_influential_citation_count(df)


def test_load_csv_preprocesses_precise_and_broad_independently(tmp_path):
    precise = pd.DataFrame(
        [
            {"paperId": f"p{i}", "influentialCitationCount": str(i)}
            for i in range(305)
        ]
    )
    broad = pd.DataFrame(
        [
            {"paperId": f"b{i}", "influentialCitationCount": str(i)}
            for i in range(305)
        ]
    )
    precise.to_csv(tmp_path / "precise.csv", index=False)
    broad.to_csv(tmp_path / "broad.csv", index=False)

    loaded = load_csv(tmp_path)

    assert len(loaded) == 600
    assert loaded["paperId"].tolist()[:3] == ["p304", "p303", "p302"]
    assert loaded["paperId"].tolist()[300:303] == ["b304", "b303", "b302"]
    assert "p4" not in loaded["paperId"].tolist()
    assert "b4" not in loaded["paperId"].tolist()
    assert "influential_citation_count" in loaded.columns
