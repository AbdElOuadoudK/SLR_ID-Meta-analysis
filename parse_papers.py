"""Helpers for processing PDFs with GROBID inside JupyterLab.

The module exposes :func:`process_pdfs` which relies on the Compose service
name ``grobid`` defined in ``grobid_config.json``. Use it from notebooks to
convert a directory of PDFs into TEI XML representations.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

from grobid_client.grobid_client import GrobidClient


TEI_SUFFIX = ".grobid.tei.xml"


def process_pdfs(
    input_dir: Path | str,
    output_dir: Path | str = "tei-output",
    concurrency: int = 2,
    grobid_config: Path | str = "grobid_config.json",
) -> List[Tuple[Path, Path]]:
    """Run GROBID's ``processFulltextDocument`` service over local PDFs.

    Parameters
    ----------
    input_dir:
        Directory containing PDF files to parse.
    output_dir:
        Destination directory for the generated TEI XML.
    concurrency:
        Number of worker threads used by the client when dispatching requests.
    grobid_config:
        Path to the Grobid client JSON configuration file.
    """

    input_path = Path(input_dir)
    if not input_path.is_dir():
        raise NotADirectoryError(f"{input_path} is not a directory")

    pdf_files = sorted(input_path.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"No PDF files found in {input_path}")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    client = GrobidClient(config_path=str(grobid_config))
    client.process(
        service="processFulltextDocument",
        input_path=str(input_path),
        output=str(output_path),
        n=concurrency,
        verbose=False,
    )

    results: List[Tuple[Path, Path]] = []
    for pdf in pdf_files:
        tei_file = output_path / (pdf.stem + TEI_SUFFIX)
        results.append((pdf, tei_file))

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process PDFs with GROBID")
    parser.add_argument("input", help="Directory containing PDF files")
    parser.add_argument(
        "-o",
        "--output",
        default="tei-output",
        help="Directory where TEI XML files will be written",
    )
    parser.add_argument(
        "-n", "--concurrency", type=int, default=2, help="Number of worker threads"
    )

    args = parser.parse_args()

    processed_files = process_pdfs(
        args.input, output_dir=args.output, concurrency=args.concurrency
    )

    for pdf_file, tei_file in processed_files:
        print(f"Processed {pdf_file} -> {tei_file}")
