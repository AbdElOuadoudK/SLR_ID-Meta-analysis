#!/usr/bin/env python3
"""
Improved GROBID TEI -> clean TXT batch extractor.

Based on user's uploaded draft (see original file). Key improvements:
- safer fallback extraction
- case-insensitive file discovery
- additional TEI skip tags (listBibl/back/bibl)
- optional citation marker removal (--remove-citations)
- CLI toggles to include captions/keep references
- normalized deduplication of title/abstract vs body
- improved error handling and logging
"""
from typing import Iterable, List, Set, Union, Optional
from lxml import etree
import re
import pathlib
import argparse
import sys
import logging
import fnmatch
import os
from pathlib import Path

from output_paths import resolve_log_dir

TEI_NS = {"tei": "http://www.tei-c.org/ns/1.0"}
_whitespace_re = re.compile(r"\s+")

# Elements whose textual content we explicitly want
DEFAULT_ALLOWED: Set[str] = {"p", "head", "s", "title", "abstract"}

# Elements to remove entirely (but preserve their tail text)
DEFAULT_SKIP: Set[str] = {
    "figure", "table", "formula", "ref", "note", "graphic", "biblStruct",
    "ptr", "listBibl", "list", "label", "figDesc", "figureDesc", "back", "bibl"
}

# Conservative regexes for citation-like tokens to optionally strip
_BRACKET_CITATION_RE = re.compile(r"\[\s*\d+(?:\s*[,\-–]\s*\d+)*(?:\s*,\s*\d+)*\s*\]")
_PARENTHESES_AUTHOR_YEAR_RE = re.compile(
    r"\(\s*[A-Z][A-Za-z\-\s\.]{1,60}?,\s*\d{4}[a-z]?\s*\)"
)


def _normalize_whitespace(s: str) -> str:
    """Collapse whitespace runs into single spaces and trim ends."""
    return _whitespace_re.sub(" ", s or "").strip()


def _append_with_space_if_needed(prefix: str, addition: str) -> str:
    """
    Append `addition` to `prefix`, inserting one space if both sides lack whitespace.
    Prevents token-gluing like "ConclusionAt" but avoids double spaces.
    """
    if not prefix:
        return addition or ""
    if not addition:
        return prefix
    if prefix[-1].isspace() or addition[0].isspace():
        return prefix + addition
    return prefix + " " + addition


def _preserve_tail_and_remove(elem: etree._Element) -> None:
    """
    Remove elem from tree but preserve its .tail, appending it to the previous sibling
    or parent.text; ensure spacing at the join boundary.
    """
    parent = elem.getparent()
    if parent is None:
        return
    tail = elem.tail or ""
    prev = elem.getprevious()
    if prev is not None:
        prev_tail = prev.tail or ""
        prev.tail = _append_with_space_if_needed(prev_tail, tail)
    else:
        parent_text = parent.text or ""
        parent.text = _append_with_space_if_needed(parent_text, tail)
    parent.remove(elem)


def _text_from_itertext(el: etree._Element) -> str:
    """Join itertext tokens using space-aware concatenation, then normalize whitespace."""
    out = ""
    for token in el.itertext():
        out = _append_with_space_if_needed(out, token or "")
    return _normalize_whitespace(out)


class GrobidBodyExtractor:
    """
    Extract title, abstract, and main body text from a GROBID TEI XML.
    """

    def __init__(self,
                 allowed_tags: Optional[Iterable[str]] = None,
                 skip_tags: Optional[Iterable[str]] = None):
        self.allowed = set(allowed_tags) if allowed_tags is not None else set(DEFAULT_ALLOWED)
        self.skip = set(skip_tags) if skip_tags is not None else set(DEFAULT_SKIP)

    def parse(self, path: Union[str, pathlib.Path]) -> etree._ElementTree:
        parser = etree.XMLParser(remove_blank_text=False, recover=True)
        try:
            return etree.parse(str(path), parser)
        except etree.XMLSyntaxError as e:
            logging.error("XMLSyntaxError parsing %s: %s", path, e)
            raise

    def _remove_skip_elements_in_tree(self, root: etree._Element) -> None:
        """
        Remove skip elements anywhere in the tree (not just <body>), preserving tails.
        Collect first then remove in reverse order to avoid iterator issues.
        """
        to_remove = [el for el in root.iter() if etree.QName(el).localname in self.skip]
        for el in reversed(to_remove):
            _preserve_tail_and_remove(el)

    def _has_allowed_ancestor(self, el: etree._Element) -> bool:
        a = el.getparent()
        while a is not None:
            if etree.QName(a).localname in self.allowed:
                return True
            a = a.getparent()
        return False

    def _collect_title_and_abstract(self, tree: etree._ElementTree) -> List[str]:
        """
        Extract title and abstract from common TEI locations.
        Returns a list of blocks: [title, abstract] (if present).
        """
        root = tree.getroot()
        blocks: List[str] = []

        # Title — common TEI locations
        title_nodes = root.xpath(
            ".//tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title | .//tei:titleStmt/tei:title | .//tei:title",
            namespaces=TEI_NS
        )
        if title_nodes:
            for tnode in title_nodes:
                txt = _text_from_itertext(tnode)
                if txt:
                    blocks.append(txt)
                    break

        # Abstract — common locations: tei:abstract or tei:profileDesc/abstract
        abstract_nodes = root.xpath(".//tei:abstract | .//tei:profileDesc/tei:abstract", namespaces=TEI_NS)
        if abstract_nodes:
            for an in abstract_nodes:
                txt = _text_from_itertext(an)
                if txt:
                    blocks.append(txt)
                    break

        return blocks

    def _collect_allowed_texts_from_body(self, body: etree._Element) -> List[str]:
        """
        Collect text from allowed elements inside the body (in document order),
        skipping nested allowed elements to avoid duplication.
        """
        xp_parts = [f"tei:{tag}" for tag in self.allowed if tag not in {"title", "abstract"}]
        xpath_expr = " | ".join(f".//{p}" for p in xp_parts) if xp_parts else ".//*"
        found = body.xpath(xpath_expr, namespaces=TEI_NS)
        blocks: List[str] = []
        for el in found:
            ln = etree.QName(el).localname
            if ln not in self.allowed:
                continue
            if self._has_allowed_ancestor(el):
                continue
            raw = _text_from_itertext(el)
            if raw:
                blocks.append(raw)
        return blocks

    def _collect_unwrapped_text_blocks(self, body: etree._Element) -> List[str]:
        """
        Fallback: iterate body nodes and collect elements that:
          - have meaningful text (text or tail)
          - are NOT inside allowed ancestors
          - are not in skip set
        This is safer than a broad XPath and reduces duplication.
        """
        blocks: List[str] = []
        for el in body.iter():
            ln = etree.QName(el).localname
            if ln in self.skip:
                continue
            # skip elements that are inside an explicitly allowed element (we don't want duplication)
            if self._has_allowed_ancestor(el):
                continue
            # gather element text and tail if any
            candidate = _text_from_itertext(el)
            if not candidate:
                continue
            # avoid capturing the whole body root again
            if ln == "body":
                continue
            blocks.append(candidate)
        return blocks

    def extract_from_tree(self, tree: etree._ElementTree, remove_citations: bool = False) -> str:
        root = tree.getroot()

        # 1) Remove skip elements globally (header + body), preserve tails safely
        self._remove_skip_elements_in_tree(root)

        # 2) Extract title & abstract (from header/common locations)
        front_blocks = self._collect_title_and_abstract(tree)

        # 3) Extract body
        body = tree.find(".//tei:text/tei:body", namespaces=TEI_NS)
        body_blocks: List[str] = []
        if body is not None:
            body_blocks = self._collect_allowed_texts_from_body(body)
            # fallback stray blocks (only add non-duplicates)
            fallback = self._collect_unwrapped_text_blocks(body)
            for fb in fallback:
                if not any(_normalize_whitespace(fb).casefold() == _normalize_whitespace(bb).casefold() or
                           _normalize_whitespace(fb).casefold() in _normalize_whitespace(bb).casefold()
                           for bb in body_blocks):
                    body_blocks.append(fb)

        # 4) Avoid duplication: if title/abstract already present in body_blocks, do not duplicate
        final_blocks: List[str] = []
        for fb in front_blocks:
            norm_fb = _normalize_whitespace(fb).casefold()
            if not any(norm_fb == _normalize_whitespace(bb).casefold() or norm_fb in _normalize_whitespace(bb).casefold()
                       for bb in body_blocks):
                final_blocks.append(fb)

        final_blocks.extend(body_blocks)

        # 5) optional simple citation marker removal
        joined = "\n\n".join(final_blocks)
        if remove_citations:
            joined = _BRACKET_CITATION_RE.sub(" ", joined)
            joined = _PARENTHESES_AUTHOR_YEAR_RE.sub(" ", joined)
            joined = _normalize_whitespace(joined)

        return joined

    def extract_from_file(self, path: Union[str, pathlib.Path], remove_citations: bool = False) -> str:
        tree = self.parse(path)
        return self.extract_from_tree(tree, remove_citations=remove_citations)


# ---------------- Batch-processing helpers ---------------- #

def _gather_input_files(inputs: Iterable[str], recursive: bool = False) -> List[pathlib.Path]:
    """
    Resolve input paths (files, globs, directories) to a deduplicated list of file paths.
    Case-insensitive for common extensions.
    """
    patterns = ["*.grobid.tei.xml", "*.tei.xml", "*.xml"]
    out = []
    for p in inputs:
        ppath = pathlib.Path(p)
        if ppath.is_file():
            out.append(ppath.resolve())
            continue
        if any(ch in p for ch in ["*", "?", "["]):
            for m in pathlib.Path(".").glob(p):
                if m.is_file():
                    out.append(m.resolve())
            continue
        if ppath.is_dir():
            if recursive:
                for root, dirs, files in os.walk(ppath):
                    for fname in files:
                        lfn = fname.lower()
                        for pat in patterns:
                            if fnmatch.fnmatch(lfn, pat):
                                out.append(pathlib.Path(root) / fname)
                                break
            else:
                for pat in patterns:
                    for m in ppath.glob(pat):
                        if m.is_file():
                            out.append(m.resolve())
            continue
        # fallback glob in cwd
        for m in pathlib.Path(".").glob(p):
            if m.is_file():
                out.append(m.resolve())

    # dedupe preserving order
    seen = set()
    deduped = []
    for f in out:
        if f not in seen:
            seen.add(f)
            deduped.append(f)
    return deduped


def _output_path_for_input(input_path: pathlib.Path, outdir: Optional[pathlib.Path]) -> pathlib.Path:
    """
    Derive output filename from input name:
      paper.grobid.tei.xml -> paper.txt
    """
    name = input_path.name
    for suffix in [".grobid.tei.xml", ".tei.xml", ".xml"]:
        if name.lower().endswith(suffix):
            base = name[:-len(suffix)]
            break
    else:
        base = input_path.stem
    odir = outdir if outdir is not None else input_path.parent
    odir.mkdir(parents=True, exist_ok=True)
    return odir / (base + ".txt")


def write_to_file(text: str, path: pathlib.Path) -> None:
    path.write_text(text, encoding="utf-8")


def _parse_args(argv: Optional[List[str]] = None):
    ap = argparse.ArgumentParser(description="Batch GROBID TEI -> clean TXT extractor (title+abstract included)")
    ap.add_argument("inputs", nargs="+", help="Input file(s), globs, and/or directories")
    ap.add_argument("-o", "--outdir", type=pathlib.Path, help="Write outputs to this directory (default: each file's directory)")
    ap.add_argument("--recursive", action="store_true", help="When input is a directory, search recursively for TEI-like files")
    ap.add_argument("--debug", action="store_true", help="Enable debug logging")
    ap.add_argument("--include-captions", action="store_true", help="Do not strip figure/table captions (default: captions removed)")
    ap.add_argument("--keep-references", action="store_true", help="Do not strip references/bibliography (default: references removed)")
    ap.add_argument("--remove-citations", action="store_true", help="Apply conservative regex removal of bracket and author-year citations")
    ap.add_argument("--log-dir", default=None, help="Directory for log output (defaults to ./logs).")
    ap.add_argument("--log-file-name", default="parse_papers.log", help="Log file name (default: parse_papers.log).")
    return ap.parse_args(argv)


def main(argv: Optional[List[str]] = None):
    args = _parse_args(argv)
    base_dir = Path(__file__).resolve().parent
    log_dir = resolve_log_dir(base_dir, args.log_dir)
    log_path = log_dir / args.log_file_name

    # Route log output to the standard logs/ directory while still echoing to console.
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if args.debug else logging.INFO)
    root.handlers.clear()
    formatter = logging.Formatter("%(levelname)s: %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    skip = set(DEFAULT_SKIP)
    if args.include_captions:
        # don't skip figure/table elements if user asks to include captions
        for t in ("figure", "table", "figDesc", "figureDesc", "figDesc"):
            skip.discard(t)
    if args.keep_references:
        # don't skip bibl-related containers
        for t in ("biblStruct", "listBibl", "bibl", "list"):
            skip.discard(t)

    extractor = GrobidBodyExtractor(skip_tags=skip)

    file_list = _gather_input_files(args.inputs, recursive=args.recursive)
    if not file_list:
        logging.error("No input files found (check paths/globs).")
        sys.exit(2)

    logging.info("Found %d file(s) to process.", len(file_list))
    for fp in file_list:
        try:
            logging.info("Processing %s", fp)
            cleaned = extractor.extract_from_file(fp, remove_citations=args.remove_citations)
            out_path = _output_path_for_input(fp, args.outdir)
            write_to_file(cleaned, out_path)
            logging.info("Wrote: %s", out_path)
        except Exception as exc:
            logging.exception("Failed to process %s: %s", fp, exc)


if __name__ == "__main__":
    main()
