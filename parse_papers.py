#!/usr/bin/env python3
"""Compatibility CLI/module alias for GROBID TEI parsing."""
import sys

from slr_meta.parsing import grobid as _grobid

sys.modules[__name__] = _grobid

if __name__ == "__main__":
    _grobid.main()
