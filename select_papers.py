#!/usr/bin/env python3
"""Compatibility CLI/module alias for paper selection and enrichment."""
import sys

from slr_meta.extraction import workflow as _workflow

sys.modules[__name__] = _workflow

if __name__ == "__main__":
    _workflow.main()
