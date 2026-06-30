#!/usr/bin/env python3
"""Compatibility CLI/module alias for Semantic Scholar collection."""
import sys

from slr_meta.collection import semantic_scholar as _semantic_scholar

sys.modules[__name__] = _semantic_scholar

if __name__ == "__main__":
    _semantic_scholar.main()
