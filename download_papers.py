#!/usr/bin/env python3
"""Compatibility CLI/module alias for PDF downloading."""
import sys

from slr_meta.downloads import papers as _papers

sys.modules[__name__] = _papers

if __name__ == "__main__":
    _papers.main()
