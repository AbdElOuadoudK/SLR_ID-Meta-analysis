"""Compatibility module alias for slr_meta.extraction.export."""
import sys

from slr_meta.extraction import export as _export

sys.modules[__name__] = _export
