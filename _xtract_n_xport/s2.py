"""Compatibility module alias for slr_meta.extraction.s2."""
import sys

from slr_meta.extraction import s2 as _s2

sys.modules[__name__] = _s2
