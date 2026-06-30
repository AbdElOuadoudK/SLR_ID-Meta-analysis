"""Compatibility module alias for slr_meta.extraction.utils."""
import sys

from slr_meta.extraction import utils as _utils

sys.modules[__name__] = _utils
