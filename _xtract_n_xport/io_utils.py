"""Compatibility module alias for slr_meta.extraction.io_utils."""
import sys

from slr_meta.extraction import io_utils as _io_utils

sys.modules[__name__] = _io_utils
