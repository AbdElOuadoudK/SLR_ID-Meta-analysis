"""Compatibility module alias for workbook template building."""
import sys

from slr_meta.sheets import builder as _builder

sys.modules[__name__] = _builder
