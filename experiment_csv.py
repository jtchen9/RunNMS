"""NMS entry point for the shared, side-effect-free single-CSV parser."""
import sys
from pathlib import Path
_common = str(Path(__file__).resolve().parent / 'sitemap' / 'CommonCheckers')
if _common not in sys.path:
    sys.path.insert(0, _common)
from checker.registration_csv import (COMMAND_COLUMNS, CSV_COLUMNS, SCHEMA_VERSION,
                                      CsvFormatError, parse_registration_csv)
