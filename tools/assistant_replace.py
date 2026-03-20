#!/usr/bin/env python3
"""
assistant_replace.py

Purpose
-------
Exact-string batch replacement helper for refactor work.

Key rules
---------
- NO regex
- NO wildcard pattern matching
- ONLY exact string replacements from the list below
- Reads app.py from the same folder as this script
- Writes a new file by default, so your original app.py stays untouched

Usage
-----
1) Put this file in the same folder as app.py
2) Run:
       python assistant_replace.py
3) It will generate:
       app_stage1.py
   and a report:
       assistant_replace_report.txt

If later you want to reuse this script, only edit the REPLACEMENTS list
and the INPUT_FILENAME / OUTPUT_FILENAME values.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple


# =============================================================================
# File names (same folder as this script)
# =============================================================================
INPUT_FILENAME = "app.py"
OUTPUT_FILENAME = "app_stage1.py"
REPORT_FILENAME = "assistant_replace_report.txt"


# =============================================================================
# Exact string replacements only
# Order matters.
#
# Notes:
# 1) Longer / more specific strings should come earlier.
# 2) These are plain str.replace() operations, nothing more.
# 3) Keep this list explicit and auditable.
# =============================================================================
REPLACEMENTS: List[Tuple[str, str]] = [
    # ---- import-level changes ----
    ("import redis\n", ""),
    ("import base64\n", "import base64\nimport config\n"),

    # ---- runtime object prefix ----
    ("r.", "config.r."),

    # ---- key helper function calls ----
    ("key_scanner_meta(", "config.key_scanner_meta("),
    ("key_uplink_stream(", "config.key_uplink_stream("),
    ("key_cmd_stream(", "config.key_cmd_stream("),
    ("key_cmdack_stream(", "config.key_cmdack_stream("),
    ("key_ap_uplink_stream(", "config.key_ap_uplink_stream("),

    # ---- constants / config names ----
    ("KEY_WHITELIST_SCANNER_META", "config.KEY_WHITELIST_SCANNER_META"),
    ("WHITELIST_SCHEMA_VERSION", "config.WHITELIST_SCHEMA_VERSION"),
    ("DEFAULT_LLM_WEBLINK", "config.DEFAULT_LLM_WEBLINK"),

    ("KEY_APPLIED_VIDEO_TS", "config.KEY_APPLIED_VIDEO_TS"),
    ("KEY_APPLIED_VIDEO", "config.KEY_APPLIED_VIDEO"),
    ("KEY_INTENT_VIDEO_TS", "config.KEY_INTENT_VIDEO_TS"),
    ("KEY_INTENT_VIDEO", "config.KEY_INTENT_VIDEO"),

    ("KEY_NB_LAST_CMDS_TIME", "config.KEY_NB_LAST_CMDS_TIME"),
    ("KEY_NB_LAST_CMDS_ERR", "config.KEY_NB_LAST_CMDS_ERR"),
    ("KEY_NB_LAST_CMDS", "config.KEY_NB_LAST_CMDS"),
    ("KEY_NB_LAST_STATUS", "config.KEY_NB_LAST_STATUS"),
    ("KEY_NB_LAST_RESULT", "config.KEY_NB_LAST_RESULT"),
    ("KEY_NB_LAST_UPLOAD", "config.KEY_NB_LAST_UPLOAD"),

    ("WEB_NMS_UPLOAD_URL", "config.WEB_NMS_UPLOAD_URL"),
    ("WEB_NMS_STATUS_URL", "config.WEB_NMS_STATUS_URL"),
    ("UPLOAD_BATCH_MAX_BYTES", "config.UPLOAD_BATCH_MAX_BYTES"),
    ("NORTHBOUND_EVERY_SEC", "config.NORTHBOUND_EVERY_SEC"),

    ("BUNDLE_META_SCHEMA_VERSION", "config.BUNDLE_META_SCHEMA_VERSION"),
    ("KEY_BUNDLE_INDEX", "config.KEY_BUNDLE_INDEX"),
    ("BUNDLE_DIR", "config.BUNDLE_DIR"),

    ("AP_UPLINK_MAXLEN", "config.AP_UPLINK_MAXLEN"),
    ("UPLINK_MAXLEN", "config.UPLINK_MAXLEN"),
    ("UPLOAD_ENABLED", "config.UPLOAD_ENABLED"),
    ("CMD_EXPIRE_SEC", "config.CMD_EXPIRE_SEC"),

    ("TIME_FMT", "config.TIME_FMT"),
    ("WEB_API_KEY", "config.WEB_API_KEY"),
    ("NMS_ID", "config.NMS_ID"),

    ("KEY_REGISTRY", "config.KEY_REGISTRY"),
    ("KEY_PREFIX", "config.KEY_PREFIX"),

    # ---- optional if you moved these into config.py later ----
    # Leave commented out for now until you really move them.
    # ("local_ts(", "config.local_ts("),
    # ("parse_local_dt(", "config.parse_local_dt("),
    # ("normalize_mac(", "config.normalize_mac("),
]


# =============================================================================
# Optional exact block removals
#
# Use this only when you are sure the block text matches exactly.
# For now it is left empty on purpose.
#
# Later, you may paste the exact original constants block here as one big string
# and replace it with "" or with a shorter marker block.
# =============================================================================
EXACT_BLOCK_REPLACEMENTS: List[Tuple[str, str]] = [
    # Example:
    # (
    #     "OLD EXACT MULTI-LINE BLOCK TEXT",
    #     ""
    # ),
]


def apply_exact_replacements(text: str, replacements: List[Tuple[str, str]]) -> tuple[str, List[tuple[str, int]]]:
    """
    Apply exact str.replace() operations in order.
    Returns the new text and a report list of (old_text_preview, count).
    """
    report: List[tuple[str, int]] = []

    for old, new in replacements:
        count = text.count(old)
        if count > 0:
            text = text.replace(old, new)
        preview = old.replace("\n", "\\n")
        if len(preview) > 80:
            preview = preview[:77] + "..."
        report.append((preview, count))

    return text, report


def dedupe_import_config(text: str) -> str:
    """
    Clean up duplicated 'import config' lines if replacement was run more than once.
    Exact line handling only.
    """
    lines = text.splitlines(keepends=True)
    seen = False
    out: List[str] = []

    for line in lines:
        if line == "import config\n":
            if seen:
                continue
            seen = True
        out.append(line)

    return "".join(out)


def collapse_double_prefix(text: str) -> str:
    """
    Safety cleanup if the script is accidentally run twice.
    Exact string cleanup only.
    """
    while "config.config." in text:
        text = text.replace("config.config.", "config.")
    return text


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    input_path = base_dir / INPUT_FILENAME
    output_path = base_dir / OUTPUT_FILENAME
    report_path = base_dir / REPORT_FILENAME

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    original_text = input_path.read_text(encoding="utf-8")

    # First pass: exact block replacements
    text, block_report = apply_exact_replacements(original_text, EXACT_BLOCK_REPLACEMENTS)

    # Second pass: exact token/string replacements
    text, replace_report = apply_exact_replacements(text, REPLACEMENTS)

    # Safety cleanup
    text = dedupe_import_config(text)
    text = collapse_double_prefix(text)

    output_path.write_text(text, encoding="utf-8")

    # Write report
    total_hits = sum(c for _, c in block_report) + sum(c for _, c in replace_report)
    lines: List[str] = []
    lines.append(f"Input file : {input_path.name}")
    lines.append(f"Output file: {output_path.name}")
    lines.append(f"Total replacement hits: {total_hits}")
    lines.append("")
    lines.append("=== Exact block replacements ===")
    for preview, count in block_report:
        lines.append(f"{count:6d}  {preview}")
    lines.append("")
    lines.append("=== Exact token/string replacements ===")
    for preview, count in replace_report:
        lines.append(f"{count:6d}  {preview}")
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"Read   : {input_path}")
    print(f"Wrote  : {output_path}")
    print(f"Report : {report_path}")
    print(f"Total replacement hits: {total_hits}")
    print("Original app.py was not modified.")


if __name__ == "__main__":
    main()