#!/usr/bin/env python3
"""
Build restriction_map.npy from a human-edited bottom-first restriction text file.

Input format supported:
    # restriction_map shape=(114, 114)
    # BOTTOM-FIRST VIEW: printed from matrix row rows-1 down to 0
    # char legend: . = free, # = blocked
    #
    display_y=000 matrix_row=113 ....#####....
    display_y=001 matrix_row=112 ....#####....

Meaning:
    . = free cell  -> 0
    # = blocked    -> 1

Important:
    The text is bottom-first for human readability, but the .npy file must be
    saved in matrix-row order. Therefore, each line is written back into
    mat[matrix_row, :]. If matrix_row labels are missing, the script assumes
    the first data row corresponds to matrix row rows-1 and counts downward.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np


SHAPE_RE = re.compile(r"shape\s*=\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)")
BOTTOM_FIRST_RE = re.compile(
    r"^display_y\s*=\s*(\d+)\s+matrix_row\s*=\s*(\d+)\s+([.#01]+)\s*$"
)
TOP_STYLE_RE = re.compile(r"^(\d+)\s+([.#01]+)\s*$")


def _parse_shape_from_header(lines: List[str]) -> Tuple[int, int] | None:
    for line in lines:
        m = SHAPE_RE.search(line)
        if m:
            return int(m.group(1)), int(m.group(2))
    return None


def _chars_to_row(s: str, *, expected_cols: int, line_no: int) -> np.ndarray:
    if len(s) != expected_cols:
        raise ValueError(
            f"line {line_no}: row length {len(s)} does not match expected cols {expected_cols}"
        )

    out = np.zeros((expected_cols,), dtype=np.uint8)
    for i, ch in enumerate(s):
        if ch in {".", "0"}:
            out[i] = 0
        elif ch in {"#", "1"}:
            out[i] = 1
        else:
            raise ValueError(f"line {line_no}: unsupported character {ch!r}")
    return out


def load_bottom_first_txt(path: Path, *, expected_shape: Tuple[int, int] | None = None) -> np.ndarray:
    """
    Load a bottom-first human-readable restriction map text file.

    Preferred format uses explicit matrix_row labels:
        display_y=000 matrix_row=113 ....####....

    Fallback format:
        If labels are absent, data rows are assumed to be bottom-first:
            first data row -> matrix row rows-1
            last data row  -> matrix row 0
    """
    lines = path.read_text(encoding="utf-8").splitlines()

    header_shape = _parse_shape_from_header(lines)
    shape = expected_shape or header_shape

    parsed: Dict[int, Tuple[int, str, int]] = {}
    unlabeled_rows: List[Tuple[str, int]] = []

    for line_no, raw in enumerate(lines, start=1):
        line = raw.rstrip("\n")
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        m = BOTTOM_FIRST_RE.match(stripped)
        if m:
            display_y = int(m.group(1))
            matrix_row = int(m.group(2))
            row_text = m.group(3)

            if matrix_row in parsed:
                raise ValueError(f"line {line_no}: duplicate matrix_row={matrix_row}")

            parsed[matrix_row] = (display_y, row_text, line_no)
            continue

        m = TOP_STYLE_RE.match(stripped)
        if m:
            # Support raw "000 ...." rows as a convenience. In bottom-first
            # files without matrix_row= labels, the numeric prefix is treated
            # as display_y rather than matrix_row.
            row_text = m.group(2)
            unlabeled_rows.append((row_text, line_no))
            continue

        # Also support bare rows such as "....####....".
        if set(stripped).issubset({".", "#", "0", "1"}):
            unlabeled_rows.append((stripped, line_no))
            continue

        raise ValueError(f"line {line_no}: cannot parse row: {raw}")

    if parsed and unlabeled_rows:
        raise ValueError("mixed labeled and unlabeled data rows are not allowed")

    if parsed:
        if shape is None:
            rows = max(parsed.keys()) + 1
            cols = len(next(iter(parsed.values()))[1])
            shape = (rows, cols)

        rows, cols = shape
        mat = np.zeros((rows, cols), dtype=np.uint8)
        seen = set()

        for matrix_row, (_display_y, row_text, line_no) in parsed.items():
            if not (0 <= matrix_row < rows):
                raise ValueError(
                    f"line {line_no}: matrix_row={matrix_row} outside valid range 0..{rows - 1}"
                )
            mat[matrix_row, :] = _chars_to_row(row_text, expected_cols=cols, line_no=line_no)
            seen.add(matrix_row)

        missing = sorted(set(range(rows)) - seen)
        if missing:
            preview = missing[:10]
            raise ValueError(
                f"missing {len(missing)} matrix rows, first missing rows={preview}"
            )

        return mat

    if not unlabeled_rows:
        raise ValueError(f"no map rows found in {path}")

    if shape is None:
        rows = len(unlabeled_rows)
        cols = len(unlabeled_rows[0][0])
        shape = (rows, cols)

    rows, cols = shape
    if len(unlabeled_rows) != rows:
        raise ValueError(
            f"found {len(unlabeled_rows)} data rows but expected {rows}"
        )

    mat = np.zeros((rows, cols), dtype=np.uint8)
    for display_y, (row_text, line_no) in enumerate(unlabeled_rows):
        matrix_row = rows - 1 - display_y
        mat[matrix_row, :] = _chars_to_row(row_text, expected_cols=cols, line_no=line_no)

    return mat


def dump_bottom_first_txt(mat: np.ndarray, path: Path) -> None:
    """Write a bottom-first text file so the generated .npy can be visually checked."""
    rows, cols = mat.shape
    with path.open("w", encoding="utf-8") as f:
        f.write(f"# restriction_map shape={mat.shape}\n")
        f.write("# BOTTOM-FIRST VIEW: printed from matrix row rows-1 down to 0\n")
        f.write("# char legend: . = free, # = blocked\n")
        f.write("#\n")
        for display_y, matrix_row in enumerate(range(rows - 1, -1, -1)):
            line = "".join("#" if int(mat[matrix_row, c]) else "." for c in range(cols))
            f.write(f"display_y={display_y:03d} matrix_row={matrix_row:03d} {line}\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        default="restriction_matrix_bottom_first.txt",
        help="Human-edited bottom-first text file",
    )
    ap.add_argument(
        "--output",
        default="restriction_map.npy",
        help="Output .npy file",
    )
    ap.add_argument(
        "--verify-txt",
        default="restriction_matrix_bottom_first_regenerated_check.txt",
        help="Optional regenerated bottom-first text for visual checking",
    )
    ap.add_argument(
        "--compare-old-npy",
        default="",
        help="Optional old restriction_map.npy to compare against",
    )
    args = ap.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    verify_path = Path(args.verify_txt) if args.verify_txt else None

    mat = load_bottom_first_txt(input_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    np.save(str(output_path), mat.astype(np.uint8))

    summary = {
        "input": str(input_path),
        "output": str(output_path),
        "shape": [int(mat.shape[0]), int(mat.shape[1])],
        "dtype": str(mat.dtype),
        "blocked_cells": int(np.count_nonzero(mat)),
        "free_cells": int(mat.size - np.count_nonzero(mat)),
    }

    if verify_path:
        verify_path.parent.mkdir(parents=True, exist_ok=True)
        dump_bottom_first_txt(mat, verify_path)
        summary["verify_txt"] = str(verify_path)

    if args.compare_old_npy:
        old_path = Path(args.compare_old_npy)
        old = np.load(str(old_path), allow_pickle=False)
        if old.shape != mat.shape:
            summary["compare_old_npy"] = {
                "path": str(old_path),
                "same_shape": False,
                "old_shape": [int(old.shape[0]), int(old.shape[1])],
            }
        else:
            diff = old.astype(np.uint8) != mat.astype(np.uint8)
            changed = int(np.count_nonzero(diff))
            summary["compare_old_npy"] = {
                "path": str(old_path),
                "same_shape": True,
                "changed_cells": changed,
                "old_blocked_cells": int(np.count_nonzero(old)),
                "new_blocked_cells": int(np.count_nonzero(mat)),
            }

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
