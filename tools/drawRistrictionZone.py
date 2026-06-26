#!/usr/bin/env python3
import csv
import json
from pathlib import Path

import numpy as np
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from m8mobility_map import _load_site_config, _load_static_map


OUT_DIR = Path("tools\debug_restriction_dump")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def dump_txt(mat: np.ndarray, path: Path) -> None:
    """
    Dump as plain text.
    Each row is one matrix row.
    0 = free, 1 = blocked.
    Row 0 is printed first.
    """
    with path.open("w", encoding="utf-8") as f:
        rows, cols = mat.shape
        f.write(f"# restriction_map shape={mat.shape}\n")
        f.write("# IMPORTANT: first printed row is matrix row 0\n")
        f.write("# char legend: . = free, # = blocked\n")
        f.write("#\n")

        for r in range(rows):
            line = "".join("#" if int(mat[r, c]) else "." for c in range(cols))
            f.write(f"{r:03d} {line}\n")


def dump_txt_bottom_first(mat: np.ndarray, path: Path) -> None:
    """
    Dump with matrix rows reversed.
    This view treats the bottom of the real room as the first displayed row.
    If this version matches the real layout, then the matrix is image-style top-origin.
    """
    with path.open("w", encoding="utf-8") as f:
        rows, cols = mat.shape
        f.write(f"# restriction_map shape={mat.shape}\n")
        f.write("# BOTTOM-FIRST VIEW: printed from matrix row rows-1 down to 0\n")
        f.write("# char legend: . = free, # = blocked\n")
        f.write("#\n")

        for display_i, r in enumerate(range(rows - 1, -1, -1)):
            line = "".join("#" if int(mat[r, c]) else "." for c in range(cols))
            f.write(f"display_y={display_i:03d} matrix_row={r:03d} {line}\n")


def dump_csv(mat: np.ndarray, path: Path) -> None:
    """
    Dump raw numeric CSV.
    Row 0 is the first CSV row.
    """
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["matrix_row_col_header"] + [f"c{c}" for c in range(mat.shape[1])])
        for r in range(mat.shape[0]):
            writer.writerow([f"r{r}"] + [int(mat[r, c]) for c in range(mat.shape[1])])


def dump_blocked_cells(mat: np.ndarray, site: dict, path: Path) -> None:
    """
    Dump every blocked cell with both coordinate interpretations.

    world_y_if_no_flip:
        row directly maps to y = row * resolution.

    world_y_if_flip:
        row maps to y = (rows - 1 - row) * resolution.
    """
    rows, cols = mat.shape
    res = float(site["grid_resolution_m"])

    blocked = []
    for r in range(rows):
        for c in range(cols):
            if int(mat[r, c]) != 0:
                blocked.append(
                    {
                        "row": r,
                        "col": c,
                        "x_m": round(c * res, 3),
                        "world_y_if_no_flip_m": round(r * res, 3),
                        "world_y_if_flip_m": round((rows - 1 - r) * res, 3),
                        "value": int(mat[r, c]),
                    }
                )

    with path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "site": site,
                "shape": [rows, cols],
                "blocked_count": len(blocked),
                "blocked_cells": blocked,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


def dump_probe_points(mat: np.ndarray, site: dict, path: Path) -> None:
    """
    Dump a quick comparison table for selected rows/cols around the known suspicious area.
    """
    rows, cols = mat.shape
    res = float(site["grid_resolution_m"])

    probes = []

    # Suspicious area from the failure: row 89, col 49.
    for r in range(84, 96):
        for c in range(45, 55):
            if 0 <= r < rows and 0 <= c < cols:
                probes.append(
                    {
                        "row": r,
                        "col": c,
                        "value": int(mat[r, c]),
                        "x_m": round(c * res, 3),
                        "y_if_no_flip_m": round(r * res, 3),
                        "y_if_flip_m": round((rows - 1 - r) * res, 3),
                    }
                )

    # Mirror area: row 20~30, col 49.
    for r in range(18, 31):
        for c in range(45, 55):
            if 0 <= r < rows and 0 <= c < cols:
                probes.append(
                    {
                        "row": r,
                        "col": c,
                        "value": int(mat[r, c]),
                        "x_m": round(c * res, 3),
                        "y_if_no_flip_m": round(r * res, 3),
                        "y_if_flip_m": round((rows - 1 - r) * res, 3),
                    }
                )

    with path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "site": site,
                "shape": [rows, cols],
                "probe_cells": probes,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


def main() -> None:
    site = _load_site_config()
    mat = _load_static_map()

    print("site =", site)
    print("restriction_map shape =", mat.shape)
    print("blocked cells =", int(np.count_nonzero(mat)))

    dump_txt(mat, OUT_DIR / "restriction_matrix_top_first.txt")
    dump_txt_bottom_first(mat, OUT_DIR / "restriction_matrix_bottom_first.txt")
    dump_csv(mat, OUT_DIR / "restriction_matrix_raw.csv")
    dump_blocked_cells(mat, site, OUT_DIR / "blocked_cells_with_coordinates.json")
    dump_probe_points(mat, site, OUT_DIR / "probe_cells_row89_and_mirror.json")

    print()
    print("Wrote:")
    for p in sorted(OUT_DIR.iterdir()):
        print(" ", p)


if __name__ == "__main__":
    main()