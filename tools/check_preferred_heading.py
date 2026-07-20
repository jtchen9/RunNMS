#!/usr/bin/env python3
from __future__ import annotations

"""
Check preferred heading at one (x_m, y_m) location.

Recommended placement:
    D:\\Data\\_Action\\_RunNMS\\tools\\check_preferred_heading.py

Usage in VS Code:
    1. Edit NPY_PATH, X_M, and Y_M under:
           if __name__ == "__main__":
    2. Press "Run Python File"

The script automatically reads axis_x_m.npy and axis_y_m.npy
from the same folder as preferred_heading_deg.npy.

Lookup rule:
    nearest LUT grid point
"""

from pathlib import Path
import math
import numpy as np


def _nearest_index(axis: np.ndarray, value: float) -> int:
    if axis.ndim != 1 or len(axis) == 0:
        raise ValueError("LUT axis must be a non-empty 1D array")

    return int(
        np.abs(axis.astype(float) - float(value)).argmin()
    )


def check_preferred_heading(
    npy_path: str | Path,
    x_m: float,
    y_m: float,
) -> dict:
    heading_path = Path(npy_path)

    if not heading_path.exists():
        raise FileNotFoundError(
            f"Preferred-heading NPY file not found:\n  {heading_path}"
        )

    lut_dir = heading_path.parent
    axis_x_path = lut_dir / "axis_x_m.npy"
    axis_y_path = lut_dir / "axis_y_m.npy"

    if not axis_x_path.exists():
        raise FileNotFoundError(
            f"Missing x-axis file:\n  {axis_x_path}"
        )

    if not axis_y_path.exists():
        raise FileNotFoundError(
            f"Missing y-axis file:\n  {axis_y_path}"
        )

    heading = np.load(heading_path)
    xs = np.load(axis_x_path)
    ys = np.load(axis_y_path)

    if heading.ndim != 2:
        raise ValueError(
            f"Expected 2D LUT, got shape={heading.shape}"
        )

    expected_shape = (len(ys), len(xs))
    if heading.shape != expected_shape:
        raise ValueError(
            "LUT shape does not match axes:\n"
            f"  heading.shape = {heading.shape}\n"
            f"  expected      = {expected_shape}"
        )

    x = float(x_m)
    y = float(y_m)

    x_min = float(xs[0])
    x_max = float(xs[-1])
    y_min = float(ys[0])
    y_max = float(ys[-1])

    if not (x_min <= x <= x_max):
        raise ValueError(
            f"x_m={x:.3f} outside LUT range "
            f"[{x_min:.3f}, {x_max:.3f}]"
        )

    if not (y_min <= y <= y_max):
        raise ValueError(
            f"y_m={y:.3f} outside LUT range "
            f"[{y_min:.3f}, {y_max:.3f}]"
        )

    ix = _nearest_index(xs, x)
    iy = _nearest_index(ys, y)

    grid_x = float(xs[ix])
    grid_y = float(ys[iy])
    preferred_heading = float(heading[iy, ix])

    print()
    print("=" * 72)
    print("PREFERRED HEADING LOOKUP")
    print("=" * 72)
    print(f"NPY file              : {heading_path}")
    print(f"Requested location    : ({x:.3f}, {y:.3f}) m")
    print(f"Nearest LUT location  : ({grid_x:.3f}, {grid_y:.3f}) m")
    print(f"LUT indices           : ix={ix}, iy={iy}")

    if math.isfinite(preferred_heading):
        print(
            f"Preferred heading     : "
            f"{preferred_heading % 360.0:.3f} deg"
        )
    else:
        print(
            "Preferred heading     : NaN "
            "(no valid heading at this LUT cell)"
        )

    print("=" * 72)
    print()

    return {
        "requested_x_m": x,
        "requested_y_m": y,
        "grid_x_index": ix,
        "grid_y_index": iy,
        "grid_x_m": grid_x,
        "grid_y_m": grid_y,
        "preferred_heading_deg": preferred_heading,
    }


if __name__ == "__main__":
    # ==============================================================
    # EDIT THESE THREE VALUES, THEN PRESS "RUN PYTHON FILE" IN VS CODE
    # ==============================================================

    NPY_PATH = (
        r"D:\Data\_Action\_RunNMS"
        r"\sitemap\DemoRoom"
        r"\preferred_direction_lut"
        r"\preferred_heading_deg.npy"
    )

    X_M = 5.0
    Y_M = 1.0

    # ==============================================================

    check_preferred_heading(
        npy_path=NPY_PATH,
        x_m=X_M,
        y_m=Y_M,
    )
