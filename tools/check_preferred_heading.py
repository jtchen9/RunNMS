#!/usr/bin/env python3
"""Compare the primary and recovery headings at the same nearest LUT cell.

Run in VS Code after editing the settings at the bottom, or import
check_preferred_heading(npy_path, x_m, y_m). All tag IDs are predictions.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np


def _nearest_index(axis, value, name):
    if axis.ndim != 1 or not axis.size or not np.isfinite(axis).all() or not (np.diff(axis) > 0).all():
        raise ValueError(f"Invalid {name} axis: expected finite, strictly increasing values")
    if not math.isfinite(value) or not float(axis[0])-1e-6 <= value <= float(axis[-1])+1e-6:
        raise ValueError(f"{name}={value} outside LUT range [{axis[0]:.3f}, {axis[-1]:.3f}]")
    return int(np.abs(axis.astype(float) - value).argmin())


def _read_alternative(path, primary, shape, iy, ix, x, y):
    with np.load(path, allow_pickle=False) as data:
        meta = json.loads(data["metadata"].item())
        if meta["version"] != 1:
            raise ValueError("Unsupported alternative LUT format")
        names = ["preferred_heading_deg.npy", "axis_x_m.npy", "axis_y_m.npy", "preferred_direction_meta.json"]
        for name in names:
            source = primary if name == names[0] else primary.parent / name
            if not source.is_file() or hashlib.sha256(source.read_bytes()).hexdigest() != meta["primary_files"].get(name):
                raise ValueError(f"Alternative LUT does not match {name}; rebuild it for this primary LUT.")
        views = json.loads(data["views"].item())
        arrays = [data[k] for k in ("heading", "primary_view", "alternative_view")]
        if any(a.shape != shape for a in arrays):
            raise ValueError("Alternative LUT shape differs from the primary grid")
        view_ids = [int(a[iy, ix]) for a in arrays[1:]]
        if any(not 0 <= i < len(views) for i in view_ids):
            raise ValueError("Invalid camera view index in alternative LUT")
        excluded = sorted({tag for z in meta["zones"] if
                           z["x"][0]-1e-6 <= x <= z["x"][1]+1e-6 and
                           z["y"][0]-1e-6 <= y <= z["y"][1]+1e-6 for tag in z["tags"]})
        predictions = [dict(zip(("front", "rear"), views[i], strict=True)) for i in view_ids]
        return float(arrays[0][iy, ix]), *predictions, excluded


def check_preferred_heading(npy_path: str | Path, x_m: float, y_m: float,
                            alternative_path: str | Path | None = None) -> dict:
    primary = Path(npy_path)
    path = primary.with_name("alternative_heading_v1.npz") if alternative_path is None else Path(alternative_path)
    heading = np.load(primary, allow_pickle=False)
    xs, ys = [np.load(primary.parent / f"axis_{d}_m.npy", allow_pickle=False) for d in ("x", "y")]
    x, y = float(x_m), float(y_m)
    ix, iy = _nearest_index(xs, x, "x_m"), _nearest_index(ys, y, "y_m")
    if heading.shape != (len(ys), len(xs)):
        raise ValueError("Primary LUT shape does not match the supplied axes")
    gx, gy = float(xs[ix]), float(ys[iy])
    h = float(heading[iy, ix])
    alt, turn, pv, av, excluded, novel, shared = math.nan, None, None, None, None, None, None
    status = "companion_missing"
    if path.is_file():
        alt, pv, av, excluded = _read_alternative(path, primary, heading.shape, iy, ix, gx, gy)
        if not math.isfinite(h):
            if math.isfinite(alt):
                raise ValueError("Alternative exists at a cell without a primary heading")
            status = "no_primary"
        elif not math.isfinite(alt):
            status = "no_alternative"
        else:
            status = "ok"
            turn = (alt - h + 180.0) % 360.0 - 180.0
            p, a = (set(v["front"]) | set(v["rear"]) for v in (pv, av))
            novel, shared = sorted(a-p), sorted(a&p)
    elif alternative_path is not None:
        raise FileNotFoundError(f"Requested alternative LUT not found: {path}")

    print(f"Requested ({x:.3f}, {y:.3f}) m -> grid ({gx:.3f}, {gy:.3f}) m [ix={ix}, iy={iy}]")
    for label, value, view in (("Primary", h, pv), ("Alternative", alt, av)):
        angle = f"{value % 360:.1f} deg" if math.isfinite(value) else "unavailable"
        tags = f"  predicted tags: front={view['front']} rear={view['rear']}" if view is not None and math.isfinite(value) else ""
        print(f"{label:11s}: {angle}{tags}")
    if status == "ok":
        print(f"Signed heading change: {turn:+.1f} deg; new tag IDs: {novel}; shared: {shared}")
    elif status == "companion_missing":
        print(f"Alternative file missing: {path}. Run build_alternative_heading_lut.py once.")
    elif status == "no_primary":
        print("No primary heading at this cell.")
    else:
        print("No alternate view met the configured criteria at this cell.")
    if excluded:
        print(f"Locally excluded tag IDs: {excluded}")
    return dict(requested_x_m=x, requested_y_m=y, grid_x_index=ix, grid_y_index=iy,
                grid_x_m=gx, grid_y_m=gy, preferred_heading_deg=h, alternative_heading_deg=alt,
                status=status, turn_deg=turn, primary_tags=pv, alternative_tags=av,
                new_tag_ids=novel, shared_tag_ids=shared, excluded_tag_ids=excluded)


if __name__ == "__main__":
    # Set the query here and click Run Python File.
    LUT_DIR = Path(__file__).resolve().parents[1] / "sitemap" / "DemoRoom" / "preferred_direction_lut"
    NPY_PATH = LUT_DIR / "preferred_heading_deg.npy"
    ALTERNATIVE_PATH = None  # Auto-detect alternative_heading_v1.npz beside NPY_PATH.
    X_M, Y_M = 4.0, 3.1
    check_preferred_heading(NPY_PATH, X_M, Y_M, ALTERNATIVE_PATH)
