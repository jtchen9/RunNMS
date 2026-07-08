from __future__ import annotations

"""
Read-only online preferred-direction LUT helper.

No Redis.
No state transitions.
No command issuance.
No collision-map access.

Default LUT:
    sitemap/DemoRoom/preferred_direction_lut/
"""

import json
import math
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np


ROOT_DIR = Path(__file__).resolve().parents[1]

DEFAULT_LUT_DIR = (
    ROOT_DIR
    / "sitemap"
    / "DemoRoom"
    / "preferred_direction_lut"
)


class PreferredDirectionLookup:
    def __init__(
        self,
        lut_dir: Optional[Path | str] = None,
    ) -> None:
        self.lut_dir = Path(
            lut_dir or DEFAULT_LUT_DIR
        )

        self._loaded = False

        self._heading: Optional[np.ndarray] = None
        self._score: Optional[np.ndarray] = None
        self._status: Optional[np.ndarray] = None
        self._effective: Optional[np.ndarray] = None
        self._span: Optional[np.ndarray] = None

        self._xs: Optional[np.ndarray] = None
        self._ys: Optional[np.ndarray] = None

        self._meta: Dict[str, Any] = {}

    def _load_once(self) -> None:
        if self._loaded:
            return

        self._heading = np.load(
            self.lut_dir
            / "preferred_heading_deg.npy"
        )

        self._score = np.load(
            self.lut_dir
            / "preferred_heading_score.npy"
        )

        self._status = np.load(
            self.lut_dir
            / "preferred_heading_status_code.npy"
        )

        self._effective = np.load(
            self.lut_dir
            / "preferred_heading_effective_count.npy"
        )

        self._span = np.load(
            self.lut_dir
            / "preferred_heading_geometry_span_deg.npy"
        )

        self._xs = np.load(
            self.lut_dir
            / "axis_x_m.npy"
        )

        self._ys = np.load(
            self.lut_dir
            / "axis_y_m.npy"
        )

        self._meta = json.loads(
            (
                self.lut_dir
                / "preferred_direction_meta.json"
            ).read_text(
                encoding="utf-8"
            )
        )

        expected_shape = tuple(
            self._meta.get(
                "grid_shape_yx"
            )
            or []
        )

        if (
            expected_shape
            and tuple(self._heading.shape)
            != expected_shape
        ):
            raise ValueError(
                "Preferred-direction LUT shape mismatch: "
                f"heading={self._heading.shape}, "
                f"meta={expected_shape}"
            )

        self._loaded = True

    @staticmethod
    def _nearest_index(
        axis: np.ndarray,
        value: float,
    ) -> int:
        if len(axis) == 0:
            raise ValueError(
                "Empty preferred-direction LUT axis"
            )

        return int(
            np.abs(
                axis.astype(float)
                - float(value)
            ).argmin()
        )

    def lookup(
        self,
        x_m: float,
        y_m: float,
    ) -> Dict[str, Any]:
        self._load_once()

        assert self._heading is not None
        assert self._score is not None
        assert self._status is not None
        assert self._effective is not None
        assert self._span is not None
        assert self._xs is not None
        assert self._ys is not None

        x = float(x_m)
        y = float(y_m)

        x_min = float(self._xs[0])
        x_max = float(self._xs[-1])

        y_min = float(self._ys[0])
        y_max = float(self._ys[-1])

        if (
            x < x_min
            or x > x_max
            or y < y_min
            or y > y_max
        ):
            return {
                "ok": False,
                "detail": (
                    "location outside preferred-direction LUT"
                ),
                "x_m": x,
                "y_m": y,
                "table_version": self._meta.get(
                    "table_version",
                    "",
                ),
            }

        ix = self._nearest_index(
            self._xs,
            x,
        )

        iy = self._nearest_index(
            self._ys,
            y,
        )

        heading = float(
            self._heading[iy, ix]
        )

        status_code = int(
            self._status[iy, ix]
        )

        if not math.isfinite(heading):
            return {
                "ok": False,
                "detail": (
                    "LUT cell has no preferred heading"
                ),

                "x_m": x,
                "y_m": y,

                "grid_x_index": ix,
                "grid_y_index": iy,

                "grid_x_m": float(
                    self._xs[ix]
                ),
                "grid_y_m": float(
                    self._ys[iy]
                ),

                "status_code": status_code,

                "table_version": self._meta.get(
                    "table_version",
                    "",
                ),
            }

        return {
            "ok": True,

            "preferred_heading_deg":
                heading % 360.0,

            "x_m": x,
            "y_m": y,

            "grid_x_index": ix,
            "grid_y_index": iy,

            "grid_x_m": float(
                self._xs[ix]
            ),

            "grid_y_m": float(
                self._ys[iy]
            ),

            "status_code": status_code,

            "score": float(
                self._score[iy, ix]
            ),

            "effective_good_count": float(
                self._effective[iy, ix]
            ),

            "geometry_span_deg": float(
                self._span[iy, ix]
            ),

            "table_version": self._meta.get(
                "table_version",
                "",
            ),

            "tag_map_sha256": self._meta.get(
                "tag_map_sha256",
                "",
            ),

            "detail": (
                "preferred direction lookup ok"
            ),
        }


_default_lookup = PreferredDirectionLookup()


def lookup_preferred_direction(
    x_m: float,
    y_m: float,
) -> Dict[str, Any]:
    """
    Tiny state-machine-facing API.

    Intended use:
        result = lookup_preferred_direction(
            x_m=target_x,
            y_m=target_y,
        )
    """
    return _default_lookup.lookup(
        x_m=x_m,
        y_m=y_m,
    )
