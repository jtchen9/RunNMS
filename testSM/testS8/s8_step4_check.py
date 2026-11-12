"""Offline smoke test for both installed heading tables. No NMS or robot required."""
from pathlib import Path
import contextlib
import io
import json
import math
import shutil
import sys
import tempfile
import unittest

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
from check_preferred_heading import check_preferred_heading


class HeadingTables(unittest.TestCase):
    folder = ROOT / "sitemap" / "DemoRoom" / "preferred_direction_lut"

    def lookup(self, x, y, folder=None):
        with contextlib.redirect_stdout(io.StringIO()):
            return check_preferred_heading((folder or self.folder) / "preferred_heading_deg.npy", x, y)

    def test_every_available_alternate(self):
        h = np.load(self.folder / "preferred_heading_deg.npy")
        with np.load(self.folder / "alternative_heading_v1.npz", allow_pickle=False) as data:
            alt, pv, av = (data[k] for k in ("heading", "primary_view", "alternative_view"))
            views = [set(front + rear) for front, rear in json.loads(data["views"].item())]
            meta = json.loads(data["metadata"].item())
        self.assertEqual(alt.shape, h.shape)
        good = np.isfinite(alt)
        self.assertTrue(good.any(), "No alternate heading anywhere on the installed map")
        self.assertTrue(np.isfinite(h[good]).all())
        turns = np.abs((alt[good] - h[good] + 180) % 360 - 180)
        self.assertTrue((turns >= meta["min_turn_deg"]).all())
        for p, a in set(zip(pv[good].tolist(), av[good].tolist())):
            self.assertGreaterEqual(len(views[a] - views[p]), meta["min_new_ids"])

    def test_lookup_on_grid_and_missing_cells(self):
        xs, ys, h = (np.load(self.folder / n) for n in
                     ("axis_x_m.npy", "axis_y_m.npy", "preferred_heading_deg.npy"))
        with np.load(self.folder / "alternative_heading_v1.npz") as data:
            alt = data["heading"]
        for mask, status in ((np.isfinite(alt), "ok"), (~np.isfinite(h), "no_primary"),
                             (np.isfinite(h) & ~np.isfinite(alt), "no_alternative")):
            indices = np.argwhere(mask)
            if indices.size:
                iy, ix = indices[0]
                r = self.lookup(xs[ix], ys[iy])
                self.assertEqual((r["grid_x_index"], r["grid_y_index"], r["status"]), (ix, iy, status))
                if status == "ok":
                    self.assertEqual(r["preferred_heading_deg"], h[iy, ix])
                    self.assertEqual(r["alternative_heading_deg"], alt[iy, ix])
        with self.assertRaises(ValueError):
            self.lookup(-1, -1)

    def test_old_install_and_mismatched_companion(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            for name in ("axis_x_m.npy", "axis_y_m.npy", "preferred_heading_deg.npy", "preferred_direction_meta.json"):
                shutil.copyfile(self.folder / name, dest / name)
            r = self.lookup(4, 3.1, dest)
            self.assertEqual(r["status"], "companion_missing")
            self.assertTrue(math.isnan(r["alternative_heading_deg"]))
            name = "alternative_heading_v1.npz"
            shutil.copyfile(self.folder / name, dest / name)
            with (dest / "preferred_direction_meta.json").open("ab") as stream:
                stream.write(b"\n")
            with self.assertRaisesRegex(ValueError, "does not match"):
                self.lookup(4, 3.1, dest)


if __name__ == "__main__":
    unittest.main(verbosity=2)
