"""Ideal-pose DemoRoom planning only. Never import this module into the runtime gate."""
from fractions import Fraction
import hashlib
import json
from math import floor, ceil
from pathlib import Path

# Bind preflight to the reviewed geometry. Hash canonical JSON so whitespace and
# line endings in a Windows checkout do not change the identity of the document.
APPROVED_GEOMETRY = 'd36b382c1f52c1bd7b30d3c44c3a50d01ec8874ae7d8cde505755cb50df4eabe'


def load_layers(site_dir, macro_policy, cfg):
    site = Path(site_dir)
    spec = json.loads((site / 'bump_layers.json').read_text(encoding='utf-8-sig'))
    canonical = json.dumps(spec, separators=(',', ':'), sort_keys=True).encode()
    if hashlib.sha256(canonical).hexdigest() != APPROVED_GEOMETRY:
        raise ValueError('bump_layers.json differs from the approved DemoRoom geometry')
    if (cfg.get('resolution_m'), cfg.get('origin_x_m', 0), cfg.get('origin_y_m', 0),
            cfg.get('restricted_value', 1), cfg.get('out_of_map_is_restricted', True)) != (.1, 0, 0, 1, True):
        raise ValueError('Expected the approved 10 cm planning grid at origin (0,0)')
    if macro_policy.get('ramp_geometry', {}).get('restriction_zone') != spec['runtime_continuous_policy']:
        raise ValueError('macro_policy.json and bump_layers.json disagree about the central zone')
    if hashlib.sha256((site / cfg['relative_path']).read_bytes()).hexdigest() != spec['sha256_planning_static']:
        raise ValueError('Unexpected planning NPY: use the file paired with this bump geometry')
    c0, _, c1, _ = spec['runtime_rectangle_cells_xyxy']
    ymin, ymax = spec['planning_closed_y_m']
    return Fraction(c0, 10), Fraction(c1, 10), Fraction(str(ymin)), Fraction(str(ymax))


def _grid_units(m):
    # Match entrance policy precision; also remove sub-nanometre macro trig noise.
    return Fraction(round(float(m) * 1_000_000_000), 100_000_000)


def walk(x0, y0, x1, y1, shape=(114, 114)):
    """Visit all occupied half-open grid cells, including exact endpoint/corner hits."""
    x, y, u, v = map(_grid_units, (x0, y0, x1, y1))
    dx, dy = u-x, v-y
    times = {Fraction(0), Fraction(1)}
    for a, b, n in ((x, u, shape[1]), (y, v, shape[0])):
        if a != b:
            for k in range(max(0, ceil(min(a,b))), min(n, floor(max(a,b))) + 1):
                times.add((k-a)/(b-a))
    times = sorted(times)
    previous = None
    for i, t in enumerate(times):
        for s in ([t, (t+times[i+1])/2] if i+1 < len(times) else [t]):
            px, py = x+s*dx, y+s*dy
            cell = floor(px), floor(py)
            if cell != previous:
                yield cell[0], cell[1], float(px/10), float(py/10)
                previous = cell


def cell_status(data, cfg, x, y):
    return _status(data, floor(_grid_units(x)), floor(_grid_units(y)))


def _status(data, col, row):
    outside = not (0 <= row < data.shape[0] and 0 <= col < data.shape[1])
    value = None if outside else int(data[row, col])
    return dict(restricted=outside or value == 1, out_of_map=outside, value=value, row=row, col=col)


def static_hit(data, cfg, x0, y0, x1, y1):
    for col, row, x, y in walk(x0, y0, x1, y1, data.shape):
        hit = _status(data, col, row)
        if hit['restricted']:
            return dict(hit, x_m=x, y_m=y)
    return None


def bump_violation(bounds, start, end):
    """Continuous planning rectangle: x half-open as before, y endpoints included.
    Convexity guarantees that an inside-to-outside segment exits only once.
    Independent runtime/core and always-blocked static checks still apply.
    """
    l, h, b, t = bounds
    def metres(p, key):
        return Fraction(str(p[key]))
    x, y = metres(start, 'x_m'), metres(start, 'y_m')
    u, v = metres(end, 'x_m'), metres(end, 'y_m')
    def inside(x, y):
        return l <= x < h and b <= y <= t
    if inside(x, y):
        return inside(u, v)
    if max(x,u)<l or min(x,u)>=h or max(y,v)<b or min(y,v)>t:
        return False
    # Include tangential contact at closed edges; the east edge itself is open.
    times = {Fraction(0), Fraction(1)}
    for a, z, edges in ((x, u, (l, h)), (y, v, (b, t))):
        if z != a:
            times.update(q for edge in edges if 0 <= (q := (edge-a)/(z-a)) <= 1)
    times = sorted(times)
    for q in times + [(a+b)/2 for a,b in zip(times,times[1:])]:
        if inside(x + q*(u-x), y + q*(v-y)):
            return True
    return False
