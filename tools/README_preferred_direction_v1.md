# preferred_direction_v1 — generation and cross-check guide

This document is intentionally operational. Keep it for future site changes.

## When must the LUT be regenerated?

Regenerate whenever any of these changes:

- a tag is moved
- a tag is added or removed
- a tag facing direction changes
- `sitemap\DemoRoom\tag_location.txt` changes
- the robot moves to a new site
- the site dimensions or inner/outer room split change
- the preferred-direction rule itself changes

The LUT depends only on the tag-location map and frozen preferred-direction rule.

It does **not** depend on:

```text
restriction_map.npy
```

Collision avoidance remains separate.

---

# A. Install files

Project root:

```text
D:\Data\_Action\_RunNMS\
```

Place:

```text
tools\build_preferred_direction_lut.py
tools\crosscheck_preferred_direction_lut.py

preferred_direction_helper\
    __init__.py
    lookup.py
```

The proven reference T2 must remain available at:

```text
D:\Data\_Action\_RunNMS\
    t2_build_preferred_heading_matrices.py
```

---

# B. Generate the LUT for DemoRoom

The generator defaults to:

```text
Input:
D:\Data\_Action\_RunNMS\
    sitemap\DemoRoom\tag_location.txt

Output:
D:\Data\_Action\_RunNMS\
    sitemap\DemoRoom\preferred_direction_lut\
```

## Recommended VS Code method

1. Open:

```text
tools\build_preferred_direction_lut.py
```

2. Press **Run Python File**

No arguments are required for DemoRoom.

## Command-line equivalent

From:

```text
D:\Data\_Action\_RunNMS
```

run:

```text
python tools\build_preferred_direction_lut.py
```

Expected output directory:

```text
sitemap\DemoRoom\preferred_direction_lut\
```

Expected files:

```text
preferred_heading_deg.npy
preferred_heading_score.npy
preferred_heading_status_code.npy
preferred_heading_effective_count.npy
preferred_heading_geometry_span_deg.npy

axis_x_m.npy
axis_y_m.npy

preferred_direction_meta.json
```

The metadata records the SHA-256 fingerprint of the tag map.

---

# C. Cross-check against proven T2

After LUT generation, run:

```text
python tools\crosscheck_preferred_direction_lut.py
```

Default behavior:

- compares representative inner-room points
- compares representative outer-room points
- recomputes each answer through the proven T2 Rule-C implementation
- compares the result with the cleaned LUT

Pass criterion:

```text
Mismatches = 0
RESULT: PASS
```

This is the recommended routine check after regeneration.

---

# D. One-time deep full-grid cross-check

For a new site, a major tag-map change, or before first production use:

```text
python tools\crosscheck_preferred_direction_lut.py --all-grid
```

This recomputes the proven T2 answer for every LUT cell.

It is much slower because every grid cell evaluates all 5-degree headings.

Pass criterion remains:

```text
Mismatches = 0
RESULT: PASS
```

---

# E. New site procedure

For a new site, do not overwrite DemoRoom blindly.

Recommended layout:

```text
sitemap\
    NewSite\
        tag_location.txt
        preferred_direction_lut\
```

Run:

```text
python tools\build_preferred_direction_lut.py ^
  --tag-file "sitemap\NewSite\tag_location.txt" ^
  --output-dir "sitemap\NewSite\preferred_direction_lut"
```

Then cross-check:

```text
python tools\crosscheck_preferred_direction_lut.py ^
  --tag-file "sitemap\NewSite\tag_location.txt" ^
  --lut-dir "sitemap\NewSite\preferred_direction_lut"
```

Important:

The current frozen generator still assumes the current site extents and room split:

```text
x: 0.0 .. 11.4 m
y: 0.0 .. 11.4 m

inner: y <= 5.20
outer: y >= 5.40
middle strip: blocked
```

Therefore, for a genuinely different site geometry, update site geometry configuration before generating the LUT. Do not reuse DemoRoom dimensions silently.

---

# F. Runtime lookup

The state machine will later call:

```python
from preferred_direction_helper import (
    lookup_preferred_direction,
)

result = lookup_preferred_direction(
    x_m=target_x,
    y_m=target_y,
)
```

The online helper reads the generated LUT only.

It does not:
- read Redis
- issue commands
- change state
- read `restriction_map.npy`

---

# G. Rule identity

Operational rule:

```text
preferred_direction_v1
```

Historical origin:

```text
former Rule C
```

The operational code no longer uses A/B/C labels.

Frozen core values:

```text
grid step       0.1 m
heading step    5 deg

front FOV       +/-35 deg
rear FOV        +/-15 deg
incidence       <=60 deg

front distance:
0.4-4.0 m       1.0
4.0-5.0 m       0.7
5.0-6.0 m       0.4

rear distance:
0.4-2.5 m       1.0
2.5-3.0 m       0.5
>3.0 m          0

front confidence 1.0
rear confidence  0.6
geometry bonus   0.10 per degree
```

---

# H. Recommended operational sequence after tag updates

```text
1. Edit tag_location.txt
2. Generate LUT
3. Run representative cross-check
4. For major changes, run --all-grid once
5. Only after PASS, let runtime use the new LUT
```

Do not skip Step 3.
