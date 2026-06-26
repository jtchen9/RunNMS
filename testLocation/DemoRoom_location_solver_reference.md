# DemoRoom Location Solver Bring-up Reference

Date: 2026-06-26  
Root folder:

```text
D:\Data\_Action\_RunNMS
```

This document records the current location-solver cleanup and bring-up work before collecting new clean data.

---

## 1. Final folder structure

### Production-evolving reusable modules

```text
D:\Data\_Action\_RunNMS\locationSolver
```

This folder contains reusable solver logic. These modules should not contain test CSV paths, report-output paths, or one-time diagnostic file I/O.

Current modules:

```text
locationSolver\
  __init__.py
  location_config.py
  location_tagmap.py
  location_geometry.py
  location_items.py
  location_distance_solver.py
  location_joint_solver.py
  location_joint_solver_yaw.py
  location_item_filter.py
  location_filtered_solver.py
  location_confidence.py
```

### Test and diagnostic scripts

```text
D:\Data\_Action\_RunNMS\testLocation
```

Current structure:

```text
testLocation\
  archive\
  input\
  output\
  t1_probe_single_tag.py
  t2_build_preferred_heading_matrices.py
  t3_test_geometry_check.py
  t4_test_measurement_items.py
  t5_test_distance_only_solver.py
  t6_test_distance_angle_solver.py
  t7_test_distance_angle_yaw_solver.py
  t8_test_item_filtering.py
  t9_test_confidence_report.py
  t10_test_full_location_pipeline.py
```

### Canonical site data

```text
D:\Data\_Action\_RunNMS\sitemap\DemoRoom\tag_location.txt
```

This is the canonical tag map. Do not copy it into `testLocation\input` as the authority.

---

## 2. Coding rules adopted

1. Folder name is lowercase-first:

```text
locationSolver
```

not `LocationSolver`.

2. Production-evolving modules should not use `argparse`.

3. Test scripts should keep adjustable paths and parameters under:

```python
if __name__ == "__main__":
    ...
```

4. Reusable functions/classes go above `if __name__ == "__main__"`.

5. Test scripts may read/write CSV/TXT/JSON. Reusable modules should not.

6. Test scripts under `testLocation` should be direct-run friendly from VS Code using:

```python
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
```

7. Avoid relative parent imports like:

```python
from ..locationSolver import ...
```

8. Old diagnostic files should be separated:
   - raw/derived inputs under `testLocation\input`
   - obsolete scripts/reports under `testLocation\archive`
   - generated outputs under `testLocation\output`

---

## 3. Coordinate and geometry convention

Coordinate system:

```text
origin: bottom-left
x: rightward
y: upward
heading 0 deg: +x
heading 90 deg: +y
heading 180 deg: -x
heading 270 deg: -y
```

Tag yaw convention:

```text
Facing Right: 0 deg
Facing Up: 90 deg
Facing Left: 180 deg
Facing Down: 270 deg
```

Camera offsets:

```text
front:
  heading_offset_deg = 0.0
  forward_offset_m = +0.055

rear:
  heading_offset_deg = 180.0
  forward_offset_m = -0.075
```

Camera pose from robot pose:

```text
x_c = x + forward_offset * cos(h)
y_c = y + forward_offset * sin(h)
h_c = h + heading_offset
```

Measurement convention:

```text
distance = camera-to-tag distance
bearing_world = atan2(tag_y - camera_y, tag_x - camera_x)
view_angle = camera_heading - bearing_world
yaw = tag_yaw + 180 - camera_heading + view_angle
```

Residual convention:

```text
residual = measured - predicted
```

---

## 4. t1-t10 test scripts and their purpose

### t1 — single-tag probe

```text
testLocation\t1_probe_single_tag.py
```

Purpose:
- Probe one tag/camera observation.
- Useful for checking raw AprilTag distance, angle, yaw behavior.
- This is mostly a diagnostic/physical sanity script.

Production module:
- none directly; this is data inspection.

---

### t2 — preferred-heading matrix builder

```text
testLocation\t2_build_preferred_heading_matrices.py
```

Purpose:
- Build preferred robot orientation lookup table.
- Used before data collection and later before production localization report.
- Resolution used in current bring-up:
  - position grid: 0.1 m
  - heading grid: 5 deg

Outputs:

```text
testLocation\output\preferred_heading_test
testLocation\output\preferred_heading_full
```

Important simplified rule:
- inner robot uses tags 31-58
- outer robot uses tags 1-30
- no doorway exceptions in first pass

Production module:
- not yet separated; this remains one-time/planner code for now.

---

### t3 — Part 1/2 geometry check

```text
testLocation\t3_test_geometry_check.py
```

Purpose:
- Test geometry prediction and residual calculation.
- Reads old `step1_observations.csv`.
- Recomputes predicted distance/angle/yaw from known ground-truth pose.

Output:

```text
testLocation\output\geometry_check
```

Key output files:

```text
geometry_check_rows.csv
geometry_check_worst_rows.csv
geometry_check_config.json
geometry_check_summary.txt
```

Production modules:

```text
locationSolver\location_config.py
locationSolver\location_tagmap.py
locationSolver\location_geometry.py
```

---

### t4 — Part 3 measurement item builder

```text
testLocation\t4_test_measurement_items.py
```

Purpose:
- Convert each observed tag into independent items:
  - distance item
  - angle item
  - yaw item

This enables item-level filtering later.

Output:

```text
testLocation\output\measurement_items
```

Production module:

```text
locationSolver\location_items.py
```

Important design:
- Do not reject the whole tag when only yaw is bad.
- Keep distance/angle/yaw as separate items.

---

### t5 — Part 4 distance-only solver

```text
testLocation\t5_test_distance_only_solver.py
```

Purpose:
- Estimate x,y from distance observations only.
- Heading is fixed.

Important limitation:
- distance-only cannot solve heading.
- In diagnostic test, ground-truth heading is used to test mechanics.

Output:

```text
testLocation\output\distance_only_solver
```

Production module:

```text
locationSolver\location_distance_solver.py
```

---

### t6 — Part 5 distance + angle solver

```text
testLocation\t6_test_distance_angle_solver.py
```

Purpose:
- Estimate x,y,heading using:
  - distance
  - view-angle

Yaw is not used.

Output:

```text
testLocation\output\distance_angle_solver
```

Production module:

```text
locationSolver\location_joint_solver.py
```

Expected role:
- This is the practical main solver foundation.

---

### t7 — Part 6 distance + angle + weak yaw solver

```text
testLocation\t7_test_distance_angle_yaw_solver.py
```

Purpose:
- Add yaw as a weak helper.
- Compare Part 5 no-yaw solution and Part 6 weak-yaw solution.

Output:

```text
testLocation\output\distance_angle_yaw_solver
```

Production module:

```text
locationSolver\location_joint_solver_yaw.py
```

Important check:
- weak yaw should not drastically move the solution.
- If yaw shift is large, yaw data or yaw weight is not trustworthy.

---

### t8 — Part 7 item-level filtering

```text
testLocation\t8_test_item_filtering.py
```

Purpose:
- Run pass1 solve.
- Classify each residual item as:
  - GOOD
  - WEAK
  - BAD
  - SEVERE
- Mark each item as:
  - USED_NORMAL
  - USED_LOW_WEIGHT
  - DISABLED
- Re-solve with filtered item weights.

Output:

```text
testLocation\output\item_filtering
```

Production modules:

```text
locationSolver\location_item_filter.py
locationSolver\location_filtered_solver.py
```

Important design:
- A bad yaw item does not disable the distance/angle items from the same tag.
- Distance/angle may be kept at low weight if needed for diversity.

---

### t9 — Part 8 confidence report

```text
testLocation\t9_test_confidence_report.py
```

Purpose:
- Evaluate whether the filtered solution is trustworthy.
- Classify result as:
  - GOOD
  - MARGINAL
  - BAD
  - FAILED

Output:

```text
testLocation\output\confidence_report
```

Production module:

```text
locationSolver\location_confidence.py
```

Important production meaning:
- GOOD: candidate to update trusted true location.
- MARGINAL: useful for logs/debug, but normally should not drive correction.
- BAD/FAILED: do not trust.

---

### t10 — full end-to-end location pipeline

```text
testLocation\t10_test_full_location_pipeline.py
```

Purpose:
- One script to run the full staged pipeline:
  1. build observations
  2. pass1 solve
  3. item-level filtering
  4. filtered re-solve
  5. confidence/diversity evaluation
  6. final output report

Output:

```text
testLocation\output\full_location_pipeline
```

Key output files:

```text
full_location_pipeline_group_summary.csv
full_location_pipeline_item_states.csv
full_location_pipeline_final_residuals.csv
full_location_pipeline_config.json
full_location_pipeline_summary.txt
```

Production modules used:

```text
location_config.py
location_tagmap.py
location_geometry.py
location_items.py
location_joint_solver_yaw.py
location_item_filter.py
location_filtered_solver.py
location_confidence.py
```

Current note:
- t10 passed after adapting to the installed `location_confidence.py` interface.

---

## 5. Current known limitations

The old 290-row dataset is not a final accuracy benchmark.

Known issues:
1. some tag coordinates were corrected after the dataset was collected
2. rear camera installation changed after the dataset was collected
3. many robot headings are bad localization headings
4. some orientations do not provide enough useful tag geometry
5. therefore large errors in t5-t10 are expected

Current interpretation:
- The pipeline mechanics are working.
- Accuracy must be judged only after collecting clean data at preferred headings.

---

## 6. Clean data collection plan

Goal:
- collect clean localization data at planned x,y positions
- use preferred heading from t2
- evaluate whether 10 cm / 5 deg target is achievable

Manual workflow:

1. User chooses a known robot location:

```text
x = user-entered
y = user-entered
```

2. Console script looks up preferred orientation from t2 output.

3. Script prints short instruction:

```text
At x=..., y=...
Preferred heading = ... deg
Please rotate robot to this heading.
Press Enter after robot is correctly oriented.
```

4. User manually turns robot to the preferred heading.

5. User presses Enter.

6. Script collects or reads one robot localization response.

7. Script prints a not-too-long report:

```text
estimated x,y,heading
confidence
distance item count
angle item count
bearing span
distance RMS
angle RMS
bad/disabled items
main residual warnings
```

8. User decides whether the collected data is useful.

---

## 7. Proposed next script after documentation

Suggested filename:

```text
testLocation\t11_console_collect_clean_data.py
```

Purpose:
- command-console script for clean data collection
- user enters x,y
- script reports preferred heading
- waits for Enter after manual heading adjustment
- reads or accepts robot observation response
- runs full location pipeline
- prints compact human-readable result

Possible future command style:

```bat
python t11_console_collect_clean_data.py
```

Interactive flow:

```text
Input ground-truth x_m: 5.20
Input ground-truth y_m: 2.40

Preferred heading at (5.20, 2.40): 90 deg
Please rotate robot to 90 deg.
Press Enter when ready...

Paste or load robot observation JSON/CSV:
...
```

Or later:

```bat
python t11_console_collect_clean_data.py --x 5.20 --y 2.40
```

But for now, to stay consistent with this project’s rule, avoid argparse and use interactive `input()`.

---

## 8. Parameter tuning plan with clean data

After clean data is collected, tune parts in this order.

### Step A — verify geometry, Part 1/2

Use clean data with known x,y,heading.

Check:
- distance residual signs
- angle residual signs
- yaw residual signs
- front/rear consistency
- individual tag residuals

Goal:
- no systematic sign/offset mistakes.

### Step B — tune measurement weights, Part 3

Tune:
- front distance weight
- front angle weight
- front yaw weight
- rear distance weight
- rear angle weight
- rear yaw weight

Expected first direction:
- front distance strongest
- front angle useful
- yaw weak
- rear weaker unless new data proves it is reliable

### Step C — tune distance-only, Part 4

Purpose:
- diagnostic x,y baseline
- not final production solver

Check:
- whether clean preferred-heading data can localize x,y from distances alone
- whether distance bias exists per tag/camera

### Step D — tune distance + angle, Part 5

Main target:
- x,y error near or below 10 cm
- heading error near or below 5 deg

Tune:
- distance sigma
- angle sigma
- front/rear weights
- robust clipping threshold
- heading seed policy

### Step E — tune weak yaw, Part 6

Use yaw only if it helps.

Check:
- yaw-induced position shift
- yaw-induced heading shift
- yaw residual consistency by camera/tag

If yaw is unstable:
- keep yaw very weak
- or disable yaw in production confidence path

### Step F — tune item filtering, Part 7

Tune:
- GOOD/WEAK/BAD/SEVERE thresholds
- low-weight multipliers
- minimum distance/angle item preservation

Goal:
- reject bad items without destroying geometry diversity.

### Step G — tune confidence report, Part 8

Tune GOOD/MARGINAL/BAD/FAILED thresholds.

Target production rule:
- only GOOD should be allowed to update trusted true location
- MARGINAL can be logged but should not trigger correction moves
- BAD/FAILED ignored for correction

---

## 9. Early target

Engineering target:

```text
position error: <= 10 cm
heading error: <= 5 deg
```

This target should be evaluated only on:
- corrected tag map
- stable front/rear camera installation
- robot at preferred localization heading
- enough visible tags
- good distance-bearing diversity

If clean preferred-heading data cannot approach this target, next likely causes are:
1. AprilTag distance bias
2. camera offset/projection-center uncertainty
3. angle/yaw sign or convention issue
4. local geometry not diverse enough
5. physical tag placement error
6. robot ground-truth x,y measurement error
