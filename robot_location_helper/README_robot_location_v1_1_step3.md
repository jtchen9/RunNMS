# Step 3 — robot_location_v1_1 concise helper

This package implements the agreed cleanup Step 3.

No state-machine code is modified.

## Public API

```python
from robot_location_helper import (
    solve_robot_location,
)

loc = solve_robot_location(
    visible_tags=visible,
)
```

`visible` is the exact output of the existing:

```python
_s3_extract_visible_tags(scanner)
```

## Responsibility boundary

The helper owns:

- normalized visible-tag input
- loading `tag_location.txt`
- Layer-1 physical screen
- Pass-1 D+A solve
- M2 distance screening
- minimum 3 active-distance floor
- GT-free yaw keep/flip/reject
- final component-wise D/A/Y solve
- pose diagnostics

The helper does **not**:

- load mobility reports
- read Redis
- save `true_location_json`
- propagate old true pose
- decide S4/S5 transitions
- issue commands
- read `restriction_map.npy`

## Frozen estimator

```text
Layer 1:
    reject front |angle| > 30 deg
    reject front distance > 5.5 m
    no rear-distance gate
    no rear-angle gate

Pass 1:
    proven Solver #2 D+A
    distance sigma 0.05 m
    angle sigma 3 deg
    8 heading starts

Distance second screen:
    M2_j = z_d,j - median(peer z_d)
    candidate if |M2| >= 0.95
    reject worst candidates only
    retain at least 3 active distances

Angle:
    no second-stage screen

Yaw:
    from original Pass-1 pose
    |predicted yaw| >= 8 deg
    best branch error <= 10 deg
    keep/flip separation >= 15 deg

Final:
    component-wise D/A/Y
    sigma = 0.05 m / 2.5 deg / 3 deg
    global weights = 1.0 / 1.0 / 0.75
```

## Source provenance

The package was extracted from the proven development files:

- Step 29 `component_preparation.py`
- Step 26 final component-wise solver
- proven Solver #2
- proven geometry and SolverResult contracts

The only changes to those solver files are package import paths.

## Install

Copy:

```text
robot_location_helper\
```

to:

```text
D:\Data\_Action\_RunNMS\
```

Copy:

```text
tools\crosscheck_robot_location_helper.py
```

to:

```text
D:\Data\_Action\_RunNMS\tools\
```

## Cross-check before S3 integration

Use one fresh passive S3 input dump:

```text
latest_input_twin-scout-charlie.json
```

Run:

```text
python tools\crosscheck_robot_location_helper.py ^
  --input-json "testLocation\output\s3_location_helper_dump\latest_input_twin-scout-charlie.json"
```

Expected:

```text
RESULT: PASS
```

The script compares the new concise helper against the existing proven
`t11_collect_validation_final_componentwise.py` current pipeline.

Do not integrate into S3 until this passes.

## Next step after live-input PASS

Replay:

- 39 development samples
- 25 validation samples

Then add confidence and sparse rare-case logging.
