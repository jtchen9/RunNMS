# Steps 4–6 — confidence, S5 GO/NO_GO, sparse rare-case logging

This package extends the already cross-checked `robot_location_v1_1` helper.

No state-machine interface is changed.

## 1. Confidence output

Every successful solve now includes:

```python
loc["confidence"]
```

Structure:

```python
{
    "level": "HIGH" | "MEDIUM" | "LOW",

    "reason_codes": [...],
    "warning_codes": [...],

    "metrics": {
        ...
    },

    "config": {
        ...
    }
}
```

This is **not** a calibrated probability of true error.

Ground truth is unavailable during live operation.

The confidence metrics are observable diagnostics only.

### Measurement support

```text
tags_observed_count
tags_layer1_usable_count

active_distance_count
active_angle_count
active_yaw_count

used_tag_count
front_used_count
rear_used_count
```

### Screening stress

```text
distance_m2_candidate_count
distance_rejected_count
distance_rejected_fraction

distance_floor_activated
distance_kept_by_floor_count

yaw_admitted_count
yaw_rejected_count
```

### Pass1 -> final stability

```text
pass1_to_final_shift_m
pass1_to_final_heading_shift_deg
```

The heading shift is diagnostic only.

It is not used as an S5 correction trigger.

### Final residual consistency

```text
final_distance_rms_m
final_distance_max_abs_m

final_angle_rms_deg
final_angle_max_abs_deg

final_yaw_rms_deg
final_yaw_max_abs_deg

final_normalized_rms
active_component_count
```

### Geometry support

```text
used_tag_bearing_span_deg
```

---

## 2. Initial confidence classifier

The first classifier is intentionally interpretable.

LOW is reserved for strong warnings:

```text
active distances < 3
used tags < 3
Pass1->final position shift > 12 cm
final normalized RMS > 2.5
```

MEDIUM warnings:

```text
Pass1->final position shift > 6 cm
final normalized RMS > 1.5
M2 distance floor activated
distance rejection fraction >= 0.5
used-tag bearing span < 30 deg
```

Otherwise:

```text
HIGH
```

These thresholds are centralized in:

```text
robot_location_helper/confidence.py
```

They are initial operational thresholds and should be replay-audited before S3/S5 integration.

---

## 3. S5-facing GO / NO_GO helper

Public API:

```python
from robot_location_helper import (
    decide_followup_correction,
)

decision = decide_followup_correction(
    location_result=loc,
    planned_location=planned_loc,
)
```

The first policy is position-only.

No heading-only trigger exists.

### Gate 1

```text
position discrepancy <= 8 cm
    -> NO_GO
```

### Gate 2

```text
position discrepancy > 8 cm
AND confidence LOW
    -> NO_GO
```

### Gate 3

```text
position discrepancy > 8 cm
AND confidence MEDIUM/HIGH
    -> GO
```

S5 will later need only:

```python
decision["go"]
```

---

## 4. Sparse rare-case logging

Public API:

```python
from robot_location_helper import RareCaseLogger

logger = RareCaseLogger(
    "locationSolver/output/rare_location_cases.jsonl"
)

logger.log_if_rare(
    reasons=loc["rare_case_reasons"],
    scanner="twin-scout-charlie",
    visible_tags=visible,
    location_result=loc,
)
```

Nothing is written unless at least one rare-case reason exists.

Confidence-origin rare triggers include:

```text
LOW_LOCATION_CONFIDENCE
M2_DISTANCE_FLOOR_ACTIVATED
HIGH_DISTANCE_REJECTION_FRACTION
LARGE_PASS1_FINAL_POSITION_SHIFT
POOR_FINAL_RESIDUAL_CONSISTENCY
WEAK_DISTANCE_SUPPORT
TOO_FEW_USED_TAGS
```

The correction decision can add later:

```text
S5_POSITION_DISCREPANCY_GT_8CM
S5_LARGE_DISCREPANCY_BLOCKED_LOW_CONFIDENCE
```

The JSONL record preserves:

```text
scanner
rare-case reasons
raw visible tags
full location result
confidence evidence
optional correction decision
optional context
```

This is replay-oriented and sparse.

---

## 5. Inspect before state-machine integration

Use:

```text
tools\inspect_robot_location_confidence.py
```

Example:

```text
python tools\inspect_robot_location_confidence.py ^
  --input-json "locationSolver\output\2026-07-08-09_15_33_twin-scout-charlie_input.json" ^
  --planned-x 9.0 ^
  --planned-y 1.5
```

It prints:

```text
pose
confidence level
reason codes
warning codes
all confidence metrics
rare-case reasons
GO / NO_GO decision
```

No state-machine code is touched.

---

## 6. Next validation gate

Before state-machine integration:

```text
1. confirm old-vs-new solver identity still passes
2. inspect confidence on live S3 input
3. replay 39 development samples
4. replay 25 validation samples
5. inspect HIGH/MEDIUM/LOW distribution
6. inspect GO/NO_GO against the known >8 cm tail cases
7. only then wire S3 and S5
```
