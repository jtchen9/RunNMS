# Solver #4 Candidate v1 — Freeze and Integration Notes

## Status

Frozen for validation. Do not continue tuning on `normal_diversity` unless a concrete failure mode is found.

## Frozen candidate

Measurement scales:

- distance sigma: 0.05 m
- angle sigma: 2.5 deg
- yaw sigma: 3.0 deg

Relative weights:

- distance: 1.0
- angle: 1.0
- yaw: 0.75

Robust rejection:

- rejection score threshold: 0.80
- minimum score gap: 0.50
- maximum rejections: 1
- minimum observations after rejection: 3

## Why this candidate

The candidate was selected for natural robustness, not for the prettiest training-set rank:

- simple relative weights
- broad high-performing neighborhood
- moderate rejection count
- good position tail
- all development samples within the 10 cm / 10 deg joint target
- avoids further tuning before independent validation

## Critical caveat

Current yaw acceptance/sign uses offline diagnostic labels, including
`yaw_use_offline_label` and `yaw_sign_corrected_deg`.

Therefore the present candidate is **not production-ready as-is**.

The robust rejection mechanism itself does not use ground truth, but the yaw
measurement presented to the solver still contains offline truth-assisted
processing.

## Validation gate before state-machine integration

1. Freeze candidate v1.
2. Collect genuinely independent robot visits.
3. Prepare a separate validation dataset without changing candidate parameters.
4. Run the exact frozen candidate.
5. Compare:
   - solver success rate
   - position mean / p90 / max
   - heading mean / p90 / max
   - joint 10 cm / 10 deg
   - rejected observation rate
   - repeated failure patterns by tag and geometry
6. Do not retune on the validation set. A new tuning cycle should require a
   clearly documented failure mode and a new candidate version.

## Future state-machine helper boundary

Do not merge the experimental runner into the mobility state machine.

Create a narrow helper boundary later, conceptually:

```python
estimate = estimate_robot_pose(
    observations=current_tag_observations,
    tag_pose_map=tag_pose_map,
    prior_pose=optional_prior_pose,
)
```

Suggested returned fields:

```text
success
x_m
y_m
heading_deg
solver_name
candidate_version
tags_input
tags_used
tags_rejected
objective_value
runtime_ms
quality_flags
failure_reason
```

The state machine should decide what to do with the estimate. The helper should
only calculate and report.

## Safety / rollout recommendation

Because robot location calibration and production-safe yaw handling are not yet
complete:

- integrate first in shadow / diagnostic mode
- do not let the helper trigger automatic correction motion
- compare helper estimates with existing state-machine behavior
- only enable control consequences after separate validation

This preserves the current production state-machine semantics and keeps solver
bring-up reversible.
