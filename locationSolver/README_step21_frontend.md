# Step 21 — Front end of the clean pipeline

This package starts from the raw long-format observation file and produces the
new rough no-yaw Pass-1 dataset needed to study Layer-2 screening.

## Pipeline

Raw observations
→ whole-tag first screen
→ existing `build_prepared_samples`
→ unchanged Solver #2
→ signed D/A residual outputs

## First screen

Reject the whole tag observation if:

- front and `abs(meas_angle_deg) > 30`
- OR front and `meas_distance_m > 5.5`

No rear-distance rule.

## Pass 1

Uses the unchanged:

`src.solvers.solver_2_distance_angle.solve_distance_angle`

with original `DistanceAngleSolverConfig()` defaults.

## Main outputs

- `first_screen_all_242_audit.csv`
- `first_screen_rejected_rows.csv`
- `first_screen_surviving_rows.csv`
- `pass1_sample_results.csv`
- `pass1_signed_component_residuals.csv`
- `pass1_ranked_distance_residuals.csv`
- `pass1_ranked_angle_residuals.csv`

The signed residual file preserves:

- signed distance residual
- signed angle residual

and attaches GT component errors only afterward for forensic study of Layer 2.

## Important

This package does not modify Solver #2, Solver #3, Solver #5, or production
state-machine code.
