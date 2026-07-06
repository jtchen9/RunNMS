# Solver #5 — GT-Free Yaw Two-Pass

## Architecture

Pass 1:
- distance + view angle only
- no yaw
- no ground truth

Between passes:
- compute distance/angle-only normalized residual score
- reject at most one standout D/A outlier
- compute predicted yaw from Pass-1 pose and known tag facing
- compare raw yaw branches `+raw_yaw` and `-raw_yaw`
- accept only a confident branch

Pass 2:
- surviving distance + angle observations
- accepted GT-free sign-resolved yaw observations
- full distance + angle + yaw solver

## Important

The solver intentionally ignores:
- `yaw_use_offline_label`
- existing `yaw_sign_corrected_deg`
- `true_yaw_deg`
- `yaw_error_deg`

Ground truth is used only by the evaluator after the estimate is produced.

## Yaw thresholds

The first implementation deliberately avoids a new sweep.

It reuses physically motivated gates from the earlier yaw study:
- minimum absolute predicted yaw: 8 deg
- accepted best branch error: <= 10 deg
- keep/flip branch separation: >= 15 deg

The only change is that Pass-1 predicted yaw replaces GT yaw as the reference.

## Run

Open in VS Code:

`src/experiments/run_solver5_gtfree_yaw.py`

and press Run.
