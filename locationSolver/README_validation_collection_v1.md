# Validation Collection v1

## Principle

The field KEEP / RETAKE decision is based only on observable acquisition quality.

It does not use ground-truth localization error and does not use Candidate v1 error against the planned point.

The quick pose printed by the checker is produced by Solver #2 (distance + view angle only) and is diagnostic only.

## Observable gates

- front distance <= 5.5 m
- rear distance <= 3.0 m
- front |view angle| <= 30 deg
- rear |view angle| <= 15 deg
- at least 4 usable observations
- at least 4 unique tags
- 5+ usable observations preferred

## Validation point design

`validation_points_v1.csv` uses positions different from the original 29 main points.
The original main points were on integer x/y coordinates; the validation plan uses half-meter offsets.

Priority A: collect first.
Priority B: collect after A, mainly for broader edge/top coverage.

Do not retune Candidate v1 using these results.
