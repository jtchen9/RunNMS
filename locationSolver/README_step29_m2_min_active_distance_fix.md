# Step 29 — Narrow M2 minimum-active-distance safeguard

This patch changes only the direct M2 distance decision.

## Old behavior

For every distance component:

```text
reject if |M2| >= 0.95
```

Validation Sample 10 exposed a pathological case where all surviving
distance components crossed the threshold.  Pass-1 was already useful,
but the final solver then lost all range anchoring.

## New behavior

```text
1. Candidate if |M2| >= 0.95
2. Rank candidates by descending |M2|
3. Reject only the worst candidates
4. Stop once 3 active distance components remain
```

Frozen parameter:

```python
min_active_distances_after_m2 = 3
```

## Deliberately unchanged

- M2 definition
- M2 threshold 0.95
- Layer-1 rules
- Pass-1 Solver #2
- no second-stage angle screen
- no middle holdout solver
- GT-free yaw admission
- final component-wise D/A/Y solver

## Installation

Replace only:

```text
locationSolver/src/common/component_preparation.py
```

with the patched file in this package.

Existing callers remain compatible because the new config field has a
default value of 3.

## Test

From the `locationSolver` project root:

```text
python tests/test_m2_min_active_distance_floor.py
```

The synthetic regression test covers the Sample-10 failure shape:
six M2 candidates cannot remove all six; exactly three remain active.
