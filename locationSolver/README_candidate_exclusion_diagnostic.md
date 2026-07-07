# Candidate-exclusion forensic diagnostic — Phase-1 screened

This revision explicitly applies the exact observable gates before any
candidate-exclusion H_j / C_j calculation:

- front distance <= 5.5 m
- rear distance <= 3.0 m
- front |measured angle| <= 30 deg
- rear |measured angle| <= 15 deg

Therefore edge/far observations that cannot enter Stage-5 Pass 1 do not
participate in the diagnostic and do not count as villains for coverage.

The current frozen Stage-5 reject/survive status is also recomputed on this
same explicitly gated observation set.

For each eligible observation j:

1. remove j
2. solve D+A with the remaining eligible observations
3. compute held-out H_j
4. compute retained-set C_j
5. export rankings

GT measurement errors are attached only afterward for forensic coverage checks.

Important output added:
- `phase1_observable_excluded.csv`

All Stage-5 code remains untouched.
