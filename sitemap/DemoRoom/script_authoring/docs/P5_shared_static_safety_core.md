# P5 Shared Static Safety Core

P5/P4 static safety is split into one shared core and thin adapters.

```text
CommonCheckers/checker/static_safety_core.py
    shared geometry and static policy logic

CommonCheckers/checker/path_rules.py
    preflight adapter that simulates planned poses from CSV rows

CommonCheckers/checker/macro_rules.py
    preflight adapter for macro script rules and bump checks

future NMS runtime adapter
    runtime adapter that reads true poses and pending command from Redis/NMS
```

The shared core intentionally does not know whether a pose is planned or true.
It only checks geometry and site policy.

Shared logic includes:

```text
restriction_map.npy loading and normalization
runtime-compatible world-to-grid conversion
non-clamped restriction-map safety lookup
legacy clamped world-to-grid helper for runtime compatibility
restriction-map segment scan
charging-zone circle checks
bump-guard rectangle crossing checks
mobility.move target calculation
macro endpoint calculation
macro start-pose geometry check
```

Preflight-only logic remains outside the core:

```text
CSV parsing
vocabulary checking
initial-pose CSV checking
180-second timing rule
validation report formatting
```

Runtime-only logic should also remain outside the core:

```text
Redis reads
true-location freshness
mobility_ready
robot_safety_state
state-machine state
stop_experiment
dynamic robot keep-out map construction
```

Runtime migration plan:

```text
1. Keep existing m8mobility_map.py dynamic obstacle logic.
2. Replace duplicated static map helpers with calls to static_safety_core.py.
3. Preserve existing dynamic effective_map behavior.
4. Add runtime bump/macro checks using the same static_safety_core.py functions.
```
