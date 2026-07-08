# S3 robot-location helper interface dump patch

This is a deliberately small patch to the current functional state machine.

## Chosen replacement boundary

Keep in S3:

1. load latest mobility report
2. extract/normalize AprilTag observations

Move into new robot-location helper:

3. load tag map
4. solve robot pose

So the future helper boundary is:

```python
loc = solve_robot_location(
    visible_tags=visible,
)
```

where `visible` is the exact output of the current:

```python
_s3_extract_visible_tags(scanner)
```

## Current functions that will eventually be replaced

The active pose-estimation portion inside:

```python
_s3_solve_true_location(scanner)
```

Specifically:
- `_load_tag_map()`
- per-tag loop calling `_s3_solve_single_tag(...)`
- `_s3_fuse_candidates(...)`

The following remain state-machine responsibilities:
- `_load_report_json(scanner)`
- `_s3_extract_visible_tags(scanner)`
- success/failure orchestration in `s3solving_true_location(...)`
- `_s3_save_true_location(...)`
- propagation fallback
- S4/S5 transitions
- planned=true handling for location precondition

## Passive dump files

After one `mobility.report.location`, inspect:

```text
testLocation\output\s3_location_helper_dump\
```

Latest proposed helper input:

```text
latest_input_<scanner>.json
```

Exact current solver output returned to S3:

```text
latest_output_<scanner>.json
```

Timestamped copies are also kept.

## Safety

Dump failures are swallowed intentionally.  The added diagnostic I/O must not
change the current state-machine path or transitions.
