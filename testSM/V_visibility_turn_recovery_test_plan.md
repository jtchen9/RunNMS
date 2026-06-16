# V-series visibility-turn location recovery tests

These tests are for the revised recovery policy:

- `visibility_turn_count` counts only NMS-issued recovery `mobility.turn` commands.
- `mobility.report.location` is mandatory after accident/collision/execution-failure recovery, but it does not consume `visibility_turn_count`.
- `VISIBILITY_TURN_LIMIT = 1`.
- A no-tag/deadspot failure goes directly to one +90° visibility turn, not to repeated `report.location`.
- After a failed visibility turn report, NMS stops. It must not issue another `report.location` from the same heading.

Run tests with the real robot service running and the NMS restarted after replacing the patched modules.

## Shared commands

Clear intercept:
```powershell
python .\testSM\t9_clear_intercept.py
```

Inspect current recovery fields:
```powershell
python .\testSM\t9_V0_audit_visibility_recovery_state.py
```

## V1 — no-tag on normal forward should issue +90° visibility turn

1. Arm intercept:
```powershell
python .\testSM\t9_set_V1_no_tag_on_next_forward.py
```
2. Run a normal forward movement, for example:
```powershell
python .\testSM\t3_c04_forward_050m_pre0_post0_real_robot.py
```
3. Expected:
- `location_recovery_context = deadspot`
- `location_recovery_phase = after_visibility_turn`
- `visibility_turn_count = 1`
- newest command is `mobility.turn` with `angle_deg = 90.0`
- no `mobility.report.location` is issued before that turn.

## V2 — if the visibility turn also has no tags, stop

1. After V1 has issued the recovery turn, arm:
```powershell
python .\testSM\t9_set_V2_no_tag_on_next_visibility_turn.py
```
2. Let the robot execute/report the recovery turn.
3. Expected:
- state becomes `s7stopped`
- `stop_reason = LOCATION_RECOVERY_EXHAUSTED` or state detail says location recovery exhausted
- no further `mobility.report.location` is issued.

## V3 — collision should first issue mandatory report.location

1. Arm intercept:
```powershell
python .\testSM\t9_set_V3_collision_on_next_forward.py
```
2. Run a normal forward movement.
3. Expected:
- `collision_veto_count = 1`
- `location_recovery_context = accident`
- `location_recovery_phase = after_report_location`
- `visibility_turn_count = 0`
- newest command is `mobility.report.location`.

## V4 — accident report.location failure should issue +90° visibility turn

1. After V3 has issued `mobility.report.location`, arm:
```powershell
python .\testSM\t9_set_V4_location_fail_on_next_report_location.py
```
2. Let robot execute/report `mobility.report.location`.
3. Expected:
- newest command is `mobility.turn` with `angle_deg = 90.0`
- `visibility_turn_count = 1`
- abnormal counters remain preserved.

## V5 — accident visibility turn failure should stop

1. After V4 has issued the recovery turn, arm:
```powershell
python .\testSM\t9_set_V5_no_tag_on_next_visibility_turn.py
```
2. Let robot execute/report the turn.
3. Expected:
- state becomes `s7stopped`
- no further `mobility.report.location`.

## V6 — MOVE_EXEC_FAIL should follow accident path

1. Arm:
```powershell
python .\testSM\t9_set_V6_move_exec_fail_on_next_forward.py
```
2. Run normal forward movement.
3. Expected:
- `exec_fail_count = 1`
- newest command is `mobility.report.location`
- `visibility_turn_count = 0`.

## V7 — second abnormal event should still stop

After one abnormal event is already counted, inject another collision or exec failure.
Expected:
- state becomes `s7stopped`
- no new visibility turn is issued.
- safety limit is still controlled by `busy_count + collision_veto_count + exec_fail_count`.

## V8 — loader preflight clears legacy fields

Upload a CSV with first row `mobility.report.location`.
Expected:
- `need_location_retry` and `retry_count` are removed/cleared.
- new fields exist:
  - `need_location_recovery`
  - `location_recovery_context`
  - `location_recovery_phase`
  - `visibility_turn_count`
