# AutoLab experiment scripts: design goals and operating guidance

**Decision baseline:** 21 November 2026, agreed with JT.  
**Purpose:** a handoff for script writers, lab operators, and developers revising the Excel template, CSV parser, checkers, preparation code, and robot shutdown handler.  
**Status:** agreed target behavior. This document does not mean the corresponding code changes have already been implemented or tested.

## 1. Goals and scope

Produce one CSV for experiment registration and, optionally, a PNG placement guide. Make approximate robot placement easy, let initial preparation establish readiness and starting positions, and avoid unnecessary commands and idle time. Preserve the existing movement/correction state machine and ordinary runtime failure handling.

The template must support all current commands, including `ap.backhaul.steer` and `system.shutdown`. Existing restriction maps, crossing policies and command validation remain applicable.

Backward compatibility with the two-file format or the earlier F/G initial-roster format is **not a requirement**. Export and registration must agree on the new format; legacy formats must not be silently misinterpreted.

### Responsibility labels

| Label | Meaning |
|---|---|
| Rule | An exporter, parser or checker requirement that can be tested automatically. |
| Runtime | Behavior enforced by the NMS or robot while executing the experiment. |
| Writer guidance | A planning responsibility; not an automatic reason to reject a script unless an existing rule already applies. |
| Operator SOP | An action performed by the person operating the lab. |

## 2. Single-file CSV contract

| Column | Contents | Role |
|---|---|---|
| A | `scanner` | Command target. |
| B | `t_offset_sec` | Authoritative command offset relative to the NMS-assigned `t₀`. |
| C | `category` | Command category. |
| D | `action` | Command action. |
| E | `args_json` | Command arguments. |
| F | Re-entry robot ID | Operator reminder on the first command row for that robot after its preceding shutdown. |
| G | Re-entry coordinate `(x, y)`, or blank | Operator placement reminder derived from the first subsequent positioning move. |
| H | Display time | Optional Excel local-time display; not a scheduling input. |
| I | Initial participating robot ID | Required initial preparation roster, beginning at I2. |
| J | Intended initial coordinate `(x, y)` | Preparation destination for the robot in I on the same row. |

**Rule:** A–E retain their established meanings. F/G do not issue commands, trigger positioning, gate a queue, or introduce a re-entry state. I/J are actual preparation inputs, not merely annotations.

**Rule:** For N participating robots, the roster occupies I2:J(N+1), one unique robot per row. Include every participating robot, including those whose useful work starts later. Do not include fixed AP map markers as robot participants.

**Rule:** Every initial robot has a coordinate, including robots with no movement commands in their experiment script. The initial roster is independent of the command target in A on the same row.

**Implementation detail:** settle the exact F/G/I/J header strings once in the shared CSV contract and use them consistently in export, import and registration. Do not infer old formats by guessing from coordinates. A roster longer than the command list must not require fabricated robot commands; any metadata-only rows need explicit parser/checker handling.

### Time display

The user does not provide a future `t₀` to register the experiment. Preparation determines the actual `t₀`, which the NMS announces through the existing experiment status flow. Column B remains authoritative.

H1 may hold that announced start time for Excel viewing. H2 uses `=$H$1+B2/86400`, with the row reference adjusted for each command row. H1 is not a registration input. Before a start time is known, do not interpret H as an actual appointment time. Use the system's existing local-time convention throughout.

**Operator SOP:** initial roster entries mean “place and power on before preparation.” Do not use H2–H(N+1) as the placement times of the initial roster; those H cells belong to unrelated command rows.

## 3. Initial preparation and approximate placement

**Operator SOP:** for each participant:

1. Use the helper map to find the intended starting area.
2. Place the robot within approximately 2 m of the intended coordinate, on a safe side of nearby restrictions.
3. Ensure a clear straight-line approach to the intended coordinate. The operator is responsible for this choice; no detour planner is requested.
4. Choose a heading from which the cameras can see usable AprilTags. Precise initial heading is unnecessary.
5. Power the Pi, cameras and motors and allow the existing health/preparation checks to run. Check that tags are correctly positioned and not blocked by movable furniture, ladders or monitors.

**Runtime:** all participants undergo initial health checks and positioning, including robots that later leave or only perform scanning. Preparation is also the warm-up period; successful checks and the positioning cycle establish readiness before the command timeline begins.

**Runtime change:** increase the initial placement-distance allowance from 1 m to **2 m**, comparing a fresh estimated position against the intended I/J destination. The true physical position is not independently known to the NMS.

Retain the existing approach safety checks. The 2 m allowance does not exempt a path from restrictions or other movement checks. The planned restriction map remains stricter than the runtime map; neither map is revised by this work.

**Runtime:** use the original movement/correction mechanism and its existing **0.4 m first-error / 0.3 m final-error thresholds**. Do not invent a new two-attempt controller, change the normal acceptance threshold, or require a correction when the original mechanism already accepts arrival. Verify the installed mechanism while implementing the tolerance change.

If preparation fails, retain its existing three-minute operator-adjustment wait and subsequent attempt. Experiment commands remain held until preparation succeeds. Preserve the parallel preparation behavior already tested with multiple robots and the existing NMS-assigned start/end-time publication.

## 4. Re-entry after shutdown

### Reminder generation — Rule

Process each robot's own commands in execution order, preserving the established ordering of commands with equal offsets. For each `system.shutdown` that has later commands for that same robot:

1. Locate its first subsequent command. Write the robot ID in F of that exported CSV row, regardless of that command's category.
2. Examine that participation interval, up to its next shutdown or the end of the script.
3. If the interval contains a `mobility.move`, put the first such move's resolved numeric destination in G of the reminder row.
4. If the interval contains no move-equivalent command, leave G blank. A location report alone does not count as a move-equivalent command.
5. A final shutdown with no subsequent command produces no re-entry reminder.

Re-entry fields are generated during export. They need no additional editable fields on CommandSheet. Import/export must regenerate equivalent reminders from the commands and preserve the initial roster. Reference physical CSV row numbers, with the header as row 1, rather than the two-row workbook layout or workbook command IDs.

### Command sequence — Rule

If a robot will move after re-entry, its script must contain `mobility.report.location` in that participation interval **before the first move-equivalent command**. This provides its actual estimated position for normal movement handling.

`mobility.move`, `mobility.in2out` and `mobility.out2in` are move-equivalent. The first move-equivalent command after re-entry must be `mobility.move`, not a crossing macro. A crossing must have its normal staging move to the launch coordinate beforehand. For example:

- `mobility.report.location`
- `mobility.move` to the resolved `IN2OUT` launch position
- `mobility.in2out`

Use the existing supported syntax and macro configuration to resolve symbolic destinations. G contains the numeric coordinate so a person can read it and the helper map can plot it. Do not invent or duplicate launch coordinates in VBA.

### Runtime behavior — unchanged

There is **no 2 m re-entry gate**, new preparation cycle, new queue gate, new timeout, or recovery state. F/G do not alter execution. The explicit report and movement commands follow the existing runtime rules, distance/path checks, correction mechanism and failure handling. A failure follows the normal experiment failure path; it does not restart global preparation.

Non-movement commands before the first move keep their normal behavior. Do not delay them behind an invented re-entry-positioning procedure. Do not automatically insert a duplicate report or move into the exported command list.

**Operator SOP:** read the reminder row's H time and power on the robot sufficiently earlier for normal registration and readiness. Place it safely near the G location with a clear approach. Powering on exactly when its first command is due is not the intended operating practice. If G is blank, no movement-placement destination has been requested for that interval.

## 5. Shutdown and economical script timing

**Runtime change:** the default `system.shutdown` delay becomes **60 seconds**, replacing 300 seconds. An explicitly specified `time_to_wait` continues to override the default. The robot schedules poweroff and returns promptly; it must not block command processing by sleeping for the whole delay.

Experiment completion is already independent of robot polling. Preserve that behavior. A final shutdown is optional and is no longer required to keep a robot polling for completion.

**Writer guidance:**

- Omit the routine location report at the very beginning of a newly designed script: preparation already establishes location and readiness.
- Omit a redundant report after the last movement: the movement procedure already provides its location result.
- Do not add several minutes of empty time merely to finish a script. Aim for a short final tail, approximately one minute where the actual work permits it.
- For RSSI experiments, preserve the intended final stationary scan dwell and `scan.stop`. A short tail is not permission to discard the final required scan.
- Schedule shutdown so required movement, reports and scans can finish. Its 60-second delay begins when the robot receives the command, not automatically at the last movement's scheduled end.
- When planning shutdown followed by re-entry, allow for shutdown delay, operator handling, boot and registration. A command scheduled before poweroff is not a meaningful re-entry plan. Apply a clear timing diagnostic without inventing a universal boot-time estimate.

**Rule:** the template exports explicitly authored commands faithfully. It does not silently remove a location report, shorten a dwell, add a move or change an offset because it appears redundant. The guidance above governs script design; any checker advice must remain distinguishable from an existing mandatory rule.

## 6. Efficient scan design — Writer guidance

These are lessons from the radiomap campaign, not new runtime mechanisms:

- Select command spacing deliberately. Do not inherit three-minute intervals simply because an older script used them. Two-minute scans are the agreed ordinary baseline in the current workflow; faster trials are separate decisions and must obey the installed checker policy.
- Balance estimated finishing times across robots, accounting for scan dwell, transit, crossings and safe separation. Equal point counts alone do not ensure equal duration.
- Assign neighboring points together to avoid unnecessary zigzags and transfers. Do not balance workload by introducing unsafe route overlap.
- Avoid a long final period with only one busy robot when nearby work can reasonably be shared. The 16-point makeup run's 13 Delta movements versus 2 Alpha, 3 Bravo and 7 Charlie movements is a concrete example to avoid.
- Scan-start commands may share an offset. Do not introduce artificial staggering solely for scanning.
- Makeup plans should target genuinely missing or untrusted measurements, preserve stable grid IDs and retain data provenance.

**Operator SOP:** save and inspect partial results about ten minutes after the actual `t₀`. Check RSSI streams as well as movement progress; a robot can move normally while scanning is broken. Preparation success does not replace this data-quality check.

## 7. Placement-helper PNG

Generate a map using the workbook's StaticMap background and the exported CSV metadata:

- Mark every initial I/J coordinate with the robot name and “initial.”
- Mark each nonblank re-entry F/G coordinate with the robot name, re-entry sequence number if useful, and its **exported CSV row number**.
- Use distinguishable markers/colors and legible nearby labels. Handle repeated coordinates without obscuring robot/row identity.
- Include a short legend: initial robots must be present before preparation; re-entry times are found in column H of the marked CSV row; choose a safe nearby position with a clear approach.
- Do not draw a 2 m disk as though every point inside it were safe. Restrictions and other robots still matter.
- Do not draw a commanded movement line for manual relocation while a robot is offline.
- Re-entry rows with blank G have no map coordinate; list them in a note or legend rather than inventing a position.

Use a temporary copy or equivalent non-destructive rendering. StaticMap must retain its original values, formulas, formatting, restriction colors and existing objects after both success and error. VBA may drive the PNG export; the production implementation must be tested in Excel.

The figure is placement guidance, not proof of clearance or actual robot location. Row labels keep it useful even when preparation changes `t₀`; the operator refers to the corresponding H cell for the current time.

## 8. Workbook workflow

Keep InitialPoses as a convenient editing sheet for initial robot destinations and existing AP markers, but remove the requirement to export a separate initial-poses CSV. Initial heading is no longer required for preparation. Keep AP configuration separate from the robot roster.

The author edits commands and intended starting coordinates, runs checking, exports one registration CSV, and optionally generates the placement PNG. Importing that CSV must allow further editing without losing the roster or changing the meaning of the commands.

Update workbook buttons, preview sheets, validation feedback and instructions together. Existing planned-route previews must continue to work with the new initial roster. Preserve supported commands and the approved map geometry.

## 9. Implementation sequence and acceptance checks

| Step | Deliverable | Acceptance evidence |
|---|---|---|
| 1 | Shared new CSV contract and fixtures | One/multiple robots, scan-only robot, repeated re-entry, symbolic launch, blank-G interval, and roster longer than command list are represented unambiguously. |
| 2 | NMS parser and preparation input changes | I/J roster feeds preparation; duplicate/missing/malformed entries rejected; F/G produce no commands; 2 m placement allowance exercised at its boundary; existing retry/parallel preparation retained. |
| 3 | Shutdown-default change | Omitted argument gives 60 seconds; explicit values retained; immediate handler return and independent experiment completion preserved. |
| 4 | Workbook export/import and checker integration | A–E preserved; F/G reminders regenerated correctly; I/J roster retained; H formulas correct; re-entry missing its required report or staging move is diagnosed at the right row. |
| 5 | Helper PNG and workbook instructions | Every marker matches its exported coordinate/row; no fabricated relocation path; labels readable; StaticMap unchanged on success and failure. |
| 6 | Integrated regression and supervised confirmation | Offline fixtures and Excel button tests pass; then confirm preparation failure paths and the changed placement allowance with hardware in the planned lab session. |

Useful negative cases include malformed coordinate vectors, a command robot absent from the initial roster, re-entry move without a preceding report, macro before staging, invalid symbolic destination, and conflicting command order. Existing path and restriction checks must still reject their established invalid cases.

Do not claim VBA execution, actual 2 m movement performance or hardware failure-path behavior was validated solely by Python tests. Record what was tested offline, in Excel, and on robots separately.

## 10. Explicit exclusions from this revision

- No backward-compatibility implementation for obsolete registration formats.
- No S8 state, alternative-heading recovery, new re-entry controller or automatic re-entry retry.
- No automatic detour planning or newly invented intermediate-movement procedure.
- No additional runtime placement check using F/G.
- No arbitrary delays added to make status screens visible.
- No changes to restriction-map geometry or the established movement/correction algorithm.
- No automatic registration, robot commands or experiment launch while developing the template.

The guiding principle is to make the author's intent and the operator's actions clear, while reusing the runtime mechanisms that already work.
