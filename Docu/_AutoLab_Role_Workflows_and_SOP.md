# AutoLab: role workflows and operating reference

Public edition · Revision 1 · 2026-11-25

This guide connects experiment design, lab operation, and research datasets. It records the current agreed procedure, including the single-file registration format, preparation retries, independent experiment completion, and radiomap conversion. It supersedes older instructions in these areas. It does not change software or prescribe new robot recovery behavior.

API access is arranged with an authorized lab or dataset operator. Access credentials and authentication instructions are intentionally excluded from this public guide.

## 1. Roles and handoffs

| Role | Responsibility | Main tools |
|---|---|---|
| [W — Script writer](#2-script-writer-workflow-w) | Turn a research objective into a checked experiment script and operator instructions. The writer may be an external contributor. | Site XLSM template, CommonCheckers, route and placement previews |
| [O — Lab operator](#3-lab-operator-workflow-o) | Maintain the lab, accept scripts, prepare robots, run experiments, check data, and preserve results. | NMS APIs, LiveDemo, DataLake, robot maintenance tools |
| [R — Researcher / data collector](#4-researcher-workflow-r) | Select evidence, construct and assess datasets, manage survey revisions, and use data reproducibly. | DataLake downloads, NMS offline converter, dataset management APIs |

One person may fill several roles. Role ownership describes work responsibility, not automatic API access. Each catalog task has one or two responsible roles; the first listed is the lead.

| Handoff | Required material |
|---|---|
| W → O | Checked command CSV, template/checker version, planned routes, placement/re-entry guidance, expected duration, AP configuration, and intended data coverage |
| O → R | Site/experiment/session/checkpoint identities, source ZIPs, matching site resources, AP configuration records, and relevant incidents |
| R → W | Missing or untrusted grid/radio pairs, required configuration, and a focused makeup request |

## 2. Script writer workflow (W)

| Order | Task | Result |
|---|---|---|
| 1 | [T01 Define the survey](#t01-define-the-survey) | Specify coverage, radios, timing, and acceptable data quality with the researcher. |
| 2 | [T03 Author the single CSV](#t03-author-the-single-csv) | Use the current published site template and initial roster. |
| 3 | [T04 Plan AP topology](#t04-plan-and-verify-ap-topology) | Include the required star-backhaul setup and configuration assumptions. |
| 4 | [T05 Check routes and timing](#t05-check-routes-and-timing) | Export a checked CSV and inspect joint and individual routes. |
| 5 | [T06 Plan shutdown and re-entry](#t06-plan-shutdown-and-re-entry) | Make operator timing and placement instructions explicit. |
| 6 | [T07 Submit and schedule](#t07-submit-and-schedule) | Obtain operator acceptance before the lab run. |
| 7 | [T15 Plan makeup coverage](#t15-select-sessions-and-plan-makeup-coverage) | Use measured gaps to produce the next script, if needed. |

**Workflow completeness:** a script alone is not the full handoff. Include its research objective, site/configuration assumptions, placement guidance, and expected coverage. External writers do not need to operate the live NMS or import datasets themselves.

## 3. Lab operator workflow (O)

| Order | Task | Result |
|---|---|---|
| 1 | [T02 Maintain site resources](#t02-maintain-site-resources) and [T21 Monthly mechanical maintenance](#t21-monthly-mechanical-maintenance) | Keep maps, tags, restrictions, and robots consistent with the physical lab. |
| 2 | [T07 Submit and schedule](#t07-submit-and-schedule) | Accept the script and reserve enough room time, including preparation and cleanup. |
| 3 | [T08 Prepare room and placement](#t08-prepare-room-and-placement) | Clear tag blockers and travel paths; position every participant. |
| 4 | [T09 Check power and running software](#t09-check-power-and-running-software) | Verify supplies, battery reserve, and the software actually running. |
| 5 | [T04 Plan AP topology](#t04-plan-and-verify-ap-topology) | Confirm intended backhaul and radio settings. |
| 6 | [T10 Register and observe preparation](#t10-register-and-observe-preparation) | All participants become ready; the NMS announces the actual start time. |
| 7 | [T11 Check data after ten minutes](#t11-check-data-after-ten-minutes) | Confirm useful RSSI and movement data before leaving the lab. |
| 8 | [T12 Monitor and handle interruptions](#t12-monitor-and-handle-interruptions) and [T06 Re-entry](#t06-plan-shutdown-and-re-entry) | Preserve evidence and perform any scheduled operator actions. |
| 9 | [T13 Verify completion and checkpoints](#t13-verify-completion-and-checkpoints) | Confirm final data was saved and the session concluded. |
| 10 | [T14 Record and hand over](#t14-record-and-hand-over) | Give the researcher identifiable data and configuration records. |
| 11 | [T20 Post-experiment care and calibration](#t20-post-experiment-care-and-calibration) | Charge, inspect, and run history-based calibration after overnight runs. |

**Workflow completeness:** preparation success does not establish scan health; dashboard refresh does not establish fresh measurements; robot shutdown does not establish successful archiving. These are three separate checks. Maintain the run record even when a run fails.

## 4. Researcher workflow (R)

| Order | Task | Result |
|---|---|---|
| 1 | [T01 Define the survey](#t01-define-the-survey) | Agree on grid/radio coverage and research requirements. |
| 2 | [T02 Site resources](#t02-maintain-site-resources) and [T14 Handoff](#t14-record-and-hand-over) | Obtain configuration and resources that match the historical runs. |
| 3 | [T15 Select sessions](#t15-select-sessions-and-plan-makeup-coverage) | Choose one checkpoint version per contributing session. |
| 4 | [T16 Download dataset sources](#t16-download-dataset-sources) | Collect one complete source ZIP per selected session/version. |
| 5 | [T17 Convert raw inputs](#t17-convert-raw-inputs) | Build one radiomap-v1 dataset ZIP with the offline NMS tool. |
| 6 | [T18 Validate and manage datasets](#t18-validate-and-manage-datasets) | Validate, explicitly import, and verify the saved survey revision. |
| 7 | [T19 Assess coverage and use data](#t19-assess-coverage-and-use-data) | Document quality limits and request makeup coverage if needed. |
| 8 | [T14 Record and hand over](#t14-record-and-hand-over) | Retain reproducible inputs, output ZIP, and revision information. |

**Workflow completeness:** distinguish successful execution from complete coverage, and distinguish an archived checkpoint from an imported dataset. Do not merge incompatible physical layouts or silently treat missing radio measurements as zero.

## 5. Shared task catalog

### T01 Define the survey

**Responsible:** [R](#4-researcher-workflow-r), [W](#2-script-writer-workflow-w). **Type:** planning convention.

Agree on the site, experiment name, grid resolution, expected APs/bands, number of useful observations, time diversity, and output quality criteria. Decide whether the survey is one run or several sessions of the same experiment. A makeup session should retain the experiment name and use a different session identity.

Specify whether the goal is spatial coverage, repeated measurements, or temporal behavior. Consecutive scans and separated revisits are different sampling designs. State the desired design rather than treating all samples as interchangeable.

**Output:** a short survey brief and acceptance criteria.

### T02 Maintain site resources

**Responsible:** [O](#3-lab-operator-workflow-o), [R](#4-researcher-workflow-r). **Type:** site maintenance plus converter input contract.

Maintain these reusable inputs under:

`D:\Data\_Action\_RunNMS\sitemap\<site>\script_authoring`

| File | Purpose |
|---|---|
| `<site>.svg` | Standard SVG layout, with coordinate mapping consistent with StaticMap |
| `AutoLab_<site>_Template.xlsm` | Authoring workbook containing the documented StaticMap sheet |
| `Grid_<site>.csv` | Exactly two columns, `x,y`, with grid coordinates in metres |

StaticMap must describe its map-cell range, cell size, origin, axis direction, restriction colours, and SVG mapping. In the revised DemoRoom workbook, the definition table begins at row 122, keys are in column C, values in S, and the map range is `C4:DL117`. Cell size is 0.1 m; the lower-left corner of C117 is (0,0). Other sites must supply their own definitions.

Retain historical resource copies when layout, restrictions, or grid changes. Record AP locations and radio configuration, including BSSID, SSID, band, channel, bandwidth, and transmit-power information. A declared power mode of `default` is not a measured numeric transmit power. Unknown values remain unknown.

**Output:** a versioned site package and records of configuration changes. Historical sessions must not be assigned today's changed layout without verification.

### T03 Author the single CSV

**Responsible:** [W](#2-script-writer-workflow-w). **Type:** file-format contract plus authoring convention.

Use the current XLSM template. The former separate initial-position CSV is no longer part of this flow.

| Columns | Meaning |
|---|---|
| A–E | Robot/scanner, time offset, category, action, and command arguments |
| F–G | Re-entry robot ID and optional placement coordinates: operator reminders |
| H | Time-offset display used by the operator |
| I–J | Initial participant roster and intended starting coordinates used by preparation |

Include all participants in I/J, even robots whose scripted work starts later. Initial headings need not be specified by the operator; the existing movement procedure uses the preferred heading.

Preparation already obtains location and moves robots to their initial positions. Do not add a redundant initial `mobility.report.location` merely to repeat preparation. Likewise, do not append a final location report when the last movement already supplies the required report.

Start scanning when intended, allow for useful measurements at destinations, and stop scanning when its work is complete. Simultaneous `scan.start` commands are acceptable; movement timing must still pass the current route and timing rules.

**Output:** one command CSV with complete preparation and operator guidance information.

### T04 Plan and verify AP topology

**Responsible:** [W](#2-script-writer-workflow-w), [O](#3-lab-operator-workflow-o). **Type:** script convention and physical verification.

For surveys requiring a star mesh, issue `ap.backhaul.steer` for each agent AP toward the intended controller using the current AP command contract. Do not guess MAC addresses or command arguments. Verify the resulting association and band; a requested steer alone is not evidence of the resulting topology.

Record relevant radio configuration changes and their timing. Keep makeup sessions consistent with the intended survey configuration, or explicitly separate differing conditions in the research design.

**Output:** known AP topology and recorded radio settings.

### T05 Check routes and timing

**Responsible:** [W](#2-script-writer-workflow-w), [O](#3-lab-operator-workflow-o). **Type:** software checks plus visual review.

Run `RunCommonCheckers` to produce the checked CSV in the configured `generated` folder and open the placement preview. Use `PreviewPlannedRoutes` to inspect the joint route and individual robot routes. Preserve existing workbook button bindings when updating the template.

Review restrictions, bump launch positions, robot separation, route crossings, timing, and balanced workloads. Group neighbouring targets to avoid unnecessary zig-zag routes. Before a crossing macro, normally issue `mobility.move` to its launch position, followed by `mobility.in2out` or `mobility.out2in`.

Use the installed checker rules and the approved experiment cadence. Do not restore an obsolete universal three-minute spacing assumption. Faster schedules still need suitable measurement coverage and multi-robot route checks.

A shutdown followed by re-entry breaks the physical route: the robot may be placed elsewhere. Do not draw a straight travel segment across that interval.

**Output:** current PASS result plus route and placement previews reviewed by the operator. A checker result for an earlier CSV does not validate a modified CSV.

### T06 Plan shutdown and re-entry

**Responsible:** [W](#2-script-writer-workflow-w), [O](#3-lab-operator-workflow-o). **Type:** command behavior plus operator convention.

`system.shutdown` has an optional `time_to_wait` in seconds; the current default is **60 seconds**. The robot schedules shutdown locally and returns promptly. Experiment completion is now independent of robot polling; the old 300-second default is obsolete.

After a scripted shutdown, the next command for that robot marks re-entry. Put its ID in column F on that row. If movement follows before its next shutdown, put the intended re-entry placement coordinate in G, matching the first `mobility.move` destination. If no movement follows, G may be blank. F/G do not themselves issue commands or establish a measured location.

Power the robot on early enough for boot and registration before its first scheduled command; allow roughly three minutes as an operating starting point, and verify readiness. Include `mobility.report.location` before the first move-equivalent command after re-entry. Use the existing movement and correction mechanism; there is no additional 2 m re-entry check introduced by this SOP.

Confirm actual power-off before powering on again. Deleting an experiment is not proof that a locally scheduled shutdown has been cancelled. For debugging, use one shutdown at a time so pending commands do not obscure the result.

**Output:** clear operator power-on times and placement reminders, without redundant shutdowns.

### T07 Submit and schedule

**Responsible:** [W](#2-script-writer-workflow-w), [O](#3-lab-operator-workflow-o). **Type:** operating convention.

The operator reviews the writer's handoff, checks room availability and maintenance status, and confirms that required APs and robots are available. Reserve time for preparation, the experiment, completion processing, and room cleanup. A public script proposal is not an instruction to start the lab automatically.

**Output:** an accepted script and agreed operating window.

### T08 Prepare room and placement

**Responsible:** [O](#3-lab-operator-workflow-o). **Type:** pre-run SOP.

Verify tags are at their mapped positions and visible. Remove ladders, monitors, and other temporary blockers. Check travel paths, bump areas, wheel clearance, and changes caused by tours or furniture movement.

Use the placement map to put robots roughly near their I/J destinations, within the configured preparation allowance (currently 2 m), on a clear side of the destination with an unobstructed path. Avoid restriction zones. Choose an initial orientation that allows tag detection; precise manual heading placement is unnecessary.

**Output:** all participants physically ready for preparation, including later entrants.

### T09 Check power and running software

**Responsible:** [O](#3-lab-operator-workflow-o). **Type:** pre-run SOP.

Record each robot's starting battery percentage and check both Pi power and motor/camera power. Estimate reserve using that battery's observed runtime; distinguish weak batteries from the rest. For long surveys, fully charged batteries are preferable to relying on a marginal estimate.

Confirm required changes have been pulled on every participant and the relevant service has restarted or the robot rebooted. Verify the running configuration, not only the repository contents. Confirm scanning works and that `scan_wifi.sh` is executable on each robot after deployment.

Check NMS/backend connectivity and secure the NMS network connection. Keep the NMS running through experiment completion. Document software and calibration versions.

**Output:** a recorded power/software readiness check.

### T10 Register and observe preparation

**Responsible:** [O](#3-lab-operator-workflow-o). **Type:** NMS behavior plus operator observation.

Register the single CSV through NMS `POST /cmd/_load_csv_file`. Successful acceptance begins the preparation flow; it does not mean all robots are ready or the command timeline has started.

All participants must be present for health checks. Observe each robot's displayed preparation step, including health, location acquisition, movement to its starting position, and confirmation. Preparation movements are issued concurrently where the implemented checks permit them.

The current retry wait is **60 seconds**. If preparation fails, correct the displayed cause during the wait and observe the next attempt. Preparation failure is recoverable through this retry flow until the experiment is deleted. Do not assume a regularly refreshed status panel proves command polling is healthy.

The NMS assigns the actual start time after preparation, rounded to the minute. Check that LiveDemo and DataLake show the accepted start time. Use that time for all subsequent offsets.

The currently agreed movement thresholds are **0.40 m for the first correction decision and 0.30 m for final confirmation**, using the existing mechanism. They are not a new recovery state and not dataset-quality thresholds.

**Output:** all participants ready and an agreed actual start time, or a clearly diagnosed retry/deletion.

### T11 Check data after ten minutes

**Responsible:** [O](#3-lab-operator-workflow-o), [R](#4-researcher-workflow-r). **Type:** mandatory operating SOP; not an automatic software gate.

About ten minutes after the actual start, save a partial checkpoint through the operator interface and examine the resulting data before leaving the lab.

Check that every expected robot produces useful RSSI scans, measurement timestamps advance, expected AP/band readings are present, movement and location reports correspond to the script, and there are no unexplained stops or accumulating delays. Check sample content, not just archive size or dashboard activity.

Distinguish acquisition time from upload time. A network outage may delay upload while measurements later arrive in batches; archive growth alone cannot establish that all data has arrived. Record the last measurement time per robot and any backlog or missing intervals.

**Output:** a recorded healthy-data check, or a specific action and follow-up check before unattended operation.

### T12 Monitor and handle interruptions

**Responsible:** [O](#3-lab-operator-workflow-o). **Type:** incident SOP.

Monitor preparation/running state, mobility stop reasons, scan freshness, and connectivity. If a run stops, preserve a partial checkpoint, relevant robot service logs, NMS logs, and mobility traces before cleanup or reboot when practicable. Record physical observations separately from estimated locations.

Do not declare the entire dataset useless merely because a session failed. Preserve usable partial data for coverage analysis. Do not restart from guessed positions; obtain the available current location evidence and plan the next script accordingly.

**Output:** preserved evidence and a documented continue, stop, or makeup decision.

### T13 Verify completion and checkpoints

**Responsible:** [O](#3-lab-operator-workflow-o), [R](#4-researcher-workflow-r). **Type:** backend behavior plus verification SOP.

The NMS completion task is independent of robot polling. All robots may be off while the NMS completes the session. Verify that DataLake records the concluded run in History and that its saved data is accessible; shutdown alone is not confirmation.

Each run of an experiment/session produces a checkpoint/version. Repeated runs may produce multiple versions. Depending on the run outcome and saved point, a checkpoint can contain full or partial data. Automatic saving needs no manual operator action. Mid-run saving is an operator action through the supported interface.

The backend checkpoint endpoint is:

`POST /lake/save_experiment_checkpoint/{site_id}/{experiment_id}/{session_uid}`

Use the deployed interface for its current request fields. This guide does not invent a request body or equate an archive's successful save with successful execution.

**Output:** an identified, accessible checkpoint and recorded completion/failure outcome.

### T14 Record and hand over

**Responsible:** [O](#3-lab-operator-workflow-o), [R](#4-researcher-workflow-r). **Type:** reproducibility SOP.

For each run, retain: site and experiment name, session UID, checkpoint version, actual start/end, script and checker version, participating robots, software/calibration versions, AP configuration, site-resource versions, starting/ending battery levels, ten-minute check result, interruptions, and source download names.

For each dataset, retain: survey ID, survey revision, selected session/checkpoint versions, converter settings/version, source hashes where available, validation results, missing coverage, and revision reason. Keep durable copies before deleting temporary workspaces.

**Output:** a run record and a dataset provenance record that another person can follow.

### T15 Select sessions and plan makeup coverage

**Responsible:** [R](#4-researcher-workflow-r), [W](#2-script-writer-workflow-w). **Type:** research convention plus converter constraint.

Select one suitable checkpoint version per contributing session, normally the most complete suitable version. Selection is explicit: do not merge every version of the same session. Partial runs can contribute useful observations.

The current converter merges sessions of the same site and experiment name. Check physical layout and intended AP configuration consistency. Preserve recorded radio-setting changes rather than silently treating all settings as identical.

Request makeup coverage for missing or untrusted grid/radio pairs, including points visited previously but lacking suitable data. Group nearby targets and balance workloads. Use the same experiment name and a different session identity for makeup runs.

**Output:** a selected source inventory and, when needed, a focused makeup request.

### T16 Download dataset sources

**Responsible:** [R](#4-researcher-workflow-r). **Type:** DataLake procedure.

Open [DataLake](https://6g-private.com/lake), choose the Experiment tab, select the required experiment/session/version, and use **Download dataset source**. Download one complete ZIP for each selected session/version.

Place the selected ZIPs directly in `D:\_Download\DataLakeWS`. Filenames are not session identifiers; the converter checks embedded identities. Do not place multiple versions of one session in this input folder.

No separate Event ZIP, Report ZIP, or command CSV is required for this conversion flow. The dataset-source ZIP provides the session data and embedded script.

**Output:** one source ZIP per selected session/version.

### T17 Convert raw inputs

**Responsible:** [R](#4-researcher-workflow-r). **Type:** offline NMS tool procedure.

Install `build_radiomap_dataset.py` and `radiomap_export_core.py` under `D:\Data\_Action\_RunNMS\tools`. This is an NMS-side offline tool, not a webserver module.

In `build_radiomap_dataset.py`, edit paths and parameters under `if __name__ == "__main__":`, then click Run Python File in VS Code. No command-line argument workflow is required.

Supply the workspace, the three raw site files from T02, expected site/experiment, stable site and survey identifiers, survey revision, AP selection, quality settings, output filename, and relevant metadata assertions. Keep the survey ID stable across revisions; use a new revision for changed published content.

The tool reads the source ZIPs, SVG, StaticMap workbook, and two-column grid directly. Intermediate site/restriction JSON files are generated internally; people do not prepare them manually. The tool does not execute workbook macros or issue robot commands.

Output goes under `D:\_Download\DataLakeWS\output`. Review `validation/validation_report.json` and `validation/missing_coverage.json` within the generated package. Keep source and output copies until validation/import and provenance preservation are complete.

**Output:** a `radiomap-v1` dataset ZIP and conversion diagnostics.

### T18 Validate and manage datasets

**Responsible:** [R](#4-researcher-workflow-r). **Type:** confirmed backend API contract. API access is arranged with an authorized dataset operator.

The input is the **converted dataset ZIP**, not a raw DataLake source ZIP.

| Operation | Method and route | Parameters and behavior |
|---|---|---|
| Validate | `POST /dataset/add_radio_map_dataset` | Multipart `file` contains the dataset ZIP; `mode=validate`. Validation is the default and performs no Redis reads or writes. |
| Import | `POST /dataset/add_radio_map_dataset` | Multipart `file`, explicitly `mode=import`. Identical ZIPs are no-ops; changed content requires a new survey revision. |
| List | `GET /dataset/get_radio_map_datasets` | Optional query `survey_id` filters saved revisions. |
| Inspect | `GET /dataset/get_radio_map_dataset/{survey_id}/{revision}` | Nonempty survey ID; revision is an integer of at least 1. Returns a revision summary. |
| Delete | `DELETE /dataset/delete_radio_map_dataset/{survey_id}/{revision}` | Query `confirm=true` is required for permanent deletion; default is false. |

Validate first, inspect reported validation findings, then explicitly import. Inspect the imported survey/revision and compare identity, counts, and coverage with the conversion output. A validation-only request does not create a Redis dataset.

Delete only an intentionally selected revision. Other revisions and DataLake archives remain. Existing game questions holding snapshots may remain until their usual expiry. The uploaded ZIP is staged temporarily outside the backend codebase and the temporary file is removed after processing succeeds or fails.

**Output:** a verified saved survey revision, or documented validation failures to correct.

### T19 Assess coverage and use data

**Responsible:** [R](#4-researcher-workflow-r). **Type:** research quality procedure.

Assess coverage by grid point and expected radio, not only by destination visits or experiment success. Preserve missing values as missing, with reasons. Do not convert absent RSSI to zero or substitute a stronger unrelated BSSID.

Use scan acquisition timestamps for measurements. The current converter uses scheduled scan windows, a configurable settling interval (45 seconds by default), recent prior status, and a default estimated target-error limit of 0.30 m. These criteria do not prove the robot was stationary or that every location solve was fresh: the source exports do not contain full movement traces and consistently timestamped solves. Record this limitation in downstream research.

Keep excluded and unassigned measurements available for review. A successful import may still represent a partial dataset. The simple grid contains no phase/room columns; retain the original detailed catalog separately if A/B/C/D grouping is needed.

Record survey/revision in analyses, together with filtering, averaging, and train/test split choices. Consider temporal or session separation when evaluating generalization rather than randomly splitting nearly identical neighbouring observations without explanation.

**Output:** a quality statement, reproducible research input, and any makeup request.

### T20 Post-experiment care and calibration

**Responsible:** [O](#3-lab-operator-workflow-o). **Type:** post-run SOP.

Record ending battery levels, place robots on charge, and inspect for heat, unusual noise, wheel wear, loose parts, and unstable behavior. Allow about five hours for charging as a planning convention, then verify the actual level; charger type and battery condition change the required time. Keep weak-battery charging measurements separate from normal batteries.

After every overnight experiment, run the established history-based calibration procedure for each robot using its mobility trace. Review evidence and proposed changes, apply appropriate calibration, and record the version deployed before the next run. Failed RSSI collection does not automatically invalidate usable mobility history.

If long-run behavior suggests accumulating resources, retain diagnostics such as file-descriptor counts and logs before reboot. Rebooting may restore operation but does not establish that a leak is fixed.

**Output:** charged, inspected robots and a recorded calibration/maintenance outcome.

### T21 Monthly mechanical maintenance

**Responsible:** [O](#3-lab-operator-workflow-o). **Type:** scheduled maintenance SOP.

Inspect motors, wheels and rollers, mounts, fasteners, camera mounts, battery connections, and cables monthly, or sooner after symptoms or impacts. Replace worn or unreliable parts as necessary. After changes affecting movement or camera alignment, perform appropriate movement/preflight checks and review calibration before unattended operation.

**Output:** a dated maintenance record and a clear ready/not-ready decision for each robot.

## 6. Version terminology

| Term | Meaning |
|---|---|
| Experiment | The named script/research activity; makeup sessions retain the intended common experiment name. |
| Session | An identified execution grouping belonging to that experiment. |
| Checkpoint/version | A saved full or partial state of an experiment/session in DataLake. |
| Dataset survey | The research dataset assembled from selected session checkpoints and matching site resources. |
| Survey revision | A version of that assembled dataset; changed published content requires a new revision. |

Checkpoint versions and survey revisions are different identifiers. Preserve both in provenance.

## 7. Maintaining this guide

The lab operator maintains operational rules; the researcher maintains conversion and dataset details with the operator. Writers receive the current guide and template package when authoring scripts. Update this document when behavior changes and record the corresponding software/template versions.

Keep these distinctions explicit in future revisions:

- A script convention is not automatically a software-enforced rule.
- Preparation acceptance is not experiment start.
- Mobility completion is not proof of RSSI coverage.
- Archive completion is not proof of dataset completeness.
- A checkpoint download is not a dataset import.

The API methods, routes, and modes above were confirmed from the deployed Swagger descriptions and operator clarification. Exact response fields and private access procedures remain in the deployed API documentation and authorized operator instructions. Do not insert credentials into this public guide.
