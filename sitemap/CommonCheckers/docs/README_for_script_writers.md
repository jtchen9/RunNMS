# AutoLab DemoRoom remote script-writer guide

This package lets a remote script writer create and preflight an AutoLab
experiment without access to the private NMS codebase, lab network, robots,
access points, Redis, or registration API.

The workbook provides the authoring GUI. Python code in
`sitemap\CommonCheckers` is the only authority that decides whether the CSV
files pass public-facing validation. The lab registration API reruns the same
CommonCheckers code before registering the experiment.

## 1. Required environment

- 64-bit Windows 10 or Windows 11
- Desktop Microsoft Excel with VBA macro support
- 64-bit CPython 3.12.x (3.12.13 has been tested)
- NumPy 2.3.5
- A writable local folder for the extracted package

Excel Online, Google Sheets, and LibreOffice are not supported because the
template uses VBA. Internet access is needed only to install Python or its
dependency.

## 2. Verify Python and install the dependency

Open PowerShell in the extracted package root—the folder containing
`sitemap`—and run:

```powershell
python --version
python -m pip install -r .\sitemap\CommonCheckers\requirements.txt
python -c "import numpy; print(numpy.__version__)"
```

The last command should print `2.3.5`.

If `python` is not found, install 64-bit Python 3.12 and select the installer
option that adds Python to `PATH`. Alternatively, put the full path to
`python.exe` in `PreflightConfig!B4`. B4 must contain one executable name or
path; do not enter a launcher command such as `py -3.12`.

## 3. Extract and trust the package

1. Obtain the ZIP file and its SHA-256 value from the lab operator.
2. Optionally verify the ZIP:

   ```powershell
   Get-FileHash .\AutoLab_DemoRoom_Public_<version>.zip -Algorithm SHA256
   ```

3. If Windows shows an **Unblock** checkbox in the ZIP file's Properties,
   select it before extraction.
4. Extract the entire ZIP. Do not move the workbook out of its package
   folders.
5. Open:

   ```text
   sitemap\DemoRoom\script_authoring\AutoLab_DemoRoom_Template.xlsm
   ```

6. Select **Enable Editing** and **Enable Content** only if the ZIP came from
   the expected lab operator and its hash matches.

Corporate policy may block VBA or `WScript.Shell`. Ask the lab operator or IT
administrator for an approved location or signing policy; do not weaken
organization-wide security settings.

## 4. Initialize the workbook

Run `InitializeScriptTemplate` before authoring a new script.

Initialization:

- derives the package root from the workbook's own location;
- verifies `sitemap\CommonCheckers`, `sitemap\DemoRoom`, and the workbook-local
  Python bridge;
- writes the package root to `PreflightConfig!B5`;
- sets B4 to `python` only if B4 is blank;
- clears B6 so the bridge beside the workbook is used;
- removes stale paths and results from `PreflightStatus`;
- reloads the fixed AP roster; and
- creates five starter `mobility.report.location` commands.

After initialization, check:

| Cell | Expected value |
|---|---|
| `PreflightConfig!B4` | `python`, or the full path to `python.exe` |
| `PreflightConfig!B5` | the extracted package root containing `sitemap` |
| `PreflightConfig!B6` | blank |

If initialization reports an invalid layout, re-extract the complete ZIP and
leave the workbook in `sitemap\DemoRoom\script_authoring`. The layout is
verified before command rows are cleared or rebuilt.

## 5. Create the experiment

1. In `InitialPoses`, enable every participating robot and enter its intended
   starting pose. AP locations are fixed roster data and should not be edited.
2. In `CommandSheet`, use dropdowns to choose the target, time, and command.
3. Run `ApplyCommandGuiRules` when needed to refresh command-specific
   parameter cells and the device dropdown.
4. Enter values in the displayed parameter cells.
5. Run `BuildArgsJson` to construct the JSON arguments.
6. Run `RefreshMapAtSelectedCommandRow` when the movement map is useful.
7. Repeat until the complete experiment timeline is represented.

`CommandCatalog` and `ParameterCatalog` document the commands and fields
exposed by the workbook. Dropdowns and VBA make authoring easier; they do not
determine validation PASS or FAIL.

For a ramp crossing, stage the robot with the reserved launch name in both
`mobility.move` coordinate cells:

- `IN2OUT`, `IN2OUT` immediately before `mobility.in2out`;
- `OUT2IN`, `OUT2IN` immediately before `mobility.out2in`.

The two commands must apply to the same robot, with no intervening mobility
command for that robot. Commands for other devices may appear between them.
CommonCheckers first verifies this sequence, then resolves the name from the
site `macro_policy.json` and applies every ordinary movement, distance, map,
furniture, and robot-clearance rule to the numeric staging move.

## 6. Run the authoritative preflight

Run `RunCommonCheckers`. It exports:

```text
sitemap\DemoRoom\script_authoring\generated\experiment_script.csv
sitemap\DemoRoom\script_authoring\generated\initial_poses.csv
```

The workbook initially writes the symbolic staging values into
`experiment_script.csv`. On PASS, the Python helper atomically replaces that
file with the NMS-ready numeric version. On FAIL, the symbolic source remains
available for correction and must not be submitted.

It also writes:

```text
validation_report.json
validation_feedback.csv
checker_stdout.txt
```

Review `PreflightStatus`, `ValidationReport`, and the feedback in
`CommandSheet`. Correct the first reported problem and rerun. Repeat until the
result is `PASS`.

Successful JSON construction, dropdown restrictions, and map refreshes are
not validation. Only CommonCheckers determines public preflight PASS or FAIL.

## 7. Submission and validation evidence

The two files required for registration are:

1. `experiment_script.csv`
2. `initial_poses.csv`

Also send the matching `validation_report.json` as recommended preflight
evidence. A passing report contains `ok: true`, `status: "pass"`, zero errors,
checker/site versions, and SHA-256 hashes for both CSV files.

The report is not a digital signature and is not trusted as authorization.
The lab operator reruns the same public CommonCheckers code during
registration. Report hashes bind the report to the CSVs that the writer
checked and help detect accidental replacement.

After PASS:

- do not edit either CSV;
- keep both CSVs and the report together;
- send them through the lab's approved transfer channel; and
- if either CSV changes, rerun `RunCommonCheckers` and send the new report.

`validation_feedback.csv` and `checker_stdout.txt` are normally unnecessary.
Include them only when troubleshooting.

## 8. Troubleshooting

| Symptom | Action |
|---|---|
| `python` is not recognized | Install Python 3.12 or put the full `python.exe` path in B4. |
| `No module named numpy` | Run the dependency installation command in section 2. |
| Package root or helper is missing | Re-extract the full ZIP and rerun initialization without moving the workbook. |
| Macros do not run | Check the ZIP source/hash, Windows Unblock state, Excel Trust Center, and corporate policy. |
| Checker reports FAIL | Correct the first issue and rerun; later issues may then appear. |
| Checker runner error | Read `generated\checker_stdout.txt` and send it to the operator if help is needed. |

## 9. Integrity and privacy

The release ZIP SHA-256 supplied by the operator verifies the distributed
package. `MANIFEST.sha256` and `MANIFEST.json` list packaged files. The
validation report separately hashes the generated CSV inputs.

Experiment CSVs contain device names, timing, actions, arguments, and intended
robot positions. Handle them as lab experiment data and use the approved
transfer channel.
