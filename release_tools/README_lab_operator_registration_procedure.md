# AutoLab lab-operator script registration procedure

Audience: operator of an AutoLab test site such as `DemoRoom`.

This document is internal. It belongs in `release_tools` and must not be
included in the public script-writer ZIP.

## 1. Validation and registration principle

The writer and operator must use identical public-facing validation code from:

```text
sitemap\CommonCheckers
```

The writer's workbook runs it during preflight. The registration endpoint runs
it again against the two uploaded CSV files before any registration mutation.

The writer's `validation_report.json` is useful evidence, but it is not a
signature and does not replace operator-side validation. Registration succeeds
only when the NMS's own copy of CommonCheckers passes.

Private registration checks may additionally inspect live state, such as the
device whitelist and existing experiment registry. They must not duplicate or
weaken public command, argument, timeline, initialization, or map rules.

## 2. Files received from the script writer

Required:

```text
experiment_script.csv
initial_poses.csv
```

Recommended:

```text
validation_report.json
```

Troubleshooting only:

```text
validation_feedback.csv
checker_stdout.txt
```

Keep the received files together in a new intake directory. Do not edit either
CSV. If a correction is needed, ask the writer for a newly validated pair.

## 3. Verify writer-side evidence

First confirm that the writer used the intended public release version.

Open `validation_report.json` and check:

- `ok` is `true`;
- `status` is `pass`;
- `error_count` is `0`;
- `site_id` is the intended test site;
- CommonCheckers and site versions match the accepted release; and
- both reported CSV hashes match the received files.

Calculate local hashes:

```powershell
Get-FileHash .\experiment_script.csv -Algorithm SHA256
Get-FileHash .\initial_poses.csv -Algorithm SHA256
```

The report normally prefixes hash values with `sha256:`. Compare the hexadecimal
part case-insensitively.

A missing or mismatched report does not justify bypassing validation. Stop and
resolve the discrepancy, or treat the two CSVs as an unverified submission and
subject them to the complete operator workflow.

## 4. Choose the experiment identity and start time

The `/cmd/_load_csv_file` endpoint derives `experiment_id` from the uploaded
command CSV filename stem.

Create a renamed copy for upload, for example:

```powershell
Copy-Item `
    .\experiment_script.csv `
    .\M2_test.csv
```

Renaming a copy does not alter its content or SHA-256. Do not change the CSV
contents.

Choose:

- a unique, meaningful experiment ID/filename;
- an optional `session_id`;
- a future local `t0` in the NMS time format; and
- `replace_existing=false` for normal registration.

The end time is derived from `t0` plus the maximum accepted
`t_offset_sec`. Ensure the lab, robots, APs, and operator are ready before
choosing `t0`.

## 5. Check live lab readiness

Before registration:

- confirm the NMS identifies itself as the intended site, normally `DemoRoom`;
- confirm NMS time and timezone are correct;
- confirm every target device expected by the script is in the live whitelist;
- confirm required robots and APs are online and polling;
- confirm robots are physically at the intended initial poses;
- confirm the test area is clear and safety mechanisms are operational;
- confirm no experiment is currently registered; and
- preserve any logs needed from the preceding run.

AutoLab currently uses a one-experiment lifecycle: a completed experiment
remains registered until an operator explicitly deletes it.

Do not use `replace_existing` as a substitute for checking or deleting the
existing experiment. It does not authorize overwriting an unrelated
registration.

## 6. Delete an old registration only when intended

If an old experiment must be removed, call:

```text
POST /cmd/_delete_experiment
```

This is an intentional state-changing operation. It clears the experiment
registry and pending command queues. According to the current API contract,
collected result history, scanner registry, whitelist, AP metadata, robot pose,
and mobility runtime state are preserved.

Review the response and confirm:

- `status` is `ok`;
- the prior registered count is expected; and
- queue cleanup completed.

Do not call this endpoint while an experiment should still be running.

## 7. Register the two CSV files

The registration endpoint is:

```text
POST /cmd/_load_csv_file
```

It is a multipart form upload with:

| Field | Meaning |
|---|---|
| `t0` | Required future NMS-local start time |
| `session_id` | Optional session identifier |
| `replace_existing` | Normally `false` |
| `csv_file` | Command CSV; filename stem becomes `experiment_id` |
| `initial_poses_file` | Matching initial-pose CSV |

Example using Windows `curl.exe`:

```powershell
$NmsBaseUrl = "http://<NMS_HOST>:<PORT>"

curl.exe -X POST `
    "$NmsBaseUrl/cmd/_load_csv_file" `
    -F "t0=2026-07-29 10:00:00" `
    -F "session_id=Session01" `
    -F "replace_existing=false" `
    -F "csv_file=@M2_test.csv;type=text/csv" `
    -F "initial_poses_file=@initial_poses.csv;type=text/csv"
```

Use the actual time format declared by the running NMS. Swagger/OpenAPI may
also be used, but both files must be uploaded in the same request.

The safe server sequence is expected to be:

1. read both uploads;
2. run shared CommonCheckers;
3. stop without mutation if public validation fails;
4. validate future `t0`;
5. build registration items and apply live-whitelist checks;
6. enforce the single-experiment gate;
7. initialize lab mobility state;
8. enqueue commands; and
9. write the experiment registry record.

## 8. Accept or reject the response

Accept registration only when the response confirms:

- top-level `status` is `ok`;
- `public_validation.ok` is `true`;
- returned `experiment_id`, `session_id`, and `lab_id` are correct;
- `added` equals the expected command count;
- `skipped_not_whitelisted` is `0`;
- `bad_rows` is `0`;
- registration state is `registered`; and
- returned start/end times are expected.

Save the complete response with the intake files and operator timestamp.

Common failures:

| Result | Operator action |
|---|---|
| HTTP 400 | Correct request format, filenames, or `t0`; do not edit CSV content. |
| HTTP 422 `public_validation_failed` | Reject the submission and return the reported issues to the writer. |
| HTTP 500 `public_validator_error` | Stop; repair the NMS validator installation before retrying. |
| Existing-experiment rejection | Inspect the registry and delete only the known old experiment if appropriate. |
| Nonzero whitelist skips | Stop; resolve device identity/availability rather than accepting a partial experiment. |

Never manually enqueue the remaining rows after a partial or failed
registration.

## 9. Verify the registered experiment

Before `t0`, confirm through the registry/status interface available at the
site:

- one expected experiment is registered;
- command count and execution window are correct;
- targets match the script;
- no unexpected old commands remain queued; and
- robots remain at their intended initial poses.

Keep enough preparation time to delete the registration safely if anything is
wrong. If deleted, correct the underlying issue and repeat the complete
registration procedure.

## 10. Audit record

Retain:

- original received files;
- uploaded renamed command CSV;
- writer validation report;
- independently calculated hashes;
- accepted CommonCheckers/site versions;
- API request parameters excluding secrets;
- complete API response;
- operator identity and registration time; and
- any deletion, rejection, or retry record.

This record connects the released validation code, the writer's exact CSV
bytes, the operator's independent validation, and the experiment registered by
the NMS.
