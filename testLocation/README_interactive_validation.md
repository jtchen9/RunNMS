# Interactive validation collection companion

Place:

`t11_collect_validation_sample.py`

beside the existing:

`t11_collect_observation_sample.py`

under `testLocation\`.

The new script imports and reuses the existing collector. It does not rewrite the
Redis command, report wait, AprilTag extraction, logging, or per-tag console table.

After the existing `run_once()` completes, it adds a short field check:

- observable front/rear distance and angle gates
- usable observation count
- unique tag count
- distance+angle quick pose estimate
- entered-vs-estimated difference for human inspection only
- KEEP / KEEP_WITH_CAUTION / RETAKE

The automatic KEEP/RETAKE conclusion does not use GT-vs-estimate error.

The quick solver intentionally excludes yaw because current yaw sign/acceptance
still depends on offline truth-assisted labels in Candidate v1.
