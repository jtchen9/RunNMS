# AutoLab public-package release procedure

Audience: AutoLab system designer or release engineer.

This document is internal. It belongs in `release_tools` and must not be
included in the public script-writer ZIP.

## 1. Purpose and release boundary

The release package gives a remote script writer only the code and data needed
to author and preflight a DemoRoom experiment:

```text
sitemap\CommonCheckers
sitemap\DemoRoom
```

The package must not contain the full NMS codebase, runtime credentials,
registration tools, internal tests, generated experiment files, or lab
operator procedures.

Public validation has one source of truth:

```text
sitemap\CommonCheckers
```

The XLSM calls that validator through the DemoRoom bridge. The NMS registration
API must call the same validator. Do not copy public-facing validation rules
into VBA or into private registration code.

## 2. Release prerequisites

Before building a release, confirm:

- the NMS working tree contains the intended reviewed changes;
- public checker tests pass;
- registration-adapter tests pass;
- the final workbook is named `AutoLab_DemoRoom_Template.xlsm`;
- the workbook contains the current VBA module and control buttons;
- `CommandCatalog` and `ParameterCatalog` match the supported public commands;
- the workbook opens without Excel repair;
- `InitializeScriptTemplate`, CSV export, map refresh, and
  `RunCommonCheckers` have passed in the source tree;
- CommonCheckers and DemoRoom version files are ready for the new release; and
- no secrets or private endpoints have been added to public files.

Recommended regression commands from the NMS root:

```powershell
python -m unittest discover `
    -s .\sitemap\CommonCheckers\tests `
    -p "test_*.py" `
    -v

python -m unittest discover `
    -s .\tests `
    -p "test_m4commands_public_validation.py" `
    -v
```

Use the actual test paths in the current codebase if they differ. Do not place
test files into the public staging allowlist.

## 3. Select a release version

Use a monotonically increasing version, for example:

```text
2026.07.28.2
```

The release builder writes this version into the copied CommonCheckers and
DemoRoom authoring version files. Reusing a version makes audit and rollback
ambiguous, so use a new version whenever public bytes change.

## 4. Build the package

From the NMS root:

```powershell
cd D:\Data\_Action\_RunNMS

powershell.exe `
    -NoProfile `
    -ExecutionPolicy Bypass `
    -File ".\release_tools\build_autolab_public_release.ps1" `
    -ReleaseVersion "0001" `
    -ArchiveExisting
```

Optional parameters:

```text
-NmsRoot
-ReleaseParent
-ReleaseVersion
-PythonExe
-ArchiveExisting
```

`-ArchiveExisting` moves an existing staging directory, ZIP, and clean-test
directory for the same version into a timestamped archive. It does not silently
overwrite them.

The script:

1. checks every required allowlisted source file;
2. creates a clean staging tree;
3. copies only the approved CommonCheckers and DemoRoom files;
4. writes the pinned public Python requirement;
5. copies the maintained script-writer README;
6. updates copied version metadata;
7. rejects forbidden files;
8. generates manifests;
9. tests importing CommonCheckers from staging;
10. creates the ZIP and computes its SHA-256;
11. extracts the ZIP into an isolated clean-test directory;
12. tests importing CommonCheckers from the extracted package; and
13. writes a timestamped log and JSON build summary.

Any failed step means the release is not publishable.

## 5. Inspect the build evidence

Require the console to end with:

```text
BUILD SUCCEEDED
```

Record these outputs:

- release version;
- ZIP path;
- ZIP SHA-256;
- staging path;
- clean-test path;
- build log path; and
- build-summary JSON path.

Review the staged-file list in the summary. Confirm that the ZIP contains only
the intended `sitemap` subtree. In particular, it must not contain:

- `release_tools`;
- NMS production modules;
- registration/operator documentation;
- `tests`, `examples`, or `generated`;
- `__pycache__` or `.pyc`;
- private robot rosters;
- obsolete restriction maps; or
- credentials, tokens, or internal URLs.

## 6. Perform the clean-machine emulation

Use only the extracted clean-test directory. Do not let the workbook use files
from the NMS source tree.

1. Open:

   ```text
   <clean-test>\sitemap\DemoRoom\script_authoring\AutoLab_DemoRoom_Template.xlsm
   ```

2. Run `InitializeScriptTemplate`.
3. Confirm `PreflightConfig!B5` becomes the clean-test package root.
4. Confirm B4 contains the intended Python executable and B6 is blank.
5. Confirm `PreflightStatus` contains no source-tree path and says `NOT RUN`.
6. Create at least one valid script covering the currently supported command
   families.
7. Run `BuildArgsJson` and map refreshes.
8. Run `RunCommonCheckers` and require PASS.
9. Introduce one known invalid value, require FAIL, then restore it and require
   PASS again.
10. Close and reopen the workbook and confirm Excel does not repair it.

This test is the release gate that emulates a remote script writer.

## 7. Publish the release

Distribute:

- `AutoLab_DemoRoom_Public_<version>.zip`; and
- its SHA-256 value through a separate trusted message or release record.

The public README is already inside the ZIP. Do not add this release procedure
or the lab-operator registration procedure to the ZIP.

Retain internally:

- the exact ZIP;
- SHA-256;
- build log;
- build-summary JSON;
- source commit identifier;
- release approval;
- clean-test result; and
- any known limitations.

## 8. Post-release change and rollback

Never modify a published ZIP in place. For any correction:

1. fix and review the source;
2. rerun tests;
3. assign a new release version;
4. rebuild from the script;
5. repeat the clean-test gate; and
6. publish the new ZIP and hash.

If a release is withdrawn, retain its evidence and mark it withdrawn in the
internal release record. Tell script writers which replacement version to use.
