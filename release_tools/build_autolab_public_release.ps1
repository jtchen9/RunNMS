[CmdletBinding()]
param(
    [string]$NmsRoot = "D:\Data\_Action\_RunNMS",
    [string]$ReleaseParent = "D:\Data\_Action\_AutoLab_Releases",
    [string]$ReleaseVersion = "2026.07.28.1",
    [string]$PythonExe = "python",
    [switch]$ArchiveExisting
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Utf8NoBom {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    $Encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Text, $Encoding)
}

function Set-JsonProperty {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Object,

        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [object]$Value
    )

    if ($Object.PSObject.Properties.Name -contains $Name) {
        $Object.$Name = $Value
    }
    else {
        $Object |
            Add-Member `
                -NotePropertyName $Name `
                -NotePropertyValue $Value
    }
}

function Assert-FileExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "Required source file is missing: $Path"
    }
}

function Assert-DirectoryExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        throw "Required source directory is missing: $Path"
    }
}

function Assert-ChildPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Parent,

        [Parameter(Mandatory = $true)]
        [string]$Child
    )

    $ParentFull = [System.IO.Path]::GetFullPath($Parent)
    $ParentFull = $ParentFull.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar
    ) + [System.IO.Path]::DirectorySeparatorChar

    $ChildFull = [System.IO.Path]::GetFullPath($Child)

    if (-not $ChildFull.StartsWith(
        $ParentFull,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Unsafe release path outside ReleaseParent: $ChildFull"
    }
}

function New-ReleaseManifest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,

        [Parameter(Mandatory = $true)]
        [string]$OutputPath,

        [Parameter(Mandatory = $true)]
        [string]$PackageName,

        [Parameter(Mandatory = $true)]
        [string]$Version
    )

    $Hashes = [ordered]@{}

    Get-ChildItem -LiteralPath $Root -Recurse -File |
        Where-Object {
            $_.FullName -ne $OutputPath -and
            $_.Name -ne "MANIFEST.json"
        } |
        Sort-Object FullName |
        ForEach-Object {
            $RelativePath = $_.FullName.Substring($Root.Length)
            $RelativePath = $RelativePath.TrimStart("\")
            $RelativePath = $RelativePath.Replace("\", "/")

            $Hash = (
                Get-FileHash `
                    -LiteralPath $_.FullName `
                    -Algorithm SHA256
            ).Hash.ToLowerInvariant()

            $Hashes[$RelativePath] = "sha256:$Hash"
        }

    $Manifest = [ordered]@{
        schema_version = 1
        package_name = $PackageName
        version = $Version
        generated_at = (Get-Date).ToString("s")
        file_hashes = $Hashes
    }

    $ManifestText = $Manifest | ConvertTo-Json -Depth 30
    Write-Utf8NoBom -Path $OutputPath -Text ($ManifestText + "`r`n")
}

function Move-ExistingBuildToArchive {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Paths,

        [Parameter(Mandatory = $true)]
        [string]$ArchiveRoot
    )

    $ExistingPaths = @(
        $Paths |
            Where-Object {
                Test-Path -LiteralPath $_
            }
    )

    if ($ExistingPaths.Count -eq 0) {
        return
    }

    if (-not $ArchiveExisting) {
        $List = $ExistingPaths -join "`r`n"
        throw (
            "Existing release output was found. Nothing was changed.`r`n" +
            "$List`r`n" +
            "Re-run with -ArchiveExisting to move it into a timestamped archive."
        )
    }

    New-Item -ItemType Directory -Path $ArchiveRoot -Force | Out-Null

    foreach ($ExistingPath in $ExistingPaths) {
        Assert-ChildPath -Parent $ReleaseParent -Child $ExistingPath

        $Destination = Join-Path `
            $ArchiveRoot `
            ([System.IO.Path]::GetFileName($ExistingPath))

        Write-Host "Archiving existing output:"
        Write-Host "  From: $ExistingPath"
        Write-Host "  To:   $Destination"

        Move-Item `
            -LiteralPath $ExistingPath `
            -Destination $Destination
    }
}

$ReleaseName = "AutoLab_DemoRoom_Public_$ReleaseVersion"
$StageRoot = Join-Path $ReleaseParent "$ReleaseName-staging"
$ZipPath = Join-Path $ReleaseParent "$ReleaseName.zip"
$CleanTestRoot = Join-Path $ReleaseParent "$ReleaseName-clean-test"

$FinalXlsm = Join-Path `
    $NmsRoot `
    "sitemap\DemoRoom\script_authoring\AutoLab_DemoRoom_Template.xlsm"

$StageCommon = Join-Path $StageRoot "sitemap\CommonCheckers"
$StageChecker = Join-Path $StageCommon "checker"
$StageCommonConfig = Join-Path $StageCommon "config"
$StageCommonDocs = Join-Path $StageCommon "docs"

$StageSite = Join-Path $StageRoot "sitemap\DemoRoom"
$StageAuthoring = Join-Path $StageSite "script_authoring"
$StageSiteConfig = Join-Path $StageAuthoring "config"
$PublicXlsm = Join-Path $StageAuthoring "AutoLab_DemoRoom_Template.xlsm"

$BuildTimestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$LogDirectory = Join-Path $ReleaseParent "logs"
$ArchiveRoot = Join-Path `
    (Join-Path $ReleaseParent "archive") `
    "$ReleaseName-$BuildTimestamp"
$LogPath = Join-Path `
    $LogDirectory `
    "$ReleaseName-$BuildTimestamp.log"
$SummaryPath = Join-Path `
    $LogDirectory `
    "$ReleaseName-$BuildTimestamp-summary.json"

New-Item -ItemType Directory -Path $ReleaseParent -Force | Out-Null
New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null

$TranscriptStarted = $false

try {
    Start-Transcript -LiteralPath $LogPath | Out-Null
    $TranscriptStarted = $true

    Write-Host "============================================================"
    Write-Host "AutoLab public release build"
    Write-Host "============================================================"
    Write-Host "NMS root:          $NmsRoot"
    Write-Host "Release version:   $ReleaseVersion"
    Write-Host "Staging root:      $StageRoot"
    Write-Host "ZIP path:          $ZipPath"
    Write-Host "Clean-test root:   $CleanTestRoot"
    Write-Host "Final XLSM source: $FinalXlsm"
    Write-Host "Log path:          $LogPath"
    Write-Host ""

    Assert-DirectoryExists -Path $NmsRoot
    Assert-FileExists -Path $FinalXlsm

    Assert-ChildPath -Parent $ReleaseParent -Child $StageRoot
    Assert-ChildPath -Parent $ReleaseParent -Child $ZipPath
    Assert-ChildPath -Parent $ReleaseParent -Child $CleanTestRoot

    Move-ExistingBuildToArchive `
        -Paths @($StageRoot, $ZipPath, $CleanTestRoot) `
        -ArchiveRoot $ArchiveRoot

    $CheckerFiles = @(
        "__init__.py",
        "argument_rules.py",
        "checker_runner.py",
        "initialization_rules.py",
        "macro_rules.py",
        "movement_rules.py",
        "path_rules.py",
        "reserved_location_rules.py",
        "script_model.py",
        "static_safety_core.py",
        "timeline_rules.py",
        "traffic_rules.py",
        "validation_report.py",
        "vocabulary_rules.py"
    )

    $SiteConfigFiles = @(
        "ap_roster.json",
        "bump_guard_zones.json",
        "macro_policy.json",
        "path_policy.json",
        "safety_policy.json",
        "zone_policy.json"
    )

    $RequiredSourceFiles = @(
        $FinalXlsm,
        (Join-Path $NmsRoot "sitemap\CommonCheckers\VERSION.json"),
        (Join-Path $NmsRoot "sitemap\CommonCheckers\config\script_policy.json"),
        (Join-Path $NmsRoot "sitemap\CommonCheckers\docs\README_for_script_writers.md"),
        (Join-Path $NmsRoot "sitemap\DemoRoom\restriction_map.npy"),
        (Join-Path $NmsRoot "sitemap\DemoRoom\script_authoring\VERSION.json"),
        (Join-Path $NmsRoot "sitemap\DemoRoom\script_authoring\autolab_xlsm_run_checker.py")
    )

    foreach ($FileName in $CheckerFiles) {
        $RequiredSourceFiles += Join-Path `
            $NmsRoot `
            "sitemap\CommonCheckers\checker\$FileName"
    }

    foreach ($FileName in $SiteConfigFiles) {
        $RequiredSourceFiles += Join-Path `
            $NmsRoot `
            "sitemap\DemoRoom\script_authoring\config\$FileName"
    }

    Write-Host "Checking required source files..."
    foreach ($SourcePath in $RequiredSourceFiles) {
        Assert-FileExists -Path $SourcePath
        Write-Host "  OK: $SourcePath"
    }

    Write-Host ""
    Write-Host "Creating clean staging directories..."
    New-Item -ItemType Directory -Path $StageChecker -Force | Out-Null
    New-Item -ItemType Directory -Path $StageCommonConfig -Force | Out-Null
    New-Item -ItemType Directory -Path $StageCommonDocs -Force | Out-Null
    New-Item -ItemType Directory -Path $StageSiteConfig -Force | Out-Null

    Write-Host "Copying CommonCheckers allowlist..."
    foreach ($FileName in $CheckerFiles) {
        Copy-Item `
            -LiteralPath (
                Join-Path `
                    $NmsRoot `
                    "sitemap\CommonCheckers\checker\$FileName"
            ) `
            -Destination (Join-Path $StageChecker $FileName)
    }

    Copy-Item `
        -LiteralPath (
            Join-Path `
                $NmsRoot `
                "sitemap\CommonCheckers\config\script_policy.json"
        ) `
        -Destination (
            Join-Path $StageCommonConfig "script_policy.json"
        )

    Copy-Item `
        -LiteralPath (
            Join-Path `
                $NmsRoot `
                "sitemap\CommonCheckers\VERSION.json"
        ) `
        -Destination (
            Join-Path $StageCommon "VERSION.json"
        )

    Write-Host "Copying DemoRoom allowlist..."
    Copy-Item `
        -LiteralPath (
            Join-Path `
                $NmsRoot `
                "sitemap\DemoRoom\restriction_map.npy"
        ) `
        -Destination (
            Join-Path $StageSite "restriction_map.npy"
        )

    Copy-Item `
        -LiteralPath $FinalXlsm `
        -Destination $PublicXlsm

    Copy-Item `
        -LiteralPath (
            Join-Path `
                $NmsRoot `
                "sitemap\DemoRoom\script_authoring\autolab_xlsm_run_checker.py"
        ) `
        -Destination (
            Join-Path $StageAuthoring "autolab_xlsm_run_checker.py"
        )

    Copy-Item `
        -LiteralPath (
            Join-Path `
                $NmsRoot `
                "sitemap\DemoRoom\script_authoring\VERSION.json"
        ) `
        -Destination (
            Join-Path $StageAuthoring "VERSION.json"
        )

    foreach ($FileName in $SiteConfigFiles) {
        Copy-Item `
            -LiteralPath (
                Join-Path `
                    $NmsRoot `
                    "sitemap\DemoRoom\script_authoring\config\$FileName"
            ) `
            -Destination (
                Join-Path $StageSiteConfig $FileName
            )
    }

    Write-Host "Writing requirements and copying public README..."
    Write-Utf8NoBom `
        -Path (Join-Path $StageCommon "requirements.txt") `
        -Text "numpy==2.3.5`r`n"

    Copy-Item `
        -LiteralPath (
            Join-Path `
                $NmsRoot `
                "sitemap\CommonCheckers\docs\README_for_script_writers.md"
        ) `
        -Destination (
            Join-Path `
                $StageCommonDocs `
                "README_for_script_writers.md"
        )

    Write-Host "Updating copied release versions..."
    $CommonVersionPath = Join-Path $StageCommon "VERSION.json"
    $CommonVersion = Get-Content `
        -LiteralPath $CommonVersionPath `
        -Raw |
        ConvertFrom-Json

    Set-JsonProperty `
        -Object $CommonVersion `
        -Name "common_checkers_version" `
        -Value $ReleaseVersion
    Set-JsonProperty `
        -Object $CommonVersion `
        -Name "status" `
        -Value "release"

    Write-Utf8NoBom `
        -Path $CommonVersionPath `
        -Text (
            ($CommonVersion | ConvertTo-Json -Depth 20) +
            "`r`n"
        )

    $SiteVersionPath = Join-Path $StageAuthoring "VERSION.json"
    $SiteVersion = Get-Content `
        -LiteralPath $SiteVersionPath `
        -Raw |
        ConvertFrom-Json

    Set-JsonProperty `
        -Object $SiteVersion `
        -Name "site_authoring_version" `
        -Value $ReleaseVersion
    Set-JsonProperty `
        -Object $SiteVersion `
        -Name "required_common_checkers_version" `
        -Value $ReleaseVersion
    Set-JsonProperty `
        -Object $SiteVersion `
        -Name "status" `
        -Value "release"

    Write-Utf8NoBom `
        -Path $SiteVersionPath `
        -Text (
            ($SiteVersion | ConvertTo-Json -Depth 20) +
            "`r`n"
        )

    Write-Host "Checking staging for forbidden files..."
    $Forbidden = @(
        Get-ChildItem -LiteralPath $StageRoot -Recurse -Force |
            Where-Object {
                $_.Name -eq "__pycache__" -or
                $_.Extension -eq ".pyc" -or
                $_.FullName -match "\\tests\\" -or
                $_.FullName -match "\\examples\\" -or
                $_.FullName -match "\\generated\\" -or
                $_.Name -eq "robot_roster.json" -or
                $_.Name -eq "restriction_map_Old.npy"
            }
    )

    if ($Forbidden.Count -gt 0) {
        $Forbidden | ForEach-Object {
            Write-Host "  FORBIDDEN: $($_.FullName)"
        }
        throw "Forbidden files were found in staging."
    }

    Write-Host "Generating manifests..."
    New-ReleaseManifest `
        -Root $StageCommon `
        -OutputPath (
            Join-Path $StageCommon "MANIFEST.json"
        ) `
        -PackageName "AutoLab CommonCheckers" `
        -Version $ReleaseVersion

    New-ReleaseManifest `
        -Root $StageSite `
        -OutputPath (
            Join-Path $StageSite "MANIFEST.json"
        ) `
        -PackageName "AutoLab DemoRoom Script Authoring" `
        -Version $ReleaseVersion

    Write-Host "Testing CommonCheckers import from staging..."
    $PreviousPythonPath = [Environment]::GetEnvironmentVariable(
        "PYTHONPATH",
        "Process"
    )
    try {
        $env:PYTHONPATH = $StageCommon
        & $PythonExe -c (
            "from checker.checker_runner import validate_script; " +
            "print('Staging CommonCheckers import: OK')"
        )

        if ($LASTEXITCODE -ne 0) {
            throw "Staging CommonCheckers import test failed."
        }
    }
    finally {
        if ($null -eq $PreviousPythonPath) {
            Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue
        }
        else {
            $env:PYTHONPATH = $PreviousPythonPath
        }
    }

    Write-Host "Creating ZIP..."
    Compress-Archive `
        -LiteralPath (Join-Path $StageRoot "sitemap") `
        -DestinationPath $ZipPath `
        -CompressionLevel Optimal

    Assert-FileExists -Path $ZipPath
    $ZipHash = (
        Get-FileHash `
            -LiteralPath $ZipPath `
            -Algorithm SHA256
    ).Hash.ToLowerInvariant()

    Write-Host "ZIP SHA-256: $ZipHash"

    Write-Host "Extracting ZIP into clean-test directory..."
    Expand-Archive `
        -LiteralPath $ZipPath `
        -DestinationPath $CleanTestRoot

    $CleanCommon = Join-Path `
        $CleanTestRoot `
        "sitemap\CommonCheckers"
    $CleanXlsm = Join-Path `
        $CleanTestRoot `
        "sitemap\DemoRoom\script_authoring\AutoLab_DemoRoom_Template.xlsm"

    Assert-DirectoryExists -Path $CleanCommon
    Assert-FileExists -Path $CleanXlsm

    Write-Host "Testing CommonCheckers import from extracted ZIP..."
    $PreviousPythonPath = [Environment]::GetEnvironmentVariable(
        "PYTHONPATH",
        "Process"
    )
    try {
        $env:PYTHONPATH = $CleanCommon
        & $PythonExe -c (
            "from checker.checker_runner import validate_script; " +
            "print('Extracted CommonCheckers import: OK')"
        )

        if ($LASTEXITCODE -ne 0) {
            throw "Extracted CommonCheckers import test failed."
        }
    }
    finally {
        if ($null -eq $PreviousPythonPath) {
            Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue
        }
        else {
            $env:PYTHONPATH = $PreviousPythonPath
        }
    }

    $StagedFiles = @(
        Get-ChildItem `
            -LiteralPath $StageRoot `
            -Recurse `
            -File |
            Sort-Object FullName |
            ForEach-Object {
                $StagedRelativePath = $_.FullName.Substring(
                    $StageRoot.Length
                )
                $StagedRelativePath = $StagedRelativePath.TrimStart("\")
                $StagedRelativePath = $StagedRelativePath.Replace("\", "/")

                [ordered]@{
                    relative_path = $StagedRelativePath
                    size_bytes = $_.Length
                }
            }
    )

    $Summary = [ordered]@{
        status = "success"
        release_name = $ReleaseName
        release_version = $ReleaseVersion
        generated_at = (Get-Date).ToString("s")
        nms_root = $NmsRoot
        staging_root = $StageRoot
        zip_path = $ZipPath
        zip_sha256 = $ZipHash
        clean_test_root = $CleanTestRoot
        clean_test_xlsm = $CleanXlsm
        clean_test_preflight_b5 = $CleanTestRoot
        log_path = $LogPath
        staged_file_count = $StagedFiles.Count
        staged_files = $StagedFiles
    }

    Write-Utf8NoBom `
        -Path $SummaryPath `
        -Text (
            ($Summary | ConvertTo-Json -Depth 30) +
            "`r`n"
        )

    Write-Host ""
    Write-Host "============================================================"
    Write-Host "BUILD SUCCEEDED"
    Write-Host "============================================================"
    Write-Host "ZIP:              $ZipPath"
    Write-Host "ZIP SHA-256:      $ZipHash"
    Write-Host "Staging:          $StageRoot"
    Write-Host "Clean test:       $CleanTestRoot"
    Write-Host "Clean-test XLSM:  $CleanXlsm"
    Write-Host "Set XLSM B5 to:   $CleanTestRoot"
    Write-Host "Build log:        $LogPath"
    Write-Host "Build summary:    $SummaryPath"
    Write-Host ""
    Write-Host "Next manual step:"
    Write-Host "Open the clean-test XLSM and run the full macro/preflight test."
}
catch {
    Write-Host ""
    Write-Host "============================================================"
    Write-Host "BUILD FAILED"
    Write-Host "============================================================"
    Write-Host $_.Exception.Message
    Write-Host $_.ScriptStackTrace
    throw
}
finally {
    if ($TranscriptStarted) {
        Stop-Transcript | Out-Null
    }
}
