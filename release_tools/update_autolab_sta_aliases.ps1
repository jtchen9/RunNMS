[CmdletBinding()]
param(
    [string]$NmsRoot = "D:\Data\_Action\_RunNMS",
    [string]$WorkbookPath = "",
    [string]$PolicyPath = "",
    [switch]$SkipBackup
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $WorkbookPath) { $WorkbookPath = Join-Path $NmsRoot 'sitemap\DemoRoom\script_authoring\AutoLab_DemoRoom_Template.xlsm' }
if (-not $PolicyPath) { $PolicyPath = Join-Path $NmsRoot 'sitemap\CommonCheckers\config\script_policy.json' }
$WorkbookPath = (Resolve-Path -LiteralPath $WorkbookPath).Path
$policy = Get-Content -LiteralPath $PolicyPath -Raw -Encoding UTF8 | ConvertFrom-Json
$names = @(); $byName = @{}; $byMac = @{}
foreach ($entry in $policy.sta_mac_by_name.PSObject.Properties) {
    $name = $entry.Name; $mac = ([string]$entry.Value).Trim().ToLowerInvariant()
    if ($name -cnotmatch '^twin-scout-[a-z0-9_-]+$' -or $mac -notmatch '^([0-9a-f]{2}:){5}[0-9a-f]{2}$') { throw "Invalid station mapping: $name" }
    if ($byMac.ContainsKey($mac)) { throw "Duplicate station MAC: $mac" }
    $names += $name; $byName[$name] = $mac; $byMac[$mac] = $name
}
if ($names.Count -eq 0) { throw 'Shared policy has no station aliases.' }
$choices = $names -join ','
if ($choices.Length -gt 255) { throw 'Alias list exceeds Excel inline validation limit; a range-based dropdown is required.' }
$actions = @('ap.sta.disassociate', 'ap.txpower.set')
foreach ($action in $actions) {
    if (@($policy.argument_rules_by_category.ap.$action.allowed_keys) -notcontains 'sta') {
        throw 'Install NMS-AP-STA-shared-policy.patch before updating the template.'
    }
}
function Convert-Station([string]$value) {
    $value = $value.Trim().ToLowerInvariant()
    if (-not $value) { return '' }
    if ($byName.ContainsKey($value)) { return $value }
    if ($byMac.ContainsKey($value)) { return $byMac[$value] }
    throw "Station is absent from shared policy: $value"
}
function Convert-Args([string]$text) {
    if (-not $text.Trim()) { return $text }
    $obj = ConvertFrom-Json -InputObject $text
    $out = [ordered]@{}
    foreach ($p in $obj.PSObject.Properties) {
        if ($p.Name -notin @('sta', 'sta_mac')) { $out[$p.Name] = $p.Value }
    }
    $station = ''
    foreach ($p in $obj.PSObject.Properties) {
        if ($p.Name -in @('sta', 'sta_mac')) {
            $candidate = Convert-Station ([string]$p.Value)
            if ($station -and $station -ne $candidate) { throw 'Conflicting sta and sta_mac in existing JSON.' }
            $station = $candidate
        }
    }
    if ($station) { $out['sta'] = $station }
    return ConvertTo-Json -InputObject $out -Compress -Depth 20
}

# Work on a temporary copy. Save and replace only after every conversion succeeds.
# Excel preserves VBA, buttons, drawings, maps, and other workbook features.
$folder = Split-Path -Parent $WorkbookPath
$token = [guid]::NewGuid().ToString('N')
$temp = Join-Path $folder ('.sta-alias-' + $token + '.xlsm')
$backup = $WorkbookPath + '.before-sta-alias-' + $token + '.bak'
Copy-Item -LiteralPath $WorkbookPath -Destination $temp
$excel = $null; $book = $null
try {
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false; $excel.DisplayAlerts = $false
    $excel.EnableEvents = $false; $excel.AutomationSecurity = 3
    $book = $excel.Workbooks.Open($temp, 0, $false)
    $commands = $book.Worksheets.Item('CommandCatalog')
    $catalog = $book.Worksheets.Item('ParameterCatalog')
    $cmd = $book.Worksheets.Item('CommandSheet')
    $preview = $book.Worksheets.Item('ExportPreview')
    $protected = @{}
    foreach ($sheet in @($catalog, $cmd, $preview, $commands)) {
        $protected[$sheet.Name] = [bool]$sheet.ProtectContents
        $sheet.Unprotect()
    }
    $last = $commands.Cells.Item($commands.Rows.Count, 1).End(-4162).Row
    for ($r = 2; $r -le $last; $r++) {
        if (([string]$commands.Cells.Item($r, 1).Value2) -eq 'ap.txpower.set') {
            $commands.Cells.Item($r, 8).Value2 = 'Set AP transmit power, optionally for one robot alias.'
        }
    }
    $positions = @{}
    $last = $catalog.Cells.Item($catalog.Rows.Count, 1).End(-4162).Row
    for ($r = 2; $r -le $last; $r++) {
        $action = [string]$catalog.Cells.Item($r, 1).Value2
        $param = [string]$catalog.Cells.Item($r, 3).Value2
        if ($action -in $actions -and $param -in @('sta', 'sta_mac')) {
            if ($positions.ContainsKey($action)) { throw "Duplicate station parameter: $action" }
            $positions[$action] = 9 + [int]$catalog.Cells.Item($r, 2).Value2
            $catalog.Cells.Item($r, 3).Value2 = 'sta'
            $catalog.Cells.Item($r, 8).Value2 = $choices
            $catalog.Cells.Item($r, 9).Value2 = 'Robot alias from script_policy.json'
            $catalog.Cells.Item($r, 10).Value2 = 'Choose a robot alias; MAC addresses are resolved by the NMS.'
            $catalog.Cells.Item($r, 7).Value2 = Convert-Station ([string]$catalog.Cells.Item($r, 7).Value2)
        }
    }
    if ($positions.Count -ne 2) { throw 'Expected one station parameter for each AP command.' }
    $last = $cmd.Cells.Item($cmd.Rows.Count, 7).End(-4162).Row
    for ($r = 4; $r -le $last; $r++) {
        $action = [string]$cmd.Cells.Item($r, 7).Value2
        if ($action -notin $actions -or ([string]$cmd.Cells.Item($r, 2).Value2) -ne 'Value') { continue }
        if (([string]$cmd.Cells.Item($r - 1, 2).Value2) -ne 'Key') { throw "Missing Key row above row $r" }
        $col = $positions[$action]; $cell = $cmd.Cells.Item($r, $col)
        $cmd.Cells.Item($r - 1, $col).Value2 = 'sta'
        $cell.Value2 = Convert-Station ([string]$cell.Value2)
        $cell.Validation.Delete()
        $cell.Validation.Add(3, 1, 1, $choices)
        $cell.Validation.IgnoreBlank = $true
        $cell.Validation.InCellDropdown = $true
        $cell.Validation.ShowError = $true
        $cell.Validation.ErrorTitle = 'Invalid robot alias'
        $cell.Validation.ErrorMessage = 'Choose a robot alias from the dropdown.'
        $cmd.Cells.Item($r, 16).Value2 = Convert-Args ([string]$cmd.Cells.Item($r, 16).Value2)
        $cmd.Cells.Item($r, 17).Value2 = 'NOT CHECKED'
        $cmd.Range($cmd.Cells.Item($r, 18), $cmd.Cells.Item($r, 20)).ClearContents()
    }
    $last = $preview.Cells.Item($preview.Rows.Count, 4).End(-4162).Row
    for ($r = 2; $r -le $last; $r++) {
        if (([string]$preview.Cells.Item($r, 4).Value2) -in $actions) {
            $preview.Cells.Item($r, 5).Value2 = Convert-Args ([string]$preview.Cells.Item($r, 5).Value2)
        }
    }
    foreach ($sheet in @($catalog, $cmd, $preview, $commands)) {
        if ($protected[$sheet.Name]) { $sheet.Protect('', $true, $true, $true, $true, $true, $true, $true) }
    }
    $book.Save()
    $book.Close($false); $book = $null
    if (-not $SkipBackup) { Copy-Item -LiteralPath $WorkbookPath -Destination $backup }
    Move-Item -LiteralPath $temp -Destination $WorkbookPath -Force
    Write-Host "Updated station aliases: $WorkbookPath"
    if (-not $SkipBackup) { Write-Host "Backup: $backup" }
}
finally {
    if ($null -ne $book) { $book.Close($false) }
    if ($null -ne $excel) { $excel.Quit() }
    Remove-Item -LiteralPath $temp -Force -ErrorAction SilentlyContinue
    [GC]::Collect(); [GC]::WaitForPendingFinalizers()
}
