<#
Save as D:\Data\_Action\_RunNMS\tools\initialize_iperf3.ps1.
Run in an existing Administrator PowerShell terminal:
    .\tools\initialize_iperf3.ps1
Or, from tools:
    .\initialize_iperf3.ps1

Setup only: discovers the upstream interface, sets its profile to Private,
and creates/refreshes TCP/UDP 5200-5239 firewall rules for iperf3.exe.
Does NOT start/stop NMS or iperf3, open windows, or run traffic tests.
Does NOT change mesh settings, interface names, addresses, gateways or config.py.
After setup, use VS Code's normal Python Run button to start NMS.
NMS's existing startup code manages the hidden 40-port iperf3 pool.
Rerun after moving the shared Windows system to another PC/interface.
#>
[CmdletBinding()]
param(
    [string]$IperfExe = 'D:\Data\Software\iperf3\iperf3.exe',
    [string]$UpstreamInterfaceAlias = ''
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
try {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Run this script in Administrator PowerShell.'
    }
    if (-not (Test-Path -LiteralPath $IperfExe -PathType Leaf)) {
        throw "iperf3.exe not found: $IperfExe"
    }
    $meshIndexes = @(Get-NetIPAddress -AddressFamily IPv4 | Where-Object {
        $_.IPAddress -eq '192.168.11.51'
    } | Select-Object -ExpandProperty InterfaceIndex -Unique)
    $upstream = @(Get-NetIPConfiguration | Where-Object {
        $_.IPv4DefaultGateway -and $_.NetAdapter.Status -eq 'Up' -and
        $meshIndexes -notcontains $_.InterfaceIndex
    })
    if ($UpstreamInterfaceAlias) {
        $upstream = @($upstream | Where-Object { $_.InterfaceAlias -eq $UpstreamInterfaceAlias })
    }
    if ($upstream.Count -ne 1) {
        throw 'Cannot uniquely identify upstream. Supply -UpstreamInterfaceAlias with its actual name.'
    }
    $adapter = $upstream[0]
    $profiles = @(Get-NetConnectionProfile -InterfaceIndex $adapter.InterfaceIndex)
    if ($profiles.Count -eq 0) { throw 'Upstream network profile is not available yet.' }
    if (@($profiles | Where-Object { $_.NetworkCategory -eq 'DomainAuthenticated' }).Count) {
        throw 'Upstream profile is domain-managed; cannot change it here.'
    }
    if (@($profiles | Where-Object { $_.NetworkCategory -ne 'Private' }).Count) {
        Set-NetConnectionProfile -InterfaceIndex $adapter.InterfaceIndex -NetworkCategory Private
    }
    $verified = @(Get-NetConnectionProfile -InterfaceIndex $adapter.InterfaceIndex)
    if ($verified.Count -eq 0 -or @($verified | Where-Object { $_.NetworkCategory -ne 'Private' }).Count) {
        throw 'Could not verify Private upstream profile.'
    }
    foreach ($protocol in @('TCP', 'UDP')) {
        $ruleName = "AutoLab-Iperf3-Pool-$protocol"
        Get-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue | Remove-NetFirewallRule
        New-NetFirewallRule -Name $ruleName -DisplayName "AutoLab iperf3 shared pool $protocol" `
            -Direction Inbound -Action Allow -Enabled True -Protocol $protocol `
            -LocalPort '5200-5239' -Program $IperfExe -InterfaceAlias $adapter.InterfaceAlias `
            -RemoteAddress LocalSubnet -Profile Private | Out-Null
    }
    Write-Host "iperf3 firewall setup complete on $($adapter.InterfaceAlias): TCP/UDP 5200-5239." -ForegroundColor Green
    Write-Host 'No processes started. Start NMS normally from VS Code.'
}
catch {
    Write-Error -Message ("iperf3 initialization failed: " + $_.Exception.Message) -ErrorAction Continue
    exit 1
}
