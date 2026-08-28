[CmdletBinding()]
param(
    [string]$SourceDirectory = $PSScriptRoot,
    [string]$InstallDirectory = (Join-Path $env:LOCALAPPDATA "Solaris\bin"),
    [ValidatePattern('^[A-Za-z0-9.-]{3,50}$')]
    [string]$NetworkProxyPackageName = "Solaris.Mesh.NetworkProxy",
    [string]$NetworkProxyPackageFamilyName
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$networkProxyPackageName = $NetworkProxyPackageName
$requiredFiles = @(
    "solaris.exe",
    "solaris-process-sandbox-helper.exe",
    "solaris-process-sandbox-helper.exe.sha256",
    "solaris-windows-network-proxy.exe",
    "solaris-windows-network-proxy.exe.sha256",
    "solaris-windows-network-proxy.msix",
    "solaris-windows-network-proxy.msix.sha256",
    "solaris-extension.json"
)
$source = [IO.Path]::GetFullPath($SourceDirectory)
$destination = [IO.Path]::GetFullPath($InstallDirectory)
New-Item -ItemType Directory -Force -Path $destination | Out-Null

foreach ($name in $requiredFiles) {
    $path = Join-Path $source $name
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "release package is missing $name"
    }
}

$helper = Join-Path $source "solaris-process-sandbox-helper.exe"
$manifest = "$helper.sha256"
$expected = (Get-Content -LiteralPath $manifest -Raw).Trim()
$actual = "sha256:$((Get-FileHash -LiteralPath $helper -Algorithm SHA256).Hash.ToLowerInvariant())"
if ($expected -ne $actual) {
    throw "sandbox helper digest does not match its manifest"
}

$proxy = Join-Path $source "solaris-windows-network-proxy.exe"
$proxyManifest = "$proxy.sha256"
$proxyExpected = (Get-Content -LiteralPath $proxyManifest -Raw).Trim()
$proxyActual = "sha256:$((Get-FileHash -LiteralPath $proxy -Algorithm SHA256).Hash.ToLowerInvariant())"
if ($proxyExpected -ne $proxyActual) {
    throw "Windows network proxy digest does not match its manifest"
}

$proxyPackage = Join-Path $source "solaris-windows-network-proxy.msix"
$proxyPackageManifest = "$proxyPackage.sha256"
$proxyPackageExpected = (Get-Content -LiteralPath $proxyPackageManifest -Raw).Trim()
$proxyPackageActual = "sha256:$((Get-FileHash -LiteralPath $proxyPackage -Algorithm SHA256).Hash.ToLowerInvariant())"
if ($proxyPackageExpected -ne $proxyPackageActual) {
    throw "Windows network proxy MSIX digest does not match its manifest"
}

function Get-InstalledNetworkProxyPackage {
    @(Get-AppxPackage -Name $networkProxyPackageName -ErrorAction SilentlyContinue) |
        Sort-Object -Property Version -Descending |
        Select-Object -First 1
}

$registrationError = $null
try {
    Add-AppxPackage -Path $proxyPackage -AllowUnsigned -ForceApplicationShutdown -ForceUpdateFromAnyVersion -ErrorAction Stop
}
catch {
    $registrationError = $_.Exception.Message
}
$installedPackage = Get-InstalledNetworkProxyPackage
$packageReady = $false
if ($installedPackage -and $installedPackage.InstallLocation) {
    if ($NetworkProxyPackageFamilyName -and $installedPackage.PackageFamilyName -ne $NetworkProxyPackageFamilyName) {
        throw "installed Windows network proxy package family name does not match the requested identity"
    }
    $installedProxy = Join-Path $installedPackage.InstallLocation "solaris-windows-network-proxy.exe"
    if (Test-Path -LiteralPath $installedProxy -PathType Leaf) {
        $installedProxyDigest = "sha256:$((Get-FileHash -LiteralPath $installedProxy -Algorithm SHA256).Hash.ToLowerInvariant())"
        $packageReady = $installedProxyDigest -eq $proxyActual
    }
}
if ($NetworkProxyPackageFamilyName -and -not $packageReady) {
    throw "the requested Windows network proxy package identity is not installed with the release proxy binary"
}

foreach ($name in $requiredFiles) {
    Copy-Item -LiteralPath (Join-Path $source $name) -Destination (Join-Path $destination $name) -Force
}

$identityPath = Join-Path $destination "solaris-windows-network-proxy.identity.json"
$packageFamilyName = $null
if ($packageReady) {
    $packageFamilyName = [string]$installedPackage.PackageFamilyName
    if ($packageFamilyName -notmatch '^[A-Za-z0-9.-]{3,50}_[0-9A-HJ-KM-NP-TV-Za-hj-km-np-tv-z]{13}$') {
        throw "Windows returned an invalid network proxy package family name"
    }
    $identity = [ordered]@{
        schema_version = 1
        package_family_name = $packageFamilyName
        application_user_model_id = "${packageFamilyName}!Proxy"
    } | ConvertTo-Json -Compress
    [IO.File]::WriteAllText($identityPath, "${identity}`n", [Text.UTF8Encoding]::new($false))
}
elseif (Test-Path -LiteralPath $identityPath) {
    Remove-Item -LiteralPath $identityPath -Force
}

$currentSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
function Set-TrustedRuntimeFileAcl {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][ValidateSet('R', 'RX')][string]$Access
    )
    & icacls.exe $Path /inheritance:r /grant:r "*${currentSid}:(${Access})" '*S-1-5-18:(R)' '*S-1-5-32-544:(R)' | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "failed to tighten Solaris runtime file DACL: $Path"
    }
}
Set-TrustedRuntimeFileAcl -Path (Join-Path $destination "solaris-process-sandbox-helper.exe") -Access RX
Set-TrustedRuntimeFileAcl -Path (Join-Path $destination "solaris-process-sandbox-helper.exe.sha256") -Access R
Set-TrustedRuntimeFileAcl -Path (Join-Path $destination "solaris-windows-network-proxy.exe") -Access RX
Set-TrustedRuntimeFileAcl -Path (Join-Path $destination "solaris-windows-network-proxy.exe.sha256") -Access R
if ($packageReady) {
    Set-TrustedRuntimeFileAcl -Path $identityPath -Access R
    Write-Host "Solaris installed to $destination; Windows network proxy package family: $packageFamilyName"
}
else {
    $detail = if ($registrationError) { $registrationError } else { "no byte-identical registered package was found" }
    Write-Warning "Solaris installed to $destination, but Windows approved-domain networking is unavailable for this account: $detail"
}
