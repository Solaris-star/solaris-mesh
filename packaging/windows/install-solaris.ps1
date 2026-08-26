[CmdletBinding()]
param(
    [string]$SourceDirectory = $PSScriptRoot,
    [string]$InstallDirectory = (Join-Path $env:LOCALAPPDATA "Solaris\bin")
)

$ErrorActionPreference = "Stop"
$requiredFiles = @(
    "solaris.exe",
    "solaris-process-sandbox-helper.exe",
    "solaris-process-sandbox-helper.exe.sha256",
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

foreach ($name in $requiredFiles) {
    Copy-Item -LiteralPath (Join-Path $source $name) -Destination (Join-Path $destination $name) -Force
}

Write-Host "Solaris installed to $destination"
