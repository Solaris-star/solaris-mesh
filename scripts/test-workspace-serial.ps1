[CmdletBinding()]
param(
    [string]$LogRoot = "target/serial-test-logs"
)

$ErrorActionPreference = "Stop"
$packages = @(
    "solaris-types",
    "solaris-protocol",
    "solaris-compact",
    "solaris-process",
    "solaris-config",
    "solaris-providers",
    "solaris-tools",
    "solaris-mcp",
    "solaris-skills",
    "solaris-memory",
    "solaris-agent",
    "solaris-cli"
)

$resolvedLogRoot = [IO.Path]::GetFullPath($LogRoot)
New-Item -ItemType Directory -Force -Path $resolvedLogRoot | Out-Null
$results = [Collections.Generic.List[object]]::new()
$startedAt = [DateTimeOffset]::UtcNow

foreach ($package in $packages) {
    $safeName = $package -replace "[^A-Za-z0-9._-]", "_"
    $logPath = Join-Path $resolvedLogRoot "$safeName.log"
    $packageStartedAt = [DateTimeOffset]::UtcNow
    Write-Host "Testing $package"

    & cargo test -p $package --all-features -- --test-threads=1 2>&1 |
        Tee-Object -FilePath $logPath
    $exitCode = $LASTEXITCODE
    $packageFinishedAt = [DateTimeOffset]::UtcNow
    $results.Add([pscustomobject]@{
        package = $package
        exit_code = $exitCode
        passed = ($exitCode -eq 0)
        started_at = $packageStartedAt.ToString("o")
        finished_at = $packageFinishedAt.ToString("o")
        duration_seconds = [math]::Round(($packageFinishedAt - $packageStartedAt).TotalSeconds, 3)
        log = [IO.Path]::GetRelativePath((Get-Location).Path, $logPath)
    })
}

$finishedAt = [DateTimeOffset]::UtcNow
$summary = [pscustomobject]@{
    started_at = $startedAt.ToString("o")
    finished_at = $finishedAt.ToString("o")
    duration_seconds = [math]::Round(($finishedAt - $startedAt).TotalSeconds, 3)
    package_count = $results.Count
    passed_count = @($results | Where-Object passed).Count
    failed_count = @($results | Where-Object { -not $_.passed }).Count
    results = @($results)
}
$summaryPath = Join-Path $resolvedLogRoot "summary.json"
$summary | ConvertTo-Json -Depth 5 | Set-Content -Path $summaryPath -Encoding utf8

if ($summary.failed_count -gt 0) {
    Write-Error "Serial workspace test failed for $($summary.failed_count) package(s). See $summaryPath."
    exit 1
}

Write-Host "Serial workspace test passed for $($summary.package_count) packages. Summary: $summaryPath"
