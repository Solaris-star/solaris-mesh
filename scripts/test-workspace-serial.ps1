[CmdletBinding()]
param(
    [string]$LogRoot = "target/serial-test-logs",
    [string[]]$Packages = @(
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
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (& git rev-parse --show-toplevel 2>$null).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($repositoryRoot)) {
    throw "The current directory is not a Git repository."
}
$repositoryRoot = [IO.Path]::GetFullPath($repositoryRoot)
Set-Location -LiteralPath $repositoryRoot
$resolvedLogRoot = [IO.Path]::GetFullPath((Join-Path $repositoryRoot $LogRoot))
New-Item -ItemType Directory -Force -Path $resolvedLogRoot | Out-Null
$summaryPath = Join-Path $resolvedLogRoot "summary.json"
$startedAt = [DateTimeOffset]::UtcNow
$results = New-Object 'Collections.Generic.List[object]'
$fatalError = $null

function Get-RelativePathCompat {
    param([string]$BasePath, [string]$TargetPath)
    $base = [IO.Path]::GetFullPath($BasePath)
    $target = [IO.Path]::GetFullPath($TargetPath)
    $separator = [IO.Path]::DirectorySeparatorChar
    if (-not $base.EndsWith([string]$separator)) {
        $base += $separator
    }
    $baseUri = New-Object Uri($base)
    $targetUri = New-Object Uri($target)
    return [Uri]::UnescapeDataString($baseUri.MakeRelativeUri($targetUri).ToString()).Replace('/', $separator)
}

function Invoke-TextCommand {
    param([string]$Executable, [string[]]$Arguments)
    $value = (& $Executable @Arguments 2>&1 | Out-String).Trim()
    return [pscustomobject]@{ value = $value; exit_code = $LASTEXITCODE }
}

function Get-AccountInfo {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return [pscustomobject]@{
        name = $identity.Name
        sid = $identity.User.Value
        is_administrator = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
        authentication_type = $identity.AuthenticationType
    }
}

function Get-TestCounts {
    param([string]$LogPath)
    $content = Get-Content -LiteralPath $LogPath -Raw -ErrorAction SilentlyContinue
    $matches = [regex]::Matches($content, 'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;')
    $passed = 0; $failed = 0; $ignored = 0
    foreach ($match in $matches) {
        $passed += [int]$match.Groups[1].Value
        $failed += [int]$match.Groups[2].Value
        $ignored += [int]$match.Groups[3].Value
    }
    return [pscustomobject]@{ passed = $passed; failed = $failed; ignored = $ignored }
}

function Get-TrackedResidualProcesses {
    $patterns = @('solaris-sandbox', 'solaris-windows-network-proxy', 'solaris-process-probe', 'solaris-child-agent')
    $items = @()
    Get-Process -ErrorAction SilentlyContinue | ForEach-Object {
        $name = $_.ProcessName
        if ($patterns | Where-Object { $name -like "*$_*" }) {
            $items += [pscustomobject]@{ id = $_.Id; name = $name }
        }
    }
    return @($items)
}

try {
    foreach ($package in $Packages) {
        $safeName = $package -replace "[^A-Za-z0-9._-]", "_"
        $logPath = Join-Path $resolvedLogRoot "$safeName.log"
        $packageStartedAt = [DateTimeOffset]::UtcNow
        $command = "cargo test -p $package --all-features -- --test-threads=1"
        Write-Host "Testing $package"
        try {
            $previousErrorActionPreference = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            & cargo test -p $package --all-features -- --test-threads=1 2>&1 | Tee-Object -FilePath $logPath
            $exitCode = $LASTEXITCODE
            $ErrorActionPreference = $previousErrorActionPreference
        }
        catch {
            $ErrorActionPreference = $previousErrorActionPreference
            $_ | Out-String | Add-Content -LiteralPath $logPath
            $exitCode = 1
        }
        $packageFinishedAt = [DateTimeOffset]::UtcNow
        $counts = Get-TestCounts -LogPath $logPath
        $hash = if (Test-Path -LiteralPath $logPath) { (Get-FileHash -LiteralPath $logPath -Algorithm SHA256).Hash.ToLowerInvariant() } else { $null }
        $results.Add([pscustomobject]@{
            package = $package
            command = $command
            exit_code = $exitCode
            passed = ($exitCode -eq 0)
            started_at = $packageStartedAt.ToString("o")
            finished_at = $packageFinishedAt.ToString("o")
            duration_seconds = [math]::Round(($packageFinishedAt - $packageStartedAt).TotalSeconds, 3)
            tests = $counts
            log = Get-RelativePathCompat -BasePath $repositoryRoot -TargetPath $logPath
            log_sha256 = $hash
        })
    }
}
catch {
    $fatalError = $_ | Out-String
}
finally {
    $finishedAt = [DateTimeOffset]::UtcNow
    $branch = Invoke-TextCommand git @('branch', '--show-current')
    $head = Invoke-TextCommand git @('rev-parse', 'HEAD')
    $commitTime = Invoke-TextCommand git @('show', '-s', '--format=%cI', 'HEAD')
    $status = Invoke-TextCommand git @('status', '--porcelain=v1', '-uall')
    $rust = Invoke-TextCommand rustc @('--version')
    $cargo = Invoke-TextCommand cargo @('--version')
    $dirtyFiles = @()
    foreach ($line in ($status.value -split "`r?`n" | Where-Object { $_.Length -ge 4 })) {
        $path = $line.Substring(3)
        $fullPath = Join-Path $repositoryRoot $path
        $dirtyFiles += [pscustomobject]@{
            status = $line.Substring(0, 2)
            path = $path
            sha256 = if (Test-Path -LiteralPath $fullPath -PathType Leaf) { (Get-FileHash -LiteralPath $fullPath -Algorithm SHA256).Hash.ToLowerInvariant() } else { $null }
        }
    }
    $summary = [pscustomobject]@{
        schema_version = 1
        repository_root = $repositoryRoot
        branch = $branch.value
        head = $head.value
        head_commit_time = $commitTime.value
        dirty = -not [string]::IsNullOrWhiteSpace($status.value)
        dirty_status = $status.value
        dirty_files = @($dirtyFiles)
        evidence_kind = if ([string]::IsNullOrWhiteSpace($status.value)) { 'commit_bound' } else { 'head_plus_dirty_worktree' }
        toolchain = [pscustomobject]@{
            rustc = $rust.value
            cargo = $cargo.value
            powershell = $PSVersionTable.PSVersion.ToString()
        }
        windows = [Environment]::OSVersion.VersionString
        account = Get-AccountInfo
        started_at = $startedAt.ToString("o")
        finished_at = $finishedAt.ToString("o")
        duration_seconds = [math]::Round(($finishedAt - $startedAt).TotalSeconds, 3)
        package_count = $results.Count
        passed_count = @($results | Where-Object passed).Count
        failed_count = @($results | Where-Object { -not $_.passed }).Count
        results = @($results | ForEach-Object { $_ })
        fatal_error = $fatalError
        residual_processes = @(Get-TrackedResidualProcesses)
    }
    $summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
}

if ($fatalError -or $summary.failed_count -gt 0) {
    Write-Error "Serial workspace test failed. See $summaryPath."
    exit 1
}

Write-Host "Serial workspace test passed for $($summary.package_count) packages. Summary: $summaryPath"
