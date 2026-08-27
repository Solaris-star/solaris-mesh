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

function Invoke-GitPorcelainZ {
    $start = New-Object Diagnostics.ProcessStartInfo
    $start.FileName = "git"
    $start.Arguments = "status --porcelain=v1 -z -uall"
    $start.WorkingDirectory = $repositoryRoot
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $process = New-Object Diagnostics.Process
    $process.StartInfo = $start
    if (-not $process.Start()) {
        throw "Failed to start git status."
    }
    $output = $process.StandardOutput.ReadToEnd()
    $errorOutput = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) {
        throw "git status failed: $errorOutput"
    }
    return $output
}

function ConvertFrom-GitPorcelainZ {
    param([AllowEmptyString()][string]$Value)
    $entries = New-Object 'Collections.Generic.List[object]'
    if ([string]::IsNullOrEmpty($Value)) {
        return @()
    }
    $fields = $Value.Split([char[]]@([char]0), [StringSplitOptions]::None)
    $index = 0
    while ($index -lt $fields.Length -and -not [string]::IsNullOrEmpty($fields[$index])) {
        $field = $fields[$index]
        if ($field.Length -lt 4) {
            throw "Malformed git porcelain entry."
        }
        $statusCode = $field.Substring(0, 2)
        $path = $field.Substring(3)
        $originalPath = $null
        if ($statusCode.IndexOf('R') -ge 0 -or $statusCode.IndexOf('C') -ge 0) {
            $index++
            if ($index -ge $fields.Length -or [string]::IsNullOrEmpty($fields[$index])) {
                throw "Malformed renamed git porcelain entry."
            }
            $originalPath = $fields[$index]
        }
        $entries.Add([pscustomobject]@{
            status = $statusCode
            path = $path
            original_path = $originalPath
        })
        $index++
    }
    return $entries.ToArray()
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
    $statusRaw = Invoke-GitPorcelainZ
    $statusEntries = @(ConvertFrom-GitPorcelainZ -Value $statusRaw)
    $statusText = ($statusEntries | ForEach-Object {
        if ($_.original_path) {
            "$($_.status) $($_.original_path) -> $($_.path)"
        } else {
            "$($_.status) $($_.path)"
        }
    }) -join "`n"
    $rust = Invoke-TextCommand rustc @('--version')
    $cargo = Invoke-TextCommand cargo @('--version')
    $dirtyFiles = @()
    foreach ($entry in $statusEntries) {
        $path = $entry.path
        $fullPath = Join-Path $repositoryRoot $path
        $dirtyFiles += [pscustomobject]@{
            status = $entry.status
            path = $path
            original_path = $entry.original_path
            sha256 = if (Test-Path -LiteralPath $fullPath -PathType Leaf) { (Get-FileHash -LiteralPath $fullPath -Algorithm SHA256).Hash.ToLowerInvariant() } else { $null }
        }
    }
    $summary = [pscustomobject]@{
        schema_version = 1
        repository_root = $repositoryRoot
        branch = $branch.value
        head = $head.value
        head_commit_time = $commitTime.value
        dirty = ($statusEntries.Count -gt 0)
        dirty_status = $statusText
        dirty_files = @($dirtyFiles)
        evidence_kind = if ($statusEntries.Count -eq 0) { 'commit_bound' } else { 'head_plus_dirty_worktree' }
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
