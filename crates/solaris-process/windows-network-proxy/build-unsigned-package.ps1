[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
    [string]$BinaryPath,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9.-]{3,50}$')]
    [string]$PackageName,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Publisher,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d+\.\d+\.\d+\.\d+$')]
    [string]$Version,

    [Parameter(Mandatory = $true)]
    [ValidateSet('x64', 'x86', 'arm64')]
    [string]$Architecture,

    [string]$MakeAppxPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Windows 11 reserves a distinct publisher namespace for packages installed
# through the AllowUnsigned deployment path. A package without this OID is rejected
# with ERROR_UNSIGNED_PACKAGE_INVALID_PUBLISHER_NAMESPACE (0x80073D2C).
$unsignedPublisherOid = 'OID.2.25.311729368913984317654407730594956997722=1'
if ($Publisher -match '(?i)(?:^|,\s*)OID\.2\.25\.\d+=1(?:,|$)') {
    $unsignedPublisher = $Publisher
}
else {
    $unsignedPublisher = "$Publisher, $unsignedPublisherOid"
}

function Resolve-MakeAppx {
    param([string]$ExplicitPath)

    if ($ExplicitPath) {
        $resolved = (Resolve-Path -LiteralPath $ExplicitPath).Path
        if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
            throw 'MakeAppx.exe was not found at the explicit path.'
        }
        return $resolved
    }
    $command = Get-Command 'MakeAppx.exe' -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }
    $kitsRoot = Join-Path ${env:ProgramFiles(x86)} 'Windows Kits\10\bin'
    $candidate = Get-ChildItem -LiteralPath $kitsRoot -Directory -ErrorAction SilentlyContinue |
        Sort-Object Name -Descending |
        ForEach-Object { Join-Path $_.FullName 'x64\MakeAppx.exe' } |
        Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } |
        Select-Object -First 1
    if (-not $candidate) {
        throw 'MakeAppx.exe is unavailable.'
    }
    return $candidate
}

function Save-PackageLogo {
    param(
        [string]$Path,
        [int]$Size
    )

    Add-Type -AssemblyName System.Drawing
    $bitmap = [System.Drawing.Bitmap]::new($Size, $Size)
    try {
        $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
        try {
            $graphics.Clear([System.Drawing.Color]::FromArgb(18, 24, 38))
            $brush = [System.Drawing.SolidBrush]::new([System.Drawing.Color]::FromArgb(59, 130, 246))
            try {
                $margin = [Math]::Max(2, [int]($Size / 6))
                $graphics.FillEllipse($brush, $margin, $margin, $Size - 2 * $margin, $Size - 2 * $margin)
            }
            finally {
                $brush.Dispose()
            }
        }
        finally {
            $graphics.Dispose()
        }
        $bitmap.Save($Path, [System.Drawing.Imaging.ImageFormat]::Png)
    }
    finally {
        $bitmap.Dispose()
    }
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$templatePath = Join-Path $scriptRoot 'AppxManifest.xml'
$resolvedBinary = (Resolve-Path -LiteralPath $BinaryPath).Path
$resolvedOutput = [System.IO.Path]::GetFullPath($OutputPath)
$outputParent = Split-Path -Parent $resolvedOutput
if (-not $outputParent) {
    throw 'OutputPath must include a parent directory.'
}
New-Item -ItemType Directory -Path $outputParent -Force | Out-Null

$temporaryRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
$workingRoot = Join-Path $temporaryRoot ("solaris-proxy-package-" + [Guid]::NewGuid().ToString('N'))
$staging = Join-Path $workingRoot 'staging'
$verification = Join-Path $workingRoot 'verification'
New-Item -ItemType Directory -Path (Join-Path $staging 'Assets') -Force | Out-Null

try {
    Copy-Item -LiteralPath $resolvedBinary -Destination (Join-Path $staging 'solaris-windows-network-proxy.exe')
    [xml]$manifest = Get-Content -LiteralPath $templatePath -Raw
    $namespace = [System.Xml.XmlNamespaceManager]::new($manifest.NameTable)
    $namespace.AddNamespace('f', 'http://schemas.microsoft.com/appx/manifest/foundation/windows10')
    $identity = $manifest.SelectSingleNode('/f:Package/f:Identity', $namespace)
    if (-not $identity) {
        throw 'The package manifest identity is missing.'
    }
    $identity.SetAttribute('Name', $PackageName)
    $identity.SetAttribute('Publisher', $unsignedPublisher)
    $identity.SetAttribute('Version', $Version)
    $identity.SetAttribute('ProcessorArchitecture', $Architecture)
    $xmlSettings = [System.Xml.XmlWriterSettings]::new()
    $xmlSettings.Encoding = [System.Text.UTF8Encoding]::new($false)
    $xmlSettings.Indent = $true
    $writer = [System.Xml.XmlWriter]::Create((Join-Path $staging 'AppxManifest.xml'), $xmlSettings)
    try {
        $manifest.Save($writer)
    }
    finally {
        $writer.Dispose()
    }

    Save-PackageLogo -Path (Join-Path $staging 'Assets\StoreLogo.png') -Size 50
    Save-PackageLogo -Path (Join-Path $staging 'Assets\Square44x44Logo.png') -Size 44
    Save-PackageLogo -Path (Join-Path $staging 'Assets\Square150x150Logo.png') -Size 150

    $makeAppx = Resolve-MakeAppx -ExplicitPath $MakeAppxPath
    & $makeAppx pack /d $staging /p $resolvedOutput /o | Out-Host
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $resolvedOutput -PathType Leaf)) {
        throw 'MakeAppx pack failed.'
    }
    & $makeAppx unpack /p $resolvedOutput /d $verification /o | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw 'MakeAppx unpack verification failed.'
    }
    if (Test-Path -LiteralPath (Join-Path $verification 'AppxSignature.p7x')) {
        throw 'The unsigned package unexpectedly contains a signature.'
    }
    $stagedBinaryHash = (Get-FileHash -LiteralPath (Join-Path $staging 'solaris-windows-network-proxy.exe') -Algorithm SHA256).Hash
    $verifiedBinaryHash = (Get-FileHash -LiteralPath (Join-Path $verification 'solaris-windows-network-proxy.exe') -Algorithm SHA256).Hash
    if ($stagedBinaryHash -ne $verifiedBinaryHash) {
        throw 'The packaged proxy binary digest changed during packaging.'
    }
    [pscustomobject]@{
        package_path = $resolvedOutput
        package_sha256 = (Get-FileHash -LiteralPath $resolvedOutput -Algorithm SHA256).Hash.ToLowerInvariant()
        binary_sha256 = $verifiedBinaryHash.ToLowerInvariant()
        package_name = $PackageName
        publisher = $unsignedPublisher
        application_id = 'Proxy'
        signed = $false
    }
}
finally {
    $resolvedWorkingRoot = [System.IO.Path]::GetFullPath($workingRoot)
    if (-not $resolvedWorkingRoot.StartsWith($temporaryRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'Refusing to remove a package directory outside the temporary root.'
    }
    if (Test-Path -LiteralPath $resolvedWorkingRoot) {
        Remove-Item -LiteralPath $resolvedWorkingRoot -Recurse -Force
    }
}
