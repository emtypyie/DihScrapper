# Copy the archive-side files from DihScrapper into a ScrapedDih checkout.
#
# The archive's workflow cannot live in DihScrapper: it fires on pushes to
# ScrapedDih/inbox/, and a workflow only sees its own repository's pushes. This
# copies the workflow, and copies (never edits) muncher.py and formatter.py, so
# the merge logic keeps exactly one source.
#
#   .\archive-repo\sync.ps1 -Check -Target ..\ScrapedDih   # report drift
#   .\archive-repo\sync.ps1 -Target ..\ScrapedDih           # install

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Target,

    [switch]$Check
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$target = (Resolve-Path -LiteralPath $Target).Path

$files = @(
    @{ From = Join-Path $PSScriptRoot '.github\workflows\muncher.yml'; To = '.github\workflows\muncher.yml' }
    @{ From = Join-Path $root 'muncher.py'; To = 'muncher.py' }
    @{ From = Join-Path $root 'formatter.py'; To = 'formatter.py' }
)

$stale = 0
foreach ($file in $files) {
    $destination = Join-Path $target $file.To
    $same = (Test-Path -LiteralPath $destination) -and
        ((Get-FileHash -LiteralPath $file.From -Algorithm SHA256).Hash -eq
         (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash)

    if ($same) {
        Write-Host "up to date  $($file.To)"
        continue
    }

    $stale++
    if ($Check) {
        Write-Host "OUTDATED   $($file.To)"
        continue
    }

    $directory = Split-Path -Parent $destination
    if (-not (Test-Path -LiteralPath $directory)) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }
    Copy-Item -LiteralPath $file.From -Destination $destination -Force
    Write-Host "installed  $($file.To)"
}

if ($stale -eq 0) {
    Write-Host "`narchive repo already matches DihScrapper"
} elseif ($Check) {
    Write-Host "`n$stale file(s) differ -- rerun without -Check to install"
} else {
    Write-Host "`n$stale file(s) installed into $target -- review, then commit and push"
}