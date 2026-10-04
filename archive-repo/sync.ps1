# Copy the archive-side files from DihScapper into a ScrapedDih checkout.
#
# The archive's workflows cannot live in DihScrapper: they act on ScrapedDih --
# the muncher fires on pushes to inbox/, and the backfill pushes to inbox/ at all
# -- and a workflow only ever sees its own repository. Both halves of that
# pipeline live in the archive repo, where the branch they move is.
#
# This copies the workflows, and copies (never edits) the code they run, so the
# logic keeps exactly one source. The backfill needs most of the app -- it reads
# history, stages a batch and publishes it -- so it brings capture, formatter,
# logger and pusher along with it.
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
    @{ From = Join-Path $PSScriptRoot '.github\workflows\backfill.yml'; To = '.github\workflows\backfill.yml' }
    @{ From = Join-Path $root 'muncher.py'; To = 'muncher.py' }
    @{ From = Join-Path $root 'backfill.py'; To = 'backfill.py' }
    @{ From = Join-Path $root 'capture.py'; To = 'capture.py' }
    @{ From = Join-Path $root 'formatter.py'; To = 'formatter.py' }
    @{ From = Join-Path $root 'logger.py'; To = 'logger.py' }
    @{ From = Join-Path $root 'pusher.py'; To = 'pusher.py' }
    # the backfill workflow installs from it
    @{ From = Join-Path $root 'requirements.txt'; To = 'requirements.txt' }
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