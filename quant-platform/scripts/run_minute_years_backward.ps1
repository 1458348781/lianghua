param(
  [int]$StartYear = 2026,
  [int]$EndYear = 2020,
  [string]$Board = "all"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

Write-Host "Using Python: $Python"

for ($year = $StartYear; $year -ge $EndYear; $year--) {
  Write-Host "==== minute year backfill: $year / $Board ===="
  & $Python (Join-Path $Root "scripts\download_minute_data.py") `
    --mode year `
    --year $year `
    --board $Board `
    --workers 10 `
    --source sina
}

