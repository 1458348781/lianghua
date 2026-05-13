param(
  [int]$Year = 2025,
  [string]$Board = "all"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

Write-Host "Using Python: $Python"

& $Python (Join-Path $Root "scripts\download_minute_data.py") `
  --mode year `
  --year $Year `
  --board $Board `
  --workers 10 `
  --source sina

