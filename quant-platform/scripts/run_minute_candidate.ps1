$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

Write-Host "Using Python: $Python"

& $Python (Join-Path $Root "scripts\download_minute_data.py") `
  --mode candidate `
  --start-date 2020-01-01 `
  --end-date 2026-05-10 `
  --board all `
  --workers 10 `
  --candidate-forward-days 5 `
  --source sina

