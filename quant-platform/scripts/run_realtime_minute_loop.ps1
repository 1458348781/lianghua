$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

Write-Host "Using Python: $Python"

& $Python (Join-Path $Root "scripts\realtime_minute_loop.py") `
  --board all `
  --interval 1 `
  --minute-refresh-interval 5 `
  --candidate-minute-workers 1 `
  --start 09:25 `
  --end 15:00 `
  --after-close-download `
  --close-download-start 15:00 `
  --after-close-workers 8 `
  --after-close-daily-workers 2 `
  --after-close-daily-retry-workers 1 `
  --after-close-daily-retries 4 `
  --after-close-skip-min-daily-rows 4800 `
  --after-close-daily-complete-ratio 0.98
