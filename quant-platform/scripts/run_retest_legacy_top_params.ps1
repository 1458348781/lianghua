param(
  [string]$InputPath = "D:\lianghua\result\parameter_sweep_top_20260509_223624.csv",
  [string]$StartDate = "2020-01-01",
  [string]$EndDate = "2026-05-10",
  [string]$Board = "all",
  [int]$Workers = 2,
  [int]$Limit = 100,
  [switch]$ForceHighWorkers
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}
if (-not (Test-Path -LiteralPath $InputPath)) {
  throw "Input file not found: $InputPath"
}
if ($Workers -gt 3 -and -not $ForceHighWorkers) {
  Write-Warning "Workers=$Workers may freeze this machine. Capping to 3. Add -ForceHighWorkers only if you really want to override."
  $Workers = 3
}

New-Item -ItemType Directory -Force -Path "D:\lianghua\result" | Out-Null

& $Python (Join-Path $Root "scripts\retest_legacy_top_params.py") `
  --input $InputPath `
  --start-date $StartDate `
  --end-date $EndDate `
  --board $Board `
  --workers $Workers `
  --limit $Limit `
  --output-dir "D:\lianghua\result"
