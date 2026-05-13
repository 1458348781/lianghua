param(
  [int]$StartYear = 2026,
  [int]$EndYear = 2020,
  [string]$Board = "all",
  [int]$Workers = 4,
  [switch]$Overwrite
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

Write-Host "Using Python: $Python"
Write-Host "Minute source: akshare_sina_intraday"
Write-Host "Range: $StartYear down to $EndYear / Board: $Board / Workers: $Workers"

for ($year = $StartYear; $year -ge $EndYear; $year--) {
  Write-Host "==== full-market minute backfill: $year / $Board ===="
  $argsList = @(
    (Join-Path $Root "scripts\download_minute_data.py"),
    "--mode", "year",
    "--year", [string]$year,
    "--board", $Board,
    "--workers", [string]$Workers,
    "--source", "sina"
  )
  if ($Overwrite) {
    $argsList += "--overwrite"
  }
  & $Python @argsList
}
