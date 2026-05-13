param(
  [string]$StartDate = "2020-01-01",
  [string]$EndDate = "2026-05-10",
  [string]$Board = "all",
  [int]$Workers = 2,
  [string]$Preset = "focused",
  [int]$MaxCombos = 300,
  [int]$SymbolLimit = 0
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

New-Item -ItemType Directory -Force -Path "D:\lianghua\result" | Out-Null

& $Python (Join-Path $Root "scripts\enumerate_divergence_params.py") `
  --start-date $StartDate `
  --end-date $EndDate `
  --board $Board `
  --workers $Workers `
  --preset $Preset `
  --max-combos $MaxCombos `
  --symbol-limit $SymbolLimit `
  --output-dir "D:\lianghua\result"
