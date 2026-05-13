param(
  [ValidateSet("smoke", "direction", "formal", "overnight")]
  [string]$Mode = "smoke",
  [int]$Trials = 0,
  [int]$Workers = 0,
  [string]$StartDate = "2024-01-01",
  [string]$EndDate = "",
  [ValidateSet("all", "main", "chinext", "star")]
  [string]$Board = "all",
  [int]$TopN = 50,
  [int]$MinTrades = 80,
  [int]$SymbolLimit = 0,
  [int]$Seed = 20260510,
  [string]$OutputDir = "D:\lianghua\result2"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  $Python = "python"
}

if ([string]::IsNullOrWhiteSpace($EndDate)) {
  $EndDate = Get-Date -Format "yyyy-MM-dd"
}

Set-Location $Root
$argsList = @(
  ".\scripts\optimize_divergence2_params.py",
  "--mode", $Mode,
  "--start-date", $StartDate,
  "--end-date", $EndDate,
  "--board", $Board,
  "--top-n", $TopN,
  "--min-trades", $MinTrades,
  "--symbol-limit", $SymbolLimit,
  "--seed", $Seed,
  "--output-dir", $OutputDir
)

if ($Trials -gt 0) {
  $argsList += @("--trials", $Trials)
}
if ($Workers -gt 0) {
  $argsList += @("--workers", $Workers)
}

& $Python @argsList
