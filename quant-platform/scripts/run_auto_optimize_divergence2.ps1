param(
  [string]$StartDate = "2015-01-01",
  [string]$EndDate = "",
  [ValidateSet("all", "main", "chinext", "star")]
  [string]$Board = "all",
  [int]$BaseSeed = 20260510,
  [int]$Workers = 3,
  [int]$MinTrades = 80,
  [double]$MinScore = 0,
  [int]$TopN = 50,
  [int]$SymbolLimit = 0,
  [ValidateSet("smoke", "direction", "formal", "formal3000", "overnight")]
  [string]$StopAfter = "formal3000",
  [switch]$IncludeOvernight,
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
  ".\scripts\auto_optimize_divergence2.py",
  "--start-date", $StartDate,
  "--end-date", $EndDate,
  "--board", $Board,
  "--base-seed", $BaseSeed,
  "--workers", $Workers,
  "--min-trades", $MinTrades,
  "--min-score", $MinScore,
  "--top-n", $TopN,
  "--symbol-limit", $SymbolLimit,
  "--stop-after", $StopAfter,
  "--output-dir", $OutputDir
)

if ($IncludeOvernight) {
  $argsList += "--include-overnight"
}

& $Python @argsList
