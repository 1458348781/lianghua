$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $Root ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
  $Python = "D:\anconda3\python.exe"
}

if (-not (Test-Path $Python)) {
  $Python = "python"
}

Set-Location $Root
& $Python .\start_platform.py --host 127.0.0.1 --port 8765
