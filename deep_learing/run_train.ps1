$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = "D:\lianghua\quant-platform\.venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
  $Python = "python"
}

Set-Location $Root
& $Python .\build_dataset.py --start-date 2020-01-01 --end-date (Get-Date -Format "yyyy-MM-dd") --board all --min-rows 120
& $Python .\train_model.py --gpu
