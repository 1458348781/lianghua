param(
  [string]$TaskName = "LiangHuaTradingDay",
  [string]$StartTime = "09:15"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Runner = Join-Path $Root "scripts\run_trading_day_all.ps1"
if (-not (Test-Path $Runner)) {
  throw "Runner not found: $Runner"
}

$Action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`""
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At $StartTime
$Settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -MultipleInstances IgnoreNew

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $Action `
  -Trigger $Trigger `
  -Settings $Settings `
  -Description "Start quant platform, realtime minute loop, and email alerts." `
  -Force | Out-Null

Write-Host "Scheduled task registered: $TaskName at $StartTime on weekdays"
