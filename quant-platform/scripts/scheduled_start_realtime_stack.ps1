$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$LogDir = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Test-PythonCommandRunning {
  param([Parameter(Mandatory = $true)][string]$Pattern)
  $procs = Get-CimInstance Win32_Process -Filter "name = 'python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like $Pattern }
  return [bool]$procs
}

function Start-HiddenPowerShell {
  param(
    [Parameter(Mandatory = $true)][string]$ScriptPath,
    [Parameter(Mandatory = $true)][string]$Name
  )
  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $out = Join-Path $LogDir "$Name`_$stamp.out.log"
  $err = Join-Path $LogDir "$Name`_$stamp.err.log"
  Start-Process -FilePath powershell.exe `
    -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $ScriptPath `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $out `
    -RedirectStandardError $err | Out-Null
}

$platformScript = Join-Path $Root "run.ps1"
$minuteScript = Join-Path $Root "scripts\run_realtime_minute_loop.ps1"
$emailScript = Join-Path $Root "scripts\run_email_signal_alert.ps1"

if (-not (Test-PythonCommandRunning "*start_platform.py*")) {
  Start-HiddenPowerShell -ScriptPath $platformScript -Name "scheduled_platform"
}

if (-not (Test-PythonCommandRunning "*realtime_minute_loop.py*")) {
  Start-HiddenPowerShell -ScriptPath $minuteScript -Name "scheduled_realtime_minute"
}

if (-not (Test-PythonCommandRunning "*wechat_signal_alert.py*")) {
  Start-HiddenPowerShell -ScriptPath $emailScript -Name "scheduled_email_alert"
}
