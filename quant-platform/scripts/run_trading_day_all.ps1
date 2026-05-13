param(
  [switch]$RestartServer,
  [switch]$VisibleWindows
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Scripts = Join-Path $Root "scripts"
$LogDir = Join-Path $Root "reports\runtime_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$RunStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$TradeDate = Get-Date -Format "yyyy-MM-dd"
$SupervisorLog = Join-Path $LogDir "trading_day_$RunStamp.log"
Start-Transcript -Path $SupervisorLog -Append | Out-Null

$ServerScript = Join-Path $Root "run.ps1"
$MinuteScript = Join-Path $Scripts "run_realtime_minute_loop.ps1"
$EmailScript = Join-Path $Scripts "run_email_signal_alert.ps1"
$ConfigPath = Join-Path $Root "config\email_alert.json"

if (-not (Test-Path $ConfigPath)) {
  throw "Email config not found. Run scripts\setup_email_secret.ps1 first."
}

function Test-PortListening {
  param([int]$Port)
  $line = netstat -ano | Select-String ":$Port" | Select-String "LISTENING"
  return [bool]$line
}

function Stop-Port {
  param([int]$Port)
  $ids = netstat -ano |
    Select-String ":$Port" |
    Select-String "LISTENING" |
    ForEach-Object { ($_ -split "\s+")[-1] } |
    Where-Object { $_ -match "^\d+$" -and $_ -ne "0" } |
    Sort-Object -Unique
  foreach ($id in $ids) {
    Stop-Process -Id ([int]$id) -Force -ErrorAction SilentlyContinue
  }
}

function Test-ProcessCommand {
  param([string]$Pattern)
  try {
    $match = Get-CimInstance Win32_Process |
      Where-Object { $_.CommandLine -like "*$Pattern*" -and $_.ProcessId -ne $PID } |
      Select-Object -First 1
    return [bool]$match
  } catch {
    Write-Host "Process check failed for ${Pattern}: $($_.Exception.Message)"
    return $false
  }
}

function Start-QuantProcess {
  param(
    [string]$Name,
    [string]$ScriptPath,
    [string]$ProcessMatch = ""
  )
  if ($ProcessMatch -and (Test-ProcessCommand -Pattern $ProcessMatch)) {
    Write-Host "$Name already running; skip duplicate start. match=$ProcessMatch"
    return
  }
  $LogPath = Join-Path $LogDir "$TradeDate`_$Name`_$RunStamp.log"
  $WindowStyle = if ($VisibleWindows) { "Normal" } else { "Hidden" }
  $Command = "Write-Host '[service-start] $Name $(Get-Date -Format s)'; & '$ScriptPath' *>&1 | Tee-Object -FilePath '$LogPath' -Append"
  $Process = Start-Process `
    -FilePath "powershell.exe" `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $Command) `
    -WorkingDirectory $Root `
    -WindowStyle $WindowStyle `
    -PassThru
  Write-Host "Started $Name pid=$($Process.Id) log=$LogPath"
}

if ($RestartServer) {
  Stop-Port -Port 8765
  Start-Sleep -Seconds 1
}

if (Test-PortListening -Port 8765) {
  Write-Host "Platform server already listening on 127.0.0.1:8765"
} else {
  Start-QuantProcess -Name "platform_server" -ScriptPath $ServerScript
  Start-Sleep -Seconds 4
}

Start-QuantProcess -Name "realtime_minute_loop" -ScriptPath $MinuteScript -ProcessMatch "realtime_minute_loop.py"
Start-QuantProcess -Name "email_signal_alert" -ScriptPath $EmailScript -ProcessMatch "wechat_signal_alert.py"

Write-Host ""
Write-Host "All trading-day services started."
Write-Host "Open: http://127.0.0.1:8765/"
Write-Host "Logs: $LogDir"
Write-Host "Supervisor log: $SupervisorLog"
Stop-Transcript | Out-Null
