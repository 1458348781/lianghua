param(
  [string]$To = "202212620012@nuist.edu.cn",
  [switch]$Once,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

$ConfigPath = Join-Path $Root "config\email_alert.json"
if (Test-Path $ConfigPath) {
  $Config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
  $env:SMTP_HOST = $Config.smtp_host
  $env:SMTP_PORT = [string]$Config.smtp_port
  $env:SMTP_USER = $Config.smtp_user
  $env:ALERT_EMAIL_TO = $Config.email_to
  $SecurePassword = ConvertTo-SecureString $Config.smtp_password_secure
  $Bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecurePassword)
  try {
    $env:SMTP_PASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($Bstr)
  } finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($Bstr)
  }
  if ($To -eq "202212620012@nuist.edu.cn" -and $Config.email_to) {
    $To = $Config.email_to
  }
}

Write-Host "Using Python: $Python"
Write-Host "Email To: $To"

$ArgsList = @(
  (Join-Path $Root "scripts\wechat_signal_alert.py"),
  "--times", "14:30,14:45",
  "--breakout-watch",
  "--auto-track-breakouts",
  "--position-watch",
  "--position-file", (Join-Path $Root "config\watch_positions.json"),
  "--board", "all",
  "--limit", "30",
  "--interval", "1",
  "--channel", "email",
  "--email-to", $To
)

if ($Once) {
  $ArgsList += "--once"
}
if ($DryRun) {
  $ArgsList += "--dry-run"
}

& $Python @ArgsList
