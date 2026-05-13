param(
  [string]$Channel = "wecom",
  [string]$WebhookUrl = ""
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
  throw "Project venv python not found: $Python"
}

Write-Host "Using Python: $Python"

$argsList = @(
  (Join-Path $Root "scripts\wechat_signal_alert.py"),
  "--times", "14:30,14:45",
  "--board", "all",
  "--limit", "30",
  "--interval", "20",
  "--channel", $Channel
)

if ($WebhookUrl -ne "") {
  $argsList += @("--webhook-url", $WebhookUrl)
}

& $Python @argsList
