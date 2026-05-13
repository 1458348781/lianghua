param(
  [string]$SmtpHost = "smtp.qq.com",
  [int]$SmtpPort = 465,
  [string]$SmtpUser,
  [string]$SmtpPassword,
  [string]$EmailTo = "202212620012@nuist.edu.cn"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$ConfigDir = Join-Path $Root "config"
$ConfigPath = Join-Path $ConfigDir "email_alert.json"
New-Item -ItemType Directory -Force -Path $ConfigDir | Out-Null

if (-not $SmtpUser) {
  $SmtpUser = Read-Host "SMTP user"
}
if (-not $SmtpPassword) {
  $SecureInput = Read-Host "SMTP authorization code" -AsSecureString
} else {
  $SecureInput = ConvertTo-SecureString $SmtpPassword -AsPlainText -Force
}

$EncryptedPassword = $SecureInput | ConvertFrom-SecureString
$Config = [ordered]@{
  smtp_host = $SmtpHost
  smtp_port = $SmtpPort
  smtp_user = $SmtpUser
  smtp_password_secure = $EncryptedPassword
  email_to = $EmailTo
}

$Config | ConvertTo-Json | Set-Content -LiteralPath $ConfigPath -Encoding UTF8
Write-Host "Email alert config saved: $ConfigPath"
