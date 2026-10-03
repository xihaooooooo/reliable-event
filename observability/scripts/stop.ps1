param([switch]$StopExample)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$pidPath = Join-Path $root 'target/evidence/m8.5-example.pid'
$receiverPidPath = Join-Path $root 'target/evidence/m8.5-receiver.pid'
if ($StopExample -and (Test-Path -LiteralPath $pidPath)) {
    $appPid = [int](Get-Content -LiteralPath $pidPath -Raw)
    $proc = Get-Process -Id $appPid -ErrorAction SilentlyContinue
    if ($null -ne $proc -and $proc.ProcessName -eq 'java') {
        $cmdline = (Get-CimInstance Win32_Process -Filter "ProcessId=$appPid").CommandLine
        if ($cmdline -like '*reliable-event-example-0.1.0-SNAPSHOT.jar*') { Stop-Process -Id $appPid -Force }
    }
    Remove-Item -LiteralPath $pidPath -Force
}
if (Test-Path -LiteralPath $receiverPidPath) {
    $receiverPid = [int](Get-Content -LiteralPath $receiverPidPath -Raw)
    $proc = Get-Process -Id $receiverPid -ErrorAction SilentlyContinue
    if ($null -ne $proc -and $proc.ProcessName -in @('powershell', 'pwsh')) {
        $cmdline = (Get-CimInstance Win32_Process -Filter "ProcessId=$receiverPid").CommandLine
        if ($cmdline -like '*local-alert-receiver.ps1*') { Stop-Process -Id $receiverPid -Force }
    }
    Remove-Item -LiteralPath $receiverPidPath -Force
}
$obsDir = Join-Path $root 'observability'
Push-Location $obsDir
try {
    docker compose down
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop the observability Compose project.' }
} finally { Pop-Location }
Write-Host 'Observability containers stopped. Named volumes were preserved.'
