param(
    [switch]$StartExample
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$obsDir = Join-Path $root 'observability'
$evidenceDir = Join-Path $root 'target/evidence'
New-Item -ItemType Directory -Force -Path $evidenceDir | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$upLog = Join-Path $evidenceDir "m8.5-compose-start-$stamp.log"
$upExit = "$upLog.exit-code.txt"
$receiverPidPath = Join-Path $evidenceDir 'm8.5-receiver.pid'
function Test-LocalTcpPort([int]$port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try { $client.Connect('127.0.0.1', $port); return $true }
    catch { return $false }
    finally { $client.Dispose() }
}
if (Test-LocalTcpPort 19080) {
    throw 'Port 19080 is already in use; inspect its owning process before continuing.'
}
Push-Location $obsDir
try {
    docker compose up -d --pull missing *> $upLog
    $upCode = $LASTEXITCODE
    Set-Content -LiteralPath $upExit -Value $upCode
    if ($upCode -ne 0) { throw "Could not start the observability Compose project or pull a missing pinned image; see $upLog" }
    $networkName = 'reliable-event-observability_default'
    $networkSubnet = docker network inspect $networkName --format '{{(index .IPAM.Config 0).Subnet}}'
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($networkSubnet)) { throw "Could not discover the M8.5 Docker bridge subnet for the local Alertmanager receiver." }
} finally { Pop-Location }

$receiverStdout = Join-Path $evidenceDir "m8.5-receiver-$stamp.stdout.log"
$receiverStderr = Join-Path $evidenceDir "m8.5-receiver-$stamp.stderr.log"
$receiverScript = Join-Path $PSScriptRoot 'local-alert-receiver.ps1'
$receiver = Start-Process -FilePath 'pwsh.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $receiverScript, '-AllowedSubnet', $networkSubnet) -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput $receiverStdout -RedirectStandardError $receiverStderr
Set-Content -LiteralPath $receiverPidPath -Value $receiver.Id
Start-Sleep -Milliseconds 500
if (-not (Test-LocalTcpPort 19080)) {
    throw "The local alert receiver did not bind port 19080; see $receiverStderr"
}

if ($StartExample) {
    $java17 = 'C:\Program Files\Java\jdk-17'
    $maven = 'D:\Maven\apache-maven-3.9.9\bin\mvn.cmd'
    $pidPath = Join-Path $evidenceDir 'm8.5-example.pid'
    $appJar = Join-Path $root 'reliable-event-example/target/reliable-event-example-0.1.0-SNAPSHOT.jar'
    $buildLog = Join-Path $evidenceDir "m8.5-example-package-$stamp.log"
    $buildExit = "$buildLog.exit-code.txt"
    $stdout = Join-Path $evidenceDir "m8.5-example-$stamp.stdout.log"
    $stderr = Join-Path $evidenceDir "m8.5-example-$stamp.stderr.log"
    $env:JAVA_HOME = $java17
    $env:PATH = (Join-Path $java17 'bin') + ';' + $env:PATH
    Push-Location $root
    try {
        & $maven '-Dmaven.repo.local=C:\Users\20659\.m2\repository' -Pobservability -pl reliable-event-example -am -DskipTests package *> $buildLog
        $buildCode = $LASTEXITCODE
        Set-Content -LiteralPath $buildExit -Value $buildCode
        if ($buildCode -ne 0) { throw "Example packaging failed; see $buildLog" }
    } finally { Pop-Location }
    if (-not (Test-Path -LiteralPath $appJar)) { throw "Packaged example JAR not found: $appJar" }
    $java = Join-Path $java17 'bin/java.exe'
    $process = Start-Process -FilePath $java -ArgumentList @('-jar', $appJar, '--spring.profiles.active=observability', '--management.otlp.tracing.endpoint=http://localhost:4318/v1/traces') -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    Set-Content -LiteralPath $pidPath -Value $process.Id
    Write-Host "Started observability profile process $($process.Id). Logs: $stdout ; $stderr"
}
Write-Host 'Observability services are starting. Existing example MySQL/RocketMQ must be bootstrapped separately.'
