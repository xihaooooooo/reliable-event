param(
    [string] $PromtoolPath,
    [string] $EvidenceDirectory
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$obsDir = Join-Path $root 'observability'
$rulesDir = Join-Path $obsDir 'rules'
$evidenceRoot = if ($EvidenceDirectory) { [IO.Path]::GetFullPath($EvidenceDirectory) } else { Join-Path $root 'target/evidence' }
$toolsRoot = Join-Path $root 'target/tools'
New-Item -ItemType Directory -Force -Path $evidenceRoot | Out-Null
$stamp = '{0}-{1}' -f (Get-Date -Format 'yyyyMMdd-HHmmss'), ([guid]::NewGuid().ToString('N').Substring(0, 8))
$roundDir = Join-Path $evidenceRoot "m8.5-promtool-$stamp"
New-Item -ItemType Directory -Force -Path $roundDir | Out-Null
$combinedLog = Join-Path $roundDir 'all-commands.log'

$localPromtool = $null
if ($PromtoolPath) {
    $localPromtool = (Resolve-Path -LiteralPath $PromtoolPath).Path
} else {
    $localPromtool = Get-ChildItem -LiteralPath $toolsRoot -Filter promtool.exe -File -Recurse -ErrorAction SilentlyContinue |
        Select-Object -First 1 -ExpandProperty FullName
}
$useLocalPromtool = -not [string]::IsNullOrWhiteSpace($localPromtool)

$localConfig = Join-Path $roundDir 'prometheus-local-check.yml'
if ($useLocalPromtool) {
    $configText = Get-Content -LiteralPath (Join-Path $obsDir 'prometheus.yml') -Raw
    $rulePath = (Join-Path $rulesDir 'reliable-event.yml').Replace('\', '/')
    $configText = $configText.Replace('/etc/prometheus/rules/reliable-event.yml', $rulePath)
    Set-Content -LiteralPath $localConfig -Value $configText -Encoding utf8NoBOM
    $versionLog = Join-Path $roundDir 'promtool-version.log'
    & $localPromtool --version *> $versionLog
    $versionExit = $LASTEXITCODE
    $versionText = Get-Content -LiteralPath $versionLog -Raw
    if ($versionExit -ne 0 -or $versionText -notmatch 'version 3\.14\.0') {
        throw "Expected Prometheus promtool v3.14.0; got exit=$versionExit, output: $versionText"
    }
    Add-Content -LiteralPath $combinedLog -Value "promtool_source=local`n$versionText"
} else {
    Add-Content -LiteralPath $combinedLog -Value 'promtool_source=quay.io/prometheus/prometheus:v3.14.0'
    $versionLog = Join-Path $roundDir 'promtool-version.log'
    Push-Location $obsDir
    try {
        docker compose run --rm --no-deps --entrypoint /bin/promtool prometheus --version *> $versionLog
    } finally {
        Pop-Location
    }
    $dockerVersionExit = $LASTEXITCODE
    $dockerVersionText = Get-Content -LiteralPath $versionLog -Raw
    Add-Content -LiteralPath $versionLog -Value "`ncommand_exit_code=$dockerVersionExit"
    Add-Content -LiteralPath $combinedLog -Value "`n===== promtool version =====`n$dockerVersionText`ncommand_exit_code=$dockerVersionExit"
    if ($dockerVersionExit -ne 0 -or $dockerVersionText -notmatch 'version 3\.14\.0') {
        Add-Content -LiteralPath $combinedLog -Value 'promtool_version_check=FAIL'
    } else {
        Add-Content -LiteralPath $combinedLog -Value 'promtool_version_check=PASS'
    }
}

function Invoke-ValidationCommand {
    param(
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [string[]] $Arguments
    )

    $log = Join-Path $roundDir "$Name.log"
    $exitFile = Join-Path $roundDir "$Name.exit-code.txt"
    Add-Content -LiteralPath $combinedLog -Value "`n===== $Name ====="
    if ($useLocalPromtool) {
        & $localPromtool @Arguments *> $log
    } else {
        Push-Location $obsDir
        try {
            docker compose run --rm --no-deps --entrypoint /bin/promtool prometheus @Arguments *> $log
        } finally {
            Pop-Location
        }
    }
    $code = $LASTEXITCODE
    Add-Content -LiteralPath $log -Value "`ncommand_exit_code=$code"
    Add-Content -LiteralPath $exitFile -Value "$code"
    Add-Content -LiteralPath $combinedLog -Value (Get-Content -LiteralPath $log -Raw)
    return [pscustomobject]@{ Name = $Name; ExitCode = $code; Log = $log }
}

$checks = @()
if ($useLocalPromtool) {
    $checks += Invoke-ValidationCommand -Name 'config-check' -Arguments @('check', 'config', $localConfig)
    $checks += Invoke-ValidationCommand -Name 'rules-check' -Arguments @('check', 'rules', (Join-Path $rulesDir 'reliable-event.yml'))
    $checks += Invoke-ValidationCommand -Name 'rules-test' -Arguments @('test', 'rules', (Join-Path $rulesDir 'rule-tests.yml'))
} else {
    $checks += Invoke-ValidationCommand -Name 'config-check' -Arguments @('check', 'config', '/etc/prometheus/prometheus.yml')
    $checks += Invoke-ValidationCommand -Name 'rules-check' -Arguments @('check', 'rules', '/etc/prometheus/rules/reliable-event.yml')
    $checks += Invoke-ValidationCommand -Name 'rules-test' -Arguments @('test', 'rules', '/etc/prometheus/rules/rule-tests.yml')
}

$summary = $checks | ForEach-Object { "$($_.Name)_exit_code=$($_.ExitCode) log=$($_.Log)" }
Add-Content -LiteralPath $combinedLog -Value "`n===== result =====`n$($summary -join "`n")"
$checks | Format-Table -AutoSize
Write-Host "Evidence round: $roundDir"
if ($checks.Where({ $_.ExitCode -ne 0 }).Count -gt 0) {
    exit 1
}
if (-not $useLocalPromtool -and ($dockerVersionExit -ne 0 -or $dockerVersionText -notmatch 'version 3\.14\.0')) {
    exit 1
}
exit 0
