param(
    [ValidateSet('pilot','full')][string]$Profile = 'pilot',
    [string]$Prefix = ('m53-' + (Get-Date -Format 'yyyyMMdd-HHmmss'))
)

$ErrorActionPreference = 'Stop'
if ($Prefix -notmatch '^[A-Za-z0-9-]{1,24}$') {
    throw 'Prefix must use 1-24 ASCII letters, digits or hyphens'
}
$benchmarkRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repoRoot = (Resolve-Path (Join-Path $benchmarkRoot '..')).Path
$runOnce = Join-Path $PSScriptRoot 'run-once.ps1'

Push-Location $repoRoot
try {
    if ($Profile -eq 'pilot') {
        & $runOnce -RunId "$Prefix-base-1" -Group baseline -Count 100 -TimeoutSeconds 300
        & $runOnce -RunId "$Prefix-multi-1" -Group multi -Count 100 -PublisherInstances 2 -TimeoutSeconds 300
        & $runOnce -RunId "$Prefix-unknown-1" -Group unknown -Count 100 -DropFirstReceipt -TimeoutSeconds 300
    } else {
        for ($round = 1; $round -le 3; $round++) {
            & $runOnce -RunId "$Prefix-base-$round" -Group baseline -Count 10000
        }
        foreach ($scale in @(100000,1000000)) {
            for ($round = 1; $round -le 3; $round++) {
                & $runOnce -RunId "$Prefix-h$scale-$round" -Group "history-$scale" -Count 10000 `
                    -HistoricalRows ($scale - 10000) -TimeoutSeconds 1800
            }
        }
        for ($round = 1; $round -le 3; $round++) {
            & $runOnce -RunId "$Prefix-poll-$round" -Group poll-200ms -Count 10000 -PollInterval '200ms'
            & $runOnce -RunId "$Prefix-batch-$round" -Group batch-100 -Count 10000 -ClaimBatchSize 100
            & $runOnce -RunId "$Prefix-thread-$round" -Group threads-16 -Count 10000 -WorkerThreads 16
            & $runOnce -RunId "$Prefix-queue-$round" -Group queue-50 -Count 10000 -WorkerQueueCapacity 50
            & $runOnce -RunId "$Prefix-multi-$round" -Group multi -Count 10000 -PublisherInstances 2
        }
        & $runOnce -RunId "$Prefix-outage-1" -Group outage -Count 10000 -PauseBrokerSeconds 15 -TimeoutSeconds 1800
        & $runOnce -RunId "$Prefix-unknown-1" -Group unknown -Count 10000 -DropFirstReceipt -TimeoutSeconds 1800
    }
    python (Join-Path $PSScriptRoot 'aggregate.py') (Join-Path $benchmarkRoot 'results') $Prefix
    if ($LASTEXITCODE -ne 0) { throw 'Matrix aggregation failed' }
} finally {
    Pop-Location
}
