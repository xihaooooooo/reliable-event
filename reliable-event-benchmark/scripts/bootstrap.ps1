param([switch]$SkipServiceStart)

$ErrorActionPreference = 'Stop'
$benchmarkRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$rocketHome = '/home/rocketmq/rocketmq-5.5.0'

function Assert-Exit([string]$step) {
    if ($LASTEXITCODE -ne 0) { throw "$step failed with exit code $LASTEXITCODE" }
}

Push-Location $benchmarkRoot
try {
    if (-not $SkipServiceStart) {
        docker compose up -d
        Assert-Exit 'Starting benchmark services'
    }
    $brokerReady = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $cluster = docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin clusterList -n nameserver:9876" 2>&1
        if ($LASTEXITCODE -eq 0 -and ($cluster -join "`n").Contains('broker-a')) {
            $brokerReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $brokerReady) { throw 'Broker registration was not visible within 120 seconds' }

    docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin updateTopic -n nameserver:9876 -c DefaultCluster -t reliable-event-benchmark"
    Assert-Exit 'Creating benchmark topic'
    docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin updateSubGroup -n nameserver:9876 -c DefaultCluster -g reliable-event-benchmark-consumer"
    Assert-Exit 'Creating benchmark consumer group'

    $routeReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $route = docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin topicRoute -n nameserver:9876 -t reliable-event-benchmark" 2>&1
        if ($LASTEXITCODE -eq 0 -and ($route -join "`n").Contains('broker-a')) {
            $routeReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $routeReady) { throw 'Benchmark topic route was not visible within 60 seconds' }

    $mysqlReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        docker compose exec -T mysql mysqladmin ping -h localhost -u root -pbenchmark-root --silent | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $mysqlReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $mysqlReady) { throw 'Benchmark MySQL was not ready within 60 seconds' }

    docker compose cp ../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql mysql:/tmp/reliable-event-outbox.sql
    Assert-Exit 'Copying Outbox schema'
    docker compose exec -T mysql mysql -ubenchmark -pbenchmark reliable_event_benchmark -e 'source /tmp/reliable-event-outbox.sql'
    Assert-Exit 'Applying Outbox schema'
    Write-Host 'Benchmark services, topic, consumer group and Outbox table are ready.'
} finally {
    Pop-Location
}
