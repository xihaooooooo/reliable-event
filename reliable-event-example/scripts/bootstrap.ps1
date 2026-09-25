$ErrorActionPreference = 'Stop'
$exampleRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$rocketHome = '/home/rocketmq/rocketmq-5.5.0'

function Assert-LastExit([string]$step) {
    if ($LASTEXITCODE -ne 0) {
        throw "$step failed with exit code $LASTEXITCODE"
    }
}

Push-Location $exampleRoot
try {
    docker compose up -d
    Assert-LastExit 'Starting example services'

    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $cluster = docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin clusterList -n nameserver:9876" 2>&1
        if ($LASTEXITCODE -eq 0 -and ($cluster -join "`n").Contains('broker-a')) {
            $ready = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) {
        throw 'RocketMQ broker registration was not visible within 120 seconds'
    }

    docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin updateTopic -n nameserver:9876 -c DefaultCluster -t reliable-event-example"
    Assert-LastExit 'Creating the example topic'
    docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin updateSubGroup -n nameserver:9876 -c DefaultCluster -g reliable-event-example-consumer"
    Assert-LastExit 'Creating the example consumer group'

    $routeReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        $route = docker compose exec -T broker sh -c "$rocketHome/bin/mqadmin topicRoute -n nameserver:9876 -t reliable-event-example" 2>&1
        if ($LASTEXITCODE -eq 0 -and ($route -join "`n").Contains('broker-a')) {
            $routeReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $routeReady) {
        throw 'Example topic route was not visible within 60 seconds'
    }

    $mysqlReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        docker compose exec -T mysql mysqladmin ping -h localhost -u root -pexample-root --silent | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $mysqlReady = $true
            break
        }
        Start-Sleep -Seconds 2
    }
    if (-not $mysqlReady) {
        throw 'Example MySQL was not ready within 60 seconds'
    }

    docker compose cp ../reliable-event-jdbc/src/main/resources/schema/reliable-event-outbox.sql mysql:/tmp/reliable-event-outbox.sql
    Assert-LastExit 'Copying the Outbox schema'
    docker compose cp src/main/resources/schema/example-tables.sql mysql:/tmp/example-tables.sql
    Assert-LastExit 'Copying the example schema'
    docker compose exec -T mysql mysql -uexample -pexample reliable_event_example -e 'source /tmp/reliable-event-outbox.sql'
    Assert-LastExit 'Applying the Outbox schema'
    docker compose exec -T mysql mysql -uexample -pexample reliable_event_example -e 'source /tmp/example-tables.sql'
    Assert-LastExit 'Applying the example schema'
    Write-Host 'Example services, Topic, consumer group and database tables are ready.'
} finally {
    Pop-Location
}
