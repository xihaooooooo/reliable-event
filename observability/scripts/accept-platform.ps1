$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$obsDir = Join-Path $root 'observability'
$evidence = Join-Path $root 'target/evidence'
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$runDir = Join-Path $evidence "m8.5-platform-$stamp"
New-Item -ItemType Directory -Path $runDir | Out-Null

function Save-Json($name, $value) {
    ConvertTo-Json -InputObject $value -Depth 40 | Set-Content -LiteralPath (Join-Path $runDir $name) -Encoding utf8
}
function Require-Http($uri, $name) {
    $response = Invoke-WebRequest -Uri $uri -TimeoutSec 10
    $response.Content | Set-Content -LiteralPath (Join-Path $runDir $name) -Encoding utf8
    return $response
}
function Collect-Spans($node, $results) {
    if ($null -eq $node) { return }
    if ($node -is [System.Collections.IDictionary]) {
        if ($node.Contains('spanId') -and $node.Contains('name')) { [void]$results.Add($node) }
        foreach ($value in $node.Values) { Collect-Spans $value $results }
    } elseif ($node -is [System.Collections.IEnumerable] -and $node -isnot [string]) {
        foreach ($value in $node) { Collect-Spans $value $results }
    }
}
function Normalize-OtelId([string]$value) {
    if ([string]::IsNullOrWhiteSpace($value)) { return '' }
    if ($value -match '^(?:[0-9a-fA-F]{16}|[0-9a-fA-F]{32})$') { return $value.ToLowerInvariant() }
    return [Convert]::ToHexString([Convert]::FromBase64String($value)).ToLowerInvariant()
}

try {
    Require-Http 'http://127.0.0.1:8090/actuator/health' 'app-health.json' | Out-Null
    $metrics = Require-Http 'http://127.0.0.1:8090/actuator/prometheus' 'app-metrics.prom'
    foreach ($metric in @('reliable_event_unfinished_overdue', 'reliable_event_snapshot_last_success_timestamp', 'reliable_event_publication_persisted_total', 'reliable_event_publish_duration_seconds_count', 'reliable_event_retention_deleted_total', 'reliable_event_retention_oldest_eligible_age')) {
        if (-not $metrics.Content.Contains($metric)) { throw "Actual scrape is missing $metric" }
    }

    $traceId = [Guid]::NewGuid().ToString('N')
    $parentId = [Guid]::NewGuid().ToString('N').Substring(0, 16)
    $created = Invoke-RestMethod -Uri 'http://127.0.0.1:8090/orders' -Method Post -ContentType 'application/json' -Headers @{ traceparent = "00-$traceId-$parentId-01" } -Body '{"itemCode":"m8-5-observability","quantity":1}'
    Save-Json 'created-order.json' $created
    $order = $null
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $order = Invoke-RestMethod -Uri "http://127.0.0.1:8090/orders/$($created.orderId)" -TimeoutSec 5
            if ($order.handledCount -eq 1) { break }
        } catch { }
        Start-Sleep -Seconds 1
    }
    if ($null -eq $order -or $order.handledCount -ne 1) { throw 'The sample order did not reach handledCount=1.' }
    Save-Json 'handled-order.json' $order

    $trace = $null
    $spans = @()
    $traceReady = $false
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $trace = Require-Http "http://127.0.0.1:3200/api/v2/traces/$traceId" 'tempo-trace.json'
            if ($trace.StatusCode -eq 200) {
                $traceObject = Get-Content -Raw -LiteralPath (Join-Path $runDir 'tempo-trace.json') | ConvertFrom-Json -AsHashtable
                $spanList = [System.Collections.ArrayList]::new()
                Collect-Spans $traceObject $spanList
                $spans = @($spanList | ForEach-Object { [PSCustomObject]@{ name = $_.name; traceId = Normalize-OtelId $_.traceId; spanId = Normalize-OtelId $_.spanId; parentSpanId = Normalize-OtelId $_.parentSpanId } })
                $httpSpan = $spans | Where-Object { $_.name -match 'POST /orders' } | Select-Object -First 1
                $registerSpan = $spans | Where-Object { $_.name -eq 'reliable-event.register' } | Select-Object -First 1
                $publishSpan = $spans | Where-Object { $_.name -eq 'reliable-event.publish' } | Select-Object -First 1
                $consumeSpan = $spans | Where-Object { $_.name -eq 'example.order.consume' } | Select-Object -First 1
                $traceReady = $trace.StatusCode -eq 200 -and $null -ne $httpSpan -and $null -ne $registerSpan -and $null -ne $publishSpan -and $null -ne $consumeSpan -and $httpSpan.parentSpanId -eq $parentId -and $registerSpan.parentSpanId -eq $httpSpan.spanId -and $publishSpan.parentSpanId -eq $registerSpan.spanId -and $consumeSpan.parentSpanId -eq $publishSpan.spanId
                if ($traceReady) { break }
            }
        } catch { }
        Start-Sleep -Seconds 1
    }
    if (-not $traceReady) { throw "Tempo did not return the complete four-span parent chain for trace $traceId. Latest spans: $($spans.name -join ', ')" }
    Save-Json 'tempo-span-relationships.json' $spans
    if ($spans.Where({ $_.traceId -and $_.traceId -ne $traceId }).Count -gt 0) { throw 'Tempo response included a span with a different trace ID.' }
    if ($httpSpan.parentSpanId -ne $parentId -or $registerSpan.parentSpanId -ne $httpSpan.spanId -or $publishSpan.parentSpanId -ne $registerSpan.spanId -or $consumeSpan.parentSpanId -ne $publishSpan.spanId) {
        throw 'Tempo span parent chain did not match HTTP → registration → publication → consumer.'
    }

    $dashResponse = Require-Http 'http://127.0.0.1:3000/api/dashboards/uid/reliable-event-ops' 'grafana-dashboard-api.json'
    if ($dashResponse.StatusCode -ne 200) { throw 'Grafana dashboard provisioning API did not return 200.' }
    $dashboard = ($dashResponse.Content | ConvertFrom-Json).dashboard
    $tracePanel = $dashboard.panels | Where-Object { $_.id -eq 14 -and $_.type -eq 'traces' } | Select-Object -First 1
    if ($null -eq $tracePanel -or $tracePanel.datasource.uid -ne 'reliable-event-tempo' -or $tracePanel.targets[0].query -notmatch 'traceId') {
        throw 'Provisioned Grafana dashboard is missing its Tempo trace-ID panel.'
    }
    $panelResults = @()
    foreach ($panel in $dashboard.panels) {
        foreach ($target in $panel.targets) {
            if ([string]::IsNullOrWhiteSpace($target.expr)) { continue }
            $query = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:9090/api/v1/query' -ContentType 'application/x-www-form-urlencoded' -Body @{ query = $target.expr }
            $panelResults += [PSCustomObject]@{ panelId = $panel.id; panelTitle = $panel.title; refId = $target.refId; expr = $target.expr; status = $query.status; sampleCount = $query.data.result.Count; results = $query.data.result }
            if ($query.status -ne 'success') { throw "Grafana panel $($panel.id) query failed: $($target.expr)" }
        }
    }
    Save-Json 'grafana-panel-promql-results.json' $panelResults
    $livePanel = $panelResults | Where-Object { $_.panelId -eq 1 -and $_.sampleCount -gt 0 } | Select-Object -First 1
    if ($null -eq $livePanel) { throw 'Dashboard overdue panel PromQL returned no live scrape sample.' }
    $grafanaQueryBody = @{
        from = 'now-15m'; to = 'now'
        queries = @(@{ refId = 'A'; datasource = @{ type = 'prometheus'; uid = 'reliable-event-prometheus' }; expr = 'reliable_event_unfinished_overdue{job="reliable-event-example"}'; instant = $true; range = $false; format = 'table' })
    } | ConvertTo-Json -Depth 10 -Compress
    $grafanaDataQuery = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:3000/api/ds/query' -ContentType 'application/json' -Body $grafanaQueryBody
    Save-Json 'grafana-datasource-query-api.json' $grafanaDataQuery
    if ($null -eq $grafanaDataQuery.results.A -or $grafanaDataQuery.results.A.error -or $grafanaDataQuery.results.A.frames.Count -lt 1) { throw 'Grafana /api/ds/query failed to fetch data from its provisioned Prometheus data source.' }
    $grafanaTempoTrace = Require-Http "http://127.0.0.1:3000/api/datasources/proxy/uid/reliable-event-tempo/api/v2/traces/$traceId" 'grafana-tempo-proxy-trace.json'
    $grafanaTempoObject = Get-Content -Raw -LiteralPath (Join-Path $runDir 'grafana-tempo-proxy-trace.json') | ConvertFrom-Json -AsHashtable
    $grafanaSpanList = [System.Collections.ArrayList]::new()
    Collect-Spans $grafanaTempoObject $grafanaSpanList
    $grafanaSpanNames = @($grafanaSpanList | ForEach-Object { $_.name } | Select-Object -Unique)
    foreach ($requiredSpan in @('http post /orders', 'reliable-event.register', 'reliable-event.publish', 'example.order.consume')) {
        if ($grafanaSpanNames -notcontains $requiredSpan) { throw "Grafana Tempo data source proxy did not return $requiredSpan for the real order trace." }
    }
    Save-Json 'grafana-tempo-trace-span-names.json' $grafanaSpanNames

    $alertBase = 'http://127.0.0.1:9093/api/v2/alerts'
    $alertName = "M85ReceiverAcceptance-$stamp"
    $starts = (Get-Date).ToUniversalTime().AddMinutes(-2).ToString('o')
    $alert = @(@{ labels = @{ alertname = $alertName; severity = 'warning'; outbox_store = 'acceptance' }; annotations = @{ summary = 'Synthetic M8.5 local receiver acceptance'; }; startsAt = $starts; generatorURL = 'http://127.0.0.1:9090/graph'; } )
    $body = ConvertTo-Json -InputObject $alert -Depth 8 -Compress
    Invoke-RestMethod -Method Post -Uri $alertBase -ContentType 'application/json' -Body $body | Out-Null
    $firingReceived = $false
    for ($i = 0; $i -lt 20; $i++) {
        $recordsBefore = Require-Http 'http://127.0.0.1:19080/alerts' 'receiver-firing.ndjson'
        if ($recordsBefore.Content -match $alertName -and $recordsBefore.Content -match '"status":"firing"') { $firingReceived = $true; break }
        Start-Sleep -Seconds 1
    }
    if (-not $firingReceived) { throw 'Alertmanager firing notification was not recorded by the local receiver.' }
    $alert[0].endsAt = (Get-Date).ToUniversalTime().ToString('o')
    $body = ConvertTo-Json -InputObject $alert -Depth 8 -Compress
    Invoke-RestMethod -Method Post -Uri $alertBase -ContentType 'application/json' -Body $body | Out-Null
    $states = @()
    for ($i = 0; $i -lt 20; $i++) {
        $recordsAfter = Require-Http 'http://127.0.0.1:19080/alerts' 'receiver-firing-and-resolved.ndjson'
        $payloads = $recordsAfter.Content -split "`n" | Where-Object { $_ }
        $states = @($payloads | ForEach-Object { (ConvertFrom-Json $_).payload.alerts | Where-Object { $_.labels.alertname -eq $alertName } | ForEach-Object { $_.status } | Where-Object { $_ -is [string] } | Select-Object -Unique })
        if (($states -contains 'firing') -and ($states -contains 'resolved')) { break }
        Start-Sleep -Seconds 1
    }
    Save-Json 'receiver-acceptance-states.json' $states
    if (-not ($states -contains 'firing') -or -not ($states -contains 'resolved')) { throw "Local receiver did not record both firing and resolved. States: $($states -join ',')" }

    Save-Json 'acceptance-summary.json' @{ traceId = $traceId; parentSpanId = $parentId; orderId = $created.orderId; eventId = $created.eventId; dashboardPanelQueries = $panelResults.Count; liveDashboardSamples = $livePanel.sampleCount; grafanaTempoTraceSpans = $grafanaSpanNames; receiverAlertName = $alertName; receiverStates = $states; }
    Write-Host "M8.5 platform acceptance passed. Evidence: $runDir"
} catch {
    $_ | Out-String | Set-Content -LiteralPath (Join-Path $runDir 'failure.txt') -Encoding utf8
    throw
}
