param(
    [ValidatePattern('^[A-Za-z0-9-]{1,48}$')][string]$RunId = ('run-' + (Get-Date -Format 'yyyyMMdd-HHmmss')),
    [ValidateRange(1,10000000)][int]$Count = 10000,
    [ValidateRange(0,10000000)][int]$HistoricalRows = 0,
    [ValidateRange(1,8)][int]$PublisherInstances = 1,
    [ValidateRange(1,10000)][int]$ClaimBatchSize = 50,
    [ValidateRange(1,256)][int]$WorkerThreads = 8,
    [ValidateRange(0,100000)][int]$WorkerQueueCapacity = 200,
    [ValidateRange(1,64)][int]$LoadThreads = 4,
    [ValidateRange(1,10000)][int]$LoadBatchSize = 100,
    [ValidateRange(0,3600)][int]$PauseBrokerSeconds = 0,
    [switch]$DropFirstReceipt,
    [ValidatePattern('^[A-Za-z0-9-]{1,48}$')][string]$Group = 'manual',
    [ValidateRange(30,7200)][int]$TimeoutSeconds = 900,
    [string]$PollInterval = '1s',
    [switch]$AdaptivePollingEnabled,
    [string]$ActivePollInterval = '100ms'
)

$ErrorActionPreference = 'Stop'
$benchmarkRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repoRoot = (Resolve-Path (Join-Path $benchmarkRoot '..')).Path
$jar = Join-Path $benchmarkRoot 'target/reliable-event-benchmark-0.1.0-SNAPSHOT.jar'
$runDir = Join-Path $benchmarkRoot "results/$RunId"
$processes = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
$publisherPids = [System.Collections.Generic.List[int]]::new()
$brokerPaused = $false
$startedAt = (Get-Date).ToUniversalTime()
$faultTimeline = Join-Path $runDir 'faults.csv'

function Assert-Exit([string]$step) {
    if ($LASTEXITCODE -ne 0) { throw "$step failed with exit code $LASTEXITCODE" }
}

function Get-GitCommit {
    $topLevel = & git -C $repoRoot rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    if ([System.IO.Path]::GetFullPath(($topLevel | Select-Object -First 1).Trim()).TrimEnd([char]92) -ine
            [System.IO.Path]::GetFullPath($repoRoot).TrimEnd([char]92)) { return $null }
    $commit = & git -C $repoRoot rev-parse HEAD 2>$null
    if ($LASTEXITCODE -eq 0) { return ($commit | Select-Object -First 1).Trim() }
    return $null
}

function Get-GitWorkingTree {
    $topLevel = & git -C $repoRoot rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    if ([System.IO.Path]::GetFullPath(($topLevel | Select-Object -First 1).Trim()).TrimEnd([char]92) -ine
            [System.IO.Path]::GetFullPath($repoRoot).TrimEnd([char]92)) { return $null }
    return (& git -C $repoRoot status --short 2>$null)
}

function Invoke-Java([string[]]$arguments) {
    & java -jar $jar @arguments
    Assert-Exit 'Benchmark Java command'
}

function Start-BenchmarkProcess([string]$name, [string[]]$arguments) {
    $stdout = Join-Path $runDir "$name.stdout.log"
    $stderr = Join-Path $runDir "$name.stderr.log"
    $command = @('-jar', ('"' + $jar + '"')) + $arguments
    $process = Start-Process -FilePath 'java' -ArgumentList $command -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    $processes.Add($process)
    return $process
}

function Wait-Ready([System.Diagnostics.Process]$process, [string]$path) {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        if (Test-Path -LiteralPath $path) { return }
        $process.Refresh()
        if ($process.HasExited) { throw "Process $($process.Id) exited before ready; see $runDir" }
        Start-Sleep -Seconds 1
    }
    throw "Process $($process.Id) did not become ready within 120 seconds"
}

function Read-Status {
    $sql = "SELECT COUNT(*),COALESCE(SUM(status=0),0),COALESCE(SUM(status=1),0),COALESCE(SUM(status=2),0),COALESCE(SUM(status=3),0),COALESCE(SUM(status=4),0) FROM reliable_event_outbox WHERE event_type='benchmark-event' AND event_key LIKE '$RunId-%'"
    $line = docker compose exec -T -e MYSQL_PWD=benchmark mysql mysql -N -B -ubenchmark reliable_event_benchmark -e $sql
    Assert-Exit 'Reading run status'
    $values = ($line | Select-Object -Last 1) -split "`t"
    if ($values.Count -ne 6) { throw "Unexpected status row: $line" }
    return [int[]]$values
}

function Get-M94SourceTreeSha256 {
    $entries = Get-ChildItem -LiteralPath $repoRoot -File -Recurse -Force |
        ForEach-Object {
            $relative = $_.FullName.Substring($repoRoot.Length).TrimStart([char[]]@([char]92, [char]47))
            if ($relative -notmatch '(^|[\\/])(?:\.git|target|results|__pycache__)([\\/]|$)|\.pyc$') {
                $fileHash = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
                "$($relative.Replace([string][char]92, '/')) $fileHash"
            }
        } | Sort-Object
    $bytes = [System.Text.Encoding]::UTF8.GetBytes(($entries -join "`n"))
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($sha.ComputeHash($bytes)).Replace('-', '').ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Get-SanitizedJdbcTarget {
    $jdbc = [Environment]::GetEnvironmentVariable('BENCHMARK_JDBC_URL')
    if (-not $jdbc) { return 'default:jdbc:mysql://localhost:3308/reliable_event_benchmark' }
    if ($jdbc -match '^jdbc:mysql://(?<authority>[^/?]+)(?:/(?<database>[^?]*))?') {
        $authority = $Matches.authority -replace '^.*@', ''
        return "jdbc:mysql://$authority/$($Matches.database)"
    }
    return 'custom-jdbc-url'
}

function Get-SanitizedRocketEndpoint {
    $endpoint = [Environment]::GetEnvironmentVariable('BENCHMARK_ROCKETMQ_ENDPOINT')
    if (-not $endpoint) { return 'default:localhost:8081' }
    $authority = $endpoint -replace '^(?:grpc://)?', '' -replace '^.*@', '' -replace '[/?#].*$', ''
    return $authority
}

function Get-SanitizedJvmMemoryOptions([string]$variableName) {
    $options = [Environment]::GetEnvironmentVariable($variableName)
    if (-not $options) { return 'default:JDK ergonomics' }
    $allowed = [regex]::Matches($options, '(?<!\S)(?:-Xms\S+|-Xmx\S+|-XX:ActiveProcessorCount=\d+)') |
        ForEach-Object { $_.Value }
    if ($allowed) { return $allowed -join ' ' }
    return 'custom-options-present; non-memory values omitted'
}

function Get-FatJarLibraryNames {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($jar)
    try {
        return @($archive.Entries | Where-Object { $_.FullName -match '^BOOT-INF/lib/[^/]+\.jar$' } |
            ForEach-Object { [System.IO.Path]::GetFileName($_.FullName) } | Sort-Object)
    } finally { $archive.Dispose() }
}

function Sample([System.IO.StreamWriter]$writer) {
    $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $status = Read-Status
    $mysqlId = (docker compose ps -q mysql).Trim()
    Assert-Exit 'Finding MySQL container'
    $brokerId = (docker compose ps -q broker).Trim()
    Assert-Exit 'Finding Broker container'
    $mysqlStats = (docker stats --no-stream --format '{{.CPUPerc}}|{{.MemUsage}}|{{.BlockIO}}' $mysqlId).Trim() -split '\|'
    Assert-Exit 'Sampling MySQL CPU'
    $brokerStats = (docker stats --no-stream --format '{{.CPUPerc}}|{{.MemUsage}}|{{.BlockIO}}' $brokerId).Trim() -split '\|'
    Assert-Exit 'Sampling Broker CPU'
    $appProcesses = @(Get-Process -Id $publisherPids.ToArray() -ErrorAction SilentlyContinue)
    $appCpuSeconds = ($appProcesses | Measure-Object -Property CPU -Sum).Sum
    $appMemoryBytes = ($appProcesses | Measure-Object -Property WorkingSet64 -Sum).Sum
    $appPrivateBytes = ($appProcesses | Measure-Object -Property PrivateMemorySize64 -Sum).Sum
    $appThreadCount = ($appProcesses | ForEach-Object { $_.Threads.Count } | Measure-Object -Sum).Sum
    $appHandleCount = ($appProcesses | Measure-Object -Property HandleCount -Sum).Sum
    $receiverProcess = @(Get-Process -Id $receiver.Id -ErrorAction SilentlyContinue)
    $receiverCpuSeconds = ($receiverProcess | Measure-Object -Property CPU -Sum).Sum
    $receiverMemoryBytes = ($receiverProcess | Measure-Object -Property WorkingSet64 -Sum).Sum
    $receiverPrivateBytes = ($receiverProcess | Measure-Object -Property PrivateMemorySize64 -Sum).Sum
    $receiverThreadCount = ($receiverProcess | ForEach-Object { $_.Threads.Count } | Measure-Object -Sum).Sum
    $receiverHandleCount = ($receiverProcess | Measure-Object -Property HandleCount -Sum).Sum
    $mysqlStatus = docker compose exec -T -e MYSQL_PWD=benchmark mysql mysql -N -B -ubenchmark reliable_event_benchmark `
        -e "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Innodb_row_lock_waits','Innodb_row_lock_time')"
    Assert-Exit 'Sampling MySQL status'
    $values = @{}
    foreach ($line in $mysqlStatus) {
        $parts = $line -split "`t"
        if ($parts.Count -eq 2) { $values[$parts[0]] = $parts[1] }
    }
    $writer.WriteLine(($now, $status[0], $status[1], $status[2], $status[3], $status[4], $status[5],
        ($mysqlStats[0] -replace '%',''), $mysqlStats[1], $mysqlStats[2],
        ($brokerStats[0] -replace '%',''), $brokerStats[1], $brokerStats[2],
        $appCpuSeconds, $appMemoryBytes, $appPrivateBytes, $appThreadCount, $appHandleCount,
        $receiverCpuSeconds, $receiverMemoryBytes, $receiverPrivateBytes, $receiverThreadCount, $receiverHandleCount,
        $values['Threads_connected'], $values['Threads_running'],
        $values['Innodb_row_lock_waits'], $values['Innodb_row_lock_time']) -join ',')
    $writer.Flush()
    return $status
}

if (-not (Test-Path -LiteralPath $jar)) { throw "Build the benchmark jar first: mvn -pl reliable-event-benchmark -am -DskipTests package" }
if (Test-Path -LiteralPath $runDir) { throw "Run directory already exists: $runDir" }
New-Item -ItemType Directory -Path $runDir | Out-Null

Push-Location $benchmarkRoot
try {
    & (Join-Path $PSScriptRoot 'bootstrap.ps1') -SkipServiceStart
    Invoke-Java @('--benchmark.mode=reset')
    if ($HistoricalRows -gt 0) {
        Invoke-Java @('--benchmark.mode=seed', "--benchmark.run-id=$RunId", "--benchmark.count=$HistoricalRows")
    }

    $runtimeLibraries = Get-FatJarLibraryNames
    $metadata = [ordered]@{
        runId = $RunId; group = $Group; startedUtc = $startedAt.ToString('o'); gitCommit = Get-GitCommit
        workingTree = Get-GitWorkingTree; javaVersion = (& java --version | Select-Object -First 1)
        mavenVersion = (& mvn --version | Select-Object -First 1)
        mysqlServerVersion = (& docker compose exec -T -e MYSQL_PWD=benchmark mysql mysql -N -B -ubenchmark reliable_event_benchmark -e 'SELECT VERSION()' | Select-Object -Last 1)
        mysqlImage = 'mysql:8.0.36'; rocketMqImage = 'apache/rocketmq:5.5.0'
        mysqlImageId = (& docker image inspect --format '{{.Id}}' mysql:8.0.36)
        rocketMqImageId = (& docker image inspect --format '{{.Id}}' apache/rocketmq:5.5.0)
        dockerServerVersion = (& docker version --format '{{.Server.Version}}')
        osVersion = [System.Environment]::OSVersion.VersionString
        cpu = @((Get-CimInstance Win32_Processor | Select-Object -ExpandProperty Name))
        logicalProcessors = [System.Environment]::ProcessorCount
        memoryBytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
        diskModels = @((Get-CimInstance Win32_DiskDrive | Select-Object -ExpandProperty Model))
        m94SourceCommit = [Environment]::GetEnvironmentVariable('M94_SOURCE_COMMIT')
        m94SourceMaterialization = [Environment]::GetEnvironmentVariable('M94_SOURCE_MATERIALIZATION')
        m94SourceTreeSha256 = Get-M94SourceTreeSha256
        m94MeasurementScriptSha256 = (Get-FileHash -LiteralPath $PSCommandPath -Algorithm SHA256).Hash.ToLowerInvariant()
        m94BenchmarkJarSha256 = (Get-FileHash -LiteralPath $jar -Algorithm SHA256).Hash.ToLowerInvariant()
        m94JdbcTarget = Get-SanitizedJdbcTarget
        m94RocketMqEndpoint = Get-SanitizedRocketEndpoint
        m94DatabasePoolSize = [Environment]::GetEnvironmentVariable('BENCHMARK_DB_POOL_SIZE')
        m94JavaToolOptions = Get-SanitizedJvmMemoryOptions 'JAVA_TOOL_OPTIONS'
        m94JdkJavaOptions = Get-SanitizedJvmMemoryOptions 'JDK_JAVA_OPTIONS'
        m94LegacyJavaOptions = Get-SanitizedJvmMemoryOptions '_JAVA_OPTIONS'
        m94JvmEnvironmentPolicy = 'experiment wrapper clears JDK_JAVA_OPTIONS and _JAVA_OPTIONS and sets JAVA_TOOL_OPTIONS=-Xms512m -Xmx512m; only allowlisted settings above are recorded'
        m94RuntimeLibraries = $runtimeLibraries
        m94TracingEnabledProperty = 'reliable-event.tracing-enabled=true (explicit publisher argument)'
        m94PublicationTracerRuntime = if (@($runtimeLibraries | Where-Object { $_ -match '^micrometer-tracing-[0-9].+\.jar$' }).Count -eq 0) {
            'PublicationTracer.NOOP (micrometer-tracing API absent from executable fat JAR)'
        } else { 'Micrometer Tracer API present; actual bean requires runtime context evidence' }
        m94TraceSampler = if (@($runtimeLibraries | Where-Object { $_ -match '^opentelemetry-sdk-[^/]+\.jar$' }).Count -eq 0) {
            'not configured; OpenTelemetry SDK absent from executable fat JAR'
        } else { 'SDK present; sampler configuration requires runtime context evidence' }
        m94SnapshotSamplerEnabledProperty = 'reliable-event.metrics-snapshot-enabled=true (explicit publisher argument)'
        m94MetricsSnapshotSampler = 'enabled by runtime auto-configuration in publisher mode; SimpleMeterRegistry bean, metrics observer, worker/cycle/scheduler are default-owned; interval PT15S explicit; first snapshot starts immediately'
        m94SamplePeriodNominalMs = 1000
        m94PublisherProcessMetrics = @('CPU seconds', 'Working Set bytes', 'Private Bytes', 'thread count', 'handle count')
        m94ReceiverProcessMetrics = @('CPU seconds', 'Working Set bytes', 'Private Bytes', 'thread count', 'handle count')
        m94ConsumerAckPolicy = 'SimpleConsumer.ack after message CSV flush'
        count = $Count; historicalRows = $HistoricalRows; publisherInstances = $PublisherInstances
        claimBatchSize = $ClaimBatchSize; workerThreads = $WorkerThreads
        workerQueueCapacity = $WorkerQueueCapacity; pollInterval = $PollInterval
        adaptivePollingEnabled = [bool]$AdaptivePollingEnabled; activePollInterval = $ActivePollInterval
        loadThreads = $LoadThreads; loadBatchSize = $LoadBatchSize
        payloadPaddingChars = 256; headerCount = 1
        pauseBrokerSeconds = $PauseBrokerSeconds; dropFirstReceipt = [bool]$DropFirstReceipt
    }
    $metadata | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $runDir 'metadata.json') -Encoding UTF8
    'epoch_ms,event' | Set-Content -LiteralPath $faultTimeline -Encoding UTF8

    $receiverReady = Join-Path $runDir 'receiver.ready'
    $receiverProgress = Join-Path $runDir 'receiver-progress.txt'
    $receiver = Start-BenchmarkProcess 'receiver' @('--benchmark.mode=receiver', "--benchmark.run-id=$RunId",
        "--benchmark.output=$(Join-Path $runDir 'messages.csv')", "--benchmark.ready-file=$receiverReady",
        "--benchmark.progress-file=$receiverProgress")
    Wait-Ready $receiver $receiverReady

    for ($index = 0; $index -lt $PublisherInstances; $index++) {
        $ready = Join-Path $runDir "publisher-$index.ready"
        $publisherArgs = @('--benchmark.mode=publisher', '--reliable-event.enabled=true',
            '--reliable-event.scheduling-enabled=true', '--reliable-event.tracing-enabled=true',
            '--reliable-event.metrics-snapshot-enabled=true', '--reliable-event.metrics-snapshot-interval=15s',
            "--benchmark.run-id=$RunId", "--benchmark.ready-file=$ready",
            "--benchmark.output=$(Join-Path $runDir "publisher-$index-meters.csv")",
            "--reliable-event.claim-batch-size=$ClaimBatchSize", "--reliable-event.worker-threads=$WorkerThreads",
            "--reliable-event.worker-queue-capacity=$WorkerQueueCapacity",
            "--reliable-event.poll-interval=$PollInterval",
            "--reliable-event.adaptive-polling-enabled=$([bool]$AdaptivePollingEnabled)",
            "--reliable-event.active-poll-interval=$ActivePollInterval")
        if ($DropFirstReceipt -and $index -eq 0) { $publisherArgs += '--benchmark.drop-first-receipt=true' }
        $publisher = Start-BenchmarkProcess "publisher-$index" $publisherArgs
        $publisherPids.Add($publisher.Id)
        Wait-Ready $publisher $ready
    }

    if ($PauseBrokerSeconds -gt 0) {
        docker compose pause broker
        Assert-Exit 'Pausing Broker'
        $brokerPaused = $true
        '{0},broker_paused' -f [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() | Add-Content -LiteralPath $faultTimeline
    }

    $load = Start-BenchmarkProcess 'load' @('--benchmark.mode=load', '--reliable-event.enabled=true',
        '--reliable-event.scheduling-enabled=false', "--benchmark.run-id=$RunId",
        "--benchmark.count=$Count", "--benchmark.batch-size=$LoadBatchSize",
        "--benchmark.load-threads=$LoadThreads", "--benchmark.output=$(Join-Path $runDir 'registration.csv')")

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $pauseUntil = (Get-Date).AddSeconds($PauseBrokerSeconds)
    $samplesPath = Join-Path $runDir 'samples.csv'
    $samples = [System.IO.StreamWriter]::new($samplesPath, $false, [System.Text.UTF8Encoding]::new($false))
    try {
        $samples.WriteLine('epoch_ms,total,pending,publishing,published,retry_wait,failed,mysql_cpu_pct,mysql_mem_usage,mysql_block_io,broker_cpu_pct,broker_mem_usage,broker_block_io,publisher_cpu_seconds,publisher_working_set_bytes,publisher_private_bytes,publisher_thread_count,publisher_handle_count,receiver_cpu_seconds,receiver_working_set_bytes,receiver_private_bytes,receiver_thread_count,receiver_handle_count,threads_connected,threads_running,innodb_row_lock_waits,innodb_row_lock_time_ms')
        do {
            if ($brokerPaused -and (Get-Date) -ge $pauseUntil) {
                docker compose unpause broker
                Assert-Exit 'Unpausing Broker'
                $brokerPaused = $false
                '{0},broker_unpaused' -f [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() | Add-Content -LiteralPath $faultTimeline
            }
            $status = Sample $samples
            $load.Refresh()
            if ($load.HasExited -and -not (Select-String -LiteralPath (Join-Path $runDir 'load.stdout.log') `
                    -Pattern "BENCHMARK_LOAD_RUN_ID=$RunId COUNT=$Count" -Quiet)) {
                throw "Load process exited without a completion marker; see $runDir"
            }
            if ((Get-Date) -ge $deadline) { throw "Run timed out after $TimeoutSeconds seconds; status=$($status -join ',')" }
            Start-Sleep -Seconds 1
        } until ($load.HasExited -and $status[0] -eq $Count -and $status[3] -eq $Count)
    } finally {
        $samples.Dispose()
    }

    $receiverDeadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $receiverDeadline) {
        $received = 0
        try { $received = [int](Get-Content -LiteralPath $receiverProgress -Raw -ErrorAction Stop) } catch { }
        if ($received -ge $Count) { break }
        $receiver.Refresh()
        if ($receiver.HasExited) { throw 'Receiver exited before the end of the run' }
        Start-Sleep -Seconds 1
    }
    $quietUntil = (Get-Date).AddSeconds(5)
    while ((Get-Date) -lt $quietUntil) {
        $receiver.Refresh()
        if ($receiver.HasExited) { throw 'Receiver exited before the end of the run' }
        Start-Sleep -Seconds 1
    }

    foreach ($process in $processes) {
        $process.Refresh()
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
            Wait-Process -Id $process.Id -ErrorAction SilentlyContinue
        }
    }

    Invoke-Java @('--benchmark.mode=report', "--benchmark.run-id=$RunId",
        "--benchmark.output=$(Join-Path $runDir 'events.csv')")

    $explainQueries = [ordered]@{
        due = 'EXPLAIN ANALYZE SELECT id, version FROM reliable_event_outbox WHERE status IN (0,3) AND next_attempt_at <= UTC_TIMESTAMP(3) AND attempt_count < max_attempts ORDER BY next_attempt_at,id LIMIT 50'
        lease = 'EXPLAIN ANALYZE SELECT id,version,lease_owner,lease_until FROM reliable_event_outbox WHERE status=1 AND lease_owner IS NOT NULL AND LENGTH(TRIM(lease_owner)) > 0 AND lease_until <= UTC_TIMESTAMP(3) ORDER BY lease_until,id LIMIT 50'
        counts = 'EXPLAIN ANALYZE SELECT status,COUNT(*) AS total FROM reliable_event_outbox WHERE status IN (0,3,4) GROUP BY status'
    }
    foreach ($entry in $explainQueries.GetEnumerator()) {
        $plan = docker compose exec -T -e MYSQL_PWD=benchmark mysql mysql -N -B -ubenchmark reliable_event_benchmark -e $entry.Value
        Assert-Exit "EXPLAIN $($entry.Key)"
        @($entry.Value, $plan) | Set-Content -LiteralPath (Join-Path $runDir "explain-$($entry.Key).txt") -Encoding UTF8
    }

    python (Join-Path $PSScriptRoot 'report.py') $runDir
    Assert-Exit 'Generating benchmark report'
    Write-Host "Benchmark run complete: $runDir"
} finally {
    if ($brokerPaused) {
        docker compose unpause broker | Out-Null
        '{0},broker_unpaused_on_cleanup' -f [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() | Add-Content -LiteralPath $faultTimeline
    }
    foreach ($process in $processes) {
        try {
            $process.Refresh()
            if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
        } catch { }
    }
    Pop-Location
}
