param([Parameter(Mandatory = $true)][string]$AllowedSubnet)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$evidence = Join-Path $root 'target/evidence'
$receiverStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$recordsPath = Join-Path $evidence "m8.5-alert-receiver-$receiverStamp.jsonl"
New-Item -ItemType Directory -Force -Path $evidence | Out-Null
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Any, 19080)
$listener.Start()
$utf8 = [System.Text.UTF8Encoding]::new($false)
$networkParts = $AllowedSubnet -split '/', 2
$script:networkAddressBytes = [System.Net.IPAddress]::Parse($networkParts[0]).GetAddressBytes()
$script:networkPrefixLength = [int]$networkParts[1]
if ($script:networkAddressBytes.Length -ne 4 -or $script:networkPrefixLength -lt 0 -or $script:networkPrefixLength -gt 32) {
    throw "Invalid IPv4 Docker subnet: $AllowedSubnet"
}

function Test-AllowedRemote([System.Net.IPAddress]$address) {
    if ([System.Net.IPAddress]::IsLoopback($address)) { return $true }
    if ($address.IsIPv4MappedToIPv6) { $address = $address.MapToIPv4() }
    $addressBytes = $address.GetAddressBytes()
    if ($addressBytes.Length -ne 4) { return $false }
    $wholeBytes = [Math]::Floor($script:networkPrefixLength / 8)
    $remainingBits = $script:networkPrefixLength % 8
    for ($index = 0; $index -lt $wholeBytes; $index++) {
        if ($addressBytes[$index] -ne $script:networkAddressBytes[$index]) { return $false }
    }
    if ($remainingBits -gt 0) {
        $mask = (0xFF -shl (8 - $remainingBits)) -band 0xFF
        if (($addressBytes[$wholeBytes] -band $mask) -ne ($script:networkAddressBytes[$wholeBytes] -band $mask)) { return $false }
    }
    return $true
}

function Write-Response($stream, [string]$status, [string]$contentType, [string]$response) {
    $responseBytes = $script:utf8.GetBytes($response)
    $responseHead = "HTTP/1.1 $status`r`nContent-Type: $contentType; charset=utf-8`r`nContent-Length: $($responseBytes.Length)`r`nConnection: close`r`n`r`n"
    $headBytes = [System.Text.Encoding]::ASCII.GetBytes($responseHead)
    $stream.Write($headBytes, 0, $headBytes.Length)
    if ($responseBytes.Length -gt 0) { $stream.Write($responseBytes, 0, $responseBytes.Length) }
    $stream.Flush()
}

while ($true) {
    $client = $listener.AcceptTcpClient()
    try {
        $stream = $client.GetStream()
        $stream.ReadTimeout = 5000
        $stream.WriteTimeout = 5000
        if (-not (Test-AllowedRemote $client.Client.RemoteEndPoint.Address)) {
            Write-Response $stream '403 Forbidden' 'application/json' '{"error":"source outside local receiver scope"}'
            continue
        }
        $headerBytes = [System.Collections.Generic.List[byte]]::new()
        while ($headerBytes.Count -lt 65536) {
            $current = $stream.ReadByte()
            if ($current -lt 0) { break }
            $headerBytes.Add([byte]$current)
            $n = $headerBytes.Count
            if ($n -ge 4 -and $headerBytes[$n-4] -eq 13 -and $headerBytes[$n-3] -eq 10 -and $headerBytes[$n-2] -eq 13 -and $headerBytes[$n-1] -eq 10) { break }
        }
        $headerText = [System.Text.Encoding]::ASCII.GetString($headerBytes.ToArray())
        $firstLine = ($headerText -split "`r`n", 2)[0]
        $method, $path = $firstLine -split ' ', 3
        $bodyLength = 0
        if ($headerText -match '(?im)^Content-Length:\s*(\d+)') { $bodyLength = [int]$Matches[1] }
        $bodyBytes = [byte[]]::new($bodyLength)
        $offset = 0
        while ($offset -lt $bodyLength) {
            $read = $stream.Read($bodyBytes, $offset, $bodyLength - $offset)
            if ($read -eq 0) { break }
            $offset += $read
        }

        if ($path -eq '/alerts' -and $method -eq 'POST') {
            $payloadJson = [System.Text.Encoding]::UTF8.GetString($bodyBytes, 0, $offset)
            $payload = ConvertFrom-Json -InputObject $payloadJson -AsHashtable -ErrorAction Stop
            $recordJson = ConvertTo-Json -InputObject @{ received_at = [DateTime]::UtcNow.ToString('o'); payload = $payload } -Depth 40 -Compress
            [System.IO.File]::AppendAllText($recordsPath, ($recordJson + "`n"), $utf8)
            $response = '{"accepted":true}'
            $status = '200 OK'
        } elseif ($path -eq '/alerts' -and $method -eq 'GET') {
            $response = if (Test-Path -LiteralPath $recordsPath) { [System.IO.File]::ReadAllText($recordsPath, $utf8) } else { '' }
            $status = '200 OK'
        } else {
            $response = '{"error":"not found"}'
            $status = '404 Not Found'
        }
        Write-Response $stream $status 'application/json' $response
    } catch {
        # A rejected local webhook remains visible in the receiver process stderr log.
        [Console]::Error.WriteLine($_.ToString())
    } finally {
        if ($null -ne $client) { $client.Dispose() }
    }
}
