param(
    [Parameter(Mandatory = $true)]
    [long]$OrderId,
    [int]$TimeoutSeconds = 60,
    [string]$BaseUrl = 'http://localhost:8090'
)

$ErrorActionPreference = 'Stop'
if ($OrderId -le 0 -or $TimeoutSeconds -le 0) {
    throw 'OrderId and TimeoutSeconds must be positive'
}

$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
while ([DateTime]::UtcNow -lt $deadline) {
    $order = Invoke-RestMethod -Uri "$BaseUrl/orders/$OrderId" -Method Get
    if ($order.handledCount -eq 1) {
        $order | ConvertTo-Json
        exit 0
    }
    Start-Sleep -Milliseconds 500
}
throw "Order $OrderId was not handled within $TimeoutSeconds seconds"
