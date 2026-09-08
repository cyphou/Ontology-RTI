<# Creates or updates aggregate-only Enterprise Finance + HR Dataflow Gen2 definitions. #>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$WorkspaceId,
    [Parameter(Mandatory = $true)][string]$LakehouseId,
    [Parameter(Mandatory = $true)][string]$LakehouseConnectionId,
    [Parameter(Mandatory = $true)][string]$GatewayClusterId,
    [switch]$SkipRefresh
)
$ErrorActionPreference = 'Stop'
$apiBase = 'https://api.fabric.microsoft.com/v1'
$headers = @{ Authorization = "Bearer $((Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token)"; 'Content-Type' = 'application/json' }
function ToBase64([string]$Value) { [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value)) }
function WaitOperation($Response) { if ($Response.StatusCode -ne 202) { return }; $uri = [string]$Response.Headers['Location']; do { Start-Sleep -Seconds 5; $operation = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers } while ($operation.status -notin @('Succeeded', 'Failed', 'Cancelled')); if ($operation.status -ne 'Succeeded') { $message = if ($operation.error -and $operation.error.message) { $operation.error.message } else { $operation | ConvertTo-Json -Depth 10 -Compress }; throw "Fabric operation $($operation.status): $message" } }
$connection = Invoke-RestMethod -Method Get -Uri "$apiBase/connections/$LakehouseConnectionId" -Headers $headers
if ($connection.connectionDetails.type -ne 'Lakehouse') { throw 'LakehouseConnectionId must reference an existing Lakehouse connection.' }
$root = Join-Path $PSScriptRoot 'datafactory'
foreach ($name in @('compensation', 'attendance', 'absence', 'recruitment')) {
    $folder = Join-Path $root $name; $displayName = "Enterprise HR $($name.Substring(0,1).ToUpper() + $name.Substring(1))"
    $replace = @{ '{{WORKSPACE_ID}}' = $WorkspaceId; '{{LAKEHOUSE_ID}}' = $LakehouseId; '{{CONNECTION_ID}}' = $LakehouseConnectionId; '{{GATEWAY_CLUSTER_ID}}' = $GatewayClusterId }
    $parts = @('mashup.pq', 'queryMetadata.json', '.platform') | ForEach-Object { $content = Get-Content (Join-Path $folder $_) -Raw; foreach ($key in $replace.Keys) { $content = $content.Replace($key, $replace[$key]) }; @{ path = $_; payload = ToBase64 $content; payloadType = 'InlineBase64' } }
    $existing = (Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=Dataflow" -Headers $headers).value | Where-Object { $_.displayName -eq $displayName } | Select-Object -First 1
    $body = @{ definition = @{ parts = $parts } } | ConvertTo-Json -Depth 12
    if ($existing) { $response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items/$($existing.id)/updateDefinition" -Headers $headers -Body $body -UseBasicParsing; $id = $existing.id } else { $response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body ((@{ displayName = $displayName; type = 'Dataflow'; definition = @{ parts = $parts } }) | ConvertTo-Json -Depth 12) -UseBasicParsing; $id = ($response.Content | ConvertFrom-Json).id }
    WaitOperation $response
    if (-not $SkipRefresh) {
        $refresh = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items/$id/jobs/instances?jobType=Refresh" -Headers $headers -Body '{"executionData":{"executeOption":"ApplyChangesIfNeeded"}}' -UseBasicParsing
        WaitOperation $refresh
    }
}