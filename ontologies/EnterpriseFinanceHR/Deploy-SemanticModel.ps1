<#
.SYNOPSIS
    Creates the Enterprise Finance + HR Direct Lake semantic model.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$WorkspaceId,
    [Parameter(Mandatory = $true)][string]$LakehouseId,
    [string]$ModelName = 'EnterpriseFinanceHRModel',
    [switch]$UpdateExisting
)

$ErrorActionPreference = 'Stop'
$apiBase = 'https://api.fabric.microsoft.com/v1'
$token = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }
$modelRoot = Join-Path $PSScriptRoot 'SemanticModel'

function ToBase64([string]$Value) {
    [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value))
}

function WaitOperation($Response) {
    if ($Response.StatusCode -ne 202) { return }
    $location = $Response.Headers['Location']
    if ($location -is [array]) { $location = $location[0] }
    do {
        Start-Sleep -Seconds 5
        $operation = Invoke-RestMethod -Method Get -Uri $location -Headers $headers
    } while ($operation.status -notin @('Succeeded', 'Failed', 'Cancelled'))
    if ($operation.status -ne 'Succeeded') {
        $message = if ($operation.error -and $operation.error.message) { $operation.error.message } else { $operation | ConvertTo-Json -Depth 10 -Compress }
        throw "Semantic model operation $($operation.status): $message"
    }
}

$existing = ((Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Headers $headers).value |
    Where-Object { $_.displayName -eq $ModelName } | Select-Object -First 1)
if ($existing) {
    Write-Host "Using existing semantic model '$ModelName': $($existing.id)" -ForegroundColor Yellow
    if (-not $UpdateExisting) { return }
}

$lakehouse = Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/lakehouses/$LakehouseId" -Headers $headers
$sqlEndpoint = $lakehouse.properties.sqlEndpointProperties.connectionString
if (-not $sqlEndpoint) { throw "Lakehouse '$LakehouseId' does not have a ready SQL endpoint." }

$parts = @()
$pbism = Get-Content (Join-Path $modelRoot 'definition.pbism') -Raw -Encoding UTF8
$parts += @{ path = 'definition.pbism'; payload = ToBase64 $pbism; payloadType = 'InlineBase64' }

foreach ($name in @('database.tmdl', 'model.tmdl', 'expressions.tmdl', 'relationships.tmdl', 'roles.tmdl')) {
    $path = Join-Path $modelRoot "definition\$name"
    if (-not (Test-Path $path)) { continue }
    $content = Get-Content $path -Raw -Encoding UTF8
    if ($name -eq 'expressions.tmdl') {
        $content = $content.Replace('{{SQL_ENDPOINT}}', $sqlEndpoint).Replace('{{LAKEHOUSE_NAME}}', 'EnterpriseFinanceHRLH')
    }
    $parts += @{ path = "definition/$name"; payload = ToBase64 $content; payloadType = 'InlineBase64' }
}

Get-ChildItem (Join-Path $modelRoot 'definition\tables') -Filter '*.tmdl' -File | Sort-Object Name | ForEach-Object {
    $parts += @{ path = "definition/tables/$($_.Name)"; payload = ToBase64 (Get-Content $_.FullName -Raw -Encoding UTF8); payloadType = 'InlineBase64' }
}

if ($existing) {
    $body = @{ definition = @{ parts = $parts } } | ConvertTo-Json -Depth 16
    $response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/semanticModels/$($existing.id)/updateDefinition" -Headers $headers -Body $body -UseBasicParsing
    WaitOperation $response
    Write-Host "Semantic model '$ModelName' updated: $($existing.id)" -ForegroundColor Green
    return
}

$body = @{ displayName = $ModelName; type = 'SemanticModel'; description = 'Direct Lake model for synthetic enterprise Finance and HR planning'; definition = @{ parts = $parts } } | ConvertTo-Json -Depth 16
$response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body $body -UseBasicParsing
WaitOperation $response

$created = ((Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Headers $headers).value |
    Where-Object { $_.displayName -eq $ModelName } | Select-Object -First 1)
if (-not $created) { throw "Semantic model '$ModelName' was not found after successful creation." }
Write-Host "Semantic model '$ModelName' created: $($created.id)" -ForegroundColor Green