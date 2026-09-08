param(
    [Parameter(Mandatory=$true)][string]$WorkspaceId,
    [string]$LakehouseId,
    [string]$LakehouseName = 'EnterpriseFinanceHRLH',
    [string]$AgentName = 'EnterpriseFinanceHR-DataAgent'
)
$ErrorActionPreference = 'Stop'
$token = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }
$baseUri = 'https://api.fabric.microsoft.com/v1'
$lakehouses = (Invoke-RestMethod -Uri "$baseUri/workspaces/$WorkspaceId/items?type=Lakehouse" -Headers $headers).value
$lakehouse = $lakehouses | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
if (-not $lakehouse) { throw "Required Lakehouse '$LakehouseName' was not found; refusing to bind the data agent to a different source." }
if ($LakehouseId -and $LakehouseId -ne $lakehouse.id) { throw "LakehouseId does not match '$LakehouseName'." }
$instructions = 'You answer only from EnterpriseFinanceHRLH synthetic data. Default every workforce response to aggregate values; never reveal worker-level records without an authorized HR workflow. Do not infer protected characteristics, make employment recommendations, or automate HR decisions. State that data is synthetic and contains no actual PII.'
$config = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json","dataSources":[{"name":"EnterpriseFinanceHRLH","type":"Lakehouse","itemId":"' + $lakehouse.id + '"}]}'
$stage = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $instructions.Replace('"', '\"') + '"}'
$parts = @(@{ path = 'Files/Config/data_agent.json'; payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($config)); payloadType = 'InlineBase64' }, @{ path = 'Files/Config/draft/stage_config.json'; payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stage)); payloadType = 'InlineBase64' })
$existing = ((Invoke-RestMethod -Uri "$baseUri/workspaces/$WorkspaceId/items?type=DataAgent" -Headers $headers).value | Where-Object { $_.displayName -eq $AgentName } | Select-Object -First 1)
if ($existing) { $uri = "$baseUri/workspaces/$WorkspaceId/dataAgents/$($existing.id)/updateDefinition"; $body = @{ definition = @{ parts = $parts } } }
else { $uri = "$baseUri/workspaces/$WorkspaceId/items"; $body = @{ displayName = $AgentName; type = 'DataAgent'; description = 'Synthetic enterprise finance and aggregate HR planning data agent.'; definition = @{ parts = $parts } } }
Invoke-RestMethod -Method Post -Uri $uri -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) | Out-Null
Write-Host "Data Agent '$AgentName' bound explicitly to $LakehouseName." -ForegroundColor Green