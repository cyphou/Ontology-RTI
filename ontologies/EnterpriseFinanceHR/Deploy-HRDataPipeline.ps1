<# Creates or updates the Enterprise Finance + HR aggregate refresh pipeline. #>
[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$WorkspaceId, [Parameter(Mandatory = $true)][string]$LakehouseId, [Parameter(Mandatory = $true)][string]$NotebookId, [Parameter(Mandatory = $true)][string]$QualityGateNotebookId, [Parameter(Mandatory = $true)][string[]]$DataflowIds, [Parameter(Mandatory = $true)][string]$SemanticModelId, [string]$PipelineName = 'Enterprise Finance HR Aggregate Refresh')
$ErrorActionPreference = 'Stop'; $apiBase = 'https://api.fabric.microsoft.com/v1'; $headers = @{ Authorization = "Bearer $((Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token)"; 'Content-Type' = 'application/json' }
function ToBase64([string]$Value) { [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Value)) }
function WaitOperation($Response) { if ($Response.StatusCode -ne 202) { return }; $uri = [string]$Response.Headers['Location']; do { Start-Sleep -Seconds 5; $operation = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers } while ($operation.status -notin @('Succeeded','Failed','Cancelled')); if ($operation.status -ne 'Succeeded') { throw "Fabric operation $($operation.status)" } }
$folder = Join-Path $PSScriptRoot 'DataPipeline'
# The definition ships with {{PLACEHOLDERS}} so no real workspace or item id is committed;
# they are substituted here from the parameters, which were previously accepted and ignored.
if ($DataflowIds.Count -lt 5) { throw "DataflowIds needs 5 ids (bronze, silver x3, gold); got $($DataflowIds.Count)." }
$tokens = [ordered]@{
    '{{WORKSPACE_ID}}'               = $WorkspaceId
    '{{LAKEHOUSE_ID}}'               = $LakehouseId
    '{{NOTEBOOK_ID}}'                = $NotebookId
    '{{QUALITY_GATE_NOTEBOOK_ID}}'   = $QualityGateNotebookId
    '{{SEMANTIC_MODEL_ID}}'          = $SemanticModelId
}
for ($i = 0; $i -lt 5; $i++) { $tokens["{{DATAFLOW_$($i + 1)_ID}}"] = $DataflowIds[$i] }
$parts = @('.platform', 'definition/pipeline-content.json') | ForEach-Object {
    $raw = Get-Content (Join-Path $folder $_) -Raw
    foreach ($k in $tokens.Keys) { $raw = $raw.Replace($k, $tokens[$k]) }
    if ($raw -match '\{\{[A-Z0-9_]+\}\}') { throw "Unsubstituted placeholder in ${_}: $($Matches[0])" }
    @{ path = $_; payload = ToBase64 $raw; payloadType = 'InlineBase64' }
}
$existing = (Invoke-RestMethod -Method Get -Uri "$apiBase/workspaces/$WorkspaceId/items?type=DataPipeline" -Headers $headers).value | Where-Object { $_.displayName -eq $PipelineName } | Select-Object -First 1
$definition = @{ definition = @{ parts = $parts } }; if ($existing) { $response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items/$($existing.id)/updateDefinition?updateMetadata=true" -Headers $headers -Body ($definition | ConvertTo-Json -Depth 10) -UseBasicParsing } else { $response = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body ((@{ displayName = $PipelineName; type = 'DataPipeline'; definition = $definition.definition } | ConvertTo-Json -Depth 10)) -UseBasicParsing }; WaitOperation $response
Write-Host 'Pipeline definition saved. Run it in Fabric with the supplied Lakehouse, notebook, dataflow, quality-gate, and semantic-model identifiers.' -ForegroundColor Green