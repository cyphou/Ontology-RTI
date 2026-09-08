param([Parameter(Mandatory=$true)][string]$WorkspaceId, [string]$EventhouseId, [string]$AgentName = 'EnterpriseFinanceHR-OperationsAgent')
$ErrorActionPreference = 'Stop'
$token = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token
$headers = @{ Authorization = "Bearer $token"; 'Content-Type' = 'application/json' }; $baseUri = 'https://api.fabric.microsoft.com/v1'
$eventhouse = if ($EventhouseId) { (Invoke-RestMethod -Uri "$baseUri/workspaces/$WorkspaceId/eventhouses/$EventhouseId" -Headers $headers) } else { ((Invoke-RestMethod -Uri "$baseUri/workspaces/$WorkspaceId/items?type=Eventhouse" -Headers $headers).value | Where-Object { $_.displayName -eq 'EnterpriseFinanceHREH' } | Select-Object -First 1) }
if (-not $eventhouse) { throw "Required Eventhouse 'EnterpriseFinanceHREH' was not found." }
$description = 'Monitors synthetic budget variance, payroll-cost anomalies, aggregate workforce movement, and forecast freshness. It never makes automated HR decisions.'
$body = @{ displayName = $AgentName; type = 'DataAgent'; description = $description } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post -Uri "$baseUri/workspaces/$WorkspaceId/items" -Headers $headers -Body $body | Out-Null
Write-Host "Operations Agent '$AgentName' created for aggregate planning exceptions." -ForegroundColor Green