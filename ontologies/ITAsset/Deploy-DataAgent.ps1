<#
.SYNOPSIS
    Deploy a Fabric Data Agent for the IT Asset Ontology.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "ITAssetAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

Write-Host "=== Deploying Data Agent: $AgentName ===" -ForegroundColor Cyan

$createBody = @{ displayName=$AgentName; description="AI Data Agent for IT Asset Management. Answers questions about servers, applications, networks, incidents, licenses, and infrastructure health." } | ConvertTo-Json -Depth 5
$agentId = $null
try {
    $resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($resp.StatusCode -eq 201) { $agentId = ($resp.Content|ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); if ($poll.status -eq 'Succeeded') { $all=(Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId=($all|Where-Object{$_.displayName -eq $AgentName -and $_.type -eq 'DataAgent'}).id } }
} catch { Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red; exit 1 }
if (-not $agentId) { exit 1 }

$aiInstructions = @"
You are an expert AI assistant for IT Asset Management. Your data source is the ITAssetOntology.

== ONTOLOGY ENTITY TYPES (11 nodes) ==

1. DataCenter (Key: DataCenterId) - Name, Location, Country, TierLevel, TotalRacks, PowerCapacityKW, CoolingType, Status
2. Rack (Key: RackId) - DataCenterId, RackName, RackUnits, PowerBudgetKW, Location, Status
3. Server (Key: ServerId) - RackId, DataCenterId, ServerName, ServerType, OS, CPUCores, MemoryGB, StorageTB, Status
   Timeseries: Timestamp, CPUPercent, MemoryPercent, DiskIOPS, NetworkMbps
4. NetworkDevice (Key: DeviceId) - DataCenterId, DeviceName, DeviceType, Manufacturer, Ports, BandwidthGbps, Status
5. Application (Key: AppId) - ServerId, AppName, AppType, Version, Owner, SLA, Status
6. Database (Key: DatabaseId) - ServerId, DbName, DbEngine, SizeGB, Status
7. VirtualMachine (Key: VMId) - ServerId, VMName, OS, CPUCores, MemoryGB, Status
8. User (Key: UserId) - UserName, Department, Role, AccessLevel, Status
9. Incident (Key: IncidentId) - ServerId, IncidentType, Severity, CreatedDate, ResolvedDate, DurationHours, RootCause, Status
10. License (Key: LicenseId) - AppId, LicenseType, Vendor, Seats, ExpirationDate, CostPerYear, Status
11. Alert (Key: AlertId) - ServerId, AlertType, Severity, Timestamp, MetricValue, ThresholdValue, IsAcknowledged

== RELATIONSHIPS (10 edges) ==
DataCenterHasRack, RackHasServer, ServerHostsApp, ServerHostsDB, ServerHostsVM, AppHasLicense, UserOwnsApp, IncidentOnServer, NetworkInDataCenter, AlertFromServer

== GUIDELINES ==
1. Navigate DataCenter -> Rack -> Server for infrastructure hierarchy.
2. CPU >90% = Critical, >80% = Warning. Memory >85% = Warning.
3. Disk IOPS >15000 indicates storage bottleneck.
4. Track incidents by MTTR (Mean Time To Resolve) per severity.
5. License compliance: compare active seats vs total seats.
6. Network: bandwidth >80% = congestion risk, packet loss >0.1% = degraded, latency >30ms = high.
7. Application SLA: track response time vs target. ErrorRate >1% = degraded.
8. Include units: %, ms, IOPS, Mbps, GB, TB, hours, USD.
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'))
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $aiInstructions + '"}'))

$updateBody = @{definition=@{parts=@(@{path="Files/Config/data_agent.json";payload=$dataAgentB64;payloadType="InlineBase64"},@{path="Files/Config/draft/stage_config.json";payload=$stageB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
try { $r = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] AI instructions configured." -ForegroundColor Green } } catch { Write-Host "[WARN] $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host "=== Data Agent Deployment Complete ===" -ForegroundColor Cyan
