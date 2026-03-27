<#
.SYNOPSIS
    Deploy a Fabric Operations Agent for Smart Building real-time monitoring.
.DESCRIPTION
    Creates an Operations Agent with goals for building management:
    equipment health, energy optimization, occupant comfort, safety, and maintenance.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$EventhouseId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "SmartBuildingOpsAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Operations Agent: $AgentName ===" -ForegroundColor Cyan

# ── Auto-detect Eventhouse/KQL Database if not provided ─────────────────────
if (-not $EventhouseId -or -not $KqlDatabaseId) {
    Write-Host "[Auto-detect] Looking up Eventhouse and KQL Database in workspace..." -ForegroundColor Yellow
    try {
        $kqlDbs = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases" -Headers $headers).value
        if ($kqlDbs -and $kqlDbs.Count -gt 0) {
            $kqlDb = $kqlDbs | Select-Object -First 1
            if (-not $KqlDatabaseId) { $KqlDatabaseId = $kqlDb.id; Write-Host "  Found KQL Database: $($kqlDb.displayName) ($KqlDatabaseId)" -ForegroundColor Green }
            if (-not $EventhouseId -and $kqlDb.properties -and $kqlDb.properties.parentEventhouseItemId) {
                $EventhouseId = $kqlDb.properties.parentEventhouseItemId
                Write-Host "  Found Eventhouse: $EventhouseId" -ForegroundColor Green
            }
        }
        if (-not $EventhouseId) {
            $ehs = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses" -Headers $headers).value
            if ($ehs -and $ehs.Count -gt 0) { $EventhouseId = $ehs[0].id; Write-Host "  Found Eventhouse: $($ehs[0].displayName) ($EventhouseId)" -ForegroundColor Green }
        }
    } catch {
        Write-Host "  [WARN] Auto-detect failed: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# Check for existing
$allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
$existing = $allItems | Where-Object { $_.displayName -eq $AgentName }
if ($existing) { Write-Host "[INFO] Agent '$AgentName' already exists ($($existing.id)). Updating..." -ForegroundColor Yellow; $agentId = $existing.id } else {
    $createBody = @{ displayName = $AgentName; type = "DataAgent"; description = "Operations monitoring agent for Smart Building - tracks HVAC, energy, occupancy, and safety in real-time." } | ConvertTo-Json -Depth 5
    try {
        $response = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
        if ($response.StatusCode -eq 201) { $agentId = ($response.Content | ConvertFrom-Json).id; Write-Host "[OK] Created: $agentId" -ForegroundColor Green }
        elseif ($response.StatusCode -eq 202) {
            $opUrl = $response.Headers['Location']; do { Start-Sleep -Seconds 3; $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))
            if ($poll.status -eq 'Succeeded') { $allI = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId = ($allI | Where-Object { $_.displayName -eq $AgentName }).id }
        }
    } catch { Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red; exit 1 }
}

if (-not $agentId) { Write-Host "[ERROR] Agent not created." -ForegroundColor Red; exit 1 }

$goals = @"
== SMART BUILDING OPERATIONS AGENT GOALS ==

Goal 1: HVAC Performance & Comfort
- Monitor supply/return temperature delta across all HVAC systems
- Alert when efficiency drops below 85% or zone temperature deviates >3F from setpoint
- Track energy consumption per HVAC unit; flag units consuming >20% above baseline
- Ensure humidity stays within 30-65% in all occupied zones

Goal 2: Energy Optimization
- Track total kWh consumption by building, floor, and time of day
- Identify peak demand periods and recommend load shedding
- Compare solar vs grid energy source mix
- Alert when power factor drops below 0.90
- Weekly energy cost analysis by building

Goal 3: Occupant Safety & Comfort
- Monitor CO2 levels: Warning >1000 PPM, Critical >1500 PPM
- Track temperature comfort band: 68-76F for occupied zones
- Monitor occupancy vs max capacity; alert at >90% utilization
- Ensure emergency access points are operational
- Track elevator inspection schedules and fault alerts

Goal 4: Alert Management
- Prioritize unacknowledged Critical and High severity alerts
- Track mean time to acknowledge and resolve by alert type
- Identify recurring alert patterns (same sensor/zone >3 alerts in 24h)
- Escalate unresolved Critical alerts after 30 minutes

Goal 5: Maintenance Optimization
- Track open maintenance tickets by priority and age
- Calculate average resolution time by ticket type
- Identify equipment with >3 maintenance events in 90 days
- Forecast maintenance needs based on sensor trend analysis
- Track maintenance cost per building monthly
"@

$instructions = @"
You are a Smart Building Operations monitoring agent. You observe real-time telemetry from the KQL database and ontology to detect anomalies and ensure optimal building performance.

== KQL TABLES ==

SensorReading (SensorId, ZoneId, BuildingId, FloorId, Timestamp, ReadingValue, MeasurementUnit, SensorType, QualityFlag, IsAnomaly)
BuildingAlert (AlertId, SensorId, ZoneId, BuildingId, Timestamp, AlertType, Severity, ReadingValue, ThresholdValue, Message, IsAcknowledged)
HVACMetric (HVACSystemId, BuildingId, FloorId, Timestamp, SupplyTempF, ReturnTempF, AirFlowCFM, HumidityPct, PowerKW, Mode, EfficiencyPct)
EnergyConsumption (MeterId, BuildingId, FloorId, Timestamp, PowerKWh, PeakDemandKW, PowerFactor, CostUSD, Source)
OccupancyMetric (ZoneId, BuildingId, FloorId, Timestamp, OccupantCount, MaxCapacity, UtilizationPct)

== RESPONSE FORMAT ==
Always include: timestamp, affected building/zone, severity, recommended action.
Use tables for multi-row data. Include units (F, %, PPM, kWh, CFM, USD).
"@

$fullInstructions = "$goals`n`n$instructions" -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'
$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dataAgentJson))
$stageConfigJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $fullInstructions + '"}'
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stageConfigJson))

$updateBody = @{ definition = @{ parts = @(
    @{ path = "Files/Config/data_agent.json"; payload = $dataAgentB64; payloadType = "InlineBase64" },
    @{ path = "Files/Config/draft/stage_config.json"; payload = $stageB64; payloadType = "InlineBase64" }
) } } | ConvertTo-Json -Depth 10

try {
    $updResp = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing
    if ($updResp.StatusCode -in @(200,202)) { Write-Host "[OK] Operations Agent configured." -ForegroundColor Green }
} catch { Write-Host "[WARN] Definition update: $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host ""
Write-Host "=== Operations Agent Deployment Complete ===" -ForegroundColor Cyan
Write-Host "  To connect to Teams: Open -> Settings -> Connections -> Add Microsoft Teams" -ForegroundColor Gray
