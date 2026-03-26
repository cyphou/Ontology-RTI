<#
.SYNOPSIS
    Deploy an AI Data Agent for the Wind Turbine ontology.
.DESCRIPTION
    Creates a Fabric Data Agent configured with ontology knowledge for wind turbine
    fleet analysis: power generation, maintenance, weather impact, and fleet optimization.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$LakehouseId,
    [Parameter(Mandatory=$false)] [string]$LakehouseName,
    [Parameter(Mandatory=$false)] [string]$AgentName = "WindTurbine-DataAgent"
)

$ErrorActionPreference = "Stop"

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Data Agent: $AgentName ===" -ForegroundColor Cyan

if (-not $LakehouseId) {
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
    $lh = $allItems | Where-Object { $_.type -eq 'Lakehouse' } | Select-Object -First 1
    if ($lh) { $LakehouseId = $lh.id; $LakehouseName = $lh.displayName } else { Write-Host "[ERROR] No Lakehouse found." -ForegroundColor Red; exit 1 }
}

$aiInstructions = @"
You are an expert Wind Turbine Fleet analyst with deep knowledge of the ontology data model.

## ONTOLOGY ENTITIES
This ontology contains the following entity types:
1. **WindFarm** - Geographic site containing multiple turbines (FarmId, FarmName, Location, Latitude, Longitude, Capacity)
2. **Turbine** - Individual wind turbine unit (TurbineId, TurbineName, FarmId, Model, RatedCapacityKW, HubHeightM, RotorDiameterM, CommissionDate)
3. **Nacelle** - Housing at tower top containing drivetrain (NacelleId, TurbineId, GearboxType, GeneratorType, CoolingSystem)
4. **Blade** - Rotor blade component (BladeId, TurbineId, BladePosition, LengthM, Material, InstallDate)
5. **Tower** - Supporting structure (TowerId, TurbineId, HeightM, Material, FoundationType)
6. **Sensor** - Measurement device (SensorId, TurbineId, SensorType, Unit, MinThreshold, MaxThreshold, Location)
7. **Technician** - Maintenance personnel (TechnicianId, Name, Specialization, CertificationLevel)
8. **WeatherStation** - Meteorological station (StationId, FarmId, StationType, Latitude, Longitude)
9. **Transformer** - Electrical transformer (TransformerId, FarmId, RatingMVA, VoltageKV, CoolingType)
10. **MaintenanceEvent** - Service event records (EventId, TurbineId, TechnicianId, EventType, Component, StartDate, DurationHours, CostUSD, Status)
11. **PowerOutput** - Generation data (OutputId, TurbineId, Date, Hour, WindSpeedMs, PowerOutputKW, CapacityFactor, RotorRPM, PitchAngleDeg, YawAngleDeg, GridFrequencyHz)
12. **Alert** - System alarms (AlertId, TurbineId, SensorId, AlertType, Severity, Timestamp, Message, IsAcknowledged)

## RELATIONSHIPS
- WindFarm CONTAINS Turbine (1:N)
- Turbine HAS Nacelle (1:1)
- Turbine HAS Blade (1:N, typically 3)
- Turbine HAS Tower (1:1)
- Turbine MONITORED_BY Sensor (1:N)
- WindFarm HAS WeatherStation (1:N)
- WindFarm HAS Transformer (1:N)
- MaintenanceEvent PERFORMED_ON Turbine (N:1)
- MaintenanceEvent PERFORMED_BY Technician (N:1)
- PowerOutput GENERATED_BY Turbine (N:1)
- Alert TRIGGERED_BY Sensor (N:1)
- Alert AFFECTS Turbine (N:1)

## GUIDELINES
- Wind speed ranges: cut-in ~3 m/s, rated ~12 m/s, cut-out ~25 m/s
- Capacity factor: good > 35%, excellent > 45%
- Vibration thresholds: Normal < 4 mm/s, Warning 4-6, High 6-8, Critical > 8
- Temperature limits: Gearbox < 90C, Generator < 95C, Bearing < 85C
- Always correlate power output with wind speed for performance analysis
- Group analysis by WindFarm, then by Turbine for fleet-level insights
- Consider weather conditions (icing, extreme wind) when assessing anomalies
"@

$dataAgentJson = @{
    "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/data_agent/2.1.0/schema.json"
    name = $AgentName
    description = "AI agent for Wind Turbine Fleet ontology — power generation, maintenance, weather, and fleet health analytics."
} | ConvertTo-Json -Depth 5

$stageConfig = @{
    "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfig/1.0.0/schema.json"
    dataSources = @( @{ type = "Lakehouse"; workspaceId = $WorkspaceId; artifactId = $LakehouseId } )
    aiInstructions = $aiInstructions
} | ConvertTo-Json -Depth 5

$daBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($dataAgentJson))
$scBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($stageConfig))

$body = @{
    displayName = $AgentName
    type = "DataAgent"
    definition = @{
        parts = @(
            @{ path = "Files/Config/data_agent.json"; payload = $daBase64; payloadType = "InlineBase64" }
            @{ path = "Files/Config/draft/stage_config.json"; payload = $scBase64; payloadType = "InlineBase64" }
        )
    }
} | ConvertTo-Json -Depth 10

$existing = $null
try {
    $items = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items?type=DataAgent" -Headers $headers).value
    $existing = $items | Where-Object { $_.displayName -eq $AgentName } | Select-Object -First 1
} catch {}

if ($existing) {
    Write-Host "  Updating existing agent $($existing.id)..." -ForegroundColor Yellow
    $updBody = @{ definition = @{ parts = @(
        @{ path = "Files/Config/data_agent.json"; payload = $daBase64; payloadType = "InlineBase64" }
        @{ path = "Files/Config/draft/stage_config.json"; payload = $scBase64; payloadType = "InlineBase64" }
    ) } } | ConvertTo-Json -Depth 10
    try {
        Invoke-RestMethod -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/dataAgents/$($existing.id)/updateDefinition" -Headers $headers -Body $updBody | Out-Null
        Write-Host "  [OK] Data Agent updated: $AgentName" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Agent update: $_" -ForegroundColor Red }
} else {
    Write-Host "  Creating new Data Agent..." -ForegroundColor Gray
    try {
        $resp = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body $body -UseBasicParsing
        if ($resp.StatusCode -eq 202 -and $resp.Headers["Location"]) {
            $loc = $resp.Headers["Location"]; $retryAfter = if ($resp.Headers["Retry-After"]) { [int]$resp.Headers["Retry-After"] } else { 5 }
            for ($i = 0; $i -lt 30; $i++) { Start-Sleep -Seconds $retryAfter; try { $poll = Invoke-RestMethod -Uri $loc -Headers $headers; if ($poll.status -eq "Succeeded") { break } } catch {} }
        }
        Write-Host "  [OK] Data Agent created: $AgentName" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Agent creation: $_" -ForegroundColor Red }
}

Write-Host "`n=== Wind Turbine Data Agent Deployment Complete ===" -ForegroundColor Cyan
