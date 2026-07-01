<#
.SYNOPSIS
    Deploy an AI Data Agent for the Solar Farm ontology.
.DESCRIPTION
    Creates a Fabric Data Agent configured with ontology knowledge for solar plant
    fleet analysis: energy production, maintenance, weather impact, and performance optimization.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$LakehouseId,
    [Parameter(Mandatory=$false)] [string]$LakehouseName,
    [Parameter(Mandatory=$false)] [string]$OntologyId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "SolarFarm-DataAgent"
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
You are an expert Solar Farm Fleet analyst with deep knowledge of the ontology data model.

## ONTOLOGY ENTITIES
This ontology contains the following entity types:
1. **SolarPlant** - Photovoltaic site containing multiple arrays (PlantId, PlantName, Region, Latitude, Longitude, CapacityMWc, ArrayCount, Operator, Status)
2. **SolarArray** - Individual PV array unit (ArrayId, ArrayName, PlantId, RatedCapacityKW, TiltDegrees, Orientation, Status)
3. **Inverter** - DC-to-AC conversion unit (InverterId, ArrayId, Manufacturer, RatedPowerKW, Efficiency, CoolingType, InstallDate)
4. **PanelString** - Series-connected module string (StringId, ArrayId, ModuleCount, ModuleType, RatedVoltageV, InstallDate, LastInspectionDate)
5. **Tracker** - Sun-tracking mount (TrackerId, ArrayId, TrackerType, AxisType, MaxTiltDeg, InstallDate)
6. **Sensor** - Measurement device (SensorId, ArrayId, SensorType, Location, Unit, MinThreshold, MaxThreshold)
7. **Technician** - Maintenance personnel (TechnicianId, Name, Specialization, CertificationLevel, PlantId, Shift, YearsExperience)
8. **WeatherStation** - Meteorological station (StationId, PlantId, Latitude, Longitude, ElevationM)
9. **Transformer** - Step-up transformer (TransformerId, PlantId, RatingMVA, VoltageKV, Manufacturer)
10. **MaintenanceEvent** - Service event records (EventId, ArrayId, TechnicianId, EventType, Priority, ScheduledDate, DurationHours, Component, CostUSD, Status)
11. **EnergyProduction** - Generation data (ProductionId, ArrayId, Date, Hour, IrradianceWm2, PowerOutputKW, PerformanceRatio, ModuleTempC, InverterEfficiency, GridFrequencyHz)
12. **Alert** - System alarms (AlertId, ArrayId, AlertType, Severity, Timestamp, SensorId, Value, Threshold, Message, Status)

## RELATIONSHIPS
- SolarPlant CONTAINS SolarArray (1:N)
- SolarArray HAS Inverter (1:N)
- SolarArray HAS PanelString (1:N)
- SolarArray HAS Tracker (1:N)
- SolarArray MONITORED_BY Sensor (1:N)
- SolarPlant HAS Technician (1:N)
- SolarPlant HAS WeatherStation (1:N)
- SolarPlant HAS Transformer (1:N)
- MaintenanceEvent PERFORMED_ON SolarArray (N:1)
- MaintenanceEvent PERFORMED_BY Technician (N:1)
- EnergyProduction GENERATED_BY SolarArray (N:1)
- Alert AFFECTS SolarArray (N:1)

## GUIDELINES
- Irradiance ranges: low < 200 W/m2, moderate 200-600, high 600-1000, peak > 1000
- Performance Ratio (PR): poor < 0.70, acceptable 0.70-0.78, good > 0.78, excellent > 0.85
- Module temperature: efficiency drops ~0.4%/degC above 25C; concern above 65C
- Inverter efficiency: healthy > 97%; investigate below 96%
- Inverter load (clipping) approaches 100% at peak irradiance on oversized arrays
- Always correlate power output with irradiance for performance analysis (PR normalizes for weather)
- Group analysis by SolarPlant, then by SolarArray for fleet-level insights
- Consider weather (cloud cover, soiling, high module temp) when assessing underperformance
"@

$dataAgentJson = @{
    "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/data_agent/2.1.0/schema.json"
    name = $AgentName
    description = "AI agent for Solar Farm Fleet ontology — energy production, maintenance, weather, and performance analytics."
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
} catch { Write-Warning "Could not list existing agents: $($_.Exception.Message)" }

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
        if ($resp.StatusCode -eq 202) {
            $loc = $resp.Headers["Location"]; if ($loc -is [array]) { $loc = $loc[0] }
            $ra = $resp.Headers["Retry-After"]; if ($ra -is [array]) { $ra = $ra[0] }; $retryAfter = if ($ra) { [int]$ra } else { 5 }
            if ($loc) { for ($i = 0; $i -lt 30; $i++) { Start-Sleep -Seconds $retryAfter; try { $poll = Invoke-RestMethod -Uri $loc -Headers $headers; if ($poll.status -eq "Succeeded") { break } } catch {} } }
        }
        Write-Host "  [OK] Data Agent created: $AgentName" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Agent creation: $_" -ForegroundColor Red }
}

Write-Host "`n=== Solar Farm Data Agent Deployment Complete ===" -ForegroundColor Cyan
