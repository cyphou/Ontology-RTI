<#
.SYNOPSIS
    Deploy an AI Operations Agent for the Wind Turbine ontology.
.DESCRIPTION
    Creates a Fabric Data Agent configured for operational decision support:
    turbine performance, predictive maintenance, weather response, grid compliance,
    and fleet optimization goals.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$EventhouseId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseName,
    [Parameter(Mandatory=$false)] [string]$AgentName = "WindTurbine-OperationsAgent"
)

$ErrorActionPreference = "Stop"

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Operations Agent: $AgentName ===" -ForegroundColor Cyan

if (-not $EventhouseId -or -not $KqlDatabaseId -or -not $QueryServiceUri) {
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
    if (-not $KqlDatabaseId) { $kqlDb = $allItems | Where-Object { $_.type -eq 'KQLDatabase' } | Select-Object -First 1; if ($kqlDb) { $KqlDatabaseId = $kqlDb.id } }
    if (-not $EventhouseId) { $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1; if ($eh) { $EventhouseId = $eh.id } }
    if (-not $QueryServiceUri -and $EventhouseId) { $ehD = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$EventhouseId" -Headers $headers; $QueryServiceUri = $ehD.properties.queryServiceUri }
}
if (-not $KqlDatabaseName) { $kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $headers; $KqlDatabaseName = $kqlDbDetails.displayName }

$aiInstructions = @"
You are a Wind Turbine Fleet Operations Agent with real-time KQL access.

## OPERATIONAL GOALS

### 1. Turbine Performance Monitoring
- Track power output vs rated capacity per turbine and farm
- Monitor capacity factor trends (target > 35%, excellent > 45%)
- Detect underperforming turbines relative to fleet average
- Correlate power output with wind speed to identify efficiency degradation
- KQL: PowerOutputMetric — AvgPowerKW, CapacityFactor, RotorRPM, PitchAngleDeg by TurbineId, FarmId

### 2. Predictive Maintenance
- Monitor vibration levels: Normal < 4 mm/s, Warning 4-6, High 6-8, Critical > 8
- Track temperature trends: Gearbox < 90C, Generator < 95C, Bearing < 85C
- Identify sensors with increasing anomaly rates
- Correlate maintenance history with current sensor readings
- KQL: TurbineReading — SensorType, Value, IsAnomaly trends; MaintenanceMetric — EventType, Component, CostUSD, DurationHours

### 3. Weather Response
- Monitor wind speed vs cut-in (3 m/s) and cut-out (25 m/s) thresholds
- Detect icing conditions (low temp + high humidity + low visibility)
- Track wind direction changes affecting yaw alignment
- Assess weather impact on fleet availability
- KQL: WeatherMetric — WindSpeedMs, TemperatureC, HumidityPct, IcingRisk by StationId, FarmId

### 4. Grid Compliance & Alerts
- Track grid frequency deviations (nominal 50 Hz, range 49.5-50.5)
- Monitor unacknowledged critical alerts
- Identify alert patterns by component type (MainBearing, Gearbox, Generator, Blade)
- Escalate recurring alerts on the same turbine
- KQL: TurbineAlert — AlertType, Severity, Component, IsAcknowledged; PowerOutputMetric — GridFrequencyHz

### 5. Fleet Optimization
- Compare performance across wind farms and turbines
- Identify maintenance cost hotspots by component
- Track technician workload and specialization alignment
- Recommend optimal maintenance scheduling based on weather windows
- KQL: MaintenanceMetric by FarmId, Component; PowerOutputMetric aggregate fleet metrics

## KQL TABLE SCHEMAS
- **TurbineReading**: TurbineId, FarmId, SensorId, SensorType, Timestamp, Value, Unit, Quality, IsAnomaly
- **TurbineAlert**: AlertId, TurbineId, FarmId, Timestamp, AlertType, Severity, MetricValue, ThresholdValue, Component, Message, IsAcknowledged
- **PowerOutputMetric**: TurbineId, FarmId, Timestamp, WindSpeedMs, PowerOutputKW, CapacityFactor, RotorRPM, PitchAngleDeg, YawAngleDeg, GridFrequencyHz
- **WeatherMetric**: StationId, FarmId, Timestamp, WindSpeedMs, WindDirectionDeg, TemperatureC, HumidityPct, PressureHPa, VisibilityKm, IcingRisk
- **MaintenanceMetric**: EventId, TurbineId, FarmId, Timestamp, EventType, Component, DurationHours, CostUSD, TechnicianId, Status
"@

$dataAgentJson = @{
    "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/data_agent/2.1.0/schema.json"
    name = $AgentName
    description = "Operations agent for Wind Turbine Fleet — real-time monitoring, predictive maintenance, weather response, grid compliance."
} | ConvertTo-Json -Depth 5

$stageConfig = @{
    "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfig/1.0.0/schema.json"
    dataSources = @( @{ type = "KQLDatabase"; clusterUri = $QueryServiceUri; databaseName = $KqlDatabaseName } )
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
        Write-Host "  [OK] Operations Agent updated: $AgentName" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Agent update: $_" -ForegroundColor Red }
} else {
    Write-Host "  Creating new Operations Agent..." -ForegroundColor Gray
    try {
        $resp = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body $body -UseBasicParsing
        if ($resp.StatusCode -eq 202 -and $resp.Headers["Location"]) {
            $loc = $resp.Headers["Location"]; $retryAfter = if ($resp.Headers["Retry-After"]) { [int]$resp.Headers["Retry-After"] } else { 5 }
            for ($i = 0; $i -lt 30; $i++) { Start-Sleep -Seconds $retryAfter; try { $poll = Invoke-RestMethod -Uri $loc -Headers $headers; if ($poll.status -eq "Succeeded") { break } } catch {} }
        }
        Write-Host "  [OK] Operations Agent created: $AgentName" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Agent creation: $_" -ForegroundColor Red }
}

Write-Host "`n=== Wind Turbine Operations Agent Deployment Complete ===" -ForegroundColor Cyan
