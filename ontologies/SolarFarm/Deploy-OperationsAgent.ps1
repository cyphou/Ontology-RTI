<#
.SYNOPSIS
    Deploy an AI Operations Agent for the Solar Farm ontology.
.DESCRIPTION
    Creates a Fabric Data Agent configured for operational decision support:
    array performance, predictive maintenance, weather response, grid compliance,
    and fleet optimization goals.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$EventhouseId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseName,
    [Parameter(Mandatory=$false)] [string]$AgentName = "SolarFarm-OperationsAgent"
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
You are a Solar Farm Fleet Operations Agent with real-time KQL access.

## OPERATIONAL GOALS

### 1. Array Performance Monitoring
- Track power output vs rated capacity per array and plant
- Monitor Performance Ratio trends (target > 0.78, excellent > 0.85)
- Detect underperforming arrays relative to fleet average
- Correlate power output with irradiance to identify efficiency degradation
- KQL: EnergyMetric — AvgPowerKW, PerformanceRatio, IrradianceWm2, InverterEfficiency by ArrayId, PlantId

### 2. Predictive Maintenance
- Monitor module temperature: efficiency loss above 25C, concern above 65C
- Track inverter efficiency (healthy > 97%, investigate below 96%)
- Identify sensors with increasing anomaly rates (soiling, hotspots, string faults)
- Correlate maintenance history with current sensor readings
- KQL: ArrayReading — SensorType, Value, IsAnomaly trends; MaintenanceMetric — EventType, Component, CostUSD, DurationHours

### 3. Weather Response
- Monitor irradiance levels and cloud cover impact on production
- Detect soiling loss (declining PR with stable irradiance)
- Track high module temperature reducing conversion efficiency
- Assess weather impact on fleet availability
- KQL: WeatherMetric — IrradianceWm2, AmbientTempC, CloudCoverPct, RainMm by StationId, PlantId

### 4. Grid Compliance & Alerts
- Track grid frequency deviations (nominal 50 Hz, range 49.5-50.5)
- Monitor unacknowledged critical alerts
- Identify alert patterns by component type (Inverter, Module, PanelString, Tracker, Transformer)
- Escalate recurring alerts on the same array
- KQL: ArrayAlert — AlertType, Severity, Component, IsAcknowledged; EnergyMetric — GridFrequencyHz

### 5. Fleet Optimization
- Compare performance across solar plants and arrays
- Identify maintenance cost hotspots by component
- Track technician workload and specialization alignment
- Recommend optimal cleaning/maintenance scheduling based on soiling and weather
- KQL: MaintenanceMetric by PlantId, Component; EnergyMetric aggregate fleet metrics

## KQL TABLE SCHEMAS
- **ArrayReading**: ArrayId, PlantId, SensorId, SensorType, Timestamp, Value, Unit, Quality, IsAnomaly
- **ArrayAlert**: AlertId, ArrayId, PlantId, Timestamp, AlertType, Severity, MetricValue, ThresholdValue, Component, Message, IsAcknowledged
- **EnergyMetric**: ArrayId, PlantId, Timestamp, IrradianceWm2, PowerOutputKW, PerformanceRatio, ModuleTempC, InverterEfficiency, GridFrequencyHz
- **WeatherMetric**: StationId, PlantId, Timestamp, IrradianceWm2, AmbientTempC, WindSpeedMs, HumidityPct, PressureHPa, CloudCoverPct, RainMm
- **MaintenanceMetric**: EventId, ArrayId, PlantId, Timestamp, EventType, Component, DurationHours, CostUSD, TechnicianId, Status
"@

$dataAgentJson = @{
    "`$schema" = "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/data_agent/2.1.0/schema.json"
    name = $AgentName
    description = "Operations agent for Solar Farm Fleet — real-time monitoring, predictive maintenance, weather response, grid compliance."
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
        if ($resp.StatusCode -eq 202) {
            $loc = $resp.Headers["Location"]; if ($loc -is [array]) { $loc = $loc[0] }
            $ra = $resp.Headers["Retry-After"]; if ($ra -is [array]) { $ra = $ra[0] }; $retryAfter = if ($ra) { [int]$ra } else { 5 }
            if ($loc) { for ($i = 0; $i -lt 30; $i++) { Start-Sleep -Seconds $retryAfter; try { $poll = Invoke-RestMethod -Uri $loc -Headers $headers; if ($poll.status -eq "Succeeded") { break } } catch {} } }
        }
        Write-Host "  [OK] Operations Agent created: $AgentName" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Agent creation: $_" -ForegroundColor Red }
}

Write-Host "`n=== Solar Farm Operations Agent Deployment Complete ===" -ForegroundColor Cyan
