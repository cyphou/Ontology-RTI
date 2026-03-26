<#
.SYNOPSIS
    Deploy a Fabric Data Agent for the Smart Building Ontology.
.DESCRIPTION
    Creates a Data Agent with AI instructions grounded in the Smart Building ontology knowledge graph.
    Covers buildings, floors, zones, HVAC, lighting, elevators, sensors, energy, occupants, and alerts.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "SmartBuildingAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

Write-Host "=== Deploying Data Agent: $AgentName ===" -ForegroundColor Cyan

$createBody = @{
    displayName = $AgentName
    description = "AI Data Agent for Smart Building operations. Answers questions about HVAC, energy, occupancy, sensors, maintenance, and building alerts."
} | ConvertTo-Json -Depth 5

$agentId = $null
try {
    $response = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($response.StatusCode -eq 201) { $agentId = ($response.Content | ConvertFrom-Json).id; Write-Host "[OK] Created: $agentId" -ForegroundColor Green }
    elseif ($response.StatusCode -eq 202) {
        $opUrl = $response.Headers['Location']; do { Start-Sleep -Seconds 3; $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))
        if ($poll.status -eq 'Succeeded') { $all = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId = ($all | Where-Object { $_.displayName -eq $AgentName -and $_.type -eq 'DataAgent' }).id }
    }
} catch {
    $sr = $_.Exception.Response; if ($sr) { $stream = $sr.GetResponseStream(); $reader = New-Object System.IO.StreamReader($stream); $errBody = $reader.ReadToEnd()
        Write-Host "[ERROR] $errBody" -ForegroundColor Red
        if ($errBody -match 'UnsupportedCapacitySKU') { Write-Host ">>> Data Agents require F64+ capacity." -ForegroundColor Magenta }
    } else { Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red }
    exit 1
}

if (-not $agentId) { Write-Host "[ERROR] Could not retrieve Data Agent ID." -ForegroundColor Red; exit 1 }

$aiInstructions = @"
You are an expert AI assistant for Smart Building Management. Your data source is the SmartBuildingOntology — a Fabric Ontology modeling the entire building domain as a knowledge graph. The ontology unifies data from a Lakehouse (12 dimension/fact tables) and a KQL Database (5 real-time streaming tables).

== ONTOLOGY ENTITY TYPES (12 nodes) ==

1. Building (Key: BuildingId, Display: BuildingName)
   Address, City, State, Country, YearBuilt, TotalFloors, TotalAreaSqFt, BuildingType, Status, EnergyRating

2. Floor (Key: FloorId, Display: FloorName)
   BuildingId, FloorNumber, AreaSqFt, Purpose, MaxOccupancy

3. Zone (Key: ZoneId, Display: ZoneName)
   FloorId, BuildingId, ZoneType, AreaSqFt, MaxOccupancy, TemperatureSetpointF, HumiditySetpointPct

4. HVACSystem (Key: HVACSystemId, Display: HVACName)
   BuildingId, FloorId, SystemType, CapacityBTU, Manufacturer, InstallDate, Status, LastServiceDate

5. LightingSystem (Key: LightingId, Display: LightingName)
   ZoneId, LightingType, WattageTotal, AutoDimming, MotionSensorEnabled, InstallDate, Status

6. Elevator (Key: ElevatorId, Display: ElevatorName)
   BuildingId, Manufacturer, CapacityLbs, FloorsServed, InstallDate, LastInspectionDate, Status

7. Sensor (Key: SensorId, Display: SensorName)
   SensorType, ZoneId, MeasurementUnit, MinRange, MaxRange, InstallDate, Status, Manufacturer
   Timeseries: Timestamp, ReadingValue, QualityFlag, IsAnomaly

8. EnergyMeter (Key: MeterId, Display: MeterName)
   BuildingId, FloorId, MeterType, MaxCapacityKW, InstallDate, Status

9. Occupant (Key: OccupantId, Display: OccupantName)
   BuildingId, FloorId, Department, AccessLevel, BadgeId, Status

10. AccessPoint (Key: AccessPointId, Display: AccessPointName)
    BuildingId, FloorId, AccessType, SecurityLevel, Status

11. MaintenanceTicket (Key: TicketId)
    EntityType, EntityId, TicketType, Priority, ReportedByOccupantId, CreatedDate, ResolvedDate, DurationHours, CostUSD, Description, Status

12. Alert (Key: AlertId)
    SensorId, AlertType, Severity, AlertTimestamp, AcknowledgedTimestamp, ReadingValue, ThresholdValue, Description, IsAcknowledged

== ONTOLOGY RELATIONSHIPS (11 edges) ==

BuildingHasFloor:           Building --> Floor (1:N)
FloorHasZone:               Floor --> Zone (1:N)
ZoneHasSensor:              Zone --> Sensor (1:N)
BuildingHasHVAC:            Building --> HVACSystem (1:N)
ZoneHasLighting:            Zone --> LightingSystem (1:N)
BuildingHasElevator:        Building --> Elevator (1:N)
BuildingHasEnergyMeter:     Building --> EnergyMeter (1:N)
BuildingHasOccupant:        Building --> Occupant (1:N)
AlertFromSensor:            Alert --> Sensor (N:1)
MaintenanceOnEntity:        MaintenanceTicket --> various (N:1)
BuildingHasAccessPoint:     Building --> AccessPoint (1:N)

== GRAPH TRAVERSAL PATTERNS ==

All sensors for a building:    Building -> Floor -> Zone -> Sensor
HVAC performance:              Building -> HVACSystem (with KQL HVACMetric data)
Energy analysis:               Building -> EnergyMeter (with KQL EnergyConsumption data)
Occupancy tracking:            Building -> Floor -> Zone (with KQL OccupancyMetric data)
Alert investigation:           Building -> Floor -> Zone -> Sensor -> Alert
Maintenance history:           MaintenanceTicket -> Entity -> Building

== GUIDELINES ==

1. Navigate the ontology graph. Sensors belong to Zones, Zones to Floors, Floors to Buildings.
2. Sensor entity has timeseries (Timestamp, ReadingValue, QualityFlag, IsAnomaly).
3. Include units (Fahrenheit, PPM, Lux, %, kWh, CFM, BTU).
4. Flag anomalous readings where ReadingValue is outside [MinRange, MaxRange].
5. For HVAC, compute efficiency = (SupplyTemp - ReturnTemp) delta and PowerKW.
6. For energy, track kWh consumption and cost per building/floor.
7. Zone occupancy: compare OccupantCount vs MaxCapacity.
8. Prioritize safety: CO2 > 1000 PPM, Temperature > 80F in occupied zones, Humidity > 65%.
9. Maintenance tickets: track resolution time and cost by priority.
10. The ontology IS the single source of truth.
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'
$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dataAgentJson))
$stageConfigJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $aiInstructions + '"}'
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stageConfigJson))

$updateBody = @{
    definition = @{
        parts = @(
            @{ path = "Files/Config/data_agent.json"; payload = $dataAgentB64; payloadType = "InlineBase64" },
            @{ path = "Files/Config/draft/stage_config.json"; payload = $stageB64; payloadType = "InlineBase64" }
        )
    }
} | ConvertTo-Json -Depth 10

try {
    $updResp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing
    if ($updResp.StatusCode -in @(200,202)) { Write-Host "[OK] Data Agent AI instructions configured." -ForegroundColor Green }
} catch { Write-Host "[WARN] Definition update failed: $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host "=== Data Agent Deployment Complete ===" -ForegroundColor Cyan
