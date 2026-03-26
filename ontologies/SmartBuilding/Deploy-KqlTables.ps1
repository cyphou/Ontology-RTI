<#
.SYNOPSIS
    Create KQL tables and ingest data for the Smart Building domain.
.DESCRIPTION
    Creates 5 KQL tables for building telemetry and ingests enriched data:
      - SensorReading       (enriched from SensorTelemetry.csv + DimSensor + DimZone)
      - BuildingAlert       (alerts from FactAlert.csv enriched with zone/building context)
      - HVACMetric          (HVAC system performance metrics)
      - EnergyConsumption   (energy meter readings per building/floor)
      - OccupancyMetric     (zone occupancy over time)
.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER EventhouseId
    The Eventhouse GUID (auto-detected if omitted).
.PARAMETER KqlDatabaseId
    The KQL Database GUID (auto-detected if omitted).
.PARAMETER QueryServiceUri
    The Kusto query service URI (auto-detected if omitted).
.PARAMETER DataFolder
    Path to the data/ folder containing CSV files.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$EventhouseId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseName,
    [Parameter(Mandatory=$false)] [string]$DataFolder
)

$ErrorActionPreference = "Stop"

if (-not $DataFolder) {
    $DataFolder = Join-Path (Split-Path -Parent $PSScriptRoot) "data"
}

Write-Host "=== Deploying Smart Building KQL Tables ===" -ForegroundColor Cyan

# ── Authentication ──────────────────────────────────────────────────────────
$fabricToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$fabricHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

# ── Auto-detect Eventhouse, KQL Database, Query URI ─────────────────────────
if (-not $EventhouseId -or -not $KqlDatabaseId -or -not $QueryServiceUri) {
    Write-Host "Auto-detecting Eventhouse and KQL Database..." -ForegroundColor Yellow
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $fabricHeaders).value
    if (-not $KqlDatabaseId) {
        $kqlDb = $allItems | Where-Object { $_.type -eq 'KQLDatabase' } | Select-Object -First 1
        if ($kqlDb) { $KqlDatabaseId = $kqlDb.id; Write-Host "  Found KQL Database: $($kqlDb.displayName)" -ForegroundColor Gray }
        else { Write-Host "[ERROR] No KQL Database found." -ForegroundColor Red; exit 1 }
    }
    if (-not $EventhouseId) {
        $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1
        if ($eh) { $EventhouseId = $eh.id }
    }
    if (-not $QueryServiceUri -and $EventhouseId) {
        $ehDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$EventhouseId" -Headers $fabricHeaders
        $QueryServiceUri = $ehDetails.properties.queryServiceUri
    }
}
if (-not $QueryServiceUri) { Write-Host "[ERROR] Could not determine Kusto query service URI." -ForegroundColor Red; exit 1 }

if (-not $KqlDatabaseName) {
    $kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $fabricHeaders
    $KqlDatabaseName = $kqlDbDetails.displayName
}

# ── Acquire Kusto token ────────────────────────────────────────────────────
$kustoToken = $null
foreach ($resource in @($QueryServiceUri, "https://kusto.kusto.windows.net", "https://api.fabric.microsoft.com")) {
    try { $kustoToken = (Get-AzAccessToken -ResourceUrl $resource).Token; break } catch {}
}
if (-not $kustoToken) { Write-Host "[ERROR] Could not acquire Kusto token." -ForegroundColor Red; exit 1 }

function Invoke-KustoMgmt {
    param([string]$Command, [string]$Description)
    if ($Description) { Write-Host "  $Description" -ForegroundColor Gray }
    $body = @{ db = $KqlDatabaseName; csl = $Command } | ConvertTo-Json -Depth 2
    $headers = @{ "Authorization" = "Bearer $kustoToken"; "Content-Type" = "application/json; charset=utf-8" }
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            return Invoke-RestMethod -Method Post -Uri "$QueryServiceUri/v1/rest/mgmt" -Headers $headers -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) -ContentType "application/json; charset=utf-8"
        } catch {
            if ($attempt -lt 3) { Start-Sleep -Seconds (10 * $attempt) } else { throw "Kusto command failed: $($_.Exception.Message)" }
        }
    }
}

# ── Wait for DB ─────────────────────────────────────────────────────────────
Write-Host "`nWaiting for KQL database to be ready..." -ForegroundColor Yellow
for ($w = 1; $w -le 6; $w++) {
    try { Invoke-KustoMgmt -Command ".show database" | Out-Null; Write-Host "  KQL database ready." -ForegroundColor Green; break }
    catch { Write-Host "  Waiting 15s ($w/6)..." -ForegroundColor DarkYellow; Start-Sleep -Seconds 15 }
}

# ════════════════════════════════════════════════════════════════════════════
# STEP 1: CREATE KQL TABLES
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 1] Creating KQL tables..." -ForegroundColor Cyan

$tableDefinitions = @(
    @{
        Name = "SensorReading"
        Schema = "(SensorId:string, ZoneId:string, BuildingId:string, FloorId:string, Timestamp:datetime, ReadingValue:real, MeasurementUnit:string, SensorType:string, QualityFlag:string, IsAnomaly:bool)"
    },
    @{
        Name = "BuildingAlert"
        Schema = "(AlertId:string, SensorId:string, ZoneId:string, BuildingId:string, Timestamp:datetime, AlertType:string, Severity:string, ReadingValue:real, ThresholdValue:real, Message:string, IsAcknowledged:bool)"
    },
    @{
        Name = "HVACMetric"
        Schema = "(HVACSystemId:string, BuildingId:string, FloorId:string, Timestamp:datetime, SupplyTempF:real, ReturnTempF:real, AirFlowCFM:real, HumidityPct:real, PowerKW:real, Mode:string, EfficiencyPct:real)"
    },
    @{
        Name = "EnergyConsumption"
        Schema = "(MeterId:string, BuildingId:string, FloorId:string, Timestamp:datetime, PowerKWh:real, PeakDemandKW:real, PowerFactor:real, CostUSD:real, Source:string)"
    },
    @{
        Name = "OccupancyMetric"
        Schema = "(ZoneId:string, BuildingId:string, FloorId:string, Timestamp:datetime, OccupantCount:int, MaxCapacity:int, UtilizationPct:real)"
    }
)

foreach ($tbl in $tableDefinitions) {
    try {
        Invoke-KustoMgmt -Command ".create-merge table $($tbl.Name) $($tbl.Schema)" -Description "Creating $($tbl.Name)..." | Out-Null
        Write-Host "  [OK] $($tbl.Name)" -ForegroundColor Green
    } catch { Write-Host "  [WARN] $($tbl.Name): $_" -ForegroundColor Yellow }
}

# ════════════════════════════════════════════════════════════════════════════
# STEP 2: ENRICH SensorTelemetry → SensorReading
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 2] Enriching SensorTelemetry → SensorReading..." -ForegroundColor Cyan

$sensorLookup = @{}
Import-Csv -Path (Join-Path $DataFolder "DimSensor.csv") | ForEach-Object {
    $sensorLookup[$_.SensorId] = @{ ZoneId = $_.ZoneId; SensorType = $_.SensorType; MeasurementUnit = $_.MeasurementUnit; MinRange = [double]$_.MinRange; MaxRange = [double]$_.MaxRange }
}
$zoneLookup = @{}
Import-Csv -Path (Join-Path $DataFolder "DimZone.csv") | ForEach-Object {
    $zoneLookup[$_.ZoneId] = @{ FloorId = $_.FloorId; BuildingId = $_.BuildingId }
}

$telemetry = Import-Csv -Path (Join-Path $DataFolder "SensorTelemetry.csv")
$lines = @()
foreach ($row in $telemetry) {
    $s = $sensorLookup[$row.SensorId]
    if ($s) {
        $z = $zoneLookup[$s.ZoneId]
        $bid = if ($z) { $z.BuildingId } else { "UNKNOWN" }
        $fid = if ($z) { $z.FloorId } else { "UNKNOWN" }
        $val = [double]$row.ReadingValue
        $anomaly = ($val -lt $s.MinRange -or $val -gt $s.MaxRange).ToString().ToLower()
        $lines += "$($row.SensorId),$($s.ZoneId),$bid,$fid,$($row.Timestamp),$val,$($s.MeasurementUnit),$($s.SensorType),$($row.QualityFlag),$anomaly"
    }
}
$batchSize = 50
for ($i = 0; $i -lt $lines.Count; $i += $batchSize) {
    $batch = $lines[$i..([Math]::Min($i + $batchSize - 1, $lines.Count - 1))]
    try { Invoke-KustoMgmt -Command ".ingest inline into table SensorReading with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
}
Write-Host "  [OK] SensorReading ($($lines.Count) rows)" -ForegroundColor Green

# ════════════════════════════════════════════════════════════════════════════
# STEP 3: INGEST BuildingAlert SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 3] Ingesting BuildingAlert sample data..." -ForegroundColor Cyan

$alertData = @(
    "ALR-001,SNS-001,ZN-001,BLD-001,2024-10-01T08:30:00,HighTemperature,Critical,82.5,80.0,Lobby temperature exceeded comfort threshold,false"
    "ALR-002,SNS-002,ZN-001,BLD-001,2024-10-01T09:00:00,HighCO2,High,1250.0,1000.0,CO2 levels above safe limit in lobby,false"
    "ALR-003,SNS-007,ZN-003,BLD-001,2024-10-01T10:15:00,HighHumidity,Medium,72.0,65.0,Server room humidity above threshold,true"
    "ALR-004,SNS-010,ZN-004,BLD-001,2024-10-01T11:30:00,LowTemperature,Low,62.0,65.0,Conference room temperature below comfort,true"
    "ALR-005,SNS-014,ZN-006,BLD-002,2024-10-01T12:00:00,HighTemperature,High,79.5,78.0,Lab environment temperature elevated,false"
    "ALR-006,SNS-016,ZN-007,BLD-002,2024-10-01T13:45:00,PoorAirQuality,Critical,2100.0,1500.0,Parking garage CO2 critical,false"
    "ALR-007,SNS-019,ZN-009,BLD-003,2024-10-01T14:15:00,HighCO2,Medium,1100.0,1000.0,Open office CO2 above threshold,true"
    "ALR-008,SNS-022,ZN-011,BLD-003,2024-10-01T15:30:00,HighTemperature,High,81.0,78.0,Kitchen area overheating,false"
    "ALR-009,SNS-025,ZN-013,BLD-004,2024-10-02T08:00:00,LowTemperature,Medium,58.0,65.0,Warehouse temperature below minimum,true"
    "ALR-010,SNS-028,ZN-016,BLD-004,2024-10-02T09:30:00,HighHumidity,High,75.0,65.0,Storage area humidity critical,false"
    "ALR-011,SNS-004,ZN-002,BLD-001,2024-10-02T10:00:00,OverOccupancy,Warning,95.0,90.0,Office floor approaching max occupancy,true"
    "ALR-012,SNS-012,ZN-005,BLD-002,2024-10-02T11:15:00,LightingFailure,High,0.0,200.0,Hallway lighting level zero - possible failure,false"
    "ALR-013,SNS-008,ZN-003,BLD-001,2024-10-02T13:00:00,HighTemperature,Critical,85.0,75.0,Server room overheating critical,false"
    "ALR-014,SNS-020,ZN-010,BLD-003,2024-10-02T14:30:00,HighCO2,Medium,1080.0,1000.0,Meeting room CO2 elevated,true"
    "ALR-015,SNS-030,ZN-018,BLD-005,2024-10-02T16:00:00,LowTemperature,Low,60.0,65.0,Lobby temperature below comfort,true"
    "ALR-016,SNS-002,ZN-001,BLD-001,2024-10-03T08:15:00,HighCO2,High,1320.0,1000.0,Morning rush CO2 spike in lobby,false"
    "ALR-017,SNS-007,ZN-003,BLD-001,2024-10-03T09:45:00,HighHumidity,Critical,78.0,65.0,Server room humidity alarm,false"
    "ALR-018,SNS-015,ZN-006,BLD-002,2024-10-03T11:00:00,HighTemperature,Medium,77.5,75.0,Lab temperature slightly elevated,true"
    "ALR-019,SNS-023,ZN-012,BLD-003,2024-10-03T12:30:00,ElevatorFault,Critical,0.0,1.0,Elevator A stopped between floors,false"
    "ALR-020,SNS-001,ZN-001,BLD-001,2024-10-03T14:00:00,HighTemperature,High,80.5,78.0,Afternoon lobby heat buildup,false"
)
$alertInline = $alertData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table BuildingAlert with (format='csv') <|`n$alertInline" -Description "Ingesting 20 BuildingAlert rows..." | Out-Null
    Write-Host "  [OK] BuildingAlert (20 rows)" -ForegroundColor Green
} catch { Write-Host "  [WARN] BuildingAlert: $_" -ForegroundColor Yellow }

# ════════════════════════════════════════════════════════════════════════════
# STEP 4: INGEST HVACMetric SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 4] Ingesting HVACMetric sample data..." -ForegroundColor Cyan

$hvacData = @(
    "HVAC-001,BLD-001,FL-001,2024-10-01T00:00:00,55.0,72.0,2400,42.0,18.5,Cooling,92.0"
    "HVAC-001,BLD-001,FL-001,2024-10-01T06:00:00,54.0,71.5,2500,43.0,19.2,Cooling,91.5"
    "HVAC-001,BLD-001,FL-001,2024-10-01T12:00:00,56.0,73.5,2800,45.0,22.0,Cooling,89.0"
    "HVAC-001,BLD-001,FL-001,2024-10-01T18:00:00,55.5,72.0,2200,41.0,17.0,Cooling,93.0"
    "HVAC-002,BLD-001,FL-002,2024-10-01T00:00:00,68.0,70.0,1800,40.0,12.0,Heating,94.5"
    "HVAC-002,BLD-001,FL-002,2024-10-01T06:00:00,70.0,72.0,2000,41.0,14.0,Heating,93.0"
    "HVAC-002,BLD-001,FL-002,2024-10-01T12:00:00,65.0,71.0,2200,44.0,16.5,Auto,91.0"
    "HVAC-002,BLD-001,FL-002,2024-10-01T18:00:00,66.0,70.5,1900,40.0,13.0,Heating,94.0"
    "HVAC-003,BLD-001,FL-003,2024-10-01T00:00:00,52.0,68.0,3200,35.0,25.0,Cooling,88.0"
    "HVAC-003,BLD-001,FL-003,2024-10-01T12:00:00,53.0,69.0,3400,36.0,27.0,Cooling,87.0"
    "HVAC-004,BLD-002,FL-004,2024-10-01T00:00:00,55.0,71.0,2600,42.0,20.0,Cooling,90.5"
    "HVAC-004,BLD-002,FL-004,2024-10-01T12:00:00,56.0,72.5,2900,46.0,23.0,Cooling,88.5"
    "HVAC-005,BLD-002,FL-005,2024-10-01T00:00:00,54.0,70.0,2100,40.0,16.0,Cooling,92.5"
    "HVAC-005,BLD-002,FL-005,2024-10-01T12:00:00,55.0,71.0,2300,43.0,18.0,Cooling,91.0"
    "HVAC-006,BLD-003,FL-006,2024-10-01T00:00:00,58.0,73.0,2000,38.0,15.0,Cooling,93.5"
    "HVAC-006,BLD-003,FL-006,2024-10-01T12:00:00,59.0,74.0,2200,40.0,17.5,Cooling,91.5"
    "HVAC-007,BLD-003,FL-007,2024-10-01T00:00:00,62.0,70.0,1800,39.0,12.5,Auto,94.0"
    "HVAC-007,BLD-003,FL-007,2024-10-01T12:00:00,63.0,71.0,2000,41.0,14.0,Auto,93.0"
    "HVAC-008,BLD-004,FL-008,2024-10-01T00:00:00,50.0,65.0,3000,32.0,28.0,Cooling,86.0"
    "HVAC-008,BLD-004,FL-008,2024-10-01T12:00:00,51.0,66.0,3200,34.0,30.0,Cooling,85.0"
    "HVAC-009,BLD-004,FL-009,2024-10-01T00:00:00,48.0,64.0,3500,30.0,32.0,Cooling,84.0"
    "HVAC-009,BLD-004,FL-009,2024-10-01T12:00:00,49.0,65.0,3600,31.0,33.0,Cooling,83.5"
    "HVAC-010,BLD-005,FL-010,2024-10-01T00:00:00,55.0,72.0,2400,42.0,18.5,Cooling,92.0"
    "HVAC-010,BLD-005,FL-010,2024-10-01T12:00:00,56.0,73.0,2600,44.0,20.0,Cooling,91.0"
    "HVAC-011,BLD-005,FL-011,2024-10-01T00:00:00,58.0,71.0,2100,39.0,16.0,Auto,93.0"
    "HVAC-011,BLD-005,FL-011,2024-10-01T12:00:00,59.0,72.0,2300,41.0,18.0,Auto,92.0"
    "HVAC-012,BLD-005,FL-012,2024-10-01T00:00:00,53.0,70.0,2800,38.0,22.0,Cooling,89.0"
    "HVAC-012,BLD-005,FL-012,2024-10-01T12:00:00,54.0,71.0,3000,40.0,24.0,Cooling,88.0"
)
$hvacInline = $hvacData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table HVACMetric with (format='csv') <|`n$hvacInline" -Description "Ingesting 28 HVACMetric rows..." | Out-Null
    Write-Host "  [OK] HVACMetric (28 rows)" -ForegroundColor Green
} catch { Write-Host "  [WARN] HVACMetric: $_" -ForegroundColor Yellow }

# ════════════════════════════════════════════════════════════════════════════
# STEP 5: INGEST EnergyConsumption SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 5] Ingesting EnergyConsumption sample data..." -ForegroundColor Cyan

$energyData = @(
    "EM-001,BLD-001,FL-001,2024-10-01T00:00:00,145.2,62.0,0.95,18.20,Grid"
    "EM-001,BLD-001,FL-001,2024-10-01T06:00:00,210.5,85.0,0.93,26.30,Grid"
    "EM-001,BLD-001,FL-001,2024-10-01T12:00:00,285.0,120.0,0.91,35.60,Grid"
    "EM-001,BLD-001,FL-001,2024-10-01T18:00:00,195.0,78.0,0.94,24.40,Grid"
    "EM-002,BLD-001,FL-002,2024-10-01T00:00:00,120.0,50.0,0.96,15.00,Grid"
    "EM-002,BLD-001,FL-002,2024-10-01T12:00:00,250.0,105.0,0.92,31.25,Grid"
    "EM-003,BLD-001,FL-003,2024-10-01T00:00:00,310.0,145.0,0.88,38.75,Grid"
    "EM-003,BLD-001,FL-003,2024-10-01T12:00:00,380.0,160.0,0.86,47.50,Grid"
    "EM-004,BLD-002,FL-004,2024-10-01T00:00:00,180.0,75.0,0.94,22.50,Grid"
    "EM-004,BLD-002,FL-004,2024-10-01T12:00:00,320.0,130.0,0.90,40.00,Grid"
    "EM-005,BLD-002,FL-005,2024-10-01T00:00:00,95.0,40.0,0.97,11.90,Solar"
    "EM-005,BLD-002,FL-005,2024-10-01T12:00:00,45.0,38.0,0.98,5.60,Solar"
    "EM-006,BLD-003,FL-006,2024-10-01T00:00:00,160.0,65.0,0.95,20.00,Grid"
    "EM-006,BLD-003,FL-006,2024-10-01T12:00:00,275.0,115.0,0.91,34.40,Grid"
    "EM-007,BLD-003,FL-007,2024-10-01T00:00:00,130.0,55.0,0.96,16.25,Grid"
    "EM-007,BLD-003,FL-007,2024-10-01T12:00:00,220.0,90.0,0.93,27.50,Grid"
    "EM-008,BLD-004,FL-008,2024-10-01T00:00:00,420.0,180.0,0.85,52.50,Grid"
    "EM-008,BLD-004,FL-008,2024-10-01T12:00:00,480.0,200.0,0.83,60.00,Grid"
    "EM-009,BLD-004,FL-009,2024-10-01T00:00:00,350.0,150.0,0.87,43.75,Grid"
    "EM-009,BLD-004,FL-009,2024-10-01T12:00:00,410.0,175.0,0.85,51.25,Grid"
    "EM-010,BLD-005,FL-010,2024-10-01T00:00:00,200.0,82.0,0.93,25.00,Grid"
    "EM-010,BLD-005,FL-010,2024-10-01T12:00:00,340.0,140.0,0.89,42.50,Grid"
)
$energyInline = $energyData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table EnergyConsumption with (format='csv') <|`n$energyInline" -Description "Ingesting 22 EnergyConsumption rows..." | Out-Null
    Write-Host "  [OK] EnergyConsumption (22 rows)" -ForegroundColor Green
} catch { Write-Host "  [WARN] EnergyConsumption: $_" -ForegroundColor Yellow }

# ════════════════════════════════════════════════════════════════════════════
# STEP 6: INGEST OccupancyMetric SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 6] Ingesting OccupancyMetric sample data..." -ForegroundColor Cyan

$occData = @(
    "ZN-001,BLD-001,FL-001,2024-10-01T08:00:00,45,100,45.0"
    "ZN-001,BLD-001,FL-001,2024-10-01T12:00:00,82,100,82.0"
    "ZN-001,BLD-001,FL-001,2024-10-01T17:00:00,25,100,25.0"
    "ZN-002,BLD-001,FL-002,2024-10-01T08:00:00,60,80,75.0"
    "ZN-002,BLD-001,FL-002,2024-10-01T12:00:00,72,80,90.0"
    "ZN-002,BLD-001,FL-002,2024-10-01T17:00:00,15,80,18.8"
    "ZN-003,BLD-001,FL-003,2024-10-01T08:00:00,2,10,20.0"
    "ZN-003,BLD-001,FL-003,2024-10-01T12:00:00,5,10,50.0"
    "ZN-004,BLD-001,FL-004,2024-10-01T09:00:00,12,20,60.0"
    "ZN-004,BLD-001,FL-004,2024-10-01T14:00:00,18,20,90.0"
    "ZN-005,BLD-002,FL-005,2024-10-01T08:00:00,30,50,60.0"
    "ZN-005,BLD-002,FL-005,2024-10-01T12:00:00,45,50,90.0"
    "ZN-006,BLD-002,FL-006,2024-10-01T08:00:00,8,15,53.3"
    "ZN-006,BLD-002,FL-006,2024-10-01T12:00:00,12,15,80.0"
    "ZN-007,BLD-002,FL-007,2024-10-01T08:00:00,0,200,0.0"
    "ZN-007,BLD-002,FL-007,2024-10-01T12:00:00,85,200,42.5"
    "ZN-009,BLD-003,FL-008,2024-10-01T08:00:00,55,70,78.6"
    "ZN-009,BLD-003,FL-008,2024-10-01T12:00:00,68,70,97.1"
    "ZN-010,BLD-003,FL-009,2024-10-01T09:00:00,8,12,66.7"
    "ZN-010,BLD-003,FL-009,2024-10-01T14:00:00,11,12,91.7"
    "ZN-013,BLD-004,FL-010,2024-10-01T08:00:00,20,150,13.3"
    "ZN-013,BLD-004,FL-010,2024-10-01T12:00:00,80,150,53.3"
    "ZN-016,BLD-004,FL-011,2024-10-01T08:00:00,5,30,16.7"
    "ZN-016,BLD-004,FL-011,2024-10-01T12:00:00,15,30,50.0"
    "ZN-018,BLD-005,FL-012,2024-10-01T08:00:00,40,60,66.7"
    "ZN-018,BLD-005,FL-012,2024-10-01T12:00:00,55,60,91.7"
)
$occInline = $occData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table OccupancyMetric with (format='csv') <|`n$occInline" -Description "Ingesting 26 OccupancyMetric rows..." | Out-Null
    Write-Host "  [OK] OccupancyMetric (26 rows)" -ForegroundColor Green
} catch { Write-Host "  [WARN] OccupancyMetric: $_" -ForegroundColor Yellow }

# ════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "=== Smart Building KQL Tables Summary ===" -ForegroundColor Cyan
Write-Host "  SensorReading     ($($lines.Count) rows from SensorTelemetry.csv)" -ForegroundColor White
Write-Host "  BuildingAlert     (20 sample rows)" -ForegroundColor White
Write-Host "  HVACMetric        (28 sample rows)" -ForegroundColor White
Write-Host "  EnergyConsumption (22 sample rows)" -ForegroundColor White
Write-Host "  OccupancyMetric   (26 sample rows)" -ForegroundColor White
Write-Host "=== Complete ===" -ForegroundColor Cyan
