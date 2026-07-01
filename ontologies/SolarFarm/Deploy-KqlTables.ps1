<#
.SYNOPSIS
    Create KQL tables and ingest data for the Solar Farm domain.
.DESCRIPTION
    Creates 6 KQL tables for solar plant telemetry:
      - ArrayReading      (enriched from SensorTelemetry.csv with array/plant context)
      - ArrayTelemetry    (wide-format ontology timeseries table)
      - ArrayAlert        (alerts with array/component context)
      - EnergyMetric      (energy production per array over time)
      - WeatherMetric     (weather station readings: irradiance, temp, wind)
      - MaintenanceMetric (maintenance event tracking with cost and duration)
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
if (-not $DataFolder) { $DataFolder = Join-Path (Split-Path -Parent $PSScriptRoot) "data" }

Write-Host "=== Deploying Solar Farm KQL Tables ===" -ForegroundColor Cyan

$fabricToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$fabricHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

if (-not $EventhouseId -or -not $KqlDatabaseId -or -not $QueryServiceUri) {
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $fabricHeaders).value
    if (-not $KqlDatabaseId) {
        $kqlDbs = $allItems | Where-Object { $_.type -eq 'KQLDatabase' }
        if ($KqlDatabaseName) { $kqlDb = $kqlDbs | Where-Object { $_.displayName -eq $KqlDatabaseName } | Select-Object -First 1 }
        if (-not $kqlDb) { $kqlDb = $kqlDbs | Select-Object -First 1 }
        if ($kqlDb) { $KqlDatabaseId = $kqlDb.id } else { exit 1 }
    }
    if (-not $EventhouseId) {
        $ehs = $allItems | Where-Object { $_.type -eq 'Eventhouse' }
        if ($KqlDatabaseName) { $eh = $ehs | Where-Object { $_.displayName -eq $KqlDatabaseName } | Select-Object -First 1 }
        if (-not $eh) { $eh = $ehs | Select-Object -First 1 }
        if ($eh) { $EventhouseId = $eh.id }
    }
    if (-not $QueryServiceUri -and $EventhouseId) { $ehD = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$EventhouseId" -Headers $fabricHeaders; $QueryServiceUri = $ehD.properties.queryServiceUri }
}
if (-not $QueryServiceUri) { exit 1 }
if (-not $KqlDatabaseName) { $kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $fabricHeaders; $KqlDatabaseName = $kqlDbDetails.displayName }

$kustoToken = $null
foreach ($resource in @($QueryServiceUri, "https://kusto.kusto.windows.net", "https://api.fabric.microsoft.com")) {
    try { $kustoToken = (Get-AzAccessToken -ResourceUrl $resource).Token; break } catch {}
}
if (-not $kustoToken) { exit 1 }

function Invoke-KustoMgmt {
    param([string]$Command, [string]$Description)
    if ($Description) { Write-Host "  $Description" -ForegroundColor Gray }
    $body = @{ db = $KqlDatabaseName; csl = $Command } | ConvertTo-Json -Depth 2
    $h = @{ "Authorization" = "Bearer $kustoToken"; "Content-Type" = "application/json; charset=utf-8" }
    for ($a = 1; $a -le 3; $a++) {
        try { return Invoke-RestMethod -Method Post -Uri "$QueryServiceUri/v1/rest/mgmt" -Headers $h -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) -ContentType "application/json; charset=utf-8" }
        catch { if ($a -lt 3) { Start-Sleep -Seconds (10 * $a) } else { throw } }
    }
}

for ($w = 1; $w -le 6; $w++) { try { Invoke-KustoMgmt -Command ".show database" | Out-Null; break } catch { Start-Sleep -Seconds 15 } }

# -- CREATE TABLES --------------------------------------------------------
Write-Host "`n[Step 1] Creating KQL tables..." -ForegroundColor Cyan

$tables = @(
    @{ Name = "ArrayReading"; Schema = "(ArrayId:string, PlantId:string, SensorId:string, SensorType:string, Timestamp:datetime, Value:real, Unit:string, Quality:string, IsAnomaly:bool)" },
    @{ Name = "ArrayTelemetry"; Schema = "(ArrayId:string, Timestamp:datetime, IrradianceWm2:real, PowerOutputKW:real, ModuleTempC:real, InverterLoadPct:real, VoltageV:real, CurrentA:real)" },
    @{ Name = "ArrayAlert"; Schema = "(AlertId:string, ArrayId:string, PlantId:string, Timestamp:datetime, AlertType:string, Severity:string, MetricValue:real, ThresholdValue:real, Component:string, Message:string, IsAcknowledged:bool)" },
    @{ Name = "EnergyMetric"; Schema = "(ArrayId:string, PlantId:string, Timestamp:datetime, IrradianceWm2:real, PowerOutputKW:real, PerformanceRatio:real, ModuleTempC:real, InverterEfficiency:real, GridFrequencyHz:real)" },
    @{ Name = "WeatherMetric"; Schema = "(StationId:string, PlantId:string, Timestamp:datetime, IrradianceWm2:real, AmbientTempC:real, WindSpeedMs:real, HumidityPct:real, PressureHPa:real, CloudCoverPct:real, RainMm:real)" },
    @{ Name = "MaintenanceMetric"; Schema = "(EventId:string, ArrayId:string, PlantId:string, Timestamp:datetime, EventType:string, Component:string, DurationHours:real, CostUSD:real, TechnicianId:string, Status:string)" }
)

foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".create-merge table $($t.Name) $($t.Schema)" -Description "Creating $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name)" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name): $_" -ForegroundColor Yellow }
}

# -- Enable streaming ingestion policies -----------------------------------
Write-Host "`n[Step 1b] Enabling streaming ingestion policies..." -ForegroundColor Cyan
foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".alter table $($t.Name) policy streamingingestion '{`"IsEnabled`": true}'" -Description "Streaming on $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name) streaming enabled" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name) streaming policy: $_" -ForegroundColor Yellow }
}

# -- Build lookups ---------------------------------------------------------
$arrayLookup = @{}
$arrayCsv = Join-Path $DataFolder "DimSolarArray.csv"
if (Test-Path $arrayCsv) { Import-Csv -Path $arrayCsv | ForEach-Object { $arrayLookup[$_.ArrayId] = $_.PlantId } }

$sensorLookup = @{}
$sensorCsv = Join-Path $DataFolder "DimSensor.csv"
if (Test-Path $sensorCsv) { Import-Csv -Path $sensorCsv | ForEach-Object { $sensorLookup[$_.SensorId] = @{ MinThreshold = [double]$_.MinThreshold; MaxThreshold = [double]$_.MaxThreshold } } }

# -- ENRICH SensorTelemetry -> ArrayReading --------------------------------
Write-Host "`n[Step 2] Enriching SensorTelemetry -> ArrayReading..." -ForegroundColor Cyan

# Solar telemetry: Timestamp,ArrayId,SensorId,SensorType,Value,Unit,Quality
$telemetry = Import-Csv -Path (Join-Path $DataFolder "SensorTelemetry.csv")
$lines = @()
foreach ($row in $telemetry) {
    $pid = $arrayLookup[$row.ArrayId]
    if (-not $pid) { $pid = "UNKNOWN" }
    $val = [double]$row.Value
    $s = $sensorLookup[$row.SensorId]
    $anomaly = if ($s) { ($val -lt $s.MinThreshold -or $val -gt $s.MaxThreshold).ToString().ToLower() } else { "false" }
    $lines += "$($row.ArrayId),$pid,$($row.SensorId),$($row.SensorType),$($row.Timestamp),$val,$($row.Unit),$($row.Quality),$anomaly"
}
for ($i = 0; $i -lt $lines.Count; $i += 50) {
    $batch = $lines[$i..([Math]::Min($i + 49, $lines.Count - 1))]
    try { Invoke-KustoMgmt -Command ".ingest inline into table ArrayReading with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch { Write-Warning "ArrayReading batch ingest failed (non-fatal): $($_.Exception.Message)" }
}
Write-Host "  [OK] ArrayReading ($($lines.Count) rows)" -ForegroundColor Green

# -- PIVOT SensorTelemetry -> ArrayTelemetry (wide ontology timeseries) -----
Write-Host "`n[Step 2b] Pivoting SensorTelemetry -> ArrayTelemetry (ontology timeseries)..." -ForegroundColor Cyan

$telemetryGrouped = @{}
foreach ($row in $telemetry) {
    $key = "$($row.ArrayId)|$($row.Timestamp)"
    if (-not $telemetryGrouped.ContainsKey($key)) {
        $telemetryGrouped[$key] = @{ ArrayId = $row.ArrayId; Timestamp = $row.Timestamp; Irradiance = 0; Power = 0; ModuleTemp = 0; InverterLoad = 0; Voltage = 0; Current = 0 }
    }
    $val = [double]$row.Value
    switch ($row.SensorType) {
        "Irradiance"   { $telemetryGrouped[$key].Irradiance = $val }
        "AcPower"      { $telemetryGrouped[$key].Power = $val }
        "ModuleTemp"   { $telemetryGrouped[$key].ModuleTemp = $val }
        "InverterLoad" { $telemetryGrouped[$key].InverterLoad = $val }
    }
}
$ttLines = @()
foreach ($entry in $telemetryGrouped.Values) {
    $ttLines += "$($entry.ArrayId),$($entry.Timestamp),$($entry.Irradiance),$($entry.Power),$($entry.ModuleTemp),$($entry.InverterLoad),$($entry.Voltage),$($entry.Current)"
}
for ($i = 0; $i -lt $ttLines.Count; $i += 50) {
    $batch = $ttLines[$i..([Math]::Min($i + 49, $ttLines.Count - 1))]
    try { Invoke-KustoMgmt -Command ".ingest inline into table ArrayTelemetry with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch { Write-Warning "ArrayTelemetry batch ingest failed (non-fatal): $($_.Exception.Message)" }
}
Write-Host "  [OK] ArrayTelemetry ($($ttLines.Count) rows)" -ForegroundColor Green

# -- INGEST ArrayAlert -----------------------------------------------------
Write-Host "`n[Step 3] Ingesting ArrayAlert sample data..." -ForegroundColor Cyan

$alertData = @(
    "SA-001,CESTAS-PV-01,CESTAS,2025-12-01T10:00:00,LowPerformanceRatio,High,0.62,0.75,Inverter,Performance ratio below expected for irradiance,false"
    "SA-002,CESTAS-PV-03,CESTAS,2025-12-01T12:00:00,HighModuleTemp,Warning,68.0,65.0,Module,Module temperature elevated reducing efficiency,true"
    "SA-003,MARVILLE-PV-02,MARVILLE,2025-12-01T11:00:00,InverterFault,Critical,0.0,1.0,Inverter,Inverter tripped offline no AC output,false"
    "SA-004,TOUL-PV-01,TOUL,2025-12-01T13:00:00,StringOpenCircuit,High,0.0,10.0,PanelString,String voltage zero possible open circuit,false"
    "SA-005,MEES-PV-02,MEES,2025-12-01T10:30:00,SoilingLoss,Medium,0.70,0.80,Module,Estimated soiling loss above threshold,true"
    "SA-006,GABARDAN-PV-01,GABARDAN,2025-12-01T14:00:00,TrackerStall,High,0.0,1.0,Tracker,Single-axis tracker not following sun,false"
    "SA-007,MASSANGIS-PV-03,MASSANGIS,2025-12-01T12:30:00,GridFrequencyDeviation,Medium,49.4,49.8,Transformer,Grid frequency below nominal band,true"
    "SA-008,CESTAS-PV-05,CESTAS,2025-12-01T11:30:00,HighInverterLoad,Warning,96.0,95.0,Inverter,Inverter load approaching clipping limit,true"
    "SA-009,MARVILLE-PV-04,MARVILLE,2025-12-01T09:30:00,LowIrradianceOutput,Low,120.0,150.0,Sensor,Pyranometer reading lower than adjacent arrays,true"
    "SA-010,TOUL-PV-03,TOUL,2025-12-01T15:00:00,ModuleHotspot,Critical,85.0,80.0,Module,Thermal hotspot detected on module backsheet,false"
    "SA-011,MEES-PV-04,MEES,2025-12-01T13:30:00,DCArcFault,Critical,0.0,1.0,PanelString,DC arc fault protection triggered,false"
    "SA-012,GABARDAN-PV-04,GABARDAN,2025-12-01T10:15:00,TransformerOverTemp,High,92.0,85.0,Transformer,Step-up transformer temperature high,false"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table ArrayAlert with (format='csv') <|`n$($alertData -join "`n")" -Description "Ingesting 12 ArrayAlert rows..." | Out-Null; Write-Host "  [OK] ArrayAlert (12 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] ArrayAlert: $_" -ForegroundColor Yellow }

# -- INGEST EnergyMetric ---------------------------------------------------
Write-Host "`n[Step 4] Ingesting EnergyMetric..." -ForegroundColor Cyan

$energyCsv = Join-Path $DataFolder "FactEnergyProduction.csv"
if (Test-Path $energyCsv) {
    $energyRows = Import-Csv -Path $energyCsv
    $eLines = @()
    foreach ($row in $energyRows) {
        $pid = $arrayLookup[$row.ArrayId]; if (-not $pid) { $pid = "UNKNOWN" }
        $ts = "$($row.Date)T$(([string]$row.Hour).PadLeft(2,'0')):00:00Z"
        $eLines += "$($row.ArrayId),$pid,$ts,$($row.IrradianceWm2),$($row.PowerOutputKW),$($row.PerformanceRatio),$($row.ModuleTempC),$($row.InverterEfficiency),$($row.GridFrequencyHz)"
    }
    for ($i = 0; $i -lt $eLines.Count; $i += 50) {
        $batch = $eLines[$i..([Math]::Min($i + 49, $eLines.Count - 1))]
        try { Invoke-KustoMgmt -Command ".ingest inline into table EnergyMetric with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch { Write-Warning "EnergyMetric batch ingest failed (non-fatal): $($_.Exception.Message)" }
    }
    Write-Host "  [OK] EnergyMetric ($($eLines.Count) rows)" -ForegroundColor Green
} else { Write-Host "  [SKIP] FactEnergyProduction.csv not found" -ForegroundColor Yellow }

# -- INGEST WeatherMetric --------------------------------------------------
Write-Host "`n[Step 5] Ingesting WeatherMetric sample data..." -ForegroundColor Cyan

$weatherData = @(
    "WS-CESTAS,CESTAS,2025-12-01T08:00:00,310,9.0,3.5,72.0,1016.0,45.0,0.0"
    "WS-CESTAS,CESTAS,2025-12-01T10:00:00,680,13.5,4.0,60.0,1015.5,20.0,0.0"
    "WS-CESTAS,CESTAS,2025-12-01T12:00:00,905,18.0,5.0,52.0,1015.0,10.0,0.0"
    "WS-CESTAS,CESTAS,2025-12-01T14:00:00,720,17.0,4.5,55.0,1014.5,15.0,0.0"
    "WS-MARVILLE,MARVILLE,2025-12-01T08:00:00,220,4.0,5.5,80.0,1012.0,70.0,0.2"
    "WS-MARVILLE,MARVILLE,2025-12-01T10:00:00,540,8.5,6.0,68.0,1011.5,40.0,0.0"
    "WS-MARVILLE,MARVILLE,2025-12-01T12:00:00,760,12.0,6.5,58.0,1011.0,25.0,0.0"
    "WS-MARVILLE,MARVILLE,2025-12-01T14:00:00,590,11.0,5.0,62.0,1010.5,30.0,0.0"
    "WS-TOUL,TOUL,2025-12-01T08:00:00,240,3.0,4.0,82.0,1013.0,65.0,0.1"
    "WS-TOUL,TOUL,2025-12-01T10:00:00,560,7.0,4.5,70.0,1012.5,35.0,0.0"
    "WS-TOUL,TOUL,2025-12-01T12:00:00,790,11.0,5.0,60.0,1012.0,20.0,0.0"
    "WS-TOUL,TOUL,2025-12-01T14:00:00,610,10.0,4.0,64.0,1011.5,28.0,0.0"
    "WS-MEES,MEES,2025-12-01T08:00:00,360,11.0,2.5,55.0,1018.0,15.0,0.0"
    "WS-MEES,MEES,2025-12-01T10:00:00,720,16.0,3.0,45.0,1017.5,8.0,0.0"
    "WS-MEES,MEES,2025-12-01T12:00:00,960,21.0,3.5,38.0,1017.0,5.0,0.0"
    "WS-MEES,MEES,2025-12-01T14:00:00,770,20.0,3.0,42.0,1016.5,10.0,0.0"
    "WS-GABARDAN,GABARDAN,2025-12-01T08:00:00,300,8.5,3.0,74.0,1016.5,40.0,0.0"
    "WS-GABARDAN,GABARDAN,2025-12-01T10:00:00,660,13.0,3.5,62.0,1016.0,22.0,0.0"
    "WS-GABARDAN,GABARDAN,2025-12-01T12:00:00,890,17.5,4.0,54.0,1015.5,12.0,0.0"
    "WS-GABARDAN,GABARDAN,2025-12-01T14:00:00,700,16.5,3.5,57.0,1015.0,16.0,0.0"
    "WS-MASSANGIS,MASSANGIS,2025-12-01T08:00:00,280,6.0,4.5,78.0,1014.0,50.0,0.0"
    "WS-MASSANGIS,MASSANGIS,2025-12-01T10:00:00,620,10.5,5.0,66.0,1013.5,30.0,0.0"
    "WS-MASSANGIS,MASSANGIS,2025-12-01T12:00:00,850,15.0,5.5,58.0,1013.0,18.0,0.0"
    "WS-MASSANGIS,MASSANGIS,2025-12-01T14:00:00,660,14.0,4.5,61.0,1012.5,22.0,0.0"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table WeatherMetric with (format='csv') <|`n$($weatherData -join "`n")" -Description "Ingesting 24 WeatherMetric rows..." | Out-Null; Write-Host "  [OK] WeatherMetric (24 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] WeatherMetric: $_" -ForegroundColor Yellow }

# -- INGEST MaintenanceMetric ----------------------------------------------
Write-Host "`n[Step 6] Ingesting MaintenanceMetric..." -ForegroundColor Cyan

$maintCsv = Join-Path $DataFolder "FactMaintenanceEvent.csv"
if (Test-Path $maintCsv) {
    $maintRows = Import-Csv -Path $maintCsv
    $mLines = @()
    foreach ($row in $maintRows) {
        $pid = $arrayLookup[$row.ArrayId]; if (-not $pid) { $pid = "UNKNOWN" }
        $mLines += "$($row.EventId),$($row.ArrayId),$pid,$($row.ScheduledDate),$($row.EventType),$($row.Component),$($row.DurationHours),$($row.CostUSD),$($row.TechnicianId),$($row.Status)"
    }
    for ($i = 0; $i -lt $mLines.Count; $i += 50) {
        $batch = $mLines[$i..([Math]::Min($i + 49, $mLines.Count - 1))]
        try { Invoke-KustoMgmt -Command ".ingest inline into table MaintenanceMetric with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch { Write-Warning "MaintenanceMetric batch ingest failed (non-fatal): $($_.Exception.Message)" }
    }
    Write-Host "  [OK] MaintenanceMetric ($($mLines.Count) rows)" -ForegroundColor Green
} else { Write-Host "  [SKIP] FactMaintenanceEvent.csv not found" -ForegroundColor Yellow }

Write-Host "`n=== Solar Farm KQL Tables Complete ===" -ForegroundColor Cyan
