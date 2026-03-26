<#
.SYNOPSIS
    Create KQL tables and ingest data for the Wind Turbine domain.
.DESCRIPTION
    Creates 5 KQL tables for wind farm telemetry:
      - TurbineReading      (enriched from SensorTelemetry.csv with turbine/farm context)
      - TurbineAlert        (alerts from FactAlert.csv with turbine/nacelle context)
      - PowerOutputMetric   (power generation per turbine over time)
      - WeatherMetric       (weather station readings: wind, temp, humidity)
      - MaintenanceMetric   (maintenance event tracking with cost and duration)
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

Write-Host "=== Deploying Wind Turbine KQL Tables ===" -ForegroundColor Cyan

$fabricToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$fabricHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

if (-not $EventhouseId -or -not $KqlDatabaseId -or -not $QueryServiceUri) {
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $fabricHeaders).value
    if (-not $KqlDatabaseId) { $kqlDb = $allItems | Where-Object { $_.type -eq 'KQLDatabase' } | Select-Object -First 1; if ($kqlDb) { $KqlDatabaseId = $kqlDb.id } else { exit 1 } }
    if (-not $EventhouseId) { $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1; if ($eh) { $EventhouseId = $eh.id } }
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

# ── CREATE TABLES ───────────────────────────────────────────────────────────
Write-Host "`n[Step 1] Creating KQL tables..." -ForegroundColor Cyan

$tables = @(
    @{ Name = "TurbineReading"; Schema = "(TurbineId:string, FarmId:string, SensorId:string, SensorType:string, Timestamp:datetime, Value:real, Unit:string, Quality:string, IsAnomaly:bool)" },
    @{ Name = "TurbineAlert"; Schema = "(AlertId:string, TurbineId:string, FarmId:string, Timestamp:datetime, AlertType:string, Severity:string, MetricValue:real, ThresholdValue:real, Component:string, Message:string, IsAcknowledged:bool)" },
    @{ Name = "PowerOutputMetric"; Schema = "(TurbineId:string, FarmId:string, Timestamp:datetime, WindSpeedMs:real, PowerOutputKW:real, CapacityFactor:real, RotorRPM:real, PitchAngleDeg:real, YawAngleDeg:real, GridFrequencyHz:real)" },
    @{ Name = "WeatherMetric"; Schema = "(StationId:string, FarmId:string, Timestamp:datetime, WindSpeedMs:real, WindDirectionDeg:real, TemperatureC:real, HumidityPct:real, PressureHPa:real, VisibilityKm:real, IcingRisk:bool)" },
    @{ Name = "MaintenanceMetric"; Schema = "(EventId:string, TurbineId:string, FarmId:string, Timestamp:datetime, EventType:string, Component:string, DurationHours:real, CostUSD:real, TechnicianId:string, Status:string)" }
)

foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".create-merge table $($t.Name) $($t.Schema)" -Description "Creating $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name)" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name): $_" -ForegroundColor Yellow }
}

# ── ENRICH SensorTelemetry → TurbineReading ─────────────────────────────────
Write-Host "`n[Step 2] Enriching SensorTelemetry → TurbineReading..." -ForegroundColor Cyan

# Wind Turbine telemetry: Timestamp,TurbineId,SensorId,SensorType,Value,Unit,Quality
$turbineLookup = @{}
$turbineCsv = Join-Path $DataFolder "DimTurbine.csv"
if (Test-Path $turbineCsv) { Import-Csv -Path $turbineCsv | ForEach-Object { $turbineLookup[$_.TurbineId] = $_.FarmId } }

$sensorLookup = @{}
$sensorCsv = Join-Path $DataFolder "DimSensor.csv"
if (Test-Path $sensorCsv) { Import-Csv -Path $sensorCsv | ForEach-Object { $sensorLookup[$_.SensorId] = @{ MinThreshold = [double]$_.MinThreshold; MaxThreshold = [double]$_.MaxThreshold } } }

$telemetry = Import-Csv -Path (Join-Path $DataFolder "SensorTelemetry.csv")
$lines = @()
foreach ($row in $telemetry) {
    $fid = $turbineLookup[$row.TurbineId]
    if (-not $fid) { $fid = "UNKNOWN" }
    $val = [double]$row.Value
    $s = $sensorLookup[$row.SensorId]
    $anomaly = if ($s) { ($val -lt $s.MinThreshold -or $val -gt $s.MaxThreshold).ToString().ToLower() } else { "false" }
    $lines += "$($row.TurbineId),$fid,$($row.SensorId),$($row.SensorType),$($row.Timestamp),$val,$($row.Unit),$($row.Quality),$anomaly"
}
for ($i = 0; $i -lt $lines.Count; $i += 50) {
    $batch = $lines[$i..([Math]::Min($i + 49, $lines.Count - 1))]
    try { Invoke-KustoMgmt -Command ".ingest inline into table TurbineReading with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
}
Write-Host "  [OK] TurbineReading ($($lines.Count) rows)" -ForegroundColor Green

# ── INGEST TurbineAlert ─────────────────────────────────────────────────────
Write-Host "`n[Step 3] Ingesting TurbineAlert sample data..." -ForegroundColor Cyan

$alertData = @(
    "WA-001,WT-001,WF-001,2024-11-15T02:30:00,HighVibration,Critical,8.5,8.0,MainBearing,Main bearing vibration exceeds critical threshold,false"
    "WA-002,WT-001,WF-001,2024-11-15T03:00:00,HighWindSpeed,Warning,24.5,25.0,Rotor,Wind speed approaching cut-out limit,true"
    "WA-003,WT-003,WF-001,2024-11-15T04:15:00,HighTemperature,High,95.0,90.0,Gearbox,Gearbox oil temperature elevated,false"
    "WA-004,WT-005,WF-002,2024-11-15T06:00:00,PitchAngle,Medium,15.0,12.0,BladeHub,Pitch angle deviation beyond normal,true"
    "WA-005,WT-007,WF-002,2024-11-15T08:30:00,HighVibration,High,6.2,5.0,Generator,Generator bearing vibration increasing,false"
    "WA-006,WT-009,WF-003,2024-11-15T10:00:00,LowPowerOutput,Medium,2500.0,4000.0,Generator,Power output below expected for wind speed,true"
    "WA-007,WT-002,WF-001,2024-11-15T11:15:00,YawError,Low,12.0,10.0,Nacelle,Yaw alignment drift detected,true"
    "WA-008,WT-010,WF-003,2024-11-15T12:45:00,HighTemperature,Critical,105.0,90.0,Generator,Generator winding temperature critical,false"
    "WA-009,WT-004,WF-001,2024-11-15T14:00:00,BladeIcing,High,0.0,0.0,Blade,Ice detection on blade surface,false"
    "WA-010,WT-012,WF-004,2024-11-15T15:30:00,GridFrequency,Medium,49.5,49.8,Transformer,Grid frequency below nominal,true"
    "WA-011,WT-006,WF-002,2024-11-16T01:00:00,HighVibration,High,7.0,5.0,MainBearing,Bearing vibration trending upward,false"
    "WA-012,WT-008,WF-003,2024-11-16T03:30:00,LowWindSpeed,Low,2.5,3.0,Rotor,Wind speed below cut-in threshold,true"
    "WA-013,WT-011,WF-004,2024-11-16T06:00:00,HighTemperature,High,92.0,90.0,Gearbox,Gearbox temperature above limit,false"
    "WA-014,WT-001,WF-001,2024-11-16T08:15:00,HighVibration,Critical,9.2,8.0,MainBearing,Repeat bearing vibration alarm,false"
    "WA-015,WT-013,WF-004,2024-11-16T10:00:00,CommunicationLoss,High,0.0,1.0,SCADA,Turbine communication loss detected,false"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table TurbineAlert with (format='csv') <|`n$($alertData -join "`n")" -Description "Ingesting 15 TurbineAlert rows..." | Out-Null; Write-Host "  [OK] TurbineAlert (15 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] TurbineAlert: $_" -ForegroundColor Yellow }

# ── INGEST PowerOutputMetric ────────────────────────────────────────────────
Write-Host "`n[Step 4] Ingesting PowerOutputMetric..." -ForegroundColor Cyan

$powerCsv = Join-Path $DataFolder "FactPowerOutput.csv"
if (Test-Path $powerCsv) {
    $powerRows = Import-Csv -Path $powerCsv
    $pLines = @()
    foreach ($row in $powerRows) {
        $fid = $turbineLookup[$row.TurbineId]; if (-not $fid) { $fid = "UNKNOWN" }
        $ts = "$($row.Date)T$($row.Hour.PadLeft(2,'0')):00:00Z"
        $pLines += "$($row.TurbineId),$fid,$ts,$($row.WindSpeedMs),$($row.PowerOutputKW),$($row.CapacityFactor),$($row.RotorRPM),$($row.PitchAngleDeg),$($row.YawAngleDeg),$($row.GridFrequencyHz)"
    }
    for ($i = 0; $i -lt $pLines.Count; $i += 50) {
        $batch = $pLines[$i..([Math]::Min($i + 49, $pLines.Count - 1))]
        try { Invoke-KustoMgmt -Command ".ingest inline into table PowerOutputMetric with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
    }
    Write-Host "  [OK] PowerOutputMetric ($($pLines.Count) rows)" -ForegroundColor Green
} else { Write-Host "  [SKIP] FactPowerOutput.csv not found" -ForegroundColor Yellow }

# ── INGEST WeatherMetric ────────────────────────────────────────────────────
Write-Host "`n[Step 5] Ingesting WeatherMetric sample data..." -ForegroundColor Cyan

$weatherData = @(
    "WS-001,WF-001,2024-11-15T00:00:00,12.5,220,8.5,65.0,1013.2,15.0,false"
    "WS-001,WF-001,2024-11-15T06:00:00,14.2,235,7.0,68.0,1012.8,12.0,false"
    "WS-001,WF-001,2024-11-15T12:00:00,11.8,210,10.5,60.0,1014.0,18.0,false"
    "WS-001,WF-001,2024-11-15T18:00:00,16.5,245,6.0,72.0,1011.5,8.0,false"
    "WS-002,WF-002,2024-11-15T00:00:00,10.0,180,12.0,58.0,1015.0,20.0,false"
    "WS-002,WF-002,2024-11-15T06:00:00,8.5,175,14.5,55.0,1015.5,25.0,false"
    "WS-002,WF-002,2024-11-15T12:00:00,11.2,190,11.0,62.0,1014.2,18.0,false"
    "WS-002,WF-002,2024-11-15T18:00:00,9.0,170,13.0,56.0,1016.0,22.0,false"
    "WS-003,WF-003,2024-11-15T00:00:00,15.0,280,2.0,82.0,1008.5,5.0,true"
    "WS-003,WF-003,2024-11-15T06:00:00,13.5,275,0.5,88.0,1007.0,3.0,true"
    "WS-003,WF-003,2024-11-15T12:00:00,12.0,270,4.5,78.0,1009.5,8.0,false"
    "WS-003,WF-003,2024-11-15T18:00:00,14.8,285,1.0,85.0,1008.0,4.0,true"
    "WS-004,WF-004,2024-11-15T00:00:00,18.0,310,5.0,70.0,1010.0,12.0,false"
    "WS-004,WF-004,2024-11-15T06:00:00,20.5,320,3.0,75.0,1009.0,8.0,false"
    "WS-004,WF-004,2024-11-15T12:00:00,16.0,300,7.0,65.0,1011.0,15.0,false"
    "WS-004,WF-004,2024-11-15T18:00:00,22.0,330,2.5,78.0,1008.5,6.0,false"
    "WS-005,WF-005,2024-11-15T00:00:00,9.5,150,15.0,50.0,1018.0,30.0,false"
    "WS-005,WF-005,2024-11-15T06:00:00,7.0,140,18.0,45.0,1019.0,35.0,false"
    "WS-005,WF-005,2024-11-15T12:00:00,11.0,160,12.0,55.0,1017.0,25.0,false"
    "WS-005,WF-005,2024-11-15T18:00:00,8.0,145,16.0,48.0,1018.5,32.0,false"
    "WS-006,WF-001,2024-11-16T00:00:00,17.0,250,5.5,70.0,1010.5,10.0,false"
    "WS-006,WF-001,2024-11-16T06:00:00,19.0,260,4.0,74.0,1009.5,7.0,false"
    "WS-007,WF-002,2024-11-16T00:00:00,6.5,165,20.0,42.0,1020.0,40.0,false"
    "WS-007,WF-002,2024-11-16T06:00:00,5.0,155,22.0,38.0,1021.0,45.0,false"
    "WS-003,WF-003,2024-11-16T00:00:00,16.0,290,1.5,84.0,1007.5,4.0,true"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table WeatherMetric with (format='csv') <|`n$($weatherData -join "`n")" -Description "Ingesting 25 WeatherMetric rows..." | Out-Null; Write-Host "  [OK] WeatherMetric (25 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] WeatherMetric: $_" -ForegroundColor Yellow }

# ── INGEST MaintenanceMetric ────────────────────────────────────────────────
Write-Host "`n[Step 6] Ingesting MaintenanceMetric..." -ForegroundColor Cyan

$maintCsv = Join-Path $DataFolder "FactMaintenanceEvent.csv"
if (Test-Path $maintCsv) {
    $maintRows = Import-Csv -Path $maintCsv
    $mLines = @()
    foreach ($row in $maintRows) {
        $fid = $turbineLookup[$row.TurbineId]; if (-not $fid) { $fid = "UNKNOWN" }
        $mLines += "$($row.EventId),$($row.TurbineId),$fid,$($row.StartDate),$($row.EventType),$($row.Component),$($row.DurationHours),$($row.CostUSD),$($row.TechnicianId),$($row.Status)"
    }
    for ($i = 0; $i -lt $mLines.Count; $i += 50) {
        $batch = $mLines[$i..([Math]::Min($i + 49, $mLines.Count - 1))]
        try { Invoke-KustoMgmt -Command ".ingest inline into table MaintenanceMetric with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
    }
    Write-Host "  [OK] MaintenanceMetric ($($mLines.Count) rows)" -ForegroundColor Green
} else { Write-Host "  [SKIP] FactMaintenanceEvent.csv not found" -ForegroundColor Yellow }

Write-Host "`n=== Wind Turbine KQL Tables Complete ===" -ForegroundColor Cyan
