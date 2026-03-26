<#
.SYNOPSIS
    Create KQL tables and ingest data for the Manufacturing Plant domain.
.DESCRIPTION
    Creates 5 KQL tables for plant telemetry:
      - SensorReading       (enriched from SensorTelemetry.csv + DimSensor + DimMachine)
      - PlantAlert          (alerts from FactAlert.csv with machine/line context)
      - ProductionMetric    (production batch throughput and quality)
      - MachineHealth       (machine vibration, temperature, current over time)
      - QualityMetric       (quality check pass/fail rates per product/line)
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

Write-Host "=== Deploying Manufacturing Plant KQL Tables ===" -ForegroundColor Cyan

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
    @{ Name = "SensorReading"; Schema = "(SensorId:string, MachineId:string, LineId:string, PlantId:string, Timestamp:datetime, ReadingValue:real, MeasurementUnit:string, SensorType:string, QualityFlag:string, IsAnomaly:bool)" },
    @{ Name = "PlantAlert"; Schema = "(AlertId:string, SensorId:string, MachineId:string, LineId:string, PlantId:string, Timestamp:datetime, AlertType:string, Severity:string, ReadingValue:real, ThresholdValue:real, Message:string, IsAcknowledged:bool)" },
    @{ Name = "ProductionMetric"; Schema = "(BatchId:string, LineId:string, PlantId:string, ProductId:string, Timestamp:datetime, QuantityProduced:int, DefectRate:real, EnergyUsedKWh:real, CycleTimeMinutes:real, OEEPercent:real)" },
    @{ Name = "MachineHealth"; Schema = "(MachineId:string, LineId:string, PlantId:string, Timestamp:datetime, VibrationMmS:real, TemperatureC:real, CurrentAmps:real, SpeedRPM:real, OilPressureBar:real, Status:string)" },
    @{ Name = "QualityMetric"; Schema = "(CheckId:string, BatchId:string, ProductId:string, LineId:string, Timestamp:datetime, TestType:string, Result:string, MeasuredValue:real, SpecMin:real, SpecMax:real, InspectorId:string)" }
)

foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".create-merge table $($t.Name) $($t.Schema)" -Description "Creating $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name)" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name): $_" -ForegroundColor Yellow }
}

# ── ENRICH SensorTelemetry → SensorReading ──────────────────────────────────
Write-Host "`n[Step 2] Enriching SensorTelemetry → SensorReading..." -ForegroundColor Cyan

$sensorLookup = @{}
Import-Csv -Path (Join-Path $DataFolder "DimSensor.csv") | ForEach-Object {
    $sensorLookup[$_.SensorId] = @{ MachineId = $_.MachineId; SensorType = $_.SensorType; MeasurementUnit = $_.MeasurementUnit; MinRange = [double]$_.MinRange; MaxRange = [double]$_.MaxRange }
}
$machineLookup = @{}
Import-Csv -Path (Join-Path $DataFolder "DimMachine.csv") | ForEach-Object { $machineLookup[$_.MachineId] = @{ LineId = $_.LineId; PlantId = $_.PlantId } }

$telemetry = Import-Csv -Path (Join-Path $DataFolder "SensorTelemetry.csv")
$lines = @()
foreach ($row in $telemetry) {
    $s = $sensorLookup[$row.SensorId]; if (-not $s) { continue }
    $m = $machineLookup[$s.MachineId]
    $lid = if ($m) { $m.LineId } else { "UNKNOWN" }; $pid = if ($m) { $m.PlantId } else { "UNKNOWN" }
    $val = [double]$row.ReadingValue; $anomaly = ($val -lt $s.MinRange -or $val -gt $s.MaxRange).ToString().ToLower()
    $lines += "$($row.SensorId),$($s.MachineId),$lid,$pid,$($row.Timestamp),$val,$($s.MeasurementUnit),$($s.SensorType),$($row.QualityFlag),$anomaly"
}
for ($i = 0; $i -lt $lines.Count; $i += 50) {
    $batch = $lines[$i..([Math]::Min($i + 49, $lines.Count - 1))]
    try { Invoke-KustoMgmt -Command ".ingest inline into table SensorReading with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
}
Write-Host "  [OK] SensorReading ($($lines.Count) rows)" -ForegroundColor Green

# ── INGEST PlantAlert ───────────────────────────────────────────────────────
Write-Host "`n[Step 3] Ingesting PlantAlert sample data..." -ForegroundColor Cyan

$alertData = @(
    "ALR-001,SNS-001,MCH-001,LN-001,PLT-001,2024-10-01T08:30:00,HighTemperature,Critical,1420.0,1400.0,Welder A1 temperature above max limit,false"
    "ALR-002,SNS-002,MCH-001,LN-001,PLT-001,2024-10-01T09:15:00,HighCurrent,High,185.0,180.0,Welder A1 current draw excessive,false"
    "ALR-003,SNS-004,MCH-003,LN-001,PLT-001,2024-10-01T10:00:00,HighSpeed,Medium,3200.0,3000.0,Lathe C1 spindle overspeed,true"
    "ALR-004,SNS-005,MCH-004,LN-002,PLT-001,2024-10-01T11:30:00,HighForce,Critical,1850.0,1800.0,Press B1 force above hydraulic limit,false"
    "ALR-005,SNS-007,MCH-005,LN-002,PLT-001,2024-10-01T12:45:00,HighTemperature,High,92.0,85.0,Assembly line humidity too high,false"
    "ALR-006,SNS-009,MCH-007,LN-003,PLT-002,2024-10-01T14:00:00,HighVibration,Medium,6.5,5.0,CNC D2 excessive vibration,true"
    "ALR-007,SNS-010,MCH-008,LN-003,PLT-002,2024-10-01T15:15:00,HighTemperature,High,210.0,200.0,Injection Molder E1 barrel overtemp,false"
    "ALR-008,SNS-012,MCH-009,LN-004,PLT-002,2024-10-01T16:30:00,HighCurrent,Medium,95.0,90.0,Conveyor F1 motor current high,true"
    "ALR-009,SNS-014,MCH-011,LN-004,PLT-003,2024-10-02T08:00:00,HighTemperature,Critical,1550.0,1500.0,Furnace G1 extreme temperature,false"
    "ALR-010,SNS-016,MCH-013,LN-005,PLT-003,2024-10-02T09:30:00,LowSpeed,Low,800.0,1000.0,Robot H2 below minimum speed,true"
    "ALR-011,SNS-003,MCH-002,LN-001,PLT-001,2024-10-02T10:15:00,HighSpeed,Medium,2800.0,2500.0,Welder A2 wire feed speed high,true"
    "ALR-012,SNS-017,MCH-014,LN-005,PLT-003,2024-10-02T11:00:00,HighVibration,High,7.2,5.0,Grinder I1 bearing vibration critical,false"
    "ALR-013,SNS-019,MCH-016,LN-006,PLT-004,2024-10-02T13:00:00,HighTemperature,Medium,75.0,70.0,Paint booth J1 temperature elevated,true"
    "ALR-014,SNS-020,MCH-017,LN-006,PLT-004,2024-10-02T14:30:00,HighHumidity,High,82.0,75.0,Drying oven K1 humidity too high,false"
    "ALR-015,SNS-001,MCH-001,LN-001,PLT-001,2024-10-02T16:00:00,HighTemperature,High,1410.0,1400.0,Welder A1 repeat temperature alert,false"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table PlantAlert with (format='csv') <|`n$($alertData -join "`n")" -Description "Ingesting 15 PlantAlert rows..." | Out-Null; Write-Host "  [OK] PlantAlert (15 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] PlantAlert: $_" -ForegroundColor Yellow }

# ── INGEST ProductionMetric ─────────────────────────────────────────────────
Write-Host "`n[Step 4] Ingesting ProductionMetric sample data..." -ForegroundColor Cyan

$prodData = @(
    "BAT-001,LN-001,PLT-001,PRD-002,2024-10-01T06:00:00,85,0.012,1200,45.0,87.5"
    "BAT-002,LN-001,PLT-001,PRD-001,2024-10-01T14:00:00,120,0.008,1650,38.0,92.0"
    "BAT-003,LN-002,PLT-001,PRD-003,2024-10-01T06:00:00,200,0.025,800,22.0,78.5"
    "BAT-004,LN-002,PLT-001,PRD-003,2024-10-01T14:00:00,195,0.022,790,23.0,80.0"
    "BAT-005,LN-003,PLT-002,PRD-004,2024-10-01T06:00:00,50,0.040,2100,65.0,72.0"
    "BAT-006,LN-003,PLT-002,PRD-005,2024-10-01T14:00:00,75,0.018,1800,52.0,85.0"
    "BAT-007,LN-004,PLT-002,PRD-002,2024-10-01T06:00:00,300,0.005,950,18.0,95.0"
    "BAT-008,LN-004,PLT-002,PRD-006,2024-10-01T14:00:00,180,0.015,1100,32.0,88.0"
    "BAT-009,LN-005,PLT-003,PRD-001,2024-10-01T06:00:00,60,0.010,2400,72.0,90.0"
    "BAT-010,LN-005,PLT-003,PRD-007,2024-10-01T14:00:00,45,0.035,2800,85.0,75.0"
    "BAT-011,LN-006,PLT-004,PRD-004,2024-10-01T06:00:00,150,0.020,1400,35.0,82.0"
    "BAT-012,LN-006,PLT-004,PRD-008,2024-10-01T14:00:00,90,0.028,1600,48.0,79.0"
    "BAT-013,LN-001,PLT-001,PRD-002,2024-10-02T06:00:00,88,0.010,1180,44.0,89.0"
    "BAT-014,LN-002,PLT-001,PRD-003,2024-10-02T06:00:00,210,0.018,820,21.0,83.0"
    "BAT-015,LN-003,PLT-002,PRD-004,2024-10-02T06:00:00,55,0.032,2050,62.0,74.0"
    "BAT-016,LN-004,PLT-002,PRD-002,2024-10-02T06:00:00,310,0.004,940,17.5,96.0"
    "BAT-017,LN-005,PLT-003,PRD-001,2024-10-02T06:00:00,65,0.008,2350,70.0,91.5"
    "BAT-018,LN-006,PLT-004,PRD-004,2024-10-02T06:00:00,155,0.016,1380,34.0,84.0"
    "BAT-019,LN-001,PLT-001,PRD-001,2024-10-02T14:00:00,115,0.009,1620,39.0,91.0"
    "BAT-020,LN-003,PLT-002,PRD-005,2024-10-02T14:00:00,80,0.014,1750,50.0,86.5"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table ProductionMetric with (format='csv') <|`n$($prodData -join "`n")" -Description "Ingesting 20 ProductionMetric rows..." | Out-Null; Write-Host "  [OK] ProductionMetric (20 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] ProductionMetric: $_" -ForegroundColor Yellow }

# ── INGEST MachineHealth ────────────────────────────────────────────────────
Write-Host "`n[Step 5] Ingesting MachineHealth sample data..." -ForegroundColor Cyan

$healthData = @(
    "MCH-001,LN-001,PLT-001,2024-10-01T08:00:00,1.2,850.0,165.0,0,45.0,Running"
    "MCH-001,LN-001,PLT-001,2024-10-01T12:00:00,1.5,870.0,172.0,0,44.5,Running"
    "MCH-001,LN-001,PLT-001,2024-10-01T16:00:00,2.1,910.0,180.0,0,43.0,Warning"
    "MCH-002,LN-001,PLT-001,2024-10-01T08:00:00,0.8,420.0,95.0,1800,0,Running"
    "MCH-002,LN-001,PLT-001,2024-10-01T12:00:00,0.9,435.0,98.0,1850,0,Running"
    "MCH-003,LN-001,PLT-001,2024-10-01T08:00:00,0.5,55.0,12.0,2400,0,Running"
    "MCH-003,LN-001,PLT-001,2024-10-01T12:00:00,0.6,58.0,13.0,2500,0,Running"
    "MCH-004,LN-002,PLT-001,2024-10-01T08:00:00,2.0,65.0,210.0,0,180.0,Running"
    "MCH-004,LN-002,PLT-001,2024-10-01T12:00:00,2.3,68.0,220.0,0,175.0,Running"
    "MCH-005,LN-002,PLT-001,2024-10-01T08:00:00,0.3,28.0,8.0,0,0,Running"
    "MCH-005,LN-002,PLT-001,2024-10-01T12:00:00,0.4,30.0,9.0,0,0,Running"
    "MCH-007,LN-003,PLT-002,2024-10-01T08:00:00,1.8,45.0,18.0,4500,0,Running"
    "MCH-007,LN-003,PLT-002,2024-10-01T12:00:00,3.5,52.0,22.0,4800,0,Warning"
    "MCH-008,LN-003,PLT-002,2024-10-01T08:00:00,0.6,195.0,85.0,0,120.0,Running"
    "MCH-008,LN-003,PLT-002,2024-10-01T12:00:00,0.8,205.0,90.0,0,118.0,Running"
    "MCH-009,LN-004,PLT-002,2024-10-01T08:00:00,0.4,42.0,82.0,60,0,Running"
    "MCH-009,LN-004,PLT-002,2024-10-01T12:00:00,0.5,45.0,88.0,62,0,Running"
    "MCH-011,LN-004,PLT-003,2024-10-01T08:00:00,1.0,1480.0,320.0,0,25.0,Running"
    "MCH-011,LN-004,PLT-003,2024-10-01T12:00:00,1.2,1520.0,340.0,0,24.0,Running"
    "MCH-013,LN-005,PLT-003,2024-10-01T08:00:00,0.2,35.0,15.0,0,0,Running"
    "MCH-013,LN-005,PLT-003,2024-10-01T12:00:00,0.3,38.0,16.0,0,0,Running"
    "MCH-014,LN-005,PLT-003,2024-10-01T08:00:00,3.0,55.0,25.0,3600,0,Running"
    "MCH-014,LN-005,PLT-003,2024-10-01T12:00:00,5.8,62.0,28.0,3800,0,Critical"
    "MCH-016,LN-006,PLT-004,2024-10-01T08:00:00,0.2,68.0,12.0,0,0,Running"
    "MCH-016,LN-006,PLT-004,2024-10-01T12:00:00,0.3,72.0,13.0,0,0,Running"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table MachineHealth with (format='csv') <|`n$($healthData -join "`n")" -Description "Ingesting 25 MachineHealth rows..." | Out-Null; Write-Host "  [OK] MachineHealth (25 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] MachineHealth: $_" -ForegroundColor Yellow }

# ── INGEST QualityMetric ────────────────────────────────────────────────────
Write-Host "`n[Step 6] Ingesting QualityMetric sample data..." -ForegroundColor Cyan

$qualityData = @(
    "QC-001,BAT-001,PRD-002,LN-001,2024-10-01T07:00:00,Dimensional,Pass,25.02,24.95,25.05,OP-003"
    "QC-002,BAT-001,PRD-002,LN-001,2024-10-01T07:05:00,Tensile,Pass,520.0,500.0,550.0,OP-003"
    "QC-003,BAT-002,PRD-001,LN-001,2024-10-01T15:00:00,Dimensional,Pass,12.48,12.45,12.55,OP-004"
    "QC-004,BAT-002,PRD-001,LN-001,2024-10-01T15:05:00,Surface,Pass,0.8,0,1.6,OP-004"
    "QC-005,BAT-003,PRD-003,LN-002,2024-10-01T07:00:00,Weight,Pass,0.52,0.48,0.55,OP-005"
    "QC-006,BAT-003,PRD-003,LN-002,2024-10-01T07:05:00,Dimensional,Fail,8.62,8.45,8.55,OP-005"
    "QC-007,BAT-005,PRD-004,LN-003,2024-10-01T07:00:00,Porosity,Pass,0.5,0,2.0,OP-007"
    "QC-008,BAT-005,PRD-004,LN-003,2024-10-01T07:05:00,Hardness,Pass,62.0,58.0,65.0,OP-007"
    "QC-009,BAT-007,PRD-002,LN-004,2024-10-01T07:00:00,Dimensional,Pass,25.01,24.95,25.05,OP-009"
    "QC-010,BAT-009,PRD-001,LN-005,2024-10-01T07:00:00,Tensile,Fail,485.0,500.0,550.0,OP-010"
    "QC-011,BAT-009,PRD-001,LN-005,2024-10-01T07:05:00,Dimensional,Pass,12.50,12.45,12.55,OP-010"
    "QC-012,BAT-011,PRD-004,LN-006,2024-10-01T07:00:00,Hardness,Pass,61.0,58.0,65.0,OP-012"
    "QC-013,BAT-013,PRD-002,LN-001,2024-10-02T07:00:00,Dimensional,Pass,25.00,24.95,25.05,OP-003"
    "QC-014,BAT-013,PRD-002,LN-001,2024-10-02T07:05:00,Tensile,Pass,530.0,500.0,550.0,OP-003"
    "QC-015,BAT-015,PRD-004,LN-003,2024-10-02T07:00:00,Porosity,Fail,2.5,0,2.0,OP-007"
    "QC-016,BAT-016,PRD-002,LN-004,2024-10-02T07:00:00,Dimensional,Pass,24.98,24.95,25.05,OP-009"
    "QC-017,BAT-017,PRD-001,LN-005,2024-10-02T07:00:00,Tensile,Pass,515.0,500.0,550.0,OP-010"
    "QC-018,BAT-018,PRD-004,LN-006,2024-10-02T07:00:00,Hardness,Pass,60.0,58.0,65.0,OP-012"
    "QC-019,BAT-014,PRD-003,LN-002,2024-10-02T07:00:00,Weight,Pass,0.51,0.48,0.55,OP-005"
    "QC-020,BAT-019,PRD-001,LN-001,2024-10-02T15:00:00,Surface,Pass,0.6,0,1.6,OP-004"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table QualityMetric with (format='csv') <|`n$($qualityData -join "`n")" -Description "Ingesting 20 QualityMetric rows..." | Out-Null; Write-Host "  [OK] QualityMetric (20 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] QualityMetric: $_" -ForegroundColor Yellow }

Write-Host "`n=== Manufacturing Plant KQL Tables Complete ===" -ForegroundColor Cyan
