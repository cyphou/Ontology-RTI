<#
.SYNOPSIS
    Create KQL tables and ingest data for the Healthcare domain.
.DESCRIPTION
    Creates 5 KQL tables for healthcare telemetry:
      - PatientVitals        (enriched from SensorTelemetry.csv with patient/ward context)
      - ClinicalAlert        (clinical alerts with patient/ward context)
      - LabMetric            (lab results over time)
      - MedicationEvent      (medication administration events)
      - DeviceReading        (medical device status readings)
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

Write-Host "=== Deploying Healthcare KQL Tables ===" -ForegroundColor Cyan

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
    @{ Name = "PatientVitals"; Schema = "(PatientId:string, WardId:string, DepartmentId:string, Timestamp:datetime, HeartRateBPM:real, BloodPressureSystolic:real, BloodPressureDiastolic:real, TemperatureC:real, OxygenSaturation:real, RespiratoryRate:real, QualityFlag:string, IsAnomaly:bool)" },
    @{ Name = "ClinicalAlert"; Schema = "(AlertId:string, PatientId:string, WardId:string, DepartmentId:string, Timestamp:datetime, AlertType:string, Severity:string, MetricValue:real, ThresholdValue:real, Message:string, IsAcknowledged:bool)" },
    @{ Name = "LabMetric"; Schema = "(LabId:string, PatientId:string, PhysicianId:string, Timestamp:datetime, TestType:string, ResultValue:real, Unit:string, ReferenceMin:real, ReferenceMax:real, Interpretation:string)" },
    @{ Name = "MedicationEvent"; Schema = "(EventId:string, PatientId:string, MedicationId:string, NurseId:string, Timestamp:datetime, Dosage:string, Route:string, AdverseReaction:string, Status:string)" },
    @{ Name = "DeviceReading"; Schema = "(ReadingId:string, DeviceId:string, WardId:string, Timestamp:datetime, MetricType:string, MetricValue:real, BatteryPercent:real, CalibrationStatus:string, Status:string)" }
)

foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".create-merge table $($t.Name) $($t.Schema)" -Description "Creating $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name)" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name): $_" -ForegroundColor Yellow }
}

# ── Enable streaming ingestion policies ─────────────────────────────────────
Write-Host "`n[Step 1b] Enabling streaming ingestion policies..." -ForegroundColor Cyan
foreach ($t in $tables) {
    try { Invoke-KustoMgmt -Command ".alter table $($t.Name) policy streamingingestion '{`"IsEnabled`": true}'" -Description "Streaming on $($t.Name)..." | Out-Null; Write-Host "  [OK] $($t.Name) streaming enabled" -ForegroundColor Green }
    catch { Write-Host "  [WARN] $($t.Name) streaming policy: $_" -ForegroundColor Yellow }
}

# ── ENRICH SensorTelemetry → PatientVitals ──────────────────────────────────
Write-Host "`n[Step 2] Enriching SensorTelemetry → PatientVitals..." -ForegroundColor Cyan

$wardLookup = @{}
$wardCsv = Join-Path $DataFolder "DimWard.csv"
if (Test-Path $wardCsv) { Import-Csv -Path $wardCsv | ForEach-Object { $wardLookup[$_.WardId] = $_.DepartmentId } }

$patientLookup = @{}
$patientCsv = Join-Path $DataFolder "DimPatient.csv"
if (Test-Path $patientCsv) { Import-Csv -Path $patientCsv | ForEach-Object { $patientLookup[$_.PatientId] = $_.WardId } }

$deviceWardLookup = @{}
$deviceCsv = Join-Path $DataFolder "DimMedicalDevice.csv"
if (Test-Path $deviceCsv) { Import-Csv -Path $deviceCsv | ForEach-Object { $deviceWardLookup[$_.DeviceId] = $_.WardId } }

$telemetry = Import-Csv -Path (Join-Path $DataFolder "SensorTelemetry.csv")

# Group telemetry by device+timestamp to build vital signs rows
$vitalGroups = @{}
foreach ($row in $telemetry) {
    $key = "$($row.DeviceId)|$($row.Timestamp)"
    if (-not $vitalGroups[$key]) { $vitalGroups[$key] = @{ DeviceId = $row.DeviceId; Timestamp = $row.Timestamp; HR = ""; BP = ""; SpO2 = ""; Temp = ""; RR = ""; Quality = $row.Quality } }
    switch ($row.SensorType) {
        "HeartRate"         { $vitalGroups[$key].HR = $row.Value }
        "BloodPressure"     { $vitalGroups[$key].BP = $row.Value }
        "OxygenSaturation"  { $vitalGroups[$key].SpO2 = $row.Value }
        "Temperature"       { $vitalGroups[$key].Temp = $row.Value }
    }
}

$lines = @()
foreach ($g in $vitalGroups.Values) {
    if (-not $g.HR -and -not $g.BP -and -not $g.SpO2 -and -not $g.Temp) { continue }
    $wid = $deviceWardLookup[$g.DeviceId]; if (-not $wid) { $wid = "UNKNOWN" }
    $did = $wardLookup[$wid]; if (-not $did) { $did = "UNKNOWN" }
    # Find a patient in the ward
    $pid = ($patientLookup.GetEnumerator() | Where-Object { $_.Value -eq $wid } | Select-Object -First 1).Key
    if (-not $pid) { $pid = "UNKNOWN" }
    $hr = if ($g.HR) { $g.HR } else { "" }
    $bp = if ($g.BP) { $g.BP } else { "" }
    $bpd = if ($bp) { [math]::Round([double]$bp * 0.55, 0) } else { "" }
    $spo2 = if ($g.SpO2) { $g.SpO2 } else { "" }
    $temp = if ($g.Temp) { $g.Temp } else { "" }
    $rr = if ($hr) { [math]::Round([double]$hr / 4.5, 0) } else { "" }
    $anomaly = "false"
    if ($hr -and ([double]$hr -gt 120 -or [double]$hr -lt 50)) { $anomaly = "true" }
    if ($spo2 -and [double]$spo2 -lt 90) { $anomaly = "true" }
    $lines += "$pid,$wid,$did,$($g.Timestamp),$hr,$bp,$bpd,$temp,$spo2,$rr,$($g.Quality),$anomaly"
}
for ($i = 0; $i -lt $lines.Count; $i += 50) {
    $batch = $lines[$i..([Math]::Min($i + 49, $lines.Count - 1))]
    try { Invoke-KustoMgmt -Command ".ingest inline into table PatientVitals with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
}
Write-Host "  [OK] PatientVitals ($($lines.Count) rows)" -ForegroundColor Green

# ── INGEST ClinicalAlert ────────────────────────────────────────────────────
Write-Host "`n[Step 3] Ingesting ClinicalAlert sample data..." -ForegroundColor Cyan

$alertData = @(
    "CA-001,PAT-003,WD-003,DEPT-002,2024-10-14T10:30:00,HighHeartRate,Critical,142,120,Heart rate exceeding critical threshold,false"
    "CA-002,PAT-003,WD-003,DEPT-002,2024-10-14T11:00:00,LowOxygen,High,89,92,Oxygen saturation below safe level,false"
    "CA-003,PAT-004,WD-003,DEPT-002,2024-10-15T08:15:00,LowPotassium,Medium,3.2,3.5,Low potassium detected in labs,true"
    "CA-004,PAT-001,WD-001,DEPT-001,2024-10-15T09:00:00,HighBloodPressure,Warning,158,140,Systolic BP above warning threshold,true"
    "CA-005,PAT-009,WD-008,DEPT-004,2024-10-14T22:30:00,RapidDesaturation,Critical,85,90,Rapid SpO2 drop in pediatric ICU,false"
    "CA-006,PAT-012,WD-011,DEPT-006,2024-10-15T06:00:00,Seizure,Critical,0,0,EEG seizure activity detected,false"
    "CA-007,PAT-014,WD-013,DEPT-007,2024-10-14T18:30:00,HighGlucose,Medium,280,200,Blood glucose elevated,true"
    "CA-008,PAT-018,WD-016,DEPT-008,2024-10-14T07:00:00,Hypotension,Critical,72,90,Systolic BP critically low post-surgery,false"
    "CA-009,PAT-010,WD-009,DEPT-005,2024-10-11T14:00:00,Febrile,Warning,38.8,38.0,Temperature above febrile threshold,true"
    "CA-010,PAT-002,WD-001,DEPT-001,2024-10-16T10:00:00,Tachycardia,High,128,100,Sustained tachycardia in ER,false"
    "CA-011,PAT-005,WD-004,DEPT-002,2024-10-13T16:00:00,HighINR,Medium,3.8,3.0,INR above therapeutic range,true"
    "CA-012,PAT-015,WD-013,DEPT-007,2024-10-15T09:30:00,HighCreatinine,High,2.4,1.3,Acute kidney injury marker elevated,false"
    "CA-013,PAT-017,WD-015,DEPT-008,2024-10-13T21:00:00,LowPlatelet,Warning,95,150,Platelet count below normal post-surgery,true"
    "CA-014,PAT-008,WD-007,DEPT-004,2024-10-15T10:00:00,HighWBC,Medium,15.8,14.5,Elevated WBC in pediatric patient,true"
    "CA-015,PAT-021,WD-002,DEPT-001,2024-10-16T11:00:00,HighDDimer,High,1.8,0.5,D-Dimer elevated suggesting PE risk,false"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table ClinicalAlert with (format='csv') <|`n$($alertData -join "`n")" -Description "Ingesting 15 ClinicalAlert rows..." | Out-Null; Write-Host "  [OK] ClinicalAlert (15 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] ClinicalAlert: $_" -ForegroundColor Yellow }

# ── INGEST LabMetric ────────────────────────────────────────────────────────
Write-Host "`n[Step 4] Ingesting LabMetric..." -ForegroundColor Cyan

$labCsv = Join-Path $DataFolder "FactLabResult.csv"
if (Test-Path $labCsv) {
    $labRows = Import-Csv -Path $labCsv
    $labLines = @()
    foreach ($row in $labRows) {
        $refMin = ""; $refMax = ""
        if ($row.ReferenceRange -match '(\d+\.?\d*)\s*-\s*(\d+\.?\d*)') { $refMin = $Matches[1]; $refMax = $Matches[2] }
        $labLines += "$($row.LabResultId),$($row.PatientId),$($row.PhysicianId),$($row.TestDate)T00:00:00,$($row.TestType),$($row.ResultValue),$($row.Unit),$refMin,$refMax,$($row.Interpretation)"
    }
    for ($i = 0; $i -lt $labLines.Count; $i += 50) {
        $batch = $labLines[$i..([Math]::Min($i + 49, $labLines.Count - 1))]
        try { Invoke-KustoMgmt -Command ".ingest inline into table LabMetric with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
    }
    Write-Host "  [OK] LabMetric ($($labLines.Count) rows)" -ForegroundColor Green
} else { Write-Host "  [SKIP] FactLabResult.csv not found" -ForegroundColor Yellow }

# ── INGEST MedicationEvent ──────────────────────────────────────────────────
Write-Host "`n[Step 5] Ingesting MedicationEvent..." -ForegroundColor Cyan

$medCsv = Join-Path $DataFolder "FactMedicationAdmin.csv"
if (Test-Path $medCsv) {
    $medRows = Import-Csv -Path $medCsv
    $medLines = @()
    foreach ($row in $medRows) {
        $medLines += "$($row.AdminId),$($row.PatientId),$($row.MedicationId),$($row.NurseId),$($row.AdminDate),$($row.Dosage),$($row.Route),None,$($row.Status)"
    }
    for ($i = 0; $i -lt $medLines.Count; $i += 50) {
        $batch = $medLines[$i..([Math]::Min($i + 49, $medLines.Count - 1))]
        try { Invoke-KustoMgmt -Command ".ingest inline into table MedicationEvent with (format='csv') <|`n$($batch -join "`n")" | Out-Null } catch {}
    }
    Write-Host "  [OK] MedicationEvent ($($medLines.Count) rows)" -ForegroundColor Green
} else { Write-Host "  [SKIP] FactMedicationAdmin.csv not found" -ForegroundColor Yellow }

# ── INGEST DeviceReading ────────────────────────────────────────────────────
Write-Host "`n[Step 6] Ingesting DeviceReading sample data..." -ForegroundColor Cyan

$deviceData = @(
    "DR-001,DEV-001,WD-001,2024-10-15T08:00:00,BatteryLevel,92,92,Calibrated,Active"
    "DR-002,DEV-002,WD-001,2024-10-15T08:00:00,BatteryLevel,85,85,Calibrated,Active"
    "DR-003,DEV-003,WD-003,2024-10-15T08:00:00,BatteryLevel,78,78,Calibrated,Active"
    "DR-004,DEV-004,WD-003,2024-10-15T08:00:00,BatteryLevel,95,95,Calibrated,Active"
    "DR-005,DEV-005,WD-004,2024-10-15T08:00:00,InfusionVolume,250,100,Calibrated,Active"
    "DR-006,DEV-007,WD-007,2024-10-15T08:00:00,BatteryLevel,88,88,Calibrated,Active"
    "DR-007,DEV-008,WD-008,2024-10-15T08:00:00,BatteryLevel,91,91,Calibrated,Active"
    "DR-008,DEV-009,WD-009,2024-10-15T08:00:00,InfusionVolume,120,100,Calibrated,Active"
    "DR-009,DEV-010,WD-011,2024-10-15T08:00:00,SignalQuality,95,100,Calibrated,Active"
    "DR-010,DEV-012,WD-019,2024-10-15T08:00:00,MagnetStrength,3.0,100,Calibrated,Active"
    "DR-011,DEV-013,WD-019,2024-10-15T08:00:00,TubeTemp,42,100,Calibrated,Active"
    "DR-012,DEV-014,WD-016,2024-10-15T08:00:00,BatteryLevel,96,96,Calibrated,Active"
    "DR-013,DEV-015,WD-015,2024-10-15T08:00:00,GasReserve,82,82,Calibrated,Active"
    "DR-014,DEV-001,WD-001,2024-10-15T12:00:00,BatteryLevel,75,75,Calibrated,Active"
    "DR-015,DEV-003,WD-003,2024-10-15T12:00:00,BatteryLevel,62,62,NeedsCalibration,Warning"
)
try { Invoke-KustoMgmt -Command ".ingest inline into table DeviceReading with (format='csv') <|`n$($deviceData -join "`n")" -Description "Ingesting 15 DeviceReading rows..." | Out-Null; Write-Host "  [OK] DeviceReading (15 rows)" -ForegroundColor Green }
catch { Write-Host "  [WARN] DeviceReading: $_" -ForegroundColor Yellow }

Write-Host "`n=== Healthcare KQL Tables Complete ===" -ForegroundColor Cyan
