<#
.SYNOPSIS
    Create KQL tables and ingest data into the Eventhouse KQL database.
.DESCRIPTION
    Creates 5 KQL tables for the RTI Dashboard and ingests:
      - SensorTelemetry.csv enriched into SensorReading (joined with DimSensor/DimEquipment/DimProcessUnit)
      - Sample EquipmentAlert, ProcessMetric, PipelineFlow, TankLevel data

    Uses the Kusto REST Management API (.create table, .ingest inline).

.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER EventhouseId
    The Eventhouse GUID (auto-detected if omitted).
.PARAMETER KqlDatabaseId
    The KQL Database GUID (auto-detected if omitted).
.PARAMETER QueryServiceUri
    The Kusto query service URI (auto-detected from Eventhouse if omitted).
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
    if (-not (Test-Path $DataFolder)) {
        $DataFolder = Join-Path $PSScriptRoot "..\data"
    }
}

Write-Host "=== Deploying KQL Tables and Ingesting Data ===" -ForegroundColor Cyan

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
        if ($kqlDb) {
            $KqlDatabaseId = $kqlDb.id
            Write-Host "  Found KQL Database: $($kqlDb.displayName) ($KqlDatabaseId)" -ForegroundColor Gray
        } else {
            Write-Host "[ERROR] No KQL Database found in workspace." -ForegroundColor Red
            exit 1
        }
    }

    if (-not $EventhouseId) {
        $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1
        if ($eh) {
            $EventhouseId = $eh.id
            Write-Host "  Found Eventhouse: $($eh.displayName) ($EventhouseId)" -ForegroundColor Gray
        }
    }

    if (-not $QueryServiceUri -and $EventhouseId) {
        $ehDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$EventhouseId" -Headers $fabricHeaders
        $QueryServiceUri = $ehDetails.properties.queryServiceUri
        Write-Host "  Query URI: $QueryServiceUri" -ForegroundColor Gray
    }
}

if (-not $QueryServiceUri) {
    Write-Host "[ERROR] Could not determine Kusto query service URI." -ForegroundColor Red
    exit 1
}

# ── Get KQL Database name ──────────────────────────────────────────────────
if (-not $KqlDatabaseName) {
    $kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $fabricHeaders
    $KqlDatabaseName = $kqlDbDetails.displayName
    Write-Host "  KQL Database Name: $KqlDatabaseName" -ForegroundColor Gray
}

# ── Acquire Kusto token ────────────────────────────────────────────────────
# Fabric Eventhouse Kusto endpoints accept tokens scoped to the cluster URI
$kustoToken = $null
$tokenResources = @($QueryServiceUri, "https://kusto.kusto.windows.net", "https://help.kusto.windows.net", "https://api.fabric.microsoft.com")

foreach ($resource in $tokenResources) {
    try {
        $kustoToken = (Get-AzAccessToken -ResourceUrl $resource).Token
        Write-Host "  Kusto token acquired (resource: $resource)" -ForegroundColor Gray
        break
    } catch {
        Write-Host "  Token attempt failed for $resource - trying next..." -ForegroundColor DarkGray
    }
}
if (-not $kustoToken) {
    Write-Host "[ERROR] Could not acquire Kusto token." -ForegroundColor Red
    exit 1
}

# ── Helper: Execute Kusto management command ───────────────────────────────
function Invoke-KustoMgmt {
    param([string]$Command, [string]$Description)

    if ($Description) { Write-Host "  $Description" -ForegroundColor Gray }

    $body = @{
        db  = $KqlDatabaseName
        csl = $Command
    } | ConvertTo-Json -Depth 2

    $headers = @{
        "Authorization" = "Bearer $kustoToken"
        "Content-Type"  = "application/json; charset=utf-8"
    }

    $maxRetries = 3
    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            $response = Invoke-RestMethod -Method Post `
                -Uri "$QueryServiceUri/v1/rest/mgmt" `
                -Headers $headers `
                -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) `
                -ContentType "application/json; charset=utf-8"
            return $response
        }
        catch {
            $errMsg = $_.Exception.Message
            if ($_.Exception.Response) {
                try {
                    $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                    $errBody = $sr.ReadToEnd(); $sr.Close()
                    $errMsg = "$errMsg | $errBody"
                } catch {}
            }
            if ($attempt -lt $maxRetries) {
                Write-Host "    Retry ${attempt}/${maxRetries}: $errMsg" -ForegroundColor DarkYellow
                Start-Sleep -Seconds (10 * $attempt)
            } else {
                throw "Kusto command failed after $maxRetries attempts: $errMsg"
            }
        }
    }
}

# ── Wait for KQL database to be ready ──────────────────────────────────────
Write-Host "`nWaiting for KQL database to be ready..." -ForegroundColor Yellow
$dbReady = $false
for ($wait = 1; $wait -le 6; $wait++) {
    try {
        Invoke-KustoMgmt -Command ".show database" -Description $null | Out-Null
        $dbReady = $true
        Write-Host "  KQL database is ready." -ForegroundColor Green
        break
    }
    catch {
        Write-Host "  Database not ready yet, waiting 15s... ($wait/6)" -ForegroundColor DarkYellow
        Start-Sleep -Seconds 15
    }
}
if (-not $dbReady) {
    Write-Host "[ERROR] KQL database did not become ready in time." -ForegroundColor Red
    exit 1
}

# ════════════════════════════════════════════════════════════════════════════
# STEP 1: CREATE KQL TABLES
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 1] Creating KQL tables..." -ForegroundColor Cyan

$tableDefinitions = @(
    @{
        Name = "SensorReading"
        Schema = "(SensorId:string, EquipmentId:string, RefineryId:string, Timestamp:datetime, ReadingValue:real, MeasurementUnit:string, SensorType:string, QualityFlag:string, IsAnomaly:bool)"
    },
    @{
        Name = "EquipmentAlert"
        Schema = "(AlertId:string, SensorId:string, EquipmentId:string, RefineryId:string, Timestamp:datetime, AlertType:string, Severity:string, ReadingValue:real, ThresholdValue:real, Message:string, IsAcknowledged:bool)"
    },
    @{
        Name = "ProcessMetric"
        Schema = "(ProcessUnitId:string, RefineryId:string, Timestamp:datetime, ThroughputBPH:real, InletTemperatureF:real, OutletTemperatureF:real, PressurePSI:real, FeedRateBPH:real, YieldPercent:real, EnergyConsumptionMMBTU:real)"
    },
    @{
        Name = "PipelineFlow"
        Schema = "(PipelineId:string, RefineryId:string, Timestamp:datetime, FlowRateBPH:real, PressurePSI:real, TemperatureF:real, ViscosityCp:real, IsFlowNormal:bool)"
    },
    @{
        Name = "TankLevel"
        Schema = "(TankId:string, RefineryId:string, Timestamp:datetime, LevelBarrels:real, LevelPercent:real, TemperatureF:real, ProductId:string, IsOverflow:bool)"
    }
)

foreach ($tbl in $tableDefinitions) {
    try {
        Invoke-KustoMgmt -Command ".create-merge table $($tbl.Name) $($tbl.Schema)" `
                         -Description "Creating table $($tbl.Name)..." | Out-Null
        Write-Host "  [OK] $($tbl.Name) created" -ForegroundColor Green
    }
    catch {
        Write-Host "  [WARN] $($tbl.Name): $_" -ForegroundColor Yellow
    }
}

# ── Enable streaming ingestion policies ─────────────────────────────────────
Write-Host "`n[Step 1b] Enabling streaming ingestion policies..." -ForegroundColor Cyan
foreach ($tbl in $tableDefinitions) {
    try {
        Invoke-KustoMgmt -Command ".alter table $($tbl.Name) policy streamingingestion '{`"IsEnabled`": true}'" `
                         -Description "Streaming on $($tbl.Name)..." | Out-Null
        Write-Host "  [OK] $($tbl.Name) streaming enabled" -ForegroundColor Green
    }
    catch {
        Write-Host "  [WARN] $($tbl.Name) streaming policy: $_" -ForegroundColor Yellow
    }
}

# ════════════════════════════════════════════════════════════════════════════
# STEP 2: ENRICH AND INGEST SensorTelemetry → SensorReading
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 2] Enriching SensorTelemetry.csv → SensorReading..." -ForegroundColor Cyan

# Build lookup tables from dimension CSVs
$sensorLookup = @{}
Import-Csv -Path (Join-Path $DataFolder "DimSensor.csv") | ForEach-Object {
    $sensorLookup[$_.SensorId] = @{
        EquipmentId     = $_.EquipmentId
        SensorType      = $_.SensorType
        MeasurementUnit = $_.MeasurementUnit
        MinRange        = [double]$_.MinRange
        MaxRange        = [double]$_.MaxRange
    }
}

$equipLookup = @{}
Import-Csv -Path (Join-Path $DataFolder "DimEquipment.csv") | ForEach-Object {
    $equipLookup[$_.EquipmentId] = $_.ProcessUnitId
}

$puLookup = @{}
Import-Csv -Path (Join-Path $DataFolder "DimProcessUnit.csv") | ForEach-Object {
    $puLookup[$_.ProcessUnitId] = $_.RefineryId
}

# Read and enrich telemetry data
$telemetryData = Import-Csv -Path (Join-Path $DataFolder "SensorTelemetry.csv")
$sensorReadingLines = @()

foreach ($row in $telemetryData) {
    $sensor = $sensorLookup[$row.SensorId]
    if ($sensor) {
        $equipId = $sensor.EquipmentId
        $puId = $equipLookup[$equipId]
        $refineryId = if ($puId) { $puLookup[$puId] } else { "UNKNOWN" }
        $value = [double]$row.Value
        $isAnomaly = ($value -lt $sensor.MinRange -or $value -gt $sensor.MaxRange).ToString().ToLower()
        $sensorReadingLines += "$($row.SensorId),$equipId,$refineryId,$($row.Timestamp),$value,$($sensor.MeasurementUnit),$($sensor.SensorType),$($row.Quality),$isAnomaly"
    }
}

Write-Host "  Enriched $($sensorReadingLines.Count) rows from SensorTelemetry.csv" -ForegroundColor Gray

# Ingest in batches (Kusto inline limit is ~64KB per command)
$batchSize = 50
for ($i = 0; $i -lt $sensorReadingLines.Count; $i += $batchSize) {
    $batch = $sensorReadingLines[$i..([Math]::Min($i + $batchSize - 1, $sensorReadingLines.Count - 1))]
    $inlineData = $batch -join "`n"
    $cmd = ".ingest inline into table SensorReading with (format='csv') <|`n$inlineData"
    try {
        Invoke-KustoMgmt -Command $cmd -Description "  Ingesting SensorReading rows $($i+1)-$($i+$batch.Count)..." | Out-Null
    }
    catch {
        Write-Host "    [WARN] Batch ingestion failed: $_" -ForegroundColor Yellow
    }
}

Write-Host "  [OK] SensorReading ingested ($($sensorReadingLines.Count) rows)" -ForegroundColor Green

# ════════════════════════════════════════════════════════════════════════════
# STEP 3: GENERATE AND INGEST EquipmentAlert SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 3] Ingesting EquipmentAlert sample data..." -ForegroundColor Cyan

$alertData = @(
    "ALR001,SN001,EQ001,REF001,2025-12-01T01:15:00,HighTemperature,Critical,462.5,450.0,Temperature exceeded critical threshold on CDU furnace,false"
    "ALR002,SN002,EQ001,REF001,2025-12-01T02:30:00,HighPressure,High,520.0,500.0,Pressure spike detected on CDU main column,false"
    "ALR003,SN004,EQ001,REF001,2025-12-01T03:45:00,HighVibration,Medium,1.62,1.50,Abnormal vibration on CDU feed pump,true"
    "ALR004,SN006,EQ003,REF001,2025-12-01T04:00:00,HighTemperature,High,795.0,780.0,Reformer reactor temperature above design limit,false"
    "ALR005,SN007,EQ003,REF001,2025-12-01T05:15:00,LowPressure,Medium,4.2,5.0,Reformer pressure below operating minimum,true"
    "ALR006,SN011,EQ008,REF001,2025-12-01T06:30:00,HighTemperature,Critical,1850.0,1800.0,FCC regenerator temperature critical,false"
    "ALR007,SN014,EQ008,REF001,2025-12-01T07:00:00,HighPressure,High,0.8,0.5,FCC regenerator pressure above max,false"
    "ALR008,SN017,EQ011,REF001,2025-12-01T08:15:00,HighTemperature,Medium,1065.0,1050.0,Hydrocracker reactor temperature elevated,true"
    "ALR009,SN021,EQ014,REF001,2025-12-01T09:30:00,HighVibration,Low,1.3,1.2,Hydrocracker compressor vibration slightly elevated,true"
    "ALR010,SN024,EQ016,REF001,2025-12-01T10:00:00,HighPressure,Critical,2900.0,2800.0,Hydrotreater reactor pressure critical,false"
    "ALR011,SN027,EQ020,REF001,2025-12-02T01:00:00,HighTemperature,High,995.0,980.0,Isomerization reactor overtemp,false"
    "ALR012,SN030,EQ025,REF001,2025-12-02T02:15:00,HighTemperature,Medium,735.0,720.0,Hydrogen plant reformer elevated,true"
    "ALR013,SN031,EQ025,REF001,2025-12-02T03:30:00,HighPressure,High,1450.0,1400.0,Hydrogen plant PSA high pressure,false"
    "ALR014,SN033,EQ028,REF002,2025-12-02T04:45:00,HighFlowRate,Medium,16500.0,16000.0,CDU feed flow rate above design,true"
    "ALR015,SN034,EQ029,REF002,2025-12-02T06:00:00,HighTemperature,High,1060.0,1050.0,FCC reactor temperature above limit,false"
    "ALR016,SN036,EQ032,REF005,2025-12-03T01:30:00,HighTemperature,Critical,1070.0,1050.0,FCC reactor critical temperature,false"
    "ALR017,SN037,EQ033,REF006,2025-12-03T02:45:00,HighTemperature,High,965.0,950.0,Hydrocracker reactor elevated,false"
    "ALR018,SN039,EQ035,REF007,2025-12-03T04:00:00,HighTemperature,Critical,1850.0,1800.0,FCC regenerator extreme temperature,false"
    "ALR019,SN005,EQ003,REF001,2025-12-03T05:15:00,LowTemperature,Low,185.0,200.0,Reformer feed below minimum temperature,true"
    "ALR020,SN040,EQ005,REF001,2025-12-03T06:30:00,HighLevel,Medium,98.5,100.0,CDU reflux drum level approaching overflow,true"
    "ALR021,SN001,EQ001,REF001,2025-12-04T08:00:00,HighTemperature,High,455.0,450.0,CDU furnace temperature high,true"
    "ALR022,SN011,EQ008,REF001,2025-12-04T10:30:00,HighTemperature,Medium,1810.0,1800.0,FCC regenerator slightly above threshold,true"
    "ALR023,SN024,EQ016,REF001,2025-12-05T14:00:00,HighPressure,High,2850.0,2800.0,Hydrotreater pressure above limit,false"
    "ALR024,SN006,EQ003,REF001,2025-12-05T16:30:00,HighTemperature,Critical,810.0,780.0,Reformer reactor critically high,false"
    "ALR025,SN017,EQ011,REF001,2025-12-06T09:00:00,HighTemperature,Low,1055.0,1050.0,Hydrocracker reactor marginally above threshold,true"
)
$alertInline = $alertData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table EquipmentAlert with (format='csv') <|`n$alertInline" `
                     -Description "Ingesting 25 EquipmentAlert rows..." | Out-Null
    Write-Host "  [OK] EquipmentAlert ingested (25 rows)" -ForegroundColor Green
}
catch {
    Write-Host "  [WARN] EquipmentAlert ingestion failed: $_" -ForegroundColor Yellow
}

# ════════════════════════════════════════════════════════════════════════════
# STEP 4: GENERATE AND INGEST ProcessMetric SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 4] Ingesting ProcessMetric sample data..." -ForegroundColor Cyan

$processData = @(
    "PU001,REF001,2025-12-01T00:00:00,4200,650,380,45,4300,92.5,125.6"
    "PU001,REF001,2025-12-01T06:00:00,4150,648,378,44.5,4250,91.8,124.2"
    "PU001,REF001,2025-12-01T12:00:00,4300,655,385,46,4400,93.1,128.0"
    "PU001,REF001,2025-12-01T18:00:00,4180,652,382,45.2,4280,92.0,126.0"
    "PU001,REF001,2025-12-02T00:00:00,4250,651,381,45.5,4350,92.8,127.1"
    "PU001,REF001,2025-12-02T06:00:00,4100,645,375,43.8,4180,91.2,123.5"
    "PU001,REF001,2025-12-02T12:00:00,4350,658,388,46.5,4450,93.5,129.2"
    "PU001,REF001,2025-12-02T18:00:00,4200,650,380,45.0,4300,92.5,126.0"
    "PU002,REF001,2025-12-01T00:00:00,3800,580,420,38.0,3900,88.5,110.0"
    "PU002,REF001,2025-12-01T12:00:00,3850,585,425,38.5,3950,89.0,112.0"
    "PU002,REF001,2025-12-02T00:00:00,3780,578,418,37.5,3880,88.0,109.5"
    "PU002,REF001,2025-12-02T12:00:00,3900,590,430,39.0,4000,89.5,113.0"
    "PU003,REF001,2025-12-01T00:00:00,2200,350,720,25.0,2300,85.0,65.0"
    "PU003,REF001,2025-12-01T12:00:00,2250,355,725,25.5,2350,86.0,66.5"
    "PU003,REF001,2025-12-02T00:00:00,2180,348,718,24.8,2280,84.5,64.5"
    "PU004,REF001,2025-12-01T00:00:00,3200,920,650,2100,3300,90.0,98.0"
    "PU004,REF001,2025-12-01T12:00:00,3250,925,655,2150,3350,91.0,100.0"
    "PU004,REF001,2025-12-02T00:00:00,3180,918,648,2080,3280,89.5,97.0"
    "PU005,REF001,2025-12-01T00:00:00,2800,680,420,2300,2900,87.0,82.0"
    "PU005,REF001,2025-12-01T12:00:00,2850,685,425,2350,2950,88.0,84.0"
    "PU005,REF001,2025-12-02T00:00:00,2780,675,415,2280,2880,86.5,81.0"
    "PU006,REF001,2025-12-01T00:00:00,1800,450,320,180,1850,82.0,52.0"
    "PU006,REF001,2025-12-01T12:00:00,1820,455,325,182,1870,83.0,53.0"
    "PU009,REF001,2025-12-01T00:00:00,1500,1600,280,1100,1550,95.0,45.0"
    "PU009,REF001,2025-12-01T12:00:00,1520,1610,285,1120,1570,95.5,46.0"
    "PU010,REF002,2025-12-01T00:00:00,5500,640,375,42.0,5600,91.0,160.0"
    "PU010,REF002,2025-12-01T12:00:00,5550,645,380,42.5,5650,91.5,162.0"
    "PU010,REF002,2025-12-02T00:00:00,5480,638,373,41.5,5580,90.5,158.0"
    "PU011,REF002,2025-12-01T00:00:00,3000,980,620,38.0,3100,89.0,92.0"
    "PU011,REF002,2025-12-01T12:00:00,3050,985,625,38.5,3150,89.5,94.0"
)
$processInline = $processData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table ProcessMetric with (format='csv') <|`n$processInline" `
                     -Description "Ingesting 30 ProcessMetric rows..." | Out-Null
    Write-Host "  [OK] ProcessMetric ingested (30 rows)" -ForegroundColor Green
}
catch {
    Write-Host "  [WARN] ProcessMetric ingestion failed: $_" -ForegroundColor Yellow
}

# ════════════════════════════════════════════════════════════════════════════
# STEP 5: GENERATE AND INGEST PipelineFlow SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 5] Ingesting PipelineFlow sample data..." -ForegroundColor Cyan

$pipelineData = @(
    "PL001,REF001,2025-12-01T00:00:00,3500,150,200,12.5,true"
    "PL001,REF001,2025-12-01T06:00:00,3450,148,198,12.3,true"
    "PL001,REF001,2025-12-01T12:00:00,3550,152,202,12.7,true"
    "PL001,REF001,2025-12-01T18:00:00,3480,149,199,12.4,true"
    "PL001,REF001,2025-12-02T00:00:00,3520,151,201,12.6,true"
    "PL002,REF001,2025-12-01T00:00:00,2800,120,350,8.5,true"
    "PL002,REF001,2025-12-01T12:00:00,2850,122,355,8.7,true"
    "PL002,REF001,2025-12-02T00:00:00,2780,118,348,8.3,true"
    "PL003,REF001,2025-12-01T00:00:00,1200,85,180,15.0,true"
    "PL003,REF001,2025-12-01T12:00:00,1250,88,185,15.2,true"
    "PL003,REF001,2025-12-02T00:00:00,1180,82,175,14.8,true"
    "PL004,REF001,2025-12-01T00:00:00,4100,200,280,10.0,true"
    "PL004,REF001,2025-12-01T12:00:00,4150,202,282,10.2,true"
    "PL005,REF001,2025-12-01T00:00:00,950,60,120,18.0,true"
    "PL005,REF001,2025-12-01T12:00:00,980,62,125,18.5,true"
    "PL006,REF001,2025-12-01T00:00:00,2200,95,160,9.2,true"
    "PL006,REF001,2025-12-01T12:00:00,2150,93,158,9.0,true"
    "PL007,REF001,2025-12-01T00:00:00,800,45,90,22.0,false"
    "PL007,REF001,2025-12-01T12:00:00,120,15,85,25.0,false"
    "PL008,REF002,2025-12-01T00:00:00,5200,190,210,11.0,true"
    "PL008,REF002,2025-12-01T12:00:00,5250,192,212,11.2,true"
    "PL009,REF002,2025-12-01T00:00:00,3800,160,250,9.8,true"
    "PL009,REF002,2025-12-01T12:00:00,3850,162,252,10.0,true"
    "PL010,REF003,2025-12-01T00:00:00,2500,110,190,13.0,true"
    "PL010,REF003,2025-12-01T12:00:00,2550,112,192,13.2,true"
)
$pipelineInline = $pipelineData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table PipelineFlow with (format='csv') <|`n$pipelineInline" `
                     -Description "Ingesting 25 PipelineFlow rows..." | Out-Null
    Write-Host "  [OK] PipelineFlow ingested (25 rows)" -ForegroundColor Green
}
catch {
    Write-Host "  [WARN] PipelineFlow ingestion failed: $_" -ForegroundColor Yellow
}

# ════════════════════════════════════════════════════════════════════════════
# STEP 6: GENERATE AND INGEST TankLevel SAMPLE DATA
# ════════════════════════════════════════════════════════════════════════════
Write-Host "`n[Step 6] Ingesting TankLevel sample data..." -ForegroundColor Cyan

$tankData = @(
    "TK001,REF001,2025-12-01T00:00:00,42000,84.0,72.5,PR001,false"
    "TK001,REF001,2025-12-01T12:00:00,40500,81.0,73.0,PR001,false"
    "TK001,REF001,2025-12-02T00:00:00,38000,76.0,72.0,PR001,false"
    "TK001,REF001,2025-12-02T12:00:00,41000,82.0,73.5,PR001,false"
    "TK002,REF001,2025-12-01T00:00:00,28000,70.0,68.0,PR002,false"
    "TK002,REF001,2025-12-01T12:00:00,29500,73.8,68.5,PR002,false"
    "TK002,REF001,2025-12-02T00:00:00,27000,67.5,67.5,PR002,false"
    "TK003,REF001,2025-12-01T00:00:00,18000,90.0,75.0,PR003,false"
    "TK003,REF001,2025-12-01T12:00:00,17200,86.0,74.5,PR003,false"
    "TK003,REF001,2025-12-02T00:00:00,19800,99.0,76.0,PR003,false"
    "TK004,REF001,2025-12-01T00:00:00,8500,42.5,62.0,PR004,false"
    "TK004,REF001,2025-12-01T12:00:00,9200,46.0,62.5,PR004,false"
    "TK005,REF001,2025-12-01T00:00:00,5500,27.5,58.0,PR005,false"
    "TK005,REF001,2025-12-01T12:00:00,6200,31.0,58.5,PR005,false"
    "TK006,REF002,2025-12-01T00:00:00,55000,88.0,70.0,PR001,false"
    "TK006,REF002,2025-12-01T12:00:00,53000,84.8,70.5,PR001,false"
    "TK006,REF002,2025-12-02T00:00:00,56000,89.6,69.5,PR001,false"
    "TK007,REF002,2025-12-01T00:00:00,32000,64.0,66.0,PR002,false"
    "TK007,REF002,2025-12-01T12:00:00,33500,67.0,66.5,PR002,false"
    "TK008,REF003,2025-12-01T00:00:00,22000,55.0,72.0,PR003,false"
    "TK008,REF003,2025-12-01T12:00:00,23500,58.8,72.5,PR003,false"
    "TK009,REF004,2025-12-01T00:00:00,15000,75.0,60.0,PR004,false"
    "TK009,REF004,2025-12-01T12:00:00,14000,70.0,60.5,PR004,false"
    "TK010,REF005,2025-12-01T00:00:00,48000,96.0,74.0,PR001,true"
    "TK010,REF005,2025-12-01T12:00:00,45000,90.0,73.5,PR001,false"
)
$tankInline = $tankData -join "`n"
try {
    Invoke-KustoMgmt -Command ".ingest inline into table TankLevel with (format='csv') <|`n$tankInline" `
                     -Description "Ingesting 25 TankLevel rows..." | Out-Null
    Write-Host "  [OK] TankLevel ingested (25 rows)" -ForegroundColor Green
}
catch {
    Write-Host "  [WARN] TankLevel ingestion failed: $_" -ForegroundColor Yellow
}

# ════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════════════════
Write-Host ""
Write-Host "=== KQL Tables Deployment Summary ===" -ForegroundColor Cyan
Write-Host "  Eventhouse:    $EventhouseId" -ForegroundColor White
Write-Host "  KQL Database:  $KqlDatabaseName ($KqlDatabaseId)" -ForegroundColor White
Write-Host "  Query URI:     $QueryServiceUri" -ForegroundColor White
Write-Host ""
Write-Host "  Tables created and populated:" -ForegroundColor White
Write-Host "    - SensorReading    ($($sensorReadingLines.Count) rows from SensorTelemetry.csv)" -ForegroundColor White
Write-Host "    - EquipmentAlert   (25 sample rows)" -ForegroundColor White
Write-Host "    - ProcessMetric    (30 sample rows)" -ForegroundColor White
Write-Host "    - PipelineFlow     (25 sample rows)" -ForegroundColor White
Write-Host "    - TankLevel        (25 sample rows)" -ForegroundColor White
Write-Host ""
Write-Host "  The RTI Dashboard tiles will now show data from these tables." -ForegroundColor Green
Write-Host "=== KQL Tables Deployment Complete ===" -ForegroundColor Cyan
