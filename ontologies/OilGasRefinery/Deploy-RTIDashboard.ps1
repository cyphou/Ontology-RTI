<#
.SYNOPSIS
    Deploy a Real-Time Intelligence (KQL) Dashboard for Oil & Gas Refinery telemetry.
.DESCRIPTION
    Creates a KQLDashboard in Microsoft Fabric connected to the Eventhouse KQL database,
    with 8 pre-built tiles covering:
      1. Sensor Readings Over Time (line/timechart)
      2. Active Alerts by Severity (pie chart)
      3. Alert Trend by Severity (line/timechart)
      4. Top Sensors by Reading Count (table)
      5. Anomaly Detection - Out-of-Range Readings (table)
      6. Equipment Maintenance Timeline (table)
      7. Process Unit Live Status (table)
      8. Production Output Over Time (line/timechart)

    Uses the Fabric REST API for KQLDashboard items with the standard
    RealTimeDashboard.json definition (schema version 20).

    PREREQUISITE: The tenant setting "Create Real-Time dashboards" must be
    enabled by the Fabric admin in the Admin Portal > Tenant settings.

.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER KqlDatabaseId
    The KQL Database GUID (auto-detected if omitted).
.PARAMETER QueryServiceUri
    The Kusto query service URI (auto-detected from Eventhouse if omitted).
.PARAMETER DashboardName
    Display name for the dashboard (default: RefineryTelemetryDashboard).
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$DashboardName = "RefineryTelemetryDashboard"
)

# ── Authentication ──────────────────────────────────────────────────────────
$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying KQL Dashboard: $DashboardName ===" -ForegroundColor Cyan

# ── Auto-detect KQL Database and Eventhouse if not provided ─────────────────
if (-not $KqlDatabaseId -or -not $QueryServiceUri) {
    Write-Host "Auto-detecting KQL Database from workspace..." -ForegroundColor Yellow
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value

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

    if (-not $QueryServiceUri) {
        $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1
        if ($eh) {
            $ehDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$($eh.id)" -Headers $headers
            $QueryServiceUri = $ehDetails.properties.queryServiceUri
            Write-Host "  Query URI: $QueryServiceUri" -ForegroundColor Gray
        } else {
            Write-Host "[ERROR] No Eventhouse found. Please provide -QueryServiceUri." -ForegroundColor Red
            exit 1
        }
    }
}

# ── Get KQL Database name ──────────────────────────────────────────────────
$kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $headers
$kqlDbName = $kqlDbDetails.displayName
Write-Host "  KQL Database: $kqlDbName" -ForegroundColor Gray

# ── Build Dashboard Definition ──────────────────────────────────────────────
# Schema: https://dataexplorer.azure.com/static/d/schema/20/dashboard.json
# Tiles are top-level, each references a pageId and dataSourceId.

$dataSourceId = [guid]::NewGuid().ToString()
$pageId       = [guid]::NewGuid().ToString()

# Helper to create visual options with inference
function New-VisualOptions {
    return @{
        xColumn             = @{ type = "infer" }
        yColumns            = @{ type = "infer" }
        yAxisMinimumValue   = @{ type = "infer" }
        yAxisMaximumValue   = @{ type = "infer" }
        seriesColumns       = @{ type = "infer" }
        hideLegend          = $false
        xColumnTitle        = ""
        yColumnTitle        = ""
        horizontalLine      = ""
        verticalLine        = ""
        xAxisScale          = "linear"
        yAxisScale          = "linear"
        crossFilterDisabled = $false
        hideTileTitle       = $false
        multipleYAxes       = @{
            base       = @{ id = "-1"; columns = @(); label = ""; yAxisMinimumValue = $null; yAxisMaximumValue = $null; yAxisScale = "linear"; horizontalLines = @() }
            additional = @()
        }
    }
}

$tiles = @(
    # ── Tile 1: Sensor Readings Over Time (line chart) ──────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Sensor Readings Over Time"
        query         = @"
SensorReading
| summarize AvgReading = avg(ReadingValue) by bin(Timestamp, 15m), SensorType
| order by Timestamp asc
"@
        layout        = @{ x = 0; y = 0; width = 12; height = 6 }
        pageId        = $pageId
        visualType    = "line"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 2: Equipment Alerts by Severity (pie) ──────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Equipment Alerts by Severity"
        query         = @"
EquipmentAlert
| summarize Count = count() by Severity
| order by Count desc
"@
        layout        = @{ x = 12; y = 0; width = 6; height = 6 }
        pageId        = $pageId
        visualType    = "pie"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 3: Alert Trend Over Time (line) ────────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Alert Trend Over Time"
        query         = @"
EquipmentAlert
| summarize AlertCount = count() by bin(Timestamp, 1h), Severity
| order by Timestamp asc
"@
        layout        = @{ x = 18; y = 0; width = 6; height = 6 }
        pageId        = $pageId
        visualType    = "line"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 4: Refinery Locations Map ──────────────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Refinery Locations"
        query         = @"
datatable(RefineryName:string, Latitude:real, Longitude:real, City:string, Country:string, CapacityBPD:long) [
    "Gulf Coast Refinery",   29.7604,  -95.3698,  "Houston",       "United States", 550000,
    "Baytown Complex",       29.7355,  -94.9774,  "Baytown",       "United States", 550000,
    "North Sea Refinery",    56.0234,   -3.7135,  "Grangemouth",   "United Kingdom", 550000,
    "Rotterdam Europoort",   51.9496,    4.1493,  "Rotterdam",     "Netherlands",   550000,
    "Singapore Jurong",       1.2655,  103.6990,  "Jurong Island", "Singapore",     550000,
    "Alberta Oil Sands",     56.7264, -111.3803,  "Fort McMurray", "Canada",        550000,
    "Middle East Hub",       24.1100,   52.7300,  "Ruwais",        "UAE",           550000,
    "Lagos Coastal",          6.5244,    3.3792,  "Lagos",         "Nigeria",       550000
]
| project RefineryName, Latitude, Longitude, City, Country, CapacityBPD
"@
        layout        = @{ x = 0; y = 6; width = 8; height = 6 }
        pageId        = $pageId
        visualType    = "map"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 5: Top Sensors by Reading Count (table) ────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Top Sensors by Reading Count"
        query         = @"
SensorReading
| summarize Readings = count(),
            AvgValue = round(avg(ReadingValue), 2),
            MinValue = round(min(ReadingValue), 2),
            MaxValue = round(max(ReadingValue), 2)
        by SensorId, SensorType, MeasurementUnit
| top 20 by Readings desc
"@
        layout        = @{ x = 8; y = 6; width = 8; height = 6 }
        pageId        = $pageId
        visualType    = "table"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 6: Anomaly Detection (table) ───────────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Anomaly Detections"
        query         = @"
SensorReading
| where IsAnomaly == true
| project Timestamp, SensorId, SensorType, ReadingValue, MeasurementUnit, QualityFlag, EquipmentId
| order by Timestamp desc
| take 100
"@
        layout        = @{ x = 16; y = 6; width = 8; height = 6 }
        pageId        = $pageId
        visualType    = "table"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 7: Process Unit Throughput (line) ──────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Process Unit Throughput Over Time"
        query         = @"
ProcessMetric
| summarize AvgThroughput = avg(ThroughputBPH),
            AvgYield = avg(YieldPercent)
        by bin(Timestamp, 1h), ProcessUnitId
| order by Timestamp asc
"@
        layout        = @{ x = 0; y = 12; width = 8; height = 5 }
        pageId        = $pageId
        visualType    = "line"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 8: Pipeline Flow Status (table) ───────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Pipeline Flow Status"
        query         = @"
PipelineFlow
| summarize arg_max(Timestamp, *) by PipelineId
| project PipelineId, Timestamp, FlowRateBPH, PressurePSI, TemperatureF, ViscosityCp, IsFlowNormal
| order by PipelineId asc
"@
        layout        = @{ x = 8; y = 12; width = 8; height = 5 }
        pageId        = $pageId
        visualType    = "table"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 9: Tank Levels (table) ─────────────────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Current Tank Levels"
        query         = @"
TankLevel
| summarize arg_max(Timestamp, *) by TankId
| project TankId, Timestamp, LevelBarrels, LevelPercent, TemperatureF, ProductId, IsOverflow
| order by LevelPercent desc
"@
        layout        = @{ x = 16; y = 12; width = 8; height = 5 }
        pageId        = $pageId
        visualType    = "table"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 10: Unacknowledged Alerts (table) ──────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Unacknowledged Alerts"
        query         = @"
EquipmentAlert
| where IsAcknowledged == false
| project Timestamp, AlertId, SensorId, EquipmentId, AlertType, Severity, ReadingValue, ThresholdValue, Message
| order by Timestamp desc
| take 50
"@
        layout        = @{ x = 0; y = 17; width = 12; height = 5 }
        pageId        = $pageId
        visualType    = "table"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 11: Sensor Quality Flag Distribution (pie) ─────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Sensor Quality Flag Distribution"
        query         = @"
SensorReading
| summarize Count = count() by QualityFlag
| order by Count desc
"@
        layout        = @{ x = 12; y = 17; width = 6; height = 5 }
        pageId        = $pageId
        visualType    = "pie"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    },

    # ── Tile 12: Tank Level Trend (line) ────────────────────────────────
    @{
        id            = [guid]::NewGuid().ToString()
        title         = "Tank Level Trend"
        query         = @"
TankLevel
| summarize AvgLevel = avg(LevelPercent) by bin(Timestamp, 1h), TankId
| order by Timestamp asc
"@
        layout        = @{ x = 18; y = 17; width = 6; height = 5 }
        pageId        = $pageId
        visualType    = "line"
        dataSourceId  = $dataSourceId
        visualOptions = New-VisualOptions
        usedParamVariables = @()
    }
)

# ── Assemble full dashboard definition ──────────────────────────────────────
$dashboardDef = @{
    '$schema'      = "https://dataexplorer.azure.com/static/d/schema/20/dashboard.json"
    schema_version = "20"
    title          = $DashboardName
    autoRefresh    = @{
        enabled         = $true
        defaultInterval = "30s"
        minInterval     = "30s"
    }
    dataSources    = @(
        @{
            id         = $dataSourceId
            name       = $kqlDbName
            clusterUri = $QueryServiceUri
            database   = $kqlDbName
            kind       = "manual-kusto"
            scopeId    = "KustoDatabaseResource"
        }
    )
    pages      = @(
        @{
            id   = $pageId
            name = "Refinery Overview"
        }
    )
    tiles      = $tiles
    parameters = @()
}

# Serialize to JSON
$dashJson = $dashboardDef | ConvertTo-Json -Depth 15 -Compress
$dashJsonB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashJson))

# ── Step A: Create KQLDashboard item ────────────────────────────────────────
Write-Host "Creating KQL Dashboard (type=KQLDashboard)..." -ForegroundColor Yellow

$createBody = @{
    displayName = $DashboardName
    type        = "KQLDashboard"
    description = "Real-Time Intelligence dashboard for Oil and Gas Refinery sensor telemetry, alerts, maintenance, and production monitoring"
} | ConvertTo-Json -Depth 5

$dashboardId = $null
try {
    $response = Invoke-WebRequest `
        -Uri "$apiBase/workspaces/$WorkspaceId/items" `
        -Method POST -Headers $headers -Body $createBody -UseBasicParsing

    if ($response.StatusCode -eq 201) {
        $dash = $response.Content | ConvertFrom-Json
        $dashboardId = $dash.id
        Write-Host "[OK] KQL Dashboard created: $dashboardId" -ForegroundColor Green
    }
    elseif ($response.StatusCode -eq 202) {
        $opUrl = $response.Headers['Location']
        Write-Host "LRO started, polling..." -ForegroundColor Yellow
        do {
            Start-Sleep -Seconds 3
            $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers
            Write-Host "  Status: $($poll.status)"
        } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))

        if ($poll.status -eq 'Succeeded') {
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $dashItem = $allItems | Where-Object { $_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard' }
            if ($dashItem) { $dashboardId = $dashItem.id }
            Write-Host "[OK] KQL Dashboard created: $dashboardId" -ForegroundColor Green
        } else {
            Write-Host "[FAIL] Dashboard creation: $($poll.status)" -ForegroundColor Red
        }
    }
}
catch {
    $sr = $_.Exception.Response
    if ($sr) {
        $stream = $sr.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $errorBody = $reader.ReadToEnd()
        Write-Host "[ERROR] $([int]$sr.StatusCode): $errorBody" -ForegroundColor Red

        if ($errorBody -match 'FeatureNotAvailable') {
            Write-Host ""
            Write-Host ">>> BLOCKED: The 'Create Real-Time dashboards' tenant setting is disabled." -ForegroundColor Magenta
            Write-Host ">>> Ask your Fabric admin to enable it in:" -ForegroundColor Magenta
            Write-Host ">>>   Admin Portal > Tenant settings > Real-Time Intelligence" -ForegroundColor Magenta
            Write-Host ">>>   Setting: 'Create Real-Time Dashboards (preview)'" -ForegroundColor Magenta
        }
        elseif ($errorBody -match 'ItemDisplayNameAlreadyInUse') {
            Write-Host "  Dashboard '$DashboardName' already exists. Will update definition..." -ForegroundColor Yellow
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $existing = $allItems | Where-Object { $_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard' }
            if ($existing) {
                $dashboardId = $existing.id
                Write-Host "  Existing Dashboard ID: $dashboardId" -ForegroundColor Gray
            }
        }
    }
    else {
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    }
}

# ── Step B: Upload dashboard definition ─────────────────────────────────────
if ($dashboardId) {
    Write-Host "Uploading dashboard definition ($($tiles.Count) tiles, data source: $kqlDbName)..." -ForegroundColor Yellow

    $updateBody = @{
        definition = @{
            parts = @(
                @{
                    path        = "RealTimeDashboard.json"
                    payload     = $dashJsonB64
                    payloadType = "InlineBase64"
                }
            )
        }
    } | ConvertTo-Json -Depth 10

    # Try type-specific endpoint first, then generic fallback
    $defApplied = $false
    foreach ($endpoint in @("$apiBase/workspaces/$WorkspaceId/kqlDashboards/$dashboardId/updateDefinition",
                            "$apiBase/workspaces/$WorkspaceId/items/$dashboardId/updateDefinition")) {
        if ($defApplied) { break }
        try {
            $updResp = Invoke-WebRequest -Uri $endpoint -Method POST -Headers $headers -Body $updateBody -UseBasicParsing

            if ($updResp.StatusCode -in @(200,202)) {
                if ($updResp.StatusCode -eq 202) {
                    $opUrl2 = $updResp.Headers['Location']
                    do {
                        Start-Sleep -Seconds 3
                        $poll2 = Invoke-RestMethod -Uri $opUrl2 -Headers $headers
                        Write-Host "  Definition update: $($poll2.status)"
                    } while ($poll2.status -notin @('Succeeded','Failed','Cancelled'))

                    if ($poll2.status -eq 'Succeeded') {
                        $defApplied = $true
                        Write-Host "[OK] Dashboard definition applied with $($tiles.Count) tiles." -ForegroundColor Green
                    } else {
                        Write-Host "[WARN] Definition update: $($poll2.status)" -ForegroundColor Yellow
                    }
                } else {
                    $defApplied = $true
                    Write-Host "[OK] Dashboard definition applied with $($tiles.Count) tiles." -ForegroundColor Green
                }
            }
        }
        catch {
            # Try next endpoint
        }
    }

    if (-not $defApplied) {
        Write-Host "[WARN] Definition update failed. Configure tiles manually in the UI." -ForegroundColor Yellow
        Write-Host "  Data source: $kqlDbName at $QueryServiceUri" -ForegroundColor Gray
    }
}
else {
    Write-Host ""
    Write-Host "[INFO] Dashboard item could not be created." -ForegroundColor Yellow
    Write-Host "  Once the tenant setting is enabled, re-run this script." -ForegroundColor Yellow
}

# ── Summary ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== KQL Dashboard Deployment Summary ===" -ForegroundColor Cyan
Write-Host "  Name:           $DashboardName"
Write-Host "  Dashboard ID:   $dashboardId"
Write-Host "  KQL Database:   $kqlDbName ($KqlDatabaseId)"
Write-Host "  Query URI:      $QueryServiceUri"
Write-Host "  Tiles:          $($tiles.Count)"
Write-Host ""
Write-Host "Dashboard tiles:" -ForegroundColor White
foreach ($t in $tiles) {
    Write-Host "  - $($t.title) [$($t.visualType)]"
}
Write-Host ""
Write-Host "KQL Tables used:" -ForegroundColor White
Write-Host "  - SensorReading    (SensorId, EquipmentId, RefineryId, Timestamp, ReadingValue, MeasurementUnit, SensorType, QualityFlag, IsAnomaly)"
Write-Host "  - EquipmentAlert   (AlertId, SensorId, EquipmentId, Timestamp, AlertType, Severity, ReadingValue, ThresholdValue, Message, IsAcknowledged)"
Write-Host "  - ProcessMetric    (ProcessUnitId, Timestamp, ThroughputBPH, YieldPercent, PressurePSI, ...)"
Write-Host "  - PipelineFlow     (PipelineId, Timestamp, FlowRateBPH, PressurePSI, TemperatureF, IsFlowNormal)"
Write-Host "  - TankLevel        (TankId, Timestamp, LevelBarrels, LevelPercent, TemperatureF, ProductId, IsOverflow)"
Write-Host "  - datatable        (Refinery locations for map tile)"
Write-Host ""
Write-Host "Open the dashboard in Fabric to view live data." -ForegroundColor White
Write-Host "=== KQL Dashboard Deployment Complete ===" -ForegroundColor Cyan
