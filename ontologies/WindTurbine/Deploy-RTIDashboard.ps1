<#
.SYNOPSIS
    Deploy a Real-Time Intelligence (KQL) Dashboard for Wind Turbine telemetry.
.DESCRIPTION
    Creates a KQLDashboard in Microsoft Fabric with 10 tiles covering:
      1. Turbine Power Output Over Time (line)
      2. Turbine Alerts by Severity (pie)
      3. Capacity Factor by Turbine (bar)
      4. Wind Speed vs Power Output (scatter)
      5. Turbine Health Summary (table)
      6. Weather Station Readings (table)
      7. Maintenance Events (table)
      8. Alert Trend Over Time (line)
      9. Unacknowledged Alerts (table)
     10. Icing Risk Periods (table)
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$DashboardName = "WindTurbineDashboard"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying KQL Dashboard: $DashboardName ===" -ForegroundColor Cyan

if (-not $KqlDatabaseId -or -not $QueryServiceUri) {
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
    if (-not $KqlDatabaseId) {
        $kqlDb = $allItems | Where-Object { $_.type -eq 'KQLDatabase' } | Select-Object -First 1
        if ($kqlDb) { $KqlDatabaseId = $kqlDb.id } else { Write-Host "[ERROR] No KQL Database found." -ForegroundColor Red; exit 1 }
    }
    if (-not $QueryServiceUri) {
        $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1
        if ($eh) { $ehD = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$($eh.id)" -Headers $headers; $QueryServiceUri = $ehD.properties.queryServiceUri }
    }
}

$kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $headers
$kqlDbName = $kqlDbDetails.displayName

$dataSourceId = [guid]::NewGuid().ToString()
$pageId = [guid]::NewGuid().ToString()

function New-VisualOptions {
    return @{
        xColumn = @{ type = "infer" }; yColumns = @{ type = "infer" }; yAxisMinimumValue = @{ type = "infer" }; yAxisMaximumValue = @{ type = "infer" }
        seriesColumns = @{ type = "infer" }; hideLegend = $false; xColumnTitle = ""; yColumnTitle = ""; horizontalLine = ""; verticalLine = ""
        xAxisScale = "linear"; yAxisScale = "linear"; crossFilterDisabled = $false; hideTileTitle = $false
        multipleYAxes = @{ base = @{ id = "-1"; columns = @(); label = ""; yAxisMinimumValue = $null; yAxisMaximumValue = $null; yAxisScale = "linear"; horizontalLines = @() }; additional = @() }
    }
}

$tiles = @(
    @{ id = [guid]::NewGuid().ToString(); title = "Turbine Power Output Over Time"; layout = @{ x = 0; y = 0; width = 12; height = 6 }; pageId = $pageId; visualType = "line"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
PowerOutputMetric
| where Timestamp > ago(24h)
| summarize AvgPowerKW = avg(PowerOutputKW) by bin(Timestamp, 1h), TurbineId
| order by Timestamp asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Turbine Alerts by Severity"; layout = @{ x = 12; y = 0; width = 6; height = 6 }; pageId = $pageId; visualType = "pie"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
TurbineAlert
| summarize AlertCount = count() by Severity
| order by AlertCount desc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Capacity Factor by Turbine"; layout = @{ x = 18; y = 0; width = 6; height = 6 }; pageId = $pageId; visualType = "bar"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
PowerOutputMetric
| summarize AvgCapacityFactor = avg(CapacityFactor) by TurbineId
| order by AvgCapacityFactor desc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Wind Speed vs Power Output"; layout = @{ x = 0; y = 6; width = 12; height = 6 }; pageId = $pageId; visualType = "scatter"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
PowerOutputMetric
| project WindSpeedMs, PowerOutputKW, TurbineId
| order by WindSpeedMs asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Turbine Health Summary"; layout = @{ x = 12; y = 6; width = 12; height = 6 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
TurbineReading
| where Timestamp > ago(24h)
| summarize AvgValue = round(avg(Value), 2), MaxValue = round(max(Value), 2), AnomalyCount = countif(IsAnomaly == true) by TurbineId, SensorType
| order by AnomalyCount desc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Weather Station Readings"; layout = @{ x = 0; y = 12; width = 12; height = 6 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
WeatherMetric
| order by Timestamp desc
| take 20
| project Timestamp, StationId, FarmId, WindSpeedMs, WindDirectionDeg, TemperatureC, HumidityPct, IcingRisk
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Maintenance Events by Component"; layout = @{ x = 12; y = 12; width = 12; height = 6 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
MaintenanceMetric
| summarize EventCount = count(), TotalCostUSD = sum(CostUSD), AvgDurationHrs = round(avg(DurationHours), 1) by Component
| order by TotalCostUSD desc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Alert Trend Over Time"; layout = @{ x = 0; y = 18; width = 12; height = 6 }; pageId = $pageId; visualType = "line"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
TurbineAlert
| summarize AlertCount = count() by bin(Timestamp, 6h), Severity
| order by Timestamp asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Unacknowledged Alerts"; layout = @{ x = 12; y = 18; width = 12; height = 6 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
TurbineAlert
| where IsAcknowledged == false
| order by Timestamp desc
| project Timestamp, TurbineId, AlertType, Severity, Component, Message
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Icing Risk Periods"; layout = @{ x = 0; y = 24; width = 24; height = 6 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
WeatherMetric
| where IcingRisk == true
| order by Timestamp desc
| project Timestamp, StationId, FarmId, WindSpeedMs, TemperatureC, HumidityPct, VisibilityKm
"@ }
)

$dashboard = @{
    "$schema" = "https://dataexplorer.azure.com/static/d/schema/20/dashboard.json"
    autoRefresh = @{ enabled = $true; defaultInterval = "30s"; minInterval = "10s" }
    pages = @( @{ id = $pageId; name = "Wind Turbine Overview"; tiles = $tiles } )
    dataSources = @( @{ id = $dataSourceId; scopeId = "global"; name = $kqlDbName; clusterUri = $QueryServiceUri; database = $kqlDbName; kind = "manual-kusto" } )
    parameters = @()
    schema_version = "20"
}

$dashboardJson = $dashboard | ConvertTo-Json -Depth 20 -Compress
$dashBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($dashboardJson))

$defBody = @{
    displayName = $DashboardName
    type = "KQLDashboard"
    definition = @{
        parts = @( @{ path = "RealTimeDashboard.json"; payload = $dashBase64; payloadType = "InlineBase64" } )
    }
} | ConvertTo-Json -Depth 10

# Check for existing dashboard
$existingDashboard = $null
try {
    $items = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items?type=KQLDashboard" -Headers $headers).value
    $existingDashboard = $items | Where-Object { $_.displayName -eq $DashboardName } | Select-Object -First 1
} catch {}

if ($existingDashboard) {
    Write-Host "  Updating existing dashboard $($existingDashboard.id)..." -ForegroundColor Yellow
    $updBody = @{ definition = @{ parts = @( @{ path = "RealTimeDashboard.json"; payload = $dashBase64; payloadType = "InlineBase64" } ) } } | ConvertTo-Json -Depth 10
    try {
        Invoke-RestMethod -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/kqlDashboards/$($existingDashboard.id)/updateDefinition" -Headers $headers -Body $updBody | Out-Null
        Write-Host "  [OK] Dashboard updated: $DashboardName (10 tiles)" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Dashboard update: $_" -ForegroundColor Red }
} else {
    Write-Host "  Creating new dashboard..." -ForegroundColor Gray
    try {
        $resp = Invoke-WebRequest -Method Post -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers -Body $defBody -UseBasicParsing
        if ($resp.StatusCode -eq 202 -and $resp.Headers["Location"]) {
            $loc = $resp.Headers["Location"]; $retryAfter = if ($resp.Headers["Retry-After"]) { [int]$resp.Headers["Retry-After"] } else { 5 }
            for ($i = 0; $i -lt 30; $i++) { Start-Sleep -Seconds $retryAfter; try { $poll = Invoke-RestMethod -Uri $loc -Headers $headers; if ($poll.status -eq "Succeeded") { break } } catch {} }
        }
        Write-Host "  [OK] Dashboard created: $DashboardName (10 tiles)" -ForegroundColor Green
    } catch { Write-Host "  [ERROR] Dashboard creation: $_" -ForegroundColor Red }
}

Write-Host "`n=== Wind Turbine Dashboard Deployment Complete ===" -ForegroundColor Cyan
