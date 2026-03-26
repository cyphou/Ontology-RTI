<#
.SYNOPSIS
    Deploy a Real-Time Intelligence (KQL) Dashboard for Smart Building telemetry.
.DESCRIPTION
    Creates a KQLDashboard in Microsoft Fabric with 10 tiles covering:
      1. Sensor Readings Over Time (line)
      2. Building Alerts by Severity (pie)
      3. HVAC Efficiency by System (bar)
      4. Energy Consumption Over Time (line)
      5. Zone Occupancy Heatmap (table)
      6. Anomaly Detections (table)
      7. HVAC Temperature Delta (line)
      8. Alert Trend Over Time (line)
      9. Unacknowledged Alerts (table)
     10. Energy Cost by Building (bar)
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$DashboardName = "SmartBuildingDashboard"
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
    @{ id = [guid]::NewGuid().ToString(); title = "Sensor Readings Over Time"; layout = @{ x = 0; y = 0; width = 12; height = 6 }; pageId = $pageId; visualType = "line"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
SensorReading
| summarize AvgReading = avg(ReadingValue) by bin(Timestamp, 15m), SensorType
| order by Timestamp asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Building Alerts by Severity"; layout = @{ x = 12; y = 0; width = 6; height = 6 }; pageId = $pageId; visualType = "pie"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
BuildingAlert
| summarize Count = count() by Severity
| order by Count desc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "HVAC Efficiency by System"; layout = @{ x = 18; y = 0; width = 6; height = 6 }; pageId = $pageId; visualType = "bar"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
HVACMetric
| summarize AvgEfficiency = round(avg(EfficiencyPct), 1), AvgPowerKW = round(avg(PowerKW), 1) by HVACSystemId
| order by AvgEfficiency asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Energy Consumption Over Time"; layout = @{ x = 0; y = 6; width = 12; height = 6 }; pageId = $pageId; visualType = "line"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
EnergyConsumption
| summarize TotalKWh = sum(PowerKWh), TotalCost = sum(CostUSD) by bin(Timestamp, 1h), BuildingId
| order by Timestamp asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Zone Occupancy Overview"; layout = @{ x = 12; y = 6; width = 12; height = 6 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
OccupancyMetric
| summarize arg_max(Timestamp, *) by ZoneId
| project ZoneId, BuildingId, Timestamp, OccupantCount, MaxCapacity, UtilizationPct
| order by UtilizationPct desc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Anomaly Detections"; layout = @{ x = 0; y = 12; width = 8; height = 5 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
SensorReading
| where IsAnomaly == true
| project Timestamp, SensorId, SensorType, ReadingValue, MeasurementUnit, ZoneId, BuildingId
| order by Timestamp desc
| take 100
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "HVAC Supply vs Return Temperature"; layout = @{ x = 8; y = 12; width = 8; height = 5 }; pageId = $pageId; visualType = "line"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
HVACMetric
| summarize AvgSupply = avg(SupplyTempF), AvgReturn = avg(ReturnTempF) by bin(Timestamp, 1h), HVACSystemId
| order by Timestamp asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Alert Trend Over Time"; layout = @{ x = 16; y = 12; width = 8; height = 5 }; pageId = $pageId; visualType = "line"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
BuildingAlert
| summarize AlertCount = count() by bin(Timestamp, 1h), Severity
| order by Timestamp asc
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Unacknowledged Alerts"; layout = @{ x = 0; y = 17; width = 12; height = 5 }; pageId = $pageId; visualType = "table"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
BuildingAlert
| where IsAcknowledged == false
| project Timestamp, AlertId, SensorId, ZoneId, BuildingId, AlertType, Severity, ReadingValue, ThresholdValue, Message
| order by Timestamp desc
| take 50
"@ },
    @{ id = [guid]::NewGuid().ToString(); title = "Energy Cost by Building"; layout = @{ x = 12; y = 17; width = 12; height = 5 }; pageId = $pageId; visualType = "bar"; dataSourceId = $dataSourceId; visualOptions = New-VisualOptions; usedParamVariables = @()
       query = @"
EnergyConsumption
| summarize TotalCost = round(sum(CostUSD), 2), TotalKWh = round(sum(PowerKWh), 1) by BuildingId
| order by TotalCost desc
"@ }
)

$dashboardDef = @{
    schema_version = "52"; title = $DashboardName
    autoRefresh = @{ enabled = $true; defaultInterval = "30s"; minInterval = "10s" }
    dataSources = @(@{ id = $dataSourceId; name = $kqlDbName; clusterUri = $QueryServiceUri; database = $kqlDbName; kind = "manual-kusto"; scopeId = "KustoDatabaseResource" })
    pages = @(@{ id = $pageId; name = "Building Overview" })
    tiles = $tiles; parameters = @()
}

$dashJson = $dashboardDef | ConvertTo-Json -Depth 15 -Compress
$dashJsonB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashJson))

Write-Host "Creating KQL Dashboard..." -ForegroundColor Yellow
$createBody = @{ displayName = $DashboardName; type = "KQLDashboard"; description = "Real-Time Intelligence dashboard for Smart Building sensor telemetry, HVAC, energy and occupancy monitoring" } | ConvertTo-Json -Depth 5

$dashboardId = $null
try {
    $response = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($response.StatusCode -eq 201) { $dashboardId = ($response.Content | ConvertFrom-Json).id; Write-Host "[OK] Created: $dashboardId" -ForegroundColor Green }
    elseif ($response.StatusCode -eq 202) {
        $opUrl = $response.Headers['Location']; do { Start-Sleep -Seconds 3; $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))
        if ($poll.status -eq 'Succeeded') { $allI = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId = ($allI | Where-Object { $_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard' }).id }
    }
} catch {
    $sr = $_.Exception.Response; if ($sr) { $stream = $sr.GetResponseStream(); $reader = New-Object System.IO.StreamReader($stream); $errorBody = $reader.ReadToEnd()
        if ($errorBody -match 'ItemDisplayNameAlreadyInUse') { $allI = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId = ($allI | Where-Object { $_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard' }).id }
        elseif ($errorBody -match 'FeatureNotAvailable') { Write-Host ">>> Enable 'Create Real-Time dashboards' in Admin Portal." -ForegroundColor Magenta }
    }
}

if ($dashboardId) {
    Write-Host "Uploading dashboard definition ($($tiles.Count) tiles)..." -ForegroundColor Yellow
    $updateBody = @{ definition = @{ parts = @(@{ path = "RealTimeDashboard.json"; payload = $dashJsonB64; payloadType = "InlineBase64" }) } } | ConvertTo-Json -Depth 10
    foreach ($ep in @("$apiBase/workspaces/$WorkspaceId/kqlDashboards/$dashboardId/updateDefinition", "$apiBase/workspaces/$WorkspaceId/items/$dashboardId/updateDefinition")) {
        try {
            $r = Invoke-WebRequest -Uri $ep -Method POST -Headers $headers -Body $updateBody -UseBasicParsing
            if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] Dashboard definition applied." -ForegroundColor Green; break }
        } catch {}
    }
}

Write-Host "=== Dashboard Deployment Complete ===" -ForegroundColor Cyan
