<#
.SYNOPSIS
    Deploy a Real-Time Intelligence KQL Dashboard for Manufacturing Plant telemetry.
.DESCRIPTION
    Creates a KQLDashboard with 10 tiles: sensor readings, alerts, OEE, machine health,
    production throughput, quality metrics, defect rate trends, and energy consumption.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$DashboardName = "ManufacturingPlantDashboard"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying KQL Dashboard: $DashboardName ===" -ForegroundColor Cyan

if (-not $KqlDatabaseId -or -not $QueryServiceUri) {
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
    if (-not $KqlDatabaseId) { $kqlDb = $allItems | Where-Object { $_.type -eq 'KQLDatabase' } | Select-Object -First 1; if ($kqlDb) { $KqlDatabaseId = $kqlDb.id } else { exit 1 } }
    if (-not $QueryServiceUri) { $eh = $allItems | Where-Object { $_.type -eq 'Eventhouse' } | Select-Object -First 1; if ($eh) { $ehD = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/eventhouses/$($eh.id)" -Headers $headers; $QueryServiceUri = $ehD.properties.queryServiceUri } }
}

$kqlDbDetails = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $headers
$kqlDbName = $kqlDbDetails.displayName
$dsId = [guid]::NewGuid().ToString(); $pgId = [guid]::NewGuid().ToString()

function New-VO { return @{ xColumn = @{type="infer"}; yColumns = @{type="infer"}; yAxisMinimumValue = @{type="infer"}; yAxisMaximumValue = @{type="infer"}; seriesColumns = @{type="infer"}; hideLegend = $false; xColumnTitle = ""; yColumnTitle = ""; horizontalLine = ""; verticalLine = ""; xAxisScale = "linear"; yAxisScale = "linear"; crossFilterDisabled = $false; hideTileTitle = $false; multipleYAxes = @{ base = @{ id="-1"; columns=@(); label=""; yAxisMinimumValue=$null; yAxisMaximumValue=$null; yAxisScale="linear"; horizontalLines=@() }; additional=@() } } }

$tiles = @(
    @{ id=[guid]::NewGuid().ToString(); title="Sensor Readings Over Time"; layout=@{x=0;y=0;width=12;height=6}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="SensorReading`n| summarize AvgReading = avg(ReadingValue) by bin(Timestamp, 15m), SensorType`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Plant Alerts by Severity"; layout=@{x=12;y=0;width=6;height=6}; pageId=$pgId; visualType="pie"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="PlantAlert`n| summarize Count = count() by Severity`n| order by Count desc" },
    @{ id=[guid]::NewGuid().ToString(); title="OEE by Production Line"; layout=@{x=18;y=0;width=6;height=6}; pageId=$pgId; visualType="bar"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ProductionMetric`n| summarize AvgOEE = round(avg(OEEPercent), 1), AvgDefect = round(avg(DefectRate)*100, 2) by LineId`n| order by AvgOEE desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Machine Health Status"; layout=@{x=0;y=6;width=12;height=6}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="MachineHealth`n| summarize arg_max(Timestamp, *) by MachineId`n| project MachineId, LineId, Timestamp, VibrationMmS, TemperatureC, CurrentAmps, SpeedRPM, Status`n| order by Status desc, VibrationMmS desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Production Throughput Over Time"; layout=@{x=12;y=6;width=12;height=6}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ProductionMetric`n| summarize TotalQty = sum(QuantityProduced), TotalEnergy = sum(EnergyUsedKWh) by bin(Timestamp, 1h), LineId`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Quality Check Results"; layout=@{x=0;y=12;width=8;height=5}; pageId=$pgId; visualType="pie"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="QualityMetric`n| summarize Count = count() by Result`n| order by Count desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Defect Rate Trend"; layout=@{x=8;y=12;width=8;height=5}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ProductionMetric`n| summarize AvgDefectRate = round(avg(DefectRate)*100, 3) by bin(Timestamp, 1h), LineId`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Anomaly Detections"; layout=@{x=16;y=12;width=8;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="SensorReading`n| where IsAnomaly == true`n| project Timestamp, SensorId, MachineId, SensorType, ReadingValue, MeasurementUnit`n| order by Timestamp desc`n| take 100" },
    @{ id=[guid]::NewGuid().ToString(); title="Unacknowledged Alerts"; layout=@{x=0;y=17;width=12;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="PlantAlert`n| where IsAcknowledged == false`n| project Timestamp, AlertId, MachineId, LineId, AlertType, Severity, ReadingValue, ThresholdValue, Message`n| order by Timestamp desc`n| take 50" },
    @{ id=[guid]::NewGuid().ToString(); title="Energy per Unit Produced"; layout=@{x=12;y=17;width=12;height=5}; pageId=$pgId; visualType="bar"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ProductionMetric`n| summarize TotalEnergy = sum(EnergyUsedKWh), TotalQty = sum(QuantityProduced) by LineId`n| extend EnergyPerUnit = round(TotalEnergy / TotalQty, 2)`n| order by EnergyPerUnit desc" }
)

$dashDef = @{ '$schema'="https://dataexplorer.azure.com/static/d/schema/20/dashboard.json"; schema_version="20"; title=$DashboardName
    autoRefresh=@{enabled=$true;defaultInterval="30s";minInterval="30s"}
    dataSources=@(@{id=$dsId;name=$kqlDbName;clusterUri=$QueryServiceUri;database=$kqlDbName;kind="manual-kusto";scopeId="KustoDatabaseResource"})
    pages=@(@{id=$pgId;name="Plant Overview"}); tiles=$tiles; parameters=@() }

$dashJson = $dashDef | ConvertTo-Json -Depth 15 -Compress
$dashB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashJson))

$createBody = @{displayName=$DashboardName;type="KQLDashboard";description="Real-Time dashboard for Manufacturing Plant monitoring"} | ConvertTo-Json -Depth 5
$dashboardId = $null
try {
    $resp = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($resp.StatusCode -eq 201) { $dashboardId = ($resp.Content | ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); if ($poll.status -eq 'Succeeded') { $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId=($allI|Where-Object{$_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard'}).id } }
} catch { $sr=$_.Exception.Response; if ($sr) { $stream=$sr.GetResponseStream(); $reader=New-Object System.IO.StreamReader($stream); $eb=$reader.ReadToEnd(); if ($eb -match 'ItemDisplayNameAlreadyInUse') { $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId=($allI|Where-Object{$_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard'}).id } } }

if ($dashboardId) {
    $updateBody = @{definition=@{parts=@(@{path="RealTimeDashboard.json";payload=$dashB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
    foreach ($ep in @("$apiBase/workspaces/$WorkspaceId/kqlDashboards/$dashboardId/updateDefinition","$apiBase/workspaces/$WorkspaceId/items/$dashboardId/updateDefinition")) {
        try { $r=Invoke-WebRequest -Uri $ep -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] Dashboard definition applied." -ForegroundColor Green; break } } catch {}
    }
}

Write-Host "=== Dashboard Deployment Complete ===" -ForegroundColor Cyan
