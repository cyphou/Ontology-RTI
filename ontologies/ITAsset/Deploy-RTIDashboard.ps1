<#
.SYNOPSIS
    Deploy a Real-Time Intelligence KQL Dashboard for IT Asset telemetry.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$DashboardName = "ITAssetDashboard"
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

function New-VO { return @{ xColumn=@{type="infer"}; yColumns=@{type="infer"}; yAxisMinimumValue=@{type="infer"}; yAxisMaximumValue=@{type="infer"}; seriesColumns=@{type="infer"}; hideLegend=$false; xColumnTitle=""; yColumnTitle=""; horizontalLine=""; verticalLine=""; xAxisScale="linear"; yAxisScale="linear"; crossFilterDisabled=$false; hideTileTitle=$false; multipleYAxes=@{base=@{id="-1";columns=@();label="";yAxisMinimumValue=$null;yAxisMaximumValue=$null;yAxisScale="linear";horizontalLines=@()};additional=@()} } }

$tiles = @(
    @{ id=[guid]::NewGuid().ToString(); title="Server CPU & Memory Over Time"; layout=@{x=0;y=0;width=12;height=6}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ServerMetric`n| summarize AvgCPU = avg(CPUPercent), AvgMemory = avg(MemoryPercent) by bin(Timestamp, 15m), ServerId`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Infrastructure Alerts by Severity"; layout=@{x=12;y=0;width=6;height=6}; pageId=$pgId; visualType="pie"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="InfraAlert`n| summarize Count = count() by Severity`n| order by Count desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Application Health Status"; layout=@{x=18;y=0;width=6;height=6}; pageId=$pgId; visualType="pie"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ApplicationHealth`n| summarize arg_max(Timestamp, *) by AppId`n| summarize Count = count() by HealthStatus`n| order by Count desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Top Servers by CPU Usage"; layout=@{x=0;y=6;width=12;height=6}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ServerMetric`n| summarize AvgCPU = round(avg(CPUPercent),1), MaxCPU = round(max(CPUPercent),1), AvgMem = round(avg(MemoryPercent),1), AvgDiskIOPS = avg(DiskIOPS) by ServerId, DataCenterId`n| top 20 by AvgCPU desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Network Bandwidth Utilization"; layout=@{x=12;y=6;width=12;height=6}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="NetworkMetric`n| summarize AvgBandwidth = avg(BandwidthUtilPct), AvgLatency = avg(LatencyMs) by bin(Timestamp, 15m), DeviceId`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Open Incidents"; layout=@{x=0;y=12;width=12;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="IncidentMetric`n| where Status == 'Open'`n| project IncidentId, ServerId, Severity, IncidentType, Timestamp, AffectedUsers, RootCause`n| order by Timestamp desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Application Response Time"; layout=@{x=12;y=12;width=12;height=5}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ApplicationHealth`n| summarize AvgResponseMs = avg(ResponseTimeMs), P95ResponseMs = percentile(ResponseTimeMs, 95) by bin(Timestamp, 15m), AppId`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Anomaly Servers"; layout=@{x=0;y=17;width=8;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ServerMetric`n| where IsAnomaly == true`n| project Timestamp, ServerId, DataCenterId, CPUPercent, MemoryPercent, DiskIOPS, NetworkMbps`n| order by Timestamp desc`n| take 50" },
    @{ id=[guid]::NewGuid().ToString(); title="Unacknowledged Alerts"; layout=@{x=8;y=17;width=8;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="InfraAlert`n| where IsAcknowledged == false`n| project Timestamp, AlertId, ServerId, DataCenterId, AlertType, Severity, MetricValue, ThresholdValue, Message`n| order by Timestamp desc`n| take 50" },
    @{ id=[guid]::NewGuid().ToString(); title="Incident Resolution Time by Severity"; layout=@{x=16;y=17;width=8;height=5}; pageId=$pgId; visualType="bar"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="IncidentMetric`n| where Status == 'Resolved'`n| summarize AvgDurationHours = round(avg(DurationHours),1), IncidentCount = count() by Severity`n| order by AvgDurationHours desc" }
)

$dashDef = @{ schema_version="52"; title=$DashboardName
    autoRefresh=@{enabled=$true;defaultInterval="30s";minInterval="10s"}
    dataSources=@(@{id=$dsId;name=$kqlDbName;clusterUri=$QueryServiceUri;database=$kqlDbName;kind="manual-kusto";scopeId="KustoDatabaseResource"})
    pages=@(@{id=$pgId;name="IT Infrastructure Overview"}); tiles=$tiles; parameters=@() }

$dashJson = $dashDef | ConvertTo-Json -Depth 15 -Compress
$dashB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashJson))

$createBody = @{displayName=$DashboardName;type="KQLDashboard";description="Real-Time dashboard for IT infrastructure monitoring"} | ConvertTo-Json -Depth 5
$dashboardId = $null
try {
    $resp = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($resp.StatusCode -eq 201) { $dashboardId = ($resp.Content|ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); if ($poll.status -eq 'Succeeded') { $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId=($allI|Where-Object{$_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard'}).id } }
} catch { $sr=$_.Exception.Response; if ($sr) { $stream=$sr.GetResponseStream(); $reader=New-Object System.IO.StreamReader($stream); $eb=$reader.ReadToEnd(); if ($eb -match 'ItemDisplayNameAlreadyInUse') { $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId=($allI|Where-Object{$_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard'}).id } } }

if ($dashboardId) {
    $updateBody = @{definition=@{parts=@(@{path="RealTimeDashboard.json";payload=$dashB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
    foreach ($ep in @("$apiBase/workspaces/$WorkspaceId/kqlDashboards/$dashboardId/updateDefinition","$apiBase/workspaces/$WorkspaceId/items/$dashboardId/updateDefinition")) {
        try { $r=Invoke-WebRequest -Uri $ep -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] Dashboard definition applied." -ForegroundColor Green; break } } catch { Write-Warning "Dashboard endpoint failed: $($_.Exception.Message)" }
    }
}

Write-Host "=== Dashboard Deployment Complete ===" -ForegroundColor Cyan
