<#
.SYNOPSIS
    Deploy a Real-Time Intelligence KQL Dashboard for Healthcare telemetry.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$QueryServiceUri,
    [Parameter(Mandatory=$false)] [string]$DashboardName = "HealthcareTelemetryDashboard"
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
    @{ id=[guid]::NewGuid().ToString(); title="Patient Vitals Over Time"; layout=@{x=0;y=0;width=12;height=6}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="PatientVitals`n| summarize AvgHR = avg(HeartRateBPM), AvgSpO2 = avg(OxygenSaturation), AvgTemp = avg(TemperatureC) by bin(Timestamp, 15m), PatientId`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Clinical Alerts by Severity"; layout=@{x=12;y=0;width=6;height=6}; pageId=$pgId; visualType="pie"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ClinicalAlert`n| summarize Count = count() by Severity`n| order by Count desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Lab Results by Interpretation"; layout=@{x=18;y=0;width=6;height=6}; pageId=$pgId; visualType="pie"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="LabMetric`n| summarize Count = count() by Interpretation`n| order by Count desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Critical Patients - Latest Vitals"; layout=@{x=0;y=6;width=12;height=6}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="PatientVitals`n| summarize arg_max(Timestamp, *) by PatientId`n| where IsAnomaly == true or HeartRateBPM > 120 or OxygenSaturation < 92`n| project PatientId, WardId, Timestamp, HeartRateBPM, BloodPressureSystolic, OxygenSaturation, TemperatureC`n| order by Timestamp desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Medication Administration Timeline"; layout=@{x=12;y=6;width=12;height=6}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="MedicationEvent`n| summarize EventCount = count() by bin(Timestamp, 1h), Route`n| order by Timestamp asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Unacknowledged Clinical Alerts"; layout=@{x=0;y=12;width=12;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ClinicalAlert`n| where IsAcknowledged == false`n| project Timestamp, AlertId, PatientId, WardId, AlertType, Severity, MetricValue, ThresholdValue, Message`n| order by Timestamp desc`n| take 50" },
    @{ id=[guid]::NewGuid().ToString(); title="Lab Results Out of Range"; layout=@{x=12;y=12;width=12;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="LabMetric`n| where Interpretation in ('High', 'Critical High', 'Low')`n| project Timestamp, LabId, PatientId, TestType, ResultValue, Unit, ReferenceMin, ReferenceMax, Interpretation`n| order by Timestamp desc`n| take 50" },
    @{ id=[guid]::NewGuid().ToString(); title="Device Status Overview"; layout=@{x=0;y=17;width=8;height=5}; pageId=$pgId; visualType="table"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="DeviceReading`n| summarize arg_max(Timestamp, *) by DeviceId`n| project DeviceId, WardId, BatteryPercent, CalibrationStatus, Status, Timestamp`n| order by BatteryPercent asc" },
    @{ id=[guid]::NewGuid().ToString(); title="Alerts by Department"; layout=@{x=8;y=17;width=8;height=5}; pageId=$pgId; visualType="bar"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="ClinicalAlert`n| summarize AlertCount = count(), CriticalCount = countif(Severity == 'Critical') by DepartmentId`n| order by AlertCount desc" },
    @{ id=[guid]::NewGuid().ToString(); title="Patient Anomaly Timeline"; layout=@{x=16;y=17;width=8;height=5}; pageId=$pgId; visualType="line"; dataSourceId=$dsId; visualOptions=New-VO; usedParamVariables=@()
       query="PatientVitals`n| where IsAnomaly == true`n| summarize AnomalyCount = count() by bin(Timestamp, 1h)`n| order by Timestamp asc" }
)

$dashDef = @{ schema_version="52"; title=$DashboardName
    autoRefresh=@{enabled=$true;defaultInterval="30s";minInterval="10s"}
    dataSources=@(@{id=$dsId;name=$kqlDbName;clusterUri=$QueryServiceUri;database=$kqlDbName;kind="manual-kusto";scopeId="KustoDatabaseResource"})
    pages=@(@{id=$pgId;name="Healthcare Operations Overview"}); tiles=$tiles; parameters=@() }

$dashJson = $dashDef | ConvertTo-Json -Depth 15 -Compress
$dashB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dashJson))

$createBody = @{displayName=$DashboardName;type="KQLDashboard";description="Real-Time dashboard for healthcare patient monitoring and clinical operations"} | ConvertTo-Json -Depth 5
$dashboardId = $null
try {
    $resp = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($resp.StatusCode -eq 201) { $dashboardId = ($resp.Content|ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); if ($poll.status -eq 'Succeeded') { $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId=($allI|Where-Object{$_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard'}).id } }
} catch { $sr=$_.Exception.Response; if ($sr) { $stream=$sr.GetResponseStream(); $reader=New-Object System.IO.StreamReader($stream); $eb=$reader.ReadToEnd(); if ($eb -match 'ItemDisplayNameAlreadyInUse') { $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $dashboardId=($allI|Where-Object{$_.displayName -eq $DashboardName -and $_.type -eq 'KQLDashboard'}).id } } }

if ($dashboardId) {
    $updateBody = @{definition=@{parts=@(@{path="RealTimeDashboard.json";payload=$dashB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
    foreach ($ep in @("$apiBase/workspaces/$WorkspaceId/kqlDashboards/$dashboardId/updateDefinition","$apiBase/workspaces/$WorkspaceId/items/$dashboardId/updateDefinition")) {
        try { $r=Invoke-WebRequest -Uri $ep -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] Dashboard definition applied." -ForegroundColor Green; break } } catch {}
    }
}

Write-Host "=== Dashboard Deployment Complete ===" -ForegroundColor Cyan
