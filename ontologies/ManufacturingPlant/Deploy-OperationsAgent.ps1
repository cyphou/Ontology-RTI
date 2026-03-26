<#
.SYNOPSIS
    Deploy a Fabric Operations Agent for Manufacturing Plant real-time monitoring.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "ManufacturingPlantOpsAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Operations Agent: $AgentName ===" -ForegroundColor Cyan

$allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
$existing = $allItems | Where-Object { $_.displayName -eq $AgentName }
if ($existing) { $agentId = $existing.id } else {
    $createBody = @{displayName=$AgentName;type="DataAgent";description="Operations monitoring agent for Manufacturing Plant"} | ConvertTo-Json -Depth 5
    try { $resp = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
        if ($resp.StatusCode -eq 201) { $agentId = ($resp.Content|ConvertFrom-Json).id }
        elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId=($allI|Where-Object{$_.displayName -eq $AgentName}).id }
    } catch { exit 1 }
}
if (-not $agentId) { exit 1 }

$fullInstructions = @"
== MANUFACTURING PLANT OPERATIONS AGENT ==

Goal 1: Production Efficiency
- Monitor OEE (Overall Equipment Effectiveness) per line. Target >85%.
- Track batch cycle times vs target. Alert when >10% slower.
- Identify lines with DefectRate >2% and escalate.
- Monitor energy consumption per unit produced.

Goal 2: Machine Health
- Track vibration levels: Warning >5 mm/s, Critical >8 mm/s.
- Monitor temperature vs operating limits per machine type.
- Current draw anomalies: >15% above baseline = potential motor issue.
- Predict maintenance needs from degrading sensor trends.

Goal 3: Quality Assurance
- Track first-pass yield by product and line.
- Alert on consecutive quality check failures (>2 in same batch).
- Monitor dimensional drift from spec center.
- Calculate Cpk (process capability) where sufficient data exists.

Goal 4: Safety & Alerts
- Prioritize Critical and High severity unacknowledged alerts.
- Track recurring alerts (same machine >3 in 24h = systematic issue).
- Monitor furnace/press temperatures vs safety limits.
- Escalate unresolved Critical alerts after 15 minutes.

Goal 5: Maintenance Optimization
- Track MTBF (Mean Time Between Failures) per machine.
- Open maintenance orders by priority and age.
- Calculate maintenance cost per machine monthly.
- Identify machines approaching scheduled service intervals.

== KQL TABLES ==
SensorReading (SensorId, MachineId, LineId, PlantId, Timestamp, ReadingValue, MeasurementUnit, SensorType, QualityFlag, IsAnomaly)
PlantAlert (AlertId, SensorId, MachineId, LineId, PlantId, Timestamp, AlertType, Severity, ReadingValue, ThresholdValue, Message, IsAcknowledged)
ProductionMetric (BatchId, LineId, PlantId, ProductId, Timestamp, QuantityProduced, DefectRate, EnergyUsedKWh, CycleTimeMinutes, OEEPercent)
MachineHealth (MachineId, LineId, PlantId, Timestamp, VibrationMmS, TemperatureC, CurrentAmps, SpeedRPM, OilPressureBar, Status)
QualityMetric (CheckId, BatchId, ProductId, LineId, Timestamp, TestType, Result, MeasuredValue, SpecMin, SpecMax, InspectorId)

Always include: timestamp, affected machine/line/plant, severity, recommended action. Use units.
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'))
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $fullInstructions + '"}'))

$updateBody = @{definition=@{parts=@(@{path="Files/Config/data_agent.json";payload=$dataAgentB64;payloadType="InlineBase64"},@{path="Files/Config/draft/stage_config.json";payload=$stageB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
try { $r = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] Operations Agent configured." -ForegroundColor Green } } catch { Write-Host "[WARN] $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host "=== Operations Agent Deployment Complete ===" -ForegroundColor Cyan
