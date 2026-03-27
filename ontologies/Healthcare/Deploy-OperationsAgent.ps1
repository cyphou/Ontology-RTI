<#
.SYNOPSIS
    Deploy a Fabric Operations Agent for Healthcare real-time monitoring.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "HealthcareOpsAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Operations Agent: $AgentName ===" -ForegroundColor Cyan

$allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
$existing = $allItems | Where-Object { $_.displayName -eq $AgentName }
if ($existing) { $agentId = $existing.id } else {
    $createBody = @{displayName=$AgentName;type="DataAgent";description="Operations monitoring agent for healthcare clinical systems"} | ConvertTo-Json -Depth 5
    try { $resp = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
        if ($resp.StatusCode -eq 201) { $agentId = ($resp.Content|ConvertFrom-Json).id }
        elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); $allI=(Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId=($allI|Where-Object{$_.displayName -eq $AgentName}).id }
    } catch { exit 1 }
}
if (-not $agentId) { exit 1 }

$fullInstructions = @"
== HEALTHCARE OPERATIONS AGENT ==

Goal 1: Patient Vital Signs Monitoring
- Track HeartRate, BP, SpO2, Temperature per patient in real-time.
- Alert thresholds: HR >120 Critical, >100 Warning. SpO2 <92% Warning, <88% Critical.
- BP Systolic >140 Hypertension, <90 Hypotension.
- Temperature >38.0C Febrile, >39.5C High Fever.
- Identify patients with multiple anomalies in the last hour.

Goal 2: Clinical Alert Management
- Track unacknowledged alerts by severity and department.
- Escalate critical alerts unacknowledged after 15 minutes.
- Identify wards with >3 active critical alerts.
- Monitor alert patterns: repeated alerts for same patient = deterioration.

Goal 3: Lab Results Monitoring
- Flag critical lab values outside reference ranges.
- Track turnaround time from order to result.
- Identify patients with multiple abnormal results = clinical concern.
- Key labs: Troponin (cardiac), Lactate (sepsis), WBC (infection), Creatinine (renal).

Goal 4: Medication Safety
- Track medication administration timing and compliance.
- Flag adverse reactions immediately.
- Monitor high-risk medications: vasopressors, anticoagulants, controlled substances.
- Verify nurse-to-patient ratios in medication-heavy wards.

Goal 5: Medical Device Management
- Track device battery levels: <20% = Critical, <50% = Warning.
- Monitor calibration status: flag overdue calibrations (>90 days).
- Identify devices offline or in error state.
- Ward-level device availability reporting.

== KQL TABLES ==
PatientVitals (PatientId, WardId, DepartmentId, Timestamp, HeartRateBPM, BloodPressureSystolic, BloodPressureDiastolic, TemperatureC, OxygenSaturation, RespiratoryRate, QualityFlag, IsAnomaly)
ClinicalAlert (AlertId, PatientId, WardId, DepartmentId, Timestamp, AlertType, Severity, MetricValue, ThresholdValue, Message, IsAcknowledged)
LabMetric (LabId, PatientId, PhysicianId, Timestamp, TestType, ResultValue, Unit, ReferenceMin, ReferenceMax, Interpretation)
MedicationEvent (EventId, PatientId, MedicationId, NurseId, Timestamp, Dosage, Route, AdverseReaction, Status)
DeviceReading (ReadingId, DeviceId, WardId, Timestamp, MetricType, MetricValue, BatteryPercent, CalibrationStatus, Status)

Always include: timestamp, affected patient/ward/department, severity, recommended clinical action. Use units.
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'))
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $fullInstructions + '"}'))

$updateBody = @{definition=@{parts=@(@{path="Files/Config/data_agent.json";payload=$dataAgentB64;payloadType="InlineBase64"},@{path="Files/Config/draft/stage_config.json";payload=$stageB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
try { $r = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] Operations Agent configured." -ForegroundColor Green } } catch { Write-Host "[WARN] $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host "=== Operations Agent Complete ===" -ForegroundColor Cyan
