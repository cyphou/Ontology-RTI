<#
.SYNOPSIS
    Deploy a Fabric Data Agent for the Healthcare Ontology.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "HealthcareDataAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

Write-Host "=== Deploying Data Agent: $AgentName ===" -ForegroundColor Cyan

$createBody = @{ displayName=$AgentName; description="AI Data Agent for Healthcare. Answers questions about hospitals, departments, patients, physicians, medications, lab results, and clinical alerts." } | ConvertTo-Json -Depth 5
$agentId = $null
try {
    $resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($resp.StatusCode -eq 201) { $agentId = ($resp.Content|ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); if ($poll.status -eq 'Succeeded') { $all=(Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId=($all|Where-Object{$_.displayName -eq $AgentName -and $_.type -eq 'DataAgent'}).id } }
} catch { Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red; exit 1 }
if (-not $agentId) { exit 1 }

$aiInstructions = @"
You are an expert AI assistant for Healthcare Operations. Your data source is the HealthcareOntology.

== ONTOLOGY ENTITY TYPES (9 nodes) ==

1. Hospital (Key: HospitalId) - HospitalName, City, State, Country, BedCapacity, TierLevel, Status
2. Department (Key: DepartmentId) - HospitalId, DepartmentName, DepartmentType, Floor, BedCount, Status
3. Ward (Key: WardId) - DepartmentId, WardName, WardType, BedCount, NurseStations, Status
   Timeseries: Timestamp, HeartRateBPM, BloodPressureSystolic, OxygenSaturation, TemperatureC
4. Physician (Key: PhysicianId) - DepartmentId, PhysicianName, Specialty, LicenseNumber, YearsExperience, Status
5. Nurse (Key: NurseId) - WardId, NurseName, Certification, ShiftPreference, YearsExperience, Status
6. Patient (Key: PatientId) - WardId, PatientName, DateOfBirth, Gender, BloodType, InsuranceProvider, AdmissionDate, Status
7. MedicalDevice (Key: DeviceId) - WardId, DeviceName, DeviceType, Manufacturer, Model, LastCalibrationDate, Status
8. Medication (Key: MedicationId) - MedicationName, Category, DosageForm, Manufacturer, UnitCost, RequiresRefrigeration, Status
9. Sensor (Key: SensorId) - DeviceId, SensorName, SensorType, Unit, MinThreshold, MaxThreshold, Status

== RELATIONSHIPS (7 edges) ==
HospitalHasDepartment, DepartmentHasWard, DepartmentHasPhysician, WardHasNurse, WardHasPatient, WardHasDevice, DeviceHasSensor

== GUIDELINES ==
1. Navigate Hospital -> Department -> Ward -> Patient for care hierarchy.
2. Heart Rate: >120 bpm = Tachycardia, <50 bpm = Bradycardia.
3. SpO2: <92% = Hypoxemia (Warning), <88% = Critical. Normal 95-100%.
4. Blood Pressure: Systolic >140 mmHg = Hypertension, <90 mmHg = Hypotension.
5. Temperature: >38.0C = Febrile, >39.5C = High Fever. Normal 36.5-37.5C.
6. Lab results: compare ResultValue with ReferenceRange for interpretation.
7. Medication: track administration routes (IV, Oral, SubQ, IM).
8. Device calibration: flag devices with LastCalibrationDate > 90 days.
9. Include units: bpm, mmHg, C, %, mL/hr, mg/dL, mEq/L.
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'))
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $aiInstructions + '"}'))

$updateBody = @{definition=@{parts=@(@{path="Files/Config/data_agent.json";payload=$dataAgentB64;payloadType="InlineBase64"},@{path="Files/Config/draft/stage_config.json";payload=$stageB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
try { $r = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($r.StatusCode -in @(200,202)) { Write-Host "[OK] AI instructions configured." -ForegroundColor Green } } catch { Write-Host "[WARN] $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host "=== Data Agent Deployment Complete ===" -ForegroundColor Cyan
