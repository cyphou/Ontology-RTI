<#
.SYNOPSIS
    Deploy a Fabric Data Agent for the Manufacturing Plant Ontology.
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyId,
    [Parameter(Mandatory=$false)] [string]$AgentName = "ManufacturingPlantAgent"
)

$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{ "Authorization" = "Bearer $token"; "Content-Type" = "application/json" }

Write-Host "=== Deploying Data Agent: $AgentName ===" -ForegroundColor Cyan

$createBody = @{ displayName = $AgentName; description = "AI Data Agent for Manufacturing Plant operations. Answers questions about production, machines, quality, maintenance, and alerts." } | ConvertTo-Json -Depth 5
$agentId = $null
try {
    $resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
    if ($resp.StatusCode -eq 201) { $agentId = ($resp.Content | ConvertFrom-Json).id }
    elseif ($resp.StatusCode -eq 202) { $opUrl=$resp.Headers['Location']; do { Start-Sleep -Seconds 3; $poll=Invoke-RestMethod -Uri $opUrl -Headers $headers } while ($poll.status -notin @('Succeeded','Failed','Cancelled')); if ($poll.status -eq 'Succeeded') { $all=(Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $headers).value; $agentId=($all|Where-Object{$_.displayName -eq $AgentName -and $_.type -eq 'DataAgent'}).id } }
} catch { Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red; exit 1 }
if (-not $agentId) { exit 1 }

$aiInstructions = @"
You are an expert AI assistant for Manufacturing Plant Operations. Your data source is the ManufacturingPlantOntology.

== ONTOLOGY ENTITY TYPES (11 nodes) ==

1. Plant (Key: PlantId) - Name, Location, Country, CapacitySqFt, Shifts, YearBuilt, Status
2. ProductionLine (Key: LineId) - PlantId, LineName, LineType, CapacityUnitsPerHour, Status
3. Machine (Key: MachineId) - LineId, PlantId, MachineName, MachineType, Manufacturer, InstallDate, Status, CriticalityLevel
4. Product (Key: ProductId) - ProductName, Category, UnitWeight, SpecStandard
5. Material (Key: MaterialId) - MaterialName, MaterialType, UnitCost, Supplier, LeadTimeDays
6. Operator (Key: OperatorId) - OperatorName, Role, LineId, PlantId, CertificationLevel, ShiftPattern
7. Sensor (Key: SensorId) - SensorType, MachineId, MeasurementUnit, MinRange, MaxRange, Status
   Timeseries: Timestamp, ReadingValue, QualityFlag, IsAnomaly
8. MaintenanceOrder (Key: OrderId) - MachineId, OrderType, Priority, AssignedOperatorId, StartDate, EndDate, CostUSD, Status
9. QualityCheck (Key: CheckId) - BatchId, ProductId, TestType, Result, MeasuredValue, SpecMin, SpecMax, InspectorId
10. ProductionBatch (Key: BatchId) - LineId, ProductId, StartTime, EndTime, QuantityProduced, DefectRate, EnergyUsedKWh, Status
11. Alert (Key: AlertId) - SensorId, AlertType, Severity, Timestamp, ReadingValue, ThresholdValue, IsAcknowledged

== RELATIONSHIPS (11 edges) ==
PlantHasLine, LineHasMachine, MachineHasSensor, LineHasOperator, MachineHasMaintenance, MaintenanceByOperator, LineProducesBatch, BatchHasQualityCheck, BatchOfProduct, ProductUsesMaterial, AlertFromSensor

== GUIDELINES ==
1. Navigate Plant -> Line -> Machine -> Sensor for equipment telemetry.
2. OEE = Availability * Performance * Quality. Target >85%.
3. DefectRate: Flag batches >2%. Track by product and line.
4. Machine criticality levels: Critical (stop line), High (degrade), Medium, Low.
5. Vibration >5 mm/s = bearing wear warning. >8 mm/s = critical.
6. Include units: C, mm/s, Amps, RPM, kWh, bar, tons.
7. Quality: compare MeasuredValue vs [SpecMin, SpecMax].
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

$dataAgentB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'))
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $aiInstructions + '"}'))

$updateBody = @{definition=@{parts=@(@{path="Files/Config/data_agent.json";payload=$dataAgentB64;payloadType="InlineBase64"},@{path="Files/Config/draft/stage_config.json";payload=$stageB64;payloadType="InlineBase64"})}} | ConvertTo-Json -Depth 10
try { $updResp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents/$agentId/updateDefinition" -Method POST -Headers $headers -Body $updateBody -UseBasicParsing; if ($updResp.StatusCode -in @(200,202)) { Write-Host "[OK] AI instructions configured." -ForegroundColor Green } } catch { Write-Host "[WARN] $($_.Exception.Message)" -ForegroundColor Yellow }

Write-Host "=== Data Agent Deployment Complete ===" -ForegroundColor Cyan
