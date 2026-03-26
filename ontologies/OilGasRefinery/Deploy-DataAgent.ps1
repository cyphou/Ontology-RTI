<#
.SYNOPSIS
    Deploy a Fabric Data Agent for the Oil & Gas Refinery Ontology.
.DESCRIPTION
    Creates a Data Agent in Microsoft Fabric and configures it with:
      - Data source: OilGasRefineryOntology (which binds to Lakehouse + KQL via the GraphModel)
      - Custom AI instructions grounded in the ontology knowledge graph
    
    PREREQUISITES:
      - Workspace must be on a Fabric capacity F64 or higher (Trial/FTL64 NOT supported).
      - The "Data Agent" tenant setting must be enabled by the Fabric admin.
      - The OilGasRefineryOntology must already be deployed in the workspace.
.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER OntologyId
    The Ontology item GUID (default: e7facc37).
.PARAMETER AgentName
    Display name for the Data Agent (default: OilGasRefineryAgent).
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyId = "e7facc37-0bc8-4c69-b40a-4cee32ef6474",
    [Parameter(Mandatory=$false)] [string]$AgentName  = "OilGasRefineryAgent"
)

# ── Authentication ──────────────────────────────────────────────────────────
$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}

Write-Host "=== Deploying Data Agent: $AgentName ===" -ForegroundColor Cyan

# ── Step 1: Create the Data Agent ──────────────────────────────────────────
$createBody = @{
    displayName = $AgentName
    description = "AI Data Agent for Oil & Gas Refinery operations. Answers questions about production, equipment, sensors, maintenance, safety, and environmental data."
} | ConvertTo-Json -Depth 5

Write-Host "Creating Data Agent..." -ForegroundColor Yellow
$agentId = $null
try {
    $response = Invoke-WebRequest `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents" `
        -Method POST -Headers $headers -Body $createBody -UseBasicParsing

    if ($response.StatusCode -eq 201) {
        $agent = $response.Content | ConvertFrom-Json
        $agentId = $agent.id
        Write-Host "[OK] Data Agent created: $agentId" -ForegroundColor Green
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
            # Retrieve the created item
            $resultUrl = $opUrl -replace '/operations/.*', "/result"
            try {
                $agentResult = Invoke-RestMethod -Uri $resultUrl -Headers $headers
                $agentId = $agentResult.id
            } catch {
                # Fallback: list items to find it
                $allItems = (Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Headers $headers).value
                $agentItem = $allItems | Where-Object { $_.displayName -eq $AgentName -and $_.type -eq 'DataAgent' }
                $agentId = $agentItem.id
            }
            Write-Host "[OK] Data Agent created: $agentId" -ForegroundColor Green
        } else {
            Write-Host "[FAIL] Data Agent creation $($poll.status)" -ForegroundColor Red
            exit 1
        }
    }
}
catch {
    $sr = $_.Exception.Response
    if ($sr) {
        $stream = $sr.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $errBody = $reader.ReadToEnd()
        Write-Host "[ERROR] $([int]$sr.StatusCode): $errBody" -ForegroundColor Red

        if ($errBody -match 'UnsupportedCapacitySKU') {
            Write-Host ""
            Write-Host ">>> Data Agents require Fabric capacity F64 or higher." -ForegroundColor Magenta
            Write-Host ">>> Current workspace is on Trial (FTL64) which is not supported." -ForegroundColor Magenta
            Write-Host ">>> Move the workspace to an F64+ capacity and re-run this script." -ForegroundColor Magenta
        }
        elseif ($errBody -match 'FeatureNotAvailable') {
            Write-Host ""
            Write-Host ">>> The 'Data Agent' tenant setting must be enabled by your Fabric admin." -ForegroundColor Magenta
        }
    }
    else {
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    }
    exit 1
}

if (-not $agentId) {
    Write-Host "[ERROR] Could not retrieve Data Agent ID." -ForegroundColor Red
    exit 1
}

# ── Step 2: Update Data Agent Definition ───────────────────────────────────
# The definition uses the correct schema discovered from getDefinition:
#   Files/Config/data_agent.json     - main config (schema reference)
#   Files/Config/draft/stage_config.json - AI instructions
Write-Host "Configuring Data Agent AI instructions..." -ForegroundColor Yellow

$aiInstructions = @"
You are an expert AI assistant for an Oil and Gas Refinery. Your sole data source is the OilGasRefineryOntology — a Fabric Ontology that models the entire refinery domain as a knowledge graph. The ontology unifies data from a Lakehouse (13 dimension/fact tables) and a KQL Database (5 real-time streaming tables) through typed entity bindings.

== YOUR DATA SOURCE ==

You query the OilGasRefineryOntology, which exposes entities and relationships as a graph. The ontology handles all underlying data access — you do NOT query Lakehouse SQL or KQL directly. Instead, you navigate the graph.

== ONTOLOGY ENTITY TYPES (13 nodes) ==

1. Refinery (Key: RefineryId, Display: RefineryName)
   Country, State, City, Latitude, Longitude, CapacityBPD, YearBuilt, Status, Operator

2. ProcessUnit (Key: ProcessUnitId, Display: ProcessUnitName)
   ProcessUnitType, RefineryId, CapacityBPD, DesignTemperatureF, DesignPressurePSI, YearInstalled, Status, Description

3. Equipment (Key: EquipmentId, Display: EquipmentName)
   EquipmentType, ProcessUnitId, Manufacturer, Model, InstallDate, LastInspectionDate, Status, CriticalityLevel, ExpectedLifeYears

4. Pipeline (Key: PipelineId, Display: PipelineName)
   FromProcessUnitId, ToProcessUnitId, RefineryId, DiameterInches, LengthFeet, Material, MaxFlowBPD, InstalledDate, Status

5. CrudeOil (Key: CrudeOilId, Display: CrudeGradeName)
   APIGravity, SulfurContentPct, Origin, Classification, PricePerBarrelUSD, Description

6. RefinedProduct (Key: ProductId, Display: ProductName)
   ProductCategory, APIGravity, SulfurLimitPPM, FlashPointF, SpecStandard, PricePerBarrelUSD, Description

7. StorageTank (Key: TankId, Display: TankName)
   RefineryId, ProductId, TankType, CapacityBarrels, CurrentLevelBarrels, DiameterFeet, HeightFeet, Material, Status, LastInspectionDate

8. Sensor (Key: SensorId, Display: SensorName)
   SensorType, EquipmentId, MeasurementUnit, MinRange, MaxRange, InstallDate, CalibrationDate, Status, Manufacturer
   Timeseries: Timestamp, ReadingValue, QualityFlag, IsAnomaly

9. Employee (Key: EmployeeId, Display: FirstName)
   Role, Department, RefineryId, HireDate, CertificationLevel, ShiftPattern, Status

10. MaintenanceEvent (Key: MaintenanceId)
    EquipmentId, MaintenanceType, Priority, PerformedByEmployeeId, StartDate, EndDate, DurationHours, CostUSD, Description, WorkOrderNumber, Status

11. SafetyAlarm (Key: AlarmId)
    SensorId, AlarmType, Severity, AlarmTimestamp, AcknowledgedTimestamp, ClearedTimestamp, AlarmValue, ThresholdValue, Description, ActionTaken, AcknowledgedByEmployeeId

12. ProductionRecord (Key: ProductionId)
    ProcessUnitId, ProductId, ProductionDate, OutputBarrels, YieldPercent, QualityGrade, EnergyConsumptionMMBTU, Notes

13. CrudeOilFeed (Key: BridgeId)
    CrudeOilId, ProcessUnitId, FeedRateBPD, EffectiveDate, Notes

== ONTOLOGY RELATIONSHIPS (15 graph edges) ==

RefineryHasProcessUnit:      Refinery --> ProcessUnit (1:N)
ProcessUnitHasEquipment:     ProcessUnit --> Equipment (1:N)
EquipmentHasSensor:          Equipment --> Sensor (1:N)
AlarmFromSensor:             SafetyAlarm --> Sensor (N:1)
MaintenanceOnEquipment:      MaintenanceEvent --> Equipment (N:1)
MaintenanceByEmployee:       MaintenanceEvent --> Employee (N:1)
RefineryHasEmployee:         Refinery --> Employee (1:N)
RefineryHasPipeline:         Refinery --> Pipeline (1:N)
PipelineFromProcessUnit:     Pipeline --> ProcessUnit (N:1)
RefineryHasStorageTank:      Refinery --> StorageTank (1:N)
StorageTankHoldsProduct:     StorageTank --> RefinedProduct (N:1)
ProductionFromProcessUnit:   ProductionRecord --> ProcessUnit (N:1)
ProductionOfProduct:         ProductionRecord --> RefinedProduct (N:1)
CrudeFeedToProcessUnit:      CrudeOilFeed --> ProcessUnit (N:1)
CrudeFeedFromCrudeOil:       CrudeOilFeed --> CrudeOil (N:1)

== GRAPH TRAVERSAL PATTERNS ==

All sensors for a refinery:      Refinery -> ProcessUnit -> Equipment -> Sensor
Maintenance history per site:    Refinery -> ProcessUnit -> Equipment -> MaintenanceEvent
Crude-to-product traceability:   CrudeOil -> CrudeOilFeed -> ProcessUnit -> ProductionRecord -> RefinedProduct
Safety alarms per refinery:      Refinery -> ProcessUnit -> Equipment -> Sensor -> SafetyAlarm
Tank inventory:                  Refinery -> StorageTank -> RefinedProduct
Employee workload:               Employee -> MaintenanceEvent -> Equipment -> ProcessUnit -> Refinery

== GUIDELINES ==

1. Navigate the ontology graph using the relationships above. Do not assume direct joins between non-adjacent entities.
2. Sensor entity has timeseries data (Timestamp, ReadingValue, QualityFlag, IsAnomaly) for real-time telemetry.
3. Always include units of measurement (PSI, degrees F, barrels, USD, MMBTU, PPM, etc.).
4. Flag anomalous sensor readings where ReadingValue is outside [MinRange, MaxRange].
5. Prioritize safety: highlight Critical/High severity alarms immediately.
6. For maintenance questions, include CostUSD, DurationHours, and Priority.
7. For crude oil classification, use APIGravity (light >31, heavy <22) and SulfurContentPct (sweet <0.5%, sour >0.5%).
8. For production efficiency, compute YieldPercent and EnergyConsumptionMMBTU per barrel.
9. Use CriticalityLevel on Equipment to triage maintenance and alarm responses.
10. The ontology IS the single source of truth — all entities and their data are accessed through it.
"@ -replace "`r`n", "\n" -replace "`n", "\n" -replace '"', '\"'

# Both parts MUST be sent together for updateDefinition to succeed
$dataAgentJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"}'
$dataAgentB64  = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($dataAgentJson))

$stageConfigJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json","aiInstructions":"' + $aiInstructions + '"}'
$stageB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stageConfigJson))

$updateBody = @{
    definition = @{
        parts = @(
            @{
                path        = "Files/Config/data_agent.json"
                payload     = $dataAgentB64
                payloadType = "InlineBase64"
            },
            @{
                path        = "Files/Config/draft/stage_config.json"
                payload     = $stageB64
                payloadType = "InlineBase64"
            }
        )
    }
} | ConvertTo-Json -Depth 10

try {
    $updateResponse = Invoke-WebRequest `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents/$agentId/updateDefinition" `
        -Method POST -Headers $headers -Body $updateBody -UseBasicParsing

    if ($updateResponse.StatusCode -in @(200,202)) {
        Write-Host "[OK] Data Agent AI instructions configured." -ForegroundColor Green

        if ($updateResponse.StatusCode -eq 202) {
            $opUrl2 = $updateResponse.Headers['Location']
            Write-Host "  LRO started, polling..." -ForegroundColor Yellow
            do {
                Start-Sleep -Seconds 3
                $poll2 = Invoke-RestMethod -Uri $opUrl2 -Headers $headers
                Write-Host "  Status: $($poll2.status)"
            } while ($poll2.status -notin @('Succeeded','Failed','Cancelled'))

            if ($poll2.status -eq 'Succeeded') {
                Write-Host "[OK] Definition update succeeded." -ForegroundColor Green
            } else {
                Write-Host "[WARN] Definition LRO status: $($poll2.status)" -ForegroundColor Yellow
                Write-Host "  AI instructions can be set manually in the Fabric UI." -ForegroundColor Yellow
            }
        }
    }
}
catch {
    $sr2 = $_.Exception.Response
    if ($sr2) {
        $s2 = $sr2.GetResponseStream()
        $rd2 = New-Object System.IO.StreamReader($s2)
        Write-Host "[WARN] Definition update: $([int]$sr2.StatusCode): $($rd2.ReadToEnd())" -ForegroundColor Yellow
        Write-Host "  The Data Agent was created. Configure AI instructions in the UI." -ForegroundColor Yellow
    }
    else {
        Write-Host "[WARN] Definition update: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# ── Step 3: Verify ─────────────────────────────────────────────────────────
Write-Host "Verifying Data Agent definition..." -ForegroundColor Yellow
try {
    $verifyResp = Invoke-WebRequest `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/dataAgents/$agentId/getDefinition" `
        -Method POST -Headers $headers -UseBasicParsing

    $verifyParts = $null
    if ($verifyResp.StatusCode -eq 202) {
        $vLoc = $verifyResp.Headers['Location']
        do { Start-Sleep -Seconds 2; $vPoll = Invoke-RestMethod -Uri $vLoc -Headers $headers } while ($vPoll.status -notin @('Succeeded','Failed'))
        if ($vPoll.status -eq 'Succeeded') {
            $vResult = Invoke-RestMethod -Uri "$vLoc/result" -Headers $headers
            $verifyParts = $vResult.definition.parts
        }
    } else {
        $verifyParts = ($verifyResp.Content | ConvertFrom-Json).definition.parts
    }

    if ($verifyParts) {
        $verifyDef  = $verifyParts | Where-Object { $_.path -eq 'Files/Config/draft/stage_config.json' }
        $verifyJson = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($verifyDef.payload)) | ConvertFrom-Json

        if ($verifyJson.aiInstructions -and $verifyJson.aiInstructions.Length -gt 0) {
            Write-Host "[OK] AI instructions verified ($($verifyJson.aiInstructions.Length) chars)." -ForegroundColor Green
        } else {
            Write-Host "[INFO] AI instructions are empty. Configure them in the Fabric UI." -ForegroundColor Yellow
        }
    }
} catch {
    Write-Host "[INFO] Could not verify definition: $($_.Exception.Message)" -ForegroundColor Yellow
}

# ── Summary ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Data Agent Deployment Summary ===" -ForegroundColor Cyan
Write-Host "  Name:       $AgentName"
Write-Host "  Agent ID:   $agentId"
Write-Host "  Workspace:  $WorkspaceId"
Write-Host ""
Write-Host "  MANUAL CONFIGURATION:" -ForegroundColor Yellow
Write-Host "  Open the Data Agent in Fabric and add the Ontology as data source:" -ForegroundColor White
Write-Host "    OilGasRefineryOntology - $OntologyId" -ForegroundColor Gray
Write-Host ""
Write-Host "  The Ontology already binds to Lakehouse (13 tables) and KQL (5 streaming tables)." -ForegroundColor Gray
Write-Host "  No need to add individual data sources separately." -ForegroundColor Gray
Write-Host ""
Write-Host "  Test with questions like:" -ForegroundColor White
Write-Host '    - "Which sensors belong to Refinery R001?"'
Write-Host '    - "Show all critical safety alarms and which equipment they affect"'
Write-Host '    - "Trace crude oil Arab Light through to refined products"'
Write-Host '    - "What is the maintenance cost for equipment in the FCC unit?"'
Write-Host '    - "List all storage tanks below 30% capacity"'
Write-Host ""
Write-Host "=== Data Agent Deployment Complete ===" -ForegroundColor Cyan
