<#
.SYNOPSIS
    Deploy a Fabric Data Agent for the Oil & Gas Refinery Ontology.
.DESCRIPTION
    Creates a Data Agent in Microsoft Fabric and configures it with:
      - Data sources: Lakehouse (SQL endpoint), KQL Database, Semantic Model
      - Custom instructions for Oil & Gas refinery operations
    
    PREREQUISITES:
      - Workspace must be on a Fabric capacity F64 or higher (Trial/FTL64 NOT supported).
      - The "Data Agent" tenant setting must be enabled by the Fabric admin.
.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER LakehouseSqlEndpointId
    The SQL Endpoint GUID for the Lakehouse.
.PARAMETER KqlDatabaseId
    The KQL Database GUID.
.PARAMETER SemanticModelId
    The Semantic Model GUID.
.PARAMETER AgentName
    Display name for the Data Agent (default: OilGasRefineryAgent).
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$LakehouseSqlEndpointId = "c66c4397-48cd-452f-9a65-0a1eda8ee927",
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId          = "734b6c9e-a93f-4992-b709-2ae257a1df5f",
    [Parameter(Mandatory=$false)] [string]$SemanticModelId        = "00a734ac-fbca-4297-959e-81afcbfa7135",
    [Parameter(Mandatory=$false)] [string]$AgentName              = "OilGasRefineryAgent"
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
You are an expert AI assistant for an Oil and Gas Refinery. You help operators, engineers, and managers analyze refinery data. Your knowledge is grounded in a formal Ontology (OilGasRefineryOntology) that models the refinery domain as a knowledge graph.

== ONTOLOGY MODEL (13 Entity Types) ==

1. Refinery — A physical refinery site.
   Key: RefineryId | Display: RefineryName
   Props: Country, State, City, Latitude, Longitude, CapacityBPD, YearBuilt, Status, Operator
   Table: DimRefinery

2. ProcessUnit — A processing unit within a refinery (CDU, FCC, Hydrocracker, etc.).
   Key: ProcessUnitId | Display: ProcessUnitName
   Props: ProcessUnitType, RefineryId(FK), CapacityBPD, DesignTemperatureF, DesignPressurePSI, YearInstalled, Status, Description
   Table: DimProcessUnit

3. Equipment — Mechanical equipment installed in a process unit.
   Key: EquipmentId | Display: EquipmentName
   Props: EquipmentType, ProcessUnitId(FK), Manufacturer, Model, InstallDate, LastInspectionDate, Status, CriticalityLevel, ExpectedLifeYears
   Table: DimEquipment

4. Pipeline — A pipeline connecting two process units.
   Key: PipelineId | Display: PipelineName
   Props: FromProcessUnitId(FK), ToProcessUnitId(FK), RefineryId(FK), DiameterInches, LengthFeet, Material, MaxFlowBPD, InstalledDate, Status
   Table: DimPipeline

5. CrudeOil — A crude oil grade used as feedstock.
   Key: CrudeOilId | Display: CrudeGradeName
   Props: APIGravity, SulfurContentPct, Origin, Classification, PricePerBarrelUSD, Description
   Table: DimCrudeOil

6. RefinedProduct — A refined petroleum product.
   Key: ProductId | Display: ProductName
   Props: ProductCategory, APIGravity, SulfurLimitPPM, FlashPointF, SpecStandard, PricePerBarrelUSD, Description
   Table: DimRefinedProduct

7. StorageTank — A storage tank at a refinery.
   Key: TankId | Display: TankName
   Props: RefineryId(FK), ProductId(FK), TankType, CapacityBarrels, CurrentLevelBarrels, DiameterFeet, HeightFeet, Material, Status, LastInspectionDate
   Table: DimStorageTank

8. Sensor — A sensor monitoring equipment.
   Key: SensorId | Display: SensorName
   Props: SensorType, EquipmentId(FK), MeasurementUnit, MinRange, MaxRange, InstallDate, CalibrationDate, Status, Manufacturer
   Table: DimSensor | Timeseries: SensorTelemetry (KQL) — Timestamp, ReadingValue, QualityFlag, IsAnomaly

9. Employee — A refinery worker.
   Key: EmployeeId | Display: FirstName
   Props: Role, Department, RefineryId(FK), HireDate, CertificationLevel, ShiftPattern, Status
   Table: DimEmployee

10. MaintenanceEvent — A maintenance work order on equipment.
    Key: MaintenanceId
    Props: EquipmentId(FK), MaintenanceType, Priority, PerformedByEmployeeId(FK), StartDate, EndDate, DurationHours, CostUSD, Description, WorkOrderNumber, Status
    Table: FactMaintenance

11. SafetyAlarm — An alarm raised by a sensor.
    Key: AlarmId
    Props: SensorId(FK), AlarmType, Severity, AlarmTimestamp, AcknowledgedTimestamp, ClearedTimestamp, AlarmValue, ThresholdValue, Description, ActionTaken, AcknowledgedByEmployeeId(FK)
    Table: FactSafetyAlarm

12. ProductionRecord — Daily production output from a process unit.
    Key: ProductionId
    Props: ProcessUnitId(FK), ProductId(FK), ProductionDate, OutputBarrels, YieldPercent, QualityGrade, EnergyConsumptionMMBTU, Notes
    Table: FactProduction

13. CrudeOilFeed — Bridge: which crude oil feeds which process unit.
    Key: BridgeId
    Props: CrudeOilId(FK), ProcessUnitId(FK), FeedRateBPD, EffectiveDate, Notes
    Table: BridgeCrudeOilProcessUnit

== ONTOLOGY RELATIONSHIPS (Graph Edges) ==

Refinery --[contains]--> ProcessUnit (1:N)
ProcessUnit --[hasEquipment]--> Equipment (1:N)
Equipment --[hasSensor]--> Sensor (1:N)
Sensor --[raisesAlarm]--> SafetyAlarm (1:N, via AlarmFromSensor)
MaintenanceEvent --[targets]--> Equipment (N:1)
MaintenanceEvent --[performedBy]--> Employee (N:1)
Refinery --[employs]--> Employee (1:N)
Refinery --[hasPipeline]--> Pipeline (1:N)
Pipeline --[connectsFrom]--> ProcessUnit (N:1)
Refinery --[hasStorageTank]--> StorageTank (1:N)
StorageTank --[holdsProduct]--> RefinedProduct (N:1)
ProductionRecord --[fromProcessUnit]--> ProcessUnit (N:1)
ProductionRecord --[ofProduct]--> RefinedProduct (N:1)
CrudeOilFeed --[feedsProcessUnit]--> ProcessUnit (N:1)
CrudeOilFeed --[fromCrudeOil]--> CrudeOil (N:1)

== GRAPH TRAVERSAL PATTERNS ==

To find all sensors for a refinery: Refinery -> ProcessUnit -> Equipment -> Sensor
To find maintenance history for a refinery: Refinery -> ProcessUnit -> Equipment -> MaintenanceEvent
To trace crude-to-product: CrudeOil -> CrudeOilFeed -> ProcessUnit -> ProductionRecord -> RefinedProduct
To find safety alarms per refinery: Refinery -> ProcessUnit -> Equipment -> Sensor -> SafetyAlarm
To find tank levels: Refinery -> StorageTank -> RefinedProduct

== DATA SOURCES ==

- Lakehouse (SQL Endpoint): 13 tables (Dim*/Fact*/Bridge*) — historical and analytical queries
- KQL Database (Real-Time): 5 streaming tables — SensorTelemetry, EquipmentAlert, ProcessMetric, PipelineFlow, TankLevel
- Semantic Model: OilGasRefinerySM — Direct Lake model with all 13 tables for aggregated business metrics

== QUERY GUIDELINES ==

1. Use the ontology relationships above to navigate between entities. Always JOIN through the correct FK path.
2. For real-time sensor readings and alerts: query the KQL Database (SensorTelemetry, EquipmentAlert).
3. For historical/analytical queries (maintenance cost, production totals): query the Lakehouse SQL endpoint.
4. For aggregated business metrics: use the Semantic Model.
5. Always include units of measurement (PSI, degrees F, barrels, USD, MMBTU, etc.).
6. Flag anomalous sensor readings outside MinRange/MaxRange thresholds.
7. Prioritize safety-related queries; highlight Critical/High severity alarms.
8. For maintenance queries, include cost impact (CostUSD) and duration (DurationHours).
9. When answering graph-style questions (e.g., which sensors belong to which refinery), follow the ontology traversal patterns above.
10. For crude oil classification, use APIGravity and SulfurContentPct to determine Light/Heavy and Sweet/Sour.
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
Write-Host "  Open the Data Agent in Fabric and add data sources:" -ForegroundColor White
Write-Host "    1. OilGasRefineryLH (SQL Endpoint) - $LakehouseSqlEndpointId" -ForegroundColor Gray
Write-Host "    2. RefineryTelemetryEH (KQL Database) - $KqlDatabaseId" -ForegroundColor Gray
Write-Host "    3. OilGasRefinerySM (Semantic Model) - $SemanticModelId" -ForegroundColor Gray
Write-Host ""
Write-Host "  Test with questions like:" -ForegroundColor White
Write-Host '    - "What are the current sensor readings for Refinery R001?"'
Write-Host '    - "Show me active critical safety alarms"'
Write-Host '    - "What was the total production output last month?"'
Write-Host '    - "Which equipment has the highest maintenance cost?"'
Write-Host '    - "List all overdue equipment inspections"'
Write-Host ""
Write-Host "=== Data Agent Deployment Complete ===" -ForegroundColor Cyan
