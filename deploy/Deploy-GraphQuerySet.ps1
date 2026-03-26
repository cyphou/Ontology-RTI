<#
.SYNOPSIS
    Deploy a Graph Query Set for any IQ Ontology domain.
.DESCRIPTION
    Creates a GraphQuerySet item in Microsoft Fabric and provides instructions
    for adding GQL queries from the domain's GraphQueries.gql file.

    KNOWN PLATFORM LIMITATION (as of 2025):
    The Fabric REST API does not fully support pushing queries into a Graph Query Set
    via the updateDefinition endpoint. The API accepts the call and returns Succeeded,
    but the queries are not persisted.

    As a result, this script:
    1. Creates the bare Graph Query Set item in the workspace.
    2. Displays instructions for manually adding queries from the domain's GraphQueries.gql.

.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER OntologyType
    Domain key: OilGasRefinery, SmartBuilding, ManufacturingPlant, ITAsset, WindTurbine.
.PARAMETER OntologyFolder
    Path to the domain ontology folder (auto-detected if omitted).
.PARAMETER GraphModelId
    The GraphModel item GUID (auto-detected from ontology if omitted).
.PARAMETER QuerySetName
    Display name for the Graph Query Set (auto-derived from OntologyType if omitted).
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyType = "OilGasRefinery",
    [Parameter(Mandatory=$false)] [string]$OntologyFolder,
    [Parameter(Mandatory=$false)] [string]$GraphModelId,
    [Parameter(Mandatory=$false)] [string]$QuerySetName
)

# ── Domain defaults ──────────────────────────────────────────────────────────
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$rootDir = Split-Path -Parent $scriptDir

$querySetDefaults = @{
    OilGasRefinery     = "OilGasRefineryQueries"
    SmartBuilding      = "SmartBuildingQueries"
    ManufacturingPlant = "ManufacturingPlantQueries"
    ITAsset            = "ITAssetQueries"
    WindTurbine        = "WindTurbineQueries"
}

if (-not $QuerySetName) { $QuerySetName = $querySetDefaults[$OntologyType] }
if (-not $QuerySetName) { $QuerySetName = "${OntologyType}Queries" }

# Resolve GQL file: domain-first, fallback to deploy/
if (-not $OntologyFolder) { $OntologyFolder = Join-Path $rootDir "ontologies\$OntologyType" }
$gqlFile = Join-Path $OntologyFolder "GraphQueries.gql"
if (-not (Test-Path $gqlFile)) { $gqlFile = Join-Path $scriptDir "RefineryGraphQueries.gql" }

# ── Authentication ──────────────────────────────────────────────────────────
$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Graph Query Set: $QuerySetName ===" -ForegroundColor Cyan

# ── Auto-detect GraphModel ID if not provided ──────────────────────────────
if (-not $GraphModelId) {
    Write-Host "Auto-detecting GraphModel from workspace..." -ForegroundColor Yellow
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
    $gmItems = $allItems | Where-Object { $_.type -eq 'GraphModel' -and $_.displayName -like '*Ontology*graph*' }
    if ($gmItems) {
        $GraphModelId = $gmItems[0].id
        Write-Host "  Found GraphModel: $($gmItems[0].displayName) ($GraphModelId)" -ForegroundColor Gray
    }
    else {
        # Fallback: pick any GraphModel
        $gmItems = $allItems | Where-Object { $_.type -eq 'GraphModel' }
        if ($gmItems) {
            $GraphModelId = $gmItems[0].id
            Write-Host "  Using GraphModel: $($gmItems[0].displayName) ($GraphModelId)" -ForegroundColor Gray
        }
        else {
            Write-Host "[ERROR] No GraphModel found in workspace." -ForegroundColor Red
            exit 1
        }
    }
}

# ── Build GQL Queries ──────────────────────────────────────────────────────
# Graph schema (from ontology graph model):
#   Nodes: Refinery, ProcessUnit, Equipment, Pipeline, CrudeOil, RefinedProduct,
#          StorageTank, Sensor, Employee, MaintenanceEvent, SafetyAlarm,
#          ProductionRecord, CrudeOilFeed
#   Edges (source -[label]-> destination):
#     Refinery         -[HAS_PROCESS_UNIT]-> ProcessUnit
#     ProcessUnit      -[HAS_EQUIPMENT]->    Equipment
#     Equipment        -[HAS_SENSOR]->       Sensor
#     Refinery         -[HAS_PIPELINE]->     Pipeline
#     Pipeline         -[PIPELINE_FROM]->    ProcessUnit
#     Refinery         -[HAS_STORAGE_TANK]-> StorageTank
#     StorageTank      -[HOLDS_PRODUCT]->    RefinedProduct
#     Refinery         -[EMPLOYS]->          Employee
#     MaintenanceEvent -[MAINTENANCE_ON]->   Equipment
#     MaintenanceEvent -[PERFORMED_BY]->     Employee
#     SafetyAlarm      -[ALARM_FROM]->       Sensor
#     ProductionRecord -[PRODUCED_BY]->      ProcessUnit
#     ProductionRecord -[PRODUCES]->         RefinedProduct
#     CrudeOilFeed     -[FEEDS_INTO]->       ProcessUnit
#     CrudeOilFeed     -[CRUDE_SOURCE]->     CrudeOil

function New-QueryId { return [guid]::NewGuid().ToString() }

$queries = @(
    # ── 1. Full Refinery Topology ───────────────────────────────────────
    @{
        displayName  = "1. Full Refinery Topology (all nodes and edges)"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (n)-[e]->(m)
RETURN n, e, m
LIMIT 200
"@
        nodes = @()
        edges = @()
    },

    # ── 2. Refinery -> Process Units -> Equipment ───────────────────────
    @{
        displayName  = "2. Refinery Process Units and Equipment"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (r:Refinery)-[:HAS_PROCESS_UNIT]->(pu:ProcessUnit)-[:HAS_EQUIPMENT]->(eq:Equipment)
RETURN r.RefineryName AS Refinery,
       pu.ProcessUnitName AS ProcessUnit,
       pu.ProcessUnitType AS UnitType,
       eq.EquipmentName AS Equipment,
       eq.EquipmentType AS EquipmentType
LIMIT 100
"@
        nodes = @()
        edges = @()
    },

    # ── 3. Equipment Sensors and Safety Alarms ──────────────────────────
    @{
        displayName  = "3. Equipment Sensors and Safety Alarms"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (eq:Equipment)-[:HAS_SENSOR]->(s:Sensor)<-[:ALARM_FROM]-(sa:SafetyAlarm)
RETURN eq.EquipmentName AS Equipment,
       s.SensorName AS Sensor,
       s.SensorType AS SensorType,
       sa.AlarmType AS AlarmType,
       sa.Severity AS Severity,
       sa.AlarmTimestamp AS Timestamp
LIMIT 100
"@
        nodes = @()
        edges = @()
    },

    # ── 4. Maintenance History with Employee and Equipment ──────────────
    @{
        displayName  = "4. Maintenance Events by Employee and Equipment"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (emp:Employee)<-[:PERFORMED_BY]-(me:MaintenanceEvent)-[:MAINTENANCE_ON]->(eq:Equipment)
RETURN emp.EmployeeName AS Technician,
       emp.JobTitle AS Role,
       me.MaintenanceType AS MaintenanceType,
       me.MaintenanceDate AS Date,
       me.DowntimeHours AS DowntimeHrs,
       me.Cost AS Cost,
       eq.EquipmentName AS Equipment
ORDER BY me.MaintenanceDate DESC
LIMIT 50
"@
        nodes = @()
        edges = @()
    },

    # ── 5. Crude Oil Supply Chain ───────────────────────────────────────
    @{
        displayName  = "5. Crude Oil Supply Chain (Source to Process Unit)"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (co:CrudeOil)<-[:CRUDE_SOURCE]-(cf:CrudeOilFeed)-[:FEEDS_INTO]->(pu:ProcessUnit)
RETURN co.CrudeOilName AS CrudeOil,
       co.APIGravity AS APIGravity,
       co.SulfurContent AS SulfurPct,
       co.Origin AS Origin,
       pu.ProcessUnitName AS ProcessUnit,
       pu.ProcessUnitType AS UnitType
LIMIT 50
"@
        nodes = @()
        edges = @()
    },

    # ── 6. Production Output by Product ─────────────────────────────────
    @{
        displayName  = "6. Production Records by Product and Process Unit"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (pu:ProcessUnit)<-[:PRODUCED_BY]-(pr:ProductionRecord)-[:PRODUCES]->(rp:RefinedProduct)
RETURN pu.ProcessUnitName AS ProcessUnit,
       rp.ProductName AS Product,
       rp.ProductCategory AS Category,
       pr.QuantityBarrels AS Barrels,
       pr.ProductionDate AS Date
ORDER BY pr.ProductionDate DESC
LIMIT 50
"@
        nodes = @()
        edges = @()
    },

    # ── 7. Storage Tank Inventory ───────────────────────────────────────
    @{
        displayName  = "7. Storage Tanks and Products by Refinery"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (r:Refinery)-[:HAS_STORAGE_TANK]->(st:StorageTank)-[:HOLDS_PRODUCT]->(rp:RefinedProduct)
RETURN r.RefineryName AS Refinery,
       st.TankName AS Tank,
       st.CapacityBarrels AS CapacityBBL,
       st.CurrentLevel AS CurrentLevel,
       rp.ProductName AS Product
LIMIT 50
"@
        nodes = @()
        edges = @()
    },

    # ── 8. Pipeline Network ─────────────────────────────────────────────
    @{
        displayName  = "8. Pipeline Network - Refinery to Process Units"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (r:Refinery)-[:HAS_PIPELINE]->(p:Pipeline)-[:PIPELINE_FROM]->(pu:ProcessUnit)
RETURN r.RefineryName AS Refinery,
       p.PipelineName AS Pipeline,
       p.DiameterInches AS Diameter,
       p.Material AS Material,
       pu.ProcessUnitName AS ConnectedUnit
LIMIT 50
"@
        nodes = @()
        edges = @()
    },

    # ── 9. End-to-End: Crude Oil -> Refinery -> Product ─────────────────
    @{
        displayName  = "9. End-to-End: Crude Oil to Refined Product"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (co:CrudeOil)<-[:CRUDE_SOURCE]-(cf:CrudeOilFeed)-[:FEEDS_INTO]->(pu:ProcessUnit)
      <-[:PRODUCED_BY]-(pr:ProductionRecord)-[:PRODUCES]->(rp:RefinedProduct)
RETURN co.CrudeOilName AS CrudeSource,
       co.Origin AS Origin,
       pu.ProcessUnitName AS ProcessUnit,
       rp.ProductName AS Product,
       pr.QuantityBarrels AS OutputBarrels
LIMIT 50
"@
        nodes = @()
        edges = @()
    },

    # ── 10. Refinery Workforce ──────────────────────────────────────────
    @{
        displayName  = "10. Refinery Workforce and Maintenance Activity"
        id           = New-QueryId
        queryMode    = "GQLCode"
        gqlQueryText = @"
MATCH (r:Refinery)-[:EMPLOYS]->(emp:Employee)<-[:PERFORMED_BY]-(me:MaintenanceEvent)
RETURN r.RefineryName AS Refinery,
       emp.EmployeeName AS Employee,
       emp.Department AS Department,
       COUNT(me) AS MaintenanceCount
LIMIT 50
"@
        nodes = @()
        edges = @()
    }
)

# ── Build the graphQuerySet.json payload ────────────────────────────────────
$querySetDef = @{
    graphInstanceObjectId       = $GraphModelId
    graphInstanceFolderObjectId = $WorkspaceId
    queries                     = $queries
}

# Serialize to JSON manually for PS5.1 compatibility
$querySetJsonRaw = $querySetDef | ConvertTo-Json -Depth 10 -Compress
$querySetB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($querySetJsonRaw))

# ── Step A: Create bare Graph Query Set item ────────────────────────────────
# Uses the type-specific /GraphQuerySets endpoint per official Fabric REST API docs.
Write-Host "Creating Graph Query Set '$QuerySetName'..." -ForegroundColor Yellow

$createBody = @{
    displayName = $QuerySetName
    description = "GQL queries for the $OntologyType ontology graph"
} | ConvertTo-Json -Depth 10

$gqsId = $null
try {
    $response = Invoke-WebRequest `
        -Uri "$apiBase/workspaces/$WorkspaceId/GraphQuerySets" `
        -Method POST -Headers $headers -Body $createBody -UseBasicParsing

    if ($response.StatusCode -eq 201) {
        $gqs = $response.Content | ConvertFrom-Json
        $gqsId = $gqs.id
        Write-Host "[OK] Graph Query Set created: $gqsId" -ForegroundColor Green
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
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $gqsItem = $allItems | Where-Object { $_.displayName -eq $QuerySetName -and $_.type -eq 'GraphQuerySet' }
            if ($gqsItem) { $gqsId = $gqsItem.id }
            Write-Host "[OK] Graph Query Set created: $gqsId" -ForegroundColor Green
        }
        else {
            Write-Host "[FAIL] Graph Query Set creation LRO: $($poll.status)" -ForegroundColor Red
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

        if ($errBody -match 'ItemDisplayNameAlreadyInUse') {
            Write-Host "  Graph Query Set '$QuerySetName' already exists. Will update definition..." -ForegroundColor Yellow
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $existing = $allItems | Where-Object { $_.displayName -eq $QuerySetName -and $_.type -eq 'GraphQuerySet' }
            if ($existing) {
                $gqsId = $existing.id
                Write-Host "  Existing GQS ID: $gqsId" -ForegroundColor Gray
            }
        }
    }
    else {
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    }
}

# ── Step B: Manual query setup instructions ────────────────────────────────────
# NOTE: The Fabric REST API updateDefinition endpoint for GraphQuerySet accepts
# the graphQuerySet.json payload and returns Succeeded, but does NOT actually
# persist queries. This is a known platform limitation as of 2025.
# Queries must be added manually via the Fabric UI.
if ($gqsId) {
    Write-Host "" -ForegroundColor White
    Write-Host "[INFO] Graph Query Set item created successfully." -ForegroundColor Green
    Write-Host "" -ForegroundColor White
    Write-Host "IMPORTANT: Queries must be added manually via the Fabric UI." -ForegroundColor Yellow
    Write-Host "The REST API does not yet support pushing queries into a Graph Query Set." -ForegroundColor Yellow
    Write-Host "" -ForegroundColor White
    Write-Host "To add queries:" -ForegroundColor Cyan
    Write-Host "  1. Open the Graph Query Set '$QuerySetName' in Fabric" -ForegroundColor White
    Write-Host "  2. Select the graph model from the ontology" -ForegroundColor White
    Write-Host "  3. Copy queries from: $gqlFile" -ForegroundColor White
    Write-Host "  4. Paste each query, give it a name, and save" -ForegroundColor White
}

# ── Summary ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Graph Query Set Deployment Summary ===" -ForegroundColor Cyan
Write-Host "  Name:         $QuerySetName"
Write-Host "  GQS ID:       $gqsId"
Write-Host "  GraphModel:   $GraphModelId"
Write-Host "  Queries:      $($queries.Count)"
Write-Host ""
Write-Host "Example queries included:" -ForegroundColor White
foreach ($q in $queries) {
    Write-Host "  - $($q.displayName)"
}
Write-Host ""
Write-Host "MANUAL STEP REQUIRED: Add queries to the Graph Query Set via Fabric UI." -ForegroundColor Yellow
Write-Host "  The REST API does not persist queries (known platform limitation)." -ForegroundColor Yellow
Write-Host "  Copy queries from: $gqlFile" -ForegroundColor Cyan
Write-Host ""
Write-Host "Open the Graph Query Set in Fabric to run queries visually." -ForegroundColor White
Write-Host ""
Write-Host "=== Graph Query Set Deployment Complete ===" -ForegroundColor Cyan
