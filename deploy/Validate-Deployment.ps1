<#
.SYNOPSIS
    Validates that an IQ Ontology deployment completed successfully.

.DESCRIPTION
    Checks that all expected Fabric items exist in the target workspace:
    - Lakehouse with tables
    - Eventhouse
    - Semantic model
    - Ontology item
    - Eventstream
    - Graph Query Set
    - KQL Dashboard
    - Data Agent (optional, F64+)
    - Operations Agent (optional)

    Supports all 5 industry domains. Uses -OntologyType to auto-resolve
    item names and expected tables, or accepts explicit overrides.

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace to validate.

.PARAMETER OntologyType
    Domain key: OilGasRefinery, SmartBuilding, ManufacturingPlant, ITAsset, WindTurbine, Healthcare.
    Defaults to OilGasRefinery for backward compatibility.

.EXAMPLE
    .\Validate-Deployment.ps1 -WorkspaceId "guid" -OntologyType SmartBuilding
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [ValidateSet("OilGasRefinery","SmartBuilding","ManufacturingPlant","ITAsset","WindTurbine","Healthcare")]
    [string]$OntologyType = "OilGasRefinery",

    [string]$LakehouseName,
    [string]$EventhouseName,
    [string]$SemanticModelName,
    [string]$OntologyName
)

# ── Domain registry ──
$domainDefaults = @{
    OilGasRefinery = @{
        Lakehouse     = "OilGasRefineryLH"
        Eventhouse    = "RefineryTelemetryEH"
        SemanticModel = "OilGasRefineryModel"
        Ontology      = "OilGasRefineryOntology"
        Eventstream   = "RefineryTelemetryStream"
        GraphQuerySet = "OilGasRefineryQueries"
        Dashboard     = "RefineryTelemetryDashboard"
        Tables        = @("dimrefinery","dimprocessunit","dimequipment","dimpipeline","dimcrudeoil","dimrefinedproduct","dimstoragetank","dimsensor","dimemployee","factmaintenance","factsafetyalarm","factproduction","bridgecrudeoilprocessunit")
    }
    SmartBuilding = @{
        Lakehouse     = "SmartBuildingLH"
        Eventhouse    = "BuildingTelemetryEH"
        SemanticModel = "SmartBuildingModel"
        Ontology      = "SmartBuildingOntology"
        Eventstream   = "BuildingTelemetryStream"
        GraphQuerySet = "SmartBuildingQueries"
        Dashboard     = "BuildingTelemetryDashboard"
        Tables        = @("dimbuilding","dimfloor","dimzone","dimhvacsystem","dimlightingsystem","dimelevator","dimsensor","dimemployee","factoccupancy","factenergyconsumption","factmaintenance","factsafetyalarm","bridgezonesensor")
    }
    ManufacturingPlant = @{
        Lakehouse     = "ManufacturingPlantLH"
        Eventhouse    = "PlantTelemetryEH"
        SemanticModel = "ManufacturingPlantModel"
        Ontology      = "ManufacturingPlantOntology"
        Eventstream   = "PlantTelemetryStream"
        GraphQuerySet = "ManufacturingPlantQueries"
        Dashboard     = "PlantTelemetryDashboard"
        Tables        = @("dimplant","dimproductionline","dimmachine","dimmaterial","dimproduct","dimsensor","dimemployee","factproductionbatch","factqualitycheck","factmaintenance","factsafetyincident","bridgemachinesensor")
    }
    ITAsset = @{
        Lakehouse     = "ITAssetLH"
        Eventhouse    = "ITTelemetryEH"
        SemanticModel = "ITAssetModel"
        Ontology      = "ITAssetOntology"
        Eventstream   = "ITTelemetryStream"
        GraphQuerySet = "ITAssetQueries"
        Dashboard     = "ITTelemetryDashboard"
        Tables        = @("dimdatacenter","dimrack","dimserver","dimvirtualmachine","dimapplication","dimnetworkdevice","dimsensor","dimemployee","factincident","factpatch","factmaintenance","bridgeapplicationserver")
    }
    WindTurbine = @{
        Lakehouse     = "WindTurbineLH"
        Eventhouse    = "WindTelemetryEH"
        SemanticModel = "WindTurbineModel"
        Ontology      = "WindTurbineOntology"
        Eventstream   = "WindTelemetryStream"
        GraphQuerySet = "WindTurbineQueries"
        Dashboard     = "WindTelemetryDashboard"
        Tables        = @("dimwindfarm","dimturbine","dimnacelle","dimblade","dimtower","dimgenerator","dimtransformer","dimsensor","dimemployee","factpoweroutput","factmaintenance","factsafetyalarm","bridgeturbinesensor")
    }
    Healthcare = @{
        Lakehouse     = "HealthcareLH"
        Eventhouse    = "HealthcareTelemetryEH"
        SemanticModel = "HealthcareModel"
        Ontology      = "HealthcareOntology"
        Eventstream   = "HealthcareTelemetryStream"
        GraphQuerySet = "HealthcareQueries"
        Dashboard     = "HealthcareTelemetryDashboard"
        Tables        = @("dimhospital","dimdepartment","dimward","dimphysician","dimnurse","dimpatient","dimmedicaldevice","dimmedication","dimsensor","factlabresult","factprocedure","factmedicationadmin","bridgewarddevice","sensortelemetry")
    }
}

$dd = $domainDefaults[$OntologyType]
if (-not $LakehouseName)     { $LakehouseName     = $dd.Lakehouse }
if (-not $EventhouseName)    { $EventhouseName    = $dd.Eventhouse }
if (-not $SemanticModelName) { $SemanticModelName = $dd.SemanticModel }
if (-not $OntologyName)      { $OntologyName      = $dd.Ontology }
$expectedTables = $dd.Tables

$FabricApiBase = "https://api.fabric.microsoft.com/v1"

# ── Authenticate ──
$account = Get-AzContext
if (-not $account) {
    Write-Host "No active Azure session. Run 'Connect-AzAccount' first." -ForegroundColor Red
    exit 1
}

$tokenObj = Get-AzAccessToken -AsSecureString -ResourceUrl "https://api.fabric.microsoft.com"
$bstr = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($tokenObj.Token)
$token = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)

$headers = @{ "Authorization" = "Bearer $token" }

function Check-Item {
    param([string]$ItemType, [string]$Endpoint, [string]$ExpectedName)
    
    try {
        $response = Invoke-RestMethod -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/$Endpoint" -Headers $headers
        $found = $response.value | Where-Object { $_.displayName -eq $ExpectedName }
        if ($found) {
            Write-Host "  [PASS] $ItemType '$ExpectedName' exists (ID: $($found.id))" -ForegroundColor Green
            return $true
        }
        else {
            Write-Host "  [FAIL] $ItemType '$ExpectedName' NOT found" -ForegroundColor Red
            return $false
        }
    }
    catch {
        Write-Host "  [WARN] Could not query $Endpoint : $_" -ForegroundColor Yellow
        return $false
    }
}

# ── Validation ──
Write-Host ""
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  Deployment Validation - $OntologyType" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Workspace: $WorkspaceId" -ForegroundColor Gray
Write-Host "  Domain:    $OntologyType" -ForegroundColor Gray
Write-Host ""

$results = @()
$results += Check-Item "Lakehouse"      "lakehouses"     $LakehouseName
$results += Check-Item "Eventhouse"     "eventhouses"    $EventhouseName
$results += Check-Item "Semantic Model" "semanticModels" $SemanticModelName

# Check ontology via generic items endpoint (ontology-specific API may not exist yet)
try {
    $allItems = Invoke-RestMethod -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $headers
    $ontology = $allItems.value | Where-Object { $_.displayName -eq $OntologyName }
    if ($ontology) {
        Write-Host "  [PASS] Ontology '$OntologyName' exists (ID: $($ontology.id), Type: $($ontology.type))" -ForegroundColor Green
        $results += $true
    }
    else {
        Write-Host "  [FAIL] Ontology '$OntologyName' NOT found" -ForegroundColor Red
        $results += $false
    }

    # ── Additional items from the items list ──
    $extraChecks = @(
        @{ Label = "Eventstream";       Name = $dd.Eventstream }
        @{ Label = "Graph Query Set";   Name = $dd.GraphQuerySet }
        @{ Label = "KQL Dashboard";     Name = $dd.Dashboard }
    )
    foreach ($chk in $extraChecks) {
        $match = $allItems.value | Where-Object { $_.displayName -eq $chk.Name }
        if ($match) {
            Write-Host "  [PASS] $($chk.Label) '$($chk.Name)' exists (ID: $($match.id), Type: $($match.type))" -ForegroundColor Green
            $results += $true
        }
        else {
            Write-Host "  [FAIL] $($chk.Label) '$($chk.Name)' NOT found" -ForegroundColor Red
            $results += $false
        }
    }

    # Agents are optional (require F64+ capacity) — warn instead of fail
    $agentChecks = @(
        @{ Label = "Data Agent";       Name = "$($OntologyType)DataAgent" }
        @{ Label = "Operations Agent"; Name = "$($OntologyType)OpsAgent" }
    )
    foreach ($chk in $agentChecks) {
        $match = $allItems.value | Where-Object { $_.displayName -eq $chk.Name }
        if ($match) {
            Write-Host "  [PASS] $($chk.Label) '$($chk.Name)' exists (ID: $($match.id))" -ForegroundColor Green
        }
        else {
            Write-Host "  [SKIP] $($chk.Label) '$($chk.Name)' not found (requires F64+ capacity)" -ForegroundColor DarkYellow
        }
    }
}
catch {
    Write-Host "  [WARN] Could not query workspace items: $_" -ForegroundColor Yellow
    $results += $false
}

# ── Check Lakehouse Tables ──
Write-Host ""
Write-Host "  Checking Lakehouse Tables..." -ForegroundColor Cyan

try {
    $lakehouses = Invoke-RestMethod -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses" -Headers $headers
    $lh = $lakehouses.value | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
    if ($lh) {
        $tables = Invoke-RestMethod -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$($lh.id)/tables" `
            -Headers $headers
        
        $tableNames = $tables.value | ForEach-Object { $_.name }
        $tableCount = 0
        foreach ($t in $expectedTables) {
            if ($tableNames -contains $t) {
                Write-Host "    [PASS] Table '$t'" -ForegroundColor Green
                $tableCount++
            }
            else {
                Write-Host "    [FAIL] Table '$t' missing" -ForegroundColor Red
            }
        }
        Write-Host ""
        Write-Host "  Tables: $tableCount / $($expectedTables.Count) present" -ForegroundColor $(if ($tableCount -eq $expectedTables.Count) { "Green" } else { "Yellow" })
    }
}
catch {
    Write-Host "  [WARN] Could not list lakehouse tables: $_" -ForegroundColor Yellow
    Write-Host "  Tables can only be verified after the notebook has been executed." -ForegroundColor Gray
}

# ── Summary ──
$passCount = ($results | Where-Object { $_ -eq $true }).Count
$totalCount = $results.Count

Write-Host ""
Write-Host "=============================================" -ForegroundColor $(if ($passCount -eq $totalCount) { "Green" } else { "Yellow" })
Write-Host "  Result: $passCount / $totalCount items validated" -ForegroundColor $(if ($passCount -eq $totalCount) { "Green" } else { "Yellow" })
Write-Host "=============================================" -ForegroundColor $(if ($passCount -eq $totalCount) { "Green" } else { "Yellow" })
Write-Host ""

if ($passCount -lt $totalCount) {
    Write-Host "  Some items are missing. Check the logs above and refer to SETUP_GUIDE.md" -ForegroundColor Yellow
    Write-Host "  for manual steps to complete the deployment." -ForegroundColor Yellow
    Write-Host ""
}
