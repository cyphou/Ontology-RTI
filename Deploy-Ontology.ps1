<#
.SYNOPSIS
    Multi-Domain Ontology Accelerator for Microsoft Fabric.

.DESCRIPTION
    Deploys a production-ready IQ Ontology to Microsoft Fabric for one of the
    supported industry domains:
      - OilGasRefinery       (default — original accelerator)
      - SmartBuilding        (HVAC, lighting, elevators, occupancy, energy)
      - ManufacturingPlant   (production lines, machines, quality, materials)
      - ITAsset              (datacenters, servers, applications, incidents)
      - WindTurbine          (wind farms, turbines, nacelles, blades, power output)

    The script auto-configures all Fabric artifacts (Lakehouse, Eventhouse, Semantic Model,
    Ontology, RTI Dashboard, Data Agent, Operations Agent) using domain-specific entity types,
    relationships, sample data, and AI instructions.

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace to deploy to.

.PARAMETER OntologyType
    Industry domain to deploy. One of: OilGasRefinery, SmartBuilding, ManufacturingPlant, ITAsset, WindTurbine.
    Defaults to interactive menu selection.

.PARAMETER SkipDataAgent
    Skip Data Agent deployment (requires F64+ capacity).

.PARAMETER SkipOperationsAgent
    Skip Operations Agent deployment.

.PARAMETER SkipDashboard
    Skip RTI Dashboard deployment.

.EXAMPLE
    # Interactive menu
    .\Deploy-Ontology.ps1 -WorkspaceId "your-workspace-guid"

.EXAMPLE
    # Direct selection
    .\Deploy-Ontology.ps1 -WorkspaceId "your-guid" -OntologyType SmartBuilding

.EXAMPLE
    # Original Oil & Gas (backward compatible)
    .\Deploy-Ontology.ps1 -WorkspaceId "your-guid" -OntologyType OilGasRefinery
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [ValidateSet("OilGasRefinery", "SmartBuilding", "ManufacturingPlant", "ITAsset", "WindTurbine")]
    [string]$OntologyType,

    [switch]$SkipDataAgent,
    [switch]$SkipOperationsAgent,
    [switch]$SkipDashboard
)

$ErrorActionPreference = "Stop"
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

# ============================================================================
# DOMAIN REGISTRY
# ============================================================================
$domains = @{
    OilGasRefinery = @{
        DisplayName  = "Oil & Gas Refinery"
        Emoji        = [char]0x26FD  # fuel pump
        Description  = "Refineries, process units, equipment, pipelines, crude oil, sensors, safety"
        Lakehouse    = "OilGasRefineryLH"
        Eventhouse   = "RefineryTelemetryEH"
        SemanticModel = "OilGasRefineryModel"
        OntologyName = "OilGasRefineryOntology"
        DataFolder   = Join-Path $scriptDir "ontologies\OilGasRefinery\data"
        OntologyFolder = Join-Path $scriptDir "ontologies\OilGasRefinery"
        Color        = "DarkYellow"
    }
    SmartBuilding = @{
        DisplayName  = "Smart Building"
        Emoji        = [char]0x1F3E2  # office building
        Description  = "Buildings, floors, zones, HVAC, lighting, elevators, occupancy, energy"
        Lakehouse    = "SmartBuildingLH"
        Eventhouse   = "BuildingTelemetryEH"
        SemanticModel = "SmartBuildingModel"
        OntologyName = "SmartBuildingOntology"
        DataFolder   = Join-Path $scriptDir "ontologies\SmartBuilding\data"
        OntologyFolder = Join-Path $scriptDir "ontologies\SmartBuilding"
        Color        = "Cyan"
    }
    ManufacturingPlant = @{
        DisplayName  = "Manufacturing Plant"
        Emoji        = [char]0x1F3ED  # factory
        Description  = "Plants, production lines, machines, quality checks, materials, batches"
        Lakehouse    = "ManufacturingPlantLH"
        Eventhouse   = "PlantTelemetryEH"
        SemanticModel = "ManufacturingPlantModel"
        OntologyName = "ManufacturingPlantOntology"
        DataFolder   = Join-Path $scriptDir "ontologies\ManufacturingPlant\data"
        OntologyFolder = Join-Path $scriptDir "ontologies\ManufacturingPlant"
        Color        = "Green"
    }
    ITAsset = @{
        DisplayName  = "IT Asset Management"
        Emoji        = [char]0x1F5A5  # desktop computer
        Description  = "Datacenters, racks, servers, VMs, applications, incidents, licenses"
        Lakehouse    = "ITAssetLH"
        Eventhouse   = "ITTelemetryEH"
        SemanticModel = "ITAssetModel"
        OntologyName = "ITAssetOntology"
        DataFolder   = Join-Path $scriptDir "ontologies\ITAsset\data"
        OntologyFolder = Join-Path $scriptDir "ontologies\ITAsset"
        Color        = "Magenta"
    }
    WindTurbine = @{
        DisplayName  = "Wind Turbine / Wind Farm"
        Emoji        = [char]0x1F32C  # wind
        Description  = "Wind farms, turbines, nacelles, blades, towers, power output, weather"
        Lakehouse    = "WindTurbineLH"
        Eventhouse   = "WindTelemetryEH"
        SemanticModel = "WindTurbineModel"
        OntologyName = "WindTurbineOntology"
        DataFolder   = Join-Path $scriptDir "ontologies\WindTurbine\data"
        OntologyFolder = Join-Path $scriptDir "ontologies\WindTurbine"
        Color        = "Blue"
    }
}

# ============================================================================
# INTERACTIVE MENU
# ============================================================================
if (-not $OntologyType) {
    Write-Host ""
    Write-Host "  ============================================================" -ForegroundColor White
    Write-Host "    Microsoft Fabric IQ Ontology Accelerator" -ForegroundColor Yellow
    Write-Host "    Multi-Domain Deployment" -ForegroundColor Yellow
    Write-Host "  ============================================================" -ForegroundColor White
    Write-Host ""
    Write-Host "  Select an industry ontology to deploy:" -ForegroundColor Gray
    Write-Host ""

    $index = 1
    $menuMap = @{}
    foreach ($key in @("OilGasRefinery", "SmartBuilding", "ManufacturingPlant", "ITAsset", "WindTurbine")) {
        $d = $domains[$key]
        $color = $d.Color
        Write-Host "    [$index] " -NoNewline -ForegroundColor White
        Write-Host "$($d.DisplayName)" -NoNewline -ForegroundColor $color
        Write-Host " - $($d.Description)" -ForegroundColor Gray
        $menuMap[$index] = $key
        $index++
    }

    Write-Host ""
    $choice = Read-Host "  Enter choice (1-5)"
    $choiceInt = [int]$choice
    if ($menuMap.ContainsKey($choiceInt)) {
        $OntologyType = $menuMap[$choiceInt]
    }
    else {
        Write-Error "Invalid selection. Exiting."
        exit 1
    }
}

$domain = $domains[$OntologyType]
Write-Host ""
Write-Host "  Selected: $($domain.DisplayName)" -ForegroundColor $domain.Color
Write-Host ""

# ============================================================================
# DISPATCH TO DOMAIN-SPECIFIC DEPLOYMENT
# ============================================================================

# All domains use the generic deployment engine
$ontologyFolder = $domain.OntologyFolder
$dataFolder = $domain.DataFolder

if (-not (Test-Path $ontologyFolder)) {
    Write-Error "Ontology folder not found: $ontologyFolder"
    exit 1
}

$engineScript = Join-Path $scriptDir "deploy\Deploy-GenericOntology.ps1"
if (-not (Test-Path $engineScript)) {
    Write-Error "Generic deployment engine not found: $engineScript"
    exit 1
}

$deployParams = @{
    WorkspaceId       = $WorkspaceId
    OntologyType      = $OntologyType
    LakehouseName     = $domain.Lakehouse
    EventhouseName    = $domain.Eventhouse
    SemanticModelName = $domain.SemanticModel
    OntologyName      = $domain.OntologyName
    DataFolder        = $dataFolder
    OntologyFolder    = $ontologyFolder
    SkipDataAgent     = $SkipDataAgent.IsPresent
    SkipOperationsAgent = $SkipOperationsAgent.IsPresent
    SkipDashboard     = $SkipDashboard.IsPresent
}

& $engineScript @deployParams
