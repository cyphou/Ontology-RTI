<#
.SYNOPSIS
    Bulk-imports Lakehouse data into an IQ Ontology using the Fabric REST API.
.DESCRIPTION
    Reads CSV files from the domain data folder, maps them to ontology entities,
    and uses the Fabric Ontology Bulk Import API to load all data in a single
    batched operation. This is faster than individual entity updates for large
    datasets.

.PARAMETER WorkspaceId
    Fabric workspace GUID.

.PARAMETER OntologyId
    IQ Ontology item GUID.

.PARAMETER LakehouseId
    Lakehouse item GUID containing the Delta tables.

.PARAMETER OntologyType
    Domain key to determine entity-to-table mappings.

.PARAMETER DataFolder
    Path to the domain's data/ folder.

.EXAMPLE
    .\Deploy-BulkImport.ps1 -WorkspaceId "guid" -OntologyId "guid" -LakehouseId "guid" -OntologyType OilGasRefinery
#>
param(
    [Parameter(Mandatory=$true)] [string]$WorkspaceId,
    [Parameter(Mandatory=$true)] [string]$OntologyId,
    [Parameter(Mandatory=$true)] [string]$LakehouseId,
    [ValidateSet("OilGasRefinery","SmartBuilding","ManufacturingPlant","ITAsset","WindTurbine","Healthcare")]
    [string]$OntologyType = "OilGasRefinery",
    [string]$DataFolder
)

$ErrorActionPreference = "Stop"
$apiBase = "https://api.fabric.microsoft.com/v1"

# ── Default data folder ─────────────────────────────────────────────────────
if (-not $DataFolder) {
    $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
    $rootDir = Split-Path -Parent $scriptDir
    $DataFolder = Join-Path $rootDir "ontologies\$OntologyType\data"
}

if (-not (Test-Path $DataFolder)) {
    Write-Host "[ERROR] Data folder not found: $DataFolder" -ForegroundColor Red
    exit 1
}

# ── Authentication ──────────────────────────────────────────────────────────
$fabricToken = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{
    "Authorization" = "Bearer $fabricToken"
    "Content-Type"  = "application/json"
}

# ── Domain entity-to-table mappings ─────────────────────────────────────────
$domainMappings = @{
    OilGasRefinery = @(
        @{ EntityType = "Refinery";        Table = "dimrefinery";              IdColumn = "RefineryId" }
        @{ EntityType = "ProcessUnit";     Table = "dimprocessunit";           IdColumn = "ProcessUnitId" }
        @{ EntityType = "Equipment";       Table = "dimequipment";             IdColumn = "EquipmentId" }
        @{ EntityType = "Sensor";          Table = "dimsensor";                IdColumn = "SensorId" }
        @{ EntityType = "CrudeOil";        Table = "dimcrudeoil";              IdColumn = "CrudeOilId" }
        @{ EntityType = "RefinedProduct";  Table = "dimrefinedproduct";        IdColumn = "ProductId" }
        @{ EntityType = "Pipeline";        Table = "dimpipeline";              IdColumn = "PipelineId" }
        @{ EntityType = "StorageTank";     Table = "dimstoragetank";           IdColumn = "TankId" }
        @{ EntityType = "Employee";        Table = "dimemployee";              IdColumn = "EmployeeId" }
    )
    SmartBuilding = @(
        @{ EntityType = "Building";   Table = "dimbuilding";    IdColumn = "BuildingId" }
        @{ EntityType = "Floor";      Table = "dimfloor";       IdColumn = "FloorId" }
        @{ EntityType = "Zone";       Table = "dimzone";        IdColumn = "ZoneId" }
        @{ EntityType = "HVACUnit";   Table = "dimhvacunit";    IdColumn = "HVACUnitId" }
        @{ EntityType = "Sensor";     Table = "dimsensor";      IdColumn = "SensorId" }
        @{ EntityType = "Tenant";     Table = "dimtenant";      IdColumn = "TenantId" }
        @{ EntityType = "Employee";   Table = "dimemployee";    IdColumn = "EmployeeId" }
    )
    ManufacturingPlant = @(
        @{ EntityType = "Plant";       Table = "dimplant";       IdColumn = "PlantId" }
        @{ EntityType = "Line";        Table = "dimline";        IdColumn = "LineId" }
        @{ EntityType = "Machine";     Table = "dimmachine";     IdColumn = "MachineId" }
        @{ EntityType = "Product";     Table = "dimproduct";     IdColumn = "ProductId" }
        @{ EntityType = "Sensor";      Table = "dimsensor";      IdColumn = "SensorId" }
        @{ EntityType = "Inspector";   Table = "diminspector";   IdColumn = "InspectorId" }
        @{ EntityType = "Shift";       Table = "dimshift";       IdColumn = "ShiftId" }
    )
    ITAsset = @(
        @{ EntityType = "DataCenter";   Table = "dimdatacenter";   IdColumn = "DataCenterId" }
        @{ EntityType = "Rack";         Table = "dimrack";         IdColumn = "RackId" }
        @{ EntityType = "Server";       Table = "dimserver";       IdColumn = "ServerId" }
        @{ EntityType = "Application";  Table = "dimapplication";  IdColumn = "AppId" }
        @{ EntityType = "NetworkDevice";Table = "dimnetworkdevice";IdColumn = "DeviceId" }
        @{ EntityType = "Sensor";       Table = "dimsensor";       IdColumn = "SensorId" }
    )
    WindTurbine = @(
        @{ EntityType = "WindFarm";    Table = "dimwindfarm";    IdColumn = "FarmId" }
        @{ EntityType = "Turbine";     Table = "dimturbine";     IdColumn = "TurbineId" }
        @{ EntityType = "Component";   Table = "dimcomponent";   IdColumn = "ComponentId" }
        @{ EntityType = "Sensor";      Table = "dimsensor";      IdColumn = "SensorId" }
        @{ EntityType = "Technician";  Table = "dimtechnician";  IdColumn = "TechnicianId" }
        @{ EntityType = "WeatherStation"; Table = "dimweatherstation"; IdColumn = "StationId" }
    )
    Healthcare = @(
        @{ EntityType = "Hospital";       Table = "dimhospital";       IdColumn = "HospitalId" }
        @{ EntityType = "Department";     Table = "dimdepartment";     IdColumn = "DepartmentId" }
        @{ EntityType = "Ward";           Table = "dimward";           IdColumn = "WardId" }
        @{ EntityType = "Physician";      Table = "dimphysician";      IdColumn = "PhysicianId" }
        @{ EntityType = "Nurse";          Table = "dimnurse";          IdColumn = "NurseId" }
        @{ EntityType = "Patient";        Table = "dimpatient";        IdColumn = "PatientId" }
        @{ EntityType = "MedicalDevice";  Table = "dimmedicaldevice";  IdColumn = "DeviceId" }
        @{ EntityType = "Medication";     Table = "dimmedication";     IdColumn = "MedicationId" }
        @{ EntityType = "Sensor";         Table = "dimsensor";         IdColumn = "SensorId" }
    )
}

$mappings = $domainMappings[$OntologyType]
if (-not $mappings) {
    Write-Host "[ERROR] No mappings for OntologyType: $OntologyType" -ForegroundColor Red
    exit 1
}

# ── Build bulk import payload ───────────────────────────────────────────────
Write-Host ""
Write-Host "=== Bulk Import: $OntologyType ===" -ForegroundColor Cyan
Write-Host "  Ontology: $OntologyId"
Write-Host "  Lakehouse: $LakehouseId"
Write-Host "  Entities: $($mappings.Count)"
Write-Host ""

$entityBindings = @()
foreach ($map in $mappings) {
    $csvFile = Get-ChildItem -Path $DataFolder -Filter "*.csv" | Where-Object { $_.BaseName -ieq "Dim$($map.EntityType)" } | Select-Object -First 1
    if ($csvFile) {
        $rowCount = ((Get-Content -Path $csvFile.FullName).Count - 1)
        Write-Host "  [OK] $($map.EntityType) -> $($map.Table) ($rowCount rows)" -ForegroundColor Green
        $entityBindings += @{
            entityTypeName = $map.EntityType
            dataSource = @{
                type = "Lakehouse"
                lakehouseId = $LakehouseId
                tableName = $map.Table
            }
            idColumnName = $map.IdColumn
        }
    } else {
        Write-Host "  [SKIP] $($map.EntityType) — no CSV found" -ForegroundColor Yellow
    }
}

if ($entityBindings.Count -eq 0) {
    Write-Host "[ERROR] No entity bindings generated." -ForegroundColor Red
    exit 1
}

$importPayload = @{
    entityBindings = $entityBindings
} | ConvertTo-Json -Depth 10

# ── Execute bulk import ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "Submitting bulk import request..." -ForegroundColor Yellow

$importUri = "$apiBase/workspaces/$WorkspaceId/ontologies/$OntologyId/bulkImport"
try {
    $response = Invoke-WebRequest -Method Post -Uri $importUri -Headers $headers -Body $importPayload -UseBasicParsing

    if ($response.StatusCode -eq 202) {
        $locationHeader = $response.Headers["Location"]
        Write-Host "  [ACCEPTED] Bulk import started. Polling..." -ForegroundColor Cyan

        # Poll for completion
        $maxWait = 300
        $elapsed = 0
        $pollInterval = 10
        while ($elapsed -lt $maxWait) {
            Start-Sleep -Seconds $pollInterval
            $elapsed += $pollInterval
            try {
                $pollResponse = Invoke-RestMethod -Method Get -Uri $locationHeader -Headers $headers
                $state = $pollResponse.status
                Write-Host "  Status: $state ($elapsed/$maxWait sec)" -ForegroundColor Gray
                if ($state -eq "Succeeded") {
                    Write-Host "  [OK] Bulk import completed successfully!" -ForegroundColor Green
                    break
                } elseif ($state -eq "Failed") {
                    Write-Host "  [FAIL] Bulk import failed: $($pollResponse | ConvertTo-Json -Depth 3)" -ForegroundColor Red
                    exit 1
                }
            } catch {
                Write-Host "  Poll error: $($_.Exception.Message)" -ForegroundColor DarkYellow
            }
        }
        if ($elapsed -ge $maxWait) {
            Write-Host "  [WARN] Timed out waiting for bulk import." -ForegroundColor Yellow
        }
    } elseif ($response.StatusCode -eq 200 -or $response.StatusCode -eq 201) {
        Write-Host "  [OK] Bulk import completed immediately." -ForegroundColor Green
    } else {
        Write-Host "  [WARN] Unexpected status: $($response.StatusCode)" -ForegroundColor Yellow
    }
}
catch {
    $errMsg = $_.Exception.Message
    if ($_.Exception.Response) {
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $errBody = $sr.ReadToEnd(); $sr.Close()
            $errMsg = "$errMsg | $errBody"
        } catch {}
    }
    Write-Host "  [ERROR] Bulk import failed: $errMsg" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Note: The bulkImport endpoint may not be available in all Fabric regions." -ForegroundColor Yellow
    Write-Host "  The standard deployment via Deploy-Ontology.ps1 uses entity-level data binding instead." -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "=== Bulk Import Complete ===" -ForegroundColor Cyan
