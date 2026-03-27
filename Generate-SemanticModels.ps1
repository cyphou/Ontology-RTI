<#
.SYNOPSIS
    Auto-generates TMDL semantic model definitions for all ontology domains.

.DESCRIPTION
    Reads CSV headers from each domain's data folder and generates complete
    TMDL (Tabular Model Definition Language) files for Direct Lake semantic
    models. Includes per-domain measures, relationships, and column type mapping.

    Supports 6 domains: SmartBuilding, ManufacturingPlant, ITAsset, WindTurbine, Healthcare.
    Output: ontologies/<Domain>/SemanticModel/ folder per domain.

.EXAMPLE
    .\Generate-SemanticModels.ps1
    .\Generate-SemanticModels.ps1 -Domain SmartBuilding
#>
param(
    [ValidateSet("SmartBuilding","ManufacturingPlant","ITAsset","WindTurbine","Healthcare","All")]
    [string]$Domain = "All"
)

$BasePath = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }

# ============================================================================
# NUMERIC COLUMN DEFINITIONS (columns that use dataType: double)
# ============================================================================

$numericColumns = @{
    "SmartBuilding" = @(
        "Floors","TotalAreaSqFt","FloorNumber","AreaSqFt","ZoneCount","MaxOccupancy",
        "MinRange","MaxRange","CapacityBTU","WattageTotalW","BulbCount","CoverageAreaSqFt",
        "CapacityLbs","MaxFloors","AlertValue","ThresholdValue","DurationHours","CostUSD",
        "ReadingValue"
    )
    "ManufacturingPlant" = @(
        "TotalAreaSqFt","ProductionCapacity","CapacityUnitsPerHour","MinRange","MaxRange",
        "UnitCost","MinStockLevel","CurrentStock","WeightKg","QuantityProduced","DefectRate",
        "EnergyUsedKWh","DefectCount","DurationHours","CostUSD","ReadingValue","Value","Threshold"
    )
    "ITAsset" = @(
        "TotalRackCapacity","TierLevel","PowerCapacityKW","RackSize","MaxPowerW","CurrentPowerW",
        "CPUCores","MemoryGB","StorageTB","vCPU","DiskGB","SizeGB","Ports","Seats","AssignedSeats",
        "CostUSD","DurationHours","Value","Threshold","CPUPercent","MemoryPercent","DiskIOPS","NetworkMbps"
    )
    "WindTurbine" = @(
        "TotalTurbines","InstalledCapacityMW","RatedCapacityKW","HubHeightM","RotorDiameterM",
        "LengthM","WeightTons","HeightM","Sections","RatingMVA","VoltageKV","ElevationM",
        "MinThreshold","MaxThreshold","YearsExperience","Latitude","Longitude",
        "WindSpeedMs","PowerOutputKW","CapacityFactor","RotorRPM","PitchAngleDeg","YawAngleDeg",
        "GridFrequencyHz","DurationHours","CostUSD","Value","Threshold","Hour"
    )
    "Healthcare" = @(
        "BedCapacity","TierLevel","BedCount","Floor","NurseStations","UnitCost",
        "YearsExperience","MinThreshold","MaxThreshold","ResultValue","DurationMinutes",
        "Dosage","Value"
    )
}

# ============================================================================
# RELATIONSHIP DEFINITIONS
# ============================================================================

$relationships = @{
    "SmartBuilding" = @(
        @{Name="dimfloor_BuildingId_dimbuilding"; From="dimfloor.BuildingId"; To="dimbuilding.BuildingId"}
        @{Name="dimzone_FloorId_dimfloor"; From="dimzone.FloorId"; To="dimfloor.FloorId"}
        @{Name="dimsensor_ZoneId_dimzone"; From="dimsensor.ZoneId"; To="dimzone.ZoneId"}
        @{Name="dimhvacsystem_ZoneId_dimzone"; From="dimhvacsystem.ZoneId"; To="dimzone.ZoneId"; Inactive=$true}
        @{Name="dimlightingsystem_ZoneId_dimzone"; From="dimlightingsystem.ZoneId"; To="dimzone.ZoneId"; Inactive=$true}
        @{Name="dimaccesspoint_ZoneId_dimzone"; From="dimaccesspoint.ZoneId"; To="dimzone.ZoneId"; Inactive=$true}
        @{Name="dimoccupant_ZoneId_dimzone"; From="dimoccupant.ZoneId"; To="dimzone.ZoneId"; Inactive=$true}
        @{Name="dimelevator_BuildingId_dimbuilding"; From="dimelevator.BuildingId"; To="dimbuilding.BuildingId"; Inactive=$true}
        @{Name="dimenergymeter_BuildingId_dimbuilding"; From="dimenergymeter.BuildingId"; To="dimbuilding.BuildingId"; Inactive=$true}
        @{Name="factalert_SensorId_dimsensor"; From="factalert.SensorId"; To="dimsensor.SensorId"}
        @{Name="factmaintenanceticket_AssignedToOccupantId_dimoccupant"; From="factmaintenanceticket.AssignedToOccupantId"; To="dimoccupant.OccupantId"}
        @{Name="sensortelemetry_SensorId_dimsensor"; From="sensortelemetry.SensorId"; To="dimsensor.SensorId"; Inactive=$true}
    )
    "ManufacturingPlant" = @(
        @{Name="dimproductionline_PlantId_dimplant"; From="dimproductionline.PlantId"; To="dimplant.PlantId"}
        @{Name="dimmachine_LineId_dimproductionline"; From="dimmachine.LineId"; To="dimproductionline.LineId"}
        @{Name="dimsensor_MachineId_dimmachine"; From="dimsensor.MachineId"; To="dimmachine.MachineId"}
        @{Name="dimoperator_LineId_dimproductionline"; From="dimoperator.LineId"; To="dimproductionline.LineId"; Inactive=$true}
        @{Name="factproductionbatch_LineId_dimproductionline"; From="factproductionbatch.LineId"; To="dimproductionline.LineId"; Inactive=$true}
        @{Name="factproductionbatch_ProductId_dimproduct"; From="factproductionbatch.ProductId"; To="dimproduct.ProductId"}
        @{Name="factqualitycheck_ProductId_dimproduct"; From="factqualitycheck.ProductId"; To="dimproduct.ProductId"; Inactive=$true}
        @{Name="factqualitycheck_LineId_dimproductionline"; From="factqualitycheck.LineId"; To="dimproductionline.LineId"; Inactive=$true}
        @{Name="factqualitycheck_InspectorId_dimoperator"; From="factqualitycheck.InspectorId"; To="dimoperator.OperatorId"}
        @{Name="factmaintenanceorder_MachineId_dimmachine"; From="factmaintenanceorder.MachineId"; To="dimmachine.MachineId"; Inactive=$true}
        @{Name="factmaintenanceorder_AssignedToOperatorId_dimoperator"; From="factmaintenanceorder.AssignedToOperatorId"; To="dimoperator.OperatorId"; Inactive=$true}
        @{Name="factalert_SensorId_dimsensor"; From="factalert.SensorId"; To="dimsensor.SensorId"}
        @{Name="sensortelemetry_SensorId_dimsensor"; From="sensortelemetry.SensorId"; To="dimsensor.SensorId"; Inactive=$true}
    )
    "ITAsset" = @(
        @{Name="dimrack_DataCenterId_dimdatacenter"; From="dimrack.DataCenterId"; To="dimdatacenter.DataCenterId"}
        @{Name="dimserver_RackId_dimrack"; From="dimserver.RackId"; To="dimrack.RackId"}
        @{Name="dimapplication_ServerId_dimserver"; From="dimapplication.ServerId"; To="dimserver.ServerId"}
        @{Name="dimdatabase_ServerId_dimserver"; From="dimdatabase.ServerId"; To="dimserver.ServerId"; Inactive=$true}
        @{Name="dimvirtualmachine_ServerId_dimserver"; From="dimvirtualmachine.ServerId"; To="dimserver.ServerId"; Inactive=$true}
        @{Name="dimnetworkdevice_DataCenterId_dimdatacenter"; From="dimnetworkdevice.DataCenterId"; To="dimdatacenter.DataCenterId"; Inactive=$true}
        @{Name="dimlicense_AppId_dimapplication"; From="dimlicense.AppId"; To="dimapplication.AppId"}
        @{Name="factalert_ServerId_dimserver"; From="factalert.ServerId"; To="dimserver.ServerId"; Inactive=$true}
        @{Name="factincident_ServerId_dimserver"; From="factincident.ServerId"; To="dimserver.ServerId"; Inactive=$true}
        @{Name="factincident_ReportedByUserId_dimuser"; From="factincident.ReportedByUserId"; To="dimuser.UserId"}
        @{Name="sensortelemetry_ServerId_dimserver"; From="sensortelemetry.ServerId"; To="dimserver.ServerId"; Inactive=$true}
    )
    "WindTurbine" = @(
        @{Name="dimturbine_WindFarmId_dimwindfarm"; From="dimturbine.WindFarmId"; To="dimwindfarm.WindFarmId"}
        @{Name="dimblade_TurbineId_dimturbine"; From="dimblade.TurbineId"; To="dimturbine.TurbineId"}
        @{Name="dimnacelle_TurbineId_dimturbine"; From="dimnacelle.TurbineId"; To="dimturbine.TurbineId"; Inactive=$true}
        @{Name="dimtower_TurbineId_dimturbine"; From="dimtower.TurbineId"; To="dimturbine.TurbineId"; Inactive=$true}
        @{Name="dimsensor_TurbineId_dimturbine"; From="dimsensor.TurbineId"; To="dimturbine.TurbineId"; Inactive=$true}
        @{Name="dimtechnician_WindFarmId_dimwindfarm"; From="dimtechnician.WindFarmId"; To="dimwindfarm.WindFarmId"; Inactive=$true}
        @{Name="dimweatherstation_WindFarmId_dimwindfarm"; From="dimweatherstation.WindFarmId"; To="dimwindfarm.WindFarmId"; Inactive=$true}
        @{Name="dimtransformer_WindFarmId_dimwindfarm"; From="dimtransformer.WindFarmId"; To="dimwindfarm.WindFarmId"; Inactive=$true}
        @{Name="factpoweroutput_TurbineId_dimturbine"; From="factpoweroutput.TurbineId"; To="dimturbine.TurbineId"; Inactive=$true}
        @{Name="factalert_TurbineId_dimturbine"; From="factalert.TurbineId"; To="dimturbine.TurbineId"; Inactive=$true}
        @{Name="factalert_SensorId_dimsensor"; From="factalert.SensorId"; To="dimsensor.SensorId"}
        @{Name="factmaintenanceevent_TurbineId_dimturbine"; From="factmaintenanceevent.TurbineId"; To="dimturbine.TurbineId"; Inactive=$true}
        @{Name="factmaintenanceevent_TechnicianId_dimtechnician"; From="factmaintenanceevent.TechnicianId"; To="dimtechnician.TechnicianId"}
        @{Name="sensortelemetry_TurbineId_dimturbine"; From="sensortelemetry.TurbineId"; To="dimturbine.TurbineId"; Inactive=$true}
        @{Name="sensortelemetry_SensorId_dimsensor"; From="sensortelemetry.SensorId"; To="dimsensor.SensorId"; Inactive=$true}
    )
    "Healthcare" = @(
        @{Name="dimdepartment_HospitalId_dimhospital"; From="dimdepartment.HospitalId"; To="dimhospital.HospitalId"}
        @{Name="dimward_DepartmentId_dimdepartment"; From="dimward.DepartmentId"; To="dimdepartment.DepartmentId"}
        @{Name="dimphysician_DepartmentId_dimdepartment"; From="dimphysician.DepartmentId"; To="dimdepartment.DepartmentId"; Inactive=$true}
        @{Name="dimnurse_WardId_dimward"; From="dimnurse.WardId"; To="dimward.WardId"}
        @{Name="dimpatient_WardId_dimward"; From="dimpatient.WardId"; To="dimward.WardId"; Inactive=$true}
        @{Name="dimmedicaldevice_WardId_dimward"; From="dimmedicaldevice.WardId"; To="dimward.WardId"; Inactive=$true}
        @{Name="dimsensor_DeviceId_dimmedicaldevice"; From="dimsensor.DeviceId"; To="dimmedicaldevice.DeviceId"}
        @{Name="factlabresult_PatientId_dimpatient"; From="factlabresult.PatientId"; To="dimpatient.PatientId"}
        @{Name="factlabresult_PhysicianId_dimphysician"; From="factlabresult.PhysicianId"; To="dimphysician.PhysicianId"; Inactive=$true}
        @{Name="factprocedure_PatientId_dimpatient"; From="factprocedure.PatientId"; To="dimpatient.PatientId"; Inactive=$true}
        @{Name="factprocedure_PhysicianId_dimphysician"; From="factprocedure.PhysicianId"; To="dimphysician.PhysicianId"; Inactive=$true}
        @{Name="factmedicationadmin_PatientId_dimpatient"; From="factmedicationadmin.PatientId"; To="dimpatient.PatientId"; Inactive=$true}
        @{Name="factmedicationadmin_MedicationId_dimmedication"; From="factmedicationadmin.MedicationId"; To="dimmedication.MedicationId"}
        @{Name="factmedicationadmin_NurseId_dimnurse"; From="factmedicationadmin.NurseId"; To="dimnurse.NurseId"; Inactive=$true}
        @{Name="bridgewarddevice_WardId_dimward"; From="bridgewarddevice.WardId"; To="dimward.WardId"; Inactive=$true}
        @{Name="bridgewarddevice_DeviceId_dimmedicaldevice"; From="bridgewarddevice.DeviceId"; To="dimmedicaldevice.DeviceId"; Inactive=$true}
        @{Name="sensortelemetry_DeviceId_dimmedicaldevice"; From="sensortelemetry.DeviceId"; To="dimmedicaldevice.DeviceId"; Inactive=$true}
    )
}

# ============================================================================
# MEASURE DEFINITIONS
# ============================================================================

$measures = @{
    "SmartBuilding" = @{
        "dimbuilding" = @(
            @{Name="Building Count"; DAX="COUNTROWS(dimbuilding)"; Format=$null}
            @{Name="Total Building Area SqFt"; DAX="SUM(dimbuilding[TotalAreaSqFt])"; Format="#,0"}
        )
        "dimfloor" = @(
            @{Name="Floor Count"; DAX="COUNTROWS(dimfloor)"; Format=$null}
            @{Name="Total Floor Area SqFt"; DAX="SUM(dimfloor[AreaSqFt])"; Format="#,0"}
        )
        "dimzone" = @(
            @{Name="Zone Count"; DAX="COUNTROWS(dimzone)"; Format=$null}
            @{Name="Total Max Occupancy"; DAX="SUM(dimzone[MaxOccupancy])"; Format="#,0"}
        )
        "dimsensor" = @(
            @{Name="Sensor Count"; DAX="COUNTROWS(dimsensor)"; Format=$null}
            @{Name="Active Sensor Count"; DAX='CALCULATE(COUNTROWS(dimsensor), dimsensor[Status] = "Active")'; Format=$null}
        )
        "dimhvacsystem" = @(
            @{Name="HVAC System Count"; DAX="COUNTROWS(dimhvacsystem)"; Format=$null}
            @{Name="Total HVAC Capacity BTU"; DAX="SUM(dimhvacsystem[CapacityBTU])"; Format="#,0"}
        )
        "dimlightingsystem" = @(
            @{Name="Lighting System Count"; DAX="COUNTROWS(dimlightingsystem)"; Format=$null}
            @{Name="Total Wattage W"; DAX="SUM(dimlightingsystem[WattageTotalW])"; Format="#,0"}
        )
        "dimaccesspoint" = @(
            @{Name="Access Point Count"; DAX="COUNTROWS(dimaccesspoint)"; Format=$null}
        )
        "dimoccupant" = @(
            @{Name="Occupant Count"; DAX="COUNTROWS(dimoccupant)"; Format=$null}
        )
        "dimelevator" = @(
            @{Name="Elevator Count"; DAX="COUNTROWS(dimelevator)"; Format=$null}
        )
        "dimenergymeter" = @(
            @{Name="Energy Meter Count"; DAX="COUNTROWS(dimenergymeter)"; Format=$null}
        )
        "factalert" = @(
            @{Name="Alert Count"; DAX="COUNTROWS(factalert)"; Format=$null}
            @{Name="Critical Alert Count"; DAX='CALCULATE(COUNTROWS(factalert), factalert[Severity] = "Critical")'; Format=$null}
            @{Name="Avg Alert Value"; DAX="AVERAGE(factalert[AlertValue])"; Format="#,0.00"}
        )
        "factmaintenanceticket" = @(
            @{Name="Ticket Count"; DAX="COUNTROWS(factmaintenanceticket)"; Format=$null}
            @{Name="Total Maintenance Cost"; DAX="SUM(factmaintenanceticket[CostUSD])"; Format="$#,0.00"}
            @{Name="Avg Ticket Duration Hours"; DAX="AVERAGE(factmaintenanceticket[DurationHours])"; Format="#,0.0"}
        )
        "sensortelemetry" = @(
            @{Name="Reading Count"; DAX="COUNTROWS(sensortelemetry)"; Format=$null}
            @{Name="Avg Reading Value"; DAX="AVERAGE(sensortelemetry[ReadingValue])"; Format="#,0.00"}
        )
    }
    "ManufacturingPlant" = @{
        "dimplant" = @(
            @{Name="Plant Count"; DAX="COUNTROWS(dimplant)"; Format=$null}
            @{Name="Total Production Capacity"; DAX="SUM(dimplant[ProductionCapacity])"; Format="#,0"}
        )
        "dimproductionline" = @(
            @{Name="Production Line Count"; DAX="COUNTROWS(dimproductionline)"; Format=$null}
            @{Name="Total Line Capacity"; DAX="SUM(dimproductionline[CapacityUnitsPerHour])"; Format="#,0"}
        )
        "dimmachine" = @(
            @{Name="Machine Count"; DAX="COUNTROWS(dimmachine)"; Format=$null}
        )
        "dimsensor" = @(
            @{Name="Sensor Count"; DAX="COUNTROWS(dimsensor)"; Format=$null}
        )
        "dimproduct" = @(
            @{Name="Product Count"; DAX="COUNTROWS(dimproduct)"; Format=$null}
        )
        "dimmaterial" = @(
            @{Name="Material Count"; DAX="COUNTROWS(dimmaterial)"; Format=$null}
            @{Name="Total Material Value"; DAX="SUMX(dimmaterial, dimmaterial[UnitCost] * dimmaterial[CurrentStock])"; Format="$#,0.00"}
        )
        "dimoperator" = @(
            @{Name="Operator Count"; DAX="COUNTROWS(dimoperator)"; Format=$null}
        )
        "factproductionbatch" = @(
            @{Name="Batch Count"; DAX="COUNTROWS(factproductionbatch)"; Format=$null}
            @{Name="Total Quantity Produced"; DAX="SUM(factproductionbatch[QuantityProduced])"; Format="#,0"}
            @{Name="Avg Defect Rate"; DAX="AVERAGE(factproductionbatch[DefectRate])"; Format="0.00%"}
            @{Name="Total Energy Used KWh"; DAX="SUM(factproductionbatch[EnergyUsedKWh])"; Format="#,0.00"}
        )
        "factqualitycheck" = @(
            @{Name="QC Check Count"; DAX="COUNTROWS(factqualitycheck)"; Format=$null}
            @{Name="Total Defect Count"; DAX="SUM(factqualitycheck[DefectCount])"; Format="#,0"}
            @{Name="Pass Rate"; DAX='DIVIDE(CALCULATE(COUNTROWS(factqualitycheck), factqualitycheck[Result] = "Pass"), COUNTROWS(factqualitycheck), 0)'; Format="0.00%"}
        )
        "factmaintenanceorder" = @(
            @{Name="Maintenance Order Count"; DAX="COUNTROWS(factmaintenanceorder)"; Format=$null}
            @{Name="Total Maintenance Cost"; DAX="SUM(factmaintenanceorder[CostUSD])"; Format="$#,0.00"}
            @{Name="Avg Repair Duration Hours"; DAX="AVERAGE(factmaintenanceorder[DurationHours])"; Format="#,0.0"}
        )
        "factalert" = @(
            @{Name="Alert Count"; DAX="COUNTROWS(factalert)"; Format=$null}
            @{Name="Critical Alert Count"; DAX='CALCULATE(COUNTROWS(factalert), factalert[Severity] = "Critical")'; Format=$null}
        )
        "sensortelemetry" = @(
            @{Name="Reading Count"; DAX="COUNTROWS(sensortelemetry)"; Format=$null}
            @{Name="Avg Reading Value"; DAX="AVERAGE(sensortelemetry[ReadingValue])"; Format="#,0.00"}
        )
    }
    "ITAsset" = @{
        "dimdatacenter" = @(
            @{Name="Data Center Count"; DAX="COUNTROWS(dimdatacenter)"; Format=$null}
            @{Name="Total Power Capacity KW"; DAX="SUM(dimdatacenter[PowerCapacityKW])"; Format="#,0"}
        )
        "dimrack" = @(
            @{Name="Rack Count"; DAX="COUNTROWS(dimrack)"; Format=$null}
            @{Name="Total Max Power W"; DAX="SUM(dimrack[MaxPowerW])"; Format="#,0"}
            @{Name="Total Current Power W"; DAX="SUM(dimrack[CurrentPowerW])"; Format="#,0"}
        )
        "dimserver" = @(
            @{Name="Server Count"; DAX="COUNTROWS(dimserver)"; Format=$null}
            @{Name="Total CPU Cores"; DAX="SUM(dimserver[CPUCores])"; Format="#,0"}
            @{Name="Total Memory GB"; DAX="SUM(dimserver[MemoryGB])"; Format="#,0"}
            @{Name="Total Storage TB"; DAX="SUM(dimserver[StorageTB])"; Format="#,0.00"}
        )
        "dimapplication" = @(
            @{Name="Application Count"; DAX="COUNTROWS(dimapplication)"; Format=$null}
        )
        "dimdatabase" = @(
            @{Name="Database Count"; DAX="COUNTROWS(dimdatabase)"; Format=$null}
            @{Name="Total DB Size GB"; DAX="SUM(dimdatabase[SizeGB])"; Format="#,0.00"}
        )
        "dimvirtualmachine" = @(
            @{Name="VM Count"; DAX="COUNTROWS(dimvirtualmachine)"; Format=$null}
            @{Name="Total vCPU"; DAX="SUM(dimvirtualmachine[vCPU])"; Format="#,0"}
        )
        "dimnetworkdevice" = @(
            @{Name="Network Device Count"; DAX="COUNTROWS(dimnetworkdevice)"; Format=$null}
        )
        "dimuser" = @(
            @{Name="User Count"; DAX="COUNTROWS(dimuser)"; Format=$null}
        )
        "dimlicense" = @(
            @{Name="License Count"; DAX="COUNTROWS(dimlicense)"; Format=$null}
            @{Name="Total License Cost"; DAX="SUM(dimlicense[CostUSD])"; Format="$#,0.00"}
            @{Name="License Utilization"; DAX="DIVIDE(SUM(dimlicense[AssignedSeats]), SUM(dimlicense[Seats]), 0)"; Format="0.00%"}
        )
        "factalert" = @(
            @{Name="Alert Count"; DAX="COUNTROWS(factalert)"; Format=$null}
            @{Name="Critical Alert Count"; DAX='CALCULATE(COUNTROWS(factalert), factalert[Severity] = "Critical")'; Format=$null}
        )
        "factincident" = @(
            @{Name="Incident Count"; DAX="COUNTROWS(factincident)"; Format=$null}
            @{Name="Avg Resolution Hours"; DAX="AVERAGE(factincident[DurationHours])"; Format="#,0.0"}
            @{Name="Open Incident Count"; DAX='CALCULATE(COUNTROWS(factincident), factincident[Status] = "Open")'; Format=$null}
        )
        "sensortelemetry" = @(
            @{Name="Telemetry Reading Count"; DAX="COUNTROWS(sensortelemetry)"; Format=$null}
            @{Name="Avg CPU Percent"; DAX="AVERAGE(sensortelemetry[CPUPercent])"; Format="#,0.00"}
            @{Name="Avg Memory Percent"; DAX="AVERAGE(sensortelemetry[MemoryPercent])"; Format="#,0.00"}
        )
    }
    "WindTurbine" = @{
        "dimwindfarm" = @(
            @{Name="Wind Farm Count"; DAX="COUNTROWS(dimwindfarm)"; Format=$null}
            @{Name="Total Installed Capacity MW"; DAX="SUM(dimwindfarm[InstalledCapacityMW])"; Format="#,0.00"}
        )
        "dimturbine" = @(
            @{Name="Turbine Count"; DAX="COUNTROWS(dimturbine)"; Format=$null}
            @{Name="Total Rated Capacity KW"; DAX="SUM(dimturbine[RatedCapacityKW])"; Format="#,0"}
        )
        "dimblade" = @(
            @{Name="Blade Count"; DAX="COUNTROWS(dimblade)"; Format=$null}
        )
        "dimnacelle" = @(
            @{Name="Nacelle Count"; DAX="COUNTROWS(dimnacelle)"; Format=$null}
        )
        "dimtower" = @(
            @{Name="Tower Count"; DAX="COUNTROWS(dimtower)"; Format=$null}
        )
        "dimsensor" = @(
            @{Name="Sensor Count"; DAX="COUNTROWS(dimsensor)"; Format=$null}
        )
        "dimtechnician" = @(
            @{Name="Technician Count"; DAX="COUNTROWS(dimtechnician)"; Format=$null}
            @{Name="Avg Years Experience"; DAX="AVERAGE(dimtechnician[YearsExperience])"; Format="#,0.0"}
        )
        "dimweatherstation" = @(
            @{Name="Weather Station Count"; DAX="COUNTROWS(dimweatherstation)"; Format=$null}
        )
        "dimtransformer" = @(
            @{Name="Transformer Count"; DAX="COUNTROWS(dimtransformer)"; Format=$null}
            @{Name="Total Rating MVA"; DAX="SUM(dimtransformer[RatingMVA])"; Format="#,0.00"}
        )
        "factpoweroutput" = @(
            @{Name="Output Record Count"; DAX="COUNTROWS(factpoweroutput)"; Format=$null}
            @{Name="Total Power Output KW"; DAX="SUM(factpoweroutput[PowerOutputKW])"; Format="#,0"}
            @{Name="Avg Capacity Factor"; DAX="AVERAGE(factpoweroutput[CapacityFactor])"; Format="0.00%"}
            @{Name="Avg Wind Speed Ms"; DAX="AVERAGE(factpoweroutput[WindSpeedMs])"; Format="#,0.00"}
        )
        "factalert" = @(
            @{Name="Alert Count"; DAX="COUNTROWS(factalert)"; Format=$null}
            @{Name="Critical Alert Count"; DAX='CALCULATE(COUNTROWS(factalert), factalert[Severity] = "Critical")'; Format=$null}
        )
        "factmaintenanceevent" = @(
            @{Name="Maintenance Event Count"; DAX="COUNTROWS(factmaintenanceevent)"; Format=$null}
            @{Name="Total Maintenance Cost"; DAX="SUM(factmaintenanceevent[CostUSD])"; Format="$#,0.00"}
            @{Name="Avg Maintenance Duration Hours"; DAX="AVERAGE(factmaintenanceevent[DurationHours])"; Format="#,0.0"}
        )
        "sensortelemetry" = @(
            @{Name="Telemetry Count"; DAX="COUNTROWS(sensortelemetry)"; Format=$null}
        )
    }
    "Healthcare" = @{
        "dimhospital" = @(
            @{Name="Hospital Count"; DAX="COUNTROWS(dimhospital)"; Format=$null}
            @{Name="Total Bed Capacity"; DAX="SUM(dimhospital[BedCapacity])"; Format="#,0"}
        )
        "dimdepartment" = @(
            @{Name="Department Count"; DAX="COUNTROWS(dimdepartment)"; Format=$null}
            @{Name="Total Department Beds"; DAX="SUM(dimdepartment[BedCount])"; Format="#,0"}
        )
        "dimward" = @(
            @{Name="Ward Count"; DAX="COUNTROWS(dimward)"; Format=$null}
            @{Name="Total Ward Beds"; DAX="SUM(dimward[BedCount])"; Format="#,0"}
        )
        "dimphysician" = @(
            @{Name="Physician Count"; DAX="COUNTROWS(dimphysician)"; Format=$null}
            @{Name="Avg Physician Experience"; DAX="AVERAGE(dimphysician[YearsExperience])"; Format="#,0.0"}
        )
        "dimnurse" = @(
            @{Name="Nurse Count"; DAX="COUNTROWS(dimnurse)"; Format=$null}
            @{Name="Avg Nurse Experience"; DAX="AVERAGE(dimnurse[YearsExperience])"; Format="#,0.0"}
        )
        "dimpatient" = @(
            @{Name="Patient Count"; DAX="COUNTROWS(dimpatient)"; Format=$null}
        )
        "dimmedicaldevice" = @(
            @{Name="Medical Device Count"; DAX="COUNTROWS(dimmedicaldevice)"; Format=$null}
        )
        "dimmedication" = @(
            @{Name="Medication Count"; DAX="COUNTROWS(dimmedication)"; Format=$null}
            @{Name="Avg Medication Unit Cost"; DAX="AVERAGE(dimmedication[UnitCost])"; Format="$#,0.00"}
        )
        "dimsensor" = @(
            @{Name="Sensor Count"; DAX="COUNTROWS(dimsensor)"; Format=$null}
        )
        "factlabresult" = @(
            @{Name="Lab Result Count"; DAX="COUNTROWS(factlabresult)"; Format=$null}
            @{Name="Avg Result Value"; DAX="AVERAGE(factlabresult[ResultValue])"; Format="#,0.00"}
        )
        "factprocedure" = @(
            @{Name="Procedure Count"; DAX="COUNTROWS(factprocedure)"; Format=$null}
            @{Name="Avg Procedure Duration Min"; DAX="AVERAGE(factprocedure[DurationMinutes])"; Format="#,0.0"}
        )
        "factmedicationadmin" = @(
            @{Name="Medication Admin Count"; DAX="COUNTROWS(factmedicationadmin)"; Format=$null}
        )
        "bridgewarddevice" = @(
            @{Name="Ward-Device Mappings"; DAX="COUNTROWS(bridgewarddevice)"; Format=$null}
        )
        "sensortelemetry" = @(
            @{Name="Telemetry Count"; DAX="COUNTROWS(sensortelemetry)"; Format=$null}
        )
    }
}

# ============================================================================
# DOMAIN DESCRIPTIONS (for documentation)
# ============================================================================

$descriptions = @{
    "SmartBuilding"      = "Direct Lake semantic model for Smart Building ontology - 13 tables with building, zone, sensor, and maintenance analysis"
    "ManufacturingPlant" = "Direct Lake semantic model for Manufacturing Plant ontology - 12 tables with plant, production, quality, and maintenance analysis"
    "ITAsset"            = "Direct Lake semantic model for IT Asset Management ontology - 12 tables with datacenter, server, application, and incident analysis"
    "WindTurbine"        = "Direct Lake semantic model for Wind Turbine ontology - 13 tables with turbine, power output, weather, and maintenance analysis"
    "Healthcare"         = "Direct Lake semantic model for Healthcare ontology - 14 tables with hospital, patient, physician, procedure, and lab analysis"
}

# LineageTag prefix per domain
$lineagePrefixes = @{
    "SmartBuilding"      = 20000000
    "ManufacturingPlant" = 30000000
    "ITAsset"            = 40000000
    "WindTurbine"        = 50000000
    "Healthcare"         = 60000000
}

# ============================================================================
# GENERATION FUNCTIONS
# ============================================================================

function New-Pbism {
    return @'
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
  "version": "4.2",
  "settings": {
    "qnaEnabled": true
  }
}
'@
}

function New-DatabaseTmdl {
    return "database`r`n`tcompatibilityLevel: 1604`r`n"
}

function New-ExpressionsTmdl {
    return 'expression DatabaseQuery = let database = Sql.Database("{{SQL_ENDPOINT}}", "{{LAKEHOUSE_NAME}}") in database' + "`r`n"
}

function New-ModelTmdl {
    param([string[]]$TableNames)
    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.AppendLine("model Model")
    [void]$sb.AppendLine("`tculture: en-US")
    [void]$sb.AppendLine("`tdefaultPowerBIDataSourceVersion: powerBI_V3")
    [void]$sb.AppendLine("`tdataAccessOptions")
    [void]$sb.AppendLine("`t`tlegacyRedirects")
    [void]$sb.AppendLine("`t`treturnErrorValuesAsNull")
    [void]$sb.AppendLine("")

    $queryOrder = ($TableNames | ForEach-Object { "`"$_`"" }) -join ","
    [void]$sb.AppendLine("annotation PBI_QueryOrder = [$queryOrder]")
    [void]$sb.AppendLine("")

    foreach ($t in $TableNames) {
        [void]$sb.AppendLine("ref table $t")
    }
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("ref expression DatabaseQuery")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("annotation PBI_QueryRelationships = []")
    return $sb.ToString()
}

function New-RelationshipsTmdl {
    param([array]$Rels)
    $sb = [System.Text.StringBuilder]::new()
    foreach ($r in $Rels) {
        $fromParts = $r.From -split "\."
        $toParts = $r.To -split "\."
        [void]$sb.AppendLine("relationship $($r.Name)")
        if ($r.Inactive -eq $true) {
            [void]$sb.AppendLine("`tisActive: false")
        }
        [void]$sb.AppendLine("`tfromColumn: $($fromParts[0]).$($fromParts[1])")
        [void]$sb.AppendLine("`ttoColumn: $($toParts[0]).$($toParts[1])")
        [void]$sb.AppendLine("`tcrossFilteringBehavior: oneDirection")
        [void]$sb.AppendLine("")
    }
    return $sb.ToString()
}

function New-TableTmdl {
    param(
        [string]$TableName,
        [string[]]$Columns,
        [string[]]$DomainNumericCols,
        [array]$TableMeasures,
        [int]$TableIndex,
        [long]$LineageBase
    )
    $tableTag = "{0:D8}-0001-0001-0001-000000000001" -f ($LineageBase + $TableIndex)
    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.AppendLine("table $TableName")
    [void]$sb.AppendLine("`tlineageTag: $tableTag")
    [void]$sb.AppendLine("")

    # Measures
    if ($TableMeasures -and $TableMeasures.Count -gt 0) {
        $mi = 1
        foreach ($m in $TableMeasures) {
            $mTag = "{0:D8}-0001-0001-0001-1{0:D11}" -f ($LineageBase + $TableIndex), $mi
            # Correct format: use separate formatting
            $mTagBase = "{0:D8}" -f ($LineageBase + $TableIndex)
            $mTagSuffix = "1{0:D11}" -f $mi
            $mTag = "$mTagBase-0001-0001-0001-$mTagSuffix"
            [void]$sb.AppendLine("`tmeasure '$($m.Name)' = $($m.DAX)")
            if ($m.Format) {
                [void]$sb.AppendLine("`t`tformatString: $($m.Format)")
            }
            [void]$sb.AppendLine("`t`tdisplayFolder: Measures")
            [void]$sb.AppendLine("`t`tlineageTag: $mTag")
            [void]$sb.AppendLine("")
            $mi++
        }
    }

    # Columns
    $ci = 1
    foreach ($col in $Columns) {
        $cTagBase = "{0:D8}" -f ($LineageBase + $TableIndex)
        $cTagSuffix = "2{0:D11}" -f $ci
        $cTag = "$cTagBase-0001-0001-0001-$cTagSuffix"

        $isNumeric = $DomainNumericCols -contains $col
        $dataType = if ($isNumeric) { "double" } else { "string" }
        $summarize = "none"

        [void]$sb.AppendLine("`tcolumn $col")
        [void]$sb.AppendLine("`t`tdataType: $dataType")
        [void]$sb.AppendLine("`t`tsummarizeBy: $summarize")
        [void]$sb.AppendLine("`t`tsourceColumn: $col")
        [void]$sb.AppendLine("`t`tlineageTag: $cTag")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("`t`tannotation SummarizationSetBy = Automatic")
        [void]$sb.AppendLine("")
        $ci++
    }

    # Partition (Direct Lake)
    [void]$sb.AppendLine("`tpartition $TableName = entity")
    [void]$sb.AppendLine("`t`tmode: directLake")
    [void]$sb.AppendLine("`t`tentityName: $TableName")
    [void]$sb.AppendLine("`t`texpressionSource: DatabaseQuery")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("`tannotation PBI_ResultType = Table")

    return $sb.ToString()
}

# ============================================================================
# MAIN GENERATION LOOP
# ============================================================================

$domainsToProcess = if ($Domain -eq "All") { @("SmartBuilding","ManufacturingPlant","ITAsset","WindTurbine","Healthcare") } else { @($Domain) }
$totalFiles = 0

foreach ($domainName in $domainsToProcess) {
    Write-Host ""
    Write-Host "=== Generating TMDL for $domainName ===" -ForegroundColor Cyan

    $dataDir = Join-Path $BasePath "ontologies\$domainName\data"
    if (-not (Test-Path $dataDir)) { Write-Warning "Data folder not found: $dataDir"; continue }

    $smDir    = Join-Path $BasePath "ontologies\$domainName\SemanticModel"
    $defDir   = Join-Path $smDir "definition"
    $tabDir   = Join-Path $defDir "tables"
    New-Item -Path $tabDir -ItemType Directory -Force | Out-Null

    # Discover CSVs and build table list
    $csvFiles = Get-ChildItem -Path $dataDir -Filter "*.csv" -File | Sort-Object Name
    $tableNames = @()
    $tableColumns = @{}

    foreach ($csv in $csvFiles) {
        $tableName = $csv.BaseName.ToLower()
        $header = (Get-Content $csv.FullName -TotalCount 1) -split ","
        $tableNames += $tableName
        $tableColumns[$tableName] = $header
    }

    Write-Host "  Tables: $($tableNames.Count)" -ForegroundColor Gray
    $domainNumeric = $numericColumns[$domainName]
    $domainRels = $relationships[$domainName]
    $domainMeasures = $measures[$domainName]
    $lineageBase = $lineagePrefixes[$domainName]

    # 1. definition.pbism
    $pbismPath = Join-Path $smDir "definition.pbism"
    [System.IO.File]::WriteAllText($pbismPath, (New-Pbism), [System.Text.UTF8Encoding]::new($false))
    Write-Host "  Created definition.pbism" -ForegroundColor Gray
    $totalFiles++

    # 2. database.tmdl
    $dbPath = Join-Path $defDir "database.tmdl"
    [System.IO.File]::WriteAllText($dbPath, (New-DatabaseTmdl), [System.Text.UTF8Encoding]::new($false))
    Write-Host "  Created database.tmdl" -ForegroundColor Gray
    $totalFiles++

    # 3. expressions.tmdl
    $exprPath = Join-Path $defDir "expressions.tmdl"
    [System.IO.File]::WriteAllText($exprPath, (New-ExpressionsTmdl), [System.Text.UTF8Encoding]::new($false))
    Write-Host "  Created expressions.tmdl" -ForegroundColor Gray
    $totalFiles++

    # 4. model.tmdl
    $modelPath = Join-Path $defDir "model.tmdl"
    [System.IO.File]::WriteAllText($modelPath, (New-ModelTmdl -TableNames $tableNames), [System.Text.UTF8Encoding]::new($false))
    Write-Host "  Created model.tmdl" -ForegroundColor Gray
    $totalFiles++

    # 5. relationships.tmdl
    $relsPath = Join-Path $defDir "relationships.tmdl"
    [System.IO.File]::WriteAllText($relsPath, (New-RelationshipsTmdl -Rels $domainRels), [System.Text.UTF8Encoding]::new($false))
    Write-Host "  Created relationships.tmdl ($($domainRels.Count) relationships)" -ForegroundColor Gray
    $totalFiles++

    # 6. Per-table TMDL files
    $tableIndex = 1
    foreach ($tableName in $tableNames) {
        $columns = $tableColumns[$tableName]
        $tblMeasures = $domainMeasures[$tableName]
        $tmdlContent = New-TableTmdl -TableName $tableName -Columns $columns -DomainNumericCols $domainNumeric -TableMeasures $tblMeasures -TableIndex $tableIndex -LineageBase $lineageBase
        $tmdlPath = Join-Path $tabDir "$tableName.tmdl"
        [System.IO.File]::WriteAllText($tmdlPath, $tmdlContent, [System.Text.UTF8Encoding]::new($false))
        Write-Host "  Created tables/$tableName.tmdl ($($columns.Count) cols, $(if($tblMeasures){$tblMeasures.Count}else{0}) measures)" -ForegroundColor Gray
        $totalFiles++
        $tableIndex++
    }

    Write-Host "  $domainName complete." -ForegroundColor Green
}

Write-Host ""
Write-Host "=======================================" -ForegroundColor Green
Write-Host "  Generated $totalFiles TMDL files" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green
