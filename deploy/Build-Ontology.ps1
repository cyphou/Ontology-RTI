# Build-Ontology.ps1
# Builds the complete Oil & Gas Refinery Ontology definition for Microsoft Fabric
# Entity Types, Data Bindings (Lakehouse + KQL), Relationships, and Contextualizations
param(
    [string]$WorkspaceId,
    [string]$LakehouseId,
    [string]$KqlDatabaseId,
    [string]$KqlClusterUri,
    [string]$KqlDatabaseName,
    [string]$OntologyId,
    [string]$FabricToken
)

$headers = @{ Authorization = "Bearer $FabricToken"; "Content-Type" = "application/json" }

# ============================================================================
# Helper: encode JSON to Base64
# ============================================================================
function ToBase64([string]$text) {
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($text))
}

# Deterministic GUID from a seed string (ensures idempotent re-pushes)
function DeterministicGuid([string]$seed) {
    $hash = [System.Security.Cryptography.MD5]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($seed))
    return ([guid]::new($hash)).ToString()
}

# ============================================================================
# ID Allocation Plan (unique 64-bit integers)
# Entity Type IDs:      1001 - 1013
# Property IDs:         2001 - 2999 (allocated per entity)
# Relationship IDs:     3001 - 3020
# Timeseries Prop IDs:  4001 - 4099
# ============================================================================

# ============================================================================
# 1. ENTITY TYPES + PROPERTIES
# ============================================================================

$entityTypes = @()

# --- DimRefinery (ID: 1001) ---
$entityTypes += @{
    id = "1001"; name = "Refinery"
    entityIdParts = @("2001")
    displayNamePropertyId = "2002"
    properties = @(
        @{ id = "2001"; name = "RefineryId"; valueType = "String" },
        @{ id = "2002"; name = "RefineryName"; valueType = "String" },
        @{ id = "2003"; name = "Country"; valueType = "String" },
        @{ id = "2004"; name = "State"; valueType = "String" },
        @{ id = "2005"; name = "City"; valueType = "String" },
        @{ id = "2006"; name = "Latitude"; valueType = "Double" },
        @{ id = "2007"; name = "Longitude"; valueType = "Double" },
        @{ id = "2008"; name = "CapacityBPD"; valueType = "BigInt" },
        @{ id = "2009"; name = "YearBuilt"; valueType = "BigInt" },
        @{ id = "2010"; name = "Status"; valueType = "String" },
        @{ id = "2011"; name = "Operator"; valueType = "String" }
    )
    tableName = "dimrefinery"
}

# --- DimProcessUnit (ID: 1002) ---
$entityTypes += @{
    id = "1002"; name = "ProcessUnit"
    entityIdParts = @("2101")
    displayNamePropertyId = "2102"
    properties = @(
        @{ id = "2101"; name = "ProcessUnitId"; valueType = "String" },
        @{ id = "2102"; name = "ProcessUnitName"; valueType = "String" },
        @{ id = "2103"; name = "ProcessUnitType"; valueType = "String" },
        @{ id = "2104"; name = "RefineryId"; valueType = "String" },
        @{ id = "2105"; name = "CapacityBPD"; valueType = "BigInt" },
        @{ id = "2106"; name = "DesignTemperatureF"; valueType = "Double" },
        @{ id = "2107"; name = "DesignPressurePSI"; valueType = "Double" },
        @{ id = "2108"; name = "YearInstalled"; valueType = "BigInt" },
        @{ id = "2109"; name = "Status"; valueType = "String" },
        @{ id = "2110"; name = "Description"; valueType = "String" }
    )
    tableName = "dimprocessunit"
}

# --- DimEquipment (ID: 1003) ---
$entityTypes += @{
    id = "1003"; name = "Equipment"
    entityIdParts = @("2201")
    displayNamePropertyId = "2202"
    properties = @(
        @{ id = "2201"; name = "EquipmentId"; valueType = "String" },
        @{ id = "2202"; name = "EquipmentName"; valueType = "String" },
        @{ id = "2203"; name = "EquipmentType"; valueType = "String" },
        @{ id = "2204"; name = "ProcessUnitId"; valueType = "String" },
        @{ id = "2205"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2206"; name = "Model"; valueType = "String" },
        @{ id = "2207"; name = "InstallDate"; valueType = "String" },
        @{ id = "2208"; name = "LastInspectionDate"; valueType = "String" },
        @{ id = "2209"; name = "Status"; valueType = "String" },
        @{ id = "2210"; name = "CriticalityLevel"; valueType = "String" },
        @{ id = "2211"; name = "ExpectedLifeYears"; valueType = "BigInt" }
    )
    tableName = "dimequipment"
}

# --- DimPipeline (ID: 1004) ---
$entityTypes += @{
    id = "1004"; name = "Pipeline"
    entityIdParts = @("2301")
    displayNamePropertyId = "2302"
    properties = @(
        @{ id = "2301"; name = "PipelineId"; valueType = "String" },
        @{ id = "2302"; name = "PipelineName"; valueType = "String" },
        @{ id = "2303"; name = "FromProcessUnitId"; valueType = "String" },
        @{ id = "2304"; name = "ToProcessUnitId"; valueType = "String" },
        @{ id = "2305"; name = "RefineryId"; valueType = "String" },
        @{ id = "2306"; name = "DiameterInches"; valueType = "Double" },
        @{ id = "2307"; name = "LengthFeet"; valueType = "Double" },
        @{ id = "2308"; name = "Material"; valueType = "String" },
        @{ id = "2309"; name = "MaxFlowBPD"; valueType = "BigInt" },
        @{ id = "2310"; name = "InstalledDate"; valueType = "String" },
        @{ id = "2311"; name = "Status"; valueType = "String" }
    )
    tableName = "dimpipeline"
}

# --- DimCrudeOil (ID: 1005) ---
$entityTypes += @{
    id = "1005"; name = "CrudeOil"
    entityIdParts = @("2401")
    displayNamePropertyId = "2402"
    properties = @(
        @{ id = "2401"; name = "CrudeOilId"; valueType = "String" },
        @{ id = "2402"; name = "CrudeGradeName"; valueType = "String" },
        @{ id = "2403"; name = "APIGravity"; valueType = "Double" },
        @{ id = "2404"; name = "SulfurContentPct"; valueType = "Double" },
        @{ id = "2405"; name = "Origin"; valueType = "String" },
        @{ id = "2406"; name = "Classification"; valueType = "String" },
        @{ id = "2407"; name = "PricePerBarrelUSD"; valueType = "Double" },
        @{ id = "2408"; name = "Description"; valueType = "String" }
    )
    tableName = "dimcrudeoil"
}

# --- DimRefinedProduct (ID: 1006) ---
$entityTypes += @{
    id = "1006"; name = "RefinedProduct"
    entityIdParts = @("2501")
    displayNamePropertyId = "2502"
    properties = @(
        @{ id = "2501"; name = "ProductId"; valueType = "String" },
        @{ id = "2502"; name = "ProductName"; valueType = "String" },
        @{ id = "2503"; name = "ProductCategory"; valueType = "String" },
        @{ id = "2504"; name = "APIGravity"; valueType = "Double" },
        @{ id = "2505"; name = "SulfurLimitPPM"; valueType = "Double" },
        @{ id = "2506"; name = "FlashPointF"; valueType = "Double" },
        @{ id = "2507"; name = "SpecStandard"; valueType = "String" },
        @{ id = "2508"; name = "PricePerBarrelUSD"; valueType = "Double" },
        @{ id = "2509"; name = "Description"; valueType = "String" }
    )
    tableName = "dimrefinedproduct"
}

# --- DimStorageTank (ID: 1007) ---
$entityTypes += @{
    id = "1007"; name = "StorageTank"
    entityIdParts = @("2601")
    displayNamePropertyId = "2602"
    properties = @(
        @{ id = "2601"; name = "TankId"; valueType = "String" },
        @{ id = "2602"; name = "TankName"; valueType = "String" },
        @{ id = "2603"; name = "RefineryId"; valueType = "String" },
        @{ id = "2604"; name = "ProductId"; valueType = "String" },
        @{ id = "2605"; name = "TankType"; valueType = "String" },
        @{ id = "2606"; name = "CapacityBarrels"; valueType = "BigInt" },
        @{ id = "2607"; name = "CurrentLevelBarrels"; valueType = "BigInt" },
        @{ id = "2608"; name = "DiameterFeet"; valueType = "String" },
        @{ id = "2609"; name = "HeightFeet"; valueType = "String" },
        @{ id = "2610"; name = "Material"; valueType = "String" },
        @{ id = "2611"; name = "Status"; valueType = "String" },
        @{ id = "2612"; name = "LastInspectionDate"; valueType = "String" }
    )
    tableName = "dimstoragetank"
}

# --- DimSensor (ID: 1008) ---
$entityTypes += @{
    id = "1008"; name = "Sensor"
    entityIdParts = @("2701")
    displayNamePropertyId = "2702"
    properties = @(
        @{ id = "2701"; name = "SensorId"; valueType = "String" },
        @{ id = "2702"; name = "SensorName"; valueType = "String" },
        @{ id = "2703"; name = "SensorType"; valueType = "String" },
        @{ id = "2704"; name = "EquipmentId"; valueType = "String" },
        @{ id = "2705"; name = "MeasurementUnit"; valueType = "String" },
        @{ id = "2706"; name = "MinRange"; valueType = "Double" },
        @{ id = "2707"; name = "MaxRange"; valueType = "Double" },
        @{ id = "2708"; name = "InstallDate"; valueType = "String" },
        @{ id = "2709"; name = "CalibrationDate"; valueType = "String" },
        @{ id = "2710"; name = "Status"; valueType = "String" },
        @{ id = "2711"; name = "Manufacturer"; valueType = "String" }
    )
    tableName = "dimsensor"
    timeseriesTable = "SensorReading"
    timeseriesProperties = @(
        @{ id = "4001"; name = "Timestamp"; valueType = "DateTime" },
        @{ id = "4002"; name = "ReadingValue"; valueType = "Double" },
        @{ id = "4003"; name = "QualityFlag"; valueType = "String" },
        @{ id = "4004"; name = "IsAnomaly"; valueType = "Boolean" }
    )
    timestampColumn = "Timestamp"
}

# --- DimEmployee (ID: 1009) ---
$entityTypes += @{
    id = "1009"; name = "Employee"
    entityIdParts = @("2801")
    displayNamePropertyId = "2802"
    properties = @(
        @{ id = "2801"; name = "EmployeeId"; valueType = "String" },
        @{ id = "2802"; name = "FullName"; valueType = "String" },
        @{ id = "2803"; name = "Role"; valueType = "String" },
        @{ id = "2804"; name = "Department"; valueType = "String" },
        @{ id = "2805"; name = "RefineryId"; valueType = "String" },
        @{ id = "2806"; name = "HireDate"; valueType = "String" },
        @{ id = "2807"; name = "CertificationLevel"; valueType = "String" },
        @{ id = "2808"; name = "ShiftPattern"; valueType = "String" },
        @{ id = "2809"; name = "Status"; valueType = "String" }
    )
    tableName = "dimemployee"
    columnMappings = @{
        "FullName" = "FirstName"
    }
}

# --- FactMaintenance (ID: 1010) ---
$entityTypes += @{
    id = "1010"; name = "MaintenanceEvent"
    entityIdParts = @("2901")
    displayNamePropertyId = "2901"
    properties = @(
        @{ id = "2901"; name = "MaintenanceId"; valueType = "String" },
        @{ id = "2902"; name = "EquipmentId"; valueType = "String" },
        @{ id = "2903"; name = "MaintenanceType"; valueType = "String" },
        @{ id = "2904"; name = "Priority"; valueType = "String" },
        @{ id = "2905"; name = "PerformedByEmployeeId"; valueType = "String" },
        @{ id = "2906"; name = "StartDate"; valueType = "String" },
        @{ id = "2907"; name = "EndDate"; valueType = "String" },
        @{ id = "2908"; name = "DurationHours"; valueType = "Double" },
        @{ id = "2909"; name = "CostUSD"; valueType = "Double" },
        @{ id = "2910"; name = "Description"; valueType = "String" },
        @{ id = "2911"; name = "WorkOrderNumber"; valueType = "String" },
        @{ id = "2912"; name = "Status"; valueType = "String" }
    )
    tableName = "factmaintenance"
}

# --- FactSafetyAlarm (ID: 1011) ---
$entityTypes += @{
    id = "1011"; name = "SafetyAlarm"
    entityIdParts = @("2951")
    displayNamePropertyId = "2951"
    properties = @(
        @{ id = "2951"; name = "AlarmId"; valueType = "String" },
        @{ id = "2952"; name = "SensorId"; valueType = "String" },
        @{ id = "2953"; name = "AlarmType"; valueType = "String" },
        @{ id = "2954"; name = "Severity"; valueType = "String" },
        @{ id = "2955"; name = "AlarmTimestamp"; valueType = "String" },
        @{ id = "2956"; name = "AcknowledgedTimestamp"; valueType = "String" },
        @{ id = "2957"; name = "ClearedTimestamp"; valueType = "String" },
        @{ id = "2958"; name = "AlarmValue"; valueType = "Double" },
        @{ id = "2959"; name = "ThresholdValue"; valueType = "Double" },
        @{ id = "2960"; name = "Description"; valueType = "String" },
        @{ id = "2961"; name = "ActionTaken"; valueType = "String" },
        @{ id = "2962"; name = "AcknowledgedByEmployeeId"; valueType = "String" }
    )
    tableName = "factsafetyalarm"
}

# --- FactProduction (ID: 1012) ---
$entityTypes += @{
    id = "1012"; name = "ProductionRecord"
    entityIdParts = @("2981")
    displayNamePropertyId = "2981"
    properties = @(
        @{ id = "2981"; name = "ProductionId"; valueType = "String" },
        @{ id = "2982"; name = "ProcessUnitId"; valueType = "String" },
        @{ id = "2983"; name = "ProductId"; valueType = "String" },
        @{ id = "2984"; name = "ProductionDate"; valueType = "String" },
        @{ id = "2985"; name = "OutputBarrels"; valueType = "BigInt" },
        @{ id = "2986"; name = "YieldPercent"; valueType = "Double" },
        @{ id = "2987"; name = "QualityGrade"; valueType = "String" },
        @{ id = "2988"; name = "EnergyConsumptionMMBTU"; valueType = "Double" },
        @{ id = "2989"; name = "Notes"; valueType = "String" }
    )
    tableName = "factproduction"
}

# --- BridgeCrudeOilProcessUnit (ID: 1013) ---
$entityTypes += @{
    id = "1013"; name = "CrudeOilFeed"
    entityIdParts = @("2991")
    displayNamePropertyId = "2991"
    properties = @(
        @{ id = "2991"; name = "BridgeId"; valueType = "String" },
        @{ id = "2992"; name = "CrudeOilId"; valueType = "String" },
        @{ id = "2993"; name = "ProcessUnitId"; valueType = "String" },
        @{ id = "2994"; name = "FeedRateBPD"; valueType = "BigInt" },
        @{ id = "2995"; name = "EffectiveDate"; valueType = "String" },
        @{ id = "2996"; name = "Notes"; valueType = "String" }
    )
    tableName = "bridgecrudeoilprocessunit"
}

# ============================================================================
# 2. RELATIONSHIPS
# ============================================================================

$relationships = @(
    @{ id = "3001"; name = "RefineryHasProcessUnit"; sourceId = "1001"; targetId = "1002" },
    @{ id = "3002"; name = "ProcessUnitHasEquipment"; sourceId = "1002"; targetId = "1003" },
    @{ id = "3003"; name = "PipelineFromProcessUnit"; sourceId = "1004"; targetId = "1002" },
    @{ id = "3004"; name = "RefineryHasPipeline"; sourceId = "1001"; targetId = "1004" },
    @{ id = "3005"; name = "RefineryHasStorageTank"; sourceId = "1001"; targetId = "1007" },
    @{ id = "3006"; name = "StorageTankHoldsProduct"; sourceId = "1007"; targetId = "1006" },
    @{ id = "3007"; name = "EquipmentHasSensor"; sourceId = "1003"; targetId = "1008" },
    @{ id = "3008"; name = "MaintenanceOnEquipment"; sourceId = "1010"; targetId = "1003" },
    @{ id = "3009"; name = "MaintenanceByEmployee"; sourceId = "1010"; targetId = "1009" },
    @{ id = "3010"; name = "AlarmFromSensor"; sourceId = "1011"; targetId = "1008" },
    @{ id = "3011"; name = "ProductionFromProcessUnit"; sourceId = "1012"; targetId = "1002" },
    @{ id = "3012"; name = "ProductionOfProduct"; sourceId = "1012"; targetId = "1006" },
    @{ id = "3013"; name = "RefineryHasEmployee"; sourceId = "1001"; targetId = "1009" },
    @{ id = "3014"; name = "CrudeFeedToProcessUnit"; sourceId = "1013"; targetId = "1002" },
    @{ id = "3015"; name = "CrudeFeedFromCrudeOil"; sourceId = "1013"; targetId = "1005" }
)

# ============================================================================
# 3. BUILD PARTS ARRAY
# ============================================================================

$parts = @()

# --- .platform ---
$platform = @"
{"metadata":{"type":"Ontology","displayName":"OilGasRefineryOntology","description":"Oil and Gas Refinery Ontology - entities, relationships, and telemetry for refinery operations"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}
"@
$parts += @{ path = ".platform"; payload = (ToBase64 $platform); payloadType = "InlineBase64" }

# --- definition.json (always empty) ---
$parts += @{ path = "definition.json"; payload = (ToBase64 "{}"); payloadType = "InlineBase64" }

# --- Entity Types ---
foreach ($et in $entityTypes) {
    # Build properties JSON array
    $propsJson = ($et.properties | ForEach-Object {
        '{"id":"' + $_.id + '","name":"' + $_.name + '","redefines":null,"baseTypeNamespaceType":null,"valueType":"' + $_.valueType + '"}'
    }) -join ','

    # Build timeseries properties if present
    $tsPropsJson = "[]"
    if ($et.timeseriesProperties) {
        $tsPropsJson = '[' + (($et.timeseriesProperties | ForEach-Object {
            '{"id":"' + $_.id + '","name":"' + $_.name + '","redefines":null,"baseTypeNamespaceType":null,"valueType":"' + $_.valueType + '"}'
        }) -join ',') + ']'
    }

    # Entity ID parts
    $idPartsJson = '[' + (($et.entityIdParts | ForEach-Object { '"' + $_ + '"' }) -join ',') + ']'

    $entityJson = '{"id":"' + $et.id + '","namespace":"usertypes","baseEntityTypeId":null,"name":"' + $et.name + '","entityIdParts":' + $idPartsJson + ',"displayNamePropertyId":"' + $et.displayNamePropertyId + '","namespaceType":"Custom","visibility":"Visible","properties":[' + $propsJson + '],"timeseriesProperties":' + $tsPropsJson + '}'

    $parts += @{ path = "EntityTypes/$($et.id)/definition.json"; payload = (ToBase64 $entityJson); payloadType = "InlineBase64" }

    # --- NonTimeSeries Data Binding (Lakehouse) ---
    $bindGuid = DeterministicGuid "NonTimeSeries-$($et.id)"
    $propBindings = ($et.properties | ForEach-Object {
        $colName = $_.name
        # Map property name to column name if mapping exists
        if ($et.columnMappings -and $et.columnMappings.ContainsKey($_.name)) {
            $colName = $et.columnMappings[$_.name]
        }
        '{"sourceColumnName":"' + $colName + '","targetPropertyId":"' + $_.id + '"}'
    }) -join ','

    $bindJson = '{"id":"' + $bindGuid + '","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":[' + $propBindings + '],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $et.tableName + '","sourceSchema":"dbo"}}}'

    $parts += @{ path = "EntityTypes/$($et.id)/DataBindings/$bindGuid.json"; payload = (ToBase64 $bindJson); payloadType = "InlineBase64" }

    # --- TimeSeries Data Binding (Eventhouse/KQL) ---
    if ($et.timeseriesTable) {
        $tsBindGuid = DeterministicGuid "TimeSeries-$($et.id)"
        $tsBindings = ($et.timeseriesProperties | ForEach-Object {
            '{"sourceColumnName":"' + $_.name + '","targetPropertyId":"' + $_.id + '"}'
        }) -join ','
        # Add the entity ID column binding (SensorId -> SensorId property)
        $entityIdPropId = $et.entityIdParts[0]
        $entityIdPropName = ($et.properties | Where-Object { $_.id -eq $entityIdPropId }).name
        $tsBindings = '{"sourceColumnName":"' + $entityIdPropName + '","targetPropertyId":"' + $entityIdPropId + '"},' + $tsBindings

        $tsBindJson = '{"id":"' + $tsBindGuid + '","dataBindingConfiguration":{"dataBindingType":"TimeSeries","timestampColumnName":"' + $et.timestampColumn + '","propertyBindings":[' + $tsBindings + '],"sourceTableProperties":{"sourceType":"KustoTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $KqlDatabaseId + '","clusterUri":"' + $KqlClusterUri + '","databaseName":"' + $KqlDatabaseName + '","sourceTableName":"' + $et.timeseriesTable + '"}}}'

        $parts += @{ path = "EntityTypes/$($et.id)/DataBindings/$tsBindGuid.json"; payload = (ToBase64 $tsBindJson); payloadType = "InlineBase64" }
    }
}

# --- Relationship Types ---
foreach ($rel in $relationships) {
    $relJson = '{"namespace":"usertypes","id":"' + $rel.id + '","name":"' + $rel.name + '","namespaceType":"Custom","source":{"entityTypeId":"' + $rel.sourceId + '"},"target":{"entityTypeId":"' + $rel.targetId + '"}}'

    $parts += @{ path = "RelationshipTypes/$($rel.id)/definition.json"; payload = (ToBase64 $relJson); payloadType = "InlineBase64" }

    # --- Contextualization: find the FK column that links source and target entities ---
    $sourceEntity = $entityTypes | Where-Object { $_.id -eq $rel.sourceId }
    $targetEntity = $entityTypes | Where-Object { $_.id -eq $rel.targetId }
    $sourcePkPropId = $sourceEntity.entityIdParts[0]
    $sourcePkName = ($sourceEntity.properties | Where-Object { $_.id -eq $sourcePkPropId }).name
    $targetPkPropId = $targetEntity.entityIdParts[0]
    $targetPkName = ($targetEntity.properties | Where-Object { $_.id -eq $targetPkPropId }).name

    # Strategy 1: FK in source entity table (source has a column matching target PK)
    $fkProp = $sourceEntity.properties | Where-Object { $_.name -eq $targetPkName }
    if (-not $fkProp) {
        # Try common FK patterns like "FromProcessUnitId", "PerformedByEmployeeId", "AcknowledgedByEmployeeId"
        $fkProp = $sourceEntity.properties | Where-Object { $_.name -like "*$targetPkName" }
    }

    if ($fkProp) {
        # FK is in source table - contextualization uses source entity's table
        $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
        $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $sourceEntity.tableName + '","sourceSchema":"dbo","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $sourcePkName + '","targetPropertyId":"' + $sourcePkPropId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $fkProp.name + '","targetPropertyId":"' + $targetPkPropId + '"}]}'
        $parts += @{ path = "RelationshipTypes/$($rel.id)/Contextualizations/$ctxGuid.json"; payload = (ToBase64 $ctxJson); payloadType = "InlineBase64" }
    } else {
        # Strategy 2: FK in target entity table (target has a column matching source PK)
        # This handles "parent has children" relationships (e.g., RefineryHasProcessUnit)
        $fkPropInTarget = $targetEntity.properties | Where-Object { $_.name -eq $sourcePkName }
        if (-not $fkPropInTarget) {
            $fkPropInTarget = $targetEntity.properties | Where-Object { $_.name -like "*$sourcePkName" }
        }
        if ($fkPropInTarget) {
            $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
            $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $targetEntity.tableName + '","sourceSchema":"dbo","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $fkPropInTarget.name + '","targetPropertyId":"' + $sourcePkPropId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $targetPkName + '","targetPropertyId":"' + $targetPkPropId + '"}]}'
            $parts += @{ path = "RelationshipTypes/$($rel.id)/Contextualizations/$ctxGuid.json"; payload = (ToBase64 $ctxJson); payloadType = "InlineBase64" }
        } else {
            Write-Warning "No FK found for relationship $($rel.name) ($($rel.id))"
        }
    }
}

Write-Host "Total parts: $($parts.Count)"
Write-Host "  Entity types: $($entityTypes.Count)"
Write-Host "  Relationships: $($relationships.Count)"

# ============================================================================
# 4. BUILD AND SEND UPDATE DEFINITION
# ============================================================================

$partsJson = ($parts | ForEach-Object {
    '{"path":"' + $_.path + '","payload":"' + $_.payload + '","payloadType":"InlineBase64"}'
}) -join ','

$bodyStr = '{"definition":{"parts":[' + $partsJson + ']}}'
Write-Host "Payload size: $($bodyStr.Length) chars"

try {
    $resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$OntologyId/updateDefinition" -Method POST -Headers $headers -Body $bodyStr -UseBasicParsing
    Write-Host "Update status: $($resp.StatusCode)"
    if ($resp.StatusCode -eq 200) {
        Write-Host "Ontology updated immediately!"
    } elseif ($resp.StatusCode -eq 202) {
        $opUrl = $resp.Headers["Location"]
        Write-Host "LRO: $opUrl"
        Write-Host "Waiting for completion..."
        $maxWait = 120; $waited = 0
        while ($waited -lt $maxWait) {
            Start-Sleep -Seconds 10
            $waited += 10
            $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $FabricToken" }
            if ($poll.status -eq "Succeeded") {
                Write-Host "Result: Succeeded ($waited`s)"
                break
            } elseif ($poll.status -eq "Failed") {
                Write-Host "Result: Failed ($waited`s)"
                if ($poll.error) { Write-Host "Error: $($poll.error.message)" }
                break
            }
            Write-Host "  Status: $($poll.status) ($waited`s)..."
        }
        if ($waited -ge $maxWait -and $poll.status -notin @("Succeeded","Failed")) {
            Write-Host "LRO timed out after $maxWait`s (status: $($poll.status))"
        }
    }
} catch {
    $sr = $_.Exception.Response
    if ($sr) {
        $stream = $sr.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        Write-Host "ERROR $([int]$sr.StatusCode): $($reader.ReadToEnd())"
    } else {
        Write-Host "ERROR: $($_.Exception.Message)"
    }
}
