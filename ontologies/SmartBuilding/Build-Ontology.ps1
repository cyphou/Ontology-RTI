# Build-Ontology-SmartBuilding.ps1
# Builds the Smart Building Ontology definition for Microsoft Fabric
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

function ToBase64([string]$text) {
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($text))
}

function DeterministicGuid([string]$seed) {
    $hash = [System.Security.Cryptography.MD5]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($seed))
    return ([guid]::new($hash)).ToString()
}

# ============================================================================
# 1. ENTITY TYPES + PROPERTIES
# ============================================================================

$entityTypes = @()

# --- DimBuilding (ID: 1001) ---
$entityTypes += @{
    id = "1001"; name = "Building"
    entityIdParts = @("2001")
    displayNamePropertyId = "2002"
    properties = @(
        @{ id = "2001"; name = "BuildingId"; valueType = "String" },
        @{ id = "2002"; name = "BuildingName"; valueType = "String" },
        @{ id = "2003"; name = "Address"; valueType = "String" },
        @{ id = "2004"; name = "City"; valueType = "String" },
        @{ id = "2005"; name = "Country"; valueType = "String" },
        @{ id = "2006"; name = "Floors"; valueType = "BigInt" },
        @{ id = "2007"; name = "TotalAreaSqFt"; valueType = "BigInt" },
        @{ id = "2008"; name = "YearBuilt"; valueType = "BigInt" },
        @{ id = "2009"; name = "BuildingType"; valueType = "String" },
        @{ id = "2010"; name = "Status"; valueType = "String" },
        @{ id = "2011"; name = "Owner"; valueType = "String" }
    )
    tableName = "dimbuilding"
}

# --- DimFloor (ID: 1002) ---
$entityTypes += @{
    id = "1002"; name = "Floor"
    entityIdParts = @("2101")
    displayNamePropertyId = "2102"
    properties = @(
        @{ id = "2101"; name = "FloorId"; valueType = "String" },
        @{ id = "2102"; name = "FloorName"; valueType = "String" },
        @{ id = "2103"; name = "BuildingId"; valueType = "String" },
        @{ id = "2104"; name = "FloorNumber"; valueType = "BigInt" },
        @{ id = "2105"; name = "AreaSqFt"; valueType = "BigInt" },
        @{ id = "2106"; name = "ZoneCount"; valueType = "BigInt" },
        @{ id = "2107"; name = "Status"; valueType = "String" }
    )
    tableName = "dimfloor"
}

# --- DimZone (ID: 1003) ---
$entityTypes += @{
    id = "1003"; name = "Zone"
    entityIdParts = @("2201")
    displayNamePropertyId = "2202"
    properties = @(
        @{ id = "2201"; name = "ZoneId"; valueType = "String" },
        @{ id = "2202"; name = "ZoneName"; valueType = "String" },
        @{ id = "2203"; name = "FloorId"; valueType = "String" },
        @{ id = "2204"; name = "ZoneType"; valueType = "String" },
        @{ id = "2205"; name = "AreaSqFt"; valueType = "BigInt" },
        @{ id = "2206"; name = "MaxOccupancy"; valueType = "BigInt" },
        @{ id = "2207"; name = "Status"; valueType = "String" }
    )
    tableName = "dimzone"
}

# --- DimHVACSystem (ID: 1004) ---
$entityTypes += @{
    id = "1004"; name = "HVACSystem"
    entityIdParts = @("2301")
    displayNamePropertyId = "2302"
    properties = @(
        @{ id = "2301"; name = "HVACId"; valueType = "String" },
        @{ id = "2302"; name = "HVACName"; valueType = "String" },
        @{ id = "2303"; name = "ZoneId"; valueType = "String" },
        @{ id = "2304"; name = "HVACType"; valueType = "String" },
        @{ id = "2305"; name = "CapacityBTU"; valueType = "BigInt" },
        @{ id = "2306"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2307"; name = "Model"; valueType = "String" },
        @{ id = "2308"; name = "InstallDate"; valueType = "String" },
        @{ id = "2309"; name = "Status"; valueType = "String" },
        @{ id = "2310"; name = "EnergyRating"; valueType = "String" }
    )
    tableName = "dimhvacsystem"
}

# --- DimLightingSystem (ID: 1005) ---
$entityTypes += @{
    id = "1005"; name = "LightingSystem"
    entityIdParts = @("2401")
    displayNamePropertyId = "2402"
    properties = @(
        @{ id = "2401"; name = "LightingId"; valueType = "String" },
        @{ id = "2402"; name = "LightingName"; valueType = "String" },
        @{ id = "2403"; name = "ZoneId"; valueType = "String" },
        @{ id = "2404"; name = "LightingType"; valueType = "String" },
        @{ id = "2405"; name = "WattageTotalW"; valueType = "BigInt" },
        @{ id = "2406"; name = "BulbCount"; valueType = "BigInt" },
        @{ id = "2407"; name = "InstallDate"; valueType = "String" },
        @{ id = "2408"; name = "Status"; valueType = "String" }
    )
    tableName = "dimlightingsystem"
}

# --- DimElevator (ID: 1006) ---
$entityTypes += @{
    id = "1006"; name = "Elevator"
    entityIdParts = @("2501")
    displayNamePropertyId = "2502"
    properties = @(
        @{ id = "2501"; name = "ElevatorId"; valueType = "String" },
        @{ id = "2502"; name = "ElevatorName"; valueType = "String" },
        @{ id = "2503"; name = "BuildingId"; valueType = "String" },
        @{ id = "2504"; name = "ElevatorType"; valueType = "String" },
        @{ id = "2505"; name = "CapacityLbs"; valueType = "BigInt" },
        @{ id = "2506"; name = "MaxFloors"; valueType = "BigInt" },
        @{ id = "2507"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2508"; name = "InstallDate"; valueType = "String" },
        @{ id = "2509"; name = "Status"; valueType = "String" }
    )
    tableName = "dimelevator"
}

# --- DimSensor (ID: 1007) ---
$entityTypes += @{
    id = "1007"; name = "Sensor"
    entityIdParts = @("2601")
    displayNamePropertyId = "2602"
    properties = @(
        @{ id = "2601"; name = "SensorId"; valueType = "String" },
        @{ id = "2602"; name = "SensorName"; valueType = "String" },
        @{ id = "2603"; name = "SensorType"; valueType = "String" },
        @{ id = "2604"; name = "ZoneId"; valueType = "String" },
        @{ id = "2605"; name = "MeasurementUnit"; valueType = "String" },
        @{ id = "2606"; name = "MinRange"; valueType = "Double" },
        @{ id = "2607"; name = "MaxRange"; valueType = "Double" },
        @{ id = "2608"; name = "InstallDate"; valueType = "String" },
        @{ id = "2609"; name = "Status"; valueType = "String" },
        @{ id = "2610"; name = "Manufacturer"; valueType = "String" }
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

# --- DimEnergyMeter (ID: 1008) ---
$entityTypes += @{
    id = "1008"; name = "EnergyMeter"
    entityIdParts = @("2701")
    displayNamePropertyId = "2702"
    properties = @(
        @{ id = "2701"; name = "MeterId"; valueType = "String" },
        @{ id = "2702"; name = "MeterName"; valueType = "String" },
        @{ id = "2703"; name = "BuildingId"; valueType = "String" },
        @{ id = "2704"; name = "MeterType"; valueType = "String" },
        @{ id = "2705"; name = "MeasurementUnit"; valueType = "String" },
        @{ id = "2706"; name = "InstallDate"; valueType = "String" },
        @{ id = "2707"; name = "Status"; valueType = "String" }
    )
    tableName = "dimenergymeter"
}

# --- DimOccupant (ID: 1009) ---
$entityTypes += @{
    id = "1009"; name = "Occupant"
    entityIdParts = @("2801")
    displayNamePropertyId = "2802"
    properties = @(
        @{ id = "2801"; name = "OccupantId"; valueType = "String" },
        @{ id = "2802"; name = "FullName"; valueType = "String" },
        @{ id = "2803"; name = "Department"; valueType = "String" },
        @{ id = "2804"; name = "ZoneId"; valueType = "String" },
        @{ id = "2805"; name = "AccessLevel"; valueType = "String" },
        @{ id = "2806"; name = "BadgeId"; valueType = "String" },
        @{ id = "2807"; name = "Status"; valueType = "String" }
    )
    tableName = "dimoccupant"
}

# --- DimAccessPoint (ID: 1010) ---
$entityTypes += @{
    id = "1010"; name = "AccessPoint"
    entityIdParts = @("2851")
    displayNamePropertyId = "2852"
    properties = @(
        @{ id = "2851"; name = "AccessPointId"; valueType = "String" },
        @{ id = "2852"; name = "AccessPointName"; valueType = "String" },
        @{ id = "2853"; name = "ZoneId"; valueType = "String" },
        @{ id = "2854"; name = "AccessPointType"; valueType = "String" },
        @{ id = "2855"; name = "Protocol"; valueType = "String" },
        @{ id = "2856"; name = "CoverageAreaSqFt"; valueType = "BigInt" },
        @{ id = "2857"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2858"; name = "Status"; valueType = "String" }
    )
    tableName = "dimaccesspoint"
}

# --- FactMaintenanceTicket (ID: 1011) ---
$entityTypes += @{
    id = "1011"; name = "MaintenanceTicket"
    entityIdParts = @("2901")
    displayNamePropertyId = "2901"
    properties = @(
        @{ id = "2901"; name = "TicketId"; valueType = "String" },
        @{ id = "2902"; name = "EquipmentType"; valueType = "String" },
        @{ id = "2903"; name = "EquipmentId"; valueType = "String" },
        @{ id = "2904"; name = "TicketType"; valueType = "String" },
        @{ id = "2905"; name = "Priority"; valueType = "String" },
        @{ id = "2906"; name = "AssignedToOccupantId"; valueType = "String" },
        @{ id = "2907"; name = "CreatedDate"; valueType = "String" },
        @{ id = "2908"; name = "ResolvedDate"; valueType = "String" },
        @{ id = "2909"; name = "DurationHours"; valueType = "Double" },
        @{ id = "2910"; name = "CostUSD"; valueType = "Double" },
        @{ id = "2911"; name = "Description"; valueType = "String" },
        @{ id = "2912"; name = "Status"; valueType = "String" }
    )
    tableName = "factmaintenanceticket"
}

# --- FactAlert (ID: 1012) ---
$entityTypes += @{
    id = "1012"; name = "Alert"
    entityIdParts = @("2951")
    displayNamePropertyId = "2951"
    properties = @(
        @{ id = "2951"; name = "AlertId"; valueType = "String" },
        @{ id = "2952"; name = "SensorId"; valueType = "String" },
        @{ id = "2953"; name = "AlertType"; valueType = "String" },
        @{ id = "2954"; name = "Severity"; valueType = "String" },
        @{ id = "2955"; name = "AlertTimestamp"; valueType = "String" },
        @{ id = "2956"; name = "AcknowledgedTimestamp"; valueType = "String" },
        @{ id = "2957"; name = "AlertValue"; valueType = "Double" },
        @{ id = "2958"; name = "ThresholdValue"; valueType = "Double" },
        @{ id = "2959"; name = "Description"; valueType = "String" },
        @{ id = "2960"; name = "Status"; valueType = "String" }
    )
    tableName = "factalert"
}

# ============================================================================
# 2. RELATIONSHIPS
# ============================================================================

$relationships = @(
    @{ id = "3001"; name = "BuildingHasFloor"; sourceId = "1001"; targetId = "1002" },
    @{ id = "3002"; name = "FloorHasZone"; sourceId = "1002"; targetId = "1003" },
    @{ id = "3003"; name = "ZoneHasHVAC"; sourceId = "1003"; targetId = "1004" },
    @{ id = "3004"; name = "ZoneHasLighting"; sourceId = "1003"; targetId = "1005" },
    @{ id = "3005"; name = "ZoneHasSensor"; sourceId = "1003"; targetId = "1007" },
    @{ id = "3006"; name = "ZoneHasAccessPoint"; sourceId = "1003"; targetId = "1010" },
    @{ id = "3007"; name = "BuildingHasElevator"; sourceId = "1001"; targetId = "1006" },
    @{ id = "3008"; name = "BuildingHasEnergyMeter"; sourceId = "1001"; targetId = "1008" },
    @{ id = "3009"; name = "AlertFromSensor"; sourceId = "1012"; targetId = "1007" },
    @{ id = "3010"; name = "TicketAssignedTo"; sourceId = "1011"; targetId = "1009" },
    @{ id = "3011"; name = "BuildingHasOccupant"; sourceId = "1003"; targetId = "1009" }
)

# ============================================================================
# 3. BUILD PARTS ARRAY
# ============================================================================

$parts = @()

$platform = @"
{"metadata":{"type":"Ontology","displayName":"SmartBuildingOntology","description":"Smart Building Ontology - buildings, floors, zones, HVAC, lighting, sensors, occupancy, and energy management"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}
"@
$parts += @{ path = ".platform"; payload = (ToBase64 $platform); payloadType = "InlineBase64" }
$parts += @{ path = "definition.json"; payload = (ToBase64 "{}"); payloadType = "InlineBase64" }

foreach ($et in $entityTypes) {
    $propsJson = ($et.properties | ForEach-Object {
        '{"id":"' + $_.id + '","name":"' + $_.name + '","redefines":null,"baseTypeNamespaceType":null,"valueType":"' + $_.valueType + '"}'
    }) -join ','

    $tsPropsJson = "[]"
    if ($et.timeseriesProperties) {
        $tsPropsJson = '[' + (($et.timeseriesProperties | ForEach-Object {
            '{"id":"' + $_.id + '","name":"' + $_.name + '","redefines":null,"baseTypeNamespaceType":null,"valueType":"' + $_.valueType + '"}'
        }) -join ',') + ']'
    }

    $idPartsJson = '[' + (($et.entityIdParts | ForEach-Object { '"' + $_ + '"' }) -join ',') + ']'

    $entityJson = '{"id":"' + $et.id + '","namespace":"usertypes","baseEntityTypeId":null,"name":"' + $et.name + '","entityIdParts":' + $idPartsJson + ',"displayNamePropertyId":"' + $et.displayNamePropertyId + '","namespaceType":"Custom","visibility":"Visible","properties":[' + $propsJson + '],"timeseriesProperties":' + $tsPropsJson + '}'

    $parts += @{ path = "EntityTypes/$($et.id)/definition.json"; payload = (ToBase64 $entityJson); payloadType = "InlineBase64" }

    # NonTimeSeries Data Binding (Lakehouse)
    $bindGuid = DeterministicGuid "NonTimeSeries-$($et.id)"
    $propBindings = ($et.properties | ForEach-Object {
        $colName = $_.name
        if ($et.columnMappings -and $et.columnMappings.ContainsKey($_.name)) {
            $colName = $et.columnMappings[$_.name]
        }
        '{"sourceColumnName":"' + $colName + '","targetPropertyId":"' + $_.id + '"}'
    }) -join ','

    $bindJson = '{"id":"' + $bindGuid + '","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":[' + $propBindings + '],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $et.tableName + '","sourceSchema":"dbo"}}}'

    $parts += @{ path = "EntityTypes/$($et.id)/DataBindings/$bindGuid.json"; payload = (ToBase64 $bindJson); payloadType = "InlineBase64" }

    # TimeSeries Data Binding (Eventhouse/KQL)
    if ($et.timeseriesTable) {
        $tsBindGuid = DeterministicGuid "TimeSeries-$($et.id)"
        $tsBindings = ($et.timeseriesProperties | ForEach-Object {
            '{"sourceColumnName":"' + $_.name + '","targetPropertyId":"' + $_.id + '"}'
        }) -join ','
        $entityIdPropId = $et.entityIdParts[0]
        $entityIdPropName = ($et.properties | Where-Object { $_.id -eq $entityIdPropId }).name
        $tsBindings = '{"sourceColumnName":"' + $entityIdPropName + '","targetPropertyId":"' + $entityIdPropId + '"},' + $tsBindings

        $tsBindJson = '{"id":"' + $tsBindGuid + '","dataBindingConfiguration":{"dataBindingType":"TimeSeries","timestampColumnName":"' + $et.timestampColumn + '","propertyBindings":[' + $tsBindings + '],"sourceTableProperties":{"sourceType":"KustoTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $KqlDatabaseId + '","clusterUri":"' + $KqlClusterUri + '","databaseName":"' + $KqlDatabaseName + '","sourceTableName":"' + $et.timeseriesTable + '"}}}'

        $parts += @{ path = "EntityTypes/$($et.id)/DataBindings/$tsBindGuid.json"; payload = (ToBase64 $tsBindJson); payloadType = "InlineBase64" }
    }
}

foreach ($rel in $relationships) {
    $relJson = '{"namespace":"usertypes","id":"' + $rel.id + '","name":"' + $rel.name + '","namespaceType":"Custom","source":{"entityTypeId":"' + $rel.sourceId + '"},"target":{"entityTypeId":"' + $rel.targetId + '"}}'

    $parts += @{ path = "RelationshipTypes/$($rel.id)/definition.json"; payload = (ToBase64 $relJson); payloadType = "InlineBase64" }

    $sourceEntity = $entityTypes | Where-Object { $_.id -eq $rel.sourceId }
    $targetEntity = $entityTypes | Where-Object { $_.id -eq $rel.targetId }
    $sourcePkPropId = $sourceEntity.entityIdParts[0]
    $sourcePkName = ($sourceEntity.properties | Where-Object { $_.id -eq $sourcePkPropId }).name
    $targetPkPropId = $targetEntity.entityIdParts[0]
    $targetPkName = ($targetEntity.properties | Where-Object { $_.id -eq $targetPkPropId }).name

    $fkProp = $sourceEntity.properties | Where-Object { $_.name -eq $targetPkName }
    if (-not $fkProp) {
        $fkProp = $sourceEntity.properties | Where-Object { $_.name -like "*$targetPkName" }
    }

    if ($fkProp) {
        $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
        $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $sourceEntity.tableName + '","sourceSchema":"dbo","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $sourcePkName + '","targetPropertyId":"' + $sourcePkPropId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $fkProp.name + '","targetPropertyId":"' + $targetPkPropId + '"}]}'
        $parts += @{ path = "RelationshipTypes/$($rel.id)/Contextualizations/$ctxGuid.json"; payload = (ToBase64 $ctxJson); payloadType = "InlineBase64" }
    } else {
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
        $maxWait = 120; $waited = 0
        while ($waited -lt $maxWait) {
            Start-Sleep -Seconds 10; $waited += 10
            $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $FabricToken" }
            if ($poll.status -eq "Succeeded") { Write-Host "Result: Succeeded ($waited`s)"; break }
            elseif ($poll.status -eq "Failed") { Write-Host "Result: Failed ($waited`s)"; if ($poll.error) { Write-Host "Error: $($poll.error.message)" }; break }
            Write-Host "  Status: $($poll.status) ($waited`s)..."
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
