# Build-Ontology-ManufacturingPlant.ps1
# Builds the Manufacturing Plant Ontology definition for Microsoft Fabric
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

function ToBase64([string]$text) { return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($text)) }
function DeterministicGuid([string]$seed) {
    $hash = [System.Security.Cryptography.MD5]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($seed))
    return ([guid]::new($hash)).ToString()
}

$entityTypes = @()

# --- DimPlant (ID: 1001) ---
$entityTypes += @{
    id = "1001"; name = "Plant"; entityIdParts = @("2001"); displayNamePropertyId = "2002"
    properties = @(
        @{ id = "2001"; name = "PlantId"; valueType = "String" },
        @{ id = "2002"; name = "PlantName"; valueType = "String" },
        @{ id = "2003"; name = "Country"; valueType = "String" },
        @{ id = "2004"; name = "State"; valueType = "String" },
        @{ id = "2005"; name = "City"; valueType = "String" },
        @{ id = "2006"; name = "TotalAreaSqFt"; valueType = "BigInt" },
        @{ id = "2007"; name = "YearBuilt"; valueType = "BigInt" },
        @{ id = "2008"; name = "ProductionCapacity"; valueType = "BigInt" },
        @{ id = "2009"; name = "Status"; valueType = "String" },
        @{ id = "2010"; name = "Manager"; valueType = "String" }
    )
    tableName = "dimplant"
}

# --- DimProductionLine (ID: 1002) ---
$entityTypes += @{
    id = "1002"; name = "ProductionLine"; entityIdParts = @("2101"); displayNamePropertyId = "2102"
    properties = @(
        @{ id = "2101"; name = "LineId"; valueType = "String" },
        @{ id = "2102"; name = "LineName"; valueType = "String" },
        @{ id = "2103"; name = "PlantId"; valueType = "String" },
        @{ id = "2104"; name = "LineType"; valueType = "String" },
        @{ id = "2105"; name = "CapacityUnitsPerHour"; valueType = "BigInt" },
        @{ id = "2106"; name = "InstallDate"; valueType = "String" },
        @{ id = "2107"; name = "Status"; valueType = "String" }
    )
    tableName = "dimproductionline"
}

# --- DimMachine (ID: 1003) ---
$entityTypes += @{
    id = "1003"; name = "Machine"; entityIdParts = @("2201"); displayNamePropertyId = "2202"
    properties = @(
        @{ id = "2201"; name = "MachineId"; valueType = "String" },
        @{ id = "2202"; name = "MachineName"; valueType = "String" },
        @{ id = "2203"; name = "LineId"; valueType = "String" },
        @{ id = "2204"; name = "MachineType"; valueType = "String" },
        @{ id = "2205"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2206"; name = "Model"; valueType = "String" },
        @{ id = "2207"; name = "InstallDate"; valueType = "String" },
        @{ id = "2208"; name = "Status"; valueType = "String" },
        @{ id = "2209"; name = "CriticalityLevel"; valueType = "String" }
    )
    tableName = "dimmachine"
}

# --- DimProduct (ID: 1004) ---
$entityTypes += @{
    id = "1004"; name = "Product"; entityIdParts = @("2301"); displayNamePropertyId = "2302"
    properties = @(
        @{ id = "2301"; name = "ProductId"; valueType = "String" },
        @{ id = "2302"; name = "ProductName"; valueType = "String" },
        @{ id = "2303"; name = "ProductCategory"; valueType = "String" },
        @{ id = "2304"; name = "UnitOfMeasure"; valueType = "String" },
        @{ id = "2305"; name = "WeightKg"; valueType = "Double" },
        @{ id = "2306"; name = "Description"; valueType = "String" }
    )
    tableName = "dimproduct"
}

# --- DimMaterial (ID: 1005) ---
$entityTypes += @{
    id = "1005"; name = "Material"; entityIdParts = @("2401"); displayNamePropertyId = "2402"
    properties = @(
        @{ id = "2401"; name = "MaterialId"; valueType = "String" },
        @{ id = "2402"; name = "MaterialName"; valueType = "String" },
        @{ id = "2403"; name = "MaterialType"; valueType = "String" },
        @{ id = "2404"; name = "Supplier"; valueType = "String" },
        @{ id = "2405"; name = "UnitCost"; valueType = "Double" },
        @{ id = "2406"; name = "UnitOfMeasure"; valueType = "String" },
        @{ id = "2407"; name = "MinStockLevel"; valueType = "BigInt" },
        @{ id = "2408"; name = "CurrentStock"; valueType = "BigInt" }
    )
    tableName = "dimmaterial"
}

# --- DimOperator (ID: 1006) ---
$entityTypes += @{
    id = "1006"; name = "Operator"; entityIdParts = @("2501"); displayNamePropertyId = "2502"
    properties = @(
        @{ id = "2501"; name = "OperatorId"; valueType = "String" },
        @{ id = "2502"; name = "FullName"; valueType = "String" },
        @{ id = "2503"; name = "Role"; valueType = "String" },
        @{ id = "2504"; name = "Shift"; valueType = "String" },
        @{ id = "2505"; name = "LineId"; valueType = "String" },
        @{ id = "2506"; name = "HireDate"; valueType = "String" },
        @{ id = "2507"; name = "CertificationLevel"; valueType = "String" },
        @{ id = "2508"; name = "Status"; valueType = "String" }
    )
    tableName = "dimoperator"
}

# --- DimSensor (ID: 1007) ---
$entityTypes += @{
    id = "1007"; name = "Sensor"; entityIdParts = @("2601"); displayNamePropertyId = "2602"
    properties = @(
        @{ id = "2601"; name = "SensorId"; valueType = "String" },
        @{ id = "2602"; name = "SensorName"; valueType = "String" },
        @{ id = "2603"; name = "SensorType"; valueType = "String" },
        @{ id = "2604"; name = "MachineId"; valueType = "String" },
        @{ id = "2605"; name = "MeasurementUnit"; valueType = "String" },
        @{ id = "2606"; name = "MinRange"; valueType = "Double" },
        @{ id = "2607"; name = "MaxRange"; valueType = "Double" },
        @{ id = "2608"; name = "InstallDate"; valueType = "String" },
        @{ id = "2609"; name = "Status"; valueType = "String" }
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

# --- FactMaintenanceOrder (ID: 1008) ---
$entityTypes += @{
    id = "1008"; name = "MaintenanceOrder"; entityIdParts = @("2701"); displayNamePropertyId = "2701"
    properties = @(
        @{ id = "2701"; name = "OrderId"; valueType = "String" },
        @{ id = "2702"; name = "MachineId"; valueType = "String" },
        @{ id = "2703"; name = "OrderType"; valueType = "String" },
        @{ id = "2704"; name = "Priority"; valueType = "String" },
        @{ id = "2705"; name = "AssignedToOperatorId"; valueType = "String" },
        @{ id = "2706"; name = "StartDate"; valueType = "String" },
        @{ id = "2707"; name = "EndDate"; valueType = "String" },
        @{ id = "2708"; name = "DurationHours"; valueType = "Double" },
        @{ id = "2709"; name = "CostUSD"; valueType = "Double" },
        @{ id = "2710"; name = "Description"; valueType = "String" },
        @{ id = "2711"; name = "Status"; valueType = "String" }
    )
    tableName = "factmaintenanceorder"
}

# --- FactQualityCheck (ID: 1009) ---
$entityTypes += @{
    id = "1009"; name = "QualityCheck"; entityIdParts = @("2801"); displayNamePropertyId = "2801"
    properties = @(
        @{ id = "2801"; name = "QCId"; valueType = "String" },
        @{ id = "2802"; name = "ProductId"; valueType = "String" },
        @{ id = "2803"; name = "LineId"; valueType = "String" },
        @{ id = "2804"; name = "InspectorId"; valueType = "String" },
        @{ id = "2805"; name = "CheckDate"; valueType = "String" },
        @{ id = "2806"; name = "CheckType"; valueType = "String" },
        @{ id = "2807"; name = "Result"; valueType = "String" },
        @{ id = "2808"; name = "DefectCount"; valueType = "BigInt" },
        @{ id = "2809"; name = "Notes"; valueType = "String" }
    )
    tableName = "factqualitycheck"
}

# --- FactProductionBatch (ID: 1010) ---
$entityTypes += @{
    id = "1010"; name = "ProductionBatch"; entityIdParts = @("2901"); displayNamePropertyId = "2901"
    properties = @(
        @{ id = "2901"; name = "BatchId"; valueType = "String" },
        @{ id = "2902"; name = "LineId"; valueType = "String" },
        @{ id = "2903"; name = "ProductId"; valueType = "String" },
        @{ id = "2904"; name = "StartTime"; valueType = "String" },
        @{ id = "2905"; name = "EndTime"; valueType = "String" },
        @{ id = "2906"; name = "QuantityProduced"; valueType = "BigInt" },
        @{ id = "2907"; name = "DefectRate"; valueType = "Double" },
        @{ id = "2908"; name = "EnergyUsedKWh"; valueType = "Double" },
        @{ id = "2909"; name = "Status"; valueType = "String" }
    )
    tableName = "factproductionbatch"
}

# --- FactAlert (ID: 1011) ---
$entityTypes += @{
    id = "1011"; name = "Alert"; entityIdParts = @("2951"); displayNamePropertyId = "2951"
    properties = @(
        @{ id = "2951"; name = "AlertId"; valueType = "String" },
        @{ id = "2952"; name = "SensorId"; valueType = "String" },
        @{ id = "2953"; name = "AlertType"; valueType = "String" },
        @{ id = "2954"; name = "Severity"; valueType = "String" },
        @{ id = "2955"; name = "Timestamp"; valueType = "String" },
        @{ id = "2956"; name = "Value"; valueType = "Double" },
        @{ id = "2957"; name = "Threshold"; valueType = "Double" },
        @{ id = "2958"; name = "Description"; valueType = "String" },
        @{ id = "2959"; name = "Status"; valueType = "String" }
    )
    tableName = "factalert"
}

# ============================================================================
# 2. RELATIONSHIPS
# ============================================================================
$relationships = @(
    @{ id = "3001"; name = "PlantHasProductionLine"; sourceId = "1001"; targetId = "1002" },
    @{ id = "3002"; name = "LineHasMachine"; sourceId = "1002"; targetId = "1003" },
    @{ id = "3003"; name = "MachineHasSensor"; sourceId = "1003"; targetId = "1007" },
    @{ id = "3004"; name = "AlertFromSensor"; sourceId = "1011"; targetId = "1007" },
    @{ id = "3005"; name = "MaintenanceOnMachine"; sourceId = "1008"; targetId = "1003" },
    @{ id = "3006"; name = "MaintenanceByOperator"; sourceId = "1008"; targetId = "1006" },
    @{ id = "3007"; name = "QualityCheckForProduct"; sourceId = "1009"; targetId = "1004" },
    @{ id = "3008"; name = "QualityCheckOnLine"; sourceId = "1009"; targetId = "1002" },
    @{ id = "3009"; name = "BatchOnLine"; sourceId = "1010"; targetId = "1002" },
    @{ id = "3010"; name = "BatchProducesProduct"; sourceId = "1010"; targetId = "1004" },
    @{ id = "3011"; name = "PlantHasOperator"; sourceId = "1002"; targetId = "1006" }
)

# ============================================================================
# 3. BUILD PARTS ARRAY (same pattern as OilGas)
# ============================================================================
$parts = @()

$platform = @"
{"metadata":{"type":"Ontology","displayName":"ManufacturingPlantOntology","description":"Manufacturing Plant Ontology - plants, production lines, machines, quality, materials, and maintenance"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}
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

    $bindGuid = DeterministicGuid "NonTimeSeries-$($et.id)"
    $propBindings = ($et.properties | ForEach-Object {
        '{"sourceColumnName":"' + $_.name + '","targetPropertyId":"' + $_.id + '"}'
    }) -join ','
    $bindJson = '{"id":"' + $bindGuid + '","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":[' + $propBindings + '],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $et.tableName + '","sourceSchema":"dbo"}}}'
    $parts += @{ path = "EntityTypes/$($et.id)/DataBindings/$bindGuid.json"; payload = (ToBase64 $bindJson); payloadType = "InlineBase64" }

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
    if (-not $fkProp) { $fkProp = $sourceEntity.properties | Where-Object { $_.name -like "*$targetPkName" } }

    if ($fkProp) {
        $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
        $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $sourceEntity.tableName + '","sourceSchema":"dbo","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $sourcePkName + '","targetPropertyId":"' + $sourcePkPropId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $fkProp.name + '","targetPropertyId":"' + $targetPkPropId + '"}]}'
        $parts += @{ path = "RelationshipTypes/$($rel.id)/Contextualizations/$ctxGuid.json"; payload = (ToBase64 $ctxJson); payloadType = "InlineBase64" }
    } else {
        $fkPropInTarget = $targetEntity.properties | Where-Object { $_.name -eq $sourcePkName }
        if (-not $fkPropInTarget) { $fkPropInTarget = $targetEntity.properties | Where-Object { $_.name -like "*$sourcePkName" } }
        if ($fkPropInTarget) {
            $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
            $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $targetEntity.tableName + '","sourceSchema":"dbo","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $fkPropInTarget.name + '","targetPropertyId":"' + $sourcePkPropId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $targetPkName + '","targetPropertyId":"' + $targetPkPropId + '"}]}'
            $parts += @{ path = "RelationshipTypes/$($rel.id)/Contextualizations/$ctxGuid.json"; payload = (ToBase64 $ctxJson); payloadType = "InlineBase64" }
        } else { Write-Warning "No FK found for relationship $($rel.name) ($($rel.id))" }
    }
}

Write-Host "Total parts: $($parts.Count) | Entity types: $($entityTypes.Count) | Relationships: $($relationships.Count)"

$partsJson = ($parts | ForEach-Object {
    '{"path":"' + $_.path + '","payload":"' + $_.payload + '","payloadType":"InlineBase64"}'
}) -join ','
$bodyStr = '{"definition":{"parts":[' + $partsJson + ']}}'
Write-Host "Payload size: $($bodyStr.Length) chars"

try {
    $resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$OntologyId/updateDefinition" -Method POST -Headers $headers -Body $bodyStr -UseBasicParsing
    Write-Host "Update status: $($resp.StatusCode)"
    if ($resp.StatusCode -eq 200) { Write-Host "Ontology updated immediately!" }
    elseif ($resp.StatusCode -eq 202) {
        $opUrl = $resp.Headers["Location"]
        $maxWait = 120; $waited = 0
        while ($waited -lt $maxWait) {
            Start-Sleep -Seconds 10; $waited += 10
            $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $FabricToken" }
            if ($poll.status -eq "Succeeded") { Write-Host "Result: Succeeded ($waited`s)"; break }
            elseif ($poll.status -eq "Failed") { Write-Host "Result: Failed ($waited`s)"; break }
            Write-Host "  Status: $($poll.status) ($waited`s)..."
        }
    }
} catch {
    $sr = $_.Exception.Response
    if ($sr) {
        $stream = $sr.GetResponseStream(); $reader = New-Object System.IO.StreamReader($stream)
        Write-Host "ERROR $([int]$sr.StatusCode): $($reader.ReadToEnd())"
    } else { Write-Host "ERROR: $($_.Exception.Message)" }
}
