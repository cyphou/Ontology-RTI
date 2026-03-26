# Build-Ontology-WindTurbine.ps1
# Builds the Wind Turbine / Wind Farm Ontology definition for Microsoft Fabric
param(
    [string]$WorkspaceId, [string]$LakehouseId, [string]$KqlDatabaseId,
    [string]$KqlClusterUri, [string]$KqlDatabaseName, [string]$OntologyId, [string]$FabricToken
)

$headers = @{ Authorization = "Bearer $FabricToken"; "Content-Type" = "application/json" }
function ToBase64([string]$text) { return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($text)) }
function DeterministicGuid([string]$seed) {
    $hash = [System.Security.Cryptography.MD5]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($seed))
    return ([guid]::new($hash)).ToString()
}

$entityTypes = @()

$entityTypes += @{
    id = "1001"; name = "WindFarm"; entityIdParts = @("2001"); displayNamePropertyId = "2002"
    properties = @(
        @{ id = "2001"; name = "WindFarmId"; valueType = "String" },
        @{ id = "2002"; name = "WindFarmName"; valueType = "String" },
        @{ id = "2003"; name = "Location"; valueType = "String" },
        @{ id = "2004"; name = "Latitude"; valueType = "Double" },
        @{ id = "2005"; name = "Longitude"; valueType = "Double" },
        @{ id = "2006"; name = "TotalTurbines"; valueType = "BigInt" },
        @{ id = "2007"; name = "InstalledCapacityMW"; valueType = "BigInt" },
        @{ id = "2008"; name = "CommissionDate"; valueType = "String" },
        @{ id = "2009"; name = "Operator"; valueType = "String" },
        @{ id = "2010"; name = "Status"; valueType = "String" }
    )
    tableName = "dimwindfarm"
}

$entityTypes += @{
    id = "1002"; name = "Turbine"; entityIdParts = @("2101"); displayNamePropertyId = "2102"
    properties = @(
        @{ id = "2101"; name = "TurbineId"; valueType = "String" },
        @{ id = "2102"; name = "TurbineName"; valueType = "String" },
        @{ id = "2103"; name = "WindFarmId"; valueType = "String" },
        @{ id = "2104"; name = "Model"; valueType = "String" },
        @{ id = "2105"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2106"; name = "RatedCapacityKW"; valueType = "BigInt" },
        @{ id = "2107"; name = "HubHeightM"; valueType = "BigInt" },
        @{ id = "2108"; name = "RotorDiameterM"; valueType = "BigInt" },
        @{ id = "2109"; name = "CommissionDate"; valueType = "String" },
        @{ id = "2110"; name = "Status"; valueType = "String" }
    )
    tableName = "dimturbine"
    timeseriesTable = "TurbineTelemetry"
    timeseriesProperties = @(
        @{ id = "4001"; name = "Timestamp"; valueType = "DateTime" },
        @{ id = "4002"; name = "WindSpeedMs"; valueType = "Double" },
        @{ id = "4003"; name = "PowerOutputKW"; valueType = "Double" },
        @{ id = "4004"; name = "RotorRPM"; valueType = "Double" },
        @{ id = "4005"; name = "PitchAngleDeg"; valueType = "Double" },
        @{ id = "4006"; name = "VibrationMmS"; valueType = "Double" },
        @{ id = "4007"; name = "GeneratorTempC"; valueType = "Double" }
    )
    timestampColumn = "Timestamp"
}

$entityTypes += @{
    id = "1003"; name = "Nacelle"; entityIdParts = @("2201"); displayNamePropertyId = "2202"
    properties = @(
        @{ id = "2201"; name = "NacelleId"; valueType = "String" },
        @{ id = "2202"; name = "NacelleName"; valueType = "String" },
        @{ id = "2203"; name = "TurbineId"; valueType = "String" },
        @{ id = "2204"; name = "GeneratorType"; valueType = "String" },
        @{ id = "2205"; name = "GearboxType"; valueType = "String" },
        @{ id = "2206"; name = "CoolingSystem"; valueType = "String" },
        @{ id = "2207"; name = "WeightTons"; valueType = "BigInt" },
        @{ id = "2208"; name = "LastInspectionDate"; valueType = "String" },
        @{ id = "2209"; name = "Status"; valueType = "String" }
    )
    tableName = "dimnacelle"
}

$entityTypes += @{
    id = "1004"; name = "Blade"; entityIdParts = @("2301"); displayNamePropertyId = "2302"
    properties = @(
        @{ id = "2301"; name = "BladeId"; valueType = "String" },
        @{ id = "2302"; name = "BladeName"; valueType = "String" },
        @{ id = "2303"; name = "TurbineId"; valueType = "String" },
        @{ id = "2304"; name = "BladePosition"; valueType = "BigInt" },
        @{ id = "2305"; name = "LengthM"; valueType = "BigInt" },
        @{ id = "2306"; name = "Material"; valueType = "String" },
        @{ id = "2307"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2308"; name = "InstallDate"; valueType = "String" },
        @{ id = "2309"; name = "LastInspectionDate"; valueType = "String" },
        @{ id = "2310"; name = "Status"; valueType = "String" }
    )
    tableName = "dimblade"
}

$entityTypes += @{
    id = "1005"; name = "Tower"; entityIdParts = @("2401"); displayNamePropertyId = "2402"
    properties = @(
        @{ id = "2401"; name = "TowerId"; valueType = "String" },
        @{ id = "2402"; name = "TowerName"; valueType = "String" },
        @{ id = "2403"; name = "TurbineId"; valueType = "String" },
        @{ id = "2404"; name = "HeightM"; valueType = "BigInt" },
        @{ id = "2405"; name = "Material"; valueType = "String" },
        @{ id = "2406"; name = "Sections"; valueType = "BigInt" },
        @{ id = "2407"; name = "FoundationType"; valueType = "String" },
        @{ id = "2408"; name = "InstallDate"; valueType = "String" },
        @{ id = "2409"; name = "Status"; valueType = "String" }
    )
    tableName = "dimtower"
}

$entityTypes += @{
    id = "1006"; name = "Sensor"; entityIdParts = @("2501"); displayNamePropertyId = "2502"
    properties = @(
        @{ id = "2501"; name = "SensorId"; valueType = "String" },
        @{ id = "2502"; name = "SensorName"; valueType = "String" },
        @{ id = "2503"; name = "TurbineId"; valueType = "String" },
        @{ id = "2504"; name = "SensorType"; valueType = "String" },
        @{ id = "2505"; name = "Location"; valueType = "String" },
        @{ id = "2506"; name = "Unit"; valueType = "String" },
        @{ id = "2507"; name = "MinThreshold"; valueType = "Double" },
        @{ id = "2508"; name = "MaxThreshold"; valueType = "Double" },
        @{ id = "2509"; name = "InstallDate"; valueType = "String" },
        @{ id = "2510"; name = "Status"; valueType = "String" }
    )
    tableName = "dimsensor"
}

$entityTypes += @{
    id = "1007"; name = "Technician"; entityIdParts = @("2601"); displayNamePropertyId = "2602"
    properties = @(
        @{ id = "2601"; name = "TechnicianId"; valueType = "String" },
        @{ id = "2602"; name = "TechnicianName"; valueType = "String" },
        @{ id = "2603"; name = "Specialization"; valueType = "String" },
        @{ id = "2604"; name = "CertificationLevel"; valueType = "String" },
        @{ id = "2605"; name = "WindFarmId"; valueType = "String" },
        @{ id = "2606"; name = "Shift"; valueType = "String" },
        @{ id = "2607"; name = "YearsExperience"; valueType = "BigInt" },
        @{ id = "2608"; name = "Status"; valueType = "String" }
    )
    tableName = "dimtechnician"
}

$entityTypes += @{
    id = "1008"; name = "WeatherStation"; entityIdParts = @("2701"); displayNamePropertyId = "2702"
    properties = @(
        @{ id = "2701"; name = "StationId"; valueType = "String" },
        @{ id = "2702"; name = "StationName"; valueType = "String" },
        @{ id = "2703"; name = "WindFarmId"; valueType = "String" },
        @{ id = "2704"; name = "Latitude"; valueType = "Double" },
        @{ id = "2705"; name = "Longitude"; valueType = "Double" },
        @{ id = "2706"; name = "ElevationM"; valueType = "BigInt" },
        @{ id = "2707"; name = "InstallDate"; valueType = "String" },
        @{ id = "2708"; name = "Status"; valueType = "String" }
    )
    tableName = "dimweatherstation"
}

$entityTypes += @{
    id = "1009"; name = "Transformer"; entityIdParts = @("2801"); displayNamePropertyId = "2802"
    properties = @(
        @{ id = "2801"; name = "TransformerId"; valueType = "String" },
        @{ id = "2802"; name = "TransformerName"; valueType = "String" },
        @{ id = "2803"; name = "WindFarmId"; valueType = "String" },
        @{ id = "2804"; name = "RatingMVA"; valueType = "BigInt" },
        @{ id = "2805"; name = "VoltageKV"; valueType = "BigInt" },
        @{ id = "2806"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2807"; name = "InstallDate"; valueType = "String" },
        @{ id = "2808"; name = "LastServiceDate"; valueType = "String" },
        @{ id = "2809"; name = "Status"; valueType = "String" }
    )
    tableName = "dimtransformer"
}

$entityTypes += @{
    id = "1010"; name = "MaintenanceEvent"; entityIdParts = @("2901"); displayNamePropertyId = "2901"
    properties = @(
        @{ id = "2901"; name = "EventId"; valueType = "String" },
        @{ id = "2902"; name = "TurbineId"; valueType = "String" },
        @{ id = "2903"; name = "TechnicianId"; valueType = "String" },
        @{ id = "2904"; name = "EventType"; valueType = "String" },
        @{ id = "2905"; name = "Priority"; valueType = "String" },
        @{ id = "2906"; name = "ScheduledDate"; valueType = "String" },
        @{ id = "2907"; name = "CompletedDate"; valueType = "String" },
        @{ id = "2908"; name = "DurationHours"; valueType = "Double" },
        @{ id = "2909"; name = "Component"; valueType = "String" },
        @{ id = "2910"; name = "Description"; valueType = "String" },
        @{ id = "2911"; name = "CostUSD"; valueType = "Double" },
        @{ id = "2912"; name = "Status"; valueType = "String" }
    )
    tableName = "factmaintenanceevent"
}

$entityTypes += @{
    id = "1011"; name = "PowerOutput"; entityIdParts = @("2951"); displayNamePropertyId = "2951"
    properties = @(
        @{ id = "2951"; name = "OutputId"; valueType = "String" },
        @{ id = "2952"; name = "TurbineId"; valueType = "String" },
        @{ id = "2953"; name = "Date"; valueType = "String" },
        @{ id = "2954"; name = "Hour"; valueType = "BigInt" },
        @{ id = "2955"; name = "WindSpeedMs"; valueType = "Double" },
        @{ id = "2956"; name = "PowerOutputKW"; valueType = "Double" },
        @{ id = "2957"; name = "CapacityFactor"; valueType = "Double" },
        @{ id = "2958"; name = "RotorRPM"; valueType = "Double" },
        @{ id = "2959"; name = "PitchAngleDeg"; valueType = "Double" },
        @{ id = "2960"; name = "YawAngleDeg"; valueType = "Double" },
        @{ id = "2961"; name = "GridFrequencyHz"; valueType = "Double" }
    )
    tableName = "factpoweroutput"
}

$entityTypes += @{
    id = "1012"; name = "Alert"; entityIdParts = @("2981"); displayNamePropertyId = "2981"
    properties = @(
        @{ id = "2981"; name = "AlertId"; valueType = "String" },
        @{ id = "2982"; name = "TurbineId"; valueType = "String" },
        @{ id = "2983"; name = "AlertType"; valueType = "String" },
        @{ id = "2984"; name = "Severity"; valueType = "String" },
        @{ id = "2985"; name = "Timestamp"; valueType = "String" },
        @{ id = "2986"; name = "SensorId"; valueType = "String" },
        @{ id = "2987"; name = "Value"; valueType = "Double" },
        @{ id = "2988"; name = "Threshold"; valueType = "Double" },
        @{ id = "2989"; name = "Description"; valueType = "String" },
        @{ id = "2990"; name = "Status"; valueType = "String" }
    )
    tableName = "factalert"
}

$relationships = @(
    @{ id = "3001"; name = "WindFarmHasTurbine"; sourceId = "1001"; targetId = "1002" },
    @{ id = "3002"; name = "TurbineHasNacelle"; sourceId = "1002"; targetId = "1003" },
    @{ id = "3003"; name = "TurbineHasBlade"; sourceId = "1002"; targetId = "1004" },
    @{ id = "3004"; name = "TurbineHasTower"; sourceId = "1002"; targetId = "1005" },
    @{ id = "3005"; name = "TurbineHasSensor"; sourceId = "1002"; targetId = "1006" },
    @{ id = "3006"; name = "WindFarmHasTechnician"; sourceId = "1001"; targetId = "1007" },
    @{ id = "3007"; name = "WindFarmHasWeatherStation"; sourceId = "1001"; targetId = "1008" },
    @{ id = "3008"; name = "WindFarmHasTransformer"; sourceId = "1001"; targetId = "1009" },
    @{ id = "3009"; name = "MaintenanceOnTurbine"; sourceId = "1010"; targetId = "1002" },
    @{ id = "3010"; name = "MaintenanceByTechnician"; sourceId = "1010"; targetId = "1007" },
    @{ id = "3011"; name = "PowerOutputFromTurbine"; sourceId = "1011"; targetId = "1002" },
    @{ id = "3012"; name = "AlertOnTurbine"; sourceId = "1012"; targetId = "1002" }
)

# BUILD PARTS
$parts = @()
$platform = '{"metadata":{"type":"Ontology","displayName":"WindTurbineOntology","description":"Wind Turbine / Wind Farm Ontology - wind farms, turbines, nacelles, blades, towers, sensors, maintenance, and power output"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'
$parts += @{ path = ".platform"; payload = (ToBase64 $platform); payloadType = "InlineBase64" }
$parts += @{ path = "definition.json"; payload = (ToBase64 "{}"); payloadType = "InlineBase64" }

foreach ($et in $entityTypes) {
    $propsJson = ($et.properties | ForEach-Object { '{"id":"' + $_.id + '","name":"' + $_.name + '","redefines":null,"baseTypeNamespaceType":null,"valueType":"' + $_.valueType + '"}' }) -join ','
    $tsPropsJson = "[]"
    if ($et.timeseriesProperties) {
        $tsPropsJson = '[' + (($et.timeseriesProperties | ForEach-Object { '{"id":"' + $_.id + '","name":"' + $_.name + '","redefines":null,"baseTypeNamespaceType":null,"valueType":"' + $_.valueType + '"}' }) -join ',') + ']'
    }
    $idPartsJson = '[' + (($et.entityIdParts | ForEach-Object { '"' + $_ + '"' }) -join ',') + ']'
    $entityJson = '{"id":"' + $et.id + '","namespace":"usertypes","baseEntityTypeId":null,"name":"' + $et.name + '","entityIdParts":' + $idPartsJson + ',"displayNamePropertyId":"' + $et.displayNamePropertyId + '","namespaceType":"Custom","visibility":"Visible","properties":[' + $propsJson + '],"timeseriesProperties":' + $tsPropsJson + '}'
    $parts += @{ path = "EntityTypes/$($et.id)/definition.json"; payload = (ToBase64 $entityJson); payloadType = "InlineBase64" }

    $bindGuid = DeterministicGuid "NonTimeSeries-$($et.id)"
    $propBindings = ($et.properties | ForEach-Object { '{"sourceColumnName":"' + $_.name + '","targetPropertyId":"' + $_.id + '"}' }) -join ','
    $bindJson = '{"id":"' + $bindGuid + '","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":[' + $propBindings + '],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $et.tableName + '","sourceSchema":"dbo"}}}'
    $parts += @{ path = "EntityTypes/$($et.id)/DataBindings/$bindGuid.json"; payload = (ToBase64 $bindJson); payloadType = "InlineBase64" }

    if ($et.timeseriesTable) {
        $tsBindGuid = DeterministicGuid "TimeSeries-$($et.id)"
        $tsBindings = ($et.timeseriesProperties | ForEach-Object { '{"sourceColumnName":"' + $_.name + '","targetPropertyId":"' + $_.id + '"}' }) -join ','
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
    $sourcePkPropId = $sourceEntity.entityIdParts[0]; $sourcePkName = ($sourceEntity.properties | Where-Object { $_.id -eq $sourcePkPropId }).name
    $targetPkPropId = $targetEntity.entityIdParts[0]; $targetPkName = ($targetEntity.properties | Where-Object { $_.id -eq $targetPkPropId }).name
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
        } else { Write-Warning "No FK found for relationship $($rel.name)" }
    }
}

Write-Host "Total parts: $($parts.Count) | Entities: $($entityTypes.Count) | Relationships: $($relationships.Count)"
$partsJson = ($parts | ForEach-Object { '{"path":"' + $_.path + '","payload":"' + $_.payload + '","payloadType":"InlineBase64"}' }) -join ','
$bodyStr = '{"definition":{"parts":[' + $partsJson + ']}}'
Write-Host "Payload size: $($bodyStr.Length) chars"

try {
    $resp = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$OntologyId/updateDefinition" -Method POST -Headers $headers -Body $bodyStr -UseBasicParsing
    if ($resp.StatusCode -eq 200) { Write-Host "Ontology updated!" }
    elseif ($resp.StatusCode -eq 202) {
        $opUrl = $resp.Headers["Location"]; $maxWait = 120; $waited = 0
        while ($waited -lt $maxWait) { Start-Sleep -Seconds 10; $waited += 10; $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $FabricToken" }; if ($poll.status -in @("Succeeded","Failed")) { Write-Host "Result: $($poll.status) ($waited`s)"; break }; Write-Host "  Status: $($poll.status) ($waited`s)..." }
    }
} catch {
    $sr = $_.Exception.Response; if ($sr) { $stream = $sr.GetResponseStream(); $reader = New-Object System.IO.StreamReader($stream); Write-Host "ERROR $([int]$sr.StatusCode): $($reader.ReadToEnd())" } else { Write-Host "ERROR: $($_.Exception.Message)" }
}
