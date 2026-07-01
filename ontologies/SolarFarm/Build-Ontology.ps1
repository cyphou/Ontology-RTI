# Build-Ontology-SolarFarm.ps1
# Builds the Solar Farm / Photovoltaic Ontology definition for Microsoft Fabric
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
    id = "1001"; name = "SolarPlant"; entityIdParts = @("2001"); displayNamePropertyId = "2002"
    properties = @(
        @{ id = "2001"; name = "PlantId"; valueType = "String" },
        @{ id = "2002"; name = "PlantName"; valueType = "String" },
        @{ id = "2003"; name = "Region"; valueType = "String" },
        @{ id = "2004"; name = "Latitude"; valueType = "Double" },
        @{ id = "2005"; name = "Longitude"; valueType = "Double" },
        @{ id = "2006"; name = "CapacityMWc"; valueType = "Double" },
        @{ id = "2007"; name = "ArrayCount"; valueType = "BigInt" },
        @{ id = "2008"; name = "Operator"; valueType = "String" },
        @{ id = "2009"; name = "Status"; valueType = "String" }
    )
    tableName = "dimsolarplant"
}

$entityTypes += @{
    id = "1002"; name = "SolarArray"; entityIdParts = @("2101"); displayNamePropertyId = "2102"
    properties = @(
        @{ id = "2101"; name = "ArrayId"; valueType = "String" },
        @{ id = "2102"; name = "ArrayName"; valueType = "String" },
        @{ id = "2103"; name = "PlantId"; valueType = "String" },
        @{ id = "2104"; name = "RatedCapacityKW"; valueType = "Double" },
        @{ id = "2105"; name = "TiltDegrees"; valueType = "Double" },
        @{ id = "2106"; name = "Orientation"; valueType = "String" },
        @{ id = "2107"; name = "Status"; valueType = "String" }
    )
    tableName = "dimsolararray"
    timeseriesTable = "ArrayTelemetry"
    timeseriesProperties = @(
        @{ id = "4001"; name = "Timestamp"; valueType = "DateTime" },
        @{ id = "4002"; name = "IrradianceWm2"; valueType = "Double" },
        @{ id = "4003"; name = "PowerOutputKW"; valueType = "Double" },
        @{ id = "4004"; name = "ModuleTempC"; valueType = "Double" },
        @{ id = "4005"; name = "InverterLoadPct"; valueType = "Double" },
        @{ id = "4006"; name = "VoltageV"; valueType = "Double" },
        @{ id = "4007"; name = "CurrentA"; valueType = "Double" }
    )
    timestampColumn = "Timestamp"
}

$entityTypes += @{
    id = "1003"; name = "Inverter"; entityIdParts = @("2201"); displayNamePropertyId = "2202"
    properties = @(
        @{ id = "2201"; name = "InverterId"; valueType = "String" },
        @{ id = "2202"; name = "InverterName"; valueType = "String" },
        @{ id = "2203"; name = "ArrayId"; valueType = "String" },
        @{ id = "2204"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2205"; name = "RatedPowerKW"; valueType = "Double" },
        @{ id = "2206"; name = "Efficiency"; valueType = "Double" },
        @{ id = "2207"; name = "CoolingType"; valueType = "String" },
        @{ id = "2208"; name = "InstallDate"; valueType = "String" },
        @{ id = "2209"; name = "Status"; valueType = "String" }
    )
    tableName = "diminverter"
}

$entityTypes += @{
    id = "1004"; name = "PanelString"; entityIdParts = @("2301"); displayNamePropertyId = "2302"
    properties = @(
        @{ id = "2301"; name = "StringId"; valueType = "String" },
        @{ id = "2302"; name = "StringName"; valueType = "String" },
        @{ id = "2303"; name = "ArrayId"; valueType = "String" },
        @{ id = "2304"; name = "ModuleCount"; valueType = "BigInt" },
        @{ id = "2305"; name = "ModuleType"; valueType = "String" },
        @{ id = "2306"; name = "RatedVoltageV"; valueType = "BigInt" },
        @{ id = "2307"; name = "InstallDate"; valueType = "String" },
        @{ id = "2308"; name = "LastInspectionDate"; valueType = "String" },
        @{ id = "2309"; name = "Status"; valueType = "String" }
    )
    tableName = "dimstring"
}

$entityTypes += @{
    id = "1005"; name = "Tracker"; entityIdParts = @("2401"); displayNamePropertyId = "2402"
    properties = @(
        @{ id = "2401"; name = "TrackerId"; valueType = "String" },
        @{ id = "2402"; name = "TrackerName"; valueType = "String" },
        @{ id = "2403"; name = "ArrayId"; valueType = "String" },
        @{ id = "2404"; name = "TrackerType"; valueType = "String" },
        @{ id = "2405"; name = "AxisType"; valueType = "String" },
        @{ id = "2406"; name = "MaxTiltDeg"; valueType = "BigInt" },
        @{ id = "2407"; name = "InstallDate"; valueType = "String" },
        @{ id = "2408"; name = "Status"; valueType = "String" }
    )
    tableName = "dimtracker"
}

$entityTypes += @{
    id = "1006"; name = "Sensor"; entityIdParts = @("2501"); displayNamePropertyId = "2502"
    properties = @(
        @{ id = "2501"; name = "SensorId"; valueType = "String" },
        @{ id = "2502"; name = "SensorName"; valueType = "String" },
        @{ id = "2503"; name = "ArrayId"; valueType = "String" },
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
        @{ id = "2605"; name = "PlantId"; valueType = "String" },
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
        @{ id = "2703"; name = "PlantId"; valueType = "String" },
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
        @{ id = "2803"; name = "PlantId"; valueType = "String" },
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
        @{ id = "2902"; name = "ArrayId"; valueType = "String" },
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
    id = "1011"; name = "EnergyProduction"; entityIdParts = @("2951"); displayNamePropertyId = "2951"
    properties = @(
        @{ id = "2951"; name = "ProductionId"; valueType = "String" },
        @{ id = "2952"; name = "ArrayId"; valueType = "String" },
        @{ id = "2953"; name = "Date"; valueType = "String" },
        @{ id = "2954"; name = "Hour"; valueType = "BigInt" },
        @{ id = "2955"; name = "IrradianceWm2"; valueType = "Double" },
        @{ id = "2956"; name = "PowerOutputKW"; valueType = "Double" },
        @{ id = "2957"; name = "PerformanceRatio"; valueType = "Double" },
        @{ id = "2958"; name = "ModuleTempC"; valueType = "Double" },
        @{ id = "2959"; name = "InverterEfficiency"; valueType = "Double" },
        @{ id = "2960"; name = "GridFrequencyHz"; valueType = "Double" }
    )
    tableName = "factenergyproduction"
}

$entityTypes += @{
    id = "1012"; name = "Alert"; entityIdParts = @("2981"); displayNamePropertyId = "2981"
    properties = @(
        @{ id = "2981"; name = "AlertId"; valueType = "String" },
        @{ id = "2982"; name = "ArrayId"; valueType = "String" },
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
    @{
        id = "3001"; name = "PlantHasArray"; sourceId = "1001"; targetId = "1002"
        ctxTable = "bridgesolarplantarray"
        srcKeys = @( @{ col = "PlantId"; propId = "2001" } )
        tgtKeys = @( @{ col = "ArrayId"; propId = "2101" } )
    },
    @{
        id = "3002"; name = "ArrayHasInverter"; sourceId = "1002"; targetId = "1003"
        ctxTable = "bridgesolararrayinverter"
        srcKeys = @( @{ col = "ArrayId"; propId = "2101" } )
        tgtKeys = @( @{ col = "InverterId"; propId = "2201" } )
    },
    @{
        id = "3003"; name = "ArrayHasString"; sourceId = "1002"; targetId = "1004"
        ctxTable = "bridgesolararraystring"
        srcKeys = @( @{ col = "ArrayId"; propId = "2101" } )
        tgtKeys = @( @{ col = "StringId"; propId = "2301" } )
    },
    @{
        id = "3004"; name = "ArrayHasTracker"; sourceId = "1002"; targetId = "1005"
        ctxTable = "bridgesolararraytracker"
        srcKeys = @( @{ col = "ArrayId"; propId = "2101" } )
        tgtKeys = @( @{ col = "TrackerId"; propId = "2401" } )
    },
    @{
        id = "3005"; name = "ArrayHasSensor"; sourceId = "1002"; targetId = "1006"
        ctxTable = "bridgesolararraysensor"
        srcKeys = @( @{ col = "ArrayId"; propId = "2101" } )
        tgtKeys = @( @{ col = "SensorId"; propId = "2501" } )
    },
    @{
        id = "3006"; name = "PlantHasTechnician"; sourceId = "1001"; targetId = "1007"
        ctxTable = "bridgesolarplanttechnician"
        srcKeys = @( @{ col = "PlantId"; propId = "2001" } )
        tgtKeys = @( @{ col = "TechnicianId"; propId = "2601" } )
    },
    @{
        id = "3007"; name = "PlantHasWeatherStation"; sourceId = "1001"; targetId = "1008"
        ctxTable = "bridgesolarplantweatherstation"
        srcKeys = @( @{ col = "PlantId"; propId = "2001" } )
        tgtKeys = @( @{ col = "StationId"; propId = "2701" } )
    },
    @{
        id = "3008"; name = "PlantHasTransformer"; sourceId = "1001"; targetId = "1009"
        ctxTable = "bridgesolarplanttransformer"
        srcKeys = @( @{ col = "PlantId"; propId = "2001" } )
        tgtKeys = @( @{ col = "TransformerId"; propId = "2801" } )
    },
    @{
        id = "3009"; name = "MaintenanceOnArray"; sourceId = "1010"; targetId = "1002"
        ctxTable = "bridgemaintenanceeventarray"
        srcKeys = @( @{ col = "EventId"; propId = "2901" } )
        tgtKeys = @( @{ col = "ArrayId"; propId = "2101" } )
    },
    @{
        id = "3010"; name = "MaintenanceByTechnician"; sourceId = "1010"; targetId = "1007"
        ctxTable = "bridgemaintenanceeventtechnician"
        srcKeys = @( @{ col = "EventId"; propId = "2901" } )
        tgtKeys = @( @{ col = "TechnicianId"; propId = "2601" } )
    },
    @{
        id = "3011"; name = "ProductionFromArray"; sourceId = "1011"; targetId = "1002"
        ctxTable = "bridgeenergyproductionarray"
        srcKeys = @( @{ col = "ProductionId"; propId = "2951" } )
        tgtKeys = @( @{ col = "ArrayId"; propId = "2101" } )
    },
    @{
        id = "3012"; name = "AlertOnArray"; sourceId = "1012"; targetId = "1002"
        ctxTable = "bridgealertarray"
        srcKeys = @( @{ col = "AlertId"; propId = "2981" } )
        tgtKeys = @( @{ col = "ArrayId"; propId = "2101" } )
    }
)

# BUILD PARTS
$parts = @()
$platform = '{"metadata":{"type":"Ontology","displayName":"SolarFarmOntology","description":"Solar Farm / Photovoltaic Ontology - solar plants, arrays, inverters, panel strings, trackers, sensors, maintenance, and energy production"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'
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
    $bindJson = '{"id":"' + $bindGuid + '","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":[' + $propBindings + '],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $et.tableName + '"}}}'
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

    if ($rel.ctxTable -and $rel.srcKeys -and $rel.tgtKeys) {
        $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
        $srcBindings = ($rel.srcKeys | ForEach-Object {
            '{"sourceColumnName":"' + $_.col + '","targetPropertyId":"' + $_.propId + '"}'
        }) -join ','
        $tgtBindings = ($rel.tgtKeys | ForEach-Object {
            '{"sourceColumnName":"' + $_.col + '","targetPropertyId":"' + $_.propId + '"}'
        }) -join ','
        $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $rel.ctxTable + '","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[' + $srcBindings + '],"targetKeyRefBindings":[' + $tgtBindings + ']}'
        $parts += @{ path = "RelationshipTypes/$($rel.id)/Contextualizations/$ctxGuid.json"; payload = (ToBase64 $ctxJson); payloadType = "InlineBase64" }
        continue
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
        $opUrl = $resp.Headers["Location"]; if ($opUrl -is [array]) { $opUrl = $opUrl[0] }
        $maxWait = 120; $waited = 0
        $poll = $null
        while ($waited -lt $maxWait) {
            Start-Sleep -Seconds 10
            $waited += 10
            $poll = Invoke-RestMethod -Uri $opUrl -Headers @{ Authorization = "Bearer $FabricToken" }
            if ($poll.status -in @("Succeeded", "Failed")) {
                Write-Host "Result: $($poll.status) ($waited`s)"
                break
            }
            Write-Host "  Status: $($poll.status) ($waited`s)..."
        }
        if (-not $poll -or $poll.status -ne "Succeeded") {
            if ($poll) {
                Write-Host "Ontology operation diagnostic:" -ForegroundColor Yellow
                Write-Host ($poll | ConvertTo-Json -Depth 20) -ForegroundColor Yellow
            }
            throw "Ontology updateDefinition operation failed."
        }
    }
} catch {
    $errBody = ""
    if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $errBody = $_.ErrorDetails.Message }
    elseif ($_.Exception.Response) { try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errBody = $sr.ReadToEnd(); $sr.Close() } catch { $errBody = $_.Exception.Message } }
    else { $errBody = $_.Exception.Message }
    Write-Host "ERROR: $errBody" -ForegroundColor Red
}
