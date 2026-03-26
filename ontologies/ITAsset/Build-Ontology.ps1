# Build-Ontology-ITAsset.ps1
# Builds the IT Asset Management Ontology definition for Microsoft Fabric
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
    id = "1001"; name = "DataCenter"; entityIdParts = @("2001"); displayNamePropertyId = "2002"
    properties = @(
        @{ id = "2001"; name = "DataCenterId"; valueType = "String" },
        @{ id = "2002"; name = "DataCenterName"; valueType = "String" },
        @{ id = "2003"; name = "Location"; valueType = "String" },
        @{ id = "2004"; name = "City"; valueType = "String" },
        @{ id = "2005"; name = "Country"; valueType = "String" },
        @{ id = "2006"; name = "TotalRackCapacity"; valueType = "BigInt" },
        @{ id = "2007"; name = "TierLevel"; valueType = "String" },
        @{ id = "2008"; name = "PowerCapacityKW"; valueType = "BigInt" },
        @{ id = "2009"; name = "Status"; valueType = "String" }
    )
    tableName = "dimdatacenter"
}

$entityTypes += @{
    id = "1002"; name = "Rack"; entityIdParts = @("2101"); displayNamePropertyId = "2102"
    properties = @(
        @{ id = "2101"; name = "RackId"; valueType = "String" },
        @{ id = "2102"; name = "RackName"; valueType = "String" },
        @{ id = "2103"; name = "DataCenterId"; valueType = "String" },
        @{ id = "2104"; name = "RackSize"; valueType = "String" },
        @{ id = "2105"; name = "MaxPowerW"; valueType = "BigInt" },
        @{ id = "2106"; name = "CurrentPowerW"; valueType = "BigInt" },
        @{ id = "2107"; name = "TemperatureZone"; valueType = "String" },
        @{ id = "2108"; name = "Status"; valueType = "String" }
    )
    tableName = "dimrack"
}

$entityTypes += @{
    id = "1003"; name = "Server"; entityIdParts = @("2201"); displayNamePropertyId = "2202"
    properties = @(
        @{ id = "2201"; name = "ServerId"; valueType = "String" },
        @{ id = "2202"; name = "ServerName"; valueType = "String" },
        @{ id = "2203"; name = "RackId"; valueType = "String" },
        @{ id = "2204"; name = "ServerType"; valueType = "String" },
        @{ id = "2205"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2206"; name = "Model"; valueType = "String" },
        @{ id = "2207"; name = "CPUCores"; valueType = "BigInt" },
        @{ id = "2208"; name = "MemoryGB"; valueType = "BigInt" },
        @{ id = "2209"; name = "StorageTB"; valueType = "BigInt" },
        @{ id = "2210"; name = "OS"; valueType = "String" },
        @{ id = "2211"; name = "IPAddress"; valueType = "String" },
        @{ id = "2212"; name = "Status"; valueType = "String" }
    )
    tableName = "dimserver"
    timeseriesTable = "ServerTelemetry"
    timeseriesProperties = @(
        @{ id = "4001"; name = "Timestamp"; valueType = "DateTime" },
        @{ id = "4002"; name = "CPUPercent"; valueType = "Double" },
        @{ id = "4003"; name = "MemoryPercent"; valueType = "Double" },
        @{ id = "4004"; name = "DiskIOPS"; valueType = "BigInt" },
        @{ id = "4005"; name = "NetworkMbps"; valueType = "Double" }
    )
    timestampColumn = "Timestamp"
}

$entityTypes += @{
    id = "1004"; name = "NetworkDevice"; entityIdParts = @("2301"); displayNamePropertyId = "2302"
    properties = @(
        @{ id = "2301"; name = "DeviceId"; valueType = "String" },
        @{ id = "2302"; name = "DeviceName"; valueType = "String" },
        @{ id = "2303"; name = "DataCenterId"; valueType = "String" },
        @{ id = "2304"; name = "DeviceType"; valueType = "String" },
        @{ id = "2305"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2306"; name = "Model"; valueType = "String" },
        @{ id = "2307"; name = "Ports"; valueType = "BigInt" },
        @{ id = "2308"; name = "IPAddress"; valueType = "String" },
        @{ id = "2309"; name = "Firmware"; valueType = "String" },
        @{ id = "2310"; name = "Status"; valueType = "String" }
    )
    tableName = "dimnetworkdevice"
}

$entityTypes += @{
    id = "1005"; name = "Application"; entityIdParts = @("2401"); displayNamePropertyId = "2402"
    properties = @(
        @{ id = "2401"; name = "AppId"; valueType = "String" },
        @{ id = "2402"; name = "AppName"; valueType = "String" },
        @{ id = "2403"; name = "ServerId"; valueType = "String" },
        @{ id = "2404"; name = "AppType"; valueType = "String" },
        @{ id = "2405"; name = "Version"; valueType = "String" },
        @{ id = "2406"; name = "Environment"; valueType = "String" },
        @{ id = "2407"; name = "Owner"; valueType = "String" },
        @{ id = "2408"; name = "SLATier"; valueType = "String" },
        @{ id = "2409"; name = "Status"; valueType = "String" }
    )
    tableName = "dimapplication"
}

$entityTypes += @{
    id = "1006"; name = "Database"; entityIdParts = @("2501"); displayNamePropertyId = "2502"
    properties = @(
        @{ id = "2501"; name = "DatabaseId"; valueType = "String" },
        @{ id = "2502"; name = "DatabaseName"; valueType = "String" },
        @{ id = "2503"; name = "ServerId"; valueType = "String" },
        @{ id = "2504"; name = "DBType"; valueType = "String" },
        @{ id = "2505"; name = "SizeGB"; valueType = "BigInt" },
        @{ id = "2506"; name = "Engine"; valueType = "String" },
        @{ id = "2507"; name = "Version"; valueType = "String" },
        @{ id = "2508"; name = "Status"; valueType = "String" }
    )
    tableName = "dimdatabase"
}

$entityTypes += @{
    id = "1007"; name = "VirtualMachine"; entityIdParts = @("2601"); displayNamePropertyId = "2602"
    properties = @(
        @{ id = "2601"; name = "VMId"; valueType = "String" },
        @{ id = "2602"; name = "VMName"; valueType = "String" },
        @{ id = "2603"; name = "ServerId"; valueType = "String" },
        @{ id = "2604"; name = "vCPU"; valueType = "BigInt" },
        @{ id = "2605"; name = "MemoryGB"; valueType = "BigInt" },
        @{ id = "2606"; name = "DiskGB"; valueType = "BigInt" },
        @{ id = "2607"; name = "OS"; valueType = "String" },
        @{ id = "2608"; name = "Status"; valueType = "String" }
    )
    tableName = "dimvirtualmachine"
}

$entityTypes += @{
    id = "1008"; name = "User"; entityIdParts = @("2701"); displayNamePropertyId = "2702"
    properties = @(
        @{ id = "2701"; name = "UserId"; valueType = "String" },
        @{ id = "2702"; name = "FullName"; valueType = "String" },
        @{ id = "2703"; name = "Department"; valueType = "String" },
        @{ id = "2704"; name = "Role"; valueType = "String" },
        @{ id = "2705"; name = "Email"; valueType = "String" },
        @{ id = "2706"; name = "AccessLevel"; valueType = "String" },
        @{ id = "2707"; name = "Status"; valueType = "String" }
    )
    tableName = "dimuser"
}

$entityTypes += @{
    id = "1009"; name = "Incident"; entityIdParts = @("2801"); displayNamePropertyId = "2801"
    properties = @(
        @{ id = "2801"; name = "IncidentId"; valueType = "String" },
        @{ id = "2802"; name = "ServerId"; valueType = "String" },
        @{ id = "2803"; name = "IncidentType"; valueType = "String" },
        @{ id = "2804"; name = "Severity"; valueType = "String" },
        @{ id = "2805"; name = "ReportedByUserId"; valueType = "String" },
        @{ id = "2806"; name = "CreatedDate"; valueType = "String" },
        @{ id = "2807"; name = "ResolvedDate"; valueType = "String" },
        @{ id = "2808"; name = "DurationHours"; valueType = "Double" },
        @{ id = "2809"; name = "RootCause"; valueType = "String" },
        @{ id = "2810"; name = "Description"; valueType = "String" },
        @{ id = "2811"; name = "Status"; valueType = "String" }
    )
    tableName = "factincident"
}

$entityTypes += @{
    id = "1010"; name = "License"; entityIdParts = @("2901"); displayNamePropertyId = "2901"
    properties = @(
        @{ id = "2901"; name = "LicenseId"; valueType = "String" },
        @{ id = "2902"; name = "AppId"; valueType = "String" },
        @{ id = "2903"; name = "LicenseType"; valueType = "String" },
        @{ id = "2904"; name = "Vendor"; valueType = "String" },
        @{ id = "2905"; name = "ExpirationDate"; valueType = "String" },
        @{ id = "2906"; name = "Seats"; valueType = "BigInt" },
        @{ id = "2907"; name = "AssignedSeats"; valueType = "BigInt" },
        @{ id = "2908"; name = "CostUSD"; valueType = "Double" },
        @{ id = "2909"; name = "Status"; valueType = "String" }
    )
    tableName = "dimlicense"
}

$entityTypes += @{
    id = "1011"; name = "Alert"; entityIdParts = @("2951"); displayNamePropertyId = "2951"
    properties = @(
        @{ id = "2951"; name = "AlertId"; valueType = "String" },
        @{ id = "2952"; name = "ServerId"; valueType = "String" },
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

$relationships = @(
    @{ id = "3001"; name = "DataCenterHasRack"; sourceId = "1001"; targetId = "1002" },
    @{ id = "3002"; name = "RackHasServer"; sourceId = "1002"; targetId = "1003" },
    @{ id = "3003"; name = "ServerHostsApp"; sourceId = "1003"; targetId = "1005" },
    @{ id = "3004"; name = "ServerHostsDB"; sourceId = "1003"; targetId = "1006" },
    @{ id = "3005"; name = "ServerHostsVM"; sourceId = "1003"; targetId = "1007" },
    @{ id = "3006"; name = "DataCenterHasNetworkDevice"; sourceId = "1001"; targetId = "1004" },
    @{ id = "3007"; name = "IncidentOnServer"; sourceId = "1009"; targetId = "1003" },
    @{ id = "3008"; name = "IncidentReportedBy"; sourceId = "1009"; targetId = "1008" },
    @{ id = "3009"; name = "AppHasLicense"; sourceId = "1005"; targetId = "1010" },
    @{ id = "3010"; name = "AlertOnServer"; sourceId = "1011"; targetId = "1003" }
)

# BUILD PARTS (reusable pattern)
$parts = @()
$platform = '{"metadata":{"type":"Ontology","displayName":"ITAssetOntology","description":"IT Asset Management Ontology - datacenters, servers, applications, databases, VMs, incidents, and licenses"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'
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
