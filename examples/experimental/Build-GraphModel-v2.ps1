# Build-GraphModel-v2.ps1
# Deploys the GraphModel definition using the correct multi-file format
# via the GraphModel-specific API endpoint.
#
# Required files: dataSources.json, graphType.json, graphDefinition.json,
#                 stylingConfiguration.json, .platform
# Endpoint: POST /v1/workspaces/{wsId}/GraphModels/{gmId}/updateDefinition

param(
    [string]$WorkspaceId,
    [string]$LakehouseId,
    [string]$GraphModelId,
    [string]$FabricToken
)

$headers = @{ Authorization = "Bearer $FabricToken" }
$basePath = "abfss://$WorkspaceId@onelake.dfs.fabric.microsoft.com/$LakehouseId/Tables"

function ToBase64([string]$text) {
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($text))
}

# ============================================================================
# 1. dataSources.json
# ============================================================================
$tables = @(
    "dimrefinery","dimprocessunit","dimequipment","dimpipeline",
    "dimcrudeoil","dimrefinedproduct","dimstoragetank","dimsensor",
    "dimemployee","factmaintenance","factsafetyalarm","factproduction",
    "bridgecrudeoilprocessunit"
)

$dsParts = @()
foreach ($t in $tables) {
    $dsParts += '{"name":"' + $t + '","type":"DeltaTable","properties":{"path":"' + $basePath + '/' + $t + '"}}'
}
$dataSourcesJson = '{"dataSources":[' + ($dsParts -join ',') + ']}'

# ============================================================================
# 2. graphType.json  (node types + edge types)
# ============================================================================
$nodeTypesJson = @(
    '{"alias":"Refinery","labels":["Refinery"],"primaryKeyProperties":["RefineryId"],"properties":[{"name":"RefineryId","type":"STRING"},{"name":"RefineryName","type":"STRING"},{"name":"Country","type":"STRING"},{"name":"State","type":"STRING"},{"name":"City","type":"STRING"},{"name":"Latitude","type":"FLOAT"},{"name":"Longitude","type":"FLOAT"},{"name":"CapacityBPD","type":"INT"},{"name":"YearBuilt","type":"INT"},{"name":"Status","type":"STRING"},{"name":"Operator","type":"STRING"}]}'
    '{"alias":"ProcessUnit","labels":["ProcessUnit"],"primaryKeyProperties":["ProcessUnitId"],"properties":[{"name":"ProcessUnitId","type":"STRING"},{"name":"ProcessUnitName","type":"STRING"},{"name":"ProcessUnitType","type":"STRING"},{"name":"RefineryId","type":"STRING"},{"name":"CapacityBPD","type":"INT"},{"name":"DesignTemperatureF","type":"FLOAT"},{"name":"DesignPressurePSI","type":"FLOAT"},{"name":"YearInstalled","type":"INT"},{"name":"Status","type":"STRING"},{"name":"Description","type":"STRING"}]}'
    '{"alias":"Equipment","labels":["Equipment"],"primaryKeyProperties":["EquipmentId"],"properties":[{"name":"EquipmentId","type":"STRING"},{"name":"EquipmentName","type":"STRING"},{"name":"EquipmentType","type":"STRING"},{"name":"ProcessUnitId","type":"STRING"},{"name":"Manufacturer","type":"STRING"},{"name":"Model","type":"STRING"},{"name":"InstallDate","type":"STRING"},{"name":"LastInspectionDate","type":"STRING"},{"name":"Status","type":"STRING"},{"name":"CriticalityLevel","type":"STRING"},{"name":"ExpectedLifeYears","type":"INT"}]}'
    '{"alias":"Pipeline","labels":["Pipeline"],"primaryKeyProperties":["PipelineId"],"properties":[{"name":"PipelineId","type":"STRING"},{"name":"PipelineName","type":"STRING"},{"name":"FromProcessUnitId","type":"STRING"},{"name":"ToProcessUnitId","type":"STRING"},{"name":"RefineryId","type":"STRING"},{"name":"DiameterInches","type":"FLOAT"},{"name":"LengthFeet","type":"FLOAT"},{"name":"Material","type":"STRING"},{"name":"MaxFlowBPD","type":"INT"},{"name":"InstalledDate","type":"STRING"},{"name":"Status","type":"STRING"}]}'
    '{"alias":"CrudeOil","labels":["CrudeOil"],"primaryKeyProperties":["CrudeOilId"],"properties":[{"name":"CrudeOilId","type":"STRING"},{"name":"CrudeGradeName","type":"STRING"},{"name":"APIGravity","type":"FLOAT"},{"name":"SulfurContentPct","type":"FLOAT"},{"name":"Origin","type":"STRING"},{"name":"Classification","type":"STRING"},{"name":"PricePerBarrelUSD","type":"FLOAT"},{"name":"Description","type":"STRING"}]}'
    '{"alias":"RefinedProduct","labels":["RefinedProduct"],"primaryKeyProperties":["ProductId"],"properties":[{"name":"ProductId","type":"STRING"},{"name":"ProductName","type":"STRING"},{"name":"ProductCategory","type":"STRING"},{"name":"APIGravity","type":"FLOAT"},{"name":"SulfurLimitPPM","type":"FLOAT"},{"name":"FlashPointF","type":"FLOAT"},{"name":"SpecStandard","type":"STRING"},{"name":"PricePerBarrelUSD","type":"FLOAT"},{"name":"Description","type":"STRING"}]}'
    '{"alias":"StorageTank","labels":["StorageTank"],"primaryKeyProperties":["TankId"],"properties":[{"name":"TankId","type":"STRING"},{"name":"TankName","type":"STRING"},{"name":"RefineryId","type":"STRING"},{"name":"ProductId","type":"STRING"},{"name":"TankType","type":"STRING"},{"name":"CapacityBarrels","type":"INT"},{"name":"CurrentLevelBarrels","type":"INT"},{"name":"DiameterFeet","type":"STRING"},{"name":"HeightFeet","type":"STRING"},{"name":"Material","type":"STRING"},{"name":"Status","type":"STRING"},{"name":"LastInspectionDate","type":"STRING"}]}'
    '{"alias":"Sensor","labels":["Sensor"],"primaryKeyProperties":["SensorId"],"properties":[{"name":"SensorId","type":"STRING"},{"name":"SensorName","type":"STRING"},{"name":"SensorType","type":"STRING"},{"name":"EquipmentId","type":"STRING"},{"name":"MeasurementUnit","type":"STRING"},{"name":"MinRange","type":"FLOAT"},{"name":"MaxRange","type":"FLOAT"},{"name":"InstallDate","type":"STRING"},{"name":"CalibrationDate","type":"STRING"},{"name":"Status","type":"STRING"},{"name":"Manufacturer","type":"STRING"}]}'
    '{"alias":"Employee","labels":["Employee"],"primaryKeyProperties":["EmployeeId"],"properties":[{"name":"EmployeeId","type":"STRING"},{"name":"FirstName","type":"STRING"},{"name":"LastName","type":"STRING"},{"name":"Role","type":"STRING"},{"name":"Department","type":"STRING"},{"name":"RefineryId","type":"STRING"},{"name":"HireDate","type":"STRING"},{"name":"CertificationLevel","type":"STRING"},{"name":"ShiftPattern","type":"STRING"},{"name":"Status","type":"STRING"}]}'
    '{"alias":"MaintenanceEvent","labels":["MaintenanceEvent"],"primaryKeyProperties":["MaintenanceId"],"properties":[{"name":"MaintenanceId","type":"STRING"},{"name":"EquipmentId","type":"STRING"},{"name":"MaintenanceType","type":"STRING"},{"name":"Priority","type":"STRING"},{"name":"PerformedByEmployeeId","type":"STRING"},{"name":"StartDate","type":"STRING"},{"name":"EndDate","type":"STRING"},{"name":"DurationHours","type":"FLOAT"},{"name":"CostUSD","type":"FLOAT"},{"name":"Description","type":"STRING"},{"name":"WorkOrderNumber","type":"STRING"},{"name":"Status","type":"STRING"}]}'
    '{"alias":"SafetyAlarm","labels":["SafetyAlarm"],"primaryKeyProperties":["AlarmId"],"properties":[{"name":"AlarmId","type":"STRING"},{"name":"SensorId","type":"STRING"},{"name":"AlarmType","type":"STRING"},{"name":"Severity","type":"STRING"},{"name":"AlarmTimestamp","type":"STRING"},{"name":"AcknowledgedTimestamp","type":"STRING"},{"name":"ClearedTimestamp","type":"STRING"},{"name":"AlarmValue","type":"FLOAT"},{"name":"ThresholdValue","type":"FLOAT"},{"name":"Description","type":"STRING"},{"name":"ActionTaken","type":"STRING"},{"name":"AcknowledgedByEmployeeId","type":"STRING"}]}'
    '{"alias":"ProductionRecord","labels":["ProductionRecord"],"primaryKeyProperties":["ProductionId"],"properties":[{"name":"ProductionId","type":"STRING"},{"name":"ProcessUnitId","type":"STRING"},{"name":"ProductId","type":"STRING"},{"name":"ProductionDate","type":"STRING"},{"name":"OutputBarrels","type":"INT"},{"name":"YieldPercent","type":"FLOAT"},{"name":"QualityGrade","type":"STRING"},{"name":"EnergyConsumptionMMBTU","type":"FLOAT"},{"name":"Notes","type":"STRING"}]}'
    '{"alias":"CrudeOilFeed","labels":["CrudeOilFeed"],"primaryKeyProperties":["BridgeId"],"properties":[{"name":"BridgeId","type":"STRING"},{"name":"CrudeOilId","type":"STRING"},{"name":"ProcessUnitId","type":"STRING"},{"name":"FeedRateBPD","type":"INT"},{"name":"EffectiveDate","type":"STRING"},{"name":"Notes","type":"STRING"}]}'
)

$edgeTypesJson = @(
    '{"alias":"RefineryHasProcessUnit","labels":["HAS_PROCESS_UNIT"],"sourceNodeType":{"alias":"Refinery"},"destinationNodeType":{"alias":"ProcessUnit"},"properties":[{"name":"RefineryId","type":"STRING"},{"name":"ProcessUnitId","type":"STRING"}]}'
    '{"alias":"ProcessUnitHasEquipment","labels":["HAS_EQUIPMENT"],"sourceNodeType":{"alias":"ProcessUnit"},"destinationNodeType":{"alias":"Equipment"},"properties":[{"name":"ProcessUnitId","type":"STRING"},{"name":"EquipmentId","type":"STRING"}]}'
    '{"alias":"PipelineFromProcessUnit","labels":["PIPELINE_FROM"],"sourceNodeType":{"alias":"Pipeline"},"destinationNodeType":{"alias":"ProcessUnit"},"properties":[{"name":"PipelineId","type":"STRING"},{"name":"FromProcessUnitId","type":"STRING"}]}'
    '{"alias":"RefineryHasPipeline","labels":["HAS_PIPELINE"],"sourceNodeType":{"alias":"Refinery"},"destinationNodeType":{"alias":"Pipeline"},"properties":[{"name":"RefineryId","type":"STRING"},{"name":"PipelineId","type":"STRING"}]}'
    '{"alias":"RefineryHasStorageTank","labels":["HAS_STORAGE_TANK"],"sourceNodeType":{"alias":"Refinery"},"destinationNodeType":{"alias":"StorageTank"},"properties":[{"name":"RefineryId","type":"STRING"},{"name":"TankId","type":"STRING"}]}'
    '{"alias":"StorageTankHoldsProduct","labels":["HOLDS_PRODUCT"],"sourceNodeType":{"alias":"StorageTank"},"destinationNodeType":{"alias":"RefinedProduct"},"properties":[{"name":"TankId","type":"STRING"},{"name":"ProductId","type":"STRING"}]}'
    '{"alias":"EquipmentHasSensor","labels":["HAS_SENSOR"],"sourceNodeType":{"alias":"Equipment"},"destinationNodeType":{"alias":"Sensor"},"properties":[{"name":"EquipmentId","type":"STRING"},{"name":"SensorId","type":"STRING"}]}'
    '{"alias":"MaintenanceOnEquipment","labels":["MAINTENANCE_ON"],"sourceNodeType":{"alias":"MaintenanceEvent"},"destinationNodeType":{"alias":"Equipment"},"properties":[{"name":"MaintenanceId","type":"STRING"},{"name":"EquipmentId","type":"STRING"}]}'
    '{"alias":"MaintenanceByEmployee","labels":["PERFORMED_BY"],"sourceNodeType":{"alias":"MaintenanceEvent"},"destinationNodeType":{"alias":"Employee"},"properties":[{"name":"MaintenanceId","type":"STRING"},{"name":"PerformedByEmployeeId","type":"STRING"}]}'
    '{"alias":"AlarmFromSensor","labels":["ALARM_FROM"],"sourceNodeType":{"alias":"SafetyAlarm"},"destinationNodeType":{"alias":"Sensor"},"properties":[{"name":"AlarmId","type":"STRING"},{"name":"SensorId","type":"STRING"}]}'
    '{"alias":"ProductionFromProcessUnit","labels":["PRODUCED_BY"],"sourceNodeType":{"alias":"ProductionRecord"},"destinationNodeType":{"alias":"ProcessUnit"},"properties":[{"name":"ProductionId","type":"STRING"},{"name":"ProcessUnitId","type":"STRING"}]}'
    '{"alias":"ProductionOfProduct","labels":["PRODUCES"],"sourceNodeType":{"alias":"ProductionRecord"},"destinationNodeType":{"alias":"RefinedProduct"},"properties":[{"name":"ProductionId","type":"STRING"},{"name":"ProductId","type":"STRING"}]}'
    '{"alias":"RefineryHasEmployee","labels":["EMPLOYS"],"sourceNodeType":{"alias":"Refinery"},"destinationNodeType":{"alias":"Employee"},"properties":[{"name":"RefineryId","type":"STRING"},{"name":"EmployeeId","type":"STRING"}]}'
    '{"alias":"CrudeFeedToProcessUnit","labels":["FEEDS_INTO"],"sourceNodeType":{"alias":"CrudeOilFeed"},"destinationNodeType":{"alias":"ProcessUnit"},"properties":[{"name":"BridgeId","type":"STRING"},{"name":"ProcessUnitId","type":"STRING"}]}'
    '{"alias":"CrudeFeedFromCrudeOil","labels":["CRUDE_SOURCE"],"sourceNodeType":{"alias":"CrudeOilFeed"},"destinationNodeType":{"alias":"CrudeOil"},"properties":[{"name":"BridgeId","type":"STRING"},{"name":"CrudeOilId","type":"STRING"}]}'
)

$graphTypeJson = '{"schemaVersion":"1.0.0","nodeTypes":[' + ($nodeTypesJson -join ',') + '],"edgeTypes":[' + ($edgeTypesJson -join ',') + ']}'

# ============================================================================
# 3. graphDefinition.json  (data source mappings)
# ============================================================================

# Node table mappings
$nodeTableMap = @{
    "Refinery" = "dimrefinery"
    "ProcessUnit" = "dimprocessunit"
    "Equipment" = "dimequipment"
    "Pipeline" = "dimpipeline"
    "CrudeOil" = "dimcrudeoil"
    "RefinedProduct" = "dimrefinedproduct"
    "StorageTank" = "dimstoragetank"
    "Sensor" = "dimsensor"
    "Employee" = "dimemployee"
    "MaintenanceEvent" = "factmaintenance"
    "SafetyAlarm" = "factsafetyalarm"
    "ProductionRecord" = "factproduction"
    "CrudeOilFeed" = "bridgecrudeoilprocessunit"
}

# Property names per node type for mappings
$nodeProps = @{
    "Refinery" = @("RefineryId","RefineryName","Country","State","City","Latitude","Longitude","CapacityBPD","YearBuilt","Status","Operator")
    "ProcessUnit" = @("ProcessUnitId","ProcessUnitName","ProcessUnitType","RefineryId","CapacityBPD","DesignTemperatureF","DesignPressurePSI","YearInstalled","Status","Description")
    "Equipment" = @("EquipmentId","EquipmentName","EquipmentType","ProcessUnitId","Manufacturer","Model","InstallDate","LastInspectionDate","Status","CriticalityLevel","ExpectedLifeYears")
    "Pipeline" = @("PipelineId","PipelineName","FromProcessUnitId","ToProcessUnitId","RefineryId","DiameterInches","LengthFeet","Material","MaxFlowBPD","InstalledDate","Status")
    "CrudeOil" = @("CrudeOilId","CrudeGradeName","APIGravity","SulfurContentPct","Origin","Classification","PricePerBarrelUSD","Description")
    "RefinedProduct" = @("ProductId","ProductName","ProductCategory","APIGravity","SulfurLimitPPM","FlashPointF","SpecStandard","PricePerBarrelUSD","Description")
    "StorageTank" = @("TankId","TankName","RefineryId","ProductId","TankType","CapacityBarrels","CurrentLevelBarrels","DiameterFeet","HeightFeet","Material","Status","LastInspectionDate")
    "Sensor" = @("SensorId","SensorName","SensorType","EquipmentId","MeasurementUnit","MinRange","MaxRange","InstallDate","CalibrationDate","Status","Manufacturer")
    "Employee" = @("EmployeeId","FirstName","LastName","Role","Department","RefineryId","HireDate","CertificationLevel","ShiftPattern","Status")
    "MaintenanceEvent" = @("MaintenanceId","EquipmentId","MaintenanceType","Priority","PerformedByEmployeeId","StartDate","EndDate","DurationHours","CostUSD","Description","WorkOrderNumber","Status")
    "SafetyAlarm" = @("AlarmId","SensorId","AlarmType","Severity","AlarmTimestamp","AcknowledgedTimestamp","ClearedTimestamp","AlarmValue","ThresholdValue","Description","ActionTaken","AcknowledgedByEmployeeId")
    "ProductionRecord" = @("ProductionId","ProcessUnitId","ProductId","ProductionDate","OutputBarrels","YieldPercent","QualityGrade","EnergyConsumptionMMBTU","Notes")
    "CrudeOilFeed" = @("BridgeId","CrudeOilId","ProcessUnitId","FeedRateBPD","EffectiveDate","Notes")
}

$ntMappingParts = @()
foreach ($alias in $nodeTableMap.Keys) {
    $tbl = $nodeTableMap[$alias]
    $props = $nodeProps[$alias]
    $pMaps = ($props | ForEach-Object { '{"propertyName":"' + $_ + '","sourceColumn":"' + $_ + '"}' }) -join ','
    $id = $alias + "_" + [guid]::NewGuid().ToString("N").Substring(0,12)
    $ntMappingParts += '{"id":"' + $id + '","nodeTypeAlias":"' + $alias + '","dataSourceName":"' + $tbl + '","propertyMappings":[' + $pMaps + ']}'
}

# Edge table mappings (sourceNodeKeyColumns / destinationNodeKeyColumns)
$edgeDefs = @(
    @{ alias="RefineryHasProcessUnit"; table="dimprocessunit"; srcKeyCols=@("RefineryId"); dstKeyCols=@("ProcessUnitId"); props=@("RefineryId","ProcessUnitId") }
    @{ alias="ProcessUnitHasEquipment"; table="dimequipment"; srcKeyCols=@("ProcessUnitId"); dstKeyCols=@("EquipmentId"); props=@("ProcessUnitId","EquipmentId") }
    @{ alias="PipelineFromProcessUnit"; table="dimpipeline"; srcKeyCols=@("PipelineId"); dstKeyCols=@("FromProcessUnitId"); props=@("PipelineId","FromProcessUnitId") }
    @{ alias="RefineryHasPipeline"; table="dimpipeline"; srcKeyCols=@("RefineryId"); dstKeyCols=@("PipelineId"); props=@("RefineryId","PipelineId") }
    @{ alias="RefineryHasStorageTank"; table="dimstoragetank"; srcKeyCols=@("RefineryId"); dstKeyCols=@("TankId"); props=@("RefineryId","TankId") }
    @{ alias="StorageTankHoldsProduct"; table="dimstoragetank"; srcKeyCols=@("TankId"); dstKeyCols=@("ProductId"); props=@("TankId","ProductId") }
    @{ alias="EquipmentHasSensor"; table="dimsensor"; srcKeyCols=@("EquipmentId"); dstKeyCols=@("SensorId"); props=@("EquipmentId","SensorId") }
    @{ alias="MaintenanceOnEquipment"; table="factmaintenance"; srcKeyCols=@("MaintenanceId"); dstKeyCols=@("EquipmentId"); props=@("MaintenanceId","EquipmentId") }
    @{ alias="MaintenanceByEmployee"; table="factmaintenance"; srcKeyCols=@("MaintenanceId"); dstKeyCols=@("PerformedByEmployeeId"); props=@("MaintenanceId","PerformedByEmployeeId") }
    @{ alias="AlarmFromSensor"; table="factsafetyalarm"; srcKeyCols=@("AlarmId"); dstKeyCols=@("SensorId"); props=@("AlarmId","SensorId") }
    @{ alias="ProductionFromProcessUnit"; table="factproduction"; srcKeyCols=@("ProductionId"); dstKeyCols=@("ProcessUnitId"); props=@("ProductionId","ProcessUnitId") }
    @{ alias="ProductionOfProduct"; table="factproduction"; srcKeyCols=@("ProductionId"); dstKeyCols=@("ProductId"); props=@("ProductionId","ProductId") }
    @{ alias="RefineryHasEmployee"; table="dimemployee"; srcKeyCols=@("RefineryId"); dstKeyCols=@("EmployeeId"); props=@("RefineryId","EmployeeId") }
    @{ alias="CrudeFeedToProcessUnit"; table="bridgecrudeoilprocessunit"; srcKeyCols=@("BridgeId"); dstKeyCols=@("ProcessUnitId"); props=@("BridgeId","ProcessUnitId") }
    @{ alias="CrudeFeedFromCrudeOil"; table="bridgecrudeoilprocessunit"; srcKeyCols=@("BridgeId"); dstKeyCols=@("CrudeOilId"); props=@("BridgeId","CrudeOilId") }
)

$etMappingParts = @()
foreach ($ed in $edgeDefs) {
    $srcColsStr = ($ed.srcKeyCols | ForEach-Object { '"' + $_ + '"' }) -join ','
    $dstColsStr = ($ed.dstKeyCols | ForEach-Object { '"' + $_ + '"' }) -join ','
    $pMaps = ($ed.props | ForEach-Object { '{"propertyName":"' + $_ + '","sourceColumn":"' + $_ + '"}' }) -join ','
    $id = $ed.alias + "_" + [guid]::NewGuid().ToString("N").Substring(0,12)
    $etMappingParts += '{"id":"' + $id + '","edgeTypeAlias":"' + $ed.alias + '","dataSourceName":"' + $ed.table + '","sourceNodeKeyColumns":[' + $srcColsStr + '],"destinationNodeKeyColumns":[' + $dstColsStr + '],"propertyMappings":[' + $pMaps + ']}'
}

$graphDefinitionJson = '{"schemaVersion":"1.0.0","nodeTables":[' + ($ntMappingParts -join ',') + '],"edgeTables":[' + ($etMappingParts -join ',') + ']}'

# ============================================================================
# 4. stylingConfiguration.json
# ============================================================================
$nodeAliases = @("Refinery","ProcessUnit","Equipment","Pipeline","CrudeOil","RefinedProduct","StorageTank","Sensor","Employee","MaintenanceEvent","SafetyAlarm","ProductionRecord","CrudeOilFeed")
$edgeAliases = @("RefineryHasProcessUnit","ProcessUnitHasEquipment","PipelineFromProcessUnit","RefineryHasPipeline","RefineryHasStorageTank","StorageTankHoldsProduct","EquipmentHasSensor","MaintenanceOnEquipment","MaintenanceByEmployee","AlarmFromSensor","ProductionFromProcessUnit","ProductionOfProduct","RefineryHasEmployee","CrudeFeedToProcessUnit","CrudeFeedFromCrudeOil")

$positions = @(); $row = 0; $col = 0
foreach ($a in $nodeAliases) {
    $x = $col * 250; $y = $row * 200
    $positions += '"' + $a + '":{"x":' + $x + ',"y":' + $y + '}'
    $col++; if ($col -ge 4) { $col = 0; $row++ }
}

$styles = @()
foreach ($a in ($nodeAliases + $edgeAliases)) {
    $styles += '"' + $a + '":{"size":30}'
}

$stylingJson = '{"schemaVersion":"1.0.0","modelLayout":{"positions":{' + ($positions -join ',') + '},"styles":{' + ($styles -join ',') + '},"pan":{"x":0,"y":0},"zoomLevel":1}}'

# ============================================================================
# 5. .platform
# ============================================================================
$platformJson = '{"$schema":"https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json","metadata":{"type":"GraphModel","displayName":"OilGasRefineryOntology_graph_e7facc3798ce4696b4ecdd94c5f7babe"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'

# ============================================================================
# DEPLOY via GraphModel-specific endpoint
# ============================================================================

$dsB64 = ToBase64 $dataSourcesJson
$gtB64 = ToBase64 $graphTypeJson
$gdB64 = ToBase64 $graphDefinitionJson
$stB64 = ToBase64 $stylingJson
$plB64 = ToBase64 $platformJson

Write-Host "dataSources.json length: $($dataSourcesJson.Length)"
Write-Host "graphType.json length: $($graphTypeJson.Length)"
Write-Host "graphDefinition.json length: $($graphDefinitionJson.Length)"
Write-Host "stylingConfiguration.json length: $($stylingJson.Length)"

$bodyStr = '{"definition":{"format":"json","parts":[' +
    '{"path":"dataSources.json","payload":"' + $dsB64 + '","payloadType":"InlineBase64"},' +
    '{"path":"graphType.json","payload":"' + $gtB64 + '","payloadType":"InlineBase64"},' +
    '{"path":"graphDefinition.json","payload":"' + $gdB64 + '","payloadType":"InlineBase64"},' +
    '{"path":"stylingConfiguration.json","payload":"' + $stB64 + '","payloadType":"InlineBase64"},' +
    '{"path":".platform","payload":"' + $plB64 + '","payloadType":"InlineBase64"}' +
    ']}}'

Write-Host "Total payload size: $($bodyStr.Length) chars"

$url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/GraphModels/$GraphModelId/updateDefinition?updateMetadata=True"
Write-Host "POST $url"

try {
    $resp = Invoke-WebRequest -Uri $url -Method POST -Headers (@{ Authorization = "Bearer $FabricToken"; "Content-Type" = "application/json" }) -Body $bodyStr -UseBasicParsing
    Write-Host "Status: $($resp.StatusCode)"
    if ($resp.StatusCode -eq 200) {
        Write-Host "Updated immediately!"
    } elseif ($resp.StatusCode -eq 202) {
        $lro = $resp.Headers["Location"]
        Write-Host "LRO: $lro"
        $maxWait = 120; $elapsed = 0
        while ($elapsed -lt $maxWait) {
            Start-Sleep 10; $elapsed += 10
            $poll = Invoke-RestMethod -Uri $lro -Headers @{ Authorization = "Bearer $FabricToken" }
            Write-Host "[$elapsed s] Status: $($poll.status)"
            if ($poll.status -eq "Succeeded") { Write-Host "GraphModel definition deployed successfully!"; break }
            if ($poll.status -eq "Failed") { Write-Host "FAILED: $($poll.error.message)"; break }
        }
    }
} catch {
    $sr = $_.Exception.Response
    if ($sr) {
        $stream = $sr.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $errBody = $reader.ReadToEnd()
        Write-Host "ERROR $([int]$sr.StatusCode): $errBody"
    } else {
        Write-Host "ERROR: $($_.Exception.Message)"
    }
}
