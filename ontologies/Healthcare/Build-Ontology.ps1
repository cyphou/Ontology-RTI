# Build-Ontology-Healthcare.ps1
# Builds the Healthcare Ontology definition for Microsoft Fabric
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
    id = "1001"; name = "Hospital"; entityIdParts = @("2001"); displayNamePropertyId = "2002"
    properties = @(
        @{ id = "2001"; name = "HospitalId"; valueType = "String" },
        @{ id = "2002"; name = "HospitalName"; valueType = "String" },
        @{ id = "2003"; name = "City"; valueType = "String" },
        @{ id = "2004"; name = "State"; valueType = "String" },
        @{ id = "2005"; name = "Country"; valueType = "String" },
        @{ id = "2006"; name = "BedCapacity"; valueType = "Double" },
        @{ id = "2007"; name = "TierLevel"; valueType = "String" },
        @{ id = "2008"; name = "Status"; valueType = "String" }
    )
    tableName = "dimhospital"
}

$entityTypes += @{
    id = "1002"; name = "Department"; entityIdParts = @("2101"); displayNamePropertyId = "2102"
    properties = @(
        @{ id = "2101"; name = "DepartmentId"; valueType = "String" },
        @{ id = "2102"; name = "DepartmentName"; valueType = "String" },
        @{ id = "2103"; name = "HospitalId"; valueType = "String" },
        @{ id = "2104"; name = "DepartmentType"; valueType = "String" },
        @{ id = "2105"; name = "Floor"; valueType = "Double" },
        @{ id = "2106"; name = "BedCount"; valueType = "Double" },
        @{ id = "2107"; name = "Status"; valueType = "String" }
    )
    tableName = "dimdepartment"
}

$entityTypes += @{
    id = "1003"; name = "Ward"; entityIdParts = @("2201"); displayNamePropertyId = "2202"
    properties = @(
        @{ id = "2201"; name = "WardId"; valueType = "String" },
        @{ id = "2202"; name = "WardName"; valueType = "String" },
        @{ id = "2203"; name = "DepartmentId"; valueType = "String" },
        @{ id = "2204"; name = "WardType"; valueType = "String" },
        @{ id = "2205"; name = "BedCount"; valueType = "Double" },
        @{ id = "2206"; name = "NurseStations"; valueType = "Double" },
        @{ id = "2207"; name = "Status"; valueType = "String" }
    )
    tableName = "dimward"
}

$entityTypes += @{
    id = "1004"; name = "Physician"; entityIdParts = @("2301"); displayNamePropertyId = "2302"
    properties = @(
        @{ id = "2301"; name = "PhysicianId"; valueType = "String" },
        @{ id = "2302"; name = "PhysicianName"; valueType = "String" },
        @{ id = "2303"; name = "DepartmentId"; valueType = "String" },
        @{ id = "2304"; name = "Specialty"; valueType = "String" },
        @{ id = "2305"; name = "LicenseNumber"; valueType = "String" },
        @{ id = "2306"; name = "YearsExperience"; valueType = "Double" },
        @{ id = "2307"; name = "Status"; valueType = "String" }
    )
    tableName = "dimphysician"
}

$entityTypes += @{
    id = "1005"; name = "Nurse"; entityIdParts = @("2401"); displayNamePropertyId = "2402"
    properties = @(
        @{ id = "2401"; name = "NurseId"; valueType = "String" },
        @{ id = "2402"; name = "NurseName"; valueType = "String" },
        @{ id = "2403"; name = "WardId"; valueType = "String" },
        @{ id = "2404"; name = "Certification"; valueType = "String" },
        @{ id = "2405"; name = "ShiftPreference"; valueType = "String" },
        @{ id = "2406"; name = "YearsExperience"; valueType = "Double" },
        @{ id = "2407"; name = "Status"; valueType = "String" }
    )
    tableName = "dimnurse"
}

$entityTypes += @{
    id = "1006"; name = "Patient"; entityIdParts = @("2501"); displayNamePropertyId = "2502"
    properties = @(
        @{ id = "2501"; name = "PatientId"; valueType = "String" },
        @{ id = "2502"; name = "PatientName"; valueType = "String" },
        @{ id = "2503"; name = "WardId"; valueType = "String" },
        @{ id = "2504"; name = "DateOfBirth"; valueType = "String" },
        @{ id = "2505"; name = "Gender"; valueType = "String" },
        @{ id = "2506"; name = "BloodType"; valueType = "String" },
        @{ id = "2507"; name = "InsuranceProvider"; valueType = "String" },
        @{ id = "2508"; name = "AdmissionDate"; valueType = "String" },
        @{ id = "2509"; name = "Status"; valueType = "String" }
    )
    tableName = "dimpatient"
}

$entityTypes += @{
    id = "1007"; name = "MedicalDevice"; entityIdParts = @("2601"); displayNamePropertyId = "2602"
    properties = @(
        @{ id = "2601"; name = "DeviceId"; valueType = "String" },
        @{ id = "2602"; name = "DeviceName"; valueType = "String" },
        @{ id = "2603"; name = "WardId"; valueType = "String" },
        @{ id = "2604"; name = "DeviceType"; valueType = "String" },
        @{ id = "2605"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2606"; name = "Model"; valueType = "String" },
        @{ id = "2607"; name = "LastCalibrationDate"; valueType = "String" },
        @{ id = "2608"; name = "Status"; valueType = "String" }
    )
    tableName = "dimmedicaldevice"
}

$entityTypes += @{
    id = "1008"; name = "Medication"; entityIdParts = @("2701"); displayNamePropertyId = "2702"
    properties = @(
        @{ id = "2701"; name = "MedicationId"; valueType = "String" },
        @{ id = "2702"; name = "MedicationName"; valueType = "String" },
        @{ id = "2703"; name = "Category"; valueType = "String" },
        @{ id = "2704"; name = "DosageForm"; valueType = "String" },
        @{ id = "2705"; name = "Manufacturer"; valueType = "String" },
        @{ id = "2706"; name = "UnitCost"; valueType = "Double" },
        @{ id = "2707"; name = "RequiresRefrigeration"; valueType = "String" },
        @{ id = "2708"; name = "Status"; valueType = "String" }
    )
    tableName = "dimmedication"
}

$entityTypes += @{
    id = "1009"; name = "Sensor"; entityIdParts = @("2801"); displayNamePropertyId = "2802"
    properties = @(
        @{ id = "2801"; name = "SensorId"; valueType = "String" },
        @{ id = "2802"; name = "SensorName"; valueType = "String" },
        @{ id = "2803"; name = "DeviceId"; valueType = "String" },
        @{ id = "2804"; name = "SensorType"; valueType = "String" },
        @{ id = "2805"; name = "Unit"; valueType = "String" },
        @{ id = "2806"; name = "MinThreshold"; valueType = "Double" },
        @{ id = "2807"; name = "MaxThreshold"; valueType = "Double" },
        @{ id = "2808"; name = "Status"; valueType = "String" }
    )
    tableName = "dimsensor"
    timeseriesTable = "PatientVitals"
    timeseriesProperties = @(
        @{ id = "4001"; name = "Timestamp"; valueType = "DateTime" },
        @{ id = "4002"; name = "HeartRateBPM"; valueType = "Double" },
        @{ id = "4003"; name = "BloodPressureSystolic"; valueType = "Double" },
        @{ id = "4004"; name = "OxygenSaturation"; valueType = "Double" },
        @{ id = "4005"; name = "TemperatureC"; valueType = "Double" }
    )
    timestampColumn = "Timestamp"
}

# --- FactLabResult (ID: 1010) ---
$entityTypes += @{
    id = "1010"; name = "LabResult"; entityIdParts = @("2901"); displayNamePropertyId = "2904"
    properties = @(
        @{ id = "2901"; name = "LabResultId"; valueType = "String" },
        @{ id = "2902"; name = "PatientId"; valueType = "String" },
        @{ id = "2903"; name = "PhysicianId"; valueType = "String" },
        @{ id = "2904"; name = "TestType"; valueType = "String" },
        @{ id = "2905"; name = "TestDate"; valueType = "String" },
        @{ id = "2906"; name = "ResultValue"; valueType = "Double" },
        @{ id = "2907"; name = "Unit"; valueType = "String" },
        @{ id = "2908"; name = "ReferenceRange"; valueType = "String" },
        @{ id = "2909"; name = "Interpretation"; valueType = "String" },
        @{ id = "2910"; name = "Status"; valueType = "String" }
    )
    tableName = "factlabresult"
}

# --- FactMedicationAdmin (ID: 1011) ---
$entityTypes += @{
    id = "1011"; name = "MedicationAdmin"; entityIdParts = @("5001"); displayNamePropertyId = "5001"
    properties = @(
        @{ id = "5001"; name = "AdminId"; valueType = "String" },
        @{ id = "5002"; name = "PatientId"; valueType = "String" },
        @{ id = "5003"; name = "MedicationId"; valueType = "String" },
        @{ id = "5004"; name = "NurseId"; valueType = "String" },
        @{ id = "5005"; name = "AdminDate"; valueType = "String" },
        @{ id = "5006"; name = "Dosage"; valueType = "String" },
        @{ id = "5007"; name = "Route"; valueType = "String" },
        @{ id = "5008"; name = "Status"; valueType = "String" }
    )
    tableName = "factmedicationadmin"
}

# --- FactProcedure (ID: 1012) ---
$entityTypes += @{
    id = "1012"; name = "Procedure"; entityIdParts = @("5101"); displayNamePropertyId = "5104"
    properties = @(
        @{ id = "5101"; name = "ProcedureId"; valueType = "String" },
        @{ id = "5102"; name = "PatientId"; valueType = "String" },
        @{ id = "5103"; name = "PhysicianId"; valueType = "String" },
        @{ id = "5104"; name = "ProcedureType"; valueType = "String" },
        @{ id = "5105"; name = "ProcedureDate"; valueType = "String" },
        @{ id = "5106"; name = "DurationMinutes"; valueType = "Double" },
        @{ id = "5107"; name = "Outcome"; valueType = "String" },
        @{ id = "5108"; name = "Room"; valueType = "String" },
        @{ id = "5109"; name = "Status"; valueType = "String" }
    )
    tableName = "factprocedure"
}

$relationships = @(
    @{ id = "3001"; name = "HospitalHasDepartment"; sourceId = "1001"; targetId = "1002" },
    @{ id = "3002"; name = "DepartmentHasWard"; sourceId = "1002"; targetId = "1003" },
    @{ id = "3003"; name = "DepartmentHasPhysician"; sourceId = "1002"; targetId = "1004" },
    @{ id = "3004"; name = "WardHasNurse"; sourceId = "1003"; targetId = "1005" },
    @{ id = "3005"; name = "WardHasPatient"; sourceId = "1003"; targetId = "1006" },
    @{ id = "3006"; name = "WardHasDevice"; sourceId = "1003"; targetId = "1007" },
    @{ id = "3007"; name = "DeviceHasSensor"; sourceId = "1007"; targetId = "1009" },
    @{ id = "3008"; name = "PatientHasLabResult"; sourceId = "1006"; targetId = "1010" },
    @{ id = "3009"; name = "PatientHasMedicationAdmin"; sourceId = "1006"; targetId = "1011" },
    @{ id = "3010"; name = "PatientHasProcedure"; sourceId = "1006"; targetId = "1012" },
    @{ id = "3011"; name = "PhysicianOrdersLabResult"; sourceId = "1004"; targetId = "1010" },
    @{ id = "3012"; name = "PhysicianPerformsProcedure"; sourceId = "1004"; targetId = "1012" },
    @{ id = "3013"; name = "MedicationAdminUsesMedication"; sourceId = "1011"; targetId = "1008" },
    @{ id = "3014"; name = "NurseAdministersMedicationAdmin"; sourceId = "1005"; targetId = "1011" }
)

# BUILD PARTS (reusable pattern)
$parts = @()
$platform = '{"metadata":{"type":"Ontology","displayName":"HealthcareOntology","description":"Healthcare Ontology - hospitals, departments, wards, patients, physicians, medical devices, medications, and clinical sensors"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'
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
    $sourceEntity = $entityTypes | Where-Object { $_.id -eq $rel.sourceId }
    $targetEntity = $entityTypes | Where-Object { $_.id -eq $rel.targetId }
    $sourcePkPropId = $sourceEntity.entityIdParts[0]; $sourcePkName = ($sourceEntity.properties | Where-Object { $_.id -eq $sourcePkPropId }).name
    $targetPkPropId = $targetEntity.entityIdParts[0]; $targetPkName = ($targetEntity.properties | Where-Object { $_.id -eq $targetPkPropId }).name
    $fkProp = $sourceEntity.properties | Where-Object { $_.name -eq $targetPkName }
    if (-not $fkProp) { $fkProp = $sourceEntity.properties | Where-Object { $_.name -like "*$targetPkName" } | Select-Object -First 1 }
    if ($fkProp) {
        $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
        $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $sourceEntity.tableName + '","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $sourcePkName + '","targetPropertyId":"' + $sourcePkPropId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $fkProp.name + '","targetPropertyId":"' + $targetPkPropId + '"}]}'
        $parts += @{ path = "RelationshipTypes/$($rel.id)/Contextualizations/$ctxGuid.json"; payload = (ToBase64 $ctxJson); payloadType = "InlineBase64" }
    } else {
        $fkPropInTarget = $targetEntity.properties | Where-Object { $_.name -eq $sourcePkName }
        if (-not $fkPropInTarget) { $fkPropInTarget = $targetEntity.properties | Where-Object { $_.name -like "*$sourcePkName" } | Select-Object -First 1 }
        if ($fkPropInTarget) {
            $ctxGuid = DeterministicGuid "Ctx-$($rel.id)"
            $ctxJson = '{"id":"' + $ctxGuid + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $targetEntity.tableName + '","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $fkPropInTarget.name + '","targetPropertyId":"' + $sourcePkPropId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $targetPkName + '","targetPropertyId":"' + $targetPkPropId + '"}]}'
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
