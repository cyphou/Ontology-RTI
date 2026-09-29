<# Builds the Enterprise Finance + HR synthetic planning ontology for Fabric. #>
param(
    [string]$WorkspaceId,
    [string]$LakehouseId,
    [string]$KqlDatabaseId,
    [string]$KqlClusterUri,
    [string]$KqlDatabaseName,
    [string]$OntologyId,
    [string]$FabricToken
)

$ErrorActionPreference = 'Stop'
function ToBase64([string]$Text) { [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Text)) }
function DeterministicGuid([string]$Seed) {
    $hash = [Security.Cryptography.MD5]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($Seed))
    ([guid]::new($hash)).ToString()
}
function PropertyType([string]$Name) {
    if ($Name -match 'Id$') { return 'String' }
    if ($Name -match 'FiscalYear') { return 'BigInt' }
    if ($Name -match 'Amount|Compensation|Headcount|Fte|Count|Positions|MetricValue') { return 'Double' }
    return 'String'
}

$definitions = @(
    @('LegalEntity', 'dimlegalentity', 'LegalEntityId,LegalEntityName,CountryCode,CurrencyCode'),
    @('BusinessUnit', 'dimbusinessunit', 'BusinessUnitId,BusinessUnitName,LegalEntityId,LeaderAlias'),
    @('CostCenter', 'dimcostcenter', 'CostCenterId,CostCenterName,BusinessUnitId,DepartmentId'),
    @('Department', 'dimdepartment', 'DepartmentId,DepartmentName,BusinessUnitId,LocationId'),
    @('Employee', 'dimemployee', 'EmployeeId,EmployeeAlias,DepartmentId,PositionId,LocationId,EmploymentStatus'),
    @('Position', 'dimposition', 'PositionId,PositionTitle,JobFamilyId,CostCenterId,Fte'),
    @('JobFamily', 'dimjobfamily', 'JobFamilyId,JobFamilyName,JobLevel'),
    @('Location', 'dimlocation', 'LocationId,LocationName,CountryCode,Region'),
    @('FiscalPeriod', 'dimfiscalperiod', 'FiscalPeriodId,FiscalYear,FiscalQuarter,PeriodName,PeriodStartDate'),
    @('Account', 'dimaccount', 'AccountId,AccountName,AccountType'),
    @('Project', 'dimproject', 'ProjectId,ProjectName,CostCenterId,ProjectStatus'),
    @('HeadcountSnapshot', 'factheadcountsnapshot', 'SnapshotId,FiscalPeriodId,DepartmentId,Headcount,Fte,OpenPositions'),
    @('CompensationPlan', 'factcompensationplan', 'CompensationPlanId,FiscalPeriodId,CostCenterId,PlannedCompensation'),
    @('WorkforceMovement', 'factworkforcemovement', 'MovementId,FiscalPeriodId,DepartmentId,MovementType,MovementCount'),
    @('BudgetPlan', 'factbudgetplan', 'BudgetPlanId,FiscalPeriodId,CostCenterId,AccountId,BudgetAmount'),
    @('ActualLedger', 'factactualledger', 'LedgerEntryId,FiscalPeriodId,CostCenterId,AccountId,ActualAmount'),
    @('ForecastScenario', 'factforecastscenario', 'ForecastId,FiscalPeriodId,CostCenterId,ScenarioName,ForecastAmount'),
    @('CalendarDate', 'dimcalendardate', 'CalendarDateId,CalendarDate,FiscalPeriodId,DayOfWeek,IsWorkingDay'),
    @('PayGrade', 'dimpaygrade', 'PayGradeId,PayGradeName,MinimumSalary,MaximumSalary'),
    @('CompensationComponent', 'dimcompensationcomponent', 'CompensationComponentId,ComponentName,ComponentCategory,IsRecurring'),
    @('EmployeeCompensationSnapshot', 'factemployeecompensationsnapshot', 'CompensationSnapshotId,EmployeeId,FiscalPeriodId,PayGradeId,CompensationComponentId,AnnualizedAmount,Fte'),
    @('WorkSchedule', 'dimworkschedule', 'WorkScheduleId,ScheduleName,ScheduledHoursPerDay,ScheduledDaysPerWeek'),
    @('AttendanceCode', 'dimattendancecode', 'AttendanceCodeId,AttendanceCodeName,AttendanceCategory,IsApprovedWork'),
    @('TimeAttendanceDaily', 'facttimeattendancedaily', 'TimeAttendanceId,EmployeeId,CalendarDateId,WorkScheduleId,AttendanceCodeId,ScheduledHours,WorkedHours,ApprovedHours,OvertimeHours'),
    @('LeaveType', 'dimleavetype', 'LeaveTypeId,LeaveTypeName,LeaveCategory,IsPaid'),
    @('AbsenceReasonCategory', 'dimabsencereasoncategory', 'AbsenceReasonCategoryId,ReasonCategoryName,ReasonGroup'),
    @('AbsenceEpisode', 'factabsenceepisode', 'AbsenceEpisodeId,EmployeeId,LeaveTypeId,AbsenceReasonCategoryId,StartCalendarDateId,EndCalendarDateId,AbsenceHours'),
    @('RecruitmentStage', 'dimrecruitmentstage', 'RecruitmentStageId,StageName,StageSequence,IsTerminal'),
    @('RecruitmentSource', 'dimrecruitmentsource', 'RecruitmentSourceId,SourceName,SourceCategory'),
    @('Candidate', 'dimcandidate', 'CandidateId,CandidateAlias,RecruitmentSourceId,CandidateStatus'),
    @('JobRequisition', 'factjobrequisition', 'JobRequisitionId,PositionId,DepartmentId,CostCenterId,OpenCalendarDateId,TargetFillCalendarDateId,RequisitionStatus,Openings'),
    @('RecruitmentStageEvent', 'factrecruitmentstageevent', 'RecruitmentStageEventId,CandidateId,JobRequisitionId,RecruitmentStageId,CalendarDateId'),
    @('Offer', 'factoffer', 'OfferId,CandidateId,JobRequisitionId,OfferCalendarDateId,OfferStatus,OfferedAnnualAmount')
)

$entityTypes = @()
for ($index = 0; $index -lt $definitions.Count; $index++) {
    $definition = $definitions[$index]
    $basePropertyId = 2001 + ($index * 20)
    $properties = @()
    $propertyNames = $definition[2].Split(',')
    for ($propertyIndex = 0; $propertyIndex -lt $propertyNames.Count; $propertyIndex++) {
        $properties += @{ id = [string]($basePropertyId + $propertyIndex); name = $propertyNames[$propertyIndex]; valueType = PropertyType $propertyNames[$propertyIndex] }
    }
    $entityTypes += @{ id = [string](1001 + $index); name = $definition[0]; tableName = $definition[1]; entityIdParts = @([string]$basePropertyId); displayNamePropertyId = [string]($basePropertyId + [Math]::Min(1, $propertyNames.Count - 1)); properties = $properties }
}

$relationshipDefinitions = @(
    @('LegalEntityHasBusinessUnit', 1001, 1002), @('BusinessUnitHasDepartment', 1002, 1004), @('BusinessUnitHasCostCenter', 1002, 1003), @('DepartmentHasEmployee', 1004, 1005), @('DepartmentOwnsCostCenter', 1004, 1003), @('EmployeeFillsPosition', 1005, 1006), @('PositionInJobFamily', 1006, 1007), @('PositionChargedToCostCenter', 1006, 1003), @('DepartmentLocatedAt', 1004, 1008), @('CostCenterFundsProject', 1003, 1011), @('HeadcountForPeriod', 1012, 1009), @('HeadcountForDepartment', 1012, 1004), @('CompensationForPeriod', 1013, 1009), @('CompensationForCostCenter', 1013, 1003), @('MovementForPeriod', 1014, 1009), @('MovementForDepartment', 1014, 1004), @('BudgetForPeriod', 1015, 1009), @('BudgetForCostCenter', 1015, 1003), @('BudgetForAccount', 1015, 1010), @('ActualForPeriod', 1016, 1009), @('ActualForCostCenter', 1016, 1003), @('ActualForAccount', 1016, 1010), @('ForecastForPeriod', 1017, 1009), @('ForecastForCostCenter', 1017, 1003),
    @('CalendarDateInPeriod', 1018, 1009), @('EmployeeCompensationForEmployee', 1021, 1005), @('EmployeeCompensationForPeriod', 1021, 1009), @('EmployeeCompensationAtPayGrade', 1021, 1019), @('EmployeeCompensationHasComponent', 1021, 1020),
    @('TimeAttendanceForEmployee', 1024, 1005), @('TimeAttendanceOnDate', 1024, 1018), @('TimeAttendanceUsesSchedule', 1024, 1022), @('TimeAttendanceUsesCode', 1024, 1023),
    @('AbsenceForEmployee', 1027, 1005), @('AbsenceHasLeaveType', 1027, 1025), @('AbsenceHasReason', 1027, 1026), @('AbsenceStartsOnDate', 1027, 1018), @('AbsenceEndsOnDate', 1027, 1018),
    @('CandidateFromSource', 1030, 1029), @('RecruitmentStageEventForCandidate', 1032, 1030), @('RecruitmentStageEventForRequisition', 1032, 1031), @('RecruitmentStageEventAtStage', 1032, 1028), @('RecruitmentStageEventOnDate', 1032, 1018),
    @('RequisitionForPosition', 1031, 1006), @('RequisitionForDepartment', 1031, 1004), @('RequisitionForCostCenter', 1031, 1003), @('RequisitionOpenedOnDate', 1031, 1018), @('RequisitionTargetFillDate', 1031, 1018),
    @('OfferForCandidate', 1033, 1030), @('OfferForRequisition', 1033, 1031), @('OfferOnDate', 1033, 1018)
)
$relationships = @()
for ($index = 0; $index -lt $relationshipDefinitions.Count; $index++) {
    $relationship = $relationshipDefinitions[$index]
    $relationships += @{ id = [string](3001 + $index); name = $relationship[0]; sourceId = [string]$relationship[1]; targetId = [string]$relationship[2] }
}

$parts = @()
$platform = '{"metadata":{"type":"Ontology","displayName":"EnterpriseFinanceHROntology","description":"Explicitly synthetic enterprise finance and aggregate workforce planning ontology"},"config":{"version":"2.0","logicalId":"00000000-0000-0000-0000-000000000000"}}'
$parts += @{ path = '.platform'; payload = ToBase64 $platform; payloadType = 'InlineBase64' }
$parts += @{ path = 'definition.json'; payload = ToBase64 '{}'; payloadType = 'InlineBase64' }
foreach ($entity in $entityTypes) {
    $propertiesJson = ($entity.properties | ForEach-Object { '{"id":"' + $_.id + '","name":"' + $_.name + '","redefines":null,"baseTypeNamespaceType":null,"valueType":"' + $_.valueType + '"}' }) -join ','
    $idPartsJson = '[' + (($entity.entityIdParts | ForEach-Object { '"' + $_ + '"' }) -join ',') + ']'
    $entityJson = '{"id":"' + $entity.id + '","namespace":"usertypes","baseEntityTypeId":null,"name":"' + $entity.name + '","entityIdParts":' + $idPartsJson + ',"displayNamePropertyId":"' + $entity.displayNamePropertyId + '","namespaceType":"Custom","visibility":"Visible","properties":[' + $propertiesJson + '],"timeseriesProperties":[]}'
    $parts += @{ path = "EntityTypes/$($entity.id)/definition.json"; payload = ToBase64 $entityJson; payloadType = 'InlineBase64' }
    $bindingId = DeterministicGuid "EnterpriseFinanceHR-NonTimeSeries-$($entity.id)"
    $propertyBindings = ($entity.properties | ForEach-Object { '{"sourceColumnName":"' + $_.name + '","targetPropertyId":"' + $_.id + '"}' }) -join ','
    $bindingJson = '{"id":"' + $bindingId + '","dataBindingConfiguration":{"dataBindingType":"NonTimeSeries","propertyBindings":[' + $propertyBindings + '],"sourceTableProperties":{"sourceType":"LakehouseTable","workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $entity.tableName + '"}}}'
    $parts += @{ path = "EntityTypes/$($entity.id)/DataBindings/$bindingId.json"; payload = ToBase64 $bindingJson; payloadType = 'InlineBase64' }
}
foreach ($relationship in $relationships) {
    $relationshipJson = '{"namespace":"usertypes","id":"' + $relationship.id + '","name":"' + $relationship.name + '","namespaceType":"Custom","source":{"entityTypeId":"' + $relationship.sourceId + '"},"target":{"entityTypeId":"' + $relationship.targetId + '"}}'
    $parts += @{ path = "RelationshipTypes/$($relationship.id)/definition.json"; payload = ToBase64 $relationshipJson; payloadType = 'InlineBase64' }

    $sourceEntity = $entityTypes | Where-Object { $_.id -eq $relationship.sourceId }
    $targetEntity = $entityTypes | Where-Object { $_.id -eq $relationship.targetId }
    $sourceKeyId = $sourceEntity.entityIdParts[0]
    $targetKeyId = $targetEntity.entityIdParts[0]
    $sourceKeyName = ($sourceEntity.properties | Where-Object { $_.id -eq $sourceKeyId }).name
    $targetKeyName = ($targetEntity.properties | Where-Object { $_.id -eq $targetKeyId }).name
    $foreignKey = $sourceEntity.properties | Where-Object { $_.name -eq $targetKeyName }
    if ($foreignKey) {
        $contextId = DeterministicGuid "EnterpriseFinanceHR-Context-$($relationship.id)"
        $contextJson = '{"id":"' + $contextId + '","dataBindingTable":{"workspaceId":"' + $WorkspaceId + '","itemId":"' + $LakehouseId + '","sourceTableName":"' + $sourceEntity.tableName + '","sourceType":"LakehouseTable"},"sourceKeyRefBindings":[{"sourceColumnName":"' + $sourceKeyName + '","targetPropertyId":"' + $sourceKeyId + '"}],"targetKeyRefBindings":[{"sourceColumnName":"' + $foreignKey.name + '","targetPropertyId":"' + $targetKeyId + '"}]}'
        $parts += @{ path = "RelationshipTypes/$($relationship.id)/Contextualizations/$contextId.json"; payload = ToBase64 $contextJson; payloadType = 'InlineBase64' }
    }
}
if (-not $WorkspaceId -or -not $LakehouseId -or -not $OntologyId -or -not $FabricToken) { throw 'WorkspaceId, LakehouseId, OntologyId, and FabricToken are required to deploy the ontology definition.' }
$partsJson = ($parts | ForEach-Object { '{"path":"' + $_.path + '","payload":"' + $_.payload + '","payloadType":"InlineBase64"}' }) -join ','
$headers = @{ Authorization = "Bearer $FabricToken"; 'Content-Type' = 'application/json' }
$response = Invoke-WebRequest -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$OntologyId/updateDefinition" -Method Post -Headers $headers -Body ('{"definition":{"parts":[' + $partsJson + ']}}') -UseBasicParsing
if ($response.StatusCode -notin @(200, 202)) { throw "Unexpected ontology update status: $($response.StatusCode)" }
Write-Host "EnterpriseFinanceHR ontology updated: $($entityTypes.Count) entities, $($relationships.Count) relationships." -ForegroundColor Green