"""Fabric notebook loader for explicitly synthetic Enterprise Finance + HR data."""

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def schema(*fields):
	return StructType([StructField(name, data_type, nullable) for name, data_type, nullable in fields])


STRING = StringType()
INTEGER = IntegerType()
DOUBLE = DoubleType()

TABLE_DEFINITIONS = [
	("DimLegalEntity.csv", "dimlegalentity", schema(("LegalEntityId", STRING, False), ("LegalEntityName", STRING, False), ("CountryCode", STRING, False), ("CurrencyCode", STRING, False))),
	("DimBusinessUnit.csv", "dimbusinessunit", schema(("BusinessUnitId", STRING, False), ("BusinessUnitName", STRING, False), ("LegalEntityId", STRING, False), ("LeaderAlias", STRING, False))),
	("DimCostCenter.csv", "dimcostcenter", schema(("CostCenterId", STRING, False), ("CostCenterName", STRING, False), ("BusinessUnitId", STRING, False), ("DepartmentId", STRING, False))),
	("DimDepartment.csv", "dimdepartment", schema(("DepartmentId", STRING, False), ("DepartmentName", STRING, False), ("BusinessUnitId", STRING, False), ("LocationId", STRING, False))),
	("DimEmployee.csv", "dimemployee", schema(("EmployeeId", STRING, False), ("EmployeeAlias", STRING, False), ("DepartmentId", STRING, False), ("PositionId", STRING, False), ("LocationId", STRING, False), ("EmploymentStatus", STRING, False))),
	("DimPosition.csv", "dimposition", schema(("PositionId", STRING, False), ("PositionTitle", STRING, False), ("JobFamilyId", STRING, False), ("CostCenterId", STRING, False), ("Fte", DOUBLE, False))),
	("DimJobFamily.csv", "dimjobfamily", schema(("JobFamilyId", STRING, False), ("JobFamilyName", STRING, False), ("JobLevel", STRING, False))),
	("DimLocation.csv", "dimlocation", schema(("LocationId", STRING, False), ("LocationName", STRING, False), ("CountryCode", STRING, False), ("Region", STRING, False))),
	("DimFiscalPeriod.csv", "dimfiscalperiod", schema(("FiscalPeriodId", STRING, False), ("FiscalYear", INTEGER, False), ("FiscalQuarter", STRING, False), ("PeriodName", STRING, False), ("PeriodStartDate", STRING, False))),
	("DimAccount.csv", "dimaccount", schema(("AccountId", STRING, False), ("AccountName", STRING, False), ("AccountType", STRING, False))),
	("DimProject.csv", "dimproject", schema(("ProjectId", STRING, False), ("ProjectName", STRING, False), ("CostCenterId", STRING, False), ("ProjectStatus", STRING, False))),
	("FactHeadcountSnapshot.csv", "factheadcountsnapshot", schema(("SnapshotId", STRING, False), ("FiscalPeriodId", STRING, False), ("DepartmentId", STRING, False), ("Headcount", DOUBLE, False), ("Fte", DOUBLE, False), ("OpenPositions", DOUBLE, False))),
	("FactCompensationPlan.csv", "factcompensationplan", schema(("CompensationPlanId", STRING, False), ("FiscalPeriodId", STRING, False), ("CostCenterId", STRING, False), ("PlannedCompensation", DOUBLE, False))),
	("FactWorkforceMovement.csv", "factworkforcemovement", schema(("MovementId", STRING, False), ("FiscalPeriodId", STRING, False), ("DepartmentId", STRING, False), ("MovementType", STRING, False), ("MovementCount", DOUBLE, False))),
	("FactBudgetPlan.csv", "factbudgetplan", schema(("BudgetPlanId", STRING, False), ("FiscalPeriodId", STRING, False), ("CostCenterId", STRING, False), ("AccountId", STRING, False), ("BudgetAmount", DOUBLE, False))),
	("FactActualLedger.csv", "factactualledger", schema(("LedgerEntryId", STRING, False), ("FiscalPeriodId", STRING, False), ("CostCenterId", STRING, False), ("AccountId", STRING, False), ("ActualAmount", DOUBLE, False))),
	("FactForecastScenario.csv", "factforecastscenario", schema(("ForecastId", STRING, False), ("FiscalPeriodId", STRING, False), ("CostCenterId", STRING, False), ("ScenarioName", STRING, False), ("ForecastAmount", DOUBLE, False))),
	("DimCalendarDate.csv", "dimcalendardate", schema(("CalendarDateId", STRING, False), ("CalendarDate", STRING, False), ("FiscalPeriodId", STRING, False), ("DayOfWeek", STRING, False), ("IsWorkingDay", STRING, False))),
	("DimPayGrade.csv", "dimpaygrade", schema(("PayGradeId", STRING, False), ("PayGradeName", STRING, False), ("MinimumSalary", DOUBLE, False), ("MaximumSalary", DOUBLE, False))),
	("DimCompensationComponent.csv", "dimcompensationcomponent", schema(("CompensationComponentId", STRING, False), ("ComponentName", STRING, False), ("ComponentCategory", STRING, False), ("IsRecurring", STRING, False))),
	("FactEmployeeCompensationSnapshot.csv", "factemployeecompensationsnapshot", schema(("CompensationSnapshotId", STRING, False), ("EmployeeId", STRING, False), ("FiscalPeriodId", STRING, False), ("PayGradeId", STRING, False), ("CompensationComponentId", STRING, False), ("AnnualizedAmount", DOUBLE, False), ("Fte", DOUBLE, False))),
	("DimWorkSchedule.csv", "dimworkschedule", schema(("WorkScheduleId", STRING, False), ("ScheduleName", STRING, False), ("ScheduledHoursPerDay", DOUBLE, False), ("ScheduledDaysPerWeek", DOUBLE, False))),
	("DimAttendanceCode.csv", "dimattendancecode", schema(("AttendanceCodeId", STRING, False), ("AttendanceCodeName", STRING, False), ("AttendanceCategory", STRING, False), ("IsApprovedWork", STRING, False))),
	("FactTimeAttendanceDaily.csv", "facttimeattendancedaily", schema(("TimeAttendanceId", STRING, False), ("EmployeeId", STRING, False), ("CalendarDateId", STRING, False), ("WorkScheduleId", STRING, False), ("AttendanceCodeId", STRING, False), ("ScheduledHours", DOUBLE, False), ("WorkedHours", DOUBLE, False), ("ApprovedHours", DOUBLE, False), ("OvertimeHours", DOUBLE, False))),
	("DimLeaveType.csv", "dimleavetype", schema(("LeaveTypeId", STRING, False), ("LeaveTypeName", STRING, False), ("LeaveCategory", STRING, False), ("IsPaid", STRING, False))),
	("DimAbsenceReasonCategory.csv", "dimabsencereasoncategory", schema(("AbsenceReasonCategoryId", STRING, False), ("ReasonCategoryName", STRING, False), ("ReasonGroup", STRING, False))),
	("FactAbsenceEpisode.csv", "factabsenceepisode", schema(("AbsenceEpisodeId", STRING, False), ("EmployeeId", STRING, False), ("LeaveTypeId", STRING, False), ("AbsenceReasonCategoryId", STRING, False), ("StartCalendarDateId", STRING, False), ("EndCalendarDateId", STRING, False), ("AbsenceHours", DOUBLE, False))),
	("DimRecruitmentStage.csv", "dimrecruitmentstage", schema(("RecruitmentStageId", STRING, False), ("StageName", STRING, False), ("StageSequence", INTEGER, False), ("IsTerminal", STRING, False))),
	("DimRecruitmentSource.csv", "dimrecruitmentsource", schema(("RecruitmentSourceId", STRING, False), ("SourceName", STRING, False), ("SourceCategory", STRING, False))),
	("DimCandidate.csv", "dimcandidate", schema(("CandidateId", STRING, False), ("CandidateAlias", STRING, False), ("RecruitmentSourceId", STRING, False), ("CandidateStatus", STRING, False))),
	("FactJobRequisition.csv", "factjobrequisition", schema(("JobRequisitionId", STRING, False), ("PositionId", STRING, False), ("DepartmentId", STRING, False), ("CostCenterId", STRING, False), ("OpenCalendarDateId", STRING, False), ("TargetFillCalendarDateId", STRING, False), ("RequisitionStatus", STRING, False), ("Openings", DOUBLE, False))),
	("FactRecruitmentStageEvent.csv", "factrecruitmentstageevent", schema(("RecruitmentStageEventId", STRING, False), ("CandidateId", STRING, False), ("JobRequisitionId", STRING, False), ("RecruitmentStageId", STRING, False), ("CalendarDateId", STRING, False))),
	("FactOffer.csv", "factoffer", schema(("OfferId", STRING, False), ("CandidateId", STRING, False), ("JobRequisitionId", STRING, False), ("OfferCalendarDateId", STRING, False), ("OfferStatus", STRING, False), ("OfferedAnnualAmount", DOUBLE, False))),
	("SyntheticDataMetadata.csv", "syntheticdatametadata", schema(("MetadataId", STRING, False), ("DatasetName", STRING, False), ("GenerationMethod", STRING, False), ("PrivacyClassification", STRING, False), ("GeneratedOn", STRING, False))),
	("SensorTelemetry.csv", "sensortelemetry", schema(("TelemetryId", STRING, False), ("FiscalPeriodId", STRING, False), ("CostCenterId", STRING, False), ("MetricName", STRING, False), ("MetricValue", DOUBLE, False), ("IsAnomaly", STRING, False))),
]

for source_file, table_name, table_schema in TABLE_DEFINITIONS:
	(
		spark.read.option("header", True).schema(table_schema).csv("Files/{}".format(source_file))
		.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
		.saveAsTable(table_name)
	)

print("Loaded explicitly synthetic Enterprise Finance + HR data; no actual PII is present.")