"""Aggregate-only quality gate for the Enterprise Finance + HR demo pipeline.

The Fabric notebook wrapper assigns EnterpriseFinanceHRLH as its default Lakehouse.
"""

from pyspark.sql import functions as F

REQUIRED_TABLES = [
    "factbudgetplan",
    "factactualledger",
    "factheadcountsnapshot",
    "factemployeecompensationsnapshot",
    "facttimeattendancedaily",
    "factabsenceepisode",
    "factjobrequisition",
    "factoffer",
]

for table_name in REQUIRED_TABLES:
    row_count = spark.table(table_name).count()
    if row_count == 0:
        raise ValueError("Required table is empty: {}".format(table_name))

invalid_compensation = (
    spark.table("factemployeecompensationsnapshot")
    .where(F.col("AnnualizedAmount") < 0)
    .count()
)
if invalid_compensation:
    raise ValueError("Compensation quality gate failed: negative annualized amounts.")

invalid_attendance = (
    spark.table("facttimeattendancedaily")
    .where((F.col("WorkedHours") < 0) | (F.col("ApprovedHours") < 0) | (F.col("OvertimeHours") < 0))
    .count()
)
if invalid_attendance:
    raise ValueError("Attendance quality gate failed: negative hours.")

small_absence_groups = (
    spark.table("factabsenceepisode")
    .groupBy("LeaveTypeId", "AbsenceReasonCategoryId")
    .count()
    .where(F.col("count") < 5)
    .count()
)

print("Enterprise Finance + HR quality gate passed.")
print("Small absence groups requiring suppression: {}".format(small_absence_groups))