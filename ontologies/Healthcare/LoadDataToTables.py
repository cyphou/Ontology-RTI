# Fabric Notebook: Load Healthcare CSV Data into Delta Tables
# Auto-generated for the Healthcare domain.
# Reads CSV files from the lakehouse Files/ folder and writes typed Delta tables.

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ============================================================================
# Table definitions: (csv_filename, table_name, schema)
# ============================================================================

TABLE_DEFINITIONS = [
    (
        "DimHospital.csv", "dimhospital",
        StructType([
            StructField("HospitalId", StringType(), False),
            StructField("HospitalName", StringType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("BedCapacity", DoubleType(), True),
            StructField("TierLevel", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimDepartment.csv", "dimdepartment",
        StructType([
            StructField("DepartmentId", StringType(), False),
            StructField("DepartmentName", StringType(), True),
            StructField("HospitalId", StringType(), True),
            StructField("DepartmentType", StringType(), True),
            StructField("Floor", DoubleType(), True),
            StructField("BedCount", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimWard.csv", "dimward",
        StructType([
            StructField("WardId", StringType(), False),
            StructField("WardName", StringType(), True),
            StructField("DepartmentId", StringType(), True),
            StructField("WardType", StringType(), True),
            StructField("BedCount", DoubleType(), True),
            StructField("NurseStations", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimPhysician.csv", "dimphysician",
        StructType([
            StructField("PhysicianId", StringType(), False),
            StructField("PhysicianName", StringType(), True),
            StructField("DepartmentId", StringType(), True),
            StructField("Specialty", StringType(), True),
            StructField("LicenseNumber", StringType(), True),
            StructField("YearsExperience", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimNurse.csv", "dimnurse",
        StructType([
            StructField("NurseId", StringType(), False),
            StructField("NurseName", StringType(), True),
            StructField("WardId", StringType(), True),
            StructField("Certification", StringType(), True),
            StructField("ShiftPreference", StringType(), True),
            StructField("YearsExperience", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimPatient.csv", "dimpatient",
        StructType([
            StructField("PatientId", StringType(), False),
            StructField("PatientName", StringType(), True),
            StructField("WardId", StringType(), True),
            StructField("DateOfBirth", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("BloodType", StringType(), True),
            StructField("InsuranceProvider", StringType(), True),
            StructField("AdmissionDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimMedicalDevice.csv", "dimmedicaldevice",
        StructType([
            StructField("DeviceId", StringType(), False),
            StructField("DeviceName", StringType(), True),
            StructField("WardId", StringType(), True),
            StructField("DeviceType", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("LastCalibrationDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimMedication.csv", "dimmedication",
        StructType([
            StructField("MedicationId", StringType(), False),
            StructField("MedicationName", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("DosageForm", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("UnitCost", DoubleType(), True),
            StructField("RequiresRefrigeration", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimSensor.csv", "dimsensor",
        StructType([
            StructField("SensorId", StringType(), False),
            StructField("SensorName", StringType(), True),
            StructField("DeviceId", StringType(), True),
            StructField("SensorType", StringType(), True),
            StructField("Unit", StringType(), True),
            StructField("MinThreshold", DoubleType(), True),
            StructField("MaxThreshold", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactLabResult.csv", "factlabresult",
        StructType([
            StructField("LabResultId", StringType(), False),
            StructField("PatientId", StringType(), True),
            StructField("PhysicianId", StringType(), True),
            StructField("TestType", StringType(), True),
            StructField("TestDate", StringType(), True),
            StructField("ResultValue", DoubleType(), True),
            StructField("Unit", StringType(), True),
            StructField("ReferenceRange", StringType(), True),
            StructField("Interpretation", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactProcedure.csv", "factprocedure",
        StructType([
            StructField("ProcedureId", StringType(), False),
            StructField("PatientId", StringType(), True),
            StructField("PhysicianId", StringType(), True),
            StructField("ProcedureType", StringType(), True),
            StructField("ProcedureDate", StringType(), True),
            StructField("DurationMinutes", DoubleType(), True),
            StructField("Outcome", StringType(), True),
            StructField("Room", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactMedicationAdmin.csv", "factmedicationadmin",
        StructType([
            StructField("AdminId", StringType(), False),
            StructField("PatientId", StringType(), True),
            StructField("MedicationId", StringType(), True),
            StructField("NurseId", StringType(), True),
            StructField("AdminDate", StringType(), True),
            StructField("Dosage", StringType(), True),
            StructField("Route", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "BridgeWardDevice.csv", "bridgewarddevice",
        StructType([
            StructField("WardId", StringType(), False),
            StructField("DeviceId", StringType(), False),
        ])
    ),
    (
        "SensorTelemetry.csv", "sensortelemetry",
        StructType([
            StructField("ReadingId", StringType(), False),
            StructField("DeviceId", StringType(), True),
            StructField("Timestamp", StringType(), True),
            StructField("SensorId", StringType(), True),
            StructField("SensorType", StringType(), True),
            StructField("Value", DoubleType(), True),
            StructField("Unit", StringType(), True),
            StructField("Quality", StringType(), True),
        ])
    ),
]

# ============================================================================
# Load CSV files into Delta tables
# ============================================================================

spark = SparkSession.builder.getOrCreate()
files_path = "Files"

success_count = 0
error_count = 0

for csv_file, table_name, schema in TABLE_DEFINITIONS:
    try:
        file_path = f"{files_path}/{csv_file}"
        print(f"\n{'='*60}")
        print(f"Loading: {csv_file} -> table '{table_name}'")
        print(f"{'='*60}")

        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .load(file_path)

        row_count = df.count()
        print(f"  Rows read: {row_count}")
        print(f"  Columns: {', '.join(df.columns)}")

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

        print(f"  OK: Table '{table_name}' created with {row_count} rows")
        success_count += 1
    except Exception as e:
        print(f"  ERROR loading {csv_file}: {e}")
        error_count += 1

print(f"\n{'='*60}")
print(f"Healthcare Load Complete: {success_count} succeeded, {error_count} failed")
print(f"{'='*60}")
