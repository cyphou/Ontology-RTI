# Fabric Notebook: Load IT Asset Management CSV Data into Delta Tables
# Auto-generated for the IT Asset Management domain.
# Reads CSV files from the lakehouse Files/ folder and writes typed Delta tables.

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ============================================================================
# Table definitions: (csv_filename, table_name, schema)
# ============================================================================

TABLE_DEFINITIONS = [
    (
        "DimDataCenter.csv", "dimdatacenter",
        StructType([
            StructField("DataCenterId", StringType(), False),
            StructField("DataCenterName", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("TotalRackCapacity", DoubleType(), True),
            StructField("TierLevel", DoubleType(), True),
            StructField("PowerCapacityKW", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimRack.csv", "dimrack",
        StructType([
            StructField("RackId", StringType(), False),
            StructField("RackName", StringType(), True),
            StructField("DataCenterId", StringType(), True),
            StructField("RackSize", DoubleType(), True),
            StructField("MaxPowerW", DoubleType(), True),
            StructField("CurrentPowerW", DoubleType(), True),
            StructField("TemperatureZone", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimServer.csv", "dimserver",
        StructType([
            StructField("ServerId", StringType(), False),
            StructField("ServerName", StringType(), True),
            StructField("RackId", StringType(), True),
            StructField("ServerType", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("CPUCores", DoubleType(), True),
            StructField("MemoryGB", DoubleType(), True),
            StructField("StorageTB", DoubleType(), True),
            StructField("OS", StringType(), True),
            StructField("IPAddress", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimApplication.csv", "dimapplication",
        StructType([
            StructField("AppId", StringType(), False),
            StructField("AppName", StringType(), True),
            StructField("ServerId", StringType(), True),
            StructField("AppType", StringType(), True),
            StructField("Version", StringType(), True),
            StructField("Environment", StringType(), True),
            StructField("Owner", StringType(), True),
            StructField("SLATier", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimDatabase.csv", "dimdatabase",
        StructType([
            StructField("DatabaseId", StringType(), False),
            StructField("DatabaseName", StringType(), True),
            StructField("ServerId", StringType(), True),
            StructField("DBType", StringType(), True),
            StructField("SizeGB", DoubleType(), True),
            StructField("Engine", StringType(), True),
            StructField("Version", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimVirtualMachine.csv", "dimvirtualmachine",
        StructType([
            StructField("VMId", StringType(), False),
            StructField("VMName", StringType(), True),
            StructField("ServerId", StringType(), True),
            StructField("vCPU", DoubleType(), True),
            StructField("MemoryGB", DoubleType(), True),
            StructField("DiskGB", DoubleType(), True),
            StructField("OS", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimNetworkDevice.csv", "dimnetworkdevice",
        StructType([
            StructField("DeviceId", StringType(), False),
            StructField("DeviceName", StringType(), True),
            StructField("DataCenterId", StringType(), True),
            StructField("DeviceType", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("Ports", DoubleType(), True),
            StructField("IPAddress", StringType(), True),
            StructField("Firmware", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimUser.csv", "dimuser",
        StructType([
            StructField("UserId", StringType(), False),
            StructField("FullName", StringType(), True),
            StructField("Department", StringType(), True),
            StructField("Role", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("AccessLevel", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimLicense.csv", "dimlicense",
        StructType([
            StructField("LicenseId", StringType(), False),
            StructField("AppId", StringType(), True),
            StructField("LicenseType", StringType(), True),
            StructField("Vendor", StringType(), True),
            StructField("ExpirationDate", StringType(), True),
            StructField("Seats", DoubleType(), True),
            StructField("AssignedSeats", DoubleType(), True),
            StructField("CostUSD", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactAlert.csv", "factalert",
        StructType([
            StructField("AlertId", StringType(), False),
            StructField("ServerId", StringType(), True),
            StructField("AlertType", StringType(), True),
            StructField("Severity", StringType(), True),
            StructField("Timestamp", StringType(), True),
            StructField("Value", DoubleType(), True),
            StructField("Threshold", DoubleType(), True),
            StructField("Description", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactIncident.csv", "factincident",
        StructType([
            StructField("IncidentId", StringType(), False),
            StructField("ServerId", StringType(), True),
            StructField("IncidentType", StringType(), True),
            StructField("Severity", StringType(), True),
            StructField("ReportedByUserId", StringType(), True),
            StructField("CreatedDate", StringType(), True),
            StructField("ResolvedDate", StringType(), True),
            StructField("DurationHours", DoubleType(), True),
            StructField("RootCause", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "SensorTelemetry.csv", "sensortelemetry",
        StructType([
            StructField("ReadingId", StringType(), False),
            StructField("ServerId", StringType(), True),
            StructField("Timestamp", StringType(), True),
            StructField("CPUPercent", DoubleType(), True),
            StructField("MemoryPercent", DoubleType(), True),
            StructField("DiskIOPS", DoubleType(), True),
            StructField("NetworkMbps", DoubleType(), True),
            StructField("QualityFlag", StringType(), True),
            StructField("IsAnomaly", StringType(), True),
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
print(f"IT Asset Load Complete: {success_count} succeeded, {error_count} failed")
print(f"{'='*60}")
