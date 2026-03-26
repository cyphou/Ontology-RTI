# Fabric Notebook: Load Manufacturing Plant CSV Data into Delta Tables
# Auto-generated for the Manufacturing Plant domain.
# Reads CSV files from the lakehouse Files/ folder and writes typed Delta tables.

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ============================================================================
# Table definitions: (csv_filename, table_name, schema)
# ============================================================================

TABLE_DEFINITIONS = [
    (
        "DimPlant.csv", "dimplant",
        StructType([
            StructField("PlantId", StringType(), False),
            StructField("PlantName", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("State", StringType(), True),
            StructField("City", StringType(), True),
            StructField("TotalAreaSqFt", DoubleType(), True),
            StructField("YearBuilt", StringType(), True),
            StructField("ProductionCapacity", DoubleType(), True),
            StructField("Status", StringType(), True),
            StructField("Manager", StringType(), True),
        ])
    ),
    (
        "DimProductionLine.csv", "dimproductionline",
        StructType([
            StructField("LineId", StringType(), False),
            StructField("LineName", StringType(), True),
            StructField("PlantId", StringType(), True),
            StructField("LineType", StringType(), True),
            StructField("CapacityUnitsPerHour", DoubleType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimMachine.csv", "dimmachine",
        StructType([
            StructField("MachineId", StringType(), False),
            StructField("MachineName", StringType(), True),
            StructField("LineId", StringType(), True),
            StructField("MachineType", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("CriticalityLevel", StringType(), True),
        ])
    ),
    (
        "DimSensor.csv", "dimsensor",
        StructType([
            StructField("SensorId", StringType(), False),
            StructField("SensorName", StringType(), True),
            StructField("SensorType", StringType(), True),
            StructField("MachineId", StringType(), True),
            StructField("MeasurementUnit", StringType(), True),
            StructField("MinRange", DoubleType(), True),
            StructField("MaxRange", DoubleType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimProduct.csv", "dimproduct",
        StructType([
            StructField("ProductId", StringType(), False),
            StructField("ProductName", StringType(), True),
            StructField("ProductCategory", StringType(), True),
            StructField("UnitOfMeasure", StringType(), True),
            StructField("WeightKg", DoubleType(), True),
            StructField("Description", StringType(), True),
        ])
    ),
    (
        "DimMaterial.csv", "dimmaterial",
        StructType([
            StructField("MaterialId", StringType(), False),
            StructField("MaterialName", StringType(), True),
            StructField("MaterialType", StringType(), True),
            StructField("Supplier", StringType(), True),
            StructField("UnitCost", DoubleType(), True),
            StructField("UnitOfMeasure", StringType(), True),
            StructField("MinStockLevel", DoubleType(), True),
            StructField("CurrentStock", DoubleType(), True),
        ])
    ),
    (
        "DimOperator.csv", "dimoperator",
        StructType([
            StructField("OperatorId", StringType(), False),
            StructField("FullName", StringType(), True),
            StructField("Role", StringType(), True),
            StructField("Shift", StringType(), True),
            StructField("LineId", StringType(), True),
            StructField("HireDate", StringType(), True),
            StructField("CertificationLevel", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactProductionBatch.csv", "factproductionbatch",
        StructType([
            StructField("BatchId", StringType(), False),
            StructField("LineId", StringType(), True),
            StructField("ProductId", StringType(), True),
            StructField("StartTime", StringType(), True),
            StructField("EndTime", StringType(), True),
            StructField("QuantityProduced", DoubleType(), True),
            StructField("DefectRate", DoubleType(), True),
            StructField("EnergyUsedKWh", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactQualityCheck.csv", "factqualitycheck",
        StructType([
            StructField("QCId", StringType(), False),
            StructField("ProductId", StringType(), True),
            StructField("LineId", StringType(), True),
            StructField("InspectorId", StringType(), True),
            StructField("CheckDate", StringType(), True),
            StructField("CheckType", StringType(), True),
            StructField("Result", StringType(), True),
            StructField("DefectCount", DoubleType(), True),
            StructField("Notes", StringType(), True),
        ])
    ),
    (
        "FactMaintenanceOrder.csv", "factmaintenanceorder",
        StructType([
            StructField("OrderId", StringType(), False),
            StructField("MachineId", StringType(), True),
            StructField("OrderType", StringType(), True),
            StructField("Priority", StringType(), True),
            StructField("AssignedToOperatorId", StringType(), True),
            StructField("StartDate", StringType(), True),
            StructField("EndDate", StringType(), True),
            StructField("DurationHours", DoubleType(), True),
            StructField("CostUSD", DoubleType(), True),
            StructField("Description", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactAlert.csv", "factalert",
        StructType([
            StructField("AlertId", StringType(), False),
            StructField("SensorId", StringType(), True),
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
        "SensorTelemetry.csv", "sensortelemetry",
        StructType([
            StructField("ReadingId", StringType(), False),
            StructField("SensorId", StringType(), True),
            StructField("Timestamp", StringType(), True),
            StructField("ReadingValue", DoubleType(), True),
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
print(f"Manufacturing Plant Load Complete: {success_count} succeeded, {error_count} failed")
print(f"{'='*60}")
