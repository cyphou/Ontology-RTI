# Fabric Notebook: Load Oil & Gas Refinery CSV Data into Delta Tables
# This notebook is deployed automatically by Deploy-OilGasOntology.ps1
# It reads CSV files from the lakehouse Files/ folder and writes Delta tables.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

# ============================================================================
# Configuration
# ============================================================================

# Table definitions: (csv_filename, table_name, schema)
TABLE_DEFINITIONS = [
    (
        "DimRefinery.csv", "dimrefinery",
        StructType([
            StructField("RefineryId", StringType(), False),
            StructField("RefineryName", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("State", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
            StructField("CapacityBPD", IntegerType(), True),
            StructField("YearBuilt", IntegerType(), True),
            StructField("Status", StringType(), True),
            StructField("Operator", StringType(), True),
        ])
    ),
    (
        "DimProcessUnit.csv", "dimprocessunit",
        StructType([
            StructField("ProcessUnitId", StringType(), False),
            StructField("ProcessUnitName", StringType(), True),
            StructField("ProcessUnitType", StringType(), True),
            StructField("RefineryId", StringType(), True),
            StructField("CapacityBPD", IntegerType(), True),
            StructField("DesignTemperatureF", DoubleType(), True),
            StructField("DesignPressurePSI", DoubleType(), True),
            StructField("YearInstalled", IntegerType(), True),
            StructField("Status", StringType(), True),
            StructField("Description", StringType(), True),
        ])
    ),
    (
        "DimEquipment.csv", "dimequipment",
        StructType([
            StructField("EquipmentId", StringType(), False),
            StructField("EquipmentName", StringType(), True),
            StructField("EquipmentType", StringType(), True),
            StructField("ProcessUnitId", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("LastInspectionDate", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("CriticalityLevel", StringType(), True),
            StructField("ExpectedLifeYears", IntegerType(), True),
        ])
    ),
    (
        "DimPipeline.csv", "dimpipeline",
        StructType([
            StructField("PipelineId", StringType(), False),
            StructField("PipelineName", StringType(), True),
            StructField("FromProcessUnitId", StringType(), True),
            StructField("ToProcessUnitId", StringType(), True),
            StructField("RefineryId", StringType(), True),
            StructField("DiameterInches", DoubleType(), True),
            StructField("LengthFeet", DoubleType(), True),
            StructField("Material", StringType(), True),
            StructField("MaxFlowBPD", IntegerType(), True),
            StructField("InstalledDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimCrudeOil.csv", "dimcrudeoil",
        StructType([
            StructField("CrudeOilId", StringType(), False),
            StructField("CrudeGradeName", StringType(), True),
            StructField("APIGravity", DoubleType(), True),
            StructField("SulfurContentPct", DoubleType(), True),
            StructField("Origin", StringType(), True),
            StructField("Classification", StringType(), True),
            StructField("PricePerBarrelUSD", DoubleType(), True),
            StructField("Description", StringType(), True),
        ])
    ),
    (
        "DimRefinedProduct.csv", "dimrefinedproduct",
        StructType([
            StructField("ProductId", StringType(), False),
            StructField("ProductName", StringType(), True),
            StructField("ProductCategory", StringType(), True),
            StructField("APIGravity", StringType(), True),
            StructField("SulfurLimitPPM", StringType(), True),
            StructField("FlashPointF", StringType(), True),
            StructField("SpecStandard", StringType(), True),
            StructField("PricePerBarrelUSD", DoubleType(), True),
            StructField("Description", StringType(), True),
        ])
    ),
    (
        "DimStorageTank.csv", "dimstoragetank",
        StructType([
            StructField("TankId", StringType(), False),
            StructField("TankName", StringType(), True),
            StructField("RefineryId", StringType(), True),
            StructField("ProductId", StringType(), True),
            StructField("TankType", StringType(), True),
            StructField("CapacityBarrels", IntegerType(), True),
            StructField("CurrentLevelBarrels", IntegerType(), True),
            StructField("DiameterFeet", StringType(), True),
            StructField("HeightFeet", StringType(), True),
            StructField("Material", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("LastInspectionDate", StringType(), True),
        ])
    ),
    (
        "DimSensor.csv", "dimsensor",
        StructType([
            StructField("SensorId", StringType(), False),
            StructField("SensorName", StringType(), True),
            StructField("SensorType", StringType(), True),
            StructField("EquipmentId", StringType(), True),
            StructField("MeasurementUnit", StringType(), True),
            StructField("MinRange", DoubleType(), True),
            StructField("MaxRange", DoubleType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("CalibrationDate", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Manufacturer", StringType(), True),
        ])
    ),
    (
        "DimEmployee.csv", "dimemployee",
        StructType([
            StructField("EmployeeId", StringType(), False),
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("Role", StringType(), True),
            StructField("Department", StringType(), True),
            StructField("RefineryId", StringType(), True),
            StructField("HireDate", StringType(), True),
            StructField("CertificationLevel", StringType(), True),
            StructField("ShiftPattern", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactMaintenance.csv", "factmaintenance",
        StructType([
            StructField("MaintenanceId", StringType(), False),
            StructField("EquipmentId", StringType(), True),
            StructField("MaintenanceType", StringType(), True),
            StructField("Priority", StringType(), True),
            StructField("PerformedByEmployeeId", StringType(), True),
            StructField("StartDate", StringType(), True),
            StructField("EndDate", StringType(), True),
            StructField("DurationHours", DoubleType(), True),
            StructField("CostUSD", DoubleType(), True),
            StructField("Description", StringType(), True),
            StructField("WorkOrderNumber", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactSafetyAlarm.csv", "factsafetyalarm",
        StructType([
            StructField("AlarmId", StringType(), False),
            StructField("SensorId", StringType(), True),
            StructField("AlarmType", StringType(), True),
            StructField("Severity", StringType(), True),
            StructField("AlarmTimestamp", StringType(), True),
            StructField("AcknowledgedTimestamp", StringType(), True),
            StructField("ClearedTimestamp", StringType(), True),
            StructField("AlarmValue", DoubleType(), True),
            StructField("ThresholdValue", DoubleType(), True),
            StructField("Description", StringType(), True),
            StructField("ActionTaken", StringType(), True),
            StructField("AcknowledgedByEmployeeId", StringType(), True),
        ])
    ),
    (
        "FactProduction.csv", "factproduction",
        StructType([
            StructField("ProductionId", StringType(), False),
            StructField("ProcessUnitId", StringType(), True),
            StructField("ProductId", StringType(), True),
            StructField("ProductionDate", StringType(), True),
            StructField("OutputBarrels", IntegerType(), True),
            StructField("YieldPercent", DoubleType(), True),
            StructField("QualityGrade", StringType(), True),
            StructField("EnergyConsumptionMMBTU", DoubleType(), True),
            StructField("Notes", StringType(), True),
        ])
    ),
    (
        "BridgeCrudeOilProcessUnit.csv", "bridgecrudeoilprocessunit",
        StructType([
            StructField("BridgeId", StringType(), False),
            StructField("CrudeOilId", StringType(), True),
            StructField("ProcessUnitId", StringType(), True),
            StructField("FeedRateBPD", IntegerType(), True),
            StructField("EffectiveDate", StringType(), True),
            StructField("Notes", StringType(), True),
        ])
    ),
]

# ============================================================================
# Load CSV files into Delta tables
# ============================================================================

spark = SparkSession.builder.getOrCreate()

# Resolve lakehouse path - in Fabric notebooks, the default lakehouse is mounted
files_path = "Files"  # Relative path within lakehouse

success_count = 0
error_count = 0

for csv_file, table_name, schema in TABLE_DEFINITIONS:
    try:
        file_path = f"{files_path}/{csv_file}"
        print(f"\n{'='*60}")
        print(f"Loading: {csv_file} -> table '{table_name}'")
        print(f"{'='*60}")

        # Read CSV with schema
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .load(file_path)

        row_count = df.count()
        print(f"  Rows read: {row_count}")
        print(f"  Columns: {', '.join(df.columns)}")

        # Write as Delta table (overwrite if exists)
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

        print(f"  ✓ Table '{table_name}' created successfully with {row_count} rows")
        success_count += 1

    except Exception as e:
        print(f"  ✗ Error loading {csv_file}: {str(e)}")
        error_count += 1

# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*60}")
print(f"LOAD SUMMARY")
print(f"{'='*60}")
print(f"  Succeeded: {success_count}")
print(f"  Failed:    {error_count}")
print(f"  Total:     {len(TABLE_DEFINITIONS)}")

if error_count == 0:
    print("\n  All tables loaded successfully!")
    print("  Next: Create a semantic model from the lakehouse ribbon.")
else:
    print(f"\n  {error_count} table(s) failed. Check errors above.")

# Show all tables
print(f"\nTables in lakehouse:")
spark.sql("SHOW TABLES").show(truncate=False)
