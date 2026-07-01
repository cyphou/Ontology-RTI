# Fabric Notebook: Load Solar Farm CSV Data into Delta Tables
# This notebook is deployed automatically by the Solar ontology deployment.
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
        "DimSolarPlant.csv", "dimsolarplant",
        StructType([
            StructField("PlantId", StringType(), False),
            StructField("PlantName", StringType(), True),
            StructField("Region", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
            StructField("CapacityMWc", DoubleType(), True),
            StructField("ArrayCount", IntegerType(), True),
            StructField("Operator", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimSolarArray.csv", "dimsolararray",
        StructType([
            StructField("ArrayId", StringType(), False),
            StructField("ArrayName", StringType(), True),
            StructField("PlantId", StringType(), True),
            StructField("RatedCapacityKW", DoubleType(), True),
            StructField("TiltDegrees", DoubleType(), True),
            StructField("Orientation", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "SensorTelemetryEnriched.csv", "sensortelemetry",
        StructType([
            StructField("Timestamp", StringType(), False),
            StructField("ArrayId", StringType(), True),
            StructField("SensorId", StringType(), True),
            StructField("SensorType", StringType(), True),
            StructField("Value", DoubleType(), True),
            StructField("Unit", StringType(), True),
            StructField("Quality", StringType(), True),
        ])
    ),
    (
        "DimInverter.csv", "diminverter",
        StructType([
            StructField("InverterId", StringType(), False),
            StructField("InverterName", StringType(), True),
            StructField("ArrayId", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("RatedPowerKW", DoubleType(), True),
            StructField("Efficiency", DoubleType(), True),
            StructField("CoolingType", StringType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimString.csv", "dimstring",
        StructType([
            StructField("StringId", StringType(), False),
            StructField("StringName", StringType(), True),
            StructField("ArrayId", StringType(), True),
            StructField("ModuleCount", IntegerType(), True),
            StructField("ModuleType", StringType(), True),
            StructField("RatedVoltageV", IntegerType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("LastInspectionDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimTracker.csv", "dimtracker",
        StructType([
            StructField("TrackerId", StringType(), False),
            StructField("TrackerName", StringType(), True),
            StructField("ArrayId", StringType(), True),
            StructField("TrackerType", StringType(), True),
            StructField("AxisType", StringType(), True),
            StructField("MaxTiltDeg", IntegerType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimSensor.csv", "dimsensor",
        StructType([
            StructField("SensorId", StringType(), False),
            StructField("SensorName", StringType(), True),
            StructField("ArrayId", StringType(), True),
            StructField("SensorType", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("Unit", StringType(), True),
            StructField("MinThreshold", DoubleType(), True),
            StructField("MaxThreshold", DoubleType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimTechnician.csv", "dimtechnician",
        StructType([
            StructField("TechnicianId", StringType(), False),
            StructField("TechnicianName", StringType(), True),
            StructField("Specialization", StringType(), True),
            StructField("CertificationLevel", StringType(), True),
            StructField("PlantId", StringType(), True),
            StructField("Shift", StringType(), True),
            StructField("YearsExperience", IntegerType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimWeatherStation.csv", "dimweatherstation",
        StructType([
            StructField("StationId", StringType(), False),
            StructField("StationName", StringType(), True),
            StructField("PlantId", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
            StructField("ElevationM", IntegerType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimTransformer.csv", "dimtransformer",
        StructType([
            StructField("TransformerId", StringType(), False),
            StructField("TransformerName", StringType(), True),
            StructField("PlantId", StringType(), True),
            StructField("RatingMVA", IntegerType(), True),
            StructField("VoltageKV", IntegerType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("LastServiceDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactMaintenanceEvent.csv", "factmaintenanceevent",
        StructType([
            StructField("EventId", StringType(), False),
            StructField("ArrayId", StringType(), True),
            StructField("TechnicianId", StringType(), True),
            StructField("EventType", StringType(), True),
            StructField("Priority", StringType(), True),
            StructField("ScheduledDate", StringType(), True),
            StructField("CompletedDate", StringType(), True),
            StructField("DurationHours", DoubleType(), True),
            StructField("Component", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("CostUSD", DoubleType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "FactEnergyProduction.csv", "factenergyproduction",
        StructType([
            StructField("ProductionId", StringType(), False),
            StructField("ArrayId", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Hour", IntegerType(), True),
            StructField("IrradianceWm2", DoubleType(), True),
            StructField("PowerOutputKW", DoubleType(), True),
            StructField("PerformanceRatio", DoubleType(), True),
            StructField("ModuleTempC", DoubleType(), True),
            StructField("InverterEfficiency", DoubleType(), True),
            StructField("GridFrequencyHz", DoubleType(), True),
        ])
    ),
    (
        "FactAlert.csv", "factalert",
        StructType([
            StructField("AlertId", StringType(), False),
            StructField("ArrayId", StringType(), True),
            StructField("AlertType", StringType(), True),
            StructField("Severity", StringType(), True),
            StructField("Timestamp", StringType(), True),
            StructField("SensorId", StringType(), True),
            StructField("Value", DoubleType(), True),
            StructField("Threshold", DoubleType(), True),
            StructField("Description", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "BridgeSolarPlantArray.csv", "bridgesolarplantarray",
        StructType([
            StructField("PlantId", StringType(), True),
            StructField("ArrayId", StringType(), True),
        ])
    ),
    (
        "BridgeSolarArrayInverter.csv", "bridgesolararrayinverter",
        StructType([
            StructField("ArrayId", StringType(), True),
            StructField("InverterId", StringType(), True),
        ])
    ),
    (
        "BridgeSolarArrayString.csv", "bridgesolararraystring",
        StructType([
            StructField("ArrayId", StringType(), True),
            StructField("StringId", StringType(), True),
        ])
    ),
    (
        "BridgeSolarArrayTracker.csv", "bridgesolararraytracker",
        StructType([
            StructField("ArrayId", StringType(), True),
            StructField("TrackerId", StringType(), True),
        ])
    ),
    (
        "BridgeSolarArraySensor.csv", "bridgesolararraysensor",
        StructType([
            StructField("ArrayId", StringType(), True),
            StructField("SensorId", StringType(), True),
        ])
    ),
    (
        "BridgeSolarPlantTechnician.csv", "bridgesolarplanttechnician",
        StructType([
            StructField("PlantId", StringType(), True),
            StructField("TechnicianId", StringType(), True),
        ])
    ),
    (
        "BridgeSolarPlantWeatherStation.csv", "bridgesolarplantweatherstation",
        StructType([
            StructField("PlantId", StringType(), True),
            StructField("StationId", StringType(), True),
        ])
    ),
    (
        "BridgeSolarPlantTransformer.csv", "bridgesolarplanttransformer",
        StructType([
            StructField("PlantId", StringType(), True),
            StructField("TransformerId", StringType(), True),
        ])
    ),
    (
        "BridgeMaintenanceEventArray.csv", "bridgemaintenanceeventarray",
        StructType([
            StructField("EventId", StringType(), True),
            StructField("ArrayId", StringType(), True),
        ])
    ),
    (
        "BridgeMaintenanceEventTechnician.csv", "bridgemaintenanceeventtechnician",
        StructType([
            StructField("EventId", StringType(), True),
            StructField("TechnicianId", StringType(), True),
        ])
    ),
    (
        "BridgeEnergyProductionArray.csv", "bridgeenergyproductionarray",
        StructType([
            StructField("ProductionId", StringType(), True),
            StructField("ArrayId", StringType(), True),
        ])
    ),
    (
        "BridgeAlertArray.csv", "bridgealertarray",
        StructType([
            StructField("AlertId", StringType(), True),
            StructField("ArrayId", StringType(), True),
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
