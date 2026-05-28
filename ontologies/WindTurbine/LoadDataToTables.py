# Fabric Notebook: Load Wind Turbine CSV Data into Delta Tables
# Auto-generated for the Wind Turbine domain.
# Reads CSV files from the lakehouse Files/ folder and writes typed Delta tables.

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ============================================================================
# Table definitions: (csv_filename, table_name, schema)
# ============================================================================

TABLE_DEFINITIONS = [
    (
        "DimWindFarm.csv", "dimwindfarm",
        StructType([
            StructField("WindFarmId", StringType(), False),
            StructField("WindFarmName", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
            StructField("TotalTurbines", LongType(), True),
            StructField("InstalledCapacityMW", LongType(), True),
            StructField("CommissionDate", StringType(), True),
            StructField("Operator", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimTurbine.csv", "dimturbine",
        StructType([
            StructField("TurbineId", StringType(), False),
            StructField("TurbineName", StringType(), True),
            StructField("WindFarmId", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("RatedCapacityKW", LongType(), True),
            StructField("HubHeightM", LongType(), True),
            StructField("RotorDiameterM", LongType(), True),
            StructField("CommissionDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimBlade.csv", "dimblade",
        StructType([
            StructField("BladeId", StringType(), False),
            StructField("BladeName", StringType(), True),
            StructField("TurbineId", StringType(), True),
            StructField("BladePosition", StringType(), True),
            StructField("LengthM", LongType(), True),
            StructField("Material", StringType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("LastInspectionDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimNacelle.csv", "dimnacelle",
        StructType([
            StructField("NacelleId", StringType(), False),
            StructField("NacelleName", StringType(), True),
            StructField("TurbineId", StringType(), True),
            StructField("GeneratorType", StringType(), True),
            StructField("GearboxType", StringType(), True),
            StructField("CoolingSystem", StringType(), True),
            StructField("WeightTons", LongType(), True),
            StructField("LastInspectionDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimTower.csv", "dimtower",
        StructType([
            StructField("TowerId", StringType(), False),
            StructField("TowerName", StringType(), True),
            StructField("TurbineId", StringType(), True),
            StructField("HeightM", LongType(), True),
            StructField("Material", StringType(), True),
            StructField("Sections", LongType(), True),
            StructField("FoundationType", StringType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimSensor.csv", "dimsensor",
        StructType([
            StructField("SensorId", StringType(), False),
            StructField("SensorName", StringType(), True),
            StructField("TurbineId", StringType(), True),
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
            StructField("WindFarmId", StringType(), True),
            StructField("Shift", StringType(), True),
            StructField("YearsExperience", LongType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimWeatherStation.csv", "dimweatherstation",
        StructType([
            StructField("StationId", StringType(), False),
            StructField("StationName", StringType(), True),
            StructField("WindFarmId", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
            StructField("ElevationM", LongType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "DimTransformer.csv", "dimtransformer",
        StructType([
            StructField("TransformerId", StringType(), False),
            StructField("TransformerName", StringType(), True),
            StructField("WindFarmId", StringType(), True),
            StructField("RatingMVA", LongType(), True),
            StructField("VoltageKV", LongType(), True),
            StructField("Manufacturer", StringType(), True),
            StructField("InstallDate", StringType(), True),
            StructField("LastServiceDate", StringType(), True),
            StructField("Status", StringType(), True),
        ])
    ),
    (
        "BridgeTurbineSensor.csv", "bridgeturbinesensor",
        StructType([
            StructField("TurbineId", StringType(), False),
            StructField("SensorId", StringType(), False),
        ])
    ),
    (
        "BridgeTurbineNacelle.csv", "bridgeturbinenacelle",
        StructType([
            StructField("TurbineId", StringType(), False),
            StructField("NacelleId", StringType(), False),
        ])
    ),
    (
        "BridgeTurbineBlade.csv", "bridgeturbineblade",
        StructType([
            StructField("TurbineId", StringType(), False),
            StructField("BladeId", StringType(), False),
        ])
    ),
    (
        "BridgeTurbineTower.csv", "bridgeturbinetower",
        StructType([
            StructField("TurbineId", StringType(), False),
            StructField("TowerId", StringType(), False),
        ])
    ),
    (
        "BridgeWindFarmTurbine.csv", "bridgewindfarmturbine",
        StructType([
            StructField("WindFarmId", StringType(), False),
            StructField("TurbineId", StringType(), False),
        ])
    ),
    (
        "BridgeWindFarmTechnician.csv", "bridgewindfarmtechnician",
        StructType([
            StructField("WindFarmId", StringType(), False),
            StructField("TechnicianId", StringType(), False),
        ])
    ),
    (
        "BridgeWindFarmWeatherStation.csv", "bridgewindfarmweatherstation",
        StructType([
            StructField("WindFarmId", StringType(), False),
            StructField("StationId", StringType(), False),
        ])
    ),
    (
        "BridgeWindFarmTransformer.csv", "bridgewindfarmtransformer",
        StructType([
            StructField("WindFarmId", StringType(), False),
            StructField("TransformerId", StringType(), False),
        ])
    ),
    (
        "BridgeMaintenanceEventTurbine.csv", "bridgemaintenanceeventturbine",
        StructType([
            StructField("EventId", StringType(), False),
            StructField("TurbineId", StringType(), False),
        ])
    ),
    (
        "BridgeMaintenanceEventTechnician.csv", "bridgemaintenanceeventtechnician",
        StructType([
            StructField("EventId", StringType(), False),
            StructField("TechnicianId", StringType(), False),
        ])
    ),
    (
        "BridgePowerOutputTurbine.csv", "bridgepoweroutputturbine",
        StructType([
            StructField("OutputId", StringType(), False),
            StructField("TurbineId", StringType(), False),
        ])
    ),
    (
        "BridgeAlertTurbine.csv", "bridgealertturbine",
        StructType([
            StructField("AlertId", StringType(), False),
            StructField("TurbineId", StringType(), False),
        ])
    ),
    (
        "FactPowerOutput.csv", "factpoweroutput",
        StructType([
            StructField("OutputId", StringType(), False),
            StructField("TurbineId", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Hour", LongType(), True),
            StructField("WindSpeedMs", DoubleType(), True),
            StructField("PowerOutputKW", DoubleType(), True),
            StructField("CapacityFactor", DoubleType(), True),
            StructField("RotorRPM", DoubleType(), True),
            StructField("PitchAngleDeg", DoubleType(), True),
            StructField("YawAngleDeg", DoubleType(), True),
            StructField("GridFrequencyHz", DoubleType(), True),
        ])
    ),
    (
        "FactAlert.csv", "factalert",
        StructType([
            StructField("AlertId", StringType(), False),
            StructField("TurbineId", StringType(), True),
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
        "FactMaintenanceEvent.csv", "factmaintenanceevent",
        StructType([
            StructField("EventId", StringType(), False),
            StructField("TurbineId", StringType(), True),
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
        "SensorTelemetry.csv", "sensortelemetry",
        StructType([
            StructField("Timestamp", StringType(), True),
            StructField("TurbineId", StringType(), True),
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
print(f"Wind Turbine Load Complete: {success_count} succeeded, {error_count} failed")
print(f"{'='*60}")
