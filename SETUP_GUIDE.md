# Setup Guide - Oil & Gas Refinery Ontology in Microsoft Fabric

This guide walks you through setting up the Oil & Gas Refinery ontology in Microsoft Fabric IQ (preview).

> **Quick Deploy**: For automated deployment, skip to the [Automated Deployment](#automated-deployment) section below.

---

## Prerequisites

### 1. Fabric Capacity
- A Microsoft Fabric workspace with an enabled [capacity](https://learn.microsoft.com/en-us/fabric/enterprise/licenses#capacity) (F2 or higher recommended).

### 2. Tenant Settings
A Fabric administrator must enable the following settings in **Admin Portal > Tenant settings**:

| Setting | Required |
|---|---|
| **Enable Ontology item (preview)** | Yes |
| **User can create Graph (preview)** | Yes |
| **Create Real-Time dashboards** | Yes (for RTI Dashboard) |
| **Users can create and share Data agent item types (preview)** | Recommended |
| **Users can use Copilot and other features powered by Azure OpenAI** | Recommended (for NL queries) |
| **Data sent to Azure OpenAI can be processed outside your capacity's geographic region** | Recommended |

---

## Step 1: Create the Lakehouse

1. Navigate to your Fabric workspace.
2. Select **+ New item** > **Lakehouse**.
3. Name it: `OilGasRefineryLH`
4. Click **Create**.

---

## Step 2: Upload Data Files

1. In the lakehouse, select **Get data** > **Upload files** from the ribbon.
2. Upload the following CSV files from the `data/` folder:

| File | Description |
|---|---|
| `DimRefinery.csv` | Refinery locations and capacities |
| `DimProcessUnit.csv` | Process units (CDU, FCC, Hydrocracker, etc.) |
| `DimEquipment.csv` | Equipment items (pumps, reactors, columns, etc.) |
| `DimPipeline.csv` | Pipeline connections between process units |
| `DimCrudeOil.csv` | Crude oil grades and properties |
| `DimRefinedProduct.csv` | Refined product specifications |
| `DimStorageTank.csv` | Storage tank details and levels |
| `DimSensor.csv` | IoT sensor specifications |
| `DimEmployee.csv` | Employees and roles |
| `FactMaintenance.csv` | Maintenance event records |
| `FactSafetyAlarm.csv` | Safety alarm events |
| `FactProduction.csv` | Daily production output |
| `BridgeCrudeOilProcessUnit.csv` | Crude oil to process unit feed mapping |

> **Note:** Do NOT upload `SensorTelemetry.csv` to the lakehouse. This file goes to Eventhouse (Step 5).

3. After uploading, **load each file to a delta table**:
   - Expand **Files** in the Explorer.
   - For each file: Right-click > **Load to Tables** > **New table**.
   - Keep default table names (lowercase versions of filenames).

When complete, you should see 13 tables in the lakehouse.

---

## Step 3: Create the Semantic Model

Follow the detailed instructions in [SEMANTIC_MODEL_GUIDE.md](SEMANTIC_MODEL_GUIDE.md) to:
- Create a Power BI semantic model from the lakehouse tables.
- Define all relationships between tables.
- Configure display names and formatting.

---

## Step 4: Generate the Ontology

### Option A: Generate from Semantic Model (Recommended)

1. Navigate to the **OilGasRefineryModel** semantic model.
2. From the top ribbon, select **Generate Ontology**.
3. Set:
   - **Workspace**: Your workspace
   - **Name**: `OilGasRefineryOntology`
4. Click **Create**.

After generation, verify and configure the ontology:

#### Rename Entity Types

| Original Table Name | Rename To |
|---|---|
| dimrefinery | Refinery |
| dimprocessunit | ProcessUnit |
| dimequipment | Equipment |
| dimpipeline | Pipeline |
| dimcrudeoil | CrudeOil |
| dimrefinedproduct | RefinedProduct |
| dimstoragetank | StorageTank |
| dimsensor | Sensor |
| dimemployee | Employee |
| factmaintenance | MaintenanceEvent |
| factsafetyalarm | SafetyAlarm |
| factproduction | ProductionRecord |
| bridgecrudeoilprocessunit | CrudeOilFeed |

#### Verify Entity Type Keys

| Entity Type | Key Property |
|---|---|
| Refinery | RefineryId |
| ProcessUnit | ProcessUnitId |
| Equipment | EquipmentId |
| Pipeline | PipelineId |
| CrudeOil | CrudeOilId |
| RefinedProduct | ProductId |
| StorageTank | TankId |
| Sensor | SensorId |
| Employee | EmployeeId |
| MaintenanceEvent | MaintenanceId |
| SafetyAlarm | AlarmId |
| ProductionRecord | ProductionId |
| CrudeOilFeed | BridgeId |

#### Configure Relationship Types

| Relationship Name | Description | Source Data | From Entity (Source Column) | To Entity (Source Column) |
|---|---|---|---|---|
| contains | Refinery contains ProcessUnit | dimprocessunit | Refinery (RefineryId) | ProcessUnit (ProcessUnitId) |
| hasEquipment | ProcessUnit has Equipment | dimequipment | ProcessUnit (ProcessUnitId) | Equipment (EquipmentId) |
| connectsFrom | Pipeline from ProcessUnit | dimpipeline | ProcessUnit (FromProcessUnitId) | Pipeline (PipelineId) |
| connectsTo | Pipeline to ProcessUnit | dimpipeline | ProcessUnit (ToProcessUnitId) | Pipeline (PipelineId) |
| stores | Tank stores Product | dimstoragetank | RefinedProduct (ProductId) | StorageTank (TankId) |
| locatedAt | Tank at Refinery | dimstoragetank | Refinery (RefineryId) | StorageTank (TankId) |
| monitors | Sensor monitors Equipment | dimsensor | Equipment (EquipmentId) | Sensor (SensorId) |
| targets | Maintenance targets Equipment | factmaintenance | Equipment (EquipmentId) | MaintenanceEvent (MaintenanceId) |
| performedBy | Maintenance by Employee | factmaintenance | Employee (PerformedByEmployeeId) | MaintenanceEvent (MaintenanceId) |
| raisedBy | Alarm raised by Sensor | factsafetyalarm | Sensor (SensorId) | SafetyAlarm (AlarmId) |
| assignedTo | Employee at Refinery | dimemployee | Refinery (RefineryId) | Employee (EmployeeId) |
| feeds | CrudeOil feeds ProcessUnit | bridgecrudeoilprocessunit | CrudeOil (CrudeOilId) | ProcessUnit (ProcessUnitId) |
| produces | ProcessUnit produces Product | factproduction | ProcessUnit (ProcessUnitId) | RefinedProduct (ProductId) |

### Option B: Build Directly from OneLake

If you prefer to build the ontology manually:

1. In your workspace, select **+ New item** > **Ontology (preview)**.
2. Name it: `OilGasRefineryOntology`
3. Manually create each entity type from the table above.
4. For each entity type:
   - Add all properties from the corresponding table.
   - Set the entity type key.
   - Add the data binding to the corresponding lakehouse table.
5. Create each relationship type as listed in the relationship table.

---

## Step 5: Set Up Eventhouse for Telemetry

For real-time sensor telemetry data:

1. In your workspace, select **+ New item** > **Eventhouse**.
2. Name it: `RefineryTelemetryEH`
3. Open the default KQL database.
4. Select **Get data** > **Local file**.
5. Create table: `SensorTelemetry`
6. Upload `data/SensorTelemetry.csv`.
7. Keep default settings and complete the import.

### Bind Telemetry to Sensor Entity

1. Open the `OilGasRefineryOntology` ontology.
2. Select the **Sensor** entity type.
3. In the **Bindings** tab, add a new binding:
   - **Source**: `RefineryTelemetryEH` > `SensorTelemetry`
   - **Key mapping**: `SensorId` → `SensorId`
4. Map the telemetry properties:
   - `Timestamp` → Date/Time property
   - `Value` → Numeric property
   - `Quality` → String property

---

## Step 6: Preview and Query the Ontology

### Graph Preview
1. In the ontology editor, select **Preview** from the ribbon.
2. Click **Refresh graph model** to populate with data.
3. Explore:
   - Click on a **Refinery** node to see its connected **ProcessUnits**
   - Navigate from **ProcessUnit** to **Equipment** to **Sensors**
   - View **MaintenanceEvents** connected to **Equipment**

### Natural Language Queries (via Data Agent)
Create a Fabric data agent connected to your ontology to ask questions like:

- *"What is the total refining capacity across all active refineries?"*
- *"Which equipment at Gulf Coast Refinery has had the most maintenance events?"*
- *"Show me all critical alarms in the last month"*
- *"What products does the FCC-1 unit produce and what is the daily output?"*
- *"Which process units are fed by Brent Crude?"*
- *"What is the current tank utilization at the Rotterdam refinery?"*
- *"List all sensors monitoring the hydrocracker reactors"*
- *"What was the total maintenance cost for the Gulf Coast Refinery in 2025?"*

> **Note**: The Data Agent requires Fabric capacity **F64 or higher**. It is not supported on Trial capacity.

---

## Step 7: RTI Dashboard (Real-Time Intelligence)

The deployment script creates a KQL Dashboard connected to the Eventhouse with 12 visualization tiles.

### Manual Setup (if not using automation)

1. In your workspace, select **+ New item** > **Real-Time Dashboard**.
2. Name it: `RefineryTelemetryDashboard`
3. Add a data source:
   - **Type**: Kusto (KQL)
   - **Cluster URI**: Your Eventhouse query service URI
   - **Database**: `RefineryTelemetryEH`
4. Create tiles for each KQL query (see `deploy/Deploy-RTIDashboard.ps1` for tile definitions).

### KQL Tables Used

| KQL Table | Columns |
|-----------|---------|
| SensorReading | SensorId, EquipmentId, RefineryId, Timestamp, ReadingValue, MeasurementUnit, SensorType, QualityFlag, IsAnomaly |
| EquipmentAlert | AlertId, SensorId, EquipmentId, RefineryId, Timestamp, AlertType, Severity, ReadingValue, ThresholdValue, Message, IsAcknowledged |
| ProcessMetric | ProcessUnitId, RefineryId, Timestamp, ThroughputBPH, InletTemperatureF, OutletTemperatureF, PressurePSI, FeedRateBPH, YieldPercent, EnergyConsumptionMMBTU |
| PipelineFlow | PipelineId, RefineryId, Timestamp, FlowRateBPH, PressurePSI, TemperatureF, ViscosityCp, IsFlowNormal |
| TankLevel | TankId, RefineryId, Timestamp, LevelBarrels, LevelPercent, TemperatureF, ProductId, IsOverflow |

---

## Step 8: Graph Query Set

The Graph Query Set provides 20 GQL (Graph Query Language) queries for exploring the ontology graph.

> **Note**: The Fabric REST API does not yet support pushing queries into a Graph Query Set programmatically.
> The deployment script creates the empty Graph Query Set item. Queries must be added manually via the Fabric UI.

### Manual Setup (Required)

1. In your workspace, open the **OilGasRefineryQueries** Graph Query Set (or create one via **+ New item** > **Graph Query Set**).
2. Select the graph model generated from the ontology.
3. Copy queries one at a time from `deploy/RefineryGraphQueries.gql`.
4. Paste each query, give it a name, and click **Run** to verify.

### Key Queries

| # | Query | Pattern |
|---|-------|---------|
| 1 | Full Topology | `MATCH (n)-[e]->(m) RETURN n, e, m` |
| 2 | Process Units & Equipment | `Refinery → ProcessUnit → Equipment` |
| 3 | Sensors & Alarms | `Equipment → Sensor ← SafetyAlarm` |
| 4 | Maintenance Events | `Employee ← MaintenanceEvent → Equipment` |
| 5 | Crude Supply Chain | `CrudeOil ← CrudeOilFeed → ProcessUnit` |
| 6 | Production Records | `ProcessUnit ← ProductionRecord → RefinedProduct` |
| 7 | Storage Tanks | `Refinery → StorageTank → RefinedProduct` |
| 8 | Pipeline Network | `Refinery → Pipeline → ProcessUnit` |
| 9 | End-to-End | `CrudeOil → ... → RefinedProduct` |
| 10 | Workforce | `Refinery → Employee ← MaintenanceEvent` |

---

## Troubleshooting

| Issue | Solution |
|---|---|
| Cannot create ontology | Verify all tenant settings are enabled (see Prerequisites) |
| Entity types not appearing | Make sure semantic model tables are visible (not hidden) and relationships defined |
| Graph preview empty | Click **Refresh graph model** in the Preview tab |
| Telemetry not showing | Ensure Eventhouse binding is configured with correct key mapping |
| NL queries not working | Enable Azure OpenAI related tenant settings |
| RTI Dashboard has no data | Upload data to Eventhouse KQL tables; verify the dashboard is pointing at the correct KQL database |
| Data Agent fails to create | Requires Fabric capacity F64 or higher (not supported on Trial capacity) |
| Graph Query Set shows no graph | Open the GQS in Fabric UI, click the graph selector, and choose the ontology graph model |

---

## Automated Deployment

Instead of performing Steps 1-5 manually, you can use the provided PowerShell deployment script to automate the process.

### Prerequisites for Automation

- **PowerShell 5.1+** (Windows) or **PowerShell 7+** (cross-platform)
- **Az PowerShell module**: Install with `Install-Module -Name Az -Scope CurrentUser`
- **Azure account** with access to your Fabric workspace
- **Fabric workspace ID** (find it in the workspace URL: `app.fabric.microsoft.com/groups/{WorkspaceId}`)

### Run the Deployment Script

```powershell
# Navigate to the accelerator folder
cd OntologyAccelerator

# Run the deployment (interactive Azure login will open if needed)
.\Deploy-OilGasOntology.ps1 -WorkspaceId "your-workspace-guid"
```

### Customize Item Names (Optional)

```powershell
.\Deploy-OilGasOntology.ps1 `
    -WorkspaceId "your-workspace-guid" `
    -LakehouseName "MyRefineryLakehouse" `
    -EventhouseName "MyTelemetryEH" `
    -SemanticModelName "MyRefineryModel" `
    -OntologyName "MyRefineryOntology"
```

### What the Script Does

| Step | Action | API Used |
|------|--------|----------|
| 0 | Authenticates to Azure (interactive if needed) | `Connect-AzAccount` |
| 1 | Creates the Lakehouse | Fabric REST API |
| 2 | Uploads all 13 CSV files to Lakehouse Files/ | OneLake DFS API |
| 3 | Creates & runs a Spark notebook to load CSVs into Delta tables | Fabric REST API |
| 4 | Creates the Eventhouse and KQL database for telemetry | Fabric REST API |
| 5 | Creates the Semantic Model (Direct Lake, TMDL, 13 tables, 17 relationships) | Fabric REST API |
| 6 | Creates the Ontology (59 definition parts) and builds the Graph Model | Fabric REST API |
| 7 | Deploys the RTI Dashboard (KQL Dashboard with 12 visualization tiles) | Fabric REST API |
| 8 | Creates a Data Agent with ontology as sole data source (requires F64+) | Fabric REST API |
| 9 | Creates Graph Query Set item (queries must be added manually via UI) | Fabric REST API |
| 10 | Creates an Operations Agent for RTI monitoring and Teams integration | Fabric REST API |

### After Deployment

The script will display a summary of what was created. Some remaining manual steps:

1. **If notebook didn't complete**: Open `OilGasRefinery_LoadTables` notebook in Fabric and run it manually.
2. **Telemetry data**: Upload `data/SensorTelemetry.csv` to the Eventhouse KQL database via **Get data > Local file**.
3. **RTI Dashboard**: Requires the **Create Real-Time dashboards** tenant setting. The dashboard auto-connects to the Eventhouse KQL database.
4. **Data Agent**: Uses the **Ontology** as its sole data source. Requires Fabric capacity **F64+** (not supported on Trial). The script will skip this step on Trial capacity.
5. **Graph Query Set**: Open the GQS in Fabric, select the graph model, and copy queries from `deploy/RefineryGraphQueries.gql`.
6. **Operations Agent**: Open the agent in Fabric, add Knowledge Source (KQL DB), configure Actions, then Start.

### Validate the Deployment

After deployment, run the validation script to verify all items were created:

```powershell
.\deploy\Validate-Deployment.ps1 -WorkspaceId "your-workspace-guid"
```

This will check for the existence of the lakehouse, eventhouse, semantic model, ontology, and all 13 Delta tables.

### Deployment Files Reference

| File | Purpose |
|------|---------|
| `Deploy-OilGasOntology.ps1` | Main deployment orchestrator (Steps 0-10) |
| `deploy/Build-Ontology.ps1` | Ontology definition builder (59 parts, entity types, relationships) |
| `deploy/Build-GraphModel-v2.ps1` | Graph model builder |
| `deploy/Deploy-RTIDashboard.ps1` | KQL Real-Time Dashboard (12 tiles, 5 KQL tables) |
| `deploy/Deploy-DataAgent.ps1` | Fabric Data Agent for NL queries |
| `deploy/Deploy-GraphQuerySet.ps1` | Graph Query Set item creator (queries added manually) |
| `deploy/Deploy-OperationsAgent.ps1` | Operations Agent for RTI monitoring and Teams integration |
| `deploy/LoadDataToTables.py` | PySpark notebook code (CSV → Delta tables) |
| `deploy/RefineryGraphQueries.gql` | GQL query reference (copy-paste fallback) |
| `deploy/Validate-Deployment.ps1` | Post-deployment validation script |
| `deploy/SemanticModel/` | TMDL semantic model definition (Direct Lake) |
| `deploy/SemanticModel.bim` | Legacy BIM definition |
