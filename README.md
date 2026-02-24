# Oil & Gas Refinery - Microsoft Fabric IQ Ontology Accelerator

## Overview

This accelerator provides a ready-to-use **Microsoft Fabric IQ Ontology (preview)** for an **Oil & Gas Refinery** company. It includes sample data, ontology design documentation, and step-by-step setup instructions to model:

- **Refineries** and their geographical locations
- **Process Units** (CDU, FCC, Hydrocracker, Reformer, etc.)
- **Equipment** (pumps, heat exchangers, compressors, columns, vessels)
- **Pipelines** connecting process units
- **Crude Oil** grades and supply
- **Refined Products** (gasoline, diesel, jet fuel, LPG, etc.)
- **Storage Tanks** with capacity tracking
- **Sensors / IoT** telemetry on equipment
- **Maintenance Events** for asset management
- **Safety Alarms** and incident tracking
- **Employees / Operators** and shift assignments

---

## Ontology Entity Model

### Entity Types

| Entity Type | Key Property | Description |
|---|---|---|
| **Refinery** | RefineryId | Oil refinery facility with location and capacity info |
| **ProcessUnit** | ProcessUnitId | Major refinery process unit (CDU, FCC, etc.) |
| **Equipment** | EquipmentId | Individual equipment item (pump, compressor, etc.) |
| **Pipeline** | PipelineId | Pipeline segment connecting process units |
| **CrudeOil** | CrudeOilId | Crude oil grade/type with API gravity and sulfur content |
| **RefinedProduct** | ProductId | Output product (gasoline, diesel, kerosene, etc.) |
| **StorageTank** | TankId | Storage tank with capacity and current level |
| **Sensor** | SensorId | IoT sensor measuring temperature, pressure, flow, etc. |
| **MaintenanceEvent** | MaintenanceId | Scheduled or unscheduled maintenance activity |
| **SafetyAlarm** | AlarmId | Safety or operational alarm event |
| **Employee** | EmployeeId | Refinery employee/operator |

### Relationship Types

| Relationship | From Entity | To Entity | Cardinality | Description |
|---|---|---|---|---|
| **contains** | Refinery | ProcessUnit | 1:N | A refinery contains multiple process units |
| **hasEquipment** | ProcessUnit | Equipment | 1:N | A process unit contains equipment |
| **feeds** | CrudeOil | ProcessUnit | N:N | Crude oil grades feed into process units |
| **produces** | ProcessUnit | RefinedProduct | N:N | Process units produce refined products |
| **connectsFrom** | Pipeline | ProcessUnit | N:1 | Pipeline connects from a process unit |
| **connectsTo** | Pipeline | ProcessUnit | N:1 | Pipeline connects to a process unit |
| **stores** | StorageTank | RefinedProduct | N:1 | Tank stores a specific product |
| **locatedAt** | StorageTank | Refinery | N:1 | Tank is located at a refinery |
| **monitors** | Sensor | Equipment | N:1 | Sensor monitors a piece of equipment |
| **targets** | MaintenanceEvent | Equipment | N:1 | Maintenance event targets equipment |
| **performedBy** | MaintenanceEvent | Employee | N:1 | Maintenance performed by an employee |
| **raisedBy** | SafetyAlarm | Sensor | N:1 | Alarm raised by a sensor reading |
| **assignedTo** | Employee | Refinery | N:1 | Employee assigned to a refinery |

---

## Files Structure

```
OntologyAccelerator/
├── README.md                              # This file
├── SETUP_GUIDE.md                         # Step-by-step Fabric setup instructions
├── SEMANTIC_MODEL_GUIDE.md                # Power BI semantic model configuration
├── Deploy-OilGasOntology.ps1              # Main automated deployment script (Steps 0-10)
├── data/
│   ├── DimRefinery.csv                    # Refinery dimension data
│   ├── DimProcessUnit.csv                 # Process unit dimension data
│   ├── DimEquipment.csv                   # Equipment dimension data
│   ├── DimPipeline.csv                    # Pipeline dimension data
│   ├── DimCrudeOil.csv                    # Crude oil grades dimension data
│   ├── DimRefinedProduct.csv              # Refined products dimension data
│   ├── DimStorageTank.csv                 # Storage tanks dimension data
│   ├── DimSensor.csv                      # Sensor dimension data
│   ├── DimEmployee.csv                    # Employee dimension data
│   ├── FactMaintenance.csv                # Maintenance events fact data
│   ├── FactSafetyAlarm.csv                # Safety alarm fact data
│   ├── FactProduction.csv                 # Daily production output fact data
│   ├── BridgeCrudeOilProcessUnit.csv      # Crude oil to process unit mapping
│   └── SensorTelemetry.csv                # Streaming telemetry (for Eventhouse)
├── deploy/
│   ├── Build-Ontology.ps1                 # Ontology definition builder (59 parts)
│   ├── Build-GraphModel-v2.ps1            # Graph model builder
│   ├── Deploy-RTIDashboard.ps1            # KQL Real-Time Dashboard (12 tiles)
│   ├── Deploy-DataAgent.ps1               # Fabric Data Agent (requires F64+)
│   ├── Deploy-OperationsAgent.ps1         # Operations Agent (RTI, Teams integration)
│   ├── Deploy-GraphQuerySet.ps1           # Graph Query Set item creator
│   ├── LoadDataToTables.py                # PySpark notebook for CSV → Delta tables
│   ├── RefineryGraphQueries.gql           # GQL query reference file
│   ├── Validate-Deployment.ps1            # Post-deployment validation
│   ├── SemanticModel.bim                  # Legacy BIM definition
│   └── SemanticModel/                     # TMDL semantic model definition
│       ├── definition.pbism               # Semantic model binding
│       └── definition/                    # Table & relationship TMDL files
└── diagrams/
    └── ontology_diagram.md                # Visual representation of the ontology
```

---

## Quick Start

### Option A: Automated Deployment (Recommended)

```powershell
# Prerequisites: PowerShell 5.1+, Az module, Fabric workspace
cd OntologyAccelerator
.\Deploy-OilGasOntology.ps1 -WorkspaceId "your-workspace-guid"
```

The script automates all 10 steps (see [SETUP_GUIDE.md - Automated Deployment](SETUP_GUIDE.md#automated-deployment)).

### Option B: Manual Setup

1. **Enable prerequisites** — See [SETUP_GUIDE.md](SETUP_GUIDE.md) for tenant settings
2. **Upload data** — Load CSV files from `data/` into a Fabric Lakehouse
3. **Create semantic model** — Follow [SEMANTIC_MODEL_GUIDE.md](SEMANTIC_MODEL_GUIDE.md)
4. **Generate ontology** — Use Fabric IQ to generate from the semantic model
5. **Set up Eventhouse** — Upload `SensorTelemetry.csv` to Eventhouse
6. **RTI Dashboard** — Open `RefineryTelemetryDashboard` in Fabric
7. **Graph Query Set** — Run GQL queries against the ontology graph

### What Gets Deployed

| Item | Type | Description |
|------|------|-------------|
| OilGasRefineryLH | Lakehouse | 13 Delta tables with refinery data |
| OilGasRefinery_LoadTables | Notebook | PySpark notebook for CSV → Delta table loading |
| RefineryTelemetryEH | Eventhouse | Real-time telemetry with 5 KQL tables |
| OilGasRefinerySM | Semantic Model | Direct Lake model (13 tables, 17 relationships) |
| OilGasRefineryOntology | Ontology | 59-part ontology definition |
| OilGasRefineryOntology_graph_* | GraphModel | Graph model with full query readiness |
| RefineryTelemetryDashboard | KQL Dashboard | 12 real-time visualization tiles |
| OilGasRefineryQueries | Graph Query Set | Empty shell (add 10 GQL queries manually via UI) |
| OilGasRefineryAgent | Data Agent | Ontology-powered NL query agent (requires F64+ capacity) |
| RefineryOperationsAgent | Operations Agent | AI agent monitoring KQL telemetry, sends Teams recommendations |

---

## Domain Context

### Refinery Process Flow

```
Crude Oil Storage → Crude Distillation Unit (CDU)
                         │
            ┌────────────┼────────────────┐
            ▼            ▼                ▼
    Light Naphtha    Heavy Naphtha     Residue
         │                │               │
         ▼                ▼               ▼
    LPG / Gas      Catalytic Reformer   Vacuum Distillation
                         │                    │
                         ▼               ┌────┴────┐
                   High Octane           ▼         ▼
                   Gasoline         Hydrocracker   FCC
                                        │         │
                                        ▼         ▼
                                    Jet Fuel   Gasoline
                                               Diesel

Final Products → Storage Tanks → Distribution
```

### Key Metrics Tracked

- **Throughput** (barrels per day)
- **Yield** (product output vs. crude input)
- **Equipment uptime / downtime**
- **Sensor readings** (temperature, pressure, flow rate, vibration)
- **Maintenance frequency and cost**
- **Safety alarm frequency and severity**
- **Tank utilization** (current level vs. capacity)

---

## KQL Real-Time Dashboard

The `RefineryTelemetryDashboard` provides 12 visualization tiles across 5 KQL tables:

| Tile | Visual | Data Source |
|------|--------|-------------|
| Sensor Readings Over Time | Line chart | SensorReading |
| Equipment Alerts by Severity | Pie chart | EquipmentAlert |
| Alert Trend Over Time | Line chart | EquipmentAlert |
| Refinery Locations | Map | Inline refinery coordinates |
| Top Sensors by Reading Count | Table | SensorReading |
| Anomaly Detections | Table | SensorReading |
| Process Unit Throughput | Line chart | ProcessMetric |
| Pipeline Flow Status | Table | PipelineFlow |
| Current Tank Levels | Table | TankLevel |
| Unacknowledged Alerts | Table | EquipmentAlert |
| Sensor Quality Distribution | Pie chart | SensorReading |
| Tank Level Trend | Line chart | TankLevel |

---

## Graph Query Set (GQL)

The `OilGasRefineryQueries` Graph Query Set is created as an empty item. Due to a Fabric REST API limitation, queries cannot be pushed programmatically and must be added manually via the Fabric UI.

**To add queries**: Open the GQS in Fabric, select the ontology graph model, then copy-paste queries from [deploy/RefineryGraphQueries.gql](deploy/RefineryGraphQueries.gql).

The reference file includes 20 GQL queries:

| # | Query | Pattern |
|---|-------|---------|
| 1 | Full Refinery Topology | `MATCH (n)-[e]->(m) RETURN n, e, m` |
| 2 | Process Units & Equipment | `Refinery → ProcessUnit → Equipment` |
| 3 | Sensors & Alarms | `Equipment → Sensor ← SafetyAlarm` |
| 4 | Maintenance Events | `Employee ← MaintenanceEvent → Equipment` |
| 5 | Crude Supply Chain | `CrudeOil ← CrudeOilFeed → ProcessUnit` |
| 6 | Production Records | `ProcessUnit ← ProductionRecord → RefinedProduct` |
| 7 | Storage Tanks | `Refinery → StorageTank → RefinedProduct` |
| 8 | Pipeline Network | `Refinery → Pipeline → ProcessUnit` |
| 9 | End-to-End | `CrudeOil → ... → RefinedProduct` |
| 10 | Workforce | `Refinery → Employee ← MaintenanceEvent` |
| 11 | All Sensors on a Specific Equipment | Filter by EquipmentId |
| 12 | Unresolved Safety Alarms | `SafetyAlarm WHERE Status = 'Active'` |
| 13 | Equipment Without Recent Maintenance | Anti-pattern detection |
| 14 | High-Severity Alarms by Refinery | Aggregated alarm analysis |
| 15 | Pipeline Connections Between Units | `ProcessUnit → Pipeline → ProcessUnit` |
| 16 | Products Stored per Refinery | `Refinery → StorageTank → RefinedProduct` |
| 17 | Employee Maintenance Workload | Workload distribution |
| 18 | Crude Oil API Gravity Analysis | Property-based filtering |
| 19 | Multi-Hop: Crude to Final Product | Full value chain traversal |
| 20 | Refinery Equipment Health Summary | Equipment status overview |

---

## Operations Agent (Real-Time Intelligence)

The `RefineryOperationsAgent` is a Fabric Operations Agent that continuously monitors KQL Database telemetry and sends actionable recommendations via Microsoft Teams.

**What it monitors:**
- Equipment sensor anomalies (temperature, pressure, flow, vibration outside thresholds)
- Critical/High severity safety alarms and unacknowledged alerts
- Production throughput drops and yield degradation
- Maintenance costs, recurring equipment failures, overdue inspections

**Prerequisites:**
- Fabric capacity (Trial may work for creation; F2+ for execution)
- Tenant admin must enable: *Operations Agent* preview + *Copilot and Azure OpenAI Service*
- Microsoft Teams with *Fabric Operations Agent* app installed

**Post-deployment setup (Fabric UI):**
1. Open the agent → Add **Knowledge Source** → Select `RefineryTelemetryEH` Eventhouse / `RefineryTelemetryDB` KQL Database
2. Configure **Actions** (optional): Power Automate flows for safety alerts, maintenance work orders, production escalations
3. **Save** to generate the playbook → **Start** the agent
4. Recipients receive proactive recommendations in Teams chat
