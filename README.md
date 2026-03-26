<p align="center">
  <img src="https://img.shields.io/badge/Microsoft%20Fabric-742774?style=for-the-badge&logo=microsoftfabric&logoColor=white" alt="Microsoft Fabric"/>
  <img src="https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="Power BI"/>
  <img src="https://img.shields.io/badge/KQL-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" alt="KQL"/>
  <img src="https://img.shields.io/badge/PowerShell-5391FE?style=for-the-badge&logo=powershell&logoColor=white" alt="PowerShell"/>
  <img src="https://img.shields.io/badge/GQL-107C10?style=for-the-badge&logo=graphql&logoColor=white" alt="GQL"/>
</p>

<h1 align="center">:zap: Microsoft Fabric IQ Ontology Accelerator</h1>

<p align="center">
  <b>Deploy production-ready IQ Ontologies across 5 industry domains on Microsoft Fabric --- fully automated, one command.</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/domains-5%20industries-742774?style=flat-square" alt="5 Domains"/>
  <img src="https://img.shields.io/badge/entity%20types-59-blue?style=flat-square" alt="59 Entity Types"/>
  <img src="https://img.shields.io/badge/CSV%20tables-64-green?style=flat-square" alt="64 Tables"/>
  <img src="https://img.shields.io/badge/sample%20rows-2%2C800%2B-orange?style=flat-square" alt="2800+ Rows"/>
  <img src="https://img.shields.io/badge/KQL%20tables-25-0078D4?style=flat-square" alt="25 KQL Tables"/>
  <img src="https://img.shields.io/badge/dashboard%20tiles-52-0078D4?style=flat-square" alt="52 Tiles"/>
  <img src="https://img.shields.io/badge/GQL%20queries-100%2B-107C10?style=flat-square" alt="GQL Queries"/>
  <img src="https://img.shields.io/badge/AI%20agents-10-FF6F00?style=flat-square" alt="AI Agents"/>
  <img src="https://img.shields.io/badge/copilot%20agents-7-5391FE?style=flat-square" alt="7 Copilot Agents"/>
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" alt="License"/>
</p>

<p align="center">
  <a href="#-quick-start">Quick Start</a> ---
  <a href="#-supported-domains">Domains</a> ---
  <a href="#-what-gets-deployed">What Gets Deployed</a> ---
  <a href="#-architecture">Architecture</a> ---
  <a href="#-multi-agent-development">Agents</a> ---
  <a href="#-development-roadmap">Roadmap</a>
</p>

---
## :globe_with_meridians: Overview

```mermaid
flowchart LR
    subgraph Input["Package"]
        CSV["CSV Data\n64 tables"]
        ONT["Ontology\n59 entity types"]
        GQL["GQL Queries\n100+ queries"]
    end

    CMD["Deploy-Ontology.ps1"] --> LH["Lakehouse\nDelta Tables"]
    CMD --> SM["Semantic Model\nDirect Lake"]
    CMD --> EH["Eventhouse\nKQL Database"]
    CMD --> OB["Ontology\nGraph Model"]
    CMD --> GQS["Graph Query Set"]
    CMD --> DASH["RTI Dashboard\n10-12 tiles"]
    CMD --> DA["Data Agent\nNL + Graph"]
    CMD --> OA["Operations Agent\nRTI + Teams"]

    Input --> CMD

    style CMD fill:#742774,color:#fff,stroke:#742774
    style LH fill:#0078D4,color:#fff,stroke:#0078D4
    style SM fill:#F2C811,color:#000,stroke:#F2C811
    style EH fill:#0078D4,color:#fff,stroke:#0078D4
    style OB fill:#107C10,color:#fff,stroke:#107C10
    style GQS fill:#107C10,color:#fff,stroke:#107C10
    style DASH fill:#0078D4,color:#fff,stroke:#0078D4
    style DA fill:#FF6F00,color:#fff,stroke:#FF6F00
    style OA fill:#FF6F00,color:#fff,stroke:#FF6F00
```

Each domain ships a **complete, ready-to-deploy package**: CSV sample data, ontology definition, graph queries, KQL enrichment tables, a real-time dashboard, and two AI agents --- all wired together and deployed with a single PowerShell command.

---

## :factory: Supported Domains

<table>
<tr>
<td width="20%" align="center">

### :oil_drum: Oil & Gas
**Refinery**
13 entities  14 CSVs
445 rows

</td>
<td width="20%" align="center">

### :office: Smart Building
**Building Ops**
12 entities  13 CSVs
498 rows

</td>
<td width="20%" align="center">

### :factory: Manufacturing
**Plant Floor**
11 entities  12 CSVs
444 rows

</td>
<td width="20%" align="center">

### :desktop_computer: IT Asset
**Infrastructure**
11 entities  12 CSVs
381 rows

</td>
<td width="20%" align="center">

### :wind_face: Wind Turbine
**Wind Farm**
12 entities  13 CSVs
651 rows

</td>
</tr>
<tr>
<td><sub>Refineries, process units, equipment, pipelines, crude oil, sensors, safety alarms</sub></td>
<td><sub>Buildings, floors, zones, HVAC, lighting, elevators, occupancy, energy meters</sub></td>
<td><sub>Plants, production lines, machines, quality checks, materials, batches, OEE</sub></td>
<td><sub>Datacenters, racks, servers, VMs, applications, incidents, licenses</sub></td>
<td><sub>Wind farms, turbines, nacelles, blades, towers, power output, weather stations</sub></td>
</tr>
</table>
---

## :zap: Quick Start

### :dart: One-Command Deployment

```powershell
# Interactive domain menu
.\Deploy-Ontology.ps1 -WorkspaceId "your-workspace-guid"
```

```
  +==============================================================+
  |    Microsoft Fabric IQ Ontology Accelerator                   |
  |    Multi-Domain Deployment                                    |
  +==============================================================+
  |                                                               |
  |    [1]  Oil & Gas Refinery                                    |
  |    [2]  Smart Building                                        |
  |    [3]  Manufacturing Plant                                   |
  |    [4]  IT Asset Management                                   |
  |    [5]  Wind Turbine / Wind Farm                              |
  |                                                               |
  +==============================================================+
```

### :wrench: Direct Domain Selection

```powershell
# Deploy a specific domain
.\Deploy-Ontology.ps1 -WorkspaceId "guid" -OntologyType SmartBuilding
.\Deploy-Ontology.ps1 -WorkspaceId "guid" -OntologyType WindTurbine

# Skip optional components
.\Deploy-Ontology.ps1 -WorkspaceId "guid" -OntologyType ITAsset -SkipDataAgent -SkipDashboard

# Original Oil & Gas (backward compatible)
.\Deploy-OilGasOntology.ps1 -WorkspaceId "your-workspace-guid"
```

> [!TIP]
> **Prerequisites:** PowerShell 5.1+, Az module, Fabric workspace (F2+ capacity). See [SETUP_GUIDE.md](SETUP_GUIDE.md) for detailed instructions.

---

## :gear: What Gets Deployed

Each domain deploys **8 Fabric items** in a coordinated pipeline:

```mermaid
flowchart TB
    subgraph Step1["1 - Data Layer"]
        direction LR
        LH["Lakehouse\nDelta tables from CSVs"]
        NB["Notebook\nPySpark to Delta"]
        EH["Eventhouse\nKQL database + tables"]
    end

    subgraph Step2["2 - Model Layer"]
        direction LR
        SM["Semantic Model\nDirect Lake + rels"]
        ONT["Ontology\nEntity types + rels"]
        GM["Graph Model\nTopology from ontology"]
    end

    subgraph Step3["3 - Experience Layer"]
        direction LR
        GQS["Graph Query Set\nGQL traversal queries"]
        DASH["RTI Dashboard\n10-12 KQL tiles"]
        DA["Data Agent\nNL queries on Lakehouse"]
        OA["Operations Agent\nRTI monitoring + Teams"]
    end

    Step1 --> Step2 --> Step3

    style Step1 fill:#E8F0FE,stroke:#0078D4,color:#000
    style Step2 fill:#F3E8F9,stroke:#742774,color:#000
    style Step3 fill:#E8F8E8,stroke:#107C10,color:#000
```

| # | Item | Per Domain | Description |
|---|------|:----------:|-------------|
| :file_cabinet: | **Lakehouse** | 11-14 tables | CSV to Delta via PySpark notebook |
| :triangular_ruler: | **Semantic Model** | 13-17 rels | Direct Lake mode, star/snowflake schema |
| :satellite: | **Eventhouse** | 5 KQL tables | Enriched telemetry + operational metrics |
| :dna: | **Ontology** | 11-13 types | Entity types, properties, relationships |
| :spider_web: | **Graph Model** | auto | Topology derived from ontology |
| :mag: | **Graph Query Set** | 20 queries | GQL traversal patterns |
| :bar_chart: | **RTI Dashboard** | 10-12 tiles | Real-time KQL visualizations |
| :robot: | **AI Agents** | 2 agents | Data Agent + Operations Agent |
---

## :building_construction: Architecture

### :open_file_folder: Project Structure

```
OntologyAccelerator/
|-- README.md                                <-- You are here
|-- SETUP_GUIDE.md                           <-- Step-by-step Fabric setup
|-- SEMANTIC_MODEL_GUIDE.md                  <-- Power BI model configuration
|-- AGENTS.md                                <-- Multi-agent architecture
|-- DEVELOPMENT_PLAN.md                      <-- Sprint roadmap
|-- Enrich-SampleData.ps1                    <-- Data enrichment tool
|
|-- Deploy-Ontology.ps1                      <-- Multi-domain entry point
|-- Deploy-OilGasOntology.ps1                <-- Original single-domain script
|
|-- deploy/                                  <-- Shared deployment engine
|   |-- Deploy-GenericOntology.ps1           <-- Generic deployment orchestrator
|   |-- Build-Ontology.ps1                   <-- Ontology definition builder
|   |-- Build-GraphModel-v2.ps1              <-- Graph model builder
|   |-- Deploy-GraphQuerySet.ps1             <-- GQL query set deployer
|   |-- Deploy-KqlTables.ps1                 <-- KQL table creation (fallback)
|   |-- Deploy-RTIDashboard.ps1              <-- Dashboard deployer (fallback)
|   |-- Deploy-DataAgent.ps1                 <-- Data Agent deployer (fallback)
|   |-- Deploy-OperationsAgent.ps1           <-- Operations Agent deployer (fallback)
|   |-- LoadDataToTables.py                  <-- PySpark notebook template
|   |-- Validate-Deployment.ps1              <-- Post-deploy validation
|   +-- SemanticModel/                       <-- TMDL semantic model (Direct Lake)
|
|-- ontologies/
|   |-- OilGasRefinery/                      <-- Oil & Gas domain
|   |   |-- Build-Ontology.ps1
|   |   |-- GraphQueries.gql
|   |   |-- Deploy-KqlTables.ps1             <-- Domain-specific KQL
|   |   |-- Deploy-RTIDashboard.ps1          <-- Domain-specific dashboard
|   |   |-- Deploy-DataAgent.ps1             <-- Domain-specific AI agent
|   |   |-- Deploy-OperationsAgent.ps1       <-- Domain-specific ops agent
|   |   +-- data/ (14 CSVs)
|   |-- SmartBuilding/                       <-- Smart Building domain  
|   |-- ManufacturingPlant/                  <-- Manufacturing domain
|   |-- ITAsset/                             <-- IT Asset domain
|   +-- WindTurbine/                         <-- Wind Turbine domain
|       +-- (same structure per domain)
|
|-- diagrams/
|   +-- ontology_diagram.md
|
+-- .github/agents/                          <-- 7 Copilot agent definitions
    +-- shared.instructions.md
```

### :arrows_counterclockwise: Domain Script Resolution

The generic deployer uses a **domain-first fallback** pattern:

```mermaid
flowchart LR
    A["Deploy-GenericOntology.ps1"] --> B{"Script in\nontologies/Domain/?"}
    B -- Yes --> C["Use domain-specific"]
    B -- No --> D["Fallback to deploy/"]

    style A fill:#742774,color:#fff
    style C fill:#107C10,color:#fff
    style D fill:#0078D4,color:#fff
```
---

## :factory: Domain Details

<details>
<summary><h3>:oil_drum: Oil & Gas Refinery</h3></summary>

**13 entity types** | **17 relationships** | **14 CSVs** | **445 rows**

```mermaid
graph TB
    subgraph Assets["Physical Assets"]
        REF["Refinery"]
        PU["ProcessUnit"]
        EQ["Equipment"]
        PL["Pipeline"]
        TK["StorageTank"]
    end

    subgraph Materials["Materials"]
        CO["CrudeOil"]
        RP["RefinedProduct"]
    end

    subgraph Monitoring["Monitoring"]
        SN["Sensor"]
        AL["SafetyAlarm"]
    end

    subgraph Operations["Operations"]
        ME["MaintenanceEvent"]
        EM["Employee"]
        PR["ProductionRecord"]
    end

    REF -->|contains| PU
    PU -->|hasEquipment| EQ
    CO -->|feeds| PU
    PU -->|produces| RP
    PL -->|connects| PU
    TK -->|stores| RP
    TK -->|locatedAt| REF
    SN -->|monitors| EQ
    AL -->|raisedBy| SN
    ME -->|targets| EQ
    ME -->|performedBy| EM
    EM -->|assignedTo| REF

    style REF fill:#742774,color:#fff
    style PU fill:#742774,color:#fff
    style EQ fill:#742774,color:#fff
```

**KQL Tables:** SensorReading | EquipmentAlert | ProcessMetric | PipelineFlow | TankLevel

**Dashboard:** 12 tiles (sensor lines, alert pies, refinery map, throughput, tank levels)

</details>

<details>
<summary><h3>:office: Smart Building</h3></summary>

**12 entity types** | **11 relationships** | **13 CSVs** | **498 rows**

```mermaid
graph TB
    subgraph Infra["Infrastructure"]
        BLD["Building"]
        FLR["Floor"]
        ZN["Zone"]
    end

    subgraph Systems["Systems"]
        HVAC["HVAC"]
        LGT["Lighting"]
        ELV["Elevator"]
        EM["EnergyMeter"]
    end

    subgraph Monitor["Monitoring"]
        SN["Sensor"]
        AP["AccessPoint"]
        AL["Alert"]
    end

    BLD -->|has| FLR
    FLR -->|contains| ZN
    ZN -->|equippedWith| HVAC
    ZN -->|equippedWith| LGT
    ZN -->|has| SN
    BLD -->|has| ELV
    SN -->|triggers| AL

    style BLD fill:#0078D4,color:#fff
    style FLR fill:#0078D4,color:#fff
    style ZN fill:#0078D4,color:#fff
```

**KQL Tables:** SensorReading | BuildingAlert | HVACMetric | EnergyConsumption | OccupancyMetric

**Dashboard:** 10 tiles (HVAC efficiency, energy cost, zone occupancy, anomalies)

</details>

<details>
<summary><h3>:factory: Manufacturing Plant</h3></summary>

**11 entity types** | **11 relationships** | **12 CSVs** | **444 rows**

```mermaid
graph TB
    subgraph Plant["Plant Floor"]
        PLT["Plant"]
        LN["ProductionLine"]
        MC["Machine"]
    end

    subgraph Prod["Production"]
        PRD["Product"]
        MAT["Material"]
        BATCH["ProductionBatch"]
    end

    subgraph Quality["Quality"]
        QC["QualityCheck"]
        SN["Sensor"]
        AL["Alert"]
    end

    PLT -->|has| LN
    LN -->|contains| MC
    MC -->|monitored_by| SN
    BATCH -->|produces| PRD
    BATCH -->|uses| MAT
    QC -->|checks| BATCH
    SN -->|triggers| AL

    style PLT fill:#107C10,color:#fff
    style LN fill:#107C10,color:#fff
    style MC fill:#107C10,color:#fff
```

**KQL Tables:** SensorReading | PlantAlert | ProductionMetric | MachineHealth | QualityMetric

**Dashboard:** 10 tiles (OEE bar, machine health, production throughput, defect rate, quality)

</details>

<details>
<summary><h3>:desktop_computer: IT Asset Management</h3></summary>

**11 entity types** | **10 relationships** | **12 CSVs** | **381 rows**

```mermaid
graph TB
    subgraph Infra["Infrastructure"]
        DC["DataCenter"]
        RK["Rack"]
        SRV["Server"]
        VM["VirtualMachine"]
    end

    subgraph Software["Software"]
        APP["Application"]
        DB["Database"]
        LIC["License"]
    end

    subgraph Ops["Operations"]
        NET["NetworkDevice"]
        USR["User"]
        INC["Incident"]
    end

    DC -->|contains| RK
    RK -->|hosts| SRV
    SRV -->|runs| VM
    VM -->|hosts| APP
    APP -->|uses| DB
    INC -->|affects| SRV
    INC -->|reported_by| USR

    style DC fill:#FF6F00,color:#fff
    style RK fill:#FF6F00,color:#fff
    style SRV fill:#FF6F00,color:#fff
```

**KQL Tables:** ServerMetric | InfraAlert | ApplicationHealth | NetworkMetric | IncidentMetric

**Dashboard:** 10 tiles (CPU/memory lines, app health, network bandwidth, incident resolution)

</details>

<details>
<summary><h3>:wind_face: Wind Turbine / Wind Farm</h3></summary>

**12 entity types** | **12 relationships** | **13 CSVs** | **651 rows**

```mermaid
graph TB
    subgraph Fleet["Wind Fleet"]
        WF["WindFarm"]
        WT["Turbine"]
        NC["Nacelle"]
        BL["Blade"]
        TW["Tower"]
    end

    subgraph Elec["Electrical"]
        TR["Transformer"]
        PO["PowerOutput"]
    end

    subgraph Monitor["Monitoring"]
        SN["Sensor"]
        WS["WeatherStation"]
        AL["Alert"]
    end

    subgraph Ops["Operations"]
        TN["Technician"]
        ME["MaintenanceEvent"]
    end

    WF -->|contains| WT
    WT -->|has| NC
    WT -->|has| BL
    WT -->|has| TW
    WT -->|monitored_by| SN
    WF -->|has| WS
    WF -->|has| TR
    PO -->|generated_by| WT
    AL -->|affects| WT
    ME -->|performed_on| WT
    ME -->|performed_by| TN

    style WF fill:#00897B,color:#fff
    style WT fill:#00897B,color:#fff
    style NC fill:#00897B,color:#fff
```

**KQL Tables:** TurbineReading | TurbineAlert | PowerOutputMetric | WeatherMetric | MaintenanceMetric

**Dashboard:** 10 tiles (power output, capacity factor, wind vs power scatter, icing risk, weather)

</details>
---

## :bar_chart: KQL Real-Time Dashboards

<p align="center">
  <img src="https://img.shields.io/badge/total%20tiles-52-0078D4?style=for-the-badge" alt="52 tiles"/>
  <img src="https://img.shields.io/badge/KQL%20tables-25-742774?style=for-the-badge" alt="25 KQL tables"/>
  <img src="https://img.shields.io/badge/auto--refresh-30s-107C10?style=for-the-badge" alt="30s refresh"/>
</p>

Each domain deploys its own RTI dashboard with domain-specific KQL queries:

| Domain | Dashboard | Tiles | Key Visuals |
|--------|-----------|:-----:|-------------|
| :oil_drum: Oil & Gas | `RefineryTelemetryDashboard` | 12 | Sensor lines, refinery map, tank levels, alert pies |
| :office: Smart Building | `SmartBuildingDashboard` | 10 | HVAC efficiency, energy cost, zone occupancy |
| :factory: Manufacturing | `ManufacturingDashboard` | 10 | OEE bars, machine health, defect trends |
| :desktop_computer: IT Asset | `ITAssetDashboard` | 10 | CPU/memory lines, app health, incident resolution |
| :wind_face: Wind Turbine | `WindTurbineDashboard` | 10 | Power output, wind-power scatter, icing risk |

---

## :spider_web: Graph Query Set (GQL)

<p align="center">
  <img src="https://img.shields.io/badge/ISO%2FIEC%2039075-2024-107C10?style=for-the-badge" alt="ISO GQL"/>
  <img src="https://img.shields.io/badge/queries%20per%20domain-20-742774?style=for-the-badge" alt="20 per domain"/>
</p>

Each domain includes **20 GQL queries** covering:

| Pattern | Example |
|---------|---------|
| :globe_with_meridians: Full topology | `MATCH (n)-[e]->(m) RETURN n, e, m` |
| :mag: Entity drill-down | `MATCH (r:Refinery)-[:contains]->(pu) RETURN r, pu` |
| :link: Multi-hop traversal | Crude to ProcessUnit to Product (3+ hops) |
| :bar_chart: Aggregations | Alert counts by severity, maintenance costs |
| :warning: Anti-patterns | Equipment without recent maintenance |

> [!NOTE]
> Due to a Fabric REST API limitation, GQL queries are deployed as an empty shell. Copy-paste queries from the domain `GraphQueries.gql` file via Fabric UI.

---

## :robot: AI Agents

<p align="center">
  <img src="https://img.shields.io/badge/Data%20Agents-5-FF6F00?style=for-the-badge" alt="5 Data Agents"/>
  <img src="https://img.shields.io/badge/Operations%20Agents-5-742774?style=for-the-badge" alt="5 Ops Agents"/>
  <img src="https://img.shields.io/badge/Microsoft%20Teams-6264A7?style=for-the-badge&logo=microsoftteams&logoColor=white" alt="Teams"/>
</p>

```mermaid
flowchart LR
    subgraph DataAgent["Data Agent"]
        LH["Lakehouse"] --> DA["NL queries\nover graph model"]
    end

    subgraph OpsAgent["Operations Agent"]
        KQL["KQL Database"] --> OA["Real-time monitoring\n5 operational goals"]
        OA --> TEAMS["Microsoft Teams\nProactive alerts"]
    end

    style DA fill:#FF6F00,color:#fff
    style OA fill:#742774,color:#fff
    style TEAMS fill:#6264A7,color:#fff
```

| Domain | Data Agent | Ops Agent | Operational Goals |
|--------|:----------:|:---------:|-------------------|
| :oil_drum: Oil & Gas | :white_check_mark: | :white_check_mark: | Equipment health - Safety - Production - Maintenance - Supply chain |
| :office: Smart Building | :white_check_mark: | :white_check_mark: | HVAC comfort - Energy optimization - Safety - Alerts - Maintenance |
| :factory: Manufacturing | :white_check_mark: | :white_check_mark: | Production efficiency - Machine health - Quality - Safety - Maintenance |
| :desktop_computer: IT Asset | :white_check_mark: | :white_check_mark: | Server health - App performance - Network - Incidents - Capacity |
| :wind_face: Wind Turbine | :white_check_mark: | :white_check_mark: | Turbine performance - Predictive maintenance - Weather - Grid - Fleet |

---

## :robot: Multi-Agent Development

This project uses **7 specialized Copilot agents** for AI-assisted development:

```mermaid
flowchart TB
    ORCH["Orchestrator\nEnd-to-end deployment"]

    ORCH --> OD["Ontology Designer\nEntity types, IDs, relationships"]
    ORCH --> DM["Data Modeler\nCSV schemas, FK integrity"]
    ORCH --> GB["Graph Builder\nGQL queries, traversals"]
    ORCH --> DEP["Deployer\nFabric REST API, Lakehouse"]
    ORCH --> DB["Dashboard Builder\nKQL tiles, RTI visuals"]
    ORCH --> AB["Agent Builder\nData + Operations agents"]

    style ORCH fill:#742774,color:#fff
    style OD fill:#107C10,color:#fff
    style DM fill:#0078D4,color:#fff
    style GB fill:#107C10,color:#fff
    style DEP fill:#FF6F00,color:#fff
    style DB fill:#0078D4,color:#fff
    style AB fill:#FF6F00,color:#fff
```

Agents auto-activate based on the file you are editing. See [AGENTS.md](AGENTS.md) for full details.

---

## :shield: Shared Constraints

| Constraint | Detail |
|:----------:|--------|
| :wrench: | **PowerShell 5.1+** --- no `&&`, use `;` for chaining |
| :key: | **Deterministic GUIDs** --- MD5 hash for idempotent deployments |
| :arrows_counterclockwise: | **Fabric REST API** --- 202 polling, 429 retry-after, LRO handling |
| :id: | **ID allocation** --- Entities 1001+, Properties 2001+, Relationships 3001+, Timeseries 4001+ |
| :outbox_tray: | **OneLake DFS** --- PUT resource=file then PATCH append then PATCH flush |

---

## :clipboard: Adding a New Domain

```mermaid
flowchart LR
    A["1 Create folder\nontologies/Domain/"] --> B["2 Add CSVs\ndata/*.csv"]
    B --> C["3 Build ontology\nBuild-Ontology.ps1"]
    C --> D["4 Write GQL\nGraphQueries.gql"]
    D --> E["5 Add KQL/Dashboard\nDeploy-*.ps1"]
    E --> F["6 Register domain\nDeploy-Ontology.ps1"]

    style A fill:#742774,color:#fff
    style F fill:#107C10,color:#fff
```

1. Create `ontologies/<DomainName>/` with `data/`, `Build-Ontology.ps1`, `GraphQueries.gql`
2. Add domain-specific: `Deploy-KqlTables.ps1`, `Deploy-RTIDashboard.ps1`, `Deploy-DataAgent.ps1`, `Deploy-OperationsAgent.ps1`
3. Add domain entry to `$domains` hashtable in `Deploy-Ontology.ps1`
4. Run `.\Deploy-Ontology.ps1 -WorkspaceId "guid" -OntologyType <DomainName>`

---

## :books: Documentation

| | Document | Description |
|---|----------|-------------|
| :page_facing_up: | [README.md](README.md) | This overview |
| :wrench: | [SETUP_GUIDE.md](SETUP_GUIDE.md) | Prerequisites, tenant settings, step-by-step setup |
| :triangular_ruler: | [SEMANTIC_MODEL_GUIDE.md](SEMANTIC_MODEL_GUIDE.md) | Power BI semantic model configuration |
| :robot: | [AGENTS.md](AGENTS.md) | Multi-agent architecture and Copilot agent definitions |
| :clipboard: | [DEVELOPMENT_PLAN.md](DEVELOPMENT_PLAN.md) | Sprint roadmap and development plan |

---

## :scroll: License

MIT

---

<p align="center">
  <sub>Built with :heart: for the Microsoft Fabric community</sub>
</p>