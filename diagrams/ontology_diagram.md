# Ontology Diagrams — All 6 Domains

## Domain Overview

| Domain | Entity Types | Relationships | KQL Tables |
|--------|:-----------:|:------------:|:----------:|
| Oil & Gas Refinery | 13 | 17 | 5 |
| Smart Building | 12 | 11 | 5 |
| Manufacturing Plant | 11 | 11 | 5 |
| IT Asset Management | 11 | 10 | 5 |
| Wind Turbine | 12 | 12 | 5 |
| Healthcare | 9 | 7 | 5 |

---

## Oil & Gas Refinery

### Entity-Relationship Model (Mermaid)

```mermaid
graph TB
    subgraph "Core Assets"
        REF[🏭 Refinery]
        PU[⚙️ ProcessUnit]
        EQ[🔧 Equipment]
        PL[🔗 Pipeline]
    end

    subgraph "Materials"
        CO[🛢️ CrudeOil]
        RP[⛽ RefinedProduct]
    end

    subgraph "Storage"
        TK[🗄️ StorageTank]
    end

    subgraph "Monitoring & Safety"
        SN[📡 Sensor]
        AL[🚨 SafetyAlarm]
    end

    subgraph "Operations"
        ME[🔩 MaintenanceEvent]
        EM[👷 Employee]
    end

    REF -->|contains| PU
    PU -->|hasEquipment| EQ
    CO -->|feeds| PU
    PU -->|produces| RP
    PL -->|connectsFrom| PU
    PL -->|connectsTo| PU
    TK -->|stores| RP
    TK -->|locatedAt| REF
    SN -->|monitors| EQ
    AL -->|raisedBy| SN
    ME -->|targets| EQ
    ME -->|performedBy| EM
    EM -->|assignedTo| REF
```

## Entity Type Details

### Refinery
```
┌─────────────────────────────────┐
│           REFINERY              │
├─────────────────────────────────┤
│ 🔑 RefineryId          (Key)   │
│    RefineryName         (String)│
│    Country              (String)│
│    State                (String)│
│    City                 (String)│
│    Latitude             (Double)│
│    Longitude            (Double)│
│    CapacityBPD          (Int)   │
│    YearBuilt            (Int)   │
│    Status               (String)│
│    Operator             (String)│
└─────────────────────────────────┘
```

### ProcessUnit
```
┌─────────────────────────────────┐
│         PROCESS UNIT            │
├─────────────────────────────────┤
│ 🔑 ProcessUnitId       (Key)   │
│    ProcessUnitName      (String)│
│    ProcessUnitType      (String)│
│ 🔗 RefineryId          (FK)    │
│    CapacityBPD          (Int)   │
│    DesignTemperatureF   (Double)│
│    DesignPressurePSI    (Double)│
│    YearInstalled        (Int)   │
│    Status               (String)│
│    Description          (String)│
└─────────────────────────────────┘
```

### Equipment
```
┌─────────────────────────────────┐
│          EQUIPMENT              │
├─────────────────────────────────┤
│ 🔑 EquipmentId         (Key)   │
│    EquipmentName        (String)│
│    EquipmentType        (String)│
│ 🔗 ProcessUnitId       (FK)    │
│    Manufacturer         (String)│
│    Model                (String)│
│    InstallDate          (Date)  │
│    LastInspectionDate   (Date)  │
│    Status               (String)│
│    CriticalityLevel     (String)│
│    ExpectedLifeYears    (Int)   │
└─────────────────────────────────┘
```

### Pipeline
```
┌─────────────────────────────────┐
│           PIPELINE              │
├─────────────────────────────────┤
│ 🔑 PipelineId          (Key)   │
│    PipelineName         (String)│
│ 🔗 FromProcessUnitId   (FK)    │
│ 🔗 ToProcessUnitId     (FK)    │
│ 🔗 RefineryId          (FK)    │
│    DiameterInches       (Double)│
│    LengthFeet           (Double)│
│    Material             (String)│
│    MaxFlowBPD           (Int)   │
│    InstalledDate        (Date)  │
│    Status               (String)│
└─────────────────────────────────┘
```

### CrudeOil
```
┌─────────────────────────────────┐
│          CRUDE OIL              │
├─────────────────────────────────┤
│ 🔑 CrudeOilId          (Key)   │
│    CrudeGradeName       (String)│
│    APIGravity           (Double)│
│    SulfurContentPct     (Double)│
│    Origin               (String)│
│    Classification       (String)│
│    PricePerBarrelUSD    (Double)│
│    Description          (String)│
└─────────────────────────────────┘
```

### RefinedProduct
```
┌─────────────────────────────────┐
│       REFINED PRODUCT           │
├─────────────────────────────────┤
│ 🔑 ProductId            (Key)  │
│    ProductName           (String│
│    ProductCategory       (String│
│    APIGravity            (Double│
│    SulfurLimitPPM        (Int)  │
│    FlashPointF           (String│
│    SpecStandard          (String│
│    PricePerBarrelUSD     (Double│
│    Description           (String│
└─────────────────────────────────┘
```

### StorageTank
```
┌─────────────────────────────────┐
│         STORAGE TANK            │
├─────────────────────────────────┤
│ 🔑 TankId              (Key)   │
│    TankName             (String)│
│ 🔗 RefineryId          (FK)    │
│ 🔗 ProductId           (FK)    │
│    TankType             (String)│
│    CapacityBarrels      (Int)   │
│    CurrentLevelBarrels  (Int)   │
│    DiameterFeet         (Double)│
│    HeightFeet           (Double)│
│    Material             (String)│
│    Status               (String)│
│    LastInspectionDate   (Date)  │
└─────────────────────────────────┘
```

### Sensor
```
┌─────────────────────────────────┐
│            SENSOR               │
├─────────────────────────────────┤
│ 🔑 SensorId            (Key)   │
│    SensorName           (String)│
│    SensorType           (String)│
│ 🔗 EquipmentId         (FK)    │
│    MeasurementUnit      (String)│
│    MinRange             (Double)│
│    MaxRange             (Double)│
│    InstallDate          (Date)  │
│    CalibrationDate      (Date)  │
│    Status               (String)│
│    Manufacturer         (String)│
└─────────────────────────────────┘
```

### MaintenanceEvent
```
┌──────────────────────────────────┐
│      MAINTENANCE EVENT           │
├──────────────────────────────────┤
│ 🔑 MaintenanceId        (Key)   │
│ 🔗 EquipmentId          (FK)    │
│    MaintenanceType       (String)│
│    Priority              (String)│
│ 🔗 PerformedByEmployeeId(FK)    │
│    StartDate             (Date)  │
│    EndDate               (Date)  │
│    DurationHours         (Double)│
│    CostUSD              (Double) │
│    Description           (String)│
│    WorkOrderNumber       (String)│
│    Status                (String)│
└──────────────────────────────────┘
```

### SafetyAlarm
```
┌──────────────────────────────────┐
│         SAFETY ALARM             │
├──────────────────────────────────┤
│ 🔑 AlarmId              (Key)   │
│ 🔗 SensorId             (FK)    │
│    AlarmType             (String)│
│    Severity              (String)│
│    AlarmTimestamp         (DateTime)│
│    AcknowledgedTimestamp  (DateTime)│
│    ClearedTimestamp       (DateTime)│
│    AlarmValue            (Double)│
│    ThresholdValue        (Double)│
│    Description           (String)│
│    ActionTaken           (String)│
│ 🔗 AcknowledgedByEmployeeId(FK) │
└──────────────────────────────────┘
```

### Employee
```
┌─────────────────────────────────┐
│           EMPLOYEE              │
├─────────────────────────────────┤
│ 🔑 EmployeeId          (Key)   │
│    FirstName            (String)│
│    LastName             (String)│
│    Role                 (String)│
│    Department           (String)│
│ 🔗 RefineryId          (FK)    │
│    HireDate             (Date)  │
│    CertificationLevel   (String)│
│    ShiftPattern         (String)│
│    Status               (String)│
└─────────────────────────────────┘
```

## Relationship Cardinalities

```
Refinery ──(1)────(N)──> ProcessUnit       "A refinery contains many process units"
ProcessUnit ──(1)────(N)──> Equipment      "A process unit has many equipment items"
CrudeOil ──(N)────(N)──> ProcessUnit       "Crude oils feed into process units (via bridge table)"
ProcessUnit ──(N)────(N)──> RefinedProduct "Process units produce products (via FactProduction)"
Pipeline ──(N)────(1)──> ProcessUnit [From] "Pipeline connects from a source process unit"
Pipeline ──(N)────(1)──> ProcessUnit [To]   "Pipeline connects to a target process unit"
StorageTank ──(N)────(1)──> RefinedProduct  "A tank stores one product type"
StorageTank ──(N)────(1)──> Refinery        "A tank is located at one refinery"
Sensor ──(N)────(1)──> Equipment            "Sensors monitor equipment"
SafetyAlarm ──(N)────(1)──> Sensor          "Alarms are raised by sensors"
MaintenanceEvent ──(N)────(1)──> Equipment  "Maintenance targets equipment"
MaintenanceEvent ──(N)────(1)──> Employee   "Maintenance performed by employee"
Employee ──(N)────(1)──> Refinery           "Employees assigned to a refinery"
```

---

## Smart Building

```mermaid
graph TB
    subgraph Infrastructure
        BLD[Building]
        FLR[Floor]
        ZN[Zone]
    end
    subgraph Systems
        HVAC[HVAC]
        LGT[Lighting]
        ELV[Elevator]
        EM[EnergyMeter]
    end
    subgraph Monitoring
        SN[Sensor]
        AP[AccessPoint]
        AL[Alert]
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

---

## Manufacturing Plant

```mermaid
graph TB
    subgraph PlantFloor
        PLT[Plant]
        LN[ProductionLine]
        MC[Machine]
    end
    subgraph Production
        PRD[Product]
        MAT[Material]
        BATCH[ProductionBatch]
    end
    subgraph Quality
        QC[QualityCheck]
        SN[Sensor]
        AL[Alert]
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

---

## IT Asset Management

```mermaid
graph TB
    subgraph Infra
        DC[DataCenter]
        RK[Rack]
        SRV[Server]
        VM[VirtualMachine]
    end
    subgraph Software
        APP[Application]
        DB[Database]
        LIC[License]
    end
    subgraph Ops
        NET[NetworkDevice]
        INC[Incident]
    end
    DC -->|contains| RK
    RK -->|hosts| SRV
    SRV -->|runs| VM
    VM -->|hosts| APP
    APP -->|uses| DB
    INC -->|affects| SRV
    style DC fill:#FF6F00,color:#fff
    style RK fill:#FF6F00,color:#fff
    style SRV fill:#FF6F00,color:#fff
```

---

## Wind Turbine

```mermaid
graph TB
    subgraph Fleet
        WF[WindFarm]
        WT[Turbine]
        NC[Nacelle]
        BL[Blade]
        TW[Tower]
    end
    subgraph Electrical
        TR[Transformer]
        PO[PowerOutput]
    end
    subgraph Mon[Monitoring]
        SN[Sensor]
        WS[WeatherStation]
        AL[Alert]
    end
    subgraph MntOps[Operations]
        TN[Technician]
        ME[MaintenanceEvent]
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
```

---

## Healthcare

```mermaid
graph TB
    subgraph Facility
        HOS[Hospital]
        DEP[Department]
        WRD[Ward]
    end
    subgraph Staff
        PHY[Physician]
        NRS[Nurse]
    end
    subgraph PatientCare[Patient Care]
        PAT[Patient]
        MED[Medication]
        DEV[MedicalDevice]
        SN[Sensor]
    end
    HOS -->|has| DEP
    DEP -->|contains| WRD
    WRD -->|admits| PAT
    WRD -->|staffedBy| NRS
    WRD -->|equippedWith| DEV
    DEV -->|monitored_by| SN
    PHY -->|assignedTo| DEP
    style HOS fill:#D32F2F,color:#fff
    style DEP fill:#D32F2F,color:#fff
    style WRD fill:#D32F2F,color:#fff
```
