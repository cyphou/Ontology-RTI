---
applyTo: '**'
---
# Shared Instructions for All Agents

## Hard Constraints

1. **PowerShell 5.1 Compatibility**
   - Never use `&&` to chain commands; use `;` or separate statements
   - Avoid `ConvertTo-Json` for payloads > 50KB — build JSON strings manually
   - Use `Invoke-WebRequest` with `-UseBasicParsing` for PS 5.1 compatibility
   - String interpolation: use `$variable` inside double-quoted strings

2. **Fabric REST API Patterns**
   - Base URL: `https://api.fabric.microsoft.com/v1`
   - OneLake Base: `https://onelake.dfs.fabric.microsoft.com`
   - Always handle HTTP 202 (poll `Location` header for LRO status)
   - Always handle HTTP 429 with `Retry-After` header (default 30s)
   - Handle `isRetriable:true` errors with 15s retry
   - Token: `Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"`
   - Storage token: `Get-AzAccessToken -ResourceTypeName Storage`

3. **Ontology Definition Format**
   - Entity types: Base64-encoded JSON in `EntityTypes/{id}/definition.json`
   - Relationships: Base64-encoded JSON in `RelationshipTypes/{id}/definition.json`
   - Data bindings: `EntityTypes/{id}/DataBindings/{guid}.json`
   - Contextualizations: `RelationshipTypes/{id}/Contextualizations/{guid}.json`
   - Platform: `.platform` file with metadata type, displayName, description

4. **ID Allocation Scheme**
   - Entity type IDs: 1001–1099
   - Property IDs: 2001–2999 (grouped by entity: 2001-2099 for entity 1001, etc.)
   - Relationship IDs: 3001–3099
   - Timeseries property IDs: 4001–4099

5. **Deterministic GUIDs**
   - Use `DeterministicGuid` function with MD5 hash for idempotent binding IDs
   - Seed pattern: `"NonTimeSeries-{entityId}"`, `"TimeSeries-{entityId}"`, `"Ctx-{relId}"`

6. **Data Conventions**
   - Lakehouse CSV files: dimension tables (Dim*), fact tables (Fact*), bridge tables (Bridge*)
   - Telemetry file: `SensorTelemetry.csv` → Eventhouse KQL table
   - Table names in ontology bindings: lowercase (e.g., `dimrefinery`, `factproduction`)
   - All IDs use string type with descriptive prefixes (e.g., `WF-001`, `WT-001`)

7. **Error Handling**
   - Always check for `ItemDisplayNameAlreadyInUse` and fall back to lookup by name
   - Read error response body from `$_.Exception.Response.GetResponseStream()`
   - Retry up to 3 times for notebook creation/definition/execution
   - Log warnings but continue deployment (non-blocking failures)

## Project Structure
```
Deploy-Ontology.ps1           # Multi-domain selector (entry point)
Deploy-OilGasOntology.ps1     # Oil & Gas specific (backward compat)
deploy/
  Deploy-GenericOntology.ps1   # Generic engine for new domains
  Build-Ontology.ps1           # Oil & Gas ontology builder
  Deploy-KqlTables.ps1         # KQL table creation
  Deploy-RTIDashboard.ps1      # RTI Dashboard
  Deploy-DataAgent.ps1         # Data Agent (F64+)
  Deploy-OperationsAgent.ps1   # Operations Agent
  Deploy-GraphQuerySet.ps1     # Graph Query Set
  Deploy-Eventstream.ps1       # Eventstream deployer
  Deploy-BulkImport.ps1        # Bulk entity import
  Send-TelemetrySimulator.ps1  # Real-time event simulator
  New-OntologyDomain.ps1       # Domain scaffolding wizard
  Validate-Deployment.ps1      # Post-deploy validation (9 item types)
  Build-GraphModel-v2.ps1      # Graph model builder
  LoadDataToTables.py          # PySpark notebook source
  SemanticModel/               # TMDL definition files
ontologies/
  OilGasRefinery/              # Oil & Gas Refinery domain
  SmartBuilding/               # Smart Building domain
  ManufacturingPlant/          # Manufacturing Plant domain
  ITAsset/                     # IT Asset Management domain
  WindTurbine/                 # Wind Turbine domain
  Healthcare/                  # Healthcare domain
.github/
  workflows/ci.yml             # GitHub Actions CI pipeline
  agents/                      # 7 Copilot agent definitions
```
