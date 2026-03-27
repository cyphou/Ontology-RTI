<#
.SYNOPSIS
    Scaffolds a new ontology domain with template files.
.DESCRIPTION
    Creates the full folder structure and template files for a new IQ Ontology
    domain, including data CSVs, deployment scripts, GQL queries, Semantic Model,
    and registers the domain in Deploy-Ontology.ps1 and Validate-Deployment.ps1.

.PARAMETER DomainName
    PascalCase domain name (e.g. "SmartAgriculture", "RetailStore").

.PARAMETER DisplayName
    Human-readable display name (e.g. "Smart Agriculture", "Retail Store").

.PARAMETER Emoji
    Single emoji for menu display (default: star).

.PARAMETER Entities
    Comma-separated list of entity names (e.g. "Farm,Field,Crop,Sensor,Equipment").

.EXAMPLE
    .\New-OntologyDomain.ps1 -DomainName "SmartAgriculture" -DisplayName "Smart Agriculture" -Emoji "🌾" -Entities "Farm,Field,Crop,Sensor,Equipment"
#>
param(
    [Parameter(Mandatory=$true)]
    [string]$DomainName,

    [Parameter(Mandatory=$true)]
    [string]$DisplayName,

    [string]$Emoji = "⭐",

    [Parameter(Mandatory=$true)]
    [string]$Entities
)

$ErrorActionPreference = "Stop"
$rootDir = Split-Path -Parent $PSScriptRoot
$domainDir = Join-Path $rootDir "ontologies\$DomainName"
$entityList = $Entities.Split(",") | ForEach-Object { $_.Trim() }

if (Test-Path $domainDir) {
    Write-Host "[ERROR] Domain folder already exists: $domainDir" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== New Ontology Domain Scaffolding ===" -ForegroundColor Cyan
Write-Host "  Domain:   $DomainName"
Write-Host "  Display:  $DisplayName"
Write-Host "  Emoji:    $Emoji"
Write-Host "  Entities: $($entityList -join ', ')"
Write-Host ""

# ── Create directory structure ──────────────────────────────────────────────
$dirs = @(
    $domainDir,
    (Join-Path $domainDir "data"),
    (Join-Path $domainDir "SemanticModel"),
    (Join-Path $domainDir "SemanticModel\definition"),
    (Join-Path $domainDir "SemanticModel\definition\tables")
)
foreach ($d in $dirs) {
    New-Item -ItemType Directory -Path $d -Force | Out-Null
}
Write-Host "[OK] Directory structure created" -ForegroundColor Green

# ── Generate entity dimension CSVs ─────────────────────────────────────────
foreach ($entity in $entityList) {
    $csvPath = Join-Path $domainDir "data\Dim$entity.csv"
    $idCol = "${entity}Id"
    $nameCol = "${entity}Name"
    $header = "$idCol,$nameCol,Status"
    $rows = @($header)
    for ($i = 1; $i -le 5; $i++) {
        $rows += "$($entity.Substring(0,3).ToUpper())-$('{0:D3}' -f $i),Sample $entity $i,Active"
    }
    $rows -join "`n" | Set-Content -Path $csvPath -NoNewline
    Write-Host "  [OK] Dim$entity.csv" -ForegroundColor Gray
}

# SensorTelemetry.csv
$telPath = Join-Path $domainDir "data\SensorTelemetry.csv"
$telHeader = "ReadingId,SensorId,Timestamp,Value,Unit,Quality"
$telRows = @($telHeader)
for ($i = 1; $i -le 10; $i++) {
    $telRows += "RD-$('{0:D3}' -f $i),SEN-$('{0:D3}' -f (($i % 5) + 1)),2025-12-01T$('{0:D2}' -f $i):00:00,$(Get-Random -Minimum 10 -Maximum 100).$(Get-Random -Minimum 0 -Maximum 99),Unit,Good"
}
$telRows -join "`n" | Set-Content -Path $telPath -NoNewline
Write-Host "[OK] CSV data files created ($($entityList.Count + 1) files)" -ForegroundColor Green

# ── Build-Ontology.ps1 template ─────────────────────────────────────────────
$ontologyLines = @()
$ontologyLines += "<#"
$ontologyLines += ".SYNOPSIS"
$ontologyLines += "    Builds the $DisplayName ontology definition."
$ontologyLines += "#>"
$ontologyLines += "param("
$ontologyLines += "    [Parameter(Mandatory=`$true)] [string]`$WorkspaceId,"
$ontologyLines += "    [Parameter(Mandatory=`$true)] [string]`$LakehouseId,"
$ontologyLines += "    [Parameter(Mandatory=`$true)] [string]`$EventhouseId,"
$ontologyLines += "    [string]`$OntologyName = `"${DomainName}Ontology`""
$ontologyLines += ")"
$ontologyLines += ""
$ontologyLines += "# TODO: Implement ontology definition following the pattern from existing domains."
$ontologyLines += '# See ontologies/OilGasRefinery/Build-Ontology.ps1 for reference.'
$ontologyLines += "# Entities: $($entityList -join ', ')"
$ontologyLines += "# ID Allocation: Entities 1001+, Properties 2001+, Relationships 3001+, Timeseries 4001+"
$ontologyLines += ""
$ontologyLines += "Write-Host `"[TODO] Implement $DisplayName ontology definition`" -ForegroundColor Yellow"
($ontologyLines -join "`n") | Set-Content -Path (Join-Path $domainDir "Build-Ontology.ps1") -NoNewline
Write-Host "[OK] Build-Ontology.ps1" -ForegroundColor Green

# ── Deploy-KqlTables.ps1 template ──────────────────────────────────────────
$kqlLines = @()
$kqlLines += "<#"
$kqlLines += ".SYNOPSIS"
$kqlLines += "    Creates KQL tables and ingests sample data for $DisplayName."
$kqlLines += "#>"
$kqlLines += "param("
$kqlLines += "    [Parameter(Mandatory=`$true)] [string]`$WorkspaceId,"
$kqlLines += "    [string]`$EventhouseId, [string]`$KqlDatabaseId,"
$kqlLines += "    [string]`$QueryServiceUri, [string]`$KqlDatabaseName,"
$kqlLines += "    [string]`$DataFolder = (Join-Path `$PSScriptRoot 'data')"
$kqlLines += ")"
$kqlLines += ""
$kqlLines += "# Authentication"
$kqlLines += "`$resource = 'https://kusto.kusto.windows.net'"
$kqlLines += "`$kustoToken = `$null"
$kqlLines += "for (`$i = 0; `$i -lt 3; `$i++) {"
$kqlLines += "    try { `$kustoToken = (Get-AzAccessToken -ResourceUrl `$resource).Token; break } catch {}"
$kqlLines += "    Start-Sleep -Seconds 2"
$kqlLines += "}"
$kqlLines += "if (-not `$kustoToken) { Write-Error 'Cannot acquire Kusto token'; exit 1 }"
$kqlLines += ""
$kqlLines += "function Invoke-KustoMgmt { param([string]`$Command)"
$kqlLines += "    `$body = @{ csl = `$Command; db = `$KqlDatabaseName } | ConvertTo-Json"
$kqlLines += "    Invoke-RestMethod -Method Post -Uri `"`$QueryServiceUri/v1/rest/mgmt`" -Headers @{ Authorization = `"Bearer `$kustoToken`"; 'Content-Type' = 'application/json' } -Body `$body"
$kqlLines += "}"
$kqlLines += ""
$kqlLines += "# ── Create SensorReading table ──"
$kqlLines += '# TODO: Define table columns matching your SensorTelemetry.csv schema.'
$kqlLines += '# Example:'
$kqlLines += '# Invoke-KustoMgmt -Command ".create-merge table SensorReading (ReadingId:string, SensorId:string, Timestamp:datetime, Value:real, Unit:string, Quality:string)"'
$kqlLines += ""
$kqlLines += "Write-Host `"[TODO] Implement $DisplayName KQL tables`" -ForegroundColor Yellow"
($kqlLines -join "`n") | Set-Content -Path (Join-Path $domainDir "Deploy-KqlTables.ps1") -NoNewline
Write-Host "[OK] Deploy-KqlTables.ps1" -ForegroundColor Green

# ── Deploy-RTIDashboard.ps1 template ───────────────────────────────────────
$dashLines = @()
$dashLines += "<#"
$dashLines += ".SYNOPSIS"
$dashLines += "    Creates a KQL Real-Time Intelligence Dashboard for $DisplayName."
$dashLines += "#>"
$dashLines += "param("
$dashLines += "    [Parameter(Mandatory=`$true)] [string]`$WorkspaceId,"
$dashLines += "    [string]`$KqlDatabaseId, [string]`$QueryServiceUri, [string]`$KqlDatabaseName"
$dashLines += ")"
$dashLines += ""
$dashLines += "`$DashboardName = '${DomainName}Dashboard'"
$dashLines += "`$fabricToken = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token"
$dashLines += "`$apiBase = 'https://api.fabric.microsoft.com/v1'"
$dashLines += "`$headers = @{ Authorization = `"Bearer `$fabricToken`"; 'Content-Type' = 'application/json' }"
$dashLines += ""
$dashLines += '# TODO: Define 10+ dashboard tiles with KQL queries.'
$dashLines += '# Each tile needs: title, queryText, visualType (line/bar/pie/table), x/y/width/height.'
$dashLines += '# See ontologies/OilGasRefinery/Deploy-RTIDashboard.ps1 for reference.'
$dashLines += ""
$dashLines += "Write-Host `"[TODO] Implement $DisplayName dashboard`" -ForegroundColor Yellow"
($dashLines -join "`n") | Set-Content -Path (Join-Path $domainDir "Deploy-RTIDashboard.ps1") -NoNewline
Write-Host "[OK] Deploy-RTIDashboard.ps1" -ForegroundColor Green

# ── Deploy-DataAgent.ps1 template ──────────────────────────────────────────
$daLines = @()
$daLines += "<#"
$daLines += ".SYNOPSIS"
$daLines += "    Creates a Fabric AI Data Agent for $DisplayName."
$daLines += "#>"
$daLines += "param("
$daLines += "    [Parameter(Mandatory=`$true)] [string]`$WorkspaceId,"
$daLines += "    [string]`$LakehouseId"
$daLines += ")"
$daLines += ""
$daLines += "`$AgentName = '${DomainName}DataAgent'"
$daLines += "`$fabricToken = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token"
$daLines += "`$apiBase = 'https://api.fabric.microsoft.com/v1'"
$daLines += "`$headers = @{ Authorization = `"Bearer `$fabricToken`"; 'Content-Type' = 'application/json' }"
$daLines += ""
$daLines += '# TODO: Define data_agent.json (schema 2.1.0) and stage_config.json (schema 1.0.0).'
$daLines += '# Include: AI instructions, Lakehouse data source, entity-aware prompts.'
$daLines += '# See ontologies/OilGasRefinery/Deploy-DataAgent.ps1 for reference.'
$daLines += ""
$daLines += "Write-Host `"[TODO] Implement $DisplayName Data Agent`" -ForegroundColor Yellow"
($daLines -join "`n") | Set-Content -Path (Join-Path $domainDir "Deploy-DataAgent.ps1") -NoNewline
Write-Host "[OK] Deploy-DataAgent.ps1" -ForegroundColor Green

# ── Deploy-OperationsAgent.ps1 template ────────────────────────────────────
$oaLines = @()
$oaLines += "<#"
$oaLines += ".SYNOPSIS"
$oaLines += "    Creates a Fabric AI Operations Agent for $DisplayName."
$oaLines += "#>"
$oaLines += "param("
$oaLines += "    [Parameter(Mandatory=`$true)] [string]`$WorkspaceId,"
$oaLines += "    [string]`$KqlDatabaseId"
$oaLines += ")"
$oaLines += ""
$oaLines += "`$AgentName = '${DomainName}OpsAgent'"
$oaLines += "`$fabricToken = (Get-AzAccessToken -ResourceUrl 'https://api.fabric.microsoft.com').Token"
$oaLines += "`$apiBase = 'https://api.fabric.microsoft.com/v1'"
$oaLines += "`$headers = @{ Authorization = `"Bearer `$fabricToken`"; 'Content-Type' = 'application/json' }"
$oaLines += ""
$oaLines += '# TODO: Define operations agent with 5 operational goals and KQL data source.'
$oaLines += '# Include: goal definitions, Teams integration, KQL database binding.'
$oaLines += '# See ontologies/OilGasRefinery/Deploy-OperationsAgent.ps1 for reference.'
$oaLines += ""
$oaLines += "Write-Host `"[TODO] Implement $DisplayName Operations Agent`" -ForegroundColor Yellow"
($oaLines -join "`n") | Set-Content -Path (Join-Path $domainDir "Deploy-OperationsAgent.ps1") -NoNewline
Write-Host "[OK] Deploy-OperationsAgent.ps1" -ForegroundColor Green

# ── GraphQueries.gql template ──────────────────────────────────────────────
$gqlLines = @()
$gqlLines += "/* $DisplayName Graph Queries */"
$gqlLines += "/* ISO/IEC 39075:2024 GQL */"
$gqlLines += ""

$qn = 1
# Per-entity list queries
foreach ($entity in $entityList) {
    $prefix = $entity.Substring(0,3).ToUpper()
    $gqlLines += "/* $qn. List all $entity entities */"
    $gqlLines += "GRAPH_TABLE ($DomainName"
    $gqlLines += "  MATCH (n:$entity)"
    $gqlLines += "  COLUMNS (n.${entity}Id, n.${entity}Name, n.Status)"
    $gqlLines += ")"
    $gqlLines += ""
    $qn++
}

# Relationship queries between consecutive pairs
for ($i = 0; $i -lt $entityList.Count - 1; $i++) {
    $from = $entityList[$i]
    $to = $entityList[$i + 1]
    $gqlLines += "/* $qn. ${from}-to-${to} relationship */"
    $gqlLines += "GRAPH_TABLE ($DomainName"
    $gqlLines += "  MATCH (a:$from)-[:${from}Has${to}]->(b:$to)"
    $gqlLines += "  COLUMNS (a.${from}Name, b.${to}Name, b.Status)"
    $gqlLines += ")"
    $gqlLines += ""
    $qn++
}

# Count aggregation per entity
foreach ($entity in $entityList) {
    $gqlLines += "/* $qn. Count ${entity} by Status */"
    $gqlLines += "GRAPH_TABLE ($DomainName"
    $gqlLines += "  MATCH (n:$entity)"
    $gqlLines += "  COLUMNS (n.Status, COUNT(*) AS Total)"
    $gqlLines += ")"
    $gqlLines += ""
    $qn++
}

# 2-hop traversal if 3+ entities
if ($entityList.Count -ge 3) {
    $gqlLines += "/* $qn. 2-hop traversal: $($entityList[0]) -> $($entityList[1]) -> $($entityList[2]) */"
    $gqlLines += "GRAPH_TABLE ($DomainName"
    $gqlLines += "  MATCH (a:$($entityList[0]))-[:$($entityList[0])Has$($entityList[1])]->(b:$($entityList[1]))-[:$($entityList[1])Has$($entityList[2])]->(c:$($entityList[2]))"
    $gqlLines += "  COLUMNS (a.$($entityList[0])Name, b.$($entityList[1])Name, c.$($entityList[2])Name)"
    $gqlLines += ")"
    $gqlLines += ""
}

$gqlLines += "/* Add more queries to reach 20 minimum. */"
$gqlLines += "/* See ontologies/OilGasRefinery/GraphQueries.gql for advanced patterns. */"
($gqlLines -join "`n") | Set-Content -Path (Join-Path $domainDir "GraphQueries.gql") -NoNewline
Write-Host "[OK] GraphQueries.gql" -ForegroundColor Green

# ── LoadDataToTables.py template ───────────────────────────────────────────
$pyLines = @()
$pyLines += "# Spark notebook: Load $DisplayName CSV data into Lakehouse Delta tables"
$pyLines += "from pyspark.sql.types import StructType, StructField, StringType"
$pyLines += ""
$pyLines += "LAKEHOUSE_NAME = `"${DomainName}LH`""
$pyLines += ""
$pyLines += "TABLE_DEFINITIONS = ["
foreach ($entity in $entityList) {
    $pyLines += "    {`"name`": `"dim$($entity.ToLower())`", `"file`": `"Dim$entity.csv`", `"schema`": StructType([StructField(`"${entity}Id`", StringType()), StructField(`"${entity}Name`", StringType()), StructField(`"Status`", StringType())])},"
}
$pyLines += "    {`"name`": `"sensortelemetry`", `"file`": `"SensorTelemetry.csv`", `"schema`": StructType([StructField(`"ReadingId`", StringType()), StructField(`"SensorId`", StringType()), StructField(`"Timestamp`", StringType()), StructField(`"Value`", StringType()), StructField(`"Unit`", StringType()), StructField(`"Quality`", StringType())])},"
$pyLines += "]"
$pyLines += ""
$pyLines += "for tbl in TABLE_DEFINITIONS:"
$pyLines += "    df = spark.read.option(`"header`", True).schema(tbl[`"schema`"]).csv(f`"Files/{tbl['file']}`")"
$pyLines += "    df.write.mode(`"overwrite`").format(`"delta`").saveAsTable(f`"{LAKEHOUSE_NAME}.{tbl['name']}`")"
$pyLines += "    print(f`"Loaded {tbl['name']}: {df.count()} rows`")"
($pyLines -join "`n") | Set-Content -Path (Join-Path $domainDir "LoadDataToTables.py") -NoNewline
Write-Host "[OK] LoadDataToTables.py" -ForegroundColor Green

# ── Semantic Model templates ───────────────────────────────────────────────
$pbism = @{
    version = "4.2"
    settings = @{}
} | ConvertTo-Json -Depth 3
Set-Content -Path (Join-Path $domainDir "SemanticModel\definition.pbism") -Value $pbism -NoNewline

$dbTmdl = @"
model Model
	culture: en-US
	defaultPowerBIDataSourceVersion: powerBI_V3
	discourageImplicitMeasures: true
	sourceQueryCulture: en-US

	annotation PBI_QueryOrder = []
"@
Set-Content -Path (Join-Path $domainDir "SemanticModel\definition\database.tmdl") -Value $dbTmdl -NoNewline

$modelTmdl = "model Model`n`tculture: en-US`n`tdefaultPowerBIDataSourceVersion: powerBI_V3`n"
foreach ($entity in $entityList) {
    $modelTmdl += "`n`tref table dim$($entity.ToLower())"
}
$modelTmdl += "`n`tref table sensortelemetry"
Set-Content -Path (Join-Path $domainDir "SemanticModel\definition\model.tmdl") -Value $modelTmdl -NoNewline

$exprTmdl = @"
expression SQL_ENDPOINT = "REPLACE_WITH_SQL_ENDPOINT" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]
expression LAKEHOUSE_NAME = "${DomainName}LH" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]
"@
Set-Content -Path (Join-Path $domainDir "SemanticModel\definition\expressions.tmdl") -Value $exprTmdl -NoNewline

# Empty relationships file
Set-Content -Path (Join-Path $domainDir "SemanticModel\definition\relationships.tmdl") -Value "// TODO: Define relationships`n" -NoNewline

# Table TMDL files
foreach ($entity in $entityList) {
    $tableName = "dim$($entity.ToLower())"
    $tmdlContent = @"
table $tableName
	lineageTag: $(New-Guid)

	column ${entity}Id
		dataType: string
		isKey: true
		sourceColumn: ${entity}Id
		lineageTag: $(New-Guid)
		summarizeBy: none

	column ${entity}Name
		dataType: string
		sourceColumn: ${entity}Name
		lineageTag: $(New-Guid)
		summarizeBy: none

	column Status
		dataType: string
		sourceColumn: Status
		lineageTag: $(New-Guid)
		summarizeBy: none

	partition $tableName = entity
		mode: directLake
		source
			entityName: $tableName
			schemaName: dbo
			expressionSource: DatabaseQuery
"@
    Set-Content -Path (Join-Path $domainDir "SemanticModel\definition\tables\$tableName.tmdl") -Value $tmdlContent -NoNewline
}

# SensorTelemetry TMDL
$telTmdl = @"
table sensortelemetry
	lineageTag: $(New-Guid)

	column ReadingId
		dataType: string
		isKey: true
		sourceColumn: ReadingId
		lineageTag: $(New-Guid)
		summarizeBy: none

	column SensorId
		dataType: string
		sourceColumn: SensorId
		lineageTag: $(New-Guid)
		summarizeBy: none

	column Timestamp
		dataType: string
		sourceColumn: Timestamp
		lineageTag: $(New-Guid)
		summarizeBy: none

	column Value
		dataType: string
		sourceColumn: Value
		lineageTag: $(New-Guid)
		summarizeBy: none

	partition sensortelemetry = entity
		mode: directLake
		source
			entityName: sensortelemetry
			schemaName: dbo
			expressionSource: DatabaseQuery
"@
Set-Content -Path (Join-Path $domainDir "SemanticModel\definition\tables\sensortelemetry.tmdl") -Value $telTmdl -NoNewline

Write-Host "[OK] Semantic Model templates created" -ForegroundColor Green

# ── Summary ─────────────────────────────────────────────────────────────────
$fileCount = (Get-ChildItem -Path $domainDir -Recurse -File).Count
Write-Host ""
Write-Host "=== Scaffolding Complete ===" -ForegroundColor Cyan
Write-Host "  Domain:    $DomainName"
Write-Host "  Location:  $domainDir"
Write-Host "  Files:     $fileCount"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Edit Build-Ontology.ps1 with entity types, properties, and relationships"
Write-Host "  2. Add realistic sample data to data/*.csv files"
Write-Host "  3. Implement Deploy-KqlTables.ps1 with 5 KQL tables"
Write-Host "  4. Design Deploy-RTIDashboard.ps1 with 10+ tiles"
Write-Host "  5. Configure Deploy-DataAgent.ps1 and Deploy-OperationsAgent.ps1"
Write-Host "  6. Write 20 GQL queries in GraphQueries.gql"
Write-Host "  7. Register domain in Deploy-Ontology.ps1 and Validate-Deployment.ps1"
Write-Host ""
Write-Host "Use the Copilot agents for each step — see AGENTS.md" -ForegroundColor Gray
