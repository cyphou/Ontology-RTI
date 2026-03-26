<#
.SYNOPSIS
    Generic Ontology Deployment Engine for Microsoft Fabric.

.DESCRIPTION
    Deploys a domain-specific IQ Ontology to Microsoft Fabric including
    Lakehouse, Eventhouse, Spark Notebook, Ontology, and optional
    RTI Dashboard, Data Agent, and Operations Agent.

    Called by Deploy-Ontology.ps1 for non-OilGas domains.
#>

param(
    [Parameter(Mandatory)] [string]$WorkspaceId,
    [Parameter(Mandatory)] [string]$OntologyType,
    [Parameter(Mandatory)] [string]$LakehouseName,
    [Parameter(Mandatory)] [string]$EventhouseName,
    [Parameter(Mandatory)] [string]$SemanticModelName,
    [Parameter(Mandatory)] [string]$OntologyName,
    [Parameter(Mandatory)] [string]$DataFolder,
    [Parameter(Mandatory)] [string]$OntologyFolder,
    [switch]$SkipDataAgent,
    [switch]$SkipOperationsAgent,
    [switch]$SkipDashboard
)

$ErrorActionPreference = "Stop"
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$rootDir = Split-Path -Parent $scriptDir

$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$OneLakeBase = "https://onelake.dfs.fabric.microsoft.com"

# ============================================================================
# HELPER FUNCTIONS (same as Deploy-OilGasOntology.ps1)
# ============================================================================

function Write-Step { param([string]$Message); Write-Host ""; Write-Host ("=" * 69) -ForegroundColor Cyan; Write-Host " $Message" -ForegroundColor Cyan; Write-Host ("=" * 69) -ForegroundColor Cyan }
function Write-Info  { param([string]$Message); Write-Host "  [INFO] $Message" -ForegroundColor Gray }
function Write-Success { param([string]$Message); Write-Host "  [OK]   $Message" -ForegroundColor Green }
function Write-Warn  { param([string]$Message); Write-Host "  [WARN] $Message" -ForegroundColor Yellow }

function Get-FabricToken {
    try { $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"; return $token.Token }
    catch { Write-Error "Failed to get Fabric token. Run 'Connect-AzAccount' first."; throw }
}

function Get-StorageToken {
    try { $token = Get-AzAccessToken -ResourceTypeName Storage; return $token.Token }
    catch { Write-Error "Failed to get Storage token."; throw }
}

function Invoke-FabricApi {
    param([string]$Method, [string]$Uri, [object]$Body = $null, [string]$BodyJson = $null, [string]$Token, [int]$MaxRetries = 10)
    $headers = @{ "Authorization" = "Bearer $Token"; "Content-Type" = "application/json" }
    if (-not $BodyJson -and $Body) { $BodyJson = $Body | ConvertTo-Json -Depth 10 }
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{ Method = $Method; Uri = $Uri; Headers = $headers; UseBasicParsing = $true }
            if ($BodyJson) { $params["Body"] = $BodyJson }
            $webResponse = Invoke-WebRequest @params
            if ($webResponse.StatusCode -eq 202) {
                $loc = $webResponse.Headers["Location"]
                if (-not $loc) { $opId = $webResponse.Headers["x-ms-operation-id"]; if ($opId) { $loc = "$FabricApiBase/operations/$opId" } }
                if ($loc) { return Wait-FabricOperation -OperationUrl $loc -Token $Token }
                return $null
            }
            if ($webResponse.Content) { try { return $webResponse.Content | ConvertFrom-Json } catch { return $webResponse.Content } }
            return $null
        } catch {
            $ex = $_.Exception; $sc = $null; $eb = ""
            if ($ex.Response) { $sc = [int]$ex.Response.StatusCode; try { $sr = New-Object System.IO.StreamReader($ex.Response.GetResponseStream()); $eb = $sr.ReadToEnd(); $sr.Close() } catch {} }
            $isRetriable = ($eb -like "*isRetriable*true*" -or $eb -like "*NotAvailableYet*")
            if ($sc -eq 429 -or $isRetriable) {
                $ra = if ($isRetriable) { 15 } else { 30 }
                Write-Warn "Retriable ($sc). Retry after $ra`s (attempt $attempt/$MaxRetries)..."
                Start-Sleep -Seconds $ra
            } else { if ($eb) { throw "Fabric API error (HTTP $sc): $eb" }; throw }
        }
    }
    throw "Max retries exceeded for $Uri"
}

function Wait-FabricOperation {
    param([string]$OperationUrl, [string]$Token, [int]$TimeoutSeconds = 600, [int]$PollIntervalSeconds = 10)
    $headers = @{ "Authorization" = "Bearer $Token" }; $elapsed = 0
    while ($elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds $PollIntervalSeconds; $elapsed += $PollIntervalSeconds
        try {
            $status = Invoke-RestMethod -Method Get -Uri $OperationUrl -Headers $headers
            Write-Info "  Operation: $($status.status) ($elapsed`s)"
            if ($status.status -eq "Succeeded") { return $status }
            if ($status.status -eq "Failed") { throw "Fabric operation failed: $($status | ConvertTo-Json -Depth 5 -Compress)" }
        } catch {
            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 429) { Start-Sleep -Seconds 30 } else { throw }
        }
    }
    throw "Operation timed out after $TimeoutSeconds`s"
}

function Upload-FileToOneLake {
    param([string]$LocalFilePath, [string]$OneLakePath, [string]$Token)
    $fileBytes = [System.IO.File]::ReadAllBytes($LocalFilePath)
    $fileName = [System.IO.Path]::GetFileName($LocalFilePath)
    Invoke-RestMethod -Method Put -Uri "${OneLakePath}/${fileName}?resource=file" -Headers @{ Authorization = "Bearer $Token"; "Content-Length" = "0" } | Out-Null
    Invoke-RestMethod -Method Patch -Uri "${OneLakePath}/${fileName}?action=append&position=0" -Headers @{ Authorization = "Bearer $Token"; "Content-Type" = "application/octet-stream"; "Content-Length" = $fileBytes.Length.ToString() } -Body $fileBytes | Out-Null
    Invoke-RestMethod -Method Patch -Uri "${OneLakePath}/${fileName}?action=flush&position=$($fileBytes.Length)" -Headers @{ Authorization = "Bearer $Token"; "Content-Length" = "0" } | Out-Null
}

# ============================================================================
# MAIN DEPLOYMENT
# ============================================================================

Write-Host ""
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host "  $OntologyType Ontology - Fabric Deployment" -ForegroundColor Yellow
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Workspace     : $WorkspaceId"
Write-Host "  Data Folder   : $DataFolder"
Write-Host "  Lakehouse     : $LakehouseName"
Write-Host "  Eventhouse    : $EventhouseName"
Write-Host "  Ontology      : $OntologyName"
Write-Host ""

if (-not (Test-Path $DataFolder)) { Write-Error "Data folder not found: $DataFolder"; exit 1 }

# Discover CSV files - separate lakehouse vs telemetry
$allCsvFiles = Get-ChildItem -Path $DataFolder -Filter "*.csv" -File
$telemetryFile = $allCsvFiles | Where-Object { $_.Name -eq "SensorTelemetry.csv" } | Select-Object -First 1
$lakehouseFiles = $allCsvFiles | Where-Object { $_.Name -ne "SensorTelemetry.csv" }
Write-Info "Found $($lakehouseFiles.Count) lakehouse CSV files + $(if ($telemetryFile) { '1 telemetry file' } else { 'no telemetry file' })"

# ------------------------------------------------------------------
# Step 0: Authenticate
# ------------------------------------------------------------------
Write-Step "Step 0: Authenticating to Azure / Fabric"
$account = Get-AzContext
if (-not $account) { Write-Info "No active session. Launching login..."; Connect-AzAccount } else { Write-Info "Using: $($account.Account.Id)" }
$fabricToken = Get-FabricToken
$storageToken = Get-StorageToken
Write-Success "Authenticated"

# ------------------------------------------------------------------
# Step 1: Create Lakehouse
# ------------------------------------------------------------------
Write-Step "Step 1: Creating Lakehouse '$LakehouseName'"
$lakehouseId = $null
try {
    $lh = Invoke-FabricApi -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Body @{ displayName = $LakehouseName; type = "Lakehouse"; description = "$OntologyType data lakehouse" } -Token $fabricToken
    $lakehouseId = $lh.id; Write-Success "Created: $lakehouseId"
} catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*" -or $_.Exception.Message -like "*already in use*") {
        Write-Warn "Already exists. Looking up..."
        $items = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Lakehouse" -Token $fabricToken
        $lakehouseId = ($items.value | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1).id
        Write-Info "Using existing: $lakehouseId"
    } else { throw }
}

# Wait for SQL endpoint
$sqlEndpointConnStr = ""
Write-Info "Waiting for SQL endpoint..."
for ($w = 15; $w -le 180; $w += 15) {
    Start-Sleep -Seconds 15
    try {
        $fabricToken = Get-FabricToken
        $lhProps = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$lakehouseId" -Token $fabricToken
        if ($lhProps.properties -and $lhProps.properties.sqlEndpointProperties -and $lhProps.properties.sqlEndpointProperties.connectionString) {
            $sqlEndpointConnStr = $lhProps.properties.sqlEndpointProperties.connectionString
            Write-Success "SQL endpoint ready ($w`s)"; break
        }
    } catch {}
    Write-Info "  Not ready ($w`s)..."
}

# ------------------------------------------------------------------
# Step 2: Upload CSV Files
# ------------------------------------------------------------------
Write-Step "Step 2: Uploading CSV files to Lakehouse"
$storageToken = Get-StorageToken
$oneLakeFilesPath = "$OneLakeBase/$WorkspaceId/$lakehouseId/Files"
try { Invoke-RestMethod -Method Put -Uri "${oneLakeFilesPath}?resource=directory" -Headers @{ Authorization = "Bearer $storageToken"; "Content-Length" = "0" } | Out-Null } catch {}
$uploaded = 0
foreach ($f in $lakehouseFiles) {
    Write-Info "Uploading $($f.Name)..."
    Upload-FileToOneLake -LocalFilePath $f.FullName -OneLakePath $oneLakeFilesPath -Token $storageToken
    $uploaded++; Write-Success "Uploaded $($f.Name)"
}
Write-Success "Uploaded $uploaded files"

# ------------------------------------------------------------------
# Step 3: Create & Run Notebook
# ------------------------------------------------------------------
Write-Step "Step 3: Creating Spark Notebook to load CSV into Delta tables"

# Prefer domain-specific typed notebook; fallback to generic inline
$domainNotebook = Join-Path $OntologyFolder "LoadDataToTables.py"
if (Test-Path $domainNotebook) {
    Write-Info "Using domain-specific notebook: $domainNotebook"
    $notebookCode = Get-Content -Path $domainNotebook -Raw -Encoding UTF8
} else {
    Write-Info "No domain notebook found — generating generic inline loader"
    $tableNames = ($lakehouseFiles | ForEach-Object { $_.BaseName.ToLower() }) -join '","'
    $notebookCode = @"
# Auto-generated notebook for $OntologyType ontology
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

tables = ["$tableNames"]
for tbl in tables:
    import os
    files_dir = "/lakehouse/default/Files/"
    candidates = [f for f in os.listdir(files_dir) if f.lower() == tbl.lower() + ".csv"]
    if candidates:
        path = f"Files/{candidates[0]}"
    else:
        path = f"Files/{tbl}.csv"
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        df.write.mode("overwrite").format("delta").saveAsTable(tbl)
        print(f"Loaded {tbl}: {df.count()} rows")
    except Exception as e:
        print(f"WARN: Could not load {tbl}: {e}")
"@
}

$notebookNativeContent = @"
# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "$lakehouseId",
# META       "default_lakehouse_name": "$LakehouseName",
# META       "default_lakehouse_workspace_id": "$WorkspaceId",
# META       "known_lakehouses": [
# META         {
# META           "id": "$lakehouseId"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

$notebookCode

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
"@
$notebookNativeContent = $notebookNativeContent -replace "`r`n", "`n"
$notebookBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($notebookNativeContent))

$nbName = "${OntologyType}_LoadTables"
$notebookId = $null
$nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
$createNbBodyJson = @{ displayName = $nbName; type = "Notebook"; description = "Loads $OntologyType CSV files into Delta tables" } | ConvertTo-Json -Depth 5

for ($a = 1; $a -le 3; $a++) {
    if ($a -gt 1) { Start-Sleep -Seconds 15; $fabricToken = Get-FabricToken; $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" } }
    try {
        $nbResp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $nbHeaders -Body $createNbBodyJson -UseBasicParsing
        if ($nbResp.StatusCode -eq 201) { $notebookId = ($nbResp.Content | ConvertFrom-Json).id }
        elseif ($nbResp.StatusCode -eq 202) {
            $nbOpUrl = $nbResp.Headers["Location"]
            if ($nbOpUrl) { for ($p = 1; $p -le 12; $p++) { Start-Sleep -Seconds 5; $poll = Invoke-RestMethod -Uri $nbOpUrl -Headers @{Authorization = "Bearer $fabricToken"}; if ($poll.status -eq "Succeeded") { break } } }
            Start-Sleep -Seconds 3
            $nbItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" -Headers @{Authorization = "Bearer $fabricToken"}).value
            $nbFound = $nbItems | Where-Object { $_.displayName -eq $nbName } | Select-Object -First 1
            if ($nbFound) { $notebookId = $nbFound.id }
        }
        if ($notebookId) { Write-Success "Notebook created: $notebookId"; break }
    } catch {
        $nbErr = ""; try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $nbErr = $sr.ReadToEnd(); $sr.Close() } catch {}
        if ("$($_.Exception.Message) $nbErr" -like "*ItemDisplayNameAlreadyInUse*") {
            $nbItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" -Headers @{Authorization = "Bearer $fabricToken"}).value
            $nbFound = $nbItems | Where-Object { $_.displayName -eq $nbName } | Select-Object -First 1
            if ($nbFound) { $notebookId = $nbFound.id; Write-Info "Using existing: $notebookId" }; break
        }
        Write-Warn "Attempt $a error: $($_.Exception.Message)"
    }
}

# Update definition & run
$definitionApplied = $false
if ($notebookId) {
    $updateDefJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $notebookBase64 + '","payloadType":"InlineBase64"}]}}'
    for ($a = 1; $a -le 3; $a++) {
        if ($a -gt 1) { Start-Sleep -Seconds 10; $fabricToken = Get-FabricToken }
        try {
            $udResp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$notebookId/updateDefinition" -Headers @{ Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json" } -Body $updateDefJson -UseBasicParsing
            if ($udResp.StatusCode -eq 200) { $definitionApplied = $true }
            elseif ($udResp.StatusCode -eq 202) {
                $udOpUrl = $udResp.Headers["Location"]
                if ($udOpUrl) { for ($p = 1; $p -le 12; $p++) { Start-Sleep -Seconds 5; $poll = Invoke-RestMethod -Uri $udOpUrl -Headers @{Authorization = "Bearer $fabricToken"}; if ($poll.status -eq "Succeeded") { $definitionApplied = $true; break }; if ($poll.status -eq "Failed") { break } } }
            }
            if ($definitionApplied) { Write-Success "Notebook definition updated"; break }
        } catch { Write-Warn "Definition update error: $($_.Exception.Message)" }
    }

    # Run notebook
    if ($definitionApplied) {
        Write-Info "Running notebook (Spark startup may take a few minutes)..."
        Start-Sleep -Seconds 15
        $notebookSuccess = $false
        for ($a = 1; $a -le 3; $a++) {
            if ($a -gt 1) { Start-Sleep -Seconds 30; $fabricToken = Get-FabricToken }
            try {
                $runResp = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$notebookId/jobs/instances?jobType=RunNotebook" -Headers @{ Authorization = "Bearer $fabricToken"; "Content-Type" = "application/json" } -UseBasicParsing
                if ($runResp.StatusCode -eq 202) {
                    $jobLoc = $runResp.Headers["Location"]
                    if ($jobLoc) {
                        for ($w = 15; $w -le 600; $w += 15) {
                            Start-Sleep -Seconds 15
                            try {
                                $js = Invoke-RestMethod -Uri $jobLoc -Headers @{Authorization = "Bearer $fabricToken"}
                                Write-Info "  Job: $($js.status) ($w`s)"
                                if ($js.status -eq "Completed") { $notebookSuccess = $true; Write-Success "Notebook completed"; break }
                                if ($js.status -in @("Failed","Cancelled")) { Write-Warn "Job $($js.status)"; break }
                            } catch { Write-Info "  Polling ($w`s)..." }
                        }
                    }
                }
                if ($notebookSuccess) { break }
            } catch { Write-Warn "Run error: $($_.Exception.Message)" }
        }
        if (-not $notebookSuccess) { Write-Warn "Notebook did not complete. Run '$nbName' manually." }
    }
}

# ------------------------------------------------------------------
# Step 4: Create Eventhouse
# ------------------------------------------------------------------
Write-Step "Step 4: Creating Eventhouse '$EventhouseName'"
$eventhouseId = $null
try {
    $eh = Invoke-FabricApi -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Body @{ displayName = $EventhouseName; type = "Eventhouse"; description = "$OntologyType sensor telemetry" } -Token $fabricToken
    $eventhouseId = $eh.id; Write-Success "Created: $eventhouseId"
} catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*" -or $_.Exception.Message -like "*already in use*") {
        $items = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Eventhouse" -Token $fabricToken
        $eventhouseId = ($items.value | Where-Object { $_.displayName -eq $EventhouseName } | Select-Object -First 1).id
        Write-Info "Using existing: $eventhouseId"
    } else { throw }
}

# KQL tables via domain-specific script (fallback to deploy/)
$kqlScript = Join-Path $OntologyFolder "Deploy-KqlTables.ps1"
if (-not (Test-Path $kqlScript)) { $kqlScript = Join-Path $scriptDir "Deploy-KqlTables.ps1" }
if ($telemetryFile -and (Test-Path $kqlScript)) {
    try {
        & $kqlScript -WorkspaceId $WorkspaceId -EventhouseId $eventhouseId -DataFolder $DataFolder
        Write-Success "KQL tables created"
    } catch { Write-Warn "KQL table issue: $_" }
}

# ------------------------------------------------------------------
# Step 5: Create Semantic Model (TMDL)
# ------------------------------------------------------------------
Write-Step "Step 5: Creating Semantic Model '$SemanticModelName' (TMDL)"
$semanticModelId = $null

# Look for domain-specific SemanticModel folder (fallback to deploy/SemanticModel/)
$tmdlRoot = Join-Path $OntologyFolder "SemanticModel"
if (-not (Test-Path $tmdlRoot)) { $tmdlRoot = Join-Path $scriptDir "SemanticModel" }

if (Test-Path $tmdlRoot) {
    $tmdlDefDir = Join-Path $tmdlRoot "definition"
    $tmdlTablesDir = Join-Path $tmdlDefDir "tables"

    # Collect all definition parts: definition.pbism + definition/*.tmdl + definition/tables/*.tmdl
    $smParts = @()

    # 1. definition.pbism
    $pbismPath = Join-Path $tmdlRoot "definition.pbism"
    if (Test-Path $pbismPath) {
        $pbismContent = Get-Content -Path $pbismPath -Raw -Encoding UTF8
        $pbismBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pbismContent))
        $smParts += '{"path":"definition.pbism","payload":"' + $pbismBase64 + '","payloadType":"InlineBase64"}'
        Write-Info "  Loaded definition.pbism"
    }

    # 2. definition/*.tmdl files
    if (Test-Path $tmdlDefDir) {
        $defFiles = Get-ChildItem -Path $tmdlDefDir -Filter "*.tmdl" -File | Sort-Object Name
        foreach ($f in $defFiles) {
            $fileContent = Get-Content -Path $f.FullName -Raw -Encoding UTF8
            # Replace placeholders in expressions.tmdl
            if ($f.Name -eq "expressions.tmdl") {
                $fileContent = $fileContent -replace '\{\{SQL_ENDPOINT\}\}', $sqlEndpointConnStr
                $fileContent = $fileContent -replace '\{\{LAKEHOUSE_NAME\}\}', $LakehouseName
            }
            $fileBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($fileContent))
            $partPath = "definition/" + $f.Name
            $smParts += '{"path":"' + $partPath + '","payload":"' + $fileBase64 + '","payloadType":"InlineBase64"}'
            Write-Info "  Loaded $partPath"
        }
    }

    # 3. definition/tables/*.tmdl files
    if (Test-Path $tmdlTablesDir) {
        $tableFiles = Get-ChildItem -Path $tmdlTablesDir -Filter "*.tmdl" -File | Sort-Object Name
        foreach ($f in $tableFiles) {
            $fileContent = Get-Content -Path $f.FullName -Raw -Encoding UTF8
            $fileBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($fileContent))
            $partPath = "definition/tables/" + $f.Name
            $smParts += '{"path":"' + $partPath + '","payload":"' + $fileBase64 + '","payloadType":"InlineBase64"}'
            Write-Info "  Loaded $partPath"
        }
    }

    Write-Info "Total TMDL parts: $($smParts.Count)"

    if ($smParts.Count -gt 0) {
        $smDescription = "Direct Lake semantic model for $OntologyType ontology"
        $partsJson = $smParts -join ","
        $createSmJson = '{"displayName":"' + $SemanticModelName + '","type":"SemanticModel","description":"' + $smDescription + '","definition":{"parts":[' + $partsJson + ']}}'

        try {
            Write-Info "Sending semantic model creation request..."
            $smHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
            $smResponse = Invoke-WebRequest -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Headers $smHeaders -Body $createSmJson -UseBasicParsing

            if ($smResponse.StatusCode -eq 202) {
                Write-Info "Semantic model creation accepted (202). Waiting..."
                $smOpUrl = $null
                try { $smOpUrl = $smResponse.Headers["Location"] } catch {}
                if (-not $smOpUrl) { try { $opId = $smResponse.Headers["x-ms-operation-id"]; if ($opId) { $smOpUrl = "$FabricApiBase/operations/$opId" } } catch {} }
                if ($smOpUrl) {
                    for ($p = 10; $p -le 120; $p += 10) {
                        Start-Sleep -Seconds 10
                        try {
                            $fabricToken = Get-FabricToken
                            $poll = Invoke-RestMethod -Method Get -Uri $smOpUrl -Headers @{ Authorization = "Bearer $fabricToken" }
                            Write-Info "  Operation: $($poll.status) ($p`s)"
                            if ($poll.status -eq "Succeeded") { break }
                            if ($poll.status -eq "Failed") { Write-Warn "SM operation failed"; break }
                        } catch { Write-Warn "SM poll error: $_"; break }
                    }
                } else { Start-Sleep -Seconds 15 }
                # Look up by name
                $smItems = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" -Token $fabricToken
                $semanticModelId = ($smItems.value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1).id
            } else {
                $semanticModelId = ($smResponse.Content | ConvertFrom-Json).id
            }
            Write-Success "Semantic model created: $semanticModelId"
        } catch {
            $errMsg = $_.Exception.Message
            if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") { Write-Warn "Semantic model '$SemanticModelName' already exists." }
            else { Write-Warn "Semantic model issue: $errMsg"; Write-Info "Create it manually — see SEMANTIC_MODEL_GUIDE.md" }
        }
    }
} else {
    Write-Warn "No TMDL folder found. Create semantic model manually — see SEMANTIC_MODEL_GUIDE.md"
}

# ------------------------------------------------------------------
# Step 6: Create Ontology
# ------------------------------------------------------------------
Write-Step "Step 6: Creating Ontology '$OntologyName'"
$ontologyId = $null
try {
    $ont = Invoke-FabricApi -Method Post -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" -Body @{ displayName = $OntologyName; type = "Ontology"; description = "$OntologyType ontology" } -Token $fabricToken
    $ontologyId = $ont.id; Write-Success "Created: $ontologyId"
} catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*" -or $_.Exception.Message -like "*already in use*") {
        $ontItems = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Ontology" -Token $fabricToken
        $ontologyId = ($ontItems.value | Where-Object { $_.displayName -eq $OntologyName } | Select-Object -First 1).id
        Write-Info "Using existing: $ontologyId"
    } else { throw }
}

# Build ontology from domain-specific script
if ($ontologyId) {
    $buildScript = Join-Path $OntologyFolder "Build-Ontology.ps1"
    if (Test-Path $buildScript) {
        Write-Info "Building ontology entities and relationships..."
        $kqlDbId = $null; $kqlClusterUri = $null
        try {
            $fabricToken = Get-FabricToken
            $kqlInfo = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases" -Token $fabricToken
            $kqlDb = $kqlInfo.value | Where-Object { $_.displayName -eq $EventhouseName }
            if ($kqlDb) {
                $kqlDbId = $kqlDb.id
                $kqlDetail = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases/$kqlDbId" -Token $fabricToken
                $kqlClusterUri = $kqlDetail.properties.queryServiceUri
            }
        } catch { Write-Warn "Could not get KQL DB details: $_" }
        try {
            $fabricToken = Get-FabricToken
            & $buildScript -WorkspaceId $WorkspaceId -LakehouseId $lakehouseId -KqlDatabaseId $kqlDbId -KqlClusterUri $kqlClusterUri -KqlDatabaseName $EventhouseName -OntologyId $ontologyId -FabricToken $fabricToken
            Write-Success "Ontology populated"
        } catch { Write-Warn "Ontology build issue: $_" }
    } else { Write-Warn "Build-Ontology.ps1 not found in $OntologyFolder" }
}

# ------------------------------------------------------------------
# Step 7: Deploy RTI Dashboard (optional)
# ------------------------------------------------------------------
if (-not $SkipDashboard) {
    Write-Step "Step 7: Deploying RTI Dashboard"
    $rtiScript = Join-Path $OntologyFolder "Deploy-RTIDashboard.ps1"
    if (-not (Test-Path $rtiScript)) { $rtiScript = Join-Path $scriptDir "Deploy-RTIDashboard.ps1" }
    if (Test-Path $rtiScript) {
        $kqlQueryUri = $null; $kqlDbId = $null
        try {
            $kqlInfo = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases" -Token $fabricToken
            $kqlDb = $kqlInfo.value | Where-Object { $_.displayName -eq $EventhouseName }
            if ($kqlDb) { $kqlDbId = $kqlDb.id; $kqlDetail = Invoke-FabricApi -Method Get -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases/$kqlDbId" -Token $fabricToken; $kqlQueryUri = $kqlDetail.properties.queryServiceUri }
        } catch {}
        if ($kqlQueryUri -and $kqlDbId) {
            try { & $rtiScript -WorkspaceId $WorkspaceId -KqlDatabaseId $kqlDbId -QueryServiceUri $kqlQueryUri; Write-Success "Dashboard deployed" }
            catch { Write-Warn "Dashboard issue: $_" }
        } else { Write-Warn "KQL DB URI not found. Skip dashboard." }
    }
} else { Write-Info "Skipping RTI Dashboard (--SkipDashboard)" }

# ------------------------------------------------------------------
# Step 8: Deploy Data Agent (optional)
# ------------------------------------------------------------------
if (-not $SkipDataAgent) {
    Write-Step "Step 8: Deploying Data Agent"
    $daScript = Join-Path $OntologyFolder "Deploy-DataAgent.ps1"
    if (-not (Test-Path $daScript)) { $daScript = Join-Path $scriptDir "Deploy-DataAgent.ps1" }
    if (Test-Path $daScript) {
        try { $agentParams = @{ WorkspaceId = $WorkspaceId }; if ($ontologyId) { $agentParams['OntologyId'] = $ontologyId }; & $daScript @agentParams; Write-Success "Data Agent deployed" }
        catch { Write-Warn "Data Agent issue (requires F64+): $_" }
    }
} else { Write-Info "Skipping Data Agent (--SkipDataAgent)" }

# ------------------------------------------------------------------
# Step 9: Deploy Graph Query Set
# ------------------------------------------------------------------
Write-Step "Step 9: Deploying Graph Query Set"
$gqsScript = Join-Path $scriptDir "Deploy-GraphQuerySet.ps1"
if (Test-Path $gqsScript) {
    try { & $gqsScript -WorkspaceId $WorkspaceId -OntologyType $OntologyType -OntologyFolder $OntologyFolder; Write-Success "Graph Query Set deployed" }
    catch { Write-Warn "Graph Query Set issue: $_" }
}

# ------------------------------------------------------------------
# Step 10: Deploy Operations Agent (optional)
# ------------------------------------------------------------------
if (-not $SkipOperationsAgent) {
    Write-Step "Step 10: Deploying Operations Agent"
    $opsScript = Join-Path $OntologyFolder "Deploy-OperationsAgent.ps1"
    if (-not (Test-Path $opsScript)) { $opsScript = Join-Path $scriptDir "Deploy-OperationsAgent.ps1" }
    if (Test-Path $opsScript) {
        try { & $opsScript -WorkspaceId $WorkspaceId; Write-Success "Operations Agent deployed" }
        catch { Write-Warn "Operations Agent issue: $_" }
    }
} else { Write-Info "Skipping Operations Agent (--SkipOperationsAgent)" }

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  $OntologyType DEPLOYMENT SUMMARY" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Workspace     : $WorkspaceId" -ForegroundColor White
Write-Host "  Lakehouse     : $LakehouseName ($lakehouseId)" -ForegroundColor White
Write-Host "  Eventhouse    : $EventhouseName ($eventhouseId)" -ForegroundColor White
if ($semanticModelId) { Write-Host "  Semantic Model: $SemanticModelName ($semanticModelId)" -ForegroundColor White }
if ($ontologyId) { Write-Host "  Ontology      : $OntologyName ($ontologyId)" -ForegroundColor White }
Write-Host ""
Write-Host "  Graph Queries : ontologies\$OntologyType\GraphQueries.gql" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Fabric Portal : https://app.fabric.microsoft.com/" -ForegroundColor Cyan
Write-Host ""
