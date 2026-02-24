<#
.SYNOPSIS
    Deploys the Oil & Gas Refinery Ontology accelerator to Microsoft Fabric.

.DESCRIPTION
    This script automates the deployment of:
    1. A Lakehouse with all CSV data files
    2. A Spark Notebook to load CSV files into Delta tables
    3. An Eventhouse for sensor telemetry streaming data
    4. A Semantic Model with all relationships
    5. An Ontology item
    6. An RTI Dashboard for real-time telemetry visualization (requires tenant setting)
    7. A Data Agent for natural-language data exploration (requires F64+ capacity)
    8. An Operations Agent for real-time monitoring and Teams-based recommendations

.PARAMETER WorkspaceId
    The GUID of the Fabric workspace to deploy to.

.PARAMETER DataFolder
    Path to the local folder containing CSV data files. Defaults to ./data

.PARAMETER LakehouseName
    Name for the lakehouse. Defaults to OilGasRefineryLH

.PARAMETER EventhouseName
    Name for the eventhouse. Defaults to RefineryTelemetryEH

.PARAMETER SemanticModelName
    Name for the semantic model. Defaults to OilGasRefineryModel

.PARAMETER OntologyName
    Name for the ontology. Defaults to OilGasRefineryOntology

.EXAMPLE
    .\Deploy-OilGasOntology.ps1 -WorkspaceId "your-workspace-guid"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceId,

    [Parameter(Mandatory = $false)]
    [string]$DataFolder,

    [Parameter(Mandatory = $false)]
    [string]$LakehouseName = "OilGasRefineryLH",

    [Parameter(Mandatory = $false)]
    [string]$EventhouseName = "RefineryTelemetryEH",

    [Parameter(Mandatory = $false)]
    [string]$SemanticModelName = "OilGasRefineryModel",

    [Parameter(Mandatory = $false)]
    [string]$OntologyName = "OilGasRefineryOntology"
)

# Resolve script root for both dot-sourced and powershell -File invocations
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
if (-not $DataFolder) { $DataFolder = Join-Path $scriptDir "data" }

$ErrorActionPreference = "Stop"

# ============================================================================
# CONFIGURATION
# ============================================================================
$FabricApiBase = "https://api.fabric.microsoft.com/v1"
$OneLakeBase = "https://onelake.dfs.fabric.microsoft.com"

# CSV files to upload to lakehouse (not telemetry)
$LakehouseFiles = @(
    "DimRefinery.csv",
    "DimProcessUnit.csv",
    "DimEquipment.csv",
    "DimPipeline.csv",
    "DimCrudeOil.csv",
    "DimRefinedProduct.csv",
    "DimStorageTank.csv",
    "DimSensor.csv",
    "DimEmployee.csv",
    "FactMaintenance.csv",
    "FactSafetyAlarm.csv",
    "FactProduction.csv",
    "BridgeCrudeOilProcessUnit.csv"
)

# Telemetry file for Eventhouse
$TelemetryFile = "SensorTelemetry.csv"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "=====================================================================" -ForegroundColor Cyan
    Write-Host " $Message" -ForegroundColor Cyan
    Write-Host "=====================================================================" -ForegroundColor Cyan
}

function Write-Info {
    param([string]$Message)
    Write-Host "  [INFO] $Message" -ForegroundColor Gray
}

function Write-Success {
    param([string]$Message)
    Write-Host "  [OK]   $Message" -ForegroundColor Green
}

function Write-Warn {
    param([string]$Message)
    Write-Host "  [WARN] $Message" -ForegroundColor Yellow
}

function Get-FabricToken {
    <#
    .SYNOPSIS
        Retrieves a bearer token for Fabric REST API.
    #>
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Fabric API token. Run 'Connect-AzAccount' first. Error: $_"
        throw
    }
}

function Get-StorageToken {
    <#
    .SYNOPSIS
        Retrieves a bearer token for OneLake (Storage) API.
    #>
    try {
        $token = Get-AzAccessToken -ResourceTypeName Storage
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Storage token. Run 'Connect-AzAccount' first. Error: $_"
        throw
    }
}

function Invoke-FabricApi {
    <#
    .SYNOPSIS
        Calls the Fabric REST API with retry logic for 429/retriable responses.
        Compatible with PowerShell 5.1+.
    #>
    param(
        [string]$Method,
        [string]$Uri,
        [object]$Body = $null,
        [string]$BodyJson = $null,
        [string]$Token,
        [int]$MaxRetries = 10
    )

    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/json"
    }

    # Use pre-built JSON if provided (avoids PS 5.1 ConvertTo-Json crash with large payloads)
    if (-not $BodyJson -and $Body) {
        $BodyJson = $Body | ConvertTo-Json -Depth 10
    }

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $params = @{
                Method      = $Method
                Uri         = $Uri
                Headers     = $headers
                UseBasicParsing = $true
            }
            if ($bodyJson) {
                $params["Body"] = $bodyJson
            }

            # Use Invoke-WebRequest to get status code and headers (PS 5.1 compatible)
            $webResponse = Invoke-WebRequest @params
            $statusCode = $webResponse.StatusCode

            # Handle 202 Accepted (Long Running Operation)
            if ($statusCode -eq 202) {
                $locationHeader = $webResponse.Headers["Location"]
                $opIdHeader = $webResponse.Headers["x-ms-operation-id"]
                if ($locationHeader) {
                    $operationUrl = $locationHeader
                }
                elseif ($opIdHeader) {
                    $operationUrl = "$FabricApiBase/operations/$opIdHeader"
                }
                else {
                    Write-Warn "202 response but no Location or operation-id header found."
                    return $null
                }
                Write-Info "Waiting for long-running operation to complete..."
                return Wait-FabricOperation -OperationUrl $operationUrl -Token $Token
            }

            # Parse JSON response body
            if ($webResponse.Content) {
                try { return $webResponse.Content | ConvertFrom-Json }
                catch { return $webResponse.Content }
            }
            return $null
        }
        catch {
            $ex = $_.Exception
            $statusCode = $null
            $errorBody = ""
            if ($ex -and $ex.Response) {
                $statusCode = [int]$ex.Response.StatusCode
                try {
                    $sr = New-Object System.IO.StreamReader($ex.Response.GetResponseStream())
                    $errorBody = $sr.ReadToEnd()
                    $sr.Close()
                } catch {}
            }

            # Check for retriable errors
            $isRetriable = $false
            if ($errorBody -like "*isRetriable*true*" -or $errorBody -like "*NotAvailableYet*") {
                $isRetriable = $true
            }

            if ($statusCode -eq 429 -or $isRetriable) {
                $retryAfter = if ($isRetriable) { 15 } else { 30 }
                try {
                    $ra = $ex.Response.Headers | Where-Object { $_.Key -eq "Retry-After" } | Select-Object -ExpandProperty Value -First 1
                    if ($ra) { $retryAfter = [int]$ra }
                } catch {}
                $reason = if ($isRetriable) { "Retriable error" } else { "Rate limited (429)" }
                Write-Warn "$reason. Retrying after $retryAfter seconds (attempt $attempt/$MaxRetries)..."
                Start-Sleep -Seconds $retryAfter
            }
            else {
                # Re-throw with error body for better diagnostics
                if ($errorBody) {
                    throw "Fabric API error (HTTP $statusCode): $errorBody"
                }
                throw
            }
        }
    }
    throw "Max retries exceeded for $Uri"
}

function Wait-FabricOperation {
    <#
    .SYNOPSIS
        Polls a Fabric long-running operation until it completes.
    #>
    param(
        [string]$OperationUrl,
        [string]$Token,
        [int]$TimeoutSeconds = 600,
        [int]$PollIntervalSeconds = 10
    )

    $headers = @{ "Authorization" = "Bearer $Token" }
    $elapsed = 0

    while ($elapsed -lt $TimeoutSeconds) {
        Start-Sleep -Seconds $PollIntervalSeconds
        $elapsed += $PollIntervalSeconds

        try {
            $status = Invoke-RestMethod -Method Get -Uri $OperationUrl -Headers $headers
            $state = $status.status
            Write-Info "  Operation status: $state ($elapsed`s elapsed)"

            if ($state -eq "Succeeded") {
                return $status
            }
            elseif ($state -eq "Failed") {
                Write-Error "Operation failed: $($status | ConvertTo-Json -Depth 5)"
                throw "Fabric operation failed"
            }
        }
        catch {
            if ($_.Exception.Response -and $_.Exception.Response.StatusCode -eq 429) {
                Write-Warn "Rate limited while polling. Waiting..."
                Start-Sleep -Seconds 30
            }
            else {
                throw
            }
        }
    }
    throw "Operation timed out after $TimeoutSeconds seconds"
}

function Upload-FileToOneLake {
    <#
    .SYNOPSIS
        Uploads a local file to OneLake via DFS API.
    #>
    param(
        [string]$LocalFilePath,
        [string]$OneLakePath,
        [string]$Token
    )

    $fileBytes = [System.IO.File]::ReadAllBytes($LocalFilePath)
    $fileName = [System.IO.Path]::GetFileName($LocalFilePath)

    # Step 1: Create the file (PUT with resource=file)
    $createUri = "${OneLakePath}/${fileName}?resource=file"
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Length" = "0"
    }
    Invoke-RestMethod -Method Put -Uri $createUri -Headers $headers | Out-Null

    # Step 2: Append data (PATCH with action=append)
    $appendUri = "${OneLakePath}/${fileName}?action=append&position=0"
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Type"  = "application/octet-stream"
        "Content-Length" = $fileBytes.Length.ToString()
    }
    Invoke-RestMethod -Method Patch -Uri $appendUri -Headers $headers -Body $fileBytes | Out-Null

    # Step 3: Flush (PATCH with action=flush)
    $flushUri = "${OneLakePath}/${fileName}?action=flush&position=$($fileBytes.Length)"
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Content-Length" = "0"
    }
    Invoke-RestMethod -Method Patch -Uri $flushUri -Headers $headers | Out-Null
}

# ============================================================================
# MAIN DEPLOYMENT SEQUENCE
# ============================================================================

Write-Host ""
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host "  Oil & Gas Refinery Ontology - Fabric Deployment" -ForegroundColor Yellow
Write-Host "======================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Workspace ID  : $WorkspaceId"
Write-Host "  Data Folder   : $DataFolder"
Write-Host "  Lakehouse     : $LakehouseName"
Write-Host "  Eventhouse    : $EventhouseName"
Write-Host "  Semantic Model: $SemanticModelName"
Write-Host "  Ontology      : $OntologyName"
Write-Host ""

# Validate data folder
if (-not (Test-Path $DataFolder)) {
    Write-Error "Data folder not found: $DataFolder"
    exit 1
}

# ------------------------------------------------------------------
# Step 0: Authenticate
# ------------------------------------------------------------------
Write-Step "Step 0: Authenticating to Azure / Fabric"

# Check if already logged in
$account = Get-AzContext
if (-not $account) {
    Write-Info "No active Azure session. Launching interactive login..."
    Connect-AzAccount
}
else {
    Write-Info "Using existing Azure session: $($account.Account.Id)"
}

$fabricToken = Get-FabricToken
$storageToken = Get-StorageToken
Write-Success "Authenticated successfully"

# ------------------------------------------------------------------
# Step 1: Create Lakehouse
# ------------------------------------------------------------------
Write-Step "Step 1: Creating Lakehouse '$LakehouseName'"

$lakehouseBody = @{
    displayName = $LakehouseName
    type        = "Lakehouse"
    description = "Oil and Gas Refinery data lakehouse for ontology accelerator"
}

try {
    $lakehouse = Invoke-FabricApi -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Body $lakehouseBody `
        -Token $fabricToken

    $lakehouseId = $lakehouse.id
    Write-Success "Lakehouse created: $lakehouseId"
}
catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*" -or $_.Exception.Message -like "*already in use*") {
        Write-Warn "Lakehouse '$LakehouseName' already exists. Looking up ID..."
        $items = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Lakehouse" `
            -Token $fabricToken
        $lakehouse = $items.value | Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
        $lakehouseId = $lakehouse.id
        Write-Info "Using existing lakehouse: $lakehouseId"
    }
    else { throw }
}

# Wait for lakehouse provisioning
Start-Sleep -Seconds 10

# Get the SQL endpoint connection string for Direct Lake semantic model
$sqlEndpointConnStr = ""
try {
    $fabricToken = Get-FabricToken
    $lhProps = Invoke-FabricApi -Method Get `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$lakehouseId" `
        -Token $fabricToken
    if ($lhProps.properties -and $lhProps.properties.sqlEndpointProperties) {
        $sqlEndpointConnStr = $lhProps.properties.sqlEndpointProperties.connectionString
        Write-Info "SQL endpoint: $sqlEndpointConnStr"
    }
} catch {
    Write-Warn "Could not retrieve SQL endpoint. Semantic model may need manual configuration."
}

# ------------------------------------------------------------------
# Step 2: Upload CSV Files to Lakehouse Files/
# ------------------------------------------------------------------
Write-Step "Step 2: Uploading CSV files to Lakehouse Files/"

$oneLakeFilesPath = "$OneLakeBase/$WorkspaceId/$lakehouseId/Files"

# Create the Files directory (may already exist)
try {
    $headers = @{
        "Authorization" = "Bearer $storageToken"
        "Content-Length" = "0"
    }
    Invoke-RestMethod -Method Put -Uri "${oneLakeFilesPath}?resource=directory" -Headers $headers | Out-Null
}
catch {
    # Directory might already exist, which is fine
}

$uploadedCount = 0
foreach ($fileName in $LakehouseFiles) {
    $localPath = Join-Path $DataFolder $fileName
    if (-not (Test-Path $localPath)) {
        Write-Warn "File not found, skipping: $fileName"
        continue
    }
    Write-Info "Uploading $fileName..."
    Upload-FileToOneLake -LocalFilePath $localPath -OneLakePath $oneLakeFilesPath -Token $storageToken
    $uploadedCount++
    Write-Success "Uploaded $fileName"
}
Write-Success "Uploaded $uploadedCount / $($LakehouseFiles.Count) files to lakehouse"

# ------------------------------------------------------------------
# Step 3: Create and Run Notebook to Load Tables
# ------------------------------------------------------------------
Write-Step "Step 3: Creating Spark Notebook to load CSV into Delta tables"

# Read the notebook content from the companion file
$notebookSourcePath = Join-Path (Join-Path $scriptDir "deploy") "LoadDataToTables.py"
$notebookCode = Get-Content -Path $notebookSourcePath -Raw
Write-Info "Notebook source loaded ($($notebookCode.Length) chars)"

# Build the ipynb JSON manually to avoid PS 5.1 ConvertTo-Json crash with large strings
# IMPORTANT: ipynb format requires source to be an array of strings (one per line)
$lines = $notebookCode -split "`r?`n"
$sourceArray = ""
for ($i = 0; $i -lt $lines.Count; $i++) {
    $escapedLine = $lines[$i].Replace('\', '\\').Replace('"', '\"').Replace("`t", '\t')
    if ($i -lt ($lines.Count - 1)) {
        $sourceArray += '"' + $escapedLine + '\n",'
    } else {
        $sourceArray += '"' + $escapedLine + '"'
    }
}
$notebookJson = '{"metadata":{"language_info":{"name":"python"},"kernel_info":{"name":"synapse_pyspark"}},"nbformat":4,"nbformat_minor":5,"cells":[{"cell_type":"code","source":[' + $sourceArray + '],"metadata":{},"outputs":[]}]}'
$notebookBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($notebookJson))
Write-Info "Notebook payload encoded (Base64 length: $($notebookBase64.Length))"

# Build the API request body JSON manually (PS 5.1 ConvertTo-Json chokes on large nested payloads)
$nbDescription = "Loads Oil and Gas Refinery CSV files from lakehouse Files into Delta tables"
$createNbJson = '{"displayName":"OilGasRefinery_LoadTables","type":"Notebook","description":"' + $nbDescription + '","definition":{"format":"ipynb","parts":[{"path":"notebook-content.py","payload":"' + $notebookBase64 + '","payloadType":"InlineBase64"}]}}'

try {
    Write-Info "Sending notebook creation request..."
    $nbHeaders = @{
        "Authorization" = "Bearer $fabricToken"
        "Content-Type"  = "application/json"
    }
    $nbResponse = Invoke-WebRequest -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Headers $nbHeaders `
        -Body $createNbJson `
        -UseBasicParsing

    if ($nbResponse.StatusCode -eq 202) {
        Write-Info "Notebook creation accepted (202). Waiting for provisioning..."
        # Poll the operation URL if available, or just wait and lookup by name
        $opUrl = $null
        try { $opUrl = $nbResponse.Headers["Location"] } catch {}
        if (-not $opUrl) {
            try { 
                $opId = $nbResponse.Headers["x-ms-operation-id"]
                if ($opId) { $opUrl = "$FabricApiBase/operations/$opId" }
            } catch {}
        }
        if ($opUrl) {
            $pollHeaders = @{ "Authorization" = "Bearer $fabricToken" }
            $maxPoll = 120; $polled = 0
            while ($polled -lt $maxPoll) {
                Start-Sleep -Seconds 10
                $polled += 10
                try {
                    $pollResp = Invoke-WebRequest -Method Get -Uri $opUrl -Headers $pollHeaders -UseBasicParsing
                    $pollData = $pollResp.Content | ConvertFrom-Json
                    Write-Info "  Operation: $($pollData.status) ($polled`s)"
                    if ($pollData.status -eq "Succeeded") { break }
                    if ($pollData.status -eq "Failed") { Write-Warn "Operation failed"; break }
                } catch {
                    Write-Warn "Poll error: $($_.Exception.Message)"
                    break
                }
            }
        }
        else {
            Start-Sleep -Seconds 15
        }
        # Look up the notebook by name
        Write-Info "Looking up notebook ID..."
        $nbItems = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" `
            -Token $fabricToken
        $notebook = $nbItems.value | Where-Object { $_.displayName -eq "OilGasRefinery_LoadTables" } | Select-Object -First 1
        $notebookId = $notebook.id
    }
    else {
        $notebook = $nbResponse.Content | ConvertFrom-Json
        $notebookId = $notebook.id
    }
    Write-Success "Notebook created: $notebookId"
}
catch {
    $nbErrBody = ""
    if ($_.Exception.Response) {
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $nbErrBody = $sr.ReadToEnd()
            $sr.Close()
        } catch {}
    }
    $errMsg = "$($_.Exception.Message) $nbErrBody"
    if ($errMsg -like "*ItemDisplayNameAlreadyInUse*" -or $errMsg -like "*already in use*") {
        Write-Warn "Notebook already exists. Looking up..."
        $items = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" `
            -Token $fabricToken
        $notebook = $items.value | Where-Object { $_.displayName -eq "OilGasRefinery_LoadTables" } | Select-Object -First 1
        $notebookId = $notebook.id
        Write-Info "Using existing notebook: $notebookId"
    }
    else { throw }
}

# Run the notebook
Write-Info "Running notebook to load CSV files into Delta tables..."
Write-Info "This operation may take several minutes as Spark session starts..."

$runBody = @{
    executionData = @{
        parameters = @{
            lakehouse_id = @{ value = $lakehouseId; type = "string" }
            workspace_id = @{ value = $WorkspaceId; type = "string" }
        }
    }
}

try {
    $runResult = Invoke-FabricApi -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$notebookId/jobs/instances?jobType=RunNotebook" `
        -Body $runBody `
        -Token $fabricToken

    Write-Success "Notebook execution started"

    # Poll for notebook completion
    if ($runResult -and $runResult.id) {
        $jobId = $runResult.id
        Write-Info "Job ID: $jobId - waiting for completion..."
        $maxWait = 600
        $waited = 0
        while ($waited -lt $maxWait) {
            Start-Sleep -Seconds 15
            $waited += 15
            $jobStatus = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$notebookId/jobs/instances/$jobId" `
                -Token $fabricToken
            $state = $jobStatus.status
            Write-Info "  Notebook job status: $state ($waited`s)"
            if ($state -eq "Completed") {
                Write-Success "Notebook execution completed successfully"
                break
            }
            elseif ($state -eq "Failed" -or $state -eq "Cancelled") {
                Write-Warn "Notebook job $state. You may need to run it manually."
                break
            }
        }
    }
}
catch {
    Write-Warn "Could not run notebook automatically: $_"
    Write-Warn "Please run the notebook 'OilGasRefinery_LoadTables' manually from the Fabric portal."
}

# ------------------------------------------------------------------
# Step 4: Create Eventhouse for Telemetry
# ------------------------------------------------------------------
Write-Step "Step 4: Creating Eventhouse '$EventhouseName' for telemetry data"

$eventhouseBody = @{
    displayName = $EventhouseName
    type        = "Eventhouse"
    description = "Eventhouse for Oil & Gas Refinery sensor telemetry streaming data"
}

try {
    $eventhouse = Invoke-FabricApi -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Body $eventhouseBody `
        -Token $fabricToken

    $eventhouseId = $eventhouse.id
    Write-Success "Eventhouse created: $eventhouseId"
}
catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*" -or $_.Exception.Message -like "*already in use*") {
        Write-Warn "Eventhouse '$EventhouseName' already exists."
        $items = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Eventhouse" `
            -Token $fabricToken
        $eventhouse = $items.value | Where-Object { $_.displayName -eq $EventhouseName } | Select-Object -First 1
        $eventhouseId = $eventhouse.id
    }
    else { throw }
}

Write-Info "Eventhouse is being provisioned. Telemetry data (SensorTelemetry.csv)"
Write-Info "must be uploaded manually via: Eventhouse > KQL Database > Get data > Local file"
Write-Info "See SETUP_GUIDE.md Step 5 for detailed instructions."

# ------------------------------------------------------------------
# Step 5: Create Semantic Model (TMDL format)
# ------------------------------------------------------------------
Write-Step "Step 5: Creating Semantic Model '$SemanticModelName' (TMDL)"

# Read all TMDL definition files from deploy/SemanticModel/ folder
$tmdlRoot = Join-Path (Join-Path $scriptDir "deploy") "SemanticModel"
$tmdlDefDir = Join-Path $tmdlRoot "definition"
$tmdlTablesDir = Join-Path $tmdlDefDir "tables"

if (Test-Path $tmdlRoot) {
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

    # 2. definition/*.tmdl files (database.tmdl, model.tmdl, expressions.tmdl, relationships.tmdl)
    $defFiles = Get-ChildItem -Path $tmdlDefDir -Filter "*.tmdl" -File | Sort-Object Name
    foreach ($f in $defFiles) {
        $fileContent = Get-Content -Path $f.FullName -Raw -Encoding UTF8

        # Replace placeholders in expressions.tmdl with actual lakehouse connection info
        if ($f.Name -eq "expressions.tmdl") {
            $fileContent = $fileContent -replace '\{\{SQL_ENDPOINT\}\}', $sqlEndpointConnStr
            $fileContent = $fileContent -replace '\{\{LAKEHOUSE_NAME\}\}', $LakehouseName
        }

        $fileBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($fileContent))
        $partPath = "definition/" + $f.Name
        $smParts += '{"path":"' + $partPath + '","payload":"' + $fileBase64 + '","payloadType":"InlineBase64"}'
        Write-Info "  Loaded $partPath"
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

    # Build JSON manually (PS 5.1 ConvertTo-Json crashes with large payloads)
    $smDescription = "Direct Lake semantic model for Oil and Gas Refinery ontology - 13 tables, 17 relationships, DAX measures"
    $partsJson = $smParts -join ","
    $createSmJson = '{"displayName":"' + $SemanticModelName + '","type":"SemanticModel","description":"' + $smDescription + '","definition":{"parts":[' + $partsJson + ']}}'
    Write-Info "SM payload size: $($createSmJson.Length) chars"

    try {
        Write-Info "Sending semantic model creation request..."
        $smHeaders = @{
            "Authorization" = "Bearer $fabricToken"
            "Content-Type"  = "application/json"
        }
        $smResponse = Invoke-WebRequest -Method Post `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
            -Headers $smHeaders `
            -Body $createSmJson `
            -UseBasicParsing

        if ($smResponse.StatusCode -eq 202) {
            Write-Info "Semantic model creation accepted (202). Waiting for provisioning..."
            $smOpUrl = $null
            try { $smOpUrl = $smResponse.Headers["Location"] } catch {}
            if (-not $smOpUrl) {
                try {
                    $smOpId = $smResponse.Headers["x-ms-operation-id"]
                    if ($smOpId) { $smOpUrl = "$FabricApiBase/operations/$smOpId" }
                } catch {}
            }
            if ($smOpUrl) {
                $smPollHeaders = @{ "Authorization" = "Bearer $fabricToken" }
                $smMaxPoll = 120; $smPolled = 0
                while ($smPolled -lt $smMaxPoll) {
                    Start-Sleep -Seconds 10
                    $smPolled += 10
                    try {
                        $fabricToken = Get-FabricToken
                        $smPollHeaders = @{ "Authorization" = "Bearer $fabricToken" }
                        $smPollResp = Invoke-WebRequest -Method Get -Uri $smOpUrl -Headers $smPollHeaders -UseBasicParsing
                        $smPollData = $smPollResp.Content | ConvertFrom-Json
                        Write-Info "  Operation: $($smPollData.status) ($smPolled`s)"
                        if ($smPollData.status -eq "Succeeded") { break }
                        if ($smPollData.status -eq "Failed") {
                            Write-Warn "SM operation failed: $($smPollResp.Content)"
                            break
                        }
                    } catch {
                        Write-Warn "SM poll error: $($_.Exception.Message)"
                        break
                    }
                }
            }
            else {
                Start-Sleep -Seconds 15
            }
            # Look up by name
            Write-Info "Looking up semantic model ID..."
            $smItems = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=SemanticModel" `
                -Token $fabricToken
            $sm = $smItems.value | Where-Object { $_.displayName -eq $SemanticModelName } | Select-Object -First 1
            $semanticModelId = $sm.id
        }
        else {
            $semanticModel = $smResponse.Content | ConvertFrom-Json
            $semanticModelId = $semanticModel.id
        }
        Write-Success "Semantic model created: $semanticModelId"
    }
    catch {
        $smErrBody = ""
        if ($_.Exception.Response) {
            try {
                $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $smErrBody = $sr.ReadToEnd()
                $sr.Close()
            } catch {}
        }
        $errMsg = "$($_.Exception.Message) $smErrBody"
        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*") {
            Write-Warn "Semantic model '$SemanticModelName' already exists."
        }
        else {
            Write-Warn "Semantic model creation encountered an issue: $errMsg"
            Write-Warn "You may need to create it manually. See SEMANTIC_MODEL_GUIDE.md"
        }
    }
}
else {
    Write-Warn "TMDL folder not found at: $tmdlRoot"
    Write-Info "Please create the semantic model manually from the lakehouse:"
    Write-Info "  1. Open '$LakehouseName' in Fabric"
    Write-Info "  2. Click 'New semantic model' in the ribbon"
    Write-Info "  3. Follow SEMANTIC_MODEL_GUIDE.md for relationship setup"
}

# ------------------------------------------------------------------
# Step 6: Create Ontology
# ------------------------------------------------------------------
Write-Step "Step 6: Creating Ontology '$OntologyName'"

$ontologyBody = @{
    displayName = $OntologyName
    description = "Oil and Gas Refinery ontology - entity types for refineries, process units, equipment, sensors, products, maintenance, and safety"
    type        = "Ontology"
}

try {
    $ontology = Invoke-FabricApi -Method Post `
        -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
        -Body $ontologyBody `
        -Token $fabricToken

    $ontologyId = $ontology.id
    Write-Success "Ontology item created: $ontologyId"
    Write-Info "Open the ontology in Fabric portal to configure entity types and relationships."
    Write-Info "  -> Or generate from the semantic model: Open '$SemanticModelName' > Generate Ontology"
}
catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*") {
        Write-Warn "Ontology '$OntologyName' already exists."
    }
    else {
        Write-Warn "Ontology creation via API may not be available yet (preview feature)."
        Write-Warn "To create it manually:"
        Write-Info "  1. Open '$SemanticModelName' in Fabric"
        Write-Info "  2. Click 'Generate Ontology' from the ribbon"
        Write-Info "  3. Follow SETUP_GUIDE.md Step 4 for entity type and relationship configuration"
    }
}

# ------------------------------------------------------------------
# Step 7: Deploy RTI Dashboard
# ------------------------------------------------------------------
Write-Step "Step 7: Deploying RTI Dashboard"

$rtiDashboardScript = Join-Path $scriptDir "deploy\Deploy-RTIDashboard.ps1"
$dashboardId = $null
if (Test-Path $rtiDashboardScript) {
    # Need KQL DB query URI
    $kqlQueryUri = $null
    try {
        $kqlDbInfo = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases" `
            -Token $fabricToken
        $kqlDb = $kqlDbInfo.value | Where-Object { $_.displayName -eq $EventhouseName }
        if ($kqlDb) {
            $kqlDbId = $kqlDb.id
            $kqlDbDetail = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases/$kqlDbId" `
                -Token $fabricToken
            $kqlQueryUri = $kqlDbDetail.properties.queryServiceUri
        }
    } catch {
        Write-Warn "Could not retrieve KQL DB details: $_"
    }

    if ($kqlQueryUri -and $kqlDbId) {
        try {
            & $rtiDashboardScript `
                -WorkspaceId $WorkspaceId `
                -KqlDatabaseId $kqlDbId `
                -QueryServiceUri $kqlQueryUri
            Write-Success "RTI Dashboard deployment script executed."
        }
        catch {
            Write-Warn "RTI Dashboard deployment encountered an issue: $_"
            Write-Info "You can re-run: deploy\Deploy-RTIDashboard.ps1 -WorkspaceId $WorkspaceId -KqlDatabaseId $kqlDbId -QueryServiceUri $kqlQueryUri"
        }
    }
    else {
        Write-Warn "Could not find KQL Database URI. Skipping RTI Dashboard."
        Write-Info "Run manually: deploy\Deploy-RTIDashboard.ps1"
    }
}
else {
    Write-Warn "RTI Dashboard script not found at: $rtiDashboardScript"
}

# ------------------------------------------------------------------
# Step 8: Deploy Data Agent
# ------------------------------------------------------------------
Write-Step "Step 8: Deploying Data Agent"

$dataAgentScript = Join-Path $scriptDir "deploy\Deploy-DataAgent.ps1"
if (Test-Path $dataAgentScript) {
    try {
        $agentParams = @{ WorkspaceId = $WorkspaceId }
        if ($ontologyId) { $agentParams['OntologyId'] = $ontologyId }

        & $dataAgentScript @agentParams
        Write-Success "Data Agent deployment script executed."
    }
    catch {
        Write-Warn "Data Agent deployment encountered an issue: $_"
        Write-Info "Data Agents require Fabric capacity F64+. Trial capacity is not supported."
        Write-Info "You can re-run: deploy\Deploy-DataAgent.ps1 -WorkspaceId $WorkspaceId -OntologyId $ontologyId"
    }
}
else {
    Write-Warn "Data Agent script not found at: $dataAgentScript"
}

# ------------------------------------------------------------------
# Step 9: Deploy Graph Query Set
# ------------------------------------------------------------------
Write-Step "Step 9: Deploying Graph Query Set with example GQL queries"

$gqsScript = Join-Path $scriptDir "deploy\Deploy-GraphQuerySet.ps1"
if (Test-Path $gqsScript) {
    try {
        $gqsParams = @{ WorkspaceId = $WorkspaceId }
        # Auto-detect GraphModel from workspace (script handles this if GraphModelId omitted)
        & $gqsScript @gqsParams
        Write-Success "Graph Query Set deployment script executed."
    }
    catch {
        Write-Warn "Graph Query Set deployment encountered an issue: $_"
        Write-Info "You can re-run: deploy\Deploy-GraphQuerySet.ps1 -WorkspaceId $WorkspaceId"
    }
}
else {
    Write-Warn "Graph Query Set script not found at: $gqsScript"
}

# ------------------------------------------------------------------
# Step 10: Deploy Operations Agent
# ------------------------------------------------------------------
Write-Step "Step 10: Deploying Operations Agent (Real-Time Intelligence)"

$opsAgentScript = Join-Path $scriptDir "deploy\Deploy-OperationsAgent.ps1"
if (Test-Path $opsAgentScript) {
    try {
        $opsParams = @{ WorkspaceId = $WorkspaceId }
        & $opsAgentScript @opsParams
        Write-Success "Operations Agent deployment script executed."
    }
    catch {
        Write-Warn "Operations Agent deployment encountered an issue: $_"
        Write-Info "You can re-run: deploy\Deploy-OperationsAgent.ps1 -WorkspaceId $WorkspaceId"
    }
}
else {
    Write-Warn "Operations Agent script not found at: $opsAgentScript"
}

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT SUMMARY" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Workspace     : $WorkspaceId" -ForegroundColor White
Write-Host "  Lakehouse     : $LakehouseName ($lakehouseId)" -ForegroundColor White
Write-Host "  Eventhouse    : $EventhouseName ($eventhouseId)" -ForegroundColor White
if ($semanticModelId) {
    Write-Host "  Semantic Model: $SemanticModelName ($semanticModelId)" -ForegroundColor White
}
if ($ontologyId) {
    Write-Host "  Ontology      : $OntologyName ($ontologyId)" -ForegroundColor White
}
Write-Host ""
Write-Host "  REMAINING MANUAL STEPS:" -ForegroundColor Yellow
Write-Host "  -------------------------------------------------" -ForegroundColor Yellow
Write-Host "  1. If notebook did not run: Execute 'OilGasRefinery_LoadTables' notebook" -ForegroundColor Yellow
Write-Host "  2. Upload SensorTelemetry.csv to Eventhouse KQL database" -ForegroundColor Yellow
Write-Host "  3. If semantic model was created manually: define relationships" -ForegroundColor Yellow
Write-Host "     (see SEMANTIC_MODEL_GUIDE.md)" -ForegroundColor Yellow
Write-Host "  4. Open ontology and configure entity types + relationships" -ForegroundColor Yellow
Write-Host "     (see SETUP_GUIDE.md Step 4)" -ForegroundColor Yellow
Write-Host "  5. RTI Dashboard: Requires 'Create Real-Time dashboards' tenant setting" -ForegroundColor Yellow
Write-Host "  6. Data Agent: Uses the Ontology as its sole data source (requires F64+)" -ForegroundColor Yellow
Write-Host "  7. Graph Query Set: Open the GQS, select graph model, copy queries from deploy/RefineryGraphQueries.gql" -ForegroundColor Yellow
Write-Host "  8. Operations Agent: Open agent in Fabric, add Knowledge Source (KQL DB), configure Actions, then Start" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Fabric Portal: https://app.fabric.microsoft.com/" -ForegroundColor Cyan
Write-Host ""
