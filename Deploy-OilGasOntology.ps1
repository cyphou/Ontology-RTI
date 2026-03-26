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
if (-not $DataFolder) { $DataFolder = Join-Path $scriptDir "ontologies\OilGasRefinery\data" }

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

# Wait for lakehouse SQL endpoint to become available (required for Direct Lake semantic model)
# The SQL endpoint is provisioned asynchronously and may take up to 2-3 minutes
$sqlEndpointConnStr = ""
$sqlEndpointMaxWait = 180
$sqlEndpointWaited = 0
Write-Info "Waiting for SQL endpoint to provision (may take up to 3 minutes)..."
while ($sqlEndpointWaited -lt $sqlEndpointMaxWait) {
    Start-Sleep -Seconds 15
    $sqlEndpointWaited += 15
    try {
        $fabricToken = Get-FabricToken
        $lhProps = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/lakehouses/$lakehouseId" `
            -Token $fabricToken
        if ($lhProps.properties -and $lhProps.properties.sqlEndpointProperties -and $lhProps.properties.sqlEndpointProperties.connectionString) {
            $sqlEndpointConnStr = $lhProps.properties.sqlEndpointProperties.connectionString
            Write-Success "SQL endpoint ready: $sqlEndpointConnStr ($sqlEndpointWaited`s)"
            break
        }
        Write-Info "  SQL endpoint not ready yet ($sqlEndpointWaited`s)..."
    } catch {
        Write-Info "  Waiting for SQL endpoint ($sqlEndpointWaited`s)..."
    }
}
if (-not $sqlEndpointConnStr) {
    Write-Warn "SQL endpoint not available after $sqlEndpointMaxWait`s. Semantic model may need manual configuration."
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

# Build Fabric native notebook format (.py with # META sections)
# This format preserves lakehouse dependency metadata (ipynb format strips it)
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
# Normalize line endings to LF (Fabric native format uses Unix line endings)
$notebookNativeContent = $notebookNativeContent -replace "`r`n", "`n"
$notebookBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($notebookNativeContent))
Write-Info "Notebook content encoded (Base64 length: $($notebookBase64.Length))"

# ---- Step 3A: Create notebook item (without definition) ----
$nbDescription = "Loads Oil and Gas Refinery CSV files from lakehouse Files into Delta tables"
$createNbBodyJson = @{
    displayName = "OilGasRefinery_LoadTables"
    type        = "Notebook"
    description = $nbDescription
} | ConvertTo-Json -Depth 5
$nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }

$notebookId = $null
for ($createAttempt = 1; $createAttempt -le 3; $createAttempt++) {
    if ($createAttempt -gt 1) {
        Write-Info "Notebook creation retry $createAttempt/3 - waiting 15s..."
        Start-Sleep -Seconds 15
        $fabricToken = Get-FabricToken
        $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
    }
    try {
        Write-Info "Creating notebook item (attempt $createAttempt)..."
        $nbCreateResp = Invoke-WebRequest -Method Post `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items" `
            -Headers $nbHeaders -Body $createNbBodyJson -UseBasicParsing
        Write-Info "  Creation HTTP status: $($nbCreateResp.StatusCode)"

        if ($nbCreateResp.StatusCode -eq 201) {
            # Immediate success - parse item from response
            $nbObj = $nbCreateResp.Content | ConvertFrom-Json
            $notebookId = $nbObj.id
        }
        elseif ($nbCreateResp.StatusCode -eq 202) {
            # Long-running operation - poll
            $nbOpUrl = $nbCreateResp.Headers["Location"]
            if ($nbOpUrl) {
                for ($p = 1; $p -le 12; $p++) {
                    Start-Sleep -Seconds 5
                    $nbPoll = Invoke-RestMethod -Uri $nbOpUrl -Headers @{Authorization = "Bearer $fabricToken"}
                    Write-Info "  Creation LRO: $($nbPoll.status) ($($p*5)s)"
                    if ($nbPoll.status -eq "Succeeded") { break }
                    if ($nbPoll.status -eq "Failed") { Write-Warn "Creation LRO failed"; break }
                }
            }
            # Look up notebook by name to get its actual item ID
            Start-Sleep -Seconds 3
            $nbItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" `
                -Headers @{Authorization = "Bearer $fabricToken"}).value
            $nbFound = $nbItems | Where-Object { $_.displayName -eq "OilGasRefinery_LoadTables" } | Select-Object -First 1
            if ($nbFound) { $notebookId = $nbFound.id }
        }

        if ($notebookId) {
            Write-Success "Notebook item created: $notebookId"
            break
        }
        Write-Warn "Notebook not found after creation attempt $createAttempt"
    }
    catch {
        $nbErrBody = ""
        try {
            $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $nbErrBody = $sr.ReadToEnd(); $sr.Close()
        } catch {}
        $errMsg = "$($_.Exception.Message) $nbErrBody"
        if ($errMsg -like "*ItemDisplayNameAlreadyInUse*" -or $errMsg -like "*already in use*") {
            Write-Warn "Notebook already exists - looking up..."
            $nbItems = (Invoke-RestMethod -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Notebook" `
                -Headers @{Authorization = "Bearer $fabricToken"}).value
            $nbFound = $nbItems | Where-Object { $_.displayName -eq "OilGasRefinery_LoadTables" } | Select-Object -First 1
            if ($nbFound) { $notebookId = $nbFound.id; Write-Info "Using existing notebook: $notebookId" }
            break
        }
        Write-Warn "Creation error (attempt $createAttempt): $errMsg"
    }
}

# ---- Step 3B: Update definition with lakehouse binding ----
$definitionApplied = $false
if ($notebookId) {
    $updateDefJson = '{"definition":{"parts":[{"path":"notebook-content.py","payload":"' + $notebookBase64 + '","payloadType":"InlineBase64"}]}}'
    for ($defAttempt = 1; $defAttempt -le 3; $defAttempt++) {
        if ($defAttempt -gt 1) {
            Write-Info "Definition update retry $defAttempt/3 - waiting 10s..."
            Start-Sleep -Seconds 10
            $fabricToken = Get-FabricToken
        }
        $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
        try {
            Write-Info "Updating notebook definition (attempt $defAttempt)..."
            $udResp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$notebookId/updateDefinition" `
                -Headers $nbHeaders -Body $updateDefJson -UseBasicParsing
            Write-Info "  updateDefinition HTTP status: $($udResp.StatusCode)"

            if ($udResp.StatusCode -eq 200) {
                $definitionApplied = $true
            }
            elseif ($udResp.StatusCode -eq 202) {
                $udOpUrl = $udResp.Headers["Location"]
                if ($udOpUrl) {
                    for ($p = 1; $p -le 12; $p++) {
                        Start-Sleep -Seconds 5
                        $udPoll = Invoke-RestMethod -Uri $udOpUrl -Headers @{Authorization = "Bearer $fabricToken"}
                        Write-Info "  Definition LRO: $($udPoll.status) ($($p*5)s)"
                        if ($udPoll.status -eq "Succeeded") { $definitionApplied = $true; break }
                        if ($udPoll.status -eq "Failed") {
                            $failDetail = $udPoll | ConvertTo-Json -Depth 5 -Compress
                            Write-Warn "Definition LRO failed: $failDetail"
                            break
                        }
                    }
                }
            }

            if ($definitionApplied) {
                Write-Success "Notebook definition updated with lakehouse binding"
                break
            }
        }
        catch {
            Write-Warn "Definition update error (attempt $defAttempt): $($_.Exception.Message)"
        }
    }
    if (-not $definitionApplied) {
        Write-Warn "Failed to update notebook definition. Please update it manually."
    }
}

# ---- Step 3C: Run the notebook ----
$notebookSuccess = $false
if ($notebookId -and $definitionApplied) {
    Write-Info "Running notebook to load CSV files into Delta tables..."
    Write-Info "This operation may take several minutes as Spark session starts..."
    Start-Sleep -Seconds 15

    for ($runAttempt = 1; $runAttempt -le 3; $runAttempt++) {
        if ($runAttempt -gt 1) {
            Write-Info "Run retry $runAttempt/3 - waiting 30s..."
            Start-Sleep -Seconds 30
            $fabricToken = Get-FabricToken
        }
        $nbHeaders = @{ "Authorization" = "Bearer $fabricToken"; "Content-Type" = "application/json" }
        try {
            Write-Info "Starting notebook run (attempt $runAttempt)..."
            $runResp = Invoke-WebRequest -Method Post `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/items/$notebookId/jobs/instances?jobType=RunNotebook" `
                -Headers $nbHeaders -UseBasicParsing
            Write-Info "  Run HTTP status: $($runResp.StatusCode)"

            if ($runResp.StatusCode -eq 202) {
                $jobLoc = $runResp.Headers["Location"]
                if ($jobLoc) {
                    $maxWait = 600; $waited = 0
                    while ($waited -lt $maxWait) {
                        Start-Sleep -Seconds 15; $waited += 15
                        try {
                            $jobStat = Invoke-RestMethod -Uri $jobLoc -Headers @{Authorization = "Bearer $fabricToken"}
                            Write-Info "  Notebook job: $($jobStat.status) ($waited`s)"
                            if ($jobStat.status -eq "Completed") {
                                Write-Success "Notebook execution completed successfully"
                                $notebookSuccess = $true; break
                            }
                            if ($jobStat.status -eq "Failed" -or $jobStat.status -eq "Cancelled") {
                                $reason = ""
                                if ($jobStat.failureReason) { $reason = $jobStat.failureReason.message }
                                Write-Warn "Notebook job $($jobStat.status): $reason"
                                break
                            }
                        }
                        catch {
                            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 404) {
                                Write-Info "  Job not ready yet ($waited`s)"
                            } else { Write-Warn "  Poll error: $($_.Exception.Message)" }
                        }
                    }
                }
            }
            if ($notebookSuccess) { break }
        }
        catch {
            Write-Warn "Notebook run error (attempt $runAttempt): $($_.Exception.Message)"
        }
    }

    if (-not $notebookSuccess) {
        Write-Warn "Notebook did not complete successfully."
        Write-Warn "Please run 'OilGasRefinery_LoadTables' manually from the Fabric portal."
    }
}
elseif ($notebookId -and -not $definitionApplied) {
    Write-Warn "Skipping notebook run - definition was not applied. Update and run manually."
}
else {
    Write-Warn "Failed to create notebook. Please create and run it manually."
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

Write-Info "Eventhouse provisioned. KQL tables and data will be loaded in Step 4b."

# ------------------------------------------------------------------
# Step 4b: Create KQL Tables and Ingest Data
# ------------------------------------------------------------------
Write-Step "Step 4b: Creating KQL tables and ingesting telemetry data"

$kqlTablesScript = Join-Path $scriptDir "deploy\Deploy-KqlTables.ps1"
if (Test-Path $kqlTablesScript) {
    try {
        $kqlParams = @{
            WorkspaceId  = $WorkspaceId
            EventhouseId = $eventhouseId
            DataFolder   = $DataFolder
        }
        & $kqlTablesScript @kqlParams
        Write-Success "KQL tables created and data ingested."
    }
    catch {
        Write-Warn "KQL table deployment encountered an issue: $_"
        Write-Info "You can re-run: deploy\Deploy-KqlTables.ps1 -WorkspaceId $WorkspaceId -EventhouseId $eventhouseId -DataFolder `"$DataFolder`""
        Write-Info "Or upload SensorTelemetry.csv manually via Eventhouse > KQL Database > Get data > Local file"
    }
}
else {
    Write-Warn "KQL tables script not found at: $kqlTablesScript"
    Write-Info "Upload SensorTelemetry.csv manually via: Eventhouse > KQL Database > Get data > Local file"
}

# ------------------------------------------------------------------
# Step 5: Create Semantic Model (TMDL format)
# ------------------------------------------------------------------
Write-Step "Step 5: Creating Semantic Model '$SemanticModelName' (TMDL)"

# Read all TMDL definition files — prefer ontologies/OilGasRefinery/SemanticModel/
$tmdlRoot = Join-Path $scriptDir "ontologies\OilGasRefinery\SemanticModel"
if (-not (Test-Path $tmdlRoot)) { $tmdlRoot = Join-Path (Join-Path $scriptDir "deploy") "SemanticModel" }
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
}
catch {
    if ($_.Exception.Message -like "*ItemDisplayNameAlreadyInUse*" -or $_.Exception.Message -like "*already in use*") {
        Write-Warn "Ontology '$OntologyName' already exists. Looking up..."
        $ontItems = Invoke-FabricApi -Method Get `
            -Uri "$FabricApiBase/workspaces/$WorkspaceId/items?type=Ontology" `
            -Token $fabricToken
        $existingOnt = $ontItems.value | Where-Object { $_.displayName -eq $OntologyName } | Select-Object -First 1
        if ($existingOnt) { $ontologyId = $existingOnt.id; Write-Info "Using existing ontology: $ontologyId" }
    }
    else {
        Write-Warn "Ontology creation encountered an issue: $($_.Exception.Message)"
    }
}

# Populate ontology with entity types, relationships, and data bindings
if ($ontologyId) {
    $buildOntologyScript = Join-Path $scriptDir "deploy\Build-Ontology.ps1"
    if (Test-Path $buildOntologyScript) {
        Write-Info "Building ontology entities and relationships..."
        # Retrieve KQL database info for time-series bindings
        $ontKqlDbId = $null
        $ontKqlClusterUri = $null
        try {
            $fabricToken = Get-FabricToken
            $ontKqlDbInfo = Invoke-FabricApi -Method Get `
                -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases" `
                -Token $fabricToken
            $ontKqlDb = $ontKqlDbInfo.value | Where-Object { $_.displayName -eq $EventhouseName }
            if ($ontKqlDb) {
                $ontKqlDbId = $ontKqlDb.id
                $ontKqlDbDetail = Invoke-FabricApi -Method Get `
                    -Uri "$FabricApiBase/workspaces/$WorkspaceId/kqlDatabases/$ontKqlDbId" `
                    -Token $fabricToken
                $ontKqlClusterUri = $ontKqlDbDetail.properties.queryServiceUri
            }
        } catch {
            Write-Warn "Could not retrieve KQL DB details for ontology: $_"
        }

        try {
            $fabricToken = Get-FabricToken
            & $buildOntologyScript `
                -WorkspaceId $WorkspaceId `
                -LakehouseId $lakehouseId `
                -KqlDatabaseId $ontKqlDbId `
                -KqlClusterUri $ontKqlClusterUri `
                -KqlDatabaseName $EventhouseName `
                -OntologyId $ontologyId `
                -FabricToken $fabricToken
            Write-Success "Ontology populated with entity types, relationships, and data bindings."
        }
        catch {
            Write-Warn "Ontology build encountered an issue: $_"
            Write-Info "You can re-run: deploy\Build-Ontology.ps1 -WorkspaceId $WorkspaceId -LakehouseId $lakehouseId -OntologyId $ontologyId"
        }
    }
    else {
        Write-Info "Build-Ontology.ps1 not found. Configure entities manually in the Fabric portal."
    }
}
else {
    Write-Warn "Ontology was not created. Create it manually and run Build-Ontology.ps1"
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
Write-Host "  2. If KQL tables were not created: Upload SensorTelemetry.csv to Eventhouse manually" -ForegroundColor Yellow
Write-Host "  3. If semantic model was created manually: define relationships" -ForegroundColor Yellow
Write-Host "     (see SEMANTIC_MODEL_GUIDE.md)" -ForegroundColor Yellow
Write-Host "  4. Open ontology and configure entity types + relationships" -ForegroundColor Yellow
Write-Host "     (see SETUP_GUIDE.md Step 4)" -ForegroundColor Yellow
Write-Host "  5. RTI Dashboard: Requires 'Create Real-Time dashboards' tenant setting" -ForegroundColor Yellow
Write-Host "  6. Data Agent: Uses the Ontology as its sole data source (requires F64+)" -ForegroundColor Yellow
Write-Host "  7. Graph Query Set: Open the GQS, select graph model, copy queries from ontologies/OilGasRefinery/GraphQueries.gql" -ForegroundColor Yellow
Write-Host "  8. Operations Agent: Open agent in Fabric, add Knowledge Source (KQL DB), configure Actions, then Start" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Fabric Portal: https://app.fabric.microsoft.com/" -ForegroundColor Cyan
Write-Host ""
