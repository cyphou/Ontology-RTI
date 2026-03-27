<#
.SYNOPSIS
    Shared helper functions for Fabric Ontology Accelerator deployment scripts.
.DESCRIPTION
    Provides common functions used across all deployment scripts:
    - Write-Step, Write-Info, Write-Success, Write-Warn (formatted output)
    - Get-FabricToken, Get-StorageToken (authentication)
    - Invoke-FabricApi (REST API with retry/LRO handling)
    - Wait-FabricOperation (long-running operation polling)
    - Upload-FileToOneLake (DFS protocol file upload)

    Dot-source this file from any deployment script:
        . (Join-Path $PSScriptRoot "helpers.ps1")
#>

# ============================================================================
# OUTPUT FORMATTING
# ============================================================================

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host ("=" * 69) -ForegroundColor Cyan
    Write-Host " $Message" -ForegroundColor Cyan
    Write-Host ("=" * 69) -ForegroundColor Cyan
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

# ============================================================================
# AUTHENTICATION
# ============================================================================

function Get-FabricToken {
    <#
    .SYNOPSIS
        Retrieves a bearer token for Fabric REST API.
        Supports Service Principal auth when $script:ClientId, $script:ClientSecret, $script:TenantId are set.
    #>
    if ($script:ClientId -and $script:ClientSecret -and $script:TenantId) {
        $secPwd = ConvertTo-SecureString $script:ClientSecret -AsPlainText -Force
        $cred = New-Object System.Management.Automation.PSCredential($script:ClientId, $secPwd)
        Connect-AzAccount -ServicePrincipal -Credential $cred -TenantId $script:TenantId -ErrorAction Stop | Out-Null
    }
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

# ============================================================================
# FABRIC REST API
# ============================================================================

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
                Method          = $Method
                Uri             = $Uri
                Headers         = $headers
                UseBasicParsing = $true
            }
            if ($BodyJson) { $params["Body"] = $BodyJson }

            $webResponse = Invoke-WebRequest @params
            $statusCode = $webResponse.StatusCode

            # Handle 202 Accepted (Long Running Operation)
            if ($statusCode -eq 202) {
                $loc = $webResponse.Headers["Location"]
                $opId = $webResponse.Headers["x-ms-operation-id"]
                if ($loc) {
                    $operationUrl = $loc
                }
                elseif ($opId) {
                    $operationUrl = "$script:FabricApiBase/operations/$opId"
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
            $sc = $null
            $errorBody = ""
            if ($ex -and $ex.Response) {
                $sc = [int]$ex.Response.StatusCode
                try {
                    $sr = New-Object System.IO.StreamReader($ex.Response.GetResponseStream())
                    $errorBody = $sr.ReadToEnd()
                    $sr.Close()
                } catch { }
            }

            $isRetriable = ($errorBody -like "*isRetriable*true*" -or $errorBody -like "*NotAvailableYet*")

            if ($sc -eq 429 -or $isRetriable) {
                $retryAfter = if ($isRetriable) { 15 } else { 30 }
                try {
                    $ra = $ex.Response.Headers | Where-Object { $_.Key -eq "Retry-After" } | Select-Object -ExpandProperty Value -First 1
                    if ($ra) { $retryAfter = [int]$ra }
                } catch { }
                $reason = if ($isRetriable) { "Retriable error" } else { "Rate limited (429)" }
                Write-Warn "$reason. Retrying after $retryAfter seconds (attempt $attempt/$MaxRetries)..."
                Start-Sleep -Seconds $retryAfter
            }
            else {
                if ($errorBody) { throw "Fabric API error (HTTP $sc): $errorBody" }
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
            Write-Info "  Operation status: $($status.status) ($elapsed`s elapsed)"

            if ($status.status -eq "Succeeded") { return $status }
            if ($status.status -eq "Failed") {
                throw "Fabric operation failed: $($status | ConvertTo-Json -Depth 5 -Compress)"
            }
        }
        catch {
            if ($_.Exception.Response -and [int]$_.Exception.Response.StatusCode -eq 429) {
                Write-Warn "Rate limited while polling. Waiting 30s..."
                Start-Sleep -Seconds 30
            }
            else { throw }
        }
    }
    throw "Operation timed out after $TimeoutSeconds seconds"
}

# ============================================================================
# ONELAKE FILE UPLOAD
# ============================================================================

function Upload-FileToOneLake {
    <#
    .SYNOPSIS
        Uploads a local file to OneLake via DFS API (PUT + PATCH append + PATCH flush).
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
    Invoke-RestMethod -Method Put -Uri $createUri -Headers @{
        "Authorization" = "Bearer $Token"
        "Content-Length" = "0"
    } | Out-Null

    # Step 2: Append data (PATCH with action=append)
    $appendUri = "${OneLakePath}/${fileName}?action=append&position=0"
    Invoke-RestMethod -Method Patch -Uri $appendUri -Headers @{
        "Authorization"  = "Bearer $Token"
        "Content-Type"   = "application/octet-stream"
        "Content-Length"  = $fileBytes.Length.ToString()
    } -Body $fileBytes | Out-Null

    # Step 3: Flush (PATCH with action=flush)
    $flushUri = "${OneLakePath}/${fileName}?action=flush&position=$($fileBytes.Length)"
    Invoke-RestMethod -Method Patch -Uri $flushUri -Headers @{
        "Authorization" = "Bearer $Token"
        "Content-Length" = "0"
    } | Out-Null
}
