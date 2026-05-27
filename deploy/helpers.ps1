<#
.SYNOPSIS
    Shared helper functions for Fabric Ontology Accelerator deployment scripts.
.DESCRIPTION
    Provides common functions used across all deployment scripts:
    - Write-Step, Write-Info, Write-Success, Write-Warn (formatted output)
    - Get-FabricToken, Get-StorageToken (authentication — az cli preferred, Az PS fallback)
    - Invoke-FabricApi (REST API with retry/LRO handling, PS7 header array fix)
    - Wait-FabricOperation (long-running operation polling)
    - Upload-FileToOneLake (DFS protocol file upload with retry)
    - DeterministicGuid (MD5-based idempotent GUID generation)
    - ToBase64 (UTF-8 string to Base64 encoding)

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
        Priority: Service Principal > az cli (MFA/CAE) > Az PowerShell.
    #>
    if ($script:ClientId -and $script:ClientSecret -and $script:TenantId) {
        $secPwd = ConvertTo-SecureString $script:ClientSecret -AsPlainText -Force
        $cred = New-Object System.Management.Automation.PSCredential($script:ClientId, $secPwd)
        Connect-AzAccount -ServicePrincipal -Credential $cred -TenantId $script:TenantId -ErrorAction Stop | Out-Null
        $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $token.Token
    }
    # Prefer az cli (handles MFA/CAE better than Az PowerShell)
    try {
        $token = az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv 2>$null
        if ($token) { return $token }
    } catch {}
    # Fallback to Az PowerShell
    try {
        $token = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Fabric API token. Run 'az login' or 'Connect-AzAccount' first. Error: $_"
        throw
    }
}

function Get-StorageToken {
    <#
    .SYNOPSIS
        Retrieves a bearer token for OneLake (Storage) API.
        Priority: az cli > Az PowerShell.
    #>
    # Prefer az cli
    try {
        $token = az account get-access-token --resource "https://storage.azure.com/" --query accessToken -o tsv 2>$null
        if ($token) { return $token }
    } catch {}
    # Fallback to Az PowerShell
    try {
        $token = Get-AzAccessToken -ResourceTypeName Storage
        return $token.Token
    }
    catch {
        Write-Error "Failed to get Storage token. Run 'az login' first. Error: $_"
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
                if ($loc -is [array]) { $loc = $loc[0] }
                $opId = $webResponse.Headers["x-ms-operation-id"]
                if ($opId -is [array]) { $opId = $opId[0] }
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
            $errRecord = $_
            $ex = $errRecord.Exception
            $sc = $null
            $errorBody = ""
            if ($ex -and $ex.Response) {
                $sc = [int]$ex.Response.StatusCode
                # PS 7: ErrorDetails.Message has the response body; PS 5: use GetResponseStream
                if ($errRecord.ErrorDetails -and $errRecord.ErrorDetails.Message) {
                    $errorBody = $errRecord.ErrorDetails.Message
                } else {
                    try {
                        $sr = New-Object System.IO.StreamReader($ex.Response.GetResponseStream())
                        $errorBody = $sr.ReadToEnd()
                        $sr.Close()
                    } catch { }
                }
                # Fallback: exception message itself (PS 7 includes body in message)
                if (-not $errorBody -and $ex.Message) { $errorBody = $ex.Message }
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
            if ($status.status -in @("Failed","Cancelled")) {
                $errMsg = if ($status.error) { $status.error.message } else { $status.status }
                throw "Fabric operation $($status.status): $errMsg"
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
        Includes retry logic with exponential backoff.
    #>
    param(
        [string]$LocalFilePath,
        [string]$OneLakePath,
        [string]$Token
    )

    $fileBytes = [System.IO.File]::ReadAllBytes($LocalFilePath)
    $fileName = [System.IO.Path]::GetFileName($LocalFilePath)
    $fileSize = $fileBytes.Length
    $headers = @{ "Authorization" = "Bearer $Token" }

    $maxRetries = 3
    for ($attempt = 1; $attempt -le $maxRetries; $attempt++) {
        try {
            # Step 1: Create the file (PUT with resource=file)
            Invoke-RestMethod -Method Put -Uri "${OneLakePath}/${fileName}?resource=file" -Headers ($headers + @{ "Content-Length" = "0" }) | Out-Null

            # Step 2: Append data (PATCH with action=append)
            Invoke-RestMethod -Method Patch -Uri "${OneLakePath}/${fileName}?action=append&position=0" -Headers ($headers + @{ "Content-Length" = $fileSize.ToString() }) -Body $fileBytes | Out-Null

            # Step 3: Flush (PATCH with action=flush)
            Invoke-RestMethod -Method Patch -Uri "${OneLakePath}/${fileName}?action=flush&position=$fileSize" -Headers ($headers + @{ "Content-Length" = "0" }) | Out-Null
            return
        } catch {
            if ($attempt -eq $maxRetries) { throw }
            Write-Warn "Upload attempt $attempt failed: $($_.Exception.Message) — retrying in $($attempt * 5)s..."
            Start-Sleep -Seconds ($attempt * 5)
        }
    }
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

function DeterministicGuid([string]$seed) {
    <#
    .SYNOPSIS
        Generates a deterministic GUID from a seed string (MD5-based, idempotent deployments).
    #>
    $hash = [System.Security.Cryptography.MD5]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes($seed))
    return ([guid]::new($hash)).ToString()
}

function ToBase64([string]$text) {
    <#
    .SYNOPSIS
        Base64-encodes a UTF-8 string.
    #>
    return [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($text))
}
