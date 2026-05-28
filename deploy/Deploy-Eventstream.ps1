<#
.SYNOPSIS
    Deploy an Eventstream for real-time telemetry ingestion into the domain's Eventhouse.
.DESCRIPTION
    Creates an Eventstream item in Microsoft Fabric using the definition API.
    The Eventstream is configured with:
      - A Custom App source (for sending events via SDK or REST).
      - A Default Stream that routes to the KQL Database destination.

    Each domain gets one Eventstream that feeds its Eventhouse KQL Database.

.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER OntologyType
    Domain key: OilGasRefinery, SmartBuilding, ManufacturingPlant, ITAsset, WindTurbine, Healthcare.
.PARAMETER EventstreamName
    Display name (auto-derived from OntologyType if omitted).
.PARAMETER KqlDatabaseId
    The KQL Database GUID (auto-detected if omitted).
.PARAMETER EventhouseName
    Eventhouse display name (auto-detected from domain registry if omitted).
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyType = "OilGasRefinery",
    [Parameter(Mandatory=$false)] [string]$EventstreamName,
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId,
    [Parameter(Mandatory=$false)] [string]$EventhouseName
)

# ── Domain defaults ──────────────────────────────────────────────────────────
$domainConfig = @{
    OilGasRefinery     = @{ Name = "RefineryTelemetryStream";     EH = "RefineryTelemetryEH";     Table = "SensorReading" }
    SmartBuilding      = @{ Name = "BuildingTelemetryStream";     EH = "BuildingTelemetryEH";     Table = "SensorReading" }
    ManufacturingPlant = @{ Name = "PlantTelemetryStream";        EH = "PlantTelemetryEH";        Table = "SensorReading" }
    ITAsset            = @{ Name = "ITTelemetryStream";           EH = "ITTelemetryEH";           Table = "ServerMetric" }
    WindTurbine        = @{ Name = "WindTelemetryStream";         EH = "WindTelemetryEH";         Table = "TurbineReading" }
    Healthcare         = @{ Name = "HealthcareTelemetryStream";  EH = "HealthcareTelemetryEH";   Table = "PatientVitals" }
}

$config = $domainConfig[$OntologyType]
if (-not $config) {
    Write-Host "[ERROR] Unknown OntologyType: $OntologyType" -ForegroundColor Red
    exit 1
}

if (-not $EventstreamName) { $EventstreamName = $config.Name }
if (-not $EventhouseName)  { $EventhouseName  = $config.EH }

# ── Authentication ──────────────────────────────────────────────────────────
$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Eventstream: $EventstreamName ===" -ForegroundColor Cyan

# ── Auto-detect KQL Database ───────────────────────────────────────────────
$allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value

$KqlDatabaseName = ""

if (-not $KqlDatabaseId) {
    Write-Host "Auto-detecting KQL Database from workspace..." -ForegroundColor Yellow
    $kqlDb = $allItems | Where-Object { $_.type -eq 'KQLDatabase' } | Select-Object -First 1
    if ($kqlDb) {
        $KqlDatabaseId = $kqlDb.id
        $KqlDatabaseName = $kqlDb.displayName
        Write-Host "  Found KQL Database: $($kqlDb.displayName) ($KqlDatabaseId)" -ForegroundColor Gray
    } else {
        Write-Host "[WARN] No KQL Database found. Eventstream will be created without KQL destination." -ForegroundColor Yellow
    }
}

if ($KqlDatabaseId -and -not $KqlDatabaseName) {
    try {
        $kqlDbInfo = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/kqlDatabases/$KqlDatabaseId" -Headers $headers
        $KqlDatabaseName = $kqlDbInfo.displayName
    } catch {
        $KqlDatabaseName = ""
    }
}

$kqlDestinationType = "Eventhouse"
$triedKqlFallback = $false

# ── Build Eventstream Definition ────────────────────────────────────────────
# The eventstream.json defines the topology: source -> stream -> destination.
# Using Custom App source (no external dependency) for easy SDK/REST ingestion.

$sourceName = "${EventstreamName}-source"
$streamName = "${EventstreamName}-stream"

$eventstreamDef = @{
    sources = @(
        @{
            name = $sourceName
            type = "CustomEndpoint"
            properties = @{
                inputSerialization = @{
                    type = "Json"
                    properties = @{
                        encoding = "UTF8"
                    }
                }
            }
        }
    )
    destinations = @()
    streams = @(
        @{
            name = $streamName
            type = "DefaultStream"
            properties = @{}
            inputNodes = @(
                @{ name = $sourceName }
            )
        }
    )
    operators = @()
    compatibilityLevel = "1.0"
}

# Add KQL Database destination if available
if ($KqlDatabaseId) {
    $eventstreamDef.destinations += @{
        name = "${EventstreamName}-kql"
        type = $kqlDestinationType
        properties = @{
            dataIngestionMode = "ProcessedIngestion"
            workspaceId = $WorkspaceId
            itemId = $KqlDatabaseId
            databaseName = $KqlDatabaseName
            tableName = $config.Table
            inputSerialization = @{
                type = "Json"
                properties = @{
                    encoding = "UTF8"
                }
            }
        }
        inputNodes = @(
            @{ name = $streamName }
        )
    }
}

# Eventstream properties (low throughput for dev/demo)
$eventstreamProps = @{
    retentionTimeInDays = 1
    eventThroughputLevel = "Low"
}

# Serialize and encode
$esJson = $eventstreamDef | ConvertTo-Json -Depth 15 -Compress
$esPropsJson = $eventstreamProps | ConvertTo-Json -Depth 5 -Compress
$esB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($esJson))
$esPropsB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($esPropsJson))

# ── Create Eventstream with definition ──────────────────────────────────────
Write-Host "Creating Eventstream with definition..." -ForegroundColor Yellow

$createBody = '{"displayName":"' + ($EventstreamName -replace '"', '\"') + '",' +
    '"type":"Eventstream",' +
    '"description":"Real-time telemetry ingestion for ' + $OntologyType + ' domain",' +
    '"definition":{"parts":[' +
        '{"path":"eventstream.json","payload":"' + $esB64 + '","payloadType":"InlineBase64"},' +
        '{"path":"eventstreamProperties.json","payload":"' + $esPropsB64 + '","payloadType":"InlineBase64"}' +
    ']}}'

$eventstreamId = $null
try {
    $response = Invoke-WebRequest `
        -Uri "$apiBase/workspaces/$WorkspaceId/items" `
        -Method POST -Headers $headers -Body $createBody -UseBasicParsing

    if ($response.StatusCode -eq 201) {
        $es = $response.Content | ConvertFrom-Json
        $eventstreamId = $es.id
        Write-Host "[OK] Eventstream created: $eventstreamId" -ForegroundColor Green
    }
    elseif ($response.StatusCode -eq 202) {
        $opUrl = $response.Headers['Location']
        if ($opUrl -is [array]) { $opUrl = $opUrl[0] }
        Write-Host "LRO started, polling..." -ForegroundColor Yellow
        do {
            Start-Sleep -Seconds 3
            $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers
            Write-Host "  Status: $($poll.status)"
        } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))

        if ($poll.status -eq 'Succeeded') {
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $esItem = $allItems | Where-Object { $_.displayName -eq $EventstreamName -and $_.type -eq 'Eventstream' }
            if ($esItem) { $eventstreamId = $esItem.id }
            Write-Host "[OK] Eventstream created: $eventstreamId" -ForegroundColor Green
        } else {
            Write-Host "[FAIL] Eventstream creation: $($poll.status)" -ForegroundColor Red
        }
    }
}
catch {
    $errBody = ""
    if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
        $errBody = $_.ErrorDetails.Message
    } elseif ($_.Exception.Response) {
        try { $sr = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream()); $errBody = $sr.ReadToEnd(); $sr.Close() } catch { $errBody = $_.Exception.Message }
    } else { $errBody = $_.Exception.Message }

    $sc = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 0 }
    if ($sc) { Write-Host "[ERROR] $sc`: $errBody" -ForegroundColor Red }
    else { Write-Host "[ERROR] $errBody" -ForegroundColor Red }

    if (-not $triedKqlFallback -and $KqlDatabaseId -and $sc -eq 400) {
        Write-Host "Retrying Eventstream with destination type 'Eventhouse'..." -ForegroundColor Yellow
        $triedKqlFallback = $true
        $kqlDestinationType = "Eventhouse"

        # Rebuild destination node with fallback destination type.
        $eventstreamDef.destinations = @(
            @{
                name = "${EventstreamName}-kql"
                type = $kqlDestinationType
                properties = @{
                    dataIngestionMode = "ProcessedIngestion"
                    workspaceId = $WorkspaceId
                    itemId = $KqlDatabaseId
                    databaseName = $KqlDatabaseName
                    tableName = $config.Table
                    inputSerialization = @{
                        type = "Json"
                        properties = @{
                            encoding = "UTF8"
                        }
                    }
                }
                inputNodes = @(
                    @{ name = $streamName }
                )
            }
        )

        $esJson = $eventstreamDef | ConvertTo-Json -Depth 15 -Compress
        $esB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($esJson))
        $createBody = '{"displayName":"' + ($EventstreamName -replace '"', '\"') + '",' +
            '"type":"Eventstream",' +
            '"description":"Real-time telemetry ingestion for ' + $OntologyType + ' domain",' +
            '"definition":{"parts":[' +
                '{"path":"eventstream.json","payload":"' + $esB64 + '","payloadType":"InlineBase64"},' +
                '{"path":"eventstreamProperties.json","payload":"' + $esPropsB64 + '","payloadType":"InlineBase64"}' +
            ']}}'

        try {
            $response = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
            if ($response.StatusCode -eq 201) {
                $es = $response.Content | ConvertFrom-Json
                $eventstreamId = $es.id
                Write-Host "[OK] Eventstream created with fallback type: $eventstreamId" -ForegroundColor Green
            }
            elseif ($response.StatusCode -eq 202) {
                $opUrl = $response.Headers['Location']
                if ($opUrl -is [array]) { $opUrl = $opUrl[0] }
                Write-Host "LRO started, polling..." -ForegroundColor Yellow
                do {
                    Start-Sleep -Seconds 3
                    $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers
                    Write-Host "  Status: $($poll.status)"
                } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))

                if ($poll.status -eq 'Succeeded') {
                    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
                    $esItem = $allItems | Where-Object { $_.displayName -eq $EventstreamName -and $_.type -eq 'Eventstream' }
                    if ($esItem) { $eventstreamId = $esItem.id }
                    Write-Host "[OK] Eventstream created with fallback type: $eventstreamId" -ForegroundColor Green
                } else {
                    Write-Host "[FAIL] Eventstream creation (fallback): $($poll.status)" -ForegroundColor Red
                }
            }
        } catch {
            $retryErr = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
            Write-Host "[ERROR] Fallback failed: $retryErr" -ForegroundColor Red

            if ($retryErr -match 'ItemDisplayNameNotAvailableYet') {
                Write-Host "Name is temporarily reserved. Waiting 20s and retrying fallback create once..." -ForegroundColor Yellow
                Start-Sleep -Seconds 20

                try {
                    $response2 = Invoke-WebRequest -Uri "$apiBase/workspaces/$WorkspaceId/items" -Method POST -Headers $headers -Body $createBody -UseBasicParsing
                    if ($response2.StatusCode -eq 201) {
                        $es2 = $response2.Content | ConvertFrom-Json
                        $eventstreamId = $es2.id
                        Write-Host "[OK] Eventstream created on delayed retry: $eventstreamId" -ForegroundColor Green
                    }
                    elseif ($response2.StatusCode -eq 202) {
                        $opUrl2 = $response2.Headers['Location']
                        if ($opUrl2 -is [array]) { $opUrl2 = $opUrl2[0] }
                        Write-Host "LRO started, polling..." -ForegroundColor Yellow
                        do {
                            Start-Sleep -Seconds 3
                            $poll2 = Invoke-RestMethod -Uri $opUrl2 -Headers $headers
                            Write-Host "  Status: $($poll2.status)"
                        } while ($poll2.status -notin @('Succeeded','Failed','Cancelled'))

                        if ($poll2.status -eq 'Succeeded') {
                            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
                            $esItem2 = $allItems | Where-Object { $_.displayName -eq $EventstreamName -and $_.type -eq 'Eventstream' }
                            if ($esItem2) { $eventstreamId = $esItem2.id }
                            Write-Host "[OK] Eventstream created on delayed retry: $eventstreamId" -ForegroundColor Green
                        } else {
                            Write-Host "[FAIL] Delayed fallback create: $($poll2.status)" -ForegroundColor Red
                        }
                    }
                } catch {
                    $retryErr2 = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { $_.Exception.Message }
                    Write-Host "[ERROR] Delayed fallback retry failed: $retryErr2" -ForegroundColor Red
                }
            }
        }
    }

    if ($errBody -match 'ItemDisplayNameAlreadyInUse') {
        Write-Host "  Eventstream '$EventstreamName' already exists." -ForegroundColor Yellow
        try {
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $existing = $allItems | Where-Object { $_.displayName -eq $EventstreamName -and $_.type -eq 'Eventstream' }
            if ($existing) {
                $eventstreamId = $existing.id
                Write-Host "  Existing Eventstream ID: $eventstreamId" -ForegroundColor Gray
            }
        } catch {}
    }
    elseif ($errBody -match 'FeatureNotAvailable') {
        Write-Host ""
        Write-Host ">>> BLOCKED: The Eventstream tenant setting may be disabled." -ForegroundColor Magenta
        Write-Host ">>> Ask your Fabric admin to enable it in:" -ForegroundColor Magenta
        Write-Host ">>>   Admin Portal > Tenant settings > Real-Time Intelligence" -ForegroundColor Magenta
    }
}

if (-not $eventstreamId) {
    throw "Eventstream deployment did not produce an Eventstream ID."
}

# ── Summary ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Eventstream Deployment Summary ===" -ForegroundColor Cyan
Write-Host "  Name:           $EventstreamName"
Write-Host "  Eventstream ID: $eventstreamId"
Write-Host "  Source:         Custom App (SDK/REST ingestion)"
Write-Host "  Destination:    KQL Database ($KqlDatabaseId)"
Write-Host "  Target Table:   $($config.Table)"
Write-Host "  Domain:         $OntologyType"
Write-Host ""
Write-Host "To send events, use the Eventstream connection string from the Fabric UI:" -ForegroundColor White
Write-Host "  1. Open the Eventstream '$EventstreamName' in Fabric" -ForegroundColor White
Write-Host "  2. Click the Custom App source node" -ForegroundColor White
Write-Host "  3. Copy the Event Hub-compatible connection string" -ForegroundColor White
Write-Host "  4. Use Azure Event Hubs SDK to send JSON events" -ForegroundColor White
Write-Host ""
Write-Host "=== Eventstream Deployment Complete ===" -ForegroundColor Cyan
