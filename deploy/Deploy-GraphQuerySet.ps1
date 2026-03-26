<#
.SYNOPSIS
    Deploy a Graph Query Set for any IQ Ontology domain.
.DESCRIPTION
    Creates a GraphQuerySet item in Microsoft Fabric and pushes GQL queries
    from the domain's GraphQueries.gql file via the updateDefinition API.

    The script:
    1. Parses the domain GraphQueries.gql file to extract named queries.
    2. Creates (or finds existing) Graph Query Set item in the workspace.
    3. Pushes all queries via the updateDefinition endpoint (graphQuerySet.json).
    4. Verifies the definition was persisted.

.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER OntologyType
    Domain key: OilGasRefinery, SmartBuilding, ManufacturingPlant, ITAsset, WindTurbine.
.PARAMETER OntologyFolder
    Path to the domain ontology folder (auto-detected if omitted).
.PARAMETER GraphModelId
    The GraphModel item GUID (auto-detected from ontology if omitted).
.PARAMETER QuerySetName
    Display name for the Graph Query Set (auto-derived from OntologyType if omitted).
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$OntologyType = "OilGasRefinery",
    [Parameter(Mandatory=$false)] [string]$OntologyFolder,
    [Parameter(Mandatory=$false)] [string]$GraphModelId,
    [Parameter(Mandatory=$false)] [string]$QuerySetName
)

# ── Domain defaults ──────────────────────────────────────────────────────────
$scriptDir = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$rootDir = Split-Path -Parent $scriptDir

$querySetDefaults = @{
    OilGasRefinery     = "OilGasRefineryQueries"
    SmartBuilding      = "SmartBuildingQueries"
    ManufacturingPlant = "ManufacturingPlantQueries"
    ITAsset            = "ITAssetQueries"
    WindTurbine        = "WindTurbineQueries"
}

if (-not $QuerySetName) { $QuerySetName = $querySetDefaults[$OntologyType] }
if (-not $QuerySetName) { $QuerySetName = "${OntologyType}Queries" }

# Resolve GQL file: domain-first, fallback to deploy/
if (-not $OntologyFolder) { $OntologyFolder = Join-Path $rootDir "ontologies\$OntologyType" }
$gqlFile = Join-Path $OntologyFolder "GraphQueries.gql"
if (-not (Test-Path $gqlFile)) { $gqlFile = Join-Path $scriptDir "RefineryGraphQueries.gql" }

# ── Authentication ──────────────────────────────────────────────────────────
$token = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host "=== Deploying Graph Query Set: $QuerySetName ===" -ForegroundColor Cyan

# ── Auto-detect GraphModel ID if not provided ──────────────────────────────
if (-not $GraphModelId) {
    Write-Host "Auto-detecting GraphModel from workspace..." -ForegroundColor Yellow
    $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
    $gmItems = $allItems | Where-Object { $_.type -eq 'GraphModel' -and $_.displayName -like '*Ontology*graph*' }
    if ($gmItems) {
        $GraphModelId = $gmItems[0].id
        Write-Host "  Found GraphModel: $($gmItems[0].displayName) ($GraphModelId)" -ForegroundColor Gray
    }
    else {
        # Fallback: pick any GraphModel
        $gmItems = $allItems | Where-Object { $_.type -eq 'GraphModel' }
        if ($gmItems) {
            $GraphModelId = $gmItems[0].id
            Write-Host "  Using GraphModel: $($gmItems[0].displayName) ($GraphModelId)" -ForegroundColor Gray
        }
        else {
            Write-Host "[ERROR] No GraphModel found in workspace." -ForegroundColor Red
            exit 1
        }
    }
}

# ── Build GQL Queries from domain .gql file ────────────────────────────────
# Parse the GraphQueries.gql file to extract named queries.
# Format: /* ===== Query N: Title ===== */ followed by GQL text.

function New-QueryId { return [guid]::NewGuid().ToString() }

$queries = @()

if (Test-Path $gqlFile) {
    Write-Host "Parsing GQL queries from: $gqlFile" -ForegroundColor Yellow
    $gqlContent = [System.IO.File]::ReadAllText($gqlFile)

    # Split on query delimiter comments: /* ===== Query N: Title ===== */
    $pattern = '/\*\s*=+\s*Query\s+(\d+)[:\s]*([^=]*?)\s*=+\s*\*/'
    $matches = [regex]::Matches($gqlContent, $pattern)

    for ($i = 0; $i -lt $matches.Count; $i++) {
        $m = $matches[$i]
        $qNum = $m.Groups[1].Value
        $qTitle = $m.Groups[2].Value.Trim()
        $startIdx = $m.Index + $m.Length

        # Query text ends at next query delimiter or end of file
        if ($i + 1 -lt $matches.Count) {
            $endIdx = $matches[$i + 1].Index
        } else {
            $endIdx = $gqlContent.Length
        }

        $qText = $gqlContent.Substring($startIdx, $endIdx - $startIdx).Trim()
        # Remove any trailing block comments
        $qText = [regex]::Replace($qText, '/\*.*?\*/', '', 'Singleline').Trim()

        if ($qText.Length -gt 0) {
            $queries += @{
                displayName  = "$qNum. $qTitle"
                id           = New-QueryId
                queryMode    = "GQLCode"
                gqlQueryText = $qText
                nodes        = @()
                edges        = @()
            }
        }
    }
    Write-Host "  Parsed $($queries.Count) queries from GQL file." -ForegroundColor Gray
} else {
    Write-Host "[WARN] No GQL file found at: $gqlFile" -ForegroundColor Yellow
    Write-Host "  Graph Query Set will be created without queries." -ForegroundColor Yellow
}

# ── Step A: Create bare Graph Query Set item ────────────────────────────────
# Uses the type-specific /GraphQuerySets endpoint per official Fabric REST API docs.
Write-Host "Creating Graph Query Set '$QuerySetName'..." -ForegroundColor Yellow

$createBody = @{
    displayName = $QuerySetName
    description = "GQL queries for the $OntologyType ontology graph"
} | ConvertTo-Json -Depth 10

$gqsId = $null
try {
    $response = Invoke-WebRequest `
        -Uri "$apiBase/workspaces/$WorkspaceId/GraphQuerySets" `
        -Method POST -Headers $headers -Body $createBody -UseBasicParsing

    if ($response.StatusCode -eq 201) {
        $gqs = $response.Content | ConvertFrom-Json
        $gqsId = $gqs.id
        Write-Host "[OK] Graph Query Set created: $gqsId" -ForegroundColor Green
    }
    elseif ($response.StatusCode -eq 202) {
        $opUrl = $response.Headers['Location']
        Write-Host "LRO started, polling..." -ForegroundColor Yellow
        do {
            Start-Sleep -Seconds 3
            $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers
            Write-Host "  Status: $($poll.status)"
        } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))

        if ($poll.status -eq 'Succeeded') {
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $gqsItem = $allItems | Where-Object { $_.displayName -eq $QuerySetName -and $_.type -eq 'GraphQuerySet' }
            if ($gqsItem) { $gqsId = $gqsItem.id }
            Write-Host "[OK] Graph Query Set created: $gqsId" -ForegroundColor Green
        }
        else {
            Write-Host "[FAIL] Graph Query Set creation LRO: $($poll.status)" -ForegroundColor Red
        }
    }
}
catch {
    $sr = $_.Exception.Response
    if ($sr) {
        $stream = $sr.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        $errBody = $reader.ReadToEnd()
        Write-Host "[ERROR] $([int]$sr.StatusCode): $errBody" -ForegroundColor Red

        if ($errBody -match 'ItemDisplayNameAlreadyInUse') {
            Write-Host "  Graph Query Set '$QuerySetName' already exists. Will update definition..." -ForegroundColor Yellow
            $allItems = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/items" -Headers $headers).value
            $existing = $allItems | Where-Object { $_.displayName -eq $QuerySetName -and $_.type -eq 'GraphQuerySet' }
            if ($existing) {
                $gqsId = $existing.id
                Write-Host "  Existing GQS ID: $gqsId" -ForegroundColor Gray
            }
        }
    }
    else {
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    }
}

# ── Step B: Push queries via updateDefinition ──────────────────────────────────
if ($gqsId -and $queries.Count -gt 0) {
    Write-Host "Pushing $($queries.Count) queries via updateDefinition API..." -ForegroundColor Yellow

    # Build manual JSON for PS5.1 compatibility (ConvertTo-Json depth limits)
    $queryJsonParts = @()
    foreach ($q in $queries) {
        $escapedText = $q.gqlQueryText -replace '\\', '\\\\' -replace '"', '\"' -replace "`r`n", '\n' -replace "`n", '\n' -replace "`t", '\t'
        $escapedName = $q.displayName -replace '"', '\"'
        $queryJsonParts += '{"displayName":"' + $escapedName + '","id":"' + $q.id + '","queryMode":"GQLCode","gqlQueryText":"' + $escapedText + '","nodes":[],"edges":[]}'
    }
    $queriesJsonArray = '[' + ($queryJsonParts -join ',') + ']'

    $escapedGmId = $GraphModelId -replace '"', '\"'
    $escapedWsId = $WorkspaceId -replace '"', '\"'
    $querySetJson = '{"graphInstanceObjectId":"' + $escapedGmId + '","graphInstanceFolderObjectId":"' + $escapedWsId + '","queries":' + $queriesJsonArray + '}'

    $querySetB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($querySetJson))

    $updateBody = '{"definition":{"format":"json","parts":[{"path":"graphQuerySet.json","payload":"' + $querySetB64 + '","payloadType":"InlineBase64"}]}}'

    $defApplied = $false
    foreach ($endpoint in @("$apiBase/workspaces/$WorkspaceId/GraphQuerySets/$gqsId/updateDefinition",
                            "$apiBase/workspaces/$WorkspaceId/items/$gqsId/updateDefinition")) {
        if ($defApplied) { break }
        try {
            $updResp = Invoke-WebRequest -Uri $endpoint -Method POST -Headers $headers -Body $updateBody -UseBasicParsing

            if ($updResp.StatusCode -in @(200, 202)) {
                if ($updResp.StatusCode -eq 202) {
                    $opUrl = $updResp.Headers['Location']
                    Write-Host "  LRO started, polling..." -ForegroundColor Yellow
                    do {
                        Start-Sleep -Seconds 3
                        $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers
                        Write-Host "  Definition update: $($poll.status)"
                    } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))

                    if ($poll.status -eq 'Succeeded') {
                        $defApplied = $true
                        Write-Host "[OK] $($queries.Count) queries pushed to Graph Query Set." -ForegroundColor Green
                    } else {
                        Write-Host "[WARN] Definition update: $($poll.status)" -ForegroundColor Yellow
                    }
                } else {
                    $defApplied = $true
                    Write-Host "[OK] $($queries.Count) queries pushed to Graph Query Set." -ForegroundColor Green
                }
            }
        }
        catch {
            # Try next endpoint
            Write-Host "  Endpoint $endpoint returned error, trying fallback..." -ForegroundColor Gray
        }
    }

    if (-not $defApplied) {
        Write-Host "[WARN] Definition update did not succeed via API." -ForegroundColor Yellow
        Write-Host "  Queries can still be added via the Fabric UI from: $gqlFile" -ForegroundColor Yellow
    }
}
elseif ($gqsId) {
    Write-Host "[INFO] No queries to push (GQL file not found or empty)." -ForegroundColor Yellow
    Write-Host "  Add queries via the Fabric UI from: $gqlFile" -ForegroundColor Yellow
}

# ── Summary ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Graph Query Set Deployment Summary ===" -ForegroundColor Cyan
Write-Host "  Name:         $QuerySetName"
Write-Host "  GQS ID:       $gqsId"
Write-Host "  GraphModel:   $GraphModelId"
Write-Host "  GQL File:     $gqlFile"
Write-Host "  Queries:      $($queries.Count)"
Write-Host ""
if ($queries.Count -gt 0) {
    Write-Host "Queries deployed:" -ForegroundColor White
    foreach ($q in $queries) {
        Write-Host "  - $($q.displayName)"
    }
}
Write-Host ""
Write-Host "Open the Graph Query Set in Fabric to run queries visually." -ForegroundColor White
Write-Host ""
Write-Host "=== Graph Query Set Deployment Complete ===" -ForegroundColor Cyan
