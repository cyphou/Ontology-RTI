<#
.SYNOPSIS
    Deploy a Fabric Operations Agent for Oil & Gas Refinery real-time monitoring.
.DESCRIPTION
    Creates an Operations Agent in Microsoft Fabric (Real-Time Intelligence) and
    configures it with business goals and instructions for refinery operations.

    The Operations Agent continuously monitors KQL Database telemetry data and
    sends actionable recommendations via Microsoft Teams.

    WHAT IT DEPLOYS:
      1. Creates the OperationsAgent item in the workspace
      2. Pushes goals & instructions via updateDefinition API
      3. Prints manual steps for Knowledge Source (KQL DB) and Actions setup

    PREREQUISITES:
      - Fabric capacity (Trial may work for creation; F2+ recommended for execution)
      - An Eventhouse + KQL Database already deployed in the workspace
      - Tenant admin must enable: "Operations Agent" preview + Copilot/Azure OpenAI
      - Microsoft Teams account for receiving agent recommendations
      - Contributor role on the workspace

.PARAMETER WorkspaceId
    The Fabric workspace GUID.
.PARAMETER EventhouseId
    The Eventhouse GUID (default: from OilGasRefinery deployment).
.PARAMETER KqlDatabaseId
    The KQL Database GUID (default: from OilGasRefinery deployment).
.PARAMETER AgentName
    Display name for the Operations Agent (default: RefineryOperationsAgent).
#>
param(
    [Parameter(Mandatory=$true)]  [string]$WorkspaceId,
    [Parameter(Mandatory=$false)] [string]$EventhouseId  = "eebfbc02-b985-4b6e-8530-8ae2a2a6166b",
    [Parameter(Mandatory=$false)] [string]$KqlDatabaseId = "734b6c9e-a93f-4992-b709-2ae257a1df5f",
    [Parameter(Mandatory=$false)] [string]$AgentName     = "RefineryOperationsAgent"
)

# ── Authentication ──────────────────────────────────────────────────────────
$token   = (Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com").Token
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}
$apiBase = "https://api.fabric.microsoft.com/v1"

Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  Deploying Operations Agent: $AgentName" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# ── Step 1: Check if agent already exists ──────────────────────────────────
Write-Host "[Step 1] Checking for existing Operations Agent..." -ForegroundColor Yellow
$agentId = $null
try {
    $existing = Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/OperationsAgents" -Headers $headers
    $found = $existing.value | Where-Object { $_.displayName -eq $AgentName }
    if ($found) {
        $agentId = $found.id
        Write-Host "  Found existing agent: $agentId" -ForegroundColor Green
    }
} catch {
    Write-Host "  Could not list agents (may not be enabled): $($_.Exception.Message)" -ForegroundColor Yellow
}

# ── Step 2: Create the Operations Agent ────────────────────────────────────
if (-not $agentId) {
    Write-Host "[Step 2] Creating Operations Agent..." -ForegroundColor Yellow

    $createBody = @{
        displayName = $AgentName
        description = "AI Operations Agent monitoring Oil & Gas refinery real-time telemetry. Tracks equipment health, sensor anomalies, production throughput, maintenance schedules, and safety compliance."
    } | ConvertTo-Json -Depth 5

    try {
        $response = Invoke-WebRequest `
            -Uri "$apiBase/workspaces/$WorkspaceId/OperationsAgents" `
            -Method POST -Headers $headers -Body $createBody -UseBasicParsing

        if ($response.StatusCode -eq 201) {
            $agent   = $response.Content | ConvertFrom-Json
            $agentId = $agent.id
            Write-Host "  [OK] Created: $agentId" -ForegroundColor Green
        }
        elseif ($response.StatusCode -eq 202) {
            # LRO
            $opUrl = $response.Headers['Location']
            Write-Host "  Provisioning (LRO)..." -ForegroundColor Yellow
            do {
                Start-Sleep -Seconds 3
                $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers
                Write-Host "    Status: $($poll.status)"
            } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))

            if ($poll.status -eq 'Succeeded') {
                $allItems  = (Invoke-RestMethod -Uri "$apiBase/workspaces/$WorkspaceId/OperationsAgents" -Headers $headers).value
                $agentItem = $allItems | Where-Object { $_.displayName -eq $AgentName }
                $agentId   = $agentItem.id
                Write-Host "  [OK] Created: $agentId" -ForegroundColor Green
            } else {
                Write-Host "  [FAIL] Provisioning $($poll.status)" -ForegroundColor Red
                exit 1
            }
        }
    }
    catch {
        $sr = $_.Exception.Response
        if ($sr) {
            $stream  = $sr.GetResponseStream()
            $reader  = New-Object System.IO.StreamReader($stream)
            $errBody = $reader.ReadToEnd()
            Write-Host "  [ERROR] $([int]$sr.StatusCode): $errBody" -ForegroundColor Red

            if ($errBody -match 'FeatureNotAvailable|ItemTypeNotSupportedInThisRegion') {
                Write-Host ""
                Write-Host "  >>> Operations Agent preview must be enabled by your Fabric admin." -ForegroundColor Magenta
                Write-Host "  >>> Go to Admin Portal > Tenant settings > enable 'Operations Agent'" -ForegroundColor Magenta
                Write-Host "  >>> Also enable 'Copilot and Azure OpenAI Service'" -ForegroundColor Magenta
            }
        } else {
            Write-Host "  [ERROR] $($_.Exception.Message)" -ForegroundColor Red
        }
        exit 1
    }
}

if (-not $agentId) {
    Write-Host "[ERROR] Could not create or find Operations Agent." -ForegroundColor Red
    exit 1
}

# ── Step 3: Configure Goals & Instructions ─────────────────────────────────
Write-Host "[Step 3] Configuring goals and instructions..." -ForegroundColor Yellow

# Build the Configurations.json payload
$goals = @"
Monitor Oil and Gas refinery operations to ensure safe, efficient, and compliant plant performance. Key objectives:

1. EQUIPMENT HEALTH: Continuously track sensor telemetry (temperature, pressure, flow rate, vibration, level) across all process units. Detect anomalies where readings exceed MinRange/MaxRange sensor thresholds.

2. SAFETY COMPLIANCE: Monitor safety alarms in real-time. Prioritize Critical and High severity alarms. Track time-to-acknowledge and time-to-clear metrics. Identify recurring alarm patterns that may indicate systemic issues.

3. PRODUCTION OPTIMIZATION: Track production throughput (OutputBarrels), yield percentages, and energy consumption (EnergyConsumptionMMBTU) per process unit. Flag significant deviations from design capacity.

4. MAINTENANCE INTELLIGENCE: Monitor maintenance events, costs (CostUSD), and durations. Alert on equipment with recurring failures or escalating repair costs. Flag overdue preventive maintenance based on inspection schedules.

5. CRUDE OIL SUPPLY CHAIN: Track crude oil feed rates, sulfur content, and API gravity changes that may impact downstream processing.
"@

$instructions = @"
You are an AI operations agent for an Oil and Gas Refinery monitoring real-time telemetry from a KQL Database.

DATA SCHEMA - The KQL Database contains these tables:
- SensorTelemetry: Real-time sensor readings with Timestamp, SensorId, SensorName, SensorType, EquipmentId, EquipmentName, ReadingValue, MeasurementUnit, MinRange, MaxRange, IsAnomaly, ProcessUnitName, RefineryName
- FactSafetyAlarm: Safety alarms with AlarmId, SensorId, AlarmType, Severity (Critical/High/Medium/Low), AlarmTimestamp, AcknowledgedTimestamp, ClearedTimestamp, AlarmValue, ThresholdValue, ActionTaken
- FactProduction: Production records with ProductionId, ProcessUnitId, ProductId, ProductionDate, OutputBarrels, YieldPercent, QualityGrade, EnergyConsumptionMMBTU
- FactMaintenance: Maintenance events with MaintenanceId, EquipmentId, MaintenanceType (Preventive/Corrective/Predictive), Priority, StartDate, EndDate, DurationHours, CostUSD, Status, WorkOrderNumber
- DimEquipment: Equipment master with EquipmentId, EquipmentName, EquipmentType, ProcessUnitId, CriticalityLevel (Critical/High/Medium/Low), Status, LastInspectionDate

MONITORING RULES:
1. ANOMALY DETECTION: Flag any SensorTelemetry reading where ReadingValue is outside [MinRange, MaxRange] or IsAnomaly = true. Group by equipment and process unit.
2. ALARM RESPONSE: For Critical severity alarms, recommend immediate action. Track if AcknowledgedTimestamp is null (unacknowledged alarms). Calculate alarm duration.
3. PRODUCTION ALERTS: Flag when YieldPercent drops below 85% or OutputBarrels falls below 70% of process unit design capacity.
4. MAINTENANCE TRIGGERS: Alert when CostUSD for a single event exceeds 50000 or when an equipment item has more than 3 corrective maintenance events in 30 days.
5. INSPECTION OVERDUE: Flag equipment where LastInspectionDate is more than 365 days ago for Critical equipment or 730 days for non-critical.

RESPONSE FORMAT:
- Always include units of measurement (PSI, degrees F, barrels, USD)
- Reference equipment by EquipmentName, ProcessUnitName, and RefineryName
- Classify urgency as Critical / High / Medium / Low
- For recommended actions, include estimated impact and timeline
- When detecting patterns, show trend data with timestamps
"@

$configJson = @"
{
  "`$schema": "https://developer.microsoft.com/json-schemas/fabric/item/operationsAgents/definition/1.0.0/schema.json",
  "configuration": {
    "goals": "$($goals -replace '"','\"' -replace "`r`n",'\n' -replace "`n",'\n')",
    "instructions": "$($instructions -replace '"','\"' -replace "`r`n",'\n' -replace "`n",'\n')",
    "dataSources": {},
    "actions": {}
  },
  "shouldRun": false
}
"@

$configB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($configJson))

$updateBody = @{
    definition = @{
        format = "OperationsAgentV1"
        parts  = @(
            @{
                path        = "Configurations.json"
                payload     = $configB64
                payloadType = "InlineBase64"
            }
        )
    }
} | ConvertTo-Json -Depth 10

try {
    $updateResp = Invoke-WebRequest `
        -Uri "$apiBase/workspaces/$WorkspaceId/OperationsAgents/$agentId/updateDefinition" `
        -Method POST -Headers $headers -Body $updateBody -UseBasicParsing

    if ($updateResp.StatusCode -in @(200, 202)) {
        Write-Host "  [OK] Goals and instructions configured." -ForegroundColor Green

        if ($updateResp.StatusCode -eq 202) {
            $opUrl = $updateResp.Headers['Location']
            do {
                Start-Sleep -Seconds 3
                $poll = Invoke-RestMethod -Uri $opUrl -Headers $headers
                Write-Host "    Status: $($poll.status)"
            } while ($poll.status -notin @('Succeeded','Failed','Cancelled'))
        }
    }
}
catch {
    $sr = $_.Exception.Response
    if ($sr) {
        $stream = $sr.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($stream)
        Write-Host "  [WARN] Definition update: $([int]$sr.StatusCode): $($reader.ReadToEnd())" -ForegroundColor Yellow
        Write-Host "  Goals/instructions can be set manually in the Fabric UI." -ForegroundColor Yellow
    } else {
        Write-Host "  [WARN] $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# ── Step 4: Verify ─────────────────────────────────────────────────────────
Write-Host "[Step 4] Verifying deployment..." -ForegroundColor Yellow
try {
    $verifyResp = Invoke-WebRequest `
        -Uri "$apiBase/workspaces/$WorkspaceId/OperationsAgents/$agentId/getDefinition?format=OperationsAgentV1" `
        -Method POST -Headers $headers -UseBasicParsing

    $verifyDef  = ($verifyResp.Content | ConvertFrom-Json).definition.parts | Where-Object { $_.path -eq 'Configurations.json' }
    $verifyJson = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($verifyDef.payload)) | ConvertFrom-Json

    $goalsLen = $verifyJson.configuration.goals.Length
    $instrLen = $verifyJson.configuration.instructions.Length
    Write-Host "  Goals:        $goalsLen chars" -ForegroundColor White
    Write-Host "  Instructions: $instrLen chars" -ForegroundColor White
    Write-Host "  shouldRun:    $($verifyJson.shouldRun)" -ForegroundColor White

    if ($goalsLen -gt 0 -and $instrLen -gt 0) {
        Write-Host "  [OK] Definition verified." -ForegroundColor Green
    } else {
        Write-Host "  [WARN] Definition may not have persisted. Configure manually." -ForegroundColor Yellow
    }
} catch {
    Write-Host "  [WARN] Could not verify: $($_.Exception.Message)" -ForegroundColor Yellow
}

# ── Summary & Manual Steps ──────────────────────────────────────────────────
Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  Operations Agent Deployment Summary" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Agent Name:   $AgentName"
Write-Host "  Agent ID:     $agentId"
Write-Host "  Workspace:    $WorkspaceId"
Write-Host "  State:        Inactive (shouldRun: false)"
Write-Host ""
Write-Host "  Items referenced:" -ForegroundColor White
Write-Host "    Eventhouse:   $EventhouseId"
Write-Host "    KQL Database: $KqlDatabaseId"
Write-Host ""
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "  MANUAL CONFIGURATION REQUIRED" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "  The Operations Agent was created with goals and instructions," -ForegroundColor White
Write-Host "  but Knowledge Source and Actions must be configured in the" -ForegroundColor White
Write-Host "  Fabric UI (the dataSources API is not yet publicly supported)." -ForegroundColor White
Write-Host ""
Write-Host "  1. OPEN the agent in Fabric:" -ForegroundColor White
Write-Host "     https://app.fabric.microsoft.com/groups/$WorkspaceId/operationsagents/$agentId" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. KNOWLEDGE SOURCE:" -ForegroundColor White
Write-Host "     - Click 'Knowledge source' in the Agent setup page" -ForegroundColor Gray
Write-Host "     - Select Eventhouse: RefineryTelemetryEH" -ForegroundColor Gray
Write-Host "     - Select KQL Database: RefineryTelemetryDB" -ForegroundColor Gray
Write-Host "     - The agent will discover: SensorTelemetry, FactSafetyAlarm," -ForegroundColor Gray
Write-Host "       FactProduction, FactMaintenance, DimEquipment tables" -ForegroundColor Gray
Write-Host ""
Write-Host "  3. ACTIONS (optional):" -ForegroundColor White
Write-Host "     Add custom actions that trigger Power Automate flows:" -ForegroundColor Gray
Write-Host "     - 'Send Safety Alert': triggers on Critical/High alarms" -ForegroundColor Gray
Write-Host "       Parameters: EquipmentName, AlarmType, Severity, ReadingValue" -ForegroundColor Gray
Write-Host "     - 'Create Maintenance Work Order': triggers on equipment failures" -ForegroundColor Gray
Write-Host "       Parameters: EquipmentName, MaintenanceType, Priority, Description" -ForegroundColor Gray
Write-Host "     - 'Escalate Production Issue': triggers on yield/throughput drops" -ForegroundColor Gray
Write-Host "       Parameters: ProcessUnitName, CurrentYield, DesignCapacity" -ForegroundColor Gray
Write-Host ""
Write-Host "  4. SAVE the agent to generate the playbook, then click START." -ForegroundColor White
Write-Host ""
Write-Host "  5. TEAMS: Install 'Fabric Operations Agent' app in Teams" -ForegroundColor White
Write-Host "     to receive proactive recommendations." -ForegroundColor Gray
Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "  Operations Agent Deployment Complete" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

# Return the agent ID for orchestrator
return $agentId
